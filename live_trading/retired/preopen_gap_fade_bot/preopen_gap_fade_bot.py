#!/usr/bin/env python3
"""
Pre-Open Gap Fade Bot — Paper Trading (Analyzer Mode)
======================================================
Champion strategy from NSE pre-open IEP gap study (Sharpe 6.12, 694 trades, Jan 2023–Mar 2026).

Logic:
  - Gap UP   ≥ 2%  → SHORT  (gap fade, expect reversion)
  - Gap DOWN ≥ 2%  → LONG   (gap fade, expect reversion)
  - Entry:   09:15:05 AM (market open, MARKET MIS)
  - SL:      0.5% against entry (SL-M order placed immediately after entry)
  - Exit:    10:00:00 AM (force-close all open positions)

Universe: Nifty 50 EQ stocks (49 stocks; TATAMOTORS excluded — demerged)
Capital:   ₹1,00,000 per trade

Paper trading uses OpenAlgo Analyzer (Sandbox) mode — no real orders sent.
P&L is logged to `paper_trades_stocks` table in live_trading/logs/preopen_gap_fade_trades.db (SQLite).

Rate limit handling:
  - OpenAlgo enforces ORDER_RATE_LIMIT = 10/second on /placeorder
  - on_entry places pairs of (entry + SL-M) orders with ORDER_THROTTLE_DELAY
    between each pair, keeping burst well under the 10/s ceiling.
  - on_monitor uses a SINGLE multiquotes call for all open positions
    (1 API hit regardless of position count, vs N individual quotes calls).
  - on_scan uses a SINGLE multiquotes call for all 49 symbols (1 API hit).

Timing (IST):
  08:55    Startup: load prev_close from DB, enable Analyzer mode
  09:14:50 Scan:    fetch LTPs via multiquotes, compute gap_pct, rank signals
  09:15:05 Entry:   place MARKET + SL-M orders (throttled at 0.15s/pair)
  09:16–09:59 Monitor: refresh all LTPs in ONE multiquotes call every 60s
  10:00:00 Exit:   close all open positions with MARKET orders
  10:05:00 Log:    write trade records to paper_trades_stocks table
"""

import os
import sys
import json
import time
import logging
import requests
# import duckdb
from pathlib import Path
from datetime import datetime, time as dt_time, date
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

# ── Path setup ────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from openalgo import api as OpenAlgoAPI
from live_trading.api_utils                 import is_market_holiday
from live_trading.shared.trade_logger       import log_trade_to_db
from live_trading.shared.telegram_notifier import wait_for_confirm

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "preopen_gap_fade_bot.log"),
        logging.StreamHandler(),
    ],
)
# Silence APScheduler's chatty per-job INFO messages ("Running job…"/"executed successfully")
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
STRATEGY_NAME   = "PREOPEN_GAP_FADE"
API_KEY         = os.getenv("OPENALGO_API_KEY")
HOST            = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
TG_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID")
IST             = pytz.timezone("Asia/Kolkata")

# Champion strategy parameters
GAP_THRESHOLD   = 2.0    # % minimum absolute gap to signal
SL_PCT          = 0.5    # % stop-loss distance from entry
EXIT_HOUR       = 10
EXIT_MINUTE     = 0

# Position sizing
CAPITAL_PER_TRADE = 100_000   # ₹1,00,000 per stock
MAX_POSITIONS     = 10

# Transaction cost estimate (for P&L logging)
COST_PCT          = 0.0007    # ~0.07% round-trip (STT + exchange + brokerage)

# Rate limit safety — OpenAlgo ORDER_RATE_LIMIT default is 10/second.
# We place 2 orders per stock (entry MARKET + SL-M). Sleeping 0.3s between each
# pair gives a rate of 6.6 orders/s, well under the 10/s ceiling.
ORDER_THROTTLE_DELAY = 0.3    # seconds between each (entry + SL-M) order pair

# SQLite trade log — written by this bot only, no lock contention
TRADE_DB_PATH = LOG_DIR / "preopen_gap_fade_trades.db"

# State file (read by status.py dashboard)
STATE_FILE = LOG_DIR / "preopen_gap_fade_state.json"

# Exchange for EQ stocks
EXCHANGE = "NSE"
PRODUCT  = "MIS"

# Trading universe: 49 Nifty 50 stocks (TATAMOTORS excluded — demerged)
# Matches the research champion exactly — all remaining stocks included.
NIFTY50_UNIVERSE = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL",
    "BPCL", "BRITANNIA", "CIPLA", "COALINDIA", "DRREDDY",
    "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK",
    "INFY", "ITC", "JSWSTEEL", "KOTAKBANK", "LT",
    "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SHRIRAMFIN",
    "SUNPHARMA", "TATACONSUM", "TATASTEEL", "TCS", "TECHM",
    "TITAN", "ULTRACEMCO", "WIPRO", "ZOMATO",
]

# No symbols excluded — all 49 stocks match the research champion universe.
EXCLUDED_SYMBOLS: set = set()


# ── Database helpers ──────────────────────────────────────────────────────────

def ensure_paper_trades_table():
    """Create paper_trades_stocks table in SQLite if it doesn't exist."""
    import sqlite3
    con = sqlite3.connect(str(TRADE_DB_PATH), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    con.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades_stocks (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date     TEXT    NOT NULL,
            symbol         TEXT    NOT NULL,
            direction      TEXT    NOT NULL,
            gap_pct        REAL,
            prev_close     REAL,
            entry_price    REAL,
            exit_price     REAL,
            sl_price       REAL,
            sl_hit         INTEGER DEFAULT 0,
            quantity       INTEGER,
            gross_pct      REAL,
            cost_pct       REAL,
            net_pct        REAL,
            gross_pnl      REAL,
            net_pnl        REAL,
            entry_order_id TEXT,
            exit_order_id  TEXT,
            notes          TEXT,
            created_at     TEXT DEFAULT (datetime('now')),
            UNIQUE (trade_date, symbol)
        )
    """)
    con.commit()
    con.close()
    logger.info("✅ paper_trades_stocks table ready (SQLite)")


def load_prev_close(client, symbols: list) -> dict:
    """
    Load most recent prev_close for universe symbols via multiquotes API.
    Replaces DuckDB access to avoid lock contention and follow live-bot mandates.
    """
    payload = [{"symbol": s, "exchange": EXCHANGE} for s in symbols]
    try:
        resp = client.multiquotes(symbols=payload)
        if not isinstance(resp, dict) or resp.get("status") != "success":
            logger.error(f"load_prev_close multiquotes error: {resp}")
            return {}
        
        results = resp.get("results", [])
        out = {}
        for item in results:
            if not isinstance(item, dict):
                continue
            sym  = item.get("symbol")
            data = item.get("data", {})
            if sym and isinstance(data, dict):
                pc = data.get("prev_close")
                if pc and float(pc) > 0:
                    out[sym] = float(pc)
        
        logger.info(f"Loaded prev_close for {len(out)}/{len(symbols)} symbols via API")
        return out
    except Exception as e:
        logger.error(f"load_prev_close exception: {e}")
        return {}


def log_trades_to_db(trades: list):
    """Insert completed trade records into paper_trades_stocks (SQLite, WAL).

    UNIQUE(trade_date, symbol) + INSERT OR IGNORE ensures a bot restart after
    10 AM cannot double-log the same day's trades.
    """
    if not trades:
        return
    import sqlite3
    con = sqlite3.connect(str(TRADE_DB_PATH), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")

    count_before = con.execute("SELECT COUNT(*) FROM paper_trades_stocks").fetchone()[0]

    for t in trades:
        con.execute("""
            INSERT OR IGNORE INTO paper_trades_stocks
                (trade_date, symbol, direction, gap_pct, prev_close,
                 entry_price, exit_price, sl_price, sl_hit, quantity,
                 gross_pct, cost_pct, net_pct, gross_pnl, net_pnl,
                 entry_order_id, exit_order_id, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            str(t.get("trade_date")), t.get("symbol"), t.get("direction"),
            t.get("gap_pct"), t.get("prev_close"),
            t.get("entry_price"), t.get("exit_price"), t.get("sl_price"),
            int(t.get("sl_hit", False)), t.get("quantity"),
            t.get("gross_pct"), t.get("cost_pct"), t.get("net_pct"),
            t.get("gross_pnl"), t.get("net_pnl"),
            t.get("entry_order_id"), t.get("exit_order_id"), t.get("notes"),
        ])

    con.commit()
    count_after = con.execute("SELECT COUNT(*) FROM paper_trades_stocks").fetchone()[0]
    con.close()

    inserted = count_after - count_before
    skipped  = len(trades) - inserted
    if skipped:
        logger.warning(
            f"📝 log_trades_to_db: {inserted} inserted, {skipped} skipped "
            f"(duplicate guard — already logged for this date)"
        )
    else:
        logger.info(f"📝 Logged {inserted} trades to paper_trades_stocks (SQLite)")


# ── OpenAlgo helpers ──────────────────────────────────────────────────────────

def send_telegram(msg: str):
    """Send Telegram notification."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
                      timeout=5)
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")


def get_multiquotes(client, symbols: list) -> dict:
    """
    Fetch LTPs for a batch of symbols using ONE multiquotes API call.
    Returns dict: {symbol: ltp}.

    Rate impact: 1 API call regardless of how many symbols are in the batch.
    OpenAlgo API_RATE_LIMIT = 10/second — a single call is trivially safe.
    """
    payload = [{"symbol": s, "exchange": EXCHANGE} for s in symbols]
    try:
        resp = client.multiquotes(symbols=payload)
        if not isinstance(resp, dict):
            logger.error(f"multiquotes unexpected response type: {type(resp)}")
            return {}
        if resp.get("status") != "success":
            logger.error(f"multiquotes error: {resp}")
            return {}
        # Response: {"status":"success","results":[{"symbol":...,"exchange":...,"data":{ltp,...}},…]}
        results = resp.get("results", [])
        result = {}
        for item in results:
            if not isinstance(item, dict):
                continue
            sym        = item.get("symbol")
            quote_data = item.get("data", {})
            if sym and isinstance(quote_data, dict):
                ltp = quote_data.get("ltp") or quote_data.get("last_price")
                if ltp and float(ltp) > 0:
                    result[sym] = float(ltp)
        return result
    except Exception as e:
        logger.error(f"multiquotes exception: {e}")
        return {}


def place_order(client, symbol: str, action: str, quantity: int,
                price_type: str = "MARKET", trigger_price: float = None) -> str:
    """
    Place a single order via OpenAlgo. Returns order_id or "" on failure.

    Rate impact: 1 API call against ORDER_RATE_LIMIT (10/second default).
    Callers must space calls with ORDER_THROTTLE_DELAY between pairs.
    """
    kwargs = {}
    if trigger_price:
        kwargs["trigger_price"] = str(round(trigger_price, 2))

    try:
        resp = client.placeorder(
            strategy=STRATEGY_NAME,
            symbol=symbol,
            action=action,
            exchange=EXCHANGE,
            price_type=price_type,
            product=PRODUCT,
            quantity=str(quantity),
            **kwargs,
        )
        if isinstance(resp, dict) and resp.get("status") == "success":
            order_id = str(resp.get("orderid", resp.get("data", {}).get("orderid", "")))
            logger.info(f"  ✅ {action} {symbol} qty={quantity} [{price_type}] → {order_id}")
            return order_id
        else:
            logger.error(f"  ❌ Order failed {symbol} {action}: {resp}")
            return ""
    except Exception as e:
        logger.error(f"  ❌ Order exception {symbol} {action}: {e}")
        return ""


# ── Bot State ─────────────────────────────────────────────────────────────────

class PreOpenGapFadeBot:
    def __init__(self):
        if not API_KEY:
            logger.error("❌ OPENALGO_API_KEY not set. Exiting.")
            sys.exit(1)

        self.client = OpenAlgoAPI(api_key=API_KEY, host=HOST)
        self.trade_date: date = None
        self.prev_close: dict = {}           # {symbol: prev_close}
        self.signals: list = []              # [{symbol, direction, gap_pct, prev_close}]
        self.positions: dict = {}            # {symbol: position_dict}
        self.completed_trades: list = []
        self.analyzer_enabled: bool = False

    # ── State file (read by status.py dashboard) ─────────────────────────────

    def _save_state(self):
        """Write current positions to JSON state file for the dashboard."""
        try:
            # Serialise positions: convert date to str, ensure JSON-safe types
            pos_out = {}
            for sym, p in self.positions.items():
                pos_out[sym] = {
                    "direction":      p.get("direction", ""),
                    "entry_price":    p.get("entry_price", 0),
                    "sl_price":       p.get("sl_price", 0),
                    "quantity":       p.get("quantity", 0),
                    "gap_pct":        p.get("gap_pct", 0),
                    "prev_close":     p.get("prev_close", 0),
                    "current_price":  p.get("current_price", 0),
                    "sl_hit":         p.get("sl_hit", False),
                    "exit_price":     p.get("exit_price"),      # None until closed
                    "net_pnl":        p.get("net_pnl"),         # None until closed
                    "entry_order_id": p.get("entry_order_id", ""),
                }

            state = {
                "strategy":    STRATEGY_NAME,
                "trade_date":  str(self.trade_date) if self.trade_date else "",
                "positions":   pos_out,
                "n_signals":   len(self.signals),
                "last_update": datetime.now(IST).isoformat(),
            }
            STATE_FILE.write_text(json.dumps(state, indent=2))
        except Exception as e:
            logger.warning(f"_save_state failed: {e}")

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _is_trading_day(self) -> bool:
        now = datetime.now(IST)
        if now.weekday() >= 5:
            logger.info("📅 Weekend — skipping")
            return False
        
        # Check holiday via API
        holiday = is_market_holiday(API_KEY, now)
        if holiday:
            logger.info(f"📅 Market holiday ({now.date()}) — skipping")
            return False
        
        logger.info(f"✅ Verified: Today ({now.date()}) is a trading day.")
        return True

    def _enable_analyzer_mode(self):
        """Switch OpenAlgo to Analyzer (paper) mode."""
        try:
            resp = self.client.analyzertoggle(mode=True)
            if isinstance(resp, dict) and resp.get("status") == "success":
                mode = resp.get("data", {}).get("mode", "unknown")
                logger.info(f"🧪 Analyzer mode: {mode}")
                self.analyzer_enabled = True
            else:
                logger.warning(f"analyzertoggle response: {resp}")
        except Exception as e:
            logger.warning(f"analyzertoggle failed: {e}")

    # ── Job 1: Market prep at 08:55 ──────────────────────────────────────────

    def on_market_prep(self):
        """08:55 AM — load prev_close, enable analyzer, reset state."""
        logger.info("=" * 70)
        logger.info(f"PRE-OPEN GAP FADE BOT — {datetime.now(IST).strftime('%Y-%m-%d')}")
        logger.info("=" * 70)

        if not self._is_trading_day():
            return

        self.trade_date = datetime.now(IST).date()
        self.signals = []
        self.positions = {}
        self.completed_trades = []

        self.prev_close = load_prev_close(self.client, NIFTY50_UNIVERSE)
        ensure_paper_trades_table()
        self._enable_analyzer_mode()
        self._save_state()

        logger.info(f"✅ Ready. prev_close loaded for {len(self.prev_close)} symbols")
        send_telegram(f"🌅 *Pre-Open Gap Fade Bot* armed for {self.trade_date}\n"
                      f"Universe: {len(NIFTY50_UNIVERSE)} stocks | Threshold: ±{GAP_THRESHOLD}%")

    # ── Job 2: Scan at 09:14:50 ──────────────────────────────────────────────

    def on_scan(self):
        """09:14:50 AM — fetch LTPs (1 multiquotes call), compute gaps."""
        if not self.trade_date:
            logger.warning("⚠️  on_scan: no trade_date — skipping")
            return

        logger.info("🔍 Scanning for gap signals (1 multiquotes call for all 46 symbols)...")

        # ONE API call for the entire universe — no rate limit pressure
        ltps = get_multiquotes(self.client, NIFTY50_UNIVERSE)
        if not ltps:
            logger.error("❌ No LTPs received — cannot scan")
            send_telegram("❌ *Gap Fade Bot* — multiquotes failed at scan time!")
            return

        logger.info(f"   LTPs received: {len(ltps)}/{len(NIFTY50_UNIVERSE)} symbols")

        candidates = []
        for sym in NIFTY50_UNIVERSE:
            if sym in EXCLUDED_SYMBOLS:
                continue
            ltp = ltps.get(sym)
            pc  = self.prev_close.get(sym)
            if not ltp or not pc or pc <= 0:
                logger.warning(f"   Skipping {sym}: ltp={ltp}, prev_close={pc}")
                continue

            gap = (ltp - pc) / pc * 100
            if abs(gap) >= GAP_THRESHOLD:
                direction = "SHORT" if gap > 0 else "LONG"
                candidates.append({
                    "symbol":    sym,
                    "direction": direction,
                    "gap_pct":   round(gap, 3),
                    "prev_close": pc,
                    "ltp_scan":  ltp,
                })

        candidates.sort(key=lambda x: abs(x["gap_pct"]), reverse=True)
        self.signals = candidates[:MAX_POSITIONS]

        if self.signals:
            lines = [f"  {s['direction']:5s} {s['symbol']:12s} gap={s['gap_pct']:+.2f}%  LTP={s['ltp_scan']:.2f}"
                     for s in self.signals]
            logger.info(f"📊 {len(self.signals)} signal(s):\n" + "\n".join(lines))
            tg_lines = "\n".join(
                f"{'🔴' if s['direction']=='SHORT' else '🟢'} {s['symbol']} {s['gap_pct']:+.2f}%"
                for s in self.signals
            )
            send_telegram(f"📊 *Gap Fade Signals* ({self.trade_date})\n{tg_lines}\n\nEntering at 09:15...")
        else:
            logger.info(f"   No gap signals today (all gaps < {GAP_THRESHOLD}%)")
            send_telegram(f"💤 *Gap Fade Bot* — No signals today ({self.trade_date})")

        self._save_state()

    # ── Job 3: Entry at 09:15:05 ─────────────────────────────────────────────

    def on_entry(self):
        """
        09:15:05 AM — place entry + SL-M orders.

        Rate-limit strategy:
          - We need 2 orders per stock (entry MARKET + SL-M).
          - Max 10 stocks → 20 orders total.
          - ORDER_RATE_LIMIT = 10/second. Sleeping ORDER_THROTTLE_DELAY (0.15s)
            between each (entry + SL-M) pair gives ~6.7 pairs/s = well within
            the 10-call/second ceiling even if other bots are also firing.
          - The 1 fresh LTP fetch at entry also uses get_multiquotes (1 call).
        """
        if not self.signals:
            logger.info("No signals — skipping entry")
            return

        logger.info(f"🚀 Placing entry orders for {len(self.signals)} signal(s)...")
        logger.info(f"   Order throttle: {ORDER_THROTTLE_DELAY}s between pairs "
                    f"(max ~{1/ORDER_THROTTLE_DELAY:.0f} pairs/s vs 10-order/s limit)")

        # ONE multiquotes call for fresh LTPs at entry — not N individual calls
        entry_syms = [s["symbol"] for s in self.signals]
        fresh_ltps = get_multiquotes(self.client, entry_syms)

        for sig in self.signals:
            sym       = sig["symbol"]
            direction = sig["direction"]
            action    = "SELL" if direction == "SHORT" else "BUY"

            ltp = fresh_ltps.get(sym) or sig["ltp_scan"]
            if ltp <= 0:
                logger.warning(f"  ⚠️ {sym}: invalid LTP — skipping")
                continue

            quantity = max(1, int(CAPITAL_PER_TRADE / ltp))

            if direction == "SHORT":
                sl_price  = round(ltp * (1 + SL_PCT / 100), 2)
                sl_action = "BUY"
            else:
                sl_price  = round(ltp * (1 - SL_PCT / 100), 2)
                sl_action = "SELL"

            logger.info(f"  {action} {sym} qty={quantity}  entry≈{ltp:.2f}  SL={sl_price:.2f}")

            # Order 1: entry MARKET
            entry_oid = place_order(self.client, sym, action, quantity, "MARKET")

            # Order 2: SL-M (placed immediately after entry)
            sl_oid = place_order(self.client, sym, sl_action, quantity, "SL-M",
                                 trigger_price=sl_price)

            self.positions[sym] = {
                "direction":      direction,
                "entry_price":    ltp,
                "sl_price":       sl_price,
                "quantity":       quantity,
                "gap_pct":        sig["gap_pct"],
                "prev_close":     sig["prev_close"],
                "entry_order_id": entry_oid,
                "sl_order_id":    sl_oid,
                "sl_hit":         False,
                "current_price":  ltp,
                "exit_price":     None,
                "net_pnl":        None,
            }

            # Throttle: sleep between each stock's order pair so we stay well
            # under the 10-orders/second rate limit ceiling
            time.sleep(ORDER_THROTTLE_DELAY)

        logger.info(f"✅ Entry complete — {len(self.positions)} position(s) active")
        self._save_state()

    # ── Job 4: Monitor every 60s (09:16–09:59) ───────────────────────────────

    def on_monitor(self):
        """
        Refresh LTPs for all open positions using ONE multiquotes call.

        Rate impact: 1 API call per minute regardless of position count.
        (Previously: N individual quotes calls — N calls/minute.)
        """
        # Exclude SL-hit AND already-exited positions (exit_price set by on_exit)
        # Without this, post-exit monitor would re-check closed positions and
        # could log spurious SL warnings on positions already squared off.
        active = {s: p for s, p in self.positions.items()
                  if not p["sl_hit"] and p.get("exit_price") is None}
        if not active:
            return

        now_str = datetime.now(IST).strftime("%H:%M:%S")

        # ONE multiquotes call for all open positions — not len(active) calls
        ltps = get_multiquotes(self.client, list(active.keys()))

        for sym, pos in active.items():
            ltp = ltps.get(sym)
            if not ltp or ltp <= 0:
                continue

            pos["current_price"] = ltp

            # Check SL hit
            sl_hit = (
                (pos["direction"] == "SHORT" and ltp >= pos["sl_price"]) or
                (pos["direction"] == "LONG"  and ltp <= pos["sl_price"])
            )
            if sl_hit:
                pos["sl_hit"]    = True
                pos["exit_price"] = pos["sl_price"]
                logger.warning(f"  🛑 SL HIT: {sym} | {pos['direction']} | "
                               f"LTP={ltp:.2f} ≥/≤ SL={pos['sl_price']:.2f}")
                send_telegram(f"🛑 *SL Hit* {sym} | {pos['direction']} | "
                              f"LTP={ltp:.2f}  SL={pos['sl_price']:.2f}")

        open_count = sum(1 for p in self.positions.values() if not p["sl_hit"])
        sl_count   = sum(1 for p in self.positions.values() if p["sl_hit"])
        logger.info(f"[{now_str}] Monitor: {open_count} open, {sl_count} SL hit "
                    f"(1 multiquotes call for {len(active)} symbols)")
        self._save_state()

    # ── Job 5: Exit at 10:00:00 ──────────────────────────────────────────────

    def _get_net_qty_from_broker(self, sym: str) -> int | None:
        """
        Fetch the net open quantity for a symbol from the broker positionbook.
        Returns 0 if the position is already flat (SL-M filled, or never opened).
        Returns None if the API call fails (caller should not skip the exit in that case).

        Field-name note: live broker responses use "netqty" / "net_qty"; the
        Analyzer (sandbox) positionbook uses "quantity".  All three are checked
        so this method works correctly in both paper and live modes.
        """
        try:
            # Note: strategy parameter is unsupported on positionbook endpoint in current server version
            resp = self.client.positionbook()
            if not isinstance(resp, dict) or resp.get("status") != "success":
                return None
            positions = resp.get("data", [])
            if not isinstance(positions, list):
                return None
            for p in positions:
                # Filter by symbol + exchange only.
                # Previously also filtered by strategy == STRATEGY_NAME, but sandbox/analyzer
                # mode positionbooks don't carry a strategy field on each position row, so every
                # lookup returned 0 (not found) and the bot incorrectly treated live positions
                # as already flat — causing it to skip the exit order at 10:00.
                if (p.get("symbol") == sym and p.get("exchange") == EXCHANGE):
                    # "netqty"/"net_qty" = live broker field names (Fyers, Zerodha, etc.)
                    # "quantity"         = Analyzer / sandbox positionbook field name
                    raw = p.get("netqty", p.get("net_qty", p.get("quantity", 0)))
                    return int(raw)
            return 0   # symbol not in positionbook → already flat
        except Exception as e:
            logger.warning(f"  ⚠️ positionbook check failed for {sym}: {e}")
            return None

    def on_exit(self):
        """10:00 AM — close all open positions using internal state (no broker API call)."""
        logger.info("🔔 10:00 AM — closing all positions for this strategy...")

        try:
            # Use self.positions as the authoritative source — we opened them, we know them.
            open_positions = {
                s: p for s, p in self.positions.items()
                if not p.get("sl_hit") and p.get("exit_price") is None
            }

            if not open_positions:
                logger.info("All positions already closed (SL hit or prior exit). Nothing to do.")
                return

            logger.info(f"  {len(open_positions)} open position(s) to close: {list(open_positions.keys())}")

            # Fetch fresh LTPs in ONE multiquotes call for accurate exit price recording
            exit_ltps = get_multiquotes(self.client, list(open_positions.keys()))

            for sym, pos in open_positions.items():
                exit_action = "BUY" if pos["direction"] == "SHORT" else "SELL"
                ltp = exit_ltps.get(sym) or pos.get("current_price") or pos.get("entry_price")

                # Cancel the pending SL-M order FIRST
                sl_oid = pos.get("sl_order_id")
                sl_already_filled = False
                if sl_oid:
                    try:
                        resp = self.client.cancelorder(order_id=sl_oid, strategy="PREOPEN_GAP_FADE")
                        if isinstance(resp, dict) and resp.get("status") == "error":
                            msg = resp.get("message", "").lower()
                            if "complete" in msg or "traded" in msg or "filled" in msg:
                                sl_already_filled = True
                            logger.warning(f"  ⚠️ Could not cancel SL-M {sl_oid} for {sym}: {resp.get('message')}")
                        else:
                            logger.info(f"  ✅ Cancelled SL-M order {sl_oid} for {sym}")
                    except Exception as ce:
                        # Sometimes OpenAlgo/SDK might throw an HTTPError
                        err_str = str(ce).lower()
                        if "complete" in err_str or "traded" in err_str or "filled" in err_str:
                            sl_already_filled = True
                        logger.warning(f"  ⚠️ Exception cancelling SL-M {sl_oid} for {sym}: {ce}")

                # If the SL-M cancellation told us it's already filled, we don't need to exit!
                if sl_already_filled:
                    logger.info(f"  ⏭️ {sym}: SL-M already completed (detected via cancel error). Marking sl_hit, skipping exit order.")
                    pos["sl_hit"]    = True
                    pos["exit_price"] = ltp   # best available price for P&L logging
                    continue

                # Fallback: Verify net qty from broker
                net_qty = self._get_net_qty_from_broker(sym)
                if net_qty == 0:
                    logger.info(f"  ⏭️ {sym}: net_qty=0 in broker — SL-M already filled. Marking sl_hit, skipping exit order.")
                    pos["sl_hit"]    = True
                    pos["exit_price"] = ltp
                    continue
                elif net_qty is None:
                    logger.warning(f"  ⚠️ {sym}: positionbook check failed — placing exit order anyway (safer)")

                logger.info(f"  Closing {sym}: {pos['direction']} qty={pos['quantity']} @ ~{ltp:.2f}...")
                exit_oid = place_order(self.client, sym, exit_action, int(pos["quantity"]), "MARKET")

                pos["exit_price"]    = ltp
                pos["exit_order_id"] = exit_oid

                time.sleep(ORDER_THROTTLE_DELAY)

            logger.info(f"✅ Exit orders placed for {len(open_positions)} position(s)")
            self._save_state()
        except Exception as e:
            logger.error(f"❌ Error in on_exit: {e}", exc_info=True)

    # ── Job 6: Log P&L at 10:05 ──────────────────────────────────────────────

    def on_log_pnl(self):
        """10:05 AM — compute P&L, log to DB, send summary.

        Idempotency guard: if today's trades are already in the DB (e.g. from a
        normal 10:05 run before a bot restart triggered the recovery path), skip
        the DB write entirely.  The UNIQUE(trade_date, symbol) constraint in
        log_trades_to_db provides a second safety net.
        """
        if not self.positions:
            logger.info("No trades today — nothing to log")
            return

        # Guard: check if we've already logged today
        try:
            import sqlite3
            con = sqlite3.connect(str(TRADE_DB_PATH), timeout=30)
            already = con.execute(
                "SELECT COUNT(*) FROM paper_trades_stocks WHERE trade_date = ?",
                [str(self.trade_date)]
            ).fetchone()[0]
            con.close()
            if already > 0:
                logger.warning(
                    f"⚠️  on_log_pnl: {already} trade(s) for {self.trade_date} already in DB "
                    f"— skipping re-insert (bot restart detected). "
                    f"DB constraint will block any accidental duplicates."
                )
                return
        except Exception as e:
            logger.warning(f"on_log_pnl guard query failed ({e}) — proceeding with insert")

        logger.info("📝 Computing P&L and logging to database...")
        total_net = 0.0
        records   = []

        for sym, pos in self.positions.items():
            entry = pos.get("entry_price", 0)
            exit_ = pos.get("exit_price") or pos.get("current_price", entry)
            qty   = pos.get("quantity", 0)

            if entry <= 0:
                continue

            gross_pct = ((entry - exit_) / entry) if pos["direction"] == "SHORT" \
                        else ((exit_ - entry) / entry)
            net_pct   = gross_pct - COST_PCT
            gross_pnl = gross_pct * entry * qty
            net_pnl   = net_pct   * entry * qty
            total_net += net_pnl

            # Store final P&L back into position dict (dashboard reads it)
            pos["net_pnl"] = round(net_pnl, 2)

            rec = {
                "trade_date":     self.trade_date,
                "symbol":         sym,
                "direction":      pos["direction"],
                "gap_pct":        pos["gap_pct"],
                "prev_close":     pos["prev_close"],
                "entry_price":    round(entry, 2),
                "exit_price":     round(exit_, 2),
                "sl_price":       pos["sl_price"],
                "sl_hit":         pos["sl_hit"],
                "quantity":       qty,
                "gross_pct":      round(gross_pct * 100, 4),
                "cost_pct":       round(COST_PCT   * 100, 4),
                "net_pct":        round(net_pct    * 100, 4),
                "gross_pnl":      round(gross_pnl, 2),
                "net_pnl":        round(net_pnl,   2),
                "entry_order_id": pos.get("entry_order_id", ""),
                "exit_order_id":  pos.get("exit_order_id", ""),
                "notes":          "SL_HIT" if pos["sl_hit"] else "",
            }
            records.append(rec)

            icon = "✅" if net_pnl > 0 else "❌"
            logger.info(f"  {icon} {sym:12s} {pos['direction']:5s} | gap={pos['gap_pct']:+.2f}% | "
                        f"entry={entry:.2f} exit={exit_:.2f} | "
                        f"net={net_pct*100:+.3f}%  ₹{net_pnl:+.0f}")

        log_trades_to_db(records)

        # Also mirror each trade into the shared paper_trades_options journal
        # so all bots appear in the same centralized table.
        for rec in records:
            log_trade_to_db(
                bot_name      = "preopen_gap_fade_bot",
                instrument    = rec["symbol"],
                option_symbol = rec["symbol"],
                option_type   = None,
                entry_time    = datetime.combine(rec["trade_date"], dt_time(9, 15)),
                exit_time     = datetime.combine(rec["trade_date"], dt_time(10, 0)),
                entry_premium = rec["entry_price"],
                exit_premium  = rec["exit_price"],
                exit_reason   = "SL_HIT" if rec["sl_hit"] else "EOD_10:00",
                quantity      = rec["quantity"],
                lots          = None,
                lot_size      = None,
                gross_pnl     = rec["gross_pnl"],
                order_id      = rec.get("entry_order_id"),
                notes         = f"direction={rec['direction']} gap={rec['gap_pct']:+.2f}%",
                strategy_type = "equity",
                direction     = rec["direction"].lower(),   # "long" (gap-down) or "short" (gap-up)
            )

        self._save_state()

        n_win  = sum(1 for r in records if r["net_pnl"] > 0)
        n_loss = len(records) - n_win
        logger.info(f"\n{'='*60}")
        logger.info(f"  DATE: {self.trade_date}  |  TRADES: {len(records)}")
        logger.info(f"  WINS: {n_win}  LOSSES: {n_loss}  WR: {n_win/max(len(records),1)*100:.1f}%")
        logger.info(f"  TOTAL NET P&L: ₹{total_net:+,.0f}")
        logger.info(f"{'='*60}")

        send_telegram(
            f"📊 *Gap Fade Summary* ({self.trade_date})\n"
            f"Trades: {len(records)} | Wins: {n_win} | WR: {n_win/max(len(records),1)*100:.0f}%\n"
            f"Net P&L: ₹{total_net:+,.0f}"
        )


# ── Scheduler setup ───────────────────────────────────────────────────────────

def main():
    bot = PreOpenGapFadeBot()

    # Write initial state file immediately so status.py shows the bot as online
    # even before the first scheduled job fires — BUT only if there is no valid
    # state file for today already (we don't want to wipe today's positions on restart).
    today_str = datetime.now(IST).date().isoformat()
    existing_positions = {}
    if STATE_FILE.exists():
        try:
            saved = json.loads(STATE_FILE.read_text())
            if saved.get("trade_date") == today_str and saved.get("positions"):
                existing_positions = saved["positions"]
        except Exception:
            pass

    if not existing_positions:
        try:
            STATE_FILE.write_text(json.dumps({
                "strategy":    STRATEGY_NAME,
                "trade_date":  "",
                "positions":   {},
                "n_signals":   0,
                "last_update": datetime.now(IST).isoformat(),
                "status":      "starting",
            }, indent=2))
        except Exception as e:
            logger.warning(f"Could not write initial state: {e}")

    # Startup recovery: if the launcher started this bot after the scheduled job
    # times (e.g. market open at 09:15 triggers the launcher, but our prep/scan/
    # entry jobs are at 08:55 / 09:14:50 / 09:15:05), fire any missed jobs now.
    # IDEMPOTENCY GUARD: if we already have positions for today in the state file,
    # restore them and skip scan/entry entirely — never double-enter the same day.
    now      = datetime.now(IST)
    now_time = now.time()
    logger.info(f"📊 Startup check: time={now_time.strftime('%H:%M:%S')}, window=[08:55, 10:30]")
    
    if dt_time(8, 55) <= now_time < dt_time(10, 30):
        if not bot._is_trading_day():
            logger.info("📅 Recovery check: Holiday or Weekend today — skipping bot logic.")
        else:
            bot.trade_date = now.date()
            logger.info(f"⚡ Entering Recovery block for {bot.trade_date}")
            if existing_positions:
                logger.info(f"⚡ Late start — restoring {len(existing_positions)} positions from state file: {list(existing_positions.keys())}")
                bot.positions  = {
                    sym: {
                        "direction":      p.get("direction", ""),
                        "entry_price":    float(p.get("entry_price") or 0),
                        "sl_price":       float(p.get("sl_price")    or 0),
                        "quantity":       int(p.get("quantity")      or 0),
                        "gap_pct":        float(p.get("gap_pct")     or 0),
                        "prev_close":     float(p.get("prev_close")  or 0),
                        "current_price":  float(p.get("current_price") or p.get("entry_price") or 0),
                        "sl_hit":         bool(p.get("sl_hit", False)),
                        "exit_price":     p.get("exit_price"),
                        "net_pnl":        p.get("net_pnl"),
                        "entry_order_id": p.get("entry_order_id", ""),
                    }
                    for sym, p in existing_positions.items()
                }
                bot._enable_analyzer_mode()
            
            # PROACTIVE EXIT: If it's already past 10 AM, always check API for orphaned positions
            if now_time >= dt_time(10, 0, 0):
                logger.warning("🚨 Recovery: It is past 10 AM. Checking API for orphaned positions...")
                bot._enable_analyzer_mode()
                bot.on_exit()
                bot.on_log_pnl()
            else:
                if not existing_positions:
                    logger.info(f"⚡ Late start at {now.strftime('%H:%M:%S')} — running startup recovery")
                    bot.on_market_prep()
                    if now_time >= dt_time(9, 14, 50):
                        bot.on_scan()
                    if now_time >= dt_time(9, 15, 5):
                        # ── Late-entry confirmation via Telegram ──────────────
                        if bot.signals:
                            signal_lines = "\n".join(
                                f"  {'SHORT' if s['direction']=='SHORT' else 'LONG ':5s} "
                                f"{s['symbol']:12s}  gap={s['gap_pct']:+.2f}%  LTP={s['ltp_scan']:.2f}"
                                for s in bot.signals
                            )
                            expire_time = (now + __import__('datetime').timedelta(minutes=10)).strftime('%H:%M')
                            confirm_msg = (
                                f"⚠️ *Pre-Open Gap Fade — LATE START* ({now.strftime('%H:%M:%S')})\n"
                                f"Found {len(bot.signals)} signal(s):\n`{signal_lines}`\n\n"
                                f"Reply `/confirm_preopen` to enter now.\n"
                                f"Offer expires at {expire_time} IST. No reply → session skipped."
                            )
                            send_telegram(confirm_msg)
                            logger.info("⏳ Waiting for Telegram /confirm_preopen (10 min window) …")
                            if wait_for_confirm("/confirm_preopen", timeout_secs=600):
                                logger.info("✅ Confirmed — entering trades now (late).")
                                bot.on_entry()
                            else:
                                logger.info("⏰ No confirm received — skipping entry for today.")
                                send_telegram("⏰ *Pre-Open Gap Fade* — confirm window expired. No trades entered today.")
                        else:
                            logger.info("   No gap signals found at late-start scan — nothing to enter.")
                else:
                    bot._save_state()
                    logger.info(f"  Positions restored: {list(bot.positions.keys())}")

    scheduler = BlockingScheduler(timezone=IST)

    scheduler.add_job(bot.on_market_prep, CronTrigger(hour=8,  minute=55, second=0,  timezone=IST), id="market_prep",      replace_existing=True)
    scheduler.add_job(bot.on_scan,        CronTrigger(hour=9,  minute=14, second=50, timezone=IST), id="scan",             replace_existing=True)
    scheduler.add_job(bot.on_entry,       CronTrigger(hour=9,  minute=15, second=5,  timezone=IST), id="entry",            replace_existing=True)
    scheduler.add_job(bot.on_monitor, "cron", hour="9",  minute="16-59", second="0",  timezone=IST, id="monitor",          replace_existing=True)
    # Also refresh LTPs at 10:00:30 – 10:04:30 so status.py shows live prices after exit
    scheduler.add_job(bot.on_monitor, "cron", hour="10", minute="0-4",   second="30", timezone=IST, id="monitor_post_exit", replace_existing=True)
    scheduler.add_job(bot.on_exit,        CronTrigger(hour=10, minute=0,  second=0,  timezone=IST), id="exit",             replace_existing=True)
    scheduler.add_job(bot.on_log_pnl,     CronTrigger(hour=10, minute=5,  second=0,  timezone=IST), id="log_pnl",          replace_existing=True)
    scheduler.add_job(bot._save_state,    "interval", minutes=1, id="heartbeat",                                           replace_existing=True)

    logger.info("=" * 70)
    logger.info("PRE-OPEN GAP FADE BOT — SCHEDULER STARTED")
    logger.info(f"  Strategy      : {STRATEGY_NAME}")
    logger.info(f"  Gap threshold : ±{GAP_THRESHOLD}%")
    logger.info(f"  Stop-loss     : {SL_PCT}%  |  Exit: {EXIT_HOUR:02d}:{EXIT_MINUTE:02d}")
    logger.info(f"  Capital/trade : ₹{CAPITAL_PER_TRADE:,}  |  Max positions: {MAX_POSITIONS}")
    logger.info(f"  Excluded      : {', '.join(sorted(EXCLUDED_SYMBOLS))}")
    logger.info(f"  Order throttle: {ORDER_THROTTLE_DELAY}s/pair  (~{1/ORDER_THROTTLE_DELAY:.0f} pairs/s < 10-order/s limit)")
    logger.info(f"  State file    : {STATE_FILE}")
    logger.info("=" * 70)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Bot stopped")
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()
