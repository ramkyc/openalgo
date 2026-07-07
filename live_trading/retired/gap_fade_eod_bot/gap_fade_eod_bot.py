#!/usr/bin/env python3
"""
Gap Fade EOD Bot — Paper Trading (Analyzer Mode)
=================================================
Champion strategy from gap_continuation_study (Sharpe 2.624 IS / 2.336 OOS, 407 trades,
Jan 2023–Mar 2026).

Logic:
  - Gap DOWN ≥ 2% and ≤ 5% from prior close → LONG (gap fade, expect intraday recovery)
  - Long only. Gap-UP shorts not included (IS Sharpe 0.82 < gate of 1.5).
  - Entry:   09:15:05 AM (market open, MARKET MIS)
  - No SL:   Full-day hold. The recovery mechanism operates on a 6-hour timescale;
             tight SLs would be stopped out by normal intraday noise before the
             institutional absorption plays out.
  - Exit:    15:14:00 PM (EOD close price — same as backtest signal)

Universe: All 50 Nifty 50 EQ stocks (no preemptive per-stock exclusions — unlike the
preopen bot, the per-stock Stage 9 analysis showed 31/50 pass WR>50% and 34/50 have
positive Sharpe. Low-Sharpe stocks dilute but do not destroy the aggregate edge.)

Filter: Skip NIFTY weekly expiry days. On expiry days WR drops from 62% to 46%
(below the 50% gate). NIFTY expiry: Thursday pre-Sep 2025 → Tuesday from Sep 2025.

Paper trading uses OpenAlgo Analyzer (Sandbox) mode — no real orders sent.
P&L is logged to `gap_fade_eod_trades` table in live_trading/logs/gap_fade_eod_trades.db (SQLite).

Signal source:
  prev_close  → loaded from `preopen_data` DuckDB table (same as preopen_gap_fade_bot)
  gap at scan → LTP from multiquotes at 09:14:50 vs prev_close
  (IEP is the best proxy for actual open price available before 09:15)

Rate limit handling:
  - on_scan uses ONE multiquotes call for all 50 symbols (1 API hit).
  - on_entry places one MARKET order per stock, throttled at ORDER_THROTTLE_DELAY.
  - on_monitor uses ONE multiquotes call for all open positions every 30 min.
  - on_exit uses ONE multiquotes call for exit price capture, then places SELL orders.

Timing (IST):
  08:55       Startup: load prev_close, enable Analyzer mode
  09:14:50    Scan:    multiquotes all 50 → compute gap_pct → select signals
  09:15:05    Entry:   BUY MARKET orders (throttled, no SL-M)
  09:45–15:14 Monitor: refresh LTPs every 30 min (state file only; no SL logic)
  15:14:00    Exit:    SELL MARKET orders for all open positions
  15:30:00    Log:     write trade records to gap_fade_eod_trades table

Research artifacts: options_data/research/gap_continuation_study/
Champion config:    gap_lo=2.0%, gap_hi=5.0%, direction=down_fade, skip_expiry=True
IS Sharpe:  2.624  WR: 59.4%  n=288 (Jan 2023–Sep 2025)
OOS Sharpe: 2.336  WR: 59.7%  n=119 (Oct 2025–Mar 2026)
"""

import os
import sys
import json
import time
import logging
import requests
# import duckdb
from pathlib import Path
from datetime import datetime, time as dt_time, date, timedelta
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

# ── Path setup ────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from openalgo import api as OpenAlgoAPI
from live_trading.api_utils           import is_market_holiday
from live_trading.shared.trade_logger import log_trade_to_db
from live_trading.shared.telegram_notifier import wait_for_confirm

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "gap_fade_eod_bot.log"),
        logging.StreamHandler(),
    ],
)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
STRATEGY_NAME   = "GAP_FADE_EOD"
API_KEY         = os.getenv("OPENALGO_API_KEY")
HOST            = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
TG_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID")
IST             = pytz.timezone("Asia/Kolkata")

# Champion strategy parameters (from gap_continuation_study results_summary.md)
GAP_LO          = 2.0    # minimum down-gap % to trigger (inclusive)
GAP_HI          = 5.0    # maximum down-gap % (exclusive of circuit-level moves)
# Long only — gap-up short had IS Sharpe 0.82 (below Stage 2 gate of 1.5)

# Position sizing
CAPITAL_PER_TRADE = 100_000   # ₹1,00,000 per stock (equity capital standard)
# No MAX_POSITIONS cap — research champion has no limit; all qualifying gap-downs
# are traded.  Paper trading anyway so capital risk is virtual.

# Transaction cost estimate for P&L logging (intraday equity round-trip)
# STT 0.025% sell + brokerage ₹20×2 + exchange + SEBI + stamp ≈ ₹100 on ₹1L
COST_FLAT         = 100.0     # ₹ flat cost per trade (matches backtest assumption)

# Throttle: one MARKET order per stock at entry. Sleeping 0.2s between orders
# gives 5 orders/s, well under OpenAlgo ORDER_RATE_LIMIT of 10/s.
ORDER_THROTTLE_DELAY = 0.2    # seconds between successive entry/exit orders

# SQLite trade log — written by this bot only, no lock contention
TRADE_DB_PATH = LOG_DIR / "gap_fade_eod_trades.db"
STATE_FILE = LOG_DIR / "gap_fade_eod_state.json"

EXCHANGE  = "NSE"
PRODUCT   = "MIS"   # intraday — no overnight delivery STT

# Full Nifty 50 universe (50 stocks including TRENT added 2024 reconstitution)
NIFTY50_UNIVERSE = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL",
    "BPCL", "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB",
    "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK",
    "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK",
    "INDUSINDBK", "INFY", "ITC", "JSWSTEEL", "KOTAKBANK",
    "LT", "M&M", "MARUTI", "NESTLEIND", "NTPC",
    "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN",
    "SHRIRAMFIN", "SUNPHARMA", "TATACONSUM", "TATASTEEL", "TCS",
    "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
]


# ── Expiry calendar ───────────────────────────────────────────────────────────

def is_nifty_expiry_day(dt: date) -> bool:
    """
    Return True if `dt` is a NIFTY weekly expiry day.

    Schedule (from options_data CLAUDE.md + Stage 10 of gap_continuation_study):
      Pre-Sep 2025  → Thursday (weekday 3)
      Sep 2025+     → Tuesday  (weekday 1)

    We use the cutoff date of 2025-09-01 (first Tuesday-expiry month).
    Holiday shifts are NOT accounted for here — if the market is a holiday,
    is_market_holiday() catches it first, and the bot skips entirely.
    """
    cutover = date(2025, 9, 1)
    if dt >= cutover:
        return dt.weekday() == 1  # Tuesday
    else:
        return dt.weekday() == 3  # Thursday


# ── Database helpers ──────────────────────────────────────────────────────────

def ensure_eod_trades_table():
    """Create gap_fade_eod_trades table in SQLite if it doesn't exist."""
    import sqlite3
    con = sqlite3.connect(str(TRADE_DB_PATH), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    con.execute("""
        CREATE TABLE IF NOT EXISTS gap_fade_eod_trades (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date     TEXT    NOT NULL,
            symbol         TEXT    NOT NULL,
            gap_pct        REAL,
            prev_close     REAL,
            entry_price    REAL,
            exit_price     REAL,
            quantity       INTEGER,
            gross_pnl      REAL,
            cost_flat      REAL,
            net_pnl        REAL,
            gross_pct      REAL,
            net_pct        REAL,
            entry_order_id TEXT,
            exit_order_id  TEXT,
            notes          TEXT,
            created_at     TEXT DEFAULT (datetime('now')),
            UNIQUE (trade_date, symbol)
        )
    """)
    con.commit()
    con.close()
    logger.info("✅ gap_fade_eod_trades table ready (SQLite)")


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
    """Insert completed trade records into gap_fade_eod_trades (SQLite, WAL).

    UNIQUE(trade_date, symbol) + INSERT OR IGNORE prevents double-logging on restart.
    """
    if not trades:
        return
    import sqlite3
    con = sqlite3.connect(str(TRADE_DB_PATH), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")

    count_before = con.execute("SELECT COUNT(*) FROM gap_fade_eod_trades").fetchone()[0]

    for t in trades:
        con.execute("""
            INSERT OR IGNORE INTO gap_fade_eod_trades
                (trade_date, symbol, gap_pct, prev_close,
                 entry_price, exit_price, quantity,
                 gross_pnl, cost_flat, net_pnl, gross_pct, net_pct,
                 entry_order_id, exit_order_id, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            str(t.get("trade_date")), t.get("symbol"),
            t.get("gap_pct"), t.get("prev_close"),
            t.get("entry_price"), t.get("exit_price"), t.get("quantity"),
            t.get("gross_pnl"), t.get("cost_flat"), t.get("net_pnl"),
            t.get("gross_pct"), t.get("net_pct"),
            t.get("entry_order_id"), t.get("exit_order_id"), t.get("notes"),
        ])

    con.commit()
    count_after = con.execute("SELECT COUNT(*) FROM gap_fade_eod_trades").fetchone()[0]
    con.close()
    inserted = count_after - count_before
    skipped  = len(trades) - inserted
    if skipped:
        logger.warning(f"📝 log_trades: {inserted} inserted, {skipped} already in DB")
    else:
        logger.info(f"📝 Logged {inserted} trade(s) to gap_fade_eod_trades (SQLite)")


# ── OpenAlgo helpers ──────────────────────────────────────────────────────────

def send_telegram(msg: str):
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
    Fetch LTPs for a batch of symbols in ONE multiquotes API call.
    Returns {symbol: ltp}. Rate impact: 1 API call.
    """
    payload = [{"symbol": s, "exchange": EXCHANGE} for s in symbols]
    try:
        resp = client.multiquotes(symbols=payload)
        if not isinstance(resp, dict) or resp.get("status") != "success":
            logger.error(f"multiquotes error: {resp}")
            return {}
        results = resp.get("results", [])
        out = {}
        for item in results:
            if not isinstance(item, dict):
                continue
            sym  = item.get("symbol")
            data = item.get("data", {})
            if sym and isinstance(data, dict):
                ltp = data.get("ltp") or data.get("last_price")
                if ltp and float(ltp) > 0:
                    out[sym] = float(ltp)
        return out
    except Exception as e:
        logger.error(f"multiquotes exception: {e}")
        return {}


def place_order(client, symbol: str, action: str, quantity: int,
                price_type: str = "MARKET") -> str:
    """Place one order. Returns order_id or '' on failure."""
    try:
        resp = client.placeorder(
            strategy=STRATEGY_NAME,
            symbol=symbol,
            action=action,
            exchange=EXCHANGE,
            price_type=price_type,
            product=PRODUCT,
            quantity=str(quantity),
        )
        if isinstance(resp, dict) and resp.get("status") == "success":
            oid = str(resp.get("orderid", resp.get("data", {}).get("orderid", "")))
            logger.info(f"  ✅ {action} {symbol} qty={quantity} [{price_type}] → {oid}")
            return oid
        else:
            logger.error(f"  ❌ Order failed {symbol} {action}: {resp}")
            return ""
    except Exception as e:
        logger.error(f"  ❌ Order exception {symbol} {action}: {e}")
        return ""


# ── Bot State ─────────────────────────────────────────────────────────────────

class GapFadeEodBot:
    def __init__(self):
        if not API_KEY:
            logger.error("❌ OPENALGO_API_KEY not set. Exiting.")
            sys.exit(1)
        self.client     = OpenAlgoAPI(api_key=API_KEY, host=HOST)
        self.trade_date : date  = None
        self.prev_close : dict  = {}    # {symbol: prev_close}
        self.signals    : list  = []    # [{symbol, gap_pct, prev_close, ltp_scan}]
        self.positions  : dict  = {}    # {symbol: position_dict}
        self.analyzer_enabled: bool = False

    # ── State file ───────────────────────────────────────────────────────────

    def _save_state(self):
        try:
            pos_out = {}
            for sym, p in self.positions.items():
                pos_out[sym] = {
                    "gap_pct":        p.get("gap_pct", 0),
                    "prev_close":     p.get("prev_close", 0),
                    "entry_price":    p.get("entry_price", 0),
                    "exit_price":     p.get("exit_price"),
                    "quantity":       p.get("quantity", 0),
                    "current_price":  p.get("current_price", 0),
                    "net_pnl":        p.get("net_pnl"),
                    "entry_order_id": p.get("entry_order_id", ""),
                }
            STATE_FILE.write_text(json.dumps({
                "strategy":    STRATEGY_NAME,
                "trade_date":  str(self.trade_date) if self.trade_date else "",
                "positions":   pos_out,
                "n_signals":   len(self.signals),
                "last_update": datetime.now(IST).isoformat(),
            }, indent=2))
        except Exception as e:
            logger.warning(f"_save_state failed: {e}")

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _is_trading_day(self) -> bool:
        now = datetime.now(IST)
        if now.weekday() >= 5:
            logger.info("📅 Weekend — skipping")
            return False
        holiday = is_market_holiday(API_KEY, now)
        if holiday:
            logger.info(f"📅 Market holiday ({now.date()}) — skipping")
            return False
        if is_nifty_expiry_day(now.date()):
            logger.info(f"📅 NIFTY expiry day ({now.date()}) — skipping per Stage 10 filter "
                        f"(WR=46% on expiry days vs 62% otherwise)")
            send_telegram(f"⏭️ *Gap Fade EOD* — NIFTY expiry day ({now.date()}). "
                          f"No trades today (Stage 10 filter).")
            return False
        logger.info(f"✅ Trading day confirmed: {now.date()}")
        return True

    def _enable_analyzer_mode(self):
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
        logger.info(f"GAP FADE EOD BOT — {datetime.now(IST).strftime('%Y-%m-%d')}")
        logger.info("=" * 70)

        if not self._is_trading_day():
            return

        self.trade_date = datetime.now(IST).date()
        self.signals    = []
        self.positions  = {}

        self.prev_close = load_prev_close(self.client, NIFTY50_UNIVERSE)
        ensure_eod_trades_table()
        self._enable_analyzer_mode()
        self._save_state()

        logger.info(f"✅ Ready. prev_close loaded for {len(self.prev_close)} symbols")
        send_telegram(f"🌅 *Gap Fade EOD Bot* armed for {self.trade_date}\n"
                      f"Universe: {len(NIFTY50_UNIVERSE)} stocks | "
                      f"Threshold: −{GAP_LO}% to −{GAP_HI}% | Exit: 15:14")

    # ── Job 2: Scan at 09:14:50 ──────────────────────────────────────────────

    def on_scan(self):
        """09:14:50 AM — fetch LTPs (1 multiquotes call), detect gap-down signals."""
        if not self.trade_date:
            logger.warning("⚠️  on_scan: no trade_date — skipping")
            return

        # ── Conflict guard: exclude symbols already held by preopen_gap_fade_bot ──
        # Both bots scan at 09:14:50 and enter at 09:15:05 for the same universe.
        # Gap-down stocks qualify for both bots simultaneously. The preopen bot places
        # orders a few ms earlier, causing this bot's orders for shared symbols to be
        # rejected. We skip those symbols proactively to avoid rejected orders being
        # added to our positions state.
        preopen_state_file = LOG_DIR / "preopen_gap_fade_state.json"
        preopen_held: set = set()
        try:
            if preopen_state_file.exists():
                saved = json.loads(preopen_state_file.read_text())
                if saved.get("trade_date") == str(self.trade_date):
                    preopen_held = set(saved.get("positions", {}).keys())
            if preopen_held:
                logger.info(f"🚫 Conflict guard: skipping {sorted(preopen_held)} "
                            f"(already held by preopen_gap_fade_bot)")
        except Exception as e:
            logger.warning(f"Could not read preopen state for conflict guard: {e}")

        logger.info("🔍 Scanning for gap-down signals (1 multiquotes call)...")
        ltps = get_multiquotes(self.client, NIFTY50_UNIVERSE)
        if not ltps:
            logger.error("❌ No LTPs received — cannot scan")
            send_telegram("❌ *Gap Fade EOD* — multiquotes failed at scan time!")
            return

        logger.info(f"   LTPs received: {len(ltps)}/{len(NIFTY50_UNIVERSE)} symbols")

        candidates = []
        for sym in NIFTY50_UNIVERSE:
            if sym in preopen_held:
                continue   # skip — preopen bot already has this position
            ltp = ltps.get(sym)
            pc  = self.prev_close.get(sym)
            if not ltp or not pc or pc <= 0:
                continue
            gap = (ltp - pc) / pc * 100
            # Signal: gap down between 2% and 5% (inclusive of bounds, exclusive of >5%)
            if -GAP_HI < gap <= -GAP_LO:
                candidates.append({
                    "symbol":    sym,
                    "gap_pct":   round(gap, 3),
                    "prev_close": pc,
                    "ltp_scan":  ltp,
                })

        # Rank by largest gap (strongest signal first); all qualifying candidates traded
        candidates.sort(key=lambda x: x["gap_pct"])   # most negative first
        self.signals = candidates

        if self.signals:
            lines = [
                f"  🟢 LONG {s['symbol']:12s}  gap={s['gap_pct']:+.2f}%  LTP={s['ltp_scan']:.2f}"
                for s in self.signals
            ]
            logger.info(f"📊 {len(self.signals)} signal(s):\n" + "\n".join(lines))
            tg_lines = "\n".join(
                f"🟢 {s['symbol']} {s['gap_pct']:+.2f}%"
                for s in self.signals
            )
            send_telegram(f"📊 *Gap Fade EOD Signals* ({self.trade_date})\n"
                          f"{tg_lines}\n\nEntering LONG at 09:15 — exit 15:14")
        else:
            logger.info(f"   No gap-down signals today "
                        f"(no stocks gapped −{GAP_LO}% to −{GAP_HI}%)")
            send_telegram(f"💤 *Gap Fade EOD* — No signals today ({self.trade_date})")

        self._save_state()

    # ── Job 3: Entry at 09:15:05 ─────────────────────────────────────────────

    def on_entry(self):
        """
        09:15:05 AM — place BUY MARKET orders.

        No SL-M order: the full-day hold mechanism requires no intraday stop.
        A tight SL would be stopped out by normal market noise well before the
        institutional absorption completes at 15:14.

        Rate-limit: 1 MARKET order per stock, throttled at ORDER_THROTTLE_DELAY.
        Max 10 stocks → 10 orders total → 2 orders/s (well under 10/s limit).
        """
        if not self.signals:
            logger.info("No signals — skipping entry")
            return

        logger.info(f"🚀 Placing BUY orders for {len(self.signals)} signal(s)...")

        # ONE multiquotes call for fresh LTPs at entry time
        entry_syms  = [s["symbol"] for s in self.signals]
        fresh_ltps  = get_multiquotes(self.client, entry_syms)

        for sig in self.signals:
            sym = sig["symbol"]
            ltp = fresh_ltps.get(sym) or sig["ltp_scan"]
            if ltp <= 0:
                logger.warning(f"  ⚠️ {sym}: invalid LTP — skipping")
                continue

            quantity = max(1, int(CAPITAL_PER_TRADE / ltp))
            logger.info(f"  BUY {sym} qty={quantity} @ ~{ltp:.2f}  gap={sig['gap_pct']:+.2f}%")

            oid = place_order(self.client, sym, "BUY", quantity, "MARKET")

            # Only add to positions if the order was accepted (non-empty order_id).
            # In Analyzer mode, rejected orders still return an order_id from the API
            # but place_order() returns "" on a non-success response. If oid is empty,
            # do not track the position — a spurious 15:14 SELL would create a synthetic short.
            if not oid:
                logger.warning(f"  ⚠️ {sym}: order returned no ID — skipping position entry "
                               f"(possible rejection or conflict with another bot)")
                time.sleep(ORDER_THROTTLE_DELAY)
                continue

            self.positions[sym] = {
                "gap_pct":        sig["gap_pct"],
                "prev_close":     sig["prev_close"],
                "entry_price":    ltp,
                "exit_price":     None,
                "quantity":       quantity,
                "current_price":  ltp,
                "net_pnl":        None,
                "entry_order_id": oid,
                "exit_order_id":  "",
            }
            time.sleep(ORDER_THROTTLE_DELAY)

        logger.info(f"✅ Entry complete — {len(self.positions)} position(s) active. "
                    f"Next action: SELL at 15:14")
        self._save_state()

    # ── Job 4: Monitor every 30 min (09:45–15:15) ────────────────────────────

    def on_monitor(self):
        """
        Refresh LTPs for all open positions in ONE multiquotes call.

        No SL logic — this job only updates current_price in the state file
        so the dashboard shows live unrealised P&L. No orders are placed here.
        """
        open_pos = {s: p for s, p in self.positions.items()
                    if p.get("exit_price") is None}
        if not open_pos:
            return

        ltps = get_multiquotes(self.client, list(open_pos.keys()))
        now_str = datetime.now(IST).strftime("%H:%M:%S")

        for sym, pos in open_pos.items():
            ltp = ltps.get(sym)
            if ltp and ltp > 0:
                pos["current_price"] = ltp

        unreal = sum(
            (p["current_price"] - p["entry_price"]) * p["quantity"]
            for p in open_pos.values()
        )
        logger.info(f"[{now_str}] Monitor: {len(open_pos)} open | "
                    f"Unrealised P&L ≈ ₹{unreal:+,.0f} "
                    f"(1 multiquotes call for {len(open_pos)} symbols)")
        self._save_state()

    # ── Job 5: Exit at 15:14:00 ──────────────────────────────────────────────

    def on_exit(self):
        """15:14 PM — sell all open positions at market price."""
        logger.info("🔔 15:14 — closing all positions (EOD exit)...")

        open_pos = {s: p for s, p in self.positions.items()
                    if p.get("exit_price") is None}
        if not open_pos:
            logger.info("No open positions to close.")
            return

        logger.info(f"  {len(open_pos)} position(s) to close: {list(open_pos.keys())}")

        # ONE multiquotes call for accurate exit price recording
        exit_ltps = get_multiquotes(self.client, list(open_pos.keys()))

        for sym, pos in open_pos.items():
            ltp = exit_ltps.get(sym) or pos.get("current_price") or pos.get("entry_price")
            logger.info(f"  SELL {sym} qty={pos['quantity']} @ ~{ltp:.2f}")

            oid = place_order(self.client, sym, "SELL", int(pos["quantity"]), "MARKET")

            pos["exit_price"]    = ltp
            pos["exit_order_id"] = oid
            time.sleep(ORDER_THROTTLE_DELAY)

        logger.info(f"✅ Exit orders placed for {len(open_pos)} position(s)")
        self._save_state()

    # ── Job 6: Log P&L at 15:30 ──────────────────────────────────────────────

    def on_log_pnl(self):
        """15:30 PM — compute P&L, log to DB, send Telegram summary."""
        if not self.positions:
            logger.info("No trades today — nothing to log")
            return

        # Idempotency guard: skip re-insert if today already logged
        try:
            import sqlite3
            con = sqlite3.connect(str(TRADE_DB_PATH), timeout=30)
            already = con.execute(
                "SELECT COUNT(*) FROM gap_fade_eod_trades WHERE trade_date = ?",
                [str(self.trade_date)]
            ).fetchone()[0]
            con.close()
            if already > 0:
                logger.warning(f"⚠️  {already} trade(s) for {self.trade_date} already in DB "
                               f"— skipping re-insert")
                return
        except Exception as e:
            logger.warning(f"on_log_pnl guard query failed ({e}) — proceeding")

        logger.info("📝 Computing P&L...")
        total_net = 0.0
        records   = []

        for sym, pos in self.positions.items():
            entry = pos.get("entry_price", 0)
            exit_ = pos.get("exit_price") or pos.get("current_price", entry)
            qty   = pos.get("quantity", 0)
            if entry <= 0:
                continue

            gross_pnl = (exit_ - entry) * qty            # LONG only
            net_pnl   = gross_pnl - COST_FLAT
            gross_pct = (exit_ - entry) / entry * 100
            net_pct   = gross_pct - (COST_FLAT / (entry * qty) * 100) if qty > 0 else gross_pct
            total_net += net_pnl

            pos["net_pnl"] = round(net_pnl, 2)

            rec = {
                "trade_date":     self.trade_date,
                "symbol":         sym,
                "gap_pct":        pos["gap_pct"],
                "prev_close":     pos["prev_close"],
                "entry_price":    round(entry, 2),
                "exit_price":     round(exit_, 2),
                "quantity":       qty,
                "gross_pnl":      round(gross_pnl, 2),
                "cost_flat":      COST_FLAT,
                "net_pnl":        round(net_pnl, 2),
                "gross_pct":      round(gross_pct, 4),
                "net_pct":        round(net_pct, 4),
                "entry_order_id": pos.get("entry_order_id", ""),
                "exit_order_id":  pos.get("exit_order_id", ""),
                "notes":          "",
            }
            records.append(rec)

            icon = "✅" if net_pnl > 0 else "❌"
            logger.info(f"  {icon} {sym:12s} | gap={pos['gap_pct']:+.2f}% | "
                        f"entry={entry:.2f}  exit={exit_:.2f} | "
                        f"gross={gross_pct:+.2f}%  net=₹{net_pnl:+.0f}")

        log_trades_to_db(records)

        # Mirror into centralised paper_trades_options journal
        for rec in records:
            log_trade_to_db(
                bot_name      = "gap_fade_eod_bot",
                instrument    = rec["symbol"],
                option_symbol = rec["symbol"],
                option_type   = None,
                entry_time    = datetime.combine(rec["trade_date"], dt_time(9, 15)),
                exit_time     = datetime.combine(rec["trade_date"], dt_time(15, 14)),
                entry_premium = rec["entry_price"],
                exit_premium  = rec["exit_price"],
                exit_reason   = "EOD_15:14",
                quantity      = rec["quantity"],
                lots          = None,
                lot_size      = None,
                gross_pnl     = rec["gross_pnl"],
                order_id      = rec.get("entry_order_id"),
                notes         = f"gap={rec['gap_pct']:+.2f}%",
                strategy_type = "equity",
                direction     = "long",   # always long — gap-down stocks only
            )

        self._save_state()

        n_win  = sum(1 for r in records if r["net_pnl"] > 0)
        n_loss = len(records) - n_win
        logger.info(f"\n{'='*60}")
        logger.info(f"  DATE: {self.trade_date}  |  TRADES: {len(records)}")
        logger.info(f"  WINS: {n_win}  LOSSES: {n_loss}  "
                    f"WR: {n_win/max(len(records),1)*100:.1f}%")
        logger.info(f"  TOTAL NET P&L: ₹{total_net:+,.0f}")
        logger.info(f"{'='*60}")

        send_telegram(
            f"📊 *Gap Fade EOD Summary* ({self.trade_date})\n"
            f"Trades: {len(records)} | Wins: {n_win} | "
            f"WR: {n_win/max(len(records),1)*100:.0f}%\n"
            f"Net P&L: ₹{total_net:+,.0f}"
        )


# ── Scheduler ─────────────────────────────────────────────────────────────────

def main():
    bot = GapFadeEodBot()

    # Write initial state so dashboard shows bot as online before first job fires
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

    # Startup recovery: fire any jobs missed between 08:55 and 15:30
    now      = datetime.now(IST)
    now_time = now.time()
    logger.info(f"📊 Startup check: {now_time.strftime('%H:%M:%S')} | "
                f"recovery window [08:55, 15:30]")

    if dt_time(8, 55) <= now_time < dt_time(15, 30):
        if not bot._is_trading_day():
            logger.info("📅 Recovery check: Holiday / Weekend / Expiry — skipping.")
        else:
            bot.trade_date = now.date()
            logger.info(f"⚡ Entering recovery block for {bot.trade_date}")

            if existing_positions:
                # Restore positions from state file — don't re-enter
                logger.info(f"⚡ Late start — restoring {len(existing_positions)} "
                            f"positions: {list(existing_positions.keys())}")
                bot.positions = {
                    sym: {
                        "gap_pct":        float(p.get("gap_pct") or 0),
                        "prev_close":     float(p.get("prev_close") or 0),
                        "entry_price":    float(p.get("entry_price") or 0),
                        "exit_price":     p.get("exit_price"),
                        "quantity":       int(p.get("quantity") or 0),
                        "current_price":  float(p.get("current_price") or p.get("entry_price") or 0),
                        "net_pnl":        p.get("net_pnl"),
                        "entry_order_id": p.get("entry_order_id", ""),
                        "exit_order_id":  p.get("exit_order_id", ""),
                    }
                    for sym, p in existing_positions.items()
                }
                bot._enable_analyzer_mode()

            if now_time >= dt_time(15, 14, 0):
                # Past exit time — close any open positions and log
                logger.warning("🚨 Recovery: past 15:14 — running exit + log")
                bot._enable_analyzer_mode()
                bot.on_exit()
                bot.on_log_pnl()
            else:
                if not existing_positions:
                    bot.on_market_prep()
                    if now_time >= dt_time(9, 14, 50):
                        bot.on_scan()
                    if now_time >= dt_time(9, 15, 5):
                        # ── Late-entry confirmation via Telegram ──────────────
                        # Gap Fade EOD holds until 15:14 so a late entry (even
                        # at 10:00+) still captures most of the day's move.
                        if bot.signals:
                            signal_lines = "\n".join(
                                f"  🟢 LONG {s['symbol']:12s}  gap={s['gap_pct']:+.2f}%  LTP={s['ltp_scan']:.2f}"
                                for s in bot.signals
                            )
                            expire_time = (now + __import__('datetime').timedelta(minutes=10)).strftime('%H:%M')
                            confirm_msg = (
                                f"⚠️ *Gap Fade EOD — LATE START* ({now.strftime('%H:%M:%S')})\n"
                                f"Found {len(bot.signals)} signal(s) (exit still at 15:14):\n"
                                f"`{signal_lines}`\n\n"
                                f"Reply `/confirm_gapeod` to enter now.\n"
                                f"Offer expires at {expire_time} IST. No reply → session skipped."
                            )
                            send_telegram(confirm_msg)
                            logger.info("⏳ Waiting for Telegram /confirm_gapeod (10 min window) …")
                            if wait_for_confirm("/confirm_gapeod", timeout_secs=600):
                                logger.info("✅ Confirmed — entering trades now (late, EOD exit still 15:14).")
                                bot.on_entry()
                            else:
                                logger.info("⏰ No confirm received — skipping entry for today.")
                                send_telegram("⏰ *Gap Fade EOD* — confirm window expired. No trades entered today.")
                        else:
                            logger.info("   No gap-down signals found at late-start scan — nothing to enter.")
                else:
                    bot._save_state()
                    logger.info(f"  Positions restored: {list(bot.positions.keys())}")

    scheduler = BlockingScheduler(timezone=IST)

    # Core jobs
    scheduler.add_job(bot.on_market_prep, CronTrigger(hour=8,  minute=55, second=0,  timezone=IST),
                      id="market_prep", replace_existing=True)
    scheduler.add_job(bot.on_scan,        CronTrigger(hour=9,  minute=14, second=50, timezone=IST),
                      id="scan",        replace_existing=True)
    scheduler.add_job(bot.on_entry,       CronTrigger(hour=9,  minute=15, second=5,  timezone=IST),
                      id="entry",       replace_existing=True)
    scheduler.add_job(bot.on_exit,        CronTrigger(hour=15, minute=25, second=0,  timezone=IST),
                      id="exit",        replace_existing=True)
    scheduler.add_job(bot.on_log_pnl,     CronTrigger(hour=15, minute=30, second=0,  timezone=IST),
                      id="log_pnl",     replace_existing=True)

    # Monitor every 30 min from 09:45 to 15:15 (LTP refresh for dashboard only)
    scheduler.add_job(bot.on_monitor, "cron",
                      hour="9-15", minute="15,45", second="0", timezone=IST,
                      id="monitor", replace_existing=True)

    # Heartbeat: save state every minute
    scheduler.add_job(bot._save_state, "interval", minutes=1,
                      id="heartbeat", replace_existing=True)

    logger.info("=" * 70)
    logger.info("GAP FADE EOD BOT — SCHEDULER STARTED")
    logger.info(f"  Strategy      : {STRATEGY_NAME}")
    logger.info(f"  Signal        : Gap down −{GAP_LO}% to −{GAP_HI}% → LONG")
    logger.info(f"  Entry / Exit  : 09:15:05 → 15:14:00 (full-day hold)")
    logger.info(f"  Stop-loss     : NONE (full-day hold; no intraday SL)")
    logger.info(f"  Capital/trade : ₹{CAPITAL_PER_TRADE:,}  |  Max positions: no cap (all qualifying signals)")
    logger.info(f"  Expiry filter : Skip NIFTY expiry days (currently Tuesdays)")
    logger.info(f"  Universe      : {len(NIFTY50_UNIVERSE)} Nifty50 stocks (no pre-exclusions)")
    logger.info(f"  Order throttle: {ORDER_THROTTLE_DELAY}s between orders")
    logger.info(f"  State file    : {STATE_FILE}")
    logger.info(f"  Research      : gap_continuation_study | OOS Sharpe=2.336 WR=59.7%")
    logger.info("=" * 70)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Bot stopped")
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()
