"""
Daily Sniper Bot v2 — Bollinger Band Mean-Reversion on CNC Stocks
=================================================================
Strategy:
  - Runs once daily at 3:20 PM IST
  - Scans WHITELIST stocks for price ≤ BB lower band → BUY (CNC)
  - Checks existing holdings: exit if price ≥ BB middle (target) or price ≤ SL
  - All events sent to Telegram

Fixes applied (2026-03-18):
  - Added full Telegram notification for every event
  - Fixed history() call: start_date/end_date + interval="D" (removed bad days= param)
  - Removed non-existent to_pandas() call (history() already returns a DataFrame)
  - Fixed position check: holdings() for CNC overnight + positionbook() same-day
  - Fixed placeorder() param names: action=, price_type= (not side=, order_type=)
  - Added proper logging (file + console) instead of bare print()
  - Added IST timezone to scheduler
"""

import os
import sys
import logging
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

# ── Path & env setup ──────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from openalgo import api as OpenAlgoAPI

# ── Constants ─────────────────────────────────────────────────────────────────
STRATEGY_NAME  = "DAILY_SNIPER"
IST            = ZoneInfo("Asia/Kolkata")

WHITELIST = [
    "DIVISLAB", "CIPLA", "DRREDDY", "SUNPHARMA",   # Pharma
    "ICICIBANK", "KOTAKBANK",                        # Banking
    "BAJAJ-AUTO", "M&M",                             # Auto
    "RELIANCE", "LT",                                # Heavyweights
]

BB_PERIOD      = 20
BB_STD         = 2.0
STOP_LOSS_PCT   = 0.03       # 3% protective SL below entry
HISTORY_DAYS    = 60         # calendar days to fetch (~30 trading days for BB_PERIOD=20)
CAPITAL_PER_TRADE = 100_000  # ₹1,00,000 per stock; qty = floor(capital / entry_price)

API_KEY        = os.getenv("OPENALGO_API_KEY")
HOST           = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
TG_TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID")
STATE_FILE     = PROJECT_ROOT / "live_trading" / "logs" / "daily_sniper_state.json"

# ── Logging ───────────────────────────────────────────────────────────────────
log_dir = PROJECT_ROOT / "live_trading" / "logs"
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "daily_sniper_bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
# Silence APScheduler's per-job INFO chatter ("Running job…" / "executed successfully")
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(message: str) -> None:
    """Send a message to the configured Telegram chat. Fails silently."""
    if not TG_TOKEN or not TG_CHAT_ID:
        logger.warning("Telegram not configured (TG_TOKEN / TG_CHAT_ID missing)")
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(
            url,
            data={
                "chat_id":    TG_CHAT_ID,
                "text":       f"🏹 *DAILY SNIPER*\n{message}",
                "parse_mode": "Markdown",
            },
            timeout=5,
        )
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")

# ── State persistence ─────────────────────────────────────────────────────────
def save_state(status: str = "waiting", extra: dict | None = None) -> None:
    """Write heartbeat state file for the status dashboard."""
    state = {
        "strategy":    STRATEGY_NAME,
        "last_update": datetime.now().isoformat(),
        "status":      status,
    }
    if extra:
        state.update(extra)
    try:
        STATE_FILE.parent.mkdir(exist_ok=True)
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        logger.warning(f"Could not save state: {e}")

# ── OpenAlgo helpers ──────────────────────────────────────────────────────────
def get_holding(client: OpenAlgoAPI, symbol: str) -> dict | None:
    """
    Return holding/position dict for symbol if currently held, else None.
    Checks holdings (overnight CNC) first, then positionbook (same-day CNC).

    Returns dict with keys: quantity, average_price
    """
    # 1. Overnight holdings
    try:
        resp = client.holdings()
        for h in resp.get("data", {}).get("holdings", []):
            if h.get("symbol") == symbol and int(h.get("quantity", 0)) > 0:
                return {
                    "quantity":      int(h["quantity"]),
                    "average_price": float(h.get("averageprice", h.get("average_price", 0))),
                }
    except Exception as e:
        logger.warning(f"  holdings() error for {symbol}: {e}")

    # 2. Same-day positionbook fallback (CNC bought today, not yet settled)
    try:
        res = requests.post(
            f"{HOST}/api/v1/positionbook",
            json={"apikey": API_KEY},
            timeout=5,
        ).json()
        for pos in res.get("data", []):
            if (pos.get("symbol") == symbol
                    and pos.get("product", "").upper() == "CNC"
                    and int(pos.get("quantity", 0)) > 0):
                return {
                    "quantity":      int(pos["quantity"]),
                    "average_price": float(pos.get("average_price", 0)),
                }
    except Exception as e:
        logger.warning(f"  positionbook() error for {symbol}: {e}")

    return None


def fetch_daily_bars(client: OpenAlgoAPI, symbol: str) -> pd.DataFrame | None:
    """
    Fetch the last HISTORY_DAYS calendar days of daily OHLC for symbol.
    Returns a DataFrame or None on failure.
    """
    end_date   = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    try:
        df = client.history(
            symbol=symbol,
            exchange="NSE",
            interval="D",
            start_date=start_date,
            end_date=end_date,
        )
        if df is None or (isinstance(df, dict) and df.get("status") == "error"):
            logger.warning(f"  {symbol}: history() returned error — {df}")
            return None
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.warning(f"  {symbol}: history() returned empty data")
            return None
        return df
    except Exception as e:
        logger.warning(f"  {symbol}: history() exception — {e}")
        return None


def enable_analyzer(client: OpenAlgoAPI) -> None:
    """Ensure OpenAlgo is in Analyze (paper) mode before placing any order."""
    try:
        resp = client.analyzertoggle(mode=True)
        mode = resp.get("data", {}).get("mode", "unknown") if isinstance(resp, dict) else "unknown"
        logger.info(f"🧪 Analyzer mode: {mode}")
    except Exception as e:
        logger.warning(f"analyzertoggle() failed: {e}")


def place_order_safe(client: OpenAlgoAPI, symbol: str, action: str, quantity: int) -> str | None:
    """
    Place a CNC MARKET order in Analyzer (paper) mode.
    Returns order_id string or None on failure.
    """
    enable_analyzer(client)   # always re-confirm paper mode just before ordering
    try:
        result = client.placeorder(
            strategy=STRATEGY_NAME,
            symbol=symbol,
            action=action,          # "BUY" or "SELL"
            exchange="NSE",
            price_type="MARKET",
            product="CNC",
            quantity=quantity,
        )
        oid = result.get("orderid") if isinstance(result, dict) else str(result)
        logger.info(f"  ✅ Order placed: {action} {quantity} {symbol} → order_id={oid}")
        return oid
    except Exception as e:
        logger.error(f"  ❌ Order failed: {action} {symbol} — {e}")
        return None

# ── Main strategy logic ───────────────────────────────────────────────────────
def check_and_trade() -> None:
    """Main strategy job — runs at 3:20 PM IST every trading day."""
    now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"")
    logger.info(f"{'='*60}")
    logger.info(f"🏹 Daily Sniper scan started at {now}")
    logger.info(f"{'='*60}")
    save_state("scanning")

    client = OpenAlgoAPI(api_key=API_KEY, host=HOST)

    entries  = []   # symbols where entry order was placed
    exits    = []   # (symbol, reason, price) where exit was placed
    holdings = []   # (symbol, entry, current, sl, middle) — still holding, no action
    no_signal = []  # symbols scanned with no signal

    for symbol in WHITELIST:
        try:
            # ── 1. Fetch daily bars ──────────────────────────────────────────
            df = fetch_daily_bars(client, symbol)
            if df is None or len(df) < BB_PERIOD:
                logger.warning(f"  {symbol}: insufficient data ({len(df) if df is not None else 0} bars) — skipping")
                continue

            # ── 2. Compute Bollinger Bands ───────────────────────────────────
            df["sma"]    = df["close"].rolling(window=BB_PERIOD).mean()
            df["std"]    = df["close"].rolling(window=BB_PERIOD).std()
            df["lower"]  = df["sma"] - BB_STD * df["std"]
            df["middle"] = df["sma"]

            current = float(df["close"].iloc[-1])
            lower   = float(df["lower"].iloc[-1])
            middle  = float(df["middle"].iloc[-1])

            logger.info(f"  {symbol:<12}  close={current:.2f}  lower={lower:.2f}  middle={middle:.2f}")

            # ── 3. Check existing holding ────────────────────────────────────
            holding = get_holding(client, symbol)

            if holding:
                entry_px = holding["average_price"]
                qty      = holding["quantity"]
                sl_px    = entry_px * (1 - STOP_LOSS_PCT)

                if current >= middle:
                    logger.info(f"  🏁 {symbol}: TARGET reached  current={current:.2f} ≥ middle={middle:.2f}")
                    place_order_safe(client, symbol, "SELL", qty)
                    exits.append((symbol, "TARGET", current, entry_px, middle, qty))

                elif current <= sl_px:
                    logger.info(f"  🛑 {symbol}: SL HIT  current={current:.2f} ≤ sl={sl_px:.2f}")
                    place_order_safe(client, symbol, "SELL", qty)
                    exits.append((symbol, "SL", current, entry_px, sl_px, qty))

                else:
                    pct = (current - entry_px) / entry_px * 100
                    logger.info(f"  ⌛ {symbol}: HOLDING  entry={entry_px:.2f}  current={current:.2f} ({pct:+.2f}%)  sl={sl_px:.2f}  tgt={middle:.2f}")
                    holdings.append((symbol, entry_px, current, sl_px, middle))

            else:
                # ── 4. Entry check ───────────────────────────────────────────
                if current <= lower:
                    qty = max(1, int(CAPITAL_PER_TRADE / current))
                    capital_deployed = qty * current
                    sl_px = round(current * (1 - STOP_LOSS_PCT), 2)
                    logger.info(f"  🎯 {symbol}: ENTRY  current={current:.2f} ≤ lower={lower:.2f}  sl={sl_px:.2f}  qty={qty} (₹{capital_deployed:,.0f})")
                    place_order_safe(client, symbol, "BUY", qty)
                    entries.append((symbol, current, lower, middle, qty, capital_deployed, sl_px))
                else:
                    no_signal.append(symbol)

        except Exception as e:
            logger.error(f"  ❌ Unexpected error for {symbol}: {e}", exc_info=True)

    # ── Build & send Telegram summary ─────────────────────────────────────────
    lines = [f"📅 *{datetime.now(IST).strftime('%d %b %Y')}  —  3:20 PM Scan*\n"]

    if entries:
        lines.append("*🎯 NEW ENTRIES*")
        for sym, price, band, tgt, qty, cap, sl in entries:
            rr = (tgt - price) / (price - sl) if (price - sl) > 0 else 0
            lines.append(
                f"  BUY `{sym}`  ₹{price:.2f} × {qty} = ₹{cap:,.0f}\n"
                f"    🛑 SL ₹{sl:.2f}  🎯 Target ₹{tgt:.2f}  (R:R 1:{rr:.1f})"
            )
        lines.append("")

    if exits:
        lines.append("*🏁 EXITS*")
        for sym, reason, curr, entry_px, ref, qty in exits:
            pnl = (curr - entry_px) * qty
            tag = "✅ TARGET" if reason == "TARGET" else "🛑 SL HIT"
            lines.append(f"  {tag} `{sym}`  entry ₹{entry_px:.2f} → exit ₹{curr:.2f} × {qty}  P&L ₹{pnl:+.0f}")
        lines.append("")

    if holdings:
        lines.append("*⌛ STILL HOLDING*")
        for sym, entry_px, curr, sl, tgt in holdings:
            pct = (curr - entry_px) / entry_px * 100
            lines.append(f"  `{sym}`  ₹{curr:.2f} ({pct:+.2f}%)  SL ₹{sl:.2f}  Tgt ₹{tgt:.2f}")
        lines.append("")

    if not entries and not exits and not holdings:
        lines.append("_No signals today — all clear._")

    send_telegram("\n".join(lines))

    # ── Update state file ──────────────────────────────────────────────────────
    save_state(
        status="done",
        extra={
            "last_scan_date": datetime.now(IST).strftime("%Y-%m-%d"),
            "entries":        [e[0] for e in entries],
            "exits":          [x[0] for x in exits],
            "holdings":       [h[0] for h in holdings],
        },
    )
    logger.info(f"✅ Scan complete — {len(entries)} entries, {len(exits)} exits, {len(holdings)} holding")


# ── Scheduler setup ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info(f"🏹 DAILY SNIPER BOT — STARTING  (paper / analyzer mode)")
    logger.info(f"   Watchlist    : {', '.join(WHITELIST)}")
    logger.info(f"   BB params    : period={BB_PERIOD}  std={BB_STD}")
    logger.info(f"   SL           : {STOP_LOSS_PCT*100:.1f}%")
    logger.info(f"   Capital/stock: ₹{CAPITAL_PER_TRADE:,}  (qty = floor(₹{CAPITAL_PER_TRADE:,} / price))")
    logger.info(f"   Fire time    : 15:20 IST daily")
    logger.info("=" * 60)

    save_state("starting")

    # Enable analyzer (paper) mode at startup
    _startup_client = OpenAlgoAPI(api_key=API_KEY, host=HOST)
    enable_analyzer(_startup_client)

    scheduler = BlockingScheduler(timezone=IST)

    # Main strategy job at 3:20 PM IST
    scheduler.add_job(
        check_and_trade,
        CronTrigger(hour=15, minute=20, timezone=IST),
        id="daily_sniper",
        name="Daily Sniper 3:20 PM",
    )

    # Heartbeat every minute
    scheduler.add_job(
        save_state,
        "interval",
        minutes=1,
        id="heartbeat",
        name="Heartbeat",
    )

    logger.info("Scheduler started — waiting for 15:20 IST")
    send_telegram(f"🚀 *Bot started* _(paper / analyzer mode)_\nWatching {len(WHITELIST)} stocks @ ₹1L/trade. Next scan: 15:20 IST")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Daily Sniper Bot stopped")
        scheduler.shutdown(wait=False)
