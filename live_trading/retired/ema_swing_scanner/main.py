"""
EMA Swing Scanner — Paper Trading Bot
======================================
Runs daily at 15:30 IST after the daily candle closes.

  1. Checks regime (NIFTY50 index > 50 EMA)
  2. Scans 19 NIFTY50 stocks for EMA pullback + RSI signals
  3. Opens pending paper positions at next day's open
  4. Updates trailing stops on existing paper positions
  5. Writes state JSON for the dashboard
  6. Sends Telegram alert with today's signals and position updates

Mode:
  EMA_SWING_PAPER_MODE=true  (default) → log-only, no real orders
  EMA_SWING_PAPER_MODE=false           → live orders via OpenAlgo (future)

Run:
    python -m live_trading.ema_swing_scanner.main
  or:
    python live_trading/ema_swing_scanner/main.py

Runs indefinitely — fires daily at 15:30 IST, skips weekends.
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, date, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from openalgo import api as OpenAlgoAPI
from live_trading.ema_swing_scanner.scanner      import run_scan, UNIVERSE, EXCHANGE
from live_trading.ema_swing_scanner.paper_trader import PaperTrader
from live_trading.shared.telegram_notifier       import send_sync

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "ema_swing_scanner.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY    = os.getenv("OPENALGO_API_KEY")
HOST       = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
PAPER_MODE = os.getenv("EMA_SWING_PAPER_MODE", "true").lower() != "false"

SCAN_HOUR   = 15
SCAN_MINUTE = 30
STARTING_CAPITAL = 1_000_000   # ₹10,00,000


# ── Today's OHLCV fetch (for position updates) ────────────────────────────────
def fetch_today_ohlcv(api_key: str, host: str) -> dict[str, dict]:
    """Fetch today's OHLCV for all universe stocks."""
    today     = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
    result    = {}

    for sym in UNIVERSE:
        try:
            r = requests.post(
                f"{host}/api/v1/history",
                json={"apikey": api_key, "symbol": sym, "exchange": EXCHANGE,
                      "interval": "D", "start_date": yesterday, "end_date": today},
                timeout=10,
            )
            data = r.json()
            if data.get("status") == "success" and data.get("data"):
                last = data["data"][-1]
                result[sym] = {
                    "open":  float(last.get("open",  0)),
                    "high":  float(last.get("high",  0)),
                    "low":   float(last.get("low",   0)),
                    "close": float(last.get("close", 0)),
                }
        except Exception as e:
            logger.warning(f"OHLCV fetch failed for {sym}: {e}")

    return result


# ── Telegram alert builders ────────────────────────────────────────────────────
def _build_signal_message(signals, index_info, regime_active, trader: PaperTrader) -> str:
    mode_tag = "📝 PAPER" if PAPER_MODE else "🔴 LIVE"
    now_str  = datetime.now().strftime("%d %b %Y %H:%M")

    lines = [f"📊 *EMA Swing Scanner — {now_str}* [{mode_tag}]", ""]

    # Regime
    regime_emoji = "✅" if regime_active else "❌"
    lines.append(
        f"{regime_emoji} *Regime*: NIFTY50 {index_info['close']:,.0f} vs EMA50 {index_info['ema50']:,.0f}"
    )
    lines.append("")

    # Signals
    if signals:
        lines.append(f"🎯 *{len(signals)} Signal(s) — Entry Tomorrow Open*")
        for s in signals:
            tier_label = {"tier1": "T1★", "tier2": "T2", "tier3": "T3"}.get(s.tier, s.tier)
            lines.append(
                f"  • `{s.symbol}` [{tier_label}] | RSI {s.rsi:.0f} | ATR {s.atr:.1f} | "
                f"Stop ≈ ₹{s.suggested_stop:.1f}"
            )
    else:
        if regime_active:
            lines.append("🔍 No signals today.")
        else:
            lines.append("⏸ Regime OFF — no entries.")

    # Open positions
    lines.append("")
    if trader.open_positions:
        lines.append(f"📂 *Open Positions ({len(trader.open_positions)})*")
        for sym, pos in trader.open_positions.items():
            be = "BE✅" if pos.breakeven_active else "  "
            lines.append(
                f"  • `{sym}` {be} | Entry ₹{pos.entry_price:.1f} | "
                f"Stop ₹{pos.stop_eod:.1f} | Day {pos.days_held}"
            )
    else:
        lines.append("📂 No open positions.")

    # Closed today
    if trader.closed_trades:
        lines.append("")
        lines.append(f"🏁 *Closed Today ({len(trader.closed_trades)})*")
        for t in trader.closed_trades:
            emoji = "🟢" if t["win"] else "🔴"
            lines.append(
                f"  {emoji} `{t['symbol']}` {t['exit_reason']} | "
                f"P&L ₹{t['pnl_inr']:+,.0f} ({t['pnl_pct']:+.1f}%)"
            )

    # Portfolio summary
    ret_pct = (trader.portfolio_value - STARTING_CAPITAL) / STARTING_CAPITAL * 100
    lines.append("")
    lines.append(
        f"💼 *Portfolio*: ₹{trader.portfolio_value:,.0f} "
        f"({ret_pct:+.1f}% from ₹{STARTING_CAPITAL/1e5:.0f}L)"
    )

    return "\n".join(lines)


# ── Core daily scan execution ─────────────────────────────────────────────────
def run_daily_scan(trader: PaperTrader):
    today     = date.today()
    today_str = str(today)
    logger.info(f"═══ Daily scan starting: {today_str} ═══")

    if not API_KEY:
        logger.error("OPENALGO_API_KEY not set in .env — aborting scan.")
        return

    # 1. Fetch today's OHLCV for position updates
    logger.info("Fetching today's OHLCV for open position updates...")
    today_ohlcv = fetch_today_ohlcv(API_KEY, HOST)

    # 2. Update existing paper positions (check stops, update trailing)
    trader.process_day(today_ohlcv, today)

    # 3. Run signal scan
    logger.info("Running signal scan...")
    regime_active, index_info, signals = run_scan(API_KEY, HOST)

    # 4. Queue new signals as pending for tomorrow
    if signals and regime_active:
        trader.add_signals(signals, today_str)

    # 5. Save state for dashboard
    trader.save_state(
        index_info=index_info or {},
        signals_today=signals,
        regime_active=regime_active,
        last_scan=datetime.now().isoformat(),
    )

    # 6. Send Telegram alert
    msg = _build_signal_message(signals, index_info or {}, regime_active, trader)
    sent = send_sync(msg)
    if sent:
        logger.info("Telegram alert sent.")
    else:
        logger.info("Telegram not configured — alert skipped.")

    logger.info(
        f"Scan done | Signals: {len(signals)} | Open: {len(trader.open_positions)} | "
        f"Portfolio: ₹{trader.portfolio_value:,.0f}"
    )


# ── Scheduler loop ────────────────────────────────────────────────────────────
def _is_weekday() -> bool:
    return datetime.now().weekday() < 5   # Mon–Fri

def _seconds_until_next_scan() -> float:
    """Return seconds until next 15:30 IST on a weekday."""
    now  = datetime.now()
    target = now.replace(hour=SCAN_HOUR, minute=SCAN_MINUTE, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    # Skip to next Monday if weekend
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def main():
    mode_str = "📝 PAPER (log only)" if PAPER_MODE else "🔴 LIVE ORDERS via OpenAlgo"
    logger.info("=" * 60)
    logger.info("EMA Swing Scanner starting up")
    logger.info(f"Mode       : {mode_str}")
    logger.info(f"Host       : {HOST}")
    logger.info(f"Scan time  : {SCAN_HOUR:02d}:{SCAN_MINUTE:02d} IST (Mon–Fri)")
    logger.info(f"Capital    : ₹{STARTING_CAPITAL:,.0f}")
    logger.info("=" * 60)

    # Build OpenAlgo client — used only when PAPER_MODE=false
    openalgo_client = None
    if not PAPER_MODE:
        if not API_KEY:
            logger.error("OPENALGO_API_KEY not set — cannot run in live mode. Set it in .env.")
            sys.exit(1)
        openalgo_client = OpenAlgoAPI(api_key=API_KEY, host=HOST)
        logger.info(f"OpenAlgo client initialised → {HOST}")

    trader = PaperTrader(
        LOGS_DIR,
        starting_capital=float(STARTING_CAPITAL),
        paper_mode=PAPER_MODE,
        openalgo_client=openalgo_client,
    )

    send_sync(
        f"🤖 *EMA Swing Scanner Online*\n"
        f"Mode: {mode_str}\n"
        f"Universe: 19 NIFTY50 stocks\n"
        f"Fires daily at 15:30 IST\n"
        f"Capital: ₹{STARTING_CAPITAL/1e5:.0f}L"
    )

    while True:
        now = datetime.now()

        # Run immediately if it's a weekday and past scan time and we haven't run today
        if _is_weekday():
            is_holiday = False
            try:
                from live_trading.api_utils import is_market_holiday
                is_holiday = is_market_holiday(API_KEY)
            except Exception:
                pass

            if is_holiday:
                logger.info("📅 Market Holiday today — skipping daily scan.")
            else:
                target_today = now.replace(
                    hour=SCAN_HOUR, minute=SCAN_MINUTE, second=0, microsecond=0
                )
                # Check if scan time just hit (within 60s window)
                elapsed = (now - target_today).total_seconds()
                if 0 <= elapsed < 60:
                    try:
                        run_daily_scan(trader)
                    except Exception as e:
                        logger.exception(f"Scan error: {e}")

        wait = _seconds_until_next_scan()
        logger.info(f"Next scan in {wait/3600:.1f}h ({datetime.now() + timedelta(seconds=wait):%d %b %H:%M})")
        time.sleep(min(wait, 55))   # wake up 55s before next scan


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("EMA Swing Scanner stopped.")
