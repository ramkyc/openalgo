"""
main.py — BANKNIFTY Monthly Iron Fly paper trading scheduler.
Entry: uv run python live_trading/banknifty_iron_fly_monthly_bot/main.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from live_trading.banknifty_iron_fly_monthly_bot.scanner        import scan
from live_trading.banknifty_iron_fly_monthly_bot.paper_trader   import PaperTrader, load_state, save_state
from live_trading.shared.telegram_notifier                    import send_async

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "live_trading" / "logs" / "banknifty_iron_fly_monthly_bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

POLL_SECS  = 30
ENTRY_HOUR, ENTRY_MIN = 10, 0


async def main() -> None:
    trader = PaperTrader()

    # Restore open position from a previous session
    trader.state = load_state()
    if trader.state and not trader.state.get("closed") and trader.state.get("legs"):
        logger.info(f"♻️  Restored open position from {trader.state.get('entry_time')}")
        logger.info(f"   Expiry: {trader.state['expiry_str']}  "
                    f"Exit: {trader.state['exit_day']} 15:15")
    else:
        logger.info("No open position — awaiting next entry day.")
        trader.state = None

    while True:
        now = datetime.now()
        t   = now.time()

        # Outside market hours → sleep
        if t < dt_time(9, 15) or (t.hour == 15 and t.minute >= 30) or t.hour > 15:
            await asyncio.sleep(60)
            continue

        # ── Active position: monitor ─────────────────────────────────────────
        if trader.state and not trader.state.get("closed") and trader.state.get("legs"):
            exit_reason, _ = trader._process_bar(now)
            if exit_reason:
                await trader.exit(exit_reason, now)
                await asyncio.sleep(POLL_SECS)
                continue

            legs    = trader.state["legs"]
            mtm     = trader.state.get("current_mtm", 0)
            n_adj   = trader.state.get("n_adjustments", 0)
            logger.info(
                f"📊 MTM=₹{mtm:+,.0f}  n_adj={n_adj}  |  "
                f"SC={legs['sell_ce']['symbol'][:20]}  "
                f"SP={legs['sell_pe']['symbol'][:20]}"
            )
            await asyncio.sleep(POLL_SECS)
            continue

        # ── No position: check if today is entry day ─────────────────────────
        # Persist standby state so dashboard knows the bot is running
        save_state({
            "strategy": "BANKNIFTY_IRON_FLY_MONTHLY",
            "closed": False,
            "legs": {},
            "paper_mode": True,
        })

        # Run scan once per minute outside entry window, once per poll inside
        signal = scan()
        if signal is None:
            if t.minute == 0:
                logger.info("Standby — not an entry day.")
            await asyncio.sleep(60)
            continue

        # ── Entry window: 10:00–10:10 IST ──────────────────────────────────
        entry_open  = now.replace(hour=ENTRY_HOUR, minute=ENTRY_MIN, second=0, microsecond=0)
        entry_close = entry_open + timedelta(minutes=10)

        if now < entry_open:
            wait = int((entry_open - now).total_seconds())
            logger.info(f"Entry window opens in {wait}s — waiting.")
            await asyncio.sleep(min(wait, 60))
            continue

        if now > entry_close:
            logger.info("Entry window closed — no trade today.")
            await asyncio.sleep(3600)
            continue

        # Execute entry
        success = await trader.enter(signal)
        if success:
            logger.info("✅ Position entered — starting MTM monitoring.")
            trader.state = load_state()
        else:
            logger.info("Entry failed — will retry next entry day.")
            await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
