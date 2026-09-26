"""
live_trading/shared/tick_watchdog.py
=====================================
Reusable dead-feed / stale-tick watchdog for WS-driven bots.

A reconnect-on-exception loop (`websockets.ConnectionClosed`, etc.) only
catches a socket that actually errors out. It does nothing for a socket
that stays open but silently stops delivering ticks — a dropped server-side
subscription, a stuck proxy — because nothing raises. This class catches
that second case: it tracks a last-seen timestamp per symbol and, on a
periodic check during market hours, alerts via Telegram if a tracked
symbol has gone quiet too long, then alerts again on recovery.

Ported from the heartbeat + dead-feed pattern proven in fyers_cs's
banknifty_bb_options_bot.py / htf_po3_bot.py (2026-07-08 incident: a bot
ran blind for 4.5 hours because nothing watched whether ticks were
arriving at all). Packaged here as a shared class rather than copy-pasted
per bot, since it's rolling out across the whole bot fleet.

Alerting alone doesn't recover anything — a human still has to notice the
Telegram message and restart the bot. On 2026-07-29 the same silent-drop
pattern recurred (banknifty_bb_opening_candle_bot, 09:16-14:55, ~4h39min)
purely because the WS *connection* never errored, only the *data* stopped,
so the bot's own reconnect-triggered resubscribe never ran. Pass
`on_dead_feed` so the watchdog can drive that same resubscribe path itself
instead of just paging a human to do it.

Usage (in any WS-driven bot):

    from live_trading.shared.tick_watchdog import TickWatchdog

    self._watchdog = TickWatchdog(
        bot_name="NIFTY EOD Hold Bot",
        tracked_symbols=lambda: [IDX_SYMBOL] +
            ([self.active_trade["symbol"]] if self.active_trade else []),
        market_open=MARKET_OPEN,
        market_close=EOD_EXIT,
        bot_logger=logger,
        on_dead_feed=self._resubscribe_all,   # optional: active recovery, not just alerting
    )

    # In the WS message handler, before any symbol-routing `return`:
    self._watchdog.on_tick(symbol)

    # In run(), alongside the WS loop (asyncio.gather or create_task):
    asyncio.create_task(self._watchdog.watch_loop())

`tracked_symbols` is a callable (not a static list) so it can reflect
symbols that only exist once a position is open — an option leg added
after entry is watched the same as the index from the moment it appears.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time as dt_time
from typing import Awaitable, Callable, Iterable

from live_trading.shared.telegram_notifier import send_async

POLL_SECS               = 15    # how often watch_loop re-checks
DEAD_FEED_SECS          = 300   # alert if a tracked symbol is silent this long
DEAD_FEED_REALERT_SECS  = 300   # re-alert cadence while still dead
RECOVERY_FRESH_SECS     = 30    # tick must be this fresh to count as "recovered"


class TickWatchdog:
    def __init__(
        self,
        bot_name: str,
        tracked_symbols: Callable[[], Iterable[str]],
        market_open: dt_time,
        market_close: dt_time,
        bot_logger: logging.Logger | None = None,
        dead_feed_secs: int = DEAD_FEED_SECS,
        realert_secs: int = DEAD_FEED_REALERT_SECS,
        on_dead_feed: Callable[[], Awaitable[None]] | None = None,
    ):
        self.bot_name        = bot_name
        self._tracked_symbols = tracked_symbols
        self.market_open     = market_open
        self.market_close    = market_close
        self._logger         = bot_logger or logging.getLogger(__name__)
        self.dead_feed_secs  = dead_feed_secs
        self.realert_secs    = realert_secs
        self._on_dead_feed   = on_dead_feed

        self._last_tick:     dict[str, datetime] = {}
        self._first_tracked: dict[str, datetime] = {}
        self._dead_alerted:  dict[str, bool] = {}
        self._dead_last_alert: dict[str, datetime] = {}

    def on_tick(self, symbol: str) -> None:
        """Call for every routed tick, regardless of symbol type. Never raises."""
        if not symbol:
            return
        self._last_tick[symbol] = datetime.now()

    async def watch_loop(self) -> None:
        """Long-running loop — add via asyncio.gather()/create_task() next to the WS loop."""
        while True:
            try:
                await self._check_once()
            except Exception:
                self._logger.exception(f"TickWatchdog check failed ({self.bot_name})")
            await asyncio.sleep(POLL_SECS)

    async def _check_once(self) -> None:
        now = datetime.now()
        if not (self.market_open <= now.time() < self.market_close):
            return  # only watch during market hours — no ticks expected otherwise

        should_recover = False

        for sym in self._tracked_symbols():
            if sym not in self._first_tracked:
                self._first_tracked[sym] = now
            reference = self._last_tick.get(sym, self._first_tracked[sym])
            elapsed = (now - reference).total_seconds()

            if elapsed >= self.dead_feed_secs:
                last_alert = self._dead_last_alert.get(sym)
                already    = self._dead_alerted.get(sym, False)
                should_alert = (not already) or (
                    last_alert is not None
                    and (now - last_alert).total_seconds() >= self.realert_secs
                )
                if should_alert:
                    should_recover = True
                    self._dead_alerted[sym]     = True
                    self._dead_last_alert[sym]  = now
                    mins = elapsed / 60
                    self._logger.warning(
                        f"💀 DEAD FEED | {self.bot_name} | {sym} — no tick for {mins:.1f} min"
                    )
                    await send_async(
                        f"💀 *Dead Feed* — {self.bot_name}\n"
                        f"No tick for `{sym}` in {mins:.1f} min during market hours.\n"
                        f"Subscription may have silently dropped — check the bot."
                    )
            elif self._dead_alerted.get(sym) and elapsed < RECOVERY_FRESH_SECS:
                self._dead_alerted[sym] = False
                self._logger.info(f"✅ Feed recovered | {self.bot_name} | {sym}")
                await send_async(f"✅ *Feed Recovered* — {self.bot_name}\n`{sym}` is ticking again.")

        # Active recovery: the connection itself never errors in this failure
        # mode (see module docstring), so the bot's normal reconnect-triggered
        # resubscribe never runs on its own. Drive it here instead of only
        # alerting a human. Gated on should_alert (not just "still dead") so
        # it fires on the same cadence as the Telegram alert/realert — once on
        # initial detection, then once per realert_secs while still dead —
        # instead of hammering the WS proxy with a resubscribe every 15s.
        if should_recover and self._on_dead_feed is not None:
            try:
                await self._on_dead_feed()
            except Exception:
                self._logger.exception(f"TickWatchdog on_dead_feed callback failed ({self.bot_name})")
