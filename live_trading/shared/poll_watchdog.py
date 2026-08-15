"""
live_trading/shared/poll_watchdog.py
=====================================
Reusable stale-data watchdog for REST-poll-driven bots (no WebSocket).

TickWatchdog (see tick_watchdog.py) catches a WS feed that stays connected
but silently stops delivering ticks. REST-poll bots have no persistent
connection to watch, but they have the analogous failure mode: a poll call
that keeps returning HTTP 200 with a stale/cached quote (broker-side issue)
instead of raising, or a poll call that keeps raising/erroring without the
bot's own retry logic ever surfacing it. Both are silent from the bot's
own perspective unless something is watching for them.

Unlike TickWatchdog, there's no separate background loop needed — poll bots
already have their own periodic cadence (POLL_SECS-style loop). Call
`check()` once per symbol, right after each poll attempt, from inside that
existing loop.

Two independent alert conditions:
  - FROZEN VALUE : the polled value hasn't changed in `frozen_secs`, despite
    successful polls (broker/API silently serving a cached/stale quote).
  - POLL FAILURES: `failure_threshold` consecutive failed/empty polls for a
    symbol (REST call erroring or returning no usable price).
Each has its own recovery alert once the condition clears.

Usage (inside an existing REST-poll loop):

    from live_trading.shared.poll_watchdog import PollWatchdog

    self._watchdog = PollWatchdog(
        bot_name="NIFTY Iron Fly Weekly Bot",
        market_open=MARKET_OPEN,
        market_close=SESSION_END,
        bot_logger=logger,
    )

    # After each poll attempt for a symbol, success/value from that attempt:
    await self._watchdog.check(symbol, ltp, success=True)   # ltp is a float
    # or, on a failed/empty poll:
    await self._watchdog.check(symbol, None, success=False)
"""

from __future__ import annotations

import logging
from datetime import datetime, time as dt_time
from typing import Optional

from live_trading.shared.telegram_notifier import send_async

FROZEN_SECS         = 600   # alert if a value hasn't changed this long despite successful polls
FAILURE_THRESHOLD   = 3     # consecutive failed/empty polls before alerting
REALERT_SECS        = 300   # re-alert cadence while a condition persists


class PollWatchdog:
    def __init__(
        self,
        bot_name: str,
        market_open: dt_time,
        market_close: dt_time,
        bot_logger: Optional[logging.Logger] = None,
        frozen_secs: int = FROZEN_SECS,
        failure_threshold: int = FAILURE_THRESHOLD,
        realert_secs: int = REALERT_SECS,
    ):
        self.bot_name          = bot_name
        self.market_open       = market_open
        self.market_close      = market_close
        self._logger           = bot_logger or logging.getLogger(__name__)
        self.frozen_secs       = frozen_secs
        self.failure_threshold = failure_threshold
        self.realert_secs      = realert_secs

        self._last_value:        dict[str, float] = {}
        self._last_changed:      dict[str, datetime] = {}
        self._frozen_alerted:    dict[str, bool] = {}
        self._frozen_last_alert: dict[str, datetime] = {}

        self._consec_failures:   dict[str, int] = {}
        self._failure_alerted:   dict[str, bool] = {}
        self._failure_last_alert: dict[str, datetime] = {}

    async def check(self, symbol: str, value: float | None, success: bool) -> None:
        """Call once per symbol per poll attempt. Never raises."""
        if not symbol:
            return
        now = datetime.now()
        try:
            if not (self.market_open <= now.time() < self.market_close):
                return  # only watch during market hours — no polling expected otherwise

            if success and value is not None:
                await self._check_frozen(symbol, value, now)
                await self._clear_failures(symbol, now)
            else:
                await self._check_failure(symbol, now)
        except Exception:
            self._logger.exception(f"PollWatchdog check failed ({self.bot_name}/{symbol})")

    # ── frozen-value detection ──────────────────────────────────────────────

    async def _check_frozen(self, symbol: str, value: float, now: datetime) -> None:
        prev = self._last_value.get(symbol)
        if prev is None or value != prev:
            self._last_value[symbol]   = value
            self._last_changed[symbol] = now
            if self._frozen_alerted.get(symbol):
                self._frozen_alerted[symbol] = False
                self._logger.info(f"✅ Quote unfroze | {self.bot_name} | {symbol}")
                await send_async(f"✅ *Quote Recovered* — {self.bot_name}\n`{symbol}` is updating again.")
            return

        last_changed = self._last_changed.get(symbol, now)
        elapsed = (now - last_changed).total_seconds()
        if elapsed < self.frozen_secs:
            return

        last_alert = self._frozen_last_alert.get(symbol)
        already    = self._frozen_alerted.get(symbol, False)
        should_alert = (not already) or (
            last_alert is not None and (now - last_alert).total_seconds() >= self.realert_secs
        )
        if should_alert:
            self._frozen_alerted[symbol]    = True
            self._frozen_last_alert[symbol] = now
            mins = elapsed / 60
            self._logger.warning(
                f"🧊 FROZEN QUOTE | {self.bot_name} | {symbol} — unchanged (₹{value}) for {mins:.1f} min"
            )
            await send_async(
                f"🧊 *Frozen Quote* — {self.bot_name}\n"
                f"`{symbol}` has returned ₹{value} unchanged for {mins:.1f} min despite successful polls.\n"
                f"Broker/API may be serving a stale cache — check the bot."
            )

    # ── poll-failure detection ──────────────────────────────────────────────

    async def _check_failure(self, symbol: str, now: datetime) -> None:
        n = self._consec_failures.get(symbol, 0) + 1
        self._consec_failures[symbol] = n
        if n < self.failure_threshold:
            return

        last_alert = self._failure_last_alert.get(symbol)
        already    = self._failure_alerted.get(symbol, False)
        should_alert = (not already) or (
            last_alert is not None and (now - last_alert).total_seconds() >= self.realert_secs
        )
        if should_alert:
            self._failure_alerted[symbol]    = True
            self._failure_last_alert[symbol] = now
            self._logger.warning(
                f"💀 POLL FAILING | {self.bot_name} | {symbol} — {n} consecutive failed/empty polls"
            )
            await send_async(
                f"💀 *Poll Failing* — {self.bot_name}\n"
                f"`{symbol}` has failed or returned no price for {n} consecutive polls.\n"
                f"Check the bot's REST connectivity."
            )

    async def _clear_failures(self, symbol: str, now: datetime) -> None:
        self._consec_failures[symbol] = 0
        if self._failure_alerted.get(symbol):
            self._failure_alerted[symbol] = False
            self._logger.info(f"✅ Polling recovered | {self.bot_name} | {symbol}")
            await send_async(f"✅ *Polling Recovered* — {self.bot_name}\n`{symbol}` is responding again.")
