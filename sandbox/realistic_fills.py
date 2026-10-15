# sandbox/realistic_fills.py
"""
Opt-in fill realism for the sandbox (analyze mode) execution engines.

Enabled by sandbox_config key ``sandbox_realistic_fills`` = true (default off,
so behaviour is unchanged until it is switched on). Three pieces:

* adverse_fill      - MARKET fills at the far touch; when the quote has no real
                      depth (bid == ask == ltp, as on the WebSocket path) LTP
                      +/- a time-of-day slippage tier instead of the bare LTP.
* stop_fill         - a triggered SL-M fills at the trigger plus overshoot (or
                      the far touch if that is worse), never at a retraced LTP.
* StopWatchdog      - catches stops whose trigger was crossed between ticks or
                      polls, using the quote's day high/low, which only ever
                      widen: a stop is missed iff the extreme crossed the
                      trigger after the order was seen.

Slippage tiers mirror the research costs model (0.3% 09:15-09:45, 0.2%
15:00-15:40, 0.1% otherwise) and are overridable through config.
"""

from datetime import datetime, time as dtime
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict, Optional

import pytz

IST = pytz.timezone("Asia/Kolkata")
TICK = Decimal("0.05")

DEFAULT_TIERS = (
    (dtime(9, 15), dtime(9, 45), Decimal("0.003")),
    (dtime(15, 0), dtime(15, 40), Decimal("0.002")),
)
DEFAULT_RATE = Decimal("0.001")


def is_enabled() -> bool:
    from database.sandbox_db import get_config

    return str(get_config("sandbox_realistic_fills", "false")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def slippage_rate(ts: Optional[datetime] = None) -> Decimal:
    ts = ts or datetime.now(IST)
    t = ts.time()
    for start, end, rate in DEFAULT_TIERS:
        if start <= t < end:
            return rate
    return DEFAULT_RATE


def _round_tick(price: Decimal, up: bool) -> Decimal:
    """Round to the 0.05 tick in the adverse direction (up for BUY)."""
    steps = price / TICK
    steps = steps.to_integral_value(rounding="ROUND_CEILING" if up else "ROUND_FLOOR")
    return (steps * TICK).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _has_depth(bid: Decimal, ask: Decimal, ltp: Decimal) -> bool:
    return bid > 0 and ask > 0 and ask > bid


def adverse_fill(
    action: str, ltp: Decimal, bid: Decimal, ask: Decimal, ts: Optional[datetime] = None
) -> Decimal:
    """Market-order fill: far touch if depth is real, else LTP +/- slippage."""
    buy = action == "BUY"
    if _has_depth(bid, ask, ltp):
        return ask if buy else bid
    rate = slippage_rate(ts)
    px = ltp * (1 + rate) if buy else ltp * (1 - rate)
    return _round_tick(px, up=buy)


def stop_fill(
    action: str,
    trigger: Decimal,
    touch: Optional[Decimal] = None,
    ts: Optional[datetime] = None,
) -> Decimal:
    """
    Fill for a triggered SL-M. A BUY stop is hit at or above its trigger, a
    SELL stop at or below. Use the observed far touch if it is already beyond
    the trigger, otherwise the trigger plus slippage overshoot.
    """
    buy = action == "BUY"
    rate = slippage_rate(ts)
    overshoot = _round_tick(trigger * (1 + rate) if buy else trigger * (1 - rate), up=buy)
    if touch and touch > 0:
        if buy and touch > trigger:
            return max(touch, trigger)
        if not buy and touch < trigger:
            return min(touch, trigger)
    return overshoot


class StopWatchdog:
    """
    Detects stop triggers crossed between ticks/polls from day high/low.

    On the first sighting of an order the current day extreme is recorded as
    the baseline. Later, a BUY stop (trigger above) has been crossed iff the
    day high is now >= trigger and the baseline high was below it; a SELL stop
    mirrors this with the day low. If the baseline already beyond the trigger
    the extreme says nothing about timing, so the tick path stays in charge.
    """

    def __init__(self):
        self._baseline: Dict[str, Decimal] = {}

    def forget(self, orderid: str) -> None:
        self._baseline.pop(orderid, None)

    def crossed(self, orderid: str, action: str, trigger: Decimal, quote: dict) -> bool:
        key = "high" if action == "BUY" else "low"
        try:
            extreme = Decimal(str(quote.get(key) or 0))
        except Exception:
            return False
        if extreme <= 0:
            return False
        base = self._baseline.get(orderid)
        if base is None:
            self._baseline[orderid] = extreme
            return False
        if action == "BUY":
            return base < trigger <= extreme
        return base > trigger >= extreme
