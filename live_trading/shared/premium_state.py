"""
Anchored-straddle premium state — shared helper for the F1/F2 premium filters.

Research source: options_data/research/pa_discovery_study (premium-momentum
family) + research/premium_filter_retrofit (per-bot evidence and addenda:
bb_deep_study/study_a_report/F2_ADDENDUM.md, macd_price_action_sr_study/
F1_ADDENDUM.md).

Definitions (must match the research exactly):
  Anchor    : at 09:20 IST, the ATM strike nearest the index spot, on the
              NEAREST expiry >= today (min_dte=0 — the research contract; the
              bot's own tradeable expiry may differ). The SAME CE+PE pair is
              used all day — never re-anchored.
  S0        : combined CE+PE close at the last 1-min bar <= 09:20.
  F2 (premium_up)  : current combined premium > S0.
  F1 (at_day_low)  : current combined premium is at its intraday low
                     (running min of 1-min combined closes since 09:15).
                     Only meaningful from 09:35 IST — before that the flag
                     returns False (no veto evidence; day extremes are
                     degenerate in the first bars).

Design: REST-only (index + 2 option 1-min history calls per evaluation).
No websocket subscriptions, no in-memory state that a restart could lose —
every evaluation reconstructs from history, so the filters survive bot
restarts by construction. Fail-open: any resolution/fetch failure returns
ok=False and the caller must fall back to unfiltered behavior.
"""

import logging
import os
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

from live_trading.api_utils import get_history                    # noqa: E402
from live_trading.shared.atm_resolver import (                    # noqa: E402
    get_atm_strike, get_weekly_expiry, INDEX_CONFIG,
)

logger = logging.getLogger(__name__)

API_KEY = os.getenv("OPENALGO_API_KEY")

INDEX_EXCHANGE = {
    "NIFTY": "NSE_INDEX", "BANKNIFTY": "NSE_INDEX", "FINNIFTY": "NSE_INDEX",
    "MIDCPNIFTY": "NSE_INDEX", "SENSEX": "BSE_INDEX", "BANKEX": "BSE_INDEX",
}

ANCHOR_HM = dt_time(9, 20)          # strike anchor: ATM at the 09:20 spot
# Research S0 = close of the 5-min bar STARTING at 09:20 (p6 convention:
# bar index <= anchor), i.e. the last 1-min close at 09:24. Verified against
# NIFTY_prem5m.parquet s0_anchor on 2026-04-15 (442.30).
S0_LAST_MIN = dt_time(9, 24)
READY_FROM = dt_time(9, 25)
F1_VALID_FROM = dt_time(9, 35)
IST_OFFSET = timedelta(hours=5, minutes=30)


def _rows_to_minutes(rows: list) -> dict:
    """history rows -> {IST wall-clock datetime: close} for TODAY only."""
    out = {}
    today = datetime.now().date()
    for r in rows or []:
        ts = r.get("timestamp")
        if ts is None:
            continue
        try:
            ts = float(ts)
            if ts > 1e12:          # milliseconds
                ts = ts / 1000.0
            # OpenAlgo history timestamps are UTC epoch → IST wall clock
            dt = (datetime.fromtimestamp(ts, tz=timezone.utc)
                  .replace(tzinfo=None) + IST_OFFSET)
        except (TypeError, ValueError):
            continue
        if dt.date() != today:
            continue
        try:
            out[dt.replace(second=0, microsecond=0)] = float(r["close"])
        except (KeyError, TypeError, ValueError):
            continue
    return out


class AnchoredStraddle:
    """Per-index, per-day anchored ATM straddle state (REST-reconstructed)."""

    def __init__(self, index: str, api_key: str | None = None):
        self.index = index.upper()
        self.api_key = api_key or API_KEY
        self._anchor_date = None      # date the cached anchor belongs to
        self._anchor = None           # dict(strike, expiry, ce, pe) or None

    # ── anchor resolution (cached per day) ────────────────────────────────
    def _resolve_anchor(self) -> dict | None:
        today = datetime.now().date()
        if self._anchor_date == today:
            return self._anchor
        self._anchor_date, self._anchor = today, None

        idx_exch = INDEX_EXCHANGE.get(self.index)
        if not idx_exch or self.index not in INDEX_CONFIG:
            logger.error(f"[premium_state] unknown index {self.index}")
            return None
        rows = get_history(self.api_key, self.index, idx_exch, "1m", 1)
        minutes = _rows_to_minutes(rows)
        # strike from the spot just before 09:20 (research anchors the STRIKE
        # at the 09:20 spot; S0 comes later, at the 09:20 5-min bar's close)
        anchor_ts = datetime.combine(today, ANCHOR_HM)
        eligible = {t: c for t, c in minutes.items() if t < anchor_ts}
        if not eligible:
            logger.warning(f"[premium_state] {self.index}: no index bars ≤ 09:20 yet")
            return None
        spot_0920 = eligible[max(eligible)]

        strike = get_atm_strike(spot_0920, self.index)
        expiry = get_weekly_expiry(self.api_key, min_dte=0, index=self.index)
        if not expiry:
            logger.warning(f"[premium_state] {self.index}: expiry resolution failed")
            return None
        # canonical symbol construction — sanctioned fallback per atm_resolver
        # docstring; avoids offset=ATM re-deriving a strike from CURRENT spot,
        # which would break the fixed-at-09:20 anchor definition.
        ce = f"{self.index}{expiry}{strike}CE"
        pe = f"{self.index}{expiry}{strike}PE"
        self._anchor = {"strike": strike, "expiry": expiry, "ce": ce, "pe": pe,
                        "spot_0920": spot_0920}
        logger.info(f"[premium_state] {self.index} anchor: {ce}/{pe} "
                    f"(spot@09:20={spot_0920:.1f})")
        return self._anchor

    # ── evaluation ────────────────────────────────────────────────────────
    def evaluate(self) -> dict:
        """
        Returns:
          ok=False, reason=...                       → caller must FAIL OPEN
          ok=True, f2_premium_up, f1_at_day_low, s0, s_now, run_min, anchor
        """
        now = datetime.now()
        if now.time() < READY_FROM:
            return {"ok": False, "reason": "before_anchor"}
        anchor = self._resolve_anchor()
        if not anchor:
            return {"ok": False, "reason": "anchor_resolution_failed"}

        opt_exch = INDEX_CONFIG[self.index]["exchange"]
        ce_min = _rows_to_minutes(get_history(self.api_key, anchor["ce"], opt_exch, "1m", 1))
        pe_min = _rows_to_minutes(get_history(self.api_key, anchor["pe"], opt_exch, "1m", 1))
        common = sorted(set(ce_min) & set(pe_min))
        if not common:
            return {"ok": False, "reason": "no_option_bars"}
        combined = {t: ce_min[t] + pe_min[t] for t in common}

        s0_ts = datetime.combine(now.date(), S0_LAST_MIN)
        pre = [t for t in common if t <= s0_ts]
        if not pre:
            return {"ok": False, "reason": "no_bars_before_anchor"}
        s0 = combined[max(pre)]
        s_now = combined[common[-1]]
        run_min = min(combined.values())

        f1 = (now.time() >= F1_VALID_FROM) and (s_now <= run_min * 1.001)
        return {
            "ok": True,
            "f2_premium_up": s_now > s0,
            "f1_at_day_low": f1,
            "s0": round(s0, 2), "s_now": round(s_now, 2),
            "run_min": round(run_min, 2),
            "anchor": f"{anchor['ce']}/{anchor['pe']}",
            "last_bar": common[-1].strftime("%H:%M"),
        }
