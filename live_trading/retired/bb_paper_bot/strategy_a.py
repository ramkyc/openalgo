"""
Study A Signal Engine — BB on NIFTY Index Price.

Detects when the NIFTY 5-min bar closes ABOVE its Bollinger Band (30, 3σ).
Signal fires only on the state transition None → OVERBOUGHT (not repeatedly).
Hysteresis of 1.0 × std prevents rapid flickering at the band boundary.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# ── Strategy parameters ───────────────────────────────────────────────────
BB_PERIOD     = 30
BB_MULT       = 3.0
BB_HYSTERESIS = 1.0      # price must pull back 1.0 × std below band to clear
ENTRY_WINDOW  = (9, 15), (10, 30)   # 09:15 – 10:30 IST
PREMIUM_MIN   = 150.0    # ATM PE must be ≥ ₹150 to enter


class StudyAEngine:
    """
    Maintains a rolling 5-minute OHLC series for the NIFTY index.
    Computes BB(30, 3σ) on each bar close and reports NEW OVERBOUGHT signals.
    """

    def __init__(self):
        self.ohlc: pd.DataFrame = pd.DataFrame(columns=["high", "low", "close"])
        self.signal_state: Optional[str] = None   # None | "OVERBOUGHT"
        self._current_window: Optional[pd.Timestamp] = None

    # ── History warm-up ───────────────────────────────────────────────────

    def load_history(self, bars_df: pd.DataFrame):
        """
        Pre-load historical 5-min OHLC (must have columns: high, low, close
        with a DatetimeIndex).  Called once at bot startup.
        """
        if bars_df.empty:
            logger.warning("[Study A] No history to load — BB will warm up on live ticks.")
            return
        self.ohlc = bars_df[["high", "low", "close"]].copy()
        logger.info(f"[Study A] History loaded: {len(self.ohlc)} bars  "
                    f"({self.ohlc.index[0]} → {self.ohlc.index[-1]})")

    # ── Tick update ───────────────────────────────────────────────────────

    def on_tick(self, ltp: float, ts) -> Optional[str]:
        """
        Process a new index tick.
        Returns "OVERBOUGHT" on a fresh signal, or None otherwise.
        ts: Unix timestamp (float) or datetime.
        """
        dt = _to_dt(ts)
        window = dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)

        # Update 5-min bar
        if window not in self.ohlc.index:
            # New bar — we have a completed previous bar; now open the new one
            new_row = pd.DataFrame(
                {"high": [ltp], "low": [ltp], "close": [ltp]}, index=[window]
            )
            self.ohlc = pd.concat([self.ohlc, new_row])
            # Keep memory bounded: 200 bars is plenty for BB(30)
            if len(self.ohlc) > 200:
                self.ohlc = self.ohlc.iloc[-200:]
        else:
            self.ohlc.at[window, "high"]  = max(self.ohlc.at[window, "high"],  ltp)
            self.ohlc.at[window, "low"]   = min(self.ohlc.at[window, "low"],   ltp)
            self.ohlc.at[window, "close"] = ltp

        self._current_window = window
        return self._evaluate_signal(ltp)

    # ── BB calculation ────────────────────────────────────────────────────

    def calculate_bb(self) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Returns (upper, lower, std) or (None, None, None) if not enough data."""
        if len(self.ohlc) < BB_PERIOD:
            return None, None, None
        closes = self.ohlc["close"]
        sma    = closes.rolling(BB_PERIOD).mean().iloc[-1]
        std    = closes.rolling(BB_PERIOD).std().iloc[-1]
        return sma + BB_MULT * std, sma - BB_MULT * std, std

    # ── Signal evaluation ─────────────────────────────────────────────────

    def _evaluate_signal(self, ltp: float) -> Optional[str]:
        upper, lower, std = self.calculate_bb()
        if upper is None:
            return None

        prev = self.signal_state

        # Determine new state with hysteresis
        if ltp > upper:
            new_state = "OVERBOUGHT"
        elif prev == "OVERBOUGHT":
            new_state = "OVERBOUGHT" if ltp >= (upper - BB_HYSTERESIS * std) else None
        else:
            new_state = None

        # Fire only on state TRANSITION None → OVERBOUGHT
        if new_state != prev:
            self.signal_state = new_state
            if new_state == "OVERBOUGHT":
                logger.info(f"[Study A] 🚨 OVERBOUGHT signal — NIFTY {ltp:.2f} > upper BB {upper:.2f}")
                return "OVERBOUGHT"

        return None

    # ── Filters ───────────────────────────────────────────────────────────

    @staticmethod
    def in_entry_window(dt: datetime) -> bool:
        """True if dt is within 09:15–10:30 IST."""
        (h0, m0), (h1, m1) = ENTRY_WINDOW
        t = dt.hour * 60 + dt.minute
        return (h0 * 60 + m0) <= t <= (h1 * 60 + m1)

    @staticmethod
    def premium_ok(ltp: float) -> bool:
        return ltp >= PREMIUM_MIN

    def reset_daily(self):
        """Call at the start of each session to reset intraday signal state."""
        self.signal_state = None
        logger.info("[Study A] Daily state reset.")


# ── Utility ───────────────────────────────────────────────────────────────

def _to_dt(ts) -> pd.Timestamp:
    if isinstance(ts, (int, float)):
        return pd.to_datetime(ts, unit="s", utc=True).tz_convert("Asia/Kolkata").tz_localize(None)
    return pd.Timestamp(ts)
