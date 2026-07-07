"""
Study B Signal Engine — BB on ATM PE Option Price.

Detects when the ATM NIFTY PE 5-min bar closes ABOVE its own Bollinger Band
(20, 2.5σ).  The option price BB is computed on a rolling intraday series
that is pre-seeded with 5 prior trading days of history so the BB is valid
from the very first bar of the day.

Fires at most ONCE per day (first qualifying signal only).
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# ── Strategy parameters ───────────────────────────────────────────────────
BB_PERIOD    = 20
BB_MULT      = 2.5
ENTRY_WINDOW = (9, 15), (10, 30)   # 09:15 – 10:30 IST
PREMIUM_MIN  = 150.0               # option must be ≥ ₹150 at signal bar


class StudyBEngine:
    """
    Maintains a rolling 5-min close series for the ATM PE option price.
    Computes BB(20, 2.5σ) and reports the FIRST intraday signal each day.
    """

    def __init__(self):
        self.ohlc: pd.DataFrame = pd.DataFrame(columns=["high", "low", "close"])
        self.signal_fired_today: bool = False

    # ── History warm-up ───────────────────────────────────────────────────

    def load_history(self, bars_df: pd.DataFrame):
        """
        Pre-load ≥ BB_PERIOD bars of prior option price history so the BB
        is hot at the 09:15 bar.  bars_df must have columns high/low/close
        and a DatetimeIndex.
        """
        if bars_df.empty:
            logger.warning("[Study B] No option history to load — BB needs warmup bars first.")
            return
        self.ohlc = bars_df[["high", "low", "close"]].copy()
        logger.info(f"[Study B] Option history loaded: {len(self.ohlc)} bars  "
                    f"({self.ohlc.index[0]} → {self.ohlc.index[-1]})")

    # ── Tick update ───────────────────────────────────────────────────────

    def on_tick(self, ltp: float, ts) -> Optional[str]:
        """
        Process a new option price tick.
        Returns "SELL" on the first qualifying signal of the day, else None.
        ts: Unix timestamp (float) or datetime.
        """
        if self.signal_fired_today:
            return None   # one signal per day max

        dt     = _to_dt(ts)
        window = dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)

        # Update 5-min bar
        if window not in self.ohlc.index:
            new_row = pd.DataFrame(
                {"high": [ltp], "low": [ltp], "close": [ltp]}, index=[window]
            )
            self.ohlc = pd.concat([self.ohlc, new_row])
            if len(self.ohlc) > 250:
                self.ohlc = self.ohlc.iloc[-250:]
        else:
            self.ohlc.at[window, "high"]  = max(self.ohlc.at[window, "high"],  ltp)
            self.ohlc.at[window, "low"]   = min(self.ohlc.at[window, "low"],   ltp)
            self.ohlc.at[window, "close"] = ltp

        return self._evaluate_signal(ltp, dt)

    # ── BB calculation ────────────────────────────────────────────────────

    def calculate_bb(self) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        if len(self.ohlc) < BB_PERIOD:
            return None, None, None
        closes = self.ohlc["close"]
        sma    = closes.rolling(BB_PERIOD).mean().iloc[-1]
        std    = closes.rolling(BB_PERIOD).std().iloc[-1]
        return sma + BB_MULT * std, sma - BB_MULT * std, std

    # ── Signal evaluation ─────────────────────────────────────────────────

    def _evaluate_signal(self, ltp: float, dt: datetime) -> Optional[str]:
        if not self.in_entry_window(dt):
            return None

        upper, _, _ = self.calculate_bb()
        if upper is None:
            return None

        if ltp > upper and ltp >= PREMIUM_MIN:
            self.signal_fired_today = True
            logger.info(
                f"[Study B] 📈 SELL signal — option ₹{ltp:.2f} > BB upper ₹{upper:.2f}"
            )
            return "SELL"
        return None

    # ── Filters ───────────────────────────────────────────────────────────

    @staticmethod
    def in_entry_window(dt: datetime) -> bool:
        (h0, m0), (h1, m1) = ENTRY_WINDOW
        t = dt.hour * 60 + dt.minute
        return (h0 * 60 + m0) <= t <= (h1 * 60 + m1)

    def reset_daily(self):
        """Call at the start of each new trading session."""
        self.signal_fired_today = False
        # Keep ohlc history intact — it seeds the next day's BB warmup
        logger.info("[Study B] Daily state reset.")


# ── Utility ───────────────────────────────────────────────────────────────

def _to_dt(ts) -> pd.Timestamp:
    if isinstance(ts, (int, float)):
        return pd.to_datetime(ts, unit="s", utc=True).tz_convert("Asia/Kolkata").tz_localize(None)
    return pd.Timestamp(ts)
