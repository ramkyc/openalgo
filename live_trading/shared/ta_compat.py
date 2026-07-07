"""
ta_compat.py — pandas_ta drop-in replacement (pure pandas / numpy)
===================================================================
Implements the four indicators used by the live bots:

    ta.ema(close, length)                            → pd.Series
    ta.rsi(close, length)                            → pd.Series
    ta.adx(high, low, close, length)                 → pd.DataFrame  (ADX_n, DMP_n, DMN_n)
    ta.macd(close, fast, slow, signal)               → pd.DataFrame  (MACD_f_s_sig,
                                                                        MACDh_f_s_sig,
                                                                        MACDs_f_s_sig)

Column names are intentionally identical to pandas_ta output so the bots need
only a one-line import change:

    # OLD:
    import pandas_ta as ta

    # NEW:
    from live_trading.shared import ta_compat as ta

All smoothing uses Wilder's EWM  (alpha = 1/length, adjust=False) to match
pandas_ta's default behaviour.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _wilder_ewm(series: pd.Series, length: int) -> pd.Series:
    """Wilder's smoothed moving average (alpha = 1/length)."""
    return series.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()


# ──────────────────────────────────────────────────────────────
# EMA
# ──────────────────────────────────────────────────────────────

def ema(close: pd.Series, length: int = 20, **_) -> pd.Series:
    """Exponential Moving Average.

    Returns a Series named  ``EMA_{length}``.
    """
    result = close.ewm(span=length, adjust=False, min_periods=length).mean()
    result.name = f"EMA_{length}"
    return result


# ──────────────────────────────────────────────────────────────
# RSI
# ──────────────────────────────────────────────────────────────

def rsi(close: pd.Series, length: int = 14, **_) -> pd.Series:
    """Relative Strength Index (Wilder smoothing).

    Returns a Series named ``RSI_{length}``.
    """
    delta = close.diff()
    gain  = delta.clip(lower=0.0)
    loss  = (-delta).clip(lower=0.0)

    avg_gain = _wilder_ewm(gain, length)
    avg_loss = _wilder_ewm(loss, length)

    rs     = avg_gain / avg_loss.replace(0, np.nan)
    result = 100.0 - (100.0 / (1.0 + rs))
    result.name = f"RSI_{length}"
    return result


# ──────────────────────────────────────────────────────────────
# MACD
# ──────────────────────────────────────────────────────────────

def macd(
    close:  pd.Series,
    fast:   int = 12,
    slow:   int = 26,
    signal: int = 9,
    **_,
) -> pd.DataFrame:
    """MACD (Moving Average Convergence / Divergence).

    Returns a DataFrame with three columns that match pandas_ta naming:

        MACD_{fast}_{slow}_{signal}   — MACD line
        MACDh_{fast}_{slow}_{signal}  — histogram (MACD − signal)
        MACDs_{fast}_{slow}_{signal}  — signal line
    """
    prefix = f"MACD_{fast}_{slow}_{signal}"

    ema_fast   = close.ewm(span=fast,   adjust=False, min_periods=fast).mean()
    ema_slow   = close.ewm(span=slow,   adjust=False, min_periods=slow).mean()
    macd_line  = ema_fast - ema_slow
    sig_line   = macd_line.ewm(span=signal, adjust=False, min_periods=signal).mean()
    hist       = macd_line - sig_line

    return pd.DataFrame(
        {
            f"MACD_{fast}_{slow}_{signal}":  macd_line,
            f"MACDh_{fast}_{slow}_{signal}": hist,
            f"MACDs_{fast}_{slow}_{signal}": sig_line,
        },
        index=close.index,
    )


# ──────────────────────────────────────────────────────────────
# ADX
# ──────────────────────────────────────────────────────────────

def adx(
    high:   pd.Series,
    low:    pd.Series,
    close:  pd.Series,
    length: int = 14,
    **_,
) -> pd.DataFrame:
    """Average Directional Index (Wilder smoothing).

    Returns a DataFrame with three columns that match pandas_ta naming:

        ADX_{length}   — ADX value
        DMP_{length}   — +DI  (Plus Directional Indicator)
        DMN_{length}   — −DI  (Minus Directional Indicator)
    """
    # ── True Range ──────────────────────────────────────────
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # ── Directional Movement ────────────────────────────────
    up_move   =  high.diff()
    down_move = -low.diff()

    plus_dm  = pd.Series(
        np.where((up_move > down_move) & (up_move > 0),   up_move,   0.0),
        index=close.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=close.index,
    )

    # ── Wilder smoothing ────────────────────────────────────
    atr       = _wilder_ewm(tr,       length)
    plus_di   = 100.0 * _wilder_ewm(plus_dm,  length) / atr
    minus_di  = 100.0 * _wilder_ewm(minus_dm, length) / atr

    # ── DX → ADX ────────────────────────────────────────────
    di_sum  = (plus_di + minus_di).replace(0, np.nan)
    dx      = 100.0 * (plus_di - minus_di).abs() / di_sum
    adx_val = _wilder_ewm(dx, length)

    return pd.DataFrame(
        {
            f"ADX_{length}": adx_val,
            f"DMP_{length}": plus_di,
            f"DMN_{length}": minus_di,
        },
        index=close.index,
    )
