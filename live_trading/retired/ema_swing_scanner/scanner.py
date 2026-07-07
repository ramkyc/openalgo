"""
scanner.py — Signal detection for the EMA Swing Scanner

Checks each stock in the universe against the V3 strategy rules:
  1. Regime filter : NIFTY50-INDEX Close > 50 EMA
  2. Trend         : Stock Close > 50 EMA
  3. Pullback      : Stock Low  <= 20 EMA
  4. RSI filter    : 35 <= RSI(14) <= 60
  5. ATR valid     : ATR(14) > 0

Returns a list of SignalResult objects — one per stock that fired today.
"""

from __future__ import annotations

import math
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── Universe & tiers ──────────────────────────────────────────────────────────
UNIVERSE = [
    # Tier 1 — 1.5× risk  (7 stocks)
    "LT", "SHRIRAMFIN", "BEL", "M&M", "BHARTIARTL", "HCLTECH", "SBILIFE",
    # Tier 2 — 1.0× risk  (8 stocks)
    "RELIANCE", "SBIN", "TECHM", "GRASIM", "COALINDIA", "TITAN", "ADANIPORTS", "TATASTEEL",
    # Tier 3 — 0.75× risk (4 stocks)
    "ICICIBANK", "AXISBANK", "NTPC", "HINDALCO",
]

TIER1 = {"LT", "SHRIRAMFIN", "BEL", "M&M", "BHARTIARTL", "HCLTECH", "SBILIFE"}
TIER2 = {"RELIANCE", "SBIN", "TECHM", "GRASIM", "COALINDIA", "TITAN", "ADANIPORTS", "TATASTEEL"}
TIER3 = {"ICICIBANK", "AXISBANK", "NTPC", "HINDALCO"}

EXCHANGE       = "NSE"
INDEX_SYMBOL   = "NIFTY"
INDEX_EXCHANGE = "NSE_INDEX"

# ── Strategy parameters ───────────────────────────────────────────────────────
EMA_FAST   = 20
EMA_SLOW   = 50
RSI_PERIOD = 14
ATR_PERIOD = 14
RSI_LOW    = 35
RSI_HIGH   = 60
LOOKBACK   = 120   # bars to fetch for indicator warmup


# ── Dataclass for a signal ────────────────────────────────────────────────────
@dataclass
class SignalResult:
    symbol:    str
    exchange:  str
    tier:      str
    date:      date
    close:     float
    low:       float
    ema_fast:  float
    ema_slow:  float
    rsi:       float
    atr:       float
    # Computed entry parameters
    suggested_stop:     float = 0.0   # close - 1×ATR (indicative; real stop from tomorrow's open)
    suggested_hard_cap: float = 0.0   # close + 3×ATR


# ── Indicator helpers ─────────────────────────────────────────────────────────
def _ema(s: pd.Series, p: int) -> pd.Series:
    return s.ewm(span=p, adjust=False).mean()

def _rsi(s: pd.Series, p: int = 14) -> pd.Series:
    d  = s.diff()
    ag = d.clip(lower=0).ewm(com=p - 1, adjust=False).mean()
    al = (-d.clip(upper=0)).ewm(com=p - 1, adjust=False).mean()
    return 100 - (100 / (1 + ag / al.replace(0, np.nan)))

def _atr(df: pd.DataFrame, p: int = 14) -> pd.Series:
    h, l, pc = df["high"], df["low"], df["close"].shift(1)
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(com=p - 1, adjust=False).mean()


# ── Data fetching ─────────────────────────────────────────────────────────────
def _fetch_daily(symbol: str, exchange: str, api_key: str, host: str,
                 lookback: int = LOOKBACK) -> pd.DataFrame:
    """Fetch recent daily OHLCV from OpenAlgo history API."""
    try:
        from datetime import datetime, timedelta
        end   = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=lookback * 2)).strftime("%Y-%m-%d")

        r = requests.post(
            f"{host}/api/v1/history",
            json={"apikey": api_key, "symbol": symbol, "exchange": exchange,
                  "interval": "D", "start_date": start, "end_date": end},
            timeout=15,
        )
        data = r.json()
        if data.get("status") != "success" or not data.get("data"):
            logger.warning(f"No data for {symbol}:{exchange}")
            return pd.DataFrame()

        df = pd.DataFrame(data["data"])
        for col in ("open", "high", "low", "close", "volume"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df["date"] = pd.to_datetime(df.get("timestamp", df.get("date")),
                                    unit="s" if "timestamp" in df.columns else None).dt.date
        return df.dropna(subset=["close"]).reset_index(drop=True)
    except Exception as e:
        logger.error(f"Fetch error {symbol}: {e}")
        return pd.DataFrame()


# ── Main scan function ────────────────────────────────────────────────────────
def run_scan(api_key: str, host: str) -> tuple[bool, Optional[dict], list[SignalResult]]:
    """
    Run the full daily scan.

    Returns:
        (regime_active, index_info, signals)
        regime_active — True if NIFTY50 index is above its 50 EMA
        index_info    — dict with index close / ema values
        signals       — list of SignalResult for stocks that fired today
    """
    # ── 1. Regime check ───────────────────────────────────────────────────
    idx_df = _fetch_daily(INDEX_SYMBOL, INDEX_EXCHANGE, api_key, host)
    if idx_df.empty or len(idx_df) < EMA_SLOW + 5:
        logger.error("Could not fetch NIFTY50 index data for regime check.")
        return False, None, []

    idx_df["ema_slow"] = _ema(idx_df["close"], EMA_SLOW)
    last_idx = idx_df.iloc[-1]
    regime_active = float(last_idx["close"]) > float(last_idx["ema_slow"])

    index_info = {
        "symbol":     INDEX_SYMBOL,
        "date":       str(last_idx["date"]),
        "close":      round(float(last_idx["close"]), 2),
        "ema50":      round(float(last_idx["ema_slow"]), 2),
        "regime_on":  regime_active,
    }

    logger.info(
        f"Regime: {'✅ ON' if regime_active else '❌ OFF'} | "
        f"NIFTY50 {index_info['close']} vs EMA50 {index_info['ema50']}"
    )

    if not regime_active:
        logger.info("Regime OFF — no new entries permitted today.")
        return False, index_info, []

    # ── 2. Stock scans ────────────────────────────────────────────────────
    signals: list[SignalResult] = []
    warmup = max(EMA_SLOW, RSI_PERIOD, ATR_PERIOD) + 5

    for symbol in UNIVERSE:
        df = _fetch_daily(symbol, EXCHANGE, api_key, host)
        if df.empty or len(df) < warmup:
            logger.warning(f"{symbol}: insufficient data ({len(df)} bars)")
            continue

        df["ema_fast"] = _ema(df["close"], EMA_FAST)
        df["ema_slow"] = _ema(df["close"], EMA_SLOW)
        df["rsi"]      = _rsi(df["close"], RSI_PERIOD)
        df["atr"]      = _atr(df, ATR_PERIOD)

        row = df.iloc[-1]
        close    = float(row["close"])
        low      = float(row["low"])
        ema_fast = float(row["ema_fast"])
        ema_slow = float(row["ema_slow"])
        rsi_val  = float(row["rsi"])
        atr_val  = float(row["atr"])

        uptrend   = close > ema_slow
        ema_touch = low   <= ema_fast
        rsi_ok    = RSI_LOW <= rsi_val <= RSI_HIGH
        atr_valid = atr_val > 0 and not math.isnan(atr_val)

        if uptrend and ema_touch and rsi_ok and atr_valid:
            tier = "tier1" if symbol in TIER1 else ("tier2" if symbol in TIER2 else "tier3")
            sig = SignalResult(
                symbol=symbol, exchange=EXCHANGE, tier=tier,
                date=row["date"], close=round(close, 2), low=round(low, 2),
                ema_fast=round(ema_fast, 2), ema_slow=round(ema_slow, 2),
                rsi=round(rsi_val, 2), atr=round(atr_val, 2),
                suggested_stop=round(close - atr_val, 2),
                suggested_hard_cap=round(close + 3 * atr_val, 2),
            )
            signals.append(sig)
            logger.info(
                f"  🎯 SIGNAL: {symbol} | Close {close:.2f} | EMA20 {ema_fast:.2f} | "
                f"RSI {rsi_val:.1f} | ATR {atr_val:.2f} | Tier: {tier}"
            )
        else:
            reasons = []
            if not uptrend:   reasons.append("no uptrend")
            if not ema_touch: reasons.append("no EMA touch")
            if not rsi_ok:    reasons.append(f"RSI {rsi_val:.1f} out of range")
            logger.debug(f"  {symbol}: no signal ({', '.join(reasons)})")

    logger.info(f"Scan complete — {len(signals)} signal(s) found.")
    return regime_active, index_info, signals
