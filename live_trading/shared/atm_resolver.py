"""
ATM option resolver — shared, multi-index symbol resolution for option bots.

Wraps live_trading.api_utils to provide:
  - get_atm_strike(spot, index)   → nearest strike for that index's step
  - get_weekly_expiry(index, ...) → nearest weekly with ≥ min_dte days
  - resolve_atm_option(...)       → (symbol, strike, expiry_str, exchange)
  - get_option_ltp(...)           → current premium via quotes API

⚠️  READ BEFORE TOUCHING SYMBOL RESOLUTION ANYWHERE IN THIS REPO:
This module is the ONE correct, shared path for ATM/OTM option-symbol
resolution. It deliberately NEVER passes `strike_int` to `get_option_symbol()`
/ `/api/v1/optionsymbol` — that field means the STRIKE INTERVAL (e.g. 50, 100),
NOT an absolute strike. Passing a computed absolute strike (e.g. 23200) as
`strike_int` silently produces a bogus/404 symbol. This exact misuse caused the
recurring "ATM symbols unresolved" failure in flat_blue_line_monthly_bot.py
(2026-06-08) and appears in `strike_int` form across half a dozen bot files.
Resolution here works correctly by passing `offset="ATM"` + `underlying_ltp`
instead — let OpenAlgo derive the strike from the live spot. If you need an
index this module doesn't yet cover, ADD IT TO `INDEX_CONFIG` below rather than
hand-rolling resolution in your bot file. If `get_option_symbol()` ever fails
you, the safe fallback is direct canonical-symbol string construction:
    f"{underlying}{expiry_str}{strike}{opt_type}"   e.g. "BANKNIFTY30JUN2654200CE"
— the symtoken master-contract mapping translates this to broker format
transparently, and it's what flat_blue_line_monthly_bot now uses directly.
"""

import logging
import os
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

from live_trading.api_utils import get_expiry_dates, get_option_symbol

API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")

logger = logging.getLogger(__name__)

# ── Per-index config: strike step + options exchange ─────────────────────────
# Add new indices here — never hardcode a strike step inline in a bot file.
INDEX_CONFIG = {
    "NIFTY":     {"strike_step": 50,  "exchange": "NFO"},
    "BANKNIFTY": {"strike_step": 100, "exchange": "NFO"},
    "FINNIFTY":  {"strike_step": 50,  "exchange": "NFO"},
    "MIDCPNIFTY":{"strike_step": 25,  "exchange": "NFO"},
    "SENSEX":    {"strike_step": 100, "exchange": "BFO"},
    "BANKEX":    {"strike_step": 100, "exchange": "BFO"},
}

# Back-compat aliases (this module was originally NIFTY-only)
NIFTY_OPT_EXCHANGE = INDEX_CONFIG["NIFTY"]["exchange"]
NIFTY_STRIKE_STEP  = INDEX_CONFIG["NIFTY"]["strike_step"]


def _cfg(index: str) -> dict:
    cfg = INDEX_CONFIG.get(index.upper())
    if not cfg:
        raise ValueError(
            f"Unknown index '{index}' — add it to INDEX_CONFIG in "
            f"live_trading/shared/atm_resolver.py (strike_step + exchange) "
            f"rather than hand-rolling resolution in your bot."
        )
    return cfg


def get_atm_strike(spot: float, index: str = "NIFTY") -> int:
    """Round spot to the nearest valid strike for `index` (per its strike step)."""
    step = _cfg(index)["strike_step"]
    return int(round(spot / step) * step)


def get_weekly_expiry(api_key: str = None, min_dte: int = 2, index: str = "NIFTY") -> str | None:
    """
    Return the nearest weekly expiry for `index` with at least min_dte days remaining.
    Returns expiry in DDMMMYY format (e.g. "27MAR26") as used by OpenAlgo.
    """
    key      = api_key or API_KEY
    cfg      = _cfg(index)
    exchange = cfg["exchange"]
    dates    = get_expiry_dates(key, index.upper(), exchange, "options")
    if not dates:
        logger.error(f"Could not fetch {index.upper()} expiry dates.")
        return None

    today = datetime.now().date()
    for d in dates:
        try:
            exp_dt = datetime.strptime(d, "%d%b%y").date()
            if (exp_dt - today).days >= min_dte:
                return d
        except ValueError:
            continue

    logger.error(f"No expiry found with ≥{min_dte} DTE. Dates available: {dates[:5]}")
    return None


def resolve_atm_option(spot: float,
                       opt_type: str = "PE",
                       api_key: str = None,
                       min_dte: int = 2,
                       index: str = "NIFTY") -> dict | None:
    """
    Resolve the full ATM option details for today's session, for any index in
    INDEX_CONFIG. Defaults to NIFTY for backward compatibility with existing callers.

    Returns dict:
      {
        "symbol":   "NIFTY26MAR24000PE",
        "strike":   24000,
        "expiry":   "27MAR26",
        "exchange": "NFO",
        "opt_type": "PE"
      }
    or None on failure.
    """
    key      = api_key or API_KEY
    cfg      = _cfg(index)
    exchange = cfg["exchange"]
    strike   = get_atm_strike(spot, index)
    expiry   = get_weekly_expiry(key, min_dte, index)
    if not expiry:
        return None

    # NOTE: deliberately NOT passing strike_int here — see module docstring.
    # offset="ATM" + underlying_ltp lets OpenAlgo derive the strike correctly.
    symbol = get_option_symbol(
        api_key=key,
        underlying=index.upper(),
        exchange=exchange,
        expiry=expiry,
        option_type=opt_type,
        offset="ATM",
        underlying_ltp=spot,
    )
    if not symbol:
        logger.error(f"Could not resolve ATM {opt_type} symbol for {index.upper()} (spot={spot}, expiry={expiry})")
        return None

    logger.info(f"ATM resolved → {symbol}  (strike≈{strike}, expiry={expiry})")
    return {
        "symbol":   symbol,
        "strike":   strike,
        "expiry":   expiry,
        "exchange": exchange,
        "opt_type": opt_type,
    }


import threading
import time

# Global cache for LTPs to prevent 429 errors from rapid-fire REST calls
_ltp_cache = {}  # symbol -> (ltp, timestamp)
_ltp_lock = threading.Lock()
LTP_CACHE_EXPIRY = 1.0  # seconds


def get_option_ltp(symbol: str, exchange: str = "NFO",
                   api_key: str = None, _retry: bool = True) -> float:
    """
    Fetch current LTP for an option symbol via OpenAlgo quotes API.
    Includes a 1-second cache to prevent 429 rate-limiting from multiple bots.

    On failure (timeout, non-200, bad payload), logs the specific cause —
    previously every failure path fell through to a silent `return 0.0`,
    which is why the 2026-05-29 continuous-timeout outage went unnoticed
    for ~4.5h beyond a generic "Error fetching LTP" line. Retries once
    (immediately, no backoff) since brief read-timeout blips are common
    and a single retry is cheap relative to the caller's 30s poll interval;
    it does NOT help with sustained outages — callers must track consecutive
    failures themselves for that.
    """
    now = time.time()
    with _ltp_lock:
        if symbol in _ltp_cache:
            val, ts = _ltp_cache[symbol]
            if now - ts < LTP_CACHE_EXPIRY:
                return val

    key = api_key or API_KEY
    try:
        res = requests.post(
            f"{HOST}/api/v1/quotes",
            json={"apikey": key, "symbol": symbol, "exchange": exchange},
            timeout=5,
        )
        if res.status_code == 200:
            data = res.json()
            if data.get("status") == "success":
                qd = data.get("data", {})
                ltp = 0.0
                if isinstance(qd, dict):
                    ltp = float(qd.get("ltp", 0) or 0)
                elif isinstance(qd, list) and qd:
                    ltp = float(qd[0].get("ltp", 0) or 0)

                if ltp > 0:
                    with _ltp_lock:
                        _ltp_cache[symbol] = (ltp, now)
                    return ltp
                logger.warning(f"LTP for {symbol}: quotes API returned no usable price ({qd!r}).")
            else:
                logger.warning(f"LTP for {symbol}: quotes API status="
                               f"{data.get('status')!r} msg={data.get('message')!r}")
        else:
            logger.warning(f"LTP for {symbol}: quotes API HTTP {res.status_code}")
    except requests.exceptions.Timeout:
        logger.error(f"LTP for {symbol}: quotes API timed out (5s).")
    except Exception as e:
        logger.error(f"Error fetching LTP for {symbol}: {e}")

    if _retry:
        return get_option_ltp(symbol, exchange, api_key, _retry=False)
    return 0.0
