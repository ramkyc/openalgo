import asyncio
import re
import requests
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

import os
from pathlib import Path
from typing import Any
from dotenv import load_dotenv

# Ensure we load from the project root .env
project_root = Path(__file__).parent.parent
load_dotenv(project_root / ".env")

HOST = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")

# Retry schedule for transient OpenAlgo failures (connection refused during a
# restart, timeouts, 5xx). Total worst-case added latency ≈ 17s. 4xx responses
# are returned to the caller untouched — retrying a bad request or bad token
# would not help. Incident 2026-07-07: a brief OpenAlgo outage at session init
# silently disabled seven bots for the whole day.
_RETRY_DELAYS = (2, 5, 10)


def _post_with_retry(url, payload, timeout):
    """POST that retries connection errors, timeouts, and 5xx with backoff.

    Returns the Response (any status < 500). Raises the last error if all
    attempts fail.
    """
    import time as _time
    last_exc = None
    for attempt in range(len(_RETRY_DELAYS) + 1):
        try:
            response = requests.post(url, json=payload, timeout=timeout)
            if response.status_code >= 500:
                raise requests.RequestException(
                    f"HTTP {response.status_code}: {response.text[:200]}"
                )
            return response
        except requests.RequestException as e:
            last_exc = e
            if attempt < len(_RETRY_DELAYS):
                delay = _RETRY_DELAYS[attempt]
                logger.warning(
                    f"Request to {url} failed ({e}) — "
                    f"retry {attempt + 1}/{len(_RETRY_DELAYS)} in {delay}s"
                )
                _time.sleep(delay)
    raise last_exc


class OpenAlgoAPI:
    def __init__(self, api_key: str | None = None, host: str | None = None):
        self.api_key = api_key or os.getenv("OPENALGO_API_KEY")
        self.host = host or HOST

    async def get_next_expiry(self, underlying: str, min_dte: int = 2) -> str | None:
        return await asyncio.to_thread(self._get_next_expiry_sync, underlying, min_dte)

    def _get_next_expiry_sync(self, underlying: str, min_dte: int = 2) -> str | None:
        exchange = "NFO"
        if underlying == "SENSEX":
            exchange = "BFO"
        elif underlying == "BANKNIFTY":
            exchange = "NFO"

        expiry_dates = get_expiry_dates(self.api_key, underlying, exchange, "options")
        if not expiry_dates:
            return None

        today = datetime.now().date()
        for expiry in expiry_dates:
            try:
                expiry_date = datetime.strptime(expiry, "%d%b%y").date()
                if (expiry_date - today).days >= min_dte:
                    return expiry
            except Exception:
                continue
        return expiry_dates[0] if expiry_dates else None

    async def get_quote(self, symbol: str) -> dict[str, Any]:
        return await asyncio.to_thread(self._get_quote_sync, symbol)

    def _get_quote_sync(self, symbol: str) -> dict[str, Any]:
        exchange = "NFO"
        if symbol.startswith("SENSEX"):
            exchange = "BFO"
        elif symbol.startswith("BANKNIFTY"):
            exchange = "NFO"

        try:
            url = f"{self.host}/api/v1/quotes"
            payload = {"apikey": self.api_key, "symbol": symbol, "exchange": exchange}
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "success":
                return data.get("data", {})
            return {}
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol}: {e}")
            return {}

    async def place_order(self, order: dict[str, Any]) -> dict[str, Any]:
        return await asyncio.to_thread(self._place_order_sync, order)

    def _place_order_sync(self, order: dict[str, Any]) -> dict[str, Any]:
        order_data = order.copy()
        # Normalize alias names to OpenAlgo REST API field names
        if "side" in order_data:
            order_data["action"] = order_data.pop("side")
        if "order_type" in order_data:
            order_data["pricetype"] = order_data.pop("order_type")
        if "strategy" not in order_data:
            order_data["strategy"] = order_data.get("strategy", "bb_extremes_sell_options")
        if "exchange" not in order_data and "symbol" in order_data:
            sym = order_data["symbol"]
            if sym.startswith("SENSEX"):
                order_data["exchange"] = "BFO"
            else:
                order_data["exchange"] = "NFO"

        payload = {"apikey": self.api_key, **order_data}
        if "price" in payload and payload["price"] is None:
            payload["price"] = 0.0

        try:
            url = f"{self.host}/api/v1/place_order"
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data
        except Exception as e:
            logger.error(f"Error placing order: {e}")
            return {"status": "error", "message": str(e)}


def get_expiry_dates(api_key, symbol, exchange, instrument_type="options"):
    """
    Fetch expiry dates for a given symbol.
    Returns a list of date strings (e.g. "28OCT25").
    """
    try:
        url = f"{HOST}/api/v1/expiry"
        payload = {
            "apikey": api_key,
            "symbol": symbol,
            "exchange": exchange,
            "instrumenttype": instrument_type
        }
        response = _post_with_retry(url, payload, timeout=10)
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, dict) and "data" in data:
             raw_dates = data["data"]
             # Robust Date Extraction: Look for DDMMMYY pattern (e.g. 10FEB26)
             cleaned_dates = []
             import re
             for d in raw_dates:
                 d_clean = d.replace("-", "").upper()
                 match = re.search(r"(\d{2}[A-Z]{3}\d{2})", d_clean)
                 if match:
                     cleaned_dates.append(match.group(1))
                 else:
                     cleaned_dates.append(d_clean) # Fallback
             return cleaned_dates
        return []
    except Exception as e:
        logger.error(f"Error fetching expiry dates: {e}")
        return []

def get_option_symbol(api_key, underlying, exchange, expiry, option_type, offset="ATM", strike_int=None, **kwargs):
    """
    Fetch the option symbol for a given underlying and criteria.
    Note: underlying_ltp is NOT an accepted request field (it's response-only).
    """
    try:
        url = f"{HOST}/api/v1/optionsymbol"
        payload = {
            "apikey": api_key,
            "underlying": underlying,
            "exchange": exchange,
            "expiry_date": expiry,
            "option_type": option_type,
            "offset": offset
        }
        if strike_int:
            payload["strike_int"] = strike_int
            
        # Add underlying_ltp if provided to avoid redundant server-side fetches
        ltp = kwargs.get("underlying_ltp")
        if ltp is not None:
            payload["underlying_ltp"] = ltp

        response = _post_with_retry(url, payload, timeout=10)
        if response.status_code != 200:
            logger.error(f"API Error fetching option symbol: {response.status_code} - {response.text}")
            return None
            
        response.raise_for_status()
        data = response.json()
        
        # Expected: {"status": "success", "symbol": "...", ...}
        if data.get("status") == "success":
            return data.get("symbol")
        else:
            logger.error(f"Error fetching option symbol: {data.get('message')}")
            return None
    except Exception as e:
        logger.error(f"Exception fetching option symbol: {e}")
        return None

def get_multiquotes(api_key, symbols: list):
    """
    Fetch real-time quotes for multiple symbols in a single request.
    'symbols' should be a list of dicts: [{"symbol": "SBIN", "exchange": "NSE"}, ...]
    """
    try:
        url = f"{HOST}/api/v1/multiquotes"
        payload = {
            "apikey": api_key,
            "symbols": symbols
        }
        response = _post_with_retry(url, payload, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success":
            return data.get("results", [])
        return []
    except Exception as e:
        logger.error(f"Error fetching multiquotes: {e}")
        return []

def get_history(api_key, symbol, exchange, interval, duration_days=5):
    """
    Fetch historical data.
    """
    try:
        url = f"{HOST}/api/v1/history"
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=duration_days)).strftime("%Y-%m-%d")
        
        # Transparently map standard shortcuts to full Fyers structural history indices
        query_symbol = symbol
        
        payload = {
            "apikey": api_key,
            "symbol": query_symbol,
            "exchange": exchange,
            "interval": interval,
            "start_date": start_date,
            "end_date": end_date,
            "source": "api"
        }
        # Increased timeout to 60s since 5-day 1-min history parses huge payloads (often >10s server-side)
        response = _post_with_retry(url, payload, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return []
    except Exception as e:
        logger.error(f"Error fetching history: {e}")
        return []
def is_market_holiday(api_key, query_date=None, exchange=None):
    """
    Check if a specific date is a market holiday using OpenAlgo API.
    """
    try:
        if query_date is None:
            query_date = datetime.now().strftime("%Y-%m-%d")
        elif isinstance(query_date, datetime):
            query_date = query_date.strftime("%Y-%m-%d")

        url = f"{HOST}/api/v1/market/holidays"
        payload = {
            "apikey": api_key,
            "date": query_date
        }
        if exchange:
            payload["exchange"] = exchange

        response = requests.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                return data.get("data", {}).get("is_holiday", False)
        return False
    except Exception as e:
        logger.error(f"Error checking market holiday: {e}")
        return False


# ---------------------------------------------------------------------------
# Fyers API — authoritative NSE F&O trading-day check
# ---------------------------------------------------------------------------
# Direct REST call — no fyers_apiv3 dependency required.
# Authorisation header format: "<BROKER_API_KEY>:<access_token>"
# Endpoint: GET https://api-t1.fyers.in/data/marketStatus
#
# Token source: THIS project's own OpenAlgo broker login (database/auth_db.py),
# looked up via the local OPENALGO_API_KEY — NOT fyers_token_service, which is
# a separate service scoped to the unrelated crk_fyers project. Calling that
# service from here returns 401s daily, since its session has nothing to do
# with this project's Fyers login. See incident 2026-06-30 (same bug found
# and fixed first in fyers_cs's copy of this file).
#
# Required env vars (openalgo/.env):
#   BROKER_API_KEY   — Fyers app_id / client_id (e.g. XY1234-100) [already present]
#   OPENALGO_API_KEY — this project's own OpenAlgo API key [already present]
#
# Result is cached per calendar date so the monitor loop (30 s) makes at
# most one Fyers API call per day. Falls back to is_market_holiday() if no
# token is available (broker not logged in for the day).
# ---------------------------------------------------------------------------

_FYERS_MARKET_STATUS_URL = "https://api-t1.fyers.in/data/marketStatus"
_trading_day_cache: dict = {}   # {"date": date, "result": (bool, str)}


def is_nse_fo_trading_day_via_fyers(api_key: str | None = None) -> tuple[bool, str]:
    """
    Return (is_trading_day, reason) using the Fyers market_status REST API.

    Fail-safe: any error returns (False, reason) so bots stay off on doubt.
    Weekend short-circuits without any network call.
    Result is cached per calendar date.
    """
    from datetime import date as _date
    today = _date.today()

    # Weekend — no API call needed
    if today.weekday() >= 5:
        return False, "Weekend"

    # Return cached answer if we already checked today
    if _trading_day_cache.get("date") == today:
        return _trading_day_cache["result"]

    result = _call_fyers_market_status(api_key)
    _trading_day_cache["date"] = today
    _trading_day_cache["result"] = result
    logger.info(f"Fyers market_status → {result}")
    return result


def _call_fyers_market_status(api_key: str | None = None) -> tuple[bool, str]:
    """Internal: fetch Fyers marketStatus and parse NSE F&O session."""
    client_id = os.getenv("BROKER_API_KEY", "").strip()
    if not client_id:
        return False, "BROKER_API_KEY not set — cannot call Fyers market_status"

    api_key = (api_key or os.getenv("OPENALGO_API_KEY", "")).strip()
    if not api_key:
        return False, "OPENALGO_API_KEY not set — cannot look up broker token"

    try:
        from database.auth_db import get_auth_token_broker
        access_token, broker = get_auth_token_broker(api_key)
        if not access_token:
            return False, "No Fyers auth token in local OpenAlgo DB — broker login required for today"
    except Exception as e:
        return False, f"Local OpenAlgo token lookup failed: {e}"

    # ── Call Fyers REST market_status ──────────────────────────────────
    auth_header = f"{client_id}:{access_token}"
    try:
        resp = requests.get(
            _FYERS_MARKET_STATUS_URL,
            headers={"Authorization": auth_header},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return False, f"Fyers market_status request failed: {e}"

    if data.get("s") != "ok":
        return False, f"Fyers market_status error: {data.get('message', data)}"

    return _parse_nfo_session(data.get("marketStatus", []))


def _parse_nfo_session(statuses: list) -> tuple[bool, str]:
    """
    Decide if NSE F&O has a session scheduled for today.

    Trading day  → startTime is a real clock time (e.g. "09:15 AM")
    Holiday      → segment absent, OR startTime is empty / "N/A" / "00:00 AM"

    Note: Fyers API may return exchange as an integer (e.g. 10 for NSE)
    or as a string ("NSE"). We normalise with str() before comparing.
    """
    def _exch(m: dict) -> str:
        return str(m.get("exchange", "") or "").strip().upper()

    def _seg(m: dict) -> str:
        return str(m.get("segment", "") or "").strip().lower()

    # Primary: NSE Futures & Options segment
    nfo = [
        m for m in statuses
        if _exch(m) in ("NSE", "NFO", "10")  # 10 = NSE numeric code in Fyers
        and any(kw in _seg(m)
                for kw in ("futures", "options", "f&o", "nfo", "derivative"))
    ]
    # Fallback: any NSE segment
    if not nfo:
        nfo = [m for m in statuses if _exch(m) in ("NSE", "10")]

    if not nfo:
        logger.warning(
            "No NSE entry found in Fyers market_status. Raw exchanges: %s",
            [m.get("exchange") for m in statuses],
        )
        return False, "No NSE entry in Fyers market_status — probable holiday"

    _EMPTY = {"", "n/a", "na", "00:00", "00:00 am", "-", "none", "null"}

    for entry in nfo:
        status = str(entry.get("status", "") or "").strip().upper()
        start  = str(entry.get("startTime", "") or "").strip()
        end    = str(entry.get("endTime",   "") or "").strip()

        # Explicit open signal → definitely a trading day
        if status in ("OPEN", "PRE OPEN", "PRE-OPEN", "PREOPEN"):
            return True, f"NSE F&O {status}: {start}–{end}"

        # Non-trivial startTime → session scheduled today (status may say CLOSE
        # simply because we're checking before/after market hours)
        if start.lower() not in _EMPTY:
            return True, f"NSE F&O session: {start}–{end} (status={status})"

    return False, "NSE F&O has no session today — market holiday or unscheduled closure"
def get_appropriate_expiry_for_day(api_key, index_name, exchange, opt_exchange):
    """
    Selects current or next expiry based on days to expiry.
    Usually keeps the current one unless it expires today or tomorrow.
    """
    try:
        from datetime import datetime
        dates = get_expiry_dates(api_key, index_name, opt_exchange)
        if not dates:
            return None
            
        # Parse the first available expiry to check proximity
        # Format assumed: DDMMMYY (e.g. 30MAR26)
        try:
            first_exp_dt = datetime.strptime(dates[0], "%d%b%y")
            days_to_expiry = (first_exp_dt.date() - datetime.now().date()).days
        except:
            days_to_expiry = 3 # Fallback if parsing fails
            
        # Strategy: Use current expiry if it's more than 1 day away
        # If it expires today (0) or tomorrow (1), and we have a next one, use that.
        if days_to_expiry > 1 or len(dates) < 2:
            return dates[0]
        else:
            return dates[1]
                
    except Exception as e:
        logger.error(f"Error selecting appropriate expiry: {e}")
        return None
