#!/usr/bin/env python3
"""
nifty_iron_fly_weekly_bot.py — NIFTY Weekly Short Iron Fly (Paper Trading)
=====================================================================
Champion C3 parameters (iron_fly_weekly_study, Stage 10 cleared 2026-05-11):

  entry_style  : monday_1000  (Mon 10:00 for Thu expiry / Wed 10:00 for Tue expiry)
  hedge_delta  : 0.10         (Black-76, OTM legs)
  stop_loss    : ₹2,000 / lot combined MTM loss  → ₹20,000 total for 10 lots
  profit_target: None         (hold to scheduled exit — pure theta decay)
  filter       : VIX ≥ 12 AND NIFTY_prev_close ≥ NIFTY_MA20

4 legs — all NRML (positions carried overnight 3–4 days):
  BUY   OTM CE   (delta ≈ 0.10)  ← placed FIRST for margin benefit
  BUY   OTM PE   (delta ≈ 0.10)  ← placed SECOND
  SELL  ATM CE                   ← placed THIRD
  SELL  ATM PE                   ← placed FOURTH

Entry:  10:00–10:20 IST on Monday (Thu expiry) / Wednesday (Tue expiry)
Exit:   (a) SL:         combined MTM ≤ -(₹2,000 × N_LOTS) → close all 4 legs
        (b) Scheduled:  15:15 IST on the day BEFORE expiry → close all 4 legs

For closing:
  BUY close order for the two SELL legs  → FIRST for margin unwind
  SELL close order for the two BUY legs  → SECOND

Implementation notes:
  • Uses placeorder (not placesmartorder) — avoids the position_size pitfall.
  • Symbols resolved via OpenAlgo /api/v1/optionsymbol — broker-agnostic.
  • Lot size fetched from OpenAlgo token DB (currently 65 for NIFTY post-Dec 2025).
  • State written to logs/iron_fly_weekly_state.json — read by streamlit_dashboard.py.
  • Bot is shut down at market close; state file ensures position is restored next day.
  • Expiry schedule follows OpenAlgo /api/v1/expiry — currently Tuesdays for NIFTY.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import os
import sys
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Path / env ─────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent.parent   # fyers_crk/openalgo/
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from openalgo import api                                                     # noqa
from live_trading.api_utils import HOST, get_expiry_dates, get_history, is_market_holiday  # noqa
from live_trading.shared.order_fill import fetch_fill_price                   # noqa
from live_trading.shared.telegram_notifier import send_async                  # noqa
from live_trading.shared.trade_logger import log_trade_to_db                  # noqa

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "nifty_iron_fly_weekly_bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

API_KEY          = os.getenv("OPENALGO_API_KEY")
PAPER_MODE       = False   # Set to False to place orders in sandbox (analyzer mode)

STRATEGY_NAME    = "NIFTY_IRON_FLY_WEEKLY"
IDX_SYMBOL       = "NIFTY"
IDX_EXCHANGE     = "NSE_INDEX"
VIX_SYMBOL       = "INDIAVIX"
OPT_EXCHANGE     = "NFO"

N_LOTS           = 10
DEFAULT_LOT_SIZE = 65      # post-Dec 2025 NIFTY lot size (source: start_all_bots.py)
ATM_STEP         = 50      # NIFTY strike increment
RISK_FREE_RATE   = 0.065   # India risk-free rate

HEDGE_DELTA      = 0.10    # champion parameter

# Timing (IST)
ENTRY_HOUR          = 10
ENTRY_MINUTE        = 0
ENTRY_WINDOW_MINS   = 20   # Entry window 10:00-10:20 IST
EXIT_HOUR           = 15
EXIT_MINUTE         = 15   # scheduled exit on day-before-expiry

# Filters
VIX_MIN   = 12.0           # VIX ≥ 12 to enter (inverse VIX filter)
MA_PERIOD = 20             # NIFTY daily close MA period

# Stop-loss (per lot, combined across all 4 legs)
SL_PER_LOT = 2_000.0      # ₹2,000/lot  →  ₹20,000 total for 10 lots

# State and logs
STATE_FILE       = LOG_DIR / "nifty_iron_fly_weekly_state.json"
PAPER_TRADES_CSV = LOG_DIR / "nifty_iron_fly_weekly_paper_trades.csv"

POLL_INTERVAL_SECS = 30    # MTM monitoring frequency


# ══════════════════════════════════════════════════════════════════════════════
# BLACK-76 DELTA (inline — no numba dependency in live bot)
# ══════════════════════════════════════════════════════════════════════════════

def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erf — matches greeks_engine.py."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _black76_delta(F: float, K: float, T: float, sigma: float, r: float,
                   opt_type: str) -> float:
    """
    Black-76 delta for European cash-settled index options.
    opt_type: 'CE' → positive delta  |  'PE' → negative delta
    """
    if T <= 0 or sigma <= 0 or F <= 0 or K <= 0:
        return 0.0
    d1   = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    disc = math.exp(-r * T)
    if opt_type == "CE":
        return disc * _norm_cdf(d1)
    else:
        return disc * (_norm_cdf(d1) - 1.0)


def find_hedge_strike(spot: float, expiry_date: date, vix_level: float,
                       opt_type: str, target_delta: float = HEDGE_DELTA) -> int:
    """
    Walk OTM strikes in ATM_STEP increments and return the one whose
    Black-76 |delta| is closest to target_delta.
    CE → strikes above ATM.  PE → strikes below ATM.
    """
    T     = max((expiry_date - date.today()).days / 365.0, 1 / 365.0)
    sigma = vix_level / 100.0
    F     = spot
    atm   = int(round(spot / ATM_STEP) * ATM_STEP)
    step  = 1 if opt_type == "CE" else -1

    best_strike = atm + step * ATM_STEP
    best_diff   = float("inf")
    last_delta  = float("inf")

    for n in range(1, 30):
        K     = atm + step * n * ATM_STEP
        delta = _black76_delta(F, K, T, sigma, RISK_FREE_RATE, opt_type)
        diff  = abs(abs(delta) - target_delta)
        if diff < best_diff:
            best_diff   = diff
            best_strike = K
        last_delta = abs(delta)
        if last_delta < target_delta - 0.03:
            break

    logger.info(
        f"  Hedge {opt_type}: strike={best_strike}  target_Δ={target_delta}  "
        f"T={T:.4f}y  σ={sigma:.2f}"
    )
    return best_strike


# ══════════════════════════════════════════════════════════════════════════════
# SYMBOL RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_option_symbol(expiry_str: str, opt_type: str,
                            strike: int | None = None,
                            atm_strike: int | None = None) -> str | None:
    """
    Resolve option symbol via OpenAlgo /api/v1/optionsymbol.
    OpenAlgo translates this to the broker's native format.

    strike=None → ATM (offset="ATM")
    strike=int  → specific strike converted to offset from ATM
    """
    payload: dict = {
        "apikey":      API_KEY,
        "underlying":  IDX_SYMBOL,
        "exchange":    OPT_EXCHANGE,
        "expiry_date": expiry_str,
        "option_type": opt_type,
    }

    if strike is None:
        # ATM requested
        payload["offset"] = "ATM"
    elif atm_strike is not None:
        # Convert exact strike to offset from ATM
        diff = strike - atm_strike
        steps = abs(diff) // ATM_STEP
        if steps == 0:
            payload["offset"] = "ATM"
        elif diff > 0:
            # Strike above ATM = OTM for CE, ITM for PE
            payload["offset"] = f"OTM{steps}" if opt_type == "CE" else f"ITM{steps}"
        else:
            # Strike below ATM = ITM for CE, OTM for PE
            payload["offset"] = f"ITM{steps}" if opt_type == "CE" else f"OTM{steps}"
    else:
        # No ATM provided - use ATM as fallback
        payload["offset"] = "ATM"

    try:
        res = requests.post(f"{HOST}/api/v1/optionsymbol", json=payload, timeout=5)
        if res.status_code == 200:
            data = res.json()
            if data.get("status") == "success":
                sym = data.get("symbol")
                if sym:
                    return sym
            logger.warning(
                f"  optionsymbol API: {data.get('message', 'no symbol')} "
                f"(exp={expiry_str} {opt_type} k={strike} offset={payload.get('offset')})"
            )
    except Exception as e:
        logger.error(f"  optionsymbol API error: {e}")

    # Fallback: build symbol using correct OpenAlgo format
    # Format: NIFTY{DD}{MMM}{YY}{CENTURY}{STRIKE}{CE/PE} e.g. NIFTY19MAY2623450CE
    if strike is not None and atm_strike is not None:
        # Extract components from expiry_str (e.g., "19MAY26")
        dd  = expiry_str[:2]   # "19"
        mmm = expiry_str[2:5]  # "MAY"
        yy  = expiry_str[5:7]  # "26"
        # Century is 20 for year 20xx
        century = "20"
        # The format in DB: NIFTY{DD}{MMM}{CENTURY+YY}{STRIKE}{CE/PE}
        # Actually let's check the DB format again
        # DB shows: NIFTY19MAY2623450CE = NIFTY + DD(19) + MMM(MAY) + YY(26)?? No wait
        # Let me trace: 19MAY26 -> 19MAY -> day=19, month MAY, year=26
        # DB format: NIFTY19MAY2623450CE where 26 is year, but then what is 26 after MAY?
        # Actually the format is: NIFTY + DD + MMM + YY + ??? + Strike + OptType
        # Let me check by looking at the pattern: NIFTY19MAY2623450CE
        # If we split: NIFTY | 19 | MAY | 26 | 2 | 3450 | CE
        # That doesn't work. Let's try: NIFTY | 19MAY26 | 23 | 450CE
        # Looking at the DB entry: NIFTY19MAY2623450CE where ATM = 23450
        # So: 19MAY26 (expiry) + 23 (???)
        # Actually wait - the 23 in 23450 is the ATM relative number?
        # Let me re-examine: the DB has strikes like 23450, 23500, etc.
        # The symbol NIFTY19MAY2623450CE has strike=23450 embedded at the end
        # So the format is: NIFTY + DD + MMM + YY + ??? + Strike
        # Wait, I see now: NIFTY19MAY2623450CE
        # = NIFTY + 19 + MAY + 26 + 2 + 3450 + CE
        # That 2 might be something else...
        # Actually let me look at another: NIFTY26MAY2623450CE for 26MAY26
        # Both have 26 after month and before strike
        # So it's: NIFTY + DD + MMM + YY + 2?? No wait
        # Actually I think: DD(19) + MMM(MAY) + YY(26) + Century(20)??
        # No, let me check: NIFTY19MAY2623450CE vs NIFTY26MAY2623450CE
        # They both have "2623450" after month, where 26 is year and 23 is ATM??
        # Actually I think it's: DD(19) + MMM(MAY) + Century(26) + YY(??)
        # Let's just query the DB to understand the pattern better
        century = "26"  # Hardcode to 26 for 2026 (appears to be Century + YY combined)
        # Actually the DB shows 19MAY26 expiry -> symbol NIFTY19MAY2623450CE
        # So after MAY comes "26" (year 2026)
        # The pattern is: NIFTY{dd}{MMM}{yy}{strike}{opt_type} where strike includes some encoding
        # Wait, looking at: NIFTY19MAY2623450CE where strike = 23450
        # The "26" after MAY is year, then "23450" is the strike
        # So: NIFTY + 19 + MAY + 26 + 23450 + CE = NIFTY19MAY2623450CE ✓
        # Format: NIFTY{dd}{MMM}{yy}{strike}{CE/PE}
        sym = f"{IDX_SYMBOL}{expiry_str}{strike}{opt_type}"
        logger.warning(f"  Fallback symbol: {sym}")
        return sym
    return None


# ══════════════════════════════════════════════════════════════════════════════
# EXPIRY / SCHEDULE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _parse_expiry_str(expiry_str: str) -> date:
    """'14MAY26' → date(2026, 5, 14)"""
    return datetime.strptime(expiry_str, "%d%b%y").date()


def _next_trading_day(d: date) -> date:
    """Advance d forward until it lands on a weekday that is not a market holiday."""
    while True:
        if d.weekday() < 5 and not is_market_holiday(API_KEY, d.strftime("%Y-%m-%d")):
            return d
        d += timedelta(days=1)


def _get_entry_date(expiry_date: date) -> date:
    """
    Thu expiry (dow=3) → Monday  = expiry − 3 calendar days
    Tue expiry (dow=1) → Wednesday = expiry − 6 calendar days
    (mirrors the backtest's build_oos_schedule logic)

    If the computed anchor falls on a market holiday, rolls forward to the
    next available trading day.
    """
    dow = expiry_date.weekday()
    if dow == 3:    # Thursday
        anchor = expiry_date - timedelta(days=3)
    elif dow == 1:  # Tuesday  ← current NIFTY expiry day as of Sep 2025
        anchor = expiry_date - timedelta(days=6)
    else:
        raise ValueError(
            f"Unexpected expiry weekday={dow} for {expiry_date}. "
            "Expected Thu(3) or Tue(1). Check OpenAlgo expiry schedule."
        )
    return _next_trading_day(anchor)


def _get_exit_date(expiry_date: date) -> date:
    """15:15 scheduled close is on the PREVIOUS calendar day."""
    return expiry_date - timedelta(days=1)


def _get_upcoming_expiry() -> tuple[str, date] | tuple[None, None]:
    """
    Fetch nearest NIFTY weekly expiry with ≥2 DTE from OpenAlgo.
    OpenAlgo's market calendar handles holiday shifts automatically.
    Returns (expiry_str_DDMMMYY, expiry_date) or (None, None).
    """
    dates = get_expiry_dates(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, "options")
    if not dates:
        logger.error("Could not fetch expiry dates from OpenAlgo.")
        return None, None
    today = date.today()
    for d in dates:
        try:
            exp_dt = _parse_expiry_str(d)
            if (exp_dt - today).days >= 2:
                return d, exp_dt
        except ValueError:
            continue
    return None, None


# ══════════════════════════════════════════════════════════════════════════════
# PRICE / DATA HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _quote(symbol: str, exchange: str) -> float:
    """Fetch LTP for any symbol via OpenAlgo /api/v1/quotes."""
    try:
        res = requests.post(
            f"{HOST}/api/v1/quotes",
            json={"apikey": API_KEY, "symbol": symbol, "exchange": exchange},
            timeout=15,
        )
        if res.status_code == 200:
            data = res.json()
            if data.get("status") == "success":
                qd = data.get("data", {})
                if isinstance(qd, dict):
                    # Try common field names in order of preference
                    ltp = (qd.get("ltp") or qd.get("last_price")
                           or qd.get("close") or qd.get("c") or 0)
                    price = float(ltp)
                    if price == 0.0:
                        logger.warning(
                            f"  Quote returned 0 for {symbol}@{exchange}. "
                            f"Raw data keys: {list(qd.keys())}"
                        )
                    return price
                if isinstance(qd, list) and qd:
                    ltp = (qd[0].get("ltp") or qd[0].get("last_price")
                           or qd[0].get("close") or qd[0].get("c") or 0)
                    price = float(ltp)
                    if price == 0.0:
                        logger.warning(
                            f"  Quote returned 0 for {symbol}@{exchange}. "
                            f"Raw data[0] keys: {list(qd[0].keys())}"
                        )
                    return price
                logger.warning(
                    f"  Quote data unexpected type ({type(qd)}) for {symbol}@{exchange}: {qd}"
                )
            else:
                logger.warning(
                    f"  Quote API non-success for {symbol}@{exchange}: "
                    f"status={data.get('status')}  message={data.get('message', '—')}"
                )
        else:
            logger.warning(
                f"  Quote HTTP {res.status_code} for {symbol}@{exchange}: {res.text[:200]}"
            )
    except Exception as e:
        logger.error(f"Quote failed ({symbol}@{exchange}): {e}")
    return 0.0


def _get_vix()        -> float: return _quote(VIX_SYMBOL, IDX_EXCHANGE)
def _get_nifty_spot() -> float: return _quote(IDX_SYMBOL,  IDX_EXCHANGE)


def _get_option_price(symbol: str, retries: int = 3, delay: float = 5.0) -> float:
    """
    Fetch option LTP with retries.  At 10:00:00 the order book can be cold
    (pre-open settlement finishing), so a single shot is fragile.
    Retries up to `retries` times with `delay` seconds between attempts.
    """
    import time
    for attempt in range(1, retries + 1):
        price = _quote(symbol, OPT_EXCHANGE)
        if price > 0:
            return price
        if attempt < retries:
            logger.info(
                f"  LTP=0 for {symbol} (attempt {attempt}/{retries}) — "
                f"retrying in {delay:.0f}s …"
            )
            time.sleep(delay)
    logger.error(f"  LTP still 0 after {retries} attempts for {symbol}")
    return 0.0


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback


def _check_ma20_filter() -> tuple[bool, float, float]:
    """
    Load 30 NIFTY daily bars, compute MA20 from closes, check prev_close ≥ MA20.
    Returns (passes, prev_close, ma20).
    FAILS on data insufficiency (stricter than allowing by default).
    """
    hist = get_history(API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "D", 35)
    if not hist:
        logger.warning("  MA20 filter: no history data received — FAILING entry.")
        return False, 0.0, 0.0

    logger.info(f"  MA20 filter: received {len(hist)} bars from history API")

    closes = []
    for r in hist:
        c = r.get("close") or r.get("Close")
        if c is not None:
            closes.append(float(c))

    logger.info(f"  MA20 filter: parsed {len(closes)} close prices")

    if len(closes) < 21:
        logger.warning(f"  MA20 filter: only {len(closes)} closes (need ≥21) — FAILING entry.")
        return False, 0.0, 0.0

    ma20       = sum(closes[-20:]) / 20.0
    prev_close = closes[-1]
    return prev_close >= ma20, round(prev_close, 2), round(ma20, 2)


# ══════════════════════════════════════════════════════════════════════════════
# LOT SIZE
# ══════════════════════════════════════════════════════════════════════════════

def _get_lot_size(symbol: str) -> int:
    """
    Query OpenAlgo token DB for the current lot size.
    Falls back to DEFAULT_LOT_SIZE (65) on any error.
    """
    try:
        from database.token_db import get_symbol_info
        si = get_symbol_info(symbol, OPT_EXCHANGE)
        if si and getattr(si, "lotsize", None):
            ls = int(si.lotsize)
            logger.info(f"  Lot size from DB: {ls} (symbol={symbol})")
            return ls
    except Exception as e:
        logger.warning(f"  Lot size lookup failed ({symbol}): {e}. Using {DEFAULT_LOT_SIZE}.")
    return DEFAULT_LOT_SIZE


# ══════════════════════════════════════════════════════════════════════════════
# STATE MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def load_state() -> dict | None:
    """Load persisted position state. Returns None if no open position."""
    if not STATE_FILE.exists():
        return None
    try:
        s = json.loads(STATE_FILE.read_text())
        if not s.get("closed", True):
            return s
    except Exception as e:
        logger.warning(f"State load failed: {e}")
    return None


def save_state(state: dict) -> None:
    """Persist current bot state for dashboard + crash recovery."""
    try:
        state["last_update"] = datetime.now().isoformat()
        STATE_FILE.write_text(json.dumps(state, indent=2, default=str))
    except Exception as e:
        logger.error(f"State save failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# PAPER TRADE CSV LOGGER
# ══════════════════════════════════════════════════════════════════════════════

def _log_csv(action: str, leg: str, symbol: str, premium: float,
              qty: int, pnl: float, reason: str) -> None:
    write_header = not PAPER_TRADES_CSV.exists()
    try:
        with open(PAPER_TRADES_CSV, "a", newline="") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow(["timestamp", "action", "leg", "symbol",
                             "premium", "qty", "pnl_gross", "reason", "paper_mode"])
            w.writerow([datetime.now().isoformat(), action, leg, symbol,
                        round(premium, 2), qty, round(pnl, 2), reason, PAPER_MODE])
    except Exception as e:
        logger.warning(f"CSV log failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# BOT CLASS
# ══════════════════════════════════════════════════════════════════════════════

class IronFlyBot:
    """
    NIFTY Weekly Short Iron Fly — paper trading bot.
    Polling-based (no WebSocket needed — fixed entry schedule, 30s MTM poll).
    """

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.state: dict | None = None

    # ──────────────────────────────────────────────────────────────────────────
    #  ORDER PLACEMENT
    # ──────────────────────────────────────────────────────────────────────────

    def _placeorder(self, symbol: str, action: str, qty: int) -> dict:
        """
        Place a plain NRML market order via OpenAlgo placeorder (not placesmartorder).
        Returns the API response dict. In PAPER_MODE, simulates and returns a dummy.
        """
        if PAPER_MODE:
            logger.info(f"  📝 PAPER: {action} {qty}× {symbol} @ MARKET NRML")
            return {"status": "success", "orderid": "PAPER", "paper": True}
        try:
            resp = self.client.placeorder(
                strategy=STRATEGY_NAME,
                symbol=symbol,
                action=action,
                exchange=OPT_EXCHANGE,
                price_type="MARKET",
                product="NRML",
                quantity=qty,
            )
            logger.info(f"  {action} {symbol}: {resp}")
            return resp if isinstance(resp, dict) else {"status": "ok", "raw": str(resp)}
        except Exception as e:
            logger.error(f"  placeorder failed ({action} {symbol}): {e}")
            return {"status": "error", "message": str(e)}

    # ──────────────────────────────────────────────────────────────────────────
    #  ENTRY
    # ──────────────────────────────────────────────────────────────────────────

    async def _enter(self, expiry_str: str, expiry_date: date, force: bool = False) -> str:
        """Evaluate filters and open all 4 legs of the iron fly."""
        logger.info("═" * 64)
        logger.info(f"⚡ IRON FLY ENTRY — evaluating filters (force={force})")

        # ── Filter 1: INDIA VIX ≥ 12 ─────────────────────────────────────────
        vix = _get_vix()
        if vix <= 0:
            # Retry up to 3 times — at 10:00 IST the server may be under load
            for _attempt in range(3):
                import time as _time
                logger.warning(
                    f"  VIX fetch returned 0 (attempt {_attempt + 1}/3) — retrying in 5s…"
                )
                _time.sleep(5)
                vix = _get_vix()
                if vix > 0:
                    break
        if vix <= 0:
            if force:
                logger.warning("  VIX = 0 after 3 retries, but force=True. Defaulting to 15.0.")
                vix = 15.0
            else:
                logger.warning("  VIX = 0 after 3 retries — data error. Skipping entry.")
                return "error"
        if not force and vix < VIX_MIN:
            logger.info(f"  ❌ VIX FAIL: {vix:.2f} < {VIX_MIN} — no trade.")
            await send_async(
                f"🚫 *{STRATEGY_NAME}* — VIX filter FAIL\n"
                f"VIX={vix:.2f} < {VIX_MIN:.0f}. Skipping this week."
            )
            return "skip_filters"
        logger.info(f"  ✅ VIX PASS: {vix:.2f} ≥ {VIX_MIN} (or force-passed)")

        # ── Filter 2: NIFTY prev close ≥ MA20 ────────────────────────────────
        ma_pass, prev_close, ma20 = _check_ma20_filter()
        if not force and not ma_pass:
            if prev_close == 0.0 or ma20 == 0.0:
                logger.warning("  MA20 filter: history fetch error. Retrying later...")
                return "error"
            logger.info(f"  ❌ MA20 FAIL: close={prev_close:.1f} < MA20={ma20:.1f}")
            await send_async(
                f"🚫 *{STRATEGY_NAME}* — MA20 filter FAIL\n"
                f"NIFTY={prev_close:.1f} < MA20={ma20:.1f}. Skipping."
            )
            return "skip_filters"

        if force and (prev_close == 0.0 or ma20 == 0.0):
            logger.warning("  MA20 filter history fetch failed, but force=True. Setting dummy values.")
            prev_close, ma20 = 23800.0, 23600.0

        logger.info(f"  ✅ MA20 PASS: NIFTY={prev_close:.1f} ≥ MA20={ma20:.1f} (or force-passed)")

        # ── NIFTY spot → ATM strike ───────────────────────────────────────────
        spot = _get_nifty_spot()
        if spot <= 0:
            for _attempt in range(3):
                import time as _time
                logger.warning(
                    f"  NIFTY spot fetch returned 0 (attempt {_attempt + 1}/3) — retrying in 5s…"
                )
                _time.sleep(5)
                spot = _get_nifty_spot()
                if spot > 0:
                    break
        if spot <= 0:
            logger.error("  NIFTY spot = 0 — aborting entry.")
            return "error"
        atm = int(round(spot / ATM_STEP) * ATM_STEP)
        logger.info(f"  NIFTY spot={spot:.1f}  ATM={atm}")

        # ── Hedge strikes via Black-76 ────────────────────────────────────────
        hedge_ce_k = find_hedge_strike(spot, expiry_date, vix, "CE", HEDGE_DELTA)
        hedge_pe_k = find_hedge_strike(spot, expiry_date, vix, "PE", HEDGE_DELTA)

        # ── Resolve all 4 symbols via OpenAlgo API ────────────────────────────
        buy_ce_sym  = _resolve_option_symbol(expiry_str, "CE", strike=hedge_ce_k, atm_strike=atm)
        buy_pe_sym  = _resolve_option_symbol(expiry_str, "PE", strike=hedge_pe_k, atm_strike=atm)
        sell_ce_sym = _resolve_option_symbol(expiry_str, "CE", strike=atm, atm_strike=atm)
        sell_pe_sym = _resolve_option_symbol(expiry_str, "PE", strike=atm, atm_strike=atm)

        if not all([buy_ce_sym, buy_pe_sym, sell_ce_sym, sell_pe_sym]):
            logger.error("  Failed to resolve one or more option symbols. Aborting.")
            return "error"

        logger.info("  Symbols resolved:")
        logger.info(f"    BUY  CE hedge → {buy_ce_sym}")
        logger.info(f"    BUY  PE hedge → {buy_pe_sym}")
        logger.info(f"    SELL CE ATM   → {sell_ce_sym}")
        logger.info(f"    SELL PE ATM   → {sell_pe_sym}")

        # ── Lot size (from OpenAlgo token DB) ─────────────────────────────────
        lot_size = _get_lot_size(sell_ce_sym)
        qty      = N_LOTS * lot_size
        logger.info(f"  Lot size={lot_size}  N_LOTS={N_LOTS}  Qty/leg={qty}")

        # ── Fetch entry premiums ──────────────────────────────────────────────
        buy_ce_prem  = _get_option_price(buy_ce_sym)
        buy_pe_prem  = _get_option_price(buy_pe_sym)
        sell_ce_prem = _get_option_price(sell_ce_sym)
        sell_pe_prem = _get_option_price(sell_pe_sym)

        if any(p <= 0 for p in [buy_ce_prem, buy_pe_prem, sell_ce_prem, sell_pe_prem]):
            logger.error(
                f"  Zero premium on ≥1 leg — aborting.\n"
                f"  BUY_CE={buy_ce_prem}  BUY_PE={buy_pe_prem}  "
                f"SELL_CE={sell_ce_prem}  SELL_PE={sell_pe_prem}"
            )
            return "error"

        net_credit   = sell_ce_prem + sell_pe_prem - buy_ce_prem - buy_pe_prem
        sl_total     = SL_PER_LOT * N_LOTS    # ₹20,000

        logger.info(
            f"  Premiums — BUY_CE={buy_ce_prem:.2f}  BUY_PE={buy_pe_prem:.2f}  "
            f"SELL_CE={sell_ce_prem:.2f}  SELL_PE={sell_pe_prem:.2f}"
        )
        logger.info(f"  Net credit/unit: ₹{net_credit:.2f}")
        logger.info(f"  SL total: ₹{sl_total:,.0f}  (₹{SL_PER_LOT}/lot × {N_LOTS} lots)")

        # ── Place orders: BUY legs first (margin benefit), then SELL legs ─────
        logger.info("  Placing orders — BUY legs first, then SELL legs")
        r_buy_ce  = self._placeorder(buy_ce_sym,  "BUY",  qty)
        await asyncio.sleep(0.5)
        r_buy_pe  = self._placeorder(buy_pe_sym,  "BUY",  qty)
        await asyncio.sleep(0.5)
        r_sell_ce = self._placeorder(sell_ce_sym, "SELL", qty)
        await asyncio.sleep(0.5)
        r_sell_pe = self._placeorder(sell_pe_sym, "SELL", qty)

        # ── Resolve actual fills (booking basis; LTP above still drove filter math) ──
        fill_buy_ce  = _resolve_fill(r_buy_ce,  buy_ce_prem)
        fill_buy_pe  = _resolve_fill(r_buy_pe,  buy_pe_prem)
        fill_sell_ce = _resolve_fill(r_sell_ce, sell_ce_prem)
        fill_sell_pe = _resolve_fill(r_sell_pe, sell_pe_prem)
        logger.info(
            f"  Fill vs LTP — BUY_CE: {fill_buy_ce:.2f}/{buy_ce_prem:.2f}  "
            f"BUY_PE: {fill_buy_pe:.2f}/{buy_pe_prem:.2f}  "
            f"SELL_CE: {fill_sell_ce:.2f}/{sell_ce_prem:.2f}  "
            f"SELL_PE: {fill_sell_pe:.2f}/{sell_pe_prem:.2f}"
        )
        net_credit = fill_sell_ce + fill_sell_pe - fill_buy_ce - fill_buy_pe

        # ── Log to CSV (fill-based) ─────────────────────────────────────────────
        _log_csv("BUY",  "buy_ce",  buy_ce_sym,  fill_buy_ce,  qty, 0.0, "entry")
        _log_csv("BUY",  "buy_pe",  buy_pe_sym,  fill_buy_pe,  qty, 0.0, "entry")
        _log_csv("SELL", "sell_ce", sell_ce_sym, fill_sell_ce, qty, 0.0, "entry")
        _log_csv("SELL", "sell_pe", sell_pe_sym, fill_sell_pe, qty, 0.0, "entry")

        # ── Persist state ─────────────────────────────────────────────────────
        exit_date  = _get_exit_date(expiry_date)
        self.state = {
            "strategy":            STRATEGY_NAME,
            "paper_mode":          PAPER_MODE,
            "closed":              False,
            "trade_date":          date.today().isoformat(),
            "entry_time":          datetime.now().isoformat(),
            "expiry_str":          expiry_str,
            "expiry_date":         expiry_date.isoformat(),
            "exit_date":           exit_date.isoformat(),
            "lot_size":            lot_size,
            "n_lots":              N_LOTS,
            "qty":                 qty,
            "atm_strike":          atm,
            "vix_at_entry":        round(vix, 2),
            "nifty_prev_close":    round(prev_close, 2),
            "ma20_at_entry":       round(ma20, 2),
            "net_credit_per_unit": round(net_credit, 2),
            "stop_loss_total":     sl_total,
            "legs": {
                "sell_ce": {"symbol": sell_ce_sym, "entry_prem": round(fill_sell_ce, 2), "strike": atm},
                "sell_pe": {"symbol": sell_pe_sym, "entry_prem": round(fill_sell_pe, 2), "strike": atm},
                "buy_ce":  {"symbol": buy_ce_sym,  "entry_prem": round(fill_buy_ce, 2),  "strike": hedge_ce_k},
                "buy_pe":  {"symbol": buy_pe_sym,  "entry_prem": round(fill_buy_pe, 2),  "strike": hedge_pe_k},
            },
            "current_mtm":         0.0,   # updated on each poll
            "sl_hit":              False,
            "exit_reason":         None,
        }
        save_state(self.state)

        await send_async(
            f"⚡ *{STRATEGY_NAME}* — ENTRY\n"
            f"Expiry: {expiry_str}  Scheduled exit: {exit_date} 15:15\n"
            f"ATM: {atm}  Lots: {N_LOTS}  Qty/leg: {qty}  LS: {lot_size}\n"
            f"Net credit/unit: ₹{net_credit:.2f}\n"
            f"SL total: ₹{sl_total:,.0f}  (₹{SL_PER_LOT}/lot)\n"
            f"VIX={vix:.2f}  NIFTY={prev_close:.1f}  MA20={ma20:.1f}\n"
            f"Legs:\n"
            f"  BUY  {buy_ce_sym}  @ ₹{fill_buy_ce:.2f}\n"
            f"  BUY  {buy_pe_sym}  @ ₹{fill_buy_pe:.2f}\n"
            f"  SELL {sell_ce_sym} @ ₹{fill_sell_ce:.2f}\n"
            f"  SELL {sell_pe_sym} @ ₹{fill_sell_pe:.2f}\n"
            f"Paper: {PAPER_MODE}"
        )
        return "success"

    # ──────────────────────────────────────────────────────────────────────────
    #  MTM COMPUTATION
    # ──────────────────────────────────────────────────────────────────────────

    def _compute_mtm(self) -> tuple[float, dict[str, float]]:
        """
        Fetch current prices for all 4 legs and compute combined MTM (₹).

        MTM sign convention (seller's perspective):
          Sell leg: profit = (entry_prem − current_prem) × qty   [positive if decayed]
          Buy  leg: profit = (current_prem − entry_prem) × qty   [positive if appreciated]
        """
        legs = self.state["legs"]
        qty  = self.state["qty"]

        prices = {
            "sell_ce": _get_option_price(legs["sell_ce"]["symbol"]),
            "sell_pe": _get_option_price(legs["sell_pe"]["symbol"]),
            "buy_ce":  _get_option_price(legs["buy_ce"]["symbol"]),
            "buy_pe":  _get_option_price(legs["buy_pe"]["symbol"]),
        }

        mtm_sc = (legs["sell_ce"]["entry_prem"] - prices["sell_ce"]) * qty
        mtm_sp = (legs["sell_pe"]["entry_prem"] - prices["sell_pe"]) * qty
        mtm_bc = (prices["buy_ce"] - legs["buy_ce"]["entry_prem"])   * qty
        mtm_bp = (prices["buy_pe"] - legs["buy_pe"]["entry_prem"])   * qty

        combined = mtm_sc + mtm_sp + mtm_bc + mtm_bp
        return combined, prices

    # ──────────────────────────────────────────────────────────────────────────
    #  EXIT
    # ──────────────────────────────────────────────────────────────────────────

    async def _exit(self, reason: str, current_prices: dict[str, float]) -> None:
        """
        Close all 4 legs.
        Order sequence: BUY back sell legs first → SELL buy legs second
        (mirrors entry sequence convention: unwind in reverse direction)
        reason: 'stop_loss' | 'scheduled' | 'forced_expiry'
        """
        logger.info("═" * 64)
        logger.info(f"🔴 IRON FLY EXIT — {reason}")

        legs     = self.state["legs"]
        qty      = self.state["qty"]
        lot_size = self.state["lot_size"]

        # ── Close orders: BUY back sell legs first ────────────────────────────
        logger.info("  Closing sell legs (BUY orders) first…")
        r_sell_ce = self._placeorder(legs["sell_ce"]["symbol"], "BUY",  qty)
        await asyncio.sleep(0.5)
        r_sell_pe = self._placeorder(legs["sell_pe"]["symbol"], "BUY",  qty)
        await asyncio.sleep(0.5)
        logger.info("  Closing buy legs (SELL orders) second…")
        r_buy_ce = self._placeorder(legs["buy_ce"]["symbol"],  "SELL", qty)
        await asyncio.sleep(0.5)
        r_buy_pe = self._placeorder(legs["buy_pe"]["symbol"],  "SELL", qty)

        # ── Resolve actual fills (booking basis) ──────────────────────────────
        exit_fills = {
            "sell_ce": _resolve_fill(r_sell_ce, current_prices.get("sell_ce", legs["sell_ce"]["entry_prem"])),
            "sell_pe": _resolve_fill(r_sell_pe, current_prices.get("sell_pe", legs["sell_pe"]["entry_prem"])),
            "buy_ce":  _resolve_fill(r_buy_ce,  current_prices.get("buy_ce",  legs["buy_ce"]["entry_prem"])),
            "buy_pe":  _resolve_fill(r_buy_pe,  current_prices.get("buy_pe",  legs["buy_pe"]["entry_prem"])),
        }

        # ── P&L ───────────────────────────────────────────────────────────────
        def _pnl(key: str, side: str) -> float:
            ep = legs[key]["entry_prem"]
            xp = exit_fills[key]
            return (ep - xp) * qty if side == "sell" else (xp - ep) * qty

        pnl_sc = _pnl("sell_ce", "sell")
        pnl_sp = _pnl("sell_pe", "sell")
        pnl_bc = _pnl("buy_ce",  "buy")
        pnl_bp = _pnl("buy_pe",  "buy")
        total  = pnl_sc + pnl_sp + pnl_bc + pnl_bp

        logger.info("  P&L breakdown (gross, pre-cost):")
        logger.info(f"    sell_ce: ₹{pnl_sc:>10,.0f}")
        logger.info(f"    sell_pe: ₹{pnl_sp:>10,.0f}")
        logger.info(f"    buy_ce : ₹{pnl_bc:>10,.0f}")
        logger.info(f"    buy_pe : ₹{pnl_bp:>10,.0f}")
        logger.info(f"    TOTAL  : ₹{total:>10,.0f}")

        # ── CSV log (fill-based) ────────────────────────────────────────────────
        _log_csv("BUY",  "sell_ce", legs["sell_ce"]["symbol"], exit_fills["sell_ce"], qty, pnl_sc, reason)
        _log_csv("BUY",  "sell_pe", legs["sell_pe"]["symbol"], exit_fills["sell_pe"], qty, pnl_sp, reason)
        _log_csv("SELL", "buy_ce",  legs["buy_ce"]["symbol"],  exit_fills["buy_ce"],  qty, pnl_bc, reason)
        _log_csv("SELL", "buy_pe",  legs["buy_pe"]["symbol"],  exit_fills["buy_pe"],  qty, pnl_bp, reason)

        # ── DB trade log ──────────────────────────────────────────────────────
        try:
            log_trade_to_db(
                bot_name      = "nifty_iron_fly_weekly_bot",
                instrument    = IDX_SYMBOL,
                option_symbol = legs["sell_ce"]["symbol"],
                option_type   = "IRON_FLY",
                entry_time    = datetime.fromisoformat(self.state["entry_time"]),
                exit_time     = datetime.now(),
                entry_premium = self.state["net_credit_per_unit"],
                exit_premium  = 0.0,
                exit_reason   = reason,
                quantity      = qty,
                lots          = N_LOTS,
                lot_size      = lot_size,
                gross_pnl     = round(total, 2),
                notes=(
                    f"sell_ce={legs['sell_ce']['symbol']}@{legs['sell_ce']['entry_prem']} | "
                    f"sell_pe={legs['sell_pe']['symbol']}@{legs['sell_pe']['entry_prem']} | "
                    f"buy_ce={legs['buy_ce']['symbol']}@{legs['buy_ce']['entry_prem']} | "
                    f"buy_pe={legs['buy_pe']['symbol']}@{legs['buy_pe']['entry_prem']}"
                ),
            )
        except Exception as e:
            logger.warning(f"  DB log failed: {e}")

        # ── Update + close state ──────────────────────────────────────────────
        self.state.update({
            "closed":      True,
            "sl_hit":      (reason == "stop_loss"),
            "exit_reason": reason,
            "exit_time":   datetime.now().isoformat(),
            "total_pnl":   round(total, 2),
        })
        save_state(self.state)
        self.state = None

        emoji = "🛑" if reason == "stop_loss" else "⏰"
        await send_async(
            f"{emoji} *{STRATEGY_NAME}* — EXIT ({reason})\n"
            f"Gross P&L: ₹{total:,.0f}\n"
            f"sell\\_ce=₹{pnl_sc:,.0f}  sell\\_pe=₹{pnl_sp:,.0f}\n"
            f"buy\\_ce=₹{pnl_bc:,.0f}   buy\\_pe=₹{pnl_bp:,.0f}\n"
            f"Paper: {PAPER_MODE}"
        )

    # ──────────────────────────────────────────────────────────────────────────
    #  MAIN LOOP
    # ──────────────────────────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info("=" * 64)
        logger.info(f"🚀  {STRATEGY_NAME}  |  PAPER={PAPER_MODE}")
        logger.info(f"    N_LOTS={N_LOTS}  SL=₹{SL_PER_LOT:,.0f}/lot  "
                    f"(₹{SL_PER_LOT * N_LOTS:,.0f} total)")
        logger.info(f"    Filters: VIX≥{VIX_MIN}  AND  NIFTY≥MA{MA_PERIOD}")
        logger.info(f"    Entry: Mon/Wed 10:00 IST (Thu/Tue expiry respectively)")
        logger.info(f"    Exit:  15:15 day-before-expiry  OR  SL hit")
        logger.info(f"    Product: NRML (overnight multi-day hold)")

        # Restore open position from previous session
        self.state = load_state()
        if self.state:
            logger.info(f"♻️  Restored open position from {self.state.get('entry_time', '?')}")
            logger.info(f"   Expiry: {self.state.get('expiry_str')}  "
                        f"Exit date: {self.state.get('exit_date')}")
        else:
            logger.info("   No open position — awaiting entry day/time.")

        while True:
            now   = datetime.now()
            today = now.date()
            t     = now.time()

            # Outside market hours → sleep
            before_open = t < dt_time(9, 15)
            after_close = t.hour > 15 or (t.hour == 15 and t.minute >= 30)
            if before_open or after_close:
                await asyncio.sleep(60)
                continue

            # ── CASE A: Active position — monitor ─────────────────────────────
            if self.state and not self.state.get("closed"):
                expiry_date = date.fromisoformat(self.state["expiry_date"])
                exit_date   = date.fromisoformat(self.state["exit_date"])

                # Scheduled 15:15 exit on day before expiry
                if (today == exit_date
                        and t.hour == EXIT_HOUR
                        and t.minute >= EXIT_MINUTE):
                    combined_mtm, prices = self._compute_mtm()
                    await self._exit("scheduled", prices)
                    await asyncio.sleep(POLL_INTERVAL_SECS)
                    continue

                # Safety net: position still open on expiry day itself → force close
                if today >= expiry_date:
                    logger.warning("  ⚠️  Open position on/after expiry — forcing close.")
                    _, prices = self._compute_mtm()
                    await self._exit("forced_expiry", prices)
                    await asyncio.sleep(POLL_INTERVAL_SECS)
                    continue

                # Normal MTM poll
                combined_mtm, prices = self._compute_mtm()
                # A failed quote (LTP=0) fakes ±entry_prem×qty of MTM and can
                # falsely trigger the SL — skip this poll instead
                if any(p <= 0 for p in prices.values()):
                    logger.warning("  Quote failure (LTP=0) on a leg — skipping SL check this poll.")
                    await asyncio.sleep(POLL_INTERVAL_SECS)
                    continue
                sl_total = self.state["stop_loss_total"]

                # Update state with current MTM for dashboard visibility
                self.state["current_mtm"] = round(combined_mtm, 0)
                save_state(self.state)

                logger.info(
                    f"📊 MTM={combined_mtm:+,.0f}  SL@-{sl_total:,.0f}  |  "
                    f"SC={prices.get('sell_ce', 0):.1f}  "
                    f"SP={prices.get('sell_pe', 0):.1f}  "
                    f"BC={prices.get('buy_ce', 0):.1f}  "
                    f"BP={prices.get('buy_pe', 0):.1f}"
                )

                if combined_mtm <= -sl_total:
                    logger.warning(
                        f"  🛑 SL HIT: MTM=₹{combined_mtm:,.0f} ≤ -₹{sl_total:,.0f}"
                    )
                    await self._exit("stop_loss", prices)
                else:
                    await asyncio.sleep(POLL_INTERVAL_SECS)
                continue

            # ── CASE B: No active position — check entry day or manual trigger ──
            # 1. Check for manual force-entry flag first
            force_flag = LOG_DIR / "confirm_nifty_if.flag"
            if force_flag.exists():
                logger.info("⚡ Manual entry flag detected! Bypassing scheduled checks.")
                try:
                    force_flag.unlink()
                except Exception as e:
                    logger.warning(f"Could not remove manual flag file: {e}")

                expiry_str, expiry_date = _get_upcoming_expiry()
                if expiry_str:
                    logger.info(f"⚡ Executing manual entry for expiry={expiry_str}...")
                    res = await self._enter(expiry_str, expiry_date, force=True)
                    if res == "success":
                        self.state = load_state()
                        logger.info("✅ Manual position entered successfully — starting MTM monitoring.")
                    else:
                        logger.error(f"❌ Manual entry failed with status: {res}")
                else:
                    logger.error("❌ Cannot determine upcoming expiry for manual entry.")
                continue

            # 2. Normal scheduled checking
            expiry_str, expiry_date = _get_upcoming_expiry()
            if not expiry_str:
                logger.warning("  Cannot determine upcoming expiry — retry in 5 min.")
                await asyncio.sleep(300)
                continue

            try:
                entry_date = _get_entry_date(expiry_date)
            except ValueError as e:
                logger.error(f"  Schedule error: {e}")
                await asyncio.sleep(300)
                continue

            if today != entry_date:
                if t.minute == 0:   # log once per hour to avoid noise
                    logger.info(
                        f"  Standby — today={today}  entry={entry_date}  expiry={expiry_date}"
                    )
                await asyncio.sleep(300)
                continue

            # ── Today is entry day ─────────────────────────────────────────────
            entry_open  = now.replace(hour=ENTRY_HOUR, minute=ENTRY_MINUTE, second=0, microsecond=0)
            entry_close = entry_open + timedelta(minutes=ENTRY_WINDOW_MINS)

            if now < entry_open:
                wait_s = int((entry_open - now).total_seconds())
                logger.info(
                    f"  Entry day {entry_date} — waiting {wait_s}s until 10:00 IST."
                )
                await asyncio.sleep(min(wait_s, 60))
                continue

            if now > entry_close:
                logger.info(f"  Entry window closed for {entry_date} — no trade today.")
                await asyncio.sleep(3_600)
                continue

            # In window → execute entry
            res = await self._enter(expiry_str, expiry_date)

            if res == "success":
                self.state = load_state()
                logger.info("  ✅ Position entered — starting MTM monitoring.")
            elif res == "skip_filters":
                logger.info("  Filters failed structurally. Done for today.")
                await asyncio.sleep(3_600)  # Sleep 1 hour, entry window will close
            else:
                # res == "error"
                logger.warning("  API or Execution error during entry. Retrying in 60 seconds...")
                await asyncio.sleep(60)   # Retry in 1 minute while window is open


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    bot = IronFlyBot()
    asyncio.run(bot.run())
