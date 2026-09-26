#!/usr/bin/env python3
"""
sensex_iron_fly_weekly_bot.py — SENSEX Weekly Short Iron Fly (Paper Trading)
=============================================================================
Champion C4 parameters (sensex_iron_fly_weekly_study, Stage 10 cleared 2026-05-13):

  hedge_delta  : 0.10         (Black-76, OTM legs)
  stop_loss    : None         (no stop loss — adjustment mechanism manages risk)
  profit_target: 50% of net credit (exit when combined MTM ≥ 50% × entry credit)
  adj_low_trig : 0.20         (adjust if short-leg |delta| falls below 0.20)
  adj_hi_trig  : 0.70         (adjust if short-leg |delta| rises above 0.70)
  adj_hysteresis: 2 polls     (trigger must persist across 2 consecutive 30s polls)
  filter       : SENSEX prev_close ≥ SENSEX MA20  (no VIX filter — MA20 alone)

4 legs — all NRML (positions carried overnight 4 trading days):
  BUY   OTM CE   (delta ≈ 0.10)  ← placed FIRST for margin benefit
  BUY   OTM PE   (delta ≈ 0.10)  ← placed SECOND
  SELL  ATM CE                   ← placed THIRD
  SELL  ATM PE                   ← placed FOURTH

Entry:  10:00–10:20 IST on the regime-defined entry day for that expiry week
        (currently: Friday of the prior week for the Thursday-expiry regime)
Exit:   (a) Profit target:  combined MTM ≥ 50% × net_credit × qty → close all 4 legs
        (b) Scheduled:      15:15 IST on the day before the actual OpenAlgo expiry
        (c) Adjustment:     if short-leg |delta| exits [0.20, 0.70] for 2 consecutive
                            polls → buy back that short leg + re-sell ATM in same type

For closing:
  BUY  close order for the two SELL legs  → FIRST for margin unwind
  SELL close order for the two BUY legs  → SECOND

Expiry schedule (SENSEX):
  Sep 2025+  → Thursday weekly expiry (current regime; may be pre-poned on holidays)
  Jan–Aug 2025 → Tuesday weekly expiry (historical OOS)
  Pre-2025   → Friday weekly expiry (historical IS)

Implementation notes:
  • Uses placeorder (not placesmartorder) — avoids the position_size pitfall.
  • Symbols resolved via OpenAlgo /api/v1/optionsymbol — broker-agnostic.
  • Lot size fetched from OpenAlgo token DB (currently 20 for SENSEX post-Nov 2025).
  • 10 lots × 20 units = 200 units per cycle (standardised position size).
  • State written to logs/sensex_iron_fly_weekly_state.json — crash-safe.
  • Delta computed via Black-76 using INDIAVIX as sigma proxy.
  • Adjustment closes only the triggered short leg; other 3 legs remain.
  • VIX is NOT an entry filter — used only for delta calculation.
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
PROJECT_ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from openalgo import api                                                     # noqa
from live_trading.api_utils import HOST, get_expiry_dates, get_history, is_market_holiday  # noqa
from live_trading.shared.order_fill import fetch_fill_price                   # noqa
from live_trading.shared.telegram_notifier import send_async                  # noqa
from live_trading.shared.trade_logger import log_trade_to_db                  # noqa
from live_trading.shared.poll_watchdog import PollWatchdog                    # noqa

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "sensex_iron_fly_weekly_bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

API_KEY          = os.getenv("OPENALGO_API_KEY")
PAPER_MODE       = False   # Set to False to place orders in sandbox (analyzer mode)

STRATEGY_NAME    = "SENSEX_IRON_FLY_WEEKLY"
IDX_SYMBOL       = "SENSEX"
IDX_EXCHANGE     = "BSE_INDEX"
VIX_SYMBOL       = "INDIAVIX"
VIX_EXCHANGE     = "NSE_INDEX"
OPT_EXCHANGE     = "BFO"

# Position sizing — standardised: 10 lots per CLAUDE.md position-size rule
N_LOTS           = 10
DEFAULT_LOT_SIZE = 20      # SENSEX lot size post-Nov 2025

ATM_STEP         = 100     # SENSEX strike increment (vs NIFTY's 50)
RISK_FREE_RATE   = 0.065   # India risk-free rate

# Champion C4 parameters
HEDGE_DELTA      = 0.10    # OTM hedge leg target delta
PROFIT_TARGET    = 0.50    # exit when MTM ≥ 50% of net entry credit
ADJ_LOW_TRIG     = 0.20    # adjust if short-leg |delta| < 0.20
ADJ_HI_TRIG      = 0.70    # adjust if short-leg |delta| > 0.70
ADJ_HYSTERESIS   = 2       # consecutive polls before triggering adjustment

# Timing (IST)
ENTRY_HOUR          = 10
ENTRY_MINUTE        = 0
ENTRY_WINDOW_MINS   = 20   # accept entry until 10:20
TEMP_ENTRY_OVERRIDE_DATE = date(2026, 6, 29)   # holiday-rollover recovery (expired)
TEMP_ENTRY_CLOSE_HOUR    = 10
TEMP_ENTRY_CLOSE_MINUTE  = 20
EXIT_HOUR           = 15
EXIT_MINUTE         = 15   # scheduled exit on the day before the actual expiry

# MA filter (no VIX filter — Stage 8 confirmed MA20 alone is the champion)
MA_PERIOD = 20

# State and logs
STATE_FILE       = LOG_DIR / "sensex_iron_fly_weekly_state.json"
PAPER_TRADES_CSV = LOG_DIR / "sensex_iron_fly_weekly_paper_trades.csv"

POLL_INTERVAL_SECS = 30    # MTM / delta monitoring frequency


# ══════════════════════════════════════════════════════════════════════════════
# BLACK-76 GREEKS (inline — no numba dependency in live bot)
# ══════════════════════════════════════════════════════════════════════════════

def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erf — matches greeks_engine.py."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _black76_delta(F: float, K: float, T: float, sigma: float, r: float,
                   opt_type: str) -> float:
    """
    Black-76 delta for European cash-settled index options.
    opt_type: 'CE' → positive delta  |  'PE' → negative delta (returned as negative float)
    """
    if T <= 0 or sigma <= 0 or F <= 0 or K <= 0:
        return 0.0
    d1   = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    disc = math.exp(-r * T)
    if opt_type == "CE":
        return disc * _norm_cdf(d1)
    else:
        return disc * (_norm_cdf(d1) - 1.0)


def _compute_delta(spot: float, strike: int, expiry_date: date,
                   vix: float, opt_type: str) -> float:
    """
    Compute Black-76 |delta| for a short leg at current market conditions.
    Returns absolute delta value (always positive for comparison with thresholds).
    """
    T     = max((expiry_date - date.today()).days / 365.0, 1 / 365.0)
    sigma = max(vix / 100.0, 0.05)   # floor at 5% to avoid degenerate calcs
    delta = _black76_delta(spot, float(strike), T, sigma, RISK_FREE_RATE, opt_type)
    return abs(delta)


def find_hedge_strike(spot: float, expiry_date: date, vix: float,
                       opt_type: str, target_delta: float = HEDGE_DELTA) -> int:
    """
    Walk OTM strikes in ATM_STEP increments; return the one whose
    Black-76 |delta| is closest to target_delta.
    CE → strikes above ATM.  PE → strikes below ATM.
    """
    T     = max((expiry_date - date.today()).days / 365.0, 1 / 365.0)
    sigma = max(vix / 100.0, 0.05)
    F     = spot
    atm   = int(round(spot / ATM_STEP) * ATM_STEP)
    step  = 1 if opt_type == "CE" else -1

    best_strike = atm + step * ATM_STEP
    best_diff   = float("inf")

    for n in range(1, 50):
        K     = atm + step * n * ATM_STEP
        delta = _black76_delta(F, float(K), T, sigma, RISK_FREE_RATE, opt_type)
        diff  = abs(abs(delta) - target_delta)
        if diff < best_diff:
            best_diff   = diff
            best_strike = K
        if abs(delta) < target_delta - 0.03:
            break

    logger.info(
        f"  Hedge {opt_type}: strike={best_strike}  target_Δ={target_delta:.2f}  "
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
    Resolve SENSEX option symbol via OpenAlgo /api/v1/optionsymbol.
    Uses offset-based resolution (OTM1, OTM2… / ITM1… / ATM) which is
    broker-agnostic and does not rely on strike_int being in master contracts.

    strike=None          → ATM (offset="ATM")
    strike=int, atm=int  → offset computed from ATM distance
    """
    payload: dict = {
        "apikey":      API_KEY,
        "underlying":  IDX_SYMBOL,
        "exchange":    OPT_EXCHANGE,
        "expiry_date": expiry_str,
        "option_type": opt_type,
    }

    if strike is None:
        payload["offset"] = "ATM"
    elif atm_strike is not None:
        diff  = strike - atm_strike
        steps = abs(diff) // ATM_STEP
        if steps == 0:
            payload["offset"] = "ATM"
        elif diff > 0:
            payload["offset"] = f"OTM{steps}" if opt_type == "CE" else f"ITM{steps}"
        else:
            payload["offset"] = f"ITM{steps}" if opt_type == "CE" else f"OTM{steps}"
    else:
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
        else:
            logger.warning(
                f"  optionsymbol HTTP {res.status_code} "
                f"(exp={expiry_str} {opt_type} k={strike}): {res.text[:200]}"
            )
    except Exception as e:
        logger.error(f"  optionsymbol API error: {e}")

    # Fallback: build symbol in OpenAlgo format SENSEX{DDMMMYY}{STRIKE}{CE/PE}
    # e.g. SENSEX15MAY2681000CE
    if strike is not None and atm_strike is not None:
        sym = f"{IDX_SYMBOL}{expiry_str}{strike}{opt_type}"
        logger.warning(f"  Fallback symbol: {sym}")
        return sym
    return None


# ══════════════════════════════════════════════════════════════════════════════
# EXPIRY / SCHEDULE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _parse_expiry_str(expiry_str: str) -> date:
    """'15MAY26' → date(2026, 5, 15)"""
    return datetime.strptime(expiry_str, "%d%b%y").date()


def _expected_expiry_weekday(expiry_date: date) -> int:
    """
    Return the nominal weekly expiry weekday for the given date regime.

    We still trust the actual OpenAlgo expiry date as source of truth, because
    exchange holidays can pre-pone the live expiry by one or more days.
    """
    if expiry_date >= date(2025, 9, 1):
        return 3  # Thursday
    if expiry_date >= date(2025, 1, 1):
        return 1  # Tuesday
    return 4      # Friday (historical IS regime)


def _next_trading_day(d: date) -> date:
    """Advance d forward until it lands on a weekday that is not a market holiday."""
    while True:
        if d.weekday() < 5 and not is_market_holiday(API_KEY, d.strftime("%Y-%m-%d")):
            return d
        d += timedelta(days=1)


def _get_entry_date(expiry_date: date) -> date:
    """
    Compute the strategy's entry date from the current SENSEX expiry regime while
    accepting the actual OpenAlgo expiry date as the live source of truth.

    Current regime (Sep 2025+):
      nominal expiry weekday = Thursday
      normal entry day       = Friday of the prior week

    If the exchange pre-pones the weekly expiry because of a holiday, OpenAlgo may
    return Wednesday / Tuesday / Monday instead of Thursday. In that case the bot
    should still enter on the same regime-defined anchor day for that expiry week:
    the previous Friday.

    If the computed anchor day is itself a market holiday, the entry rolls forward
    to the next available trading day.
    """
    regime_dow = _expected_expiry_weekday(expiry_date)
    actual_dow = expiry_date.weekday()

    if actual_dow >= 5:
        raise ValueError(f"Unexpected non-weekday SENSEX expiry for {expiry_date}.")

    expiry_week_monday = expiry_date - timedelta(days=actual_dow)

    if regime_dow == 3:      # Thursday regime → previous Friday
        anchor = expiry_week_monday - timedelta(days=3)
    elif regime_dow == 1:    # Tuesday regime → previous Wednesday
        anchor = expiry_week_monday - timedelta(days=5)
    elif regime_dow == 4:    # Friday regime → previous Monday
        anchor = expiry_week_monday - timedelta(days=7)
    else:
        raise ValueError(f"Unsupported SENSEX expiry regime weekday={regime_dow}.")

    return _next_trading_day(anchor)


def _get_exit_date(expiry_date: date) -> date:
    """Scheduled 15:15 close is on the day BEFORE expiry (Wednesday for Thursday expiry)."""
    return expiry_date - timedelta(days=1)


def _get_upcoming_expiry() -> tuple[str, date] | tuple[None, None]:
    """
    Fetch nearest SENSEX weekly BFO expiry with ≥2 DTE from OpenAlgo.
    Returns (expiry_str_DDMMMYY, expiry_date) or (None, None).
    """
    dates = get_expiry_dates(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, "options")
    if not dates:
        logger.error("Could not fetch SENSEX expiry dates from OpenAlgo.")
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
            timeout=5,
        )
        if res.status_code == 200:
            data = res.json()
            if data.get("status") == "success":
                qd = data.get("data", {})
                if isinstance(qd, dict):
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


def _get_vix()         -> float: return _quote(VIX_SYMBOL, VIX_EXCHANGE)
def _get_sensex_spot() -> float: return _quote(IDX_SYMBOL,  IDX_EXCHANGE)


def _get_option_price(symbol: str, retries: int = 3, delay: float = 5.0) -> float:
    """
    Fetch option LTP with retries.  At 10:00:00 the order book can be cold;
    a single shot is fragile.  Retries up to `retries` times with `delay` seconds.
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
    Load last 35 SENSEX daily bars, compute MA20, check prev_close ≥ MA20.
    Returns (passes, prev_close, ma20).
    FAILS on data insufficiency (stricter than allowing by default).
    """
    hist = get_history(API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "D", 35)
    if not hist:
        logger.warning("  MA20 filter: no SENSEX history data received — FAILING entry.")
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
    Query OpenAlgo token DB for current SENSEX lot size.
    Falls back to DEFAULT_LOT_SIZE (20) on any error.
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

class SensexIronFlyBot:
    """
    SENSEX Weekly Short Iron Fly — paper trading bot.
    Champion C4: hedge_delta=0.10, PT=50%, adj [0.20–0.70], no SL.
    Polling-based (no WebSocket needed — fixed entry schedule, 30s MTM/delta poll).
    """

    def __init__(self):
        self.client  = api(api_key=API_KEY, host=HOST)
        self.state:  dict | None = None
        # Per-leg consecutive-trigger counters for adjustment hysteresis
        self._adj_consec: dict[str, int] = {"sell_ce": 0, "sell_pe": 0}

        # REST-poll watchdog — catches frozen/stale quotes and consecutive
        # poll failures on the option legs + index spot polled every
        # POLL_INTERVAL_SECS. Market hours mirror this bot's own run() loop
        # bounds (9:15-15:30).
        self._watchdog = PollWatchdog(
            bot_name="SENSEX Iron Fly Weekly Bot",
            market_open=dt_time(9, 15),
            market_close=dt_time(15, 30),
            bot_logger=logger,
        )

    # ──────────────────────────────────────────────────────────────────────────
    #  ORDER PLACEMENT
    # ──────────────────────────────────────────────────────────────────────────

    def _placeorder(self, symbol: str, action: str, qty: int) -> dict:
        """
        Place a plain NRML market order via OpenAlgo placeorder (not placesmartorder).
        In PAPER_MODE, simulates and returns a dummy response.
        """
        if PAPER_MODE:
            logger.info(f"  📝 PAPER: {action} {qty}× {symbol} @ MARKET NRML BFO")
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

    async def _enter(self, expiry_str: str, expiry_date: date) -> None:
        """Evaluate MA20 filter and open all 4 legs of the iron fly."""
        logger.info("═" * 64)
        logger.info("⚡ SENSEX IRON FLY ENTRY — evaluating filters")

        # ── Filter: SENSEX prev_close ≥ SENSEX MA20 ──────────────────────────
        # (No VIX filter — Stage 8 confirmed MA20 alone is the champion)
        ma_pass, prev_close, ma20 = _check_ma20_filter()
        # prev_close/ma20 both 0.0 means data error (not a genuine filter fail).
        # Retry up to 3 times so a transient history-API hiccup at 10:00 IST
        # doesn't silently skip the week.
        if not ma_pass and prev_close == 0.0 and ma20 == 0.0:
            import time as _time
            for _attempt in range(3):
                logger.warning(
                    f"  MA20 filter: data error on attempt {_attempt + 1}/3 — retrying in 5s…"
                )
                _time.sleep(5)
                ma_pass, prev_close, ma20 = _check_ma20_filter()
                if not (prev_close == 0.0 and ma20 == 0.0):
                    break  # got real data (pass or genuine fail)
        if not ma_pass:
            if prev_close == 0.0 and ma20 == 0.0:
                logger.error("  MA20 filter: data error after 3 retries — aborting entry.")
                return
            logger.info(f"  ❌ MA20 FAIL: SENSEX={prev_close:.1f} < MA20={ma20:.1f}")
            await send_async(
                f"🚫 *{STRATEGY_NAME}* — MA20 filter FAIL\n"
                f"SENSEX close={prev_close:.1f} < MA20={ma20:.1f}. Skipping this week."
            )
            return
        logger.info(f"  ✅ MA20 PASS: SENSEX={prev_close:.1f} ≥ MA20={ma20:.1f}")

        # ── SENSEX spot → ATM strike ──────────────────────────────────────────
        spot = _get_sensex_spot()
        if spot <= 0:
            # Retry up to 3 times — server may be under load at 10:00 IST
            import time as _time
            for _attempt in range(3):
                logger.warning(
                    f"  SENSEX spot = 0 (attempt {_attempt + 1}/3) — retrying in 5s…"
                )
                _time.sleep(5)
                spot = _get_sensex_spot()
                if spot > 0:
                    break
        if spot <= 0:
            logger.error("  SENSEX spot = 0 after 3 retries — aborting entry.")
            return
        atm = int(round(spot / ATM_STEP) * ATM_STEP)
        logger.info(f"  SENSEX spot={spot:.1f}  ATM={atm}")

        # ── VIX for hedge strike computation (not an entry filter) ────────────
        vix = _get_vix()
        if vix <= 0:
            logger.warning("  VIX unavailable — using 15.0 as sigma proxy for hedge strikes.")
            vix = 15.0

        # ── Hedge strikes via Black-76 ────────────────────────────────────────
        hedge_ce_k = find_hedge_strike(spot, expiry_date, vix, "CE", HEDGE_DELTA)
        hedge_pe_k = find_hedge_strike(spot, expiry_date, vix, "PE", HEDGE_DELTA)

        # ── Resolve all 4 symbols via OpenAlgo API ────────────────────────────
        buy_ce_sym  = _resolve_option_symbol(expiry_str, "CE", strike=hedge_ce_k, atm_strike=atm)
        buy_pe_sym  = _resolve_option_symbol(expiry_str, "PE", strike=hedge_pe_k, atm_strike=atm)
        sell_ce_sym = _resolve_option_symbol(expiry_str, "CE", strike=atm,        atm_strike=atm)
        sell_pe_sym = _resolve_option_symbol(expiry_str, "PE", strike=atm,        atm_strike=atm)

        if not all([buy_ce_sym, buy_pe_sym, sell_ce_sym, sell_pe_sym]):
            logger.error("  Failed to resolve one or more option symbols. Aborting.")
            return

        logger.info("  Symbols resolved:")
        logger.info(f"    BUY  CE hedge  → {buy_ce_sym}")
        logger.info(f"    BUY  PE hedge  → {buy_pe_sym}")
        logger.info(f"    SELL CE ATM    → {sell_ce_sym}")
        logger.info(f"    SELL PE ATM    → {sell_pe_sym}")

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
            return

        net_credit   = sell_ce_prem + sell_pe_prem - buy_ce_prem - buy_pe_prem
        pt_threshold = PROFIT_TARGET * net_credit * qty   # ₹ amount to exit at PT

        logger.info(
            f"  Premiums — BUY_CE={buy_ce_prem:.2f}  BUY_PE={buy_pe_prem:.2f}  "
            f"SELL_CE={sell_ce_prem:.2f}  SELL_PE={sell_pe_prem:.2f}"
        )
        logger.info(f"  Net credit/unit: ₹{net_credit:.2f}")
        logger.info(
            f"  Profit target: ₹{pt_threshold:,.0f}  "
            f"(={PROFIT_TARGET:.0%} × ₹{net_credit:.2f} × {qty} units)"
        )
        logger.info(f"  No stop-loss — adjustment mechanism [Δ {ADJ_LOW_TRIG}–{ADJ_HI_TRIG}] manages risk")

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
        net_credit   = fill_sell_ce + fill_sell_pe - fill_buy_ce - fill_buy_pe
        pt_threshold = PROFIT_TARGET * net_credit * qty   # recomputed on fill-based credit

        # ── Log to CSV (fill-based) ─────────────────────────────────────────────
        _log_csv("BUY",  "buy_ce",  buy_ce_sym,  fill_buy_ce,  qty, 0.0, "entry")
        _log_csv("BUY",  "buy_pe",  buy_pe_sym,  fill_buy_pe,  qty, 0.0, "entry")
        _log_csv("SELL", "sell_ce", sell_ce_sym, fill_sell_ce, qty, 0.0, "entry")
        _log_csv("SELL", "sell_pe", sell_pe_sym, fill_sell_pe, qty, 0.0, "entry")

        # ── Persist state ─────────────────────────────────────────────────────
        exit_date  = _get_exit_date(expiry_date)
        self.state = {
            "strategy":             STRATEGY_NAME,
            "paper_mode":           PAPER_MODE,
            "closed":               False,
            "trade_date":           date.today().isoformat(),
            "entry_time":           datetime.now().isoformat(),
            "expiry_str":           expiry_str,
            "expiry_date":          expiry_date.isoformat(),
            "exit_date":            exit_date.isoformat(),
            "lot_size":             lot_size,
            "n_lots":               N_LOTS,
            "qty":                  qty,
            "atm_strike":           atm,
            "vix_at_entry":         round(vix, 2),
            "sensex_prev_close":    round(prev_close, 2),
            "ma20_at_entry":        round(ma20, 2),
            "net_credit_per_unit":  round(net_credit, 2),
            "pt_threshold":         round(pt_threshold, 2),
            "adj_count":            0,
            "legs": {
                "sell_ce": {
                    "symbol":     sell_ce_sym,
                    "entry_prem": round(fill_sell_ce, 2),
                    "strike":     atm,
                    "open":       True,
                },
                "sell_pe": {
                    "symbol":     sell_pe_sym,
                    "entry_prem": round(fill_sell_pe, 2),
                    "strike":     atm,
                    "open":       True,
                },
                "buy_ce": {
                    "symbol":     buy_ce_sym,
                    "entry_prem": round(fill_buy_ce, 2),
                    "strike":     hedge_ce_k,
                    "open":       True,
                },
                "buy_pe": {
                    "symbol":     buy_pe_sym,
                    "entry_prem": round(fill_buy_pe, 2),
                    "strike":     hedge_pe_k,
                    "open":       True,
                },
            },
            "current_mtm":  0.0,
            "exit_reason":  None,
        }
        # Reset hysteresis counters
        self._adj_consec = {"sell_ce": 0, "sell_pe": 0}
        save_state(self.state)

        await send_async(
            f"⚡ *{STRATEGY_NAME}* — ENTRY\n"
            f"Expiry: {expiry_str}  Sched exit: {exit_date} 15:15\n"
            f"ATM: {atm}  Lots: {N_LOTS}  Qty/leg: {qty}  LS: {lot_size}\n"
            f"Net credit/unit: ₹{net_credit:.2f}  PT: ₹{pt_threshold:,.0f}\n"
            f"SENSEX MA20 filter: {prev_close:.0f} ≥ {ma20:.0f} ✅\n"
            f"Adjustment range: Δ [{ADJ_LOW_TRIG}–{ADJ_HI_TRIG}]\n"
            f"Legs:\n"
            f"  BUY  {buy_ce_sym}  @ ₹{fill_buy_ce:.2f}\n"
            f"  BUY  {buy_pe_sym}  @ ₹{fill_buy_pe:.2f}\n"
            f"  SELL {sell_ce_sym} @ ₹{fill_sell_ce:.2f}\n"
            f"  SELL {sell_pe_sym} @ ₹{fill_sell_pe:.2f}\n"
            f"Paper: {PAPER_MODE}"
        )

    # ──────────────────────────────────────────────────────────────────────────
    #  MTM + DELTA COMPUTATION
    # ──────────────────────────────────────────────────────────────────────────

    def _compute_mtm(self) -> tuple[float, dict[str, float]]:
        """
        Fetch current prices for all open legs and compute combined MTM (₹).
        MTM sign convention (seller's perspective):
          Sell leg: profit = (entry_prem − current_prem) × qty
          Buy  leg: profit = (current_prem − entry_prem) × qty
        """
        legs = self.state["legs"]
        qty  = self.state["qty"]
        prices: dict[str, float] = {}

        for key, leg in legs.items():
            if leg.get("open", True):
                prices[key] = _get_option_price(leg["symbol"])
            else:
                prices[key] = leg["entry_prem"]   # closed leg contributes nothing live

        mtm_sc = (legs["sell_ce"]["entry_prem"] - prices["sell_ce"]) * qty if legs["sell_ce"]["open"] else 0.0
        mtm_sp = (legs["sell_pe"]["entry_prem"] - prices["sell_pe"]) * qty if legs["sell_pe"]["open"] else 0.0
        mtm_bc = (prices["buy_ce"] - legs["buy_ce"]["entry_prem"]) * qty if legs["buy_ce"]["open"] else 0.0
        mtm_bp = (prices["buy_pe"] - legs["buy_pe"]["entry_prem"]) * qty if legs["buy_pe"]["open"] else 0.0

        combined = mtm_sc + mtm_sp + mtm_bc + mtm_bp
        return combined, prices

    async def _watch_legs(self, prices: dict[str, float]) -> None:
        """Feed each currently-open leg's latest poll result (from
        _compute_mtm) to the REST-poll watchdog — one check() per open
        symbol per poll cycle. Closed legs (post-adjustment) are skipped
        since their 'price' is a static entry_prem, not a live poll."""
        legs = (self.state or {}).get("legs", {})
        for leg_key, leg in legs.items():
            if not leg.get("open", True):
                continue
            price = prices.get(leg_key)
            success = price is not None and price > 0
            await self._watchdog.check(leg["symbol"], price if success else None, success=success)

    def _compute_short_leg_deltas(self, spot: float, vix: float,
                                    expiry_date: date) -> dict[str, float]:
        """
        Compute Black-76 |delta| for both short legs at current market conditions.
        Returns {"sell_ce": delta_ce, "sell_pe": delta_pe}.
        """
        legs = self.state["legs"]
        deltas: dict[str, float] = {}
        for key in ("sell_ce", "sell_pe"):
            leg = legs[key]
            if not leg.get("open", True):
                deltas[key] = 0.0
                continue
            opt_type = "CE" if key == "sell_ce" else "PE"
            deltas[key] = _compute_delta(spot, leg["strike"], expiry_date, vix, opt_type)
        return deltas

    # ──────────────────────────────────────────────────────────────────────────
    #  ADJUSTMENT
    # ──────────────────────────────────────────────────────────────────────────

    async def _adjust_leg(self, leg_key: str, expiry_str: str,
                           expiry_date: date, spot: float, vix: float) -> None:
        """
        Adjustment: close the triggered short leg, re-open ATM in same option type.
        leg_key: 'sell_ce' or 'sell_pe'
        """
        opt_type    = "CE" if leg_key == "sell_ce" else "PE"
        legs        = self.state["legs"]
        qty         = self.state["qty"]
        old_symbol  = legs[leg_key]["symbol"]
        old_strike  = legs[leg_key]["strike"]
        old_delta   = _compute_delta(spot, old_strike, expiry_date, vix, opt_type)
        old_price   = _get_option_price(old_symbol)

        logger.info(f"  ⚙️  ADJUSTMENT — {leg_key.upper()}: "
                    f"strike={old_strike}  |delta|={old_delta:.3f}  "
                    f"(threshold [{ADJ_LOW_TRIG}, {ADJ_HI_TRIG}])")

        # New ATM strike
        new_atm = int(round(spot / ATM_STEP) * ATM_STEP)
        new_symbol = _resolve_option_symbol(expiry_str, opt_type, strike=new_atm, atm_strike=new_atm)
        if not new_symbol:
            logger.error(f"  Adjustment failed — could not resolve new ATM {opt_type} symbol.")
            return

        new_price = _get_option_price(new_symbol)
        if new_price <= 0:
            logger.error(f"  Adjustment failed — zero price for {new_symbol}.")
            return

        logger.info(f"  Closing {old_symbol} @ ₹{old_price:.2f}  →  Re-selling {new_symbol} @ ₹{new_price:.2f}")

        # Step 1: Buy back the old short leg (close)
        buy_resp = self._placeorder(old_symbol, "BUY", qty)
        await asyncio.sleep(0.5)
        # Step 2: Sell new ATM leg (open)
        sell_resp = self._placeorder(new_symbol, "SELL", qty)

        # Resolve actual fills (booking basis)
        old_fill = _resolve_fill(buy_resp,  old_price)
        new_fill = _resolve_fill(sell_resp, new_price)

        # CSV log for both sides of the adjustment (fill-based)
        pnl_close = (legs[leg_key]["entry_prem"] - old_fill) * qty
        _log_csv("BUY",  f"{leg_key}_adj_close", old_symbol,  old_fill,  qty, pnl_close, "adjustment")
        _log_csv("SELL", f"{leg_key}_adj_open",  new_symbol,  new_fill,  qty, 0.0,       "adjustment")

        # Update state: mark old leg closed, record new leg
        legs[leg_key].update({
            "symbol":     new_symbol,
            "entry_prem": round(new_fill, 2),
            "strike":     new_atm,
            "open":       True,
        })
        self.state["adj_count"] = self.state.get("adj_count", 0) + 1
        self._adj_consec[leg_key] = 0   # reset hysteresis counter
        save_state(self.state)

        await send_async(
            f"⚙️  *{STRATEGY_NAME}* — ADJUSTMENT #{self.state['adj_count']}\n"
            f"Leg: {leg_key.upper()}  |delta|={old_delta:.3f}\n"
            f"Closed: {old_symbol} @ ₹{old_fill:.2f}\n"
            f"Re-opened: {new_symbol} @ ₹{new_fill:.2f}\n"
            f"New ATM: {new_atm}  Paper: {PAPER_MODE}"
        )

    # ──────────────────────────────────────────────────────────────────────────
    #  EXIT
    # ──────────────────────────────────────────────────────────────────────────

    async def _exit(self, reason: str, current_prices: dict[str, float]) -> None:
        """
        Close all 4 legs (including any adjusted legs).
        Order sequence: BUY back sell legs first → SELL buy legs second.
        reason: 'profit_target' | 'scheduled' | 'forced_expiry'
        """
        logger.info("═" * 64)
        logger.info(f"🔴 SENSEX IRON FLY EXIT — {reason}")

        legs     = self.state["legs"]
        qty      = self.state["qty"]
        lot_size = self.state["lot_size"]

        # ── Close sell legs (BUY orders) first — margin unwind ────────────────
        exit_fills: dict[str, float] = {}
        logger.info("  Closing sell legs (BUY orders) first…")
        for key in ("sell_ce", "sell_pe"):
            if legs[key].get("open", True):
                resp = self._placeorder(legs[key]["symbol"], "BUY", qty)
                exit_fills[key] = _resolve_fill(resp, current_prices.get(key, legs[key]["entry_prem"]))
                await asyncio.sleep(0.5)

        logger.info("  Closing buy legs (SELL orders) second…")
        for key in ("buy_ce", "buy_pe"):
            if legs[key].get("open", True):
                resp = self._placeorder(legs[key]["symbol"], "SELL", qty)
                exit_fills[key] = _resolve_fill(resp, current_prices.get(key, legs[key]["entry_prem"]))
                await asyncio.sleep(0.5)

        # ── P&L (fill-based) ─────────────────────────────────────────────────
        def _pnl(key: str, side: str) -> float:
            ep = legs[key]["entry_prem"]
            xp = exit_fills.get(key, current_prices.get(key, ep))
            return (ep - xp) * qty if side == "sell" else (xp - ep) * qty

        pnl_sc = _pnl("sell_ce", "sell")
        pnl_sp = _pnl("sell_pe", "sell")
        pnl_bc = _pnl("buy_ce",  "buy")
        pnl_bp = _pnl("buy_pe",  "buy")
        total  = pnl_sc + pnl_sp + pnl_bc + pnl_bp
        adj_n  = self.state.get("adj_count", 0)

        logger.info(f"  P&L breakdown (gross, pre-cost):  adj_count={adj_n}")
        logger.info(f"    sell_ce: ₹{pnl_sc:>10,.0f}")
        logger.info(f"    sell_pe: ₹{pnl_sp:>10,.0f}")
        logger.info(f"    buy_ce : ₹{pnl_bc:>10,.0f}")
        logger.info(f"    buy_pe : ₹{pnl_bp:>10,.0f}")
        logger.info(f"    TOTAL  : ₹{total:>10,.0f}")

        # ── CSV log (fill-based) ────────────────────────────────────────────────
        _log_csv("BUY",  "sell_ce", legs["sell_ce"]["symbol"],
                 exit_fills.get("sell_ce", legs["sell_ce"]["entry_prem"]),
                 qty, pnl_sc, reason)
        _log_csv("BUY",  "sell_pe", legs["sell_pe"]["symbol"],
                 exit_fills.get("sell_pe", legs["sell_pe"]["entry_prem"]),
                 qty, pnl_sp, reason)
        _log_csv("SELL", "buy_ce",  legs["buy_ce"]["symbol"],
                 exit_fills.get("buy_ce", legs["buy_ce"]["entry_prem"]),
                 qty, pnl_bc, reason)
        _log_csv("SELL", "buy_pe",  legs["buy_pe"]["symbol"],
                 exit_fills.get("buy_pe", legs["buy_pe"]["entry_prem"]),
                 qty, pnl_bp, reason)

        # ── DB trade log ──────────────────────────────────────────────────────
        try:
            log_trade_to_db(
                bot_name      = "sensex_iron_fly_weekly_bot",
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
                    f"adj_count={adj_n} | "
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
            "exit_reason": reason,
            "exit_time":   datetime.now().isoformat(),
            "total_pnl":   round(total, 2),
        })
        save_state(self.state)
        self.state = None
        self._adj_consec = {"sell_ce": 0, "sell_pe": 0}

        emoji = "🎯" if reason == "profit_target" else "⏰"
        await send_async(
            f"{emoji} *{STRATEGY_NAME}* — EXIT ({reason})\n"
            f"Gross P&L: ₹{total:,.0f}  Adjustments: {adj_n}\n"
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
        logger.info(f"    N_LOTS={N_LOTS}  LOT_SIZE={DEFAULT_LOT_SIZE}  "
                    f"(qty={N_LOTS * DEFAULT_LOT_SIZE} units)")
        logger.info(f"    PT={PROFIT_TARGET:.0%} of credit  No SL")
        logger.info(f"    Adj trigger: |Δ| < {ADJ_LOW_TRIG} or |Δ| > {ADJ_HI_TRIG} "
                    f"for {ADJ_HYSTERESIS} consecutive polls")
        logger.info(f"    Filter: SENSEX ≥ MA{MA_PERIOD}  (no VIX filter)")
        logger.info("    Entry: Regime-derived prior-week anchor day at 10:00 IST")
        logger.info("    Exit:  Day-before-expiry 15:15 IST OR profit target")
        logger.info(f"    Product: NRML BFO (4-day overnight hold)")

        # Restore open position from previous session
        self.state = load_state()
        if self.state and not self.state.get("closed") and self.state.get("legs"):
            logger.info(f"♻️  Restored open position from {self.state.get('entry_time', '?')}")
            logger.info(f"   Expiry: {self.state.get('expiry_str')}  "
                        f"Exit date: {self.state.get('exit_date')}")
            # Restore hysteresis counters (default to 0 if not in old state)
            self._adj_consec = {
                "sell_ce": self.state.get("_adj_consec_ce", 0),
                "sell_pe": self.state.get("_adj_consec_pe", 0),
            }
        else:
            logger.info("   No open position — awaiting entry day/time.")
            self.state = None

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

            # ── CASE A: Active position — monitor MTM, PT, deltas ─────────────
            if self.state and not self.state.get("closed") and self.state.get("legs"):
                expiry_date = date.fromisoformat(self.state["expiry_date"])
                exit_date   = date.fromisoformat(self.state["exit_date"])
                expiry_str  = self.state["expiry_str"]

                # Scheduled 15:15 exit on day before expiry (Wednesday)
                if (today == exit_date
                        and t.hour == EXIT_HOUR
                        and t.minute >= EXIT_MINUTE):
                    combined_mtm, prices = self._compute_mtm()
                    await self._watch_legs(prices)
                    await self._exit("scheduled", prices)
                    await asyncio.sleep(POLL_INTERVAL_SECS)
                    continue

                # Safety net: position still open on or after expiry day → force close
                if today >= expiry_date:
                    logger.warning("  ⚠️  Open position on/after expiry — forcing close.")
                    _, prices = self._compute_mtm()
                    await self._watch_legs(prices)
                    await self._exit("forced_expiry", prices)
                    await asyncio.sleep(POLL_INTERVAL_SECS)
                    continue

                # Normal monitoring poll
                combined_mtm, prices = self._compute_mtm()
                await self._watch_legs(prices)
                # A failed quote (LTP=0) on an open leg fakes ±entry_prem×qty
                # of MTM and can falsely trigger the PT — skip this poll instead
                if any(prices.get(k, 0) <= 0 for k, leg in self.state["legs"].items()
                       if leg.get("open", True)):
                    logger.warning("  Quote failure (LTP=0) on an open leg — "
                                   "skipping PT/adjustment this poll.")
                    await asyncio.sleep(POLL_INTERVAL_SECS)
                    continue
                pt_threshold = self.state["pt_threshold"]

                # Update state MTM for dashboard visibility
                self.state["current_mtm"]   = round(combined_mtm, 0)
                # Persist hysteresis counters in state (crash recovery)
                self.state["_adj_consec_ce"] = self._adj_consec["sell_ce"]
                self.state["_adj_consec_pe"] = self._adj_consec["sell_pe"]
                save_state(self.state)

                logger.info(
                    f"📊 MTM={combined_mtm:+,.0f}  PT@+{pt_threshold:,.0f}  adj={self.state.get('adj_count',0)}  |  "
                    f"SC={prices.get('sell_ce', 0):.1f}  "
                    f"SP={prices.get('sell_pe', 0):.1f}  "
                    f"BC={prices.get('buy_ce', 0):.1f}  "
                    f"BP={prices.get('buy_pe', 0):.1f}"
                )

                # ── Check profit target ────────────────────────────────────────
                if combined_mtm >= pt_threshold:
                    logger.info(
                        f"  🎯 PROFIT TARGET HIT: MTM=₹{combined_mtm:,.0f} ≥ ₹{pt_threshold:,.0f}"
                    )
                    await self._exit("profit_target", prices)
                    await asyncio.sleep(POLL_INTERVAL_SECS)
                    continue

                # ── Check delta-based adjustment triggers ──────────────────────
                spot = _get_sensex_spot()
                await self._watchdog.check(IDX_SYMBOL, spot if spot > 0 else None, success=(spot > 0))
                vix  = _get_vix()
                if vix <= 0:
                    vix = self.state.get("vix_at_entry", 15.0)

                if spot > 0:
                    deltas = self._compute_short_leg_deltas(spot, vix, expiry_date)
                    for leg_key in ("sell_ce", "sell_pe"):
                        if not self.state["legs"][leg_key].get("open", True):
                            continue
                        d = deltas[leg_key]
                        triggered = (d < ADJ_LOW_TRIG or d > ADJ_HI_TRIG)
                        if triggered:
                            self._adj_consec[leg_key] += 1
                            logger.info(
                                f"  ⚠️  {leg_key.upper()} adj trigger: |Δ|={d:.3f}  "
                                f"consec={self._adj_consec[leg_key]}/{ADJ_HYSTERESIS}"
                            )
                        else:
                            self._adj_consec[leg_key] = 0

                        if self._adj_consec[leg_key] >= ADJ_HYSTERESIS:
                            await self._adjust_leg(leg_key, expiry_str, expiry_date, spot, vix)
                else:
                    logger.warning("  SENSEX spot = 0 — skipping delta check this poll.")

                await asyncio.sleep(POLL_INTERVAL_SECS)
                continue

            # ── CASE B: No active position — check entry day ───────────────────
            expiry_str, expiry_date = _get_upcoming_expiry()
            if not expiry_str:
                logger.warning("  Cannot determine upcoming SENSEX expiry — retry in 5 min.")
                await asyncio.sleep(300)
                continue

            # Persist standby state so dashboard knows the bot is running and has no position
            save_state({
                "strategy": "SENSEX_IRON_FLY_WEEKLY",
                "closed": False,
                "legs": {},
                "expiry_str": expiry_str,
                "paper_mode": PAPER_MODE,
                "last_update": datetime.now().isoformat()
            })

            try:
                entry_date = _get_entry_date(expiry_date)
            except ValueError as e:
                logger.error(f"  Schedule error: {e}")
                await asyncio.sleep(300)
                continue

            # Allow entry on the anchor date OR on any later day still within
            # the expiry week (handles holiday-rollover where the anchor was
            # missed because the exchange holiday API didn't know about it).
            exit_date = _get_exit_date(expiry_date)
            if today < entry_date or today > exit_date:
                if t.minute == 0:   # log once per hour to avoid noise
                    logger.info(
                        f"  Standby — today={today}  entry={entry_date}  expiry={expiry_date}"
                    )
                await asyncio.sleep(300)
                continue

            if today > entry_date:
                logger.info(
                    f"  Rolled-forward entry: anchor={entry_date} was missed "
                    f"(holiday?), entering today={today}."
                )

            # ── Today is the computed entry day for this expiry week ───────────
            entry_open  = now.replace(hour=ENTRY_HOUR, minute=ENTRY_MINUTE, second=0, microsecond=0)
            entry_close = entry_open + timedelta(minutes=ENTRY_WINDOW_MINS)
            if today == TEMP_ENTRY_OVERRIDE_DATE:
                entry_close = now.replace(
                    hour=TEMP_ENTRY_CLOSE_HOUR,
                    minute=TEMP_ENTRY_CLOSE_MINUTE,
                    second=0,
                    microsecond=0,
                )
                logger.info(
                    f"  Temporary entry-window override active for {today}: "
                    f"close moved to {entry_close.strftime('%H:%M')} IST."
                )

            if now < entry_open:
                wait_s = int((entry_open - now).total_seconds())
                logger.info(
                    f"  Entry day {entry_date} for expiry {expiry_date} — waiting {wait_s}s until 10:00 IST."
                )
                await asyncio.sleep(min(wait_s, 60))
                continue

            if now > entry_close:
                logger.info(f"  Entry window closed for {entry_date} — no trade today.")
                await asyncio.sleep(3_600)
                continue

            # In window → execute entry
            await self._enter(expiry_str, expiry_date)

            # Reload state after entry attempt
            self.state = load_state()
            if self.state:
                logger.info("  ✅ Position entered — starting MTM/delta monitoring.")
                self._adj_consec = {"sell_ce": 0, "sell_pe": 0}
            else:
                logger.info("  Filter failed or entry error — no position. Done for today.")
                await asyncio.sleep(3_600)


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    bot = SensexIronFlyBot()
    asyncio.run(bot.run())
