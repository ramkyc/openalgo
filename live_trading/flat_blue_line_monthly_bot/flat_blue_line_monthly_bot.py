#!/usr/bin/env python3
"""
flat_blue_line_monthly_bot.py — Flat Blue Line (Double Fly) Monthly Options Bot
================================================================================
Research: options_data/research/flat_blue_line_monthly/FINAL_REPORT.md
Stage 11 paper trading — both NIFTY and BANKNIFTY in one process.

Champion parameters (minute-level intraday execution):
  NIFTY     : N_C=3, N_P=2  (Sharpe +1.77, WR 71%, IS+OOS 2023-2025)
  BANKNIFTY : N_C=1, N_P=3  (Sharpe +2.86, WR 85%, IS+OOS 2023-2025)

6 legs per instrument — all NRML (positional, held overnight):
  BUY   1×  ATM Call                     ← placed FIRST  (margin benefit)
  BUY   1×  ATM Put                      ← SECOND
  BUY   N_C× OTM Call hedge (Δ ≈ 0.10)  ← THIRD
  BUY   N_P× OTM Put hedge  (Δ ≈ 0.10)  ← FOURTH
  SELL  2×  OTM Call at ATM+D            ← FIFTH
  SELL  2×  OTM Put  at ATM−D            ← SIXTH
  D = round((ATM_C_px + ATM_P_px) / 2, strike_step)

Entry : 10:00 IST on first trading day after prior monthly expiry.
        Grace window: up to ENTRY_GRACE_TDAYS trading days past the entry day.
        IV filter: ATM Call Black-76 IV < 14% → skip month (low premium).

Exits (checked every ~60 s during market hours):
  1. Profit target  — gross running MTM ≥ target (₹22,500 NIFTY / dynamic BN)
  2. Breakeven stop — spot crosses theoretical expiry-payoff BE_L or BE_U
  3. Pre-expiry     — 3 trading days before monthly expiry at 15:15 IST

Exit order (NRML unwind — short legs repurchased first):
  BUY  2×SC → BUY  2×SP → SELL 1×ATM_C → SELL 1×ATM_P → SELL N_C×HC → SELL N_P×HP

Implementation notes:
  • Symbols resolved via /api/v1/optionsymbol with strike_int (broker-agnostic).
  • Monthly expiry identified by filtering get_expiry_dates() for gap >= 25 days.
  • State written to live_trading/logs/flat_blue_line_monthly_state.json after
    every entry/exit — crash-safe restart resumes monitoring without re-entering.
  • Breakevens computed once at entry from Black-76 expiry payoff grid.
  • IV proxy for hedge-delta uses Black-76 IV from ATM call price at entry.
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
from itertools import groupby
from pathlib import Path

import numpy as np
import requests
from dotenv import load_dotenv

# ── Path / env ─────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent.parent   # fyers_crk/openalgo/
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from openalgo import api                                                        # noqa
from live_trading.api_utils import HOST, get_expiry_dates                       # noqa
from live_trading.shared.telegram_notifier import send_async                    # noqa
from live_trading.shared.trade_logger import log_trade_to_db                    # noqa
from live_trading.shared.order_fill import fetch_fill_price                     # noqa

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "flat_blue_line_monthly_bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

API_KEY       = os.getenv("OPENALGO_API_KEY")
# NOTE: deliberately NO internal paper-trading flag here. Per project doctrine
# (CLAUDE.md → "How paper trading actually works"): bots ALWAYS fire real
# OpenAlgo REST orders; OpenAlgo's own Sandbox/Analyze Mode (toggled in the
# UI) transparently intercepts and simulates fills. An internal PAPER_MODE
# flag here previously short-circuited _order() to fake a "success" without
# ever calling placeorder() — orders never reached OpenAlgo and never showed
# in the Positions UI. Don't reintroduce one; flip OpenAlgo's UI mode instead.

STRATEGY_NAME = "FLAT_BLUE_LINE_MONTHLY"
OPT_EXCHANGE  = "NFO"
IDX_EXCHANGE  = "NSE_INDEX"
VIX_SYMBOL    = "INDIAVIX"

BASE_LOTS     = 5          # lots per base unit (from research spec)
RISK_FREE     = 0.065      # Black-76 risk-free rate (6.5% p.a.)
MIN_IV        = 0.14       # ATM call IV < 14% → skip month
TARGET_DELTA  = 0.10       # hedge leg target delta
MIN_DTE       = 5          # skip if fewer than 5 DTE at entry

# Entry timing
ENTRY_HOUR            = 10
ENTRY_MIN             = 0
ENTRY_WINDOW_MINS     = 20   # accept entry up to 10:20 IST
ENTRY_GRACE_TDAYS     = 5    # allow entry up to 5 trading days after entry_day

# Exit timing
PRE_EXPIRY_TDAYS      = 3    # close 3 trading days before monthly expiry
PRE_EXPIRY_HOUR       = 15
PRE_EXPIRY_MIN        = 15

POLL_SECS             = 60   # monitoring interval (1 minute)
ORDER_DELAY           = 1.5  # seconds between leg orders

# Profit target
NIFTY_TARGET_PER_LOT  = 4_500.0   # ₹4,500/lot × 5 lots = ₹22,500
NIFTY_REF_RATIO       = 4_500.0 / (774.0 * 50.0)   # ≈ 0.1163

# Per-instrument config
INSTRUMENTS = {
    "NIFTY": {
        "underlying": "NIFTY",
        "idx_symbol": "NIFTY",
        "strike_step": 50,
        "n_c": 3,
        "n_p": 2,
    },
    "BANKNIFTY": {
        "underlying": "BANKNIFTY",
        "idx_symbol": "BANKNIFTY",
        "strike_step": 100,
        "n_c": 1,
        "n_p": 3,
    },
}

STATE_FILE       = LOG_DIR / "flat_blue_line_monthly_state.json"
PAPER_TRADES_CSV = LOG_DIR / "flat_blue_line_monthly_paper_trades.csv"


# ══════════════════════════════════════════════════════════════════════════════
# BLACK-76 (inline — no numba dependency in live bot)
# ══════════════════════════════════════════════════════════════════════════════

def _ncdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _b76_price(F: float, K: float, T: float, sig: float, r: float, ot: int) -> float:
    """Black-76 price. ot: +1 call, -1 put."""
    if T <= 0:
        return max(0.0, ot * (F - K))
    d1 = (math.log(F / K) + 0.5 * sig * sig * T) / (sig * math.sqrt(T))
    d2 = d1 - sig * math.sqrt(T)
    disc = math.exp(-r * T)
    if ot == 1:
        return disc * (F * _ncdf(d1) - K * _ncdf(d2))
    return disc * (K * _ncdf(-d2) - F * _ncdf(-d1))


def _b76_vega(F: float, K: float, T: float, sig: float, r: float) -> float:
    if T <= 0:
        return 0.0
    d1 = (math.log(F / K) + 0.5 * sig * sig * T) / (sig * math.sqrt(T))
    return F * math.exp(-r * T) * math.sqrt(T) * math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)


def _b76_delta(F: float, K: float, T: float, sig: float, r: float, ot: int) -> float:
    if T <= 0 or sig <= 0:
        return 0.0
    d1 = (math.log(F / K) + 0.5 * sig * sig * T) / (sig * math.sqrt(T))
    disc = math.exp(-r * T)
    return disc * _ncdf(d1) if ot == 1 else disc * (_ncdf(d1) - 1.0)


def compute_iv(price: float, spot: float, strike: float, T: float, ot: int = 1) -> float:
    """Black-76 implied volatility via Newton-Raphson. Returns 0 on failure."""
    if T <= 0 or price < 1e-4:
        return 0.0
    F = spot
    a, b = 1e-4, 5.0
    if price <= _b76_price(F, strike, T, a, RISK_FREE, ot):
        return a
    if price >= _b76_price(F, strike, T, b, RISK_FREE, ot):
        return b
    sig = max(0.10, min(0.5, math.sqrt(2 * math.pi / T) * price / F))
    for _ in range(100):
        p = _b76_price(F, strike, T, sig, RISK_FREE, ot)
        v = _b76_vega(F, strike, T, sig, RISK_FREE)
        diff = price - p
        if abs(diff) < 1e-7:
            return sig
        if abs(v) > 1e-8:
            step = max(-0.5, min(0.5, diff / v))
        else:
            step = 0.0
        if p > price:
            b = sig
        else:
            a = sig
        sig = max(a, min(b, sig + step))
    return (a + b) / 2.0


def find_hedge_strike(spot: float, iv: float, T: float,
                      step: int, opt_type: str) -> int:
    """Walk OTM strikes until |delta| is closest to TARGET_DELTA."""
    ot  = 1 if opt_type == "CE" else -1
    dir_ = 1 if opt_type == "CE" else -1
    atm  = int(round(spot / step) * step)
    best_k, best_diff = atm, float("inf")
    for n in range(1, 51):
        K    = atm + dir_ * n * step
        d    = _b76_delta(spot, K, T, iv, RISK_FREE, ot)
        diff = abs(abs(d) - TARGET_DELTA)
        if diff < best_diff:
            best_diff, best_k = diff, K
        if abs(d) < TARGET_DELTA * 0.4:
            break
    return best_k


def compute_breakevens(atm: int, sc: int, sp: int, hc: int, hp: int,
                        prices: dict, n_c: int, n_p: int) -> tuple[float, float]:
    """
    Find spot levels where at-expiry payoff = 0 (theoretical BE from B76 payoff).
    prices: {"atm_c", "atm_p", "sc", "sp", "hc", "hp"} in points.
    """
    net = (prices["atm_c"] + prices["atm_p"]
           + n_c * prices["hc"] + n_p * prices["hp"]
           - 2 * prices["sc"] - 2 * prices["sp"])

    S = np.linspace(atm * 0.65, atm * 1.35, 20_000)
    pnl = (np.maximum(S - atm, 0) + np.maximum(atm - S, 0)
           - 2 * np.maximum(S - sc, 0) - 2 * np.maximum(sp - S, 0)
           + n_c * np.maximum(S - hc, 0) + n_p * np.maximum(hp - S, 0)
           - net)

    crossings = []
    for i in np.where(np.diff(np.sign(pnl)))[0]:
        s1, s2, p1, p2 = S[i], S[i+1], pnl[i], pnl[i+1]
        if p2 != p1:
            crossings.append(float(s1 - p1 * (s2 - s1) / (p2 - p1)))

    if len(crossings) >= 2:
        return min(crossings), max(crossings)
    elif len(crossings) == 1:
        c = crossings[0]
        return (c, atm * 1.35) if c < atm else (atm * 0.65, c)
    return atm * 0.65, atm * 1.35


def compute_mtm(legs: dict, current: dict, lot_size: int, n_c: int, n_p: int) -> float:
    """
    Gross running P&L (₹) for the 6-leg position.
    legs["atm_c"]["entry_prem"] etc; current = {"atm_c": ltp, ...}
    """
    unit = lot_size * BASE_LOTS
    return unit * (
        (current["atm_c"] - legs["atm_c"]["entry_prem"])
        + (current["atm_p"] - legs["atm_p"]["entry_prem"])
        + 2 * (legs["sc"]["entry_prem"] - current["sc"])
        + 2 * (legs["sp"]["entry_prem"] - current["sp"])
        + n_c * (current["hc"] - legs["hc"]["entry_prem"])
        + n_p * (current["hp"] - legs["hp"]["entry_prem"])
    )


def profit_target(instrument: str, straddle_pts: float, lot_size: int) -> float:
    if instrument == "NIFTY":
        return NIFTY_TARGET_PER_LOT * BASE_LOTS
    return NIFTY_REF_RATIO * straddle_pts * lot_size * BASE_LOTS


# ══════════════════════════════════════════════════════════════════════════════
# PRICE / SYMBOL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _quote(symbol: str, exchange: str) -> float:
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
                if isinstance(qd, list) and qd:
                    qd = qd[0]
                if isinstance(qd, dict):
                    ltp = (qd.get("ltp") or qd.get("last_price")
                           or qd.get("close") or qd.get("c") or 0)
                    price = float(ltp)
                    if price == 0.0:
                        logger.warning(f"  Quote=0 for {symbol}. Keys: {list(qd.keys())}")
                    return price
    except Exception as e:
        logger.error(f"Quote failed {symbol}: {e}")
    return 0.0


def _get_ltp(symbol: str, retries: int = 3, delay: float = 5.0) -> float:
    for attempt in range(1, retries + 1):
        p = _quote(symbol, OPT_EXCHANGE)
        if p > 0:
            return p
        if attempt < retries:
            import time; time.sleep(delay)
    return 0.0


def _multiquote(items: list[tuple[str, str]],
                retries: int = 3, delay: float = 3.0) -> dict[str, float]:
    """
    Batch-fetch LTPs for multiple (symbol, exchange) pairs via ONE
    /api/v1/multiquotes round-trip — instead of firing one /api/v1/quotes
    call per symbol sequentially.

    Why this matters: this strategy needs 6 leg quotes + spot per tick per
    instrument (14 sequential single-quote calls across NIFTY+BANKNIFTY,
    each with its own 5s timeout/retry). Against OpenAlgo's single-worker
    Flask server (gunicorn -w 1, eventlet) shared by ~16 concurrent bots,
    that serial hammering produces "Read timed out" errors. One batched
    call collapses N round-trips into 1.

    Returns {symbol: ltp} for whatever resolved with ltp > 0; missing/zero/
    error entries are simply absent so callers can fall back to entry_prem
    or skip, exactly as the old per-symbol path allowed.
    """
    pending = list(dict.fromkeys(items))   # de-dupe, preserve order
    out: dict[str, float] = {}
    for attempt in range(1, retries + 1):
        if not pending:
            break
        payload = {
            "apikey":  API_KEY,
            "symbols": [{"symbol": s, "exchange": e} for s, e in pending],
        }
        try:
            res = requests.post(f"{HOST}/api/v1/multiquotes", json=payload, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data.get("status") == "success":
                    for row in data.get("results", []):
                        sym = row.get("symbol")
                        qd  = row.get("data") or {}
                        ltp = (qd.get("ltp") or qd.get("last_price")
                               or qd.get("close") or qd.get("c") or 0)
                        try:
                            price = float(ltp)
                        except (TypeError, ValueError):
                            price = 0.0
                        if sym and price > 0:
                            out[sym] = price
        except Exception as e:
            logger.error(f"multiquotes error: {e}")
        pending = [(s, e) for s, e in pending if s not in out]
        if pending and attempt < retries:
            import time; time.sleep(delay)
    if pending:
        logger.warning(f"  multiquotes: no LTP for {[s for s, _ in pending]}")
    return out


def _resolve_fill(resp: dict, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id:
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback


def _resolve_symbol(underlying: str, expiry_str: str,
                    opt_type: str, strike: int) -> str | None:
    """
    Build the OpenAlgo option symbol directly from its canonical,
    broker-agnostic naming convention:

        {underlying}{expiry DDMMMYY}{strike}{CE/PE}   e.g. BANKNIFTY30JUN2654200CE

    OpenAlgo's master-contract mapping (symtoken) translates this to whatever
    the broker's native symbol is internally (e.g. Fyers' monthly-only format
    NSE:BANKNIFTY26JUN54200CE) — no API round-trip needed.

    NOTE: previously this called /api/v1/optionsymbol with `strike_int=<abs strike>`,
    but that field means STRIKE INTERVAL (e.g. 100), not an absolute strike — the
    service derived a bogus ATM strike from it and 404'd ("symbols unresolved")
    on every single entry attempt. Direct construction sidesteps that failure
    mode entirely; downstream _get_ltp() / price checks already validate the
    symbol resolves to a real, quotable instrument.
    """
    return f"{underlying}{expiry_str}{int(strike)}{opt_type}"


# ══════════════════════════════════════════════════════════════════════════════
# EXPIRY CALENDAR HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _parse_exp(d: str) -> date:
    return datetime.strptime(d, "%d%b%y").date()


def _get_monthly_expiries(underlying: str) -> list[tuple[str, date]]:
    """
    Fetch all option expiries from OpenAlgo and keep only the monthly ones —
    defined as the LAST listed expiry within each calendar month. This is
    robust to indices (e.g. NIFTY) that run weekly + monthly expiries side
    by side; a gap-based heuristic mis-anchors on the first weekly it sees.
    Returns sorted list of (expiry_str "DDMMMYY", expiry_date).
    """
    raw = get_expiry_dates(API_KEY, underlying, OPT_EXCHANGE, "options")
    if not raw:
        return []
    parsed = sorted((_parse_exp(d), d) for d in raw if d)
    monthly = [
        max(group, key=lambda x: x[0])
        for _, group in groupby(parsed, key=lambda x: (x[0].year, x[0].month))
    ]
    return [(d, exp_dt) for exp_dt, d in monthly]


def _n_tdays_after(d: date, n: int) -> date:
    """Return date n trading days (Mon–Fri) after d."""
    result, count = d + timedelta(days=1), 0
    while count < n:
        if result.weekday() < 5:
            count += 1
            if count < n:
                result += timedelta(days=1)
        else:
            result += timedelta(days=1)
    return result


def _n_tdays_before(d: date, n: int) -> date:
    """Return date n trading days (Mon–Fri) before d."""
    result, count = d - timedelta(days=1), 0
    while True:
        if result.weekday() < 5:
            count += 1
            if count == n:
                return result
        result -= timedelta(days=1)


def _first_tday_after(d: date) -> date:
    """First Monday–Friday after d."""
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt


def _trading_days_between(start: date, end: date) -> int:
    """Count Mon-Fri days in (start, end] inclusive of end, exclusive of start."""
    count, d = 0, start + timedelta(days=1)
    while d <= end:
        if d.weekday() < 5:
            count += 1
        d += timedelta(days=1)
    return count


def _get_cycle(underlying: str) -> dict | None:
    """
    Find the current or upcoming monthly cycle for the instrument.
    Returns dict with expiry info or None on failure.
    """
    monthly = _get_monthly_expiries(underlying)
    if len(monthly) < 2:
        logger.error(f"[{underlying}] Need ≥2 monthly expiries; got {len(monthly)}")
        return None

    today = date.today()
    # Find the nearest upcoming monthly with DTE >= MIN_DTE
    current_idx = None
    for i, (_, exp_dt) in enumerate(monthly):
        if (exp_dt - today).days >= MIN_DTE:
            current_idx = i
            break
    if current_idx is None:
        logger.warning(f"[{underlying}] No upcoming monthly expiry with DTE >= {MIN_DTE}")
        return None

    expiry_str, expiry_date = monthly[current_idx]

    # Entry day = first trading day after prior monthly expiry
    if current_idx > 0:
        _, prior_date = monthly[current_idx - 1]
        entry_day = _first_tday_after(prior_date)
    else:
        # No prior in list — fall back to first weekday of expiry month
        entry_day = date(expiry_date.year, expiry_date.month, 1)
        while entry_day.weekday() >= 5:
            entry_day += timedelta(days=1)

    # Pre-expiry exit day (3 trading days before expiry)
    pre_exit_date = _n_tdays_before(expiry_date, PRE_EXPIRY_TDAYS)

    # Month key (for state tracking)
    month_key = f"{expiry_date.year}-{expiry_date.month:02d}"

    return {
        "expiry_str":    expiry_str,
        "expiry_date":   expiry_date,
        "entry_day":     entry_day,
        "pre_exit_date": pre_exit_date,
        "month_key":     month_key,
        "dte":           (expiry_date - today).days,
    }


# ══════════════════════════════════════════════════════════════════════════════
# STATE MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception as e:
            logger.warning(f"State load failed: {e}")
    return {"NIFTY": {"closed": True}, "BANKNIFTY": {"closed": True}}


def _save_state(state: dict) -> None:
    try:
        state["last_update"] = datetime.now().isoformat()
        STATE_FILE.write_text(json.dumps(state, indent=2, default=str))
    except Exception as e:
        logger.error(f"State save failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# PAPER TRADE CSV
# ══════════════════════════════════════════════════════════════════════════════

def _log_csv(instrument: str, action: str, leg: str, symbol: str,
              premium: float, qty: int, pnl: float, reason: str) -> None:
    write_header = not PAPER_TRADES_CSV.exists()
    try:
        with open(PAPER_TRADES_CSV, "a", newline="") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow(["timestamp", "instrument", "action", "leg", "symbol",
                             "premium", "qty", "pnl_gross", "reason"])
            w.writerow([datetime.now().isoformat(), instrument, action, leg,
                        symbol, round(premium, 2), qty, round(pnl, 2), reason])
    except Exception as e:
        logger.warning(f"CSV log failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# BOT CLASS
# ══════════════════════════════════════════════════════════════════════════════

class FlatBlueLineBot:
    """Flat Blue Line Monthly Bot — NIFTY + BANKNIFTY, 6-leg NRML Double Fly."""

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.state  = _load_state()

    # ──────────────────────────────────────────────────────────────────────────
    #  ORDER PLACEMENT
    # ──────────────────────────────────────────────────────────────────────────

    def _order(self, symbol: str, action: str, qty: int) -> dict:
        """
        ALWAYS fires a real OpenAlgo REST order — never short-circuit this.
        OpenAlgo's own Sandbox/Analyze Mode (set via the UI mode toggle, not
        in bot code) transparently intercepts and simulates the fill, which
        is what makes it show up correctly in the Positions/Orderbook UI.
        See the PAPER_MODE removal note near the top of this file.
        """
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
            return resp if isinstance(resp, dict) else {"status": "ok"}
        except Exception as e:
            logger.error(f"  placeorder failed ({action} {symbol}): {e}")
            return {"status": "error", "message": str(e)}

    # ──────────────────────────────────────────────────────────────────────────
    #  ENTRY — one instrument
    # ──────────────────────────────────────────────────────────────────────────

    async def _enter(self, instrument: str, cycle: dict) -> None:
        cfg         = INSTRUMENTS[instrument]
        underlying  = cfg["underlying"]
        step        = cfg["strike_step"]
        n_c         = cfg["n_c"]
        n_p         = cfg["n_p"]
        expiry_str  = cycle["expiry_str"]
        expiry_date = cycle["expiry_date"]
        month_key   = cycle["month_key"]

        logger.info("─" * 60)
        logger.info(f"[{instrument}] ENTRY  expiry={expiry_str}  month={month_key}")

        # ── Spot (routed through _multiquote for built-in retry/backoff —
        #    the bare single-shot _quote() here had no retry and was
        #    intermittently timing out under concurrent-bot load, aborting
        #    entry with "spot=0") ───────────────────────────────────────────
        spot = _multiquote([(cfg["idx_symbol"], IDX_EXCHANGE)]).get(cfg["idx_symbol"], 0.0)
        if spot <= 0:
            logger.error(f"[{instrument}] spot=0 — aborting entry")
            return
        atm = int(round(spot / step) * step)
        logger.info(f"[{instrument}] spot={spot:.1f}  ATM={atm}")

        # ── ATM prices ────────────────────────────────────────────────────────
        atm_c_sym = _resolve_symbol(underlying, expiry_str, "CE", atm)
        atm_p_sym = _resolve_symbol(underlying, expiry_str, "PE", atm)
        if not atm_c_sym or not atm_p_sym:
            logger.error(f"[{instrument}] ATM symbols unresolved")
            return
        atm_q    = _multiquote([(atm_c_sym, OPT_EXCHANGE), (atm_p_sym, OPT_EXCHANGE)])
        atm_c_px = atm_q.get(atm_c_sym, 0.0)
        atm_p_px = atm_q.get(atm_p_sym, 0.0)
        if atm_c_px <= 0 or atm_p_px <= 0:
            logger.error(f"[{instrument}] ATM prices zero ({atm_c_px}/{atm_p_px})")
            return

        # ── IV filter ─────────────────────────────────────────────────────────
        T  = max((expiry_date - date.today()).days / 365.0, 1 / 365.0)
        iv = compute_iv(atm_c_px, spot, atm, T, ot=1)
        if iv < MIN_IV:
            logger.info(f"[{instrument}] LOW-IV SKIP: IV={iv:.1%} < {MIN_IV:.0%}")
            await send_async(
                f"🚫 *{STRATEGY_NAME}* [{instrument}] — LOW-IV skip\n"
                f"IV={iv:.1%} < {MIN_IV:.0%}. Month {month_key} skipped."
            )
            self.state[instrument] = {
                "closed": True, "month_key": month_key,
                "exit_reason": "low_vol", "entry_time": None,
            }
            _save_state(self.state)
            return

        logger.info(f"[{instrument}] IV={iv:.1%} ≥ {MIN_IV:.0%} ✓")

        # ── Short strangle strikes ─────────────────────────────────────────────
        D     = max(int(round((atm_c_px + atm_p_px) / 2.0 / step) * step), step)
        sc_k  = atm + D
        sp_k  = atm - D

        # ── Hedge strikes (0.10 delta) ─────────────────────────────────────────
        hc_k  = find_hedge_strike(spot, iv, T, step, "CE")
        hp_k  = find_hedge_strike(spot, iv, T, step, "PE")

        logger.info(
            f"[{instrument}] D={D}  SC={sc_k}/SP={sp_k}  HC={hc_k}/HP={hp_k}"
            f"  N_C={n_c} N_P={n_p}"
        )

        # ── Resolve all 6 symbols ─────────────────────────────────────────────
        sc_sym = _resolve_symbol(underlying, expiry_str, "CE", sc_k)
        sp_sym = _resolve_symbol(underlying, expiry_str, "PE", sp_k)
        hc_sym = _resolve_symbol(underlying, expiry_str, "CE", hc_k)
        hp_sym = _resolve_symbol(underlying, expiry_str, "PE", hp_k)
        if not all([sc_sym, sp_sym, hc_sym, hp_sym]):
            logger.error(f"[{instrument}] one or more symbols unresolved — aborting")
            return

        # ── Fetch all prices (one batched round-trip) ─────────────────────────
        leg_q = _multiquote([(sc_sym, OPT_EXCHANGE), (sp_sym, OPT_EXCHANGE),
                             (hc_sym, OPT_EXCHANGE), (hp_sym, OPT_EXCHANGE)])
        sc_px = leg_q.get(sc_sym, 0.0)
        sp_px = leg_q.get(sp_sym, 0.0)
        hc_px = leg_q.get(hc_sym, 0.0)
        hp_px = leg_q.get(hp_sym, 0.0)
        if any(p <= 0 for p in [sc_px, sp_px, hc_px, hp_px]):
            logger.error(f"[{instrument}] zero price on leg(s) — aborting")
            return

        entry_prices = {
            "atm_c": atm_c_px, "atm_p": atm_p_px,
            "sc": sc_px, "sp": sp_px, "hc": hc_px, "hp": hp_px,
        }
        straddle_pts = atm_c_px + atm_p_px

        # ── Lot size ──────────────────────────────────────────────────────────
        try:
            from database.token_db import get_symbol_info
            si       = get_symbol_info(atm_c_sym, OPT_EXCHANGE)
            lot_size = int(si.lotsize) if si and getattr(si, "lotsize", None) else 0
        except Exception:
            lot_size = 0
        if not lot_size:
            lot_size = 65 if instrument == "NIFTY" else 30
            logger.warning(f"[{instrument}] lot_size fallback → {lot_size}")
        logger.info(f"[{instrument}] lot_size={lot_size}")

        qty_unit = lot_size * BASE_LOTS   # per 1× leg

        # ── Breakevens ────────────────────────────────────────────────────────
        be_lo, be_hi = compute_breakevens(
            atm, sc_k, sp_k, hc_k, hp_k, entry_prices, n_c, n_p
        )

        # ── Profit target ──────────────────────────────────────────────────────
        tgt = profit_target(instrument, straddle_pts, lot_size)

        logger.info(
            f"[{instrument}] BE=[{be_lo:.0f},{be_hi:.0f}]  target=₹{tgt:,.0f}"
            f"  straddle={straddle_pts:.1f}"
        )

        # ── Place orders (BUY legs FIRST, then SELL legs) ─────────────────────
        buy_legs = [
            (atm_c_sym, "BUY",  1 * qty_unit,    "atm_c"),
            (atm_p_sym, "BUY",  1 * qty_unit,    "atm_p"),
            (hc_sym,    "BUY",  n_c * qty_unit,  "hc"),
            (hp_sym,    "BUY",  n_p * qty_unit,  "hp"),
        ]
        sell_legs = [
            (sc_sym,    "SELL", 2 * qty_unit,    "sc"),
            (sp_sym,    "SELL", 2 * qty_unit,    "sp"),
        ]

        # Book P&L against actual fills, not the pre-order LTP snapshot used
        # to pick strikes — 6 sequential leg orders drift apart by the time
        # all are filled.
        fills: dict[str, float] = {}
        for sym, action, qty, tag in buy_legs + sell_legs:
            resp = self._order(sym, action, qty)
            await asyncio.sleep(ORDER_DELAY)
            fills[tag] = _resolve_fill(resp, entry_prices.get(tag, 0.0))
            _log_csv(instrument, action, tag, sym, fills[tag], qty, 0.0, "entry")
        logger.info(f"[{instrument}] Fill prices: {fills}  (LTP snapshot: {entry_prices})")

        # ── Persist state ──────────────────────────────────────────────────────
        self.state[instrument] = {
            "closed":       False,
            "month_key":    month_key,
            "expiry_str":   expiry_str,
            "expiry_date":  expiry_date.isoformat(),
            "pre_exit_date": cycle["pre_exit_date"].isoformat(),
            "entry_time":   datetime.now().isoformat(),
            "lot_size":     lot_size,
            "n_lots":       BASE_LOTS,
            "atm_strike":   atm,
            "sc_strike":    sc_k,
            "sp_strike":    sp_k,
            "hc_strike":    hc_k,
            "hp_strike":    hp_k,
            "n_c":          n_c,
            "n_p":          n_p,
            "be_lower":     round(be_lo, 2),
            "be_upper":     round(be_hi, 2),
            "profit_target": round(tgt, 2),
            "straddle_pts": round(straddle_pts, 2),
            "current_mtm":  0.0,
            "current_spot": spot,
            "legs": {
                "atm_c": {"symbol": atm_c_sym, "entry_prem": round(fills["atm_c"], 2), "strike": atm,  "action": "BUY",  "qty": 1 * qty_unit},
                "atm_p": {"symbol": atm_p_sym, "entry_prem": round(fills["atm_p"], 2), "strike": atm,  "action": "BUY",  "qty": 1 * qty_unit},
                "sc":    {"symbol": sc_sym,    "entry_prem": round(fills["sc"],    2), "strike": sc_k, "action": "SELL", "qty": 2 * qty_unit},
                "sp":    {"symbol": sp_sym,    "entry_prem": round(fills["sp"],    2), "strike": sp_k, "action": "SELL", "qty": 2 * qty_unit},
                "hc":    {"symbol": hc_sym,    "entry_prem": round(fills["hc"],    2), "strike": hc_k, "action": "BUY",  "qty": n_c * qty_unit},
                "hp":    {"symbol": hp_sym,    "entry_prem": round(fills["hp"],    2), "strike": hp_k, "action": "BUY",  "qty": n_p * qty_unit},
            },
            "exit_reason": None,
            "exit_time":   None,
        }
        _save_state(self.state)

        await send_async(
            f"⚡ *{STRATEGY_NAME}* [{instrument}] — ENTRY\n"
            f"Expiry: {expiry_str}  Pre-exit: {cycle['pre_exit_date']}\n"
            f"ATM={atm}  D={D}  SC={sc_k}/SP={sp_k}  HC={hc_k}/HP={hp_k}\n"
            f"IV={iv:.1%}  Straddle={straddle_pts:.1f}  LS={lot_size}\n"
            f"BE=[{be_lo:.0f}, {be_hi:.0f}]  Target=₹{tgt:,.0f}\n"
            f"N_C={n_c}  N_P={n_p}"
        )
        logger.info(f"[{instrument}] ✅ Entry complete. Monitoring started.")

    # ──────────────────────────────────────────────────────────────────────────
    #  EXIT — one instrument
    # ──────────────────────────────────────────────────────────────────────────

    async def _exit(self, instrument: str, reason: str, current_prices: dict) -> None:
        s        = self.state[instrument]
        legs     = s["legs"]
        lot_size = s["lot_size"]
        n_c      = s["n_c"]
        n_p      = s["n_p"]

        logger.info("─" * 60)
        logger.info(f"[{instrument}] EXIT  reason={reason}")

        # Close order: BUY back short legs first, then SELL long legs
        close_legs = [
            ("sc",    legs["sc"],    "BUY"),
            ("sp",    legs["sp"],    "BUY"),
            ("atm_c", legs["atm_c"], "SELL"),
            ("atm_p", legs["atm_p"], "SELL"),
            ("hc",    legs["hc"],    "SELL"),
            ("hp",    legs["hp"],    "SELL"),
        ]
        # Book P&L against actual exit fills, not the LTP snapshot that
        # triggered the exit — 6 sequential leg orders drift apart by the
        # time all are filled.
        exit_fills: dict[str, float] = {}
        for tag, leg, action in close_legs:
            resp = self._order(leg["symbol"], action, leg["qty"])
            await asyncio.sleep(ORDER_DELAY)
            exit_fills[tag] = _resolve_fill(resp, current_prices.get(tag, leg["entry_prem"]))
        logger.info(f"[{instrument}] Exit fills: {exit_fills}  (LTP snapshot: {current_prices})")

        # P&L
        gross = compute_mtm(legs, exit_fills, lot_size, n_c, n_p)

        # Per-leg CSV
        def _lp(key):
            return exit_fills.get(key, legs[key]["entry_prem"])

        pnl_parts = {
            "atm_c": ((_lp("atm_c") - legs["atm_c"]["entry_prem"]) * legs["atm_c"]["qty"]),
            "atm_p": ((_lp("atm_p") - legs["atm_p"]["entry_prem"]) * legs["atm_p"]["qty"]),
            "sc":    ((legs["sc"]["entry_prem"]    - _lp("sc"))  * legs["sc"]["qty"]),
            "sp":    ((legs["sp"]["entry_prem"]    - _lp("sp"))  * legs["sp"]["qty"]),
            "hc":    ((_lp("hc") - legs["hc"]["entry_prem"]) * legs["hc"]["qty"]),
            "hp":    ((_lp("hp") - legs["hp"]["entry_prem"]) * legs["hp"]["qty"]),
        }
        for tag, pnl in pnl_parts.items():
            leg = legs[tag]
            _log_csv(instrument, "CLOSE", tag, leg["symbol"], _lp(tag), leg["qty"], pnl, reason)

        logger.info(f"[{instrument}] Gross P&L=₹{gross:,.0f}")

        # DB trade log
        try:
            log_trade_to_db(
                bot_name      = "flat_blue_line_monthly_bot",
                instrument    = instrument,
                option_symbol = legs["atm_c"]["symbol"],
                option_type   = "DOUBLE_FLY",
                entry_time    = datetime.fromisoformat(s["entry_time"]),
                exit_time     = datetime.now(),
                entry_premium = s["straddle_pts"],
                exit_premium  = 0.0,
                exit_reason   = reason,
                quantity      = legs["atm_c"]["qty"],
                lots          = BASE_LOTS,
                lot_size      = lot_size,
                gross_pnl     = round(gross, 2),
                notes=(
                    f"buy_atm_c={legs['atm_c']['symbol']}@{legs['atm_c']['entry_prem']} | "
                    f"buy_atm_p={legs['atm_p']['symbol']}@{legs['atm_p']['entry_prem']} | "
                    f"sell_c={legs['sc']['symbol']}@{legs['sc']['entry_prem']} | "
                    f"sell_p={legs['sp']['symbol']}@{legs['sp']['entry_prem']} | "
                    f"hedge_c={legs['hc']['symbol']}@{legs['hc']['entry_prem']} | "
                    f"hedge_p={legs['hp']['symbol']}@{legs['hp']['entry_prem']} | "
                    f"BE=[{s['be_lower']:.0f},{s['be_upper']:.0f}]"
                ),
            )
        except Exception as e:
            logger.warning(f"[{instrument}] DB log failed: {e}")

        # Update state
        self.state[instrument].update({
            "closed":      True,
            "exit_reason": reason,
            "exit_time":   datetime.now().isoformat(),
            "total_pnl":   round(gross, 2),
        })
        _save_state(self.state)

        emoji = "🎯" if reason == "target" else ("🛑" if reason == "be_stop" else "⏰")
        await send_async(
            f"{emoji} *{STRATEGY_NAME}* [{instrument}] — EXIT ({reason})\n"
            f"Gross P&L: ₹{gross:,.0f}"
        )

    # ──────────────────────────────────────────────────────────────────────────
    #  MONITORING TICK — one instrument
    # ──────────────────────────────────────────────────────────────────────────

    async def _monitor(self, instrument: str, now: datetime) -> None:
        s = self.state.get(instrument, {})
        if s.get("closed", True):
            return  # not in trade

        legs        = s["legs"]
        n_c         = s["n_c"]
        n_p         = s["n_p"]
        lot_size    = s["lot_size"]
        expiry_date = date.fromisoformat(s["expiry_date"])
        pre_exit    = datetime.fromisoformat(s["pre_exit_date"] + f" {PRE_EXPIRY_HOUR:02d}:{PRE_EXPIRY_MIN:02d}:00")

        cfg = INSTRUMENTS[instrument]

        # ── Pre-expiry forced exit ─────────────────────────────────────────────
        if now >= pre_exit:
            q = _multiquote([(v["symbol"], OPT_EXCHANGE) for v in legs.values()])
            prices = {k: q.get(v["symbol"], v["entry_prem"]) for k, v in legs.items()}
            logger.info(f"[{instrument}] PRE-EXPIRY EXIT  now={now}  threshold={pre_exit}")
            await self._exit(instrument, "pre_expiry", prices)
            return

        # Safety net: still open on expiry day
        if now.date() >= expiry_date:
            q = _multiquote([(v["symbol"], OPT_EXCHANGE) for v in legs.values()])
            prices = {k: q.get(v["symbol"], v["entry_prem"]) for k, v in legs.items()}
            logger.warning(f"[{instrument}] Open on expiry day — forced close")
            await self._exit(instrument, "forced_expiry", prices)
            return

        # ── Fetch current prices + spot — ONE batched round-trip ──────────────
        items = [(leg["symbol"], OPT_EXCHANGE) for leg in legs.values()]
        items.append((cfg["idx_symbol"], IDX_EXCHANGE))
        q = _multiquote(items)
        prices = {k: q.get(leg["symbol"], leg["entry_prem"]) for k, leg in legs.items()}
        spot   = q.get(cfg["idx_symbol"], 0.0)

        # ── MTM ───────────────────────────────────────────────────────────────
        mtm = compute_mtm(legs, prices, lot_size, n_c, n_p)
        self.state[instrument]["current_mtm"]  = round(mtm, 0)
        self.state[instrument]["current_spot"] = spot
        _save_state(self.state)

        logger.info(
            f"[{instrument}] spot={spot:.0f}  MTM=₹{mtm:+,.0f}"
            f"  BE=[{s['be_lower']:.0f},{s['be_upper']:.0f}]"
            f"  tgt=₹{s['profit_target']:,.0f}"
        )

        # ── Exit 1: Profit target ─────────────────────────────────────────────
        if mtm >= s["profit_target"]:
            logger.info(f"[{instrument}] 🎯 TARGET HIT  MTM=₹{mtm:,.0f}")
            await self._exit(instrument, "target", prices)
            return

        # ── Exit 2: Breakeven stop (spot-based) ───────────────────────────────
        if spot > 0 and (spot <= s["be_lower"] or spot >= s["be_upper"]):
            logger.info(
                f"[{instrument}] 🛑 BE STOP  spot={spot:.0f}"
                f"  BE=[{s['be_lower']:.0f},{s['be_upper']:.0f}]"
            )
            await self._exit(instrument, "be_stop", prices)
            return

    # ──────────────────────────────────────────────────────────────────────────
    #  ENTRY CHECK — one instrument
    # ──────────────────────────────────────────────────────────────────────────

    async def _check_entry(self, instrument: str, now: datetime) -> None:
        today = now.date()
        s     = self.state.get(instrument, {})

        # Get current cycle info
        cycle = _get_cycle(INSTRUMENTS[instrument]["underlying"])
        if not cycle:
            return

        month_key  = cycle["month_key"]
        entry_day  = cycle["entry_day"]

        # Already have a state for this month (in-trade or skipped)
        if not s.get("closed", True) and s.get("month_key") == month_key:
            return   # in trade this month
        if s.get("closed", True) and s.get("month_key") == month_key and s.get("exit_reason"):
            return   # already handled this month (skipped or exited)

        # Too early or not a weekday
        if today < entry_day or today.weekday() >= 5:
            return

        # Grace window: how many trading days past entry_day are we?
        tdays_late = _trading_days_between(entry_day, today)
        if tdays_late > ENTRY_GRACE_TDAYS:
            logger.info(
                f"[{instrument}] Entry window expired (day {tdays_late} of month,"
                f" grace={ENTRY_GRACE_TDAYS})"
            )
            # Mark as skipped so we don't log this every poll
            self.state[instrument] = {
                "closed": True, "month_key": month_key,
                "exit_reason": "entry_window_expired", "entry_time": None,
            }
            _save_state(self.state)
            return

        # Check entry time window (10:00–10:20 IST)
        t = now.time()
        if not (t.hour == ENTRY_HOUR and t.minute < ENTRY_WINDOW_MINS):
            return

        logger.info(
            f"[{instrument}] ENTRY WINDOW  today={today}  entry_day={entry_day}"
            f"  expiry={cycle['expiry_str']}  DTE={cycle['dte']}"
        )
        await self._enter(instrument, cycle)

    # ──────────────────────────────────────────────────────────────────────────
    #  MAIN LOOP
    # ──────────────────────────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info("=" * 64)
        logger.info(f"🚀  {STRATEGY_NAME}  |  orders fire via OpenAlgo "
                    f"(paper/live mode is set in the OpenAlgo UI, not here)")
        logger.info(f"    NIFTY N_C={INSTRUMENTS['NIFTY']['n_c']} N_P={INSTRUMENTS['NIFTY']['n_p']}")
        logger.info(f"    BANKNIFTY N_C={INSTRUMENTS['BANKNIFTY']['n_c']} N_P={INSTRUMENTS['BANKNIFTY']['n_p']}")
        logger.info(f"    Entry: 10:00 IST  Grace: {ENTRY_GRACE_TDAYS} tdays")
        logger.info(f"    Pre-expiry exit: {PRE_EXPIRY_TDAYS} tdays before expiry @ {PRE_EXPIRY_HOUR}:{PRE_EXPIRY_MIN:02d}")
        logger.info(f"    IV filter: ATM call IV < {MIN_IV:.0%} → skip")

        # Restore open positions from state file
        for inst in ("NIFTY", "BANKNIFTY"):
            s = self.state.get(inst, {})
            if not s.get("closed", True):
                logger.info(
                    f"♻️  [{inst}] restored open position from {s.get('entry_time')}"
                    f"  expiry={s.get('expiry_str')}  pre_exit={s.get('pre_exit_date')}"
                )
            else:
                logger.info(f"   [{inst}] no open position — awaiting entry.")

        while True:
            now = datetime.now()
            t   = now.time()

            # Outside market hours
            before_open = t < dt_time(9, 15)
            after_close = t.hour > 15 or (t.hour == 15 and t.minute >= 30)
            if before_open or after_close:
                await asyncio.sleep(60)
                continue

            for inst in ("NIFTY", "BANKNIFTY"):
                try:
                    s = self.state.get(inst, {})
                    if not s.get("closed", True):
                        await self._monitor(inst, now)
                    else:
                        await self._check_entry(inst, now)
                except Exception as e:
                    logger.exception(f"[{inst}] unexpected error: {e}")

            await asyncio.sleep(POLL_SECS)


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if not API_KEY:
        logger.error("OPENALGO_API_KEY not set in .env — aborting.")
        sys.exit(1)
    asyncio.run(FlatBlueLineBot().run())
