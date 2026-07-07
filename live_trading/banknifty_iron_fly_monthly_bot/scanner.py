"""
scanner.py — Entry signal detection for BANKNIFTY Monthly Iron Fly.
Scans once per session at 09:55 IST. No position = evaluate entry.

Filters (Stage 8 re-evaluated 2026-06-09 — single hard gate):
  - VIX >= 10     — skip only extreme-low-volatility months (INDIA VIX < 10)

ADX(14) is computed and logged for reference but is NOT a hard gate.
Research showed ADX alone does not improve on unfiltered Sharpe; removing it
restores ~5 missed OOS cycles (Aug–Dec 2025) with no net harm to strategy quality.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from live_trading.api_utils import HOST, get_expiry_dates, is_market_holiday

logger = logging.getLogger(__name__)

API_KEY        = __import__("os").getenv("OPENALGO_API_KEY")
VIX_SYMBOL     = "INDIAVIX"
IDX_EXCHANGE   = "NSE_INDEX"
OPT_EXCHANGE   = "NFO"
ATM_STEP       = 100      # BANKNIFTY strike increment
HEDGE_DELTA    = 0.10     # champion param (v2 re-study, all 10 stages, 2026-07-05)
RISK_FREE_RATE = 0.07
MIN_DTE        = 2
ADX_PERIOD     = 14       # used for informational ADX logging only (not a gate)
VIX_MIN        = 10.0     # hard gate: skip if INDIA VIX < 10 (extreme low vol)


# ── helpers ──────────────────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _black76_delta(F: float, K: float, T: float, sigma: float,
                   r: float, opt_type: str) -> float:
    if T <= 0 or sigma <= 0 or F <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    disc = math.exp(-r * T)
    if opt_type == "CE":
        return disc * _norm_cdf(d1)
    return disc * (_norm_cdf(d1) - 1.0)


def _parse_expiry(d: str) -> date:
    return datetime.strptime(d, "%d%b%y").date()


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
        logger.error(f"Quote failed {symbol}@{exchange}: {e}")
    return 0.0


def get_bnf_spot() -> float:
    return _quote("BANKNIFTY", IDX_EXCHANGE)


def get_vix() -> float:
    return _quote(VIX_SYMBOL, IDX_EXCHANGE)


def _resolve_symbol(underlying: str, exchange: str, expiry_str: str,
                    opt_type: str, strike: int | None = None,
                    atm: int | None = None) -> str | None:
    """
    /api/v1/optionsymbol selects strikes by offset from ATM (ATM/ITMn/OTMn) —
    strike_int is the strike interval, not an absolute strike. Translate the
    requested strike into an OTM offset relative to the given ATM; without a
    reference ATM the request resolves to the ATM strike.
    """
    if strike is None or atm is None or strike == atm:
        offset = "ATM"
    else:
        # Hedge strikes are OTM by construction (CE above / PE below ATM)
        offset = f"OTM{round(abs(strike - atm) / ATM_STEP)}"
    payload = {
        "apikey": API_KEY,
        "underlying": underlying,
        "exchange": exchange,
        "expiry_date": expiry_str,
        "option_type": opt_type,
        "offset": offset,
        "strike_int": ATM_STEP,
    }

    try:
        res = requests.post(f"{HOST}/api/v1/optionsymbol", json=payload, timeout=5)
        if res.status_code == 200:
            data = res.json()
            if data.get("status") == "success":
                return data.get("symbol")
    except Exception as e:
        logger.error(f"optionsymbol error: {e}")
    return None


def find_hedge_strike(spot: float, expiry_date: date,
                     vix: float, opt_type: str) -> int:
    T     = max((expiry_date - date.today()).days / 365.0, 1.0 / 365.0)
    sigma = vix / 100.0
    F     = spot
    atm   = int(round(spot / ATM_STEP) * ATM_STEP)
    step  = 1 if opt_type == "CE" else -1

    best_strike, best_diff = atm, float("inf")
    last_delta = float("inf")

    for n in range(1, 30):
        K     = atm + step * n * ATM_STEP
        delta = abs(_black76_delta(F, K, T, sigma, RISK_FREE_RATE, opt_type))
        diff  = abs(delta - HEDGE_DELTA)
        if diff < best_diff:
            best_diff, best_strike = diff, K
        last_delta = delta
        if last_delta < HEDGE_DELTA - 0.03:
            break
    return best_strike


# ── ADX filter ────────────────────────────────────────────────────────────────

def _compute_adx(bars: list[dict], period: int = ADX_PERIOD) -> float | None:
    """
    Compute Wilder's ADX from a list of OHLC bar dicts.
    Each bar must have keys: 'high', 'low', 'close'.
    Returns ADX value, or None if insufficient data.

    Needs ~5x period of warm-up bars for the Wilder smoothing to converge —
    too few bars (previously period+2 = 16) inflates the reading ~2x, same
    cold-start bug found and fixed in banknifty_bb_opening_candle_bot.py.
    """
    n = len(bars)
    if n < period * 5:
        return None

    plus_dm:  list[float] = []
    minus_dm: list[float] = []
    tr_list:  list[float] = []

    for i in range(1, n):
        high, low       = bars[i]["high"],   bars[i]["low"]
        p_high, p_low   = bars[i-1]["high"], bars[i-1]["low"]
        p_close         = bars[i-1]["close"]

        up   = high - p_high
        down = p_low - low
        plus_dm.append(max(up, 0.0)   if up > down   else 0.0)
        minus_dm.append(max(down, 0.0) if down > up   else 0.0)
        tr_list.append(max(high - low, abs(high - p_close), abs(low - p_close)))

    def wilder_smooth(vals: list[float], p: int) -> list[float]:
        """Wilder's Running Moving Average."""
        if len(vals) < p:
            return []
        result = [sum(vals[:p])]
        for v in vals[p:]:
            result.append(result[-1] - result[-1] / p + v)
        return result

    tr_s  = wilder_smooth(tr_list,  period)
    pdm_s = wilder_smooth(plus_dm,  period)
    mdm_s = wilder_smooth(minus_dm, period)

    min_len = min(len(tr_s), len(pdm_s), len(mdm_s))
    if min_len == 0:
        return None

    dx_list: list[float] = []
    for i in range(min_len):
        atr = tr_s[i]
        if atr == 0:
            dx_list.append(0.0)
            continue
        pdi  = 100.0 * pdm_s[i] / atr
        mdi  = 100.0 * mdm_s[i] / atr
        summ = pdi + mdi
        dx_list.append(100.0 * abs(pdi - mdi) / summ if summ > 0 else 0.0)

    if len(dx_list) < period:
        return None

    # Smooth DX → ADX
    adx = sum(dx_list[-period:]) / period
    return adx


def _get_adx() -> float | None:
    """
    Fetch ~180 calendar days (~120 trading days) of daily bars from OpenAlgo
    and compute ADX(14) — wide enough for Wilder smoothing to converge.
    Returns None on any data failure — caller should treat None as filter pass.
    """
    end_date   = date.today()
    start_date = end_date - timedelta(days=180)
    try:
        res = requests.post(
            f"{HOST}/api/v1/history",
            json={
                "apikey":     API_KEY,
                "symbol":     "BANKNIFTY",
                "exchange":   IDX_EXCHANGE,
                "interval":   "D",
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date":   end_date.strftime("%Y-%m-%d"),
            },
            timeout=10,
        )
        if res.status_code == 200:
            data = res.json()
            if data.get("status") == "success":
                bars = data.get("data", [])
                if bars:
                    return _compute_adx(bars, ADX_PERIOD)
    except Exception as e:
        logger.warning(f"ADX fetch failed: {e}")
    return None


# ── expiry calendar helpers ────────────────────────────────────────────────────

def _get_all_monthly_expiries() -> list[tuple[str, date]]:
    """
    Fetch all BANKNIFTY expiries from OpenAlgo and filter to monthly-only.
    Monthly = consecutive expiries >= 25 days apart.
    Returns sorted list of (expiry_str, expiry_date), oldest first.
    """
    raw = get_expiry_dates(API_KEY, "BANKNIFTY", OPT_EXCHANGE, "options")
    if not raw:
        return []

    # Parse and sort
    parsed: list[tuple[date, str]] = []
    for d in raw:
        try:
            parsed.append((_parse_expiry(d), d))
        except ValueError:
            continue
    parsed.sort(key=lambda x: x[0])

    # Keep only monthly (gap >= 25 days between consecutive expiries)
    monthly: list[tuple[str, date]] = []
    for exp_dt, d in parsed:
        if not monthly:
            monthly.append((d, exp_dt))
        elif (exp_dt - monthly[-1][1]).days >= 25:
            monthly.append((d, exp_dt))

    return monthly


def _entry_date_from_prior_expiry(prior_expiry_date: date) -> date:
    """
    First trading day after the prior monthly expiry.
    Skips weekends and market holidays.
    """
    entry = prior_expiry_date + timedelta(days=1)
    while entry.weekday() >= 5 or is_market_holiday(API_KEY, entry.strftime("%Y-%m-%d")):
        entry += timedelta(days=1)
    return entry


# The broker's expiry list only contains FUTURE dates, so on the entry morning
# itself the just-expired contract is gone and the entry day can no longer be
# derived from the list. Persist it while the prior expiry is still visible
# (the days before expiry) and read it back on the entry morning.
ENTRY_MEMO_FILE = (Path(__file__).parent.parent / "logs"
                   / "banknifty_iron_fly_monthly_entry_memo.json")


def _save_entry_memo(entry_date: date, expiry_str: str, prior_date: date) -> None:
    try:
        ENTRY_MEMO_FILE.write_text(json.dumps({
            "next_entry":    entry_date.isoformat(),
            "target_expiry": expiry_str,
            "prior_expiry":  prior_date.isoformat(),
        }))
    except OSError as e:
        logger.warning(f"Could not persist entry memo: {e}")


def _load_entry_memo() -> dict | None:
    try:
        return json.loads(ENTRY_MEMO_FILE.read_text())
    except (OSError, ValueError):
        return None


# ── main scan ──────────────────────────────────────────────────────────────────

def scan() -> dict | None:
    """
    Evaluate entry signal for today.
    Returns entry signal dict on signal, None on no-trade.
    """
    # ── expiry calendar: find current and prior monthly expiry ────────────────
    all_monthly = _get_all_monthly_expiries()
    if len(all_monthly) < 2:
        logger.error("Need at least 2 monthly expiries to compute entry date.")
        return None

    today = date.today()

    # Find current cycle: nearest upcoming expiry with DTE >= MIN_DTE
    current_idx: int | None = None
    for i, (_, exp_dt) in enumerate(all_monthly):
        if (exp_dt - today).days >= MIN_DTE:
            current_idx = i
            break

    if current_idx is None:
        logger.warning("No upcoming monthly expiry with DTE >= MIN_DTE found.")
        return None

    # current_idx == 0 covers the entry morning itself: the prior expiry has
    # already dropped off the broker's list, so use the persisted memo.
    if current_idx == 0:
        expiry_str, expiry_date = all_monthly[0]
        memo = _load_entry_memo()
        if (memo
                and memo.get("target_expiry") == expiry_str
                and memo.get("next_entry") == today.isoformat()):
            entry_date = today
            prior_iso  = memo.get("prior_expiry")
            prior_date = (date.fromisoformat(prior_iso) if prior_iso
                          else today - timedelta(days=1))
        else:
            if datetime.now().minute == 0:
                logger.info(
                    f"Today={today} is mid-cycle for the {expiry_date} contract "
                    f"(entry memo: {memo}). Next entry is computed near expiry."
                )
            return None
    else:
        # Standard case: we have a prior expiry in the list
        expiry_str,  expiry_date = all_monthly[current_idx]
        _prior_str,  prior_date  = all_monthly[current_idx - 1]
        entry_date = _entry_date_from_prior_expiry(prior_date)
        # Remember for the entry morning, when prior_date is no longer listed
        _save_entry_memo(entry_date, expiry_str, prior_date)

    if today != entry_date:
        if datetime.now().minute == 0:
            logger.info(
                f"Today={today} is not an entry day. "
                f"Next entry={entry_date}  (after prior_exp={prior_date})  "
                f"Target expiry={expiry_date}"
            )
        return None

    logger.info(
        f"Entry day detected: prior_exp={prior_date}  "
        f"expiry={expiry_str} ({expiry_date})"
    )

    # ── VIX filter (hard gate) ────────────────────────────────────────────────
    vix = get_vix()
    if vix <= 0:
        logger.warning("  VIX data unavailable — FAILING entry (data error).")
        return None
    elif vix < VIX_MIN:
        logger.info(f"  ⛔ VIX={vix:.2f} < {VIX_MIN} — SKIP (low volatility).")
        return None
    else:
        logger.info(f"  ✅ VIX={vix:.2f} >= {VIX_MIN}")

    # ── ADX (informational only — not a hard gate) ────────────────────────────
    adx = _get_adx()
    if adx is not None:
        logger.info(f"  ADX={adx:.1f} (informational)")
    else:
        logger.warning("  ADX data unavailable — proceeding (ADX is not a hard gate)")

    # ── BANKNIFTY spot → ATM strike ───────────────────────────────────────────
    spot = get_bnf_spot()
    if spot <= 0:
        logger.error("  BANKNIFTY spot = 0 — aborting.")
        return None

    atm = int(round(spot / ATM_STEP) * ATM_STEP)
    logger.info(f"  Spot={spot:.1f}  ATM={atm}")

    # Use VIX for hedge strike computation; fallback to 15% if unavailable
    vix_for_hedge = vix if vix > 0 else 15.0
    hedge_ce_k = find_hedge_strike(spot, expiry_date, vix_for_hedge, "CE")
    hedge_pe_k = find_hedge_strike(spot, expiry_date, vix_for_hedge, "PE")

    # ── Resolve all 4 option symbols ──────────────────────────────────────────
    buy_ce_sym  = _resolve_symbol("BANKNIFTY", OPT_EXCHANGE, expiry_str, "CE", hedge_ce_k, atm)
    buy_pe_sym  = _resolve_symbol("BANKNIFTY", OPT_EXCHANGE, expiry_str, "PE", hedge_pe_k, atm)
    sell_ce_sym = _resolve_symbol("BANKNIFTY", OPT_EXCHANGE, expiry_str, "CE", atm, atm)
    sell_pe_sym = _resolve_symbol("BANKNIFTY", OPT_EXCHANGE, expiry_str, "PE", atm, atm)

    if not all([buy_ce_sym, buy_pe_sym, sell_ce_sym, sell_pe_sym]):
        logger.error("Failed to resolve one or more symbols.")
        return None

    logger.info(f"  BUY  CE hedge → {buy_ce_sym} (K={hedge_ce_k})")
    logger.info(f"  BUY  PE hedge → {buy_pe_sym} (K={hedge_pe_k})")
    logger.info(f"  SELL CE ATM   → {sell_ce_sym} (K={atm})")
    logger.info(f"  SELL PE ATM   → {sell_pe_sym} (K={atm})")

    # ── Exit date: 15:15 on day-before-expiry ─────────────────────────────────
    exit_day = expiry_date - timedelta(days=1)
    while exit_day.weekday() >= 5:
        exit_day -= timedelta(days=1)

    return {
        "expiry_str":    expiry_str,
        "expiry_date":   expiry_date,
        "entry_date":    entry_date,
        "exit_day":      exit_day,
        "atm_strike":    atm,
        "hedge_ce_k":    hedge_ce_k,
        "hedge_pe_k":    hedge_pe_k,
        "buy_ce_sym":    buy_ce_sym,
        "buy_pe_sym":    buy_pe_sym,
        "sell_ce_sym":   sell_ce_sym,
        "sell_pe_sym":   sell_pe_sym,
        "vix_at_entry":  vix,
        "spot_at_entry": spot,
    }
