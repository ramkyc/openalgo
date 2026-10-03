"""
paper_trader.py — Position tracking, delta monitoring, adjustments, P&L.
BANKNIFTY Monthly Short Iron Fly (paper mode).
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import time
from datetime import date, datetime, timedelta, time as dt_time
from pathlib import Path

import requests

from live_trading.api_utils import HOST
from live_trading.shared.order_fill import fetch_fill_price
from live_trading.shared.trade_logger import log_trade_to_db
from live_trading.shared.poll_watchdog import PollWatchdog

logger = logging.getLogger(__name__)

API_KEY        = __import__("os").getenv("OPENALGO_API_KEY")
STRATEGY_NAME  = "BANKNIFTY_IRON_FLY_MONTHLY"
PAPER_MODE     = False   # Set to False to place orders in sandbox (analyzer mode)
OPT_EXCHANGE   = "NFO"
IDX_EXCHANGE   = "NSE_INDEX"
N_LOTS         = 10
DEFAULT_LOT_SIZE = 30     # BANKNIFTY post-Nov 2025
ATM_STEP       = 100
HEDGE_DELTA    = 0.10
ADJ_LOW_TRIG   = 0.20
ADJ_HI_TRIG    = 0.75
ADJ_HYSTERESIS = 2        # 2 consecutive polls before adjustment fires
PROFIT_TARGET  = 0.50
RISK_FREE_RATE = 0.07
ADJ_HOUR, ADJ_MIN   = 10, 0
EXIT_HOUR, EXIT_MIN = 15, 15

LOG_DIR = Path(__file__).parent.parent / "logs"
PAPER_CSV = LOG_DIR / "banknifty_iron_fly_monthly_paper_trades.csv"
STATE_FILE = LOG_DIR / "banknifty_iron_fly_monthly_state.json"

# REST-poll watchdog (frozen-quote / poll-failure alerts). main.py's main()
# runs a persistent asyncio event loop (asyncio.run(main())), so _quote()
# (called synchronously from that loop) bridges to the watchdog's async
# check() via a fire-and-forget task on the already-running loop, rather
# than asyncio.run() (which would fail — a loop is already running).
# Market hours mirror main.py's own poll-window check: 09:15–15:30 IST.
_watchdog = PollWatchdog(
    bot_name="BankNifty Iron Fly Monthly Bot",
    market_open=dt_time(9, 15),
    market_close=dt_time(15, 30),
    bot_logger=logger,
)


def _watchdog_check(symbol: str, value: float | None, success: bool) -> None:
    """Schedule a PollWatchdog.check() from a sync call site. Only fires if
    called from inside a running event loop (main.py's main() always is in
    production); silently no-ops otherwise."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(_watchdog.check(symbol, value, success))


# ── Black-76 helpers ─────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _black76_delta(F: float, K: float, T: float, sigma: float,
                   r: float, opt_type: str) -> float:
    if T <= 0 or sigma <= 0 or F <= 0 or K <= 0:
        return 0.0
    d1  = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    disc = math.exp(-r * T)
    return (disc * _norm_cdf(d1)) if opt_type == "CE" else (disc * (_norm_cdf(d1) - 1.0))


def _black76_iv(price: float, F: float, K: float, T: float,
                r: float, opt_type: str) -> float:
    """Implied vol via Black-76 (Newton-Raphson, 50 iterations)."""
    if price <= 0 or T <= 0 or F <= 0 or K <= 0:
        return 0.0
    intrinsic = max(K - F, 0.0) if opt_type == "PE" else max(F - K, 0.0)
    if price <= intrinsic:
        return 0.0
    sigma = 0.20
    for _ in range(50):
        d1   = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
        disc = math.exp(-r * T)
        if opt_type == "CE":
            cdf_d1 = _norm_cdf(d1)
            price_model = disc * F * cdf_d1 - disc * K * _norm_cdf(d1 - sigma * math.sqrt(T))
        else:
            cdf_neg_d1 = _norm_cdf(-d1)
            price_model = disc * K * cdf_neg_d1 - disc * F * _norm_cdf(-d1 - sigma * math.sqrt(T))
        diff = price_model - price
        if abs(diff) < 1e-6:
            break
        vega = disc * F * math.sqrt(T) * (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-d1 ** 2 / 2)
        if vega < 1e-6:
            break
        sigma -= diff / vega
        sigma = max(sigma, 0.001)
    return max(sigma, 0.0)


def _get_leg_delta(spot: float, strike: float, T: float, sigma: float,
                   opt_type: str) -> float:
    F = spot * math.exp(RISK_FREE_RATE * T)
    return _black76_delta(F, strike, T, sigma, RISK_FREE_RATE, opt_type)


# ── price helpers ────────────────────────────────────────────────────────────

def _quote(symbol: str, exchange: str) -> float:
    """Fetch LTP, then feed the result to the poll watchdog before returning."""
    price = _quote_impl(symbol, exchange)
    _watchdog_check(symbol, price if price > 0 else None, success=(price > 0))
    return price


def _quote_impl(symbol: str, exchange: str) -> float:
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
        logger.warning(f"Quote failed {symbol}@{exchange}: {e}")
    return 0.0


def _get_bnf_spot() -> float:
    return _quote("BANKNIFTY", IDX_EXCHANGE)


def _get_option_price(sym: str, retries: int = 3, delay: float = 5.0) -> float:
    """Quote with retries — transient failures (rate limits) return 0 once."""
    for attempt in range(retries):
        p = _quote(sym, OPT_EXCHANGE)
        if p > 0:
            return p
        if attempt < retries - 1:
            time.sleep(delay)
    logger.error(f"  LTP still 0 after {retries} attempts for {sym}")
    return 0.0


# ── Greeks monitoring (display only — never drives an order) ─────────────────
# "Broker" Greeks = OpenAlgo /optiongreeks: Black-76 with the IV implied from each
# leg's own live LTP (Fyers sends no Greeks). The bot's model delta (_get_leg_delta)
# uses the ENTRY VIX as a flat vol; the gap between the two shows how far the
# adjustment trigger is from what the market prices.
GREEKS_EVERY_S = 60       # 4 legs per refresh; endpoint is limited to 30/min


def _broker_greeks(symbol: str) -> dict | None:
    try:
        res = requests.post(
            f"{HOST}/api/v1/optiongreeks",
            json={"apikey": API_KEY, "symbol": symbol, "exchange": OPT_EXCHANGE,
                  "interest_rate": RISK_FREE_RATE * 100},
            timeout=5,
        )
        if res.status_code == 200:
            d = res.json()
            if d.get("status") == "success" and (d.get("greeks") or {}).get("delta") is not None:
                g = d["greeks"]
                return {"delta": abs(float(g["delta"])), "iv": d.get("implied_volatility"),
                        "gamma": g.get("gamma"), "theta": g.get("theta"), "vega": g.get("vega")}
        logger.warning(f"  Greeks API {res.status_code} for {symbol}: {res.text[:120]}")
    except Exception as e:
        logger.warning(f"  Greeks fetch failed {symbol}: {e}")
    return None


def _trigger_delta(symbol: str, strike: float, opt_type: str, ltp: float, spot: float,
                   expiry_date: date, now: datetime, flat_sigma: float, T_days: float) -> tuple[float, str]:
    """|delta| that drives the adjustment trigger, on the basis the backtests validated
    (study banknifty_iron_fly_delta_basis_study, 2026-10-03): per-leg implied vol from the
    leg's own LTP. Order: OpenAlgo /optiongreeks -> local per-leg-IV Black-76 (minute-level T
    to 15:30 expiry) -> legacy flat entry-VIX (flat VIX was materially worse in backtest)."""
    bg = _broker_greeks(symbol)
    if bg and bg.get("delta") is not None:
        return abs(bg["delta"]), "broker"
    try:
        exp_dt = datetime.combine(expiry_date, dt_time(15, 30))
        T = (exp_dt - now.replace(tzinfo=None)).total_seconds() / (365 * 24 * 3600)
        if ltp > 0 and T > 0:
            F = spot * math.exp(RISK_FREE_RATE * T)
            iv = _black76_iv(ltp, F, strike, T, RISK_FREE_RATE, opt_type)
            if iv > 0:
                return abs(_black76_delta(F, strike, T, iv, RISK_FREE_RATE, opt_type)), "local_iv"
    except Exception as e:
        logger.warning(f"  local IV delta failed {symbol}: {e}")
    return abs(_get_leg_delta(spot, strike, T_days, flat_sigma, opt_type)), "flat_vix_fallback"


def _opt_type(tag: str) -> str:
    return "CE" if tag.endswith("ce") else "PE"


# ── order placement ──────────────────────────────────────────────────────────

def _placeorder(symbol: str, action: str, qty: int) -> dict | None:
    """
    Place a NRML market order via OpenAlgo.
    In PAPER_MODE, logs only. BUY legs must always be placed before SELL legs
    by the caller to gain the margin benefit.

    Returns the OpenAlgo response dict (containing 'orderid') on success, or
    None on failure/PAPER_MODE. Callers relying on the old bool semantics can
    still use `if not _placeorder(...)` — None and a non-empty dict behave
    identically as falsy/truthy in that context.
    """
    if PAPER_MODE:
        logger.info(f"  [PAPER] {action} {qty}× {symbol}")
        return None
    payload = {
        "apikey":    API_KEY,
        "strategy":  STRATEGY_NAME,
        "symbol":    symbol,
        "action":    action,
        "exchange":  OPT_EXCHANGE,
        "pricetype": "MARKET",
        "product":   "NRML",
        "quantity":  str(qty),
    }
    try:
        res = requests.post(f"{HOST}/api/v1/placeorder", json=payload, timeout=10)
        if res.status_code == 200:
            data = res.json()
            if data.get("status") == "success":
                logger.info(f"  ✅ Order placed: {action} {qty}× {symbol}")
                return data
        logger.error(f"  ❌ Order failed ({symbol}): {res.text}")
    except Exception as e:
        logger.error(f"  ❌ Order exception ({symbol}): {e}")
    return None


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id:
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback


# ── symbol resolution ───────────────────────────────────────────────────────

def _resolve_symbol(underlying: str, exchange: str, expiry_str: str,
                    opt_type: str, strike: int | None = None) -> str | None:
    payload = {
        "apikey": API_KEY,
        "underlying": underlying,
        "exchange": exchange,
        "expiry_date": expiry_str,
        "option_type": opt_type,
    }
    payload["offset"] = "ATM"
    if strike is not None:
        payload["strike_int"] = strike

    try:
        res = requests.post(f"{HOST}/api/v1/optionsymbol", json=payload, timeout=5)
        if res.status_code == 200:
            data = res.json()
            if data.get("status") == "success":
                return data.get("symbol")
    except Exception as e:
        logger.error(f"optionsymbol error: {e}")
    return None


# ── lot size ────────────────────────────────────────────────────────────────

def _get_lot_size() -> int:
    try:
        from database.token_db import get_symbol_info
        si = get_symbol_info("BANKNIFTY", OPT_EXCHANGE)
        if si and getattr(si, "lotsize", None):
            return int(si.lotsize)
    except Exception:
        pass
    return DEFAULT_LOT_SIZE


# ── state ────────────────────────────────────────────────────────────────────────

def load_state() -> dict | None:
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
    try:
        state["last_update"] = datetime.now().isoformat()
        STATE_FILE.write_text(json.dumps(state, indent=2, default=str))
    except Exception as e:
        logger.error(f"State save failed: {e}")


# ── CSV logging ──────────────────────────────────────────────────────────────

def _log_csv(action: str, leg: str, symbol: str, premium: float,
             qty: int, pnl: float, reason: str) -> None:
    write_header = not PAPER_CSV.exists()
    try:
        with open(PAPER_CSV, "a", newline="") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow(["timestamp", "action", "leg", "symbol",
                            "premium", "qty", "pnl_gross", "reason", "paper_mode"])
            w.writerow([datetime.now().isoformat(), action, leg, symbol,
                        round(premium, 2), qty, round(pnl, 2), reason, PAPER_MODE])
    except Exception as e:
        logger.warning(f"CSV log failed: {e}")


# ── P&L ─────────────────────────────────────────────────────────────────────

def compute_pnl(legs: dict, prices: dict, qty: int) -> float:
    sc = (legs["sell_ce"]["entry_prem"] - prices["sell_ce"]) * qty
    sp = (legs["sell_pe"]["entry_prem"] - prices["sell_pe"]) * qty
    bc = (prices["buy_ce"]  - legs["buy_ce"]["entry_prem"])  * qty
    bp = (prices["buy_pe"]  - legs["buy_pe"]["entry_prem"])  * qty
    return sc + sp + bc + bp


# ── PaperTrader ─────────────────────────────────────────────────────────────

class PaperTrader:
    """
    BANKNIFTY Monthly Iron Fly paper trader.
    Polling-based (30s interval) — no WebSocket needed.

    Entry signal comes from scanner.py. This class owns the position
    lifecycle: entry, MTM monitoring, delta adjustments, exit.
    """

    def __init__(self):
        self.state: dict | None = None
        self._last_greeks_ts = 0.0

    # ── delta bookkeeping (display only) ─────────────────────────────────────

    def _stamp_entry_delta(self, tag: str, leg: dict, spot: float, T: float,
                           sigma: float, now: datetime) -> None:
        """Record the deltas at which this leg was put on (model + broker)."""
        try:
            leg["entry_delta"] = round(abs(_get_leg_delta(spot, leg["strike"], T, sigma, _opt_type(tag))), 4)
            leg["entry_spot"] = round(spot, 2)
            leg["entry_ts"] = now.isoformat(timespec="seconds")
            bg = _broker_greeks(leg["symbol"])
            if bg:
                leg["entry_broker_delta"] = round(bg["delta"], 4)
                leg["entry_broker_iv"] = bg["iv"]
        except Exception as e:
            logger.warning(f"  entry delta stamp failed for {tag}: {e}")

    def _monitor_deltas(self, spot: float, T: float, sigma: float, now: datetime) -> None:
        """Refresh model |delta| every poll and broker |delta|/IV every GREEKS_EVERY_S, all open legs."""
        try:
            poll_broker = (time.time() - self._last_greeks_ts) >= GREEKS_EVERY_S
            for tag, leg in self.state["legs"].items():
                if leg.get("closed"):
                    continue
                leg["delta"] = round(abs(_get_leg_delta(spot, leg["strike"], T, sigma, _opt_type(tag))), 4)
                if poll_broker:
                    bg = _broker_greeks(leg["symbol"])
                    if bg:
                        leg["broker_delta"] = round(bg["delta"], 4)
                        leg["broker_iv"] = bg["iv"]
                        leg["broker_ts"] = now.isoformat(timespec="seconds")
            if poll_broker:
                self._last_greeks_ts = time.time()
            self.state["delta_ts"] = now.isoformat(timespec="seconds")
            self.state["spot_now"] = round(spot, 2)
            self.state["model_sigma"] = round(sigma, 4)
            self.state["adj_band"] = [ADJ_LOW_TRIG, ADJ_HI_TRIG]
            save_state(self.state)
        except Exception as e:
            logger.warning(f"  delta monitor failed (ignored): {e}")

    def _record_adjustment(self, tag: str, old_sym: str, old_strike: int, old_leg_snapshot: dict,
                           trigger_delta: float, old_fill: float, new_fill: float, pnl: float,
                           spot: float, T: float, sigma: float, now: datetime) -> None:
        """Append to state['adjustments'] and stamp entry deltas on the new leg."""
        leg = self.state["legs"][tag]
        self._stamp_entry_delta(tag, leg, spot, T, sigma, now)
        self.state.setdefault("adjustments", []).append({
            "ts": now.isoformat(timespec="seconds"),
            "leg": tag,
            "old_symbol": old_sym, "old_strike": old_strike,
            "new_symbol": leg["symbol"], "new_strike": leg["strike"],
            "side": "high" if trigger_delta > ADJ_HI_TRIG else "low",
            "trigger_delta": round(trigger_delta, 4),
            "trigger_broker_delta": old_leg_snapshot.get("broker_delta"),
            "old_entry_delta": old_leg_snapshot.get("entry_delta"),
            "spot": round(spot, 2),
            "old_fill": round(old_fill, 2), "new_fill": round(new_fill, 2),
            "realized_pnl": round(pnl, 0),
            "new_entry_delta": leg.get("entry_delta"),
            "new_entry_broker_delta": leg.get("entry_broker_delta"),
        })

    # ── entry ────────────────────────────────────────────────────────────────

    async def enter(self, signal: dict) -> bool:
        """
        Open all 4 legs of the iron fly.
        BUY legs placed before SELL legs for margin benefit.
        Returns True if position entered, False on failure.
        """
        logger.info("═" * 64)
        logger.info("⚡ BANKNIFTY IRON FLY ENTRY")

        expiry_str  = signal["expiry_str"]
        expiry_date = signal["expiry_date"]
        exit_day    = signal["exit_day"]
        atm         = signal["atm_strike"]
        vix         = signal["vix_at_entry"]
        spot        = signal["spot_at_entry"]

        # ── fetch entry premiums ──────────────────────────────────────────────
        buy_ce_prem  = _get_option_price(signal["buy_ce_sym"])
        buy_pe_prem  = _get_option_price(signal["buy_pe_sym"])
        sell_ce_prem = _get_option_price(signal["sell_ce_sym"])
        sell_pe_prem = _get_option_price(signal["sell_pe_sym"])

        for name, p in [
            ("buy_ce", buy_ce_prem), ("buy_pe", buy_pe_prem),
            ("sell_ce", sell_ce_prem), ("sell_pe", sell_pe_prem),
        ]:
            if p <= 0:
                logger.error(f"  Zero premium for {name} — aborting entry.")
                return False

        net_credit = sell_ce_prem + sell_pe_prem - buy_ce_prem - buy_pe_prem
        lot_size   = _get_lot_size()
        qty        = N_LOTS * lot_size

        logger.info(f"  Net credit/unit: ₹{net_credit:.2f}  Lots={N_LOTS}  Qty/leg={qty}")
        logger.info(f"  BUY  CE {signal['buy_ce_sym']}  @ ₹{buy_ce_prem:.2f}")
        logger.info(f"  BUY  PE {signal['buy_pe_sym']}  @ ₹{buy_pe_prem:.2f}")
        logger.info(f"  SELL CE {signal['sell_ce_sym']} @ ₹{sell_ce_prem:.2f}")
        logger.info(f"  SELL PE {signal['sell_pe_sym']} @ ₹{sell_pe_prem:.2f}")

        # ── place orders: BUY legs first for margin benefit ──────────────────
        order_ok = True
        entry_ltp = {
            "buy_ce": buy_ce_prem, "buy_pe": buy_pe_prem,
            "sell_ce": sell_ce_prem, "sell_pe": sell_pe_prem,
        }
        fills: dict[str, float] = {}
        for tag, sym, side in [
            ("buy_ce",  signal["buy_ce_sym"],  "BUY"),
            ("buy_pe",  signal["buy_pe_sym"],  "BUY"),
            ("sell_ce", signal["sell_ce_sym"], "SELL"),
            ("sell_pe", signal["sell_pe_sym"], "SELL"),
        ]:
            resp = _placeorder(sym, side, qty)
            if not resp:
                logger.error(f"  Order failed for {sym} — position may be partial.")
                order_ok = False
            fills[tag] = _resolve_fill(resp, entry_ltp[tag])

        if fills != entry_ltp:
            logger.info(
                f"  Fill vs LTP — buy_ce: {fills['buy_ce']:.2f}/{buy_ce_prem:.2f}  "
                f"buy_pe: {fills['buy_pe']:.2f}/{buy_pe_prem:.2f}  "
                f"sell_ce: {fills['sell_ce']:.2f}/{sell_ce_prem:.2f}  "
                f"sell_pe: {fills['sell_pe']:.2f}/{sell_pe_prem:.2f}"
            )

        # ── log entry to CSV (fill-based) ─────────────────────────────────────
        _log_csv("BUY",  "buy_ce",  signal["buy_ce_sym"],  fills["buy_ce"],  qty, 0.0, "entry")
        _log_csv("BUY",  "buy_pe",  signal["buy_pe_sym"],  fills["buy_pe"],  qty, 0.0, "entry")
        _log_csv("SELL", "sell_ce", signal["sell_ce_sym"], fills["sell_ce"], qty, 0.0, "entry")
        _log_csv("SELL", "sell_pe", signal["sell_pe_sym"], fills["sell_pe"], qty, 0.0, "entry")

        # ── fill-based net credit (booking basis; LTP still drives live decisions) ──
        net_credit = fills["sell_ce"] + fills["sell_pe"] - fills["buy_ce"] - fills["buy_pe"]

        # ── persist state ────────────────────────────────────────────────────
        self.state = {
            "strategy":            "BANKNIFTY_IRON_FLY_MONTHLY",
            "paper_mode":          PAPER_MODE,
            "closed":              False,
            "trade_date":          date.today().isoformat(),
            "entry_time":          datetime.now().isoformat(),
            "expiry_str":          expiry_str,
            "expiry_date":         expiry_date.isoformat(),
            "exit_day":           exit_day.isoformat(),
            "lot_size":           lot_size,
            "n_lots":             N_LOTS,
            "qty":                qty,
            "atm_strike":         atm,
            "vix_at_entry":       round(vix, 2),
            "spot_at_entry":      round(spot, 2),
            "net_credit_per_unit": round(net_credit, 2),
            # FIX: PT basis = net_credit × full qty (not per-lot-size)
            "premium_collected":  round(net_credit * qty, 2),
            "streak_ce":          0,
            "streak_pe":          0,
            "n_adjustments":      0,
            "adj_realized_pnl":   0.0,
            "current_mtm":        0.0,
            "exit_reason":        None,
            "legs": {
                "sell_ce": {"symbol": signal["sell_ce_sym"],
                            "entry_prem": round(fills["sell_ce"], 2),
                            "strike": atm, "closed": False},
                "sell_pe": {"symbol": signal["sell_pe_sym"],
                            "entry_prem": round(fills["sell_pe"], 2),
                            "strike": atm, "closed": False},
                "buy_ce":  {"symbol": signal["buy_ce_sym"],
                            "entry_prem": round(fills["buy_ce"], 2),
                            "strike": signal["hedge_ce_k"], "closed": False},
                "buy_pe":  {"symbol": signal["buy_pe_sym"],
                            "entry_prem": round(fills["buy_pe"], 2),
                            "strike": signal["hedge_pe_k"], "closed": False},
            },
        }
        _T0 = max((expiry_date - date.today()).days / 365.0, 1e-6)
        for _tag, _leg in self.state["legs"].items():
            self._stamp_entry_delta(_tag, _leg, spot, _T0, vix / 100.0, datetime.now())
        save_state(self.state)

        from live_trading.shared.telegram_notifier import send_async
        await send_async(
            f"⚡ *BANKNIFTY IRON FLY* — ENTRY\n"
            f"Expiry: {expiry_str}  Exit: {exit_day} 15:15\n"
            f"ATM: {atm}  Lots: {N_LOTS}  Qty/leg: {qty}\n"
            f"Net credit/unit: ₹{net_credit:.2f}  PT basis: ₹{net_credit * qty:,.0f}\n"
            f"VIX={vix:.1f}  Spot={spot:.1f}\n"
            f"BUY CE @ ₹{fills['buy_ce']:.2f}  BUY PE @ ₹{fills['buy_pe']:.2f}\n"
            f"SELL CE @ ₹{fills['sell_ce']:.2f}  SELL PE @ ₹{fills['sell_pe']:.2f}"
            + ("" if order_ok else "\n⚠️ One or more orders may have failed — check broker.")
        )
        return True

    # ── intrabar tick processing ───────────────────────────────────────────────

    def _process_bar(self, now: datetime) -> tuple[str | None, bool]:
        """
        Called each poll cycle. Returns (exit_reason or None, adjusted_flag).
        Exit reasons: 'profit_target', 'scheduled', 'forced_expiry', None.
        """
        if not self.state or self.state.get("closed") or not self.state.get("legs"):
            return None, False

        expiry_date = date.fromisoformat(self.state["expiry_date"])
        exit_day    = date.fromisoformat(self.state["exit_day"])
        today       = now.date()
        t           = now.time()

        # ── 15:15 scheduled exit ──────────────────────────────────────────
        if (today == exit_day and t.hour == EXIT_HOUR and t.minute >= EXIT_MIN):
            return "scheduled", False

        # ── safety: still open on expiry day ────────────────────────────────
        if today >= expiry_date:
            return "forced_expiry", False

        # ── fetch live prices ──────────────────────────────────────────────
        legs = self.state["legs"]
        prices = {
            "sell_ce": _get_option_price(legs["sell_ce"]["symbol"]),
            "sell_pe": _get_option_price(legs["sell_pe"]["symbol"]),
            "buy_ce":  _get_option_price(legs["buy_ce"]["symbol"]),
            "buy_pe":  _get_option_price(legs["buy_pe"]["symbol"]),
        }

        # A failed quote (LTP=0) fakes ±entry_prem×qty of MTM and can falsely
        # trigger the profit target — skip this poll instead
        if any(p <= 0 for p in prices.values()):
            logger.warning("  Quote failure (LTP=0) on a leg — skipping PT/adjustment this poll.")
            return None, False

        qty = self.state["qty"]
        # Open-leg MTM + any realized P&L from prior adjustments
        open_mtm = compute_pnl(legs, prices, qty)
        total_mtm = open_mtm + self.state.get("adj_realized_pnl", 0.0)
        self.state["current_mtm"] = round(total_mtm, 0)
        save_state(self.state)

        # ── profit target check ────────────────────────────────────────────
        # premium_collected = net_credit × full qty — correct PT basis
        premium = self.state["premium_collected"]
        if premium > 0 and total_mtm >= PROFIT_TARGET * premium:
            logger.info(f"  🎯 PROFIT TARGET HIT: MTM=₹{total_mtm:,.0f} ≥ "
                        f"{PROFIT_TARGET:.0%}×₹{premium:,.0f}")
            return "profit_target", False

        # ── delta adjustment (only during adjustment window, short legs) ──
        adj_window_open  = (t.hour == ADJ_HOUR and t.minute >= ADJ_MIN) or (t.hour > ADJ_HOUR)
        adj_window_close = (t.hour < 15)

        spot = _get_bnf_spot()
        if spot <= 0:
            return None, False

        T     = max((expiry_date - today).days / 365.0, 1e-6)
        sigma = self.state.get("vix_at_entry", 15.0) / 100.0
        self._monitor_deltas(spot, T, sigma, now)   # display only; every poll, all 4 legs

        if not (adj_window_open and adj_window_close):
            return None, False

        # Current ATM for any adjustments
        new_atm = int(round(spot / ATM_STEP) * ATM_STEP)
        did_adjust = False

        # ── Check short CE leg ────────────────────────────────────────────
        if not legs["sell_ce"]["closed"]:
            cur_ce_strike = legs["sell_ce"]["strike"]   # FIX: use current strike, not entry atm
            ce_delta, _ce_src = _trigger_delta(legs["sell_ce"]["symbol"], cur_ce_strike, "CE",
                                           prices["sell_ce"], spot, expiry_date, now, sigma, T)
            legs["sell_ce"]["trigger_delta"], legs["sell_ce"]["trigger_src"] = round(ce_delta, 4), _ce_src
            ce_outside = (ce_delta < ADJ_LOW_TRIG) or (ce_delta > ADJ_HI_TRIG)

            # FIX: increment streak when outside, reset when inside
            if ce_outside:
                self.state["streak_ce"] += 1
            else:
                self.state["streak_ce"] = 0

            if self.state["streak_ce"] >= ADJ_HYSTERESIS:
                new_sym = _resolve_symbol("BANKNIFTY", OPT_EXCHANGE,
                                          self.state["expiry_str"], "CE", new_atm)
                if new_sym:
                    old_sym  = legs["sell_ce"]["symbol"]
                    old_prem = _get_option_price(old_sym)
                    new_prem = _get_option_price(new_sym)
                    # Place orders first: BUY back old, SELL new ATM
                    buy_resp  = _placeorder(old_sym, "BUY",  qty)
                    sell_resp = _placeorder(new_sym, "SELL", qty)
                    old_fill  = _resolve_fill(buy_resp,  old_prem)
                    new_fill  = _resolve_fill(sell_resp, new_prem)
                    # Realized P&L on closed short CE (fill-based)
                    adj_pnl_ce = (legs["sell_ce"]["entry_prem"] - old_fill) * qty
                    self.state["adj_realized_pnl"] = (
                        self.state.get("adj_realized_pnl", 0.0) + adj_pnl_ce
                    )
                    # Log fill-based prices
                    _log_csv("BUY",  "sell_ce", old_sym, old_fill, qty, adj_pnl_ce, "adjustment")
                    _log_csv("SELL", "sell_ce", new_sym, new_fill, qty, 0.0,        "adjustment")
                    # FIX: update sell_ce only — buy hedge (buy_ce) is NOT touched
                    _old_snap = dict(legs["sell_ce"])
                    legs["sell_ce"].update({
                        "symbol":     new_sym,
                        "entry_prem": round(new_fill, 2),
                        "strike":     new_atm,
                    })
                    for _k in ("broker_delta", "broker_iv", "broker_ts", "delta"):
                        legs["sell_ce"].pop(_k, None)   # stale values belong to the old strike
                    self._record_adjustment("sell_ce", old_sym, _old_snap["strike"], _old_snap,
                                            ce_delta, old_fill, new_fill, adj_pnl_ce,
                                            spot, T, sigma, now)
                    self.state["streak_ce"] = 0
                    self.state["n_adjustments"] += 1
                    did_adjust = True
                    logger.info(
                        f"  ⚙️ ADJ CE: {old_sym} → {new_sym} "
                        f"Δ={ce_delta:.2f}  adj_pnl=₹{adj_pnl_ce:,.0f}"
                    )

        # ── Check short PE leg ────────────────────────────────────────────
        if not legs["sell_pe"]["closed"]:
            cur_pe_strike = legs["sell_pe"]["strike"]   # FIX: use current strike
            pe_delta, _pe_src = _trigger_delta(legs["sell_pe"]["symbol"], cur_pe_strike, "PE",
                                           prices["sell_pe"], spot, expiry_date, now, sigma, T)
            legs["sell_pe"]["trigger_delta"], legs["sell_pe"]["trigger_src"] = round(pe_delta, 4), _pe_src
            pe_outside = (pe_delta < ADJ_LOW_TRIG) or (pe_delta > ADJ_HI_TRIG)

            # FIX: increment streak when outside, reset when inside
            if pe_outside:
                self.state["streak_pe"] += 1
            else:
                self.state["streak_pe"] = 0

            if self.state["streak_pe"] >= ADJ_HYSTERESIS:
                new_sym = _resolve_symbol("BANKNIFTY", OPT_EXCHANGE,
                                          self.state["expiry_str"], "PE", new_atm)
                if new_sym:
                    old_sym  = legs["sell_pe"]["symbol"]
                    old_prem = _get_option_price(old_sym)
                    new_prem = _get_option_price(new_sym)
                    # Place orders first: BUY back old, SELL new ATM
                    buy_resp  = _placeorder(old_sym, "BUY",  qty)
                    sell_resp = _placeorder(new_sym, "SELL", qty)
                    old_fill  = _resolve_fill(buy_resp,  old_prem)
                    new_fill  = _resolve_fill(sell_resp, new_prem)
                    # Realized P&L on closed short PE (fill-based)
                    adj_pnl_pe = (legs["sell_pe"]["entry_prem"] - old_fill) * qty
                    self.state["adj_realized_pnl"] = (
                        self.state.get("adj_realized_pnl", 0.0) + adj_pnl_pe
                    )
                    # Log fill-based prices
                    _log_csv("BUY",  "sell_pe", old_sym, old_fill, qty, adj_pnl_pe, "adjustment")
                    _log_csv("SELL", "sell_pe", new_sym, new_fill, qty, 0.0,        "adjustment")
                    # FIX: update sell_pe only — buy hedge (buy_pe) is NOT touched
                    _old_snap = dict(legs["sell_pe"])
                    legs["sell_pe"].update({
                        "symbol":     new_sym,
                        "entry_prem": round(new_fill, 2),
                        "strike":     new_atm,
                    })
                    for _k in ("broker_delta", "broker_iv", "broker_ts", "delta"):
                        legs["sell_pe"].pop(_k, None)   # stale values belong to the old strike
                    self._record_adjustment("sell_pe", old_sym, _old_snap["strike"], _old_snap,
                                            pe_delta, old_fill, new_fill, adj_pnl_pe,
                                            spot, T, sigma, now)
                    self.state["streak_pe"] = 0
                    self.state["n_adjustments"] += 1
                    did_adjust = True
                    logger.info(
                        f"  ⚙️ ADJ PE: {old_sym} → {new_sym} "
                        f"Δ={pe_delta:.2f}  adj_pnl=₹{adj_pnl_pe:,.0f}"
                    )

        if did_adjust:
            save_state(self.state)

        return None, did_adjust

    # ── exit ─────────────────────────────────────────────────────────────────

    async def exit(self, reason: str, now: datetime) -> None:
        logger.info("═" * 64)
        logger.info(f"🔴 EXIT — {reason}")

        legs = self.state["legs"]
        qty  = self.state["qty"]

        prices = {
            "sell_ce": _get_option_price(legs["sell_ce"]["symbol"]),
            "sell_pe": _get_option_price(legs["sell_pe"]["symbol"]),
            "buy_ce":  _get_option_price(legs["buy_ce"]["symbol"]),
            "buy_pe":  _get_option_price(legs["buy_pe"]["symbol"]),
        }

        # ── place close orders: BUY back short legs, SELL long hedges ────────
        exit_fills: dict[str, float] = {}
        for leg_key, sym, side in [
            ("sell_ce", legs["sell_ce"]["symbol"], "BUY"),
            ("sell_pe", legs["sell_pe"]["symbol"], "BUY"),
            ("buy_ce",  legs["buy_ce"]["symbol"],  "SELL"),
            ("buy_pe",  legs["buy_pe"]["symbol"],  "SELL"),
        ]:
            resp = _placeorder(sym, side, qty)
            exit_fills[leg_key] = _resolve_fill(resp, prices.get(leg_key, legs[leg_key]["entry_prem"]))

        open_mtm   = compute_pnl(legs, exit_fills, qty)
        adj_pnl    = self.state.get("adj_realized_pnl", 0.0)
        total_pnl  = open_mtm + adj_pnl
        logger.info(f"  Open MTM: ₹{open_mtm:,.0f}  Adj realized: ₹{adj_pnl:,.0f}  "
                    f"Total gross P&L: ₹{total_pnl:,.0f}")

        # ── log each leg close to CSV (fill-based) ────────────────────────────
        for leg_key in ("sell_ce", "sell_pe", "buy_ce", "buy_pe"):
            sym  = legs[leg_key]["symbol"]
            prem = exit_fills[leg_key]
            _log_csv("CLOSE", leg_key, sym, prem, qty, 0.0, reason)

        # ── DB trade log ──────────────────────────────────────────────────────
        lot_size = self.state.get("lot_size", _get_lot_size())
        try:
            log_trade_to_db(
                bot_name      = "banknifty_iron_fly_monthly_bot",
                instrument    = "BANKNIFTY",
                option_symbol = legs["sell_ce"]["symbol"],
                option_type   = "IRON_FLY",
                entry_time    = datetime.fromisoformat(self.state["entry_time"]),
                exit_time     = now,
                entry_premium = self.state.get("net_credit_per_unit", 0.0),
                exit_premium  = 0.0,
                exit_reason   = reason,
                quantity      = qty,
                lots          = N_LOTS,
                lot_size      = lot_size,
                gross_pnl     = round(total_pnl, 2),
                notes=(
                    f"sell_ce={legs['sell_ce']['symbol']}@{legs['sell_ce']['entry_prem']} | "
                    f"sell_pe={legs['sell_pe']['symbol']}@{legs['sell_pe']['entry_prem']} | "
                    f"buy_ce={legs['buy_ce']['symbol']}@{legs['buy_ce']['entry_prem']} | "
                    f"buy_pe={legs['buy_pe']['symbol']}@{legs['buy_pe']['entry_prem']}"
                ),
            )
        except Exception as e:
            logger.warning(f"  DB log failed: {e}")

        self.state.update({
            "closed":           True,
            "exit_reason":      reason,
            "exit_time":        now.isoformat(),
            "open_mtm":         round(open_mtm, 2),
            "adj_realized_pnl": round(adj_pnl, 2),
            "total_pnl":        round(total_pnl, 2),
        })
        save_state(self.state)
        self.state = None

        from live_trading.shared.telegram_notifier import send_async
        emoji = "🎯" if reason == "profit_target" else "⏰"
        await send_async(
            f"{emoji} *BANKNIFTY IRON FLY* — EXIT ({reason})\n"
            f"Open MTM: ₹{open_mtm:,.0f}  Adj P&L: ₹{adj_pnl:,.0f}\n"
            f"Total Gross P&L: ₹{total_pnl:,.0f}"
        )
