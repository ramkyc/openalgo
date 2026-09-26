#!/usr/bin/env python3
"""
nifty_atm_straddle_scalp_bot.py — NIFTY ATM Short Straddle Scalp (Paper Trading)
=================================================================================
Champion config `10:30_sl20_tgt0.75` (atm_short_straddle_scalp_study, re-validated
2026-08 after margin recalibration, Stage 0 → Stage 11 all PASS):
  options_data/research/atm_short_straddle_scalp_study/FINDINGS.md
  options_data/research/atm_short_straddle_scalp_study/DECISIONS.md
  Verdict: APPROVED FOR PAPER TRADING

Strategy:
  Sell an ATM straddle (SELL 1 lot ATM CE + SELL 1 lot ATM PE, same strike) on
  NIFTY at a single fixed daily entry time. Hold intraday only — same-day exit,
  no overnight carry.

Entry:
  10:30 IST — resolve NIFTY spot, ATM strike, nearest weekly expiry (no DTE
  floor — Stage 10 expiry-segmentation explicitly validated DTE=0/expiry-day
  trades for this config, see expiry_segmentation.py CHAMPION + gate PASS).
  SELL ATM CE and SELL ATM PE, 10 lots each.

Per-leg risk management (mirrors backtest_is.py / backtest_oos.py simulate_day()):
  SL     : if a leg's premium rises to 120% of its own entry (20% adverse move),
           buy that leg back. Enforced via broker-side SL-M order at
           trigger = entry_premium * 1.20.
  Trail  : once one leg stops out, the surviving leg's stop is trailed down to
           its own entry price (breakeven) — cancel the resting SL-M and
           replace it at trigger = entry_premium. Held for the straddle-level
           target or EOD from there.
  Target : combined straddle P&L >= 0.75% of margin utilized → close both legs
           (or the sole survivor) at market.
  EOD    : unconditional close at 15:14 IST (MIS sandbox hard cutoff — the
           square-off fires at 15:15, backtest square-off was 15:15).

Margin / target formula (STATIC — mirrors backtest exactly, not a live API call,
see margin_calibration D13 in the research study for why this is a deliberate
signal-parity choice over nifty_atm_straddle_margin_calibration.py):
  units    = lot_size * N_LOTS
  notional = spot_at_entry * units
  margin   = MARGIN_PCT_OF_NOTIONAL(0.1326) * notional
  target_rs = TARGET_PCT(0.0075) * margin

Signal rules (ALL required):
  1. Time == 10:30 IST entry window (single fixed daily entry, no other signal)
  2. No position already open
  3. Valid NIFTY spot + resolvable ATM CE/PE symbols for the nearest weekly
     expiry (min_dte=0 — expiry day included, see Stage 10 above)
  4. Non-zero entry premium on both legs

Exit rules (first to trigger wins, evaluated every 30s):
  SL      : leg premium >= entry * 1.20 → close that leg, trail survivor to breakeven
  TARGET  : combined P&L >= target_rs → close remaining leg(s) at market
  EOD     : 15:14 IST → close remaining leg(s) at market, no exceptions

Shared utilities:
  live_trading.api_utils                — is_market_holiday
  live_trading.shared.atm_resolver      — resolve_atm_option, get_option_ltp
  live_trading.shared.order_fill        — fetch_fill_price
  live_trading.shared.telegram_notifier — send_async
  live_trading.shared.trade_logger      — log_trade_to_db (option_type="SHORT_STRADDLE",
                                           one combined call per exit event)
  live_trading.shared.poll_watchdog     — PollWatchdog
  openalgo.api                          — placeorder (entries AND exits — no placesmartorder)

No PAPER_MODE flag: this bot always fires real placeorder() calls. OpenAlgo's
Sandbox/Analyze Mode UI toggle is what intercepts and simulates fills — flipping
to real money means flipping that toggle, not touching this file.
"""

import asyncio
import atexit
import json
import logging
import os
import sys
import time as _time_mod
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from openalgo import api

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils                import is_market_holiday
from live_trading.shared.atm_resolver      import resolve_atm_option, get_option_ltp
from live_trading.shared.order_fill        import fetch_fill_price
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger      import log_trade_to_db
from live_trading.shared.poll_watchdog     import PollWatchdog

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "nifty_atm_straddle_scalp_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER", "http://127.0.0.1:8080")

if not API_KEY:
    logger.error("❌ OPENALGO_API_KEY not found. Exiting.")
    sys.exit(1)

# ── Strategy constants ────────────────────────────────────────────────────────
STRATEGY_NAME = "NIFTY_ATM_STRADDLE_SCALP"
BOT_NAME      = "nifty_atm_straddle_scalp_bot"

IDX_SYMBOL       = "NIFTY"
IDX_EXCHANGE     = "NSE_INDEX"
OPT_EXCHANGE     = "NFO"

N_LOTS           = 10
DEFAULT_LOT_SIZE = 65      # last-resort fallback only — resolved per-contract at entry

# Champion params (10:30_sl20_tgt0.75)
ENTRY_HOUR             = 10
ENTRY_MINUTE           = 30
ENTRY_WINDOW_MINS      = 5     # 10:30-10:35 — single fixed-time entry, tight window
SL_MULT                = 1.20  # leg SL at 120% of its own entry premium
TARGET_PCT             = 0.0075   # straddle target = 0.75% of margin utilized
MARGIN_PCT_OF_NOTIONAL = 0.1326   # static, mirrors backtest — see module docstring
MIN_DTE                = 0        # no floor — Stage 10 validated DTE=0 (expiry day)

EOD_EXIT      = dt_time(15, 14)   # hard limit — sandbox auto-squaresoff ALL MIS at 15:15
EXIT_POLL_SEC = 30

# State + lockfile
STATE_FILE = LOGS_DIR / "nifty_atm_straddle_scalp_state.json"
PID_FILE   = LOGS_DIR / "nifty_atm_straddle_scalp_bot.pid"


# ── PID lockfile ──────────────────────────────────────────────────────────────

def _acquire_pid_lock() -> None:
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            os.kill(old_pid, 0)
            logger.error(
                f"❌ Another instance already running (PID {old_pid}). "
                f"Delete {PID_FILE} if stale."
            )
            sys.exit(1)
        except ProcessLookupError:
            logger.warning(f"⚠️ Stale PID file (PID {old_pid}) — removing.")
            PID_FILE.unlink(missing_ok=True)
        except (PermissionError, ValueError):
            logger.error("❌ Could not verify existing PID. Aborting.")
            sys.exit(1)
    PID_FILE.write_text(str(os.getpid()))
    atexit.register(_release_pid_lock)
    logger.info(f"🔒 PID lock acquired (PID {os.getpid()})")


def _release_pid_lock() -> None:
    try:
        if PID_FILE.exists() and int(PID_FILE.read_text().strip()) == os.getpid():
            PID_FILE.unlink()
            logger.info("🔓 PID lock released.")
    except Exception:
        pass


# ── Quotes ────────────────────────────────────────────────────────────────────

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
                    return float(qd.get("ltp") or qd.get("last_price") or qd.get("close") or 0)
                if isinstance(qd, list) and qd:
                    return float(qd[0].get("ltp") or qd[0].get("last_price") or qd[0].get("close") or 0)
            else:
                logger.warning(f"  Quote API non-success for {symbol}@{exchange}: {data.get('message', '—')}")
        else:
            logger.warning(f"  Quote HTTP {res.status_code} for {symbol}@{exchange}: {res.text[:200]}")
    except Exception as e:
        logger.error(f"  Quote failed ({symbol}@{exchange}): {e}")
    return 0.0


def _get_nifty_spot() -> float:
    return _quote(IDX_SYMBOL, IDX_EXCHANGE)


def _get_option_price(symbol: str, retries: int = 3, delay: float = 3.0) -> float:
    """Fetch option LTP with retries — the order book can be cold right at the
    10:30:00 tick, so a single shot is fragile."""
    for attempt in range(1, retries + 1):
        price = get_option_ltp(symbol, OPT_EXCHANGE, API_KEY)
        if price > 0:
            return price
        if attempt < retries:
            logger.info(f"  LTP=0 for {symbol} (attempt {attempt}/{retries}) — retrying in {delay:.0f}s…")
            _time_mod.sleep(delay)
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


def _check_fill(client, order_id: str) -> tuple[bool, float]:
    """Returns (is_filled, fill_price) by scanning the orderbook for order_id."""
    try:
        ob = client.orderbook()
        if isinstance(ob, dict) and ob.get("status") == "success":
            data = ob.get("data") or {}
            orders = data.get("orders", []) if isinstance(data, dict) else []
        elif isinstance(ob, list):
            orders = ob
        else:
            return False, 0.0
        for o in orders:
            if not isinstance(o, dict):
                continue
            if str(o.get("orderid", "")) == str(order_id):
                status = str(o.get("order_status") or o.get("status") or "").lower()
                if status in ("complete", "filled", "traded"):
                    price = float(o.get("average_price", 0) or o.get("price", 0) or 0)
                    return True, price
                return False, 0.0
    except Exception as e:
        logger.warning(f"  Orderbook check failed for {order_id}: {e}")
    return False, 0.0


def _cancel_order(client, order_id: str) -> None:
    try:
        # cancelorder accepts only order_id/strategy — extra fields are
        # forwarded into the payload and rejected with HTTP 400
        res = client.cancelorder(order_id=order_id, strategy=STRATEGY_NAME)
        logger.info(f"  Cancel {order_id}: {res}")
    except Exception as e:
        logger.warning(f"  Cancel order {order_id} failed: {e}")


# ── Lot size helper ───────────────────────────────────────────────────────────

def _get_lot_size(symbol: str) -> int:
    try:
        from database.token_db import get_symbol_info
        si = get_symbol_info(symbol, OPT_EXCHANGE)
        if si and getattr(si, "lotsize", None):
            ls = int(si.lotsize)
            logger.info(f"  Lot size from DB: {ls}  ({symbol})")
            return ls
    except Exception as e:
        logger.warning(f"  Lot size DB lookup failed for {symbol}: {e}. Using {DEFAULT_LOT_SIZE}.")
    return DEFAULT_LOT_SIZE


# ── State management ──────────────────────────────────────────────────────────

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
    try:
        state["last_update"] = datetime.now().isoformat()
        STATE_FILE.write_text(json.dumps(state, indent=2, default=str))
    except Exception as e:
        logger.error(f"State save failed: {e}")


# ── Main bot class ────────────────────────────────────────────────────────────

class NiftyAtmStraddleScalpBot:
    """
    NIFTY ATM Short Straddle Scalp — paper trading bot.
    Polling-based (no WebSocket needed — single fixed daily entry, 30s poll).
    """

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.state: dict | None = load_state()
        self._entry_attempted_date: date | None = None

        self._watchdog = PollWatchdog(
            bot_name="NIFTY ATM Straddle Scalp Bot",
            market_open=dt_time(9, 15),
            market_close=dt_time(15, 30),
            bot_logger=logger,
        )

        logger.info(
            f"📐 {STRATEGY_NAME} | Entry {ENTRY_HOUR:02d}:{ENTRY_MINUTE:02d} IST | "
            f"SL={SL_MULT}× (breakeven trail) | Target={TARGET_PCT*100:.2f}% of margin | "
            f"EOD {EOD_EXIT} | {N_LOTS} lots/leg"
        )

    # ── Order helpers ─────────────────────────────────────────────────────────

    def _place(self, symbol: str, action: str, qty: int) -> dict:
        return self.client.placeorder(
            strategy   = STRATEGY_NAME,
            symbol     = symbol,
            action     = action,
            exchange   = OPT_EXCHANGE,
            price_type = "MARKET",
            product    = "MIS",
            quantity   = str(qty),
        )

    def _place_sl_m(self, symbol: str, qty: int, trigger_price: float) -> dict | None:
        try:
            return self.client.placeorder(
                strategy      = STRATEGY_NAME,
                symbol        = symbol,
                action        = "BUY",
                exchange      = OPT_EXCHANGE,
                price_type    = "SL-M",
                trigger_price = str(round(trigger_price, 2)),
                product       = "MIS",
                quantity      = str(qty),
            )
        except Exception as e:
            logger.error(f"  Broker-side SL-M placement exception ({symbol}): {e}")
            return None

    # ── Entry ─────────────────────────────────────────────────────────────────

    async def _enter(self) -> str:
        logger.info("═" * 64)
        logger.info("⚡ STRADDLE ENTRY — resolving ATM legs")

        spot = _get_nifty_spot()
        if spot <= 0:
            for attempt in range(3):
                logger.warning(f"  NIFTY spot=0 (attempt {attempt+1}/3) — retrying in 5s…")
                await asyncio.sleep(5)
                spot = _get_nifty_spot()
                if spot > 0:
                    break
        if spot <= 0:
            logger.error("  NIFTY spot = 0 — aborting entry.")
            return "error"
        logger.info(f"  NIFTY spot={spot:.2f}")

        ce = await asyncio.to_thread(resolve_atm_option, spot, "CE", API_KEY, MIN_DTE, "NIFTY")
        pe = await asyncio.to_thread(resolve_atm_option, spot, "PE", API_KEY, MIN_DTE, "NIFTY")
        if not ce or not pe:
            logger.error(f"  Could not resolve ATM legs (ce={ce}, pe={pe}) — aborting.")
            return "error"
        if ce["strike"] != pe["strike"]:
            logger.warning(f"  CE/PE strike mismatch ({ce['strike']} vs {pe['strike']}) — proceeding anyway.")

        ce_prem = await asyncio.to_thread(_get_option_price, ce["symbol"])
        pe_prem = await asyncio.to_thread(_get_option_price, pe["symbol"])
        if ce_prem <= 0 or pe_prem <= 0:
            logger.error(f"  Zero premium on a leg (CE={ce_prem}, PE={pe_prem}) — aborting.")
            return "error"

        lot_size = _get_lot_size(ce["symbol"])
        qty      = lot_size * N_LOTS

        logger.info(f"  ATM {ce['strike']}  CE={ce_prem:.2f}  PE={pe_prem:.2f}  qty/leg={qty}  expiry={ce['expiry']}")

        r_ce = self._place(ce["symbol"], "SELL", qty)
        await asyncio.sleep(0.5)
        r_pe = self._place(pe["symbol"], "SELL", qty)

        if r_ce.get("status") != "success" or r_pe.get("status") != "success":
            logger.error(f"  SELL order(s) FAILED — CE:{r_ce}  PE:{r_pe}")
            await send_async(f"❌ {BOT_NAME} entry FAILED — CE:{r_ce.get('message','?')}  PE:{r_pe.get('message','?')}")
            return "error"

        fill_ce = _resolve_fill(r_ce, ce_prem)
        fill_pe = _resolve_fill(r_pe, pe_prem)

        units    = qty
        notional = spot * units
        margin   = round(MARGIN_PCT_OF_NOTIONAL * notional, 2)
        target_rs = round(TARGET_PCT * margin, 2)

        sl_ce = round(fill_ce * SL_MULT, 2)
        sl_pe = round(fill_pe * SL_MULT, 2)

        sl_ce_resp = self._place_sl_m(ce["symbol"], qty, sl_ce)
        sl_pe_resp = self._place_sl_m(pe["symbol"], qty, sl_pe)

        sl_ce_id = str(sl_ce_resp.get("orderid", "")) if sl_ce_resp and sl_ce_resp.get("status") == "success" else None
        sl_pe_id = str(sl_pe_resp.get("orderid", "")) if sl_pe_resp and sl_pe_resp.get("status") == "success" else None

        if not sl_ce_id:
            logger.error(f"  ⚠️ CE broker-side SL-M FAILED to place ({sl_ce_resp}) — app-side polling fallback only.")
        if not sl_pe_id:
            logger.error(f"  ⚠️ PE broker-side SL-M FAILED to place ({sl_pe_resp}) — app-side polling fallback only.")
        if not sl_ce_id or not sl_pe_id:
            await send_async(f"⚠️ {BOT_NAME} broker-side SL-M failed on ≥1 leg — falling back to app-side 30s polling for that leg.")

        self.state = {
            "strategy":         STRATEGY_NAME,
            "closed":           False,
            "trade_date":       date.today().isoformat(),
            "entry_time":       datetime.now().isoformat(),
            "expiry_str":       ce["expiry"],
            "lot_size":         lot_size,
            "n_lots":           N_LOTS,
            "qty":              qty,
            "atm_strike":       ce["strike"],
            "spot_at_entry":    round(spot, 2),
            "margin":           margin,
            "target_rs":        target_rs,
            "breakeven_active": False,
            "legs": {
                "sell_ce": {
                    "symbol": ce["symbol"], "entry_prem": round(fill_ce, 2),
                    "sl_level": sl_ce, "sl_order_id": sl_ce_id,
                    "closed": False, "exit_prem": None, "exit_reason": None,
                },
                "sell_pe": {
                    "symbol": pe["symbol"], "entry_prem": round(fill_pe, 2),
                    "sl_level": sl_pe, "sl_order_id": sl_pe_id,
                    "closed": False, "exit_prem": None, "exit_reason": None,
                },
            },
            "current_mtm":  0.0,
            "exit_reason":  None,
            "exit_time":    None,
            "total_pnl":    None,
        }
        save_state(self.state)

        await send_async(
            f"📉 {BOT_NAME} SOLD ATM straddle @ {ce['strike']}\n"
            f"  SELL {ce['symbol']} @ ₹{fill_ce:.2f}  SL ₹{sl_ce:.2f}"
            f"{'' if sl_ce_id else ' (⚠️ app-side only)'}\n"
            f"  SELL {pe['symbol']} @ ₹{fill_pe:.2f}  SL ₹{sl_pe:.2f}"
            f"{'' if sl_pe_id else ' (⚠️ app-side only)'}\n"
            f"  Qty/leg: {qty}  Margin: ₹{margin:,.0f}  Target: ₹{target_rs:,.0f}\n"
            f"  Expiry: {ce['expiry']}"
        )
        return "success"

    # ── MTM / combined P&L ────────────────────────────────────────────────────

    def _combined_pnl(self, prices: dict[str, float]) -> float:
        legs = self.state["legs"]
        qty  = self.state["qty"]
        total = 0.0
        for key, leg in legs.items():
            if leg["closed"]:
                total += (leg["entry_prem"] - leg["exit_prem"]) * qty
            else:
                price = prices.get(key)
                if price is None or price <= 0:
                    return None  # can't compute combined P&L with a bad quote
                total += (leg["entry_prem"] - price) * qty
        return total

    # ── Leg close / exit ──────────────────────────────────────────────────────

    async def _close_leg_market(self, leg_key: str, reason: str) -> None:
        leg  = self.state["legs"][leg_key]
        if leg["closed"]:
            return
        qty  = self.state["qty"]

        if leg.get("sl_order_id"):
            await asyncio.to_thread(_cancel_order, self.client, leg["sl_order_id"])

        fallback_ltp = await asyncio.to_thread(get_option_ltp, leg["symbol"], OPT_EXCHANGE, API_KEY)
        fallback_ltp = fallback_ltp or leg["entry_prem"]
        res = self._place(leg["symbol"], "BUY", qty)
        if res.get("status") != "success":
            logger.error(f"  [{leg_key}] EXIT order FAILED: {res}. Clearing leg — sandbox will square off at 15:15.")
        exit_fill = _resolve_fill(res, fallback_ltp)

        leg["closed"]      = True
        leg["exit_prem"]   = round(exit_fill, 2)
        leg["exit_reason"] = reason
        logger.warning(f"  [{leg_key}] closed ({reason}) @ ₹{exit_fill:.2f}")

    async def _on_leg_sl_fill(self, leg_key: str, fill_price: float) -> None:
        leg = self.state["legs"][leg_key]
        if leg["closed"]:
            return
        leg["closed"]      = True
        leg["exit_prem"]   = round(fill_price, 2)
        leg["exit_reason"] = "SL"
        logger.warning(f"  [{leg_key}] 🛑 broker-side SL-M filled @ ₹{fill_price:.2f}")

        sibling_key = "sell_pe" if leg_key == "sell_ce" else "sell_ce"
        sibling     = self.state["legs"][sibling_key]

        if not sibling["closed"] and not self.state["breakeven_active"]:
            if sibling.get("sl_order_id"):
                await asyncio.to_thread(_cancel_order, self.client, sibling["sl_order_id"])
            new_trigger = sibling["entry_prem"]
            resp = await asyncio.to_thread(self._place_sl_m, sibling["symbol"], self.state["qty"], new_trigger)
            if resp and resp.get("status") == "success":
                sibling["sl_order_id"] = str(resp.get("orderid", ""))
                logger.info(f"  [{sibling_key}] 🛡️ trailed to breakeven @ ₹{new_trigger:.2f}  order_id={sibling['sl_order_id']}")
            else:
                sibling["sl_order_id"] = None
                logger.error(f"  [{sibling_key}] ⚠️ breakeven SL-M FAILED ({resp}) — app-side polling fallback only.")
            sibling["sl_level"] = round(new_trigger, 2)
            self.state["breakeven_active"] = True
            await send_async(
                f"🛑 {BOT_NAME} [{leg_key}] SL hit @ ₹{fill_price:.2f}\n"
                f"  Survivor [{sibling_key}] trailed to breakeven ₹{new_trigger:.2f}"
            )

        save_state(self.state)
        await self._maybe_finalize()

    async def _force_close_remaining(self, reason: str) -> None:
        legs = self.state["legs"]
        for key, leg in legs.items():
            if not leg["closed"]:
                await self._close_leg_market(key, reason)
        save_state(self.state)
        await self._maybe_finalize()

    async def _maybe_finalize(self) -> None:
        legs = self.state["legs"]
        if not all(l["closed"] for l in legs.values()):
            save_state(self.state)
            return

        qty      = self.state["qty"]
        lot_size = self.state["lot_size"]
        ce, pe   = legs["sell_ce"], legs["sell_pe"]
        pnl_ce   = (ce["entry_prem"] - ce["exit_prem"]) * qty
        pnl_pe   = (pe["entry_prem"] - pe["exit_prem"]) * qty
        total    = round(pnl_ce + pnl_pe, 2)

        reasons = []
        for r in (ce["exit_reason"], pe["exit_reason"]):
            if r and r not in reasons:
                reasons.append(r)
        exit_reason = "+".join(reasons) if reasons else "unknown"

        self.state.update({
            "closed":      True,
            "exit_reason": exit_reason,
            "exit_time":   datetime.now().isoformat(),
            "total_pnl":   total,
            "current_mtm": total,
        })
        save_state(self.state)

        msg = (
            f"{'✅' if total > 0 else '❌'} {BOT_NAME} CLOSED straddle @ {self.state['atm_strike']}\n"
            f"  CE: entry ₹{ce['entry_prem']:.2f} → exit ₹{ce['exit_prem']:.2f}  ({ce['exit_reason']})\n"
            f"  PE: entry ₹{pe['entry_prem']:.2f} → exit ₹{pe['exit_prem']:.2f}  ({pe['exit_reason']})\n"
            f"  P&L: ₹{total:,.0f}  Qty/leg: {qty}"
        )
        logger.warning(msg)
        await send_async(msg)

        log_trade_to_db(
            bot_name      = BOT_NAME,
            instrument    = IDX_SYMBOL,
            option_symbol = ce["symbol"],
            option_type   = "SHORT_STRADDLE",
            entry_time    = datetime.fromisoformat(self.state["entry_time"]),
            exit_time     = datetime.now(),
            entry_premium = round(ce["entry_prem"] + pe["entry_prem"], 2),
            exit_premium  = 0.0,
            exit_reason   = exit_reason,
            quantity      = qty,
            lots          = N_LOTS,
            lot_size      = lot_size,
            gross_pnl     = total,
            notes         = (
                f"sell_ce={ce['symbol']}@{ce['entry_prem']}->{ce['exit_prem']}({ce['exit_reason']}) | "
                f"sell_pe={pe['symbol']}@{pe['entry_prem']}->{pe['exit_prem']}({pe['exit_reason']}) | "
                f"margin={self.state['margin']} target={self.state['target_rs']}"
            ),
        )
        self.state = None

    # ── Exit monitor loop ─────────────────────────────────────────────────────

    async def _exit_monitor_loop(self) -> None:
        while True:
            await asyncio.sleep(EXIT_POLL_SEC)
            if self.state is None or self.state.get("closed"):
                continue

            legs = self.state["legs"]

            # 1. Reconcile broker-side SL-M fills first
            for key, leg in list(legs.items()):
                if leg["closed"] or not leg.get("sl_order_id"):
                    continue
                filled, fill_price = await asyncio.to_thread(_check_fill, self.client, leg["sl_order_id"])
                if filled:
                    await self._on_leg_sl_fill(key, fill_price)
            if self.state is None:
                continue

            open_keys = [k for k, l in self.state["legs"].items() if not l["closed"]]
            if not open_keys:
                continue

            # 2. EOD hard exit
            now_t = datetime.now().time()
            if now_t >= EOD_EXIT:
                logger.warning("  ⏰ EOD — forcing close of remaining leg(s).")
                await self._force_close_remaining("EOD")
                continue

            # 3. Fetch live prices, feed watchdog
            prices = {}
            for key in open_keys:
                sym = self.state["legs"][key]["symbol"]
                px  = await asyncio.to_thread(get_option_ltp, sym, OPT_EXCHANGE, API_KEY)
                prices[key] = px
                await self._watchdog.check(sym, px if px > 0 else None, success=px > 0)

            if any(p <= 0 for p in prices.values()):
                logger.warning("  Quote failure (LTP=0) on an open leg — skipping this poll.")
                continue

            # 4. Fallback-only SL (broker-side SL-M order failed to place)
            for key in open_keys:
                leg = self.state["legs"][key]
                if not leg.get("sl_order_id") and prices[key] >= leg["sl_level"]:
                    logger.warning(f"  [{key}] app-side SL: LTP={prices[key]:.2f} ≥ SL={leg['sl_level']:.2f}")
                    await self._on_leg_sl_fill(key, prices[key])
            if self.state is None or self.state.get("closed"):
                continue

            # 5. Combined target check
            combined = self._combined_pnl(prices)
            if combined is None:
                continue
            self.state["current_mtm"] = round(combined, 0)
            save_state(self.state)

            logger.info(
                f"📊 combined P&L=₹{combined:+,.0f}  target=₹{self.state['target_rs']:,.0f}  "
                + "  ".join(f"{k}={v:.1f}" for k, v in prices.items())
            )

            if combined >= self.state["target_rs"]:
                logger.info(f"  🎯 Target hit: ₹{combined:,.0f} ≥ ₹{self.state['target_rs']:,.0f}")
                await self._force_close_remaining("Target")

    # ── EOD hard-exit guard (safety net) ──────────────────────────────────────

    async def _eod_guard_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            if self.state is None or self.state.get("closed"):
                continue
            now_t = datetime.now().time()
            if now_t < dt_time(15, 5):
                continue
            if now_t >= EOD_EXIT:
                open_keys = [k for k, l in self.state["legs"].items() if not l["closed"]]
                if open_keys:
                    logger.warning(f"  EOD guard — forcing close of {open_keys}.")
                    await self._force_close_remaining("EOD_GUARD")

    # ── State dump loop ───────────────────────────────────────────────────────

    async def _state_dump_loop(self) -> None:
        while True:
            if self.state is not None:
                save_state(self.state)
            await asyncio.sleep(5)

    # ── Entry scheduler loop ──────────────────────────────────────────────────

    async def _entry_scheduler_loop(self) -> None:
        while True:
            now, today, t = datetime.now(), datetime.now().date(), datetime.now().time()

            if t < dt_time(9, 15) or t >= EOD_EXIT:
                await asyncio.sleep(60)
                continue

            if self.state is not None and not self.state.get("closed"):
                await asyncio.sleep(30)
                continue

            if is_market_holiday(API_KEY, today.isoformat(), exchange="NSE"):
                await asyncio.sleep(1800)
                continue

            if self._entry_attempted_date == today:
                await asyncio.sleep(30)
                continue

            entry_open  = now.replace(hour=ENTRY_HOUR, minute=ENTRY_MINUTE, second=0, microsecond=0)
            entry_close = entry_open + timedelta(minutes=ENTRY_WINDOW_MINS)

            if now < entry_open:
                wait_s = int((entry_open - now).total_seconds())
                await asyncio.sleep(min(wait_s, 60))
                continue

            if now > entry_close:
                logger.info(f"  Entry window closed for {today} — no trade today.")
                self._entry_attempted_date = today
                await asyncio.sleep(30)
                continue

            res = await self._enter()
            self._entry_attempted_date = today
            if res == "success":
                logger.info("  ✅ Position entered — exit monitor loop takes over.")
            else:
                logger.warning(f"  Entry failed ({res}) — will not retry today.")
            await asyncio.sleep(30)

    # ── Main run ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        _acquire_pid_lock()
        logger.info(f"🚀 {STRATEGY_NAME} starting — {date.today()}")

        if self.state:
            logger.info(f"♻️  Restored open position from {self.state.get('entry_time', '?')}")
        else:
            logger.info("   No open position — awaiting 10:30 IST entry window.")

        await send_async(
            f"🚀 {BOT_NAME} started\n"
            f"  NIFTY ATM Short Straddle | Entry {ENTRY_HOUR:02d}:{ENTRY_MINUTE:02d} IST\n"
            f"  SL={SL_MULT}× (breakeven trail)  Target={TARGET_PCT*100:.2f}% of margin  "
            f"{N_LOTS} lots/leg  EOD {EOD_EXIT}"
        )

        await asyncio.gather(
            self._entry_scheduler_loop(),
            self._exit_monitor_loop(),
            self._eod_guard_loop(),
            self._state_dump_loop(),
        )


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    bot = NiftyAtmStraddleScalpBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("⛔ Bot stopped (KeyboardInterrupt).")


if __name__ == "__main__":
    main()
