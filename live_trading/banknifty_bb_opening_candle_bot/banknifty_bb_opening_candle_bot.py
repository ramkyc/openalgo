"""
BANKNIFTY BB Opening Candle Bot — LIVE (fyers_cs instance)
============================================================
live_trading/banknifty_bb_opening_candle_bot/banknifty_bb_opening_candle_bot.py

Ported from fyers_crk's paper-trading instance of the same bot (2026-07-16),
after both a live-readiness review and a fill-check watchdog fix (see
FILL_CHECK_DEADLINE below). N_LOTS is deliberately 1 here (vs fyers_crk's 10)
per the "1 lot flat until 20+ live trades observed" sizing doctrine.

Research-validated (bb_opening_candle_study, 2026-06-28):
  options_data/research/bb_opening_candle_study/FINDINGS.md
  8/8 validation stages PASS.

Strategy:
  At the 09:15 IST 1-min bar close, check each ATM option:
    Signal: bar HIGH > BB(20, 2σ) upper band
    Entry : SELL LIMIT at (Close_09:15 + High_09:15) / 2 → placed at 09:16 open
    SL    : fill_price + 10 pts (fixed)
    Target: evolving 20-bar rolling SMA (BB midband) — exit when close ≤ SMA
    EOD   : 15:14 IST unconditional close

Three legs run simultaneously (independent, each with its own limit order):
  • BANKNIFTY CE  — nearest monthly NFO expiry
  • BANKNIFTY PE  — nearest monthly NFO expiry
  • SENSEX PE     — nearest weekly BFO expiry

Pre-trade filters (applied once per day at session open):
  ADX filter : compute BANKNIFTY daily ADX(14) from prior-day history.
               Skip all legs if ADX > 35 (strong trending regime destroys edge).
  VIX filter : read INDIAVIX LTP at 09:15. Skip all legs if VIX ≥ 18 (advisory).

BB seed: last 20 bars of the prior trading day loaded via get_history() so that
  the 09:15 bar has a valid BB(20) reading immediately.

Fill mechanics:
  Limit order placed at 09:16 open. At the 09:16 bar close (~09:17:00) the bot
  checks the orderbook: filled → activate SL/target monitoring; not filled → cancel.

Research: OOS avg +5.52 pts/trade (BNF CE+PE), +5.99 pts (SENSEX PE).
  MC P(positive) = 100% (BNF), 98.3% (SENSEX). Walk-forward: 11/13 windows pass.
"""

from __future__ import annotations

import asyncio
import atexit
import json
import logging
import os
import sys
from collections import deque
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import websockets
from dotenv import load_dotenv
from openalgo import api

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo/
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils                import get_expiry_dates, get_option_symbol, get_history, is_market_holiday, get_quote
from live_trading.shared.atm_resolver      import get_option_ltp
from live_trading.shared.order_fill        import fetch_fill_price
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger      import log_trade_to_db
from live_trading.shared.decision_logger   import DecisionLogger
from live_trading.shared.tick_watchdog     import TickWatchdog

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

_handlers = [logging.FileHandler(LOGS_DIR / "banknifty_bb_opening_candle_bot.log")]
if sys.stdout.isatty():
    # start_all_bots.py launches this as a subprocess with stdout/stderr
    # redirected into the SAME log file above — adding a StreamHandler there
    # too would double-write every line. Only attach it for interactive runs
    # (manual `uv run` from a terminal).
    _handlers.append(logging.StreamHandler())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=_handlers,
)
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER",   "http://127.0.0.1:5001")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")

if not API_KEY:
    logger.error("❌ OPENALGO_API_KEY not found. Exiting.")
    sys.exit(1)

# ── Strategy constants ────────────────────────────────────────────────────────
STRATEGY_NAME = "BNF_BB_OC"

N_LOTS      = 1        # lots per leg (all three legs independently)
                       # LIVE account: 1 lot flat per user's doctrine until 20+
                       # live trades are observed (see fyers_crk's N_LOTS=10,
                       # which is the paper-mode instance of this same bot).
BB_PERIOD   = 20
BB_STD_MULT = 2.0
SL_PTS      = 10.0     # fixed stop: fill_price + 10 pts
SL_LIMIT_CAP_PTS = 20.0  # SL-L limit price = trigger + this many extra pts.
                       # Bounds worst-case broker-side slippage on the stop
                       # exit — was uncapped SL-M before, which realized
                       # 45-94pt slippage in the thin opening window across
                       # two documented incidents (2026-07-20, 2026-07-30/31).
                       # If price gaps straight through the whole cap, the
                       # resting SL-L won't fill; _check_sl_order_filled's
                       # cap-breach branch then force-exits at market instead
                       # of leaving the leg unprotected indefinitely.

ADX_SKIP_THRESHOLD = 35.0   # skip day if prior-day BNF ADX(14) > 35
VIX_SKIP_THRESHOLD = 18.0   # skip day if INDIAVIX at 09:15 >= 18 (advisory)

WARMUP_DAYS      = 3         # prior-trading-day bars used to seed BB(20)
MIN_BARS_NEEDED  = BB_PERIOD + 1  # 21 bars minimum before first signal check

# MIS intraday EOD exit hard limit — must be ≤ 15:14
PRE_MARKET   = dt_time(9,  10)   # symbol resolution + BB warmup (5-min buffer)
MARKET_OPEN  = dt_time(9,  15)
SESSION_END  = dt_time(15, 14)
SIGNAL_MIN   = 9 * 60 + 15   # 09:15 bucket

# _check_limit_fill() normally runs when a live tick closes the 09:16 bar
# (see _on_tick's prev_bucket == SIGNAL_MIN + 1 branch). If the option prints
# no ticks in that window (thin liquidity on that specific strike/minute),
# the bar never closes and the fill check never fires — the limit order then
# sits unmonitored (no SL, no target checks) until the 15:14 EOD sweep.
# Confirmed live 2026-07-16: BNF_CE got its 09:15 signal, placed the limit,
# and stayed stuck at LIMIT_PLACED for the rest of the session with no tick
# ever closing that bar. _fill_check_watchdog_loop() forces the same check
# on a wall-clock timer, independent of ticks, past this deadline.
FILL_CHECK_DEADLINE = dt_time(9, 18)

# If the bot process wasn't alive during the live 09:15→09:16 bucket
# rollover (e.g. restarted after a launcher outage — see incident
# 2026-07-09), it can recover the signal from historical 1-min candles
# instead of sitting idle for the rest of the day. Bounded to this
# deadline: past it, the entry price would be chasing a candle that
# closed too long ago to represent the opening-breakout edge, so the
# leg is skipped for the day instead.
CATCHUP_DEADLINE = dt_time(9, 30)

STATE_FILE   = LOGS_DIR / "banknifty_bb_opening_candle_state.json"
PID_FILE     = LOGS_DIR / "banknifty_bb_opening_candle_bot.pid"

# Decision-state logging (jsonl + throttled heartbeat — see shared/decision_logger.py)
DECISION_LOG   = LOGS_DIR / "banknifty_bb_opening_candle_decisions.jsonl"
HEARTBEAT_SECS = 300

# ── Leg definitions ───────────────────────────────────────────────────────────
LEG_CONFIGS = {
    "BNF_CE": {
        "index_sym":        "BANKNIFTY",
        "idx_exchange":     "NSE_INDEX",
        "opt_type":         "CE",
        "opt_exchange":     "NFO",
        "expiry_type":      "monthly",   # nearest monthly with DTE 3–35
        "default_lot_size": 30,
    },
    "BNF_PE": {
        "index_sym":        "BANKNIFTY",
        "idx_exchange":     "NSE_INDEX",
        "opt_type":         "PE",
        "opt_exchange":     "NFO",
        "expiry_type":      "monthly",
        "default_lot_size": 30,
    },
    "SENSEX_PE": {
        "index_sym":        "SENSEX",
        "idx_exchange":     "BSE_INDEX",
        "opt_type":         "PE",
        "opt_exchange":     "BFO",
        "expiry_type":      "weekly",    # nearest weekly with DTE 1–7
        "default_lot_size": 10,
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# Tick-size rounding — NSE/BSE F&O LIMIT prices must be a multiple of ₹0.05
# ══════════════════════════════════════════════════════════════════════════════

def _round_to_tick(price: float, tick: float = 0.05) -> float:
    return round(round(price / tick) * tick, 2)


# ══════════════════════════════════════════════════════════════════════════════
# PID lock — single-instance guard
# ══════════════════════════════════════════════════════════════════════════════

def _acquire_pid_lock() -> None:
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            os.kill(old_pid, 0)
            logger.error(f"❌ Another instance already running (PID {old_pid}). Exiting.")
            sys.exit(1)
        except ProcessLookupError:
            logger.warning(f"⚠️ Stale PID file (PID {old_pid}). Removing.")
            PID_FILE.unlink(missing_ok=True)
        except (PermissionError, ValueError):
            logger.error(f"❌ Cannot verify PID {old_pid}. Delete {PID_FILE} to override.")
            sys.exit(1)
    PID_FILE.write_text(str(os.getpid()))
    atexit.register(_release_pid_lock)
    logger.info(f"🔒 PID lock acquired (PID {os.getpid()})")


def _release_pid_lock() -> None:
    try:
        if PID_FILE.exists() and int(PID_FILE.read_text().strip()) == os.getpid():
            PID_FILE.unlink()
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# REST helpers
# ══════════════════════════════════════════════════════════════════════════════

def _multiquote(items: list[tuple[str, str]],
                retries: int = 3, delay: float = 3.0) -> dict[str, float]:
    pending = list(dict.fromkeys(items))
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
            if res.status_code == 200 and res.json().get("status") == "success":
                for row in res.json().get("results", []):
                    sym  = row.get("symbol")
                    qd   = row.get("data") or {}
                    ltp  = (qd.get("ltp") or qd.get("last_price") or
                            qd.get("close") or qd.get("c") or 0)
                    try:
                        p = float(ltp)
                    except (TypeError, ValueError):
                        p = 0.0
                    if sym and p > 0:
                        out[sym] = p
        except Exception as e:
            logger.error(f"multiquotes error: {e}")
        pending = [(s, e) for s, e in pending if s not in out]
        if pending and attempt < retries:
            import time
            time.sleep(delay)
    return out


def _get_lot_size(symbol: str, opt_exchange: str, default: int) -> int:
    try:
        from database.token_db import get_symbol_info
        si = get_symbol_info(symbol, opt_exchange)
        if si and getattr(si, "lotsize", None):
            ls = int(si.lotsize)
            logger.info(f"  Lot size from DB: {ls}  ({symbol})")
            return ls
    except Exception as e:
        logger.warning(f"  Lot size DB lookup failed: {e}. Using default {default}.")
    return default


def _get_expiry(index_sym: str, opt_exchange: str, expiry_type: str) -> str | None:
    dates = get_expiry_dates(API_KEY, index_sym, opt_exchange, "options")
    if not dates:
        return None
    today = datetime.now().date()
    candidates = []
    for d in dates:
        try:
            exp_dt = datetime.strptime(d, "%d%b%y").date()
            dte = (exp_dt - today).days
            if expiry_type == "monthly":
                if 3 <= dte <= 35:
                    candidates.append((dte, d))
            else:  # weekly
                if 1 <= dte <= 8:
                    candidates.append((dte, d))
        except ValueError:
            continue
    if not candidates:
        logger.warning(f"  No {expiry_type} expiry for {index_sym} ({opt_exchange}). Available: {dates[:5]}")
        return None
    candidates.sort()
    dte, exp_str = candidates[0]
    logger.info(f"  {index_sym} {expiry_type} expiry: {exp_str}  (DTE={dte})")
    return exp_str


def _check_fill(client, order_id: str, symbol: str, opt_exchange: str) -> tuple[str, float]:
    """
    Returns (status, fill_price), status is one of:
    "filled", "rejected", "cancelled", "pending" (still open / not yet resolved).
    Parses the orderbook for the given order_id.
    """
    try:
        ob = client.orderbook()
        if isinstance(ob, dict) and ob.get("status") == "success":
            # /orderbook wraps the list: data = {"orders": [...], "statistics": {...}}
            data = ob.get("data") or {}
            orders = data.get("orders", []) if isinstance(data, dict) else []
        elif isinstance(ob, list):
            orders = ob
        else:
            return "pending", 0.0
        for o in orders:
            if not isinstance(o, dict):
                continue
            if str(o.get("orderid", "")) == str(order_id):
                status = str(o.get("order_status") or o.get("status") or "").lower()
                if status in ("complete", "filled", "traded"):
                    price = float(o.get("average_price", 0) or o.get("price", 0) or 0)
                    return "filled", price
                if status == "rejected":
                    return "rejected", 0.0
                if status in ("cancelled", "canceled"):
                    return "cancelled", 0.0
                return "pending", 0.0
    except Exception as e:
        logger.warning(f"  Orderbook check failed for {order_id}: {e}")
    return "pending", 0.0


def _cancel_order(client, order_id: str, symbol: str, opt_exchange: str) -> None:
    try:
        # cancelorder accepts only order_id/strategy — extra fields are
        # forwarded into the payload and rejected with HTTP 400
        res = client.cancelorder(
            order_id=order_id,
            strategy=STRATEGY_NAME,
        )
        logger.info(f"  Cancel {order_id}: {res}")
    except Exception as e:
        logger.warning(f"  Cancel order {order_id} failed: {e}")


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback


# ══════════════════════════════════════════════════════════════════════════════
# ADX computation — prior-day BANKNIFTY ADX(14) from REST history
# ══════════════════════════════════════════════════════════════════════════════

def _compute_daily_adx(period: int = 14) -> float | None:
    """
    Pull 180 calendar days (~120 trading days) of daily BANKNIFTY OHLC from OpenAlgo history.
    Compute ADX(14) using EWM smoothing (alpha=1/period).
    EWM needs ~5x period of warm-up bars to converge (Wilder ADX cold-start bias) —
    30 days was not enough and inflated readings ~2x. Returns the most recent
    completed day's ADX, or None on failure.
    """
    try:
        raw = get_history(API_KEY, "BANKNIFTY", "NSE_INDEX", "D", 180)
        if not raw:
            return None
        df = pd.DataFrame(raw)
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["high", "low", "close"]).reset_index(drop=True)
        if len(df) < period * 5:
            return None

        df["tr"]  = np.maximum(df["high"] - df["low"],
                    np.maximum(abs(df["high"] - df["close"].shift()),
                               abs(df["low"]  - df["close"].shift())))
        df["+dm"] = np.where(
            (df["high"] - df["high"].shift()) > (df["low"].shift() - df["low"]),
            np.maximum(df["high"] - df["high"].shift(), 0), 0
        )
        df["-dm"] = np.where(
            (df["low"].shift() - df["low"]) > (df["high"] - df["high"].shift()),
            np.maximum(df["low"].shift() - df["low"], 0), 0
        )
        a = 1 / period
        for c in ["tr", "+dm", "-dm"]:
            df[f"{c}_s"] = df[c].ewm(alpha=a, adjust=False).mean()
        df["+di"] = 100 * df["+dm_s"] / df["tr_s"]
        df["-di"] = 100 * df["-dm_s"] / df["tr_s"]
        df["dx"]  = 100 * abs(df["+di"] - df["-di"]) / (df["+di"] + df["-di"])
        df["adx"] = df["dx"].ewm(alpha=a, adjust=False).mean()

        adx_val = float(df["adx"].iloc[-1])
        logger.info(f"  Daily BANKNIFTY ADX(14) = {adx_val:.1f}")
        return adx_val
    except Exception as e:
        logger.warning(f"  ADX computation failed: {e}. ADX filter bypassed.")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# OptionPremiumBar — 1-min bar builder with BB computation
# ══════════════════════════════════════════════════════════════════════════════

class OptionPremiumBar:
    """
    Builds 1-min OHLC bars from live option ticks.
    Tracks the previous bucket (minute) so callers can detect which bar just closed.
    """

    def __init__(self, label: str):
        self.label = label
        self.bars: deque = deque(maxlen=200)  # completed bars (warmup + intraday)
        self._bucket:      int   = -1
        self._prev_bucket: int   = -1
        self._open:  float = 0.0
        self._high:  float = 0.0
        self._low:   float = float("inf")
        self._last:  float = 0.0
        self.ltp:    float = 0.0

    def update(self, ltp: float, ts: datetime) -> bool:
        """
        Feed a tick. Returns True when a 1-min bar completes.
        On True, self._prev_bucket holds the just-completed minute bucket.
        """
        if ltp <= 0:
            return False
        self.ltp = ltp
        bucket = ts.hour * 60 + ts.minute

        if self._bucket == -1:
            self._bucket = bucket
            self._open = self._high = self._low = self._last = ltp
            return False

        if bucket != self._bucket:
            if self._open > 0:
                self.bars.append({
                    "open":  self._open,
                    "high":  self._high,
                    "low":   self._low,
                    "close": self._last,
                })
            self._prev_bucket = self._bucket
            self._bucket = bucket
            self._open = self._high = self._low = self._last = ltp
            return True

        self._high = max(self._high, ltp)
        self._low  = min(self._low, ltp)
        self._last = ltp
        return False

    def warmup(self, bars: list[dict]) -> None:
        """Pre-load prior-day 1-min bars to seed BB(20)."""
        for b in bars[-190:]:
            self.bars.append({
                "open":  float(b.get("open",  0)),
                "high":  float(b.get("high",  0)),
                "low":   float(b.get("low",   0)),
                "close": float(b.get("close", 0)),
            })
        logger.info(f"  [{self.label}] Warmup: {len(self.bars)} bars loaded.")

    def compute_bb(self) -> dict | None:
        """BB(20, 2σ) on completed bar closes. Returns None if insufficient bars."""
        if len(self.bars) < MIN_BARS_NEEDED:
            return None
        closes = pd.Series([b["close"] for b in self.bars])
        sma    = closes.rolling(BB_PERIOD).mean().iloc[-1]
        std    = closes.rolling(BB_PERIOD).std().iloc[-1]
        if pd.isna(sma) or pd.isna(std) or std == 0:
            return None
        upper = sma + BB_STD_MULT * std
        lower = sma - BB_STD_MULT * std
        close = closes.iloc[-1]
        high  = self.bars[-1]["high"] if self.bars else 0.0
        return {
            "upper": round(float(upper), 2),
            "sma":   round(float(sma),   2),
            "lower": round(float(lower),  2),
            "close": round(float(close),  2),
            "high":  round(float(high),   2),
            "std":   round(float(std),    2),
            "bars":  len(self.bars),
            "signal": bool(high > upper),          # High > upper band
            "close_above_upper": bool(close > upper),
        }


# ══════════════════════════════════════════════════════════════════════════════
# Per-leg runtime state
# ══════════════════════════════════════════════════════════════════════════════

class LegState:
    """Runtime state for a single option leg."""

    WARMUP         = "WARMUP"
    READY          = "READY"
    SKIP_DAY       = "SKIP_DAY"
    CHECKED        = "CHECKED"        # 09:15 bar checked — no signal
    LIMIT_PLACED   = "LIMIT_PLACED"   # limit order open, waiting fill
    ACTIVE         = "ACTIVE"         # filled, monitoring SL/SMA
    CLOSED         = "CLOSED"         # trade completed

    def __init__(self, key: str, cfg: dict):
        self.key        = key
        self.cfg        = cfg
        self.bars       = OptionPremiumBar(key)
        self.status     = self.WARMUP

        # Resolved at session open
        self.symbol:    str | None = None
        self.expiry:    str | None = None
        self.lot_size:  int = cfg["default_lot_size"]
        self.qty:       int = 0

        # Limit order
        self.midpoint:  float | None = None
        self.order_id:  str | None   = None
        self.fill_price: float        = 0.0
        self.sl_price:   float        = 0.0
        self.sl_limit_price: float    = 0.0    # SL-L cap = sl_price + SL_LIMIT_CAP_PTS
        self.sl_order_id: str | None  = None   # resting broker-side SL-L order id

        # Position tracking
        self.entry_time:  str | None = None
        self.exit_reason: str | None = None
        self.gross_pnl:   float      = 0.0

        # Index spot at 09:15 (for dashboard)
        self.spot_at_signal: float = 0.0
        self.bb_at_signal:   dict  = {}

    def to_dict(self) -> dict:
        return {
            "key":            self.key,
            "status":         self.status,
            "symbol":         self.symbol,
            "expiry":         self.expiry,
            "lot_size":       self.lot_size,
            "qty":            self.qty,
            "midpoint":       self.midpoint,
            "order_id":       self.order_id,
            "fill_price":     self.fill_price,
            "sl_price":       self.sl_price,
            "sl_limit_price": self.sl_limit_price,
            "sl_order_id":    self.sl_order_id,
            "entry_time":     self.entry_time,
            "exit_reason":    self.exit_reason,
            "gross_pnl":      self.gross_pnl,
            "ltp":            round(self.bars.ltp, 2),
            "spot_at_signal": self.spot_at_signal,
            "bb_at_signal":   self.bb_at_signal,
            "bb_now":         self.bars.compute_bb() or {},
        }


# ══════════════════════════════════════════════════════════════════════════════
# Bot
# ══════════════════════════════════════════════════════════════════════════════

class BNFBBOpeningCandleBot:

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.ws     = None

        # Three legs
        self.legs: dict[str, LegState] = {
            k: LegState(k, v) for k, v in LEG_CONFIGS.items()
        }
        # Index and VIX live prices
        self.bnf_ltp:    float = 0.0
        self.sensex_ltp: float = 0.0
        self.vix_ltp:    float = 0.0

        # Session-level flags
        self.pre_market_done:  bool = False
        self.session_started:  bool = False
        self._next_init_retry = datetime.min   # retry gate for failed leg init
        self.eod_done:         bool = False
        self.first_connect:    bool = True
        self.skip_day:         bool = False    # True when ADX or VIX filter fires
        self.skip_reason:      str  = ""
        self.daily_adx:        float | None = None

        # WS subscriptions
        self._subscribed: set[str] = set()

        # Symbol → leg routing
        self._sym_to_leg: dict[str, str] = {}

        # Decision-state logging (jsonl + throttled heartbeat)
        self._dlog = DecisionLogger(DECISION_LOG, heartbeat_secs=HEARTBEAT_SECS, bot_logger=logger)

        self._watchdog = TickWatchdog(
            bot_name="BankNifty BB Opening Candle Bot",
            tracked_symbols=lambda: ["BANKNIFTY", "SENSEX", "INDIAVIX"] + [
                leg.symbol for leg in self.legs.values()
                if leg.status == LegState.ACTIVE and leg.symbol
            ],
            market_open=MARKET_OPEN,
            market_close=SESSION_END,
            bot_logger=logger,
            on_dead_feed=self._resubscribe_all,
        )

        self._restore_state()

    # ── State restore ─────────────────────────────────────────────────────────

    def _restore_state(self) -> None:
        if not STATE_FILE.exists():
            return
        try:
            state = json.loads(STATE_FILE.read_text())
            last = state.get("last_update", "")
            if not last:
                return
            if datetime.fromisoformat(last).date() != datetime.now().date():
                return
            logger.info("🔄 Restoring today's session state…")
            for key, leg in self.legs.items():
                ls = state.get("legs", {}).get(key, {})
                if not ls:
                    continue
                leg.symbol   = ls.get("symbol")
                leg.expiry   = ls.get("expiry")
                leg.lot_size = ls.get("lot_size", leg.lot_size)
                leg.qty      = ls.get("qty", 0)
                leg.status   = ls.get("status", LegState.WARMUP)
                if leg.symbol:
                    self._sym_to_leg[leg.symbol] = key
                # Restore active positions
                if leg.status == LegState.ACTIVE:
                    leg.fill_price = float(ls.get("fill_price", 0))
                    leg.sl_price   = float(ls.get("sl_price",   0))
                    leg.sl_limit_price = float(ls.get("sl_limit_price", 0)) or round(leg.sl_price + SL_LIMIT_CAP_PTS, 2)
                    leg.midpoint   = float(ls.get("midpoint",   0))
                    leg.order_id   = ls.get("order_id")
                    leg.sl_order_id = ls.get("sl_order_id")
                    leg.entry_time = ls.get("entry_time")
                    leg.qty        = int(ls.get("qty", 0))
                    logger.warning(
                        f"🔄 [{key}] Restored ACTIVE trade: "
                        f"symbol={leg.symbol}  fill=₹{leg.fill_price:.2f}  SL=₹{leg.sl_price:.2f}  "
                        f"sl_order_id={leg.sl_order_id}"
                    )
                elif leg.status in (LegState.CLOSED, LegState.CHECKED, LegState.SKIP_DAY):
                    logger.info(f"🔄 [{key}] Restored status: {leg.status} — no new entries today.")
        except Exception as e:
            logger.warning(f"_restore_state error: {e}")

    # ── Pre-market init (09:10) — symbol resolution + BB warmup ─────────────

    async def _pre_market_init(self, bnf_spot: float) -> None:
        """Resolve ATM symbols and warm up BB at 09:10 — 5-min buffer before signal."""
        if is_market_holiday(API_KEY):
            return
        logger.info(f"⏰ Pre-market init (09:10). BANKNIFTY ≈ {bnf_spot:.0f}")

        for key, leg in self.legs.items():
            cfg = leg.cfg
            if leg.status in (LegState.ACTIVE, LegState.LIMIT_PLACED, LegState.CLOSED, LegState.CHECKED):
                logger.info(f"  [{key}] Status restored as {leg.status} — skipping symbol resolution.")
                if leg.symbol:
                    await self._subscribe(leg.symbol, cfg["opt_exchange"])
                continue

            exp = await asyncio.to_thread(
                _get_expiry, cfg["index_sym"], cfg["opt_exchange"], cfg["expiry_type"]
            )
            if not exp:
                logger.warning(f"  [{key}] No expiry found — leg inactive today.")
                leg.status = LegState.SKIP_DAY
                continue

            sym = await asyncio.to_thread(
                get_option_symbol,
                API_KEY, cfg["index_sym"], cfg["opt_exchange"], exp, cfg["opt_type"], "ATM",
            )
            if not sym:
                logger.error(f"  [{key}] ATM symbol resolution failed (spot≈{bnf_spot:.0f}, expiry={exp}).")
                leg.status = LegState.SKIP_DAY
                continue

            leg.symbol   = sym
            leg.expiry   = exp
            leg.lot_size = _get_lot_size(sym, cfg["opt_exchange"], cfg["default_lot_size"])
            leg.qty      = N_LOTS * leg.lot_size
            leg.status   = LegState.WARMUP
            self._sym_to_leg[sym] = key
            logger.info(f"  [{key}] Symbol={sym}  Expiry={exp}  Lot={leg.lot_size}  Qty={leg.qty}")

            await self._subscribe(sym, cfg["opt_exchange"])

        await self._subscribe("BANKNIFTY", "NSE_INDEX")
        await self._subscribe("SENSEX",    "BSE_INDEX")
        await self._subscribe("INDIAVIX",  "NSE_INDEX")

        for key, leg in self.legs.items():
            if not leg.symbol or leg.status not in (LegState.WARMUP,):
                continue
            await self._warmup_leg(leg)

    # ── Session open (09:15) — ADX filter + Telegram ──────────────────────────

    async def _session_open(self, bnf_spot: float) -> None:
        if is_market_holiday(API_KEY):
            logger.info("⛔ Market holiday. Skipping session init.")
            return

        logger.info(f"🔔 Session open. BANKNIFTY ≈ {bnf_spot:.0f}")

        # Fallback: if pre_market_init didn't run (late start), do it now
        if not self.pre_market_done:
            logger.warning("⚠️ Pre-market init not done — running symbol resolution at 09:15 (fallback).")
            await self._pre_market_init(bnf_spot)

        # ── ADX filter (computed from prior-day daily history) ────────────────
        self.daily_adx = await asyncio.to_thread(_compute_daily_adx)
        if self.daily_adx is not None and self.daily_adx > ADX_SKIP_THRESHOLD:
            self.skip_day    = True
            self.skip_reason = f"ADX={self.daily_adx:.1f} > {ADX_SKIP_THRESHOLD} (strong trend — skip all legs)"
            logger.warning(f"⛔ {self.skip_reason}")
            for leg in self.legs.values():
                leg.status = LegState.SKIP_DAY
            await send_async(
                f"⛔ *BNF BB Opening Candle — ADX Filter*\n"
                f"Daily BANKNIFTY ADX(14) = {self.daily_adx:.1f} > {ADX_SKIP_THRESHOLD:.0f}.\n"
                f"All three legs SKIPPED today.\n"
                f"_Research: ADX>35 → avg PnL −1.24 pts (destroys edge)_"
            )
            return

        if self.first_connect:
            self.first_connect = False
            adx_str = f"{self.daily_adx:.1f}" if self.daily_adx is not None else "N/A"
            await send_async(
                f"🤖 *BNF BB Opening Candle Bot — Online*\n"
                f"Signal  : 09:15 ATM option High > BB({BB_PERIOD},{BB_STD_MULT}σ)\n"
                f"Entry   : SELL LIMIT at (Close + High)/2 @ 09:16\n"
                f"SL      : Fill + {SL_PTS:.0f} pts  |  Target: evolving {BB_PERIOD}-bar SMA\n"
                f"Legs    : BNF CE · BNF PE · SENSEX PE  ({N_LOTS} lots each)\n"
                f"Daily ADX(14): {adx_str}  (skip if >{ADX_SKIP_THRESHOLD:.0f})\n"
                f"Research: OOS avg +5.52 pts (BNF), MC 100% positive"
            )

        # Catch up legs that missed the live 09:15→09:16 bucket rollover
        # because the process wasn't running through it (see CATCHUP_DEADLINE).
        # Spawned as background tasks — never awaited inline here — because
        # this coroutine runs on the same tick-processing loop as every other
        # leg's SL monitoring; blocking it on a history fetch would stall them.
        now_t = datetime.now().time()
        if not self.skip_day and dt_time(9, 16) <= now_t < CATCHUP_DEADLINE:
            for leg in self.legs.values():
                if leg.status == LegState.READY:
                    asyncio.create_task(self._catchup_missed_signal(leg))

    # ── Catch-up: recover a missed 09:15 signal check from history ──────────

    async def _catchup_missed_signal(self, leg: LegState) -> None:
        """
        Recover the 09:15 opening-candle bar from historical 1-min data when
        the bot wasn't alive for the live bucket rollover that normally
        triggers _check_signal_at_915 (see _on_tick's bar-close routing).
        Without this, a leg still READY past 09:16 stays READY for the rest
        of the day — the trigger is a one-shot live event, not a poll.
        """
        if leg.status != LegState.READY or not leg.symbol:
            return
        if datetime.now().time() >= CATCHUP_DEADLINE:
            logger.warning(f"  [{leg.key}] Catch-up window closed ({CATCHUP_DEADLINE}) — skipping today.")
            leg.status = LegState.SKIP_DAY
            return

        logger.warning(f"  [{leg.key}] Bot wasn't alive for the live 09:15 candle — recovering it from history.")
        cfg = leg.cfg
        raw = None
        for attempt in range(1, 4):
            try:
                raw = await asyncio.to_thread(
                    get_history, API_KEY, leg.symbol, cfg["opt_exchange"], "1m", WARMUP_DAYS
                )
            except Exception as e:
                logger.warning(f"  [{leg.key}] Catch-up history fetch error (attempt {attempt}/3): {e}")
                raw = None
            if raw:
                break
            await asyncio.sleep(5)

        if not raw:
            logger.error(f"  [{leg.key}] Catch-up: no history after 3 attempts — leg stays READY, no further retry.")
            return

        df = pd.DataFrame(raw)
        if "timestamp" in df.columns:
            df["dt"] = (
                pd.to_datetime(df["timestamp"], unit="s", utc=True)
                .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
            )
        elif "date" in df.columns:
            df["dt"] = pd.to_datetime(df["date"])
        else:
            logger.error(f"  [{leg.key}] Catch-up: history response has no timestamp/date column.")
            return
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close"])

        today = datetime.now().date()
        opening_bar = df[
            (df["dt"].dt.date == today) & (df["dt"].dt.hour == 9) & (df["dt"].dt.minute == 15)
        ]
        if opening_bar.empty:
            logger.error(f"  [{leg.key}] Catch-up: no 09:15 bar found in history — leg stays READY, no further retry.")
            return

        row = opening_bar.iloc[-1]
        leg.bars.bars.append({
            "open":  float(row["open"]),
            "high":  float(row["high"]),
            "low":   float(row["low"]),
            "close": float(row["close"]),
        })
        leg.bars._prev_bucket = SIGNAL_MIN
        logger.info(
            f"  [{leg.key}] Catch-up: recovered 09:15 bar from history "
            f"(O={row['open']:.2f} H={row['high']:.2f} L={row['low']:.2f} C={row['close']:.2f})."
        )

        await self._check_signal_at_915(leg)

        # The 09:16 bucket-close fill check is also a one-shot live event we
        # missed — if catch-up placed an order, check it ourselves shortly
        # after instead of leaving it unmonitored (no SL) until EOD cleanup.
        if leg.status == LegState.LIMIT_PLACED:
            await asyncio.sleep(60)
            await self._check_limit_fill(leg)

    # ── BB warmup from history ────────────────────────────────────────────────

    async def _warmup_leg(self, leg: LegState) -> None:
        cfg = leg.cfg
        MAX_RETRIES = 3
        raw = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                raw = await asyncio.to_thread(
                    get_history, API_KEY, leg.symbol, cfg["opt_exchange"], "1m", WARMUP_DAYS
                )
                if raw:
                    break
                if attempt < MAX_RETRIES:
                    logger.warning(f"  [{leg.key}] Empty history (attempt {attempt}/{MAX_RETRIES}) — retrying in 5s.")
                    await asyncio.sleep(5)
            except Exception as e:
                if attempt < MAX_RETRIES:
                    logger.warning(f"  [{leg.key}] Warmup fetch error (attempt {attempt}/{MAX_RETRIES}): {e} — retrying in 5s.")
                    await asyncio.sleep(5)
                else:
                    logger.error(f"  [{leg.key}] Warmup failed after {MAX_RETRIES} attempts: {e}")
                    leg.status = LegState.READY
                    return

        try:
            if not raw:
                logger.warning(f"  [{leg.key}] No history after {MAX_RETRIES} attempts — BB will warm on live ticks.")
                leg.status = LegState.READY
                return

            df = pd.DataFrame(raw)
            if "timestamp" in df.columns:
                df["dt"] = (
                    pd.to_datetime(df["timestamp"], unit="s", utc=True)
                    .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
                )
            elif "date" in df.columns:
                df["dt"] = pd.to_datetime(df["date"])
            else:
                leg.status = LegState.READY
                return

            df = df.sort_values("dt")
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["close"])

            # Only use bars BEFORE today (prior-day data only)
            today_start = pd.Timestamp.now().normalize()
            prior = df[df["dt"] < today_start]

            if prior.empty:
                logger.warning(f"  [{leg.key}] No prior-day bars found — BB will warm on live ticks.")
                leg.status = LegState.READY
                return

            bars = prior[["open", "high", "low", "close"]].to_dict("records")
            leg.bars.warmup(bars)
            leg.status = LegState.READY
            bb = leg.bars.compute_bb()
            logger.info(
                f"  [{leg.key}] ✅ Warm-up done: {len(leg.bars.bars)} bars. "
                f"BB upper={bb['upper'] if bb else 'N/A'}"
            )
        except Exception as e:
            logger.error(f"  [{leg.key}] Warmup error: {e}")
            leg.status = LegState.READY

    # ── Signal check at 09:15 bar close ──────────────────────────────────────

    async def _check_signal_at_915(self, leg: LegState) -> None:
        """Called when the 09:15 bar completes for this leg."""
        if leg.status not in (LegState.READY,):
            return
        if self.skip_day:
            leg.status = LegState.SKIP_DAY
            return

        # VIX filter — check live VIX at 09:15
        if self.vix_ltp > 0 and self.vix_ltp >= VIX_SKIP_THRESHOLD:
            logger.warning(
                f"  [{leg.key}] VIX={self.vix_ltp:.2f} ≥ {VIX_SKIP_THRESHOLD} — skipping entry today."
            )
            leg.status = LegState.SKIP_DAY
            return

        bb = leg.bars.compute_bb()
        if not bb:
            logger.warning(f"  [{leg.key}] Insufficient bars for BB at 09:15 ({len(leg.bars.bars)} bars). Skipping.")
            leg.status = LegState.CHECKED
            return

        # Signal: 09:15 candle HIGH > BB upper band
        if not bb["signal"]:
            logger.info(
                f"  [{leg.key}] No signal — High={bb['high']:.2f} ≤ BB upper={bb['upper']:.2f}"
            )
            leg.status = LegState.CHECKED
            return

        # Signal detected
        midpoint = _round_to_tick((bb["close"] + bb["high"]) / 2)
        leg.midpoint = midpoint
        leg.bb_at_signal = bb.copy()
        leg.spot_at_signal = self.bnf_ltp if "BNF" in leg.key else self.sensex_ltp
        logger.info(
            f"📶 [{leg.key}] SIGNAL — High={bb['high']:.2f} > BB upper={bb['upper']:.2f}  "
            f"Midpoint=(({bb['close']:.2f}+{bb['high']:.2f})/2)=₹{midpoint:.2f}"
        )
        await self._place_limit_order(leg, midpoint)

    # ── Place SELL LIMIT at midpoint ──────────────────────────────────────────

    async def _place_limit_order(self, leg: LegState, limit_price: float) -> None:
        cfg = leg.cfg
        try:
            resp = self.client.placeorder(
                strategy   = STRATEGY_NAME,
                symbol     = leg.symbol,
                action     = "SELL",
                exchange   = cfg["opt_exchange"],
                price_type = "LIMIT",
                price      = str(limit_price),
                product    = "MIS",
                quantity   = str(leg.qty),
            )
        except Exception as e:
            logger.error(f"  [{leg.key}] placeorder exception: {e}")
            leg.status = LegState.CHECKED   # don't leave in limbo
            return

        if resp.get("status") == "success":
            leg.order_id = str(resp.get("orderid", ""))
            leg.status   = LegState.LIMIT_PLACED
            logger.info(
                f"✅ [{leg.key}] LIMIT SELL placed at ₹{limit_price:.2f}  "
                f"qty={leg.qty}  order_id={leg.order_id}"
            )
            await send_async(
                f"📋 *BNF BB Opening Candle — Limit Order Placed*\n"
                f"Leg    : {leg.key}  |  {leg.symbol}\n"
                f"Signal : High ₹{leg.bb_at_signal['high']:.2f} > BB ₹{leg.bb_at_signal['upper']:.2f}\n"
                f"Limit  : ₹{limit_price:.2f}  (midpoint of close+high)\n"
                f"Qty    : {leg.qty} ({N_LOTS} lots × {leg.lot_size})\n"
                f"_Order will be checked for fill at 09:17_"
            )
        else:
            logger.error(f"  [{leg.key}] Limit order rejected: {resp}")
            leg.status = LegState.CHECKED

    # ── Fill check at 09:16 bar close ─────────────────────────────────────────

    async def _check_limit_fill(self, leg: LegState) -> None:
        """
        Called at the 09:16 bar close.  Checks orderbook for fill.
        If filled → activate.  If not → cancel.
        """
        if leg.status != LegState.LIMIT_PLACED:
            return
        if not leg.order_id:
            leg.status = LegState.CHECKED
            return

        fill_status, fill_price = await asyncio.to_thread(
            _check_fill, self.client, leg.order_id, leg.symbol, leg.cfg["opt_exchange"]
        )

        if fill_status == "filled":
            # Use the orderbook fill price if returned, else midpoint, as the fallback
            # chain for the authoritative lookup via OpenAlgo orderstatus below.
            fallback_fp = fill_price if fill_price > 0 else leg.midpoint
            fp = await asyncio.to_thread(
                _resolve_fill, {"orderid": leg.order_id}, fallback_fp
            )
            logger.info(f"  [{leg.key}] Entry fill=₹{fp:.2f} vs LTP-based fallback=₹{fallback_fp:.2f}")
            leg.fill_price = fp
            leg.sl_price   = round(fp + SL_PTS, 2)
            leg.sl_limit_price = round(leg.sl_price + SL_LIMIT_CAP_PTS, 2)
            leg.entry_time = datetime.now().isoformat()
            leg.status     = LegState.ACTIVE
            logger.info(
                f"✅ [{leg.key}] FILLED at ₹{fp:.2f}  SL=₹{leg.sl_price:.2f}  "
                f"(fill+{SL_PTS:.0f} pts)"
            )

            # Rest a real broker-side SL-L (stop-limit) order instead of relying
            # only on our own tick-by-tick LTP check, which needs a live WS tick
            # to even notice a breach — confirmed live 2026-07-20: BNF_PE/SENSEX_PE
            # both gapped 20-70pts past sl_price between two ticks in the thin
            # 09:16 opening-breakout window, realizing 45-94pt losses on a 10pt SL
            # (plain SL-M has no price protection once triggered). SL-L caps the
            # fill at sl_limit_price; _check_sl_order_filled force-exits at
            # market if price runs past that cap while the order sits unfilled.
            await self._place_sl_order(leg)

            await send_async(
                f"📉 *BNF BB Opening Candle — ENTRY FILLED*\n"
                f"Leg    : {leg.key}  |  {leg.symbol}\n"
                f"Fill   : ₹{fp:.2f}  (limit at ₹{leg.midpoint:.2f})\n"
                f"SL     : ₹{leg.sl_price:.2f} → cap ₹{leg.sl_limit_price:.2f}  (fill + {SL_PTS:.0f} pts"
                f"{', broker SL-L resting' if leg.sl_order_id else ', ⚠️ app-side only'})\n"
                f"Target : evolving {BB_PERIOD}-bar SMA\n"
                f"Qty    : {leg.qty}"
            )
        else:
            # Not filled — cancel
            logger.info(f"  [{leg.key}] Not filled at 09:16 close — cancelling order {leg.order_id}.")
            await asyncio.to_thread(
                _cancel_order, self.client, leg.order_id, leg.symbol, leg.cfg["opt_exchange"]
            )
            leg.order_id = None
            leg.status   = LegState.CHECKED
            await send_async(
                f"🚫 *BNF BB Opening Candle — Limit Not Filled*\n"
                f"Leg    : {leg.key}  |  {leg.symbol}\n"
                f"Limit  : ₹{leg.midpoint:.2f} — 09:16 bar did not reach this level.\n"
                f"Order cancelled. No trade today for this leg."
            )

    # ── (Re)place the broker-side SL-L order ────────────────────────────────
    # Shared by the entry-fill path and by _check_sl_order_filled's rejection
    # recovery. Sets leg.sl_order_id on success; leaves it None on failure so
    # _check_sl's app-side fallback engages instead. Uses SL-L (Fyers order
    # type 4, OpenAlgo price_type="SL") rather than SL-M — a plain SL-M has no
    # price protection once triggered and realized 45-99pt slippage across two
    # documented incidents. SL-L caps the fill at sl_limit_price; if price gaps
    # straight through the whole cap, the order won't fill and
    # _check_sl_order_filled's cap-breach branch escalates to a market exit
    # instead of leaving the leg unprotected.

    async def _place_sl_order(self, leg: LegState) -> None:
        cfg = leg.cfg
        if not leg.sl_limit_price:
            leg.sl_limit_price = round(leg.sl_price + SL_LIMIT_CAP_PTS, 2)
        try:
            sl_resp = self.client.placeorder(
                strategy      = STRATEGY_NAME,
                symbol        = leg.symbol,
                action        = "BUY",
                exchange      = cfg["opt_exchange"],
                price_type    = "SL",
                price         = str(leg.sl_limit_price),
                trigger_price = str(leg.sl_price),
                product       = "MIS",
                quantity      = str(leg.qty),
            )
        except Exception as e:
            logger.error(f"  [{leg.key}] Broker-side SL-L placement exception: {e}")
            sl_resp = None

        if sl_resp and sl_resp.get("status") == "success":
            leg.sl_order_id = str(sl_resp.get("orderid", ""))
            logger.info(
                f"  [{leg.key}] 🛡️ Broker-side SL-L resting @ trigger ₹{leg.sl_price:.2f} "
                f"cap ₹{leg.sl_limit_price:.2f}  order_id={leg.sl_order_id}"
            )
        else:
            leg.sl_order_id = None
            logger.error(
                f"  [{leg.key}] ⚠️ Broker-side SL-L FAILED to place ({sl_resp}) — "
                f"falling back to app-side tick monitoring only for this leg."
            )
            await send_async(
                f"⚠️ *{leg.key} — Broker-side SL-L order failed to place!*\n"
                f"Falling back to app-side tick monitoring only — slippage risk on SL exit."
            )

    # ── SMA exit check (bar-level) ────────────────────────────────────────────

    async def _check_sma_exit(self, leg: LegState) -> None:
        """Exit when the current 20-bar rolling SMA is touched (close ≤ SMA)."""
        if leg.status != LegState.ACTIVE:
            return
        bb = leg.bars.compute_bb()
        if not bb:
            return
        if bb["close"] <= bb["sma"]:
            ltp = leg.bars.ltp
            exit_p = ltp if ltp > 0 else bb["close"]
            logger.info(
                f"🎯 [{leg.key}] SMA touch — close ₹{bb['close']:.2f} ≤ SMA ₹{bb['sma']:.2f} → exit"
            )
            await self._close_leg(leg, exit_p, "SMA reversion")

    # ── SL check (tick-level) ─────────────────────────────────────────────────
    # Fallback only: once a broker-side SL-L order is resting (leg.sl_order_id
    # set — the normal case), that order is the authority on the exit. Also
    # closing the position from here would race it and risk a double BUY
    # (flipping the short into a long). This path only fires when the SL-L
    # placement itself failed, so the position has no protection otherwise.

    async def _check_sl(self, leg: LegState, ltp: float) -> None:
        if leg.status != LegState.ACTIVE or ltp <= 0 or leg.sl_order_id:
            return
        if ltp >= leg.sl_price:
            logger.warning(
                f"🛑 [{leg.key}] STOP LOSS (app-side fallback) — LTP ₹{ltp:.2f} ≥ SL ₹{leg.sl_price:.2f} → exit"
            )
            await self._close_leg(leg, ltp, f"SL +{SL_PTS:.0f}pts")

    # ── Broker-side SL-L fill reconciliation (bar-level) ────────────────────
    # Polls the resting SL-L order once per bar close so app state (and
    # SMA-exit) don't keep treating the leg as ACTIVE after the exchange has
    # already closed it out. Also watches for the cap-breach case an SL-L
    # doesn't protect against by itself: if price gaps straight through the
    # whole sl_limit_price band, the passive limit order never fills and the
    # leg would otherwise sit exposed until the next bar (or EOD) — escalates
    # to a market exit instead.

    async def _check_sl_order_filled(self, leg: LegState) -> bool:
        if leg.status != LegState.ACTIVE or not leg.sl_order_id:
            return False
        status, fill_price = await asyncio.to_thread(
            _check_fill, self.client, leg.sl_order_id, leg.symbol, leg.cfg["opt_exchange"]
        )
        if status == "filled":
            logger.warning(f"🛑 [{leg.key}] Broker-side SL-L filled @ ₹{fill_price:.2f}")
            await self._close_leg(
                leg, fill_price, f"SL +{SL_PTS:.0f}pts", already_filled_order_id=leg.sl_order_id
            )
            return True
        if status in ("rejected", "cancelled"):
            # The resting order is dead — this leg has had NO stop protection
            # since it died. Surface it immediately and re-place, falling back
            # to app-side monitoring (via _place_sl_order's own failure path)
            # if the re-placement also fails.
            dead_id = leg.sl_order_id
            logger.error(
                f"⚠️ [{leg.key}] Broker-side SL-L order {dead_id} was {status} by the "
                f"broker — leg was UNPROTECTED. Re-placing SL-L order."
            )
            await send_async(
                f"⚠️ *{leg.key} — Broker-side SL-L order {status}!*\n"
                f"Order {dead_id} was {status} — this leg had NO stop protection until now.\n"
                f"Re-placing SL-L @ ₹{leg.sl_price:.2f} (cap ₹{leg.sl_limit_price:.2f})."
            )
            leg.sl_order_id = None
            await self._place_sl_order(leg)
            return False

        # status == "pending": SL-L is still resting. If LTP has already run
        # past our cap, a passive limit sitting at that cap is very unlikely
        # to fill from here — waiting risks leaving the leg exposed
        # indefinitely in a one-directional move. Escalate to a market exit
        # via the existing _close_leg path (cancels the resting order, then
        # places a MARKET close) rather than trusting the limit further.
        ltp = leg.bars.ltp
        if ltp > 0 and leg.sl_limit_price and ltp >= leg.sl_limit_price:
            logger.warning(
                f"🚨 [{leg.key}] SL-L cap breached — LTP ₹{ltp:.2f} ≥ cap ₹{leg.sl_limit_price:.2f} "
                f"with order still pending. Escalating to market exit."
            )
            await send_async(
                f"🚨 *{leg.key} — SL-L cap breached, escalating to market*\n"
                f"LTP ₹{ltp:.2f} ran past cap ₹{leg.sl_limit_price:.2f} while the SL-L order "
                f"(trigger ₹{leg.sl_price:.2f}) sat unfilled. Closing at market."
            )
            await self._close_leg(leg, ltp, f"SL +{SL_PTS:.0f}pts (SL-L cap breach, market escalation)")
            return True
        return False

    # ── Close a leg ───────────────────────────────────────────────────────────

    async def _close_leg(
        self, leg: LegState, exit_price: float, reason: str, *, already_filled_order_id: str | None = None
    ) -> None:
        if leg.status != LegState.ACTIVE:
            return
        leg.status = LegState.CLOSED   # mark immediately to prevent double-close

        cfg = leg.cfg
        qty = leg.qty

        # Cancel the resting broker-side SL-L order before sending our own
        # close order (SMA exit / EOD / app-side fallback / cap-breach
        # escalation), so we never have two live exit orders open on the same
        # leg at once. Skip when this close *is* that SL-L order having
        # filled — nothing to cancel.
        if leg.sl_order_id and leg.sl_order_id != already_filled_order_id:
            await asyncio.to_thread(_cancel_order, self.client, leg.sl_order_id, leg.symbol, cfg["opt_exchange"])

        if already_filled_order_id:
            exit_fill = exit_price
        else:
            try:
                res = self.client.placeorder(
                    strategy   = STRATEGY_NAME,
                    symbol     = leg.symbol,
                    action     = "BUY",
                    exchange   = cfg["opt_exchange"],
                    price_type = "MARKET",
                    product    = "MIS",
                    quantity   = str(qty),
                )
            except Exception as e:
                logger.error(f"  [{leg.key}] Close order exception: {e}")
                res = {"status": "error"}

            order_ok = res.get("status") == "success"
            if not order_ok:
                logger.warning(f"  [{leg.key}] Close order non-success (auto-squareoff?): {res}")

            # Resolve the actual exit fill (falls back to the LTP that triggered the exit
            # decision if the order lookup fails or the order never actually executed).
            exit_fill = _resolve_fill(res, exit_price)
        logger.info(f"  [{leg.key}] Exit fill=₹{exit_fill:.2f} vs LTP=₹{exit_price:.2f}")

        gross  = (leg.fill_price - exit_fill) * qty
        won    = gross > 0
        emoji  = "🟢" if won else "🔴"
        leg.gross_pnl   = gross
        leg.exit_reason = reason

        logger.info(
            f"{emoji} [{leg.key}] CLOSED {leg.symbol} @ ₹{exit_fill:.2f}  "
            f"reason={reason}  gross=₹{gross:,.0f}"
        )
        await send_async(
            f"{emoji} *BNF BB Opening Candle — EXIT ({reason})*\n"
            f"Leg    : {leg.key}  |  {leg.symbol}\n"
            f"Entry  : ₹{leg.fill_price:.2f}  →  Exit: ₹{exit_fill:.2f}\n"
            f"Gross P&L : ₹{gross:,.0f}  ({N_LOTS} lots)\n"
            f"_Closed at {datetime.now().strftime('%H:%M:%S')}_"
        )
        log_trade_to_db(
            bot_name      = "banknifty_bb_opening_candle_bot",
            instrument    = leg.cfg["index_sym"],
            option_symbol = leg.symbol,
            option_type   = leg.cfg["opt_type"],
            entry_time    = leg.entry_time,
            exit_time     = datetime.now(),
            entry_premium = leg.fill_price,
            exit_premium  = exit_fill,
            exit_reason   = reason,
            quantity      = qty,
            lots          = N_LOTS,
            lot_size      = leg.lot_size,
            gross_pnl     = gross,
            order_id      = leg.order_id,
        )

    # ── EOD close ─────────────────────────────────────────────────────────────

    async def _eod_close_all(self) -> None:
        if self.eod_done:
            return
        self.eod_done = True
        logger.info("🔔 15:14 IST — EOD close.")
        for leg in self.legs.values():
            if leg.status == LegState.ACTIVE:
                ltp = await asyncio.to_thread(
                    get_option_ltp, leg.symbol, leg.cfg["opt_exchange"], API_KEY
                )
                exit_p = ltp if ltp > 0 else leg.fill_price
                await self._close_leg(leg, exit_p, "EOD 15:14")
            elif leg.status == LegState.LIMIT_PLACED and leg.order_id:
                # Cancel any pending limit order at EOD
                await asyncio.to_thread(
                    _cancel_order, self.client, leg.order_id, leg.symbol, leg.cfg["opt_exchange"]
                )
                leg.status = LegState.CHECKED

    # ── Decision-state logging ────────────────────────────────────────────────

    def _leg_verdict(self, leg: LegState) -> str:
        """What's currently happening for this leg — checked in the same order
        leg.status is actually driven by _check_signal_at_915 / _place_limit_order
        / _check_limit_fill / _check_sma_exit / _check_sl / _close_leg."""
        if self.skip_day:
            return f"BLOCKED: day skip ({self.skip_reason})"
        if leg.status == LegState.WARMUP:
            return f"warming up: {len(leg.bars.bars)}/{MIN_BARS_NEEDED} bars"
        if leg.status == LegState.SKIP_DAY:
            return "done for today: leg skipped (no expiry/symbol, or VIX filter)"
        if leg.status == LegState.READY:
            return "READY — waiting for 09:15 bar close to check BB signal"
        if leg.status == LegState.CHECKED:
            return "done for today: no signal, or limit not filled at 09:16"
        if leg.status == LegState.LIMIT_PLACED:
            mp = f"₹{leg.midpoint:.2f}" if leg.midpoint is not None else "?"
            return f"LIMIT_PLACED @ {mp} — waiting for 09:16 fill check"
        if leg.status == LegState.ACTIVE:
            return (f"ACTIVE: holding {leg.symbol} entry=₹{leg.fill_price:.2f} "
                    f"SL=₹{leg.sl_price:.2f} (target=evolving SMA)")
        if leg.status == LegState.CLOSED:
            return f"done for today: closed ({leg.exit_reason})"
        return f"unknown status: {leg.status}"

    def _verdict(self, now_t: dt_time) -> str:
        """Overall summary across all three legs — mirrors the bot's own
        per-leg state structure since each leg gates independently."""
        if not self.session_started:
            return "waiting for session start"
        if self.skip_day:
            return f"BLOCKED: day skip ({self.skip_reason})"
        return " | ".join(f"{k}: {self._leg_verdict(leg)}" for k, leg in self.legs.items())

    def _heartbeat_text(self) -> str:
        now_t = datetime.now().time()
        lines = [
            f"💓 DECISION STATE {datetime.now().strftime('%H:%M:%S')} ─ {self._verdict(now_t)}",
            f"    BANKNIFTY={self.bnf_ltp:.1f}  SENSEX={self.sensex_ltp:.1f}  "
            f"VIX={self.vix_ltp:.2f}  ADX={self.daily_adx if self.daily_adx is not None else 'N/A'}",
        ]
        for key, leg in self.legs.items():
            lines.append(f"    [{key}] {self._leg_verdict(leg)}")
        return "\n".join(lines)

    # ── Tick routing ──────────────────────────────────────────────────────────

    async def _on_tick(self, sym: str, ltp: float, ts: datetime) -> None:
        # Index ticks
        if sym in ("BANKNIFTY", "NSE_INDEX:BANKNIFTY"):
            self.bnf_ltp = ltp
            # Throttled to HEARTBEAT_SECS internally — cheap to call on every tick
            self._dlog.maybe_heartbeat(self._heartbeat_text)
            if not self.pre_market_done and ts.time() >= PRE_MARKET:
                self.pre_market_done = True
                await self._pre_market_init(ltp)
            # Re-attempt legs whose symbol resolution failed (SKIP_DAY with no
            # symbol = OpenAlgo was unreachable, not a strategy skip). Retry
            # every 120s until 09:45 so a transient outage at pre-market init
            # can't bench the legs for the day (incident 2026-07-07).
            elif (self.pre_market_done and not self.skip_day
                  and ts.time() < dt_time(9, 45)
                  and datetime.now() >= self._next_init_retry
                  and any(l.status == LegState.SKIP_DAY and l.symbol is None
                          for l in self.legs.values())):
                self._next_init_retry = datetime.now() + timedelta(seconds=120)
                logger.warning("⚠️ Re-attempting init for unresolved legs (transient outage?).")
                await self._pre_market_init(ltp)
            if not self.session_started and ts.time() >= MARKET_OPEN:
                self.session_started = True
                await self._session_open(ltp)
            return

        if sym in ("SENSEX", "BSE_INDEX:SENSEX"):
            self.sensex_ltp = ltp
            return

        if sym in ("INDIAVIX", "NSE_INDEX:INDIAVIX"):
            self.vix_ltp = ltp
            return

        # Option ticks — route to correct leg
        leg_key = self._sym_to_leg.get(sym)
        if not leg_key:
            return
        leg = self.legs[leg_key]

        # SL check at tick level
        if leg.status == LegState.ACTIVE:
            await self._check_sl(leg, ltp)

        # Bar close logic
        bar_closed = leg.bars.update(ltp, ts)
        if not bar_closed:
            return

        # Always logged on every completed bar, regardless of leg status, so
        # the log explains "why not" (skip/warmup/checked) not just "why yes".
        self._dlog.log_bar({
            "leg":        leg.key,
            "symbol":     leg.symbol,
            "status":     leg.status,
            "skip_day":   self.skip_day,
            "skip_reason": self.skip_reason,
            "vix_ltp":    round(self.vix_ltp, 2),
            "daily_adx":  round(self.daily_adx, 1) if self.daily_adx is not None else None,
            "bb":         leg.bars.compute_bb(),
            "fill_price": leg.fill_price if leg.status in (LegState.ACTIVE, LegState.CLOSED) else None,
            "sl_price":   leg.sl_price if leg.status in (LegState.ACTIVE, LegState.CLOSED) else None,
            "verdict":    self._leg_verdict(leg),
        })

        prev_bucket = leg.bars._prev_bucket
        if prev_bucket == SIGNAL_MIN:
            # 09:15 bar just closed → check signal
            await self._check_signal_at_915(leg)
        elif prev_bucket == SIGNAL_MIN + 1:
            # 09:16 bar just closed → check fill
            await self._check_limit_fill(leg)
        elif leg.status == LegState.ACTIVE:
            # Subsequent bars → reconcile the resting SL-L order first (it may
            # have already closed the position at the broker, or need cap-breach
            # escalation); only check the SMA target if the leg is still open.
            if leg.sl_order_id and await self._check_sl_order_filled(leg):
                pass
            else:
                await self._check_sma_exit(leg)

    # ── WebSocket helpers ─────────────────────────────────────────────────────

    async def _subscribe(self, symbol: str, exchange: str, mode: int = 2) -> None:
        if self.ws and symbol not in self._subscribed:
            try:
                await self.ws.send(json.dumps({
                    "action":   "subscribe",
                    "symbol":   symbol,
                    "exchange": exchange,
                    "mode":     mode,
                }))
                self._subscribed.add(symbol)
                logger.info(f"  📡 Subscribed: {symbol} ({exchange})")
            except Exception as e:
                logger.warning(f"  Subscribe error {symbol}: {e}")

    async def _resubscribe_all(self) -> None:
        self._subscribed.clear()
        await self._subscribe("BANKNIFTY", "NSE_INDEX")
        await self._subscribe("SENSEX",    "BSE_INDEX")
        await self._subscribe("INDIAVIX",  "NSE_INDEX")
        for leg in self.legs.values():
            if leg.symbol:
                await self._subscribe(leg.symbol, leg.cfg["opt_exchange"])

    # ── State dump loop ───────────────────────────────────────────────────────

    async def _state_dump_loop(self) -> None:
        while True:
            try:
                STATE_FILE.write_text(json.dumps({
                    "strategy":    STRATEGY_NAME,
                    "last_update": datetime.now().isoformat(),
                    "skip_day":    self.skip_day,
                    "skip_reason": self.skip_reason,
                    "daily_adx":   round(self.daily_adx, 1) if self.daily_adx else None,
                    "vix_ltp":     round(self.vix_ltp, 2),
                    "bnf_ltp":     round(self.bnf_ltp, 2),
                    "sensex_ltp":  round(self.sensex_ltp, 2),
                    "legs":        {k: v.to_dict() for k, v in self.legs.items()},
                }, default=str))
            except Exception:
                pass
            await asyncio.sleep(2)

    # ── Fill-check watchdog ──────────────────────────────────────────────────
    # Wall-clock fallback for _check_limit_fill(): the normal path only runs
    # when a live tick closes the 09:16 bar (see _on_tick). If a leg's symbol
    # prints no tick in that window, the bar never closes and the fill check
    # never fires, leaving a resting limit order unmonitored until EOD. This
    # loop force-runs the (idempotent) check for any leg still LIMIT_PLACED
    # past FILL_CHECK_DEADLINE, independent of tick arrival.

    async def _fill_check_watchdog_loop(self) -> None:
        while True:
            try:
                if datetime.now().time() >= FILL_CHECK_DEADLINE:
                    for leg in self.legs.values():
                        if leg.status == LegState.LIMIT_PLACED:
                            logger.warning(
                                f"[{leg.key}] fill-check watchdog: still LIMIT_PLACED past "
                                f"{FILL_CHECK_DEADLINE} with no bar-close tick — forcing check."
                            )
                            await self._check_limit_fill(leg)
            except Exception:
                logger.exception("fill-check watchdog error")
            await asyncio.sleep(10)

    # ── Pre-market watchdog ───────────────────────────────────────────────────
    # Wall-clock fallback for _pre_market_init(): the normal path only runs
    # from inside _on_tick, gated on a live BANKNIFTY index tick landing with
    # ts.time() >= PRE_MARKET. If the index feed drops or subscribes late
    # right around 09:10 (same class as the WS proxy subscription-replay bug),
    # the intended 5-min safety buffer before the 09:15 signal can collapse to
    # seconds — confirmed near-miss 2026-07-30 (init didn't fire until
    # 09:15:14). This loop force-runs pre-market init on a wall-clock timer,
    # independent of tick arrival, using a REST quote as the spot fallback
    # when no live tick has come in yet.
    async def _pre_market_watchdog_loop(self) -> None:
        while True:
            try:
                if not self.pre_market_done and datetime.now().time() >= PRE_MARKET:
                    self.pre_market_done = True
                    spot = self.bnf_ltp
                    if not spot:
                        logger.warning(
                            "⚠️ Pre-market watchdog: no live BANKNIFTY tick yet at "
                            f"{PRE_MARKET} — falling back to REST quote."
                        )
                        spot = await asyncio.to_thread(get_quote, API_KEY, "BANKNIFTY", "NSE_INDEX")
                        if not spot:
                            logger.error(
                                "❌ Pre-market watchdog: REST quote fallback also failed — "
                                "pre-market init will retry via the unresolved-legs branch."
                            )
                            self.pre_market_done = False
                            await asyncio.sleep(5)
                            continue
                    logger.warning(
                        f"⏰ Pre-market watchdog: firing _pre_market_init(spot={spot:.0f}) "
                        f"on wall clock (no tick had triggered it yet)."
                    )
                    await self._pre_market_init(spot)
            except Exception:
                logger.exception("pre-market watchdog error")
            await asyncio.sleep(5)

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def main_loop(self) -> None:
        if datetime.now().time() >= MARKET_OPEN:
            self.session_started = True
            await self._session_open(self.bnf_ltp)

        asyncio.create_task(self._state_dump_loop())
        asyncio.create_task(self._watchdog.watch_loop())
        asyncio.create_task(self._fill_check_watchdog_loop())
        asyncio.create_task(self._pre_market_watchdog_loop())
        retry_delay = 5

        while True:
            if datetime.now().time() >= SESSION_END:
                await self._eod_close_all()
                logger.info("✅ Past 15:14 IST — shutting down.")
                break

            try:
                logger.info(f"🔌 Connecting to WebSocket: {WS_URL}")
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60) as ws:
                    self.ws = ws
                    await ws.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))
                    await self._resubscribe_all()
                    retry_delay = 5

                    async for raw in ws:
                        if datetime.now().time() >= SESSION_END:
                            await self._eod_close_all()
                            return

                        msg = json.loads(raw)
                        if msg.get("type") != "market_data":
                            continue

                        sym   = msg.get("symbol", "")
                        self._watchdog.on_tick(sym)
                        mdata = msg.get("data", {})
                        ltp   = float(mdata.get("ltp", 0) or mdata.get("lp", 0))
                        ts_r  = mdata.get("t")
                        ts    = datetime.fromtimestamp(float(ts_r)) if ts_r else datetime.now()

                        if ltp <= 0:
                            continue

                        await self._on_tick(sym, ltp, ts)

            except Exception as e:
                logger.warning(f"WebSocket error: {e}. Reconnecting in {retry_delay}s…")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _acquire_pid_lock()
    bot = BNFBBOpeningCandleBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("Stop signal received — bot terminated.")
