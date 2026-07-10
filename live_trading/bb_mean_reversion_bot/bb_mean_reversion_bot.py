"""
BB Mean Reversion Bot
=====================
live_trading/bb_mean_reversion_bot/bb_mean_reversion_bot.py

Research study: options_data/research/bb_mean_reversion_candle_study/
  IS  Sharpe : +1.008  (BANKNIFTY 1/5 SHORT RR4.0, trend-filtered)
  OOS Sharpe : +2.425  |  Win rate ~28–30%  |  MC stability 97.3%
  Bootstrap  : +1.51 median Sharpe  |  All pipeline stages PASS

Strategy:
  Watch 1-minute BANKNIFTY INDEX candles.  When a red candle (close < open)
  pierces the upper Bollinger Band (20, 2σ), the spike has been rejected —
  a mean-reversion signal.  BUY an ATM monthly PE expecting the index to
  revert toward the BB midband.

  This is a DEBIT trade (BUY PE) — NOT selling CE.
  Maximum loss per trade = option premium paid.  No margin required.

Five gates — ALL must pass before entry:
  1. TREND  : Previous-day BANKNIFTY close ≤ 20-day SMA of daily closes
               (Bear/Sideways regime only; silent on Bull days)
  2. TRIGGER: 1-min bar is red AND high > upper BB(20, 2σ)
  3. HTF    : trigger_close < open of the containing 5-min candle
  4. NAT_RR : (trigger_close − bb_mid) / (trigger_high − trigger_close) ≥ 1.25
               (sweet spot filter; removes low-quality 0–50 pt zone)
  5. DTE    : Monthly expiry DTE is NOT in the 8–14 day band
               (8–14 DTE is a consistent loser from Stage 9 analysis)

Entry:
  BUY ATM PE at MARKET immediately after trigger bar closes.
  Entry price = live PE LTP at trigger bar close time.

Stop loss:
  Primary: BANKNIFTY spot tick ≥ trigger_high  → SELL PE at MARKET
  Secondary: PE LTP ≤ sl_opt_price              → SELL PE at MARKET
  (dual-track monitoring — index SL is faster, option price SL is belt-and-suspenders)

Target:
  PE LTP ≥ tp_opt_price  where  tp_opt = entry_opt + 4.0 × risk_opt
  risk_opt  = risk_index × ATM_DELTA_APPROX  (0.50 for ATM PE)
  risk_index = trigger_high − trigger_close

EOD exit: unconditional SELL at 15:15 IST.

Position sizing:
  Paper  : 1 lot (30 units)
  Live   : 6 lots (after Stage 11 gate — explicit sign-off required)

Shared utilities:
  live_trading.api_utils             — get_expiry_dates, get_option_symbol, get_history
  live_trading.shared.atm_resolver   — get_option_ltp
  live_trading.shared.telegram_notifier — send_async
  live_trading.shared.trade_logger   — log_trade_to_db
  openalgo.api                       — placesmartorder (entry), placeorder (exit)
"""

import asyncio
import json
import logging
import os
import sys
from collections import deque
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import websockets
from dotenv import load_dotenv
from openalgo import api

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils                import (
    get_expiry_dates, get_option_symbol, get_history, is_market_holiday
)
from live_trading.shared.atm_resolver      import get_option_ltp
from live_trading.shared.order_fill        import fetch_fill_price
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger      import log_trade_to_db

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "bb_mean_reversion_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER",   "http://127.0.0.1:5001")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")

if not API_KEY:
    raise RuntimeError(
        "OPENALGO_API_KEY not set in .env. "
        "Add it to fyers_crk/openalgo/.env and restart."
    )

PAPER_MODE = os.getenv("BB_MEAN_REVERSION_PAPER_MODE", "true").lower() != "false"

# ── Strategy constants ────────────────────────────────────────────────────────
STRATEGY_NAME    = "BB_MEAN_REVERSION"

IDX_SYMBOL       = "BANKNIFTY"
IDX_EXCHANGE     = "NSE_INDEX"
OPT_EXCHANGE     = "NFO"
STRIKE_STEP      = 100           # BANKNIFTY ATM rounding step

# Bollinger Band parameters (research champion)
BB_PERIOD        = 20
BB_STD_MULT      = 2.0

# Trend filter: 20-day SMA of daily closes (Gate 1)
TREND_SMA_DAYS   = 20

# Natural R:R gate threshold (Gate 4 — distance analysis sweet spot)
NAT_RR_THRESHOLD = 1.25

# DTE exclusion band (Gate 5 — Stage 9 finding: 8–14 DTE consistently loses)
DTE_EXCL_LOW     = 8
DTE_EXCL_HIGH    = 14

# Expiry selection for BANKNIFTY monthly
MIN_DTE          = 7
MAX_DTE          = 45

# ATM delta approximation for risk translation.
# A true ATM PE has delta ≈ −0.50; we use 0.50 (abs).
# This is a conservative approximation — actual delta may be 0.45–0.55.
ATM_DELTA_APPROX = 0.50

# Risk-reward target (research champion: 4.0×)
RR_TARGET        = 4.0

# Minimum viable PE premium at entry
MIN_PREMIUM      = 10.0

# Position sizing
DEFAULT_LOT_SIZE = 30            # BANKNIFTY current lot size (NSE-mandated)
N_LOTS_PAPER     = 6             # paper trading — 6 lots (research-validated quantity)
N_LOTS_LIVE      = 6             # live — same quantity, flip PAPER_MODE to activate

# Session timing
MARKET_OPEN      = dt_time(9,  15)
ENTRY_START      = dt_time(9,  30)
ENTRY_END        = dt_time(14, 45)
SESSION_END      = dt_time(15, 14)   # sandbox auto-squareoff cutoff is 15:15; exit 1 min early

# Warm-up: pre-load 1-min history to initialise BB(20)
WARMUP_DAYS      = 3
MIN_BARS_FOR_BB  = BB_PERIOD + 5   # 25 bars minimum before first signal check

# State file for dashboard heartbeat
STATE_FILE = LOGS_DIR / "bb_mean_reversion_state.json"


# ── Helper: lot size from DB (falls back to DEFAULT_LOT_SIZE) ────────────────
def _get_lot_size(symbol: str) -> int:
    try:
        from database.token_db import get_symbol_info
        si = get_symbol_info(symbol, OPT_EXCHANGE)
        if si and getattr(si, "lotsize", None):
            ls = int(si.lotsize)
            logger.info(f"  Lot size from DB: {ls}  ({symbol})")
            return ls
    except Exception as e:
        logger.warning(f"  Lot size DB lookup failed: {e}. Using {DEFAULT_LOT_SIZE}.")
    return DEFAULT_LOT_SIZE


# ── Helper: trend filter (Gate 1) ────────────────────────────────────────────
def _check_trend_gate() -> bool:
    """
    Return True if BANKNIFTY is in Bear/Sideways regime (SHORT signals permitted).
    Logic: previous day close ≤ 20-day SMA of daily closes.

    Fail behaviour:
      - API/network errors           → FAIL-CLOSED (return False, no trades).
        A transient error should not bypass a regime filter that exists
        specifically to block bull-trend entries.
      - Insufficient bars returned   → FAIL-CLOSED (same reason).
      - Missing close column         → FAIL-CLOSED.
    """
    try:
        # "D" is the correct daily interval for the OpenAlgo history schema.
        # "1d" is NOT a valid interval and returns a 400 BAD REQUEST.
        # 60 calendar days ≈ 40+ trading bars — 30 was marginal (19-21 bars
        # depending on holidays) and made the gate fail closed spuriously.
        raw = get_history(API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "D", 60)
        if not raw or len(raw) < TREND_SMA_DAYS + 1:
            logger.warning(
                f"  Gate 1 (Trend): insufficient daily history "
                f"({len(raw) if raw else 0} bars, need {TREND_SMA_DAYS + 1}). "
                f"Fail-CLOSED — blocking entries."
            )
            return False

        df = pd.DataFrame(raw)
        # Normalise column names
        close_col = next(
            (c for c in ["close", "c", "Close"] if c in df.columns), None
        )
        if close_col is None:
            logger.warning("  Gate 1 (Trend): no close column found. Fail-CLOSED.")
            return False

        df[close_col] = pd.to_numeric(df[close_col], errors="coerce")
        df = df.dropna(subset=[close_col]).tail(TREND_SMA_DAYS + 1)

        closes  = df[close_col].values
        sma20   = float(np.mean(closes[:-1]))   # SMA of the 20 days BEFORE today
        prev_cl = float(closes[-2])             # yesterday's close (second-to-last)

        is_bear_sideways = prev_cl <= sma20
        regime = "Bear/Sideways ✅" if is_bear_sideways else "Bull ⛔"
        logger.info(
            f"  Gate 1 (Trend): prev_close={prev_cl:.0f}  SMA20={sma20:.0f}  "
            f"regime={regime}"
        )
        return is_bear_sideways

    except Exception as e:
        logger.error(f"  Gate 1 (Trend): error fetching history — {e}. Fail-CLOSED.")
        return False


# ── Helper: monthly expiry selection ─────────────────────────────────────────
def _get_monthly_expiry() -> str | None:
    """Nearest BANKNIFTY monthly expiry with DTE in [MIN_DTE, MAX_DTE]."""
    dates = get_expiry_dates(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, "options")
    if not dates:
        logger.warning("  Could not fetch BANKNIFTY expiry dates.")
        return None

    today = datetime.now().date()
    valid = []
    for d in dates:
        try:
            exp_dt = datetime.strptime(d, "%d%b%y").date()
            dte = (exp_dt - today).days
            if MIN_DTE <= dte <= MAX_DTE:
                valid.append((dte, d, exp_dt))
        except ValueError:
            continue

    if not valid:
        logger.warning(
            f"  No BANKNIFTY monthly expiry with DTE {MIN_DTE}–{MAX_DTE}. "
            f"Available: {dates[:6]}"
        )
        return None

    valid.sort()
    dte, expiry_str, _ = valid[0]
    logger.info(f"  Monthly expiry: {expiry_str}  (DTE={dte})")
    return expiry_str


# ── Helper: DTE exclusion check (Gate 5) ─────────────────────────────────────
def _dte_ok(expiry_str: str) -> bool:
    """
    Return True if it is safe to trade this expiry.
    Excludes the 8–14 DTE band (Stage 9 finding: consistently loss-making).
    """
    try:
        exp_dt = datetime.strptime(expiry_str, "%d%b%y").date()
        dte = (exp_dt - datetime.now().date()).days
        if DTE_EXCL_LOW <= dte <= DTE_EXCL_HIGH:
            logger.info(
                f"  Gate 5 (DTE): DTE={dte} is in exclusion band "
                f"[{DTE_EXCL_LOW}–{DTE_EXCL_HIGH}]. Skipping today."
            )
            return False
        logger.info(f"  Gate 5 (DTE): DTE={dte} ✅")
        return True
    except Exception as e:
        logger.warning(f"  Gate 5 (DTE): parse error {e}. Allowing (fail-open).")
        return True


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback


# ══════════════════════════════════════════════════════════════════════════════
class IndexBarBuilder:
# ══════════════════════════════════════════════════════════════════════════════
    """
    Builds 1-minute OHLC bars from live BANKNIFTY index ticks.
    Also tracks the current 5-minute candle open for HTF gate (Gate 3).

    All bars are in INDEX PRICE terms (not option premium).
    BB(20, 2σ) is computed on the 1-min index close prices.
    """

    def __init__(self):
        self.bars: deque = deque(maxlen=200)   # completed 1-min bars

        # Current (incomplete) 1-min bar
        self._bucket: int   = -1
        self._open:   float = 0.0
        self._high:   float = 0.0
        self._low:    float = float("inf")
        self._last:   float = 0.0

        # 5-min HTF tracking
        self._htf_bucket: int   = -1           # 5-min bucket id
        self._htf_open:   float = 0.0          # open of the current 5-min bar

        # Last completed bar (for signal check)
        self.last_completed: dict | None = None

    # ── Tick ingestion ────────────────────────────────────────────────────────

    def update(self, ltp: float, ts: datetime) -> bool:
        """
        Feed a live tick.  Returns True when a 1-min bar completes
        (signal check should run at that point).
        """
        bucket_1m = ts.hour * 60 + ts.minute
        bucket_5m = ts.hour * 100 + (ts.minute // 5) * 5   # unique per 5-min slot

        # ── 5-min HTF tracking ────────────────────────────────────────────────
        if self._htf_bucket == -1:
            self._htf_bucket = bucket_5m
            self._htf_open   = ltp
        elif bucket_5m != self._htf_bucket:
            self._htf_bucket = bucket_5m
            self._htf_open   = ltp   # new 5-min bar opens at this tick's price

        # ── 1-min bar tracking ────────────────────────────────────────────────
        if self._bucket == -1:
            self._bucket = bucket_1m
            self._open = self._high = self._last = ltp
            self._low = ltp
            return False

        if bucket_1m != self._bucket:
            # Bar completed
            if self._open > 0:
                bar = {
                    "open":  self._open,
                    "high":  self._high,
                    "low":   self._low,
                    "close": self._last,
                }
                self.bars.append(bar)
                self.last_completed = bar
            # Start new bar
            self._bucket = bucket_1m
            self._open = self._high = self._last = ltp
            self._low = ltp
            return True

        self._high = max(self._high, ltp)
        self._low  = min(self._low,  ltp)
        self._last = ltp
        return False

    # ── Warm-up ───────────────────────────────────────────────────────────────

    def warmup_from_history(self, raw_bars: list[dict]) -> None:
        """Pre-load 1-min history bars (prior-day data) to seed BB(20)."""
        for b in raw_bars[-190:]:
            self.bars.append({
                "open":  float(b.get("open",  0)),
                "high":  float(b.get("high",  0)),
                "low":   float(b.get("low",   0)),
                "close": float(b.get("close", 0)),
            })
        if self.bars:
            logger.info(
                f"  IndexBarBuilder warm-up: {len(self.bars)} bars loaded. "
                f"Last close: {self.bars[-1]['close']:.0f}"
            )

    # ── BB computation ────────────────────────────────────────────────────────

    def compute_bb(self) -> dict | None:
        """
        Compute BB(20, 2σ) on completed 1-min INDEX price closes.
        Returns None if not enough bars.
        """
        if len(self.bars) < MIN_BARS_FOR_BB:
            return None

        closes = pd.Series([b["close"] for b in self.bars])
        sma    = closes.rolling(BB_PERIOD).mean().iloc[-1]
        std    = closes.rolling(BB_PERIOD).std(ddof=1).iloc[-1]

        if pd.isna(sma) or pd.isna(std) or std == 0:
            return None

        upper  = sma + BB_STD_MULT * std
        lower  = sma - BB_STD_MULT * std
        last_b = self.bars[-1]

        return {
            "upper":  round(float(upper), 2),
            "sma":    round(float(sma),   2),
            "lower":  round(float(lower),  2),
            "close":  round(float(last_b["close"]), 2),
            "open":   round(float(last_b["open"]),  2),
            "high":   round(float(last_b["high"]),  2),
            "low":    round(float(last_b["low"]),   2),
            "n_bars": len(self.bars),
        }

    # ── HTF open ──────────────────────────────────────────────────────────────

    @property
    def htf_open(self) -> float:
        """Open price of the current 5-min candle that contains the last tick."""
        return self._htf_open


# ══════════════════════════════════════════════════════════════════════════════
class BbMeanReversionBot:
# ══════════════════════════════════════════════════════════════════════════════
    """
    BB Mean Reversion — BUY ATM PE when BANKNIFTY index red candle pierces
    upper BB(20, 2σ) with 5 gate checks.
    Research: IS Sharpe 1.008 | OOS Sharpe 2.425 | MC Stability 97.3%
    """

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.ws     = None

        # Live prices
        self.bnf_ltp: float = 0.0
        self.pe_ltp:  float = 0.0

        # Index bar builder (1-min bars + 5-min HTF tracking)
        self.idx_bars = IndexBarBuilder()

        # Session state
        self.expiry:        str | None   = None
        self.pe_symbol:     str | None   = None
        self.lot_size:      int          = DEFAULT_LOT_SIZE
        self.n_lots:        int          = N_LOTS_PAPER
        self.trend_ok:      bool         = False    # Gate 1 result
        self.dte_ok:        bool         = False    # Gate 5 result
        self.signal_fired:  bool         = False    # True once signal taken/passed today
        self.active_trade:  dict | None  = None
        self._is_closing:   bool         = False
        self._session_started  = False
        self._eod_exit_done    = False
        self._first_connect    = True
        self._subscribed_syms: set[str]  = set()

        # Dashboard snapshot
        self.bb_snapshot: dict = {}

        # Restore any active trade from a previous restart today
        self._restore_state()

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE RESTORE
    # ══════════════════════════════════════════════════════════════════════════

    def _restore_state(self) -> None:
        if not STATE_FILE.exists():
            return
        try:
            state = json.loads(STATE_FILE.read_text())
            last_update_str = state.get("last_update", "")
            if not last_update_str:
                return
            last_update = datetime.fromisoformat(last_update_str)
            if last_update.date() != datetime.now().date():
                logger.info("_restore_state: state file from a previous day — skipping.")
                return

            phase = state.get("phase", "")
            self.pe_symbol = state.get("pe_symbol")
            self.expiry    = state.get("expiry")

            if phase == "IN_TRADE":
                at = state.get("active_trade")
                if at:
                    self.active_trade = at
                    self.signal_fired = True
                    logger.warning(
                        f"🔄 Restored IN_TRADE from state: {at.get('symbol')}  "
                        f"entry=₹{at.get('entry_opt'):.2f}  SL=₹{at.get('sl_opt'):.2f}  "
                        f"target=₹{at.get('tp_opt'):.2f}  index SL={at.get('sl_index'):.0f}"
                    )
            elif phase in ("COMPLETED", "INACTIVE_TREND", "INACTIVE_DTE"):
                self.signal_fired = True
                logger.info(f"🔄 Restored {phase} — signal_fired=True, no new entries today.")
        except Exception as e:
            logger.warning(f"_restore_state: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  SESSION INITIALISATION
    # ══════════════════════════════════════════════════════════════════════════

    async def _session_open(self, bnf_spot: float) -> None:
        """Run all pre-session checks and warm up bar history."""
        if is_market_holiday(API_KEY):
            logger.info("⛔ Market holiday — bot idle today.")
            return

        logger.info(f"🔔 Session open. BANKNIFTY spot ≈ {bnf_spot:.0f}")

        # ── Gate 1: Trend filter ──────────────────────────────────────────────
        self.trend_ok = await asyncio.to_thread(_check_trend_gate)
        if not self.trend_ok:
            logger.info(
                "⛔ Gate 1 (Trend): BANKNIFTY is in a Bull trend. "
                "No entries today. Bot will monitor but not trade."
            )
            await send_async(
                f"📊 *BB Mean Reversion — Online (INACTIVE)*\n"
                f"BANKNIFTY is in a Bull trend today.\n"
                f"_Gate 1 (Trend filter) inactive — no trades._\n"
                f"Bot will resume on the next Bear/Sideways day."
            )
            return

        # ── Expiry resolution ─────────────────────────────────────────────────
        self.expiry = await asyncio.to_thread(_get_monthly_expiry)
        if not self.expiry:
            logger.warning("  No valid monthly expiry found. No entries today.")
            return

        # ── Gate 5: DTE exclusion ─────────────────────────────────────────────
        self.dte_ok = await asyncio.to_thread(_dte_ok, self.expiry)
        if not self.dte_ok:
            logger.info(
                f"⛔ Gate 5 (DTE): expiry {self.expiry} is in the 8–14 DTE band. "
                f"No trades today."
            )
            await send_async(
                f"📊 *BB Mean Reversion — Online (DTE EXCLUDED)*\n"
                f"Expiry `{self.expiry}` is in the 8–14 DTE exclusion band.\n"
                f"_No trades today (Stage 9 finding: 8–14 DTE consistently loses)._"
            )
            return

        # ── Resolve ATM PE symbol (use spot at session open as approximation) ──
        self.pe_symbol = await asyncio.to_thread(
            get_option_symbol,
            API_KEY, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, "PE", "ATM",
        )
        if not self.pe_symbol:
            logger.warning(
                f"  ATM PE symbol resolution failed "
                f"(spot={bnf_spot:.0f}, expiry={self.expiry}). No entries today."
            )
            return

        # ── Lot size ──────────────────────────────────────────────────────────
        self.lot_size = _get_lot_size(self.pe_symbol)
        self.n_lots   = N_LOTS_PAPER if PAPER_MODE else N_LOTS_LIVE

        logger.info(
            f"  ATM PE: {self.pe_symbol}  |  Expiry: {self.expiry}  "
            f"| Lot: {self.lot_size}  |  Lots: {self.n_lots}  "
            f"| Mode: {'PAPER' if PAPER_MODE else '⚠️ LIVE'}"
        )

        # ── Warm up index bars from history ───────────────────────────────────
        await self._warmup_history()

        # ── Subscribe PE for tick feed ────────────────────────────────────────
        await self._subscribe(self.pe_symbol, OPT_EXCHANGE)

        # ── Resume: check if existing trade exit already triggered ────────────
        await self._check_resume_exit()

        # ── Telegram notification ─────────────────────────────────────────────
        if self._first_connect:
            self._first_connect = False
            await send_async(
                f"🤖 *BB Mean Reversion — Online*\n"
                f"Instrument : BANKNIFTY  |  BUY ATM PE\n"
                f"Expiry     : `{self.expiry}`\n"
                f"PE symbol  : `{self.pe_symbol}`\n"
                f"Signal     : Red candle + high > BB({BB_PERIOD},{BB_STD_MULT}σ)\n"
                f"Gates      : Trend✅ HTF NatRR≥{NAT_RR_THRESHOLD} DTE✅\n"
                f"Target     : {RR_TARGET}×R  |  Mode: {'PAPER' if PAPER_MODE else '⚠️ LIVE'}\n"
                f"Lots       : {self.n_lots} × {self.lot_size} = {self.n_lots * self.lot_size} units\n"
                f"Research   : IS +1.008  OOS +2.425  MC 97.3%"
            )

        logger.info(
            f"  Session ready. Bars={len(self.idx_bars.bars)}  "
            f"BB({BB_PERIOD},{BB_STD_MULT}σ)  "
            f"Entry window: {ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')}"
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  HISTORY WARM-UP
    # ══════════════════════════════════════════════════════════════════════════

    async def _warmup_history(self) -> None:
        """Pre-load 1-min BANKNIFTY INDEX history to seed BB(20)."""
        logger.info(f"  📡 Warming up BANKNIFTY index 1-min bars …")
        try:
            raw = await asyncio.to_thread(
                get_history, API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "1m", WARMUP_DAYS
            )
            if not raw:
                logger.warning("  Warm-up: no history returned. BB will seed from live ticks.")
                return

            df = pd.DataFrame(raw)
            ts_col = next((c for c in ["timestamp", "date", "t"] if c in df.columns), None)
            if ts_col and ts_col == "timestamp":
                df["dt"] = (
                    pd.to_datetime(df[ts_col], unit="s", utc=True)
                    .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
                )
            elif ts_col:
                df["dt"] = pd.to_datetime(df[ts_col])

            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df.get(col, pd.Series(dtype=float)), errors="coerce")
            df = df.dropna(subset=["close"])

            # Exclude today's bars — warm-up is prior-day data only
            today_str = datetime.now().date().isoformat()
            if "dt" in df.columns:
                df = df[df["dt"].dt.date.astype(str) < today_str]

            self.idx_bars.warmup_from_history(df[["open", "high", "low", "close"]].to_dict("records"))
            self.bb_snapshot = self.idx_bars.compute_bb() or {}

        except Exception as e:
            logger.error(f"  Warm-up error: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  RESUME-SAFE EXIT CHECK
    # ══════════════════════════════════════════════════════════════════════════

    async def _check_resume_exit(self) -> None:
        """
        If restarting mid-session with an active trade, check whether the
        SL or target was already triggered. Close immediately if so.
        """
        if not self.active_trade:
            return

        sl_index  = self.active_trade["sl_index"]
        tp_opt    = self.active_trade["tp_opt"]
        sl_opt    = self.active_trade["sl_opt"]
        symbol    = self.active_trade["symbol"]

        logger.info(
            f"  🔄 [RESUME] Active trade: {symbol}  "
            f"entry=₹{self.active_trade['entry_opt']:.2f}  "
            f"SL_index={sl_index:.0f}  SL_opt=₹{sl_opt:.2f}  TP=₹{tp_opt:.2f}"
        )

        # Check index SL via REST
        if self.bnf_ltp >= sl_index and self.bnf_ltp > 0:
            logger.warning(
                f"  🛑 [RESUME] Index SL already breached: "
                f"spot={self.bnf_ltp:.0f} ≥ SL={sl_index:.0f}. Closing now."
            )
            await self._close_trade(self.pe_ltp or sl_opt, "SL-index (resume)")
            return

        # Check option target/SL via REST LTP
        ltp = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        if ltp <= 0:
            logger.warning("  [RESUME] Could not fetch PE LTP. Will monitor via ticks.")
            return

        if ltp >= tp_opt:
            logger.info(f"  🎯 [RESUME] Target already hit: LTP ₹{ltp:.2f} ≥ TP ₹{tp_opt:.2f}.")
            await self._close_trade(ltp, "Target 4R (resume)")
        elif ltp <= sl_opt:
            logger.warning(f"  🛑 [RESUME] Option SL already hit: LTP ₹{ltp:.2f} ≤ SL ₹{sl_opt:.2f}.")
            await self._close_trade(ltp, "SL-option (resume)")
        else:
            logger.info(
                f"  [RESUME] No exit triggered (LTP ₹{ltp:.2f} between "
                f"SL ₹{sl_opt:.2f} and TP ₹{tp_opt:.2f}). Continuing."
            )

    # ══════════════════════════════════════════════════════════════════════════
    #  SIGNAL ENGINE — runs on every completed 1-min INDEX bar
    # ══════════════════════════════════════════════════════════════════════════

    async def _check_signal(self) -> None:
        """
        Evaluate all 5 gates on the just-completed 1-min bar.
        Enters trade if all pass.
        """
        # Already traded or inactive today
        if self.active_trade or self.signal_fired:
            return
        if not self.trend_ok or not self.dte_ok:
            return

        now = datetime.now().time()
        if not (ENTRY_START <= now <= ENTRY_END):
            return

        bb = self.idx_bars.compute_bb()
        if bb is None:
            return

        self.bb_snapshot = bb
        bar = self.idx_bars.last_completed
        if not bar:
            return

        # ── Gate 2: Red candle + high > upper BB ─────────────────────────────
        is_red   = bar["close"] < bar["open"]
        high_ok  = bar["high"] > bb["upper"]
        if not (is_red and high_ok):
            return

        trig_open  = bar["open"]
        trig_close = bar["close"]
        trig_high  = bar["high"]
        bb_mid     = bb["sma"]

        logger.info(
            f"🔔 Trigger candle: open={trig_open:.0f}  close={trig_close:.0f}  "
            f"high={trig_high:.0f}  BB upper={bb['upper']:.0f}  SMA={bb_mid:.0f}"
        )

        # ── Gate 3: HTF — trigger_close < current 5-min candle open ──────────
        htf_open = self.idx_bars.htf_open
        if htf_open <= 0 or trig_close >= htf_open:
            logger.info(
                f"  Gate 3 (HTF) FAIL: trigger_close={trig_close:.0f} "
                f"≥ 5-min open={htf_open:.0f}. Skip."
            )
            self.signal_fired = True   # one chance per session even on gate fail
            return
        logger.info(f"  Gate 3 (HTF) ✅  trigger_close={trig_close:.0f} < 5-min open={htf_open:.0f}")

        # ── Gate 4: Natural R:R ───────────────────────────────────────────────
        risk_index = trig_high - trig_close
        if risk_index <= 0:
            logger.info("  Gate 4 (NatRR): risk_index ≤ 0. Skip.")
            self.signal_fired = True
            return

        dist_to_sma = trig_close - bb_mid
        nat_rr      = dist_to_sma / risk_index if risk_index > 0 else -1

        if nat_rr < NAT_RR_THRESHOLD:
            logger.info(
                f"  Gate 4 (NatRR) FAIL: nat_rr={nat_rr:.2f} < {NAT_RR_THRESHOLD}. "
                f"(dist_to_sma={dist_to_sma:.0f}  risk={risk_index:.0f}) Skip."
            )
            self.signal_fired = True
            return
        logger.info(
            f"  Gate 4 (NatRR) ✅  nat_rr={nat_rr:.2f}  "
            f"(dist_to_sma={dist_to_sma:.0f}  risk_index={risk_index:.0f})"
        )

        # ── All gates passed — enter trade ────────────────────────────────────
        logger.info(
            f"✅ All 5 gates PASS. Entering trade. "
            f"sl_index={trig_high:.0f}  bb_mid={bb_mid:.0f}"
        )
        await self._enter_trade(trig_close, trig_high, risk_index, bb_mid)

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE ENTRY
    # ══════════════════════════════════════════════════════════════════════════

    async def _enter_trade(
        self,
        trigger_close: float,
        trigger_high:  float,
        risk_index:    float,
        bb_mid:        float,
    ) -> None:
        """BUY ATM PE at market. Compute SL and target in option terms."""
        if self.active_trade:
            return

        # Resolve ATM PE at the CURRENT spot (signal bar close ≈ current spot)
        atm_strike = int(round(self.bnf_ltp / STRIKE_STEP) * STRIKE_STEP)
        pe_sym = await asyncio.to_thread(
            get_option_symbol,
            API_KEY, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, "PE", "ATM",
            underlying_ltp=self.bnf_ltp,
        )
        if not pe_sym:
            pe_sym = self.pe_symbol   # fall back to session-open ATM

        if not pe_sym:
            logger.error("  Entry: could not resolve PE symbol. Aborting.")
            self.signal_fired = True
            return

        # Fetch live PE LTP — retry transient quote failures (rate limits)
        # before abandoning what may be the day's only signal
        pe_ltp = 0.0
        for attempt in range(3):
            pe_ltp = await asyncio.to_thread(get_option_ltp, pe_sym, OPT_EXCHANGE, API_KEY)
            if pe_ltp > 0:
                break
            logger.warning(f"  Entry: PE LTP = 0 for {pe_sym} (attempt {attempt + 1}/3)")
            await asyncio.sleep(5)
        if pe_ltp <= 0:
            logger.warning(f"  Entry: PE LTP still 0 for {pe_sym} after 3 attempts. Aborting.")
            self.signal_fired = True
            return

        if pe_ltp < MIN_PREMIUM:
            logger.info(
                f"  Entry: PE LTP ₹{pe_ltp:.2f} < ₹{MIN_PREMIUM} minimum. Aborting."
            )
            self.signal_fired = True
            return

        # ── Risk and target in option terms ──────────────────────────────────
        risk_opt = round(risk_index * ATM_DELTA_APPROX, 2)
        sl_opt   = round(pe_ltp - risk_opt, 2)      # PE falls if index rises to SL
        tp_opt   = round(pe_ltp + RR_TARGET * risk_opt, 2)  # PE rises at target

        qty = self.n_lots * self.lot_size

        logger.info(
            f"  BUY PE: {pe_sym}  LTP=₹{pe_ltp:.2f}  "
            f"qty={qty}  SL_index={trigger_high:.0f}  "
            f"sl_opt=₹{sl_opt:.2f}  tp_opt=₹{tp_opt:.2f}  "
            f"risk_opt=₹{risk_opt:.2f}  RR={RR_TARGET}×"
        )

        # ── Subscribe PE if not already ──────────────────────────────────────
        if pe_sym not in self._subscribed_syms:
            await self._subscribe(pe_sym, OPT_EXCHANGE)
        self.pe_symbol = pe_sym

        # ── Place BUY order ───────────────────────────────────────────────────
        try:
            res = self.client.placesmartorder(
                strategy      = STRATEGY_NAME,
                symbol        = pe_sym,
                action        = "BUY",
                exchange      = OPT_EXCHANGE,
                price_type    = "MARKET",
                product       = "MIS",
                quantity      = qty,
                position_size = qty,    # positive = long
            )
        except Exception as e:
            logger.error(f"  BUY order exception: {e}")
            self.signal_fired = True
            return

        if res.get("status") != "success":
            logger.error(f"  BUY order rejected: {res}")
            self.signal_fired = True
            return

        # Book against the actual fill, not the pre-order LTP snapshot, and
        # recompute the SL/target thresholds from the fill so they reflect
        # the real entry basis.
        entry_opt = _resolve_fill(res, pe_ltp)
        if entry_opt != pe_ltp:
            logger.info(f"  Entry fill=₹{entry_opt:.2f} vs LTP snapshot=₹{pe_ltp:.2f}")
        sl_opt = round(entry_opt - risk_opt, 2)
        tp_opt = round(entry_opt + RR_TARGET * risk_opt, 2)

        self.active_trade = {
            "symbol":        pe_sym,
            "strike":        atm_strike,
            "expiry":        self.expiry,
            "entry_opt":     entry_opt,
            "sl_opt":        sl_opt,
            "tp_opt":        tp_opt,
            "sl_index":      trigger_high,
            "risk_opt":      risk_opt,
            "risk_index":    risk_index,
            "qty":           qty,
            "lot_size":      self.lot_size,
            "n_lots":        self.n_lots,
            "order_id":      str(res.get("orderid", "")),
            "entry_time":    datetime.now().isoformat(),
            "bnf_at_entry":  self.bnf_ltp,
            "bb_mid":        bb_mid,
            "trigger_close": trigger_close,
            "trigger_high":  trigger_high,
            "nat_rr":        round((trigger_close - bb_mid) / risk_index, 3),
        }
        self.signal_fired = True

        logger.info(
            f"✅ BOUGHT {pe_sym} @ ₹{entry_opt:.2f}  "
            f"(qty={qty}, order={res.get('orderid')})"
        )
        await send_async(
            f"📉 *BB Mean Reversion — ENTRY*\n"
            f"Bought `{pe_sym}` ({self.n_lots} lot, qty={qty})\n"
            f"Entry premium  : ₹{entry_opt:.2f}\n"
            f"Stop loss (opt): ₹{sl_opt:.2f}   (index SL: {trigger_high:.0f})\n"
            f"Target (4R)    : ₹{tp_opt:.2f}\n"
            f"Risk (opt)     : ₹{risk_opt:.2f}  (index: {risk_index:.0f} pts)\n"
            f"Natural R:R    : {self.active_trade['nat_rr']:.2f}\n"
            f"BANKNIFTY      : {self.bnf_ltp:.0f}\n"
            f"BB SMA (target): {bb_mid:.0f}\n"
            f"_Signal: {datetime.now().strftime('%H:%M')}_"
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  TICK-LEVEL EXIT CHECKS
    # ══════════════════════════════════════════════════════════════════════════

    async def _on_index_tick(self, ltp: float) -> None:
        """
        Primary SL check on every BANKNIFTY index tick.
        Exits immediately if spot ≥ trigger_high (index stop loss).
        Also triggers session open on first tick.
        """
        self.bnf_ltp = ltp

        if not self._session_started and ltp > 0:
            self._session_started = True
            await self._session_open(ltp)

        if not self.active_trade or self._is_closing:
            return

        sl_index = self.active_trade["sl_index"]
        if ltp >= sl_index:
            logger.warning(
                f"  🛑 INDEX SL: BANKNIFTY {ltp:.0f} ≥ {sl_index:.0f}. "
                f"Closing PE now."
            )
            await self._close_trade(self.pe_ltp or 0, "SL-index")

    async def _on_option_tick(self, ltp: float) -> None:
        """
        Monitor PE LTP for target hit or option-price SL (belt-and-suspenders).
        Called on every PE tick.
        """
        self.pe_ltp = ltp

        if not self.active_trade or self._is_closing:
            return

        tp_opt = self.active_trade["tp_opt"]
        sl_opt = self.active_trade["sl_opt"]

        if ltp >= tp_opt:
            logger.info(
                f"  🎯 TARGET: PE LTP ₹{ltp:.2f} ≥ TP ₹{tp_opt:.2f}. Closing."
            )
            await self._close_trade(ltp, f"Target {RR_TARGET}R")

        elif ltp <= sl_opt:
            logger.warning(
                f"  🛑 OPTION SL: PE LTP ₹{ltp:.2f} ≤ SL ₹{sl_opt:.2f}. Closing."
            )
            await self._close_trade(ltp, "SL-option")

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE EXIT
    # ══════════════════════════════════════════════════════════════════════════

    async def _close_trade(self, exit_opt: float, reason: str) -> None:
        """SELL the PE to close the long position."""
        if not self.active_trade or self._is_closing:
            return
        self._is_closing = True

        symbol = self.active_trade["symbol"]
        qty    = self.active_trade["qty"]

        try:
            # Use placeorder (NOT placesmartorder) for exits — exact qty, no
            # risk of accidentally closing another strategy's position.
            res = self.client.placeorder(
                strategy   = STRATEGY_NAME,
                symbol     = symbol,
                action     = "SELL",
                exchange   = OPT_EXCHANGE,
                price_type = "MARKET",
                product    = "MIS",
                quantity   = str(qty),
            )
        except Exception as e:
            logger.error(f"  SELL order exception: {e}")
            self._is_closing = False
            return

        order_ok = res.get("status") == "success"
        if not order_ok:
            logger.warning(
                f"  SELL non-success (may be auto-squareoff): {res}. "
                f"Logging trade and clearing state."
            )

        # Book P&L against the actual fill, not the LTP that triggered the exit.
        exit_fill = _resolve_fill(res, exit_opt)
        if exit_fill != exit_opt:
            logger.info(f"  Exit fill=₹{exit_fill:.2f} vs LTP trigger=₹{exit_opt:.2f}")
        exit_opt = exit_fill

        # Capture fields before clearing state
        entry_opt   = self.active_trade["entry_opt"]
        entry_time  = self.active_trade.get("entry_time")
        order_id    = self.active_trade.get("order_id")
        n_lots      = self.active_trade.get("n_lots", self.n_lots)
        lot_size    = self.active_trade.get("lot_size", self.lot_size)

        # Net P&L (BUY then SELL — long option)
        gross_pnl  = (exit_opt - entry_opt) * qty
        try:
            sys.path.insert(0, str(ROOT.parent / "options_data"))
            from research.transaction_costs import compute_round_trip_cost
            cost = compute_round_trip_cost(
                entry_premium     = entry_opt,
                exit_premium      = exit_opt,
                lot_size          = lot_size,
                qty               = n_lots,
                exchange          = "NSE",
                side              = "buy",
                brokerage_per_order = 20,
            )
        except Exception:
            cost = 0.0
        net_pnl = gross_pnl - cost

        won    = net_pnl > 0
        emoji  = "🟢" if won else "🔴"
        pct    = (exit_opt - entry_opt) / entry_opt * 100 if entry_opt > 0 else 0

        # Clear state
        self.active_trade = None
        self._is_closing  = False

        logger.info(
            f"{emoji} CLOSED {symbol} @ ₹{exit_opt:.2f}  "
            f"reason={reason}  gross=₹{gross_pnl:,.0f}  cost=₹{cost:,.0f}  "
            f"net=₹{net_pnl:,.0f}  change={pct:+.1f}%"
        )

        # Log to shared DB
        try:
            log_trade_to_db(
                bot_name      = "bb_mean_reversion_bot",
                instrument    = IDX_SYMBOL,
                option_symbol = symbol,
                option_type   = "PE",
                entry_time    = datetime.fromisoformat(entry_time) if entry_time else datetime.now(),
                exit_time     = datetime.now(),
                entry_premium = entry_opt,
                exit_premium  = exit_opt,
                exit_reason   = reason,
                quantity      = qty,
                lots          = n_lots,
                lot_size      = lot_size,
                gross_pnl     = round(gross_pnl, 2),
                net_pnl       = round(net_pnl, 2),
                order_id      = order_id,
            )
        except Exception as e:
            logger.warning(f"  log_trade_to_db failed: {e}")

        await send_async(
            f"{emoji} *BB Mean Reversion — EXIT ({reason})*\n"
            f"Symbol     : `{symbol}`\n"
            f"Entry      : ₹{entry_opt:.2f}  →  Exit: ₹{exit_opt:.2f}  ({pct:+.1f}%)\n"
            f"Gross P&L  : ₹{gross_pnl:+,.0f}\n"
            f"Net P&L    : ₹{net_pnl:+,.0f}\n"
            f"BANKNIFTY  : {self.bnf_ltp:.0f}\n"
            f"_Time: {datetime.now().strftime('%H:%M')}_"
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  EOD EXIT
    # ══════════════════════════════════════════════════════════════════════════

    async def _eod_exit(self) -> None:
        """Unconditional position close at SESSION_END (15:15 IST)."""
        if self._eod_exit_done:
            return
        self._eod_exit_done = True

        if not self.active_trade:
            logger.info("🔔 EOD: no open position.")
            return

        logger.warning("⏰ EOD exit triggered — closing PE at market.")
        ltp = self.pe_ltp or self.active_trade["entry_opt"]
        await self._close_trade(ltp, "EOD 15:15")

    # ══════════════════════════════════════════════════════════════════════════
    #  WEBSOCKET HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _subscribe(self, symbol: str, exchange: str, mode: int = 2) -> None:
        if self.ws and symbol not in self._subscribed_syms:
            try:
                await self.ws.send(json.dumps({
                    "action":   "subscribe",
                    "symbol":   symbol,
                    "exchange": exchange,
                    "mode":     mode,
                }))
                self._subscribed_syms.add(symbol)
                logger.info(f"  📡 Subscribed: {symbol}  (mode={mode})")
            except Exception as e:
                logger.warning(f"  Subscribe error {symbol}: {e}")

    async def _resubscribe_all(self) -> None:
        self._subscribed_syms.clear()
        await self._subscribe(IDX_SYMBOL, IDX_EXCHANGE)
        if self.pe_symbol and self._session_started:
            await self._subscribe(self.pe_symbol, OPT_EXCHANGE)

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE DUMP (dashboard heartbeat)
    # ══════════════════════════════════════════════════════════════════════════

    async def _state_dump_loop(self) -> None:
        while True:
            try:
                if not self.trend_ok and self._session_started:
                    phase = "INACTIVE_TREND"
                elif not self.dte_ok and self._session_started:
                    phase = "INACTIVE_DTE"
                elif self.active_trade:
                    phase = "IN_TRADE"
                elif self.signal_fired:
                    phase = "COMPLETED"
                elif len(self.idx_bars.bars) < MIN_BARS_FOR_BB:
                    phase = "WARMUP"
                else:
                    phase = "MONITORING"

                STATE_FILE.write_text(json.dumps({
                    "strategy":     STRATEGY_NAME,
                    "phase":        phase,
                    "last_update":  datetime.now().isoformat(),
                    "bnf_ltp":      round(self.bnf_ltp, 2),
                    "pe_ltp":       round(self.pe_ltp,  2),
                    "pe_symbol":    self.pe_symbol,
                    "expiry":       self.expiry,
                    "trend_ok":     self.trend_ok,
                    "dte_ok":       self.dte_ok,
                    "signal_fired": self.signal_fired,
                    "paper_mode":   PAPER_MODE,
                    "lot_size":     self.lot_size,
                    "n_lots":       self.n_lots,
                    "bars_loaded":  len(self.idx_bars.bars),
                    "bb":           self.bb_snapshot,
                    "htf_open":     round(self.idx_bars.htf_open, 2),
                    "active_trade": {
                        "symbol":    self.active_trade["symbol"],
                        "entry_opt": self.active_trade["entry_opt"],
                        "sl_opt":    self.active_trade["sl_opt"],
                        "tp_opt":    self.active_trade["tp_opt"],
                        "sl_index":  self.active_trade["sl_index"],
                        "risk_opt":  self.active_trade["risk_opt"],
                        "nat_rr":    self.active_trade["nat_rr"],
                        "qty":       self.active_trade["qty"],
                        "entry_time": self.active_trade["entry_time"],
                    } if self.active_trade else None,
                }, default=str))
            except Exception:
                pass
            await asyncio.sleep(2)

    # ══════════════════════════════════════════════════════════════════════════
    #  MAIN LOOP
    # ══════════════════════════════════════════════════════════════════════════

    async def main_loop(self) -> None:
        """
        1. Start state-dump background task.
        2. Bootstrap session if already past market open.
        3. Connect WebSocket with auto-reconnect.
        4. Route ticks: index → signal + SL check; PE → target check.
        5. EOD exit at 15:15.
        """
        asyncio.create_task(self._state_dump_loop())

        # Bootstrap immediately if starting mid-session
        if datetime.now().time() >= MARKET_OPEN and not self._session_started:
            self._session_started = True
            await self._session_open(self.bnf_ltp)

        retry_delay = 5

        while True:
            if datetime.now().time() >= SESSION_END:
                await self._eod_exit()
                logger.info("✅ Past 15:15 IST — shutting down.")
                break

            try:
                logger.info(f"🔌 Connecting to WebSocket: {WS_URL}")
                async with websockets.connect(
                    WS_URL, ping_interval=30, ping_timeout=60
                ) as ws:
                    self.ws = ws

                    # Authenticate BEFORE subscribing (mandatory)
                    await ws.send(json.dumps({
                        "action":  "authenticate",
                        "api_key": API_KEY,
                    }))

                    # Always subscribe BANKNIFTY index
                    await self._subscribe(IDX_SYMBOL, IDX_EXCHANGE)

                    # Re-subscribe PE if reconnecting mid-session
                    if self._session_started and self.pe_symbol and self.trend_ok and self.dte_ok:
                        await self._subscribe(self.pe_symbol, OPT_EXCHANGE)

                    retry_delay = 5

                    async for raw in ws:
                        # EOD check inside tick loop
                        if datetime.now().time() >= SESSION_END:
                            await self._eod_exit()
                            return

                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        if msg.get("type") != "market_data":
                            continue

                        sym   = msg.get("symbol", "")
                        mdata = msg.get("data", {})
                        ltp   = float(mdata.get("ltp", 0) or mdata.get("lp", 0))
                        ts_raw = mdata.get("t")
                        ts    = (datetime.fromtimestamp(float(ts_raw))
                                 if ts_raw else datetime.now())

                        if ltp <= 0:
                            continue

                        # ── BANKNIFTY index ticks ────────────────────────────
                        if sym in (IDX_SYMBOL, f"{IDX_EXCHANGE}:{IDX_SYMBOL}"):
                            bar_completed = self.idx_bars.update(ltp, ts)
                            await self._on_index_tick(ltp)

                            # Run signal check on each completed 1-min bar
                            if bar_completed and self._session_started:
                                await self._check_signal()

                        # ── PE option ticks ──────────────────────────────────
                        elif self.pe_symbol and sym in (
                            self.pe_symbol,
                            f"{OPT_EXCHANGE}:{self.pe_symbol}",
                        ):
                            await self._on_option_tick(ltp)

            except websockets.exceptions.ConnectionClosed:
                logger.warning(
                    f"WebSocket closed. Reconnecting in {retry_delay}s …"
                )
                self._subscribed_syms.clear()
                self.ws = None
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

            except Exception as e:
                logger.warning(
                    f"WebSocket error: {e}. Reconnecting in {retry_delay}s …"
                )
                self._subscribed_syms.clear()
                self.ws = None
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info(
        f"\n{'='*60}\n"
        f"  BB MEAN REVERSION BOT  |  BANKNIFTY ATM PE (BUY)\n"
        f"  Mode: {'PAPER (1 lot)' if PAPER_MODE else '⚠️  LIVE (' + str(N_LOTS_LIVE) + ' lots)'}\n"
        f"  Research: IS +1.008  |  OOS +2.425  |  MC 97.3%\n"
        f"  Gates: Trend | Trigger | HTF | NatRR≥{NAT_RR_THRESHOLD} | DTE\n"
        f"{'='*60}"
    )
    bot = BbMeanReversionBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("Stop signal received — bot terminated.")
