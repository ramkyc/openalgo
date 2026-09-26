"""
NIFTY BB Overbought Bot
=======================
live_trading/nifty_bb_overbought_bot/nifty_bb_overbought_bot.py

Research-validated (bb_deep_study, 2026-03-19):
  options_data/research/bb_deep_study/study_a_report/STUDY_A_RESULTS.md
  IS Sharpe +3.63  |  OOS Sharpe +10.65 (OVERBOUGHT-only)
  Win rate 65.9% IS / 68.8% OOS  |  E4 hit rate 43%  |  SL rate 0% at ≥₹150

Strategy:
  When NIFTY closes a 5-minute bar ABOVE its upper Bollinger Band (30-period, 3σ)
  during the morning window (09:15–10:30 IST), SELL the nearest weekly ATM PE.
  The hypothesis: a 3σ excursion is statistically extreme and will mean-revert,
  allowing the sold PE premium to decay ≥30% before EOD.

Signal rules (ALL required):
  1. NIFTY 5-min bar CLOSES above BB(30, 3σ) upper band
  2. Bar close time falls within 09:15 – 10:30 IST
  3. ATM PE premium ≥ ₹150 at the moment of entry
  4. [Optional] Daily ADX-14 < 25 — skips trending days (USE_ADX_FILTER flag, default OFF;
     champion config has NO ADX filter — OOS Sharpe 10.65 achieved without it)
  5. No position already open (one trade per session)

  → Signal fires → SELL ATM NIFTY PE (nearest weekly, ≥2 DTE)

Exit rules (first to trigger wins):
  E4  (profit target) : PE premium falls 30% from entry → BUY to close
  SL  (stop loss)     : PE premium doubles (2× entry) → BUY to close
  EOD (time stop)     : Close at 15:15 IST unconditionally

Research notes:
  • SL has never fired at ≥₹150 entry in IS or OOS period (n=60 trades).
    Monitor closely if premium filter is ever loosened below ₹150.
  • OVERSOLD / CE signals have NO edge in OOS — not traded here.
  • SENSEX and BANKNIFTY fail Stage 9 — NIFTY only.
  • Expiry-day tighter filter: skip if ATM PE < ₹200 on expiry day.
  • ADX daily regime guard is OPTIONAL (USE_ADX_FILTER = False by default).
    ADX(daily) > 25 → skip trending days (hurts edge by −6.67 Sharpe when enabled).
    Champion config has no ADX filter — the OOS Sharpe 10.65 figure is without it.

Position sizing: 1 lot flat (75 units). DO NOT scale until 15+ live trades.

Shared utilities (no duplication):
  live_trading.api_utils                — get_expiry_dates, get_option_symbol, get_history
  live_trading.shared.atm_resolver      — get_option_ltp
  live_trading.shared.telegram_notifier — send_async
  database.token_db.get_symbol_info     — dynamic lot size
  openalgo.api                          — placesmartorder
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
from live_trading.shared import ta_compat as ta
import websockets
from dotenv import load_dotenv
from openalgo import api

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils import (
    get_expiry_dates, get_option_symbol, get_history, is_market_holiday
)
from live_trading.shared.atm_resolver      import get_option_ltp
from live_trading.shared.order_fill        import fetch_fill_price
from live_trading.shared.premium_state     import AnchoredStraddle
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger      import log_trade_to_db
from live_trading.shared.decision_logger   import DecisionLogger
from live_trading.shared.tick_watchdog     import TickWatchdog

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "nifty_bb_overbought_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER",   "http://127.0.0.1:8080")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:8765")

if not API_KEY:
    logger.error("❌ OPENALGO_API_KEY not found. Exiting.")
    sys.exit(1)

# ── Strategy constants ────────────────────────────────────────────────────────
STRATEGY_NAME    = "NIFTY_BB_OVERBOUGHT"

IDX_SYMBOL       = "NIFTY"
IDX_EXCHANGE     = "NSE_INDEX"
VIX_SYMBOL       = "INDIAVIX"        # tracked for dashboard but not used as a filter
VIX_EXCHANGE     = "NSE_INDEX"
OPT_EXCHANGE     = "NFO"

N_LOTS           = 10                # standardised: 10 lots per CLAUDE.md position-size rule
DEFAULT_LOT_SIZE = 65                # fallback if DB lookup fails (post-Dec 2025 lot size)

# BB parameters (research-optimal)
BB_PERIOD        = 30
BB_STD_MULT      = 3.0               # 3σ — research shows 2σ has weak edge; 3σ is the edge

# Entry filters
MIN_PREMIUM      = 150.0             # ₹150 minimum ATM PE premium at entry
MIN_PREMIUM_EXPIRY = 200.0           # ₹200 on expiry day (tail-risk guard)
ADX_DAILY_MAX    = 25.0              # Skip if daily ADX-14 ≥ 25 (trending = bad for MR)
USE_ADX_FILTER   = False             # Optional regime guard — OFF by default (champion config
                                     # has NO ADX filter; OOS Sharpe 10.65 without it).
                                     # Set True to re-enable the trending-day skip.
MIN_DTE          = 2
MAX_DTE          = 7

# F2 premium-strength filter (research/premium_filter_retrofit → F2_ADDENDUM.md
# in bb_deep_study/study_a_report/). State = 09:20-anchored ATM straddle vs its
# anchor premium. Modes:
#   "off"    — filter fully disabled
#   "shadow" — evaluate + log + record in state; NO behavior change (default)
#   "size"   — full N_LOTS when premium-up, N_LOTS//2 when premium-weak
#   "hard"   — skip entry entirely when premium-weak (NOT recommended: OOS
#              evidence thin — see addendum; use "size")
# Fail-open: if the premium state can't be evaluated, trade at full size.
F2_MODE          = "shadow"

# Exit parameters
E4_DECAY         = 0.30              # 30% premium drop from entry → profit target
SL_MULTIPLE      = 2.0               # 2× entry premium → stop loss

# Session timing
MARKET_OPEN      = dt_time(9,  15)
ENTRY_START      = dt_time(9,  15)
ENTRY_END        = dt_time(10, 30)   # no new entries after 10:30
SESSION_END      = dt_time(15, 14)   # EOD close; sandbox auto-squareoff cutoff is 15:15

# Warm-up: 5 days of 1-min history → resampled to 5-min
WARMUP_DAYS      = 5
MIN_BARS_REQUIRED = BB_PERIOD + 5   # need at least 35 completed 5-min bars

# State file for Telegram /status
STATE_FILE       = LOGS_DIR / "nifty_bb_overbought_state.json"

# Decision-state logging (jsonl + throttled heartbeat — see shared/decision_logger.py)
DECISION_LOG     = LOGS_DIR / "nifty_bb_overbought_decisions.jsonl"
HEARTBEAT_SECS   = 300


# ── Helper: dynamic lot size ──────────────────────────────────────────────────
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


# ── Helper: expiry selection ──────────────────────────────────────────────────
def _get_suitable_expiry() -> str | None:
    dates = get_expiry_dates(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, "options")
    today = datetime.now().date()
    for d in dates:
        try:
            exp_dt = datetime.strptime(d, "%d%b%y").date()
            dte = (exp_dt - today).days
            if MIN_DTE <= dte <= MAX_DTE:
                logger.info(f"  Suitable expiry: {d}  (DTE={dte})")
                return d
            if dte > MAX_DTE:
                continue
        except ValueError:
            continue
    logger.warning(f"  No expiry found DTE {MIN_DTE}–{MAX_DTE}. Dates: {dates[:6]}")
    return None


# ── Helper: daily ADX filter ─────────────────────────────────────────────────
def _get_daily_adx() -> float | None:
    """
    Fetch NIFTY daily ADX-14.

    Strategy (fastest first):
      1. Request "D" interval directly — ~180 calendar days (~120 trading
         days), cheap regardless of window size since it's one bar/day.
      2. If that returns nothing (broker may not support "D" for NSE_INDEX),
         fall back to 60 calendar days of 1-min bars resampled to daily.
         Kept at 60 (not widened) because wider 1-min pulls have historically
         timed out server-side (~90 days = ~23,000 bars). The stricter
         min-bar guard below means this fallback now correctly returns None
         instead of a biased ADX when it can't gather enough warm-up bars.

    Wilder's EWM ADX needs ~5x period of warm-up bars to converge — a
    30-60 day / ~20-40 trading-day pull was found to inflate ADX by ~2x
    (verified against banknifty_bb_opening_candle_bot's identical bug).

    Today's partial bar is excluded so only fully completed sessions feed
    the Wilder RMA.

    Returns the latest ADX-14 value, or None if computation fails.
    """
    MIN_ADX_BARS = 14 * 5   # ~5x period for EWM convergence

    def _compute_adx_from_df(df: "pd.DataFrame") -> "float | None":
        for col in ["high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["high", "low", "close"]).reset_index(drop=True)
        if len(df) < MIN_ADX_BARS:
            logger.warning(f"  Daily ADX: only {len(df)} daily bars — need ≥ {MIN_ADX_BARS} for convergence.")
            return None
        adx_df = ta.adx(df["high"], df["low"], df["close"], length=14)
        if adx_df is None or "ADX_14" not in adx_df.columns:
            logger.warning("  Daily ADX: pandas-ta returned no ADX_14 column.")
            return None
        val = float(adx_df["ADX_14"].iloc[-1])
        return val if not np.isnan(val) else None

    def _rows_to_daily_df(rows: list, from_1min: bool = False) -> "pd.DataFrame | None":
        """Convert raw history rows to a daily OHLCV DataFrame."""
        if not rows:
            return None
        df = pd.DataFrame(rows)
        if "timestamp" not in df.columns:
            logger.warning("  Daily ADX: unexpected history format (no timestamp column).")
            return None
        df["dt"] = (pd.to_datetime(df["timestamp"], unit="s", utc=True)
                    .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None))
        df["date"] = df["dt"].dt.date
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        if from_1min:
            # Resample 1-min bars → daily OHLCV
            df = (df.groupby("date")
                    .agg(open=("open", "first"),
                         high=("high", "max"),
                         low=("low", "min"),
                         close=("close", "last"))
                    .reset_index())
        df = df.sort_values("date").reset_index(drop=True)
        # Exclude today's partial bar
        from datetime import date as _date
        df = df[df["date"] < _date.today()].reset_index(drop=True)
        return df

    try:
        # ── Attempt 1: daily interval (fast — ~180 calendar days) ────────
        raw_d = get_history(API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "D", 180)
        if raw_d:
            daily = _rows_to_daily_df(raw_d, from_1min=False)
            if daily is not None and len(daily) >= MIN_ADX_BARS:
                logger.info(f"  Daily ADX: using 'D' interval ({len(daily)} bars).")
                return _compute_adx_from_df(daily)
            logger.warning(f"  Daily ADX: 'D' interval returned {len(daily) if daily is not None else 0} bars — trying 1-min fallback.")
        else:
            logger.warning("  Daily ADX: 'D' interval returned no data — trying 1-min fallback.")

        # ── Attempt 2: 1-min bars resampled to daily (100 days) ──────────
        # Bumped from 60 -> 100 calendar days (~70 trading days) to just
        # clear MIN_ADX_BARS. Not pushed further: wider 1-min pulls have
        # historically timed out server-side (~90 days = ~23,000 bars) —
        # this path is a rare fallback (attempt 1 covers the common case),
        # so a modest bump is the right tradeoff over the old guaranteed-
        # insufficient 60-day window.
        raw1m = get_history(API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "1m", 100)
        if not raw1m:
            logger.warning("  Daily ADX: 1-min fallback also returned no data.")
            return None
        daily = _rows_to_daily_df(raw1m, from_1min=True)
        if daily is None:
            return None
        logger.info(f"  Daily ADX: using 1-min→daily resample ({len(daily)} daily bars).")
        return _compute_adx_from_df(daily)

    except Exception as e:
        logger.warning(f"  Daily ADX computation failed: {e}")
        return None


def _check_fill(client, order_id: str, symbol: str, opt_exchange: str) -> tuple[bool, float]:
    """
    Returns (is_filled, fill_price).
    Parses the orderbook for the given order_id.
    """
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
class NiftyBBOverboughtBot:
# ══════════════════════════════════════════════════════════════════════════════
    """
    BB(30, 3σ) Overbought signal on NIFTY 5-min bars → sell ATM PE.

    Research-validated paper trading bot (bb_deep_study Study A).
    Replaces the old BB 5M Scanner's discretionary alert approach with
    automated order placement via OpenAlgo (running in Analyze mode).
    """

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.ws     = None

        # Live price
        self.nifty_ltp: float = 0.0
        self.vix_ltp:   float = 0.0

        # 5-min bar builder
        self.bars: deque        = deque(maxlen=300)   # completed 5-min bars
        self._bar_bucket: int   = -1                  # current 5-min bucket index
        self._bar_open:  float  = 0.0
        self._bar_high:  float  = 0.0
        self._bar_low:   float  = float("inf")
        self._last_ltp:  float  = 0.0

        # Active PE position (one per session)
        self.active_pe: dict | None = None

        # Session metadata
        self.expiry:       str | None   = None
        self.lot_size:     int          = DEFAULT_LOT_SIZE
        self.daily_adx:    float | None = None   # yesterday's daily ADX-14
        self.is_expiry_day: bool        = False  # True if today is expiry day

        # State flags
        self._session_started  = False
        self._next_init_attempt = datetime.min   # retry gate for session init
        self._eod_exit_done    = False
        self._first_connect    = True
        self._subscribed_syms: set[str] = set()

        # Dashboard snapshot
        self.bb_snapshot: dict = {}

        # F2 premium-strength state (09:20-anchored ATM straddle; REST-based,
        # restart-safe — reconstructed from history on every evaluation)
        self.prem_state = AnchoredStraddle(IDX_SYMBOL, API_KEY)
        self.f2_last: dict = {}   # last evaluation, surfaced in state file

        # Decision-state logging (jsonl + throttled heartbeat)
        self._dlog = DecisionLogger(DECISION_LOG, heartbeat_secs=HEARTBEAT_SECS, bot_logger=logger)

        self._watchdog = TickWatchdog(
            bot_name="NIFTY BB Overbought Bot",
            tracked_symbols=lambda: [IDX_SYMBOL, VIX_SYMBOL] + (
                [self.active_pe["symbol"]] if self.active_pe else []
            ),
            market_open=MARKET_OPEN,
            market_close=SESSION_END,
            bot_logger=logger,
        )

        # Restore any active PE trade from today's state file (mid-session restart)
        self._restore_state()

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE RESTORE (mid-session restart recovery)
    # ══════════════════════════════════════════════════════════════════════════

    def _restore_state(self) -> None:
        """
        Reload today's active PE trade from the state file on mid-session restart.
        Only activates when the state file timestamp is from today and an
        active_trade is recorded. The entry guard (active_pe is not None) in
        _enter_trade() prevents any re-entry after restore.
        """
        if not STATE_FILE.exists():
            return
        try:
            state = json.loads(STATE_FILE.read_text())
            last_update = datetime.fromisoformat(state.get("last_update", ""))
            if last_update.date() != datetime.now().date():
                return   # stale — yesterday's state, ignore
            at = state.get("active_trade")
            if not at:
                return
            self.active_pe = {
                "symbol":     at["symbol"],
                "entry_prem": at["entry_prem"],
                "e4_target":  at["e4_target"],
                "sl_prem":    at["sl_prem"],
                "sl_order_id": at.get("sl_order_id"),
                "qty":        at["qty"],
                "lot_size":   at.get("qty", self.lot_size),   # fallback to default
                "order_id":   "",
                "entry_time": at.get("entry_time", ""),
            }
            logger.info(
                f"[_restore_state] Restored active PE: {at['symbol']}  "
                f"entry=₹{at['entry_prem']}  sl=₹{at['sl_prem']}  qty={at['qty']}"
            )
        except Exception as e:
            logger.warning(f"[_restore_state] Could not read state file: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  5-MIN BAR BUILDER
    # ══════════════════════════════════════════════════════════════════════════

    def _update_bar(self, ltp: float, ts: datetime) -> bool:
        """
        Feed a NIFTY tick into the running 5-minute bar.
        Returns True the moment a bar COMPLETES (5-min bucket boundary crossed).
        Bucket index = floor(total_minutes / 5), so 09:15→bucket 111, 09:20→112, etc.
        """
        total_min   = ts.hour * 60 + ts.minute
        cur_bucket  = total_min // 5   # new bucket every 5 minutes

        if self._bar_bucket == -1:
            self._bar_bucket = cur_bucket
            self._bar_open   = ltp
            self._bar_high   = ltp
            self._bar_low    = ltp
            self._last_ltp   = ltp
            return False

        if cur_bucket != self._bar_bucket:
            # 5-min boundary crossed — close previous bar
            if self._bar_open > 0:
                self.bars.append({
                    "open":  self._bar_open,
                    "high":  self._bar_high,
                    "low":   self._bar_low,
                    "close": self._last_ltp,
                })
            # Start new bar
            self._bar_bucket = cur_bucket
            self._bar_open   = ltp
            self._bar_high   = ltp
            self._bar_low    = ltp
            self._last_ltp   = ltp
            return True

        # Same 5-min bucket — update running bar
        self._bar_high = max(self._bar_high, ltp)
        self._bar_low  = min(self._bar_low,  ltp)
        self._last_ltp = ltp
        return False

    # ══════════════════════════════════════════════════════════════════════════
    #  SIGNAL ENGINE
    # ══════════════════════════════════════════════════════════════════════════

    def _compute_bb_signal(self) -> bool:
        """
        Compute BB(30, 3σ) on completed 5-min closes.
        Returns True if the latest bar's close is ABOVE the upper band.
        Requires at least MIN_BARS_REQUIRED completed bars.
        """
        if len(self.bars) < MIN_BARS_REQUIRED:
            return False

        closes = pd.Series([b["close"] for b in self.bars])
        sma    = closes.rolling(BB_PERIOD).mean().iloc[-1]
        std    = closes.rolling(BB_PERIOD).std(ddof=0).iloc[-1]

        if pd.isna(sma) or pd.isna(std) or std == 0:
            return False

        upper  = sma + BB_STD_MULT * std
        lower  = sma - BB_STD_MULT * std
        close  = closes.iloc[-1]

        # Save snapshot for dashboard
        self.bb_snapshot = {
            "close":    round(float(close), 2),
            "sma":      round(float(sma),   2),
            "upper":    round(float(upper),  2),
            "lower":    round(float(lower),  2),
            "std":      round(float(std),    2),
            "bars":     len(self.bars),
            "distance_pct": round((float(close) - float(upper)) / float(upper) * 100, 3),
        }

        is_overbought = bool(close > upper)
        if is_overbought:
            logger.info(
                f"📶 BB OVERBOUGHT: NIFTY={close:.1f} > upper={upper:.1f} "
                f"(SMA={sma:.1f}, +{(close-upper):.1f}pts)"
            )
        return is_overbought

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE ENTRY
    # ══════════════════════════════════════════════════════════════════════════

    async def _enter_trade(self) -> None:
        """
        Resolve ATM PE symbol, check ≥₹150 premium (≥₹200 on expiry day),
        check ADX daily filter, then SELL via placesmartorder.
        """
        if self.active_pe is not None:
            return   # one position per session

        if not self.expiry:
            logger.warning("  [PE] No suitable expiry today — skipping.")
            return

        # ── ADX daily regime filter (optional) ───────────────────────────────
        # Disabled by default: champion config has NO ADX filter.
        # Enable with USE_ADX_FILTER = True to suppress entries on trending days.
        if USE_ADX_FILTER and self.daily_adx is not None and self.daily_adx >= ADX_DAILY_MAX:
            logger.info(
                f"  [PE] ADX filter: daily ADX={self.daily_adx:.1f} ≥ {ADX_DAILY_MAX} "
                f"(trending day — mean-reversion edge suppressed). Entry skipped."
            )
            await send_async(
                f"⏭ *BB Overbought — Entry Skipped*\n"
                f"NIFTY BB(30,3σ) fired but daily ADX={self.daily_adx:.1f} ≥ {ADX_DAILY_MAX}\n"
                f"_Trending day — mean-reversion edge not present._"
            )
            return

        spot = self.nifty_ltp
        if spot <= 0:
            return

        # ── Resolve ATM PE symbol ─────────────────────────────────────────────
        symbol = await asyncio.to_thread(
            get_option_symbol,
            API_KEY, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, "PE", "ATM",
        )
        if not symbol:
            logger.error(f"  [PE] ATM symbol resolution failed (spot={spot:.0f}, expiry={self.expiry})")
            return

        # ── Fetch live premium ────────────────────────────────────────────────
        opt_ltp = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        if opt_ltp <= 0:
            logger.warning(f"  [PE] Option LTP = 0 for {symbol}. Skipping.")
            return

        # ── Premium filter (tighter on expiry day) ────────────────────────────
        min_prem = MIN_PREMIUM_EXPIRY if self.is_expiry_day else MIN_PREMIUM
        if opt_ltp < min_prem:
            logger.info(
                f"  [PE] Premium ₹{opt_ltp:.2f} < ₹{min_prem:.0f} minimum "
                f"({'expiry day' if self.is_expiry_day else 'normal day'}). Entry skipped."
            )
            await send_async(
                f"⏭ *BB Overbought — Entry Skipped*\n"
                f"NIFTY BB(30,3σ) fired but {symbol} premium ₹{opt_ltp:.2f} "
                f"< ₹{min_prem:.0f} minimum{'  (expiry day)' if self.is_expiry_day else ''}.\n"
                f"_Entry requires premium ≥ ₹{min_prem:.0f} for viable E4 target._"
            )
            return

        # ── F2 premium-strength filter (see F2_MODE) ──────────────────────────
        entry_lots = N_LOTS
        f2 = await asyncio.to_thread(self.prem_state.evaluate)
        self.f2_last = {**f2, "checked_at": datetime.now().strftime("%H:%M:%S"),
                        "mode": F2_MODE}
        if F2_MODE != "off":
            if f2.get("ok"):
                f2_up = f2["f2_premium_up"]
                if not f2_up and F2_MODE == "size":
                    entry_lots = max(1, N_LOTS // 2)
                elif not f2_up and F2_MODE == "hard":
                    logger.info(f"  [F2] premium-weak (S={f2['s_now']} ≤ S0={f2['s0']}) "
                                f"mode=hard → entry skipped.")
                    await send_async(
                        f"⏭ *BB Overbought — Entry Skipped (F2)*\n"
                        f"Straddle premium ₹{f2['s_now']} ≤ 09:20 anchor ₹{f2['s0']}\n"
                        f"_Premium-weak state — F2 hard filter._")
                    return
                logger.info(
                    f"  [F2] {'PREMIUM-UP' if f2_up else 'premium-weak'} "
                    f"S0=₹{f2['s0']} S=₹{f2['s_now']} ({f2['anchor']}) "
                    f"mode={F2_MODE} → lots={entry_lots}"
                )
            else:
                logger.warning(f"  [F2] state unavailable ({f2.get('reason')}) "
                               f"— FAIL-OPEN, full size.")

        # ── Compute exits ─────────────────────────────────────────────────────
        lot_size  = _get_lot_size(symbol)
        qty       = entry_lots * lot_size
        e4_target = round(opt_ltp * (1.0 - E4_DECAY), 2)   # 30% below entry
        sl_prem   = round(opt_ltp * SL_MULTIPLE, 2)          # 2× entry

        logger.info(
            f"  [PE] Entering: {symbol}  LTP=₹{opt_ltp:.2f}  "
            f"E4=₹{e4_target:.2f} (-30%)  SL=₹{sl_prem:.2f} (2×)  qty={qty}"
        )

        # ── Place order ───────────────────────────────────────────────────────
        try:
            res = self.client.placesmartorder(
                strategy      = STRATEGY_NAME,
                symbol        = symbol,
                action        = "SELL",
                exchange      = OPT_EXCHANGE,
                price_type    = "MARKET",
                product       = "MIS",
                quantity      = qty,
                position_size = -qty,
            )
        except Exception as e:
            logger.error(f"  [PE] placesmartorder exception: {e}")
            return

        if res.get("status") == "success":
            fill_prem = _resolve_fill(res, opt_ltp)
            e4_target = round(fill_prem * (1.0 - E4_DECAY), 2)   # 30% below fill
            sl_prem   = round(fill_prem * SL_MULTIPLE, 2)          # 2× fill
            logger.info(f"  [PE] Entry fill=₹{fill_prem:.2f} vs LTP=₹{opt_ltp:.2f}")

            # ── Broker-side SL-M order (resting stop, engages even if the app
            # crashes / websocket drops — protects against the tick-poll gap
            # that let SL overshoot in banknifty_bb_opening_candle_bot). ──────
            sl_order_id = None
            try:
                sl_resp = self.client.placeorder(
                    strategy      = STRATEGY_NAME,
                    symbol        = symbol,
                    action        = "BUY",
                    exchange      = OPT_EXCHANGE,
                    price_type    = "SL-M",
                    trigger_price = str(sl_prem),
                    product       = "MIS",
                    quantity      = str(qty),
                )
            except Exception as e:
                logger.error(f"  [PE] Broker-side SL-M placement exception: {e}")
                sl_resp = None

            if sl_resp and sl_resp.get("status") == "success":
                sl_order_id = str(sl_resp.get("orderid", ""))
                logger.info(f"  [PE] 🛡️ Broker-side SL-M resting @ trigger ₹{sl_prem:.2f}  order_id={sl_order_id}")
            else:
                logger.error(f"  [PE] ⚠️ Broker-side SL-M FAILED to place ({sl_resp}) — falling back to app-side tick monitoring only.")
                await send_async("⚠️ *NIFTY BB Overbought — Broker-side SL-M order failed to place!*\nFalling back to app-side tick monitoring only — slippage risk on SL exit.")

            self.active_pe = {
                "symbol":     symbol,
                "entry_prem": fill_prem,
                "e4_target":  e4_target,
                "sl_prem":    sl_prem,
                "sl_order_id": sl_order_id,
                "qty":        qty,
                "lot_size":   lot_size,
                "n_lots":     entry_lots,
                "f2_mode":    F2_MODE,
                "f2_ok":      bool(f2.get("ok")),
                "f2_premium_up": f2.get("f2_premium_up"),
                "order_id":   str(res.get("orderid", "")),
                "entry_time": datetime.now().isoformat(),
            }
            self.lot_size = lot_size
            await self._subscribe(symbol, OPT_EXCHANGE)

            f2_line = (
                f"F2: {'premium-up ✅' if f2.get('f2_premium_up') else 'premium-weak ⚠️'}"
                f" (S0 ₹{f2.get('s0')} → ₹{f2.get('s_now')}, mode={F2_MODE})"
                if f2.get("ok") else f"F2: unavailable ({f2.get('reason')}, fail-open)"
            )
            logger.info(
                f"✅ [PE] SOLD {symbol} @ ₹{fill_prem:.2f}  "
                f"({entry_lots} lots, qty={qty}, order={res.get('orderid')})"
            )
            await send_async(
                f"📉 *NIFTY BB Overbought — ENTRY*\n"
                f"Sold `{symbol}`  ({entry_lots} lots, qty={qty})\n"
                f"{f2_line}\n"
                f"Entry premium : ₹{fill_prem:.2f}  (LTP ₹{opt_ltp:.2f})\n"
                f"Profit target : ₹{e4_target:.2f}  (−30% → E4)\n"
                f"Stop loss     : ₹{sl_prem:.2f}  (2× entry"
                f"{', broker SL-M resting' if sl_order_id else ', ⚠️ app-side only'})\n"
                f"Time stop     : 15:15 IST\n"
                f"NIFTY: {spot:.1f}  |  Upper BB: ₹{self.bb_snapshot.get('upper', 0):.1f}\n"
                f"Daily ADX: {self.daily_adx:.1f}  (filter {'ON' if USE_ADX_FILTER else 'OFF'})\n"
                f"_Signal: {datetime.now().strftime('%H:%M')}_"
            )
        else:
            logger.error(f"  [PE] Order rejected: {res}")

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE EXIT
    # ══════════════════════════════════════════════════════════════════════════

    async def _close_trade(
        self, exit_prem: float, reason: str, *, already_filled_order_id: str | None = None
    ) -> None:
        """Buy back the sold PE to flatten the short position."""
        if not self.active_pe:
            return

        symbol      = self.active_pe["symbol"]
        qty         = self.active_pe["qty"]
        sl_order_id = self.active_pe.get("sl_order_id")

        # Cancel the resting broker-side SL-M order before taking any other
        # close path — unless it's the one that just filled (nothing to cancel).
        if sl_order_id and sl_order_id != already_filled_order_id:
            await asyncio.to_thread(_cancel_order, self.client, sl_order_id, symbol, OPT_EXCHANGE)

        if already_filled_order_id:
            exit_fill = exit_prem
            order_ok  = True
            logger.info(f"  [PE] SL-M already filled @ ₹{exit_fill:.2f} — no new close order needed.")
        else:
            try:
                # Use placeorder (NOT placesmartorder) for exits.
                # placesmartorder(position_size=0) reads the broker's NET position across ALL
                # strategies — in live mode another bot holding the same symbol would cause
                # this exit to close both positions. placeorder with exact qty is safe.
                res = self.client.placeorder(
                    strategy   = STRATEGY_NAME,
                    symbol     = symbol,
                    action     = "BUY",
                    exchange   = OPT_EXCHANGE,
                    price_type = "MARKET",
                    product    = "MIS",
                    quantity   = str(qty),
                )
            except Exception as e:
                logger.error(f"  [PE] Close order exception: {e}")
                return

            order_ok = res.get("status") == "success"
            if not order_ok:
                # Position may already be closed by sandbox auto-squareoff (15:15 MIS cutoff).
                # Do NOT return early — always log the trade so performance.db stays accurate.
                logger.warning(
                    f"  [PE] Exit order non-success (likely auto-squareoff already "
                    f"closed position): {res}  — logging trade and clearing state."
                )

            # Resolve the actual exit fill (falls back to the LTP that triggered the exit
            # decision if the order lookup fails or the order never actually executed).
            exit_fill = _resolve_fill(res, exit_prem)
            logger.info(f"  [PE] Exit fill=₹{exit_fill:.2f} vs LTP=₹{exit_prem:.2f}")

        # Capture trade fields before clearing state.
        entry_prem  = self.active_pe["entry_prem"]
        _entry_time = self.active_pe.get("entry_time")
        _order_id   = self.active_pe.get("order_id")
        _lot_size   = self.active_pe.get("lot_size", qty)
        gross      = (entry_prem - exit_fill) * qty
        won        = gross > 0
        emoji      = "🟢" if won else "🔴"
        decay_pct  = (entry_prem - exit_fill) / entry_prem * 100

        # Clear state unconditionally.
        self.active_pe = None

        auto_sq_note = " _(closed by auto-squareoff)_" if not order_ok else ""
        logger.info(
            f"{emoji} [PE] CLOSED {symbol} @ ₹{exit_fill:.2f}  "
            f"reason={reason}  gross=₹{gross:,.0f}  decay={decay_pct:.1f}%"
        )
        await send_async(
            f"{emoji} *NIFTY BB Overbought — EXIT ({reason})*\n"
            f"Symbol : `{symbol}`\n"
            f"Entry  : ₹{entry_prem:.2f}  →  Exit: ₹{exit_fill:.2f}  "
            f"({decay_pct:+.1f}%)\n"
            f"Gross P&L : ₹{gross:,.0f}  (1 lot)\n"
            f"_Closed at {datetime.now().strftime('%H:%M:%S')}{auto_sq_note}_"
        )
        log_trade_to_db(
            bot_name      = "nifty_bb_overbought_bot",
            instrument    = "NIFTY",
            option_symbol = symbol,
            option_type   = "PE",
            entry_time    = _entry_time,
            exit_time     = datetime.now(),
            entry_premium = entry_prem,
            exit_premium  = exit_fill,
            exit_reason   = reason,
            quantity      = qty,
            lots          = N_LOTS,
            lot_size      = _lot_size,
            gross_pnl     = gross,
            order_id      = _order_id,
        )

    async def _eod_close_all(self) -> None:
        """Unconditional close at 15:15 IST (time stop)."""
        if self._eod_exit_done:
            return
        self._eod_exit_done = True
        logger.info("🔔 15:15 IST time stop — closing all positions.")
        if self.active_pe:
            ltp = await asyncio.to_thread(
                get_option_ltp, self.active_pe["symbol"], OPT_EXCHANGE, API_KEY
            )
            exit_p = ltp if ltp > 0 else self.active_pe["entry_prem"]
            await self._close_trade(exit_p, "EOD 15:15")

    # ══════════════════════════════════════════════════════════════════════════
    #  DECISION-STATE LOGGING
    # ══════════════════════════════════════════════════════════════════════════

    def _verdict(self, now_t: dt_time) -> str:
        """What's currently blocking entry — checked in the order these gates
        actually apply in _on_index_tick() / _enter_trade(), so it always
        names the real blocker."""
        if not self._session_started:
            return "waiting for session start"
        if self.active_pe:
            ap = self.active_pe
            return (f"ACTIVE: holding {ap['symbol']} entry=₹{ap['entry_prem']:.2f} "
                    f"E4=₹{ap['e4_target']:.2f} SL=₹{ap['sl_prem']:.2f}")
        if self._eod_exit_done:
            return "done for today: EOD exit complete"
        if not self.expiry:
            return "BLOCKED: no suitable expiry found"
        if now_t < ENTRY_START:
            return "waiting for entry window to open"
        if now_t > ENTRY_END:
            return "entry window closed — no signal fired today"
        if USE_ADX_FILTER and self.daily_adx is not None and self.daily_adx >= ADX_DAILY_MAX:
            return f"BLOCKED: ADX filter (ADX={self.daily_adx:.1f} >= {ADX_DAILY_MAX})"
        if len(self.bars) < MIN_BARS_REQUIRED:
            return f"warming up: {len(self.bars)}/{MIN_BARS_REQUIRED} bars"
        bb = self.bb_snapshot
        if bb and bb.get("close", 0) > bb.get("upper", float("inf")):
            return "🔥 BB signal fired — checking premium filter / F2 before entry"
        return "🔥 in window — watching for NIFTY 5-min close > BB(30,3σ) upper"

    def _heartbeat_text(self) -> str:
        now_t = datetime.now().time()
        lines = [
            f"💓 DECISION STATE {datetime.now().strftime('%H:%M:%S')} ─ {self._verdict(now_t)}",
            f"    NIFTY={self.nifty_ltp:.1f}  VIX={self.vix_ltp:.2f}  "
            f"bars={len(self.bars)}  expiry={self.expiry}",
        ]
        if self.bb_snapshot:
            bb = self.bb_snapshot
            lines.append(
                f"    BB close={bb.get('close')}  upper={bb.get('upper')}  "
                f"sma={bb.get('sma')}  dist%={bb.get('distance_pct')}"
            )
        if self.active_pe:
            lines.append(f"    active: {self.active_pe['symbol']}")
        return "\n".join(lines)

    # ══════════════════════════════════════════════════════════════════════════
    #  TICK HANDLERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _on_index_tick(self, ltp: float, ts: datetime) -> None:
        """Called for every NIFTY index tick."""
        self.nifty_ltp = ltp
        now            = datetime.now()

        # Throttled to HEARTBEAT_SECS internally — cheap to call on every tick
        self._dlog.maybe_heartbeat(self._heartbeat_text)

        # One-time session initialisation — latches only when expiry resolves;
        # failed attempts retry every 120s so a transient OpenAlgo outage at
        # open can't disable the bot for the day (incident 2026-07-07).
        if (not self._session_started and now.time() >= MARKET_OPEN
                and now >= self._next_init_attempt):
            if is_market_holiday(API_KEY):
                logger.warning("⚠️ Tick received but today is a holiday. Ignoring.")
                return
            await self._session_open(ltp)
            if self.expiry:
                self._session_started = True
            else:
                self._next_init_attempt = now + timedelta(seconds=120)
                logger.warning("  ⚠️  Session init incomplete (no expiry) — retrying in 120s.")

        # Build 5-min bar
        bar_closed = self._update_bar(ltp, ts)
        if not bar_closed:
            return   # bar still in progress

        # Always compute BB snapshot so the dashboard shows live band values
        # even outside the entry window (e.g. after 10:30 or before 09:15).
        in_window = ENTRY_START <= now.time() <= ENTRY_END
        signal    = self._compute_bb_signal()   # always updates self.bb_snapshot

        # Always logged on every completed 5-min bar, regardless of window/gates,
        # so the log always has real values explaining "why not" — not just "why yes".
        self._dlog.log_bar({
            "phase":        "ACTIVE" if self.active_pe else "WAITING",
            "in_window":    in_window,
            "signal":       signal,
            "nifty_ltp":    round(self.nifty_ltp, 2),
            "vix_ltp":      round(self.vix_ltp, 2),
            "expiry":       self.expiry,
            "daily_adx":    round(self.daily_adx, 1) if self.daily_adx is not None else None,
            "bb":           self.bb_snapshot,
            "active_trade": self.active_pe["symbol"] if self.active_pe else None,
            "verdict":      self._verdict(now.time()),
        })

        # Gate actual entry behind the window / position / expiry guards
        if not in_window:
            return
        if not self.expiry:
            return
        if self.active_pe is not None:
            return   # already in a position this session

        if signal:
            await self._enter_trade()

    async def _on_option_tick(self, ltp: float) -> None:
        """Called for PE option ticks — monitors E4 target and SL."""
        if not self.active_pe or ltp <= 0:
            return

        entry_prem = self.active_pe["entry_prem"]
        e4_target  = self.active_pe["e4_target"]
        sl_prem    = self.active_pe["sl_prem"]

        # E4: premium has decayed 30% from entry
        if ltp <= e4_target:
            logger.info(
                f"🎯 [PE] E4 TARGET HIT: LTP=₹{ltp:.2f} ≤ E4=₹{e4_target:.2f} "
                f"(−30% from ₹{entry_prem:.2f})"
            )
            await self._close_trade(ltp, "E4 −30%")

        # SL: premium has doubled.
        # Fallback-only — when a broker-side SL-M order is resting, that order
        # is authoritative and is reconciled via _check_sl_order_filled(); this
        # tick-poll branch only fires if SL-M placement failed at entry.
        elif not self.active_pe.get("sl_order_id") and ltp >= sl_prem:
            logger.warning(
                f"🛑 [PE] STOP LOSS TRIGGERED: LTP=₹{ltp:.2f} ≥ SL=₹{sl_prem:.2f} "
                f"(2× entry ₹{entry_prem:.2f})"
            )
            await self._close_trade(ltp, f"SL 2×")

    async def _check_sl_order_filled(self) -> bool:
        """Poll the orderbook for the resting broker-side SL-M order. Returns
        True (and closes the trade) if it has filled."""
        if not self.active_pe or not self.active_pe.get("sl_order_id"):
            return False
        sl_order_id = self.active_pe["sl_order_id"]
        filled, fill_price = await asyncio.to_thread(
            _check_fill, self.client, sl_order_id, self.active_pe["symbol"], OPT_EXCHANGE
        )
        if not filled:
            return False
        logger.warning(f"🛑 [PE] Broker-side SL-M filled @ ₹{fill_price:.2f}")
        await self._close_trade(fill_price, "SL 2× (broker)", already_filled_order_id=sl_order_id)
        return True

    # ══════════════════════════════════════════════════════════════════════════
    #  SESSION INITIALISATION & WARM-UP
    # ══════════════════════════════════════════════════════════════════════════

    async def _session_open(self, spot: float) -> None:
        """Called once at 09:15 first tick or on restart. Resolves expiry/ADX/lot."""
        # 0. Holiday Check
        if is_market_holiday(API_KEY):
            logger.info("⛔ Market Holiday detected. Skipping session initialization.")
            return

        logger.info(f"🔔 Session open. NIFTY spot ≈ {spot:.0f}")

        # 0.1 Online Notification
        if self._first_connect:
            self._first_connect = False
            await send_async(
                f"🤖 *NIFTY BB Overbought Bot — Online*\n"
                f"Signal  : NIFTY 5-min close > BB({BB_PERIOD}, {BB_STD_MULT}σ) upper\n"
                f"Window  : {ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')} IST\n"
                f"Filters : PE ≥ ₹{MIN_PREMIUM:.0f}  |  ADX filter {'ON (<' + str(int(ADX_DAILY_MAX)) + ')' if USE_ADX_FILTER else 'OFF'}\n"
                f"Target  : −30% premium (E4)  |  SL: 2× entry\n"
                f"Sizing  : {N_LOTS} lot flat  |  Exit: 15:15 IST\n"
                f"Research: IS Sharpe +3.63  |  OOS +10.65  |  WR 69%"
            )

        # Expiry resolution
        self.expiry = await asyncio.to_thread(_get_suitable_expiry)

        if self.expiry:
            # Lot size from DB
            sample = await asyncio.to_thread(
                get_option_symbol,
                API_KEY, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, "PE", "ATM",
            )
            if sample:
                self.lot_size = _get_lot_size(sample)

            # Is today expiry day? (DTE=0 means today is expiry)
            today  = datetime.now().date()
            exp_dt = datetime.strptime(self.expiry, "%d%b%y").date()
            dte    = (exp_dt - today).days
            self.is_expiry_day = (dte == 0)

            logger.info(
                f"  Expiry: {self.expiry}  DTE={dte}  "
                f"{'⚠️ EXPIRY DAY (premium filter ≥₹200)' if self.is_expiry_day else 'non-expiry'}"
            )
        else:
            logger.warning("  No suitable expiry (DTE 2–7). No entries today.")

        # Daily ADX filter
        self.daily_adx = await asyncio.to_thread(_get_daily_adx)
        if self.daily_adx is not None:
            adx_ok = self.daily_adx < ADX_DAILY_MAX
            logger.info(
                f"  Daily ADX-14: {self.daily_adx:.1f}  "
                f"({'✅ < 25 — mean-reversion regime' if adx_ok else '❌ ≥ 25 — trending, entries blocked'})"
            )
        else:
            logger.warning("  Daily ADX unavailable — filter not applied today.")

    async def _warmup_history(self) -> None:
        """
        Pre-load 5 days of NIFTY 1-min history and resample to 5-min bars.
        Ensures BB(30) is warm (needs 30 × 5-min bars = 150 min) before 09:15.
        """
        logger.info(f"📡 Warming up NIFTY 5-min bars ({WARMUP_DAYS} days history)…")
        try:
            raw = await asyncio.to_thread(
                get_history, API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "1m", WARMUP_DAYS
            )
            if not raw:
                logger.warning("  ⚠️ No history returned — BB will warm on live ticks.")
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
                logger.warning("  ⚠️ History format unrecognised — skipping warm-up.")
                return

            df = df.set_index("dt").sort_index()
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["close"])

            # Resample 1-min → 5-min (aligned to :00, :05, :10, … buckets)
            df5 = df.resample("5min", offset="15min").agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
            ).dropna()

            for _, row in df5.tail(290).iterrows():
                self.bars.append({
                    "open":  float(row["open"]),
                    "high":  float(row["high"]),
                    "low":   float(row["low"]),
                    "close": float(row["close"]),
                })

            if self.bars:
                self.nifty_ltp = self.bars[-1]["close"]
                # Force instant initial state calculation so the dashboard populates instantly
                self._compute_bb_signal()
                
            logger.info(
                f"  ✅ Warm-up complete: {len(self.bars)} 5-min bars loaded.  "
                f"Last close ≈ {self.nifty_ltp:.0f}"
            )
        except Exception as e:
            logger.error(f"  History warm-up error: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  WEBSOCKET HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _subscribe(self, symbol: str, exchange: str) -> None:
        if self.ws and symbol not in self._subscribed_syms:
            try:
                await self.ws.send(json.dumps({
                    "action":   "subscribe",
                    "symbol":   symbol,
                    "exchange": exchange,
                    "mode":     2,
                }))
                self._subscribed_syms.add(symbol)
                logger.info(f"  📡 Subscribed: {symbol}")
            except Exception as e:
                logger.warning(f"  Subscribe error for {symbol}: {e}")

    async def _resubscribe_all(self) -> None:
        self._subscribed_syms.clear()
        await self._subscribe(IDX_SYMBOL, IDX_EXCHANGE)
        await self._subscribe(VIX_SYMBOL, VIX_EXCHANGE)
        if self.active_pe:
            await self._subscribe(self.active_pe["symbol"], OPT_EXCHANGE)

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE PERSISTENCE
    # ══════════════════════════════════════════════════════════════════════════

    async def _daily_adx_retry_loop(self) -> None:
        """Retry daily ADX fetch every 5 min until a value is obtained.
        Tries immediately on first call (so a failed _session_open ADX fetch
        is retried without a 5-min wait), then sleeps between subsequent tries."""
        while self.daily_adx is None:
            val = await asyncio.to_thread(_get_daily_adx)
            if val is not None:
                self.daily_adx = val
                adx_ok = val < ADX_DAILY_MAX
                logger.info(
                    f"  Daily ADX-14 (retry): {val:.1f}  "
                    f"({'✅ < 25 — MR regime' if adx_ok else '❌ ≥ 25 — trending'})"
                )
                break
            await asyncio.sleep(300)   # wait 5 minutes before next retry

    async def _state_dump_loop(self) -> None:
        while True:
            if self.active_pe and self.active_pe.get("sl_order_id"):
                try:
                    await self._check_sl_order_filled()
                except Exception as e:
                    logger.warning(f"  [PE] SL-M reconciliation check failed: {e}")
            try:
                STATE_FILE.write_text(json.dumps({
                    "strategy":     STRATEGY_NAME,
                    "last_update":  datetime.now().isoformat(),
                    "nifty_ltp":    round(self.nifty_ltp, 2),
                    "vix_ltp":      round(self.vix_ltp,   2),
                    "daily_adx":    round(self.daily_adx, 1) if self.daily_adx else None,
                    "adx_filter":   f"< {ADX_DAILY_MAX}",
                    "expiry":       self.expiry,
                    "is_expiry_day": self.is_expiry_day,
                    "lot_size":     self.lot_size,
                    "n_lots":       N_LOTS,
                    "entry_window": f"{ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')}",
                    "bb_params":    f"BB({BB_PERIOD}, {BB_STD_MULT}σ)",
                    "bars_loaded":  len(self.bars),
                    "bb_snapshot":  self.bb_snapshot,
                    "f2_mode":      F2_MODE,
                    "f2_last":      self.f2_last or None,
                    "active_trade": {
                        "symbol":     self.active_pe["symbol"],
                        "entry_prem": self.active_pe["entry_prem"],
                        "e4_target":  self.active_pe["e4_target"],
                        "sl_prem":    self.active_pe["sl_prem"],
                        "sl_order_id": self.active_pe.get("sl_order_id"),
                        "qty":        self.active_pe["qty"],
                        "entry_time": self.active_pe["entry_time"],
                    } if self.active_pe else None,
                }, default=str))
            except Exception:
                pass
            await asyncio.sleep(2)

    # ══════════════════════════════════════════════════════════════════════════
    #  MAIN WEBSOCKET LOOP
    # ══════════════════════════════════════════════════════════════════════════

    async def main_loop(self) -> None:
        """
        1. Pre-market warm-up: load 5-day NIFTY 1-min history → 5-min bars.
        2. Bootstrap session data (expiry, ADX, lot size) immediately if market
           is already open — no tick dependency, so dashboard shows real values
           even on after-hours or mid-session restarts.
        3. Start state-dump background task.
        4. Connect OpenAlgo WebSocket with auto-reconnect.
        5. Process ticks until 15:15 EOD.
        """
        await self._warmup_history()

        # Bootstrap session data immediately if we are past market open.
        # This means expiry / ADX / lot-size are set in the very first state
        # dump rather than waiting for the first live tick.
        if datetime.now().time() >= MARKET_OPEN:
            await self._session_open(self.nifty_ltp)
            # Latch only on success — otherwise the tick path retries init.
            self._session_started = bool(self.expiry)

        asyncio.create_task(self._state_dump_loop())
        asyncio.create_task(self._daily_adx_retry_loop())
        asyncio.create_task(self._watchdog.watch_loop())

        retry_delay = 5

        while True:
            now = datetime.now()
            if now.time() >= SESSION_END:
                await self._eod_close_all()
                logger.info("✅ Past 15:15 IST — shutting down.")
                break

            try:
                logger.info(f"🔌 Connecting to WebSocket: {WS_URL}")
                async with websockets.connect(
                    WS_URL, ping_interval=30, ping_timeout=60
                ) as ws:
                    self.ws = ws
                    await ws.send(json.dumps({
                        "action":  "authenticate",
                        "api_key": API_KEY,
                    }))
                    await self._resubscribe_all()

                    if not self._first_connect:
                        logger.info("WebSocket connected/reconnected.")

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
                        ts_raw = mdata.get("t")
                        ts     = (datetime.fromtimestamp(float(ts_raw))
                                  if ts_raw else datetime.now())

                        if ltp <= 0:
                            continue

                        if sym in (IDX_SYMBOL, f"{IDX_EXCHANGE}:{IDX_SYMBOL}"):
                            await self._on_index_tick(ltp, ts)

                        elif sym in (VIX_SYMBOL, f"{VIX_EXCHANGE}:{VIX_SYMBOL}"):
                            self.vix_ltp = ltp

                        elif self.active_pe and self.active_pe.get("symbol") == sym:
                            await self._on_option_tick(ltp)

            except Exception as e:
                logger.warning(
                    f"WebSocket error: {e}. Reconnecting in {retry_delay}s…"
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    bot = NiftyBBOverboughtBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("Stop signal received — bot terminated.")
