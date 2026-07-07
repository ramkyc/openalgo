"""
BANKNIFTY BB Options Bot
========================
live_trading/banknifty_bb_options_bot/banknifty_bb_options_bot.py

Research-validated (bb_options_study extended re-run, 2026-03-22):
  options_data/research/bb_options_study/results_summary.md
  IS Sharpe +2.198  |  OOS Sharpe +2.692  |  WR 86%  |  100 OOS trades
  All 10 pipeline stages PASS (Stage 10b with expiry-day exclusion applied)

Strategy:
  Watch the 1-minute premium bars of BANKNIFTY ATM CE and ATM PE.
  When a premium CLOSES above its upper Bollinger Band (BB(20, 2.0σ)) the
  premium is statistically expensive and will mean-revert. SELL that option.
  Take only the FIRST signal of the day; ignore all subsequent signals.

  The edge: option premiums that spike above their 20-bar upper BB revert to
  the cumulative session mean with ~84% win rate across 22 months of IS data.

Signal rules (ALL required):
  1. BANKNIFTY ATM CE or PE 1-min bar closes ABOVE BB(20, 2.0σ) upper band
  2. Bar close time falls within 09:30 – 14:00 IST (skip first 15 min)
  3. Premium at entry ≥ ₹10 (minimum viable premium)
  4. No position already open (one trade per session, first signal only)
  5. TODAY IS NOT a BANKNIFTY monthly expiry day — if it is, NO trades at all
  6. Market depth filter passes (3-layer check at signal confirmation time):
       Layer 1 — Spread ≤ 3% of mid (thin-market / bad-fill guard)
       Layer 2 — Top-5 bid-ask imbalance ≤ 60% bid (buyers not still dominant)
                 AND total top-5 qty ≥ 300 units (minimum liquidity gate)
       Layer 3 — Supply wall (levels 6–20) logged for confidence; non-blocking
     If no depth data is available, rule 6 is bypassed (fail-open).

  → Signal fires → depth confirmed → SELL that ATM option (CE or PE, whichever triggered)

Exit rules (first to trigger wins):
  SMA  (profit target) : 1-min bar closes AT OR BELOW the cumulative session mean
                         (mean of all 1-min closes since 09:15 today)
                         → premium has reverted → BUY to close
                         [Matches research: find_sma_reversion(direction="down")
                          in 02a_precompute_events.py]
  SL   (stop loss)     : Live premium rises to 1.5× entry premium (tick-level)
                         → BUY to close immediately
  EOD  (time stop)     : Close unconditionally at 15:14 IST

Research parameters:
  BB window      : 20 bars (1-min)
  BB std dev     : 2.0σ
  SL multiple    : 1.5× entry premium
  Instrument     : BANKNIFTY monthly options (NFO)
  Lot size       : 30 (BANKNIFTY, current exchange-registered size)
  Min DTE        : 7 days (monthly contract only)
  Entry window   : 09:30 – 14:00 IST
  EOD close      : 15:14 IST

MANDATORY EXPIRY-DAY RULE:
  DO NOT trade on BANKNIFTY monthly expiry day. Near expiry, elevated gamma
  causes erratic premium behavior that destroys the mean-reversion signal
  (expiry-day Sharpe = -1.04 in backtesting vs +2.28 on non-expiry days).
  When today is detected as an expiry day → log, notify, and exit cleanly.

Shared utilities (no duplication):
  from live_trading.api_utils import (
    get_expiry_dates, get_option_symbol, get_history, is_market_holiday
)
  live_trading.shared.atm_resolver      — get_option_ltp
  live_trading.shared.telegram_notifier — send_async
  database.token_db.get_symbol_info     — dynamic lot size
  openalgo.api                          — placesmartorder
"""

import atexit
import asyncio
import json
import logging
import os
import sys
from collections import deque
from datetime import datetime, time as dt_time
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

from live_trading.api_utils                import get_expiry_dates, get_option_symbol, get_history, is_market_holiday
from live_trading.shared.atm_resolver      import get_option_ltp
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger      import log_trade_to_db

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "banknifty_bb_options_bot.log"),
        logging.StreamHandler(),
    ],
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
STRATEGY_NAME    = "BANKNIFTY_BB_OPTIONS"

IDX_SYMBOL       = "BANKNIFTY"
IDX_EXCHANGE     = "NSE_INDEX"
VIX_SYMBOL       = "INDIAVIX"
VIX_EXCHANGE     = "NSE_INDEX"
OPT_EXCHANGE     = "NFO"

N_LOTS           = 10                # standardised: 10 lots per CLAUDE.md position-size rule
DEFAULT_LOT_SIZE = 30                # BANKNIFTY lot size

# BB parameters (research champion: BB(20, 2.0σ))
BB_PERIOD        = 20
BB_STD_MULT      = 2.0

# Minimum viable premium at entry
MIN_PREMIUM      = 10.0              # ₹10 floor to avoid micro-premium noise

# Entry distance from session mean — Stage 11 research (11_min_distance_filter.py)
# Optimal range: ₹5–₹100. Outside this range expected P&L degrades significantly.
#   < ₹5   : marginal signals with near-zero net edge after slippage (Sharpe drops)
#   > ₹100 : SL at 1.5× entry is so large that losers wipe multiple winners
MIN_ENTRY_DISTANCE = 5.0             # ₹5 minimum gap above session mean
MAX_ENTRY_DISTANCE = 100.0           # ₹100 maximum gap above session mean

# SL — Symmetry SL (Stage 2b validated, 2026-06-12)
# SL = entry + (entry − session_mean) = 1:1 R:R (risk = profit target distance)
# Floor at 1.05× prevents excessively tight stops on small-distance signals.
# IS Sharpe 2.40 vs flat_150 IS 1.55; OOS net +₹2,372 vs flat_150 OOS -₹13,765.
# High-distance trades (₹80–100): symmetry SL loses -₹128 vs flat_150 -₹26,502.
SL_FLOOR_MULT    = 1.05    # minimum stop regardless of entry distance

# Expiry selection — BANKNIFTY monthly only (min 3 DTE)
MIN_DTE          = 3
MAX_DTE          = 35               # monthly contract; 35-day upper cap

# Market depth filter constants (3-layer, added 2026-03-28)
# Cannot be backtested on historical data — validated entirely in paper trading.
DEPTH_MAX_SPREAD_PCT    = 3.0    # Layer 1: skip if bid-ask spread > 3% of mid
DEPTH_BUYER_THRESHOLD   = 0.60   # Layer 2: skip if top-5 bid imbalance > 60%
DEPTH_MIN_TOTAL_QTY     = 300    # Layer 2: skip if top-5 total qty < 300 units
DEPTH_WALL_RATIO        = 2.0    # Layer 3: wall_score threshold (logged, non-blocking)
DEPTH_LEVELS_PRIMARY    = 5      # Levels used for imbalance & spread calc
DEPTH_LEVELS_WALL       = 20     # Upper level for supply wall calc (levels 6–20)

# Session timing
MARKET_OPEN      = dt_time(9,  15)
ENTRY_START      = dt_time(9,  30)   # skip the opening 15 min (research rule)
ENTRY_END        = dt_time(14,  0)   # no new entries after 14:00
SESSION_END      = dt_time(15, 14)   # EOD close

# Dead zone — Stage 12 research (12_time_window_study.py + 12b_time_window_oos.py)
# Signals 11:00–12:30 have IS Sharpe −0.474 and OOS Sharpe −0.603.
# Excluding this window: IS Sharpe 0.441→1.030, OOS Sharpe 1.721→2.353.
# Strategy D (split window): accept 09:30–11:00 OR 12:30–14:00 only.
DEAD_ZONE_START  = dt_time(11,  0)   # dead zone begins
DEAD_ZONE_END    = dt_time(12, 30)   # dead zone ends

# Warm-up: pre-load history to initialise BB
WARMUP_DAYS      = 3
MIN_BARS_REQUIRED = BB_PERIOD + 5   # 25 completed 1-min bars before first check

# State file for dashboard / Telegram /status
STATE_FILE   = LOGS_DIR / "banknifty_bb_options_state.json"
# Per-bar decision log — one JSON line per 1-min bar close for replay/debug
DECISION_LOG = LOGS_DIR / "banknifty_bb_options_decisions.jsonl"
# PID lockfile — prevents duplicate instances
PID_FILE     = LOGS_DIR / "banknifty_bb_options_bot.pid"


# ── PID lockfile — single-instance guard ─────────────────────────────────────

def _acquire_pid_lock() -> None:
    """
    Write our PID to PID_FILE.  If a file already exists and that process is
    still alive → abort immediately (duplicate instance protection).
    Stale files from a previous crash are silently removed.
    """
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            os.kill(old_pid, 0)          # signal 0 = existence check only
            logger.error(
                f"❌  Another instance is already running (PID {old_pid}). "
                f"Refusing to start.  If this is wrong, delete: {PID_FILE}"
            )
            sys.exit(1)
        except ProcessLookupError:
            logger.warning(f"⚠️  Stale PID file (PID {old_pid}) — process is gone. Removing.")
            PID_FILE.unlink(missing_ok=True)
        except PermissionError:
            # os.kill() denied = process exists but belongs to another user
            logger.error(
                f"❌  PID {old_pid} exists (permission denied on signal). "
                f"Cannot verify — aborting to be safe.  Delete {PID_FILE} to override."
            )
            sys.exit(1)
        except ValueError:
            logger.warning("⚠️  Corrupt PID file. Removing.")
            PID_FILE.unlink(missing_ok=True)

    PID_FILE.write_text(str(os.getpid()))
    atexit.register(_release_pid_lock)
    logger.info(f"🔒 PID lock acquired (PID {os.getpid()})")


def _release_pid_lock() -> None:
    """Remove PID lockfile on exit (registered via atexit — runs on crash too)."""
    try:
        if PID_FILE.exists() and int(PID_FILE.read_text().strip()) == os.getpid():
            PID_FILE.unlink()
            logger.info("🔓 PID lock released.")
    except Exception:
        pass


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


# ── Helper: monthly expiry selection ─────────────────────────────────────────
def _get_monthly_expiry() -> str | None:
    """
    Return the nearest BANKNIFTY monthly expiry with DTE in [MIN_DTE, MAX_DTE].
    Monthly expiries are the last available in each calendar month.
    """
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
        logger.warning(f"  No BANKNIFTY expiry found with DTE {MIN_DTE}–{MAX_DTE}. "
                       f"Available: {dates[:6]}")
        return None

    # Pick the nearest qualifying expiry
    valid.sort()
    dte, expiry_str, exp_dt = valid[0]
    logger.info(f"  Monthly expiry selected: {expiry_str}  (DTE={dte})")
    return expiry_str


# ── Helper: is today expiry day? ─────────────────────────────────────────────
def _is_expiry_day() -> bool:
    """
    Returns True if today is a BANKNIFTY monthly expiry day.
    Uses the expiry list from OpenAlgo — if DTE=0 exists, today is expiry.
    MANDATORY: No trades on expiry day (research Stage 10 finding).
    """
    dates = get_expiry_dates(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, "options")
    if not dates:
        return False
    today = datetime.now().date()
    for d in dates:
        try:
            exp_dt = datetime.strptime(d, "%d%b%y").date()
            if (exp_dt - today).days == 0:
                logger.info(f"  ⚠️  TODAY IS BANKNIFTY EXPIRY DAY ({d}). No trades.")
                return True
        except ValueError:
            continue
    return False


# ══════════════════════════════════════════════════════════════════════════════
class OptionPremiumBar:
# ══════════════════════════════════════════════════════════════════════════════
    """
    Builds 1-minute OHLC bars from live option ticks (CE or PE).
    Tracks enough history for BB(20) computation.
    """

    def __init__(self, label: str):
        self.label        = label
        self.bars: deque  = deque(maxlen=200)   # completed 1-min bars (rolling, includes warmup)
        self.session_closes: list[float] = []   # today's bars only — for cumulative session mean
        self._bucket: int = -1
        self._open: float = 0.0
        self._high: float = 0.0
        self._low:  float = float("inf")
        self._last: float = 0.0

    def update(self, ltp: float, ts: datetime) -> bool:
        """Feed a tick. Returns True when a 1-min bar completes."""
        bucket = ts.hour * 60 + ts.minute   # 1-min bucket

        if self._bucket == -1:
            self._bucket = bucket
            self._open = self._high = self._low = self._last = ltp
            return False

        if bucket != self._bucket:
            # Bar closed — store it
            if self._open > 0:
                self.bars.append({
                    "open":  self._open,
                    "high":  self._high,
                    "low":   self._low,
                    "close": self._last,
                })
                self.session_closes.append(self._last)   # intraday accumulator
            # Start new bar
            self._bucket = bucket
            self._open = self._high = self._low = self._last = ltp
            return True

        self._high = max(self._high, ltp)
        self._low  = min(self._low,  ltp)
        self._last = ltp
        return False

    def compute_bb(self) -> dict | None:
        """
        Compute BB(20, 2.0σ) on completed bar closes.
        Returns dict with upper, sma, lower, close, distance_pct
        or None if insufficient bars.
        """
        if len(self.bars) < MIN_BARS_REQUIRED:
            return None

        closes = pd.Series([b["close"] for b in self.bars])
        sma    = closes.rolling(BB_PERIOD).mean().iloc[-1]
        std    = closes.rolling(BB_PERIOD).std().iloc[-1]

        if pd.isna(sma) or pd.isna(std) or std == 0:
            return None

        upper = sma + BB_STD_MULT * std
        lower = sma - BB_STD_MULT * std
        close = closes.iloc[-1]

        return {
            "upper":        round(float(upper), 2),
            "sma":          round(float(sma),   2),
            "lower":        round(float(lower),  2),
            "close":        round(float(close),  2),
            "std":          round(float(std),    2),
            "bars":         len(self.bars),
            "above_upper":  bool(close > upper),
            "below_sma":    bool(close < sma),
            "distance_pct": round((float(close) - float(upper)) / float(upper) * 100, 3),
        }

    def warmup_from_history(self, raw_bars: list[dict]) -> None:
        """
        Pre-load completed 1-min bars from history data.
        Each dict must have 'open', 'high', 'low', 'close' keys.
        Note: warmup bars are NOT added to session_closes — they are prior-day data
        and must not pollute the intraday cumulative mean used for exit decisions.
        """
        for b in raw_bars[-190:]:   # max 190 bars (well within deque limit)
            self.bars.append({
                "open":  float(b.get("open",  0)),
                "high":  float(b.get("high",  0)),
                "low":   float(b.get("low",   0)),
                "close": float(b.get("close", 0)),
            })
        logger.info(f"  [{self.label}] Warm-up: {len(self.bars)} 1-min bars loaded.")

    def reset_session(self) -> None:
        """
        Clear the intraday accumulator at the start of each trading session.
        Called by BankniftyBBOptionsBot._session_open() before warmup.
        """
        self.session_closes = []

    def session_cumulative_mean(self) -> float | None:
        """
        Cumulative mean of all 1-min bar closes since today's session open.
        This matches the research exit definition exactly:
          sma = all_closes[all_closes.index <= ts].mean()   (02a_precompute_events.py)
        Returns None if no intraday bars have completed yet.
        """
        if not self.session_closes:
            return None
        return float(np.mean(self.session_closes))


# ══════════════════════════════════════════════════════════════════════════════
class BankniftyBBOptionsBot:
# ══════════════════════════════════════════════════════════════════════════════
    """
    BB(20, 2.0σ) signal on BANKNIFTY ATM option 1-min premium bars.
    Sells whichever option (CE or PE) closes above its upper BB first.
    Research: IS Sharpe +2.198 | OOS Sharpe +2.692 | WR 86% (non-expiry days).
    """

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.ws     = None

        # Live prices
        self.bnf_ltp:   float = 0.0
        self.vix_ltp:   float = 0.0
        self.ce_ltp:    float = 0.0
        self.pe_ltp:    float = 0.0

        # 1-min premium bar builders
        self.ce_bars = OptionPremiumBar("CE")
        self.pe_bars = OptionPremiumBar("PE")

        # Option symbols for current session
        self.ce_symbol:   str | None = None
        self.pe_symbol:   str | None = None
        self.expiry:      str | None = None

        # Active short position (one per session)
        self.active_trade: dict | None = None
        self._is_closing: bool         = False   # prevents duplicate exit orders

        # Session state
        self.lot_size:        int  = DEFAULT_LOT_SIZE
        self.is_expiry_day:   bool = False
        self.signal_fired:    bool = False   # True once first signal taken

        # State machine flags
        self._session_started  = False
        self._eod_exit_done    = False
        self._first_connect    = True
        self._subscribed_syms: set[str] = set()

        # Dashboard snapshots
        self.ce_snapshot: dict = {}
        self.pe_snapshot: dict = {}

        # Market depth snapshots (populated from mode-3 WebSocket ticks)
        # Format: {bids: [{price, qty}, ...], asks: [{price, qty}, ...]}
        self.ce_depth: dict = {}
        self.pe_depth: dict = {}
        self._depth_logged: set[str] = set()  # track first-receive for format logging

        # Restore any active trade from a previous run today
        self._restore_state()

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE RESTORE (mid-session restart recovery)
    # ══════════════════════════════════════════════════════════════════════════

    def _restore_state(self) -> None:
        """
        On startup, reload state saved by _state_dump_loop.
        Restores active_trade and signal_fired only if the state file was written
        today — prevents ghost positions from a previous trading day.
        """
        if not STATE_FILE.exists():
            return
        try:
            state = json.loads(STATE_FILE.read_text())
            last_update_str = state.get("last_update", "")
            if not last_update_str:
                return
            last_update = datetime.fromisoformat(last_update_str)
            if last_update.date() != datetime.now().date():
                logger.info("_restore_state: state file is from a previous day — skipping restore")
                return

            phase = state.get("phase", "")

            # Restore symbols so tick routing works before _session_open fires
            self.ce_symbol = state.get("ce_symbol")
            self.pe_symbol = state.get("pe_symbol")
            self.expiry    = state.get("expiry")

            if phase == "ACTIVE":
                at = state.get("active_trade")
                if at:
                    self.active_trade = at
                    self.signal_fired = True
                    self._is_closing  = False
                    logger.warning(
                        f"🔄 Restored ACTIVE trade from state file: "
                        f"{at.get('opt_type')} {at.get('symbol')} "
                        f"entry={at.get('entry_prem')} sl={at.get('sl_prem')}"
                    )
            elif phase == "COMPLETED":
                self.signal_fired = True
                logger.info("🔄 Restored COMPLETED session — signal_fired=True, no new entries today")

        except Exception as e:
            logger.warning(f"_restore_state: could not read state file: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  SESSION INITIALISATION
    # ══════════════════════════════════════════════════════════════════════════

    async def _session_open(self, bnf_spot: float) -> None:
        """Fetch expiry and resolve option symbols if not already active."""
        # 0. Holiday Check
        if is_market_holiday(API_KEY):
            logger.info("⛔ Market Holiday detected. Skipping session initialization.")
            return

        # 1. Check if today is expiry day (mandatory skip rule).
        # 2. Resolve monthly expiry and ATM CE / PE symbols.
        # 3. Pre-subscribe both options for tick feed.
        # 4. Warm up BB bars from history.
        logger.info(f"🔔 Session open. BANKNIFTY spot ≈ {bnf_spot:.0f}")

        # 0.1 Online Notification
        if self._first_connect:
            self._first_connect = False
            await send_async(
                f"🤖 *BANKNIFTY BB Options Bot — Online*\n"
                f"Signal  : ATM CE or PE 1-min close > BB({BB_PERIOD},{BB_STD_MULT}σ) upper\n"
                f"Depth   : Spread≤{DEPTH_MAX_SPREAD_PCT}%  Imbal≤{DEPTH_BUYER_THRESHOLD}  "
                f"MinQty={DEPTH_MIN_TOTAL_QTY}  Wall={DEPTH_WALL_RATIO}× (levels 6–{DEPTH_LEVELS_WALL})\n"
                f"Window  : {ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')} IST\n"
                f"Exit    : Session-mean reversion  |  SL: symmetry (1:1 R:R, floor {SL_FLOOR_MULT}×)  |  EOD: 15:14\n"
                f"Sizing  : {N_LOTS} lot flat (qty={N_LOTS * DEFAULT_LOT_SIZE})\n"
                f"Expiry  : Monthly NFO (≥{MIN_DTE} DTE). Expiry-day skip: ON\n"
                f"Research: IS Sharpe +2.20  |  OOS +2.69  |  WR 86%"
            )

        # ── Expiry day check (MANDATORY — no trades on expiry day) ────────────
        self.is_expiry_day = await asyncio.to_thread(_is_expiry_day)
        if self.is_expiry_day:
            logger.info(
                "⛔ BANKNIFTY monthly expiry day detected. "
                "Strategy rule: NO trades on expiry day. "
                "Bot will run in monitor-only mode."
            )
            await send_async(
                f"⛔ *BANKNIFTY BB Options — Expiry Day Skip*\n"
                f"Today is a BANKNIFTY monthly expiry day.\n"
                f"_Strategy rule: no trades on expiry day (Stage 10 finding)._\n"
                f"Bot is running in monitor-only mode."
            )
            return   # stop setup — no subscriptions needed without trading

        # ── Monthly expiry resolution ─────────────────────────────────────────
        self.expiry = await asyncio.to_thread(_get_monthly_expiry)
        if not self.expiry:
            logger.warning("  No suitable monthly expiry (DTE 7–45). No entries today.")
            return

        # ── Resolve ATM CE and PE symbols ────────────────────────────────────
        self.ce_symbol = await asyncio.to_thread(
            get_option_symbol,
            API_KEY, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, "CE", "ATM",
        )
        self.pe_symbol = await asyncio.to_thread(
            get_option_symbol,
            API_KEY, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, "PE", "ATM",
        )

        if not self.ce_symbol or not self.pe_symbol:
            logger.error(
                f"  ATM symbol resolution failed "
                f"(spot={bnf_spot:.0f}, expiry={self.expiry}). "
                f"CE={self.ce_symbol}  PE={self.pe_symbol}"
            )
            return

        logger.info(f"  ATM CE: {self.ce_symbol}  |  ATM PE: {self.pe_symbol}  |  Expiry: {self.expiry}")

        # ── Lot size ──────────────────────────────────────────────────────────
        self.lot_size = _get_lot_size(self.ce_symbol)

        # ── Subscribe both options for tick feed (mode=3 for full depth) ────────
        await self._subscribe(self.ce_symbol, OPT_EXCHANGE, mode=3)
        await self._subscribe(self.pe_symbol, OPT_EXCHANGE, mode=3)

        # ── Reset intraday session accumulators (before warmup) ───────────────
        # Must be cleared BEFORE warmup so prior-day history bars are not
        # included in the session cumulative mean used for exit decisions.
        self.ce_bars.reset_session()
        self.pe_bars.reset_session()

        # ── Warm-up BB bars from history ──────────────────────────────────────
        await self._warmup_history()

        # ── Resume check: close immediately if exit already triggered ─────────
        # Covers the case where the bot was restarted after an exit condition
        # was already met (LTP ≤ SMA or LTP ≥ SL) — we shouldn't wait for the
        # next bar close; we should exit right now using a live REST price.
        await self._check_resume_exit()

        logger.info(
            f"  Session ready. "
            f"CE bars={len(self.ce_bars.bars)}  PE bars={len(self.pe_bars.bars)}  "
            f"BB({BB_PERIOD},{BB_STD_MULT}σ)  SL=symmetry(floor {SL_FLOOR_MULT}×)"
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  BB WARM-UP FROM HISTORY
    # ══════════════════════════════════════════════════════════════════════════

    async def _warmup_history(self) -> None:
        """
        Pre-load 3 days of 1-min history for both ATM CE and PE to warm BB(20).
        BANKNIFTY is more volatile — 3 days gives 3×375 = 1,125 bars, plenty.
        """
        for sym, builder in [(self.ce_symbol, self.ce_bars), (self.pe_symbol, self.pe_bars)]:
            if not sym:
                continue
            logger.info(f"  📡 [{builder.label}] Warming up from {sym} history…")
            try:
                raw = await asyncio.to_thread(
                    get_history, API_KEY, sym, OPT_EXCHANGE, "1m", WARMUP_DAYS
                )
                if not raw:
                    logger.warning(f"  [{builder.label}] No history — BB will warm on live ticks.")
                    continue

                df = pd.DataFrame(raw)
                if "timestamp" in df.columns:
                    df["dt"] = (
                        pd.to_datetime(df["timestamp"], unit="s", utc=True)
                        .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
                    )
                elif "date" in df.columns:
                    df["dt"] = pd.to_datetime(df["date"])
                else:
                    logger.warning(f"  [{builder.label}] Unrecognised history format — skipping.")
                    continue

                df = df.sort_values("dt")
                for col in ["open", "high", "low", "close"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["close"])

                bars = df[["open", "high", "low", "close"]].to_dict("records")
                builder.warmup_from_history(bars)

                # ── Seed session_closes with today's completed bars ───────────
                # warmup_from_history() intentionally skips session_closes to
                # avoid polluting the intraday mean with prior-day data.  But
                # after a mid-session restart we need to replay today's bars so
                # the exit SMA is anchored at 09:15, not at restart time.
                today_start = pd.Timestamp.now().normalize()   # midnight today IST
                today_df = df[df["dt"] >= today_start]
                if not today_df.empty:
                    builder.session_closes.extend(today_df["close"].tolist())
                    logger.info(
                        f"  [{builder.label}] Session mean seeded: "
                        f"{len(today_df)} today's bars  "
                        f"(mean ≈ ₹{today_df['close'].mean():.2f})"
                    )

                # Ensure the dashboard populates instantaneously upon restart
                if builder.label == "CE":
                    self.ce_snapshot = builder.compute_bb() or {}
                elif builder.label == "PE":
                    self.pe_snapshot = builder.compute_bb() or {}

                logger.info(
                    f"  [{builder.label}] ✅ Warm-up: {len(builder.bars)} bars. "
                    f"Last close ≈ ₹{builder.bars[-1]['close']:.2f}" if builder.bars else ""
                )
            except Exception as e:
                logger.error(f"  [{builder.label}] History warm-up error: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  MARKET DEPTH FILTER (added 2026-03-28)
    # ══════════════════════════════════════════════════════════════════════════

    def _parse_depth(self, mdata: dict) -> dict | None:
        """
        Parse depth data from an OpenAlgo WebSocket message data payload.
        Handles two common layouts:
          Format A: mdata["depth"]["buy"] / ["sell"]  (Fyers / most brokers)
          Format B: mdata["bids"] / mdata["asks"]     (some normalised feeds)
        Returns {bids: [{price, qty}, ...], asks: [{price, qty}, ...]} or None.
        """
        raw_bids, raw_asks = [], []

        if "depth" in mdata:
            d = mdata["depth"]
            raw_bids = d.get("buy", d.get("bids", []))
            raw_asks = d.get("sell", d.get("asks", []))
        elif "bids" in mdata or "asks" in mdata:
            raw_bids = mdata.get("bids", [])
            raw_asks = mdata.get("asks", [])
        else:
            return None

        def normalise(levels: list) -> list[dict]:
            out = []
            for lvl in levels:
                price = float(lvl.get("price", lvl.get("p", 0)))
                qty   = int(lvl.get("quantity", lvl.get("qty", lvl.get("q", 0))))
                if price > 0:
                    out.append({"price": price, "qty": qty})
            return out

        bids = normalise(raw_bids)
        asks = normalise(raw_asks)
        if not bids and not asks:
            return None
        return {"bids": bids, "asks": asks}

    def _depth_confirms_entry(self, opt_type: str) -> bool:
        """
        3-layer market depth filter applied at signal confirmation time.

        Layer 1 — Spread gate (hard fail):
          If best_ask − best_bid > DEPTH_MAX_SPREAD_PCT % of mid → skip.
          Protects against thin-market artefacts and poor fill conditions.

        Layer 2 — Bid-ask imbalance (hard fail):
          If top-DEPTH_LEVELS_PRIMARY bid qty > DEPTH_BUYER_THRESHOLD of total
          qty → buyers still dominant → premium likely to keep rising → skip.
          Also gate: total qty < DEPTH_MIN_TOTAL_QTY → market too thin → skip.

        Layer 3 — Supply wall (logged, non-blocking):
          ask/bid ratio on levels 6–20. A wall_score ≥ DEPTH_WALL_RATIO means
          supply is pre-positioned; logged as additional confidence metric.

        Returns True (proceed with trade) or False (skip this bar).
        Fail-open: if no depth snapshot exists, returns True.
        """
        depth = self.ce_depth if opt_type == "CE" else self.pe_depth

        if not depth:
            logger.info(
                f"  [{opt_type}] Depth filter: no snapshot yet — "
                f"proceeding without depth check (fail-open)."
            )
            return True

        bids = depth.get("bids", [])
        asks = depth.get("asks", [])

        if not bids or not asks:
            logger.info(f"  [{opt_type}] Depth filter: empty book — fail-open.")
            return True

        best_bid = bids[0]["price"]
        best_ask = asks[0]["price"]
        mid      = (best_bid + best_ask) / 2.0

        # Layer 1 — Spread gate
        spread_pct = ((best_ask - best_bid) / mid * 100) if mid > 0 else 0.0
        if spread_pct > DEPTH_MAX_SPREAD_PCT:
            logger.info(
                f"  [{opt_type}] ⛔ Depth Layer 1 BLOCKED: spread={spread_pct:.2f}% "
                f"> {DEPTH_MAX_SPREAD_PCT}%  (bid={best_bid}, ask={best_ask})"
            )
            return False

        # Layer 2 — Bid-ask imbalance (top DEPTH_LEVELS_PRIMARY levels)
        top_bid_qty = sum(b["qty"] for b in bids[:DEPTH_LEVELS_PRIMARY])
        top_ask_qty = sum(a["qty"] for a in asks[:DEPTH_LEVELS_PRIMARY])
        total_qty   = top_bid_qty + top_ask_qty

        if total_qty < DEPTH_MIN_TOTAL_QTY:
            logger.info(
                f"  [{opt_type}] ⛔ Depth Layer 2 BLOCKED: thin market "
                f"(top-{DEPTH_LEVELS_PRIMARY} total qty={total_qty} < {DEPTH_MIN_TOTAL_QTY})"
            )
            return False

        imbalance = top_bid_qty / total_qty
        if imbalance > DEPTH_BUYER_THRESHOLD:
            logger.info(
                f"  [{opt_type}] ⛔ Depth Layer 2 BLOCKED: buyers dominant "
                f"(imbalance={imbalance:.2f} > {DEPTH_BUYER_THRESHOLD})  "
                f"bid_qty={top_bid_qty}  ask_qty={top_ask_qty}"
            )
            return False

        # Layer 3 — Supply wall (levels DEPTH_LEVELS_PRIMARY+1 to DEPTH_LEVELS_WALL)
        wall_bid_qty = sum(b["qty"] for b in bids[DEPTH_LEVELS_PRIMARY:DEPTH_LEVELS_WALL])
        wall_ask_qty = sum(a["qty"] for a in asks[DEPTH_LEVELS_PRIMARY:DEPTH_LEVELS_WALL])
        wall_score   = (wall_ask_qty / wall_bid_qty) if wall_bid_qty > 0 else float("inf")

        logger.info(
            f"  [{opt_type}] ✅ Depth filter PASSED — "
            f"spread={spread_pct:.2f}%  imbalance={imbalance:.2f}  "
            f"top5(bid={top_bid_qty}, ask={top_ask_qty})  "
            f"wall_score={wall_score:.1f}x ({'strong supply' if wall_score >= DEPTH_WALL_RATIO else 'neutral'})"
        )
        return True

    # ══════════════════════════════════════════════════════════════════════════
    #  SIGNAL ENGINE — called on every completed 1-min bar
    # ══════════════════════════════════════════════════════════════════════════

    async def _check_signals(self) -> None:
        """
        Run BB signal check on both CE and PE bars.
        Enters trade if premium closes above upper BB (first signal only).
        """
        if self.active_trade or self.signal_fired:
            return   # already traded or signal already taken today

        now = datetime.now().time()
        if not (ENTRY_START <= now <= ENTRY_END):
            return   # outside entry window
        if DEAD_ZONE_START <= now < DEAD_ZONE_END:
            return   # dead zone 11:00–12:30: IS Sharpe −0.47, OOS −0.60 (Stage 12)

        # Check CE
        ce_bb = self.ce_bars.compute_bb()
        if ce_bb:
            self.ce_snapshot = ce_bb
            if ce_bb["above_upper"] and self.ce_ltp >= MIN_PREMIUM:
                logger.info(
                    f"📶 BB signal — CE: ₹{ce_bb['close']:.2f} > upper ₹{ce_bb['upper']:.2f} "
                    f"(BB({BB_PERIOD},{BB_STD_MULT}σ)  dist={ce_bb['distance_pct']:+.2f}%)"
                )
                if not self._depth_confirms_entry("CE"):
                    logger.info(
                        f"  [CE] BB signal present but depth filter BLOCKED entry. "
                        f"Will re-evaluate on next bar close."
                    )
                    return   # skip this bar; signal_fired stays False → recheck next bar
                # Guard: only enter if close > session mean — ensures the exit
                # condition (close <= mean) is not already satisfied at entry.
                # Happens after mid-session restarts when session mean is seeded
                # from morning bars at higher price levels.
                ce_sess_mean = self.ce_bars.session_cumulative_mean()
                if ce_sess_mean is not None:
                    ce_distance = ce_bb["close"] - ce_sess_mean
                    if ce_distance < MIN_ENTRY_DISTANCE:
                        logger.info(
                            f"  [CE] Signal skipped — distance ₹{ce_distance:.2f} < ₹{MIN_ENTRY_DISTANCE:.0f} "
                            f"(close ₹{ce_bb['close']:.2f}, session mean ₹{ce_sess_mean:.2f}, no edge after slippage)"
                        )
                        return
                    if ce_distance > MAX_ENTRY_DISTANCE:
                        logger.info(
                            f"  [CE] Signal skipped — distance ₹{ce_distance:.2f} > ₹{MAX_ENTRY_DISTANCE:.0f} "
                            f"(sym SL would be ₹{ce_bb['close'] * 2:.0f}; loser risk exceeds edge)"
                        )
                        return
                await self._enter_trade("CE", self.ce_symbol, ce_bb)
                return   # done for today

        # Check PE (only if CE didn't fire)
        pe_bb = self.pe_bars.compute_bb()
        if pe_bb:
            self.pe_snapshot = pe_bb
            if pe_bb["above_upper"] and self.pe_ltp >= MIN_PREMIUM:
                logger.info(
                    f"📶 BB signal — PE: ₹{pe_bb['close']:.2f} > upper ₹{pe_bb['upper']:.2f} "
                    f"(BB({BB_PERIOD},{BB_STD_MULT}σ)  dist={pe_bb['distance_pct']:+.2f}%)"
                )
                if not self._depth_confirms_entry("PE"):
                    logger.info(
                        f"  [PE] BB signal present but depth filter BLOCKED entry. "
                        f"Will re-evaluate on next bar close."
                    )
                    return   # skip this bar; recheck next bar
                # Guard: only enter if close > session mean — ensures the exit
                # condition (close <= mean) is not already satisfied at entry.
                pe_sess_mean = self.pe_bars.session_cumulative_mean()
                if pe_sess_mean is not None:
                    pe_distance = pe_bb["close"] - pe_sess_mean
                    if pe_distance < MIN_ENTRY_DISTANCE:
                        logger.info(
                            f"  [PE] Signal skipped — distance ₹{pe_distance:.2f} < ₹{MIN_ENTRY_DISTANCE:.0f} "
                            f"(close ₹{pe_bb['close']:.2f}, session mean ₹{pe_sess_mean:.2f}, no edge after slippage)"
                        )
                        return
                    if pe_distance > MAX_ENTRY_DISTANCE:
                        logger.info(
                            f"  [PE] Signal skipped — distance ₹{pe_distance:.2f} > ₹{MAX_ENTRY_DISTANCE:.0f} "
                            f"(sym SL would be ₹{pe_bb['close'] * 2:.0f}; loser risk exceeds edge)"
                        )
                        return
                await self._enter_trade("PE", self.pe_symbol, pe_bb)

    # ══════════════════════════════════════════════════════════════════════════
    #  RESUME-SAFE EXIT CHECK (called once after warmup on every session open)
    # ══════════════════════════════════════════════════════════════════════════

    async def _check_resume_exit(self) -> None:
        """
        Called once immediately after _warmup_history() on every session open,
        including bot restarts during the trading day.

        If an active trade exists and its exit condition is already met
        (SL breach OR premium ≤ SMA target) — close it immediately using the
        current REST LTP without waiting for the next 1-min bar close.

        This prevents positions from lingering indefinitely when the bot is
        restarted while an exit should have triggered.
        """
        if not self.active_trade:
            return

        opt_type = self.active_trade["opt_type"]
        symbol   = self.active_trade["symbol"]
        sl_prem  = self.active_trade["sl_prem"]

        # Use the current session mean if live bars exist; otherwise fall back to
        # the entry-time session mean stored in active_trade (set at order entry).
        builder   = self.ce_bars if opt_type == "CE" else self.pe_bars
        sess_mean = builder.session_cumulative_mean()
        sma_tgt   = (round(sess_mean, 2) if sess_mean is not None
                     else self.active_trade["sma_target"])

        logger.info(
            f"  🔄 [RESUME CHECK] Active trade found on startup: "
            f"{opt_type} {symbol}  entry=₹{self.active_trade['entry_prem']:.2f}  "
            f"SL=₹{sl_prem:.2f}  session mean=₹{sma_tgt:.2f}"
        )

        # Fetch live LTP via REST
        ltp = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        if ltp <= 0:
            logger.warning(
                f"  [RESUME CHECK] Could not fetch live LTP for {symbol}. "
                f"Will fall back to bar-close exit checks."
            )
            return

        logger.info(
            f"  [RESUME CHECK] Live LTP = ₹{ltp:.2f}  "
            f"SL = ₹{sl_prem:.2f}  session mean = ₹{sma_tgt:.2f}"
        )

        # ── SL check ─────────────────────────────────────────────────────────
        if ltp >= sl_prem:
            logger.warning(
                f"  🛑 [RESUME] STOP LOSS already breached on restart: "
                f"LTP ₹{ltp:.2f} ≥ SL ₹{sl_prem:.2f}. Closing immediately."
            )
            await self._close_trade(ltp, "SL 1.5× (resume check)")
            return

        # ── Session mean / profit target check ───────────────────────────────
        if ltp <= sma_tgt:
            logger.info(
                f"  🎯 [RESUME] Session mean already hit on restart: "
                f"LTP ₹{ltp:.2f} ≤ session mean ₹{sma_tgt:.2f}. Closing immediately."
            )
            await self._close_trade(ltp, "SMA reversion (resume check)")
            return

        logger.info(
            f"  [RESUME CHECK] No exit triggered (LTP ₹{ltp:.2f} is between "
            f"session mean ₹{sma_tgt:.2f} and SL ₹{sl_prem:.2f}). Continuing to monitor."
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  SMA EXIT CHECK (called on every completed 1-min bar)
    # ══════════════════════════════════════════════════════════════════════════

    async def _check_sma_exit(self) -> None:
        """
        Check if the active position's premium has closed AT OR BELOW the
        cumulative session mean — matching the research exit definition exactly:

          find_sma_reversion(direction="down"):
            sma = all_closes[all_closes.index <= ts].mean()
            if close <= sma: exit

        The cumulative session mean is the mean of every completed 1-min bar
        since today's session open (09:15 IST). It is anchored to the session's
        running average and does NOT drift up/down with recent bars the way a
        rolling BB-SMA does. This prevents false reversion signals in trending
        sessions where the rolling SMA rises to meet a rising premium.

        Called on every completed 1-min bar for the traded option.
        """
        if not self.active_trade:
            return

        opt_type  = self.active_trade["opt_type"]
        builder   = self.ce_bars if opt_type == "CE" else self.pe_bars
        bb        = builder.compute_bb()
        sess_mean = builder.session_cumulative_mean()

        if bb is None or sess_mean is None:
            return   # insufficient data — wait for more bars

        close = bb["close"]
        if close <= sess_mean:
            ltp = self.ce_ltp if opt_type == "CE" else self.pe_ltp
            exit_prem = ltp if ltp > 0 else close
            logger.info(
                f"🎯 [{opt_type}] SMA REVERSION: close ₹{close:.2f} ≤ "
                f"session mean ₹{sess_mean:.2f} → closing trade."
            )
            await self._close_trade(exit_prem, "SMA reversion")

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE ENTRY
    # ══════════════════════════════════════════════════════════════════════════

    async def _enter_trade(self, opt_type: str, symbol: str, bb: dict) -> None:
        """SELL the given option at market. Sets up SL and exit targets."""
        if self.active_trade:
            return

        if not symbol:
            logger.warning(f"  [{opt_type}] Symbol is None — cannot enter.")
            return

        # Fetch current live premium
        ltp = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        if ltp <= 0:
            logger.warning(f"  [{opt_type}] LTP = 0 for {symbol}. Skipping.")
            return

        if ltp < MIN_PREMIUM:
            logger.info(
                f"  [{opt_type}] Premium ₹{ltp:.2f} < ₹{MIN_PREMIUM:.0f} minimum. Skipping."
            )
            return

        qty      = N_LOTS * self.lot_size

        # Profit target: entry-time cumulative session mean (matches research).
        # Falls back to rolling BB-SMA if no session bars exist yet (edge case).
        builder   = self.ce_bars if opt_type == "CE" else self.pe_bars
        sess_mean = builder.session_cumulative_mean()
        sma       = round(sess_mean, 2) if sess_mean is not None else bb["sma"]

        # Symmetry SL: risk the same distance as the profit target (1:1 R:R).
        # SL = entry + (entry − session_mean)  [= 2 × entry − sma_target]
        # Floor at SL_FLOOR_MULT prevents dangerously tight stops on close-distance
        # entries (₹5–₹45 from mean).
        entry_distance = ltp - sma
        symmetry_sl    = ltp + entry_distance          # 1:1 R:R
        sl_prem        = max(round(ltp * SL_FLOOR_MULT, 2), round(symmetry_sl, 2))

        logger.info(
            f"  [{opt_type}] Entering: {symbol}  LTP=₹{ltp:.2f}  "
            f"SL=₹{sl_prem:.2f} (sym, dist=₹{entry_distance:.1f})  "
            f"session mean=₹{sma:.2f}  qty={qty}"
        )

        # Lock signal BEFORE placing order — "first signal only" must hold even
        # if the broker rejects the order (e.g. IP whitelist, rate limit, etc.).
        # Without this, every rejected order left signal_fired=False and the next
        # bar fired another entry attempt.
        self.signal_fired = True

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
            logger.error(f"  [{opt_type}] placesmartorder exception: {e}")
            return

        if res.get("status") == "success":
            self.active_trade = {
                "opt_type":   opt_type,
                "symbol":     symbol,
                "entry_prem": ltp,
                "sl_prem":    sl_prem,
                "sma_target": sma,
                "qty":        qty,
                "lot_size":   self.lot_size,
                "order_id":   str(res.get("orderid", "")),
                "entry_time": datetime.now().isoformat(),
            }

            logger.info(
                f"✅ [{opt_type}] SOLD {symbol} @ ₹{ltp:.2f}  "
                f"(1 lot, qty={qty}, order={res.get('orderid')})"
            )
            await send_async(
                f"📉 *BANKNIFTY BB Options — ENTRY*\n"
                f"Sold `{symbol}`  ({opt_type}, 1 lot, qty={qty})\n"
                f"Entry premium  : ₹{ltp:.2f}\n"
                f"Profit target  : ₹{sma:.2f}  (session mean reversion)\n"
                f"Stop loss      : ₹{sl_prem:.2f}  (1.5× entry)\n"
                f"Time stop      : 15:14 IST\n"
                f"BANKNIFTY      : {self.bnf_ltp:.0f}  |  VIX: {self.vix_ltp:.2f}\n"
                f"BB upper       : ₹{bb['upper']:.2f}  |  SMA: ₹{sma:.2f}\n"
                f"_Signal: {datetime.now().strftime('%H:%M')}_"
            )
        else:
            logger.error(f"  [{opt_type}] Order rejected: {res}")

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE EXIT
    # ══════════════════════════════════════════════════════════════════════════

    async def _close_trade(self, exit_prem: float, reason: str) -> None:
        """Buy back the short option to flatten the position."""
        if not self.active_trade or self._is_closing:
            return

        self._is_closing = True
        opt_type = self.active_trade["opt_type"]
        symbol   = self.active_trade["symbol"]
        qty      = self.active_trade["qty"]

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
            logger.error(f"  [{opt_type}] Close order exception: {e}")
            self._is_closing = False
            return

        order_ok = res.get("status") == "success"
        if not order_ok:
            # Position may already be closed by sandbox auto-squareoff (15:15 MIS cutoff).
            # Do NOT return early — always log the trade so performance.db stays accurate.
            logger.warning(
                f"  [{opt_type}] Exit order non-success (likely auto-squareoff already "
                f"closed position): {res}  — logging trade and clearing state."
            )

        # Capture trade fields before clearing state.
        entry_prem  = self.active_trade["entry_prem"]
        _entry_time = self.active_trade.get("entry_time")
        _order_id   = self.active_trade.get("order_id")
        _lot_size   = self.active_trade.get("lot_size", qty)
        gross      = (entry_prem - exit_prem) * qty
        won        = gross > 0
        emoji      = "🟢" if won else "🔴"
        decay_pct  = (entry_prem - exit_prem) / entry_prem * 100

        # Clear state unconditionally (prevents _is_closing lock on order failure).
        self.active_trade = None
        self._is_closing = False

        auto_sq_note = " _(closed by auto-squareoff)_" if not order_ok else ""
        logger.info(
            f"{emoji} [{opt_type}] CLOSED {symbol} @ ₹{exit_prem:.2f}  "
            f"reason={reason}  gross=₹{gross:,.0f}  change={decay_pct:.1f}%"
        )
        await send_async(
            f"{emoji} *BANKNIFTY BB Options — EXIT ({reason})*\n"
            f"Symbol  : `{symbol}`  ({opt_type})\n"
            f"Entry   : ₹{entry_prem:.2f}  →  Exit: ₹{exit_prem:.2f}  "
            f"({decay_pct:+.1f}%)\n"
            f"Gross P&L : ₹{gross:,.0f}  (1 lot)\n"
            f"_Closed at {datetime.now().strftime('%H:%M:%S')}{auto_sq_note}_"
        )
        log_trade_to_db(
            bot_name      = "banknifty_bb_options_bot",
            instrument    = "BANKNIFTY",
            option_symbol = symbol,
            option_type   = opt_type,
            entry_time    = _entry_time,
            exit_time     = datetime.now(),
            entry_premium = entry_prem,
            exit_premium  = exit_prem,
            exit_reason   = reason,
            quantity      = qty,
            lots          = N_LOTS,
            lot_size      = _lot_size,
            gross_pnl     = gross,
            order_id      = _order_id,
        )

    async def _eod_close_all(self) -> None:
        """Unconditional close at 15:14 IST (time stop)."""
        if self._eod_exit_done:
            return
        self._eod_exit_done = True
        logger.info("🔔 15:14 IST time stop — closing all positions.")
        if self.active_trade:
            opt_type = self.active_trade["opt_type"]
            ltp = await asyncio.to_thread(
                get_option_ltp,
                self.active_trade["symbol"], OPT_EXCHANGE, API_KEY
            )
            exit_p = ltp if ltp > 0 else self.active_trade["entry_prem"]
            await self._close_trade(exit_p, "EOD 15:14")

    # ══════════════════════════════════════════════════════════════════════════
    #  TICK HANDLERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _on_index_tick(self, ltp: float) -> None:
        """Process index ticks: trigger session open and track spot."""
        if ltp <= 0:
            return

        self.bnf_ltp = ltp

        # Also retry _session_open if it was called at startup (before WS was
        # connected) and failed to select ATM — ce_symbol will be None in that
        # case.  Without this retry the bot would sit in WARMUP forever after a
        # mid-session restart where OpenAlgo was not yet ready at boot time.
        atm_missing = self._session_started and self.ce_symbol is None and not self.is_expiry_day
        if (not self._session_started or atm_missing) and datetime.now().time() >= MARKET_OPEN:
            if is_market_holiday(API_KEY):
                logger.warning("⚠️ Tick received but today is a holiday. Ignoring.")
                return
            if atm_missing:
                logger.info("⚠️  ATM was not selected at startup — retrying _session_open with live spot.")
            self._session_started = True
            await self._session_open(ltp)

    async def _on_option_tick(self, symbol: str, ltp: float, ts: datetime) -> None:
        """Called for CE and PE option ticks."""
        if ltp <= 0:
            return

        is_ce = (symbol == self.ce_symbol)
        is_pe = (symbol == self.pe_symbol)

        if is_ce:
            self.ce_ltp = ltp
        elif is_pe:
            self.pe_ltp = ltp

        # ── SL check (tick-level for speed) ──────────────────────────────────
        if self.active_trade and not self._is_closing:
            trade_sym = self.active_trade["symbol"]
            if symbol == trade_sym and ltp >= self.active_trade["sl_prem"]:
                logger.warning(
                    f"🛑 [{self.active_trade['opt_type']}] STOP LOSS: "
                    f"LTP=₹{ltp:.2f} ≥ SL=₹{self.active_trade['sl_prem']:.2f} "
                    f"(1.5× entry ₹{self.active_trade['entry_prem']:.2f})"
                )
                await self._close_trade(ltp, "SL 1.5×")
                return

        # ── Feed tick into 1-min bar builder ─────────────────────────────────
        bar_closed = False
        if is_ce:
            bar_closed = self.ce_bars.update(ltp, ts)
        elif is_pe:
            bar_closed = self.pe_bars.update(ltp, ts)

        # ── On bar close: check signal / SMA exit ────────────────────────────
        if bar_closed and not self.is_expiry_day:
            await self._check_sma_exit()
            if not self.active_trade and not self.signal_fired:
                await self._check_signals()
            # ── Per-bar decision log (one line per minute for replay/debug) ──
            # Only write on CE bar close to avoid duplicate lines per minute.
            if is_ce:
                try:
                    ce_sm = self.ce_bars.session_cumulative_mean()
                    pe_sm = self.pe_bars.session_cumulative_mean()
                    now_t = datetime.now().time()
                    record = {
                        "ts":              datetime.now().isoformat(timespec="seconds"),
                        "phase":           "ACTIVE" if self.active_trade else ("COMPLETED" if self.signal_fired else "MONITORING"),
                        "signal_fired":    self.signal_fired,
                        "in_window":       ENTRY_START <= now_t <= ENTRY_END,
                        "ce_bb":           self.ce_snapshot,
                        "pe_bb":           self.pe_snapshot,
                        "ce_session_mean": round(ce_sm, 2) if ce_sm else None,
                        "pe_session_mean": round(pe_sm, 2) if pe_sm else None,
                        "ce_above_upper":  self.ce_snapshot.get("above_upper") if self.ce_snapshot else None,
                        "pe_above_upper":  self.pe_snapshot.get("above_upper") if self.pe_snapshot else None,
                        "ce_gt_mean":      (self.ce_snapshot.get("close", 0) > ce_sm) if (self.ce_snapshot and ce_sm) else None,
                        "pe_gt_mean":      (self.pe_snapshot.get("close", 0) > pe_sm) if (self.pe_snapshot and pe_sm) else None,
                        "active_trade":    self.active_trade,
                    }
                    with open(DECISION_LOG, "a") as f:
                        f.write(json.dumps(record) + "\n")
                except Exception:
                    pass  # never let logging kill the bot

    # ══════════════════════════════════════════════════════════════════════════
    #  WEBSOCKET HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _subscribe(self, symbol: str, exchange: str, mode: int = 2) -> None:
        """
        Subscribe a symbol on the WebSocket.
        mode=2 : LTP + quote (index, VIX)
        mode=3 : Full depth snapshot — used for ATM CE/PE to power the depth filter
        """
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
                logger.warning(f"  Subscribe error for {symbol}: {e}")

    async def _resubscribe_all(self) -> None:
        self._subscribed_syms.clear()
        await self._subscribe(IDX_SYMBOL, IDX_EXCHANGE)           # mode=2
        await self._subscribe(VIX_SYMBOL, VIX_EXCHANGE)           # mode=2
        if self.ce_symbol:
            await self._subscribe(self.ce_symbol, OPT_EXCHANGE, mode=3)
        if self.pe_symbol:
            await self._subscribe(self.pe_symbol, OPT_EXCHANGE, mode=3)

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE PERSISTENCE (dashboard / Telegram /status)
    # ══════════════════════════════════════════════════════════════════════════

    async def _state_dump_loop(self) -> None:
        while True:
            try:
                if self.is_expiry_day:
                    phase = "SKIP_EXPIRY"
                elif self.active_trade:
                    phase = "ACTIVE"
                elif self.signal_fired:
                    phase = "COMPLETED"
                elif not self.ce_bars.bars:
                    phase = "WARMUP"
                else:
                    phase = "MONITORING"

                STATE_FILE.write_text(json.dumps({
                    "strategy":      STRATEGY_NAME,
                    "phase":         phase,
                    "last_update":   datetime.now().isoformat(),
                    "bnf_ltp":       round(self.bnf_ltp, 2),
                    "vix_ltp":       round(self.vix_ltp, 2),
                    "ce_ltp":        round(self.ce_ltp,  2),
                    "pe_ltp":        round(self.pe_ltp,  2),
                    "expiry":        self.expiry,
                    "ce_symbol":     self.ce_symbol,
                    "pe_symbol":     self.pe_symbol,
                    "is_expiry_day": self.is_expiry_day,
                    "lot_size":      self.lot_size,
                    "n_lots":        N_LOTS,
                    "bb_params":     f"BB({BB_PERIOD},{BB_STD_MULT}σ)",
                    "sl_floor_mult":  SL_FLOOR_MULT,
                    "entry_window":  f"{ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')}",
                    "signal_fired":  self.signal_fired,
                    "ce_bars":       len(self.ce_bars.bars),
                    "pe_bars":       len(self.pe_bars.bars),
                    "ce_bb":         self.ce_snapshot,
                    "pe_bb":         self.pe_snapshot,
                    "ce_session_mean": round(self.ce_bars.session_cumulative_mean() or 0, 2),
                    "pe_session_mean": round(self.pe_bars.session_cumulative_mean() or 0, 2),
                    "active_trade":  {
                        "opt_type":   self.active_trade["opt_type"],
                        "symbol":     self.active_trade["symbol"],
                        "entry_prem": self.active_trade["entry_prem"],
                        "sl_prem":    self.active_trade["sl_prem"],
                        "sma_target": self.active_trade["sma_target"],
                        "qty":        self.active_trade["qty"],
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
        1. Bootstrap session immediately if past market open (bypassing live tick delay).
        2. Start state-dump background task.
        3. Connect to OpenAlgo WebSocket with auto-reconnect.
        4. Process ticks: SL (tick-level) + signal/SMA-exit (bar-level) until 15:20.
        """
        if datetime.now().time() >= MARKET_OPEN:
            self._session_started = True
            await self._session_open(self.bnf_ltp)

        asyncio.create_task(self._state_dump_loop())

        retry_delay = 5

        while True:
            now = datetime.now()
            if now.time() >= SESSION_END:
                await self._eod_close_all()
                logger.info("✅ Past 15:14 IST — shutting down.")
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

                    # Re-subscribe all symbols correctly (handles clearing self._subscribed_syms)
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
                        mdata = msg.get("data", {})

                        # ── Depth extraction (BEFORE ltp gate) ───────────────
                        # mode-3 messages may carry ltp=0 alongside depth data;
                        # always parse depth first so the filter snapshot stays fresh.
                        if sym in (self.ce_symbol, self.pe_symbol) and self.ce_symbol:
                            depth = self._parse_depth(mdata)
                            if depth:
                                # Log the depth structure on first receive per symbol
                                if sym not in self._depth_logged:
                                    self._depth_logged.add(sym)
                                    logger.info(
                                        f"  📊 First depth tick for {sym}: "
                                        f"{len(depth['bids'])} bid levels, "
                                        f"{len(depth['asks'])} ask levels. "
                                        f"Best bid={depth['bids'][0]['price'] if depth['bids'] else 'n/a'}  "
                                        f"Best ask={depth['asks'][0]['price'] if depth['asks'] else 'n/a'}"
                                    )
                                if sym == self.ce_symbol:
                                    self.ce_depth = depth
                                elif sym == self.pe_symbol:
                                    self.pe_depth = depth

                        ltp   = float(mdata.get("ltp", 0) or mdata.get("lp", 0))
                        ts_raw = mdata.get("t")
                        ts     = (datetime.fromtimestamp(float(ts_raw))
                                  if ts_raw else datetime.now())

                        if ltp <= 0:
                            continue

                        if sym in (IDX_SYMBOL, f"{IDX_EXCHANGE}:{IDX_SYMBOL}"):
                            await self._on_index_tick(ltp)

                        elif sym in (VIX_SYMBOL, f"{VIX_EXCHANGE}:{VIX_SYMBOL}"):
                            self.vix_ltp = ltp

                        elif sym in (self.ce_symbol, self.pe_symbol):
                            await self._on_option_tick(sym, ltp, ts)

            except Exception as e:
                logger.warning(
                    f"WebSocket error: {e}. Reconnecting in {retry_delay}s…"
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _acquire_pid_lock()          # ← must be before anything else; exits if duplicate
    bot = BankniftyBBOptionsBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("Stop signal received — bot terminated.")
