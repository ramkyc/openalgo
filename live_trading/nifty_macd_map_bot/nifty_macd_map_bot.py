"""
NIFTY MACD Money Map Bot
========================
live_trading/nifty_macd_map_bot/nifty_macd_map_bot.py

Sells the ATM counter option when the MACD(5,13,3) histogram fires a qualified
zero-line cross on 15-minute NIFTY index bars.

Signal logic (faithful translation of backtest_oos.py champion config):

    Histogram zero-cross (Long / Bullish):
        Bar N  : histogram crosses from ≤0 to >0
        Bar N+1: histogram distance > DIST_THRESH × hist_std  AND  MACD line > 0
        → SELL ATM PE (premium decays as market rises)

    Histogram zero-cross (Short / Bearish):
        Bar N  : histogram crosses from ≥0 to <0
        Bar N+1: histogram distance > DIST_THRESH × hist_std  AND  MACD line < 0
        → SELL ATM CE (premium decays as market falls)

Parameters (champion from Stage 4 OOS, validated Stages 5–10):
    MACD fast=5  slow=13  signal=3
    DIST_THRESH = 1.5 × rolling σ of histogram (computed from loaded history + live bars)
    DELAY = 1 bar (signal fires on close of bar AFTER the cross)

Research results (OOS Oct 2025 – Mar 2026):
    NIFTY: Sharpe 8.99 | WR 78.9% | 38 trades | MaxDD −6.8%
    Stage 9 multi-instrument: NIFTY ✅ + BANKNIFTY ✅ (2/3 pass)

Filters:
    Entry window : 09:30 – 14:00 IST  (first viable signal after warm-up)
    DTE range    : 2 – 7 days
    One trade per direction per session (long leg / short leg independent)
    SL           : 2 × entry premium

Quantity:
    N_LOTS = 10 (standardised to 10 lots per CLAUDE.md position-size rule)
    Lot size fetched dynamically from OpenAlgo token DB at session start

Shared utilities (no duplication):
    live_trading.shared.ta_compat        — MACD (pure pandas, matches pandas_ta output)
    live_trading.shared.atm_resolver     — get_option_ltp
    live_trading.shared.telegram_notifier— send_async
    live_trading.api_utils               — get_expiry_dates, get_option_symbol, get_history
    database.token_db.get_symbol_info    — dynamic lot size
    openalgo.api                         — placesmartorder

Trade mode (Analyze / Live) is set in the OpenAlgo app — not in this bot.
"""

import asyncio
import csv
import json
import logging
import os
import sys
from collections import deque
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path

import pandas as pd
import websockets
from dotenv import load_dotenv
from openalgo import api

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

# ── Research cost module (options_data sibling repo) ─────────────────────────
_RESEARCH_ROOT = ROOT.parent.parent / "options_data"
if _RESEARCH_ROOT.exists() and str(_RESEARCH_ROOT) not in sys.path:
    sys.path.insert(0, str(_RESEARCH_ROOT))

try:
    from research.transaction_costs import compute_round_trip_cost
except ImportError:
    def compute_round_trip_cost(**_kwargs):  # type: ignore[misc]
        return 0.0

from live_trading.shared import ta_compat as ta
from live_trading.api_utils               import get_expiry_dates, get_option_symbol, get_history
from live_trading.shared.atm_resolver     import get_option_ltp
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger     import log_trade_to_db
from live_trading.shared.order_fill       import fetch_fill_price

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "nifty_macd_map_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER",   "http://127.0.0.1:5001")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")

if not API_KEY:
    logger.error("❌ OPENALGO_API_KEY not found in environment. Please check .env. Exiting.")
    sys.exit(1)

PAPER_MODE = os.getenv("NIFTY_MACD_MAP_PAPER_MODE", "true").lower() != "false"

# ── Strategy constants ────────────────────────────────────────────────────────
STRATEGY_NAME    = "NIFTY_MACD_MAP"


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback


IDX_SYMBOL       = "NIFTY"
IDX_EXCHANGE     = "NSE_INDEX"
OPT_EXCHANGE     = "NFO"

N_LOTS           = 10              # standardised: 10 lots per CLAUDE.md position-size rule
DEFAULT_LOT_SIZE = 65              # fallback if DB lookup fails (post-Dec 2025 lot size)

# MACD parameters (champion config)
MACD_FAST        = 5
MACD_SLOW        = 13
MACD_SIG         = 3

# Signal parameters
DIST_THRESH      = 1.5             # histogram must be > DIST_THRESH × std to qualify
DELAY            = 1               # bars between zero-cross and actual entry

# Risk management
SL_MULTIPLE      = 2.0             # SL at 2 × entry premium

# Entry / session timing (all IST)
MARKET_OPEN      = dt_time(9, 15)
ENTRY_START      = dt_time(9, 30)  # earliest viable signal: first 15-min bar close + DELAY
ENTRY_END        = dt_time(14, 0)  # matches research entry_cutoff
EOD_EXIT         = dt_time(15, 15) # matches research eod_exit
SESSION_END      = dt_time(15, 30)

# DTE
MIN_DTE          = 2
MAX_DTE          = 7

# Minimum completed 15-min bars before signals are generated
MIN_BARS_REQUIRED = 30             # ≥30 bars ≈ 7.5 hours; SLOW=13 needs ≥13 + SIG warmup

# State file for Telegram /status dashboard
STATE_FILE       = LOGS_DIR / "nifty_macd_map_state.json"

# ta_compat MACD column names
_MACD_LINE_COL = f"MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
_MACD_HIST_COL = f"MACDh_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
_MACD_SIG_COL  = f"MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"


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
        logger.warning(f"  Lot size DB lookup failed ({symbol}): {e}. Using default {DEFAULT_LOT_SIZE}.")
    return DEFAULT_LOT_SIZE


# ── Helper: expiry selection ──────────────────────────────────────────────────
def _get_suitable_expiry() -> str | None:
    """
    Return the nearest NIFTY weekly expiry with DTE in [MIN_DTE, MAX_DTE].
    Uses shared api_utils.get_expiry_dates — returns OpenAlgo DDMMMYY string or None.
    """
    dates = get_expiry_dates(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, "options")
    today = datetime.now().date()
    for d in dates:
        try:
            exp_dt = datetime.strptime(d, "%d%b%y").date()
            dte    = (exp_dt - today).days
            if MIN_DTE <= dte <= MAX_DTE:
                logger.info(f"  Suitable expiry: {d}  (DTE={dte})")
                return d
        except ValueError:
            continue
    logger.warning(f"  No expiry found with DTE {MIN_DTE}–{MAX_DTE}. Dates: {dates[:8]}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
class NiftyMacdMapBot:
# ══════════════════════════════════════════════════════════════════════════════
    """
    MACD(5,13,3) histogram zero-cross → NIFTY ATM counter-option seller.
    Streams NIFTY index ticks via WebSocket, builds 15-min OHLC bars in-process,
    and evaluates the MACD Money Map signal on every 15-min bar close.

    Signal evaluation happens at each 15-min bar close.  The DELAY=1 / next-open
    entry mechanic works as follows:
      1. Bar N closes → cross is detected by examining hist[N] vs hist[N-1].
      2. Signals are QUEUED in self._pending_entries (not entered immediately).
      3. On the FIRST TICK of bar N+1, the pending entries are executed — this
         approximates bar N+1 OPEN, matching the backtest convention.
    """

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.ws     = None

        # Live price
        self.nifty_ltp: float = 0.0

        # 15-min bar builder
        # bars: completed 15-min bars (OHLC dicts), history + live
        self.bars: deque = deque(maxlen=500)

        self._bar_open:     float = 0.0
        self._bar_high:     float = 0.0
        self._bar_low:      float = float("inf")
        self._bar_slot:     int   = -1    # current 15-min slot index (minute // 15)
        self._last_ltp:     float = 0.0

        # Cross-state (persisted across bar closes for DELAY=1 mechanic)
        self._prev_cross_up:  bool = False   # previous bar had histogram ≤0→>0 cross
        self._prev_cross_dn:  bool = False   # previous bar had histogram ≥0→<0 cross

        # Active positions — keyed by opt_type ("PE" / "CE"), both legs independent
        self.active_trades: dict[str, dict | None] = {"PE": None, "CE": None}

        # Session metadata (resolved at first tick / session start)
        self.expiry:     str | None = None
        self.hist_std:   float      = 0.0    # rolling σ of MACD histogram (from history)

        # State flags
        self._session_started   = False
        self._next_init_attempt = datetime.min   # retry gate for session init
        self._eod_exit_done     = False
        self._first_connect     = True
        self._subscribed_syms: set[str] = set()

        # Indicator snapshot for dashboard / state dump
        self.indicator_snapshot: dict = {}

        # Pending entries (N+1 open mechanic): signals are stored here at bar-N+1
        # close and executed on the very first tick of bar N+2 (i.e. at N+2 open).
        # This matches the backtest's delay=1 enter-at-next-open convention.
        self._pending_entries: list[str] = []

        # Restore position state from previous run (survives bot restart)
        self._restore_state()

    # ══════════════════════════════════════════════════════════════════════════
    #  RESTART STATE RESTORATION
    # ══════════════════════════════════════════════════════════════════════════

    def _restore_state(self) -> None:
        """
        On startup, reload active trades and expiry from STATE_FILE if present.
        Ensures the bot can resume SL monitoring and EOD exit after a crash or restart.
        """
        if not STATE_FILE.exists():
            return
        try:
            saved = json.loads(STATE_FILE.read_text())
            if saved.get("active_pe"):
                self.active_trades["PE"] = saved["active_pe"]
                logger.info(
                    f"  ♻️  Restored PE position: {saved['active_pe']['symbol']}"
                )
            if saved.get("active_ce"):
                self.active_trades["CE"] = saved["active_ce"]
                logger.info(
                    f"  ♻️  Restored CE position: {saved['active_ce']['symbol']}"
                )
            if saved.get("expiry"):
                self.expiry = saved["expiry"]
                logger.info(f"  ♻️  Restored expiry: {self.expiry}")
        except Exception as e:
            logger.warning(f"  State restore failed (starting fresh): {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  HISTORY LOAD (warm-up at session start)
    # ══════════════════════════════════════════════════════════════════════════

    def _load_history(self) -> None:
        """
        Fetch the last ~60 completed 15-min bars from the OpenAlgo history API
        to warm up the MACD indicators and compute the initial hist_std.

        Uses shared api_utils.get_history.  Any missing data is silently skipped —
        the session still runs; signals simply require MIN_BARS_REQUIRED live bars.
        """
        try:
            # get_history returns a list of dicts; duration_days=10 gives ~5 trading
            # days of 15-min bars (≈125 bars), well above MIN_BARS_REQUIRED=30
            hist = get_history(API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "15m", 10)
            if not hist:
                logger.warning("  History load: no data returned — running cold start.")
                return

            for row in hist:
                self.bars.append({
                    "open":  float(row["open"]),
                    "high":  float(row["high"]),
                    "low":   float(row["low"]),
                    "close": float(row["close"]),
                })

            # Compute initial hist_std from loaded history
            if len(self.bars) >= MIN_BARS_REQUIRED:
                df = pd.DataFrame(list(self.bars))
                macd_df = ta.macd(df["close"], fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)
                if macd_df is not None and _MACD_HIST_COL in macd_df.columns:
                    self.hist_std = float(macd_df[_MACD_HIST_COL].std())
                    logger.info(
                        f"  History loaded: {len(self.bars)} bars  "
                        f"|  hist_std = {self.hist_std:.5f}"
                    )
        except Exception as e:
            logger.warning(f"  History load failed ({e}). Running cold start.")

    # ══════════════════════════════════════════════════════════════════════════
    #  15-MIN BAR BUILDER
    # ══════════════════════════════════════════════════════════════════════════

    def _update_bar(self, ltp: float, ts: datetime) -> bool:
        """
        Feed one raw tick into the running 15-min bar.
        Returns True when a bar is COMPLETED (15-min slot boundary crossed).
        The completed bar is appended to self.bars before returning.

        Slot index = (hour * 60 + minute) // 15, so boundaries fall at
        :00, :15, :30, :45 of every hour — matching standard exchange bars.
        """
        cur_slot = (ts.hour * 60 + ts.minute) // 15

        if self._bar_slot == -1:
            # First tick ever — initialise
            self._bar_slot  = cur_slot
            self._bar_open  = ltp
            self._bar_high  = ltp
            self._bar_low   = ltp
            self._last_ltp  = ltp
            return False

        if cur_slot != self._bar_slot:
            # Slot boundary → close bar, open new one
            if self._bar_open > 0:
                self.bars.append({
                    "open":  self._bar_open,
                    "high":  self._bar_high,
                    "low":   self._bar_low,
                    "close": self._last_ltp,
                })

            self._bar_slot  = cur_slot
            self._bar_open  = ltp
            self._bar_high  = ltp
            self._bar_low   = ltp
            self._last_ltp  = ltp
            return True    # ← bar completed

        # Same slot — update running bar
        self._bar_high  = max(self._bar_high, ltp)
        self._bar_low   = min(self._bar_low,  ltp)
        self._last_ltp  = ltp
        return False

    # ══════════════════════════════════════════════════════════════════════════
    #  SIGNAL ENGINE
    # ══════════════════════════════════════════════════════════════════════════

    def _compute_signals(self, bar_time: datetime) -> list[str]:
        """
        Evaluate the MACD Money Map signal on the just-completed 15-min bar.
        Returns a list of option types to sell: ["PE"], ["CE"], or [].

        MACD Money Map signal (DELAY=1 implementation):
          Step 1 — on bar N:  detect zero-cross; store in _prev_cross_up/_dn.
          Step 2 — on bar N+1 (current call): if _prev_cross_up/_dn is True AND
                   abs(hist) > DIST_THRESH × hist_std AND ml sign matches → signal.

        Entry window enforced here: ENTRY_START ≤ bar_time.time() ≤ ENTRY_END.
        """
        if len(self.bars) < MIN_BARS_REQUIRED:
            logger.debug(f"  Warm-up: {len(self.bars)}/{MIN_BARS_REQUIRED} bars")
            self._prev_cross_up = False
            self._prev_cross_dn = False
            return []

        # Window check
        now_t = bar_time.time()
        in_window = ENTRY_START <= now_t <= ENTRY_END

        # ── Compute MACD ──────────────────────────────────────────────────────
        df = pd.DataFrame(list(self.bars))
        macd_df = ta.macd(df["close"], fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)

        if macd_df is None or _MACD_HIST_COL not in macd_df.columns:
            return []

        hist_series = macd_df[_MACD_HIST_COL]
        ml_series   = macd_df[_MACD_LINE_COL]

        def _v(s, offset=0):
            v = s.iloc[-(1 + offset)]
            return float(v) if not pd.isna(v) else float("nan")

        hist_cur   = _v(hist_series)        # current bar (N+1, just closed)
        hist_prev  = _v(hist_series, 1)     # previous bar (N)
        hist_prev2 = _v(hist_series, 2)     # two bars ago (N−1, to confirm N was the cross)
        ml_cur     = _v(ml_series)

        if any(v != v for v in [hist_cur, hist_prev, hist_prev2, ml_cur]):
            return []

        # ── Update hist_std dynamically (running σ over all available hist bars) ──
        valid_hist = hist_series.dropna()
        if len(valid_hist) >= MACD_SLOW + MACD_SIG:
            self.hist_std = float(valid_hist.std())

        if self.hist_std <= 0:
            return []

        # ── Detect cross on previous bar (N) — used as DELAY=1 mechanic ──────
        # Cross up on bar N: hist_prev changed sign from ≤0 to >0
        cross_up_on_prev = (hist_prev2 <= 0) and (hist_prev > 0)
        # Cross down on bar N: hist_prev changed sign from ≥0 to <0
        cross_dn_on_prev = (hist_prev2 >= 0) and (hist_prev < 0)

        # Update cross-state for this evaluation
        self._prev_cross_up = cross_up_on_prev
        self._prev_cross_dn = cross_dn_on_prev

        # ── Distance filter on current bar (N+1) ──────────────────────────────
        hist_dist_ok = abs(hist_cur) > DIST_THRESH * self.hist_std

        # ── Persist indicator snapshot ─────────────────────────────────────────
        self.indicator_snapshot = {
            "bar_time":       bar_time.strftime("%H:%M"),
            "nifty_ltp":      round(self.nifty_ltp, 2),
            "hist_cur":       round(hist_cur, 5),
            "hist_prev":      round(hist_prev, 5),
            "hist_std":       round(self.hist_std, 5),
            "dist_ratio":     round(abs(hist_cur) / self.hist_std, 2) if self.hist_std > 0 else 0,
            "dist_thresh":    DIST_THRESH,
            "ml_cur":         round(ml_cur, 5),
            "prev_cross_up":  cross_up_on_prev,
            "prev_cross_dn":  cross_dn_on_prev,
            "in_window":      in_window,
            "bars_total":     len(self.bars),
        }

        if not in_window:
            return []

        signals = []

        # ── Long signal: cross_up on prev bar + dist ok + ml > 0 → SELL PE ───
        if cross_up_on_prev and hist_dist_ok and ml_cur > 0:
            logger.info(
                f"📶 LONG signal @ {bar_time.strftime('%H:%M')}: "
                f"hist={hist_cur:.5f}  dist={abs(hist_cur)/self.hist_std:.2f}×σ  "
                f"ml={ml_cur:.5f}>0  →  SELL PE"
            )
            signals.append("PE")

        # ── Short signal: cross_dn on prev bar + dist ok + ml < 0 → SELL CE ──
        if cross_dn_on_prev and hist_dist_ok and ml_cur < 0:
            logger.info(
                f"📶 SHORT signal @ {bar_time.strftime('%H:%M')}: "
                f"hist={hist_cur:.5f}  dist={abs(hist_cur)/self.hist_std:.2f}×σ  "
                f"ml={ml_cur:.5f}<0  →  SELL CE"
            )
            signals.append("CE")

        return signals

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE ENTRY
    # ══════════════════════════════════════════════════════════════════════════

    async def _enter_trade(self, opt_type: str) -> None:
        """
        Resolve the ATM option symbol, fetch live LTP, place a SELL (short) order.
        opt_type: "PE" for long-signal leg, "CE" for short-signal leg.
        """
        if self.active_trades[opt_type] is not None:
            logger.info(f"  [{opt_type}] Position already open — skipping duplicate entry.")
            return

        if not self.expiry:
            logger.warning(f"  [{opt_type}] No suitable expiry today — entry skipped.")
            return

        spot = self.nifty_ltp
        if spot <= 0:
            return

        # ATM symbol resolution
        symbol = await asyncio.to_thread(
            get_option_symbol,
            API_KEY, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, opt_type, "ATM",
        )
        if not symbol:
            logger.error(
                f"  [{opt_type}] ATM symbol resolution failed "
                f"(spot={spot:.0f}, expiry={self.expiry})"
            )
            return

        # Live premium
        opt_ltp = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        if opt_ltp <= 0:
            logger.warning(f"  [{opt_type}] Option LTP = 0 for {symbol}. Entry skipped.")
            return

        lot_size = _get_lot_size(symbol)
        qty      = N_LOTS * lot_size
        sl_prem  = round(opt_ltp * SL_MULTIPLE, 2)

        logger.info(
            f"  [{opt_type}] Entering: {symbol}  LTP=₹{opt_ltp:.2f}  "
            f"qty={qty} ({N_LOTS} lot × {lot_size})  SL=₹{sl_prem:.2f}"
        )

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
            # Resolve actual fill price; recompute the SL threshold from it
            # (not the raw LTP snapshot quoted before the order was placed).
            fill_prem = _resolve_fill(res, opt_ltp)
            sl_prem   = round(fill_prem * SL_MULTIPLE, 2)
            self.active_trades[opt_type] = {
                "symbol":     symbol,
                "entry_prem": fill_prem,
                "sl_prem":    sl_prem,
                "qty":        qty,
                "lot_size":   lot_size,
                "order_id":   str(res.get("orderid", "")),
                "entry_time": datetime.now().isoformat(),
                "opt_type":   opt_type,
            }
            await self._subscribe(symbol, OPT_EXCHANGE)

            logger.info(
                f"✅ [{opt_type}] SOLD {symbol} @ ₹{fill_prem:.2f}  "
                f"({N_LOTS} lot, qty={qty}, order={res.get('orderid')})"
            )
            await send_async(
                f"📉 *NIFTY MACD Map — ENTRY*\n"
                f"Sold `{symbol}`  ({N_LOTS} lot × {lot_size})\n"
                f"Entry premium : ₹{fill_prem:.2f}\n"
                f"Safety SL     : ₹{sl_prem:.2f}  (2× entry)\n"
                f"Exit          : EOD 15:15 IST\n"
                f"NIFTY: {spot:.1f}\n"
                f"hist dist: {self.indicator_snapshot.get('dist_ratio','?')}×σ  "
                f"ml: {self.indicator_snapshot.get('ml_cur','?')}\n"
                f"_Signal: {datetime.now().strftime('%H:%M')}_"
            )
        else:
            logger.error(f"  [{opt_type}] Order rejected: {res}")

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE EXIT
    # ══════════════════════════════════════════════════════════════════════════

    async def _close_trade(self, opt_type: str, exit_prem: float, reason: str) -> None:
        """Buy back the sold option to flatten the short position."""
        trade = self.active_trades.get(opt_type)
        if not trade:
            return

        symbol = trade["symbol"]
        qty    = trade["qty"]

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
            logger.error(f"  [{opt_type}] close placeorder exception: {e}")
            return

        order_ok = res.get("status") == "success"
        if not order_ok:
            # Position may already be closed by sandbox auto-squareoff (15:15 MIS cutoff).
            # Do NOT return early — always log the trade so performance.db stays accurate.
            logger.warning(
                f"  [{opt_type}] Exit order non-success (likely auto-squareoff already "
                f"closed position): {res}  — logging trade and clearing state."
            )

        # Resolve actual fill price for the close order (falls back to the
        # LTP snapshot that triggered this exit if the lookup fails).
        exit_prem = _resolve_fill(res, exit_prem)

        # Capture trade fields before clearing state.
        entry_prem   = trade["entry_prem"]
        _entry_time  = trade.get("entry_time")
        _lot_size    = trade["lot_size"]
        pnl_per_unit = entry_prem - exit_prem
        pnl_gross    = pnl_per_unit * qty

        # Net P&L: mandatory for Stage 11 gate (F&O costs ~₹55–85/lot round trip)
        cost = compute_round_trip_cost(
            entry_premium=entry_prem,
            exit_premium=exit_prem,
            lot_size=_lot_size,
            qty=N_LOTS,
            exchange="NSE",
            side="sell",
            brokerage_per_order=20,
        )
        pnl_net = pnl_gross - cost

        # Clear state unconditionally.
        self.active_trades[opt_type] = None

        auto_sq_note = " _(closed by auto-squareoff)_" if not order_ok else ""
        logger.info(
            f"✅ [{opt_type}] CLOSED {symbol} @ ₹{exit_prem:.2f}  "
            f"Gross P&L: ₹{pnl_gross:,.0f}  Cost: ₹{cost:,.0f}  "
            f"Net P&L: ₹{pnl_net:,.0f}  Reason: {reason}"
        )
        await send_async(
            f"📤 *NIFTY MACD Map — EXIT ({reason})*\n"
            f"Closed `{symbol}`\n"
            f"Entry: ₹{entry_prem:.2f}  |  Exit: ₹{exit_prem:.2f}\n"
            f"Gross P&L: ₹{pnl_gross:,.0f}  |  Cost: ₹{cost:,.0f}\n"
            f"*Net P&L: ₹{pnl_net:,.0f}*  ({N_LOTS} lot × {_lot_size})\n"
            f"_Closed: {datetime.now().strftime('%H:%M')}{auto_sq_note}_"
        )

        # Stage 11 gate tracking — append to paper_trades.csv
        try:
            csv_path  = LOGS_DIR / "paper_trades.csv"
            trade_row = {
                "date":        datetime.now().strftime("%Y-%m-%d"),
                "strategy":    STRATEGY_NAME,
                "symbol":      symbol,
                "opt_type":    opt_type,
                "entry_prem":  entry_prem,
                "exit_prem":   exit_prem,
                "qty":         qty,
                "lots":        N_LOTS,
                "gross_pnl":   round(pnl_gross, 2),
                "cost":        round(cost, 2),
                "net_pnl":     round(pnl_net, 2),
                "reason":      reason,
                "entry_time":  _entry_time,
                "exit_time":   datetime.now().isoformat(),
            }
            write_header = not csv_path.exists()
            with open(csv_path, "a", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=trade_row.keys())
                if write_header:
                    w.writeheader()
                w.writerow(trade_row)
        except Exception as e:
            logger.warning(f"  paper_trades.csv write failed: {e}")

        # Dual-write to performance.db — use correct positional kwargs (was broken:
        # previously passed a dict as the first arg, silently failing on every trade).
        log_trade_to_db(
            bot_name      = "nifty_macd_map_bot",
            instrument    = IDX_SYMBOL,
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
            gross_pnl     = pnl_gross,
            net_pnl       = round(pnl_net, 2),
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  SL MONITOR
    # ══════════════════════════════════════════════════════════════════════════

    async def _check_sl(self, symbol: str, ltp: float) -> None:
        """
        Called on every option tick. Closes the position if ltp ≥ sl_prem.
        """
        for opt_type, trade in list(self.active_trades.items()):
            if trade and trade["symbol"] == symbol:
                if ltp >= trade["sl_prem"]:
                    logger.warning(
                        f"  🛑 [{opt_type}] SL hit: {symbol} LTP=₹{ltp:.2f} "
                        f"≥ SL=₹{trade['sl_prem']:.2f}"
                    )
                    await self._close_trade(opt_type, ltp, "SL")

    # ══════════════════════════════════════════════════════════════════════════
    #  EOD EXIT
    # ══════════════════════════════════════════════════════════════════════════

    async def _eod_exit(self) -> None:
        """Unconditional close of all open positions at EOD_EXIT time (15:15 IST)."""
        if self._eod_exit_done:
            return
        self._eod_exit_done = True

        logger.info("⏰ EOD exit: closing all open positions at 15:15 IST.")
        for opt_type in list(self.active_trades.keys()):
            trade = self.active_trades.get(opt_type)
            if trade:
                live_prem = await asyncio.to_thread(
                    get_option_ltp, trade["symbol"], OPT_EXCHANGE, API_KEY
                )
                if live_prem <= 0:
                    live_prem = trade["entry_prem"]   # fallback — avoid 0-prem close
                await self._close_trade(opt_type, live_prem, "EOD")

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE DUMP (for /status dashboard)
    # ══════════════════════════════════════════════════════════════════════════

    def _write_state(self) -> None:
        try:
            state = {
                "strategy":    STRATEGY_NAME,
                "last_update": datetime.now().isoformat(),   # dashboard heartbeat key
                "updated_at":  datetime.now().isoformat(),
                "expiry":      self.expiry,
                "hist_std":    round(self.hist_std, 6),
                "bars_loaded": len(self.bars),
                "active_pe":      self.active_trades.get("PE"),
                "active_ce":      self.active_trades.get("CE"),
                "pending_entries": self._pending_entries,
                "indicators":     self.indicator_snapshot,
                "paper_mode":  PAPER_MODE,
            }
            STATE_FILE.write_text(json.dumps(state, indent=2, default=str))
        except Exception:
            pass

    async def _state_writer(self) -> None:
        """Write state file every 3 seconds so the dashboard always has a fresh heartbeat."""
        while True:
            self._write_state()
            await asyncio.sleep(3)  # state written on loop every 3s

    # ══════════════════════════════════════════════════════════════════════════
    #  WEBSOCKET HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _subscribe(self, symbol: str, exchange: str) -> None:
        if not self.ws or symbol in self._subscribed_syms:
            return
        try:
            msg = json.dumps({
                "action": "subscribe",
                "symbol": symbol,
                "exchange": exchange,
                "mode": 2,
            })
            await self.ws.send(msg)
            self._subscribed_syms.add(symbol)
            logger.info(f"  Subscribed: {symbol} ({exchange})")
        except Exception as e:
            logger.warning(f"  Subscribe failed ({symbol}): {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  TICK HANDLER
    # ══════════════════════════════════════════════════════════════════════════

    async def _on_tick(self, msg: dict) -> None:
        # WS proxy wraps ticks as: {"type": "market_data", "symbol": ..., "data": {ltp, t, ...}}
        if msg.get("type") != "market_data":
            return
        symbol   = msg.get("symbol", "")
        mdata    = msg.get("data", {})
        ltp      = float(mdata.get("ltp", 0) or mdata.get("lp", 0))
        ts_raw   = mdata.get("t")

        try:
            ts = datetime.fromtimestamp(float(ts_raw)) if ts_raw else datetime.now()
        except (ValueError, TypeError):
            ts = datetime.now()

        now_t = ts.time()

        # ── Route by symbol ───────────────────────────────────────────────────
        if symbol == IDX_SYMBOL:
            self.nifty_ltp = ltp
            # Keep the snapshot current between bar closes so the dashboard
            # always shows a live price, not just the value from last bar close
            self.indicator_snapshot["nifty_ltp"] = round(ltp, 2)

            # ── Session init on first NIFTY tick ─────────────────────────────
            # Init only latches on SUCCESS (history + expiry both resolved);
            # a failed attempt re-arms with a cooldown so a transient OpenAlgo
            # outage at open can't silently disable the bot for the whole day
            # (incident 2026-07-07).
            if (not self._session_started and now_t >= MARKET_OPEN
                    and datetime.now() >= self._next_init_attempt):
                logger.info("🔔 Session start — initialising MACD Map bot.")
                self._load_history()
                self.expiry = _get_suitable_expiry()
                if not self.bars or not self.expiry:
                    self._next_init_attempt = datetime.now() + timedelta(seconds=120)
                    logger.warning(
                        f"  ⚠️  Session init incomplete (bars={len(self.bars)}, "
                        f"expiry={self.expiry}) — retrying in 120s."
                    )
                    return
                self._session_started = True
                logger.info(
                    f"  NIFTY={ltp:.1f}  Expiry={self.expiry}  "
                    f"hist_std={self.hist_std:.5f}  bars={len(self.bars)}"
                )
                await send_async(
                    f"🟢 *NIFTY MACD Map — Session Start*\n"
                    f"NIFTY: {ltp:.1f}  |  Expiry: {self.expiry}\n"
                    f"Hist σ: {self.hist_std:.5f}  |  Bars loaded: {len(self.bars)}\n"
                    f"Entry window: 09:30 – 14:00 IST\n"
                    f"EOD exit: 15:15 IST | SL: 2× entry premium"
                )

            # ── EOD exit ──────────────────────────────────────────────────────
            if now_t >= EOD_EXIT and not self._eod_exit_done:
                await self._eod_exit()
                return

            if now_t > SESSION_END:
                return

            # ── 15-min bar builder ────────────────────────────────────────────
            bar_completed = self._update_bar(ltp, ts)

            # ── N+1 OPEN entry: execute any signals queued from the previous
            #    bar close.  The first tick after a bar closes is the open of
            #    the next bar — this matches the backtest's delay=1 / next-open
            #    entry convention.  We check BEFORE processing the new bar so
            #    that a pending entry fires on the very first tick.
            if self._pending_entries and self._session_started:
                pending = self._pending_entries[:]
                self._pending_entries = []
                logger.info(
                    f"  ⏩ Executing {len(pending)} pending N+1-open "
                    f"{'entr' + ('y' if len(pending) == 1 else 'ies')}: {pending}"
                )
                for opt_type in pending:
                    if self.active_trades.get(opt_type) is None:
                        await self._enter_trade(opt_type)

            if bar_completed and self._session_started:
                bar_time = ts
                signals = self._compute_signals(bar_time)
                self._write_state()

                # Queue signals for execution at the OPEN of the next bar
                # (first tick after this bar closes = N+2 open in bar-count terms).
                if signals:
                    logger.info(
                        f"  📌 Queuing {signals} for N+1-open entry "
                        f"(next tick = next bar open)"
                    )
                    self._pending_entries.extend(
                        s for s in signals
                        if s not in self._pending_entries
                    )

        else:
            # ── Option tick: SL check ─────────────────────────────────────────
            if ltp > 0:
                await self._check_sl(symbol, ltp)

    # ══════════════════════════════════════════════════════════════════════════
    #  WEBSOCKET LOOP
    # ══════════════════════════════════════════════════════════════════════════

    async def _ws_loop(self) -> None:
        """Connect, subscribe, and process ticks with exponential back-off reconnect."""
        backoff = 1
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=20) as ws:
                    self.ws = ws
                    backoff = 1
                    logger.info(f"🔗 WebSocket connected: {WS_URL}")

                    # Authenticate first — proxy requires this before routing market data
                    await ws.send(json.dumps({
                        "action":  "authenticate",
                        "api_key": API_KEY,
                    }))

                    # Subscribe to NIFTY index
                    await self._subscribe(IDX_SYMBOL, IDX_EXCHANGE)

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                            await self._on_tick(msg)
                        except json.JSONDecodeError:
                            pass

            except (websockets.exceptions.ConnectionClosed,
                    ConnectionRefusedError, OSError) as e:
                logger.warning(
                    f"  WebSocket disconnected ({type(e).__name__}: {e}). "
                    f"Reconnecting in {backoff}s..."
                )
                self.ws = None
                self._subscribed_syms.clear()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    # ══════════════════════════════════════════════════════════════════════════
    #  ENTRY POINT
    # ══════════════════════════════════════════════════════════════════════════

    async def run(self) -> None:
        logger.info("=" * 60)
        logger.info(f"NIFTY MACD Money Map Bot — {STRATEGY_NAME}")
        logger.info(
            f"MACD({MACD_FAST},{MACD_SLOW},{MACD_SIG})  "
            f"dist={DIST_THRESH}×σ  delay={DELAY}bar"
        )
        logger.info(f"Entry: 09:30–14:00 IST  |  SL: {SL_MULTIPLE}×  |  EOD: 15:15 IST")
        logger.info(f"N_LOTS={N_LOTS}  MIN_DTE={MIN_DTE}  MAX_DTE={MAX_DTE}")
        logger.info(f"PAPER_MODE={PAPER_MODE}")
        logger.info("=" * 60)
        await asyncio.gather(
            self._ws_loop(),
            self._state_writer(),
        )


# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(NiftyMacdMapBot().run())
