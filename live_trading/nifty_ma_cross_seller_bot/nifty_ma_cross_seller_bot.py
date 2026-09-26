#!/usr/bin/env python3
"""
NIFTY MA Crossover Options Seller Bot
======================================
live_trading/nifty_ma_cross_seller_bot/nifty_ma_cross_seller_bot.py

Champion strategy from ma_cross_options_seller_study (ALL 10 stages pass).
Research: options_data/research/ma_cross_options_seller_study/results_summary.md

Signal (Trend variant):
  SMA(15) / SMA(225) on NIFTY INDEX 1-min bars resampled to 3-min.
  Bearish cross (fast < slow) → SELL ATM CE  (sell call, profit as market falls/stays)
  Bullish cross (fast > slow) → SELL ATM PE  (sell put, profit as market rises/stays)

Core Rules (ALL validated):
  - OVERNIGHT hold (NRML) — position carried across sessions until reversal or expiry
  - 10 lots per trade, one position at a time
  - Entry cutoff:   14:00 IST  (no new entries after this)
  - Expiry gate:    14:30 IST on expiry day — mandatory exit
  - EOD EOD exit:   15:15 IST on expiry day (safety fallback)
  - Anti-whipsaw:   450-bar lockout between accepted crosses (in 3m bars)
  - Hard SL:        3× entry premium (Extension Study A)
  - *** MANDATORY: Skip ALL entries on calendar days 8–14 of any month (Week-2 filter)
                   Week-2 OOS Sharpe was −1.18, WR 43.8% — zero edge ***
  - Optional filter: Skip if VIX < 13 AND daily ADX(14) ≥ 25 (danger zone, Stage 7)

Instrument details:
  - NIFTY weekly Tuesday expiry (post-Sep 2025; holiday shift → prior trading day via get_expiry_dates)
  - ATM round: nearest 50 points
  - Lot size: 65 (post-Jan 6 2026); fetched dynamically from token DB at session start
  - Min DTE: ≥ 2 days

Research Performance (OOS Jan 2025 – Jun 2026):
  Champion A: NIFTY f=15 s=225 3m SMA Trend Overnight
  OOS: 74 trades | Sharpe 1.64 | WR 55.4%
  MC: 97.5% stability | Bootstrap: 80.3% robustness, median Sharpe 1.64
  Walk-forward: 14 windows, 0 catastrophic | Regime gate: ✓ PASS
  With Week-2 filter: Sharpe 2.00, WR 58.6%, n=58

Stage 11 gate: ≥ 20 sessions, net P&L positive, live WR within ±10% of OOS (55.4%)

Shared utilities:
  live_trading.shared.ta_compat        — SMA (pure pandas, consistent column names)
  live_trading.shared.atm_resolver     — get_option_ltp
  live_trading.shared.telegram_notifier— send_async
  live_trading.api_utils               — get_expiry_dates, get_option_symbol, get_history
  database.token_db.get_symbol_info    — dynamic lot size
  openalgo.api                         — placesmartorder

Trade mode (Analyze / Live) is set in the OpenAlgo app UI — not in this bot.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections import deque
from datetime import datetime, time as dt_time, date, timedelta
from pathlib import Path

import pandas as pd
import websockets
from dotenv import load_dotenv
from openalgo import api

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils                 import get_expiry_dates, get_option_symbol, get_history
from live_trading.shared import ta_compat   as ta
from live_trading.shared.atm_resolver       import get_option_ltp
from live_trading.shared.telegram_notifier  import send_async
from live_trading.shared.trade_logger       import log_trade_to_db
from live_trading.shared.order_fill         import fetch_fill_price
from live_trading.shared.decision_logger    import DecisionLogger
from live_trading.shared.tick_watchdog      import TickWatchdog

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "nifty_ma_cross_seller_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER",   "http://127.0.0.1:8080")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:8765")

if not API_KEY:
    logger.error("❌ OPENALGO_API_KEY not found in environment. Exiting.")
    sys.exit(1)

# ── Strategy constants ────────────────────────────────────────────────────────
STRATEGY_NAME    = "NIFTY_MA_CROSS_SELLER"


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback


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


IDX_SYMBOL       = "NIFTY"
IDX_EXCHANGE     = "NSE_INDEX"
VIX_SYMBOL       = "INDIAVIX"
VIX_EXCHANGE     = "NSE_INDEX"
OPT_EXCHANGE     = "NFO"

N_LOTS           = 10
DEFAULT_LOT_SIZE = 65       # post-Jan 6 2026 lot size

# SMA parameters (champion: f=15, s=225, tf=3m, variant=Trend)
SMA_FAST    = 15
SMA_SLOW    = 225
TF_MINUTES  = 3             # 3m bars built from 1m ticks
ATM_STEP    = 50            # NIFTY strike rounding step

# Risk management (Stage 10 validated)
SL_MULTIPLE = 3.0           # 3× entry premium (Extension Study A)

# Lockout: 2 × slow_period = 450 3m bars between accepted crosses (anti-whipsaw)
LOCKOUT_BARS = 2 * SMA_SLOW   # 450

# DTE constraints
MIN_DTE = 2                 # must have ≥ 2 days to expiry at entry

# Session timing (IST)
MARKET_OPEN    = dt_time(9, 15)
ENTRY_CUTOFF   = dt_time(14, 0)    # no new entries after 14:00
EXPIRY_GATE    = dt_time(14, 30)   # mandatory exit on expiry day at 14:30
SESSION_END    = dt_time(15, 14)   # safety EOD exit on expiry day; sandbox auto-squareoff cutoff is 15:15

# Minimum 3m bars before signals can fire (need SMA_SLOW bars at minimum)
MIN_BARS_REQUIRED = SMA_SLOW + 5  # 230

# Optional danger-zone filter (Stage 7 finding)
DANGER_VIX_MAX  = 13.0     # if VIX < 13 AND ADX ≥ 25 → skip entry
DANGER_ADX_MIN  = 25.0

# State / trade log files
STATE_FILE       = LOGS_DIR / "nifty_ma_cross_seller_state.json"
PAPER_TRADES_CSV = LOGS_DIR / "nifty_ma_cross_seller_paper_trades.csv"

# Decision-state logging (jsonl + throttled heartbeat — see shared/decision_logger.py)
DECISION_LOG     = LOGS_DIR / "nifty_ma_cross_seller_decisions.jsonl"
HEARTBEAT_SECS   = 300

# Bars to load at session start (15 days of 1m history → ample warm-up for SMA 225 on 3m)
HISTORY_DAYS = 15


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get_lot_size(symbol: str) -> int:
    """Fetch current lot size from OpenAlgo token DB. Falls back to DEFAULT_LOT_SIZE."""
    try:
        from database.token_db import get_symbol_info
        si = get_symbol_info(symbol, OPT_EXCHANGE)
        if si and getattr(si, "lotsize", None):
            ls = int(si.lotsize)
            logger.info(f"  Lot size from DB: {ls}  ({symbol})")
            return ls
    except Exception as e:
        logger.warning(f"  Lot size DB lookup failed ({e}). Using default {DEFAULT_LOT_SIZE}.")
    return DEFAULT_LOT_SIZE


def _get_suitable_expiry(today: date) -> tuple[str | None, date | None]:
    """
    Return the nearest NIFTY weekly Tuesday expiry string + date, with DTE ≥ MIN_DTE.
    Returns (None, None) if nothing suitable found.
    """
    dates = get_expiry_dates(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, "options")
    for d in dates:
        try:
            exp_dt = datetime.strptime(d, "%d%b%y").date()
            dte = (exp_dt - today).days
            if dte >= MIN_DTE:
                logger.info(f"  Suitable expiry: {d}  (DTE={dte})")
                return d, exp_dt
        except ValueError:
            continue
    logger.warning(f"  No expiry with DTE ≥ {MIN_DTE}. Available: {dates[:5]}")
    return None, None


def _is_expiry_day(today: date) -> bool:
    """Return True if today is a NIFTY expiry day."""
    dates = get_expiry_dates(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, "options")
    for d in dates:
        try:
            if datetime.strptime(d, "%d%b%y").date() == today:
                return True
        except ValueError:
            continue
    return False


def _is_week2(today: date) -> bool:
    """Return True if today's calendar day is in 8–14 (Week-2 filter)."""
    return 8 <= today.day <= 14


def _parse_row_dt(row: dict) -> datetime | None:
    """Parse timestamp from a get_history row (epoch int or ISO string)."""
    raw = row.get("timestamp", row.get("date", ""))
    if raw == "" or raw is None:
        return None
    try:
        return datetime.fromtimestamp(float(raw))
    except (ValueError, TypeError):
        pass
    try:
        return datetime.fromisoformat(str(raw))
    except (ValueError, TypeError):
        return None


def _1m_bars_to_3m(bars_1m: list[dict]) -> list[dict]:
    """
    Convert a list of completed 1m OHLC dicts to 3m bars.
    Drops trailing incomplete groups.
    """
    out = []
    for i in range(0, len(bars_1m) - (len(bars_1m) % TF_MINUTES), TF_MINUTES):
        chunk = bars_1m[i:i + TF_MINUTES]
        if len(chunk) < TF_MINUTES:
            break
        out.append({
            "open":  chunk[0]["open"],
            "high":  max(c["high"] for c in chunk),
            "low":   min(c["low"]  for c in chunk),
            "close": chunk[-1]["close"],
        })
    return out


# ══════════════════════════════════════════════════════════════════════════════
class NiftyMaCrossSellerBot:
# ══════════════════════════════════════════════════════════════════════════════
    """
    SMA(15)/SMA(225) on 3m NIFTY bars — overnight ATM option seller.
    Streams 1-min NIFTY index ticks via WebSocket, accumulates 1m bars,
    resamples to 3m, and fires on MA crossover signals.
    Position held NRML (overnight) until reversal cross, SL hit, or expiry gate.
    """

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)

        # Live price tracking
        self.nifty_ltp: float = 0.0
        self.vix_ltp:   float = 0.0

        # 1-min bar builder  (deque of completed 1m OHLC dicts)
        self._bars_1m: deque = deque()
        self._bar_open:   float = 0.0
        self._bar_high:   float = 0.0
        self._bar_low:    float = float("inf")
        self._bar_minute: int   = -1
        self._last_ltp:   float = 0.0

        # Count of 1m bars since the last completed 3m bar
        self._1m_since_3m: int = 0

        # Completed 3m bars (maxlen generous — well beyond 225 + lockout)
        self.bars_3m: deque = deque(maxlen=2000)

        # Anti-whipsaw lockout: bars since last accepted cross (in 3m bars)
        self._bars_since_cross: int = LOCKOUT_BARS  # start unlocked

        # Active position state (None = flat)
        self.active_trade: dict | None = None

        # Session metadata
        self.expiry_str:  str  | None = None
        self.expiry_date: date | None = None
        self.lot_size: int = DEFAULT_LOT_SIZE
        self._session_started = False
        self._eod_checked     = False

        # Previous 3m bar's fast/slow SMA values (for cross detection)
        self._prev_fast: float | None = None
        self._prev_slow: float | None = None

        self._subscribed_syms: set[str] = set()

        # Decision-state logging (jsonl + throttled heartbeat)
        self._dlog = DecisionLogger(DECISION_LOG, heartbeat_secs=HEARTBEAT_SECS, bot_logger=logger)

        # Dead-feed watchdog: alerts if NIFTY/VIX ticks go quiet for
        # DEAD_FEED_SECS during market hours. The option leg is deliberately
        # excluded — unlike the trend_seller bots, this bot never subscribes
        # to it over the WS feed (_subscribe() only sends NIFTY/VIX); position
        # monitoring uses a REST get_option_ltp() poll every 3m bar instead,
        # backed by a broker-side SL-M order as primary protection. Tracking
        # it here guaranteed a false DEAD FEED alert for the entire duration
        # of every trade.
        self._watchdog = TickWatchdog(
            bot_name="NIFTY MA Cross Seller Bot",
            tracked_symbols=lambda: [IDX_SYMBOL, VIX_SYMBOL],
            market_open=MARKET_OPEN,
            market_close=SESSION_END,
            bot_logger=logger,
        )

        self._restore_state()

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE PERSISTENCE
    # ══════════════════════════════════════════════════════════════════════════

    def _restore_state(self) -> None:
        """Reload active trade from the state file if it was written today or earlier
        (overnight positions persist across dates, so we don't filter by date here)."""
        if not STATE_FILE.exists():
            return
        try:
            state = json.loads(STATE_FILE.read_text())
            trade = state.get("active_trade")
            if trade:
                exp_date_str = trade.get("expiry_date", "")
                expired = False
                if exp_date_str:
                    try:
                        expired = date.fromisoformat(exp_date_str) < datetime.now().date()
                    except ValueError:
                        pass
                if expired:
                    # A past-expiry active_trade means this bot's own
                    # EXPIRY_GATE/EOD_EXPIRY exit was missed (e.g. a failed
                    # LTP poll skipped it — see _monitor_position). By now the
                    # contract is gone from the master file and the broker's
                    # own squareoff/settlement has already closed it — there's
                    # nothing left for this bot to exit. Discard the stale
                    # state instead of resuming a dead position.
                    logger.warning(
                        f"♻️  Discarding stale ACTIVE trade {trade.get('symbol')} "
                        f"— expiry {exp_date_str} already passed; the position "
                        f"was already settled by the platform, not this bot."
                    )
                    state["active_trade"] = None
                    STATE_FILE.write_text(json.dumps(state, indent=2))
                else:
                    self.active_trade = trade
                    logger.warning(
                        f"♻️  Restored ACTIVE trade: {trade.get('symbol')} "
                        f"entry={trade.get('entry_prem')} sl={trade.get('sl_prem')}"
                    )
            bars_since = state.get("bars_since_cross")
            if bars_since is not None:
                self._bars_since_cross = int(bars_since)
        except Exception as e:
            logger.warning(f"_restore_state failed: {e}")

    def _save_state(self, signal_snapshot: dict | None = None) -> None:
        """
        Persist bot state to JSON. Called after every 3m bar and on position changes.
        Writes rich indicator + filter context so the dashboard can show live decision state.
        """
        try:
            today = datetime.now().date()
            now   = datetime.now()

            # ── Compute current SMA values from bars_3m ───────────────────────
            sma_fast = sma_slow = gap = None
            bars_count = len(self.bars_3m)
            if bars_count >= SMA_SLOW:
                closes   = pd.Series([b["close"] for b in self.bars_3m])
                fast_s   = closes.rolling(SMA_FAST).mean()
                slow_s   = closes.rolling(SMA_SLOW).mean()
                sma_fast = round(float(fast_s.iloc[-1]), 2)
                sma_slow = round(float(slow_s.iloc[-1]), 2)
                gap      = round(sma_fast - sma_slow, 2)

            # ── Filter statuses ───────────────────────────────────────────────
            is_week2          = _is_week2(today)
            after_entry_cutoff = now.time() >= ENTRY_CUTOFF
            lockout_remaining  = max(0, LOCKOUT_BARS - self._bars_since_cross)
            locked             = lockout_remaining > 0

            # Danger zone (soft filter)
            danger_zone = False
            if self.vix_ltp > 0 and self.vix_ltp < DANGER_VIX_MAX:
                danger_zone = True   # ADX part checked at entry-time only; VIX < 13 is the visible flag

            # DTE info from active trade or next suitable expiry cache
            expiry_str  = self.expiry_str
            expiry_date = self.expiry_date
            dte = None
            if expiry_date:
                dte = (expiry_date - today).days

            # Determine high-level signal readiness
            if self.active_trade is not None:
                readiness = "IN_POSITION"
            elif is_week2:
                readiness = "WEEK2_BLOCKED"
            elif after_entry_cutoff:
                readiness = "CUTOFF_PASSED"
            elif locked:
                readiness = "LOCKOUT"
            elif bars_count < MIN_BARS_REQUIRED:
                readiness = "WARMING_UP"
            elif danger_zone:
                readiness = "DANGER_ZONE"
            else:
                readiness = "READY"

            STATE_FILE.write_text(json.dumps({
                # core
                "active_trade":       self.active_trade,
                "bars_since_cross":   self._bars_since_cross,
                "last_update":        now.isoformat(),
                # SMA engine
                "sma_fast":           sma_fast,
                "sma_slow":           sma_slow,
                "sma_gap":            gap,
                "bars_3m_count":      bars_count,
                "sma_warmed":         bars_count >= MIN_BARS_REQUIRED,
                # cross tracking (injected from signal_snapshot if provided)
                "last_cross_dir":     (signal_snapshot or {}).get("last_cross_dir"),
                "last_cross_bars_ago":(signal_snapshot or {}).get("last_cross_bars_ago"),
                # live prices
                "nifty_ltp":          round(self.nifty_ltp, 2),
                "vix_ltp":            round(self.vix_ltp, 2),
                # expiry
                "expiry_str":         expiry_str,
                "expiry_date":        expiry_date.isoformat() if expiry_date else None,
                "dte":                dte,
                # filters
                "filters": {
                    "week2_blocked":       is_week2,
                    "calendar_day":        today.day,
                    "after_entry_cutoff":  after_entry_cutoff,
                    "locked":              locked,
                    "lockout_remaining":   lockout_remaining,
                    "lockout_total":       LOCKOUT_BARS,
                    "bars_since_cross":    self._bars_since_cross,
                    "danger_zone_vix":     danger_zone,
                    "vix_threshold":       DANGER_VIX_MAX,
                    "dte_ok":              (dte is not None and dte >= MIN_DTE),
                    "min_dte":             MIN_DTE,
                },
                "readiness":          readiness,
            }, indent=2))
        except Exception as e:
            logger.error(f"_save_state failed: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  HISTORY WARM-UP
    # ══════════════════════════════════════════════════════════════════════════

    def _load_history(self) -> None:
        """
        Fetch 15 days of 1-min NIFTY bars and convert to 3m bars to warm up SMA(225).
        SMA(225) on 3m bars needs 225 × 3 = 675 1m bars = ~9 sessions.
        duration_days=15 provides a comfortable buffer.
        """
        try:
            raw = get_history(API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "1m", HISTORY_DAYS)
            if not raw:
                logger.warning("  History load: no data — cold start.")
                return

            today = datetime.now().date()
            bars_by_day: dict[date, list[dict]] = {}

            for row in raw:
                dt = _parse_row_dt(row)
                if dt is None:
                    continue
                if dt.time() < MARKET_OPEN:
                    continue
                d = dt.date()
                if d not in bars_by_day:
                    bars_by_day[d] = []
                bars_by_day[d].append({
                    "open":  float(row["open"]),
                    "high":  float(row["high"]),
                    "low":   float(row["low"]),
                    "close": float(row["close"]),
                })

            # Convert each day's 1m bars to 3m bars
            all_3m: list[dict] = []
            for d in sorted(bars_by_day.keys()):
                if d > today:
                    continue
                day_3m = _1m_bars_to_3m(bars_by_day[d])
                all_3m.extend(day_3m)

            for bar in all_3m:
                self.bars_3m.append(bar)

            # Track how many 1m bars from today are already consumed into 3m bars
            today_1m = bars_by_day.get(today, [])
            completed_groups = (len(today_1m) // TF_MINUTES) * TF_MINUTES
            self._1m_since_3m = len(today_1m) - completed_groups

            logger.info(
                f"  History loaded: {len(self.bars_3m)} 3m bars  "
                f"(today residual: {self._1m_since_3m} 1m bars)"
            )

            # Initialise prev_fast / prev_slow from loaded history
            self._update_prev_sma()

        except Exception as e:
            logger.warning(f"  History load failed ({e}). Cold start.")

    def _update_prev_sma(self) -> None:
        """Update _prev_fast/_prev_slow from the last completed 3m bar."""
        if len(self.bars_3m) < SMA_SLOW:
            return
        closes = pd.Series([b["close"] for b in self.bars_3m])
        self._prev_fast = float(closes.rolling(SMA_FAST).mean().iloc[-1])
        self._prev_slow = float(closes.rolling(SMA_SLOW).mean().iloc[-1])

    # ══════════════════════════════════════════════════════════════════════════
    #  1-MIN BAR BUILDER
    # ══════════════════════════════════════════════════════════════════════════

    def _update_1m_bar(self, ltp: float, ts: datetime) -> bool:
        """Feed one raw tick. Returns True when a 1-minute bar completes."""
        cur_min = ts.hour * 60 + ts.minute

        if self._bar_minute == -1:
            self._bar_minute = cur_min
            self._bar_open   = ltp
            self._bar_high   = ltp
            self._bar_low    = ltp
            self._last_ltp   = ltp
            return False

        if cur_min != self._bar_minute:
            if self._bar_open > 0:
                self._bars_1m.append({
                    "open":  self._bar_open,
                    "high":  self._bar_high,
                    "low":   self._bar_low,
                    "close": self._last_ltp,
                })
                self._1m_since_3m += 1

            self._bar_minute = cur_min
            self._bar_open   = ltp
            self._bar_high   = ltp
            self._bar_low    = ltp
            self._last_ltp   = ltp
            return True

        self._bar_high = max(self._bar_high, ltp)
        self._bar_low  = min(self._bar_low,  ltp)
        self._last_ltp = ltp
        return False

    def _try_complete_3m_bar(self) -> bool:
        """
        If we have TF_MINUTES=3 new 1m bars since the last 3m bar, build one.
        Returns True when a new 3m bar is completed.
        """
        if self._1m_since_3m < TF_MINUTES:
            return False

        # Grab the last TF_MINUTES 1m bars from the deque
        recent = list(self._bars_1m)[-TF_MINUTES:]
        if len(recent) < TF_MINUTES:
            return False

        bar_3m = {
            "open":  recent[0]["open"],
            "high":  max(b["high"] for b in recent),
            "low":   min(b["low"]  for b in recent),
            "close": recent[-1]["close"],
        }
        self.bars_3m.append(bar_3m)
        self._1m_since_3m -= TF_MINUTES
        return True

    # ══════════════════════════════════════════════════════════════════════════
    #  SMA CROSS SIGNAL ENGINE
    # ══════════════════════════════════════════════════════════════════════════

    def _check_signal(self) -> str | None:
        """
        Compute SMA(15)/SMA(225) on the completed 3m bar deque.
        Detects crossover from previous bar to current bar.
        Returns "CE" (bearish cross → sell CE), "PE" (bullish cross → sell PE), or None.
        """
        if len(self.bars_3m) < MIN_BARS_REQUIRED:
            return None

        closes = pd.Series([b["close"] for b in self.bars_3m])
        fast_s  = closes.rolling(SMA_FAST).mean()
        slow_s  = closes.rolling(SMA_SLOW).mean()

        cur_fast  = float(fast_s.iloc[-1])
        cur_slow  = float(slow_s.iloc[-1])
        prev_fast = float(fast_s.iloc[-2])
        prev_slow = float(slow_s.iloc[-2])

        if any(pd.isna(v) for v in [cur_fast, cur_slow, prev_fast, prev_slow]):
            return None

        # Increment lockout counter
        self._bars_since_cross += 1

        # Log current SMA values for debugging
        logger.info(
            f"  [SMA] fast={cur_fast:.1f} slow={cur_slow:.1f}  "
            f"prev_fast={prev_fast:.1f} prev_slow={prev_slow:.1f}  "
            f"lockout={self._bars_since_cross}/{LOCKOUT_BARS}"
        )

        # Bullish cross: fast was below slow, now above
        if prev_fast <= prev_slow and cur_fast > cur_slow:
            logger.info(f"  📶 Bullish cross detected (fast crossed above slow)")
            if self._bars_since_cross < LOCKOUT_BARS:
                logger.info(f"  ⏳ Lockout active ({self._bars_since_cross}/{LOCKOUT_BARS}) — skipping PE entry")
                return None
            return "PE"

        # Bearish cross: fast was above slow, now below
        if prev_fast >= prev_slow and cur_fast < cur_slow:
            logger.info(f"  📶 Bearish cross detected (fast crossed below slow)")
            if self._bars_since_cross < LOCKOUT_BARS:
                logger.info(f"  ⏳ Lockout active ({self._bars_since_cross}/{LOCKOUT_BARS}) — skipping CE entry")
                return None
            return "CE"

        return None

    def _sma_snapshot(self) -> dict | None:
        """Compute current fast/slow SMA + previous-bar values with no side
        effects (unlike _check_signal, which also advances the lockout
        counter). Used purely for decision-state logging."""
        if len(self.bars_3m) < MIN_BARS_REQUIRED:
            return None
        closes = pd.Series([b["close"] for b in self.bars_3m])
        fast_s = closes.rolling(SMA_FAST).mean()
        slow_s = closes.rolling(SMA_SLOW).mean()
        cur_fast, cur_slow   = float(fast_s.iloc[-1]), float(slow_s.iloc[-1])
        prev_fast, prev_slow = float(fast_s.iloc[-2]), float(slow_s.iloc[-2])
        if any(pd.isna(v) for v in [cur_fast, cur_slow, prev_fast, prev_slow]):
            return None
        return {
            "fast":      round(cur_fast, 2),
            "slow":      round(cur_slow, 2),
            "gap":       round(cur_fast - cur_slow, 2),
            "prev_fast": round(prev_fast, 2),
            "prev_slow": round(prev_slow, 2),
        }

    def _detect_reversal(self) -> bool:
        """
        Return True if a reversal cross has just fired (opposite to the active trade).
        Used to exit overnight positions.
        """
        if self.active_trade is None:
            return False
        if len(self.bars_3m) < MIN_BARS_REQUIRED:
            return False

        closes    = pd.Series([b["close"] for b in self.bars_3m])
        fast_s    = closes.rolling(SMA_FAST).mean()
        slow_s    = closes.rolling(SMA_SLOW).mean()
        cur_fast  = float(fast_s.iloc[-1])
        cur_slow  = float(slow_s.iloc[-1])
        prev_fast = float(fast_s.iloc[-2])
        prev_slow = float(slow_s.iloc[-2])

        if any(pd.isna(v) for v in [cur_fast, cur_slow, prev_fast, prev_slow]):
            return False

        opt_type = self.active_trade.get("opt_type", "")

        # If we sold CE (bearish position), reversal = bullish cross
        if opt_type == "CE" and prev_fast <= prev_slow and cur_fast > cur_slow:
            logger.info(f"  🔄 Reversal (bullish cross) — exiting CE position")
            return True

        # If we sold PE (bullish position), reversal = bearish cross
        if opt_type == "PE" and prev_fast >= prev_slow and cur_fast < cur_slow:
            logger.info(f"  🔄 Reversal (bearish cross) — exiting PE position")
            return True

        return False

    # ══════════════════════════════════════════════════════════════════════════
    #  BROKER-SIDE SL-M HELPER
    # ══════════════════════════════════════════════════════════════════════════

    async def _place_sl_m(self, symbol: str, qty: int, trigger_price: float) -> str | None:
        """
        Place a resting broker-side SL-M (buy-to-close) order for an NRML short.
        NSE F&O SL-M orders are DAY validity — they expire at session close even
        for NRML (overnight) positions, so this must be re-placed every trading
        day the position remains open (see _session_setup()).
        """
        try:
            sl_resp = self.client.placeorder(
                strategy      = STRATEGY_NAME,
                symbol        = symbol,
                action        = "BUY",
                exchange      = OPT_EXCHANGE,
                price_type    = "SL-M",
                trigger_price = str(trigger_price),
                product       = "NRML",
                quantity      = str(qty),
            )
        except Exception as e:
            logger.error(f"  Broker-side SL-M placement exception: {e}")
            sl_resp = None

        if sl_resp and sl_resp.get("status") == "success":
            sl_order_id = str(sl_resp.get("orderid", ""))
            logger.info(f"  🛡️ Broker-side SL-M resting @ trigger ₹{trigger_price:.2f}  order_id={sl_order_id}")
            return sl_order_id

        logger.error(f"  ⚠️ Broker-side SL-M FAILED to place ({sl_resp}) — falling back to app-side monitoring only.")
        await send_async(f"⚠️ *{STRATEGY_NAME}* — Broker-side SL-M order failed to place ({symbol})!\nFalling back to app-side 3m-bar monitoring only — slippage risk on SL exit.")
        return None

    # ══════════════════════════════════════════════════════════════════════════
    #  ENTRY
    # ══════════════════════════════════════════════════════════════════════════

    async def _enter_trade(self, opt_type: str, now: datetime) -> None:
        today = now.date()

        # ── Week-2 filter (MANDATORY) ─────────────────────────────────────────
        if _is_week2(today):
            logger.info(f"  🚫 Week-2 filter (day {today.day}) — skipping {opt_type} entry")
            return

        # ── Entry cutoff ──────────────────────────────────────────────────────
        if now.time() >= ENTRY_CUTOFF:
            logger.info(f"  🚫 After entry cutoff {ENTRY_CUTOFF} — skipping {opt_type} entry")
            return

        # ── Only one position at a time ───────────────────────────────────────
        if self.active_trade is not None:
            logger.info(f"  🚫 Already have open position ({self.active_trade.get('opt_type')}) — skip")
            return

        # ── Get suitable expiry ───────────────────────────────────────────────
        expiry_str, expiry_dt = _get_suitable_expiry(today)
        if not expiry_str or expiry_dt is None:
            logger.warning(f"  No suitable expiry — skip {opt_type} entry")
            return

        # ── Optional danger-zone filter (Stage 7) ────────────────────────────
        if self.vix_ltp > 0 and self.vix_ltp < DANGER_VIX_MAX:
            # Compute daily ADX from recent 3m bars as proxy
            # This is a soft filter; skip if data is thin
            closes = pd.Series([b["close"] for b in self.bars_3m])
            highs  = pd.Series([b["high"]  for b in self.bars_3m])
            lows   = pd.Series([b["low"]   for b in self.bars_3m])
            try:
                adx_df = ta.adx(highs, lows, closes, length=14)
                adx_col = "ADX_14"
                if adx_df is not None and adx_col in adx_df.columns:
                    adx_val = float(adx_df[adx_col].iloc[-1])
                    if not pd.isna(adx_val) and adx_val >= DANGER_ADX_MIN:
                        logger.info(
                            f"  ⚠️  Danger zone: VIX={self.vix_ltp:.1f} < {DANGER_VIX_MAX} "
                            f"AND ADX={adx_val:.1f} ≥ {DANGER_ADX_MIN} — skipping {opt_type} entry"
                        )
                        return
            except Exception:
                pass  # soft filter — don't skip on data error

        spot = self.nifty_ltp
        if spot <= 0:
            logger.warning("  NIFTY LTP = 0 — skipping entry")
            return

        # ── Resolve ATM option symbol ─────────────────────────────────────────
        symbol = await asyncio.to_thread(
            get_option_symbol,
            API_KEY, IDX_SYMBOL, OPT_EXCHANGE, expiry_str, opt_type, "ATM",
        )
        if not symbol:
            logger.error(f"  [{opt_type}] ATM symbol resolution failed (spot={spot:.0f}, expiry={expiry_str})")
            return

        # ── Fetch live premium ────────────────────────────────────────────────
        opt_ltp = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        if opt_ltp <= 0:
            logger.warning(f"  [{opt_type}] LTP = 0 for {symbol}. Skipping entry.")
            return

        # ── Lot size (dynamic) ────────────────────────────────────────────────
        lot_size = _get_lot_size(symbol)
        qty      = N_LOTS * lot_size
        sl_prem  = round(opt_ltp * SL_MULTIPLE, 2)

        logger.info(
            f"  [{opt_type}] ENTRY: {symbol}  LTP=₹{opt_ltp:.2f}  "
            f"qty={qty} ({N_LOTS} lots × {lot_size})  "
            f"SL=₹{sl_prem:.2f} (3×)  expiry={expiry_str}"
        )

        # ── Place SELL order (NRML — overnight hold) ──────────────────────────
        # MARKET first; if the venue cannot price it at that instant (transient
        # quote gap), retry once as LIMIT at the observed LTP rather than
        # dropping the cross — the lockout means the next one is days away.
        res = None
        for price_type, price in (("MARKET", None), ("LIMIT", opt_ltp)):
            order_kwargs = {
                "strategy":      STRATEGY_NAME,
                "symbol":        symbol,
                "action":        "SELL",
                "exchange":      OPT_EXCHANGE,
                "price_type":    price_type,
                "product":       "NRML",
                "quantity":      qty,
                "position_size": -qty,    # negative = short target
            }
            if price is not None:
                order_kwargs["price"] = price
            try:
                res = self.client.placesmartorder(**order_kwargs)
            except Exception as e:
                logger.error(f"  [{opt_type}] placesmartorder exception ({price_type}): {e}")
                res = None
            if isinstance(res, dict) and res.get("status") in ("success", "ok"):
                break
            if price_type == "MARKET":
                logger.warning(f"  [{opt_type}] MARKET order failed: {res} — "
                               f"retrying as LIMIT @ ₹{opt_ltp:.2f} in 5s")
                await asyncio.sleep(5)

        if isinstance(res, dict) and res.get("status") in ("success", "ok"):
            fill_prem = _resolve_fill(res, opt_ltp)
            sl_prem   = round(fill_prem * SL_MULTIPLE, 2)

            sl_order_id = await self._place_sl_m(symbol, qty, sl_prem)

            self.active_trade = {
                "symbol":       symbol,
                "opt_type":     opt_type,
                "entry_prem":   fill_prem,
                "sl_prem":      sl_prem,
                "sl_order_id":  sl_order_id,
                "sl_order_day": today.isoformat(),
                "qty":          qty,
                "lot_size":     lot_size,
                "expiry_str":   expiry_str,
                "expiry_date":  expiry_dt.isoformat(),
                "entry_ts":     now.isoformat(),
                "entry_day":    today.isoformat(),
            }
            self._bars_since_cross = 0
            self._save_state()

            await send_async(
                f"📉 *{STRATEGY_NAME}* — ENTRY\n"
                f"SELL {opt_type} {symbol}\n"
                f"Premium: ₹{fill_prem:.2f} | SL: ₹{sl_prem:.2f} | Qty: {qty}"
                f"{' (broker SL-M resting)' if sl_order_id else ' (⚠️ app-side only)'}\n"
                f"Expiry: {expiry_str} | Week-2 filter ✓ | NRML (overnight)"
            )
        else:
            logger.error(f"  [{opt_type}] placesmartorder failed: {res}")

    # ══════════════════════════════════════════════════════════════════════════
    #  EXIT
    # ══════════════════════════════════════════════════════════════════════════

    async def _exit_trade(self, reason: str, exit_prem: float | None = None, *, already_filled_order_id: str | None = None) -> None:
        if self.active_trade is None:
            return

        trade  = self.active_trade
        symbol = trade["symbol"]
        qty    = trade["qty"]

        sl_order_id = trade.get("sl_order_id")
        if sl_order_id and sl_order_id != already_filled_order_id:
            await asyncio.to_thread(_cancel_order, self.client, sl_order_id, symbol, OPT_EXCHANGE)

        # Fetch current premium for logging if not provided
        if exit_prem is None:
            try:
                exit_prem = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
            except Exception:
                exit_prem = 0.0

        logger.info(
            f"  [{trade['opt_type']}] EXIT ({reason}): {symbol}  "
            f"entry=₹{trade['entry_prem']:.2f}  exit=₹{exit_prem:.2f}  qty={qty}"
        )

        if already_filled_order_id:
            exit_fill = exit_prem or 0.0
            logger.info(f"  SL-M already filled @ ₹{exit_fill:.2f} — no new close order needed.")
        else:
            try:
                res = self.client.placesmartorder(
                    strategy      = STRATEGY_NAME,
                    symbol        = symbol,
                    action        = "BUY",
                    exchange      = OPT_EXCHANGE,
                    price_type    = "MARKET",
                    product       = "NRML",
                    quantity      = qty,
                    position_size = 0,    # flatten
                )
                logger.info(f"  BUY-to-close: {res}")
            except Exception as e:
                logger.error(f"  Exit order failed: {e}")
                res = None

            # Resolve actual fill price for the close order (falls back to the
            # LTP snapshot that triggered this exit if the lookup fails).
            exit_fill = _resolve_fill(res, exit_prem or 0.0)

        pnl = (trade["entry_prem"] - exit_fill) * qty if exit_fill else 0.0
        logger.info(f"  Estimated P&L: ₹{pnl:.0f}")

        # Log trade
        try:
            log_trade_to_db(
                bot_name      = "nifty_ma_cross_seller_bot",
                instrument    = IDX_SYMBOL,
                option_symbol = symbol,
                option_type   = trade["opt_type"],
                entry_time    = trade["entry_ts"],
                exit_time     = datetime.now(),
                entry_premium = trade["entry_prem"],
                exit_premium  = exit_fill or 0.0,
                exit_reason   = reason,
                quantity      = qty,
                lots          = N_LOTS,
                lot_size      = trade["lot_size"],
                gross_pnl     = round(pnl, 2),
                direction     = "sell",
            )
        except Exception as e:
            logger.warning(f"  log_trade_to_db failed: {e}")

        await send_async(
            f"✅ *{STRATEGY_NAME}* — EXIT ({reason})\n"
            f"{symbol}  entry=₹{trade['entry_prem']:.2f}  exit=₹{exit_fill:.2f}\n"
            f"Est. P&L: ₹{pnl:.0f}"
        )

        self.active_trade = None
        self._save_state()

    # ══════════════════════════════════════════════════════════════════════════
    #  POSITION MONITOR (called on every completed 3m bar)
    # ══════════════════════════════════════════════════════════════════════════

    async def _monitor_position(self, now: datetime) -> None:
        if self.active_trade is None:
            return

        trade = self.active_trade

        # Reconciliation: has the resting broker-side SL-M order already
        # filled? It fires instantly on trigger — this only catches the app
        # state up (checked every 3m bar, vs. instant exchange enforcement).
        if trade.get("sl_order_id"):
            filled, fill_price = await asyncio.to_thread(
                _check_fill, self.client, trade["sl_order_id"], trade["symbol"], OPT_EXCHANGE
            )
            if filled:
                logger.warning(f"  ⛔ Broker-side SL-M filled @ ₹{fill_price:.2f}")
                await self._exit_trade("SL_3X (broker)", exit_prem=fill_price, already_filled_order_id=trade["sl_order_id"])
                return

        today = now.date()
        exp_date_str = trade.get("expiry_date", "")

        # ── Expiry gate (exit at 14:30 on expiry day) — time-based, checked
        # BEFORE the LTP fetch below on purpose: near expiry a contract's
        # quote can start erroring out (thin liquidity / delisting) right
        # when this exit matters most. Gating this on a successful LTP fetch
        # let a single failed poll skip the mandatory exit entirely, leaving
        # the position to expire un-exited by the bot (platform-side
        # squareoff_manager had to clean it up instead, and this bot's own
        # state was never cleared — see _restore_state's reconciliation for
        # the resulting stale-state fallback). _exit_trade fetches its own
        # exit premium and tolerates that failing too, so this fires
        # unconditionally by time regardless of quote availability. ─────────
        if exp_date_str:
            try:
                exp_date = date.fromisoformat(exp_date_str)
                if today == exp_date and now.time() >= EXPIRY_GATE:
                    logger.info(f"  ⏰ Expiry gate (14:30 on expiry day) — exiting")
                    await self._exit_trade("EXPIRY_GATE")
                    return
            except ValueError:
                pass

        # ── EOD safety exit on expiry day at 15:15 ───────────────────────────
        if exp_date_str:
            try:
                if today == date.fromisoformat(exp_date_str) and now.time() >= SESSION_END:
                    logger.info(f"  ⏰ EOD safety exit on expiry day — exiting")
                    await self._exit_trade("EOD_EXPIRY")
                    return
            except ValueError:
                pass

        # ── Fetch current option premium ──────────────────────────────────────
        try:
            cur_prem = await asyncio.to_thread(
                get_option_ltp, trade["symbol"], OPT_EXCHANGE, API_KEY
            )
        except Exception as e:
            logger.warning(f"  LTP fetch for {trade['symbol']} failed: {e}")
            return

        logger.info(
            f"  [MON] {trade['opt_type']} {trade['symbol']}  "
            f"entry=₹{trade['entry_prem']:.2f}  now=₹{cur_prem:.2f}  "
            f"sl=₹{trade['sl_prem']:.2f}"
        )

        # ── SL check (3× entry premium) — fallback-only: the broker-side
        # SL-M order is the primary enforcement mechanism now. This only
        # fires if that order failed to place. ───────────────────────────────
        if not trade.get("sl_order_id") and cur_prem >= trade["sl_prem"]:
            logger.warning(
                f"  ⛔ SL HIT: {cur_prem:.2f} ≥ {trade['sl_prem']:.2f} — exiting"
            )
            await self._exit_trade("SL_3X", exit_prem=cur_prem)
            return

        # ── Reversal cross ─────────────────────────────────────────────────────
        if self._detect_reversal():
            await self._exit_trade("REVERSAL_CROSS", exit_prem=cur_prem)
            return

    # ══════════════════════════════════════════════════════════════════════════
    #  OVERNIGHT SL-M REFRESH
    # ══════════════════════════════════════════════════════════════════════════

    async def _refresh_overnight_sl_m(self, today: date) -> None:
        """
        NSE F&O SL-M orders are DAY validity — even for NRML (overnight)
        positions, yesterday's resting SL-M order expires at session close
        and will NOT protect the position today. Called once at session
        start whenever a position was carried over from a prior day.

        First reconciles: it's possible yesterday's SL-M filled late (after
        the last state write, before this restart) but the exchange purged
        the resulting fill notification along with the DAY order at EOD —
        so we also sanity-check via a live LTP read before assuming still-open.
        Then places a fresh SL-M order for today's session.
        """
        trade = self.active_trade
        if trade is None:
            return

        if trade.get("sl_order_day") == today.isoformat():
            # Already refreshed today (e.g. duplicate _session_setup call) — skip.
            return

        # Reconcile: if yesterday's SL-M order somehow shows filled (unlikely once
        # the exchange purges DAY orders, but check defensively before re-placing).
        old_sl_order_id = trade.get("sl_order_id")
        if old_sl_order_id:
            filled, fill_price = await asyncio.to_thread(
                _check_fill, self.client, old_sl_order_id, trade["symbol"], OPT_EXCHANGE
            )
            if filled:
                logger.warning(f"  ⛔ Overnight SL-M filled before restart @ ₹{fill_price:.2f}")
                await self._exit_trade("SL_3X (broker, overnight)", exit_prem=fill_price, already_filled_order_id=old_sl_order_id)
                return

        # Position is still open — yesterday's DAY-validity SL-M order has
        # expired regardless of whether it filled, so place a fresh one.
        sl_order_id = await self._place_sl_m(trade["symbol"], trade["qty"], trade["sl_prem"])
        trade["sl_order_id"]  = sl_order_id
        trade["sl_order_day"] = today.isoformat()
        self._save_state()

    # ══════════════════════════════════════════════════════════════════════════
    #  SESSION START SETUP
    # ══════════════════════════════════════════════════════════════════════════

    async def _session_setup(self) -> None:
        logger.info("═" * 64)
        logger.info("🌅 Session start — loading history and resolving metadata")

        await asyncio.to_thread(self._load_history)

        today = datetime.now().date()
        expiry_str, expiry_dt = await asyncio.to_thread(_get_suitable_expiry, today)
        self.expiry_str  = expiry_str
        self.expiry_date = expiry_dt

        on_expiry = await asyncio.to_thread(_is_expiry_day, today)
        week2     = _is_week2(today)

        logger.info(f"  Today: {today} | Expiry: {expiry_str} | On-expiry: {on_expiry} | Week-2: {week2}")

        if self.active_trade:
            logger.info(f"  Carrying overnight {self.active_trade['opt_type']} position: {self.active_trade['symbol']}")
            await self._refresh_overnight_sl_m(today)
        else:
            if week2:
                logger.info("  ⚠️  Week-2 day — no new entries allowed today")
            elif on_expiry:
                logger.info("  ⚠️  Expiry day — only exits allowed (entry cutoff 14:00, gate 14:30)")

        await send_async(
            f"☀️ *{STRATEGY_NAME}* started\n"
            f"Date: {today}  |  Expiry: {expiry_str or 'None'}\n"
            f"Week-2 block: {'🚫 YES' if week2 else '✅ No'}  |  "
            f"Expiry day: {'⚠️ YES' if on_expiry else 'No'}  |  "
            f"Open position: {self.active_trade['opt_type'] if self.active_trade else 'None'}"
        )

        self._session_started = True

    # ══════════════════════════════════════════════════════════════════════════
    #  DECISION-STATE LOGGING
    # ══════════════════════════════════════════════════════════════════════════

    def _verdict(self, now: datetime) -> str:
        """What's currently blocking entry — checked in the order these gates
        actually apply in _enter_trade()/_on_3m_bar_close() (session start →
        active position → Week-2 → entry cutoff → expiry → warm-up →
        anti-whipsaw lockout), so it always names the real blocker."""
        if not self._session_started:
            return "waiting for session start"
        if self.active_trade is not None:
            return f"ACTIVE: holding {self.active_trade.get('opt_type')} overnight (NRML)"
        today = now.date()
        if _is_week2(today):
            return f"BLOCKED: Week-2 filter (day {today.day} of month) — no entries"
        if now.time() >= ENTRY_CUTOFF:
            return f"done for today: after entry cutoff ({ENTRY_CUTOFF.strftime('%H:%M')})"
        if not self.expiry_str:
            return "BLOCKED: no suitable expiry found"
        if len(self.bars_3m) < MIN_BARS_REQUIRED:
            return f"warming up: {len(self.bars_3m)}/{MIN_BARS_REQUIRED} 3m bars"
        if self._bars_since_cross < LOCKOUT_BARS:
            return f"BLOCKED: anti-whipsaw lockout ({self._bars_since_cross}/{LOCKOUT_BARS} bars)"
        if self.vix_ltp > 0 and self.vix_ltp < DANGER_VIX_MAX:
            return f"🔥 in window — watching for SMA cross (danger-zone check pending, VIX={self.vix_ltp:.1f})"
        return "🔥 in window — watching for SMA(15/225) cross"

    def _heartbeat_text(self) -> str:
        now = datetime.now()
        lines = [
            f"💓 DECISION STATE {now.strftime('%H:%M:%S')} ─ {self._verdict(now)}",
            f"    NIFTY={self.nifty_ltp:.1f}  VIX={self.vix_ltp:.2f}  "
            f"3m_bars={len(self.bars_3m)}  expiry={self.expiry_str}",
        ]
        sma = self._sma_snapshot()
        if sma:
            lines.append(
                f"    SMA fast={sma['fast']} slow={sma['slow']} gap={sma['gap']}  "
                f"lockout={self._bars_since_cross}/{LOCKOUT_BARS}"
            )
        if self.active_trade:
            lines.append(
                f"    active: {self.active_trade.get('opt_type')} {self.active_trade.get('symbol')}  "
                f"entry=₹{self.active_trade.get('entry_prem')}  sl=₹{self.active_trade.get('sl_prem')}"
            )
        return "\n".join(lines)

    # ══════════════════════════════════════════════════════════════════════════
    #  WEBSOCKET MESSAGE HANDLER
    # ══════════════════════════════════════════════════════════════════════════

    async def _handle_ws_message(self, message: str | bytes) -> None:
        try:
            data = json.loads(message) if isinstance(message, (str, bytes)) else message
        except (json.JSONDecodeError, TypeError):
            return

        # Handle both {"data": {...}} envelope and raw dict
        feed = data.get("data", data) if isinstance(data, dict) else {}
        if not isinstance(feed, dict):
            return

        symbol = feed.get("symbol", feed.get("tk", ""))
        self._watchdog.on_tick(symbol)
        ltp    = feed.get("ltp", feed.get("last_price", feed.get("c", 0)))

        if not symbol or not ltp:
            return

        try:
            ltp = float(ltp)
        except (TypeError, ValueError):
            return

        if ltp <= 0:
            return

        now = datetime.now()

        if symbol == IDX_SYMBOL:
            self.nifty_ltp = ltp

            # Throttled to HEARTBEAT_SECS internally — cheap to call on every tick
            self._dlog.maybe_heartbeat(self._heartbeat_text)

            # Session setup on first NIFTY tick after market open
            if not self._session_started and now.time() >= MARKET_OPEN:
                await self._session_setup()

            # Only process bar logic during session
            if not self._session_started:
                return
            if now.time() < MARKET_OPEN:
                return

            # Build 1m bar
            bar_completed = self._update_1m_bar(ltp, now)

            if bar_completed:
                # Try to complete a 3m bar
                if self._try_complete_3m_bar():
                    await self._on_3m_bar_close(now)

        elif symbol == VIX_SYMBOL:
            self.vix_ltp = ltp

    async def _on_3m_bar_close(self, now: datetime) -> None:
        """Called when each 3m bar completes."""
        # Decision-state snapshot fires on every completed 3m bar regardless
        # of gating, so the log always has real values explaining "why not".
        sma = self._sma_snapshot()
        self._dlog.log_bar({
            "phase":            "ACTIVE" if self.active_trade else "WATCHING",
            "bar_time":         now.strftime("%H:%M"),
            "week2_blocked":    _is_week2(now.date()),
            "after_cutoff":     now.time() >= ENTRY_CUTOFF,
            "bars_3m":          len(self.bars_3m),
            "bars_since_cross": self._bars_since_cross,
            "locked":           self._bars_since_cross < LOCKOUT_BARS,
            "expiry":           self.expiry_str,
            "sma":              sma,
            "vix_ltp":          round(self.vix_ltp, 2),
            "verdict":          self._verdict(now),
            "active_trade":     self.active_trade.get("opt_type") if self.active_trade else None,
        })

        # Monitor active position first (SL / expiry gate / reversal)
        await self._monitor_position(now)

        # If still have position after monitor, don't look for new entry
        if self.active_trade is not None:
            self._save_state()
            return

        # Check for new signal
        signal = self._check_signal()
        if signal is not None:
            await self._enter_trade(signal, now)
        else:
            # Save state on every bar so dashboard always has fresh indicator snapshot
            self._save_state()

    # ══════════════════════════════════════════════════════════════════════════
    #  WEBSOCKET SUBSCRIPTION
    # ══════════════════════════════════════════════════════════════════════════

    async def _subscribe(self, ws) -> None:
        """Subscribe to NIFTY index + INDIA VIX via the OpenAlgo WebSocket."""
        syms = [
            {"symbol": IDX_SYMBOL, "exchange": IDX_EXCHANGE},
            {"symbol": VIX_SYMBOL, "exchange": VIX_EXCHANGE},
        ]
        msg = json.dumps({"action": "subscribe", "apikey": API_KEY, "symbols": syms})
        await ws.send(msg)
        logger.info(f"  Subscribed: {IDX_SYMBOL}, {VIX_SYMBOL}")

    # ══════════════════════════════════════════════════════════════════════════
    #  MAIN LOOP
    # ══════════════════════════════════════════════════════════════════════════

    async def run(self) -> None:
        logger.info(f"🚀 {STRATEGY_NAME} starting")
        logger.info(f"   SMA({SMA_FAST}/{SMA_SLOW}) on {TF_MINUTES}m bars | "
                    f"NRML overnight | 10 lots | SL {SL_MULTIPLE}× | Week-2 blocked")
        logger.info("   OpenAlgo mode (paper/live) is set in the OpenAlgo UI — not here")

        asyncio.create_task(self._watchdog.watch_loop())

        reconnect_delay = 5

        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
                    # Authenticate first — proxy requires this before routing market data
                    await ws.send(json.dumps({
                        "action":  "authenticate",
                        "api_key": API_KEY,
                    }))
                    await asyncio.sleep(0.5)

                    await self._subscribe(ws)
                    reconnect_delay = 5  # reset on successful connect

                    async for message in ws:
                        await self._handle_ws_message(message)

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket closed: {e}. Reconnecting in {reconnect_delay}s…")
            except Exception as e:
                logger.error(f"WebSocket error: {e}. Reconnecting in {reconnect_delay}s…")

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)
            self._session_started = False   # re-do session setup on reconnect


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    bot = NiftyMaCrossSellerBot()
    asyncio.run(bot.run())
