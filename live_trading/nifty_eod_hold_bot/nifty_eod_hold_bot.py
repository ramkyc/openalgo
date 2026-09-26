"""
NIFTY EOD Hold Bot
==================
live_trading/nifty_eod_hold_bot/nifty_eod_hold_bot.py

Sells an ATM NIFTY weekly option on a 1-min candlestick reversal signal
(hammer/shooting-star + ADX ≥ 25 + MACD direction + EMA-20 context), then
holds unconditionally to 15:14 IST for EOD exit.  No stop-loss.

Signal logic (faithful 1-for-1 translation of backtest_oos.py champion configs):

    Signal window : 09:15 – 09:44 IST (first 30 × 1-min bars)
    Entry         : open of bar AFTER signal bar (DELAY = 1 bar)

    Bearish signal → SELL ATM CE (premium decays as market falls)
        Conditions:
          • ADX ≥ 25
          • MACD direction (per combo) is bearish
          • Shooting-star candle: upper wick ≥ 2 × body  AND  lower wick ≤ body
          • close > EMA-20

    Bullish signal → SELL ATM PE (premium decays as market rises)
        Conditions:
          • ADX ≥ 25
          • MACD direction (per combo) is bullish
          • Hammer candle: lower wick ≥ 2 × body  AND  upper wick ≤ body
          • close < EMA-20

Champion combos (both run simultaneously; first valid signal per session wins):
    adx25_macd_slope_c2.0__sell  — bull = macd_line[i] > macd_line[i-1]
    adx25_hist_slope_c2.0__sell  — bull = histogram[i]  > histogram[i-1]

MACD params  : fast=12, slow=26, signal=9 (standard)
ADX params   : Wilder-smoothed, period=14, threshold=25
Candle strict: 2.0 (lower/upper wick ≥ 2× body)

VIX soft filter (recommended): skip session if INDIAVIX ≥ 17 at session open.
  Backtested effect: Sharpe 2.613 → 3.456, only 3 trades dropped over 16-month IS.

Research results (OOS Oct 2025 – Mar 2026, NIFTY):
    adx25_macd_slope: Sharpe 2.867  |  WR 73.3%  |  30 trades
    adx25_hist_slope: Sharpe 2.353  |  WR 75.0%  |  24 trades
    Stage 9 multi-instrument: NIFTY ✅ + BANKNIFTY ✅ (2/3 pass) — NIFTY-only deployment

Parameters:
    N_LOTS = 10 lots
    DEFAULT_LOT_SIZE = 65   (post-SEBI Dec 2025 lot reduction)
    Exit : 15:14 IST — unconditional, no SL, no TP

Shared utilities:
    live_trading.shared.ta_compat        — EMA, MACD, ADX (pure pandas, Wilder-smoothed)
    live_trading.shared.atm_resolver     — get_option_ltp
    live_trading.shared.telegram_notifier— send_async
    live_trading.api_utils               — get_expiry_dates, get_option_symbol, get_history
    database.token_db.get_symbol_info    — dynamic lot size
    openalgo.api                         — placesmartorder
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
    def compute_round_trip_cost(**_kwargs):   # type: ignore[misc]
        return 0.0

from live_trading.shared import ta_compat as ta
from live_trading.api_utils                import get_expiry_dates, get_option_symbol, get_history
from live_trading.shared.atm_resolver      import get_option_ltp
from live_trading.shared.order_fill        import fetch_fill_price
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
        logging.FileHandler(LOGS_DIR / "nifty_eod_hold_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER",   "http://127.0.0.1:8080")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:8765")

if not API_KEY:
    logger.error("❌ OPENALGO_API_KEY not found in environment. Please check .env. Exiting.")
    sys.exit(1)

PAPER_MODE = os.getenv("NIFTY_EOD_HOLD_PAPER_MODE", "true").lower() != "false"

# ── Strategy constants ────────────────────────────────────────────────────────
STRATEGY_NAME    = "NIFTY_EOD_HOLD"

IDX_SYMBOL       = "NIFTY"
IDX_EXCHANGE     = "NSE_INDEX"
VIX_SYMBOL       = "INDIAVIX"
OPT_EXCHANGE     = "NFO"

N_LOTS           = 10              # 10 lots per CLAUDE.md position-size rule
DEFAULT_LOT_SIZE = 65              # post-SEBI Dec 2025 NIFTY lot size

ATM_STEP         = 50              # NIFTY strike increment

# MACD parameters (both combos share these)
MACD_FAST        = 12
MACD_SLOW        = 26
MACD_SIG         = 9

# ADX
ADX_LENGTH       = 14
ADX_THRESH       = 25

# Candle strictness
CANDLE_STRICT    = 2.0             # wick ≥ 2× body

# VIX soft filter
VIX_SKIP_THRESH  = 17.0            # skip session if INDIAVIX ≥ 17 at open

# Entry / session timing (all IST)
MARKET_OPEN      = dt_time(9, 15)
ENTRY_START      = dt_time(9, 15)  # matches backtest: signal window opens at first bar
ENTRY_END        = dt_time(9, 44)  # last signal bar at 09:44 close
EOD_EXIT         = dt_time(15, 14) # unconditional exit — matches backtest FORCE_T
SESSION_END      = dt_time(15, 35)

# DTE
# MIN_DTE=2 matches the validated backtest/spec exactly (stage1_spec.md:
# "Nearest weekly expiry with DTE >= 2. If DTE < 2, use next weekly expiry").
# There is no upper bound in the backtest (get_expiry() in backtest_is.py,
# backtest_banknifty.py, backtest_sensex.py all only check dte >= MIN_DTE) --
# a MAX_DTE=7 cap had been added here without backtest support, causing the
# bot to wrongly stand down every Monday (nearest expiry DTE=1, next DTE=8,
# rejected by the cap) instead of rolling to next week's expiry as designed
# (2026-07-13 finding).
MIN_DTE          = 2

# Warm-up bars before signals can fire
# MACD(12,26,9) needs 26 + 9 = 35 bars minimum; we warm up from history
MIN_BARS_REQUIRED = 40

# State file
STATE_FILE       = LOGS_DIR / "nifty_eod_hold_state.json"

# Decision-state logging (jsonl + throttled heartbeat — see shared/decision_logger.py)
DECISION_LOG     = LOGS_DIR / "nifty_eod_hold_decisions.jsonl"
HEARTBEAT_SECS   = 300

# MACD column names
_MACD_LINE_COL = f"MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
_MACD_HIST_COL = f"MACDh_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
_MACD_SIG_COL  = f"MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
_ADX_COL       = f"ADX_{ADX_LENGTH}"
_EMA_COL       = f"EMA_{20}"


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


# ── Helper: ATM strike ────────────────────────────────────────────────────────
def _atm_strike(spot: float) -> int:
    return int(round(spot / ATM_STEP) * ATM_STEP)


# ── Helper: expiry selection ──────────────────────────────────────────────────
def _get_suitable_expiry() -> str | None:
    """
    Return nearest NIFTY weekly expiry with DTE >= MIN_DTE (no upper bound --
    matches backtest_is.py's get_expiry() and stage1_spec.md exactly).
    Returns OpenAlgo DDMMMYY string or None.
    """
    dates = get_expiry_dates(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, "options")
    today = datetime.now().date()
    for d in dates:
        try:
            exp_dt = datetime.strptime(d, "%d%b%y").date()
            dte    = (exp_dt - today).days
            if dte >= MIN_DTE:
                logger.info(f"  Suitable expiry: {d}  (DTE={dte})")
                return d
        except ValueError:
            continue
    logger.warning(f"  No expiry found with DTE >= {MIN_DTE}. Dates: {dates[:8]}")
    return None


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback


# ══════════════════════════════════════════════════════════════════════════════
class NiftyEodHoldBot:
# ══════════════════════════════════════════════════════════════════════════════
    """
    1-min reversal signal (ADX + MACD + hammer/shooting-star + EMA-20) → sell
    ATM NIFTY weekly option in the 09:15–09:44 opening window.  Hold to 15:14
    IST for unconditional EOD exit.  No stop-loss.

    Both champion combos (macd_slope + hist_slope) are evaluated on every
    signal bar.  The first valid signal per session is taken; subsequent bars
    in the window are skipped once a position is open.

    Delay = 1: signal detected at bar N close → enter at bar N+1 open.
    Implemented via _pending_entries list cleared on first tick of bar N+1.
    """

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.ws     = None

        # Live prices
        self.nifty_ltp: float = 0.0
        self.vix_ltp:   float = 0.0

        # 1-min bar builder (one bar per minute)
        self.bars: deque = deque(maxlen=500)   # completed 1-min OHLC dicts

        self._bar_open:   float = 0.0
        self._bar_high:   float = 0.0
        self._bar_low:    float = float("inf")
        self._bar_slot:   int   = -1    # minute-of-day slot (hour*60 + minute)
        self._last_ltp:   float = 0.0

        # Active position (only one leg per session — first signal wins)
        # Keyed by opt_type: "PE" or "CE"
        self.active_trade: dict | None = None
        self.active_opt_type: str | None = None

        # Session metadata
        self.expiry:     str | None = None
        self.lot_size:   int        = DEFAULT_LOT_SIZE

        # State flags
        self._session_started    = False
        self._next_init_attempt  = datetime.min   # retry gate for session init
        self._signal_taken       = False   # one signal per session
        self._vix_checked        = False   # VIX gate evaluated at session open
        self._vix_skip_session   = False   # True if VIX ≥ 17 → skip today
        self._eod_exit_done      = False
        self._first_connect      = True
        self._subscribed_syms: set[str] = set()

        # Pending entry (bar N signal → execute at first tick of bar N+1)
        self._pending_entries: list[dict] = []   # [{opt_type, strike, bar_time}]

        # Indicator snapshot for dashboard
        self.indicator_snapshot: dict = {}

        # Decision-state logging (jsonl + throttled heartbeat)
        self._dlog = DecisionLogger(DECISION_LOG, heartbeat_secs=HEARTBEAT_SECS, bot_logger=logger)

        # Dead-feed watchdog: alerts if NIFTY/VIX/open-leg ticks go quiet for
        # DEAD_FEED_SECS during market hours (Task #14 — a hung-but-not-erroring
        # socket would otherwise never trigger the reconnect-on-exception loop).
        self._watchdog = TickWatchdog(
            bot_name="NIFTY EOD Hold Bot",
            tracked_symbols=lambda: [IDX_SYMBOL, VIX_SYMBOL] + (
                [self.active_trade["symbol"]] if self.active_trade else []
            ),
            market_open=MARKET_OPEN,
            market_close=EOD_EXIT,
            bot_logger=logger,
        )

        # Restore from previous run
        self._restore_state()

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE RESTORE
    # ══════════════════════════════════════════════════════════════════════════

    def _restore_state(self) -> None:
        if not STATE_FILE.exists():
            return
        try:
            saved = json.loads(STATE_FILE.read_text())
            if saved.get("active_trade"):
                self.active_trade    = saved["active_trade"]
                self.active_opt_type = saved.get("active_opt_type")
                self._signal_taken   = True
                logger.info(f"  ♻️  Restored position: {self.active_trade.get('symbol')}")
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
        Fetch recent 1-min bars from OpenAlgo history API to warm up indicators.
        We need ≥40 bars for MACD(12,26,9) + ADX(14) + EMA(20) to stabilise.
        duration_days=5 gives ~375 1-min bars for today plus prior days.
        """
        try:
            hist = get_history(API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "1m", 5)
            if not hist:
                logger.warning("  History load: no data returned — running cold start.")
                return
            today = datetime.now().date()
            count = 0

            def _parse_row_dt(row: dict) -> datetime | None:
                """Parse timestamp from a history row.
                get_history returns Unix epoch seconds (int/float) under 'timestamp',
                or an ISO string under 'date'. datetime.fromisoformat() chokes on
                raw epoch integers, so we detect and convert appropriately.
                """
                raw = row.get("timestamp", row.get("date", ""))
                if raw == "" or raw is None:
                    return None
                try:
                    # Unix epoch (int or float, or numeric string)
                    return datetime.fromtimestamp(float(raw))
                except (ValueError, TypeError):
                    pass
                try:
                    return datetime.fromisoformat(str(raw))
                except (ValueError, TypeError):
                    return None

            for row in hist:
                # Only load bars from today's session (MARKET_OPEN onwards)
                row_dt = _parse_row_dt(row)
                if row_dt is None:
                    continue
                if row_dt.date() != today:
                    continue
                if row_dt.time() < MARKET_OPEN:
                    continue
                self.bars.append({
                    "open":  float(row["open"]),
                    "high":  float(row["high"]),
                    "low":   float(row["low"]),
                    "close": float(row["close"]),
                })
                count += 1

            # If today has fewer than MIN_BARS_REQUIRED bars, also load prior day
            if count < MIN_BARS_REQUIRED:
                for row in hist:
                    row_dt = _parse_row_dt(row)
                    if row_dt is None:
                        continue
                    if row_dt.date() >= today:
                        continue
                    self.bars.appendleft({
                        "open":  float(row["open"]),
                        "high":  float(row["high"]),
                        "low":   float(row["low"]),
                        "close": float(row["close"]),
                    })
            logger.info(f"  History loaded: {len(self.bars)} bars")
        except Exception as e:
            logger.warning(f"  History load failed ({e}). Running cold start.")

    # ══════════════════════════════════════════════════════════════════════════
    #  1-MIN BAR BUILDER
    # ══════════════════════════════════════════════════════════════════════════

    def _update_bar(self, ltp: float, ts: datetime) -> bool:
        """
        Feed one raw tick into the running 1-min bar.
        Returns True when a NEW minute starts (previous bar is completed).
        Slot = hour * 60 + minute (each minute is one slot).
        """
        cur_slot = ts.hour * 60 + ts.minute

        if self._bar_slot == -1:
            self._bar_slot  = cur_slot
            self._bar_open  = ltp
            self._bar_high  = ltp
            self._bar_low   = ltp
            self._last_ltp  = ltp
            return False

        if cur_slot != self._bar_slot:
            # Minute boundary → close completed bar
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
            return True   # bar completed

        # Same minute — update running bar
        self._bar_high  = max(self._bar_high, ltp)
        self._bar_low   = min(self._bar_low,  ltp)
        self._last_ltp  = ltp
        return False

    # ══════════════════════════════════════════════════════════════════════════
    #  INDICATOR COMPUTATION
    # ══════════════════════════════════════════════════════════════════════════

    def _compute_indicators(self) -> dict | None:
        """
        Compute EMA(20), MACD(12,26,9), and ADX(14) on self.bars.
        Returns dict of last-row values, or None if insufficient data.
        """
        if len(self.bars) < MIN_BARS_REQUIRED:
            return None
        df = pd.DataFrame(list(self.bars))
        c, h, l = df["close"], df["high"], df["low"]

        ema_s  = ta.ema(c, length=20)
        macd_df = ta.macd(c, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)
        adx_df  = ta.adx(h, l, c, length=ADX_LENGTH)

        if macd_df is None or adx_df is None:
            return None
        if _MACD_LINE_COL not in macd_df.columns or _ADX_COL not in adx_df.columns:
            return None

        n = len(df) - 1
        if n < 1:
            return None

        return {
            # current bar
            "open":      float(df["open"].iloc[n]),
            "high":      float(df["high"].iloc[n]),
            "low":       float(df["low"].iloc[n]),
            "close":     float(c.iloc[n]),
            "ema20":     float(ema_s.iloc[n]),
            "macd":      float(macd_df[_MACD_LINE_COL].iloc[n]),
            "hist":      float(macd_df[_MACD_HIST_COL].iloc[n]),
            "adx":       float(adx_df[_ADX_COL].iloc[n]),
            # previous bar (for slope detection)
            "prev_macd": float(macd_df[_MACD_LINE_COL].iloc[n - 1]),
            "prev_hist": float(macd_df[_MACD_HIST_COL].iloc[n - 1]),
        }

    # ══════════════════════════════════════════════════════════════════════════
    #  SIGNAL ENGINE
    # ══════════════════════════════════════════════════════════════════════════

    def _check_signal(self, ind: dict) -> str | None:
        """
        Evaluate both champion combos on the completed 1-min bar.
        Returns "bullish" → sell PE, "bearish" → sell CE, or None.

        Combos evaluated in priority order:
          1. macd_slope (primary champion)
          2. hist_slope (secondary champion)
        First combo that fires wins.
        """
        o      = ind["open"]
        h      = ind["high"]
        l      = ind["low"]
        c      = ind["close"]
        ema20  = ind["ema20"]
        adx_v  = ind["adx"]

        if adx_v < ADX_THRESH:
            return None

        body = abs(c - o)
        if body == 0:
            return None

        lw = min(o, c) - l        # lower wick length
        uw = h - max(o, c)        # upper wick length

        hammer    = (lw >= CANDLE_STRICT * body) and (uw <= body)
        shoot_str = (uw >= CANDLE_STRICT * body) and (lw <= body)

        for macd_variant in ("macd_slope", "hist_slope"):
            if macd_variant == "macd_slope":
                bull = ind["macd"] > ind["prev_macd"]
                bear = ind["macd"] < ind["prev_macd"]
            else:  # hist_slope
                bull = ind["hist"] > ind["prev_hist"]
                bear = ind["hist"] < ind["prev_hist"]

            if bull and hammer and c < ema20:
                return "bullish"
            if bear and shoot_str and c > ema20:
                return "bearish"

        # Log when no signal (for debugging)
        body = abs(c - o)
        lw = min(o, c) - l
        uw = h - max(o, c)
        hammer = (lw >= CANDLE_STRICT * body) and (uw <= body) if body > 0 else False
        shoot_str = (uw >= CANDLE_STRICT * body) and (lw <= body) if body > 0 else False

        logger.info(
            f"[SIG] NO SIGNAL - ADX≥{ADX_THRESH}={adx_v>=ADX_THRESH}({adx_v:.1f}), "
            f"EMA context={c<ema20 if c and ema20 else None}, "
            f"candle=hammer:{hammer}/shoot:{shoot_str}, "
            f"close={c:.1f} EMA={ema20:.1f}"
        )

        return None

    # ══════════════════════════════════════════════════════════════════════════
    #  ORDER MANAGEMENT
    # ══════════════════════════════════════════════════════════════════════════

    async def _enter_trade(self, opt_type: str, spot_px: float) -> None:
        """Place sell order for ATM option at open of bar N+1."""
        if self._signal_taken:
            logger.debug(f"  Signal already taken this session — skipping {opt_type}.")
            return
        if not self.expiry:
            logger.warning("  No expiry — cannot enter trade.")
            return

        strike = _atm_strike(spot_px)
        symbol = get_option_symbol(IDX_SYMBOL, self.expiry, strike, opt_type)
        if not symbol:
            logger.warning(f"  Could not resolve option symbol for {opt_type} {strike}.")
            return

        lot_size = _get_lot_size(symbol)
        qty = N_LOTS * lot_size

        # Get live premium
        prem = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        if prem <= 0:
            logger.warning(f"  Zero premium for {symbol} — skipping entry.")
            return

        cost = compute_round_trip_cost(
            entry_premium=prem,
            exit_premium=prem * 0.5,   # rough estimate for cost purposes
            lot_size=lot_size,
            qty=N_LOTS,
            exchange="NSE",
            side="sell",
            brokerage_per_order=20,
        )

        logger.info(
            f"  📥 ENTER SELL {opt_type}: {symbol}  prem={prem:.2f}"
            f"  qty={qty}  lot_size={lot_size}  paper={PAPER_MODE}"
        )

        resp = None
        if not PAPER_MODE:
            try:
                resp = self.client.placesmartorder(
                    strategy=STRATEGY_NAME,
                    symbol=symbol,
                    action="SELL",
                    exchange=OPT_EXCHANGE,
                    price_type="MARKET",
                    product="MIS",
                    quantity=qty,
                    position_size=qty,
                )
                logger.info(f"  Order response: {resp}")
            except Exception as e:
                logger.error(f"  ❌ Order placement failed: {e}")
                return
        else:
            logger.info(f"  📝 PAPER: Would sell {qty} × {symbol} @ {prem:.2f}")

        # Book against the actual fill, not the pre-order LTP snapshot.
        entry_fill = _resolve_fill(resp, prem)
        if entry_fill != prem:
            logger.info(f"  Entry fill=₹{entry_fill:.2f} vs LTP snapshot=₹{prem:.2f}")

        self.active_trade    = {
            "symbol":      symbol,
            "opt_type":    opt_type,
            "strike":      strike,
            "expiry":      self.expiry,
            "entry_prem":  round(entry_fill, 2),
            "entry_time":  datetime.now().isoformat(),
            "qty":         qty,
            "lot_size":    lot_size,
            "cost_rt":     round(cost, 2),
        }
        self.active_opt_type = opt_type
        self._signal_taken   = True

        # Subscribe to option feed for live MTM
        await self._subscribe(symbol, OPT_EXCHANGE)

        # CSV log
        self._log_paper_trade("ENTRY", symbol, opt_type, entry_fill, qty, lot_size, 0.0, "entry")

        await send_async(
            f"🔔 *{STRATEGY_NAME}* — SELL {opt_type}\n"
            f"Symbol: `{symbol}`\n"
            f"Premium: ₹{entry_fill:.2f}  Qty: {qty}  Lots: {N_LOTS}\n"
            f"Paper: {PAPER_MODE}"
        )

    async def _eod_exit(self) -> None:
        """Unconditional close of the open position at 15:14 IST."""
        if self._eod_exit_done:
            return
        self._eod_exit_done = True

        if not self.active_trade:
            logger.info("⏰ EOD: no open position to close.")
            return

        trade    = self.active_trade
        opt_type = self.active_opt_type
        symbol   = trade["symbol"]
        entry_p  = float(trade["entry_prem"])
        qty      = int(trade["qty"])
        lot_size = trade["lot_size"]

        logger.info(f"⏰ EOD exit: closing {symbol} (qty={qty}) at 15:14 IST.")

        live_prem = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        if live_prem <= 0:
            live_prem = entry_p   # fallback

        cost_rt = float(trade.get("cost_rt", 0))

        resp = None
        if not PAPER_MODE:
            try:
                resp = self.client.placesmartorder(
                    strategy=STRATEGY_NAME,
                    symbol=symbol,
                    action="BUY",
                    exchange=OPT_EXCHANGE,
                    price_type="MARKET",
                    product="MIS",
                    quantity=qty,
                    position_size=0,
                )
                logger.info(f"  Exit order response: {resp}")
            except Exception as e:
                logger.error(f"  ❌ Exit order failed: {e}")
        else:
            logger.info(f"  📝 PAPER: Would buy back {qty} × {symbol} @ {live_prem:.2f}")

        # Book P&L against the actual fill, not the LTP that triggered the exit.
        exit_fill = _resolve_fill(resp, live_prem)
        if exit_fill != live_prem:
            logger.info(f"  Exit fill=₹{exit_fill:.2f} vs LTP trigger=₹{live_prem:.2f}")

        pnl = (entry_p - exit_fill) * qty - cost_rt   # seller: premium decay = profit

        logger.info(
            f"  ✅ Exit {symbol}  entry={entry_p:.2f}  exit={exit_fill:.2f}"
            f"  pnl=₹{pnl:,.0f}"
        )

        self._log_paper_trade("EXIT", symbol, opt_type or "", exit_fill, qty, lot_size,
                               round(pnl, 2), "eod_15:14")

        # Dual-write to performance.db — was broken (wrong kwargs silently failed).
        # gross_pnl excludes cost_rt so it is consistent with all other bots.
        gross_pnl = (entry_p - exit_fill) * qty
        log_trade_to_db(
            bot_name      = "nifty_eod_hold_bot",
            instrument    = IDX_SYMBOL,
            option_symbol = symbol,
            option_type   = opt_type or "",
            entry_time    = trade.get("entry_time"),
            exit_time     = datetime.now(),
            entry_premium = entry_p,
            exit_premium  = exit_fill,
            exit_reason   = "eod_15:14",
            quantity      = qty,
            lots          = N_LOTS,
            lot_size      = lot_size,
            gross_pnl     = gross_pnl,
            net_pnl       = round(gross_pnl - cost_rt, 2),
        )

        self.active_trade    = None
        self.active_opt_type = None

        await send_async(
            f"⏰ *{STRATEGY_NAME}* — EOD EXIT\n"
            f"Symbol: `{symbol}`\n"
            f"Entry: ₹{entry_p:.2f}  Exit: ₹{exit_fill:.2f}\n"
            f"Net P&L: ₹{pnl:,.0f}  (paper={PAPER_MODE})"
        )

    # ══════════════════════════════════════════════════════════════════════════
    #  PAPER TRADE CSV LOGGER
    # ══════════════════════════════════════════════════════════════════════════

    def _log_paper_trade(
        self, action: str, symbol: str, opt_type: str,
        prem: float, qty: int, lot_size: int, pnl: float, reason: str
    ) -> None:
        csv_path = LOGS_DIR / "nifty_eod_hold_paper_trades.csv"
        write_header = not csv_path.exists()
        try:
            with open(csv_path, "a", newline="") as f:
                w = csv.writer(f)
                if write_header:
                    w.writerow([
                        "timestamp", "action", "symbol", "opt_type",
                        "premium", "qty", "lot_size", "pnl", "reason", "paper_mode",
                    ])
                w.writerow([
                    datetime.now().isoformat(), action, symbol, opt_type,
                    round(prem, 2), qty, lot_size, round(pnl, 2), reason, PAPER_MODE,
                ])
        except Exception as e:
            logger.warning(f"  CSV log failed: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE DUMP (for dashboard)
    # ══════════════════════════════════════════════════════════════════════════

    def _write_state(self) -> None:
        try:
            state = {
                "strategy":       STRATEGY_NAME,
                "last_update":    datetime.now().isoformat(),
                "updated_at":     datetime.now().isoformat(),
                "expiry":         self.expiry,
                "bars_loaded":    len(self.bars),
                "active_trade":   self.active_trade,
                "active_opt_type": self.active_opt_type,
                "signal_taken":   self._signal_taken,
                "vix_skip":       self._vix_skip_session,
                "vix_ltp":        round(self.vix_ltp, 2),
                "pending_entries": [e.get("opt_type") for e in self._pending_entries],
                "indicators":     self.indicator_snapshot,
                "paper_mode":     PAPER_MODE,
            }
            STATE_FILE.write_text(json.dumps(state, indent=2, default=str))
        except Exception:
            pass

    async def _state_writer(self) -> None:
        while True:
            self._write_state()
            await asyncio.sleep(3)

    # ══════════════════════════════════════════════════════════════════════════
    #  WEBSOCKET HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _subscribe(self, symbol: str, exchange: str) -> None:
        if not self.ws or symbol in self._subscribed_syms:
            return
        try:
            msg = json.dumps({
                "action":   "subscribe",
                "symbol":   symbol,
                "exchange": exchange,
                "mode":     2,
            })
            await self.ws.send(msg)
            self._subscribed_syms.add(symbol)
            logger.info(f"  Subscribed: {symbol} ({exchange})")
        except Exception as e:
            logger.warning(f"  Subscribe failed ({symbol}): {e}")

    async def _resubscribe_all(self) -> None:
        """
        Unconditionally resubscribe index + VIX + any open option leg on every
        (re)connect. The old call site only ever resubscribed index + VIX, so
        an open position's option feed silently dropped off after a mid-session
        reconnect. Currently benign (the option-tick handler is a documented
        no-op — this bot exits on wall-clock, not option price) but this closes
        the same latent gap fixed in nifty_macd_map_bot, in case MTM/SL logic
        is ever added to that handler.
        """
        await self._subscribe(IDX_SYMBOL, IDX_EXCHANGE)
        await self._subscribe(VIX_SYMBOL, IDX_EXCHANGE)
        if self.active_trade:
            await self._subscribe(self.active_trade["symbol"], OPT_EXCHANGE)

    # ══════════════════════════════════════════════════════════════════════════
    #  DECISION-STATE LOGGING
    # ══════════════════════════════════════════════════════════════════════════

    def _verdict(self, now_t: dt_time) -> str:
        """What's currently blocking entry — checked in the order these gates
        actually apply in _on_tick(), so it always names the real blocker."""
        if not self._session_started:
            return "waiting for session start"
        if self.active_trade:
            return f"ACTIVE: holding {self.active_opt_type} to EOD ({EOD_EXIT.strftime('%H:%M')})"
        if self._eod_exit_done:
            return "done for today: EOD exit complete"
        if self._vix_skip_session:
            return f"BLOCKED: VIX skip (VIX={self.vix_ltp:.2f} >= {VIX_SKIP_THRESH})"
        if self._signal_taken:
            return "done for today: signal already taken"
        if now_t < ENTRY_START:
            return "waiting for entry window to open"
        if now_t > ENTRY_END:
            return "entry window closed — no signal fired today"
        if not self.expiry:
            return "BLOCKED: no suitable expiry found"
        if len(self.bars) < MIN_BARS_REQUIRED:
            return f"warming up: {len(self.bars)}/{MIN_BARS_REQUIRED} bars"
        return "🔥 in window — watching for ADX/MACD/candle signal"

    def _heartbeat_text(self) -> str:
        now_t = datetime.now().time()
        lines = [
            f"💓 DECISION STATE {datetime.now().strftime('%H:%M:%S')} ─ {self._verdict(now_t)}",
            f"    NIFTY={self.nifty_ltp:.1f}  VIX={self.vix_ltp:.2f}  "
            f"bars={len(self.bars)}  expiry={self.expiry}",
        ]
        if self.indicator_snapshot:
            ind = self.indicator_snapshot
            lines.append(
                f"    last bar={ind.get('bar_time')}  ADX={ind.get('adx')}  "
                f"EMA20={ind.get('ema20')}  MACD={ind.get('macd')}  hist={ind.get('hist')}"
            )
        if self.active_trade:
            lines.append(f"    active: {self.active_opt_type} {self.active_trade.get('symbol')}")
        return "\n".join(lines)

    # ══════════════════════════════════════════════════════════════════════════
    #  TICK HANDLER
    # ══════════════════════════════════════════════════════════════════════════

    async def _on_tick(self, msg: dict) -> None:
        # WS proxy wraps ticks as: {"type": "market_data", "symbol": ..., "data": {ltp, t, ...}}
        if msg.get("type") != "market_data":
            return
        symbol = msg.get("symbol", "")
        self._watchdog.on_tick(symbol)
        mdata  = msg.get("data", {})
        ltp    = float(mdata.get("ltp", 0) or mdata.get("lp", 0))
        ts_raw = mdata.get("t")

        try:
            ts = datetime.fromtimestamp(float(ts_raw)) if ts_raw else datetime.now()
        except (ValueError, TypeError):
            ts = datetime.now()

        now_t = ts.time()

        # ── VIX tick ──────────────────────────────────────────────────────────
        if symbol == VIX_SYMBOL:
            self.vix_ltp = ltp
            # Apply VIX gate at session open (first VIX tick in window)
            if not self._vix_checked and now_t >= MARKET_OPEN:
                self._vix_checked = True
                if ltp >= VIX_SKIP_THRESH:
                    self._vix_skip_session = True
                    logger.warning(
                        f"  ⚠️  VIX filter: INDIAVIX={ltp:.2f} ≥ {VIX_SKIP_THRESH} "
                        f"— signals DISABLED for today."
                    )
                else:
                    logger.info(f"  ✅ VIX filter: INDIAVIX={ltp:.2f} < {VIX_SKIP_THRESH} — signals enabled.")
            return

        # ── Option position MTM tick ──────────────────────────────────────────
        if self.active_trade and symbol == self.active_trade.get("symbol"):
            # Just update dashboard snapshot — no SL monitoring
            pass

        # ── NIFTY index tick ──────────────────────────────────────────────────
        if symbol != IDX_SYMBOL:
            return

        self.nifty_ltp = ltp

        # Throttled to HEARTBEAT_SECS internally — cheap to call on every tick
        self._dlog.maybe_heartbeat(self._heartbeat_text)

        # ── Session init on first NIFTY tick ─────────────────────────────────
        # Only latches on success; failed attempts retry every 120s so a
        # transient OpenAlgo outage at open can't disable the bot for the day
        # (incident 2026-07-07).
        if (not self._session_started and now_t >= MARKET_OPEN
                and datetime.now() >= self._next_init_attempt):
            logger.info("🔔 Session start — initialising NIFTY EOD Hold bot.")
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
                f"  NIFTY={ltp:.1f}  Expiry={self.expiry}  bars={len(self.bars)}"
            )

        # ── EOD exit ─────────────────────────────────────────────────────────
        if now_t >= EOD_EXIT and not self._eod_exit_done:
            await self._eod_exit()
            return

        # Past session end — nothing more to do
        if now_t >= SESSION_END:
            return

        # ── Execute pending entries (bar N+1 open) ────────────────────────────
        if self._pending_entries:
            entries = list(self._pending_entries)
            self._pending_entries.clear()
            for entry in entries:
                await self._enter_trade(entry["opt_type"], ltp)
            return   # do not re-evaluate signal on same tick

        # ── Build 1-min bar ───────────────────────────────────────────────────
        bar_completed = self._update_bar(ltp, ts)

        if not bar_completed:
            return

        in_window = ENTRY_START <= now_t <= ENTRY_END

        # Compute indicators on completed bars — best-effort (None until
        # warmed up); done regardless of window/gates so decision logging
        # below always has real values to explain "why not."
        ind = self._compute_indicators()

        if ind is not None:
            # Update dashboard snapshot
            self.indicator_snapshot = {
                "nifty_ltp":  round(self.nifty_ltp, 2),
                "vix_ltp":    round(self.vix_ltp, 2),
                "bar_time":   now_t.strftime("%H:%M"),
                "in_window":  in_window,
                "adx":        round(ind["adx"], 2),
                "ema20":      round(ind["ema20"], 2),
                "macd":       round(ind["macd"], 5),
                "hist":       round(ind["hist"], 5),
                "prev_macd":  round(ind["prev_macd"], 5),
                "prev_hist":  round(ind["prev_hist"], 5),
            }

        self._dlog.log_bar({
            "phase":         "ACTIVE" if self.active_trade else ("DONE" if self._signal_taken else "WATCHING"),
            "bar_time":      now_t.strftime("%H:%M"),
            "in_window":     in_window,
            "signal_taken":  self._signal_taken,
            "vix_skip":      self._vix_skip_session,
            "vix_ltp":       round(self.vix_ltp, 2),
            "bars_loaded":   len(self.bars),
            "expiry":        self.expiry,
            "indicators":    ind,
            "verdict":       self._verdict(now_t),
            "active_trade":  self.active_opt_type,
        })

        # Bar just completed — evaluate signal if still in window
        if not in_window:
            return

        if self._signal_taken:
            return

        if self._vix_skip_session:
            return

        if not self.expiry:
            return

        if ind is None:
            return

        # ── Signal check ──────────────────────────────────────────────────────
        sig = self._check_signal(ind)
        if sig is None:
            return

        opt_type = "PE" if sig == "bullish" else "CE"

        logger.info(
            f"  🎯 Signal: {sig} → SELL ATM {opt_type}  "
            f"bar={now_t.strftime('%H:%M')}  ADX={ind['adx']:.1f}  "
            f"MACD={ind['macd']:.4f}  hist={ind['hist']:.4f}"
        )

        # Queue for execution at bar N+1 open (delay = 1)
        self._pending_entries.append({
            "opt_type": opt_type,
            "bar_time": now_t.strftime("%H:%M"),
        })

    # ══════════════════════════════════════════════════════════════════════════
    #  WEBSOCKET RUN LOOP
    # ══════════════════════════════════════════════════════════════════════════

    async def run(self) -> None:
        logger.info(f"🚀 {STRATEGY_NAME} starting  (paper={PAPER_MODE})")

        asyncio.create_task(self._state_writer())
        asyncio.create_task(self._watchdog.watch_loop())

        retry_delay = 5
        while True:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=30,
                    close_timeout=10,
                ) as ws:
                    self.ws = ws
                    retry_delay = 5

                    if self._first_connect:
                        self._first_connect = False
                        logger.info(f"  Connected to WebSocket: {WS_URL}")

                    # Authenticate — proxy requires "authenticate" (not "auth")
                    await ws.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))
                    await asyncio.sleep(0.5)

                    # Subscribe to NIFTY index + VIX + any open option leg (reconnect-safe)
                    await self._resubscribe_all()

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        await self._on_tick(msg)

            except (websockets.ConnectionClosed, ConnectionRefusedError, OSError) as e:
                logger.warning(f"  WebSocket disconnected ({e}). Reconnecting in {retry_delay}s…")
                self.ws = None
                self._subscribed_syms.clear()
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

            except Exception as e:
                logger.error(f"  Unexpected error in run loop: {e}", exc_info=True)
                self.ws = None
                self._subscribed_syms.clear()
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    bot = NiftyEodHoldBot()
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
