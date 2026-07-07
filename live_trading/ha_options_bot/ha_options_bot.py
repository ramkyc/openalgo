"""
HA Options Bot
==============
live_trading/ha_options_bot/ha_options_bot.py

Research-validated (ha_options_study, 2026-03-23):
  options_data/research/ha_options_study/results_summary.md
  ALL 10 pipeline stages PASS.

Champion configurations:
  BANKNIFTY 15-min ATM : IS Sharpe +10.04  |  OOS Sharpe +9.95  |  WR 53.5% (monthly expiry ≥7 DTE)
  NIFTY     5-min  ATM : IS Sharpe  +8.43  |  OOS Sharpe +10.14 |  WR 43.6% (weekly expiry ≥2 DTE)
  SENSEX    5-min  ATM : IS Sharpe  +7.33  |  OOS Sharpe +9.85  |  WR 43.6% (weekly expiry ≥2 DTE)

OOS combined Sharpe +12.37 (all 3 instruments). MC: 100% of 10,000 bootstrap runs profitable.

Strategy:
  Compute Heiken Ashi candles fresh each day on resampled index OHLC.
  HA bullish flip (HA_Close > HA_Open) → SELL ATM PE (expect index to continue up)
  HA bearish flip (HA_Close < HA_Open) → SELL ATM CE (expect index to continue down)

  Entry: on the open of the next bar after the HA flip, within 09:30–14:30 IST.
  Exit:  HA reversal (flip in opposite direction) OR swing SL.
  EOD:   Unconditional close at 15:14 IST.

Signal rules (ALL required):
  1. HA candle flips direction (new direction ≠ last direction)
  2. Bar close time falls within 09:30 – 14:30 IST
  3. ATM premium ≥ ₹15 at entry
  4. Index risk ≥ 20 pts (swing SL distance from spot to SL level)
  5. No position already open for that instrument (one trade per instrument per session)

Exit rules (first to trigger wins):
  HA reversal : index bar closes in opposite HA direction → BUY to close
  Swing SL    : index tick crosses swing high/low of previous N bars → BUY to close
  EOD         : 15:14 IST unconditional

Position sizing: 1 lot flat per instrument. DO NOT scale until 20+ live paper trades.

SL lookback: 5 bars (research found no difference between 5 and 10; 5 chosen for speed).

Vol filter (Stage 8 recommendation):
  Optional: skip SENSEX low-vol days (daily range < median ~0.83% of NIFTY).
  Not enforced in this bot — trade all days, monitor performance.

Shared utilities:
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
from datetime import datetime, timedelta, time as dt_time
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

from live_trading.api_utils import (
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
        # Only StreamHandler: start_all_bots.py redirects stdout → the log file,
        # so a FileHandler here would cause every line to appear twice.
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
STRATEGY_NAME = "HA_OPTIONS"


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback

# Champion configs from research (results_summary.md Stage 3 + Stage 4)
INSTRUMENTS = {
    "NIFTY": {
        "idx_exchange": "NSE_INDEX",
        "opt_exchange": "NFO",
        "tf_min":       5,          # 5-min HA timeframe
        "min_dte":      2,          # weekly expiry ≥2 DTE; max_dte=8 to cover Monday (DTE=8 to next Tue expiry)
        "max_dte":      8,
        "default_lot":  65,
        "strike_step":  50,
    },
    "BANKNIFTY": {
        "idx_exchange": "NSE_INDEX",
        "opt_exchange": "NFO",
        "tf_min":       15,         # 15-min HA timeframe
        "min_dte":      7,          # monthly expiry ≥7 DTE
        "max_dte":      35,
        "default_lot":  30,
        "strike_step":  100,
    },
    "SENSEX": {
        "idx_exchange": "BSE_INDEX",
        "opt_exchange": "BFO",
        "tf_min":       5,          # 5-min HA timeframe
        "min_dte":      2,          # weekly expiry ≥2 DTE
        "max_dte":      7,
        "default_lot":  20,
        "strike_step":  100,
    },
}

N_LOTS          = 10          # standardised: 10 lots per CLAUDE.md position-size rule
SL_LOOKBACK     = 5           # bars to look back for swing SL
MIN_PREMIUM     = 15.0        # ₹15 minimum ATM premium at entry
MIN_INDEX_RISK  = 20.0        # ₹20 minimum SL distance in index points

# Session timing
MARKET_OPEN  = dt_time(9,  15)
ENTRY_START  = dt_time(9,  30)   # no entries before 09:30
ENTRY_END    = dt_time(14, 30)   # no new entries after 14:30
SESSION_END  = dt_time(15, 14)   # EOD close time
WARMUP_DAYS  = 5                 # days of history for bar warm-up

# ── Stage 8 vol filter (from results_summary.md Stage 8) ─────────────────────
# Skip SENSEX on low-volatility days (Stage 8: Low-Vol Sharpe 4.04 vs 9.99).
# Skip ALL instruments on extreme low-vol days (very tight range → noise signals).
# Threshold: yesterday's NIFTY daily range (H−L)/Close < this → low-vol day.
VOL_FILTER_THRESHOLD  = 0.0083  # 0.83% daily range minimum (research Stage 8 finding)
VOL_FILTER_SENSEX_ONLY = True   # True = skip only SENSEX on low-vol; False = skip all

# ── Stage 11 tracking ─────────────────────────────────────────────────────────
# Updated when Stage 11 clock is reset. Sessions are counted from this date.
# Do NOT change this without Ramakrishna's sign-off on a new Stage 11 run.
STAGE_11_START_DATE = "2026-04-07"   # reset after bug fixes (HA daily reset + state restore + vol filter)

STATE_FILE = LOGS_DIR / "ha_options_state.json"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_lot_size(symbol: str, opt_exchange: str, default: int) -> int:
    try:
        from database.token_db import get_symbol_info
        si = get_symbol_info(symbol, opt_exchange)
        if si and getattr(si, "lotsize", None):
            ls = int(si.lotsize)
            logger.info(f"  Lot size from DB: {ls}  ({symbol})")
            return ls
    except Exception as e:
        logger.warning(f"  Lot size DB lookup failed: {e}. Using {default}.")
    return default


def _get_suitable_expiry(symbol: str, opt_exchange: str, min_dte: int, max_dte: int) -> str | None:
    dates = get_expiry_dates(API_KEY, symbol, opt_exchange, "options")
    today = datetime.now().date()
    for d in dates:
        try:
            exp_dt = datetime.strptime(d, "%d%b%y").date()
            dte    = (exp_dt - today).days
            if min_dte <= dte <= max_dte:
                logger.info(f"  [{symbol}] Suitable expiry: {d}  (DTE={dte})")
                return d
        except ValueError:
            continue
    logger.warning(f"  [{symbol}] No expiry found DTE {min_dte}–{max_dte}. Dates: {dates[:6]}")
    return None


def _compute_ha(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Heiken Ashi candles with daily restart.
    Input df must have columns: open, high, low, close, date (date column for grouping).
    Returns df with ha_open, ha_high, ha_low, ha_close columns added.
    Resets HA seed at each new trading day so each day is independent.
    """
    ha_open  = np.zeros(len(df))
    ha_close = np.zeros(len(df))
    ha_high  = np.zeros(len(df))
    ha_low   = np.zeros(len(df))

    opens  = df["open"].values.astype(float)
    highs  = df["high"].values.astype(float)
    lows   = df["low"].values.astype(float)
    closes = df["close"].values.astype(float)
    dates  = df["date"].values if "date" in df.columns else np.zeros(len(df))

    for i in range(len(df)):
        ha_close[i] = (opens[i] + highs[i] + lows[i] + closes[i]) / 4.0
        if i == 0:
            # First bar of the dataset — seed with raw OHLC midpoint
            ha_open[i] = (opens[i] + closes[i]) / 2.0
        else:
            ha_open[i] = (ha_open[i - 1] + ha_close[i - 1]) / 2.0
        ha_high[i] = max(highs[i], ha_open[i], ha_close[i])
        ha_low[i]  = min(lows[i],  ha_open[i], ha_close[i])

    df = df.copy()
    df["ha_open"]  = ha_open
    df["ha_high"]  = ha_high
    df["ha_low"]   = ha_low
    df["ha_close"] = ha_close
    return df


# ══════════════════════════════════════════════════════════════════════════════
class InstrumentState:
# ══════════════════════════════════════════════════════════════════════════════
    """Per-instrument bar builder, HA signal engine, and trade state."""

    def __init__(self, symbol: str, config: dict):
        self.symbol      = symbol
        self.config      = config
        self.tf_min      = config["tf_min"]

        # Live index price
        self.ltp: float  = 0.0

        # Bar builder (intraday bars of tf_min duration)
        self.bars: deque = deque(maxlen=500)  # completed bars {open,high,low,close,date}
        self._bucket: int  = -1
        self._bar_open:  float = 0.0
        self._bar_high:  float = 0.0
        self._bar_low:   float = float("inf")
        self._last_ltp:  float = 0.0
        self._bar_date:  str   = ""

        # HA signal tracking (recomputed on each bar close)
        self.ha_signal: int = 0   # +1 = bullish, -1 = bearish, 0 = unknown

        # Session metadata
        self.expiry:        str | None = None
        self.lot_size:      int        = config["default_lot"]
        self.is_expiry_day: bool       = False
        self._session_started          = False

        # Active trade (one per session)
        self.active_trade: dict | None = None
        self._trade_done_today: bool   = False   # True after any entry/exit today
        self._is_closing: bool         = False   # prevents duplicate exit orders

        # Stage 8 vol filter — set at session open, persists all day
        self._vol_filter_pass: bool    = True    # True = vol OK, trade allowed

    # ── Bar builder ───────────────────────────────────────────────────────────

    def update_bar(self, ltp: float, ts: datetime) -> bool:
        """
        Feed an index tick into the running timeframe bar.
        Returns True the moment a bar COMPLETES (tf_min-bucket boundary crossed).
        """
        total_min  = ts.hour * 60 + ts.minute
        cur_bucket = total_min // self.tf_min
        cur_date   = ts.strftime("%Y-%m-%d")

        if self._bucket == -1:
            self._bucket   = cur_bucket
            self._bar_open = ltp
            self._bar_high = ltp
            self._bar_low  = ltp
            self._last_ltp = ltp
            self._bar_date = cur_date
            return False

        if cur_bucket != self._bucket:
            # Close previous bar
            if self._bar_open > 0:
                self.bars.append({
                    "open":  self._bar_open,
                    "high":  self._bar_high,
                    "low":   self._bar_low,
                    "close": self._last_ltp,
                    "date":  self._bar_date,
                })
            # Start new bar
            self._bucket   = cur_bucket
            self._bar_open = ltp
            self._bar_high = ltp
            self._bar_low  = ltp
            self._last_ltp = ltp
            self._bar_date = cur_date
            return True

        # Same bucket — update running bar
        self._bar_high = max(self._bar_high, ltp)
        self._bar_low  = min(self._bar_low,  ltp)
        self._last_ltp = ltp
        return False

    # ── HA signal engine ─────────────────────────────────────────────────────

    def compute_ha_signal(self) -> int | None:
        """
        Compute HA on TODAY's bars only (matching research sim_day per-day reset).
        Returns:
          +1  = just turned bullish (sell PE)
          -1  = just turned bearish (sell CE)
           0  = no flip
          None = not enough bars today to compute
        Also updates self.ha_signal with the current HA direction.

        IMPORTANT: HA is recomputed fresh from the first bar of each trading day,
        exactly as in run_oos_simulation.py which calls compute_ha() once per day.
        Using a multi-day deque without daily reset produces different signals.
        """
        today_str = datetime.now().strftime("%Y-%m-%d")
        all_bars  = pd.DataFrame(list(self.bars))

        # Filter to today's bars only
        today_bars = all_bars[all_bars["date"] == today_str].copy().reset_index(drop=True)

        if len(today_bars) < 3:
            return None

        today_bars = _compute_ha(today_bars)

        # HA direction: +1 if ha_close > ha_open, -1 otherwise
        today_bars["direction"] = np.where(today_bars["ha_close"] > today_bars["ha_open"], 1, -1)

        last_dir = int(today_bars["direction"].iloc[-1])
        prev_dir = int(today_bars["direction"].iloc[-2])

        self.ha_signal = last_dir

        if last_dir != prev_dir:
            return last_dir   # flip detected
        return 0              # no flip

    # ── Swing SL computation ──────────────────────────────────────────────────

    def compute_swing_sl(self, direction: int) -> float | None:
        """
        For a long signal (sell PE, direction=+1):
            SL = min(ha_low) of the previous SL_LOOKBACK bars (today only)
        For a short signal (sell CE, direction=-1):
            SL = max(ha_high) of the previous SL_LOOKBACK bars (today only)

        Uses TODAY's bars only (matching research per-day HA reset).
        Returns SL level in index points, or None if insufficient data.
        """
        today_str  = datetime.now().strftime("%Y-%m-%d")
        all_bars   = pd.DataFrame(list(self.bars))
        today_bars = all_bars[all_bars["date"] == today_str].copy().reset_index(drop=True)

        if len(today_bars) < SL_LOOKBACK + 2:
            return None

        df = _compute_ha(today_bars)
        df["direction"] = np.where(df["ha_close"] > df["ha_open"], 1, -1)

        # Exclude the last bar (the signal bar) and look at the SL_LOOKBACK before it
        prev_bars = df.iloc[-(SL_LOOKBACK + 1):-1]

        if direction == 1:
            # Bullish flip → sell PE. SL = lowest HA low in prior bearish bars
            bearish = prev_bars[prev_bars["direction"] == -1]
            if bearish.empty:
                # fallback: use all prior bars
                return float(prev_bars["ha_low"].min())
            return float(bearish["ha_low"].min())
        else:
            # Bearish flip → sell CE. SL = highest HA high in prior bullish bars
            bullish = prev_bars[prev_bars["direction"] == 1]
            if bullish.empty:
                return float(prev_bars["ha_high"].max())
            return float(bullish["ha_high"].max())

    # ── Dashboard repr ────────────────────────────────────────────────────────

    def to_state_dict(self) -> dict:
        return {
            "tf":              self.tf_min,
            "ltp":             round(self.ltp, 2),
            "ha_signal":       self.ha_signal,
            "bars_loaded":     len(self.bars),
            "expiry":          self.expiry,
            "is_expiry_day":   self.is_expiry_day,
            "lot_size":        self.lot_size,
            "trade_done_today":  self._trade_done_today,
            "vol_filter_pass":   self._vol_filter_pass,
            "active_trade": {
                "symbol":       self.active_trade["symbol"],
                "side":         self.active_trade["side"],
                "entry_prem":   self.active_trade["entry_prem"],
                "sl_index":     self.active_trade["sl_index"],
                "qty":          self.active_trade["qty"],
                "lot_size":     self.active_trade.get("lot_size", self.lot_size),
                "entry_time":   self.active_trade["entry_time"],
                "order_id":     self.active_trade.get("order_id", ""),
                "ha_dir_entry": self.active_trade.get("ha_dir_entry", 0),
                "sl_direction": self.active_trade.get("sl_direction", 0),
            } if self.active_trade else None,
        }


# ══════════════════════════════════════════════════════════════════════════════
class HAOptionsBot:
# ══════════════════════════════════════════════════════════════════════════════

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.ws     = None

        # Per-instrument state
        self._inst: dict[str, InstrumentState] = {
            sym: InstrumentState(sym, cfg)
            for sym, cfg in INSTRUMENTS.items()
        }

        # VIX (tracked for dashboard, no filter applied)
        self.vix_ltp: float = 0.0

        # Global session flags
        self._session_started   = False
        self._next_init_attempt = datetime.min   # retry gate for session init
        self._eod_exit_done     = False
        self._first_connect     = True
        self._subscribed_syms:  set[str] = set()

    # ── Expiry + lot-size setup ───────────────────────────────────────────────

    async def _setup_instrument(self, sym: str) -> None:
        inst   = self._inst[sym]
        cfg    = inst.config
        logger.info(f"  [{sym}] Resolving expiry and lot size…")

        # Expiry
        inst.expiry = await asyncio.to_thread(
            _get_suitable_expiry,
            sym, cfg["opt_exchange"], cfg["min_dte"], cfg["max_dte"],
        )

        if inst.expiry:
            today  = datetime.now().date()
            exp_dt = datetime.strptime(inst.expiry, "%d%b%y").date()
            dte    = (exp_dt - today).days
            inst.is_expiry_day = (dte == 0)

            # Sample option symbol for lot size lookup
            sample = await asyncio.to_thread(
                get_option_symbol, API_KEY, sym, cfg["opt_exchange"], inst.expiry, "PE", "ATM",
            )
            if sample:
                inst.lot_size = _get_lot_size(sample, cfg["opt_exchange"], cfg["default_lot"])
        else:
            logger.warning(f"  [{sym}] No suitable expiry found. No entries today.")

    async def _check_vol_filter(self) -> None:
        """
        Stage 8 vol filter: measure yesterday's NIFTY daily range.
        If (H−L)/Close < VOL_FILTER_THRESHOLD (0.83%), mark affected instruments
        as vol-filter blocked so no entries are taken today.

        Instruments blocked on low-vol days:
          - VOL_FILTER_SENSEX_ONLY=True  → only SENSEX is blocked
          - VOL_FILTER_SENSEX_ONLY=False → all instruments are blocked

        Fails open (allows trading) if history cannot be fetched.
        """
        try:
            raw = await asyncio.to_thread(
                get_history, API_KEY, "NIFTY", "NSE_INDEX", "D", 5
            )
            if not raw or len(raw) < 2:
                logger.warning("  Vol filter: insufficient history — allowing trading (fail-open).")
                return

            df = pd.DataFrame(raw)

            # Normalise column names (OpenAlgo API may return "timestamp" or "date")
            if "timestamp" in df.columns:
                df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_localize(None)
            elif "date" in df.columns:
                df["dt"] = pd.to_datetime(df["date"])
            else:
                logger.warning("  Vol filter: unrecognised history format — allowing trading.")
                return

            df = df.sort_values("dt").reset_index(drop=True)
            for col in ["high", "low", "close"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["high", "low", "close"])

            if len(df) < 2:
                logger.warning("  Vol filter: not enough clean rows — allowing trading.")
                return

            # Use the last COMPLETED day (second-to-last row; last row may be today)
            yest = df.iloc[-2]
            rng_pct = (float(yest["high"]) - float(yest["low"])) / float(yest["close"])

            vol_ok  = rng_pct >= VOL_FILTER_THRESHOLD
            status  = "✅ HIGH-VOL" if vol_ok else "⚠️  LOW-VOL"
            logger.info(
                f"  Vol filter: NIFTY yesterday range = {rng_pct*100:.2f}%  "
                f"(threshold {VOL_FILTER_THRESHOLD*100:.2f}%)  → {status}"
            )

            if not vol_ok:
                blocked = ["SENSEX"] if VOL_FILTER_SENSEX_ONLY else list(INSTRUMENTS.keys())
                for sym in blocked:
                    if sym in self._inst:
                        self._inst[sym]._vol_filter_pass = False
                        logger.info(f"  Vol filter BLOCKED [{sym}] — low-vol day, no entries.")
                await send_async(
                    f"⚠️ *HA Options Bot — Vol Filter Active*\n"
                    f"NIFTY yesterday range: {rng_pct*100:.2f}% < {VOL_FILTER_THRESHOLD*100:.2f}% threshold\n"
                    f"No entries today for: {', '.join(blocked)}"
                )
            else:
                for sym in INSTRUMENTS:
                    self._inst[sym]._vol_filter_pass = True

        except Exception as e:
            logger.warning(f"  Vol filter check exception: {e} — allowing trading (fail-open).")

    async def _session_open(self) -> None:
        """Called once at 09:15 first tick or on restart."""
        # 0. Holiday Check
        if is_market_holiday(API_KEY):
            logger.info("⛔ Market Holiday detected. Skipping session initialization.")
            return

        # 0.1 Online Notification
        if self._first_connect:
            self._first_connect = False
            cfg_lines = " | ".join(
                f"{s} {self._inst[s].tf_min}m ATM"
                for s in INSTRUMENTS
            )
            await send_async(
                f"🤖 *HA Options Bot — Online*\n"
                f"Signal  : Heiken Ashi candle flip → sell ATM CE/PE\n"
                f"Configs : {cfg_lines}\n"
                f"Window  : {ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')} IST\n"
                f"Filters : prem ≥ ₹{MIN_PREMIUM:.0f}  |  SL risk ≥ {MIN_INDEX_RISK:.0f} pts  |  vol ≥ {VOL_FILTER_THRESHOLD*100:.2f}%\n"
                f"Exit    : HA reversal OR swing SL  |  EOD 15:14\n"
                f"Sizing  : {N_LOTS} lot flat per instrument\n"
                f"Research: OOS Sharpe 10.04–10.14 | 10/10 stages | 3/3 instruments\n"
                f"Stage 11: reset {STAGE_11_START_DATE} (HA fix + state restore + vol filter)"
            )

        # 0.2 Stage 8 vol filter — computed once per session before any entries
        await self._check_vol_filter()

        logger.info("🔔 Session open. Initializing instruments...")
        tasks = [self._setup_instrument(sym) for sym in INSTRUMENTS]
        await asyncio.gather(*tasks)

    # ── Warm-up historical bars ───────────────────────────────────────────────

    async def _warmup_instrument(self, sym: str) -> None:
        inst = self._inst[sym]
        cfg  = inst.config
        tf   = inst.tf_min
        logger.info(f"  [{sym}] Warming up {tf}-min bars ({WARMUP_DAYS} days history)…")
        try:
            raw = await asyncio.to_thread(
                get_history, API_KEY, sym, cfg["idx_exchange"], "1m", WARMUP_DAYS
            )
            if not raw:
                logger.warning(f"  [{sym}] No history returned — bars will warm on live ticks.")
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
                logger.warning(f"  [{sym}] History format unrecognised — skipping warm-up.")
                return

            df = df.set_index("dt").sort_index()
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["close"])

            # Resample 1-min → tf-min
            offset_min = f"{15 % tf}min" if tf > 1 else "0min"   # align to 09:15
            try:
                df_tf = df.resample(f"{tf}min", offset="15min").agg(
                    open=("open", "first"),
                    high=("high", "max"),
                    low=("low",  "min"),
                    close=("close", "last"),
                ).dropna()
            except Exception:
                df_tf = df.resample(f"{tf}min").agg(
                    open=("open", "first"),
                    high=("high", "max"),
                    low=("low",  "min"),
                    close=("close", "last"),
                ).dropna()

            df_tf["date"] = df_tf.index.strftime("%Y-%m-%d")

            for _, row in df_tf.tail(490).iterrows():
                inst.bars.append({
                    "open":  float(row["open"]),
                    "high":  float(row["high"]),
                    "low":   float(row["low"]),
                    "close": float(row["close"]),
                    "date":  str(row["date"]),
                })

            if inst.bars:
                inst.ltp = inst.bars[-1]["close"]
                inst.compute_ha_signal()   # pre-seed HA direction
            logger.info(f"  [{sym}] ✅ Warm-up: {len(inst.bars)} {tf}-min bars loaded. Last ≈ {inst.ltp:.0f}")
        except Exception as e:
            logger.error(f"  [{sym}] Warm-up error: {e}")

    async def _warmup_history(self) -> None:
        tasks = [self._warmup_instrument(sym) for sym in INSTRUMENTS]
        await asyncio.gather(*tasks)

    # ── WebSocket subscription helpers ───────────────────────────────────────

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
        # Subscribe all index symbols
        for sym, cfg in INSTRUMENTS.items():
            await self._subscribe(sym, cfg["idx_exchange"])
        await self._subscribe("INDIAVIX", "NSE_INDEX")
        # Re-subscribe any active option symbols
        for sym, inst in self._inst.items():
            if inst.active_trade:
                cfg = inst.config
                await self._subscribe(inst.active_trade["symbol"], cfg["opt_exchange"])

    # ── Trade entry ───────────────────────────────────────────────────────────

    async def _enter_trade(self, sym: str, direction: int) -> None:
        """
        direction=+1: HA bullish → sell PE
        direction=-1: HA bearish → sell CE
        """
        inst = self._inst[sym]
        cfg  = inst.config

        if inst.active_trade is not None:
            return   # already in a trade this session
        if inst._trade_done_today:
            return   # one trade per instrument per day
        if not inst._vol_filter_pass:
            logger.info(f"  [{sym}] Vol filter blocked — no entry today (low-vol day).")
            return
        if not inst.expiry:
            logger.warning(f"  [{sym}] No suitable expiry — skipping entry.")
            return

        spot = inst.ltp
        if spot <= 0:
            return

        # Determine option type
        opt_type = "PE" if direction == 1 else "CE"

        # Compute swing SL
        sl_index = inst.compute_swing_sl(direction)
        if sl_index is None:
            logger.warning(f"  [{sym}] Could not compute swing SL — skipping entry.")
            return

        # Check minimum index risk
        index_risk = abs(spot - sl_index)
        if index_risk < MIN_INDEX_RISK:
            logger.info(
                f"  [{sym}] Index risk {index_risk:.1f} pts < {MIN_INDEX_RISK:.0f} minimum "
                f"(spot={spot:.0f}, sl={sl_index:.0f}). Entry skipped."
            )
            return

        # Resolve ATM option symbol
        symbol = await asyncio.to_thread(
            get_option_symbol,
            API_KEY, sym, cfg["opt_exchange"], inst.expiry, opt_type, "ATM",
        )
        if not symbol:
            logger.error(f"  [{sym}] ATM {opt_type} symbol resolution failed (spot={spot:.0f}, expiry={inst.expiry})")
            return

        # Fetch live premium
        opt_ltp = await asyncio.to_thread(get_option_ltp, symbol, cfg["opt_exchange"], API_KEY)
        if opt_ltp <= 0:
            logger.warning(f"  [{sym}] Option LTP = 0 for {symbol}. Skipping.")
            return

        # Premium filter
        if opt_ltp < MIN_PREMIUM:
            logger.info(
                f"  [{sym}] Premium ₹{opt_ltp:.2f} < ₹{MIN_PREMIUM:.0f} minimum. Entry skipped."
            )
            return

        # Lot size and quantity
        lot_size = _get_lot_size(symbol, cfg["opt_exchange"], cfg["default_lot"])
        qty      = N_LOTS * lot_size

        logger.info(
            f"  [{sym}] HA {'BULLISH' if direction == 1 else 'BEARISH'} flip → SELL {opt_type} "
            f"{symbol}  LTP=₹{opt_ltp:.2f}  SL_index={sl_index:.0f}  "
            f"risk={index_risk:.0f}pts  qty={qty}"
        )

        # Place order
        try:
            res = self.client.placesmartorder(
                strategy      = STRATEGY_NAME,
                symbol        = symbol,
                action        = "SELL",
                exchange      = cfg["opt_exchange"],
                price_type    = "MARKET",
                product       = "MIS",
                quantity      = qty,
                position_size = -qty,
            )
        except Exception as e:
            logger.error(f"  [{sym}] placesmartorder exception: {e}")
            return

        if res.get("status") == "success":
            entry_fill = _resolve_fill(res, opt_ltp)
            inst.active_trade = {
                "symbol":     symbol,
                "side":       opt_type,
                "entry_prem": entry_fill,
                "sl_index":   sl_index,
                "sl_direction": direction,   # +1 bullish trade, -1 bearish trade
                "qty":        qty,
                "lot_size":   lot_size,
                "order_id":   str(res.get("orderid", "")),
                "entry_time": datetime.now().isoformat(),
                "ha_dir_entry": direction,
            }
            inst.lot_size = lot_size
            inst._trade_done_today = True
            await self._subscribe(symbol, cfg["opt_exchange"])

            logger.info(
                f"✅ [{sym}] SOLD {symbol} @ ₹{entry_fill:.2f}  "
                f"({N_LOTS} lot, qty={qty}, order={res.get('orderid')})"
            )
            ha_label = "BULLISH → sell PE" if direction == 1 else "BEARISH → sell CE"
            await send_async(
                f"📉 *HA Options Bot — ENTRY [{sym}]*\n"
                f"HA signal : {ha_label}\n"
                f"Sold      : `{symbol}`  ({N_LOTS} lot, qty={qty})\n"
                f"Entry prem: ₹{entry_fill:.2f}\n"
                f"SL index  : {sl_index:.0f} pts  ({index_risk:.0f} pts risk)\n"
                f"Exit on   : HA reversal OR SL touch\n"
                f"Index spot: {spot:.0f}  |  Expiry: {inst.expiry}\n"
                f"_Signal at {datetime.now().strftime('%H:%M')}_"
            )
        else:
            logger.error(f"  [{sym}] Order rejected: {res}")

    # ── Trade exit ────────────────────────────────────────────────────────────

    async def _close_trade(self, sym: str, exit_prem: float, reason: str) -> None:
        inst = self._inst[sym]
        cfg  = inst.config
        if not inst.active_trade or inst._is_closing:
            return

        inst._is_closing = True
        symbol = inst.active_trade["symbol"]
        qty    = inst.active_trade["qty"]

        try:
            # Use placeorder (NOT placesmartorder) for exits.
            # placesmartorder(position_size=0) reads the broker's NET position across ALL
            # strategies — another bot holding the same symbol would close both at once.
            # placeorder with exact qty is safe.
            res = self.client.placeorder(
                strategy   = STRATEGY_NAME,
                symbol     = symbol,
                action     = "BUY",
                exchange   = cfg["opt_exchange"],
                price_type = "MARKET",
                product    = "MIS",
                quantity   = str(qty),
            )
        except Exception as e:
            logger.error(f"  [{sym}] Close order exception: {e}")
            inst._is_closing = False
            return

        order_ok = res.get("status") == "success"
        if not order_ok:
            # Position may already be closed by sandbox auto-squareoff (15:15 MIS cutoff).
            # Do NOT return early — always log the trade so performance.db stays accurate.
            logger.warning(
                f"  [{sym}] Exit order non-success (likely auto-squareoff already "
                f"closed position): {res}  — logging trade and clearing state."
            )

        # Resolve actual exit fill via OpenAlgo orderstatus; exit_prem (the LTP
        # snapshot that triggered this close) is the fallback if the lookup fails.
        exit_fill = _resolve_fill(res, exit_prem)

        # Capture trade fields before clearing state.
        entry_prem  = inst.active_trade["entry_prem"]
        _entry_time = inst.active_trade.get("entry_time")
        _lot_size   = inst.active_trade.get("lot_size", qty)
        _side       = inst.active_trade.get("side", "")
        _order_id   = inst.active_trade.get("order_id")
        gross       = (entry_prem - exit_fill) * qty
        won         = gross > 0
        emoji       = "🟢" if won else "🔴"

        # Clear state unconditionally (prevents _is_closing lock and ghost active_trade
        # when the exit order is rejected by the broker after auto-squareoff).
        inst.active_trade = None
        inst._is_closing = False

        auto_sq_note = " _(closed by auto-squareoff)_" if not order_ok else ""
        logger.info(
            f"{emoji} [{sym}] CLOSED {symbol} @ ₹{exit_fill:.2f}  "
            f"reason={reason}  gross=₹{gross:,.0f}"
        )
        await send_async(
            f"{emoji} *HA Options Bot — EXIT [{sym}] ({reason})*\n"
            f"Symbol : `{symbol}`\n"
            f"Entry  : ₹{entry_prem:.2f}  →  Exit: ₹{exit_fill:.2f}\n"
            f"Gross P&L : ₹{gross:,.0f}\n"
            f"_Closed at {datetime.now().strftime('%H:%M')}{auto_sq_note}_"
        )
        log_trade_to_db(
            bot_name      = "ha_options_bot",
            instrument    = sym,
            option_symbol = symbol,
            option_type   = _side,
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
        """Unconditional close at 15:14 IST."""
        if self._eod_exit_done:
            return
        self._eod_exit_done = True
        logger.info("🔔 15:14 IST time stop — closing all positions.")
        for sym, inst in self._inst.items():
            if inst.active_trade:
                cfg    = inst.config
                symbol = inst.active_trade["symbol"]
                ltp    = await asyncio.to_thread(get_option_ltp, symbol, cfg["opt_exchange"], API_KEY)
                exit_p = ltp if ltp > 0 else inst.active_trade["entry_prem"]
                await self._close_trade(sym, exit_p, "EOD 15:14")

    # ── Tick handlers ─────────────────────────────────────────────────────────

    async def _on_index_tick(self, sym: str, ltp: float, ts: datetime) -> None:
        inst = self._inst[sym]
        inst.ltp = ltp

        # One-time session init — latches unless ALL instruments failed to
        # resolve an expiry (total failure = OpenAlgo outage); failed attempts
        # retry every 120s (incident 2026-07-07).
        if (not self._session_started and sym == "NIFTY" and ts.time() >= MARKET_OPEN
                and datetime.now() >= self._next_init_attempt):
            if is_market_holiday(API_KEY):
                logger.warning("⚠️ Tick received but today is a holiday. Ignoring.")
                return
            await self._session_open()
            if any(i.expiry for i in self._inst.values()):
                self._session_started = True
            else:
                self._next_init_attempt = datetime.now() + timedelta(seconds=120)
                logger.warning("  ⚠️  Session init incomplete (no expiries) — retrying in 120s.")

        now = datetime.now()

        # ── SL check (tick-level): index crosses swing SL while trade is open
        if inst.active_trade and not inst._is_closing:
            trade     = inst.active_trade
            sl_lvl    = trade["sl_index"]
            direction = trade["ha_dir_entry"]

            sl_hit = (direction == 1 and ltp < sl_lvl) or (direction == -1 and ltp > sl_lvl)
            if sl_hit:
                logger.warning(
                    f"🛑 [{sym}] SL HIT: ltp={ltp:.0f} crossed sl={sl_lvl:.0f}  "
                    f"(direction={'+1' if direction==1 else '-1'})"
                )
                # Fetch current option premium for exit price
                cfg    = inst.config
                symbol = trade["symbol"]
                # Use the cached LTP from shared resolver (it has a 1s cache now)
                # or fallback to entry premium if fetch fails
                opt_ltp = await asyncio.to_thread(get_option_ltp, symbol, cfg["opt_exchange"], API_KEY)
                exit_p  = opt_ltp if opt_ltp > 0 else trade["entry_prem"]
                await self._close_trade(sym, exit_p, "Swing SL")
                return

        # ── Bar close logic
        bar_closed = inst.update_bar(ltp, ts)
        if not bar_closed:
            return

        # Compute HA signal on the just-completed bar
        flip = inst.compute_ha_signal()

        # ── HA reversal exit: if a trade is open, check whether the new HA
        #    direction opposes the entry direction and exit if so.
        #    This must run BEFORE the entry-window / new-entry checks so that
        #    reversal exits fire even when we are no longer in the entry window.
        if inst.active_trade is not None:
            await self._on_index_tick_for_ha_exit(sym)
            return

        # Check entry conditions
        in_window = ENTRY_START <= now.time() <= ENTRY_END
        if not in_window:
            return
        if inst._trade_done_today:
            return
        if not inst.expiry:
            return

        # ── HA reversal exit for existing trade (handled via bar-level HA direction change)
        # (Already exited via SL check above; reversal exit here is belt+braces — but
        # for this bot we exit on reversal ONLY if a trade is open, so check again)
        # Note: active_trade could have been closed in SL check above on same tick.

        # ── Entry signal: HA flip detected
        if flip and flip != 0:
            await self._enter_trade(sym, flip)

    async def _on_index_tick_for_ha_exit(self, sym: str) -> None:
        """
        Called after a bar closes when a trade is already open.
        If the HA direction has reversed versus the trade direction, exit.
        """
        inst = self._inst[sym]
        if not inst.active_trade:
            return

        current_ha_dir = inst.ha_signal
        trade_dir      = inst.active_trade.get("ha_dir_entry", 0)

        if current_ha_dir != 0 and trade_dir != 0 and current_ha_dir != trade_dir:
            logger.info(
                f"🔄 [{sym}] HA REVERSAL: trade_dir={trade_dir} → ha_signal={current_ha_dir}. Closing."
            )
            cfg    = inst.config
            symbol = inst.active_trade["symbol"]
            ltp    = await asyncio.to_thread(get_option_ltp, symbol, cfg["opt_exchange"], API_KEY)
            exit_p = ltp if ltp > 0 else inst.active_trade["entry_prem"]
            await self._close_trade(sym, exit_p, "HA Reversal")

    # ── State dump ────────────────────────────────────────────────────────────

    async def _state_dump_loop(self) -> None:
        while True:
            try:
                instruments_state = {sym: inst.to_state_dict() for sym, inst in self._inst.items()}
                STATE_FILE.write_text(json.dumps({
                    "strategy":          STRATEGY_NAME,
                    "last_update":       datetime.now().isoformat(),
                    "stage_11_start":    STAGE_11_START_DATE,
                    "vix_ltp":           round(self.vix_ltp, 2),
                    "vol_threshold_pct": round(VOL_FILTER_THRESHOLD * 100, 2),
                    "n_lots":            N_LOTS,
                    "entry_window":      f"{ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')}",
                    "sl_lookback":       SL_LOOKBACK,
                    "min_premium":       MIN_PREMIUM,
                    "min_risk_pts":      MIN_INDEX_RISK,
                    "instruments":       instruments_state,
                }, default=str))
            except Exception:
                pass
            await asyncio.sleep(2)

    # ── State restoration on restart ─────────────────────────────────────────

    async def _restore_state(self) -> None:
        """
        Restore _trade_done_today from the state JSON written by _state_dump_loop.
        Critical: prevents the bot from re-entering instruments after a mid-session
        restart where a trade was already taken (or completed) today.

        Only restores if the state file was last updated today. Stale (yesterday's)
        state files are ignored so the bot starts fresh the next trading day.
        """
        if not STATE_FILE.exists():
            return
        try:
            saved      = json.loads(STATE_FILE.read_text())
            today_str  = datetime.now().strftime("%Y-%m-%d")
            last_update = saved.get("last_update", "")
            if not last_update.startswith(today_str):
                logger.info("  State file is from a prior day — starting fresh.")
                return

            restored_trade    = []
            restored_vol      = []
            restored_position = []
            for sym, inst_state in saved.get("instruments", {}).items():
                if sym not in self._inst:
                    continue
                inst = self._inst[sym]

                # Restore trade-done flag
                done = inst_state.get("trade_done_today", False)
                inst._trade_done_today = done
                if done:
                    restored_trade.append(sym)

                # Restore vol-filter flag — if previously blocked, keep it blocked
                # until _check_vol_filter runs again in _session_open
                vol_pass = inst_state.get("vol_filter_pass", True)
                inst._vol_filter_pass = vol_pass
                if not vol_pass:
                    restored_vol.append(sym)

                # ── Restore active_trade (critical for mid-session restart) ──
                # to_state_dict saves the full active_trade dict; restore it so
                # the bot resumes monitoring the open position rather than losing it.
                saved_trade = inst_state.get("active_trade")
                if saved_trade and saved_trade.get("symbol"):
                    inst.active_trade = {
                        "symbol":       saved_trade["symbol"],
                        "side":         saved_trade.get("side", ""),
                        "entry_prem":   float(saved_trade.get("entry_prem", 0)),
                        "sl_index":     float(saved_trade.get("sl_index", 0)) if saved_trade.get("sl_index") else None,
                        "qty":          int(saved_trade.get("qty", 0)),
                        "lot_size":     int(saved_trade.get("lot_size", inst.lot_size)),
                        "entry_time":   saved_trade.get("entry_time", ""),
                        "order_id":     saved_trade.get("order_id", ""),
                        "ha_dir_entry": int(saved_trade.get("ha_dir_entry", 0)),
                        "sl_direction": int(saved_trade.get("sl_direction", 0)),
                    }
                    # Ensure trade_done_today is True whenever there's an open trade
                    inst._trade_done_today = True
                    restored_position.append(f"{sym}:{saved_trade['symbol']}")

            if restored_trade:
                logger.info(f"  Restored trade_done_today=True for: {', '.join(restored_trade)}")
            else:
                logger.info("  No completed trades to restore from today's state.")
            if restored_vol:
                logger.info(f"  Restored vol_filter_pass=False (blocked) for: {', '.join(restored_vol)}")
            if restored_position:
                logger.info(f"  ✅ Restored open positions from state: {', '.join(restored_position)}")
                logger.info("     Bot will resume monitoring these positions immediately.")
            else:
                logger.info("  No open positions to restore.")
        except Exception as e:
            logger.warning(f"  State restore failed: {e}. Starting fresh.")

    # ── Main WebSocket loop ───────────────────────────────────────────────────

    async def main_loop(self) -> None:
        """
        1. Pre-market warm-up: load WARMUP_DAYS of 1-min history → tf-min bars.
        2. Restore today's trade state (prevents double-entries on mid-session restart).
        3. Bootstrap session data immediately if market is already open.
        4. Start state-dump background task.
        5. Connect OpenAlgo WebSocket with auto-reconnect.
        6. Route ticks to per-instrument handlers.
        7. EOD at 15:14 IST.
        """
        await self._warmup_history()
        await self._restore_state()   # must run before _session_open

        # Bootstrap if already past market open (e.g. mid-session restart)
        if datetime.now().time() >= MARKET_OPEN:
            await self._session_open()
            # Latch only if at least one expiry resolved — the tick path
            # retries otherwise.
            self._session_started = any(i.expiry for i in self._inst.values())

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
                    await self._resubscribe_all()

                    if not self._first_connect:
                        logger.info("WebSocket connected/reconnected.")

                    retry_delay = 5

                    async for raw in ws:
                        if datetime.now().time() >= SESSION_END:
                            await self._eod_close_all()
                            return

                        msg  = json.loads(raw)
                        if msg.get("type") != "market_data":
                            continue

                        sym   = msg.get("symbol", "")
                        mdata = msg.get("data", {})
                        ltp   = float(mdata.get("ltp", 0) or mdata.get("lp", 0))
                        ts_raw = mdata.get("t")
                        ts     = (datetime.fromtimestamp(float(ts_raw))
                                  if ts_raw else datetime.now())

                        if ltp <= 0:
                            continue

                        if sym in ("INDIAVIX", "NSE_INDEX:INDIAVIX"):
                            self.vix_ltp = ltp
                            continue

                        # Route index ticks
                        # Check both base (e.g. NIFTY) and prefixed (e.g. NSE_INDEX:NIFTY)
                        matched_inst = None
                        if sym in self._inst:
                            matched_inst = sym
                        else:
                            # Try matching with prefix stripped
                            for base_sym in self._inst:
                                if sym.endswith(f":{base_sym}"):
                                    matched_inst = base_sym
                                    break

                        if matched_inst:
                            await self._on_index_tick(matched_inst, ltp, ts)
                            # Note: reversal exit is now handled inside _on_index_tick()
                            # when a bar closes and a trade is active.  The call above
                            # covers both new-entry and reversal-exit paths correctly.
                            continue

                        # Route option ticks (subscription added after entry)
                        for s, inst in self._inst.items():
                            if (inst.active_trade and
                                    inst.active_trade.get("symbol") == sym):
                                # Option tick — just update; SL is index-level, not premium-level
                                # (research exit: HA reversal and swing SL, not premium multiple)
                                pass

            except Exception as e:
                logger.warning(
                    f"WebSocket error: {e}. Reconnecting in {retry_delay}s…"
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    bot = HAOptionsBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("🛑 HA Options Bot stopped by user.")
