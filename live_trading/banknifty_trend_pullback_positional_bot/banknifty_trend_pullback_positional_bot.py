"""
BANKNIFTY Trend Pullback Positional Bot
========================================
live_trading/banknifty_trend_pullback_positional_bot/banknifty_trend_pullback_positional_bot.py

Research-validated (trend_pullback_positional_study, 2026-07-05):
  options_data/research/trend_pullback_positional_study/results_summary.md
  options_data/research/trend_pullback_positional_study/backtest_sweep_banknifty.py
  Combined IS+OOS (145 trades, 2022-07-01 -> 2026-07-02):
    Win Rate 60.0%  |  Sharpe 1.850  |  Net P&L +Rs 1,013,689  |  Max DD -13.4%
  Stages 0,1,2b,4,5,6,7,8,10 PASS (Stage 9 skipped by explicit user agreement --
  this line is already BANKNIFTY-only, NIFTY dropped Stage 1, SENSEX dropped
  Stage 4). MC stability 99.97%, bootstrap robustness 96.2%, no catastrophic
  walk-forward window, no catastrophic expiry/lot-size/month segment.

Strategy (positional, multi-day hold -- NO daily EOD flatten):
  Regime = EMA(9)/EMA(26) cross on 15-min BANKNIFTY index bars, classified
  against a SMA(50) basis: a bullish cross is "aligned" only if close > SMA50
  at the cross bar; a bearish cross only if close < SMA50. Non-aligned crosses
  are ignored entirely (no regime start/end). A regime lasts until the NEXT
  aligned cross of EITHER direction.

  Within an aligned regime, watch for the FIRST close-beyond-BB(20,2 sigma)
  pullback pierce OPPOSITE the regime direction (bullish regime -> lower-band
  pierce; bearish regime -> upper-band pierce). Only the first pierce per
  regime counts -- if it isn't confirmed (see below), there is no second
  attempt in that regime.

  Once a pierce fires, drop to 1-min bars and watch for a reversal-confirming
  candle within 30 minutes of the pierce bar's close:
    bullish regime (pullback down) -> bullish engulfing or hammer -> go long
    bearish regime (pullback up)   -> bearish engulfing or shooting star -> go short
  Entry fires on the bar AFTER the confirming candle (approximated live by
  acting on it as soon as it is observed via REST poll, since by the time a
  completed 1-min bar is visible via history the "next bar open" moment has
  already passed -- see _confirm_monitor_loop). Entry is skipped if the
  confirming bar's "next" timestamp is at or after 15:10 IST.

  Trade direction -> option side (continuation direction, matches the
  validated naked premium-selling convention -- NOT the rejected debit
  vertical-spread convention):
    trade_direction == "short" -> SELL ATM CE
    trade_direction == "long"  -> SELL ATM PE

Exit priority (first to trigger wins, checked in this order in the backtest):
  1. regime_end          : the aligned regime that produced this trade has
                            ended (a new aligned cross fired) -> exit at market
  2. expiry_force_exit    : today >= the traded contract's expiry date AND
                            time >= 15:14 IST -> exit at market (NOT a daily
                            EOD rule -- this bot is NRML/positional and holds
                            across days otherwise)
  3. target               : live premium <= entry_credit * (1 - TARGET_RETAIN)
  4. sl                   : live premium >= entry_credit * SL_MULT

  regime_end is detected on 15-min bar boundaries (a different, coarser loop
  than the continuous target/SL poll) -- see README.md "Known live-vs-backtest
  divergences" for why priority ordering is a best-effort approximation in
  real time rather than a strict per-bar sequential check like the backtest.

Champion parameters (results_summary.md "Champion Config"):
  Regime EMA:     (9, 26)              Regime basis: SMA(50)
  BB:             (20, 2.0 sigma), close-beyond-band pierce variant
  SL_MULT:        2.5x entry premium
  TARGET_RETAIN:  0.50 (keep 50% of entry premium)
  Entry cutoff:   15:10 IST            Expiry force-exit: 15:14 IST
  Confirmation window: 30 minutes from pierce bar close
  Sizing:         10 lots/trade (N_LOTS)
  Instrument:     BANKNIFTY only (NIFTY dropped Stage 1, SENSEX dropped Stage 4)
  Product:        NRML (multi-day hold, no unconditional daily flatten)

Shared utilities:
  live_trading.api_utils                - get_history, get_expiry_dates, is_market_holiday
  live_trading.shared.atm_resolver      - resolve_atm_option, get_option_ltp
  live_trading.shared.telegram_notifier - send_async
  live_trading.shared.trade_logger      - log_trade_to_db
  openalgo.api                          - placeorder (entries AND exits -- no placesmartorder)
"""

import atexit
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, date, time as dt_time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import websockets
from dotenv import load_dotenv
from openalgo import api

# -- Path / env ----------------------------------------------------------------
ROOT = Path(__file__).parent.parent.parent   # .../openalgo (fyers_crk)
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils                import get_history, is_market_holiday
from live_trading.shared.atm_resolver      import resolve_atm_option, get_option_ltp
from live_trading.shared.order_fill        import fetch_fill_price
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger      import log_trade_to_db

# -- Logging ---------------------------------------------------------------------
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "banknifty_trend_pullback_positional_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# -- Environment -------------------------------------------------------------------
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER",   "http://127.0.0.1:5001")   # fyers_crk port
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:8765")

if not API_KEY:
    logger.error("OPENALGO_API_KEY not found. Exiting.")
    sys.exit(1)

# -- Strategy constants --------------------------------------------------------------
STRATEGY_NAME = "BANKNIFTY_TREND_PULLBACK_POSITIONAL"
BOT_NAME      = "banknifty_trend_pullback_positional_bot"


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback

SYMBOL           = "BANKNIFTY"
IDX_EXCHANGE     = "NSE_INDEX"
OPT_EXCHANGE     = "NFO"
DEFAULT_LOT_SIZE = 30   # BANKNIFTY lot size, post-Nov-2025 (see project memory) -- overridden by _get_lot_size()

N_LOTS = 10   # standardised: 10 lots per CLAUDE.md position-size rule

# Regime / pullback parameters (research champion -- backtest_sweep_banknifty.py)
EMA_FAST             = 9
EMA_SLOW             = 26
REGIME_BASIS_PERIOD  = 50
BB_PERIOD            = 20
BB_STD               = 2.0
ENTRY_SEARCH_MINUTES = 30

# Exit parameters (champion: pierce=close, SL_MULT=2.5x, TARGET_RETAIN=0.50)
SL_MULT        = 2.5
TARGET_RETAIN  = 0.50

# Timing
ENTRY_CUTOFF      = dt_time(15, 10)   # no confirmed entries at/after this time
EXPIRY_FORCE_TIME = dt_time(15, 14)   # force-exit on/after expiry date at/after this time
MIN_DTE           = 2

# History needed: >= REGIME_BASIS_PERIOD+1 completed 15-min bars for regime
# readiness, plus margin for cold-start regime reconstruction.
HISTORY_DAYS = 20

# State + lockfile
STATE_FILE = LOGS_DIR / "banknifty_trend_pullback_positional_state.json"
PID_FILE   = LOGS_DIR / "banknifty_trend_pullback_positional_bot.pid"

EXIT_POLL_SEC    = 30   # target/SL/expiry-force poll interval
CONFIRM_POLL_SEC = 15   # 1-min confirmation-candle poll interval (only while awaiting)


# -- PID lockfile ----------------------------------------------------------------

def _acquire_pid_lock() -> None:
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            os.kill(old_pid, 0)
            logger.error(
                f"Another instance already running (PID {old_pid}). "
                f"Delete {PID_FILE} if stale."
            )
            sys.exit(1)
        except ProcessLookupError:
            logger.warning(f"Stale PID file (PID {old_pid}) -- removing.")
            PID_FILE.unlink(missing_ok=True)
        except (PermissionError, ValueError):
            logger.error("Could not verify existing PID. Aborting.")
            sys.exit(1)
    PID_FILE.write_text(str(os.getpid()))
    atexit.register(_release_pid_lock)
    logger.info(f"PID lock acquired (PID {os.getpid()})")


def _release_pid_lock() -> None:
    try:
        if PID_FILE.exists() and int(PID_FILE.read_text().strip()) == os.getpid():
            PID_FILE.unlink()
            logger.info("PID lock released.")
    except Exception:
        pass


# -- Lot size helper ---------------------------------------------------------------

def _get_lot_size(symbol: str, opt_exchange: str, default: int) -> int:
    try:
        from database.token_db import get_symbol_info
        si = get_symbol_info(symbol, opt_exchange)
        if si and getattr(si, "lotsize", None):
            ls = int(si.lotsize)
            logger.info(f"  Lot size from DB: {ls}  ({symbol})")
            return ls
    except Exception as e:
        logger.warning(f"  Lot size DB lookup failed for {symbol}: {e}. Using {default}.")
    return default


# -- Candle-pattern helpers (verbatim translation of backtest_sweep_banknifty.py) --

def _body(row) -> float:
    return abs(row["close"] - row["open"])


def _upper_wick(row) -> float:
    return row["high"] - max(row["open"], row["close"])


def _lower_wick(row) -> float:
    return min(row["open"], row["close"]) - row["low"]


def _range(row) -> float:
    return row["high"] - row["low"]


def _is_bearish_engulfing(prev, cur) -> bool:
    return (cur["close"] < cur["open"] and prev["close"] > prev["open"]
            and cur["open"] >= prev["close"] and cur["close"] <= prev["open"])


def _is_bullish_engulfing(prev, cur) -> bool:
    return (cur["close"] > cur["open"] and prev["close"] < prev["open"]
            and cur["open"] <= prev["close"] and cur["close"] >= prev["open"])


def _is_shooting_star(cur) -> bool:
    rng = _range(cur)
    if rng <= 0:
        return False
    body, uw, lw = _body(cur), _upper_wick(cur), _lower_wick(cur)
    return uw >= 2 * body and lw <= 0.3 * rng and body <= 0.3 * rng


def _is_hammer(cur) -> bool:
    rng = _range(cur)
    if rng <= 0:
        return False
    body, uw, lw = _body(cur), _upper_wick(cur), _lower_wick(cur)
    return lw >= 2 * body and uw <= 0.3 * rng and body <= 0.3 * rng


# -- Bar building ------------------------------------------------------------------

def _load_1min_df(raw_bars: list[dict]) -> pd.DataFrame | None:
    """Convert list[dict] from get_history into a clean, session-filtered 1-min OHLC df."""
    if not raw_bars:
        return None
    df = pd.DataFrame(raw_bars)
    if "timestamp" in df.columns:
        df["dt"] = (
            pd.to_datetime(df["timestamp"], unit="s", utc=True)
            .dt.tz_convert("Asia/Kolkata")
            .dt.tz_localize(None)
        )
    elif "date" in df.columns:
        df["dt"] = pd.to_datetime(df["date"])
    else:
        logger.warning("  History: no timestamp/date column found.")
        return None

    df = df.sort_values("dt").set_index("dt")
    df = df[["open", "high", "low", "close"]].astype(float)
    df = df.between_time("09:15", "15:29")
    return df if not df.empty else None


def _build_15min_df(df1: pd.DataFrame) -> pd.DataFrame | None:
    """Resample a multi-day continuous 1-min df into 15-min bars (day-aligned
    automatically since 09:15 falls on a 15-min-of-hour boundary)."""
    r = df1.resample("15min", closed="left", label="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    )
    r = r.between_time("09:15", "15:29").dropna()
    min_bars = REGIME_BASIS_PERIOD + 1
    if len(r) < min_bars:
        logger.warning(f"  Insufficient 15-min bars: {len(r)} (need >= {min_bars})")
        return None
    return r


def _add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Verbatim translation of add_indicators() in backtest_sweep_banknifty.py."""
    df = df.copy()
    df["ema9"]  = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema26"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    df["bb_basis"] = df["close"].rolling(BB_PERIOD).mean()
    bb_std = df["close"].rolling(BB_PERIOD).std(ddof=0)
    df["bb_upper"] = df["bb_basis"] + BB_STD * bb_std
    df["bb_lower"] = df["bb_basis"] - BB_STD * bb_std
    df["regime_basis"] = df["close"].rolling(REGIME_BASIS_PERIOD).mean()
    return df


def _replay_current_regime(df: pd.DataFrame) -> dict | None:
    """Full recompute of find_cross_events() + find_aligned_regimes(), returning
    only the LATEST (still-open, live) aligned regime -- mirrors the backtest's
    cross-detection/alignment logic exactly, but has no notion of "end" since
    we are live (the regime is open until the next aligned cross fires)."""
    if len(df) < REGIME_BASIS_PERIOD + 1:
        return None
    diff = (df["ema9"] - df["ema26"]).values
    sign = np.sign(diff)
    cross_idx = np.where(np.diff(sign) != 0)[0] + 1
    events = [
        {"i": int(i), "direction": "bullish" if diff[i] > 0 else "bearish"}
        for i in cross_idx if i >= max(REGIME_BASIS_PERIOD, EMA_SLOW)
    ]
    aligned = [
        ev for ev in events
        if (ev["direction"] == "bullish" and df["close"].iloc[ev["i"]] > df["regime_basis"].iloc[ev["i"]])
        or (ev["direction"] == "bearish" and df["close"].iloc[ev["i"]] < df["regime_basis"].iloc[ev["i"]])
    ]
    if not aligned:
        return None
    last = aligned[-1]
    return {
        "direction": last["direction"],
        "since_i":   last["i"],
        "since_ts":  df.index[last["i"]],
    }


def _find_pierce(df: pd.DataFrame, since_i: int, direction: str) -> dict | None:
    """Verbatim translation of find_first_pullback_setup(), bounded only by
    "since regime start through now" (the backtest also bounds by a known
    regime end, which live we don't have yet)."""
    pullback_side  = "lower" if direction == "bullish" else "upper"
    trade_direction = "long" if direction == "bullish" else "short"
    win = df.iloc[since_i:]
    close = win["close"].values
    lower = win["bb_lower"].values
    upper = win["bb_upper"].values
    hit = (close < lower) if pullback_side == "lower" else (close > upper)
    idx = np.where(hit)[0]
    if len(idx) == 0:
        return None
    pierce_i  = since_i + int(idx[0])
    pierce_ts = df.index[pierce_i]
    return {
        "pierce_ts":      pierce_ts,
        "pierce_bar_end": pierce_ts + pd.Timedelta(minutes=15),
        "trade_direction": trade_direction,
    }


# -- Position dataclass ---------------------------------------------------------

class Position:
    __slots__ = (
        "direction", "opt_symbol", "opt_type", "exchange",
        "entry_time", "credit", "sl_level", "tgt_level",
        "lot_size", "n_lots", "quantity", "order_id", "expiry_date",
    )

    def __init__(self, direction, opt_symbol, opt_type, exchange,
                 entry_time, credit, lot_size, n_lots, order_id, expiry_date):
        self.direction   = direction
        self.opt_symbol  = opt_symbol
        self.opt_type    = opt_type
        self.exchange    = exchange
        self.entry_time  = entry_time
        self.credit      = credit
        self.sl_level    = round(credit * SL_MULT, 2)
        self.tgt_level   = round(credit * (1 - TARGET_RETAIN), 2)
        self.lot_size    = lot_size
        self.n_lots      = n_lots
        self.quantity    = lot_size * n_lots
        self.order_id    = order_id
        self.expiry_date = expiry_date

    def to_dict(self) -> dict:
        return {
            "direction":   self.direction,
            "opt_symbol":  self.opt_symbol,
            "opt_type":    self.opt_type,
            "exchange":    self.exchange,
            "entry_time":  self.entry_time.isoformat(),
            "credit":      self.credit,
            "sl_level":    self.sl_level,
            "tgt_level":   self.tgt_level,
            "lot_size":    self.lot_size,
            "n_lots":      self.n_lots,
            "quantity":    self.quantity,
            "order_id":    self.order_id,
            "expiry_date": self.expiry_date.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Position":
        pos = cls(
            direction=d["direction"], opt_symbol=d["opt_symbol"], opt_type=d["opt_type"],
            exchange=d["exchange"], entry_time=datetime.fromisoformat(d["entry_time"]),
            credit=d["credit"], lot_size=d["lot_size"], n_lots=d["n_lots"],
            order_id=d["order_id"], expiry_date=date.fromisoformat(d["expiry_date"]),
        )
        # sl_level/tgt_level are re-derived from credit in __init__ -- restore
        # verbatim instead, in case SL_MULT/TARGET_RETAIN ever change later.
        pos.sl_level  = d["sl_level"]
        pos.tgt_level = d["tgt_level"]
        return pos


# -- Main bot class ---------------------------------------------------------------

class TrendPullbackPositionalBot:

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.lot_size = DEFAULT_LOT_SIZE

        self.position: Position | None = None

        # regime/pullback/confirmation state
        self.regime: dict = {}            # {"direction":..., "since_ts": iso str}
        self.pierce_found = False
        self.pierce_info: dict | None = None   # {"pierce_ts","pierce_bar_end","trade_direction"} (ts as iso str)
        self.awaiting_confirm = False
        self.confirm_deadline: datetime | None = None
        self.no_trade_this_regime = False

        self._last_bar_ts: pd.Timestamp | None = None   # 15-min WS boundary tracker
        self._ltp: float = 0.0
        self._exit_lock = asyncio.Lock()

        logger.info(
            f"{STRATEGY_NAME} | EMA({EMA_FAST},{EMA_SLOW}) vs SMA{REGIME_BASIS_PERIOD} regime | "
            f"BB({BB_PERIOD},{BB_STD}) close-pierce | SL={SL_MULT}x TGT=keep{int(TARGET_RETAIN*100)}% | "
            f"expiry-force {EXPIRY_FORCE_TIME} | {N_LOTS} lots | product=NRML"
        )

    # -- State persistence --------------------------------------------------------

    def _load_state(self) -> None:
        if not STATE_FILE.exists():
            return
        try:
            d = json.loads(STATE_FILE.read_text())
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return

        if d.get("position"):
            self.position = Position.from_dict(d["position"])
            logger.info(f"Restored open position: {self.position.opt_symbol} "
                        f"credit={self.position.credit} entry={self.position.entry_time}")

        self.regime               = d.get("regime") or {}
        self.pierce_found         = bool(d.get("pierce_found", False))
        self.pierce_info          = d.get("pierce_info")
        self.awaiting_confirm     = bool(d.get("awaiting_confirm", False))
        cd                        = d.get("confirm_deadline")
        self.confirm_deadline     = datetime.fromisoformat(cd) if cd else None
        self.no_trade_this_regime = bool(d.get("no_trade_this_regime", False))

        if self.regime:
            logger.info(f"Restored regime: {self.regime.get('direction')} since "
                        f"{self.regime.get('since_ts')}  pierce_found={self.pierce_found} "
                        f"awaiting_confirm={self.awaiting_confirm}")

    def _save_state(self) -> None:
        try:
            state = {
                "last_update":          datetime.now().isoformat(),
                "strategy":             STRATEGY_NAME,
                "position":             self.position.to_dict() if self.position else None,
                "regime":               self.regime or None,
                "pierce_found":         self.pierce_found,
                "pierce_info":          self.pierce_info,
                "awaiting_confirm":     self.awaiting_confirm,
                "confirm_deadline":     self.confirm_deadline.isoformat() if self.confirm_deadline else None,
                "no_trade_this_regime": self.no_trade_this_regime,
                "ltp":                  self._ltp,
            }
            STATE_FILE.write_text(json.dumps(state, indent=2, default=str))
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    # -- Startup -------------------------------------------------------------------

    def _startup_checks(self) -> bool:
        today = date.today()
        if is_market_holiday(API_KEY, today.isoformat(), exchange="NSE"):
            logger.info("Market holiday today. Bot will idle and exit cleanly.")
            return False
        self.lot_size = _get_lot_size(SYMBOL, OPT_EXCHANGE, DEFAULT_LOT_SIZE)
        logger.info(f"  {SYMBOL}: lot_size={self.lot_size}  N_LOTS={N_LOTS}  qty={self.lot_size * N_LOTS}")
        return True

    # -- Order helpers ---------------------------------------------------------------

    def _place_sell(self, opt_symbol: str, exchange: str, quantity: int) -> dict:
        return self.client.placeorder(
            strategy=STRATEGY_NAME, symbol=opt_symbol, action="SELL",
            exchange=exchange, price_type="MARKET", product="NRML", quantity=str(quantity),
        )

    def _place_buy(self, opt_symbol: str, exchange: str, quantity: int) -> dict:
        # Always placeorder (not placesmartorder) for exits -- avoids multi-bot position wipeout
        return self.client.placeorder(
            strategy=STRATEGY_NAME, symbol=opt_symbol, action="BUY",
            exchange=exchange, price_type="MARKET", product="NRML", quantity=str(quantity),
        )

    # -- Regime / pullback scan (called on every 15-min bar close) ------------------

    async def _scan_regime(self, bar_ts: pd.Timestamp) -> None:
        raw = get_history(API_KEY, SYMBOL, IDX_EXCHANGE, "1m", HISTORY_DAYS)
        df1 = _load_1min_df(raw)
        if df1 is None or df1.empty:
            logger.warning(f"  No 1-min history available -- skipping bar {bar_ts}.")
            return
        df15 = _build_15min_df(df1)
        if df15 is None:
            return
        df15 = _add_indicators(df15)

        cur_regime = _replay_current_regime(df15)
        if cur_regime is None:
            self._save_state()
            return

        new_since = cur_regime["since_ts"].isoformat()
        prev_since = self.regime.get("since_ts")

        if new_since != prev_since:
            logger.info(f"Regime transition -> {cur_regime['direction']} since {new_since}")
            if self.position is not None:
                ltp = get_option_ltp(self.position.opt_symbol, self.position.exchange, API_KEY) \
                      or self.position.credit
                await self._close_position("regime_end", ltp)
            elif self.awaiting_confirm:
                logger.info("  Prior regime ended before confirmation -- setup abandoned.")
                await send_async(
                    f"{BOT_NAME}: regime ended before pullback confirmation completed -- "
                    f"no trade taken for that setup."
                )
            self.regime = {"direction": cur_regime["direction"], "since_ts": new_since}
            self.pierce_found = False
            self.pierce_info = None
            self.awaiting_confirm = False
            self.confirm_deadline = None
            self.no_trade_this_regime = False

        if self.position is not None or self.pierce_found or self.no_trade_this_regime:
            self._save_state()
            return

        since_i = df15.index.get_loc(cur_regime["since_ts"])
        pierce = _find_pierce(df15, since_i, cur_regime["direction"])
        if pierce is None:
            self._save_state()
            return

        self.pierce_found = True
        self.pierce_info = {
            "pierce_ts":       pierce["pierce_ts"].isoformat(),
            "pierce_bar_end":  pierce["pierce_bar_end"].isoformat(),
            "trade_direction": pierce["trade_direction"],
        }
        deadline = pierce["pierce_bar_end"] + timedelta(minutes=ENTRY_SEARCH_MINUTES)
        now = datetime.now()
        if now > deadline.to_pydatetime():
            logger.warning(
                f"  Pierce found @ {pierce['pierce_ts']} but confirmation window already "
                f"elapsed (deadline was {deadline}) -- treating as a missed/stale signal, "
                f"no trade this regime. This can happen after a cold start mid-regime."
            )
            self.no_trade_this_regime = True
        else:
            self.awaiting_confirm = True
            self.confirm_deadline = deadline.to_pydatetime()
            logger.info(
                f"  Pullback pierce detected @ {pierce['pierce_ts']} "
                f"({pierce['trade_direction']}) -- awaiting 1-min confirmation until {deadline}"
            )
            await send_async(
                f"{BOT_NAME}: pullback pierce detected ({pierce['trade_direction']}) -- "
                f"awaiting candle confirmation until {deadline.strftime('%H:%M')}"
            )
        self._save_state()

    # -- 1-min confirmation monitor (fast poll, only while awaiting) ----------------

    async def _confirm_monitor_loop(self) -> None:
        while True:
            await asyncio.sleep(CONFIRM_POLL_SEC)
            if not self.awaiting_confirm or self.position is not None:
                continue

            now = datetime.now()
            if self.confirm_deadline and now > self.confirm_deadline:
                logger.info("Confirmation window expired -- no trade this regime.")
                self.awaiting_confirm = False
                self.no_trade_this_regime = True
                self._save_state()
                continue

            raw = get_history(API_KEY, SYMBOL, IDX_EXCHANGE, "1m", 3)
            df1 = _load_1min_df(raw)
            if df1 is None or len(df1) < 2:
                continue

            pierce_bar_end = pd.Timestamp(self.pierce_info["pierce_bar_end"])
            deadline_ts    = pd.Timestamp(self.confirm_deadline)
            window = df1[(df1.index >= pierce_bar_end) & (df1.index <= deadline_ts)]
            if len(window) < 2:
                continue

            trade_direction = self.pierce_info["trade_direction"]
            fired_at = None
            decision_close = None
            for k in range(1, len(window) - 1):
                prev, cur = window.iloc[k - 1], window.iloc[k]
                fired = (
                    (_is_bullish_engulfing(prev, cur) or _is_hammer(cur)) if trade_direction == "long"
                    else (_is_bearish_engulfing(prev, cur) or _is_shooting_star(cur))
                )
                if fired:
                    fired_at = window.index[k + 1]
                    decision_close = float(cur["close"])
                    break

            if fired_at is None:
                continue

            if fired_at.time() > ENTRY_CUTOFF:
                logger.info(f"  Confirmation found @ {fired_at} but past {ENTRY_CUTOFF} cutoff -- no trade.")
                self.awaiting_confirm = False
                self.no_trade_this_regime = True
                self._save_state()
                continue

            logger.info(f"  Confirmation candle found -- entering {trade_direction} "
                        f"(decision_close={decision_close})")
            await self._enter_trade(trade_direction, decision_close)

    # -- Entry -----------------------------------------------------------------------

    async def _enter_trade(self, trade_direction: str, decision_close: float) -> None:
        opt_type = "CE" if trade_direction == "short" else "PE"

        info = resolve_atm_option(
            decision_close, opt_type=opt_type, api_key=API_KEY,
            min_dte=MIN_DTE, index=SYMBOL,
        )
        if info is None:
            logger.error("  ATM resolution failed -- abandoning this setup (no retry within same regime).")
            self.awaiting_confirm = False
            self.no_trade_this_regime = True
            self._save_state()
            return

        ltp = get_option_ltp(info["symbol"], info["exchange"], API_KEY)
        if ltp <= 0:
            logger.error(f"  {info['symbol']} LTP=0 -- abandoning this setup.")
            self.awaiting_confirm = False
            self.no_trade_this_regime = True
            self._save_state()
            return

        qty = self.lot_size * N_LOTS
        res = self._place_sell(info["symbol"], info["exchange"], qty)
        if res.get("status") != "success":
            logger.error(f"  SELL order FAILED: {res}")
            await send_async(f"{BOT_NAME}: SELL {info['symbol']} FAILED: {res.get('message', '?')}")
            self.awaiting_confirm = False
            self.no_trade_this_regime = True
            self._save_state()
            return

        order_id    = res.get("orderid", "")
        entry_time  = datetime.now()
        fill_prem   = _resolve_fill(res, ltp)
        credit      = round(fill_prem, 2)
        expiry_date = datetime.strptime(info["expiry"], "%d%b%y").date()

        self.position = Position(
            direction=trade_direction, opt_symbol=info["symbol"], opt_type=opt_type,
            exchange=info["exchange"], entry_time=entry_time, credit=credit,
            lot_size=self.lot_size, n_lots=N_LOTS, order_id=order_id, expiry_date=expiry_date,
        )
        self.awaiting_confirm = False
        self._save_state()

        msg = (
            f"SOLD {info['symbol']}\n"
            f"  {BOT_NAME} {trade_direction.upper()} regime pullback confirmed\n"
            f"  Credit: Rs {credit:.2f}  Qty: {qty}\n"
            f"  SL: Rs {self.position.sl_level:.2f}  Target: Rs {self.position.tgt_level:.2f}\n"
            f"  Expiry: {info['expiry']}"
        )
        logger.info(msg)
        await send_async(msg)

    # -- Exit ------------------------------------------------------------------------

    async def _close_position(self, reason: str, exit_premium: float) -> None:
        async with self._exit_lock:
            pos = self.position
            if pos is None:
                return

            res = self._place_buy(pos.opt_symbol, pos.exchange, pos.quantity)
            if res.get("status") != "success":
                logger.error(f"  EXIT order FAILED: {res}. Position may remain open at the broker.")

            # Resolve actual fill price for the close order (falls back to the
            # LTP snapshot that triggered this exit if the lookup fails).
            exit_fill = _resolve_fill(res, exit_premium)

            pnl_per_unit = pos.credit - exit_fill
            gross_pnl    = pnl_per_unit * pos.quantity

            msg = (
                f"{'PROFIT' if gross_pnl > 0 else 'LOSS'} CLOSED {pos.opt_symbol}\n"
                f"  {BOT_NAME} reason={reason}\n"
                f"  Credit: Rs {pos.credit:.2f}  Exit: Rs {exit_fill:.2f}\n"
                f"  Gross P&L: Rs {gross_pnl:,.0f}  Qty: {pos.quantity}"
            )
            logger.warning(msg)
            await send_async(msg)

            log_trade_to_db(
                bot_name=BOT_NAME, instrument=SYMBOL, option_symbol=pos.opt_symbol,
                option_type=pos.opt_type, entry_time=pos.entry_time, exit_time=datetime.now(),
                entry_premium=pos.credit, exit_premium=round(exit_fill, 2), exit_reason=reason,
                quantity=pos.quantity, lots=pos.n_lots, lot_size=pos.lot_size,
                gross_pnl=round(gross_pnl, 2), order_id=pos.order_id,
                strategy_type="options", direction="sell",
            )
            self.position = None
            # Only one trade per aligned-regime episode -- do not re-scan for a
            # second pullback in the same still-open regime after any exit that
            # is not itself a regime transition (regime_end already resets this
            # flag to False for the NEW regime right after this call returns).
            self.no_trade_this_regime = True
            self._save_state()

    # -- Exit monitor loop (target / SL / expiry-force poll) -------------------------

    async def _exit_monitor_loop(self) -> None:
        while True:
            await asyncio.sleep(EXIT_POLL_SEC)
            pos = self.position
            if pos is None:
                continue

            now = datetime.now()
            if now.date() >= pos.expiry_date and now.time() >= EXPIRY_FORCE_TIME:
                ltp = get_option_ltp(pos.opt_symbol, pos.exchange, API_KEY) or pos.credit
                await self._close_position("expiry_force_exit", ltp)
                continue

            ltp = get_option_ltp(pos.opt_symbol, pos.exchange, API_KEY)
            if ltp <= 0:
                continue

            if ltp <= pos.tgt_level:
                logger.info(f"  Target: {pos.opt_symbol} LTP={ltp:.2f} <= TGT={pos.tgt_level:.2f}")
                await self._close_position("target", ltp)
            elif ltp >= pos.sl_level:
                logger.warning(f"  SL: {pos.opt_symbol} LTP={ltp:.2f} >= SL={pos.sl_level:.2f}")
                await self._close_position("sl", ltp)

    # -- 15-min bar boundary detection (WS tick driven) -------------------------------

    def _on_15min_boundary(self, tick_ts: datetime) -> pd.Timestamp | None:
        """Return the closed bar timestamp when we cross a 15-min boundary, else None."""
        floored = tick_ts.replace(minute=(tick_ts.minute // 15) * 15, second=0, microsecond=0)
        bar_ts = pd.Timestamp(floored)
        prev = self._last_bar_ts
        if prev is None:
            self._last_bar_ts = bar_ts
            return None
        if bar_ts > prev:
            self._last_bar_ts = bar_ts
            return prev
        return None

    # -- WebSocket loop ---------------------------------------------------------------

    async def _ws_loop(self) -> None:
        retry_delay = 5
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=20) as ws:
                    self.ws = ws

                    await ws.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))
                    await ws.send(json.dumps({
                        "action": "subscribe", "symbol": SYMBOL, "exchange": IDX_EXCHANGE, "mode": 1,
                    }))
                    logger.info(f"Subscribed: {SYMBOL} ({IDX_EXCHANGE})")
                    logger.info(f"WebSocket connected: {WS_URL}")
                    retry_delay = 5

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if msg.get("type") != "market_data":
                            continue
                        if msg.get("symbol") != SYMBOL:
                            continue
                        ltp = (msg.get("data") or {}).get("ltp")
                        if not ltp:
                            continue
                        self._ltp = float(ltp)

                        ts_raw = msg.get("timestamp") or msg.get("ts")
                        try:
                            tick_ts = datetime.fromisoformat(ts_raw) if ts_raw else datetime.now()
                        except (ValueError, TypeError):
                            tick_ts = datetime.now()

                        now_t = tick_ts.time()
                        if now_t < dt_time(9, 15) or now_t > dt_time(15, 30):
                            continue

                        closed_bar = self._on_15min_boundary(tick_ts)
                        if closed_bar is not None:
                            logger.debug(f"  Bar closed: {closed_bar} -- scanning regime")
                            asyncio.create_task(self._scan_regime(closed_bar))

            except websockets.ConnectionClosed as e:
                logger.warning(f"WS disconnected ({e}). Retry in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
            except Exception as e:
                logger.error(f"WS error: {e}. Retry in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    # -- Main run ----------------------------------------------------------------------

    async def run(self) -> None:
        _acquire_pid_lock()
        logger.info(f"Starting {STRATEGY_NAME} -- {date.today()}")
        self._load_state()
        if not self._startup_checks():
            return

        # Establish current regime immediately on startup, instead of waiting
        # for the next live 15-min bar close (which could be up to 15 minutes
        # away and risks missing an already-in-progress pierce/confirmation).
        raw = get_history(API_KEY, SYMBOL, IDX_EXCHANGE, "1m", HISTORY_DAYS)
        df1 = _load_1min_df(raw)
        if df1 is not None and not df1.empty:
            df15 = _build_15min_df(df1)
            if df15 is not None:
                self._last_bar_ts = df15.index[-1]
                await self._scan_regime(df15.index[-1])

        await send_async(
            f"{BOT_NAME} started\n"
            f"  BANKNIFTY 15-min EMA({EMA_FAST},{EMA_SLOW}) vs SMA{REGIME_BASIS_PERIOD} regime\n"
            f"  BB({BB_PERIOD},{BB_STD}) pullback -> 1-min candle confirm -> sell ATM option\n"
            f"  SL={SL_MULT}x  Target=keep{int(TARGET_RETAIN*100)}%  {N_LOTS} lots  product=NRML\n"
            f"  Exit priority: regime_end -> expiry_force_exit({EXPIRY_FORCE_TIME}) -> target -> sl"
        )

        await asyncio.gather(
            self._ws_loop(),
            self._exit_monitor_loop(),
            self._confirm_monitor_loop(),
        )


# -- Entrypoint ----------------------------------------------------------------------

def main() -> None:
    bot = TrendPullbackPositionalBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Bot stopped (KeyboardInterrupt).")


if __name__ == "__main__":
    main()
