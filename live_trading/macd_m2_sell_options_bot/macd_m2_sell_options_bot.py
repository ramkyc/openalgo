"""
MACD M2 Sell Options Bot
========================
live_trading/macd_m2_sell_options_bot/macd_m2_sell_options_bot.py

Research-validated (macd_price_action_sr_study sell-side extension, 2026-06-21):
  options_data/research/macd_price_action_sr_study/sell_strategy_spec.txt
  IS  Sharpe +9.42  |  Win% 73%  |  PF 5.23  |  DD% 3.1%  |  n=269
  OOS Sharpe +6.16  |  Win% 71%  |  PF 3.16  |  DD% 3.2%  |  n=309
  Walk-forward: 10/10 windows positive  |  Bootstrap 5th pct Sharpe=6.37
  8/8 decision gates PASS — STRONG GREEN LIGHT

Strategy:
  Watch 15-minute MACD(12,26,9) on NIFTY and BANKNIFTY spot index.
  When the MACD line crosses FROM BELOW ZERO to ABOVE ZERO (bull M2) AND
  the index close is within ±0.2% of the prior day's pivot point (SR3):
    → SELL ATM PE option on that instrument.
  When the MACD line crosses FROM ABOVE ZERO to BELOW ZERO (bear M2) AND
  the index close is within ±0.2% of the prior day's pivot point:
    → SELL ATM CE option on that instrument.

  The edge: MACD M2 crossings near a pivot level mark high-conviction
  inflection zones.  The sold option decays rapidly as the expected move
  fails to materialise.  Win rate 70–73% over 6.5 years IS+OOS.

Signal rules (ALL required):
  1. MACD(12,26,9) zero-line crossover on 15-min bars (M2 — from below 0 to above,
     or from above 0 to below)
  2. Index close within ±0.2% of prior day's pivot point PP=(H+L+C)/3
  3. Signal bar close time falls before 14:30 IST (no entries at or after 14:30)
  4. No position already open for that instrument
  5. Option DTE ≥ 2 days (skip if only same-day or next-day expiry available)
  6. Option LTP at entry > ₹10 (minimum viable credit)

Exit rules (first to trigger wins):
  SL   (stop loss)  : Buy back if live premium reaches 1.5× entry credit
  TGT  (target)     : Buy back when premium decays to 50% of entry credit (keep 50%)
  EOD  (time stop)  : Close unconditionally at 15:14 IST  ← hard rule, no exceptions
  Session filter    : No new entries at or after 14:30 IST

Research parameters (Phase S4 optimal, 2026-06-21):
  MACD:          (12, 26, 9) — zero-line crossover (M2)
  SR3 tolerance: ±0.2% of prior day's pivot
  SL multiple:   1.5× entry credit (Phase S4 winner: 1.5× beats 2×)
  Target keep:   50% of credit received  (Phase S4 winner)
  Entry window:  09:15 – 14:30 IST
  EOD close:     15:14 IST
  Instruments:   NIFTY + BANKNIFTY (both active, independent positions)
  Lot sizing:    N_LOTS per instrument (PAPER: 5 lots each)
  Expiry:        Weekly, nearest with DTE ≥ 2
  Options exch:  NFO

Shared utilities:
  live_trading.api_utils                — get_history, get_expiry_dates, get_option_symbol
  live_trading.shared.atm_resolver      — get_option_ltp
  live_trading.shared.telegram_notifier — send_async
  live_trading.shared.trade_logger      — log_trade_to_db
  openalgo.api                          — placeorder (entries AND exits — no placesmartorder)
"""

import atexit
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, date, time as dt_time
from pathlib import Path

import pandas as pd
import websockets
from dotenv import load_dotenv
from openalgo import api

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo (fyers_crk)
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils                import (
    get_expiry_dates, get_option_symbol, get_history, is_market_holiday,
)
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
        logging.FileHandler(LOGS_DIR / "macd_m2_sell_options_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER",   "http://127.0.0.1:5001")   # fyers_crk port
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:8765")

if not API_KEY:
    logger.error("❌ OPENALGO_API_KEY not found. Exiting.")
    sys.exit(1)

# ── Strategy constants ────────────────────────────────────────────────────────
STRATEGY_NAME = "MACD_M2_SELL_OPTIONS"
BOT_NAME      = "macd_m2_sell_options_bot"

INSTRUMENTS = [
    {"symbol": "NIFTY",     "exchange": "NSE_INDEX", "opt_exchange": "NFO",
     "strike_step": 50,  "default_lot_size": 25},
    {"symbol": "BANKNIFTY", "exchange": "NSE_INDEX", "opt_exchange": "NFO",
     "strike_step": 100, "default_lot_size": 15},
]

N_LOTS = 5   # paper trading: 5 lots per instrument

# MACD parameters (research champion)
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIG  = 9
SR3_TOL   = 0.002   # ±0.2% of prior day's pivot

# Exit parameters (Phase S4 winner)
SL_MULT      = 1.5   # buy back if premium reaches 1.5× credit received
TGT_KEEP_PCT = 0.50  # buy back when premium decays to 50% of credit (keep 50%)
MIN_CREDIT   = 10.0  # ₹10 minimum viable credit at entry

# Timing
ENTRY_END = dt_time(14, 30)   # no new entries at or after 14:30
EOD_EXIT  = dt_time(15, 14)   # hard limit — sandbox auto-squaresoff ALL MIS at 15:15
MIN_DTE   = 2

# History needed for MACD warm-up (MACD_SLOW+MACD_SIG bars on 15-min)
HISTORY_DAYS = 5

# State + lockfile
STATE_FILE = LOGS_DIR / "macd_m2_sell_options_state.json"
PID_FILE   = LOGS_DIR / "macd_m2_sell_options_bot.pid"

# Exit poll interval
EXIT_POLL_SEC = 30


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


# ── Lot size helper ───────────────────────────────────────────────────────────

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


# ── Expiry helper ─────────────────────────────────────────────────────────────

def _get_expiry(symbol: str, opt_exchange: str) -> str | None:
    """Return nearest weekly expiry string with DTE ≥ MIN_DTE."""
    dates = get_expiry_dates(API_KEY, symbol, opt_exchange, "options")
    if not dates:
        logger.warning(f"  No expiry dates returned for {symbol}.")
        return None
    today = date.today()
    for d in dates:
        try:
            exp_dt = datetime.strptime(d, "%d%b%y").date()
            dte = (exp_dt - today).days
            if dte >= MIN_DTE:
                logger.info(f"  Expiry selected: {d}  (DTE={dte})  [{symbol}]")
                return d
        except ValueError:
            continue
    logger.warning(f"  No expiry with DTE≥{MIN_DTE} for {symbol}. Available: {dates[:4]}")
    return None


# ── MACD + SR3 signal computation ─────────────────────────────────────────────

def _build_15min_df(raw_bars: list[dict]) -> pd.DataFrame | None:
    """Convert list[dict] from get_history (1-min) into 15-min OHLCV."""
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

    r = df.resample("15min", closed="left", label="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    )
    r = r.between_time("09:15", "15:14").dropna()
    min_bars = MACD_SLOW + MACD_SIG + 5
    if len(r) < min_bars:
        logger.warning(f"  Insufficient 15-min bars: {len(r)} (need ≥{min_bars})")
        return None
    return r


def _compute_signals(df15: pd.DataFrame) -> pd.DataFrame:
    """Add macd_line, bull_m2, bear_m2, sr3_ok, signal columns."""
    ema_fast  = df15["close"].ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow  = df15["close"].ewm(span=MACD_SLOW, adjust=False).mean()
    macd_line = ema_fast - ema_slow

    df15 = df15.copy()
    df15["macd_line"] = macd_line
    df15["bull_m2"]   = (macd_line.shift(1) < 0) & (macd_line >= 0)
    df15["bear_m2"]   = (macd_line.shift(1) > 0) & (macd_line <= 0)

    # SR3: prior day's pivot PP = (H+L+C)/3
    daily = df15.resample("D").agg({"high": "max", "low": "min", "close": "last"})
    pp    = (daily["high"] + daily["low"] + daily["close"]) / 3
    sr    = pp.reindex(df15.index, method="ffill").shift(1)
    df15["sr3_ok"] = (df15["close"] - sr).abs() / sr.clip(lower=1) <= SR3_TOL

    df15["signal"] = "none"
    df15.loc[df15["bull_m2"] & df15["sr3_ok"], "signal"] = "bull"
    df15.loc[df15["bear_m2"] & df15["sr3_ok"], "signal"] = "bear"
    return df15


# ── Position dataclass ────────────────────────────────────────────────────────

class Position:
    __slots__ = (
        "symbol", "direction", "opt_symbol", "opt_type",
        "entry_time", "credit", "sl_level", "tgt_level",
        "lot_size", "n_lots", "quantity", "order_id",
    )

    def __init__(
        self,
        symbol: str, direction: str, opt_symbol: str, opt_type: str,
        entry_time: datetime, credit: float, lot_size: int, n_lots: int,
        order_id: str,
    ):
        self.symbol     = symbol
        self.direction  = direction
        self.opt_symbol = opt_symbol
        self.opt_type   = opt_type
        self.entry_time = entry_time
        self.credit     = credit
        self.sl_level   = round(credit * SL_MULT, 2)
        self.tgt_level  = round(credit * (1 - TGT_KEEP_PCT), 2)
        self.lot_size   = lot_size
        self.n_lots     = n_lots
        self.quantity   = lot_size * n_lots
        self.order_id   = order_id


# ── Main bot class ────────────────────────────────────────────────────────────

class MacdM2SellBot:

    def __init__(self):
        self.client = api.API(api_key=API_KEY, host=HOST)

        self.positions: dict[str, Position | None] = {
            inst["symbol"]: None for inst in INSTRUMENTS
        }
        self.lot_sizes: dict[str, int] = {}

        # last processed 15-min bar timestamp per instrument (avoid double-fire)
        self._last_bar: dict[str, pd.Timestamp | None] = {
            inst["symbol"]: None for inst in INSTRUMENTS
        }
        # live index LTP from WebSocket
        self._ltp: dict[str, float] = {inst["symbol"]: 0.0 for inst in INSTRUMENTS}

        # track which bar we already acted on, per instrument
        self._acted_bar: dict[str, str] = {}   # symbol → bar_ts isoformat

        self._subscribed: set[str] = set()

        logger.info(
            f"📐 {STRATEGY_NAME} | MACD({MACD_FAST},{MACD_SLOW},{MACD_SIG}) "
            f"SR3±{SR3_TOL*100:.1f}% | SL={SL_MULT}× TGT=keep{int(TGT_KEEP_PCT*100)}% | "
            f"EOD {EOD_EXIT} | {N_LOTS} lots/instrument"
        )

    # ── Startup ───────────────────────────────────────────────────────────────

    def _startup_checks(self) -> bool:
        today = date.today()
        if is_market_holiday(API_KEY, today.isoformat(), exchange="NSE"):
            logger.info("🏖️  Market holiday today. Bot will idle and exit cleanly.")
            return False
        for inst in INSTRUMENTS:
            sym = inst["symbol"]
            ls  = _get_lot_size(sym, inst["opt_exchange"], inst["default_lot_size"])
            self.lot_sizes[sym] = ls
            logger.info(f"  {sym}: lot_size={ls}  N_LOTS={N_LOTS}  qty={ls*N_LOTS}")
        return True

    # ── Order helpers ─────────────────────────────────────────────────────────

    def _place_sell(self, opt_symbol: str, opt_exchange: str, quantity: int) -> dict:
        return self.client.placeorder(
            strategy   = STRATEGY_NAME,
            symbol     = opt_symbol,
            action     = "SELL",
            exchange   = opt_exchange,
            price_type = "MARKET",
            product    = "MIS",
            quantity   = str(quantity),
        )

    def _place_buy(self, opt_symbol: str, opt_exchange: str, quantity: int) -> dict:
        # Always placeorder (not placesmartorder) for exits — avoids multi-bot position wipeout
        return self.client.placeorder(
            strategy   = STRATEGY_NAME,
            symbol     = opt_symbol,
            action     = "BUY",
            exchange   = opt_exchange,
            price_type = "MARKET",
            product    = "MIS",
            quantity   = str(quantity),
        )

    # ── Entry ─────────────────────────────────────────────────────────────────

    async def _try_entry(self, symbol: str, direction: str, bar_ts: pd.Timestamp) -> None:
        inst        = next(i for i in INSTRUMENTS if i["symbol"] == symbol)
        opt_type    = "PE" if direction == "bull" else "CE"
        opt_exchange = inst["opt_exchange"]

        bar_key = bar_ts.isoformat()
        if self._acted_bar.get(symbol) == bar_key:
            return
        self._acted_bar[symbol] = bar_key

        logger.info(
            f"  ▶ [{symbol}] {direction.upper()} M2+SR3 — Sell ATM {opt_type}  bar={bar_ts}"
        )

        expiry = _get_expiry(symbol, opt_exchange)
        if not expiry:
            logger.warning(f"  [{symbol}] No valid expiry — skipping entry.")
            return

        opt_sym = get_option_symbol(
            API_KEY, symbol, opt_exchange, expiry, opt_type, offset="ATM",
            underlying_ltp=self._ltp[symbol],
        )
        if not opt_sym:
            logger.warning(f"  [{symbol}] Could not resolve ATM {opt_type} — skipping.")
            return

        ltp = get_option_ltp(opt_sym, opt_exchange, API_KEY)
        if ltp <= 0:
            logger.warning(f"  [{symbol}] {opt_sym} LTP=0 — skipping.")
            return
        if ltp < MIN_CREDIT:
            logger.warning(f"  [{symbol}] {opt_sym} LTP={ltp:.2f} < ₹{MIN_CREDIT} min — skipping.")
            return

        lot_size = self.lot_sizes.get(symbol, inst["default_lot_size"])
        qty      = lot_size * N_LOTS

        res = self._place_sell(opt_sym, opt_exchange, qty)
        if res.get("status") != "success":
            logger.error(f"  [{symbol}] SELL order FAILED: {res}")
            await send_async(
                f"❌ {BOT_NAME} [{symbol}] SELL {opt_sym} FAILED: {res.get('message','?')}"
            )
            return

        order_id = res.get("orderid", "")
        credit   = round(ltp, 2)
        pos = Position(
            symbol=symbol, direction=direction, opt_symbol=opt_sym, opt_type=opt_type,
            entry_time=datetime.now(), credit=credit, lot_size=lot_size,
            n_lots=N_LOTS, order_id=order_id,
        )
        self.positions[symbol] = pos

        msg = (
            f"📉 {BOT_NAME} SOLD {opt_sym}\n"
            f"  [{symbol}] {direction.upper()} M2+SR3\n"
            f"  Credit: ₹{credit:.2f}  Qty: {qty}\n"
            f"  SL: ₹{pos.sl_level:.2f}  Target: ₹{pos.tgt_level:.2f}\n"
            f"  Expiry: {expiry}"
        )
        logger.info(msg)
        await send_async(msg)

    # ── Exit ──────────────────────────────────────────────────────────────────

    async def _close_position(self, symbol: str, reason: str, exit_premium: float) -> None:
        pos = self.positions[symbol]
        if pos is None:
            return

        inst         = next(i for i in INSTRUMENTS if i["symbol"] == symbol)
        opt_exchange = inst["opt_exchange"]

        res = self._place_buy(pos.opt_symbol, opt_exchange, pos.quantity)
        if res.get("status") != "success":
            logger.error(
                f"  [{symbol}] EXIT order FAILED: {res}. "
                "Clearing from state — sandbox will square off at 15:15."
            )

        pnl_per_unit = pos.credit - exit_premium
        gross_pnl    = pnl_per_unit * pos.quantity - 50.0   # ₹50 brokerage/cost

        msg = (
            f"{'✅' if gross_pnl > 0 else '❌'} {BOT_NAME} CLOSED {pos.opt_symbol}\n"
            f"  [{symbol}] {reason}\n"
            f"  Credit: ₹{pos.credit:.2f}  Exit: ₹{exit_premium:.2f}\n"
            f"  P&L: ₹{gross_pnl:,.0f}  Qty: {pos.quantity}"
        )
        logger.warning(msg)
        await send_async(msg)

        log_trade_to_db(
            bot_name      = BOT_NAME,
            instrument    = symbol,
            option_symbol = pos.opt_symbol,
            option_type   = pos.opt_type,
            entry_time    = pos.entry_time,
            exit_time     = datetime.now(),
            entry_premium = pos.credit,
            exit_premium  = round(exit_premium, 2),
            exit_reason   = reason,
            quantity      = pos.quantity,
            lots          = pos.n_lots,
            lot_size      = pos.lot_size,
            gross_pnl     = round(gross_pnl, 2),
            order_id      = pos.order_id,
            strategy_type = "options",
            direction     = "sell",
        )
        self.positions[symbol] = None

    # ── Exit monitor loop ─────────────────────────────────────────────────────

    async def _exit_monitor_loop(self) -> None:
        while True:
            await asyncio.sleep(EXIT_POLL_SEC)
            now_t = datetime.now().time()

            for inst in INSTRUMENTS:
                sym = inst["symbol"]
                pos = self.positions[sym]
                if pos is None:
                    continue

                if now_t >= EOD_EXIT:
                    ltp = get_option_ltp(pos.opt_symbol, inst["opt_exchange"], API_KEY) or pos.credit
                    await self._close_position(sym, "EOD", ltp)
                    continue

                ltp = get_option_ltp(pos.opt_symbol, inst["opt_exchange"], API_KEY)
                if ltp <= 0:
                    continue

                if ltp >= pos.sl_level:
                    logger.warning(
                        f"  [{sym}] SL: {pos.opt_symbol} LTP={ltp:.2f} ≥ SL={pos.sl_level:.2f}"
                    )
                    await self._close_position(sym, "SL", ltp)
                elif ltp <= pos.tgt_level:
                    logger.info(
                        f"  [{sym}] Target: {pos.opt_symbol} LTP={ltp:.2f} ≤ TGT={pos.tgt_level:.2f}"
                    )
                    await self._close_position(sym, "Target", ltp)

    # ── EOD hard-exit guard ───────────────────────────────────────────────────

    async def _eod_guard_loop(self) -> None:
        """Safety net: unconditional close sweep from 15:05 every 30s."""
        while True:
            await asyncio.sleep(30)
            now_t = datetime.now().time()
            if now_t < dt_time(15, 5):
                continue
            for inst in INSTRUMENTS:
                sym = inst["symbol"]
                if self.positions[sym] is not None and now_t >= EOD_EXIT:
                    logger.warning(f"  [{sym}] EOD guard — forcing close.")
                    ltp = (
                        get_option_ltp(
                            self.positions[sym].opt_symbol,
                            inst["opt_exchange"],
                            API_KEY,
                        )
                        or self.positions[sym].credit
                    )
                    await self._close_position(sym, "EOD_GUARD", ltp)

    # ── 15-min bar boundary detection ─────────────────────────────────────────

    def _on_15min_boundary(self, symbol: str, tick_ts: datetime) -> pd.Timestamp | None:
        """Return closed bar timestamp when we cross a 15-min boundary, else None."""
        floored = tick_ts.replace(
            minute=(tick_ts.minute // 15) * 15, second=0, microsecond=0
        )
        bar_ts = pd.Timestamp(floored)
        prev   = self._last_bar.get(symbol)
        if prev is None:
            self._last_bar[symbol] = bar_ts
            return None                       # cold start — no closed bar yet
        if bar_ts > prev:
            self._last_bar[symbol] = bar_ts
            return prev                       # return the bar that just CLOSED
        return None

    # ── State dump loop ───────────────────────────────────────────────────────

    async def _state_dump_loop(self) -> None:
        while True:
            try:
                pos_info = {}
                for sym, pos in self.positions.items():
                    if pos:
                        pos_info[sym] = {
                            "opt_symbol": pos.opt_symbol,
                            "direction":  pos.direction,
                            "opt_type":   pos.opt_type,
                            "credit":     pos.credit,
                            "sl_level":   pos.sl_level,
                            "tgt_level":  pos.tgt_level,
                            "entry_time": pos.entry_time.isoformat(),
                            "quantity":   pos.quantity,
                        }
                    else:
                        pos_info[sym] = None

                STATE_FILE.write_text(json.dumps({
                    "last_update": datetime.now().isoformat(),
                    "strategy":    STRATEGY_NAME,
                    "positions":   pos_info,
                    "ltp":         self._ltp,
                }, default=str))
            except Exception:
                pass
            await asyncio.sleep(3)

    # ── WebSocket loop ────────────────────────────────────────────────────────

    async def _ws_loop(self) -> None:
        retry_delay = 5
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=20) as ws:
                    self.ws = ws
                    self._subscribed.clear()

                    # 1. Authenticate first — mandatory before any subscribe
                    await ws.send(json.dumps({
                        "action":  "authenticate",
                        "api_key": API_KEY,
                    }))

                    # 2. Subscribe to NIFTY and BANKNIFTY spot ticks
                    for inst in INSTRUMENTS:
                        await ws.send(json.dumps({
                            "action":   "subscribe",
                            "symbol":   inst["symbol"],
                            "exchange": inst["exchange"],
                            "mode":     1,   # LTP only
                        }))
                        self._subscribed.add(inst["symbol"])
                        logger.info(f"  Subscribed: {inst['symbol']} ({inst['exchange']})")

                    logger.info(f"🔗 WebSocket connected: {WS_URL}")
                    retry_delay = 5

                    # 3. Tick loop
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        sym = msg.get("symbol") or msg.get("s")
                        ltp = msg.get("ltp") or msg.get("l")
                        if not sym or not ltp or sym not in self._ltp:
                            continue

                        self._ltp[sym] = float(ltp)

                        ts_raw = msg.get("timestamp") or msg.get("ts")
                        try:
                            tick_ts = datetime.fromisoformat(ts_raw) if ts_raw else datetime.now()
                        except (ValueError, TypeError):
                            tick_ts = datetime.now()

                        now_t = tick_ts.time()
                        if now_t < dt_time(9, 15) or now_t >= EOD_EXIT:
                            continue

                        closed_bar = self._on_15min_boundary(sym, tick_ts)
                        if closed_bar is not None:
                            logger.debug(f"  [{sym}] Bar closed: {closed_bar} — scanning")
                            asyncio.create_task(self._scan_instrument(sym, closed_bar))

            except websockets.ConnectionClosed as e:
                logger.warning(f"⚠️ WS disconnected ({e}). Retry in {retry_delay}s…")
                self._subscribed.clear()
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
            except Exception as e:
                logger.error(f"❌ WS error: {e}. Retry in {retry_delay}s…")
                self._subscribed.clear()
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    # ── Signal scan ───────────────────────────────────────────────────────────

    async def _scan_instrument(self, symbol: str, bar_ts: pd.Timestamp) -> None:
        """Called on each 15-min bar close. Fetches history, checks MACD+SR3."""
        if bar_ts.time() >= ENTRY_END:
            return
        if self.positions[symbol] is not None:
            return

        inst = next(i for i in INSTRUMENTS if i["symbol"] == symbol)
        raw  = get_history(API_KEY, symbol, inst["exchange"], "1m", HISTORY_DAYS)
        df15 = _build_15min_df(raw)
        if df15 is None:
            logger.warning(f"  [{symbol}] Cannot build 15-min DF — skipping bar {bar_ts}.")
            return

        df15 = _compute_signals(df15)

        if bar_ts not in df15.index:
            return
        sig = df15.loc[bar_ts, "signal"]
        if sig in ("bull", "bear"):
            await self._try_entry(symbol, sig, bar_ts)

    # ── Main run ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        _acquire_pid_lock()
        logger.info(f"🚀 {STRATEGY_NAME} starting — {date.today()}")
        if not self._startup_checks():
            return

        await send_async(
            f"🚀 {BOT_NAME} started\n"
            f"  NIFTY + BANKNIFTY | MACD({MACD_FAST},{MACD_SLOW},{MACD_SIG}) M2+SR3\n"
            f"  Bull→Sell PE | Bear→Sell CE\n"
            f"  SL={SL_MULT}×  Target=keep{int(TGT_KEEP_PCT*100)}%  "
            f"{N_LOTS} lots each  EOD {EOD_EXIT}"
        )

        await asyncio.gather(
            self._ws_loop(),
            self._exit_monitor_loop(),
            self._eod_guard_loop(),
            self._state_dump_loop(),
        )


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    bot = MacdM2SellBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("⛔ Bot stopped (KeyboardInterrupt).")


if __name__ == "__main__":
    main()
