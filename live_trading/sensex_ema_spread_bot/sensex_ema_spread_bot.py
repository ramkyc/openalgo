#!/usr/bin/env python3
"""
SENSEX EMA Spread Bot
=====================
live_trading/sensex_ema_spread_bot/sensex_ema_spread_bot.py

Identical strategy to nifty_ema_spread_bot — EMA(5,13) crossover on 15-min
SENSEX INDEX bars → 100-point ATM debit spread on SENSEX weekly options.

Key differences from NIFTY bot:
  - IDX_EXCHANGE  = "BSE_INDEX"   (not NSE_INDEX)
  - OPT_EXCHANGE  = "BFO"         (BSE F&O, not NFO)
  - STRIKE_STEP   = 100
  - SPREAD_WIDTH  = 100
  - DEFAULT_LOT_SIZE = 10

Research (Stage 9 Multi-Instrument OOS 2024-07 → 2026-06):
    SENSEX: Sharpe 2.90 | WR 58.1% | 353 trades | ₹1.9L net P&L

Study: research/index_spread_study/results_summary.md
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, date, time as dt_time
from pathlib import Path

import pandas as pd
import requests
import websockets
from dotenv import load_dotenv
from openalgo import api

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.shared import ta_compat as ta
from live_trading.api_utils               import get_expiry_dates, get_history
from live_trading.shared.atm_resolver     import get_atm_strike, get_weekly_expiry
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger     import log_trade_to_db
from live_trading.shared.order_fill       import fetch_fill_price
from live_trading.api_utils               import HOST
from live_trading.shared.decision_logger  import DecisionLogger
from live_trading.shared.tick_watchdog    import TickWatchdog

LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "sensex_ema_spread_bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("OPENALGO_API_KEY")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:8765")

if not API_KEY:
    logger.error("OPENALGO_API_KEY not found in .env — exiting.")
    sys.exit(1)

# 2026-07-06: NIFTY/BANKNIFTY/SENSEX EMA spread bots all went silent mid-session
# with zero traceback — neither an unhandled asyncio task exception nor a signal
# was ever logged, so the actual cause couldn't be reconstructed after the fact.
# These two handlers exist purely so a repeat leaves evidence.
def _log_unhandled_exception(loop, context):
    exc = context.get("exception")
    logger.error(f"Unhandled asyncio exception: {context.get('message')}", exc_info=exc)


def _log_signal(signum, frame):
    logger.warning(f"Received signal {signum} — process terminating.")
    sys.exit(0)


signal.signal(signal.SIGTERM, _log_signal)
signal.signal(signal.SIGINT, _log_signal)

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

STRATEGY_NAME = "SENSEX_EMA_SPREAD"

IDX_SYMBOL    = "SENSEX"
IDX_EXCHANGE  = "BSE_INDEX"    # ← BSE index feed
OPT_EXCHANGE  = "BFO"          # ← BSE F&O options exchange

STRIKE_STEP   = 100
SPREAD_WIDTH  = 100

EMA_FAST      = 5
EMA_SLOW      = 13
MIN_BARS      = 30

N_LOTS           = 10
DEFAULT_LOT_SIZE = 10          # SENSEX lot size

MIN_DTE         = 1
LAST_ENTRY_TIME = dt_time(14, 0)
MARKET_OPEN     = dt_time(9, 15)
SESSION_END     = dt_time(15, 30)

TARGET_MULT     = 1.5
SL_MULT         = 0.05
MIN_ENTRY_DEBIT = 10.0

ORDER_DELAY = 1.5

STATE_FILE  = LOGS_DIR / "sensex_ema_spread_state.json"
TRADES_CSV  = LOGS_DIR / "sensex_ema_spread_trades.csv"

# Decision-state logging (jsonl + throttled heartbeat — see shared/decision_logger.py)
DECISION_LOG   = LOGS_DIR / "sensex_ema_spread_decisions.jsonl"
HEARTBEAT_SECS = 300


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _multiquote(items: list[tuple[str, str]],
                retries: int = 3, delay: float = 3.0) -> dict[str, float]:
    pending = list(dict.fromkeys(items))
    out: dict[str, float] = {}
    for attempt in range(1, retries + 1):
        if not pending:
            break
        payload = {"apikey": API_KEY,
                   "symbols": [{"symbol": s, "exchange": e} for s, e in pending]}
        try:
            res = requests.post(f"{HOST}/api/v1/multiquotes", json=payload, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data.get("status") == "success":
                    for row in data.get("results", []):
                        sym = row.get("symbol")
                        qd  = row.get("data") or {}
                        ltp = (qd.get("ltp") or qd.get("last_price")
                               or qd.get("close") or qd.get("c") or 0)
                        try:
                            price = float(ltp)
                        except (TypeError, ValueError):
                            price = 0.0
                        if sym and price > 0:
                            out[sym] = price
        except Exception as e:
            logger.error(f"multiquotes error: {e}")
        pending = [(s, e) for s, e in pending if s not in out]
        if pending and attempt < retries:
            time.sleep(delay)
    if pending:
        logger.warning(f"multiquotes: no LTP for {[s for s, _ in pending]}")
    return out


def _order(client, symbol: str, action: str, qty: int) -> dict:
    try:
        resp = client.placeorder(
            strategy=STRATEGY_NAME,
            symbol=symbol,
            action=action,
            exchange=OPT_EXCHANGE,
            price_type="MARKET",
            product="NRML",
            quantity=qty,
        )
        logger.info(f"  {action} {symbol} x{qty}: {resp}")
        return resp if isinstance(resp, dict) else {"status": "ok"}
    except Exception as e:
        logger.error(f"  placeorder failed ({action} {symbol}): {e}")
        return {"status": "error", "message": str(e)}


def _get_lot_size() -> int:
    try:
        from database.token_db import get_symbol_info
        si = get_symbol_info("SENSEX", OPT_EXCHANGE)
        if si and getattr(si, "lotsize", None):
            return int(si.lotsize)
    except Exception as e:
        logger.warning(f"  Lot size DB lookup failed: {e}. Using {DEFAULT_LOT_SIZE}.")
    return DEFAULT_LOT_SIZE


def _otm_symbol(atm_strike: int, opt_type: str, expiry_str: str,
                direction: str) -> str:
    otm = atm_strike + SPREAD_WIDTH if direction == "BULL" else atm_strike - SPREAD_WIDTH
    return f"{IDX_SYMBOL}{expiry_str}{otm}{opt_type}"


def _atm_symbol(atm_strike: int, opt_type: str, expiry_str: str) -> str:
    return f"{IDX_SYMBOL}{expiry_str}{atm_strike}{opt_type}"


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_state(state: dict) -> None:
    try:
        state["updated"] = datetime.now().isoformat()
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        logger.error(f"Failed to save state: {e}")


def _log_trade_csv(row: dict) -> None:
    write_header = not TRADES_CSV.exists()
    with open(TRADES_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            w.writeheader()
        w.writerow(row)


def _dte_ok(expiry_str: str) -> bool:
    try:
        return (datetime.strptime(expiry_str, "%d%b%y").date() - date.today()).days >= MIN_DTE
    except ValueError:
        return False


def _resolve_fill(resp: dict, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id:
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback


# ══════════════════════════════════════════════════════════════════════════════
class SensexEmaSpreadBot:
# ══════════════════════════════════════════════════════════════════════════════

    def __init__(self):
        self.client   = api(api_key=API_KEY, host=HOST)
        self.lot_size = _get_lot_size()
        self.qty      = N_LOTS * self.lot_size

        self._bar_open = self._bar_high = self._bar_low = self._bar_close = None
        self._bar_ts   = None
        self._closes: list[float] = []
        self._ema_fast: float | None = None
        self._ema_slow: float | None = None
        self._signal   = 0
        self._last_ltp: float = 0.0
        self._last_bar_time: str | None = None
        self._pos: dict | None = None

        state = _load_state()
        if state.get("position"):
            self._pos    = state["position"]
            self._signal = state.get("signal", 0)
            logger.info(f"  Restored position: {self._pos}")

        # Decision-state logging (jsonl + throttled heartbeat)
        self._dlog = DecisionLogger(DECISION_LOG, heartbeat_secs=HEARTBEAT_SECS, bot_logger=logger)

        # Dead-feed watchdog: alerts if SENSEX ticks go quiet for
        # DEAD_FEED_SECS during market hours (Task #14 — a hung-but-not-
        # erroring socket would otherwise never trigger the reconnect-on-
        # exception loop). Only IDX_SYMBOL is tracked -- the spread legs are
        # priced via REST (_multiquote), never over WS, so tracking them
        # would be a permanent false dead-feed alarm.
        self._watchdog = TickWatchdog(
            bot_name="SENSEX EMA Spread Bot",
            tracked_symbols=lambda: [IDX_SYMBOL],
            market_open=MARKET_OPEN,
            market_close=SESSION_END,
            bot_logger=logger,
        )

    def _bar_bucket(self, ts: datetime) -> datetime:
        m = (ts.minute // 15) * 15
        return ts.replace(minute=m, second=0, microsecond=0)

    def _on_tick(self, ltp: float, ts: datetime) -> bool:
        self._last_ltp = ltp
        bucket = self._bar_bucket(ts)
        if self._bar_ts is None:
            self._bar_ts = bucket
            self._bar_open = self._bar_high = self._bar_low = self._bar_close = ltp
            return False
        if bucket == self._bar_ts:
            self._bar_high  = max(self._bar_high, ltp)
            self._bar_low   = min(self._bar_low, ltp)
            self._bar_close = ltp
            return False
        self._closes.append(self._bar_close)
        self._last_bar_time = self._bar_ts.strftime("%H:%M")
        self._bar_ts = bucket
        self._bar_open = self._bar_high = self._bar_low = self._bar_close = ltp
        return True

    def _update_ema(self) -> int:
        if len(self._closes) < EMA_SLOW:
            return 0
        ser = pd.Series(self._closes, dtype=float)
        ef  = float(ser.ewm(span=EMA_FAST, adjust=False).mean().iloc[-1])
        es  = float(ser.ewm(span=EMA_SLOW, adjust=False).mean().iloc[-1])
        self._ema_fast = ef
        self._ema_slow = es
        return 1 if ef > es else -1

    def _persist_full_state(self) -> None:
        """Write position + signal + live EMA/spot snapshot for the dashboard."""
        _save_state({
            "position": self._pos,
            "signal":   self._signal,
            "ema_state": {
                "ema5":          round(self._ema_fast, 2) if self._ema_fast is not None else None,
                "ema13":         round(self._ema_slow, 2) if self._ema_slow is not None else None,
                "last_bar_time": self._last_bar_time,
                "spot":          self._last_ltp,
                "bars_loaded":   len(self._closes),
            },
        })

    def _is_crossover(self, new_state: int) -> bool:
        return new_state != 0 and new_state != self._signal

    def _spread_value(self) -> float | None:
        if self._pos is None:
            return None
        quotes = _multiquote([(self._pos["long_sym"],  OPT_EXCHANGE),
                              (self._pos["short_sym"], OPT_EXCHANGE)])
        lv = quotes.get(self._pos["long_sym"])
        sv = quotes.get(self._pos["short_sym"])
        return max(0.0, lv - sv) if lv is not None and sv is not None else None

    async def _enter(self, direction: int, spot: float) -> bool:
        if datetime.now().time() >= LAST_ENTRY_TIME:
            return False
        opt_type   = "CE" if direction == 1 else "PE"
        atm_strike = get_atm_strike(spot, IDX_SYMBOL)
        expiry_str = get_weekly_expiry(API_KEY, MIN_DTE, IDX_SYMBOL)
        if not expiry_str or not _dte_ok(expiry_str):
            logger.warning("  No suitable SENSEX expiry. Skipping.")
            return False

        dir_label = "BULL" if direction == 1 else "BEAR"
        long_sym  = _atm_symbol(atm_strike, opt_type, expiry_str)
        short_sym = _otm_symbol(atm_strike, opt_type, expiry_str, dir_label)

        quotes = _multiquote([(long_sym, OPT_EXCHANGE), (short_sym, OPT_EXCHANGE)])
        long_ltp  = quotes.get(long_sym,  0.0)
        short_ltp = quotes.get(short_sym, 0.0)
        if long_ltp <= 0 or short_ltp <= 0:
            logger.warning(f"  Bad quotes: {long_sym}={long_ltp}, {short_sym}={short_ltp}")
            return False

        entry_debit = long_ltp - short_ltp
        if entry_debit < MIN_ENTRY_DEBIT:
            logger.warning(f"  Debit ₹{entry_debit:.1f} < min ₹{MIN_ENTRY_DEBIT}. Skipping.")
            return False

        r1 = _order(self.client, long_sym,  "BUY",  self.qty)
        await asyncio.sleep(ORDER_DELAY)
        r2 = _order(self.client, short_sym, "SELL", self.qty)

        # Book P&L against actual fills, not the pre-order LTP snapshot —
        # sequential leg orders drift apart by the time both are filled.
        long_fill  = _resolve_fill(r1, long_ltp)
        short_fill = _resolve_fill(r2, short_ltp)
        entry_debit = round(long_fill - short_fill, 2)
        logger.info(f"  Fill prices: long={long_fill:.2f} short={short_fill:.2f} "
                    f"debit(fill)={entry_debit:.2f} vs debit(ltp)={(long_ltp - short_ltp):.2f}")

        self._pos = {
            "direction": dir_label, "long_sym": long_sym, "short_sym": short_sym,
            "atm_strike": atm_strike, "opt_type": opt_type, "expiry": expiry_str,
            "entry_debit": entry_debit, "entry_time": datetime.now().isoformat(),
            "qty": self.qty,
        }
        self._signal = direction
        _save_state({"position": self._pos, "signal": self._signal})

        await send_async(
            f"📈 SENSEX EMA Spread ENTRY\n"
            f"Direction: {dir_label}\n"
            f"Long:  {long_sym} @ ₹{long_ltp:.1f}\n"
            f"Short: {short_sym} @ ₹{short_ltp:.1f}\n"
            f"Debit (R): ₹{entry_debit:.2f}  "
            f"TP: ₹{entry_debit*TARGET_MULT:.2f}  SL: ₹{entry_debit*SL_MULT:.2f}\n"
            f"Expiry: {expiry_str}"
        )
        return True

    async def _exit(self, reason: str, spread_val: float | None = None):
        if self._pos is None:
            return
        pos = self._pos
        if spread_val is None:
            spread_val = self._spread_value() or 0.0

        # Fallback LTPs in case the fill lookup fails post-order
        fallback_quotes = _multiquote([(pos["long_sym"],  OPT_EXCHANGE),
                                       (pos["short_sym"], OPT_EXCHANGE)])
        long_ltp_fb  = fallback_quotes.get(pos["long_sym"],  0.0)
        short_ltp_fb = fallback_quotes.get(pos["short_sym"], 0.0)

        r1 = _order(self.client, pos["long_sym"],  "SELL", pos["qty"])
        await asyncio.sleep(ORDER_DELAY)
        r2 = _order(self.client, pos["short_sym"], "BUY",  pos["qty"])

        # Book P&L against actual fills, not the LTP used to trigger the exit
        long_fill  = _resolve_fill(r1, long_ltp_fb)
        short_fill = _resolve_fill(r2, short_ltp_fb)
        fill_spread_val = round(long_fill - short_fill, 2)
        logger.info(f"  Exit fills: long={long_fill:.2f} short={short_fill:.2f} "
                    f"spread(fill)={fill_spread_val:.2f} vs spread(decision)={spread_val:.2f}")
        spread_val = max(0.0, fill_spread_val)

        R     = pos["entry_debit"]
        gross = (spread_val - R) * pos["qty"]
        pnl_r = (spread_val - R) / R if R > 0 else 0.0

        _log_trade_csv({
            "date": datetime.now().strftime("%Y-%m-%d"),
            "exit_time": datetime.now().isoformat(),
            "direction": pos["direction"], "long_sym": pos["long_sym"],
            "short_sym": pos["short_sym"], "entry_debit": R,
            "exit_value": round(spread_val, 2), "gross_pnl": round(gross, 0),
            "exit_reason": reason,
        })

        log_trade_to_db(
            bot_name      = "sensex_ema_spread_bot",
            instrument    = IDX_SYMBOL,
            option_symbol = pos["long_sym"],
            option_type   = "DEBIT_SPREAD",
            entry_time    = pos["entry_time"],
            exit_time     = datetime.now(),
            entry_premium = R,
            exit_premium  = round(spread_val, 2),
            exit_reason   = reason,
            quantity      = pos["qty"],
            lots          = N_LOTS,
            lot_size      = self.lot_size,
            gross_pnl     = round(gross, 2),
            notes         = f"long={pos['long_sym']} short={pos['short_sym']} direction={pos['direction']}",
        )

        await send_async(
            f"{'✅' if gross >= 0 else '❌'} SENSEX EMA Spread EXIT\n"
            f"Reason: {reason}  P&L: {pnl_r:+.2f}R  Gross: ₹{gross:+,.0f}"
        )
        logger.info(f"  EXIT [{reason}]: spread=₹{spread_val:.2f}  gross=₹{gross:+,.0f}")
        self._pos = None
        _save_state({"position": None, "signal": self._signal})

    # ══════════════════════════════════════════════════════════════════════
    #  DECISION-STATE LOGGING
    # ══════════════════════════════════════════════════════════════════════

    def _verdict(self, now_t: dt_time) -> str:
        """What's currently blocking entry — checked in the order these gates
        actually apply in run()/_on_bar_close(), so it always names the real
        blocker."""
        if now_t < MARKET_OPEN:
            return "waiting for session start"
        if now_t >= SESSION_END:
            return "session ended"
        if self._pos is not None:
            return (f"ACTIVE: holding {self._pos['direction']} spread "
                    f"({self._pos['long_sym']}/{self._pos['short_sym']})")
        if len(self._closes) < EMA_SLOW:
            return f"warming up: {len(self._closes)}/{EMA_SLOW} bars for EMA({EMA_FAST},{EMA_SLOW})"
        if now_t >= LAST_ENTRY_TIME:
            return f"BLOCKED: past last entry time ({LAST_ENTRY_TIME.strftime('%H:%M')})"
        if len(self._closes) < MIN_BARS:
            return f"warming up: {len(self._closes)}/{MIN_BARS} bars for entry eligibility"
        return f"🔥 watching for EMA({EMA_FAST},{EMA_SLOW}) crossover (state={self._signal})"

    def _heartbeat_text(self) -> str:
        now_t = datetime.now().time()
        lines = [f"💓 DECISION STATE {datetime.now().strftime('%H:%M:%S')} ─ {self._verdict(now_t)}"]
        ema_txt = (f"EMA{EMA_FAST}={self._ema_fast:.1f} EMA{EMA_SLOW}={self._ema_slow:.1f}"
                   if self._ema_fast is not None and self._ema_slow is not None
                   else "EMA=warming up")
        lines.append(
            f"    {IDX_SYMBOL}={self._last_ltp:.1f}  bars={len(self._closes)}  "
            f"{ema_txt}  signal={self._signal}"
        )
        if self._pos:
            lines.append(
                f"    active: {self._pos['direction']} long={self._pos['long_sym']} "
                f"short={self._pos['short_sym']} debit={self._pos['entry_debit']}"
            )
        return "\n".join(lines)

    async def _on_bar_close(self, close: float):
        new_state = self._update_ema()
        is_xover  = self._is_crossover(new_state)

        # Logged unconditionally, before any gate checks below, so the log
        # always has real values explaining "why not" — not just "why yes".
        self._dlog.log_bar({
            "phase":        "ACTIVE" if self._pos is not None else "WATCHING",
            "bar_close":    close,
            "bar_time":     self._last_bar_time,
            "ema_fast":     round(self._ema_fast, 2) if self._ema_fast is not None else None,
            "ema_slow":     round(self._ema_slow, 2) if self._ema_slow is not None else None,
            "new_state":    new_state,
            "signal":       self._signal,
            "is_crossover": is_xover,
            "bars_loaded":  len(self._closes),
            "position":     self._pos,
            "verdict":      self._verdict(datetime.now().time()),
        })

        if self._pos is not None:
            sv = self._spread_value()
            if sv is not None:
                R = self._pos["entry_debit"]
                if sv >= R * TARGET_MULT:
                    await self._exit("profit_target", sv)
                    is_xover = False
                elif sv <= R * SL_MULT:
                    await self._exit("stop_loss", sv)
                    is_xover = False

        if is_xover and self._pos is not None:
            await self._exit("signal_reversal", self._spread_value())
            await asyncio.sleep(ORDER_DELAY)

        if is_xover and self._pos is None and len(self._closes) >= MIN_BARS:
            await self._enter(new_state, close)

        self._signal = new_state if new_state != 0 else self._signal

    async def _load_history(self) -> list[float]:
        try:
            raw = get_history(API_KEY, IDX_SYMBOL, IDX_EXCHANGE,
                              interval="1m", duration_days=2)
            # get_history returns a list of candle dicts, not a DataFrame
            if not raw:
                return []
            hist = pd.DataFrame(raw)
            if "timestamp" in hist.columns:
                idx = (pd.to_datetime(hist["timestamp"], unit="s", utc=True)
                       .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None))
            elif "date" in hist.columns:
                idx = pd.to_datetime(hist["date"])
            else:
                return []
            hist = hist.set_index(idx).sort_index().between_time("09:15", "15:30")
            closes = hist["close"].astype(float).resample("15min").last().dropna().tolist()
            logger.info(f"  Pre-loaded {len(closes)} × 15-min bars")
            return closes
        except Exception as e:
            logger.warning(f"  History pre-load failed: {e}")
            return []

    async def _state_writer(self) -> None:
        """Write state file every 3s; also log a liveness heartbeat every ~5 min
        so a silently-dead process is visible as a gap instead of indistinguishable
        from normal no-bar-close quiet."""
        tick = 0
        while True:
            self._persist_full_state()
            tick += 1
            if tick % 100 == 0:
                logger.info(f"  heartbeat: alive, pos={'yes' if self._pos else 'no'}, signal={self._signal}")
            await asyncio.sleep(3)

    async def run(self):
        asyncio.get_running_loop().set_exception_handler(_log_unhandled_exception)
        logger.info(f"🚀 {STRATEGY_NAME} starting")
        await send_async(f"🚀 {STRATEGY_NAME} started — EMA({EMA_FAST},{EMA_SLOW}) 15m spread")
        self._closes = await self._load_history()
        if len(self._closes) >= EMA_SLOW:
            self._update_ema()
        asyncio.create_task(self._state_writer())
        asyncio.create_task(self._watchdog.watch_loop())

        while True:
            try:
                async with websockets.connect(WS_URL) as ws:
                    # Proxy requires auth before it accepts subscriptions
                    await ws.send(json.dumps({
                        "action":  "authenticate",
                        "api_key": API_KEY,
                    }))
                    await ws.send(json.dumps({
                        "action":   "subscribe",
                        "symbol":   IDX_SYMBOL,
                        "exchange": IDX_EXCHANGE,
                        "mode":     2,
                    }))
                    async for raw_msg in ws:
                        now = datetime.now()
                        if now.time() > SESSION_END:
                            break
                        if now.time() < MARKET_OPEN:
                            continue
                        try:
                            msg = json.loads(raw_msg)
                        except json.JSONDecodeError:
                            continue
                        # Ticks arrive wrapped: {"type": "market_data", "data": {...}}
                        if msg.get("type") == "market_data":
                            self._watchdog.on_tick(IDX_SYMBOL)
                            msg = msg.get("data") or {}
                        ltp = (msg.get("ltp") or msg.get("last_price")
                               or msg.get("close") or msg.get("c"))
                        if ltp is None:
                            continue
                        try:
                            ltp = float(ltp)
                        except (TypeError, ValueError):
                            continue
                        if ltp <= 0:
                            continue
                        # Throttled to HEARTBEAT_SECS internally — cheap to call on every tick
                        self._dlog.maybe_heartbeat(self._heartbeat_text)
                        if self._on_tick(ltp, now) and self._closes:
                            await self._on_bar_close(self._closes[-1])

                # Session-end break lands here with no exception. Without a pause the
                # outer while immediately reconnects, gets a tick, and breaks again —
                # a tight loop hammering the WS server until midnight rolls the date
                # over and now.time() > SESSION_END stops being true.
                if datetime.now().time() > SESSION_END:
                    await asyncio.sleep(60)
            except (websockets.ConnectionClosed, OSError) as e:
                logger.warning(f"  WebSocket disconnected: {e} — reconnecting in 10s")
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"  Unexpected error: {e}", exc_info=True)
                await asyncio.sleep(15)


if __name__ == "__main__":
    bot = SensexEmaSpreadBot()
    asyncio.run(bot.run())
