#!/usr/bin/env python3
"""
NIFTY EMA Spread Bot
====================
live_trading/nifty_ema_spread_bot/nifty_ema_spread_bot.py

Enters a 50-point ATM debit spread on NIFTY weekly options whenever the
EMA(5,13) crossover fires on 15-minute NIFTY INDEX bars.

Signal logic (faithful translation of research/index_spread_study/backtest_oos_v2.py):

    BULL crossover (EMA5 crosses above EMA13):
        → BUY ATM CE  +  SELL (ATM+50) CE   [Bull Call Spread]

    BEAR crossover (EMA5 crosses below EMA13):
        → BUY ATM PE  +  SELL (ATM−50) PE   [Bear Put Spread]

    On signal reversal: close existing spread → immediately open opposite spread.

Exit conditions (checked on each 15-min bar close):
    1. Signal reversal   — primary exit (76% of OOS trades)
    2. Profit target     — spread value ≥ entry_debit × 1.5
    3. Stop-loss         — spread value ≤ entry_debit × 0.05

Parameters (Stage 4 OOS champion, all 10 stages pass):
    EMA fast=5  slow=13
    Spread width: 50 points
    Min DTE: 1 calendar day at entry
    Last entry: 14:00 IST

Research results (OOS 2024-07-01 → 2026-06-24):
    NIFTY: Sharpe 6.14 | WR 57.9% | 428 trades | MaxDD ₹27,014

Product: NRML (positional, holds overnight until signal reversal or expiry)

Shared utilities used:
    live_trading.shared.ta_compat          — EMA (pure pandas, matches pandas_ta)
    live_trading.shared.atm_resolver       — get_atm_strike, get_weekly_expiry
    live_trading.shared.telegram_notifier  — send_async
    live_trading.shared.trade_logger       — log_trade_to_db
    live_trading.api_utils                 — get_expiry_dates, get_history
    database.token_db.get_symbol_info      — dynamic lot size

Trade mode (Analyze / Live) is set in the OpenAlgo UI — not in this bot.
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

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.shared import ta_compat as ta
from live_trading.api_utils               import get_expiry_dates, get_history
from live_trading.shared.atm_resolver     import get_atm_strike, get_weekly_expiry
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger     import log_trade_to_db
from live_trading.shared.order_fill       import fetch_fill_price
from live_trading.api_utils               import HOST

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "nifty_ema_spread_bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
API_KEY = os.getenv("OPENALGO_API_KEY")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")

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

STRATEGY_NAME = "NIFTY_EMA_SPREAD"

IDX_SYMBOL    = "NIFTY"
IDX_EXCHANGE  = "NSE_INDEX"
OPT_EXCHANGE  = "NFO"

STRIKE_STEP   = 50          # NIFTY strike granularity
SPREAD_WIDTH  = 50          # OTM leg offset in points

EMA_FAST      = 5
EMA_SLOW      = 13
MIN_BARS      = 30          # minimum 15-min bars for warm-up before trading

N_LOTS           = 10
DEFAULT_LOT_SIZE = 65       # post-Jan-2026 NIFTY lot size

MIN_DTE         = 1         # calendar days remaining at entry time
LAST_ENTRY_TIME = dt_time(14, 0)
MARKET_OPEN     = dt_time(9, 15)
SESSION_END     = dt_time(15, 30)

TARGET_MULT = 1.5           # exit when spread_value ≥ entry_debit × 1.5
SL_MULT     = 0.05          # exit when spread_value ≤ entry_debit × 0.05
MIN_ENTRY_DEBIT = 5.0       # minimum spread value to accept an entry

POLL_SECS   = 3             # tick aggregation cycle
ORDER_DELAY = 1.5           # seconds between sequential leg orders

STATE_FILE    = LOGS_DIR / "nifty_ema_spread_state.json"
TRADES_CSV    = LOGS_DIR / "nifty_ema_spread_trades.csv"


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _multiquote(items: list[tuple[str, str]],
                retries: int = 3, delay: float = 3.0) -> dict[str, float]:
    """Batch-fetch LTPs for (symbol, exchange) pairs via /api/v1/multiquotes."""
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
    """Fire a NRML market order — mode (paper/live) is set in OpenAlgo UI."""
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
        # use a known NIFTY CE symbol to fetch lot size
        si = get_symbol_info("NIFTY", OPT_EXCHANGE)
        if si and getattr(si, "lotsize", None):
            ls = int(si.lotsize)
            logger.info(f"  Lot size from DB: {ls}")
            return ls
    except Exception as e:
        logger.warning(f"  Lot size DB lookup failed: {e}. Using {DEFAULT_LOT_SIZE}.")
    return DEFAULT_LOT_SIZE


def _otm_symbol(atm_strike: int, opt_type: str, expiry_str: str,
                direction: str) -> str:
    """
    Construct the OTM leg canonical symbol string.
    NIFTY: Bull call → ATM+50 CE; Bear put → ATM-50 PE
    Canonical format: f"{underlying}{expiry_str}{strike}{opt_type}"
    e.g. "NIFTY03JUL2524150CE"
    """
    if direction == "BULL":
        otm = atm_strike + SPREAD_WIDTH
    else:
        otm = atm_strike - SPREAD_WIDTH
    return f"{IDX_SYMBOL}{expiry_str}{otm}{opt_type}"


def _atm_symbol(atm_strike: int, opt_type: str, expiry_str: str) -> str:
    return f"{IDX_SYMBOL}{expiry_str}{atm_strike}{opt_type}"


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
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
    """Return True if expiry_str (DDMMMYY) is at least MIN_DTE calendar days away."""
    try:
        exp_date = datetime.strptime(expiry_str, "%d%b%y").date()
        return (exp_date - date.today()).days >= MIN_DTE
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
class NiftyEmaSpreadBot:
# ══════════════════════════════════════════════════════════════════════════════

    def __init__(self):
        self.client   = api(api_key=API_KEY, host=HOST)
        self.lot_size = _get_lot_size()
        self.qty      = N_LOTS * self.lot_size

        # 15-min bar builder
        self._bar_open  = None   # current open
        self._bar_high  = None
        self._bar_low   = None
        self._bar_close = None
        self._bar_ts    = None   # start time of current 15-min bar (floor)
        self._closes: list[float] = []   # historical 15-min closes for EMA

        # EMA state
        self._ema_fast: float | None = None
        self._ema_slow: float | None = None
        self._signal: int = 0   # +1 BULL, -1 BEAR, 0 undefined
        self._last_ltp: float = 0.0
        self._last_bar_time: str | None = None

        # Position state (from state file or empty)
        self._pos: dict | None = None
        self._today_entries = 0

        # Load persisted state
        state = _load_state()
        if state.get("position"):
            self._pos    = state["position"]
            self._signal = state.get("signal", 0)
            logger.info(f"  Restored position from state: {self._pos}")

    # ── 15-min bar builder ────────────────────────────────────────────────────

    def _bar_bucket(self, ts: datetime) -> datetime:
        """Floor timestamp to nearest 15-min boundary."""
        m = (ts.minute // 15) * 15
        return ts.replace(minute=m, second=0, microsecond=0)

    def _on_tick(self, ltp: float, ts: datetime) -> bool:
        """Feed a tick. Returns True if a new 15-min bar just closed."""
        self._last_ltp = ltp
        bucket = self._bar_bucket(ts)
        if self._bar_ts is None:
            self._bar_ts    = bucket
            self._bar_open  = self._bar_high = self._bar_low = self._bar_close = ltp
            return False
        if bucket == self._bar_ts:
            self._bar_high  = max(self._bar_high, ltp)
            self._bar_low   = min(self._bar_low, ltp)
            self._bar_close = ltp
            return False
        # New bar started → previous bar is closed
        closed_close = self._bar_close
        self._closes.append(closed_close)
        self._last_bar_time = self._bar_ts.strftime("%H:%M")
        self._bar_ts    = bucket
        self._bar_open  = self._bar_high = self._bar_low = self._bar_close = ltp
        return True

    # ── EMA crossover ─────────────────────────────────────────────────────────

    def _update_ema(self) -> int:
        """
        Recompute EMA(5,13) on accumulated 15-min closes.
        Returns new signal state (+1 BULL, -1 BEAR, 0 insufficient data).
        """
        n = len(self._closes)
        if n < EMA_SLOW:
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
        """True if signal state flipped from previous bar."""
        return new_state != 0 and new_state != self._signal

    # ── Spread value ──────────────────────────────────────────────────────────

    def _spread_value(self) -> float | None:
        """Fetch current spread value (long leg LTP − short leg LTP)."""
        if self._pos is None:
            return None
        items = [
            (self._pos["long_sym"],  OPT_EXCHANGE),
            (self._pos["short_sym"], OPT_EXCHANGE),
        ]
        quotes = _multiquote(items)
        long_ltp  = quotes.get(self._pos["long_sym"])
        short_ltp = quotes.get(self._pos["short_sym"])
        if long_ltp is None or short_ltp is None:
            return None
        return max(0.0, long_ltp - short_ltp)

    # ── Entry ─────────────────────────────────────────────────────────────────

    async def _enter(self, direction: int, spot: float) -> bool:
        """Resolve symbols, quote spread, place both legs."""
        now = datetime.now().time()
        if now >= LAST_ENTRY_TIME:
            logger.info("  Past 14:00 — no new entry.")
            return False

        opt_type   = "CE" if direction == 1 else "PE"
        atm_strike = get_atm_strike(spot, IDX_SYMBOL)
        expiry_str = get_weekly_expiry(API_KEY, MIN_DTE, IDX_SYMBOL)
        if not expiry_str or not _dte_ok(expiry_str):
            logger.warning(f"  No suitable expiry (min_dte={MIN_DTE}). Skipping.")
            return False

        long_sym  = _atm_symbol(atm_strike, opt_type, expiry_str)
        short_sym = _otm_symbol(atm_strike, opt_type, expiry_str,
                                "BULL" if direction == 1 else "BEAR")
        dir_label = "BULL" if direction == 1 else "BEAR"
        logger.info(f"  [{dir_label}] {long_sym} BUY + {short_sym} SELL")

        # Quote both legs
        quotes = _multiquote([(long_sym, OPT_EXCHANGE), (short_sym, OPT_EXCHANGE)])
        long_ltp  = quotes.get(long_sym,  0.0)
        short_ltp = quotes.get(short_sym, 0.0)
        if long_ltp <= 0 or short_ltp <= 0:
            logger.warning(f"  Could not quote legs: {long_sym}={long_ltp}, {short_sym}={short_ltp}")
            return False

        entry_debit = long_ltp - short_ltp
        if entry_debit < MIN_ENTRY_DEBIT:
            logger.warning(f"  Spread debit ₹{entry_debit:.1f} < min ₹{MIN_ENTRY_DEBIT}. Skipping.")
            return False

        # Place long leg first, then short leg
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
            "direction":   dir_label,
            "long_sym":    long_sym,
            "short_sym":   short_sym,
            "atm_strike":  atm_strike,
            "opt_type":    opt_type,
            "expiry":      expiry_str,
            "entry_debit": entry_debit,
            "entry_time":  datetime.now().isoformat(),
            "qty":         self.qty,
        }
        self._signal = direction
        _save_state({"position": self._pos, "signal": self._signal})

        tp  = round(entry_debit * TARGET_MULT, 2)
        sl  = round(entry_debit * SL_MULT, 2)
        msg = (f"📈 NIFTY EMA Spread ENTRY\n"
               f"Direction: {dir_label}\n"
               f"Long:  {long_sym} @ ₹{long_ltp:.1f}\n"
               f"Short: {short_sym} @ ₹{short_ltp:.1f}\n"
               f"Debit (R): ₹{entry_debit:.2f}\n"
               f"TP: ₹{tp:.2f}  SL: ₹{sl:.2f}\n"
               f"Expiry: {expiry_str}")
        await send_async(msg)
        logger.info(f"  ENTRY: {dir_label} debit=₹{entry_debit:.2f} TP=₹{tp:.2f} SL=₹{sl:.2f}")
        self._today_entries += 1
        return True

    # ── Exit ──────────────────────────────────────────────────────────────────

    async def _exit(self, reason: str, spread_val: float | None = None):
        """Close both legs of the current spread."""
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

        # Close long leg (sell), then close short leg (buy)
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

        R      = pos["entry_debit"]
        pnl_r  = (spread_val - R) / R if R > 0 else 0.0
        gross  = (spread_val - R) * pos["qty"]

        _log_trade_csv({
            "date":        datetime.now().strftime("%Y-%m-%d"),
            "exit_time":   datetime.now().isoformat(),
            "direction":   pos["direction"],
            "long_sym":    pos["long_sym"],
            "short_sym":   pos["short_sym"],
            "entry_debit": R,
            "exit_value":  round(spread_val, 2),
            "gross_pnl":   round(gross, 0),
            "exit_reason": reason,
        })

        log_trade_to_db(
            bot_name      = "nifty_ema_spread_bot",
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

        msg = (f"{'✅' if gross >= 0 else '❌'} NIFTY EMA Spread EXIT\n"
               f"Reason: {reason}\n"
               f"Direction: {pos['direction']}\n"
               f"Entry debit: ₹{R:.2f}  Exit: ₹{spread_val:.2f}\n"
               f"P&L (R): {pnl_r:+.2f}R  Gross: ₹{gross:+,.0f}")
        await send_async(msg)
        logger.info(f"  EXIT [{reason}]: spread={spread_val:.2f} R={pnl_r:+.2f}  gross=₹{gross:+,.0f}")

        self._pos = None
        _save_state({"position": None, "signal": self._signal})

    # ── On bar close ──────────────────────────────────────────────────────────

    async def _on_bar_close(self, close: float):
        """Called each time a 15-min bar closes. Core strategy logic."""
        new_state = self._update_ema()
        is_xover  = self._is_crossover(new_state)

        # Check exits on open position first
        if self._pos is not None:
            sv = self._spread_value()
            if sv is not None:
                R = self._pos["entry_debit"]
                if sv >= R * TARGET_MULT:
                    await self._exit("profit_target", sv)
                    is_xover = False   # already closed
                elif sv <= R * SL_MULT:
                    await self._exit("stop_loss", sv)
                    is_xover = False

        # Signal reversal: close and re-enter opposite
        if is_xover and self._pos is not None:
            sv = self._spread_value()
            await self._exit("signal_reversal", sv)
            await asyncio.sleep(ORDER_DELAY)

        # New entry on crossover
        if is_xover and self._pos is None and len(self._closes) >= MIN_BARS:
            spot = close   # spot ≈ last index close
            await self._enter(new_state, spot)

        self._signal = new_state if new_state != 0 else self._signal

    # ── WebSocket main loop ───────────────────────────────────────────────────

    async def _load_history(self) -> list[float]:
        """Pre-load 2 days of 1-min NIFTY history, resample to 15-min closes."""
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
            hist_15 = hist["close"].astype(float).resample("15min").last().dropna()
            closes  = hist_15.tolist()
            logger.info(f"  Pre-loaded {len(closes)} × 15-min bars for EMA warm-up")
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
        """Main event loop — stream ticks via WebSocket, build 15-min bars."""
        asyncio.get_running_loop().set_exception_handler(_log_unhandled_exception)
        logger.info(f"🚀 {STRATEGY_NAME} starting  (paper/live set in OpenAlgo UI)")
        await send_async(f"🚀 {STRATEGY_NAME} started — EMA({EMA_FAST},{EMA_SLOW}) 15m spread")

        # Pre-load history for EMA warm-up
        self._closes = await self._load_history()
        if len(self._closes) >= EMA_SLOW:
            self._update_ema()
        asyncio.create_task(self._state_writer())

        while True:
            try:
                async with websockets.connect(WS_URL) as ws:
                    # Proxy requires auth before it accepts subscriptions
                    await ws.send(json.dumps({
                        "action":  "authenticate",
                        "api_key": API_KEY,
                    }))
                    # Subscribe to NIFTY INDEX tick feed
                    await ws.send(json.dumps({
                        "action":   "subscribe",
                        "symbol":   IDX_SYMBOL,
                        "exchange": IDX_EXCHANGE,
                        "mode":     2,
                    }))
                    logger.info(f"  WebSocket connected → subscribing {IDX_EXCHANGE}:{IDX_SYMBOL}")

                    async for raw_msg in ws:
                        now = datetime.now()
                        if now.time() > SESSION_END:
                            logger.info("  Past session end — disconnecting.")
                            break
                        if now.time() < MARKET_OPEN:
                            continue

                        try:
                            msg = json.loads(raw_msg)
                        except json.JSONDecodeError:
                            continue

                        # Ticks arrive wrapped: {"type": "market_data", "data": {...}}
                        if msg.get("type") == "market_data":
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

                        bar_closed = self._on_tick(ltp, now)
                        if bar_closed and len(self._closes) >= 1:
                            await self._on_bar_close(self._closes[-1])

            except (websockets.ConnectionClosed, OSError, ConnectionRefusedError) as e:
                logger.warning(f"  WebSocket disconnected: {e} — reconnecting in 10s")
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"  Unexpected error: {e}", exc_info=True)
                await asyncio.sleep(15)


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    bot = NiftyEmaSpreadBot()
    asyncio.run(bot.run())
