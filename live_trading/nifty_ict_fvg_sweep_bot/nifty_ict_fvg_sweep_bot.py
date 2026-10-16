"""
NIFTY ICT FVG + Liquidity Sweep Confluence Bot — Paper / Live Bot
===================================================================

Strategy (from options_data/research/ict_fvg_liquidity_sweep_study, results_summary.md):
  1. Detect Equal Lows (EQL) / Equal Highs (EQH) within 5.0 pts tolerance (k_swing=1).
  2. Identify intraday Fair Value Gaps (FVG) sitting adjacent to equal levels.
  3. Liquidity sweep taps into old FVG with a wick, but candle closes back inside.
  4. Displacement candle body >= 1.2 * ATR(14) breaks structure (BOS beyond prior swing).
  5. New FVG formed during displacement leg.
  6. Entry when price pulls back to touch 50% Consequent Encroachment (CE) of new FVG.
  7. Execution: Bullish signal -> BUY NIFTY ATM CE (10 lots). Bearish signal -> BUY NIFTY ATM PE (10 lots).
  8. Exit: Spot-based Stop Loss (sweep extreme wick), Spot-based Target (prior BOS level), or EOD 15:15 IST.

Champion Config:
  1-min NIFTY spot bars, k_swing=1, eq_tolerance=5.0 pts, disp_mult=1.2, 10 lots NRML.
  IS Sharpe 3.967, OOS Sharpe 7.097, MC stability 95.3%, 0 walk-forward catastrophic folds.
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
import pandas_ta_classic as ta
import websockets
from dotenv import load_dotenv
from openalgo import api

# ── Path / Env Setup ─────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils import get_expiry_dates, get_history
from live_trading.shared.atm_resolver import resolve_atm_option, get_option_ltp
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger import log_trade_to_db
from live_trading.shared.decision_logger import DecisionLogger
from live_trading.shared.tick_watchdog import TickWatchdog

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "nifty_ict_fvg_sweep_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER",   "http://127.0.0.1:8080")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:8765")

if not API_KEY:
    logger.error("❌ OPENALGO_API_KEY not found in environment. Please check .env. Exiting.")
    sys.exit(1)

STRATEGY_NAME = "NIFTY_ICT_FVG_SWEEP"

IDX_SYMBOL   = "NIFTY"
IDX_EXCHANGE = "NSE_INDEX"
OPT_EXCHANGE = "NFO"

N_LOTS           = 10
DEFAULT_LOT_SIZE = 65
MIN_DTE          = 1
MAX_DTE          = 7

EQ_TOLERANCE     = 5.0
K_SWING          = 1
DISP_MULT        = 1.2
MAX_DISP_BARS    = 10

MARKET_OPEN  = dt_time(9, 15)
CUTOFF_TIME  = dt_time(15, 15)
SESSION_END  = dt_time(15, 14)

STATE_FILE   = LOGS_DIR / "nifty_ict_fvg_sweep_state.json"
DECISION_LOG = LOGS_DIR / "nifty_ict_fvg_sweep_decisions.jsonl"


def _get_lot_size(symbol: str) -> int:
    try:
        from database.token_db import get_symbol_info
        si = get_symbol_info(symbol, OPT_EXCHANGE)
        if si and getattr(si, "lotsize", None):
            ls = int(si.lotsize)
            logger.info(f"  Lot size from DB: {ls}  ({symbol})")
            return ls
    except Exception as e:
        logger.warning(f"  Lot size DB lookup failed for {symbol}: {e}. Using default {DEFAULT_LOT_SIZE}.")
    return DEFAULT_LOT_SIZE


def detect_ict_sweep_signals(df_1m: pd.DataFrame, eq_tolerance: float = 5.0, k_swing: int = 1, disp_mult: float = 1.2) -> list[dict]:
    """
    Evaluates intraday 1-minute bars for completed ICT FVG + Liquidity Sweep setups.
    Returns list of candidate trade setups.
    """
    if len(df_1m) < 30:
        return []

    df = df_1m.copy()
    if "atr14" not in df.columns:
        df["atr14"] = ta.atr(df["high"], df["low"], df["close"], length=14)
        df["atr14"] = df["atr14"].bfill()

    highs = df["high"].values
    lows = df["low"].values
    opens = df["open"].values
    closes = df["close"].values
    atrs = df["atr14"].values
    timestamps = df.index
    times = [ts.time() if hasattr(ts, "time") else ts for ts in timestamps]
    n = len(df)

    # 1. Identify Fair Value Gaps (FVGs)
    fvgs = []
    for i in range(2, n):
        if lows[i] > highs[i - 2]:
            fvgs.append({
                "bar_idx": i, "dir": "bull",
                "top": lows[i], "bot": highs[i - 2],
                "ce": (lows[i] + highs[i - 2]) / 2.0,
            })
        elif highs[i] < lows[i - 2]:
            fvgs.append({
                "bar_idx": i, "dir": "bear",
                "top": lows[i - 2], "bot": highs[i],
                "ce": (lows[i - 2] + highs[i]) / 2.0,
            })

    # 2. Identify Swing Highs / Swing Lows (fractals k_swing=1)
    swing_highs = []
    swing_lows = []
    for i in range(k_swing, n - k_swing):
        if highs[i] == highs[max(0, i - k_swing) : min(n, i + k_swing + 1)].max():
            swing_highs.append((i, highs[i]))
        if lows[i] == lows[max(0, i - k_swing) : min(n, i + k_swing + 1)].min():
            swing_lows.append((i, lows[i]))

    events = []

    # Bullish Setups
    for a_idx, (i_a, l_a) in enumerate(swing_lows):
        for i_b, l_b in swing_lows[a_idx + 1:]:
            if i_b - i_a < 2 or i_b - i_a > 45:
                continue
            if abs(l_a - l_b) <= eq_tolerance:
                eql_level = min(l_a, l_b)
                cand_old = [f for f in fvgs if f["dir"] == "bull" and f["bar_idx"] < i_b and f["bot"] < eql_level and f["top"] <= eql_level + 15.0]
                if not cand_old:
                    continue
                for s in range(i_b + k_swing, min(n, i_b + 45)):
                    if times[s] >= dt_time(15, 0):
                        break
                    if lows[s] < eql_level:
                        for old_fvg in cand_old:
                            if old_fvg["bar_idx"] >= s:
                                continue
                            if any(min(opens[k], closes[k]) < old_fvg["bot"] for k in range(old_fvg["bar_idx"] + 1, s)):
                                continue
                            if (lows[s] <= old_fvg["top"] and lows[s] >= old_fvg["bot"] 
                                and closes[s] > eql_level and min(opens[s], closes[s]) >= old_fvg["bot"]):
                                sweep_low = lows[s]
                                prior_shs = [sh for sh in swing_highs if i_a <= sh[0] < s]
                                if not prior_shs:
                                    continue
                                prior_bos = max(sh[1] for sh in prior_shs)
                                disp_bar = -1
                                for d in range(s + 1, min(n, s + 11)):
                                    if closes[d] > opens[d] and abs(closes[d] - opens[d]) >= disp_mult * atrs[d] and closes[d] > prior_bos:
                                        disp_bar = d
                                        break
                                if disp_bar == -1:
                                    continue
                                new_fvg = next((f for f in fvgs if f["dir"] == "bull" and s < f["bar_idx"] <= disp_bar + 2), None)
                                if new_fvg is None:
                                    continue
                                ce_level = new_fvg["ce"]
                                for e in range(new_fvg["bar_idx"] + 1, n):
                                    if times[e] >= dt_time(15, 15) or lows[e] <= sweep_low:
                                        break
                                    if lows[e] <= ce_level:
                                        events.append({
                                            "dir": "long", "trigger_bar_idx": e, "trigger_ts": timestamps[e],
                                            "entry_price": ce_level, "stop_loss_spot": sweep_low,
                                            "target_spot": prior_bos, "eq_level": eql_level,
                                            "sweep_bar": s, "disp_bar": disp_bar
                                        })
                                        break
                                break
                        break

    # Bearish Setups
    for a_idx, (i_a, h_a) in enumerate(swing_highs):
        for i_b, h_b in swing_highs[a_idx + 1:]:
            if i_b - i_a < 2 or i_b - i_a > 45:
                continue
            if abs(h_a - h_b) <= eq_tolerance:
                eqh_level = max(h_a, h_b)
                cand_old = [f for f in fvgs if f["dir"] == "bear" and f["bar_idx"] < i_b and f["top"] > eqh_level and f["bot"] >= eqh_level - 15.0]
                if not cand_old:
                    continue
                for s in range(i_b + k_swing, min(n, i_b + 45)):
                    if times[s] >= dt_time(15, 0):
                        break
                    if highs[s] > eqh_level:
                        for old_fvg in cand_old:
                            if old_fvg["bar_idx"] >= s:
                                continue
                            if any(max(opens[k], closes[k]) > old_fvg["top"] for k in range(old_fvg["bar_idx"] + 1, s)):
                                continue
                            if (highs[s] >= old_fvg["bot"] and highs[s] <= old_fvg["top"] 
                                and closes[s] < eqh_level and max(opens[s], closes[s]) <= old_fvg["top"]):
                                sweep_high = highs[s]
                                prior_sls = [sl for sl in swing_lows if i_a <= sl[0] < s]
                                if not prior_sls:
                                    continue
                                prior_bos = min(sl[1] for sl in prior_sls)
                                disp_bar = -1
                                for d in range(s + 1, min(n, s + 11)):
                                    if closes[d] < opens[d] and abs(closes[d] - opens[d]) >= disp_mult * atrs[d] and closes[d] < prior_bos:
                                        disp_bar = d
                                        break
                                if disp_bar == -1:
                                    continue
                                new_fvg = next((f for f in fvgs if f["dir"] == "bear" and s < f["bar_idx"] <= disp_bar + 2), None)
                                if new_fvg is None:
                                    continue
                                ce_level = new_fvg["ce"]
                                for e in range(new_fvg["bar_idx"] + 1, n):
                                    if times[e] >= dt_time(15, 15) or highs[e] >= sweep_high:
                                        break
                                    if highs[e] >= ce_level:
                                        events.append({
                                            "dir": "short", "trigger_bar_idx": e, "trigger_ts": timestamps[e],
                                            "entry_price": ce_level, "stop_loss_spot": sweep_high,
                                            "target_spot": prior_bos, "eq_level": eqh_level,
                                            "sweep_bar": s, "disp_bar": disp_bar
                                        })
                                        break
                                break
                        break

    return events


class NiftyIctAvgSweepBot:
    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.ws = None
        self.decision_logger = DecisionLogger(DECISION_LOG, heartbeat_secs=300, bot_logger=logger)
        self.watchdog = TickWatchdog(
            bot_name=STRATEGY_NAME,
            tracked_symbols=lambda: [IDX_SYMBOL],
            market_open=MARKET_OPEN,
            market_close=SESSION_END,
            bot_logger=logger,
        )

        self.bars = deque(maxlen=400)
        self.nifty_ltp = 0.0
        self.active_trade = None
        self.signal_done_today = False
        self.last_bar_time = None
        self.lot_size = DEFAULT_LOT_SIZE

        self._load_state()

    def _load_state(self):
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, "r") as f:
                    st = json.load(f)
                    today_str = datetime.now().strftime("%Y-%m-%d")
                    if st.get("date") == today_str:
                        self.active_trade = st.get("active_trade")
                        self.signal_done_today = st.get("signal_done_today", False)
                        logger.info(f"Loaded state for today ({today_str}): active_trade={bool(self.active_trade)}, signal_done={self.signal_done_today}")
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}")

    def _save_state(self):
        st = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "last_update": datetime.now().isoformat(),
            "nifty_ltp": self.nifty_ltp,
            "active_trade": self.active_trade,
            "signal_done_today": self.signal_done_today,
            "bar_count": len(self.bars),
        }
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(st, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state file: {e}")

    def _bootstrap_history(self):
        logger.info("Fetching intraday history for NIFTY spot...")
        raw = get_history(API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "1m", 1)
        if not raw:
            logger.warning("Could not fetch history for NIFTY spot.")
            return

        df = pd.DataFrame(raw)
        if df.empty:
            return

        if "timestamp" in df.columns:
            df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        elif "date" in df.columns:
            df["dt"] = pd.to_datetime(df["date"])

        today = datetime.now().date()
        df = df[df["dt"].dt.date == today].sort_values("dt")
        for _, row in df.iterrows():
            bar = {
                "ts": row["dt"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
            self.bars.append(bar)
            self.nifty_ltp = float(row["close"])

        logger.info(f"Bootstrapped {len(self.bars)} bars for today. Latest LTP: {self.nifty_ltp}")

    def _evaluate_signals(self):
        now_dt = datetime.now()
        now_t = now_dt.time()

        if now_t >= CUTOFF_TIME or self.signal_done_today or self.active_trade is not None:
            return

        if len(self.bars) < 30:
            return

        df_bars = pd.DataFrame(list(self.bars)).set_index("ts")[["open", "high", "low", "close"]]
        events = detect_ict_sweep_signals(df_bars, eq_tolerance=EQ_TOLERANCE, k_swing=K_SWING, disp_mult=DISP_MULT)
        if not events:
            return

        # Check if the latest event triggered on the final (most recent completed) bar
        latest_event = events[-1]
        if latest_event["trigger_bar_idx"] == len(df_bars) - 1:
            self._execute_entry(latest_event)

    def _execute_entry(self, event: dict):
        direction = event["dir"]
        opt_type = "CE" if direction == "long" else "PE"

        logger.info(f"⚡ ICT FVG Sweep Signal Detected! Direction={direction.upper()} | Spot={self.nifty_ltp:.2f}")

        opt_info = resolve_atm_option(spot=self.nifty_ltp, opt_type=opt_type, api_key=API_KEY, min_dte=MIN_DTE, index=IDX_SYMBOL)
        if not opt_info:
            logger.error("❌ Failed to resolve ATM option. Entry aborted.")
            return

        symbol = opt_info["symbol"]
        self.lot_size = _get_lot_size(symbol)
        qty = N_LOTS * self.lot_size

        # PAPER ONLY: no broker order is ever sent from this bot. Fills are
        # simulated at the option LTP.
        logger.info(f"[PAPER] BUY {symbol} | Qty: {qty} ({N_LOTS} lots)")
        fill_price = get_option_ltp(symbol, OPT_EXCHANGE, API_KEY) or 100.0

        self.active_trade = {
            "dir": direction,
            "symbol": symbol,
            "strike": opt_info["strike"],
            "expiry": opt_info["expiry"],
            "opt_type": opt_type,
            "qty": qty,
            "entry_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "entry_spot": self.nifty_ltp,
            "entry_opt_price": fill_price,
            "stop_loss_spot": event["stop_loss_spot"],
            "target_spot": event["target_spot"],
            "eq_level": event["eq_level"],
        }
        self.signal_done_today = True
        self._save_state()

        msg = (
            f"🚀 <b>{STRATEGY_NAME} ENTRY</b>\n"
            f"Direction: <b>{direction.upper()}</b>\n"
            f"Option: <b>{symbol}</b> @ ₹{fill_price:.2f}\n"
            f"Qty: {qty} ({N_LOTS} lots)\n"
            f"Spot Entry: {self.nifty_ltp:.2f}\n"
            f"Spot SL: {event['stop_loss_spot']:.2f}\n"
            f"Spot Target: {event['target_spot']:.2f}"
        )
        send_async(msg)

    def _manage_open_position(self):
        if not self.active_trade:
            return

        trade = self.active_trade
        now_t = datetime.now().time()

        spot_sl = trade["stop_loss_spot"]
        spot_tgt = trade["target_spot"]
        direction = trade["dir"]
        symbol = trade["symbol"]
        qty = trade["qty"]

        exit_reason = None
        if now_t >= CUTOFF_TIME:
            exit_reason = "EOD 15:15 IST Force Close"
        elif direction == "long":
            if self.nifty_ltp <= spot_sl:
                exit_reason = f"Spot SL Hit ({self.nifty_ltp:.2f} <= {spot_sl:.2f})"
            elif self.nifty_ltp >= spot_tgt:
                exit_reason = f"Spot Target Hit ({self.nifty_ltp:.2f} >= {spot_tgt:.2f})"
        elif direction == "short":
            if self.nifty_ltp >= spot_sl:
                exit_reason = f"Spot SL Hit ({self.nifty_ltp:.2f} >= {spot_sl:.2f})"
            elif self.nifty_ltp <= spot_tgt:
                exit_reason = f"Spot Target Hit ({self.nifty_ltp:.2f} <= {spot_tgt:.2f})"

        if exit_reason:
            logger.info(f"🏁 Closing Position: {exit_reason}")
            # PAPER ONLY: simulated exit at option LTP, no broker order.
            exit_price = get_option_ltp(symbol, OPT_EXCHANGE, API_KEY) or trade["entry_opt_price"]

            pnl = (exit_price - trade["entry_opt_price"]) * qty
            pnl_pct = ((exit_price - trade["entry_opt_price"]) / trade["entry_opt_price"]) * 100.0

            log_trade_to_db(
                bot_name="nifty_ict_fvg_sweep_bot",
                instrument=IDX_SYMBOL,
                option_symbol=symbol,
                option_type=trade["opt_type"],
                entry_time=trade["entry_time"],
                exit_time=datetime.now(),
                entry_premium=trade["entry_opt_price"],
                exit_premium=exit_price,
                exit_reason=exit_reason,
                quantity=qty,
                lots=N_LOTS,
                lot_size=self.lot_size,
                gross_pnl=pnl,
                notes="PAPER",
                direction="long",
            )

            msg = (
                f"🏁 <b>{STRATEGY_NAME} EXIT</b>\n"
                f"Option: <b>{symbol}</b>\n"
                f"Reason: {exit_reason}\n"
                f"Entry Price: ₹{trade['entry_opt_price']:.2f}\n"
                f"Exit Price: ₹{exit_price:.2f}\n"
                f"P&L: <b>₹{pnl:+,.2f} ({pnl_pct:+.2f}%)</b>"
            )
            send_async(msg)

            self.active_trade = None
            self._save_state()

    async def _on_tick(self, tick_data: dict):
        symbol = tick_data.get("symbol")
        self.watchdog.on_tick(symbol)
        if symbol == IDX_SYMBOL:
            ltp = float(tick_data.get("ltp") or 0.0)
            if ltp > 0:
                self.nifty_ltp = ltp
                self._manage_open_position()

    async def _on_bar(self, bar_data: dict):
        ts = bar_data.get("ts")
        if self.last_bar_time == ts:
            return
        self.last_bar_time = ts

        self.bars.append(bar_data)
        self.nifty_ltp = bar_data["close"]
        logger.info(f"Bar complete [{ts}]: Close={bar_data['close']:.2f} | Total bars={len(self.bars)}")

        self._evaluate_signals()
        self._manage_open_position()
        self._save_state()

        self.decision_logger.log_bar({
            "nifty_ltp": self.nifty_ltp,
            "active_trade": self.active_trade,
            "signal_done_today": self.signal_done_today,
            "bar_count": len(self.bars),
        })

    async def run(self):
        logger.info(f"Starting {STRATEGY_NAME} bot...")
        self._bootstrap_history()
        self._save_state()

        cur_bar = None
        asyncio.create_task(self.watchdog.watch_loop())

        while True:
            try:
                async with websockets.connect(WS_URL) as ws:
                    self.ws = ws
                    # Proxy requires authenticate before routing market data
                    await ws.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))
                    sub_msg = {"action": "subscribe", "symbol": IDX_SYMBOL,
                               "exchange": IDX_EXCHANGE, "mode": 2}
                    await ws.send(json.dumps(sub_msg))
                    logger.info(f"Subscribed to {IDX_SYMBOL} on WebSocket {WS_URL}")

                    while True:
                        msg = await ws.recv()
                        raw_msg = json.loads(msg)
                        # WS proxy wraps ticks: {"type":"market_data","symbol":..,"data":{ltp,t,..}}
                        if raw_msg.get("type") == "market_data" and raw_msg.get("symbol") == IDX_SYMBOL:
                            md = raw_msg.get("data") or {}
                            data = {"type": "tick", "symbol": raw_msg["symbol"],
                                    "ltp": md.get("ltp") or md.get("lp"),
                                    "timestamp": md.get("t")}
                            ltp = float(data.get("ltp") or 0.0)
                            if ltp <= 0:
                                continue

                            ts_val = data.get("timestamp") or datetime.now().timestamp()
                            dt_obj = datetime.fromtimestamp(ts_val)
                            bar_time = dt_obj.strftime("%H:%M")

                            await self._on_tick(data)

                            if cur_bar is None or cur_bar["time_str"] != bar_time:
                                if cur_bar is not None:
                                    await self._on_bar(cur_bar["data"])

                                cur_bar = {
                                    "time_str": bar_time,
                                    "data": {
                                        "ts": dt_obj.strftime("%Y-%m-%d %H:%M:%00"),
                                        "open": ltp,
                                        "high": ltp,
                                        "low": ltp,
                                        "close": ltp,
                                    }
                                }
                            else:
                                cur_bar["data"]["high"] = max(cur_bar["data"]["high"], ltp)
                                cur_bar["data"]["low"] = min(cur_bar["data"]["low"], ltp)
                                cur_bar["data"]["close"] = ltp

            except Exception as e:
                logger.warning(f"WebSocket disconnected ({e}). Reconnecting in 5s...")
                await asyncio.sleep(5)


if __name__ == "__main__":
    bot = NiftyIctAvgSweepBot()
    asyncio.run(bot.run())
