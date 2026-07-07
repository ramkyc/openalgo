"""
BB Paper Trading Bot — Study A + Study B

Study A: Sell ATM NIFTY PE when the NIFTY index 5-min bar closes above BB(30, 3σ).
Study B: Sell ATM NIFTY PE when the option's own 5-min price closes above BB(20, 2.5σ).

Both run in a single asyncio process sharing one WebSocket connection.
Positions are tracked and exited independently per strategy.

Usage:
    python -m live_trading.bb_paper_bot.main
  or:
    python live_trading/bb_paper_bot/main.py

Environment variables (from .env):
    OPENALGO_API_KEY   — required
    HOST_SERVER        — default http://127.0.0.1:5001
    WEBSOCKET_URL      — default ws://127.0.0.1:5001/ws
    TELEGRAM_BOT_TOKEN — optional, for alerts
    TELEGRAM_CHAT_ID   — optional
    BB_PAPER_MODE      — "true" (default) = log only; "false" = live orders
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, time
from pathlib import Path

import pandas as pd
import requests
import websockets
from dotenv import load_dotenv
from openalgo import api

# ── Path setup ────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.bb_paper_bot.strategy_a      import StudyAEngine
from live_trading.bb_paper_bot.strategy_b      import StudyBEngine
from live_trading.bb_paper_bot.position_manager import PositionManager
from live_trading.shared.atm_resolver           import resolve_atm_option, get_option_ltp
from live_trading.shared.telegram_notifier      import send_async

# ── Logging ───────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "bb_paper_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────
API_KEY    = os.getenv("OPENALGO_API_KEY")
HOST       = os.getenv("HOST_SERVER",   "http://127.0.0.1:5001")
WS_URL     = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")
PAPER_MODE = os.getenv("BB_PAPER_MODE", "true").lower() != "false"

STRATEGY_A = "BB_PAPER_A"
STRATEGY_B = "BB_PAPER_B"

# SL multipliers per strategy (Study A: 2×, Study B: 1.5×)
SL_MULT = {STRATEGY_A: 2.0, STRATEGY_B: 1.5}

SESSION_END = time(15, 15)   # force EOD exit
MARKET_OPEN = time(9,  15)


# ══════════════════════════════════════════════════════════════════════════
class BBPaperBot:
# ══════════════════════════════════════════════════════════════════════════

    def __init__(self):
        self.client  = api(api_key=API_KEY, host=HOST)
        self.eng_a   = StudyAEngine()
        self.eng_b   = StudyBEngine()
        self.pm      = PositionManager(self.client)
        self.ws      = None                # live websocket handle

        # Resolved at session open
        self.atm_info:   dict | None = None   # from resolve_atm_option()
        self.atm_symbol: str  | None = None   # e.g. "NIFTY26MAR24000PE"
        self.atm_exchange: str       = "NFO"
        self.spot_at_open: float     = 0.0

        self._session_started   = False
        self._eod_exit_done     = False
        self._first_connect     = True

    # ── Warm-up ───────────────────────────────────────────────────────────

    async def initialize_history(self):
        """Pre-load NIFTY index history → warm up Study A BB."""
        logger.info("📡 Warming up NIFTY index history for Study A…")
        try:
            end   = datetime.now()
            start = end.strftime("%Y-%m-%d")
            back5 = (end.replace(hour=0) - pd.Timedelta(days=7)).strftime("%Y-%m-%d")

            res = await asyncio.to_thread(
                requests.post,
                f"{HOST}/api/v1/history",
                json={"apikey": API_KEY, "symbol": "NIFTY", "exchange": "NSE_INDEX",
                      "interval": "1m", "start_date": back5, "end_date": start,
                      "source": "api"},
                timeout=15,
            )
            data = res.json()
            if data.get("status") == "success" and data.get("data"):
                df = _parse_history(data["data"])
                # Resample 1-min → 5-min
                df5 = df.resample("5min", offset="15min").agg(
                    {"high": "max", "low": "min", "close": "last"}
                ).dropna()
                self.eng_a.load_history(df5)
                # Best-guess for spot (last known close)
                self.spot_at_open = float(df5["close"].iloc[-1])
                logger.info(f"✅ NIFTY history loaded ({len(df5)} 5-min bars). "
                            f"Last close ≈ {self.spot_at_open:.0f}")
            else:
                logger.warning("⚠️  Could not fetch NIFTY history — Study A BB will warm up on live ticks.")
        except Exception as e:
            logger.error(f"History warm-up error: {e}")

    async def initialize_option_history(self):
        """
        Resolve today's ATM PE, then pre-load its price history
        to warm up Study B's BB.
        """
        if not self.spot_at_open:
            logger.warning("⚠️  Spot price unknown — skipping option history pre-load.")
            return

        logger.info(f"📡 Resolving ATM PE (spot ≈ {self.spot_at_open:.0f})…")
        info = await asyncio.to_thread(
            resolve_atm_option,
            self.spot_at_open, "PE", API_KEY, 2
        )
        if not info:
            logger.warning("⚠️  ATM resolution failed — Study B will wait for first tick.")
            return

        self.atm_info     = info
        self.atm_symbol   = info["symbol"]
        self.atm_exchange = info["exchange"]
        logger.info(f"✅ ATM resolved: {self.atm_symbol}")

        # Load option price history
        logger.info(f"📡 Warming up option history for Study B ({self.atm_symbol})…")
        try:
            back7 = (datetime.now().replace(hour=0) - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
            today = datetime.now().strftime("%Y-%m-%d")
            res   = await asyncio.to_thread(
                requests.post,
                f"{HOST}/api/v1/history",
                json={"apikey": API_KEY, "symbol": self.atm_symbol,
                      "exchange": self.atm_exchange, "interval": "1m",
                      "start_date": back7, "end_date": today, "source": "api"},
                timeout=15,
            )
            data = res.json()
            if data.get("status") == "success" and data.get("data"):
                df   = _parse_history(data["data"])
                df5  = df.resample("5min", offset="15min").agg(
                    {"high": "max", "low": "min", "close": "last"}
                ).dropna()
                self.eng_b.load_history(df5)
                logger.info(f"✅ Option history loaded ({len(df5)} 5-min bars)")
            else:
                logger.warning(f"⚠️  No option history for {self.atm_symbol}")
        except Exception as e:
            logger.error(f"Option history warm-up error: {e}")

    # ── Tick handlers ─────────────────────────────────────────────────────

    async def on_index_tick(self, ltp: float, ts):
        """Called for every NIFTY index tick."""
        now = datetime.now()

        # Resolve ATM on the very first tick of the session
        if not self._session_started and now.time() >= MARKET_OPEN:
            self._session_started = True
            self.spot_at_open = ltp
            if not self.atm_symbol:
                await self._late_resolve_atm(ltp)

        # Study A signal
        signal = self.eng_a.on_tick(ltp, ts)
        if signal == "OVERBOUGHT" and not self.pm.has_position(STRATEGY_A):
            if self.eng_a.in_entry_window(now):
                await self._enter_study_a(ltp, now)

    async def on_option_tick(self, ltp: float, ts):
        """Called for every ATM PE option tick."""
        if ltp <= 0:
            return
        now = datetime.now()

        # Exit checks — run on every tick for both open positions
        for strategy in (STRATEGY_A, STRATEGY_B):
            if self.pm.has_position(strategy):
                reason = self.pm.check_exit(strategy, ltp)
                if reason:
                    await self._close_position(strategy, ltp, reason)

        # Study B signal
        signal = self.eng_b.on_tick(ltp, ts)
        if signal == "SELL" and not self.pm.has_position(STRATEGY_B):
            await self._enter_study_b(ltp, now)

    # ── Entry helpers ─────────────────────────────────────────────────────

    async def _enter_study_a(self, index_ltp: float, now: datetime):
        if not self.atm_symbol:
            logger.warning("[Study A] ATM not resolved yet — skipping entry.")
            return
        # Fetch live option premium
        opt_ltp = await asyncio.to_thread(
            get_option_ltp, self.atm_symbol, self.atm_exchange, API_KEY
        )
        if opt_ltp < 150:
            logger.info(f"[Study A] Option premium ₹{opt_ltp:.2f} < ₹150 — filtered.")
            return

        ok = self.pm.enter(
            strategy=STRATEGY_A, symbol=self.atm_symbol, exchange=self.atm_exchange,
            entry_px=opt_ltp, sl_mult=SL_MULT[STRATEGY_A], paper_mode=PAPER_MODE,
        )
        if ok:
            pos = self.pm.get_position(STRATEGY_A)
            msg = (
                f"📋 *[Study A] New Paper Trade*\n"
                f"Sold `{self.atm_symbol}`\n"
                f"Entry: ₹{opt_ltp:.2f}  |  NIFTY: {index_ltp:.2f}\n"
                f"Target: ₹{pos.target_price:.2f}  |  SL: ₹{pos.sl_price:.2f}\n"
                f"_(BB30 3σ index breach — {now.strftime('%H:%M')})_"
            )
            await send_async(msg)

    async def _enter_study_b(self, opt_ltp: float, now: datetime):
        if not self.atm_symbol:
            return
        ok = self.pm.enter(
            strategy=STRATEGY_B, symbol=self.atm_symbol, exchange=self.atm_exchange,
            entry_px=opt_ltp, sl_mult=SL_MULT[STRATEGY_B], paper_mode=PAPER_MODE,
        )
        if ok:
            pos = self.pm.get_position(STRATEGY_B)
            msg = (
                f"📋 *[Study B] New Paper Trade*\n"
                f"Sold `{self.atm_symbol}`\n"
                f"Entry: ₹{opt_ltp:.2f}\n"
                f"Target: ₹{pos.target_price:.2f}  |  SL: ₹{pos.sl_price:.2f}\n"
                f"_(BB20 2.5σ option breach — {now.strftime('%H:%M')})_"
            )
            await send_async(msg)

    # ── Exit helper ───────────────────────────────────────────────────────

    async def _close_position(self, strategy: str, exit_px: float, reason: str):
        trade = self.pm.close(strategy, exit_px, reason, paper_mode=PAPER_MODE)
        if trade:
            emoji = "🟢" if trade["won"] else "🔴"
            tag   = "A" if "A" in strategy else "B"
            msg   = (
                f"{emoji} *[Study {tag}] Trade Closed — {reason}*\n"
                f"Symbol: `{trade['symbol']}`\n"
                f"Entry ₹{trade['entry_px']}  →  Exit ₹{trade['exit_px']}\n"
                f"Net P&L: ₹{trade['net']:,.0f}  |  Duration: {trade['duration_min']} min"
            )
            await send_async(msg)

    async def _eod_close_all(self):
        """Force-close all open positions at 15:15."""
        if self._eod_exit_done:
            return
        self._eod_exit_done = True
        for strategy in (STRATEGY_A, STRATEGY_B):
            if self.pm.has_position(strategy):
                ltp = await asyncio.to_thread(
                    get_option_ltp, self.atm_symbol, self.atm_exchange, API_KEY
                )
                logger.info(f"[EOD] Closing {strategy} @ ₹{ltp:.2f}")
                await self._close_position(strategy, ltp, "EOD")

    # ── ATM late-resolve (first live tick) ────────────────────────────────

    async def _late_resolve_atm(self, spot: float):
        """Resolve ATM using live spot if history warm-up couldn't do it."""
        logger.info(f"[Session] First tick received — resolving ATM (spot={spot:.0f})")
        info = await asyncio.to_thread(resolve_atm_option, spot, "PE", API_KEY, 2)
        if info:
            self.atm_info     = info
            self.atm_symbol   = info["symbol"]
            self.atm_exchange = info["exchange"]
            logger.info(f"✅ ATM (late-resolved): {self.atm_symbol}")
            # Subscribe to option WebSocket feed
            if self.ws:
                await self.ws.send(json.dumps({
                    "action": "subscribe",
                    "symbol": self.atm_symbol,
                    "exchange": self.atm_exchange,
                    "mode": 2,
                }))
                logger.info(f"📡 Subscribed to {self.atm_symbol}")
        else:
            logger.error("❌ ATM late-resolution failed.")

    # ── Main WebSocket loop ───────────────────────────────────────────────

    async def main_loop(self):
        # Pre-market warm-up
        await self.initialize_history()
        await self.initialize_option_history()

        retry_delay = 5

        while True:
            now = datetime.now()

            # EOD check
            if now.time() >= SESSION_END:
                await self._eod_close_all()
                logger.info("✅ Past 15:15 IST — shutting down.")
                break

            try:
                logger.info(f"🔌 Connecting to WebSocket: {WS_URL}")
                async with websockets.connect(
                    WS_URL, ping_interval=30, ping_timeout=60
                ) as ws:
                    self.ws = ws
                    await ws.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))

                    # Always subscribe to NIFTY index
                    await ws.send(json.dumps({
                        "action": "subscribe", "symbol": "NIFTY",
                        "exchange": "NSE_INDEX", "mode": 2,
                    }))
                    logger.info("✅ Subscribed to NIFTY index")

                    # Subscribe to ATM PE if already resolved
                    if self.atm_symbol:
                        await ws.send(json.dumps({
                            "action": "subscribe",
                            "symbol": self.atm_symbol,
                            "exchange": self.atm_exchange,
                            "mode": 2,
                        }))
                        logger.info(f"✅ Subscribed to {self.atm_symbol}")

                    if self._first_connect:
                        mode_str = "📝 Paper mode (logging only)" if PAPER_MODE else "🔴 LIVE orders"
                        await send_async(
                            f"🤖 *BB Paper Bot Online*\n"
                            f"Study A: BB(30, 3σ) index breach → sell ATM PE\n"
                            f"Study B: BB(20, 2.5σ) option breach → sell ATM PE\n"
                            f"Mode: {mode_str}\n"
                            f"_Signals: 09:15–10:30  |  Entry ≥ ₹150_"
                        )
                        self._first_connect = False
                    else:
                        logger.info("WebSocket reconnected.")

                    retry_delay = 5

                    async for raw in ws:
                        # EOD guard inside loop
                        if datetime.now().time() >= SESSION_END:
                            await self._eod_close_all()
                            return

                        msg = json.loads(raw)
                        if msg.get("type") != "market_data":
                            continue

                        sym    = msg.get("symbol", "")
                        mdata  = msg.get("data", {})
                        ltp    = float(mdata.get("ltp", 0) or mdata.get("lp", 0))
                        ts     = mdata.get("t") or datetime.now().timestamp()

                        if ltp <= 0:
                            continue

                        if sym == "NIFTY":
                            await self.on_index_tick(ltp, ts)
                        elif sym == self.atm_symbol:
                            await self.on_option_tick(ltp, ts)

            except Exception as e:
                logger.warning(f"WebSocket error: {e}. Reconnecting in {retry_delay}s…")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


# ── Helpers ───────────────────────────────────────────────────────────────

def _parse_history(raw: list) -> pd.DataFrame:
    """Convert raw history API response to a DatetimeIndex DataFrame."""
    df = pd.DataFrame(raw)
    if "timestamp" in df.columns:
        df["datetime"] = (
            pd.to_datetime(df["timestamp"], unit="s", utc=True)
            .dt.tz_convert("Asia/Kolkata")
            .dt.tz_localize(None)
        )
    elif "date" in df.columns:
        df["datetime"] = pd.to_datetime(df["date"])
    df.set_index("datetime", inplace=True)
    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[["high", "low", "close"]].dropna()


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    bot = BBPaperBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("Stop signal received.")
