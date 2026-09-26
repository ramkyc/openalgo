"""
SENSEX Trend Seller Bot — SHORT-ONLY
=====================================
live_trading/sensex_trend_seller_bot/sensex_trend_seller_bot.py

Research-validated (Stage 9, 2026-03-21):  options_data/research/nifty_trend_seller_study/
  Short leg (sell CE on bearish signal) : IS +0.531 → OOS +2.181  WR 73.5%  ✅
  Long  leg (sell PE on bullish signal) : IS -0.488 → OOS -0.188          ❌  DO NOT TRADE

This bot runs SHORT-ONLY: when the 5-condition bearish confluence fires on
1-minute SENSEX bars, it SELLS the ATM CE (counter option) and holds until
the SL is hit or 15:14 IST EOD exit.

Strategy conditions (all 5 required, on each completed 1-min bar close):
    1. Close < EMA(20)               [bearish trend — index below 20-period EMA]
    2. ADX(14) > 30                  [strong trend — matches nifty_trend_seller_study champion]
    3. ADX[now] > ADX[5 bars ago]    [ADX-D: trend accelerating, 5-bar window]
    4. RSI(14) < 45                  [bearish momentum — tighter than 50 for signal quality]
    5. MACD(5,13,3) line crosses below signal line  [trigger]

→ On signal: SELL ATM SENSEX CE  (premium decays as SENSEX falls)

Filters applied before entry:
    • Entry window : 10:00 – 13:00 IST
    • VIX filter   : INDIAVIX ≤ 22  (same as NIFTY bot — India-wide risk gauge)
    • DTE range    : 2 – 7 days to expiry (BSE weekly; now Thursday from Sep 2025)
    • One CE position per session — no duplicate entry

Risk management:
    • Safety SL : 2× entry premium
    • EOD exit  : unconditional close at 15:14 IST

Instrument config:
    • Index         : SENSEX  (BSE_INDEX)
    • Options exch  : BFO  (BSE F&O)
    • Lot size      : 20  (fetched dynamically from token DB)
    • Strike step   : 100  (ATM resolved by OpenAlgo api_utils)

Shared utilities (no duplication):
    live_trading.api_utils               — get_expiry_dates, get_option_symbol, get_history
    live_trading.shared.atm_resolver     — get_option_ltp
    live_trading.shared.telegram_notifier— send_async
    database.token_db.get_symbol_info    — dynamic lot size
    openalgo.api                         — placesmartorder
"""

import asyncio
import json
import logging
import os
import sys
from collections import deque
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path

import pandas as pd
from live_trading.shared import ta_compat as ta
import websockets
from dotenv import load_dotenv
from openalgo import api

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils                import get_expiry_dates, get_option_symbol, get_history, is_market_holiday
from live_trading.shared.atm_resolver      import get_option_ltp
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger      import log_trade_to_db
from live_trading.shared.order_fill        import fetch_fill_price
from live_trading.shared.decision_logger   import DecisionLogger
from live_trading.shared.tick_watchdog     import TickWatchdog

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "sensex_trend_seller_bot.log"),
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

# ── Strategy constants ────────────────────────────────────────────────────────
STRATEGY_NAME    = "SENSEX_TREND_SELLER"


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


IDX_SYMBOL       = "SENSEX"
IDX_EXCHANGE     = "BSE_INDEX"
VIX_SYMBOL       = "INDIAVIX"
VIX_EXCHANGE     = "NSE_INDEX"
OPT_EXCHANGE     = "BFO"

N_LOTS           = 10           # number of lots per trade
DEFAULT_LOT_SIZE = 20           # fallback if DB lookup fails (BSE lot size)

# Indicator parameters — research-optimal for SENSEX short-only
# (Stage 9, options_data/research/nifty_trend_seller_study/09_multi_instrument.py)
EMA_LEN          = 20
ADX_LEN          = 14
RSI_LEN          = 14
MACD_FAST        = 5
MACD_SLOW        = 13
MACD_SIG         = 3
ADX_RISING_BARS  = 5            # ADX-D: compare now vs 5 bars ago (matches nifty_trend_seller_study champion)

# Entry filters (mirroring nifty_trend_seller_study champion params — this bot was
# copied from the NIFTY Trend Seller; no independent SENSEX parameter sweep was run)
ADX_THRESHOLD    = 30.0         # same as NIFTY champion (was 25 — corrected 2026-04-22)
RSI_SHORT_MAX    = 45.0         # RSI < 45 for short signal (was 50 — corrected 2026-04-22)
VIX_MAX          = 22.0
MIN_DTE          = 2
MAX_DTE          = 7
SL_MULTIPLE      = 2.0          # SL at 2× entry premium

# Session timing
MARKET_OPEN      = dt_time(9,  15)
ENTRY_START      = dt_time(10,  0)
ENTRY_END        = dt_time(13,  0)
SESSION_END      = dt_time(15, 14)

# Minimum completed bars before signals are generated (warm-up guard)
MIN_BARS_REQUIRED = 50

# State file for Telegram /status dashboard
STATE_FILE       = LOGS_DIR / "sensex_trend_seller_state.json"

# Decision-state logging (jsonl + throttled heartbeat — see shared/decision_logger.py)
DECISION_LOG     = LOGS_DIR / "sensex_trend_seller_decisions.jsonl"
HEARTBEAT_SECS   = 300

# pandas_ta column names
_MACD_COL  = f"MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
_MACDS_COL = f"MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
_ADX_COL   = f"ADX_{ADX_LEN}"


# ── Helper: dynamic lot size ──────────────────────────────────────────────────
def _get_lot_size(symbol: str) -> int:
    """
    Fetch current SENSEX lot size from the OpenAlgo token database.
    Falls back to DEFAULT_LOT_SIZE (20) if lookup fails.
    """
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


# ── Helper: expiry selection ──────────────────────────────────────────────────
def _get_suitable_expiry() -> str | None:
    """
    Return the nearest SENSEX weekly expiry with DTE in [MIN_DTE, MAX_DTE].
    BSE weekly: Thursday from Sep 2025 onwards (prior: Tuesday).
    Uses api_utils.get_expiry_dates — same shared utility as NIFTY bot.
    """
    dates = get_expiry_dates(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, "options")
    today = datetime.now().date()
    for d in dates:
        try:
            exp_dt = datetime.strptime(d, "%d%b%y").date()
            dte = (exp_dt - today).days
            if MIN_DTE <= dte <= MAX_DTE:
                logger.info(f"  Suitable expiry: {d}  (DTE={dte})")
                return d
            if dte > MAX_DTE:
                continue
        except ValueError:
            continue
    logger.warning(f"  No expiry found with DTE {MIN_DTE}–{MAX_DTE}. Dates: {dates[:6]}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
class SensexTrendSellerBot:
# ══════════════════════════════════════════════════════════════════════════════
    """
    ADX + RSI + MACD bearish confluence — SENSEX ATM CE seller.

    SHORT-ONLY: streams 1-min SENSEX index ticks via WebSocket, builds
    1-min OHLC bars in-process, and evaluates the 5-condition bearish signal
    on every bar close.  On signal → SELL ATM CE.

    Research basis: Stage 9 multi-instrument study (2026-03-21).
    SENSEX short-only: OOS Sharpe +2.181, WR 73.5% (Oct 2025 – Mar 2026).
    """

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.ws     = None

        # Live price tracking
        self.sensex_ltp: float = 0.0
        self.vix_ltp:    float = 0.0

        # 1-min bar builder
        self.bars: deque         = deque(maxlen=200)
        self._bar_open:  float   = 0.0
        self._bar_high:  float   = 0.0
        self._bar_low:   float   = float("inf")
        self._bar_minute: int    = -1
        self._last_ltp:  float   = 0.0

        # Active CE position (short-only — no PE leg)
        self.active_ce: dict | None = None

        # Session metadata (resolved at 09:15 first tick)
        self.expiry:   str | None = None
        self.lot_size: int        = DEFAULT_LOT_SIZE

        # State flags
        self._session_started  = False
        self._next_init_attempt = datetime.min   # retry gate for session init
        self._eod_exit_done    = False
        self._first_connect    = True
        self._subscribed_syms: set[str] = set()

        # Latest indicator values for state dump / dashboard
        self.indicator_snapshot: dict = {}

        # Decision-state logging (jsonl + throttled heartbeat)
        self._dlog = DecisionLogger(DECISION_LOG, heartbeat_secs=HEARTBEAT_SECS, bot_logger=logger)

        self._watchdog = TickWatchdog(
            bot_name="SENSEX Trend Seller Bot",
            tracked_symbols=lambda: [IDX_SYMBOL, VIX_SYMBOL] + (
                [self.active_ce["symbol"]] if self.active_ce else []
            ),
            market_open=MARKET_OPEN,
            market_close=SESSION_END,
            bot_logger=logger,
        )

        # Restore any active trade from a previous run today
        self._restore_state()

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE RESTORE (mid-session restart recovery)
    # ══════════════════════════════════════════════════════════════════════════

    def _restore_state(self) -> None:
        """
        On startup, reload state saved by _state_dump_loop.
        Restores active_ce and expiry only if the state file was written
        today — prevents ghost positions from a previous trading day.
        """
        if not STATE_FILE.exists():
            return
        try:
            state = json.loads(STATE_FILE.read_text())
            last_update_str = state.get("last_update", "")
            if not last_update_str:
                return
            last_update = datetime.fromisoformat(last_update_str)
            if last_update.date() != datetime.now().date():
                logger.info("_restore_state: state file is from a previous day — skipping restore")
                return

            # Restore expiry for option symbol resolution
            if state.get("expiry"):
                self.expiry = state["expiry"]

            # Restore active CE trade — the `if self.active_ce is not None` guard
            # in _try_entry will prevent duplicate entries automatically
            ce_trade = (state.get("active_trades") or {}).get("CE")
            if ce_trade:
                self.active_ce = ce_trade
                logger.warning(
                    f"🔄 Restored ACTIVE CE trade: "
                    f"{ce_trade.get('symbol')} entry={ce_trade.get('entry_prem')} sl={ce_trade.get('sl_prem')}"
                )
                logger.info("_restore_state: mid-session restart — active CE trade restored")
            else:
                logger.debug("_restore_state: no active CE trade to restore")

        except Exception as e:
            logger.warning(f"_restore_state: could not read state file: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  1-MIN BAR BUILDER
    # ══════════════════════════════════════════════════════════════════════════

    def _update_bar(self, ltp: float, ts: datetime) -> bool:
        """
        Feed one raw tick into the running 1-min bar.
        Returns True the moment a bar is COMPLETED (minute boundary crossed).
        """
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
                self.bars.append({
                    "open":  self._bar_open,
                    "high":  self._bar_high,
                    "low":   self._bar_low,
                    "close": self._last_ltp,
                })
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

    # ══════════════════════════════════════════════════════════════════════════
    #  SIGNAL ENGINE  (SHORT-ONLY)
    # ══════════════════════════════════════════════════════════════════════════

    def _compute_signals(self) -> bool:
        """
        Compute EMA / ADX / RSI / MACD on the completed-bar deque.
        Returns True if the bearish confluence is met (→ sell CE).

        Research-optimal parameters (SENSEX short-only, Stage 9):
            ADX > 25, RSI < 50, ADX-D 7 bars, MACD cross down
        Requires >= MIN_BARS_REQUIRED completed bars.
        """
        if len(self.bars) < MIN_BARS_REQUIRED:
            return False

        df     = pd.DataFrame(list(self.bars))
        closes = df["close"]
        highs  = df["high"]
        lows   = df["low"]

        ema_s   = ta.ema(closes, length=EMA_LEN)
        adx_df  = ta.adx(highs, lows, closes, length=ADX_LEN)
        rsi_s   = ta.rsi(closes, length=RSI_LEN)
        macd_df = ta.macd(closes, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)

        if adx_df is None or macd_df is None or ema_s is None or rsi_s is None:
            return False
        if _ADX_COL not in adx_df.columns:
            return False
        if _MACD_COL not in macd_df.columns or _MACDS_COL not in macd_df.columns:
            return False

        def _val(s, offset=0):
            v = s.iloc[-(1 + offset)]
            return float(v) if not pd.isna(v) else float("nan")

        close      = _val(closes)
        ema        = _val(ema_s)
        adx        = _val(adx_df[_ADX_COL])
        adx_old    = _val(adx_df[_ADX_COL], offset=ADX_RISING_BARS)
        rsi        = _val(rsi_s)
        macd_now   = _val(macd_df[_MACD_COL])
        macds_now  = _val(macd_df[_MACDS_COL])
        macd_prev  = _val(macd_df[_MACD_COL],  offset=1)
        macds_prev = _val(macd_df[_MACDS_COL], offset=1)

        # Any NaN → skip
        if any(v != v for v in [close, ema, adx, adx_old, rsi,
                                  macd_now, macds_now, macd_prev, macds_prev]):
            return False

        adx_strong      = adx > ADX_THRESHOLD
        adx_rising      = adx > adx_old
        macd_cross_down = (macd_prev > macds_prev) and (macd_now <= macds_now)

        # Save snapshot for dashboard regardless of signal
        self.indicator_snapshot = {
            "close":       round(close, 2),
            "ema":         round(ema, 2),
            "adx":         round(adx, 2),
            "adx_old":     round(adx_old, 2),
            "rsi":         round(rsi, 2),
            "macd_line":   round(macd_now, 4),
            "macd_signal": round(macds_now, 4),
            "conditions": {
                "ema_bearish":     bool(close < ema),
                "adx_strong":      bool(adx_strong),
                "adx_rising":      bool(adx_rising),
                "rsi_bearish":     bool(rsi < RSI_SHORT_MAX),
                "macd_cross_down": bool(macd_cross_down),
            },
            "bars_evaluated": len(self.bars),
        }

        # All 5 bearish conditions
        if (close < ema) and adx_strong and adx_rising and (rsi < RSI_SHORT_MAX) and macd_cross_down:
            logger.info(
                f"📶 SHORT signal: SENSEX={close:.1f} < EMA={ema:.1f}, "
                f"ADX={adx:.1f}↑ (vs {adx_old:.1f} 7b ago), "
                f"RSI={rsi:.1f}, MACD×↓  →  SELL CE"
            )
            return True

        # Log when no signal (for debugging)
        logger.info(
            f"[SIG] NO SIGNAL - close<EMA={close<ema}, ADX>{ADX_THRESHOLD}={adx_strong}({adx:.1f}), "
            f"ADX_rising={adx_rising}, RSI<{RSI_SHORT_MAX}={rsi<RSI_SHORT_MAX}({rsi:.1f}), MACD×↓={macd_cross_down}"
        )

        return False

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE ENTRY
    # ══════════════════════════════════════════════════════════════════════════

    async def _enter_trade(self) -> None:
        """
        Resolve the ATM CE symbol, fetch live LTP, place a SELL order.
        SHORT-ONLY: always sells CE on bearish signal.
        """
        if self.active_ce is not None:
            return   # position already open — no duplicate entry

        if not self.expiry:
            logger.warning("  [CE] No suitable expiry today — entry skipped.")
            return

        spot = self.sensex_ltp
        if spot <= 0:
            logger.warning("  [CE] SENSEX LTP is 0 — entry skipped.")
            return

        # Resolve ATM CE symbol via shared api_utils
        symbol = await asyncio.to_thread(
            get_option_symbol,
            API_KEY, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, "CE", "ATM",
        )
        if not symbol:
            logger.error(
                f"  [CE] ATM symbol resolution failed "
                f"(spot={spot:.0f}, expiry={self.expiry})"
            )
            return

        # Fetch live premium via shared atm_resolver
        opt_ltp = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        if opt_ltp <= 0:
            logger.warning(f"  [CE] Option LTP = 0 for {symbol}. Entry skipped.")
            return

        lot_size = _get_lot_size(symbol)
        qty      = N_LOTS * lot_size
        sl_prem  = round(opt_ltp * SL_MULTIPLE, 2)

        logger.info(
            f"  [CE] Entering: {symbol}  LTP=₹{opt_ltp:.2f}  "
            f"qty={qty} ({N_LOTS} lots × {lot_size})  SL=₹{sl_prem:.2f}"
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
            logger.error(f"  [CE] placesmartorder exception: {e}")
            return

        if res.get("status") == "success":
            fill_prem = _resolve_fill(res, opt_ltp)
            sl_prem   = round(fill_prem * SL_MULTIPLE, 2)

            # ── Broker-side SL-M order (resting stop, engages even if the app
            # crashes / websocket drops). ────────────────────────────────────
            sl_order_id = None
            try:
                sl_resp = self.client.placeorder(
                    strategy      = STRATEGY_NAME,
                    symbol        = symbol,
                    action        = "BUY",
                    exchange      = OPT_EXCHANGE,
                    price_type    = "SL-M",
                    trigger_price = str(sl_prem),
                    product       = "MIS",
                    quantity      = str(qty),
                )
            except Exception as e:
                logger.error(f"  [CE] Broker-side SL-M placement exception: {e}")
                sl_resp = None

            if sl_resp and sl_resp.get("status") == "success":
                sl_order_id = str(sl_resp.get("orderid", ""))
                logger.info(f"  [CE] 🛡️ Broker-side SL-M resting @ trigger ₹{sl_prem:.2f}  order_id={sl_order_id}")
            else:
                logger.error(f"  [CE] ⚠️ Broker-side SL-M FAILED to place ({sl_resp}) — falling back to app-side tick monitoring only.")
                await send_async("⚠️ *SENSEX Trend Seller — Broker-side SL-M order failed to place!*\nFalling back to app-side tick monitoring only — slippage risk on SL exit.")

            self.active_ce = {
                "symbol":      symbol,
                "entry_prem":  fill_prem,
                "sl_prem":     sl_prem,
                "sl_order_id": sl_order_id,
                "qty":         qty,
                "lot_size":    lot_size,
                "order_id":    str(res.get("orderid", "")),
                "entry_time":  datetime.now().isoformat(),
            }
            self.lot_size = lot_size

            await self._subscribe(symbol, OPT_EXCHANGE)

            logger.info(
                f"✅ [CE] SOLD {symbol} @ ₹{fill_prem:.2f}  "
                f"({N_LOTS} lots, qty={qty}, order={res.get('orderid')})"
            )
            await send_async(
                f"📉 *SENSEX Trend Seller — ENTRY*\n"
                f"Sold `{symbol}`  ({N_LOTS} lots)\n"
                f"Entry premium : ₹{fill_prem:.2f}\n"
                f"Safety SL     : ₹{sl_prem:.2f}  (2× entry"
                f"{', broker SL-M resting' if sl_order_id else ', ⚠️ app-side only'})\n"
                f"Exit          : EOD 15:14 IST\n"
                f"SENSEX: {spot:.1f}  |  VIX: {self.vix_ltp:.2f}\n"
                f"_Signal: {datetime.now().strftime('%H:%M')}_"
            )
        else:
            logger.error(f"  [CE] Order rejected: {res}")

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE EXIT
    # ══════════════════════════════════════════════════════════════════════════

    async def _close_trade(
        self, exit_prem: float, reason: str, *, already_filled_order_id: str | None = None
    ) -> None:
        """Buy back the sold CE to flatten the short position."""
        if not self.active_ce:
            return

        symbol      = self.active_ce["symbol"]
        qty         = self.active_ce["qty"]
        sl_order_id = self.active_ce.get("sl_order_id")

        # Cancel the resting broker-side SL-M order before taking any other
        # close path — unless it's the one that just filled (nothing to cancel).
        if sl_order_id and sl_order_id != already_filled_order_id:
            await asyncio.to_thread(_cancel_order, self.client, sl_order_id, symbol, OPT_EXCHANGE)

        # Capture trade fields before clearing state.
        entry_prem  = self.active_ce["entry_prem"]
        _entry_time = self.active_ce.get("entry_time")
        _order_id   = self.active_ce.get("order_id")
        _lot_size   = self.active_ce.get("lot_size", qty)

        if already_filled_order_id:
            exit_fill = exit_prem
            order_ok  = True
            logger.info(f"  [CE] SL-M already filled @ ₹{exit_fill:.2f} — no new close order needed.")
        else:
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
                logger.error(f"  [CE] Close order exception: {e}")
                return

            order_ok = res.get("status") == "success"
            if not order_ok:
                # Position may already be closed by sandbox auto-squareoff (15:15 MIS cutoff).
                # Do NOT return early — always log the trade so performance.db stays accurate.
                logger.warning(
                    f"  [CE] Exit order non-success (likely auto-squareoff already "
                    f"closed position): {res}  — logging trade and clearing state."
                )

            # If exit_prem is 0 or equals entry (LTP fetch failed upstream), re-fetch
            # after order placement — the MARKET fill should now be reflected in LTP.
            # This is only the LTP-side fallback; the actual fill price (preferred)
            # is resolved via orderstatus immediately below.
            if exit_prem <= 0 or exit_prem == entry_prem:
                for attempt in range(3):
                    await asyncio.sleep(0.3)
                    fresh = await asyncio.to_thread(
                        get_option_ltp, symbol, OPT_EXCHANGE, API_KEY
                    )
                    if 0 < fresh < entry_prem:   # plausible: option decayed
                        logger.info(f"  Post-order LTP re-fetch: ₹{fresh:.2f} (attempt {attempt + 1})")
                        exit_prem = fresh
                        break
                else:
                    logger.warning(
                        f"  Post-order LTP re-fetch still unreliable "
                        f"(last={exit_prem:.2f}) — gross P&L may be inaccurate in DB."
                    )

            # Resolve actual fill price for the close order (falls back to the
            # LTP snapshot/re-fetch above if the orderstatus lookup fails).
            exit_fill   = _resolve_fill(res, exit_prem)

        gross       = (entry_prem - exit_fill) * qty
        won         = gross > 0
        emoji       = "🟢" if won else "🔴"

        # Clear state unconditionally.
        self.active_ce = None

        auto_sq_note = " _(closed by auto-squareoff)_" if not order_ok else ""
        logger.info(
            f"{emoji} [CE] CLOSED {symbol} @ ₹{exit_fill:.2f}  "
            f"reason={reason}  gross=₹{gross:,.0f}"
        )
        await send_async(
            f"{emoji} *SENSEX Trend Seller — EXIT ({reason})*\n"
            f"Symbol : `{symbol}`\n"
            f"Entry  : ₹{entry_prem:.2f}  →  Exit: ₹{exit_fill:.2f}\n"
            f"Gross P&L: ₹{gross:,.0f}  ({N_LOTS} lots)\n"
            f"_Closed at {datetime.now().strftime('%H:%M:%S')}{auto_sq_note}_"
        )
        log_trade_to_db(
            bot_name      = "sensex_trend_seller_bot",
            instrument    = IDX_SYMBOL,
            option_symbol = symbol,
            option_type   = "CE",
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
        """Close the CE position unconditionally at 15:14 IST."""
        if self._eod_exit_done:
            return
        self._eod_exit_done = True
        logger.info("🔔 EOD 15:14 IST — closing all open positions.")
        if self.active_ce:
            ltp = 0.0
            for attempt in range(5):
                ltp = await asyncio.to_thread(
                    get_option_ltp, self.active_ce["symbol"], OPT_EXCHANGE, API_KEY
                )
                if ltp > 0:
                    break
                logger.warning(f"  EOD LTP fetch returned 0 (attempt {attempt + 1}/5) — retrying…")
                await asyncio.sleep(0.5)
            if ltp <= 0:
                logger.error(
                    "  EOD LTP fetch failed after 5 attempts — "
                    "will use 0.0 so gross P&L is recalculated from fill price post-order."
                )
            await self._close_trade(ltp if ltp > 0 else 0.0, "EOD 15:14")

    # ══════════════════════════════════════════════════════════════════════════
    #  DECISION-STATE LOGGING
    # ══════════════════════════════════════════════════════════════════════════

    def _verdict(self, now_t: dt_time) -> str:
        """What's currently blocking entry — checked in the order these gates
        actually apply in _on_index_tick(), so it always names the real blocker."""
        if not self._session_started:
            return "waiting for session start"
        if self._eod_exit_done:
            return "done for today: EOD exit complete"
        if self.active_ce:
            return (f"ACTIVE: holding CE {self.active_ce.get('symbol')} "
                    f"(SL=₹{self.active_ce.get('sl_prem')}, EOD exit {SESSION_END.strftime('%H:%M')})")
        if now_t < ENTRY_START:
            return "waiting for entry window to open"
        if now_t > ENTRY_END:
            return "entry window closed — no signal fired today"
        if self.vix_ltp > 0 and self.vix_ltp > VIX_MAX:
            return f"BLOCKED: VIX filter (VIX={self.vix_ltp:.2f} > {VIX_MAX})"
        if not self.expiry:
            return "BLOCKED: no suitable expiry found"
        if len(self.bars) < MIN_BARS_REQUIRED:
            return f"warming up: {len(self.bars)}/{MIN_BARS_REQUIRED} bars"
        return "🔥 in window — watching for ADX/RSI/MACD bearish signal"

    def _heartbeat_text(self) -> str:
        now_t = datetime.now().time()
        lines = [
            f"💓 DECISION STATE {datetime.now().strftime('%H:%M:%S')} ─ {self._verdict(now_t)}",
            f"    SENSEX={self.sensex_ltp:.1f}  VIX={self.vix_ltp:.2f}  "
            f"bars={len(self.bars)}  expiry={self.expiry}",
        ]
        if self.indicator_snapshot:
            ind = self.indicator_snapshot
            lines.append(
                f"    close={ind.get('close')}  EMA={ind.get('ema')}  ADX={ind.get('adx')} "
                f"(was {ind.get('adx_old')})  RSI={ind.get('rsi')}  "
                f"MACD={ind.get('macd_line')}/{ind.get('macd_signal')}"
            )
        if self.active_ce:
            lines.append(f"    active: CE {self.active_ce.get('symbol')}")
        return "\n".join(lines)

    # ══════════════════════════════════════════════════════════════════════════
    #  TICK HANDLERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _on_index_tick(self, ltp: float, ts: datetime) -> None:
        """Called for every SENSEX index tick from the WebSocket stream."""
        self.sensex_ltp = ltp
        now             = datetime.now()

        # Throttled to HEARTBEAT_SECS internally — cheap to call on every tick
        self._dlog.maybe_heartbeat(self._heartbeat_text)

        # Latches only when expiry resolves; failed attempts retry every 120s
        # so a transient OpenAlgo outage at open can't disable the bot for the
        # day (incident 2026-07-07).
        if (not self._session_started and now.time() >= MARKET_OPEN
                and now >= self._next_init_attempt):
            await self._session_open(ltp)
            if self.expiry:
                self._session_started = True
            else:
                self._next_init_attempt = now + timedelta(seconds=120)
                logger.warning("  ⚠️  Session init incomplete (no expiry) — retrying in 120s.")

        bar_closed = self._update_bar(ltp, ts)
        if not bar_closed:
            return

        # Compute indicators on every bar close — always run so the dashboard
        # shows live ADX/RSI/MACD regardless of entry window or VIX filter.
        signal = self._compute_signals()    # populates indicator_snapshot

        now_t = now.time()
        self._dlog.log_bar({
            "phase":        "ACTIVE" if self.active_ce else "WATCHING",
            "bar_time":     now_t.strftime("%H:%M"),
            "in_window":    ENTRY_START <= now_t <= ENTRY_END,
            "vix_ltp":      round(self.vix_ltp, 2),
            "bars_loaded":  len(self.bars),
            "expiry":       self.expiry,
            "signal":       bool(signal),
            "indicators":   self.indicator_snapshot,
            "verdict":      self._verdict(now_t),
            "active_trade": self.active_ce.get("symbol") if self.active_ce else None,
        })

        # Entry window guard (block entries outside 10:00–13:00)
        if not (ENTRY_START <= now.time() <= ENTRY_END):
            return

        # Entry guards (block trade, not indicator display)
        if self.vix_ltp > 0 and self.vix_ltp > VIX_MAX:
            return
        if not self.expiry:
            return

        # Compute bearish confluence signal and enter
        if self.active_ce is None and signal:
            await self._enter_trade()

    async def _on_option_tick(self, ltp: float) -> None:
        """Called for CE option ticks — monitors safety SL only."""
        if not self.active_ce or ltp <= 0:
            return

        # Fallback-only — when a broker-side SL-M order is resting, that order
        # is authoritative and is reconciled via _check_sl_order_filled(); this
        # tick-poll branch only fires if SL-M placement failed at entry.
        if not self.active_ce.get("sl_order_id") and ltp >= self.active_ce["sl_prem"]:
            logger.warning(
                f"🛑 [CE] SAFETY SL TRIGGERED: "
                f"LTP=₹{ltp:.2f} ≥ SL=₹{self.active_ce['sl_prem']:.2f}"
            )
            await self._close_trade(ltp, f"Safety SL ({SL_MULTIPLE}×)")

    async def _check_sl_order_filled(self) -> bool:
        """Poll the orderbook for the resting broker-side SL-M order. Returns
        True (and closes the trade) if it has filled."""
        if not self.active_ce or not self.active_ce.get("sl_order_id"):
            return False
        sl_order_id = self.active_ce["sl_order_id"]
        filled, fill_price = await asyncio.to_thread(
            _check_fill, self.client, sl_order_id, self.active_ce["symbol"], OPT_EXCHANGE
        )
        if not filled:
            return False
        logger.warning(f"🛑 [CE] Broker-side SL-M filled @ ₹{fill_price:.2f}")
        await self._close_trade(
            fill_price, f"Safety SL ({SL_MULTIPLE}×, broker)",
            already_filled_order_id=sl_order_id,
        )
        return True

    # ══════════════════════════════════════════════════════════════════════════
    #  SESSION INITIALISATION & WARM-UP
    # ══════════════════════════════════════════════════════════════════════════

    async def _session_open(self, spot: float) -> None:
        """Called once at 09:15 first tick or on restart. Resolves today's expiry and lot size."""
        # 0. Holiday Check
        if is_market_holiday(API_KEY):
            logger.info("⛔ Market Holiday detected. Skipping session initialization.")
            return

        logger.info(f"🔔 Session open. SENSEX spot ≈ {spot:.0f}")

        # 0.1 Online Notification
        if self._first_connect:
            self._first_connect = False
            await send_async(
                f"🤖 *SENSEX Trend Seller Bot — Online*\n"
                f"SHORT-ONLY: ADX(14)>{ADX_THRESHOLD:.0f}↑({ADX_RISING_BARS}b) + "
                f"RSI<{RSI_SHORT_MAX:.0f} + MACD(5,13,3)×↓ → Sell ATM CE\n"
                f"Entry window : {ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')} IST\n"
                f"Filters      : VIX≤{VIX_MAX}  |  DTE {MIN_DTE}–{MAX_DTE}\n"
                f"Quantity     : {N_LOTS} lots  (lot size fetched dynamically)\n"
                f"Safety SL    : {SL_MULTIPLE}× entry premium\n"
                f"Exit         : EOD 15:14 IST\n"
                f"Research     : Stage 9 OOS Sharpe +2.181, WR 73.5%"
            )

        self.expiry = await asyncio.to_thread(_get_suitable_expiry)

        if self.expiry:
            sample_sym = await asyncio.to_thread(
                get_option_symbol,
                API_KEY, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, "CE", "ATM",
            )
            if sample_sym:
                self.lot_size = _get_lot_size(sample_sym)
                logger.info(f"  Expiry: {self.expiry}  |  Lot size: {self.lot_size}")
        else:
            logger.warning(
                "  No suitable expiry found (DTE 2–7). "
                "No entries will be made today."
            )

    async def _warmup_history(self) -> None:
        """
        Pre-load 7 days of SENSEX 1-min history via api_utils.get_history
        so that EMA/ADX/RSI/MACD indicators are warm before the 10:00 entry window.
        """
        logger.info("📡 Warming up SENSEX 1-min history (7 days)…")
        try:
            raw = await asyncio.to_thread(
                get_history, API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "1m", 7
            )
            if not raw:
                logger.warning("  ⚠️ No history returned — indicators will warm on live ticks.")
                return

            df = pd.DataFrame(raw)
            if "timestamp" in df.columns:
                df["dt"] = (
                    pd.to_datetime(df["timestamp"], unit="s", utc=True)
                    .dt.tz_convert("Asia/Kolkata")
                    .dt.tz_localize(None)
                )
            elif "date" in df.columns:
                df["dt"] = pd.to_datetime(df["date"])
            else:
                logger.warning("  ⚠️ History format unrecognised — skipping warm-up.")
                return

            df = df.sort_values("dt").tail(200)
            for _, row in df.iterrows():
                c = float(row.get("close", 0))
                self.bars.append({
                    "open":  float(row.get("open",  c)),
                    "high":  float(row.get("high",  c)),
                    "low":   float(row.get("low",   c)),
                    "close": c,
                })
            if self.bars:
                self.sensex_ltp = self.bars[-1]["close"]
            logger.info(
                f"  ✅ Warm-up complete: {len(self.bars)} bars loaded. "
                f"Last close ≈ {self.sensex_ltp:.0f}"
            )
        except Exception as e:
            logger.error(f"  History warm-up error: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  WEBSOCKET HELPERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _subscribe(self, symbol: str, exchange: str) -> None:
        """Subscribe to a symbol. Idempotent — skips already-subscribed symbols."""
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
        """Re-subscribe to all required symbols after a reconnect."""
        self._subscribed_syms.clear()
        await self._subscribe(IDX_SYMBOL, IDX_EXCHANGE)
        await self._subscribe(VIX_SYMBOL, VIX_EXCHANGE)
        if self.active_ce:
            await self._subscribe(self.active_ce["symbol"], OPT_EXCHANGE)

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE PERSISTENCE  (dashboard & Telegram /status)
    # ══════════════════════════════════════════════════════════════════════════

    async def _state_dump_loop(self) -> None:
        """Write live state to JSON every 2s — read by telegram_status.py."""
        while True:
            if self.active_ce and self.active_ce.get("sl_order_id"):
                try:
                    await self._check_sl_order_filled()
                except Exception as e:
                    logger.warning(f"  [CE] SL-M reconciliation check failed: {e}")
            try:
                STATE_FILE.write_text(json.dumps({
                    "strategy":      STRATEGY_NAME,
                    "last_update":   datetime.now().isoformat(),
                    "sensex_ltp":    round(self.sensex_ltp, 2),
                    "vix_ltp":       round(self.vix_ltp,    2),
                    "vix_filter":    f"≤{VIX_MAX}",
                    "expiry":        self.expiry,
                    "lot_size":      self.lot_size,
                    "n_lots":        N_LOTS,
                    "entry_window":  f"{ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')}",
                    "bars_loaded":   len(self.bars),
                    "indicators":    self.indicator_snapshot,
                    "active_trades": {
                        "CE": {
                            "symbol":      self.active_ce["symbol"],
                            "entry_prem":  self.active_ce["entry_prem"],
                            "sl_prem":     self.active_ce["sl_prem"],
                            "sl_order_id": self.active_ce.get("sl_order_id"),
                            "qty":         self.active_ce["qty"],
                            "lot_size":    self.active_ce.get("lot_size"),
                            "order_id":    self.active_ce.get("order_id"),
                            "entry_time":  self.active_ce["entry_time"],
                        } if self.active_ce else None
                    },
                }, default=str))
            except Exception:
                pass
            await asyncio.sleep(2)

    # ══════════════════════════════════════════════════════════════════════════
    #  MAIN WEBSOCKET LOOP
    # ══════════════════════════════════════════════════════════════════════════

    async def main_loop(self) -> None:
        """
        Bot entry point.
        1. Pre-market history warm-up (7 days SENSEX 1-min).
        2. Start background state-dump task.
        3. Connect to OpenAlgo WebSocket with auto-reconnect (exponential backoff).
        4. Stream ticks until EOD (15:20), then close CE position and exit.
        """
        await self._warmup_history()
        asyncio.create_task(self._state_dump_loop())
        asyncio.create_task(self._watchdog.watch_loop())

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

                        msg = json.loads(raw)
                        if msg.get("type") != "market_data":
                            continue

                        sym   = msg.get("symbol", "")
                        self._watchdog.on_tick(sym)
                        mdata = msg.get("data", {})
                        ltp   = float(mdata.get("ltp", 0) or mdata.get("lp", 0))
                        ts_raw = mdata.get("t")
                        ts     = (datetime.fromtimestamp(float(ts_raw))
                                  if ts_raw else datetime.now())

                        if ltp <= 0:
                            continue

                        if sym in (IDX_SYMBOL, f"{IDX_EXCHANGE}:{IDX_SYMBOL}"):
                            await self._on_index_tick(ltp, ts)

                        elif sym in (VIX_SYMBOL, f"{VIX_EXCHANGE}:{VIX_SYMBOL}"):
                            self.vix_ltp = ltp

                        elif self.active_ce and self.active_ce.get("symbol") == sym:
                            await self._on_option_tick(ltp)

            except Exception as e:
                logger.warning(
                    f"WebSocket error: {e}. Reconnecting in {retry_delay}s…"
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    bot = SensexTrendSellerBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("Stop signal received — bot terminated.")
