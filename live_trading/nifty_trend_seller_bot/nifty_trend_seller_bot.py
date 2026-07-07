"""
Nifty Trend Seller Bot
======================
live_trading/nifty_trend_seller_bot/nifty_trend_seller_bot.py

Sells the ATM counter option when all 5 confluence conditions align on
1-minute NIFTY index bars:

    Long signal  (bullish)  →  SELL ATM PE  (premium decay as market rises)
    Short signal (bearish)  →  SELL ATM CE  (premium decay as market falls)

Both legs can be open simultaneously — independent positions.
All open positions exit unconditionally at 15:14 IST (EOD).

Entry Conditions (all 5 required, evaluated on each completed 1-min bar close):
    1. Close > EMA(20)           [trend direction — reversed for short: close < EMA]
    2. ADX(14) > 30              [trend is strong, not sideways]
    3. ADX[now] > ADX[5 bars ago][trend accelerating — ADX-D definition]
    4. RSI(14) > 55 (long)       [momentum confirms — < 45 for short]
    5. MACD(5,13,3) signal-line cross upward [timing trigger — downward for short]

Filters applied before entry:
    • Entry window   : 10:00 – 13:00 IST only
    • VIX filter     : INDIAVIX ≤ 22  (subscribed via WebSocket)
    • DTE range      : 2 – 7 days to expiry  (skip if no suitable contract today)
    • One position per leg  : no duplicate PE; no duplicate CE

Risk management:
    • Safety SL      : 2× entry premium  (< 3% of trades triggered historically)
    • No intraday profit target — theta decay to EOD is the edge

Quantity:
    • 10 lots × dynamic lot size  (fetched from OpenAlgo token DB at session open)
    • Lot size updates automatically on each session start — no code change needed

Shared utilities (no duplication of existing code):
    live_trading.shared.atm_resolver     — get_atm_strike, get_option_ltp
    live_trading.shared.telegram_notifier— send_async
    live_trading.api_utils               — get_expiry_dates, get_option_symbol, get_history
    database.token_db.get_symbol_info    — dynamic lot size (same as candle_breaker_bot)
    openalgo.api                         — placesmartorder

Trade mode (Analyze / Live) is set in the OpenAlgo app — not in this bot.
The bot calls placesmartorder normally; OpenAlgo routes accordingly.
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

from live_trading.api_utils                  import get_expiry_dates, get_option_symbol, get_history
from live_trading.shared.atm_resolver        import get_option_ltp
from live_trading.shared.telegram_notifier   import send_async
from live_trading.shared.trade_logger        import log_trade_to_db
from live_trading.shared.order_fill          import fetch_fill_price

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "nifty_trend_seller_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Environment ───────────────────────────────────────────────────────────────
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER",   "http://127.0.0.1:5001")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")

if not API_KEY:
    logger.error("❌ OPENALGO_API_KEY not found in environment. Please check .env. Exiting.")
    sys.exit(1)

# ── Strategy constants ────────────────────────────────────────────────────────
STRATEGY_NAME       = "NIFTY_TREND_SELLER"


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback


IDX_SYMBOL          = "NIFTY"
IDX_EXCHANGE        = "NSE_INDEX"
VIX_SYMBOL          = "INDIAVIX"
VIX_EXCHANGE        = "NSE_INDEX"
OPT_EXCHANGE        = "NFO"

N_LOTS              = 10            # number of lots per leg
DEFAULT_LOT_SIZE    = 65            # fallback if DB lookup fails

# Indicator parameters (champion config from research)
EMA_LEN             = 20
ADX_LEN             = 14
RSI_LEN             = 14
MACD_FAST           = 5
MACD_SLOW           = 13
MACD_SIG            = 3
ADX_RISING_BARS     = 5             # ADX-D: compare now vs 5 bars ago

# Entry filters
ADX_THRESHOLD       = 30.0
RSI_LONG_MIN        = 55.0          # RSI > 55 for long
RSI_SHORT_MAX       = 45.0          # RSI < 45 for short
VIX_MAX             = 22.0
MIN_DTE             = 2
MAX_DTE             = 7
SL_MULTIPLE         = 2.0           # SL at 2× entry premium

# Session timing
MARKET_OPEN         = dt_time(9,  15)
ENTRY_START         = dt_time(10,  0)
ENTRY_END           = dt_time(13,  0)
SESSION_END         = dt_time(15, 14)

# Minimum completed bars before signals are generated (warm-up guard)
MIN_BARS_REQUIRED   = 50

# State file for Telegram /status dashboard
STATE_FILE          = LOGS_DIR / "nifty_trend_seller_state.json"

# MACD / ADX pandas_ta column names
_MACD_COL   = f"MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
_MACDS_COL  = f"MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
_ADX_COL    = f"ADX_{ADX_LEN}"


# ── Helper: dynamic lot size (identical pattern to candle_breaker_bot) ────────
def _get_lot_size(symbol: str) -> int:
    """
    Fetch current lot size from the OpenAlgo token database.
    Falls back to DEFAULT_LOT_SIZE if the DB lookup fails.
    This is the same approach used by candle_breaker_bot, so lot-size
    changes (SEBI/NSE mandated) are picked up automatically on the next
    session start without any code change.
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


# ── Helper: expiry selection with DTE 2–7 ────────────────────────────────────
def _get_suitable_expiry() -> str | None:
    """
    Return the nearest NIFTY weekly expiry whose DTE falls within [MIN_DTE, MAX_DTE].
    Uses api_utils.get_expiry_dates (shared utility) — no duplication.
    Returns the OpenAlgo DDMMMYY string (e.g. "27MAR26") or None.
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
                # Dates are sorted ascending; further dates will only be larger DTE
                continue
        except ValueError:
            continue
    logger.warning(f"  No expiry found with DTE {MIN_DTE}–{MAX_DTE}. Dates: {dates[:6]}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
class NiftyTrendSellerBot:
# ══════════════════════════════════════════════════════════════════════════════
    """
    ADX + RSI + MACD confluence — NIFTY ATM counter-option seller.
    Streams 1-min NIFTY index ticks via WebSocket, builds 1-min OHLC bars
    in-process, and evaluates the 5-condition signal on every bar close.
    """

    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.ws     = None

        # Live price tracking
        self.nifty_ltp: float = 0.0
        self.vix_ltp:   float = 0.0

        # 1-min bar builder
        self.bars: deque        = deque(maxlen=200)   # completed bars (OHLC dicts)
        self._bar_open:  float  = 0.0
        self._bar_high:  float  = 0.0
        self._bar_low:   float  = float("inf")
        self._bar_minute: int   = -1                  # last completed minute index
        self._last_ltp:  float  = 0.0                 # last tick before bar close

        # Active positions — keyed by opt_type; both legs independent
        self.active_trades: dict[str, dict | None] = {"PE": None, "CE": None}

        # Session metadata (resolved at 09:15 first tick)
        self.expiry:     str | None = None
        self.lot_size:   int        = DEFAULT_LOT_SIZE

        # State flags
        self._session_started  = False
        self._next_init_attempt = datetime.min   # retry gate for session init
        self._eod_exit_done    = False
        self._first_connect    = True
        self._subscribed_syms: set[str] = set()

        # Latest indicator values — updated on each bar close, read by state dump
        self.indicator_snapshot: dict = {}

        # Per-leg running P&L counters (reset at bot restart; for Apr-May 2026 review)
        self.pe_pnl_total:  float = 0.0
        self.ce_pnl_total:  float = 0.0
        self.pe_trades:     int   = 0
        self.ce_trades:     int   = 0
        self.pe_wins:       int   = 0
        self.ce_wins:       int   = 0

        # Restore any active trades from a previous run today
        self._restore_state()

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE RESTORE (mid-session restart recovery)
    # ══════════════════════════════════════════════════════════════════════════

    def _restore_state(self) -> None:
        """
        On startup, reload state saved by _state_dump_loop.
        Restores active_trades and expiry only if the state file was written
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

            # Restore expiry so option symbol resolution works correctly
            if state.get("expiry"):
                self.expiry = state["expiry"]

            # Restore active trades — the existing `if self.active_trades[opt_type] is not None`
            # guard in _try_entry will prevent duplicate entries automatically
            saved_trades = state.get("active_trades", {})
            restored_legs = []
            for opt_type in ("PE", "CE"):
                trade = saved_trades.get(opt_type)
                if trade:
                    self.active_trades[opt_type] = trade
                    restored_legs.append(opt_type)
                    logger.warning(
                        f"🔄 Restored ACTIVE {opt_type} trade: "
                        f"{trade.get('symbol')} entry={trade.get('entry_prem')} sl={trade.get('sl_prem')}"
                    )

            if restored_legs:
                logger.info(f"_restore_state: mid-session restart — restored legs: {restored_legs}")
            else:
                logger.debug("_restore_state: no active trades to restore")

        except Exception as e:
            logger.warning(f"_restore_state: could not read state file: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    #  1-MIN BAR BUILDER
    # ══════════════════════════════════════════════════════════════════════════

    def _update_bar(self, ltp: float, ts: datetime) -> bool:
        """
        Feed one raw tick into the running 1-min bar.
        Returns True the moment a bar is COMPLETED (minute boundary crossed).
        The completed bar is appended to self.bars before returning.
        """
        cur_min = ts.hour * 60 + ts.minute

        if self._bar_minute == -1:
            # First tick ever — initialise running bar
            self._bar_minute = cur_min
            self._bar_open   = ltp
            self._bar_high   = ltp
            self._bar_low    = ltp
            self._last_ltp   = ltp
            return False

        if cur_min != self._bar_minute:
            # Minute boundary: close previous bar and start a new one
            if self._bar_open > 0:
                self.bars.append({
                    "open":  self._bar_open,
                    "high":  self._bar_high,
                    "low":   self._bar_low,
                    "close": self._last_ltp,   # last known price of the completed bar
                })

            self._bar_minute = cur_min
            self._bar_open   = ltp
            self._bar_high   = ltp
            self._bar_low    = ltp
            self._last_ltp   = ltp
            return True   # ← bar completed

        # Same minute — update running bar
        self._bar_high = max(self._bar_high, ltp)
        self._bar_low  = min(self._bar_low,  ltp)
        self._last_ltp = ltp
        return False

    # ══════════════════════════════════════════════════════════════════════════
    #  SIGNAL ENGINE
    # ══════════════════════════════════════════════════════════════════════════

    def _compute_signals(self) -> list[str]:
        """
        Compute EMA / ADX / RSI / MACD on the completed-bar deque.
        Returns a list of option types to sell: ["PE"], ["CE"], ["PE","CE"], or [].

        All 5 conditions must be true simultaneously.  Requires >= MIN_BARS_REQUIRED bars.
        """
        if len(self.bars) < MIN_BARS_REQUIRED:
            return []

        df      = pd.DataFrame(list(self.bars))
        closes  = df["close"]
        highs   = df["high"]
        lows    = df["low"]

        # ── Indicators (all from pandas_ta — no duplication of indicator code) ──
        ema_s   = ta.ema(closes, length=EMA_LEN)
        adx_df  = ta.adx(highs, lows, closes, length=ADX_LEN)
        rsi_s   = ta.rsi(closes, length=RSI_LEN)
        macd_df = ta.macd(closes, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)

        # Guard: if any indicator returned None (not enough history yet)
        if adx_df is None or macd_df is None or ema_s is None or rsi_s is None:
            return []
        for col in (_ADX_COL, _MACD_COL, _MACDS_COL):
            if col not in adx_df.columns and col not in macd_df.columns:
                return []

        # ── Extract latest values ──────────────────────────────────────────────
        def _val(s, offset=0):
            idx = -(1 + offset)
            v = s.iloc[idx]
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

        # Any NaN → bail
        if any(
            v != v   # nan != nan is True
            for v in [close, ema, adx, adx_old, rsi, macd_now, macds_now, macd_prev, macds_prev]
        ):
            return []

        # ── Five conditions ────────────────────────────────────────────────────
        adx_strong       = adx > ADX_THRESHOLD
        adx_rising       = adx > adx_old                       # ADX-D: vs 5 bars ago
        macd_cross_up    = (macd_prev < macds_prev) and (macd_now >= macds_now)
        macd_cross_down  = (macd_prev > macds_prev) and (macd_now <= macds_now)

        signals = []

        # LONG confluence → sell PE (counter option)
        if (close > ema) and adx_strong and adx_rising and (rsi > RSI_LONG_MIN) and macd_cross_up:
            signals.append("PE")
            logger.info(
                f"📶 LONG signal: close={close:.1f} > EMA={ema:.1f}, "
                f"ADX={adx:.1f}↑, RSI={rsi:.1f}, MACD×↑  →  SELL PE"
            )

        # SHORT confluence → sell CE (counter option)
        if (close < ema) and adx_strong and adx_rising and (rsi < RSI_SHORT_MAX) and macd_cross_down:
            signals.append("CE")
            logger.info(
                f"📶 SHORT signal: close={close:.1f} < EMA={ema:.1f}, "
                f"ADX={adx:.1f}↑, RSI={rsi:.1f}, MACD×↓  →  SELL CE"
            )

        # Log when no signal (for debugging)
        if not signals:
            logger.info(
                f"[SIG] NO SIGNAL - LONG: close>EMA={close>ema}, ADX>{ADX_THRESHOLD}={adx_strong}, "
                f"ADX_rising={adx_rising}, RSI>{RSI_LONG_MIN}={rsi>RSI_LONG_MIN}, MACD×↑={macd_cross_up} | "
                f"SHORT: close<EMA={close<ema}, RSI<{RSI_SHORT_MAX}={rsi<RSI_SHORT_MAX}, MACD×↓={macd_cross_down}"
            )

        # Persist indicator snapshot for the state dump / dashboard
        self.indicator_snapshot = {
            "close":        round(close, 2),
            "ema":          round(ema, 2),
            "adx":          round(adx, 2),
            "adx_old":      round(adx_old, 2),
            "rsi":          round(rsi, 2),
            "macd_line":    round(macd_now, 4),
            "macd_signal":  round(macds_now, 4),
            "conditions": {
                "ema_ok_long":   bool(close > ema),
                "ema_ok_short":  bool(close < ema),
                "adx_strong":    bool(adx > ADX_THRESHOLD),
                "adx_rising":    bool(adx > adx_old),
                "rsi_bull":      bool(rsi > RSI_LONG_MIN),
                "rsi_bear":      bool(rsi < RSI_SHORT_MAX),
                "macd_cross_up": bool(macd_cross_up),
                "macd_cross_dn": bool(macd_cross_down),
            },
            "bars_evaluated": len(self.bars),
        }

        return signals

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE ENTRY
    # ══════════════════════════════════════════════════════════════════════════

    async def _enter_trade(self, opt_type: str) -> None:
        """
        Resolve the ATM option symbol, fetch live LTP, place a SELL (short) order.
        opt_type: "PE" for long-signal leg, "CE" for short-signal leg.

        Uses:
          api_utils.get_option_symbol    — symbol resolution    (shared)
          shared.atm_resolver.get_option_ltp — live premium     (shared)
          database.token_db.get_symbol_info  — lot size         (shared pattern)
          openalgo.api.placesmartorder       — order placement  (shared)
        """
        if self.active_trades[opt_type] is not None:
            return   # already have this leg open — no double entry

        if not self.expiry:
            logger.warning(f"  [{opt_type}] No suitable expiry today — entry skipped.")
            return

        spot = self.nifty_ltp
        if spot <= 0:
            return

        # ── Resolve ATM symbol via shared api_utils ───────────────────────────
        symbol = await asyncio.to_thread(
            get_option_symbol,
            API_KEY, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, opt_type, "ATM",
        )
        if not symbol:
            logger.error(
                f"  [{opt_type}] ATM symbol resolution failed "
                f"(spot={spot:.0f}, expiry={self.expiry})"
            )
            return

        # ── Fetch live premium via shared atm_resolver ────────────────────────
        opt_ltp = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        if opt_ltp <= 0:
            logger.warning(f"  [{opt_type}] Option LTP = 0 for {symbol}. Entry skipped.")
            return

        # ── Dynamic lot size — same pattern as candle_breaker_bot ────────────
        lot_size = _get_lot_size(symbol)
        qty      = N_LOTS * lot_size
        sl_prem  = round(opt_ltp * SL_MULTIPLE, 2)

        logger.info(
            f"  [{opt_type}] Entering: {symbol}  LTP=₹{opt_ltp:.2f}  "
            f"qty={qty} ({N_LOTS} lots × {lot_size})  SL=₹{sl_prem:.2f}"
        )

        # ── Place order (SELL to open short; position_size negative = short) ──
        try:
            res = self.client.placesmartorder(
                strategy     = STRATEGY_NAME,
                symbol       = symbol,
                action       = "SELL",
                exchange     = OPT_EXCHANGE,
                price_type   = "MARKET",
                product      = "MIS",
                quantity     = qty,
                position_size= -qty,   # negative = short position target
            )
        except Exception as e:
            logger.error(f"  [{opt_type}] placesmartorder exception: {e}")
            return

        if res.get("status") == "success":
            fill_prem = _resolve_fill(res, opt_ltp)
            sl_prem   = round(fill_prem * SL_MULTIPLE, 2)
            self.active_trades[opt_type] = {
                "symbol":      symbol,
                "entry_prem":  fill_prem,
                "sl_prem":     sl_prem,
                "qty":         qty,
                "lot_size":    lot_size,
                "order_id":    str(res.get("orderid", "")),
                "entry_time":  datetime.now().isoformat(),
                "opt_type":    opt_type,
            }
            self.lot_size = lot_size

            # Subscribe to option feed for SL monitoring
            await self._subscribe(symbol, OPT_EXCHANGE)

            logger.info(
                f"✅ [{opt_type}] SOLD {symbol} @ ₹{fill_prem:.2f}  "
                f"({N_LOTS} lots, qty={qty}, order={res.get('orderid')})"
            )
            await send_async(
                f"📉 *Nifty Trend Seller — ENTRY*\n"
                f"Sold `{symbol}`  ({N_LOTS} lots)\n"
                f"Entry premium : ₹{fill_prem:.2f}\n"
                f"Safety SL     : ₹{sl_prem:.2f}  (2× entry)\n"
                f"Exit          : EOD 15:14 IST\n"
                f"NIFTY: {spot:.1f}  |  VIX: {self.vix_ltp:.2f}\n"
                f"_Signal: {datetime.now().strftime('%H:%M')}_"
            )
        else:
            logger.error(f"  [{opt_type}] Order rejected: {res}")

    # ══════════════════════════════════════════════════════════════════════════
    #  TRADE EXIT
    # ══════════════════════════════════════════════════════════════════════════

    async def _close_trade(self, opt_type: str, exit_prem: float, reason: str) -> None:
        """
        Buy back the sold option to flatten the short position.
        Uses openalgo.api.placesmartorder — same shared client pattern.
        """
        trade = self.active_trades.get(opt_type)
        if not trade:
            return

        symbol = trade["symbol"]
        qty    = trade["qty"]

        # Use placeorder (NOT placesmartorder) for exits.
        # placesmartorder(position_size=0) reads the broker's NET position across ALL
        # strategies — if HA Options Bot also has a short on the same symbol, the smart
        # order will close both at once, leaving NTS's position ghost-open internally.
        try:
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
            logger.error(f"  [{opt_type}] Close order exception: {e}")
            return

        order_ok = res.get("status") == "success"
        if not order_ok:
            # Position may already be closed by sandbox auto-squareoff (15:15 MIS cutoff).
            # Do NOT return early — always log the trade so performance.db stays accurate.
            logger.warning(
                f"  [{opt_type}] Exit order non-success (likely auto-squareoff already "
                f"closed position): {res}  — logging trade and clearing state."
            )

        # Resolve actual fill price for the close order (falls back to the
        # LTP snapshot the caller used to trigger this exit if unavailable).
        exit_fill   = _resolve_fill(res, exit_prem)

        # Capture trade fields before clearing state.
        gross       = (trade["entry_prem"] - exit_fill) * qty
        won         = gross > 0
        emoji       = "🟢" if won else "🔴"
        _entry_time = trade.get("entry_time")
        _order_id   = trade.get("order_id")
        _lot_size   = trade.get("lot_size", qty)
        entry_prem  = trade["entry_prem"]

        # Clear state unconditionally.
        self.active_trades[opt_type] = None

        # ── Update per-leg running counters ────────────────────────────────
        if opt_type == "PE":
            self.pe_pnl_total += gross
            self.pe_trades    += 1
            if won:
                self.pe_wins  += 1
        else:
            self.ce_pnl_total += gross
            self.ce_trades    += 1
            if won:
                self.ce_wins  += 1

        auto_sq_note = " _(closed by auto-squareoff)_" if not order_ok else ""
        logger.info(
            f"{emoji} [{opt_type}] CLOSED {symbol} @ ₹{exit_fill:.2f}  "
            f"reason={reason}  gross=₹{gross:,.0f}"
        )
        await send_async(
            f"{emoji} *Nifty Trend Seller — EXIT ({reason})*\n"
            f"Symbol : `{symbol}`\n"
            f"Entry  : ₹{entry_prem:.2f}  →  Exit: ₹{exit_fill:.2f}\n"
            f"Gross P&L: ₹{gross:,.0f}  ({N_LOTS} lots)\n"
            f"_Closed at {datetime.now().strftime('%H:%M:%S')}{auto_sq_note}_"
        )
        log_trade_to_db(
            bot_name      = "nifty_trend_seller_bot",
            instrument    = IDX_SYMBOL,
            option_symbol = symbol,
            option_type   = opt_type,
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
        """Close all open positions unconditionally at 15:14 IST."""
        if self._eod_exit_done:
            return
        self._eod_exit_done = True
        logger.info("🔔 EOD 15:14 IST — closing all open positions.")
        for opt_type in ("PE", "CE"):
            trade = self.active_trades.get(opt_type)
            if trade:
                ltp = await asyncio.to_thread(
                    get_option_ltp, trade["symbol"], OPT_EXCHANGE, API_KEY
                )
                exit_p = ltp if ltp > 0 else trade["entry_prem"]
                await self._close_trade(opt_type, exit_p, "EOD 15:14")

    # ══════════════════════════════════════════════════════════════════════════
    #  TICK HANDLERS
    # ══════════════════════════════════════════════════════════════════════════

    async def _on_index_tick(self, ltp: float, ts: datetime) -> None:
        """Called for every NIFTY index tick from the WebSocket stream."""
        self.nifty_ltp = ltp
        now            = datetime.now()

        # One-time session initialisation at market open — latches only when
        # expiry resolves; failed attempts retry every 120s so a transient
        # OpenAlgo outage at open can't disable the bot for the day
        # (incident 2026-07-07).
        if (not self._session_started and now.time() >= MARKET_OPEN
                and now >= self._next_init_attempt):
            await self._session_open(ltp)
            if self.expiry:
                self._session_started = True
            else:
                self._next_init_attempt = now + timedelta(seconds=120)
                logger.warning("  ⚠️  Session init incomplete (no expiry) — retrying in 120s.")

        # Feed tick into 1-min bar builder
        bar_closed = self._update_bar(ltp, ts)
        if not bar_closed:
            return   # bar still in progress — wait for close

        # ── Compute indicators on every bar close ───────────────────────────
        # Always run so the dashboard shows live ADX/RSI/MACD regardless of
        # whether we are inside the entry window or not.
        signals = self._compute_signals()   # populates indicator_snapshot

        # ── Entry window guard (block entries outside 10:00–13:00) ──────────
        if not (ENTRY_START <= now.time() <= ENTRY_END):
            return

        # ── Entry guards (block trade entry, not indicator display) ──────────
        if self.vix_ltp > 0 and self.vix_ltp > VIX_MAX:
            return
        if not self.expiry:
            return

        # ── Enter if conditions met ──────────────────────────────────────────
        for opt_type in signals:
            if self.active_trades[opt_type] is None:
                await self._enter_trade(opt_type)

    async def _on_option_tick(self, ltp: float, opt_type: str) -> None:
        """Called for option ticks — monitors safety SL only."""
        trade = self.active_trades.get(opt_type)
        if not trade or ltp <= 0:
            return

        if ltp >= trade["sl_prem"]:
            logger.warning(
                f"🛑 [{opt_type}] SAFETY SL TRIGGERED: "
                f"LTP=₹{ltp:.2f} ≥ SL=₹{trade['sl_prem']:.2f}"
            )
            await self._close_trade(opt_type, ltp, f"Safety SL ({SL_MULTIPLE}×)")

    # ══════════════════════════════════════════════════════════════════════════
    #  SESSION INITIALISATION & WARM-UP
    # ══════════════════════════════════════════════════════════════════════════

    async def _session_open(self, spot: float) -> None:
        """
        Called once at 09:15 first tick.
        Resolves today's expiry and caches lot size.
        """
        logger.info(f"🔔 Session open. NIFTY spot ≈ {spot:.0f}")
        self.expiry = await asyncio.to_thread(_get_suitable_expiry)

        if self.expiry:
            # Resolve a representative ATM symbol to probe lot size
            sample_sym = await asyncio.to_thread(
                get_option_symbol,
                API_KEY, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, "PE", "ATM",
            )
            if sample_sym:
                self.lot_size = _get_lot_size(sample_sym)
                logger.info(
                    f"  Expiry: {self.expiry}  |  Lot size: {self.lot_size}"
                )
        else:
            logger.warning(
                "  No suitable expiry found (DTE 2–7). "
                "No entries will be made today."
            )

    async def _warmup_history(self) -> None:
        """
        Pre-load 7 days of NIFTY 1-min history via api_utils.get_history
        so that EMA/ADX/RSI/MACD indicators are fully warm before 10:00.
        Uses the shared get_history utility — no duplication.
        """
        logger.info("📡 Warming up NIFTY 1-min history (7 days)…")
        try:
            raw = await asyncio.to_thread(
                get_history, API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "1m", 7
            )
            if not raw:
                logger.warning(
                    "  ⚠️ No history returned — indicators will warm on live ticks."
                )
                return

            df = pd.DataFrame(raw)
            # OpenAlgo history returns timestamp (epoch) or date string
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
                self.nifty_ltp = self.bars[-1]["close"]
            logger.info(
                f"  ✅ Warm-up complete: {len(self.bars)} bars loaded. "
                f"Last close ≈ {self.nifty_ltp:.0f}"
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
        for opt_type, trade in self.active_trades.items():
            if trade:
                await self._subscribe(trade["symbol"], OPT_EXCHANGE)

    # ══════════════════════════════════════════════════════════════════════════
    #  STATE PERSISTENCE  (dashboard & Telegram /status)
    # ══════════════════════════════════════════════════════════════════════════

    async def _state_dump_loop(self) -> None:
        """Write live state to JSON every 2 s — read by telegram_status.py."""
        while True:
            try:
                STATE_FILE.write_text(json.dumps({
                    "strategy":      STRATEGY_NAME,
                    "last_update":   datetime.now().isoformat(),
                    "nifty_ltp":     round(self.nifty_ltp, 2),
                    "vix_ltp":       round(self.vix_ltp,   2),
                    "vix_filter":    f"≤{VIX_MAX}",
                    "expiry":        self.expiry,
                    "lot_size":      self.lot_size,
                    "n_lots":        N_LOTS,
                    "entry_window":  f"{ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')}",
                    "bars_loaded":      len(self.bars),
                    "indicators":       self.indicator_snapshot,
                    "active_trades": {
                        k: {
                            "symbol":     v["symbol"],
                            "entry_prem": v["entry_prem"],
                            "sl_prem":    v["sl_prem"],
                            "qty":        v["qty"],
                            "entry_time": v["entry_time"],
                        }
                        for k, v in self.active_trades.items() if v is not None
                    },
                    # Per-leg running totals (since bot start — for Apr-May 2026 review)
                    "pe_summary": {
                        "trades":    self.pe_trades,
                        "wins":      self.pe_wins,
                        "win_rate":  round(self.pe_wins / self.pe_trades, 3) if self.pe_trades else None,
                        "pnl_total": round(self.pe_pnl_total, 2),
                    },
                    "ce_summary": {
                        "trades":    self.ce_trades,
                        "wins":      self.ce_wins,
                        "win_rate":  round(self.ce_wins / self.ce_trades, 3) if self.ce_trades else None,
                        "pnl_total": round(self.ce_pnl_total, 2),
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
        1. Pre-market history warm-up.
        2. Start background state-dump task.
        3. Connect to OpenAlgo WebSocket with auto-reconnect (exponential backoff).
        4. Stream ticks until EOD (15:20), then close all positions and exit.
        """
        await self._warmup_history()
        asyncio.create_task(self._state_dump_loop())

        retry_delay = 5

        while True:
            now = datetime.now()

            # EOD outer guard
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

                    if self._first_connect:
                        self._first_connect = False
                        await send_async(
                            f"🤖 *Nifty Trend Seller Bot — Online*\n"
                            f"Confluence: ADX(14)>30↑ + RSI + MACD(5,13,3)\n"
                            f"Entry window : {ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')} IST\n"
                            f"Filters      : VIX≤{VIX_MAX}  |  DTE {MIN_DTE}–{MAX_DTE}\n"
                            f"Quantity     : {N_LOTS} lots  (lot size fetched dynamically)\n"
                            f"Safety SL    : {SL_MULTIPLE}× entry premium\n"
                            f"Exit         : EOD 15:14 IST"
                        )
                    else:
                        logger.info("WebSocket reconnected.")

                    retry_delay = 5   # reset backoff on successful connect

                    async for raw in ws:
                        # EOD inner guard
                        if datetime.now().time() >= SESSION_END:
                            await self._eod_close_all()
                            return

                        msg = json.loads(raw)
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

                        if sym in (IDX_SYMBOL, f"{IDX_EXCHANGE}:{IDX_SYMBOL}"):
                            await self._on_index_tick(ltp, ts)

                        elif sym in (VIX_SYMBOL, f"{VIX_EXCHANGE}:{VIX_SYMBOL}"):
                            self.vix_ltp = ltp

                        else:
                            # Option tick — check which active leg it belongs to
                            for opt_type, trade in self.active_trades.items():
                                if trade and trade.get("symbol") == sym:
                                    await self._on_option_tick(ltp, opt_type)
                                    break

            except Exception as e:
                logger.warning(
                    f"WebSocket error: {e}. Reconnecting in {retry_delay}s…"
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    bot = NiftyTrendSellerBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("Stop signal received — bot terminated.")
