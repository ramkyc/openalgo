"""
HTF Power of 3 (PO3) Bot
=======================================
live_trading/htf_po3_bot/htf_po3_bot.py

Research-validated (htf_po3_study, 2026-03-21) — ALL 10 PIPELINE STAGES PASS.
Reference: options_data/research/htf_po3_study/results_summary.md

Strategy — Sell ATM PE on bullish 60-min PO3 signal:
  1. Accumulation  : first accum_minutes of each 60-min bar → record accum_high/low.
  2. Manipulation  : price dips below accum_low → find most recent bullish FVG on
                     5-min bars (gap ≥ fvg_min_size pts) formed before the dip.
  3. CISD          : next 1-min bar closes above FVG top → LONG signal → sell ATM PE.
  4. Entry window  : 09:45–14:30 IST. One trade per instrument per session.
  5. Exit rules    :
       Target: PE premium falls to target_pct × entry_premium → buy to close.
       SL    : PE premium rises to sl_mult × entry_premium    → buy to close.
       EOD   : Unconditional close at 15:14 IST.

Champions (10/10 pipeline stages, 2026-03-21):
  NIFTY    : accum=30m, fvg_tf=5m, fvg_min=20, sl=2.0×, tgt=0.7
             IS Sharpe=7.70, OOS Sharpe=4.92  WR=83.3% OOS  expiry=weekly ≥2 DTE
  BANKNIFTY: accum=15m, fvg_tf=5m, fvg_min=20, sl=1.5×, tgt=0.3
             IS Sharpe=5.96, OOS Sharpe=4.96  WR=52.6% OOS  expiry=monthly ≥7 DTE

Expiry-day insight (Stage 10): both instruments show 100% WR on expiry days
  with avg P&L 60-67% higher than non-expiry days. No filter applied; size up
  on expiry days manually once sufficient live data confirms this.

Instruments: NIFTY + BANKNIFTY run as two independent state machines in one process.
SENSEX: excluded — OOS Sharpe −3.765 (structural BSE options liquidity issue).

Shared utilities:
  live_trading.api_utils                — get_expiry_dates, get_option_symbol, get_history
  live_trading.shared.atm_resolver      — get_option_ltp
  live_trading.shared.telegram_notifier — send_async
"""

import asyncio
import json
import logging
import os
import sys
from collections import deque
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import websockets
from dotenv import load_dotenv
from openalgo import api

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils                import get_expiry_dates, get_option_symbol, is_market_holiday
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
        logging.FileHandler(LOGS_DIR / "htf_po3_bot.log"),
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

STRATEGY_NAME = "HTF_PO3_SELL_PE"
N_LOTS        = 10   # standardised: 10 lots per CLAUDE.md position-size rule


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback

# ── Instrument configurations ─────────────────────────────────────────────────
# NIFTY disabled 2026-07-15 — running BANKNIFTY-only on this account for now.
# Re-enable by moving this block back into INSTRUMENTS below.
# _DISABLED_NIFTY_CFG = {
#     "idx_symbol":    "NIFTY",
#     "idx_exchange":  "NSE_INDEX",
#     "opt_exchange":  "NFO",
#     "strike_step":   50,
#     "default_lot":   65,   # NSE cut NIFTY lot 75→65 eff. Jan 6 2026 (weekly)/Jan 27 (monthly); fallback only — live uses broker lotsize
#     "min_dte":       2,
#     "max_dte":       14,
#     "accum_minutes": 30,
#     "fvg_tf_min":    5,
#     "fvg_min_size":  20.0,
#     "sl_mult":       2.0,
#     "target_pct":    0.7,
#     # Accum-range cap (Stage 11, 2026-07-10): thin sample (29 historical
#     # trades) so this is directional, not precisely tuned. Removes the
#     # single worst historical NIFTY day. See bot-gate-decisions-2026-07 memory.
#     "accum_range_cap": 80.0,
# }
INSTRUMENTS = {
    "BANKNIFTY": {
        "idx_symbol":    "BANKNIFTY",
        "idx_exchange":  "NSE_INDEX",
        "opt_exchange":  "NFO",
        "strike_step":   100,
        "default_lot":   35,
        "min_dte":       7,
        "max_dte":       45,
        "accum_minutes": 15,
        "fvg_tf_min":    5,
        "fvg_min_size":  20.0,
        "sl_mult":       1.5,
        "target_pct":    0.3,
        # Accum-range cap (Stage 11, 2026-07-10): max ever seen across 106
        # backtested trades (Dec 2024-Jul 2026) was 286.85 — this cap costs
        # zero historical trades while blocking a repeat of the 2026-07-08
        # disaster (367.3 pt accum range, -₹116,310 SL loss).
        "accum_range_cap": 300.0,
    },
}

# ── Session timing ────────────────────────────────────────────────────────────
MARKET_OPEN  = dt_time(9,  15)
ENTRY_START  = dt_time(9,  45)    # no CISD entries before 09:45
ENTRY_END    = dt_time(14, 30)    # no new entries after 14:30
EOD_EXIT     = dt_time(15, 14)    # hard close all positions

# ── Post-trade adverse excursion watch ────────────────────────────────────────
# Any trade where exit_premium / entry_premium exceeds this threshold is
# flagged to adverse_excursion_watch.jsonl for manual review after close.
# The 1.5× SL catches genuine disasters; this 1.2× watch catches the smaller
# adverse moves that are within SL but warrant intraday trajectory review.
ADVERSE_WATCH_THRESHOLD = 1.20
ADVERSE_WATCH_FILE      = LOGS_DIR / "adverse_excursion_watch.jsonl"

# ── State file ────────────────────────────────────────────────────────────────
STATE_FILE = LOGS_DIR / "htf_po3_state.json"

# ── Watchdog / heartbeat tuning (mirrors banknifty_bb_options_bot) ────────────
HEARTBEAT_SECS         = 300   # decision-state snapshot every 5 min
DEAD_FEED_SECS         = 300   # alert if no index tick for 5 min during market hours
DEAD_FEED_REALERT_SECS = 300   # re-alert every 5 min while feed stays dead
DECISION_LOG = LOGS_DIR / "htf_po3_decisions.jsonl"


# ══════════════════════════════════════════════════════════════════════════════
class BarBuilder:
    """Generic N-minute OHLC bar builder fed by 1-min ticks."""

    def __init__(self, tf_minutes: int, maxbars: int = 500):
        self.tf        = tf_minutes
        self.bars: deque = deque(maxlen=maxbars)
        self._bucket   = -1
        self._open     = 0.0
        self._high     = 0.0
        self._low      = float("inf")
        self._close    = 0.0

    def update(self, price: float, ts: datetime) -> bool:
        """Feed a price tick. Returns True when a new bar completes."""
        total_min  = ts.hour * 60 + ts.minute
        cur_bucket = total_min // self.tf

        if self._bucket == -1:
            self._bucket = cur_bucket
            self._open = self._high = price
            self._low  = self._close = price
            return False

        if cur_bucket != self._bucket:
            # Close the previous bar
            if self._open > 0:
                self.bars.append({
                    "open":  self._open,
                    "high":  self._high,
                    "low":   self._low,
                    "close": self._close,
                    "bucket": self._bucket,
                })
            # Start new bar
            self._bucket = cur_bucket
            self._open = self._high = price
            self._low  = self._close = price
            return True

        self._high  = max(self._high, price)
        self._low   = min(self._low,  price)
        self._close = price
        return False

    def reset(self):
        self.bars.clear()
        self._bucket = -1
        self._open = self._high = self._close = 0.0
        self._low = float("inf")


# ══════════════════════════════════════════════════════════════════════════════
class Po3State:
    """
    Per-instrument PO3 state machine tracking the 60-min HTF bar phases.

    Phases per HTF bar:
      ACCUM         — first accum_minutes: record high/low
      SKIPPED_RANGE — accum_high-accum_low exceeded accum_range_cap: no entry this bar
      WATCH         — after accumulation: watch for manipulation (price < accum_low)
      MANIP         — manipulation detected: look for bullish FVG
      WAIT_CISD     — FVG identified: wait for 1-min close above FVG top
      SIGNAL        — CISD confirmed → fire entry
      TRADED        — trade placed for this HTF bar (one per bar)
    """

    def __init__(self, cfg: dict):
        self.cfg         = cfg
        self.name        = cfg["idx_symbol"]
        self.bar1m       = BarBuilder(1, maxbars=400)    # 1-min bars for CISD
        self.bar5m       = BarBuilder(cfg["fvg_tf_min"], maxbars=300)  # FVG bars
        self.ltp         = 0.0

        # HTF bar tracking (60-min)
        self._htf_bucket   = -1
        self._htf_start_min = 0    # total minutes at HTF bar open
        self._accum_end_min = 0    # total minutes when accumulation phase ends
        self._accum_high   = 0.0
        self._accum_low    = float("inf")
        self._phase        = "ACCUM"  # ACCUM | SKIPPED_RANGE | WATCH | MANIP | WAIT_CISD | TRADED
        self._range_skip_notify = False  # set True for one tick when a bar is filtered out

        # FVG zone (set when manipulation detected)
        self._fvg_top    = 0.0
        self._fvg_bottom = 0.0

        # Active trade
        self.active: Optional[dict] = None
        self._session_traded = False   # one trade per daily session
        # Set by _restore_state() when active/session_traded was restored from
        # today's state file — tells _session_start() not to clobber it on a
        # mid-session process restart (2026-07-16: a restart wiped a live
        # position's SL tracking because _session_start() always fired on
        # first ws-connect and unconditionally reset every symbol).
        self._restored_today = False

        # Expiry metadata (set at session start)
        self.expiry:   Optional[str] = None
        self.lot_size: int           = cfg["default_lot"]

    def reset_session(self):
        """Called at market open each day to reset session state."""
        self._htf_bucket   = -1
        self._accum_high   = 0.0
        self._accum_low    = float("inf")
        self._phase        = "ACCUM"
        self._range_skip_notify = False
        self._fvg_top      = self._fvg_bottom = 0.0
        self._session_traded = False
        self.active        = None
        self.bar1m.reset()
        self.bar5m.reset()
        logger.info(f"[{self.name}] Session reset.")

    def _reset_htf_bar(self, htf_bucket: int, ts: datetime):
        """Start a fresh HTF bar."""
        self._htf_bucket    = htf_bucket
        total_min           = ts.hour * 60 + ts.minute
        self._htf_start_min = total_min
        self._accum_end_min = total_min + self.cfg["accum_minutes"]
        self._accum_high    = self.ltp
        self._accum_low     = self.ltp
        self._phase         = "ACCUM"
        self._range_skip_notify = False
        self._fvg_top       = self._fvg_bottom = 0.0
        logger.debug(f"[{self.name}] HTF bar {htf_bucket}: accum phase starts "
                     f"(ends at minute {self._accum_end_min})")

    def _find_bullish_fvg(self) -> Optional[tuple]:
        """
        Scan completed 5-min bars for the most recent bullish FVG:
          bars[i-2].high < bars[i].low  AND  gap_size >= fvg_min_size
        Returns (top, bottom) of the latest qualifying FVG, or None.
        """
        bars = list(self.bar5m.bars)
        if len(bars) < 3:
            return None
        fvg_min = self.cfg["fvg_min_size"]
        result = None
        for i in range(2, len(bars)):
            bottom = bars[i - 2]["high"]
            top    = bars[i]["low"]
            if top > bottom and (top - bottom) >= fvg_min:
                result = (top, bottom)   # keep most recent
        return result

    def on_tick(self, price: float, ts: datetime) -> bool:
        """
        Process a live tick. Returns True if a CISD entry signal fires.
        The caller must ensure entry window (09:45–14:30) before acting.
        """
        self.ltp = price

        # ── HTF bar tracking ──────────────────────────────────────────────────
        total_min  = ts.hour * 60 + ts.minute
        # Align HTF buckets to market open (09:15 = 555 min) so bars are
        # 09:15–10:14, 10:15–11:14, … matching the backtest's htf_boundaries().
        # Using total_min // 60 would create bars at clock hours (10:00, 11:00)
        # which misaligns the accumulation window vs what was validated.
        MKT_OPEN_MIN = 9 * 60 + 15  # 555
        htf_bucket = max(0, (total_min - MKT_OPEN_MIN) // 60)

        if htf_bucket != self._htf_bucket:
            self._reset_htf_bar(htf_bucket, ts)

        # ── Already traded this HTF bar / session ─────────────────────────────
        if self._session_traded:
            return False

        # ── Build 5-min bars (always, for FVG detection later) ───────────────
        new_5m = self.bar5m.update(price, ts)

        # ── Build 1-min bars ─────────────────────────────────────────────────
        new_1m = self.bar1m.update(price, ts)

        # ── ACCUM phase: update high/low ──────────────────────────────────────
        if self._phase == "ACCUM":
            self._accum_high = max(self._accum_high, price)
            self._accum_low  = min(self._accum_low,  price)
            if total_min >= self._accum_end_min:
                accum_range = self._accum_high - self._accum_low
                cap = self.cfg.get("accum_range_cap")
                if cap and accum_range > cap:
                    # Pre-trade filter: a blown-out accumulation range means the
                    # "accumulation" wasn't a genuine tight base — undercuts the
                    # PO3 premise. Park this HTF bar in a terminal phase so no
                    # WATCH/MANIP/WAIT_CISD/SIGNAL transition can fire this bar.
                    self._phase = "SKIPPED_RANGE"
                    self._range_skip_notify = True
                    logger.warning(
                        f"[{self.name}] Accumulation range {accum_range:.1f} > "
                        f"cap {cap:.1f}  (high={self._accum_high:.1f} "
                        f"low={self._accum_low:.1f}) — filtering this HTF bar, no entries."
                    )
                else:
                    self._phase = "WATCH"
                    logger.info(
                        f"[{self.name}] Accumulation done: "
                        f"high={self._accum_high:.1f}  low={self._accum_low:.1f}  "
                        f"range={accum_range:.1f}  → watching for manipulation"
                    )
            return False

        # ── WATCH phase: detect manipulation (price dips below accum_low) ─────
        if self._phase == "WATCH":
            if price < self._accum_low:
                # Manipulation detected — find nearest bullish FVG
                fvg = self._find_bullish_fvg()
                if fvg:
                    self._fvg_top, self._fvg_bottom = fvg
                    self._phase = "WAIT_CISD"
                    logger.info(
                        f"[{self.name}] Manipulation ✓  price={price:.1f} < "
                        f"accum_low={self._accum_low:.1f}  "
                        f"FVG zone: {self._fvg_bottom:.1f}–{self._fvg_top:.1f}"
                    )
                else:
                    self._phase = "MANIP"   # manip confirmed but no FVG yet
                    logger.info(
                        f"[{self.name}] Manipulation ✓ (no FVG found yet — will keep scanning)"
                    )
            return False

        # ── MANIP phase: FVG not found yet — keep scanning on each new 5m bar ─
        if self._phase == "MANIP":
            if new_5m:
                fvg = self._find_bullish_fvg()
                if fvg:
                    self._fvg_top, self._fvg_bottom = fvg
                    self._phase = "WAIT_CISD"
                    logger.info(
                        f"[{self.name}] FVG found after manip: "
                        f"{self._fvg_bottom:.1f}–{self._fvg_top:.1f}"
                    )
            return False

        # ── WAIT_CISD: look for 1-min close ABOVE FVG top ─────────────────────
        if self._phase == "WAIT_CISD":
            if new_1m and self.bar1m.bars:
                last_close = self.bar1m.bars[-1]["close"]
                if last_close > self._fvg_top:
                    self._phase = "SIGNAL"
                    logger.info(
                        f"[{self.name}] 🔥 CISD confirmed: "
                        f"1m-close={last_close:.1f} > FVG_top={self._fvg_top:.1f}  "
                        f"→ entry signal"
                    )
                    return True   # ← SIGNAL

        return False


# ══════════════════════════════════════════════════════════════════════════════
class HTFPo3Bot:
# ══════════════════════════════════════════════════════════════════════════════
    """
    Dual-instrument PO3 bot (NIFTY + BANKNIFTY).

    WebSocket feeds live index ticks; each instrument has its own Po3State
    state machine. On CISD signal, resolves ATM PE via OpenAlgo API and
    places a paper SELL order. Monitors position every 30s for SL/target/EOD.
    """

    def __init__(self):
        self.client  = api(api_key=API_KEY, host=HOST)
        self.ws      = None
        self._subscribed: set = set()

        # Per-instrument state
        self.states = {
            sym: Po3State(cfg) for sym, cfg in INSTRUMENTS.items()
        }

        self._session_started = False
        self._eod_done        = False
        self._first_connect   = True

        # LTP-polling reliability: consecutive get_option_ltp failures per
        # instrument, so a dead feed (2026-05-29: ~4.5h of blind SL
        # monitoring) surfaces as an escalating alert instead of a silent
        # `continue` every 30s poll cycle. See bot-gate-decisions-2026-07 memory.
        self._ltp_fail_streak: dict[str, int] = {}

        # Index-feed dead-feed watchdog: per-instrument last-tick timestamp +
        # alert state, mirroring banknifty_bb_options_bot's _ops_watchdog_loop
        # (2026-07-08 incident: nothing watched whether ticks were arriving at
        # all — only whether SL/target math kept working on stale ticks).
        # _ws_connect only reconnects on an exception, so a hung-but-not-
        # erroring socket would otherwise go unnoticed indefinitely.
        self._last_index_tick: dict[str, datetime] = {}
        self._feed_dead_alerted: dict[str, bool] = {}
        self._feed_dead_last_alert: dict[str, datetime] = {}

        # Restore any active trades from a previous run today
        self._restore_state()

    # ── State restore (mid-session restart recovery) ──────────────────────────

    def _restore_state(self) -> None:
        """
        On startup, reload per-instrument state saved by _save_state.
        Restores active trades and session_traded flags only if the state
        file was written today — prevents stale state from a previous day.
        """
        if not STATE_FILE.exists():
            return
        try:
            saved = json.loads(STATE_FILE.read_text())
            last_update_str = saved.get("last_update", "")
            if not last_update_str:
                return
            last_update = datetime.fromisoformat(last_update_str)
            if last_update.date() != datetime.now().date():
                logger.info("_restore_state: state file is from a previous day — skipping restore")
                return

            restored_any = False
            for sym, state in self.states.items():
                sym_data = saved.get(sym)
                if not sym_data:
                    continue
                # Restore expiry so any needed API calls use the right contract
                if sym_data.get("expiry"):
                    state.expiry = sym_data["expiry"]
                # Restore session-traded gate so we don't re-enter today
                if sym_data.get("session_traded"):
                    state._session_traded = True
                    state._restored_today = True
                    restored_any = True
                # Restore active trade for SL / target monitoring
                at = sym_data.get("active")
                if at:
                    state.active = at
                    state._restored_today = True
                    logger.warning(
                        f"🔄 [{sym}] Restored ACTIVE trade: "
                        f"{at.get('symbol')} entry={at.get('entry_prem')} sl={at.get('sl_prem')}"
                    )
                    restored_any = True

            if restored_any:
                logger.info("_restore_state: mid-session restart — active trades and session gates restored")
        except Exception as e:
            logger.warning(f"_restore_state: could not read state file: {e}")

    # ── Expiry & lot-size helpers ──────────────────────────────────────────────

    def _get_expiry(self, sym: str) -> Optional[str]:
        cfg = INSTRUMENTS[sym]
        dates = get_expiry_dates(API_KEY, sym, cfg["opt_exchange"], "options")
        if not dates:
            logger.warning(f"[{sym}] No expiry dates returned.")
            return None
        today = datetime.now().date()
        for d in dates:
            try:
                exp_dt = datetime.strptime(d, "%d%b%y").date()
                dte = (exp_dt - today).days
                if cfg["min_dte"] <= dte <= cfg["max_dte"]:
                    logger.info(f"[{sym}] Expiry selected: {d}  (DTE={dte})")
                    return d
            except ValueError:
                continue
        logger.warning(f"[{sym}] No expiry in DTE {cfg['min_dte']}–{cfg['max_dte']}. "
                       f"Available: {dates[:4]}")
        return None

    def _get_lot_size(self, sym: str, option_symbol: str) -> int:
        try:
            from database.token_db import get_symbol_info
            si = get_symbol_info(option_symbol, INSTRUMENTS[sym]["opt_exchange"])
            if si and getattr(si, "lotsize", None):
                return int(si.lotsize)
        except Exception:
            pass
        return INSTRUMENTS[sym]["default_lot"]

    def _margin_ok(self, symbol: str, exchange: str, qty: int) -> bool:
        """Check Fyers' own required margin (OpenAlgo /api/v1/margin, backed by
        Fyers' multiorder-margin API) against actual available cash
        (/api/v1/funds) before risking a SELL entry. Fails closed (skips the
        trade) on any error — a missed signal is cheap, an under-margined
        live order is not."""
        try:
            margin_res = self.client.margin(positions=[{
                "symbol":    symbol,
                "exchange":  exchange,
                "action":    "SELL",
                "product":   "MIS",
                "pricetype": "MARKET",
                "quantity":  str(qty),
            }])
            if margin_res.get("status") != "success":
                logger.error(f"Margin check failed for {symbol}: {margin_res}")
                return False
            required = float(margin_res["data"]["total_margin_required"])

            funds_res = self.client.funds()
            if funds_res.get("status") != "success":
                logger.error(f"Funds check failed: {funds_res}")
                return False
            available = float(funds_res["data"]["availablecash"])

            logger.info(f"Margin check {symbol}: required=₹{required:,.2f}  available=₹{available:,.2f}")
            return available >= required
        except Exception as e:
            logger.error(f"Margin/funds check exception for {symbol}: {e}")
            return False

    # ── Session initialisation ─────────────────────────────────────────────────

    async def _session_start(self):
        """Run at market open: reset state machines and resolve expiries."""
        # 0. Holiday Check
        if is_market_holiday(API_KEY):
            logger.info("⛔ Market Holiday detected. HTF PO3 Bot will remain in standby.")
            return

        logger.info("=" * 60)
        logger.info("🌅  Market session starting — HTF PO3 Bot")
        logger.info("=" * 60)
        self._eod_done = False

        for sym, state in self.states.items():
            if state._restored_today:
                logger.info(
                    f"[{sym}] Mid-session restart — skipping reset_session() "
                    f"to preserve restored active trade / session gate."
                )
            else:
                state.reset_session()
            state.expiry = await asyncio.to_thread(self._get_expiry, sym)
            if state.expiry:
                logger.info(f"[{sym}] Ready. Expiry={state.expiry}  "
                            f"accum={INSTRUMENTS[sym]['accum_minutes']}m  "
                            f"fvg_min={INSTRUMENTS[sym]['fvg_min_size']}  "
                            f"sl={INSTRUMENTS[sym]['sl_mult']}×  "
                            f"tgt={INSTRUMENTS[sym]['target_pct']}")
            else:
                logger.warning(f"[{sym}] No suitable expiry — will skip trades today.")

        await send_async(
            "🚀 *HTF PO3 Bot — Session Start*\n"
            + "\n".join(
                f"`{sym}`: expiry={st.expiry or 'N/A'}  "
                f"accum={INSTRUMENTS[sym]['accum_minutes']}m  "
                f"fvg_min={INSTRUMENTS[sym]['fvg_min_size']}"
                for sym, st in self.states.items()
            )
        )
        self._session_started = True

    # ── Trade entry ────────────────────────────────────────────────────────────

    async def _enter_trade(self, sym: str) -> None:
        state = self.states[sym]
        cfg   = INSTRUMENTS[sym]

        if state.active or state._session_traded:
            return
        if not state.expiry:
            logger.warning(f"[{sym}] Signal fired but no expiry — skipping.")
            return

        now = datetime.now()
        if not (ENTRY_START <= now.time() <= ENTRY_END):
            logger.info(f"[{sym}] Signal outside entry window ({now.strftime('%H:%M')}) — skip.")
            return

        spot = state.ltp
        if spot <= 0:
            return

        # ── Resolve ATM PE ────────────────────────────────────────────────────
        option_sym = await asyncio.to_thread(
            get_option_symbol,
            API_KEY, sym, cfg["opt_exchange"], state.expiry, "PE", "ATM",
        )
        if not option_sym:
            logger.error(f"[{sym}] ATM PE symbol resolution failed (spot={spot:.0f}).")
            return

        # ── Fetch live PE LTP ─────────────────────────────────────────────────
        opt_ltp = await asyncio.to_thread(get_option_ltp, option_sym, cfg["opt_exchange"], API_KEY)
        if opt_ltp <= 0:
            logger.warning(f"[{sym}] PE LTP=0 for {option_sym}. Skipping entry.")
            return

        lot_size = self._get_lot_size(sym, option_sym)
        qty      = N_LOTS * lot_size

        sl_prem  = round(opt_ltp * cfg["sl_mult"],    2)
        tgt_prem = round(opt_ltp * cfg["target_pct"], 2)

        logger.info(
            f"[{sym}] ▶ ENTRY  {option_sym}  LTP=₹{opt_ltp:.2f}  "
            f"SL=₹{sl_prem:.2f} ({cfg['sl_mult']}×)  "
            f"Tgt=₹{tgt_prem:.2f} ({cfg['target_pct']}×)  qty={qty}"
        )

        # ── Pre-entry margin check ────────────────────────────────────────────
        # Fyers' own multiorder-margin calculator (via OpenAlgo /api/v1/margin)
        # gives the actual required margin for this SELL; compare against
        # actual available cash (/api/v1/funds) before risking the order.
        if not await asyncio.to_thread(self._margin_ok, option_sym, cfg["opt_exchange"], qty):
            logger.error(f"[{sym}] Insufficient margin for {option_sym} (qty={qty}). Skipping entry.")
            await send_async(
                f"⚠️ *HTF PO3 [{sym}]* — entry skipped: insufficient margin for "
                f"`{option_sym}` (qty={qty})."
            )
            return

        # ── Place SELL order ──────────────────────────────────────────────────
        try:
            # Use placeorder (NOT placesmartorder) for entries.
            # placesmartorder(position_size=...) reads the broker's NET position
            # across ALL strategies for this exact symbol — if another live bot
            # already holds the same option contract, OpenAlgo reports
            # "Positions Already Matched" (status=success) without ever placing
            # this order, and this bot would then record a phantom position it
            # doesn't actually hold. placeorder with exact qty always adds
            # exactly this bot's intended size, regardless of what any other
            # bot holds in the same symbol (same reasoning as the exit below).
            res = self.client.placeorder(
                strategy   = STRATEGY_NAME,
                symbol     = option_sym,
                action     = "SELL",
                exchange   = cfg["opt_exchange"],
                price_type = "MARKET",
                product    = "MIS",
                quantity   = qty,
            )
        except Exception as e:
            logger.error(f"[{sym}] placeorder exception: {e}")
            return

        if res.get("status") != "success":
            logger.error(f"[{sym}] Order failed: {res}")
            return

        # ── Resolve actual entry fill (falls back to pre-order LTP) ────────────
        entry_fill = _resolve_fill(res, opt_ltp)
        sl_prem    = round(entry_fill * cfg["sl_mult"],    2)
        tgt_prem   = round(entry_fill * cfg["target_pct"], 2)

        state.active = {
            "symbol":     option_sym,
            "entry_prem": entry_fill,
            "sl_prem":    sl_prem,
            "tgt_prem":   tgt_prem,
            "qty":        qty,
            "lot_size":   lot_size,
            "order_id":   str(res.get("orderid", "")),
            "entry_time": now.isoformat(),
            "exit_reason": None,
        }
        state._session_traded = True
        state.lot_size = lot_size

        # Subscribe to option ticks (best-effort; monitoring via polling fallback)
        await self._subscribe(option_sym, cfg["opt_exchange"])

        logger.info(f"[{sym}] ✅ SOLD {option_sym} @ ₹{entry_fill:.2f}  (order={res.get('orderid')})")
        await send_async(
            f"📉 *HTF PO3 — ENTRY [{sym}]*\n"
            f"Sold `{option_sym}`  (1 lot, qty={qty})\n"
            f"Entry premium : ₹{entry_fill:.2f}\n"
            f"Target        : ₹{tgt_prem:.2f}  ({int((1-cfg['target_pct'])*100)}% decay)\n"
            f"Stop-loss     : ₹{sl_prem:.2f}   ({cfg['sl_mult']}× entry)\n"
            f"EOD exit      : 15:14 IST\n"
            f"PO3 signal    : accum={INSTRUMENTS[sym]['accum_minutes']}m  "
            f"FVG≥{INSTRUMENTS[sym]['fvg_min_size']}pts  CISD confirmed"
        )
        self._save_state()

    # ── Trade exit ─────────────────────────────────────────────────────────────

    async def _exit_trade(self, sym: str, reason: str, current_prem: float) -> None:
        state = self.states[sym]
        pos   = state.active
        if not pos:
            return

        cfg = INSTRUMENTS[sym]

        try:
            # Use placeorder (NOT placesmartorder) for exits.
            # placesmartorder(position_size=0) reads the broker's NET position across ALL
            # strategies — in live mode another bot holding the same symbol would cause
            # this exit to close both positions. placeorder with exact qty is safe.
            res = self.client.placeorder(
                strategy   = STRATEGY_NAME,
                symbol     = pos["symbol"],
                action     = "BUY",
                exchange   = cfg["opt_exchange"],
                price_type = "MARKET",
                product    = "MIS",
                quantity   = str(pos["qty"]),
            )
        except Exception as e:
            logger.error(f"[{sym}] Exit order exception: {e}")
            return

        order_ok  = res.get("status") == "success"
        order_id  = res.get("orderid")

        # ── Resolve actual fill price via OpenAlgo orderstatus ──────────────────
        # current_prem is the LTP snapshot taken BEFORE the order was placed and
        # is used as the fallback if the fill lookup fails (e.g. EOD auto-squareoff).
        actual_exit_prem = _resolve_fill(res, current_prem)
        if order_ok and order_id and actual_exit_prem != current_prem:
            logger.info(
                f"[{sym}] Fill price from orderstatus: ₹{actual_exit_prem:.2f} "
                f"(pre-order LTP was ₹{current_prem:.2f})"
            )

        gross_pnl = (pos["entry_prem"] - actual_exit_prem) * pos["qty"]

        logger.info(
            f"[{sym}] ◀ EXIT ({reason})  {pos['symbol']}  "
            f"fill=₹{actual_exit_prem:.2f}  gross_pnl=₹{gross_pnl:.0f}"
        )

        if not order_ok:
            # Position may already be closed by sandbox auto-squareoff (15:15 MIS cutoff).
            # Do NOT return early — always log the trade so performance.db stays accurate.
            logger.warning(
                f"[{sym}] Exit order non-success (likely auto-squareoff already closed "
                f"position): {res}  — logging trade and clearing state."
            )

        emoji  = "✅" if gross_pnl >= 0 else "❌"
        reason_label = {"target": "🎯 Target hit", "sl": "🛑 Stop-loss hit",
                        "eod": "🕓 EOD close"}.get(reason, reason)
        auto_sq_note = " _(closed by auto-squareoff)_" if not order_ok else ""

        await send_async(
            f"{emoji} *HTF PO3 — EXIT [{sym}]* — {reason_label}\n"
            f"Symbol        : `{pos['symbol']}`\n"
            f"Entry premium : ₹{pos['entry_prem']:.2f}\n"
            f"Exit premium  : ₹{actual_exit_prem:.2f}\n"
            f"Gross P&L     : ₹{gross_pnl:+.0f}  ({N_LOTS} lot)\n"
            f"Exit reason   : {reason_label}{auto_sq_note}"
        )
        log_trade_to_db(
            bot_name      = "htf_po3_bot",
            instrument    = sym,
            option_symbol = pos["symbol"],
            option_type   = "PE",
            entry_time    = pos.get("entry_time"),
            exit_time     = datetime.now(),
            entry_premium = pos["entry_prem"],
            exit_premium  = actual_exit_prem,
            exit_reason   = reason,
            quantity      = pos["qty"],
            lots          = N_LOTS,
            lot_size      = pos.get("lot_size", pos["qty"]),
            gross_pnl     = gross_pnl,
            order_id      = order_id,
        )

        # ── Post-trade adverse excursion watch ────────────────────────────────
        # Flag any trade where the option rose >20% from entry (i.e., the
        # short moved against us). These are worth reviewing intraday to
        # calibrate whether the 1.5× SL needs tightening in future.
        if pos["entry_prem"] > 0:
            excursion_ratio = actual_exit_prem / pos["entry_prem"]
            if excursion_ratio > ADVERSE_WATCH_THRESHOLD:
                record = {
                    "flagged_at":      datetime.now().isoformat(),
                    "trade_date":      datetime.now().date().isoformat(),
                    "instrument":      sym,
                    "symbol":          pos["symbol"],
                    "entry_prem":      pos["entry_prem"],
                    "exit_prem":       actual_exit_prem,
                    "excursion_ratio": round(excursion_ratio, 3),
                    "sl_threshold":    round(pos["entry_prem"] * ADVERSE_WATCH_THRESHOLD, 2),
                    "exit_reason":     reason,
                    "qty":             pos["qty"],
                    "gross_pnl":       gross_pnl,
                }
                try:
                    with ADVERSE_WATCH_FILE.open("a") as fh:
                        fh.write(json.dumps(record) + "\n")
                    logger.warning(
                        f"[{sym}] ⚠ ADVERSE EXCURSION: ratio={excursion_ratio:.3f}× "
                        f"(>{ADVERSE_WATCH_THRESHOLD}× watch threshold). "
                        f"Review BANKNIFTY intraday action for {datetime.now().date()} — "
                        f"logged to {ADVERSE_WATCH_FILE.name}"
                    )
                    await send_async(
                        f"⚠️ *HTF PO3 — Adverse Excursion Watch [{sym}]*\n"
                        f"Option rose {excursion_ratio:.2f}× from entry "
                        f"(₹{pos['entry_prem']:.2f} → ₹{actual_exit_prem:.2f}).\n"
                        f"Review `{ADVERSE_WATCH_FILE.name}` after close to assess "
                        f"whether intraday SL tightening is warranted."
                    )
                except Exception as exc:
                    logger.error(f"[{sym}] Failed to write adverse excursion record: {exc}")

        state.active = None
        self._save_state()

    # ── Position monitor ───────────────────────────────────────────────────────

    # Consecutive-failure thresholds for the LTP-feed watchdog below.
    # Poll interval is 30s, so 3 = ~90s before the first alert, then a
    # re-alert every 10 more failed cycles (~5 min) so a sustained outage
    # (like 2026-05-29's ~4.5h) keeps paging instead of alerting once and
    # going quiet.
    LTP_FAIL_ALERT_THRESHOLD = 3
    LTP_FAIL_REALERT_EVERY   = 10

    async def _monitor_positions(self) -> None:
        """Poll every 30 seconds to check SL / target / EOD for all open positions."""
        while True:
            await asyncio.sleep(30)
            now = datetime.now()

            for sym, state in self.states.items():
                pos = state.active
                if not pos:
                    continue
                cfg = INSTRUMENTS[sym]

                # ── EOD hard exit ──────────────────────────────────────────────
                if now.time() >= EOD_EXIT and not self._eod_done:
                    ltp = await asyncio.to_thread(
                        get_option_ltp, pos["symbol"], cfg["opt_exchange"], API_KEY)
                    if ltp <= 0:
                        logger.warning(
                            f"[{sym}] EOD LTP fetch failed for {pos['symbol']} — "
                            f"closing at entry premium (₹{pos['entry_prem']:.2f}) as fallback mark."
                        )
                        await send_async(
                            f"⚠️ *HTF PO3 Bot* `{sym}` EOD close: LTP fetch failed — "
                            f"closed at entry premium as fallback. Real P&L is unknown; "
                            f"verify against the broker manually."
                        )
                    await self._exit_trade(sym, "eod", ltp if ltp > 0 else pos["entry_prem"])
                    continue

                # ── Fetch current premium ──────────────────────────────────────
                ltp = await asyncio.to_thread(
                    get_option_ltp, pos["symbol"], cfg["opt_exchange"], API_KEY)
                if ltp <= 0:
                    streak = self._ltp_fail_streak.get(sym, 0) + 1
                    self._ltp_fail_streak[sym] = streak
                    logger.warning(
                        f"[{sym}] LTP fetch failed for {pos['symbol']} "
                        f"({streak} consecutive) — SL/target check skipped this cycle."
                    )
                    if streak == self.LTP_FAIL_ALERT_THRESHOLD or (
                        streak > self.LTP_FAIL_ALERT_THRESHOLD and
                        (streak - self.LTP_FAIL_ALERT_THRESHOLD) % self.LTP_FAIL_REALERT_EVERY == 0
                    ):
                        await send_async(
                            f"🚨 *HTF PO3 Bot — LTP feed stale* `{sym}` `{pos['symbol']}`\n"
                            f"{streak} consecutive failed LTP fetches (~{streak * 30}s). "
                            f"SL/target monitoring is BLIND for this position — "
                            f"check broker/API health now."
                        )
                    continue

                if self._ltp_fail_streak.get(sym):
                    recovered_after = self._ltp_fail_streak[sym]
                    self._ltp_fail_streak[sym] = 0
                    logger.info(f"[{sym}] LTP feed recovered after {recovered_after} failed cycles.")
                    await send_async(
                        f"✅ *HTF PO3 Bot* `{sym}` LTP feed recovered after "
                        f"{recovered_after} failed cycles (~{recovered_after * 30}s blind)."
                    )

                # ── SL check ──────────────────────────────────────────────────
                if ltp >= pos["sl_prem"]:
                    logger.warning(f"[{sym}] SL triggered: LTP=₹{ltp:.2f} ≥ SL=₹{pos['sl_prem']:.2f}")
                    await self._exit_trade(sym, "sl", ltp)

                # ── Target check ──────────────────────────────────────────────
                elif ltp <= pos["tgt_prem"]:
                    logger.info(f"[{sym}] Target hit: LTP=₹{ltp:.2f} ≤ Tgt=₹{pos['tgt_prem']:.2f}")
                    await self._exit_trade(sym, "target", ltp)

    # ── EOD close all ─────────────────────────────────────────────────────────

    async def _eod_close_all(self) -> None:
        """Ensure all open positions are closed at 15:14 IST."""
        if self._eod_done:
            return
        self._eod_done = True
        logger.info("⏰ EOD: closing all open positions.")
        for sym, state in self.states.items():
            if state.active:
                pos = state.active
                cfg = INSTRUMENTS[sym]
                ltp = await asyncio.to_thread(
                    get_option_ltp, pos["symbol"], cfg["opt_exchange"], API_KEY)
                await self._exit_trade(sym, "eod", ltp if ltp > 0 else pos["entry_prem"])
        await send_async("🕓 *HTF PO3 Bot — EOD* All positions closed.")

    # ── WebSocket ─────────────────────────────────────────────────────────────

    async def _subscribe(self, symbol: str, exchange: str) -> None:
        if not self.ws or symbol in self._subscribed:
            return
        try:
            sub_msg = json.dumps({
                "action":   "subscribe",
                "symbol":   symbol,
                "exchange": exchange,
            })
            await self.ws.send(sub_msg)
            self._subscribed.add(symbol)
            logger.info(f"  Subscribed: {symbol} [{exchange}]")
        except Exception as e:
            logger.warning(f"  Subscribe error ({symbol}): {e}")

    async def _resubscribe_all(self) -> None:
        """
        Unconditionally resubscribe both indices + any open option leg(s) on
        every (re)connect. The old call site only ever resubscribed the two
        index symbols, so an open position's option feed would silently drop
        off after a mid-session reconnect. Currently benign (SL/target are
        monitored via 30s REST polling in _monitor_positions, not option
        ticks) but closes the same latent gap fixed in nifty_macd_map_bot /
        nifty_eod_hold_bot (fyers_crk), in case option-tick-driven logic is
        ever added here.
        """
        for sym, cfg in INSTRUMENTS.items():
            await self._subscribe(cfg["idx_symbol"], cfg["idx_exchange"])
        for state in self.states.values():
            if state.active:
                await self._subscribe(state.active["symbol"], INSTRUMENTS[state.name]["opt_exchange"])

    async def _ws_connect(self) -> None:
        """WebSocket loop — reconnects on drop."""
        while True:
            try:
                async with websockets.connect(
                    WS_URL, ping_interval=20, ping_timeout=15
                ) as ws:
                    self.ws = ws
                    logger.info(f"✅ WebSocket connected: {WS_URL}")

                    # MUST authenticate before subscribing — proxy rejects subscribe
                    # requests from unauthenticated clients with NOT_AUTHENTICATED.
                    await ws.send(json.dumps({
                        "action":  "authenticate",
                        "api_key": API_KEY,
                    }))

                    if self._first_connect:
                        self._first_connect = False
                        await self._session_start()

                    # Resubscribe indices + any open option leg (survives reconnect)
                    await self._resubscribe_all()

                    async for raw in ws:
                        await self._on_message(raw)

            except Exception as e:
                logger.error(f"WebSocket error: {e}  — reconnecting in 5s…")
                self.ws = None
                self._subscribed.clear()
                await asyncio.sleep(5)

    async def _on_message(self, raw: str) -> None:
        """Route incoming tick to the correct state machine."""
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Only process market_data messages — skip auth confirmations, pings, errors
        if msg.get("type") != "market_data":
            return

        symbol = msg.get("symbol", "")
        # Proxy nests market data under "data": {"ltp": ..., "timestamp": ...}
        m_data = msg.get("data", {})
        ltp    = float(m_data.get("ltp", 0) or m_data.get("lp", 0) or 0)
        ts_raw = m_data.get("timestamp") or m_data.get("t") or msg.get("timestamp") or msg.get("ts")

        if not ltp or not symbol:
            return

        # Parse timestamp
        try:
            if isinstance(ts_raw, (int, float)):
                ts = datetime.fromtimestamp(ts_raw)
            elif isinstance(ts_raw, str):
                ts = datetime.fromisoformat(ts_raw)
            else:
                ts = datetime.now()
        except Exception:
            ts = datetime.now()

        now = ts

        # ── EOD check ─────────────────────────────────────────────────────────
        if now.time() >= EOD_EXIT and not self._eod_done:
            await self._eod_close_all()
            return

        # ── Route to instrument state machine ─────────────────────────────────
        for sym, state in self.states.items():
            cfg = INSTRUMENTS[sym]
            if symbol not in (cfg["idx_symbol"], f"{cfg['idx_exchange']}:{cfg['idx_symbol']}"):
                continue

            self._last_index_tick[sym] = now

            # Feed tick; check if signal fired
            signal = state.on_tick(ltp, now)

            if state._range_skip_notify:
                state._range_skip_notify = False
                accum_range = state._accum_high - state._accum_low
                logger.warning(
                    f"[{sym}] No entries this HTF bar — accum range "
                    f"{accum_range:.1f} > cap {cfg['accum_range_cap']:.1f}."
                )
                await send_async(
                    f"⏭️ *HTF PO3 — {sym}* Accumulation range filter tripped\n"
                    f"range={accum_range:.1f}  cap={cfg['accum_range_cap']:.1f}  "
                    f"(high={state._accum_high:.1f} low={state._accum_low:.1f})\n"
                    f"No entries for this HTF bar."
                )

            if signal:
                await self._enter_trade(sym)

    # ── State persistence ─────────────────────────────────────────────────────

    def _save_state(self) -> None:
        try:
            snap = {
                "last_update": datetime.now().isoformat(),   # for dashboard heartbeat
                **{
                    sym: {
                        "active":         st.active,
                        "phase":          st._phase,
                        "session_traded": st._session_traded,
                        "expiry":         st.expiry,
                        "ltp":            st.ltp,
                        "accum_high":     st._accum_high,
                        "accum_low":      st._accum_low,
                        "accum_range_cap": INSTRUMENTS[sym].get("accum_range_cap"),
                        "ltp_fail_streak": self._ltp_fail_streak.get(sym, 0),
                    }
                    for sym, st in self.states.items()
                }
            }
            STATE_FILE.write_text(json.dumps(snap, indent=2, default=str))
        except Exception as e:
            logger.warning(f"State save failed: {e}")

    # ── Periodic state dump (heartbeat for dashboard) ────────────────────────

    async def _state_dump_loop(self) -> None:
        """Write state file every 2 s so the dashboard always sees a fresh heartbeat,
        even when no trades have been entered yet today."""
        while True:
            self._save_state()
            await asyncio.sleep(2)

    # ── Heartbeat + dead-feed watchdog (ported from banknifty_bb_options_bot) ──

    def _phase_verdict(self, sym: str) -> str:
        """First blocking gate for this instrument, in entry-funnel order —
        answers 'why hasn't this instrument entered yet' at a glance."""
        state = self.states[sym]
        now_t = datetime.now().time()
        if state._session_traded:
            return "done for today (one trade per session)"
        if not state.expiry:
            return "no suitable expiry resolved"
        if not (ENTRY_START <= now_t <= ENTRY_END):
            return "outside entry window"
        phase = state._phase
        if phase == "ACCUM":
            return f"accumulating (high={state._accum_high:.1f} low={state._accum_low:.1f})"
        if phase == "SKIPPED_RANGE":
            return "accum range exceeded cap — no entry this bar"
        if phase == "WATCH":
            return f"watching for manipulation (below {state._accum_low:.1f})"
        if phase == "MANIP":
            return "manipulation confirmed — scanning for bullish FVG"
        if phase == "WAIT_CISD":
            return f"awaiting CISD close above FVG top {state._fvg_top:.1f}"
        return phase

    def _log_heartbeat(self) -> None:
        """Full decision-state snapshot for both instruments, logged every
        HEARTBEAT_SECS, plus one appended DECISION_LOG record per instrument
        for replay/debug."""
        now = datetime.now()
        lines = [f"💓 HTF PO3 DECISION STATE {now.strftime('%H:%M:%S')}"]
        for sym, state in self.states.items():
            line = f"  {sym}: phase={state._phase}  ltp={state.ltp:.1f}  → {self._phase_verdict(sym)}"
            if state.active:
                at = state.active
                line += (f"  | POS {at['symbol']} entry=₹{at['entry_prem']:.2f}"
                         f" sl=₹{at['sl_prem']:.2f} tgt=₹{at['tgt_prem']:.2f}")
            lines.append(line)
        logger.info("\n".join(lines))

        for sym, state in self.states.items():
            try:
                record = {
                    "ts":             now.isoformat(timespec="seconds"),
                    "instrument":     sym,
                    "phase":          state._phase,
                    "ltp":            state.ltp,
                    "session_traded": state._session_traded,
                    "accum_high":     state._accum_high,
                    "accum_low":      state._accum_low,
                    "fvg_top":        state._fvg_top or None,
                    "fvg_bottom":     state._fvg_bottom or None,
                    "active":         state.active,
                }
                with open(DECISION_LOG, "a") as f:
                    f.write(json.dumps(record, default=str) + "\n")
            except Exception:
                pass  # never let logging kill the bot

    async def _ops_watchdog_loop(self) -> None:
        """
        Dead-feed watchdog + periodic heartbeat for the index tick stream both
        PO3 state machines depend on. _ws_connect only reconnects on an
        exception raised inside the `async for raw in ws` loop — a hung-but-
        not-erroring socket (subscription silently dropped, proxy stuck)
        would otherwise go unnoticed indefinitely, same class of gap fixed in
        banknifty_bb_options_bot on 2026-07-08.
        """
        last_heartbeat: Optional[datetime] = None
        while True:
            try:
                now = datetime.now()
                t = now.time()
                if MARKET_OPEN <= t < EOD_EXIT:
                    if (last_heartbeat is None
                            or (now - last_heartbeat).total_seconds() >= HEARTBEAT_SECS):
                        self._log_heartbeat()
                        last_heartbeat = now

                    for sym in INSTRUMENTS:
                        last = self._last_index_tick.get(sym)
                        elapsed = (now - last).total_seconds() if last else None
                        dead_alerted = self._feed_dead_alerted.get(sym, False)

                        if dead_alerted:
                            if elapsed is not None and elapsed < 30:
                                msg = f"✅ HTF PO3 [{sym}]: index feed recovered — ticks resumed"
                                logger.info(msg)
                                await send_async(msg)
                                self._feed_dead_alerted[sym] = False
                                self._feed_dead_last_alert.pop(sym, None)
                            else:
                                last_alert = self._feed_dead_last_alert.get(sym)
                                if (last_alert is None or
                                        (now - last_alert).total_seconds() >= DEAD_FEED_REALERT_SECS):
                                    dur = f"{int(elapsed // 60)}m" if elapsed else "since open"
                                    msg = (f"⚠️ HTF PO3 [{sym}]: STILL no index ticks for {dur} "
                                           f"— feed still dead (check app.py / adapter)")
                                    logger.warning(msg)
                                    await send_async(msg)
                                    self._feed_dead_last_alert[sym] = now
                        else:
                            no_tick_yet = (last is None and t >= dt_time(9, 20))
                            tick_stale  = (elapsed is not None and elapsed > DEAD_FEED_SECS)
                            if no_tick_yet or tick_stale:
                                dur = f"{int(elapsed // 60)}m" if elapsed else "since open"
                                msg = (f"⚠️ HTF PO3 [{sym}]: no index ticks for {dur} "
                                       f"— feed may be dead (check app.py / adapter)")
                                logger.warning(msg)
                                await send_async(msg)
                                self._feed_dead_alerted[sym] = True
                                self._feed_dead_last_alert[sym] = now
            except Exception:
                logger.exception("ops watchdog error")
            await asyncio.sleep(15)

    # ── Time-based session watcher ────────────────────────────────────────────

    async def _session_watcher(self) -> None:
        """Detect new trading day and reset state machines."""
        last_day = None
        while True:
            now = datetime.now()
            today = now.date()
            if today != last_day and now.weekday() < 5:
                if now.time() >= MARKET_OPEN:
                    if not self._session_started:
                        last_day = today
                        self._session_started = True
                        self._eod_done        = False
                    elif last_day is not None and last_day != today:
                        # New trading day
                        last_day              = today
                        self._session_started = False
                        self._eod_done        = False
                        for state in self.states.values():
                            state.reset_session()
                        self._session_started = True
                        await self._session_start()
            await asyncio.sleep(30)

    # ── Main ──────────────────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info("=" * 60)
        logger.info("   HTF PO3 Bot — NIFTY + BANKNIFTY")
        logger.info("=" * 60)
        await asyncio.gather(
            self._ws_connect(),
            self._monitor_positions(),
            self._session_watcher(),
            self._state_dump_loop(),
            self._ops_watchdog_loop(),
        )


# ── Entry ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    bot = HTFPo3Bot()
    asyncio.run(bot.run())
