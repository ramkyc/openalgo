"""
NIFTY GEX + ICT v2 Bot — breakout-only, sell-side.

Strategy (from options_data/research/gex_ict_v2_study, results_summary.md):
  1. Prior-day value area (VAH/VAL) computed from a fixed-range (10-pt bin)
     volume profile of the nearest-month NIFTY future.
  2. First VA break of the session (spot high > VAH or spot low < VAL) is
     the trigger. Only the FIRST break of the day counts.
  3. GEX levels (call_resistance / put_support / top-3 |GEX| strikes) are
     snapshotted once at 09:20 IST — these define the candidate "reach"
     levels. GEX *regime* (positive/negative gamma vs HVL) is re-snapshotted
     every 5 minutes and evaluated at the moment a candidate level is first
     reached ("regime refresh" — the study's core v2 change vs v1).
  4. Regime = negative_gamma at reach time -> "breakout" module (the only
     module this bot trades; "fade" is excluded from the live bot exactly
     as decided in the research phase — it never cleared the ~30/yr Monte
     Carlo frequency floor and was excluded from Stage 1 onward).
  5. Confirmation: any of MSS or IFVG (5-min bars) in the break's
     continuation direction, within a same-session confirmation window.
     (Breaker Block is part of the "any of MSS/Breaker/IFVG" research
     definition but never fired first in the study's full history, so it
     contributes nothing in practice — omitted here as a live no-op.)
  6. Execution: bullish confirm -> SELL ATM PE. Bearish confirm -> SELL ATM CE.
  7. Exit: spot-price stop/target1 (buffer_pct=1.50% around the reached
     level), or EOD 15:14 IST, whichever comes first. At this buffer the
     stop leg is nearly inert (~1.7% of trades) -- downside protection is
     mostly position sizing + EOD exit, not the stop (see results_summary.md
     caveats).

Champion config (NIFTY-only; SENSEX dropped at Stage 3 OOS): buffer_pct=1.50%,
breakout-only, sell-side, 5-min regime refresh, any-of MSS/IFVG confirm.
Combined IS+OOS: n=239, Sharpe 2.48, net +Rs.143,993, ~98 trades/yr.

At most ONE trade per day (matches research's "at most one signal per day").
No PAPER_MODE flag -- this bot always calls the real OpenAlgo order API;
paper vs. live execution is controlled by OpenAlgo's own Sandbox/Analyze
Mode toggle, not by anything in this file.
"""
import asyncio
import json
import logging
import os
import re
import sys
from collections import deque
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path

import numpy as np
import pandas as pd
import websockets
from dotenv import load_dotenv
from openalgo import api

# ── Path / env setup ─────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from database.token_db_enhanced import fno_search_symbols
from live_trading.api_utils import get_expiry_dates, get_history
from live_trading.shared.atm_resolver import resolve_atm_option, get_option_ltp
from live_trading.shared.order_fill import fetch_fill_price
from live_trading.shared.telegram_notifier import send_async
from live_trading.shared.trade_logger import log_trade_to_db
from live_trading.shared.decision_logger import DecisionLogger
from live_trading.shared.tick_watchdog   import TickWatchdog
from services.gex_service import get_gex_data

# Cross-repo import: pure ICT confirmation functions from options_data research.
# Safe -- this module's import chain is pandas/numpy-only (verified: no
# duckdb/numba/vectorbt), unlike gex_ict_confluence_study.stage0_discovery
# which pulls in greeks_engine's numba dependency (not installed in this
# venv). first_break/reaches_level_after logic is therefore reimplemented
# inline below (as an online/causal equivalent) instead of cross-imported.
OPTIONS_DATA_ROOT = Path("/Users/ramakrishna/Developer/options_data")
sys.path.insert(0, str(OPTIONS_DATA_ROOT))
from research.gex_ict_v2_study.ict_signals import detect_mss, detect_ifvg, resample_ohlcv  # noqa: E402

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "nifty_gex_ict_v2_bot.log"),
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

STRATEGY_NAME = "NIFTY_GEX_ICT_V2"

IDX_SYMBOL   = "NIFTY"
IDX_EXCHANGE = "NSE_INDEX"
OPT_EXCHANGE = "NFO"

N_LOTS           = 10
DEFAULT_LOT_SIZE = 65
MIN_DTE          = 1
MAX_DTE          = 7

BUFFER_PCT          = 0.0150   # champion buffer_pct (Stage 2)
ROW_SIZE            = 10.0     # value-area bin size (points)
VALUE_AREA_PCT      = 0.70
HISTORY_DAYS_VA     = 5        # calendar-day lookback to find the prior trading session
SNAPSHOT_TIME       = dt_time(9, 20)
REGIME_REFRESH_MIN  = 5
RESAMPLE_MIN        = 5
CONFIRM_WINDOW_MIN  = 375
ICT_WINDOW_END      = dt_time(15, 29)
MIN_BARS_FOR_MSS    = 15

MARKET_OPEN  = dt_time(9, 15)
SESSION_END  = dt_time(15, 14)   # live-infra EOD (research champion used 15:20/15:29)

STATE_FILE = LOGS_DIR / "nifty_gex_ict_v2_bot_state.json"

# Decision-state logging (jsonl + throttled heartbeat — see shared/decision_logger.py)
DECISION_LOG   = LOGS_DIR / "nifty_gex_ict_v2_bot_decisions.jsonl"
HEARTBEAT_SECS = 300


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback


def _get_lot_size(symbol: str) -> int:
    """Fetch current lot size from the OpenAlgo token database. Falls back to
    DEFAULT_LOT_SIZE if the DB lookup fails, so SEBI/NSE lot-size changes are
    picked up automatically on the next session start."""
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


def _get_suitable_expiry() -> str | None:
    """Nearest NIFTY weekly expiry with DTE in [MIN_DTE, MAX_DTE]. Research champion
    used nearest weekly expiry with min_dte=1 (never trades 0-DTE)."""
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
    logger.warning(f"  No expiry found with DTE {MIN_DTE}-{MAX_DTE}. Dates: {dates[:6]}")
    return None


def _find_futures_symbol() -> dict | None:
    """Nearest-month NIFTY future -- used for the prior-day value-area profile,
    mirroring the research's compute_volume_profile() which profiles the
    nearest-month future's price/volume, not the index or options."""
    try:
        # NOTE: the symbol cache's `underlying` field is not populated for FUT
        # contracts (confirmed empty across NIFTY/BANKNIFTY/stock futures alike),
        # so filtering on it always returns zero rows. Search by name instead and
        # require an exact "<IDX_SYMBOL><digits>" prefix to exclude lookalikes
        # such as NIFTYNXT50FUT matching a "NIFTY" query.
        futures = fno_search_symbols(query=IDX_SYMBOL, exchange=OPT_EXCHANGE, instrumenttype="FUT", limit=20)
        prefix_re = re.compile(rf"^{re.escape(IDX_SYMBOL)}\d")
        futures = [f for f in futures if prefix_re.match(f.get("symbol", ""))]
        if not futures:
            return None

        def parse_expiry(exp_str: str) -> datetime:
            try:
                return datetime.strptime(exp_str, "%d-%b-%y")
            except (ValueError, TypeError):
                return datetime.max

        futures.sort(key=lambda f: parse_expiry(f.get("expiry", "")))
        return {"symbol": futures[0]["symbol"], "exchange": futures[0]["exchange"]}
    except Exception as e:
        logger.warning(f"  Futures symbol lookup failed: {e}")
        return None


def _history_df(raw_bars: list[dict]) -> pd.DataFrame | None:
    if not raw_bars:
        return None
    df = pd.DataFrame(raw_bars)
    if "timestamp" in df.columns:
        df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    elif "date" in df.columns:
        df["dt"] = pd.to_datetime(df["date"])
    else:
        return None
    return df.sort_values("dt")


def _compute_prev_day_value_area() -> tuple[float, float] | None:
    """Fixed-range (ROW_SIZE-pt bin) volume profile of the prior trading day's
    nearest-month future -- POC then expand to VALUE_AREA_PCT of session volume.
    Mirrors research's compute_volume_profile() exactly (row_size=10, va_pct=0.70)."""
    fut = _find_futures_symbol()
    if not fut:
        logger.warning("  No futures contract found for value-area computation.")
        return None

    raw = get_history(API_KEY, fut["symbol"], fut["exchange"], "1m", HISTORY_DAYS_VA)
    df = _history_df(raw)
    if df is None or df.empty or "volume" not in df.columns:
        logger.warning(f"  No usable history for futures {fut['symbol']}.")
        return None

    df["date_only"] = df["dt"].dt.date
    today = datetime.now().date()
    prior_dates = sorted(d for d in df["date_only"].unique() if d < today)
    if not prior_dates:
        return None
    prev_day = prior_dates[-1]

    day_df = df[
        (df["date_only"] == prev_day)
        & (df["dt"].dt.strftime("%H:%M").between("09:15", "15:29"))
    ]
    if day_df.empty or day_df["volume"].astype(float).sum() <= 0:
        return None

    closes = day_df["close"].astype(float)
    vols = day_df["volume"].astype(float)
    lo, hi = float(closes.min()), float(closes.max())
    if hi <= lo:
        return None

    bin_lo = np.floor(lo / ROW_SIZE) * ROW_SIZE
    bin_hi = np.ceil(hi / ROW_SIZE) * ROW_SIZE + ROW_SIZE
    bins = np.arange(bin_lo, bin_hi, ROW_SIZE)
    binned = pd.cut(closes, bins=bins, include_lowest=True)
    vol_by_bin = vols.groupby(binned, observed=True).sum().sort_index()
    vol_by_bin = vol_by_bin[vol_by_bin > 0]
    if vol_by_bin.empty:
        return None

    poc_bin = vol_by_bin.idxmax()
    total_vol = vol_by_bin.sum()
    target = total_vol * VALUE_AREA_PCT
    bins_sorted = list(vol_by_bin.index)
    poc_idx = bins_sorted.index(poc_bin)
    lo_idx = hi_idx = poc_idx
    captured = vol_by_bin.iloc[poc_idx]
    while captured < target and (lo_idx > 0 or hi_idx < len(bins_sorted) - 1):
        vol_below = vol_by_bin.iloc[lo_idx - 1] if lo_idx > 0 else -1
        vol_above = vol_by_bin.iloc[hi_idx + 1] if hi_idx < len(bins_sorted) - 1 else -1
        if vol_above >= vol_below:
            hi_idx += 1
            captured += vol_by_bin.iloc[hi_idx]
        else:
            lo_idx -= 1
            captured += vol_by_bin.iloc[lo_idx]

    vah = float(bins_sorted[hi_idx].right)
    val = float(bins_sorted[lo_idx].left)
    return vah, val


def _derive_gex_levels(spot: float, chain: list[dict]) -> dict:
    """Replicates research's compute_gex_profile() level/regime derivation
    (HVL zero-crossing, call_resistance/put_support, top-3 |GEX| levels)
    using the live GEX chain's per-strike ce_gex/pe_gex/net_gex fields.
    Live chain's gamma*OI*lotsize scaling differs from research's
    gamma*OI*lotsize*spot^2*0.01, but that's a uniform positive scalar
    across all strikes -- rankings/argmax/zero-crossing are unaffected."""
    strikes = sorted({float(row["strike"]) for row in chain})
    net_by_strike = {float(row["strike"]): float(row.get("net_gex", 0) or 0) for row in chain}
    ce_by_strike  = {float(row["strike"]): float(row.get("ce_gex", 0) or 0) for row in chain}
    pe_by_strike  = {float(row["strike"]): float(row.get("pe_gex", 0) or 0) for row in chain}

    cum = 0.0
    cum_vals = []
    for k in strikes:
        cum += net_by_strike.get(k, 0.0)
        cum_vals.append(cum)

    hvl = None
    for i in range(1, len(strikes)):
        s0, s1 = cum_vals[i - 1], cum_vals[i]
        sign0 = 0 if s0 == 0 else (1 if s0 > 0 else -1)
        sign1 = 0 if s1 == 0 else (1 if s1 > 0 else -1)
        if sign0 != 0 and sign1 != 0 and sign0 != sign1:
            k0, k1 = strikes[i - 1], strikes[i]
            hvl = k0 + (k1 - k0) * (-s0) / (s1 - s0) if s1 != s0 else k0
            break
    if hvl is None:
        hvl = min(strikes, key=lambda k: abs(k - spot)) if strikes else spot

    calls_above = {k: v for k, v in ce_by_strike.items() if k > spot and v != 0}
    puts_below  = {k: v for k, v in pe_by_strike.items() if k < spot and v != 0}
    call_resistance = max(calls_above, key=calls_above.get) if calls_above else None
    put_support     = max(puts_below, key=puts_below.get) if puts_below else None

    excluded = {call_resistance, put_support}
    magnitude = sorted(
        ((k, abs(v)) for k, v in net_by_strike.items() if k not in excluded),
        key=lambda kv: kv[1], reverse=True,
    )
    gex_levels = [k for k, _ in magnitude[:3]]

    return {
        "hvl": hvl,
        "call_resistance": call_resistance,
        "put_support": put_support,
        "gex_levels": gex_levels,
        "regime": "positive_gamma" if spot > hvl else "negative_gamma",
    }


def _bars_indexed_df(bars: deque) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(list(bars)).set_index("ts")[["open", "high", "low", "close"]]
    df["volume"] = 0
    return df


class NiftyGexIctV2Bot:
    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.ws = None

        self.nifty_ltp: float = 0.0

        self.bars: deque = deque(maxlen=500)
        self._bar_open: float = 0.0
        self._bar_high: float = 0.0
        self._bar_low: float = float("inf")
        self._bar_minute: int = -1

        # Session metadata (resolved at 09:15)
        self.expiry: str | None = None
        self.lot_size: int = DEFAULT_LOT_SIZE
        self.vah: float | None = None
        self.val: float | None = None

        # GEX snapshot / regime state
        self.gex_0920: dict | None = None
        self.current_regime: str | None = None
        self._last_regime_refresh_min: datetime | None = None

        # VA-break / level-reach / ICT confirmation pipeline state (reset daily)
        self.break_dir: str | None = None
        self.break_ts = None
        self.candidate_levels: list[float] = []
        self.level_order: list[float] = []
        self.level_touched: list[bool] = []
        self.reached_level: float | None = None
        self.reach_ts = None
        self.module: str | None = None
        self.continuation_dir: str | None = None
        self.entry_dir: str | None = None
        self.window_end: datetime | None = None
        self.signal_done_today: bool = False

        self.active_trade: dict | None = None

        self._session_started = False
        self._next_init_attempt = datetime.min
        self._eod_exit_done = False
        self._first_connect = True
        self._subscribed_syms: set[str] = set()

        self.trades_today = 0
        self.wins_today = 0
        self.pnl_today = 0.0

        # Decision-state logging (jsonl + throttled heartbeat)
        self._dlog = DecisionLogger(DECISION_LOG, heartbeat_secs=HEARTBEAT_SECS, bot_logger=logger)

        # Dead-feed watchdog: alerts if NIFTY ticks go quiet for DEAD_FEED_SECS
        # during market hours (Task #14 — a hung-but-not-erroring socket would
        # otherwise never trigger the reconnect-on-exception loop). Only
        # IDX_SYMBOL is tracked -- option legs are priced via REST
        # (get_option_ltp), never over WS, so tracking them would be a
        # permanent false dead-feed alarm.
        self._watchdog = TickWatchdog(
            bot_name="NIFTY GEX+ICT v2 Bot",
            tracked_symbols=lambda: [IDX_SYMBOL],
            market_open=MARKET_OPEN,
            market_close=SESSION_END,
            bot_logger=logger,
        )

        self._restore_state()

    # ── State persistence ────────────────────────────────────────────────────

    def _restore_state(self) -> None:
        if not STATE_FILE.exists():
            return
        try:
            state = json.loads(STATE_FILE.read_text())
            last_update_str = state.get("last_update", "")
            if not last_update_str:
                return
            last_update = datetime.fromisoformat(last_update_str)
            if last_update.date() != datetime.now().date():
                logger.info("_restore_state: state file is from a previous day -- skipping restore")
                return

            self.expiry = state.get("expiry") or self.expiry
            if state.get("lot_size"):
                self.lot_size = state["lot_size"]
            self.vah = state.get("vah", self.vah)
            self.val = state.get("val", self.val)
            self.gex_0920 = state.get("gex_0920") or self.gex_0920
            self.current_regime = state.get("current_regime") or self.current_regime
            self.break_dir = state.get("break_dir")
            self.break_ts = pd.to_datetime(state["break_ts"]) if state.get("break_ts") else None
            self.candidate_levels = state.get("candidate_levels") or []
            self.level_order = state.get("level_order") or []
            self.level_touched = state.get("level_touched") or []
            self.reached_level = state.get("reached_level")
            self.reach_ts = pd.to_datetime(state["reach_ts"]) if state.get("reach_ts") else None
            self.module = state.get("module")
            self.continuation_dir = state.get("continuation_dir")
            self.entry_dir = state.get("entry_dir")
            self.window_end = datetime.fromisoformat(state["window_end"]) if state.get("window_end") else None
            self.signal_done_today = state.get("signal_done_today", False)

            active = state.get("active_trade")
            if active:
                self.active_trade = active
                logger.warning(f"🔄 Restored ACTIVE trade: {active.get('symbol')} entry={active.get('entry_prem')}")

            if self.expiry and self.vah is not None:
                self._session_started = True

            if self.gex_0920 or self.break_dir or self.active_trade:
                logger.info("_restore_state: mid-session restart -- restored signal-pipeline state")
        except Exception as e:
            logger.warning(f"_restore_state: could not read state file: {e}")

    async def _state_dump_loop(self) -> None:
        while True:
            try:
                STATE_FILE.write_text(json.dumps({
                    "strategy": STRATEGY_NAME,
                    "last_update": datetime.now().isoformat(),
                    "nifty_ltp": round(self.nifty_ltp, 2),
                    "expiry": self.expiry,
                    "lot_size": self.lot_size,
                    "n_lots": N_LOTS,
                    "bars_loaded": len(self.bars),
                    "vah": self.vah,
                    "val": self.val,
                    "gex_0920": self.gex_0920,
                    "current_regime": self.current_regime,
                    "break_dir": self.break_dir,
                    "break_ts": self.break_ts.isoformat() if self.break_ts is not None else None,
                    "candidate_levels": self.candidate_levels,
                    "level_order": self.level_order,
                    "level_touched": self.level_touched,
                    "reached_level": self.reached_level,
                    "reach_ts": self.reach_ts.isoformat() if self.reach_ts is not None else None,
                    "module": self.module,
                    "continuation_dir": self.continuation_dir,
                    "entry_dir": self.entry_dir,
                    "window_end": self.window_end.isoformat() if self.window_end is not None else None,
                    "signal_done_today": self.signal_done_today,
                    "active_trade": self.active_trade,
                    "trades_today": self.trades_today,
                    "wins_today": self.wins_today,
                    "pnl_today": round(self.pnl_today, 2),
                }, default=str))
            except Exception:
                pass
            await asyncio.sleep(2)

    # ── Warm-up ───────────────────────────────────────────────────────────────

    async def _warmup_today_bars(self) -> None:
        logger.info("📡 Warming up today's NIFTY 1-min bars…")
        try:
            raw = await asyncio.to_thread(get_history, API_KEY, IDX_SYMBOL, IDX_EXCHANGE, "1m", 1)
            df = _history_df(raw)
            if df is None or df.empty:
                return
            today = datetime.now().date()
            df = df[df["dt"].dt.date == today]
            for _, row in df.iterrows():
                c = float(row.get("close", 0))
                self.bars.append({
                    "ts": row["dt"].to_pydatetime().replace(second=0, microsecond=0),
                    "open": float(row.get("open", c)),
                    "high": float(row.get("high", c)),
                    "low": float(row.get("low", c)),
                    "close": c,
                })
            if self.bars:
                self.nifty_ltp = self.bars[-1]["close"]
            logger.info(f"  ✅ Warm-up: {len(self.bars)} bars loaded for today.")
        except Exception as e:
            logger.error(f"  Today warm-up error: {e}")

    # ── Session open (09:15) ─────────────────────────────────────────────────

    async def _session_open(self, spot: float) -> None:
        logger.info(f"🔔 Session open. NIFTY spot ≈ {spot:.0f}")
        self.expiry = await asyncio.to_thread(_get_suitable_expiry)
        if self.expiry:
            resolved = await asyncio.to_thread(resolve_atm_option, spot, "PE", API_KEY, MIN_DTE, IDX_SYMBOL)
            if resolved:
                self.lot_size = _get_lot_size(resolved["symbol"])
            logger.info(f"  Expiry: {self.expiry} | Lot size: {self.lot_size}")
        else:
            logger.warning("  No suitable expiry found -- no entries today.")

        va = await asyncio.to_thread(_compute_prev_day_value_area)
        if va:
            self.vah, self.val = va
            logger.info(f"  Value area: VAH={self.vah:.1f} VAL={self.val:.1f}")
        else:
            logger.warning("  Volume profile unavailable -- no entries today.")

    async def _snapshot_gex(self, initial: bool) -> None:
        if not self.expiry:
            return
        success, data, _ = await asyncio.to_thread(get_gex_data, IDX_SYMBOL, OPT_EXCHANGE, self.expiry, API_KEY)
        if not success or not data.get("chain"):
            logger.warning(f"  GEX fetch failed: {data}")
            return
        spot = data.get("spot_price") or self.nifty_ltp
        levels = _derive_gex_levels(spot, data["chain"])
        self.current_regime = levels["regime"]
        if initial:
            self.gex_0920 = levels
            logger.info(
                f"📊 GEX 09:20 snapshot: HVL={levels['hvl']:.1f} "
                f"call_res={levels['call_resistance']} put_sup={levels['put_support']} "
                f"gex_levels={levels['gex_levels']} regime={levels['regime']}"
            )

    # ── Bar building ──────────────────────────────────────────────────────────

    def _update_bar(self, ltp: float, ts: datetime) -> bool:
        """Update the running 1-min bar. Returns True and leaves self.bars[-1]
        as the just-closed bar when a new minute boundary is crossed."""
        minute_key = ts.hour * 60 + ts.minute
        if self._bar_minute == -1:
            self._bar_minute = minute_key
            self._bar_open = self._bar_high = self._bar_low = ltp
            return False

        if minute_key == self._bar_minute:
            self._bar_high = max(self._bar_high, ltp)
            self._bar_low = min(self._bar_low, ltp)
            return False

        closed = False
        if self._bar_open > 0:
            bar_hour, bar_min = divmod(self._bar_minute, 60)
            bar_ts = ts.replace(hour=bar_hour, minute=bar_min, second=0, microsecond=0)
            self.bars.append({
                "ts": bar_ts, "open": self._bar_open,
                "high": self._bar_high, "low": self._bar_low, "close": ltp,
            })
            closed = True

        self._bar_minute = minute_key
        self._bar_open = self._bar_high = self._bar_low = ltp
        return closed

    # ── Signal pipeline ───────────────────────────────────────────────────────

    async def _check_signal_pipeline(self) -> None:
        if self.signal_done_today or self.active_trade is not None:
            return
        if self.vah is None or self.val is None or not self.expiry or not self.bars:
            return

        bar = self.bars[-1]
        bar_ts = bar["ts"]
        now = datetime.now()

        if self.gex_0920 is None:
            if now.time() >= SNAPSHOT_TIME:
                await self._snapshot_gex(initial=True)
            if self.gex_0920 is None:
                return

        bar_minute = bar_ts.replace(second=0, microsecond=0)
        if (self._last_regime_refresh_min is None
                or bar_minute - self._last_regime_refresh_min >= timedelta(minutes=REGIME_REFRESH_MIN)):
            await self._snapshot_gex(initial=False)
            self._last_regime_refresh_min = bar_minute

        # Step 1: first VA break of the day (sticky)
        if self.break_dir is None:
            if bar["high"] > self.vah:
                self.break_dir = "up"
            elif bar["low"] < self.val:
                self.break_dir = "down"
            if self.break_dir is None:
                return
            self.break_ts = bar_ts

            if self.break_dir == "up":
                raw = [self.gex_0920["call_resistance"]] + self.gex_0920["gex_levels"]
                self.candidate_levels = [lvl for lvl in raw if lvl is not None and lvl > self.vah]
            else:
                raw = [self.gex_0920["put_support"]] + self.gex_0920["gex_levels"]
                self.candidate_levels = [lvl for lvl in raw if lvl is not None and lvl < self.val]

            if not self.candidate_levels:
                logger.info(f"  VA break {self.break_dir} @ {bar_ts} but no candidate levels -- no signal today.")
                self.signal_done_today = True
                return

            self.level_order = sorted(self.candidate_levels, key=lambda x: abs(x - self.candidate_levels[0]))
            self.level_touched = [False] * len(self.level_order)
            logger.info(f"  VA break {self.break_dir} @ {bar_ts} (VAH={self.vah:.1f} VAL={self.val:.1f}) "
                        f"candidates={self.level_order}")

        # Step 2: level reach (priority order, cumulative since break)
        if self.reached_level is None:
            for i, lvl in enumerate(self.level_order):
                if not self.level_touched[i]:
                    if self.break_dir == "up" and bar["high"] > lvl:
                        self.level_touched[i] = True
                    elif self.break_dir == "down" and bar["low"] < lvl:
                        self.level_touched[i] = True
            for i, lvl in enumerate(self.level_order):
                if self.level_touched[i]:
                    self.reached_level = lvl
                    self.reach_ts = bar_ts
                    self.window_end = min(
                        bar_ts + timedelta(minutes=CONFIRM_WINDOW_MIN),
                        datetime.combine(bar_ts.date(), ICT_WINDOW_END),
                    )
                    logger.info(f"  Level reached: {lvl} @ {bar_ts} (regime={self.current_regime})")
                    break
            if self.reached_level is None:
                return

        # Step 3: module classification, frozen at reach time
        if self.module is None:
            self.module = "fade" if self.current_regime == "positive_gamma" else "breakout"
            if self.module == "fade":
                logger.info("  Module = fade -- excluded from live bot per research design. No trade today.")
                self.signal_done_today = True
                return
            self.continuation_dir = "bullish" if self.break_dir == "up" else "bearish"
            self.entry_dir = "long" if self.break_dir == "up" else "short"
            logger.info(f"  Module = breakout, continuation_dir={self.continuation_dir}, entry_dir={self.entry_dir}")

        # Step 4: confirmation window
        if now > self.window_end:
            logger.info("  Confirmation window expired with no ICT confirm -- no signal today.")
            self.signal_done_today = True
            return

        # Step 5: ICT confirmation (MSS or IFVG)
        if len(self.bars) < MIN_BARS_FOR_MSS:
            return
        df5 = resample_ohlcv(_bars_indexed_df(self.bars), RESAMPLE_MIN)
        if df5.empty:
            return

        mss = [m for m in detect_mss(df5, k=2)
               if m["direction"] == self.continuation_dir and self.reach_ts < m["ts"] <= self.window_end]
        ifv = [e for e in detect_ifvg(df5)
               if e["direction"] == self.continuation_dir and self.reach_ts < e["ts"] <= self.window_end]
        candidates = [dict(m, kind="MSS", ts=m["ts"]) for m in mss] + \
                     [dict(e, kind="IFVG", ts=e["ts"]) for e in ifv]
        if not candidates:
            return
        conf = min(candidates, key=lambda c: c["ts"])
        logger.info(f"  ICT confirm: {conf['kind']} @ {conf['ts']}")
        await self._enter_trade(conf["kind"])
        self.signal_done_today = True

    # ── Entry / exit ──────────────────────────────────────────────────────────

    async def _enter_trade(self, confirm_kind: str) -> None:
        if self.active_trade is not None or not self.expiry:
            return
        spot = self.nifty_ltp
        if spot <= 0:
            return
        opt_type = "PE" if self.entry_dir == "long" else "CE"

        resolved = await asyncio.to_thread(resolve_atm_option, spot, opt_type, API_KEY, MIN_DTE, IDX_SYMBOL)
        if not resolved:
            logger.error(f"  ATM symbol resolution failed (spot={spot:.0f}, expiry={self.expiry})")
            return
        symbol = resolved["symbol"]

        opt_ltp = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        if opt_ltp <= 0:
            logger.warning(f"  Option LTP=0 for {symbol}. Entry skipped.")
            return

        lot_size = _get_lot_size(symbol)
        qty = N_LOTS * lot_size

        reached = self.reached_level
        if self.entry_dir == "long":
            stop = reached * (1 - BUFFER_PCT)
        else:
            stop = reached * (1 + BUFFER_PCT)
        further = [lvl for lvl in self.candidate_levels
                   if (lvl > reached if self.entry_dir == "long" else lvl < reached)]
        if further:
            target1 = min(further, key=lambda x: abs(x - reached))
        else:
            target1 = self.gex_0920["call_resistance"] if self.entry_dir == "long" else self.gex_0920["put_support"]

        logger.info(
            f"  Entering {opt_type} SELL {symbol} LTP=₹{opt_ltp:.2f} qty={qty} "
            f"({N_LOTS}×{lot_size}) confirm={confirm_kind} spot_stop={stop:.1f} spot_target1={target1}"
        )

        try:
            res = self.client.placesmartorder(
                strategy=STRATEGY_NAME, symbol=symbol, action="SELL", exchange=OPT_EXCHANGE,
                price_type="MARKET", product="MIS", quantity=qty, position_size=-qty,
            )
        except Exception as e:
            logger.error(f"  placesmartorder exception: {e}")
            return

        if res.get("status") == "success":
            fill_prem = _resolve_fill(res, opt_ltp)
            self.active_trade = {
                "opt_type": opt_type, "symbol": symbol, "entry_prem": fill_prem,
                "qty": qty, "lot_size": lot_size, "order_id": str(res.get("orderid", "")),
                "entry_time": datetime.now().isoformat(), "confirm_kind": confirm_kind,
                "spot_stop": stop, "spot_target1": float(target1) if target1 is not None else None,
                "entry_dir": self.entry_dir,
            }
            self.lot_size = lot_size
            logger.info(f"✅ SOLD {symbol} @ ₹{fill_prem:.2f} ({N_LOTS} lots, order={res.get('orderid')})")
            await send_async(
                f"📉 *NIFTY GEX+ICT v2 — ENTRY*\n"
                f"Sold `{symbol}` ({N_LOTS} lots) — {confirm_kind} confirm\n"
                f"Entry premium : ₹{fill_prem:.2f}\n"
                f"Spot stop     : {stop:.1f}\n"
                f"Spot target1  : {target1}\n"
                f"Exit          : EOD 15:14 IST\n"
                f"NIFTY: {spot:.1f}\n"
                f"_Signal: {datetime.now().strftime('%H:%M')}_"
            )
        else:
            logger.error(f"  Order rejected: {res}")

    async def _monitor_active_trade(self, bar: dict) -> None:
        trade = self.active_trade
        if not trade:
            return
        high, low = bar["high"], bar["low"]
        if trade["entry_dir"] == "long":
            if low <= trade["spot_stop"]:
                await self._close_trade("Spot Stop")
                return
            if trade["spot_target1"] is not None and high >= trade["spot_target1"]:
                await self._close_trade("Target1")
                return
        else:
            if high >= trade["spot_stop"]:
                await self._close_trade("Spot Stop")
                return
            if trade["spot_target1"] is not None and low <= trade["spot_target1"]:
                await self._close_trade("Target1")
                return

    async def _close_trade(self, reason: str) -> None:
        trade = self.active_trade
        if not trade:
            return
        symbol = trade["symbol"]
        qty = trade["qty"]

        exit_ltp = await asyncio.to_thread(get_option_ltp, symbol, OPT_EXCHANGE, API_KEY)
        exit_snapshot = exit_ltp if exit_ltp > 0 else trade["entry_prem"]

        try:
            res = self.client.placeorder(
                strategy=STRATEGY_NAME, symbol=symbol, action="BUY", exchange=OPT_EXCHANGE,
                price_type="MARKET", product="MIS", quantity=str(qty),
            )
        except Exception as e:
            logger.error(f"  Close order exception: {e}")
            return

        order_ok = res.get("status") == "success"
        if not order_ok:
            logger.warning(f"  Exit order non-success (likely auto-squareoff): {res} -- logging and clearing state.")

        exit_fill = _resolve_fill(res, exit_snapshot)
        gross = (trade["entry_prem"] - exit_fill) * qty
        won = gross > 0
        emoji = "🟢" if won else "🔴"

        self.active_trade = None
        self.signal_done_today = True
        self.trades_today += 1
        if won:
            self.wins_today += 1
        self.pnl_today += gross

        auto_sq_note = " _(closed by auto-squareoff)_" if not order_ok else ""
        logger.info(f"{emoji} CLOSED {symbol} @ ₹{exit_fill:.2f} reason={reason} gross=₹{gross:,.0f}")
        await send_async(
            f"{emoji} *NIFTY GEX+ICT v2 — EXIT ({reason})*\n"
            f"Symbol : `{symbol}`\n"
            f"Entry  : ₹{trade['entry_prem']:.2f} → Exit: ₹{exit_fill:.2f}\n"
            f"Gross P&L: ₹{gross:,.0f} ({N_LOTS} lots)\n"
            f"_Closed at {datetime.now().strftime('%H:%M:%S')}{auto_sq_note}_"
        )
        log_trade_to_db(
            bot_name="nifty_gex_ict_v2_bot", instrument=IDX_SYMBOL, option_symbol=symbol,
            option_type=trade["opt_type"], entry_time=trade["entry_time"], exit_time=datetime.now(),
            entry_premium=trade["entry_prem"], exit_premium=exit_fill, exit_reason=reason,
            quantity=qty, lots=N_LOTS, lot_size=trade["lot_size"], gross_pnl=gross,
            order_id=trade.get("order_id"),
        )

    async def _eod_close_all(self) -> None:
        if self._eod_exit_done:
            return
        self._eod_exit_done = True
        if self.active_trade:
            logger.info("🔔 EOD 15:14 IST -- closing open position.")
            await self._close_trade("EOD 15:14")

    # ── Decision-state logging ───────────────────────────────────────────────

    def _verdict(self, now_t: dt_time) -> str:
        """What's currently blocking entry — checked in the order these gates
        actually apply in _on_index_tick()/_check_signal_pipeline(), so it
        always names the real blocker."""
        if not self._session_started:
            return "waiting for session start"
        if now_t >= SESSION_END:
            return "session ended"
        if self.active_trade:
            t = self.active_trade
            return f"ACTIVE: holding {t['opt_type']} {t['symbol']} to target1/stop/EOD"
        if self.signal_done_today:
            return "done for today: signal pipeline resolved (trade taken, module=fade, or window expired)"
        if self.vah is None or self.val is None:
            return "BLOCKED: no prior-day value area computed"
        if not self.expiry:
            return "BLOCKED: no suitable expiry found"
        if self.gex_0920 is None:
            if now_t < SNAPSHOT_TIME:
                return f"waiting for GEX 09:20 snapshot ({SNAPSHOT_TIME.strftime('%H:%M')})"
            return "BLOCKED: GEX snapshot fetch pending/failed"
        if self.break_dir is None:
            return f"🔥 watching for first VA break (VAH={self.vah:.1f} VAL={self.val:.1f})"
        if self.reached_level is None:
            return (f"🔥 break={self.break_dir} @ {self.break_ts} — watching for "
                    f"candidate level reach {self.level_order}")
        if self.module is None:
            return f"level reached {self.reached_level} — classifying module (regime={self.current_regime})"
        if len(self.bars) < MIN_BARS_FOR_MSS:
            return f"warming up: {len(self.bars)}/{MIN_BARS_FOR_MSS} bars for MSS/IFVG"
        window_txt = self.window_end.strftime('%H:%M') if self.window_end else "?"
        return (f"🔥 module=breakout dir={self.continuation_dir} — watching for MSS/IFVG "
                f"confirm (window ends {window_txt})")

    def _heartbeat_text(self) -> str:
        now_t = datetime.now().time()
        lines = [f"💓 DECISION STATE {datetime.now().strftime('%H:%M:%S')} ─ {self._verdict(now_t)}"]
        lines.append(
            f"    NIFTY={self.nifty_ltp:.1f}  bars={len(self.bars)}  expiry={self.expiry}  "
            f"VAH={self.vah}  VAL={self.val}"
        )
        if self.gex_0920:
            lines.append(
                f"    GEX: regime={self.current_regime}  call_res={self.gex_0920.get('call_resistance')}  "
                f"put_sup={self.gex_0920.get('put_support')}  levels={self.gex_0920.get('gex_levels')}"
            )
        if self.active_trade:
            t = self.active_trade
            lines.append(
                f"    active: {t['opt_type']} {t['symbol']}  entry={t['entry_prem']}  "
                f"stop={t.get('spot_stop')}  target1={t.get('spot_target1')}"
            )
        return "\n".join(lines)

    # ── Tick routing ──────────────────────────────────────────────────────────

    async def _on_index_tick(self, ltp: float, ts: datetime) -> None:
        self.nifty_ltp = ltp
        now = datetime.now()

        # Throttled to HEARTBEAT_SECS internally — cheap to call on every tick
        self._dlog.maybe_heartbeat(self._heartbeat_text)

        if (not self._session_started and now.time() >= MARKET_OPEN
                and now >= self._next_init_attempt):
            await self._session_open(ltp)
            if self.expiry and self.vah is not None:
                self._session_started = True
            else:
                self._next_init_attempt = now + timedelta(seconds=120)
                logger.warning("  ⚠️  Session init incomplete -- retrying in 120s.")

        bar_closed = self._update_bar(ltp, ts)
        if not bar_closed:
            return

        # Logged unconditionally, before the active-trade / signal-pipeline
        # branches below, so the log always has real values explaining
        # "why not" — not just "why yes".
        bar = self.bars[-1]
        self._dlog.log_bar({
            "phase":             "ACTIVE" if self.active_trade else ("DONE" if self.signal_done_today else "WATCHING"),
            "bar_time":          bar["ts"].strftime("%H:%M") if bar.get("ts") else None,
            "bar_close":         bar["close"],
            "vah":               self.vah,
            "val":               self.val,
            "break_dir":         self.break_dir,
            "reached_level":     self.reached_level,
            "module":            self.module,
            "continuation_dir":  self.continuation_dir,
            "current_regime":    self.current_regime,
            "signal_done_today": self.signal_done_today,
            "active_trade":      self.active_trade.get("symbol") if self.active_trade else None,
            "bars_loaded":       len(self.bars),
            "verdict":           self._verdict(now.time()),
        })

        if self.active_trade is not None:
            await self._monitor_active_trade(self.bars[-1])
            return

        await self._check_signal_pipeline()

    # ── WebSocket subscription ───────────────────────────────────────────────

    async def _subscribe(self, symbol: str, exchange: str) -> None:
        if self.ws and symbol not in self._subscribed_syms:
            try:
                await self.ws.send(json.dumps({"action": "subscribe", "symbol": symbol, "exchange": exchange, "mode": 2}))
                self._subscribed_syms.add(symbol)
                logger.info(f"  📡 Subscribed: {symbol}")
            except Exception as e:
                logger.warning(f"  Subscribe error for {symbol}: {e}")

    async def _resubscribe_all(self) -> None:
        self._subscribed_syms.clear()
        await self._subscribe(IDX_SYMBOL, IDX_EXCHANGE)

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def main_loop(self) -> None:
        await self._warmup_today_bars()
        asyncio.create_task(self._state_dump_loop())
        asyncio.create_task(self._watchdog.watch_loop())

        retry_delay = 5
        while True:
            now = datetime.now()
            if now.time() >= SESSION_END:
                await self._eod_close_all()
                logger.info("✅ Past 15:14 IST -- shutting down.")
                break

            try:
                logger.info(f"🔌 Connecting to WebSocket: {WS_URL}")
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60) as ws:
                    self.ws = ws
                    await ws.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))
                    await self._resubscribe_all()

                    if self._first_connect:
                        self._first_connect = False
                        await send_async(
                            f"🤖 *NIFTY GEX+ICT v2 Bot — Online*\n"
                            f"Breakout-only, sell-side (bull→sell PE, bear→sell CE)\n"
                            f"GEX 09:20 snapshot + 5-min regime refresh\n"
                            f"Confirm: MSS or IFVG (window +{CONFIRM_WINDOW_MIN}min)\n"
                            f"buffer_pct={BUFFER_PCT*100:.2f}% spot stop/target1\n"
                            f"Quantity: {N_LOTS} lots  |  Min DTE {MIN_DTE}\n"
                            f"Exit: target1 / stop / EOD 15:14 IST\n"
                            f"Backtest: NIFTY OOS Sharpe 4.10, combined Sharpe 2.48 (see results_summary.md)"
                        )
                    else:
                        logger.info("WebSocket reconnected.")

                    retry_delay = 5

                    async for raw in ws:
                        if datetime.now().time() >= SESSION_END:
                            await self._eod_close_all()
                            return

                        msg = json.loads(raw)
                        if msg.get("type") != "market_data":
                            continue

                        sym = msg.get("symbol", "")
                        self._watchdog.on_tick(sym)
                        mdata = msg.get("data", {})
                        ltp = float(mdata.get("ltp", 0) or mdata.get("lp", 0))
                        ts_raw = mdata.get("t")
                        ts = datetime.fromtimestamp(float(ts_raw)) if ts_raw else datetime.now()

                        if ltp <= 0:
                            continue

                        if sym in (IDX_SYMBOL, f"{IDX_EXCHANGE}:{IDX_SYMBOL}"):
                            await self._on_index_tick(ltp, ts)

            except Exception as e:
                logger.warning(f"WebSocket error: {e}. Reconnecting in {retry_delay}s…")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


if __name__ == "__main__":
    bot = NiftyGexIctV2Bot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("Stop signal received -- bot terminated.")
