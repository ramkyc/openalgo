"""
ATM Opening-Range POC Reversion Bot
====================================
live_trading/atm_poc_reversion_bot/atm_poc_reversion_bot.py

Research study: options_data/research/atm_opening_range_poc_reversion_study/
  Full pipeline (Discovery -> IS -> OOS -> Bootstrap -> Walk-forward ->
  Add-trigger sweep -> Max-adds sweep -> Regime Filter -> Multi-Instrument ->
  Expiry Segmentation -> Analyzer Validation): all stages PASS. See
  results_summary.md for the complete results table and DECISIONS.md for the
  14 numbered locked decisions this bot implements.

Strategy (results_summary.md, 01_strategy_definition.md, DECISIONS.md):
  Each trading day, the NIFTY ATM CE and ATM PE (nearest-50 strike to NIFTY
  spot at the 09:15 session open, fixed for the day -- confirmed by reading
  stage0_discovery.py's build_daily_meta()/load_spot_opens(), which resolve
  strike from the 09:15 index open bar, NOT literally "at first entry" as
  01_strategy_definition.md's prose says) each build an expanding intraday
  Volume Profile POC from 09:15, independently. The opening range is 09:15-
  09:29 IST (15 one-minute bars, requires >= MIN_OR_BARS=10 or the option is
  skipped for the day). When either option's own premium later moves beyond
  its own opening-range high/low, that is a breakout: the strategy trades
  mean reversion back to that option's own live POC -- an upside breakout is
  SOLD (target = POC below), a downside breakout is BOUGHT (target = POC
  above). CE and PE run as two fully independent tracks, and within each
  option, "short" (upside) and "long" (downside) also run independently, so
  up to 4 tracks can be live for a given day. A track can scale in up to
  MAX_ADDS times on further adverse extension, gated on ALL THREE add
  thresholds simultaneously (distance to developing POC, min time gap, min
  price gap vs. the prior extreme). Once any add fires, the target for every
  leg in that track (entry + adds) switches from the frozen opening POC to
  the live developing POC, recomputed every bar for the rest of the day.

Locked configuration (DECISIONS.md #11, #12, #13, as amended by the round-2
follow-up study -- confirmed_close and the B_THRESH=15 change below; see
options_data/research/atm_opening_range_poc_reversion_study/sl_final_locked_config_v7.py):
  THRESHOLD (no-add target distance)              = Rs 30
  A_THRESH  (add-trigger: distance to dev. POC)   = Rs 15
  B_THRESH  (add-trigger: min. time gap)          = 15 min  (round-2 correction, was 3 min)
  C_THRESH  (add-trigger: price gap vs. prior ext)= Rs 5
  MAX_ADDS  (scale-in cap)                        = 2

Round-2 follow-up corrections (2026-08-30), applied after re-running the
strategy's mechanics through further investigation on top of the original
11-stage pipeline -- not a new pipeline stage, a direct port of the locked
research decisions:
  - Entry/add fill = CONFIRMED CLOSE, not the wick. A breakout only fires if
    the SAME 1-min bar's CLOSE also clears the running extreme (not just its
    high/low) -- fill price is that bar's close. Previously this bot fired
    and filled on the bar's intrabar high/low the instant it printed, which
    is not achievable in live trading (you only know a bar made a new
    high/low after it has already closed). See SymbolTrack.process_bar().
  - POC-move gate: investigated, explicitly REJECTED, stays OFF (never
    implemented in this bot -- no code change, noted here so this file
    reflects the same decision as the study's final locked config).
  - B_THRESH raised 3 -> 15 minutes: best Sharpe and fewest SL exits across
    both study windows, roughly P&L-neutral (+4.6% FY2025-26, -5.3% 2026
    Apr-Aug) vs. the original 3-minute value.

Deliberate live-only departures from the backtest (none of these are
strategy-mechanic changes -- they are execution/risk adaptations required
for real order flow, same category as every other bot in this repo):
  - Stop-loss (NEW, not present in the study -- results_summary.md Risk #7
    explicitly flags this as required before live deployment). Rs-based,
    tied to the strategy's own units: SL = entry leg's own price moved a
    further Rs 60 (= 2 x THRESHOLD) adverse, anchored to the ORIGINAL entry
    leg's price and never moved by subsequent adds. Checked via a fast LTP
    poll independent of the 1-min-bar cadence used for entries/adds/targets.
  - EOD square-off at 15:14 IST, not the study's 15:29 IST. The sandbox
    force-squares-off all MIS positions at 15:15 IST; any close order after
    that silently fails (see CLAUDE.md "MIS/intraday EOD exit hard limit").
  - Expiry resolution uses min_dte=2 (this repo's standard live-safety
    convention, see resolve_atm_option/_template_bot.py) rather than
    whatever expiry happened to be nearest in the historical data -- avoids
    trading the illiquid, gamma-unstable final 1-2 DTE session.
  - Lot size resolved dynamically per-symbol via database.token_db, not the
    study's hardcoded LOT_SIZE=75 approximation (DECISIONS.md #7 explicitly
    flags that as needing correction "before any live-promotion decision").
    DEFAULT_LOT_SIZE=65 is a documented fallback only, used solely if the DB
    lookup fails -- never the primary source.
  - Position size: 10 lots per leg (explicit user decision, not a study
    parameter -- the study only validates a fixed contract count of 1x QTY
    per leg via LOT_SIZE*QTY; sizing itself carries no separate edge claim).
  - Startup gate (2026-09-26): OpenAlgo holiday calendar (NFO) is primary;
    Fyers marketStatus is re-asked until 09:05 if it says no session, since
    it reports none before ~08:45 (bot missed 2026-09-23..25 that way).
  - Instrument scope: NIFTY only for this build. BANKNIFTY/SENSEX (both
    validated in Stage 9, both with their own documented risks -- BANKNIFTY
    far-DTE weakness, SENSEX's shorter validation window) are deferred to a
    future bot once this one is proven in paper trading.

Architecture: REST-polling via get_history(), not tick/WebSocket bar-
building. The strategy's core mechanic (volume-weighted POC) needs 1-min
OHLCV+volume bars in exactly the shape the backtest used; polling
get_history() keeps live and backtest bar semantics identical, whereas
building bars from ticks risks a live/backtest mismatch on the one mechanic
this whole study validates. Each poll tick: (a) fetch any newly-completed
1-min bars per option symbol and replay the ported state machine over them
in order; (b) a batched LTP check for stop-loss, independent of the bar
cadence so SL response isn't gated on a full minute.

Paper-trading watch items (results_summary.md Risks and Constraints,
carried over unchanged since this bot re-implements the exact same locked
config): Rs30 threshold's walk-forward near-miss (73.2% vs 75% bar),
Thursday weakness (Sharpe 1.152, worst per-bucket drawdown of the whole
regime analysis).

Shared utilities:
  live_trading.api_utils               -- get_history, is_nse_fo_trading_day_via_fyers
  live_trading.shared.atm_resolver     -- resolve_atm_option, get_atm_strike
  live_trading.shared.order_fill       -- fetch_fill_price
  live_trading.shared.telegram_notifier -- send_async
  live_trading.shared.trade_logger     -- log_trade_to_db, _cost_round_trip
  live_trading.shared.decision_logger  -- DecisionLogger
  openalgo.api                         -- placeorder (always real, no PAPER_MODE)

Read first, every time, no exceptions:
  - docs/trading/bot-pipeline.md          -- Stage 11-13 doctrine, technical rules
  - docs/trading/deployment-checklist.md  -- final gate before declaring done
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import UTC, date, datetime
from datetime import time as dt_time
from pathlib import Path

from dotenv import load_dotenv

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent  # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

import requests  # noqa: E402

from live_trading.api_utils import (  # noqa: E402
    HOST,
    get_history,
    is_market_holiday,
    is_nse_fo_trading_day_via_fyers,
)
from live_trading.shared.atm_resolver import get_atm_strike, resolve_atm_option  # noqa: E402
from live_trading.shared.decision_logger import DecisionLogger  # noqa: E402
from live_trading.shared.order_fill import fetch_fill_price  # noqa: E402
from live_trading.shared.telegram_notifier import send_async  # noqa: E402
from live_trading.shared.trade_logger import _cost_round_trip, log_trade_to_db  # noqa: E402
from openalgo import api  # noqa: E402

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "atm_poc_reversion_bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

API_KEY = os.getenv("OPENALGO_API_KEY")

# ──────────────────────────────────────────────────────────────────────────────
# No PAPER_MODE / DRY_RUN / SIMULATE flag here, deliberately. Orders always go
# through _order() -> client.placeorder() -> real OpenAlgo REST order.
# OpenAlgo's own Sandbox/Analyze Mode (a UI toggle, not a bot setting) is the
# only thing that distinguishes paper from live. See CLAUDE.md "How paper
# trading actually works" and the flat_blue_line_monthly_bot 2026-06-08
# incident referenced in _template_bot.py.
# ──────────────────────────────────────────────────────────────────────────────

STRATEGY_NAME = "ATM_POC_REVERSION"
IDX_SYMBOL = "NIFTY"
IDX_EXCHANGE = "NSE_INDEX"
OPT_EXCHANGE = "NFO"
MIN_DTE = 2  # live-safety expiry floor, see header docstring

# Locked strategy config (DECISIONS.md #11, #12, #13) -- do not change without
# re-running the study's validation pipeline.
THRESHOLD = 30.0  # no-add target distance (Rs)
A_THRESH = 15.0  # add-trigger: distance to developing POC (Rs)
B_THRESH = 15.0  # add-trigger: min time gap (minutes) -- round-2 correction, was 3.0
C_THRESH = 5.0  # add-trigger: min price gap vs. prior extreme (Rs)
MAX_ADDS = 2

# Live-only stop-loss (NEW -- not validated by the study, see header docstring)
SL_DISTANCE = 2 * THRESHOLD  # Rs 60, anchored to the entry leg's own price

# Opening-range / session constants (stage0_discovery.py, verbatim)
OR_END_TIME = dt_time(9, 29)
SESSION_START = dt_time(9, 15)
MIN_OR_BARS = 10

# MIS/intraday EOD hard limit -- sandbox force-squares-off at 15:15 IST.
EOD_EXIT_TIME = dt_time(15, 14)

# No fresh entry on a bar opening after 14:30 (inclusive of 14:30 itself);
# adds to an already-entered track are not gated. Study v3 fix #2, as in
# sl_trades_confirmed_close_and_poc_gate_v4.py. Missing from the original
# build -- 2026-08-31 paper trade entered at 14:46.
ENTRY_CUTOFF_TIME = dt_time(14, 30)

# Startup gate: how long to keep re-asking Fyers when the OpenAlgo holiday
# calendar says it is a trading day but Fyers marketStatus says no session
# (it does that before ~08:45, see _await_trading_day).
FYERS_RECHECK_DEADLINE = dt_time(9, 5)
FYERS_RECHECK_SECS = 120

N_LOTS = 10  # explicit user decision, per leg
DEFAULT_LOT_SIZE = 65  # fallback only -- primary source is database.token_db

POLL_SECS = 20  # unified loop: bar fetch + SL check every tick (faster
# than the template's default 60s because this strategy
# carries a live SL, unlike most siblings)
ORDER_DELAY = 1.5  # seconds between sequential order calls

STATE_FILE = LOGS_DIR / "atm_poc_reversion_state.json"


# ══════════════════════════════════════════════════════════════════════════════
# QUOTES — batch via /api/v1/multiquotes (copied from _template_bot.py)
# ══════════════════════════════════════════════════════════════════════════════


def _multiquote(
    items: list[tuple[str, str]], retries: int = 3, delay: float = 3.0
) -> dict[str, float]:
    """
    Batch-fetch LTPs for multiple (symbol, exchange) pairs in ONE
    /api/v1/multiquotes round-trip, with retry/backoff baked in.
    Returns {symbol: ltp} for whatever resolved with ltp > 0; missing/zero/
    error entries are simply absent -- callers should .get(sym, fallback).
    """
    pending = list(dict.fromkeys(items))
    out: dict[str, float] = {}
    for attempt in range(1, retries + 1):
        if not pending:
            break
        payload = {
            "apikey": API_KEY,
            "symbols": [{"symbol": s, "exchange": e} for s, e in pending],
        }
        try:
            res = requests.post(f"{HOST}/api/v1/multiquotes", json=payload, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data.get("status") == "success":
                    for row in data.get("results", []):
                        sym = row.get("symbol")
                        qd = row.get("data") or {}
                        ltp = (
                            qd.get("ltp")
                            or qd.get("last_price")
                            or qd.get("close")
                            or qd.get("c")
                            or 0
                        )
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
        logger.warning(f"  multiquotes: no LTP for {[s for s, _ in pending]}")
    return out


# ══════════════════════════════════════════════════════════════════════════════
# ORDER PLACEMENT — always real, never gated
# ══════════════════════════════════════════════════════════════════════════════


def _order(client, symbol: str, action: str, qty: int, product: str = "MIS") -> dict:
    try:
        resp = client.placeorder(
            strategy=STRATEGY_NAME,
            symbol=symbol,
            action=action,
            exchange=OPT_EXCHANGE,
            price_type="MARKET",
            product=product,
            quantity=qty,
        )
        logger.info(f"  {action} {symbol} x{qty}: {resp}")
        return resp if isinstance(resp, dict) else {"status": "ok"}
    except Exception as e:
        logger.error(f"  placeorder failed ({action} {symbol}): {e}")
        return {"status": "error", "message": str(e)}


def _get_lot_size(symbol: str, exchange: str) -> int:
    try:
        from database.token_db import get_symbol_info

        info = get_symbol_info(symbol, exchange)
        if info and getattr(info, "lotsize", None):
            return int(info.lotsize)
    except Exception as e:
        logger.warning(f"[{symbol}] lot size DB lookup failed: {e}")
    logger.warning(f"[{symbol}] falling back to DEFAULT_LOT_SIZE={DEFAULT_LOT_SIZE}")
    return DEFAULT_LOT_SIZE


# ══════════════════════════════════════════════════════════════════════════════
# STATE PERSISTENCE — crash-safe restart, no re-entry on relaunch
# ══════════════════════════════════════════════════════════════════════════════


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
    return {}


def _save_state(state: dict) -> None:
    try:
        state["last_update"] = datetime.now().isoformat()
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        logger.error(f"Failed to save state: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# BAR FETCH — 1-min OHLCV via get_history, completed bars for today only
# ══════════════════════════════════════════════════════════════════════════════


def minutes_between(t1: dt_time, t2: dt_time) -> float:
    """Verbatim port of stage0_discovery.py's minutes_between()."""
    return (datetime.combine(date.min, t2) - datetime.combine(date.min, t1)).total_seconds() / 60


def _epoch_to_ist(ts) -> datetime:
    from datetime import timedelta, timezone

    ts = float(ts)
    if ts > 1e12:  # milliseconds
        ts /= 1000.0
    return datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None) + timedelta(hours=5, minutes=30)


def _fetch_today_bars(symbol: str, exchange: str) -> list[dict]:
    """Completed 1-min OHLCV bars for today, oldest first. The currently-
    forming (incomplete) bar is dropped -- only bars strictly before the
    current minute are guaranteed complete."""
    rows = get_history(API_KEY, symbol, exchange, "1m", 1)
    today = datetime.now().date()
    out = []
    for r in rows or []:
        ts = r.get("timestamp")
        if ts is None:
            continue
        try:
            dt_ = _epoch_to_ist(ts)
        except (TypeError, ValueError):
            continue
        if dt_.date() != today:
            continue
        try:
            out.append(
                {
                    "dt": dt_.replace(second=0, microsecond=0),
                    "time": dt_.time(),
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "volume": float(r.get("volume") or 0),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda b: b["dt"])
    cur_minute = datetime.now().replace(second=0, microsecond=0)
    return [b for b in out if b["dt"] < cur_minute]


def _time_to_str(t: dt_time) -> str:
    return t.strftime("%H:%M:%S")


def _str_to_time(s: str) -> dt_time:
    return dt_time.fromisoformat(s)


# ══════════════════════════════════════════════════════════════════════════════
# STATE MACHINE — faithful re-port of add_sweep_is.py's simulate_day_track()
# ══════════════════════════════════════════════════════════════════════════════


class SymbolTrack:
    """Per-option-symbol (CE or PE) opening-range + expanding-POC state
    machine. Holds two independent directional tracks ("short"/"long") in
    self.state, exactly mirroring simulate_day_track()'s per-day state dict.
    """

    def __init__(
        self,
        bot: AtmPocReversionBot,
        opt_type: str,
        symbol: str,
        expiry: str,
        strike: float,
        lot_size: int,
    ):
        self.bot = bot
        self.opt_type = opt_type
        self.symbol = symbol
        self.expiry = expiry
        self.strike = strike
        self.lot_size = lot_size

        self.or_bars: list[dict] = []
        self.or_locked = False
        self.skip_today = False
        self.or_high: float | None = None
        self.or_low: float | None = None

        self.vol_by_price: dict[float, float] = {}
        self.poc_price: float | None = None
        self.poc_vol: float = -1.0
        self.poc_initial: float | None = None

        self.state: dict[str, dict] = {}  # "short"/"long" -> dir-state dict
        self.last_processed_dt: datetime | None = None

    # ── POC ──────────────────────────────────────────────────────────────
    def _update_poc(self, close_px: float, volume: float) -> None:
        key = round(float(close_px), 2)
        v = self.vol_by_price.get(key, 0.0) + float(volume)
        self.vol_by_price[key] = v
        if v > self.poc_vol:
            self.poc_vol = v
            self.poc_price = key

    def _try_lock_or(self) -> None:
        if len(self.or_bars) < MIN_OR_BARS:
            logger.warning(
                f"[{self.symbol}] OR window closed with only "
                f"{len(self.or_bars)} bars (< {MIN_OR_BARS}) -- skipping today"
            )
            self.skip_today = True
            self.or_locked = True
            return

        self.or_high = max(b["high"] for b in self.or_bars)
        self.or_low = min(b["low"] for b in self.or_bars)
        self.poc_initial = self.poc_price
        last_or_time = self.or_bars[-1]["time"]

        dist_high0 = self.or_high - self.poc_initial
        dist_low0 = self.poc_initial - self.or_low
        for direction, dist0, extreme0 in (
            ("short", dist_high0, self.or_high),
            ("long", dist_low0, self.or_low),
        ):
            if dist0 < THRESHOLD:
                continue
            self.state[direction] = {
                "entered": False,
                "closed": False,
                "running_extreme": extreme0,
                "last_extreme_time": _time_to_str(last_or_time),
                "last_extreme_price": extreme0,
                "legs": [],
                "n_adds": 0,
                "target": self.poc_initial,
                "dynamic": False,
                "sl_price": None,
            }

        self.or_locked = True
        self.or_bars = []  # no longer needed once locked
        logger.info(
            f"[{self.symbol}] OR locked: high={self.or_high:.2f} "
            f"low={self.or_low:.2f} poc_initial={self.poc_initial:.2f} "
            f"tracks={list(self.state.keys())}"
        )

    # ── per-bar processing ──────────────────────────────────────────────
    def process_bar(self, bar: dict) -> None:
        if self.skip_today:
            self.last_processed_dt = bar["dt"]
            return

        if not self.or_locked:
            if bar["time"] <= OR_END_TIME:
                self.or_bars.append(bar)
                self._update_poc(bar["close"], bar["volume"])
                self.last_processed_dt = bar["dt"]
                return
            self._try_lock_or()
            if self.skip_today:
                self.last_processed_dt = bar["dt"]
                return
            # fall through -- this bar is the first post-OR bar

        if bar["time"] <= OR_END_TIME:
            self.last_processed_dt = bar["dt"]
            return

        self._update_poc(bar["close"], bar["volume"])
        dev_poc = self.poc_price

        for direction in ("short", "long"):
            st = self.state.get(direction)
            if not st or st["closed"]:
                continue
            # Study semantics: past the cutoff an un-entered track neither
            # enters nor advances its running_extreme.
            if not st["entered"] and bar["time"] > ENTRY_CUTOFF_TIME:
                continue

            # Confirmed-close mechanic (round-2 correction): a breakout only
            # fires if THIS bar's close also clears the running extreme, not
            # just its wick -- you can't transact at an intrabar high/low
            # tick in live trading, only after the bar has closed. Fill price
            # is that bar's close. If the wick broke out but the close
            # reverted back under the level, nothing fires this bar and
            # running_extreme is unchanged -- the next bar must clear the
            # SAME level to be considered.
            if direction == "short":
                wick_breakout = bar["high"] > st["running_extreme"]
                is_new_extreme = wick_breakout and bar["close"] > st["running_extreme"]
                extreme_price = bar["close"] if is_new_extreme else None
            else:
                wick_breakout = bar["low"] < st["running_extreme"]
                is_new_extreme = wick_breakout and bar["close"] < st["running_extreme"]
                extreme_price = bar["close"] if is_new_extreme else None

            if is_new_extreme:
                dist_to_dev_poc = (
                    (extreme_price - dev_poc) if direction == "short" else (dev_poc - extreme_price)
                )
                time_gap = minutes_between(_str_to_time(st["last_extreme_time"]), bar["time"])
                price_gap = (
                    (extreme_price - st["last_extreme_price"])
                    if direction == "short"
                    else (st["last_extreme_price"] - extreme_price)
                )

                if not st["entered"]:
                    st["entered"] = True
                    self.bot._enter_leg(self, direction, "entry", extreme_price, bar["dt"])
                elif (
                    st["n_adds"] < MAX_ADDS
                    and dist_to_dev_poc >= A_THRESH
                    and time_gap >= B_THRESH
                    and price_gap >= C_THRESH
                ):
                    st["n_adds"] += 1
                    self.bot._enter_leg(
                        self, direction, f"add{st['n_adds']}", extreme_price, bar["dt"]
                    )
                    st["dynamic"] = True

                st["running_extreme"] = extreme_price
                st["last_extreme_time"] = _time_to_str(bar["time"])
                st["last_extreme_price"] = extreme_price

            if not st["entered"]:
                continue

            if st["dynamic"]:
                st["target"] = dev_poc

            entry_time = datetime.fromisoformat(st["legs"][0]["dt"]).time()
            if bar["time"] > entry_time:
                touched = (
                    (bar["low"] <= st["target"])
                    if direction == "short"
                    else (bar["high"] >= st["target"])
                )
                if touched:
                    self.bot._exit_position(self, direction, st["target"], bar["dt"], "TARGET")

        self.last_processed_dt = bar["dt"]

    # ── persistence ─────────────────────────────────────────────────────
    def to_dict(self) -> dict:
        return {
            "opt_type": self.opt_type,
            "symbol": self.symbol,
            "expiry": self.expiry,
            "strike": self.strike,
            "lot_size": self.lot_size,
            "or_locked": self.or_locked,
            "skip_today": self.skip_today,
            "or_high": self.or_high,
            "or_low": self.or_low,
            "poc_initial": self.poc_initial,
            "poc_price": self.poc_price,
            "poc_vol": self.poc_vol,
            "vol_by_price": {str(k): v for k, v in self.vol_by_price.items()},
            "or_bars": []
            if self.or_locked
            else [
                {
                    "dt": b["dt"].isoformat(),
                    "open": b["open"],
                    "high": b["high"],
                    "low": b["low"],
                    "close": b["close"],
                    "volume": b["volume"],
                }
                for b in self.or_bars
            ],
            "state": self.state,
            "last_processed_dt": self.last_processed_dt.isoformat()
            if self.last_processed_dt
            else None,
        }

    @classmethod
    def from_dict(cls, bot: AtmPocReversionBot, d: dict) -> SymbolTrack:
        t = cls(bot, d["opt_type"], d["symbol"], d["expiry"], d["strike"], d["lot_size"])
        t.or_locked = d["or_locked"]
        t.skip_today = d["skip_today"]
        t.or_high = d["or_high"]
        t.or_low = d["or_low"]
        t.poc_initial = d["poc_initial"]
        t.poc_price = d["poc_price"]
        t.poc_vol = d["poc_vol"]
        t.vol_by_price = {float(k): v for k, v in d.get("vol_by_price", {}).items()}
        t.state = d.get("state", {})
        for bd in d.get("or_bars", []):
            dtv = datetime.fromisoformat(bd["dt"])
            t.or_bars.append(
                {
                    "dt": dtv,
                    "time": dtv.time(),
                    "open": bd["open"],
                    "high": bd["high"],
                    "low": bd["low"],
                    "close": bd["close"],
                    "volume": bd["volume"],
                }
            )
        lp = d.get("last_processed_dt")
        t.last_processed_dt = datetime.fromisoformat(lp) if lp else None
        return t


# ══════════════════════════════════════════════════════════════════════════════
# BOT
# ══════════════════════════════════════════════════════════════════════════════


class AtmPocReversionBot:
    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.today = datetime.now().date().isoformat()
        self.tracks: dict[str, SymbolTrack] = {}
        self._session_started = False
        self._dlog = DecisionLogger(
            LOGS_DIR / "atm_poc_reversion_decisions.jsonl", heartbeat_secs=300, bot_logger=logger
        )
        self._restore_state()

    # ── state ────────────────────────────────────────────────────────────
    def _restore_state(self) -> None:
        saved = _load_state()
        if saved.get("date") != self.today:
            return
        self._session_started = saved.get("session_started", False)
        for k, d in saved.get("tracks", {}).items():
            self.tracks[k] = SymbolTrack.from_dict(self, d)
        if self.tracks:
            logger.info(f"Restored state for {self.today}: {list(self.tracks.keys())}")

    def _serialize_state(self) -> dict:
        return {
            "date": self.today,
            "session_started": self._session_started,
            "tracks": {k: v.to_dict() for k, v in self.tracks.items()},
            "last_update": datetime.now().isoformat(),
        }

    # ── session resolution ──────────────────────────────────────────────
    def _resolve_session(self) -> bool:
        bars = _fetch_today_bars(IDX_SYMBOL, IDX_EXCHANGE)
        opening_bar = next((b for b in bars if b["time"] == SESSION_START), None)
        if opening_bar is None:
            logger.info("NIFTY 09:15 bar not yet available -- waiting")
            return False

        spot_open = opening_bar["open"]
        strike = get_atm_strike(spot_open, IDX_SYMBOL)
        logger.info(f"Session resolving: NIFTY 09:15 open={spot_open:.2f} -> ATM strike {strike}")

        resolved: dict[str, SymbolTrack] = {}
        for opt_type in ("CE", "PE"):
            info = resolve_atm_option(
                spot_open, opt_type=opt_type, api_key=API_KEY, min_dte=MIN_DTE, index=IDX_SYMBOL
            )
            if not info:
                logger.error(f"Failed to resolve {opt_type} symbol -- retrying next tick")
                return False
            lot_size = _get_lot_size(info["symbol"], info["exchange"])
            resolved[opt_type] = SymbolTrack(
                self, opt_type, info["symbol"], info["expiry"], info["strike"], lot_size
            )
            logger.info(
                f"{opt_type}: {info['symbol']} (expiry={info['expiry']}, lot_size={lot_size})"
            )

        self.tracks = resolved
        self._session_started = True
        _save_state(self._serialize_state())
        send_async(
            f"{STRATEGY_NAME}: session started. Strike={strike} "
            f"CE={self.tracks['CE'].symbol} PE={self.tracks['PE'].symbol}"
        )
        return True

    # ── bar processing ──────────────────────────────────────────────────
    def _fetch_and_process(self) -> None:
        changed = False
        for track in self.tracks.values():
            bars = _fetch_today_bars(track.symbol, OPT_EXCHANGE)
            if track.last_processed_dt is not None:
                bars = [b for b in bars if b["dt"] > track.last_processed_dt]
            for bar in bars:
                track.process_bar(bar)
                changed = True
        if changed:
            _save_state(self._serialize_state())

    # ── order execution ─────────────────────────────────────────────────
    def _enter_leg(
        self, track: SymbolTrack, direction: str, leg_name: str, price: float, bar_dt: datetime
    ) -> None:
        st = track.state[direction]
        qty = N_LOTS * track.lot_size
        action = "SELL" if direction == "short" else "BUY"
        resp = _order(self.client, track.symbol, action, qty)
        order_id = resp.get("orderid") if isinstance(resp, dict) else None

        st["legs"].append(
            {
                "leg": leg_name,
                "dt": bar_dt.isoformat(),
                "price": price,
                "action": action,
                "qty": qty,
                "order_id": order_id,
            }
        )
        if leg_name == "entry":
            st["sl_price"] = (
                round(price + SL_DISTANCE, 2)
                if direction == "short"
                else round(price - SL_DISTANCE, 2)
            )

        logger.info(
            f"[{track.symbol}] {direction.upper()} {leg_name} @ {price:.2f} "
            f"qty={qty} order={order_id} sl={st['sl_price']}"
        )
        self._dlog.log_bar(
            {
                "event": "entry" if leg_name == "entry" else "add",
                "symbol": track.symbol,
                "opt_type": track.opt_type,
                "direction": direction,
                "leg": leg_name,
                "price": price,
                "qty": qty,
                "n_adds": st["n_adds"],
                "sl_price": st["sl_price"],
                "target": st["target"],
                "order_id": order_id,
            }
        )
        send_async(
            f"{STRATEGY_NAME}: {direction.upper()} {leg_name} {track.symbol} "
            f"@ {price:.2f} qty={qty} SL={st['sl_price']}"
        )
        _save_state(self._serialize_state())
        time.sleep(ORDER_DELAY)

    def _exit_position(
        self,
        track: SymbolTrack,
        direction: str,
        exit_trigger_price: float,
        exit_dt: datetime,
        reason: str,
    ) -> None:
        st = track.state[direction]
        if st["closed"]:
            return
        legs = st["legs"]
        total_qty = sum(leg["qty"] for leg in legs)
        action = "BUY" if direction == "short" else "SELL"
        resp = _order(self.client, track.symbol, action, total_qty)
        order_id = resp.get("orderid") if isinstance(resp, dict) else None
        fill_price = (
            fetch_fill_price(order_id, STRATEGY_NAME, api_key=API_KEY) if order_id else None
        )
        actual_exit = fill_price if fill_price is not None else exit_trigger_price
        lp_direction = "sell" if direction == "short" else "buy"

        logger.info(
            f"[{track.symbol}] EXIT {direction.upper()} reason={reason} "
            f"trigger={exit_trigger_price:.2f} fill={actual_exit:.2f} qty={total_qty}"
        )

        total_net = 0.0
        for leg in legs:
            entry_price = leg["price"]
            qty = leg["qty"]
            gross_pnl = (
                (entry_price - actual_exit) if direction == "short" else (actual_exit - entry_price)
            ) * qty
            # net_pnl computed explicitly here (not left to trade_logger's
            # default resolution) because positionbook_pnl.fetch_closed_pnl()
            # returns ONE combined P&L per symbol once flat -- calling it once
            # per leg (entry+adds share the same symbol) would attribute the
            # full combined P&L to every leg. _cost_round_trip() is per-row,
            # so it stays correctly leg-scoped.
            cost = _cost_round_trip(
                entry_price, actual_exit, track.lot_size, N_LOTS, IDX_SYMBOL, lp_direction
            )
            net_pnl = round(gross_pnl - cost, 2)
            total_net += net_pnl
            try:
                log_trade_to_db(
                    bot_name="atm_poc_reversion_bot",
                    instrument=IDX_SYMBOL,
                    option_symbol=track.symbol,
                    option_type=track.opt_type,
                    entry_time=datetime.fromisoformat(leg["dt"]),
                    exit_time=exit_dt,
                    entry_premium=entry_price,
                    exit_premium=actual_exit,
                    exit_reason=reason,
                    quantity=qty,
                    lots=N_LOTS,
                    lot_size=track.lot_size,
                    gross_pnl=round(gross_pnl, 2),
                    net_pnl=net_pnl,
                    order_id=str(order_id) if order_id else leg.get("order_id"),
                    direction=lp_direction,
                )
            except Exception as e:
                logger.error(f"[{track.symbol}] log_trade_to_db failed for leg {leg['leg']}: {e}")

        st["closed"] = True
        st["exit_price"] = actual_exit
        st["exit_reason"] = reason
        self._dlog.log_bar(
            {
                "event": "exit",
                "symbol": track.symbol,
                "opt_type": track.opt_type,
                "direction": direction,
                "reason": reason,
                "exit_price": actual_exit,
                "n_legs": len(legs),
                "total_net_pnl": round(total_net, 2),
                "order_id": order_id,
            }
        )
        send_async(
            f"{STRATEGY_NAME}: EXIT {direction.upper()} {track.symbol} "
            f"reason={reason} @ {actual_exit:.2f} legs={len(legs)} "
            f"net=Rs{total_net:,.0f}"
        )
        _save_state(self._serialize_state())
        time.sleep(ORDER_DELAY)

    # ── stop-loss (fast poll, independent of bar cadence) ───────────────
    def _check_stop_losses(self) -> None:
        items = [
            (t.symbol, OPT_EXCHANGE)
            for t in self.tracks.values()
            if any(
                st["entered"] and not st["closed"] and st.get("sl_price") is not None
                for st in t.state.values()
            )
        ]
        if not items:
            return
        quotes = _multiquote(items)
        for track in self.tracks.values():
            ltp = quotes.get(track.symbol)
            if ltp is None:
                continue
            for direction, st in list(track.state.items()):
                if not st["entered"] or st["closed"] or st.get("sl_price") is None:
                    continue
                breached = (
                    (ltp >= st["sl_price"]) if direction == "short" else (ltp <= st["sl_price"])
                )
                if breached:
                    logger.warning(
                        f"[{track.symbol}] SL HIT {direction} ltp={ltp:.2f} sl={st['sl_price']}"
                    )
                    self._exit_position(track, direction, ltp, datetime.now(), "SL")

    # ── EOD square-off ───────────────────────────────────────────────────
    def _eod_square_off(self) -> None:
        items = [
            (t.symbol, OPT_EXCHANGE)
            for t in self.tracks.values()
            if any(st["entered"] and not st["closed"] for st in t.state.values())
        ]
        quotes = _multiquote(items) if items else {}
        for track in self.tracks.values():
            for direction, st in list(track.state.items()):
                if st["entered"] and not st["closed"]:
                    ltp = quotes.get(track.symbol)
                    exit_price = ltp if ltp is not None else st["legs"][-1]["price"]
                    self._exit_position(track, direction, exit_price, datetime.now(), "EOD")

    # ── main loop ────────────────────────────────────────────────────────
    def _await_trading_day(self) -> tuple[bool, str]:
        """OpenAlgo holiday calendar first, then Fyers marketStatus.

        Fyers returns CLOSE with an empty session before ~08:45, which made the
        bot exit for the day when the launcher started at 08:13-08:31 on
        2026-09-23..25. When the calendar says trading day, a negative Fyers
        answer is re-asked until FYERS_RECHECK_DEADLINE before giving up.
        """
        if date.fromisoformat(self.today).weekday() >= 5:
            return False, "Weekend"
        if is_market_holiday(API_KEY, self.today, exchange=OPT_EXCHANGE):
            return False, f"OpenAlgo holiday calendar: {OPT_EXCHANGE} holiday"
        while True:
            ok, reason = is_nse_fo_trading_day_via_fyers(API_KEY)
            if ok:
                return True, reason
            if datetime.now().time() >= FYERS_RECHECK_DEADLINE:
                return False, reason
            logger.warning(
                f"{STRATEGY_NAME}: Fyers says no session ({reason}) but the holiday "
                f"calendar says trading day -- rechecking in {FYERS_RECHECK_SECS}s"
            )
            time.sleep(FYERS_RECHECK_SECS)

    def run(self) -> None:
        is_trading_day, reason = self._await_trading_day()
        if not is_trading_day:
            logger.info(f"{STRATEGY_NAME}: not a trading day ({reason}) -- exiting")
            return

        logger.info(f"{STRATEGY_NAME} starting for {self.today}")
        while True:
            now = datetime.now()

            if now.time() >= EOD_EXIT_TIME:
                break

            if now.time() < SESSION_START:
                wait = (datetime.combine(now.date(), SESSION_START) - now).total_seconds()
                time.sleep(max(1.0, min(POLL_SECS, wait)))
                continue

            if not self._session_started:
                if not self._resolve_session():
                    time.sleep(POLL_SECS)
                    continue

            try:
                self._fetch_and_process()
                self._check_stop_losses()
            except Exception:
                logger.exception("loop error")

            time.sleep(POLL_SECS)

        logger.info("EOD reached -- squaring off any open positions")
        try:
            self._eod_square_off()
        except Exception:
            logger.exception("EOD square-off failed")
        _save_state(self._serialize_state())
        logger.info(f"{STRATEGY_NAME} finished for {self.today}")


if __name__ == "__main__":
    bot = AtmPocReversionBot()
    bot.run()
