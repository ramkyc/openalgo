#!/usr/bin/env python3
"""
VP Swing Screener — hourly scan engine
live_trading/vp_swing_screener/vp_swing_screener.py

Research-validated (vp_swing_reversion_study, completed 2026-08-09):
  options_data/research/vp_swing_reversion_study/results_summary.md, DECISIONS.md
  Full IS+OOS (2023-11-30→2026-08-07): 3,907 trades, WR ~69%,
  Sharpe 4.0 (IS) / 5.2 (OOS). Stages 2-9, 12, 13 all run.
  Stage 12 overnight-gap tail risk (worst 1%ile -5.44%) reviewed and accepted
  2026-08-09 as a quantified, documented cost -- NOT remediated in code.

Strategy (60-min bars, long-only, multi-day swing hold, no forced EOD close):
  Rolling PROFILE_DAYS=10-trading-day volume profile (N = 6 bars/day x 10 =
  60 bars). A touch of the rolling window's lower low -> long candidate.
  Exit: target = rolling-window POC (recomputed every bar), or hard 3%
  stop-loss (STOP_LOSS_PCT). No pyramiding -- one position per symbol.
  Universe: 53 NIFTY50 stocks (swing_core.NIFTY50_STOCKS, verified against
  this instance's symtoken master 2026-08-09 -- TMCV/TMPV/LTM all resolve).

⚠️  THIS IS A SCREENER, NOT AN ORDER-PLACING BOT -- it never calls
placeorder() and never will. It only detects signals and writes them to
STATE_FILE for live_trading/streamlit_dashboard.py's
render_vp_swing_screener_panel() to display. A detected candidate becomes
a tracked "open position" only when you manually execute it through your
own broker terminal and click "Confirm" on the dashboard panel (which
writes directly into open_positions in the state file below) -- there is
no automatic promotion, and no positionbook auto-detection, because this
screener cannot reliably distinguish a position you took off one of its
candidates from an unrelated holding.

Signal logic below is a line-for-line port of
options_data/research/vp_swing_reversion_study/swing_core.py's
resample_tf() and scan_symbol_trades() -- see that file for the
authoritative research version. Each scan only evaluates the single most
recently completed 60-min bar per symbol (not a full historical replay),
so a scan that is skipped or delayed (process down, OpenAlgo outage) can
miss a signal that would have fired on an intermediate bar -- this is a
known, accepted limitation of a periodic REST-poll screener rather than a
continuous tick-driven bot.
"""
from __future__ import annotations

import atexit
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo (fyers_crk)
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils import get_history, get_multiquotes, is_nse_fo_trading_day_via_fyers  # noqa: E402

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "vp_swing_screener.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS — mirrored 1:1 from swing_core.py (champion config: SL=3%, PD=10)
# ══════════════════════════════════════════════════════════════════════════════

API_KEY = os.getenv("OPENALGO_API_KEY")

STRATEGY_NAME = "VP_SWING_SCREENER"
EXCHANGE      = "NSE"

TF_MIN             = 60
SESSION_MINUTES    = 375
BIN_PCT            = 0.00025
MIN_EXTENSION_PCT  = 0.0010
TOUCH_TOL_PCT      = 0.0005
CAPITAL_PER_TRADE  = 100_000

STOP_LOSS_PCT  = 0.03   # champion (Stage 3 sweep winner)
PROFILE_DAYS   = 10     # champion (Stage 3 sweep winner)
BARS_PER_DAY   = SESSION_MINUTES // TF_MIN     # 6
N_BARS         = BARS_PER_DAY * PROFILE_DAYS   # 60

BOOK_VALUE = 4_600_000   # Rs.46L, same capital base used throughout the study

# How much 1-min history to fetch per symbol per scan. N_BARS=60 needs 10
# trading days of 60-min bars; 18 calendar days of 1-min data comfortably
# covers that plus weekends/holidays plus the in-progress current day.
HISTORY_LOOKBACK_DAYS = 18
REQUEST_GAP_SEC = 1.0   # be gentle on the single-eventlet-worker REST API

# 60-min bar-close boundaries under resample_tf's origin=09:15/label=left
# convention: the bar labeled 09:15 covers [09:15,10:15) and is complete
# (and scannable) starting at 10:15. 15:15 is the final, partial (15-min)
# bar of the session -- included for exact parity with the backtest, which
# does not special-case it (see resample_tf()/DECISIONS.md).
SCAN_TIMES = ["10:15", "11:15", "12:15", "13:15", "14:15", "15:15"]

# Fast-path stop/target watchdog for confirmed open positions only (real
# capital at risk, unlike unconfirmed candidates) -- runs independently of
# the hourly SCAN_TIMES full scan so a stop/target breach doesn't wait up to
# 55 minutes to be caught. See check_open_positions() for why this doesn't
# apply to entry-signal detection.
POSITION_CHECK_INTERVAL_SEC = 300   # 5 min
POSITION_CHECK_START, POSITION_CHECK_END = "09:15", "15:40"   # NSE CAS 2026-08-03 moved close 15:30->15:40

STATE_FILE = LOGS_DIR / "vp_swing_screener_state.json"
PID_FILE   = LOGS_DIR / "vp_swing_screener.pid"

NIFTY50_STOCKS = [
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL", "BPCL",
    "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY", "EICHERMOT",
    "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO",
    "HINDUNILVR", "ICICIBANK", "INDUSINDBK", "INFY", "ITC", "JSWSTEEL",
    "KOTAKBANK", "LT", "LTM", "M&M", "MARUTI", "NESTLEIND", "NTPC", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SHRIRAMFIN", "SUNPHARMA",
    "TATACONSUM", "TATASTEEL", "TCS", "TECHM", "TITAN", "TMCV", "TMPV",
    "TRENT", "ULTRACEMCO", "WIPRO",
]


# ── PID lockfile ─────────────────────────────────────────────────────────────

def _acquire_pid_lock() -> None:
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            os.kill(old_pid, 0)
            logger.error(f"Another instance already running (PID {old_pid}). "
                         f"Delete {PID_FILE} if stale.")
            sys.exit(1)
        except ProcessLookupError:
            logger.warning(f"Stale PID file (PID {old_pid}) -- removing.")
            PID_FILE.unlink(missing_ok=True)
        except (PermissionError, ValueError):
            logger.error("Could not verify existing PID. Aborting.")
            sys.exit(1)
    PID_FILE.write_text(str(os.getpid()))
    atexit.register(_release_pid_lock)


def _release_pid_lock() -> None:
    try:
        if PID_FILE.exists() and int(PID_FILE.read_text().strip()) == os.getpid():
            PID_FILE.unlink()
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# STATE PERSISTENCE
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
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        logger.error(f"Failed to save state: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL LOGIC — line-for-line port of swing_core.py's resample_tf() and the
# per-bar body of scan_symbol_trades(), applied to only the latest bar.
# ══════════════════════════════════════════════════════════════════════════════

def _history_to_frame(raw: list) -> pd.DataFrame:
    """Match nifty_ema_spread_bot._load_history()'s raw->DataFrame convention."""
    if not raw:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    hist = pd.DataFrame(raw)
    if "timestamp" in hist.columns:
        idx = (pd.to_datetime(hist["timestamp"], unit="s", utc=True)
               .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None))
    elif "date" in hist.columns:
        idx = pd.to_datetime(hist["date"])
    else:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    idx = idx.rename("ts")   # force the reset_index() column name below to "ts"
    hist = hist.set_index(idx).sort_index().between_time("09:15", "15:39")
    hist = hist[["open", "high", "low", "close", "volume"]].astype(float)
    return hist.reset_index()


def resample_tf(df: pd.DataFrame, tf_min: int) -> pd.DataFrame:
    """Verbatim port of swing_core.resample_tf() -- per-day grouping with
    origin=day+09:15, label=left, closed=left, to avoid a bar spanning the
    overnight gap and to match the backtest's session-anchored bin edges
    exactly (a plain df.resample("60min") would anchor bins to midnight,
    NOT 09:15, and silently produce different bar boundaries)."""
    if df.empty:
        return df
    df = df.set_index("ts")
    out = []
    for d, day_df in df.groupby(df.index.date):
        day_start = pd.Timestamp(d) + pd.Timedelta(hours=9, minutes=15)
        r = day_df.resample(
            f"{tf_min}min", origin=day_start, label="left", closed="left"
        ).agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        r = r.dropna(subset=["open"])
        out.append(r)
    if not out:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    res = pd.concat(out).reset_index().rename(columns={"index": "ts"})
    return res


def _window_stats(window_close: np.ndarray, window_vol: np.ndarray,
                   window_low: np.ndarray) -> tuple[float, float]:
    """Verbatim port of scan_symbol_trades()'s per-bar roll_low/poc_price
    computation (lines 119-126 of swing_core.py)."""
    roll_low = window_low.min()
    bin_size = max(window_close[-1] * BIN_PCT, 0.01)
    bins = np.floor(window_close / bin_size)
    unique_bins, inv = np.unique(bins, return_inverse=True)
    bin_vols = np.zeros(len(unique_bins))
    np.add.at(bin_vols, inv, window_vol)
    best_idx = np.argmax(bin_vols)
    poc_price = (unique_bins[best_idx] + 0.5) * bin_size
    return float(roll_low), float(poc_price)


def _scan_symbol(symbol: str, pos: dict | None) -> tuple[dict | None, dict | None, str | None]:
    """Evaluate the latest completed 60-min bar for `symbol` against
    scan_symbol_trades()'s exact per-bar rules.

    Returns (new_candidate, updated_position, exit_reason):
      - new_candidate: dict for state["candidates"], or None
      - updated_position: dict for state["open_positions"] (pos, possibly with
        a refreshed target_poc), or None if there's no position / it just closed
      - exit_reason: "stop" | "target" | None, for logging only
    """
    raw = get_history(API_KEY, symbol, EXCHANGE, "1m", duration_days=HISTORY_LOOKBACK_DAYS)
    df = _history_to_frame(raw)
    if df.empty:
        logger.warning(f"  {symbol}: no history returned, skipping")
        return None, pos, None

    res = resample_tf(df, TF_MIN)
    n = len(res)
    if n <= N_BARS + 1:
        logger.warning(f"  {symbol}: only {n} resampled 60-min bars (need > {N_BARS + 1}), skipping")
        return None, pos, None

    lows    = res["low"].values
    highs   = res["high"].values
    closes  = res["close"].values
    volumes = res["volume"].values

    i = n - 1   # latest completed bar
    roll_low, poc_price = _window_stats(
        closes[i - N_BARS:i], volumes[i - N_BARS:i], lows[i - N_BARS:i]
    )
    bar_low, bar_high = float(lows[i]), float(highs[i])

    if pos:
        stop_price = float(pos["stop"])
        if bar_low <= stop_price:
            logger.info(f"  {symbol}: STOP hit, bar_low={bar_low:.2f} <= stop={stop_price:.2f}")
            return None, None, "stop"

        tol = poc_price * TOUCH_TOL_PCT
        if (bar_low - tol) <= poc_price <= (bar_high + tol):
            logger.info(f"  {symbol}: TARGET hit, poc={poc_price:.2f} within bar [{bar_low:.2f},{bar_high:.2f}]")
            return None, None, "target"

        updated = dict(pos)
        updated["target_poc"] = round(poc_price, 2)
        return None, updated, None

    if bar_low < roll_low * (1 - MIN_EXTENSION_PCT):
        entry_price = bar_low
        stop_price = entry_price * (1 - STOP_LOSS_PCT)
        candidate = {
            "symbol": symbol,
            "touch_price": round(entry_price, 2),
            "poc": round(poc_price, 2),
            "stop": round(stop_price, 2),
            "detected_at": datetime.now().isoformat(timespec="seconds"),
        }
        logger.info(f"  {symbol}: CANDIDATE touch={entry_price:.2f} poc={poc_price:.2f} stop={stop_price:.2f}")
        return candidate, None, None

    return None, None, None


# ══════════════════════════════════════════════════════════════════════════════
# SCAN CYCLE
# ══════════════════════════════════════════════════════════════════════════════

def run_scan_cycle() -> None:
    state = _load_state()
    existing_positions = {p["symbol"]: p for p in state.get("open_positions", []) if p.get("symbol")}

    new_positions: list[dict] = []
    new_candidates: list[dict] = []

    for symbol in NIFTY50_STOCKS:
        pos = existing_positions.get(symbol)
        try:
            candidate, updated_pos, exit_reason = _scan_symbol(symbol, pos)
        except Exception as e:
            logger.exception(f"  {symbol}: scan failed: {e}")
            candidate, updated_pos, exit_reason = None, pos, None

        if candidate:
            new_candidates.append(candidate)
        if updated_pos:
            new_positions.append(updated_pos)
        elif pos and exit_reason:
            logger.info(f"  {symbol}: position closed ({exit_reason})")

        time.sleep(REQUEST_GAP_SEC)

    state["open_positions"] = new_positions
    state["candidates"] = new_candidates
    state["book_value"] = BOOK_VALUE
    state["last_scan"] = datetime.now().isoformat(timespec="seconds")
    _save_state(state)

    logger.info(f"Scan complete: {len(new_candidates)} new candidate(s), "
                f"{len(new_positions)} open position(s) tracked")


# ══════════════════════════════════════════════════════════════════════════════
# POSITION WATCHDOG — 5-min LTP-based stop/target check, confirmed positions only
# ══════════════════════════════════════════════════════════════════════════════

def check_open_positions() -> None:
    """Fast-path stop/target check against live LTP, confirmed open
    positions only. Deliberately NOT extended to candidate detection: a new
    signal needs a full 60-min bar + rolling volume-profile recompute (roll_low,
    POC) to mean anything, and the backtest itself only evaluated bar-closes --
    scanning entries any faster would depart from what was actually validated.
    A stop/target on an already-open position has no such requirement: it's
    just current LTP vs. numbers already known (`stop`, `target_poc`, last set
    by the hourly scan or by _confirm_vp_candidate() at entry), so it can be
    checked cheaply and often via a single get_multiquotes() batch call.

    Target check mirrors _scan_symbol()'s TOUCH_TOL_PCT tolerance; stop is an
    exact breach, same as the hourly scan's bar_low <= stop_price rule."""
    state = _load_state()
    open_positions = state.get("open_positions", [])
    if not open_positions:
        return

    symbols = [{"symbol": p["symbol"], "exchange": EXCHANGE} for p in open_positions]
    results = get_multiquotes(API_KEY, symbols)
    ltp_map = {}
    for r in results:
        sym = r.get("symbol")
        data = r.get("data", {})
        if sym and data:
            ltp_map[sym] = float(data.get("ltp", 0))

    remaining = []
    for pos in open_positions:
        symbol = pos["symbol"]
        ltp = ltp_map.get(symbol)
        if not ltp:
            logger.warning(f"  [position-watch] {symbol}: no LTP returned, keeping position")
            remaining.append(pos)
            continue

        stop_price = float(pos["stop"])
        if ltp <= stop_price:
            logger.info(f"  [position-watch] {symbol}: STOP hit intra-hour, "
                        f"ltp={ltp:.2f} <= stop={stop_price:.2f}")
            continue

        target_price = float(pos.get("target_poc") or 0)
        tol = target_price * TOUCH_TOL_PCT
        if target_price and ltp >= (target_price - tol):
            logger.info(f"  [position-watch] {symbol}: TARGET hit intra-hour, "
                        f"ltp={ltp:.2f} >= target={target_price:.2f} (tol {tol:.2f})")
            continue

        remaining.append(pos)

    closed = len(open_positions) - len(remaining)
    state["open_positions"] = remaining
    state["last_position_check"] = datetime.now().isoformat(timespec="seconds")
    _save_state(state)
    if closed:
        logger.info(f"  [position-watch] {closed} position(s) closed intra-hour")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP — wake every 20s, fire a scan once per SCAN_TIMES slot per day
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    _acquire_pid_lock()
    logger.info(f"🔬 VP Swing Screener starting | universe={len(NIFTY50_STOCKS)} stocks | "
                f"scan times={SCAN_TIMES} IST | SIGNAL-ONLY, no order placement")

    scanned_today: set[str] = set()
    current_date = date.today()
    next_position_check = datetime.now()

    while True:
        now = datetime.now()
        if now.date() != current_date:
            current_date = now.date()
            scanned_today = set()

        if now.weekday() < 5:
            hhmm = now.strftime("%H:%M")
            if hhmm in SCAN_TIMES and hhmm not in scanned_today:
                scanned_today.add(hhmm)
                is_trading, reason = is_nse_fo_trading_day_via_fyers(API_KEY)
                if is_trading:
                    logger.info(f"── Scan @ {hhmm} IST ──")
                    try:
                        run_scan_cycle()
                    except Exception as e:
                        logger.exception(f"Scan cycle failed: {e}")
                else:
                    logger.info(f"  Not a trading day ({reason}) -- skipping scan")

            if POSITION_CHECK_START <= hhmm <= POSITION_CHECK_END and now >= next_position_check:
                next_position_check = now + timedelta(seconds=POSITION_CHECK_INTERVAL_SEC)
                try:
                    check_open_positions()
                except Exception as e:
                    logger.exception(f"Position watchdog failed: {e}")

        time.sleep(20)


if __name__ == "__main__":
    main()
