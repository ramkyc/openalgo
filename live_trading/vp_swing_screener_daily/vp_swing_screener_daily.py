#!/usr/bin/env python3
"""
VP Swing Screener (Daily) — once-per-day scan engine
live_trading/vp_swing_screener_daily/vp_swing_screener_daily.py

Research-validated (vp_swing_reversion_daily_study, completed 2026-08-12):
  options_data/research/vp_swing_reversion_daily_study/results_summary.md, DECISIONS.md
  Full IS+OOS (2023-11-17->2026-08-07). Champion config carried over unchanged
  from the 60-min study's Stage 3 sweep winner (SL=3%, PROFILE_DAYS=10),
  re-run at daily-bar resolution. Stages 2-9, 12, 13 all run.
  Stage 12 overnight-gap tail risk (worst 1%ile -5.63%) reviewed and accepted
  2026-08-12 as a quantified, documented cost -- NOT remediated in code.
  Stage 13 margin/capital utilisation: final capital basis Rs.50,00,000
  (DECISIONS.md #6) -- G-13-B (peak deployment <=95%) PASSES at 87.1%;
  G-13-C (1.5x stress <=90%) still FAILs at 130.6%, accepted 2026-08-12 as a
  documented cost, same category as the Stage 12 acceptance.

Strategy (DAILY bars, long-only, multi-day swing hold, no forced EOD close):
  Rolling PROFILE_DAYS=10-trading-day volume profile (N_BARS = PROFILE_DAYS =
  10 unconditionally -- one bar IS one day, no bars_per_day multiplication,
  see options_data DECISIONS.md #3). A touch of the rolling window's lower
  low -> long candidate. Exit: target = rolling-window POC (recomputed once
  per day), or hard 3% stop-loss (STOP_LOSS_PCT). No pyramiding -- one
  position per symbol. Universe: 53 NIFTY50 stocks (swing_core.NIFTY50_STOCKS,
  identical list to the 60-min screener's, re-verified against this
  instance's symtoken master).

  Because a daily bar only completes at the session close (~15:40 IST,
  NSE Closing Auction Session rollout 2026-08-03 moved this from 15:30), a
  signal detected on day D's close is only actionable at day D+1's open at
  the earliest -- structurally different cadence from the 60-min/15-min
  screeners, which detect and can act on a signal within the same session.
  This screener therefore scans ONCE per trading day, shortly after close
  (SCAN_TIME, 15:45 IST) rather than at multiple intraday bar-close times.

  Additionally, an INTRADAY_CHECK_TIME (15:30 IST) heads-up pass evaluates
  the same touch condition against today's still-forming bar (not yet
  closed) and, if triggered, shows day-low-so-far / LTP / %-off-low in a
  separate "provisional" list -- letting you optionally enter into today's
  close instead of waiting for D+1's open, at the cost of the exact low (and
  any recovery off it) not being final until 15:40. The touch condition
  itself is monotonic through the session (today's low can only fall
  further by close), so an intraday touch is guaranteed to still hold at the
  15:45 scan -- see _scan_symbol_intraday()'s docstring. Single snapshot by
  design, not a poll loop.

⚠️  THIS IS A SCREENER, NOT AN ORDER-PLACING BOT -- it never calls
placeorder() and never will. It only detects signals and writes them to
STATE_FILE for live_trading/streamlit_dashboard.py's
render_vp_swing_daily_screener_panel() to display. A detected candidate
becomes a tracked "open position" only when you manually execute it through
your own broker terminal and click "Confirm" on the dashboard panel (which
writes directly into open_positions in the state file below) -- there is no
automatic promotion, and no positionbook auto-detection, because this
screener cannot reliably distinguish a position you took off one of its
candidates from an unrelated holding.

Signal logic below is a line-for-line port of
options_data/research/vp_swing_reversion_daily_study/swing_core.py's
resample_daily() and scan_symbol_trades() -- see that file for the
authoritative research version. Each scan only evaluates the single most
recently completed daily bar per symbol (not a full historical replay), so a
scan that is skipped or delayed (process down, OpenAlgo outage) can miss a
signal that would have fired on an intermediate day -- this is a known,
accepted limitation of a periodic REST-poll screener rather than a
continuous tick-driven bot, same category of limitation as the 60-min
screener's own docstring documents.
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
        logging.FileHandler(LOGS_DIR / "vp_swing_screener_daily.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS — mirrored 1:1 from options_data's daily-study swing_core.py
# (champion config carried over unchanged: SL=3%, PD=10)
# ══════════════════════════════════════════════════════════════════════════════

API_KEY = os.getenv("OPENALGO_API_KEY")

STRATEGY_NAME = "VP_SWING_SCREENER_DAILY"
EXCHANGE      = "NSE"

BIN_PCT            = 0.00025
MIN_EXTENSION_PCT  = 0.0010
TOUCH_TOL_PCT      = 0.0005
CAPITAL_PER_TRADE  = 100_000

STOP_LOSS_PCT  = 0.03   # champion (carried over from 60-min study's Stage 3 sweep winner)
PROFILE_DAYS   = 10     # champion (carried over from 60-min study's Stage 3 sweep winner)
N_BARS         = PROFILE_DAYS   # one bar == one day at daily resolution, no multiplication

BOOK_VALUE = 5_000_000   # Rs.50L -- final Stage 13 capital basis (DECISIONS.md #6)

# How much 1-min history to fetch per symbol per scan. N_BARS=10 needs 11
# trading days of daily bars; 40 calendar days comfortably covers that plus
# weekends/holidays plus the in-progress current day.
HISTORY_LOOKBACK_DAYS = 40
REQUEST_GAP_SEC = 1.0   # be gentle on the single-eventlet-worker REST API

# Single once-per-day scan, shortly after the 15:40 session close (NSE CAS
# rollout 2026-08-03 moved close 15:30->15:40), so the day's daily bar
# (open/high/low/close/volume) is fully settled before the rolling-window
# signal is evaluated.
SCAN_TIME = "15:45"

# Optional early heads-up, ~10 min before close: same touch condition as
# SCAN_TIME but evaluated against today's still-forming bar (resample_daily()
# already emits a row for the in-progress day). The touch test
# (bar_low < roll_low*(1-ext)) is monotonic through the session -- today's low
# can only fall further by 15:40, never rise back above threshold -- so an
# intraday touch here is guaranteed to still hold at the SCAN_TIME close scan.
# What's NOT guaranteed: the exact low (could still drop further) or that any
# recovery off it holds through the last ~10 minutes. Written to a separate
# state["intraday_candidates"] key so it's never confused with a
# close-confirmed candidate. Single snapshot by design, not a poll loop.
INTRADAY_CHECK_TIME = "15:30"

# Fast-path stop/target watchdog for confirmed open positions only (real
# capital at risk, unlike unconfirmed candidates) -- runs independently of
# the once-daily full scan so a stop/target breach doesn't wait a full day
# to be caught. See check_open_positions() for why this doesn't apply to
# entry-signal detection (identical rationale to the 60-min screener).
POSITION_CHECK_INTERVAL_SEC = 300   # 5 min
POSITION_CHECK_START, POSITION_CHECK_END = "09:15", "15:40"

STATE_FILE = LOGS_DIR / "vp_swing_screener_daily_state.json"
PID_FILE   = LOGS_DIR / "vp_swing_screener_daily.pid"

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
# SIGNAL LOGIC — line-for-line port of swing_core.py's resample_daily() and the
# per-bar body of scan_symbol_trades(), applied to only the latest bar.
# ══════════════════════════════════════════════════════════════════════════════

def _history_to_frame(raw: list) -> pd.DataFrame:
    """Match nifty_ema_spread_bot._load_history()'s raw->DataFrame convention
    (identical to the 60-min screener's own _history_to_frame())."""
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


def resample_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Verbatim port of options_data daily-study swing_core.resample_daily():
    group 1-min bars by calendar trading day into one OHLCV row/day."""
    if df.empty:
        return df
    df = df.set_index("ts")
    out = []
    for d, day_df in df.groupby(df.index.date):
        if day_df.empty:
            continue
        out.append({
            "ts": pd.Timestamp(d),
            "open": day_df["open"].iloc[0],
            "high": day_df["high"].max(),
            "low": day_df["low"].min(),
            "close": day_df["close"].iloc[-1],
            "volume": day_df["volume"].sum(),
        })
    if not out:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    return pd.DataFrame(out).sort_values("ts").reset_index(drop=True)


def _window_stats(window_close: np.ndarray, window_vol: np.ndarray,
                   window_low: np.ndarray) -> tuple[float, float]:
    """Verbatim port of scan_symbol_trades()'s per-bar roll_low/poc_price
    computation -- identical to the 60-min screener's own _window_stats()."""
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
    """Evaluate the latest completed daily bar for `symbol` against
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

    res = resample_daily(df)
    n = len(res)
    if n <= N_BARS + 1:
        logger.warning(f"  {symbol}: only {n} resampled daily bars (need > {N_BARS + 1}), skipping")
        return None, pos, None

    lows    = res["low"].values
    highs   = res["high"].values
    closes  = res["close"].values
    volumes = res["volume"].values

    i = n - 1   # latest completed daily bar
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


def _scan_symbol_intraday(symbol: str, has_position: bool, already_candidate: bool) -> dict | None:
    """Early heads-up version of _scan_symbol()'s new-candidate branch,
    evaluated against today's still-forming bar instead of a completed one
    (resample_daily() already emits a row for the in-progress day from
    whatever 1-min bars have printed so far). See INTRADAY_CHECK_TIME's
    comment for why the touch condition is safe to act on early even though
    the exact low and any recovery off it are not yet final.

    Only the new-candidate path applies -- stop/target exits on already-open
    positions are handled by check_open_positions()'s 5-min LTP watch, not
    this once-a-day heads-up, so symbols with a position or an existing
    close-confirmed candidate are skipped entirely.
    """
    if has_position or already_candidate:
        return None

    raw = get_history(API_KEY, symbol, EXCHANGE, "1m", duration_days=HISTORY_LOOKBACK_DAYS)
    df = _history_to_frame(raw)
    if df.empty:
        return None

    res = resample_daily(df)
    n = len(res)
    if n <= N_BARS + 1:
        return None

    lows    = res["low"].values
    closes  = res["close"].values
    volumes = res["volume"].values

    i = n - 1   # today's still-forming bar
    roll_low, poc_price = _window_stats(
        closes[i - N_BARS:i], volumes[i - N_BARS:i], lows[i - N_BARS:i]
    )
    day_low_so_far = float(lows[i])
    ltp = float(closes[i])   # latest available 1-min close == current LTP

    if day_low_so_far >= roll_low * (1 - MIN_EXTENSION_PCT):
        return None

    entry_price = day_low_so_far
    stop_price = entry_price * (1 - STOP_LOSS_PCT)
    pct_off_low = ((ltp - day_low_so_far) / day_low_so_far * 100) if day_low_so_far else 0.0

    candidate = {
        "symbol": symbol,
        "touch_price": round(entry_price, 2),
        "poc": round(poc_price, 2),
        "stop": round(stop_price, 2),
        "day_low_so_far": round(day_low_so_far, 2),
        "ltp": round(ltp, 2),
        "pct_off_low": round(pct_off_low, 2),
        "detected_at": datetime.now().isoformat(timespec="seconds"),
    }
    logger.info(f"  {symbol}: INTRADAY candidate day_low={day_low_so_far:.2f} "
                f"ltp={ltp:.2f} off_low={pct_off_low:.2f}% poc={poc_price:.2f}")
    return candidate


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
    # candidates is now authoritative for the day -- the earlier heads-up
    # scan's provisional list would otherwise linger stale next to it.
    state["intraday_candidates"] = []
    state["book_value"] = BOOK_VALUE
    state["last_scan"] = datetime.now().isoformat(timespec="seconds")
    _save_state(state)

    logger.info(f"Scan complete: {len(new_candidates)} new candidate(s), "
                f"{len(new_positions)} open position(s) tracked")


def run_intraday_check_cycle() -> None:
    """One heads-up pass at INTRADAY_CHECK_TIME (~10 min before close).
    Signal-only, same as the main scan -- writes state["intraday_candidates"]
    for the dashboard to display; never places orders. See
    INTRADAY_CHECK_TIME's comment for what is and isn't final about these."""
    state = _load_state()
    existing_positions  = {p["symbol"] for p in state.get("open_positions", []) if p.get("symbol")}
    existing_candidates = {c["symbol"] for c in state.get("candidates", []) if c.get("symbol")}

    intraday_candidates: list[dict] = []
    for symbol in NIFTY50_STOCKS:
        try:
            candidate = _scan_symbol_intraday(
                symbol,
                has_position=symbol in existing_positions,
                already_candidate=symbol in existing_candidates,
            )
        except Exception as e:
            logger.exception(f"  {symbol}: intraday scan failed: {e}")
            candidate = None

        if candidate:
            intraday_candidates.append(candidate)

        time.sleep(REQUEST_GAP_SEC)

    state["intraday_candidates"] = intraday_candidates
    state["last_intraday_check"] = datetime.now().isoformat(timespec="seconds")
    _save_state(state)

    logger.info(f"Intraday check complete: {len(intraday_candidates)} provisional candidate(s)")


# ══════════════════════════════════════════════════════════════════════════════
# POSITION WATCHDOG — 5-min LTP-based stop/target check, confirmed positions only
# ══════════════════════════════════════════════════════════════════════════════

def check_open_positions() -> None:
    """Fast-path stop/target check against live LTP, confirmed open
    positions only. Identical rationale to the 60-min screener's own
    check_open_positions(): a new signal needs a full daily bar + rolling
    volume-profile recompute to mean anything, but a stop/target on an
    already-open position is just current LTP vs. numbers already known
    (`stop`, `target_poc`), so it can be checked cheaply and often via a
    single get_multiquotes() batch call."""
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
            logger.info(f"  [position-watch] {symbol}: STOP hit intra-day, "
                        f"ltp={ltp:.2f} <= stop={stop_price:.2f}")
            continue

        target_price = float(pos.get("target_poc") or 0)
        tol = target_price * TOUCH_TOL_PCT
        if target_price and ltp >= (target_price - tol):
            logger.info(f"  [position-watch] {symbol}: TARGET hit intra-day, "
                        f"ltp={ltp:.2f} >= target={target_price:.2f} (tol {tol:.2f})")
            continue

        remaining.append(pos)

    closed = len(open_positions) - len(remaining)
    state["open_positions"] = remaining
    state["last_position_check"] = datetime.now().isoformat(timespec="seconds")
    _save_state(state)
    if closed:
        logger.info(f"  [position-watch] {closed} position(s) closed intra-day")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP — wake every 20s, fire one scan per trading day at/after SCAN_TIME
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    _acquire_pid_lock()
    logger.info(f"🔬 VP Swing Screener (Daily) starting | universe={len(NIFTY50_STOCKS)} stocks | "
                f"intraday heads-up={INTRADAY_CHECK_TIME} IST, final scan={SCAN_TIME} IST | "
                f"book={BOOK_VALUE/1e5:.1f}L | SIGNAL-ONLY, no order placement")

    scanned_today = False
    intraday_checked_today = False
    current_date = date.today()
    next_position_check = datetime.now()

    while True:
        now = datetime.now()
        if now.date() != current_date:
            current_date = now.date()
            scanned_today = False
            intraday_checked_today = False

        if now.weekday() < 5:
            hhmm = now.strftime("%H:%M")

            # Heads-up pass at INTRADAY_CHECK_TIME, strictly before SCAN_TIME
            # -- if the process starts after SCAN_TIME has already passed
            # (e.g. a restart), skip the now-pointless heads-up rather than
            # firing it after the authoritative scan already ran.
            if (INTRADAY_CHECK_TIME <= hhmm < SCAN_TIME) and not intraday_checked_today:
                intraday_checked_today = True
                is_trading, reason = is_nse_fo_trading_day_via_fyers(API_KEY)
                if is_trading:
                    logger.info(f"── Intraday heads-up @ {hhmm} IST ──")
                    try:
                        run_intraday_check_cycle()
                    except Exception as e:
                        logger.exception(f"Intraday check cycle failed: {e}")
                else:
                    logger.info(f"  Not a trading day ({reason}) -- skipping intraday check")

            # Single once-daily scan at/after SCAN_TIME -- ">=" (not "==") so a
            # process started after 15:35 (e.g. deploying this evening, or
            # recovering from a restart) still runs today's scan immediately
            # instead of waiting until tomorrow.
            if hhmm >= SCAN_TIME and not scanned_today:
                scanned_today = True
                is_trading, reason = is_nse_fo_trading_day_via_fyers(API_KEY)
                if is_trading:
                    logger.info(f"── Daily scan @ {hhmm} IST ──")
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
