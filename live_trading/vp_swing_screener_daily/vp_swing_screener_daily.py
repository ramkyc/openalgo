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

  A third, symmetric heads-up runs the other end of the session: from
  EQUILIBRIUM_POLL_START to EQUILIBRIUM_POLL_END (09:07-09:15 IST), NSE's
  own pre-open call-auction window, it repeatedly polls NSE's pre-open
  market feed (not Fyers -- see _fetch_nse_preopen_iep()) for the live
  Indicative Equilibrium Price (IEP) of every symbol still sitting in
  state["candidates"] (yesterday's close-confirmed touches, not yet
  manually confirmed) and shows the gap vs. touch price -- so the decision
  to confirm at today's open is informed by where the market is actually
  settling, not just yesterday's close data. The IEP itself is still
  settling during this window (orders enter 09:00-09:08, match through
  ~09:12) and loses meaning once continuous trading takes over at 09:15,
  which is why this is a repeated poll bounded to that window rather than a
  single snapshot -- see check_equilibrium_prices()'s docstring. Read-only,
  decision support only.

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
import requests
from dotenv import load_dotenv

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils import (  # noqa: E402
    get_history,
    get_multiquotes,
    is_nse_fo_trading_day_via_fyers,
)
from live_trading.shared.performance_db import log_trade  # noqa: E402
from live_trading.vp_swing_screener_daily.signal_tracker import (  # noqa: E402
    fetch_pending,
    log_signal,
    resolve_signal,
)

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
BOT_NAME      = "vp_swing_screener_daily"   # key into performance.db / vp_swing_signals
EXCHANGE      = "NSE"

BIN_PCT            = 0.00025
MIN_EXTENSION_PCT  = 0.0010
TOUCH_TOL_PCT      = 0.0005
CAPITAL_PER_TRADE  = 100_000

STOP_LOSS_PCT  = 0.03   # champion (carried over from 60-min study's Stage 3 sweep winner)
PROFILE_DAYS   = 10     # champion (carried over from 60-min study's Stage 3 sweep winner)
N_BARS         = PROFILE_DAYS   # one bar == one day at daily resolution, no multiplication

# Minimum (poc - entry) / entry room a candidate must clear to be raised at
# all. Without this, a candidate can be flagged with the touch/ltp already
# sitting at or past the POC target, leaving no room to cover friction
# (brokerage + slippage + STT) before it "hits target" -- see the 2026-08-20
# ONGC incident (confirmed with ~0% room, closed near-breakeven).
#
# NOT backtest-optimal: options_data/research/vp_swing_reversion_daily_study/
# stage14_target_room_sweep.py found Sharpe/PnL decrease monotonically as
# this threshold rises (0% room is Sharpe-best -- sub-1%-room trades have an
# 86-88% win rate that offsets their thin size). Kept at 1% anyway as a
# deliberate risk override, not a performance-maximizing choice: the
# backtest's cost model doesn't capture confirm-timing slippage, and 1% only
# costs ~1.3-2% Sharpe vs. unfiltered. User decision 2026-08-20.
MIN_TARGET_ROOM_PCT = 0.01

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

# Pre-open equilibrium heads-up: NSE's pre-open session runs order entry
# 09:00-09:08, order matching/trade confirmation ~09:08-09:12, buffer
# ~09:12-09:15, then continuous trading takes over. The Indicative
# Equilibrium Price (IEP) is still settling throughout that window as the
# call-auction order book fills in -- unlike a broker quote's `open` field
# (a single point read after the fact), NSE's own pre-open market feed
# exposes this as a live-updating figure, so this polls it repeatedly
# rather than reading it once. EQUILIBRIUM_POLL_END is a hard cutoff at
# continuous-trading open (09:15), past which the IEP concept no longer
# applies. See check_equilibrium_prices() / _fetch_nse_preopen_iep().
EQUILIBRIUM_POLL_START    = "09:07"
EQUILIBRIUM_POLL_END      = "09:15"
EQUILIBRIUM_POLL_INTERVAL_SEC = 30   # ~16 polls across the 8-min window

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


def _log_closed_trade(pos: dict, exit_price: float, exit_reason: str) -> None:
    """Write a stop/target-closed position to the shared performance.db,
    same convention every other bot uses. `source="live"` -- Confirm here
    means the user already executed the trade through their own broker
    terminal (see render_vp_swing_daily_screener_panel's caption), not a
    sandbox/paper fill."""
    entry_price = float(pos.get("entry_price") or 0)
    qty = int(pos.get("qty") or 0)
    if entry_price <= 0 or qty <= 0:
        return
    log_trade(
        bot_name=BOT_NAME,
        strategy_type="equity",
        instrument=pos.get("symbol", ""),
        symbol=pos.get("symbol", ""),
        entry_time=pos.get("entry_time") or pos.get("since"),
        exit_time=datetime.now(),
        entry_price=entry_price,
        exit_price=exit_price,
        exit_reason=exit_reason,
        quantity=qty,
        gross_pnl=(exit_price - entry_price) * qty,
        direction="long",
        source="live",
        notes="VP Swing Screener (Daily) — manual confirm, signal-only bot",
    )


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
            _log_closed_trade(pos, stop_price, "stop")
            return None, None, "stop"

        tol = poc_price * TOUCH_TOL_PCT
        if (bar_low - tol) <= poc_price <= (bar_high + tol):
            logger.info(f"  {symbol}: TARGET hit, poc={poc_price:.2f} within bar [{bar_low:.2f},{bar_high:.2f}]")
            _log_closed_trade(pos, poc_price, "target")
            return None, None, "target"

        updated = dict(pos)
        updated["target_poc"] = round(poc_price, 2)
        return None, updated, None

    if bar_low < roll_low * (1 - MIN_EXTENSION_PCT):
        entry_price = bar_low
        target_room = (poc_price - entry_price) / entry_price if entry_price else 0.0
        if target_room < MIN_TARGET_ROOM_PCT:
            logger.info(f"  {symbol}: touch={entry_price:.2f} poc={poc_price:.2f} -- only "
                        f"{target_room * 100:.2f}% room to target, below {MIN_TARGET_ROOM_PCT * 100:.0f}% "
                        f"minimum, skipping")
            return None, None, None
        stop_price = entry_price * (1 - STOP_LOSS_PCT)
        close_price = float(closes[i])
        pct_off_touch = ((close_price - entry_price) / entry_price * 100) if entry_price else 0.0
        candidate = {
            "symbol": symbol,
            "touch_price": round(entry_price, 2),
            "poc": round(poc_price, 2),
            "stop": round(stop_price, 2),
            "close": round(close_price, 2),
            "pct_off_touch": round(pct_off_touch, 2),
            "detected_at": datetime.now().isoformat(timespec="seconds"),
        }
        logger.info(f"  {symbol}: CANDIDATE touch={entry_price:.2f} poc={poc_price:.2f} stop={stop_price:.2f} "
                    f"close={close_price:.2f} off_touch={pct_off_touch:+.2f}%")
        log_signal(
            bot_name=BOT_NAME, symbol=symbol, source="final_scan",
            detected_at=candidate["detected_at"], signal_price=entry_price,
            touch_price=entry_price, poc_target=poc_price, stop_price=stop_price,
            reference_price=close_price,
        )
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

    target_room = (poc_price - ltp) / ltp if ltp else 0.0
    if target_room < MIN_TARGET_ROOM_PCT:
        logger.info(f"  {symbol}: ltp={ltp:.2f} poc={poc_price:.2f} -- only "
                    f"{target_room * 100:.2f}% room to target, below {MIN_TARGET_ROOM_PCT * 100:.0f}% "
                    f"minimum, skipping")
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
    log_signal(
        bot_name=BOT_NAME, symbol=symbol, source="intraday_headsup",
        detected_at=candidate["detected_at"], signal_price=ltp,
        touch_price=entry_price, poc_target=poc_price, stop_price=stop_price,
        reference_price=ltp,
    )
    return candidate


# ══════════════════════════════════════════════════════════════════════════════
# SCAN CYCLE
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_pending_signals() -> None:
    """Once-daily pass (piggybacked on run_scan_cycle, right after the
    day's bar is complete): re-check every not-yet-resolved signal --
    confirmed or not -- against bars since it was detected, same stop/target
    rule _scan_symbol() applies to a real position. Lets an unconfirmed
    candidate's hypothetical outcome be reviewed later, not just the ones
    that became real positions."""
    pending = fetch_pending(BOT_NAME)
    if not pending:
        return
    logger.info(f"── Resolving {len(pending)} pending signal(s) ──")
    for sig in pending:
        symbol = sig["symbol"]
        try:
            raw = get_history(API_KEY, symbol, EXCHANGE, "1m", duration_days=HISTORY_LOOKBACK_DAYS)
            df = _history_to_frame(raw)
            if df.empty:
                continue
            res = resample_daily(df)
            if res.empty:
                continue
            detected_date = datetime.fromisoformat(sig["detected_at"]).date()
            stop_price = float(sig["stop_price"] or 0)
            poc_price  = float(sig["poc_target"] or 0)
            tol = poc_price * TOUCH_TOL_PCT

            subsequent = res[res["ts"].dt.date > detected_date]
            for _, bar in subsequent.iterrows():
                bar_low, bar_high = float(bar["low"]), float(bar["high"])
                if bar_low <= stop_price:
                    resolve_signal(sig["id"], "stop", stop_price)
                    logger.info(f"  {symbol}: signal STOP hit ({sig['source']}, "
                                f"detected {detected_date})")
                    break
                if (bar_low - tol) <= poc_price <= (bar_high + tol):
                    resolve_signal(sig["id"], "target", poc_price)
                    logger.info(f"  {symbol}: signal TARGET hit ({sig['source']}, "
                                f"detected {detected_date})")
                    break
        except Exception as e:
            logger.exception(f"  {symbol}: signal resolution failed: {e}")
        time.sleep(REQUEST_GAP_SEC)


def run_scan_cycle() -> None:
    _resolve_pending_signals()
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
    # Same staleness reasoning: this morning's 09:07-09:15 equilibrium polls
    # (if any) were against yesterday's candidates list, now superseded.
    state["equilibrium_candidates"] = []
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


_NSE_PREOPEN_URL = "https://www.nseindia.com/api/market-data-pre-open?key=ALL"
_NSE_REFERER_URL = "https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market"
_NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": _NSE_REFERER_URL,
}

# Reused across the whole 09:07-09:15 poll window rather than re-established
# every poll -- NSE's anti-bot layer gates the API behind cookies obtained
# from an ordinary page load, so re-doing that handshake every 30s would be
# both wasteful and more bot-like, not less. Reset once per trading day (see
# main()) and again on any request failure, since a stale/rejected cookie
# jar is the most likely cause of a failure.
_nse_session: requests.Session | None = None


def _get_nse_session() -> requests.Session:
    global _nse_session
    if _nse_session is None:
        s = requests.Session()
        s.headers.update(_NSE_HEADERS)
        try:
            s.get(_NSE_REFERER_URL, timeout=10)
        except requests.RequestException as e:
            logger.warning(f"  [equilibrium] NSE session warm-up failed: {e}")
        _nse_session = s
    return _nse_session


def _fetch_nse_preopen_iep() -> dict[str, float]:
    """Poll NSE's own pre-open market feed for the live Indicative
    Equilibrium Price (IEP) per symbol -- the same figure NSE's pre-open
    market page shows updating as the call-auction order book fills in
    during 09:00-09:08 and settles through ~09:08-09:12 matching. This is
    NSE's own website, not Fyers/OpenAlgo -- a broker quote's `open` field
    only reflects a single point read after the fact, not the live-evolving
    IEP this screener wants during the poll window.

    Returns {} on any failure (timeout, non-200, malformed JSON) -- callers
    must treat that as "skip this poll", not a fatal error. NSE's site has
    no SLA for this endpoint and occasional blocks/timeouts are expected;
    a failed poll here also resets _nse_session so the next poll gets a
    fresh cookie jar rather than repeatedly hitting a rejected one.
    """
    global _nse_session
    session = _get_nse_session()
    try:
        resp = session.get(_NSE_PREOPEN_URL, timeout=10)
        if resp.status_code != 200:
            logger.warning(f"  [equilibrium] NSE pre-open feed returned HTTP {resp.status_code}")
            _nse_session = None
            return {}
        payload = resp.json()
    except Exception as e:
        logger.warning(f"  [equilibrium] NSE pre-open feed request failed: {e}")
        _nse_session = None
        return {}

    iep_by_symbol: dict[str, float] = {}
    for row in payload.get("data", []):
        meta = row.get("metadata", {})
        symbol = meta.get("symbol")
        iep = meta.get("iep")
        if not symbol or iep in (None, "", "-"):
            continue
        try:
            iep_by_symbol[symbol] = float(iep)
        except (TypeError, ValueError):
            continue
    return iep_by_symbol


def check_equilibrium_prices() -> None:
    """Called on every tick during EQUILIBRIUM_POLL_START..END (09:07-09:15
    IST, NSE's pre-open call-auction window). Polls NSE's own pre-open
    market feed (_fetch_nse_preopen_iep(), NOT a Fyers/OpenAlgo quote) for
    the live IEP of every symbol currently in state["candidates"] --
    close-confirmed touches from yesterday's scan, not yet manually
    confirmed into a position -- and overwrites state["equilibrium_candidates"]
    with the latest reading each poll, so the dashboard always shows the
    freshest gap-vs-touch-price figure while the window is open. Purely
    decision support for "should I confirm this at today's open" -- never
    places orders, never touches open_positions.

    A poll that returns no usable data (NSE feed down, no candidates
    matched) leaves the previous reading in state untouched rather than
    clearing it -- state["last_equilibrium_check"] tells the dashboard how
    stale the displayed figure is.
    """
    state = _load_state()
    candidates = state.get("candidates", [])
    if not candidates:
        return

    iep_by_symbol = _fetch_nse_preopen_iep()
    if not iep_by_symbol:
        return   # NSE feed unavailable this poll -- try again next tick

    equilibrium_candidates: list[dict] = []
    for c in candidates:
        symbol = c["symbol"]
        iep = iep_by_symbol.get(symbol)
        if not iep:
            continue

        touch_price = float(c.get("touch_price") or 0)
        gap_pct = ((iep - touch_price) / touch_price * 100) if touch_price else 0.0

        entry = dict(c)
        entry.update({
            "equilibrium_price": round(iep, 2),
            "gap_vs_touch_pct": round(gap_pct, 2),
            "checked_at": datetime.now().isoformat(timespec="seconds"),
        })
        equilibrium_candidates.append(entry)

    if not equilibrium_candidates:
        logger.warning("  [equilibrium] NSE feed returned data but none matched pending candidates")
        return

    # Re-load + write rather than reusing the `state` read at the top --
    # run_scan_cycle()/check_open_positions() can write state independently
    # between this function's read and its write.
    state = _load_state()
    state["equilibrium_candidates"] = equilibrium_candidates
    state["last_equilibrium_check"] = datetime.now().isoformat(timespec="seconds")
    _save_state(state)
    logger.info(f"  [equilibrium] {len(equilibrium_candidates)} candidate(s) updated @ "
                f"{datetime.now().strftime('%H:%M:%S')}")


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
            _log_closed_trade(pos, ltp, "stop")
            continue

        target_price = float(pos.get("target_poc") or 0)
        tol = target_price * TOUCH_TOL_PCT
        if target_price and ltp >= (target_price - tol):
            logger.info(f"  [position-watch] {symbol}: TARGET hit intra-day, "
                        f"ltp={ltp:.2f} >= target={target_price:.2f} (tol {tol:.2f})")
            _log_closed_trade(pos, ltp, "target")
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
                f"pre-open equilibrium poll={EQUILIBRIUM_POLL_START}-{EQUILIBRIUM_POLL_END} IST, "
                f"intraday heads-up={INTRADAY_CHECK_TIME} IST, final scan={SCAN_TIME} IST | "
                f"book={BOOK_VALUE/1e5:.1f}L | SIGNAL-ONLY, no order placement")

    scanned_today = False
    intraday_checked_today = False
    current_date = date.today()
    next_position_check = datetime.now()
    next_equilibrium_poll = datetime.now()

    while True:
        now = datetime.now()
        if now.date() != current_date:
            current_date = now.date()
            scanned_today = False
            intraday_checked_today = False
            global _nse_session
            _nse_session = None   # force a fresh NSE cookie jar each trading day

        if now.weekday() < 5:
            hhmm = now.strftime("%H:%M")

            # Pre-open equilibrium poll, repeated every EQUILIBRIUM_POLL_INTERVAL_SEC
            # across the narrow EQUILIBRIUM_POLL_START..END window -- the NSE
            # IEP it reads is still settling through that window and stops
            # meaning anything once continuous trading gets going at 09:15.
            if EQUILIBRIUM_POLL_START <= hhmm < EQUILIBRIUM_POLL_END and now >= next_equilibrium_poll:
                next_equilibrium_poll = now + timedelta(seconds=EQUILIBRIUM_POLL_INTERVAL_SEC)
                is_trading, reason = is_nse_fo_trading_day_via_fyers(API_KEY)
                if is_trading:
                    try:
                        check_equilibrium_prices()
                    except Exception as e:
                        logger.exception(f"Equilibrium poll failed: {e}")

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
