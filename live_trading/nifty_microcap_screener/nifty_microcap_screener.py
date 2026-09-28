#!/usr/bin/env python3
"""
NIFTY Microcap Screener — once-per-day scan engine
live_trading/nifty_microcap_screener/nifty_microcap_screener.py

Research-validated (options_data/research/nifty_microcap_momentum_study,
completed 2026-08-25): trailing 252-trading-day return skipping the most
recent 21 trading days (Jegadeesh-Titman 12-1 momentum), ranked
cross-sectionally each calendar month among symbols with >=278 trading days
trailing history, top-15% long, equal-weighted, rebalanced MONTHLY -- the
Stage-2 sweep CHAMPION config (Sharpe 1.66; results_summary.md section 1),
not the Stage-1 pre-sweep default (weekly/top-10%, Sharpe 1.43) that every
earlier draft of this script wrongly used. Decision on the last trading day
of the month's close, executed at the next trading day's open -- i.e. the
1st trading session of the following month (results_summary.md section 2).
Typical book size ~11-12 names (mean 12.0), not the ~23 a top-10% cut
produces. Cross-sectional rotation, not a per-symbol entry/exit system: a
position "exits" purely by falling out of the next month's top-15% ranking,
not by any price-based stop or target -- structurally different from every
other screener in this directory (VP Swing, EMA spreads, ...), which are all
single-touch/target models.

Universe: 250 symbols, current NIFTY Microcap 250 constituents as of
2026-08-25 (MICROCAP_STOCKS below), a one-time static snapshot pulled
read-only from options_data/data/options_data.duckdb's
microcap_constituent_daily table -- same embedding pattern as
vp_swing_screener_daily.py's NIFTY50_STOCKS. Today's constituents, not
point-in-time -- the validated study's own survivorship-bias caveat applies
here too. Re-pull and update this list by hand if/when the index is
reconstituted; there is no runtime cross-repo DB query (this repo talks to
options_data.duckdb never -- see this project's CLAUDE.md "Broker Token
Boundaries" section for the analogous "one-way, snapshot-only" pattern applied
to a different dependency).

Split/bonus adjustment: verified empirically (2026-08-25) that Fyers'
historical daily API already returns split/bonus-adjusted closes, not raw
prices -- confirmed on CUPID's own March 2026 4:1 bonus (ex-date 2026-03-09)
and April 2024 1:10 split + 1:1 bonus (ex-date 2024-04-04), and independently
on TATASTEEL's well-documented July 2022 1:10 split: none of these show the
one-day price cliff a raw/unadjusted feed would produce, and CUPID's own
12-1 momentum window (Rs.19.18 on 2025-06-24 -> Rs.192.06 on 2026-07-02)
reproduces the screener's +901.4% reading exactly, i.e. that reading is a
genuine (if extreme) organic price move, not a corporate-action artifact.
No adjustment layer is needed or ported here -- one would double-adjust
already-adjusted data. options_data/research/split_adjustment.py exists for
a DIFFERENT data source (preopen_data's raw NSE bhavcopy, which has no
adjusted-close column) and is not applicable to this Fyers-sourced pipeline;
do not port it here. The one residual risk this doesn't cover is a corporate
action OTHER than a split/bonus (e.g. a demerger or large one-off special
dividend) that Fyers' feed does not retroactively adjust for -- so a
candidate at an extreme trailing 12-1 momentum is still worth a quick sanity
look, but as a general outlier check, not because the pipeline is known to
be wrong -- flagged in the dashboard panel and here.

⚠️  THIS IS A SCREENER, NOT AN ORDER-PLACING BOT -- it never calls
placeorder() and never will. It computes the monthly top-15% target list and
writes it to STATE_FILE for live_trading/streamlit_dashboard.py's
render_nifty_microcap_screener_panel() to display. A target-list name
becomes a tracked "open position" only when you manually execute it through
your own broker terminal and click "Confirm" on the dashboard panel, typing
in the ACTUAL fill price and qty yourself -- there is no automatic
promotion, and no positionbook auto-detection, same rationale as every other
screener in this directory: this screener cannot reliably distinguish a
position you took off one of its candidates from an unrelated holding.

Rebalance is monthly, not daily, so the scan does not surface a
continuously-shifting "today's" list. Each daily run instead checks whether
the PRIOR calendar month's close can now be proven final (see run_scan_cycle
below) and, the moment it can, computes that month's top-15% list exactly
ONCE and writes it forever to rebalance_db.monthly_target_lists -- never
recomputed afterwards. The dashboard only surfaces that permanent record,
and only for a few days around when it was finalized (see
streamlit_dashboard.py's VISIBILITY_WINDOW_DAYS) -- not every day.

Exit semantics (no price-based stop/target exists for this strategy, unlike
every other bot's template): a held symbol that drops out of the latest
finalized month's top-15% list is surfaced in state["exit_candidates"], not
removed from open_positions automatically. It is closed -- in
rebalance_db.positions (permanent) and logged to performance.db (the
completed-trade P&L ledger) -- only when you click "Confirm Exit" on the
dashboard after you have actually sold it, typing in the ACTUAL exit fill
price yourself -- deliberately the same manual-confirm,
never-silently-mutate-your-position philosophy as the entry side, applied
symmetrically since this strategy's "exit" is a ranking event, not a price
event. At the NEXT month-end finalization, rebalance_db.positions (not the
prior month's candidate list) is what determines which held names dropped
out and which new top-15% names are not yet held.

Signal logic below (compute_momentum_panel, build_monthly_rebalance_dates,
build_target_portfolio) is a line-for-line port of
options_data/research/nifty_microcap_momentum_study/stage0_discovery.py,
stage2_param_sweep.py (monthly rebalance-date construction and the
top_pct/freq champion selection) and screener.py -- see those files for the
authoritative research version. Each
scan fetches trailing daily bars fresh per symbol via the OpenAlgo REST API
(interval="D") rather than querying options_data.duckdb, since a live bot
must never reach into the research repo's database (options_data/CLAUDE.md,
read-only-during-research boundary) and research code must never call the
live OpenAlgo API (options_data/.claude/skills/backtest-writer's forbidden
list) -- REST is the only channel allowed to cross that boundary.
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

import pandas as pd
from dotenv import load_dotenv

# ── Path / env ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent   # .../openalgo
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from live_trading.api_utils import (  # noqa: E402
    get_history,
    is_market_holiday,
    is_nse_fo_trading_day_via_fyers,
)
from live_trading.shared.performance_db import log_trade  # noqa: E402
from live_trading.shared.rebalance_notifier import notify_finalized_list  # noqa: E402
from live_trading.nifty_microcap_screener import rebalance_db  # noqa: E402

# ── Logging ───────────────────────────────────────────────────────────────────
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "nifty_microcap_screener.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS — signal params mirrored 1:1 from options_data's validated study
# ══════════════════════════════════════════════════════════════════════════════

API_KEY = os.getenv("OPENALGO_API_KEY")

STRATEGY_NAME = "NIFTY_MICROCAP_SCREENER"
BOT_NAME      = "nifty_microcap_screener"   # key into performance.db
EXCHANGE      = "NSE"

LOOKBACK_DAYS      = 252    # ~12 months of trading days
SKIP_DAYS          = 21     # ~1 month skip (short-term reversal avoidance)
MIN_HISTORY_DAYS   = LOOKBACK_DAYS + SKIP_DAYS + 5   # buffer, matches stage0_discovery.py

# TOP_PCT=0.15 + monthly cadence is the Stage-2 sweep CHAMPION config, not the
# Stage-1 pre-sweep default (weekly/top-10%, Sharpe 1.43). The champion --
# Sharpe 1.66, the config every later validation stage (OOS, Monte Carlo,
# Bootstrap, Multi-Instrument, Regime Filter) was actually run against -- is
# top-15%/monthly (results_summary.md section 1 and stage2_param_sweep.py's
# TOP_PCTS/FREQUENCIES sweep). Typical book size ~11-12 names, not 23.
TOP_PCT = 0.15

SCREEN_CAPITAL = 10_00_000.0   # Rs.10L -- ONE-TIME seed for rebalance_db.capital_ledger.
# book_value is now NAV-style, tracked in rebalance_db (seed + realized P&L -
# withdrawals) rather than reset to this constant every scan -- see
# rebalance_db.get_book_value(). SCREEN_CAPITAL itself is read only by
# ensure_capital_seeded(), and only actually applied once (the ledger's first
# ever row); every later scan derives book_value from the ledger's running sum.

SWP_MONTHLY_AMOUNT = 10_000.0   # Rs.10K/month systematic withdrawal, user-confirmed
# on the dashboard (never auto-deducted -- same manual-confirm philosophy as
# entries/exits). At most one withdrawal per rebalance_month -- see
# rebalance_db.record_withdrawal()/has_withdrawal_for().

# How much daily history to fetch per symbol per scan. MIN_HISTORY_DAYS=278
# trading days needs a comfortable calendar-day buffer for weekends/holidays --
# but that buffer has to cover eligibility at reb_dates[-2] (the PRIOR
# month's close, what finalization actually evaluates against), not at
# "today". reb_dates[-2] sits ~1 month (~21 trading days) further back than
# today, so the naive 420-calendar-day buffer (originally sized only for a
# "today" signal) left ~zero real margin -- confirmed by a live test on
# 2026-08-25 that failed with "0 eligible symbols" for every one of the 250
# symbols. 480 calendar days (~16 months) restores a real margin (~40
# trading days / ~8 weeks) on top of the 278-day minimum, same buffer
# philosophy as vp_swing_screener_daily.py's HISTORY_LOOKBACK_DAYS.
HISTORY_LOOKBACK_DAYS = 480
REQUEST_GAP_SEC = 1.0   # be gentle on the single-eventlet-worker REST API

# Rebalance is MONTHLY per the champion config: decision on the last trading
# day of the calendar month's close, executed at the next trading day's open
# (results_summary.md section 2 -- "next trading day" after a month-end close
# is the 1st trading session of the following month). The scan still runs
# once per trading day, but NOT to surface a continuously-shifting provisional
# signal -- its only job on every non-finalizing day is the cheap check of
# whether today is provably the month's last trading day (or, as catch-up,
# whether the PRIOR month's close was missed -- see run_scan_cycle below).
# The actual top-15% list is computed exactly once per month, the first day
# that check succeeds, and never recomputed afterwards. Offset
# from vp_swing_screener_daily.py's 15:45 SCAN_TIME to avoid both bots
# hammering the REST API at the same instant.
SCAN_TIME = "16:00"

STATE_FILE = LOGS_DIR / "nifty_microcap_screener_state.json"
PID_FILE   = LOGS_DIR / "nifty_microcap_screener.pid"

# Current NIFTY Microcap 250 constituents, static snapshot pulled read-only
# from options_data/data/options_data.duckdb's microcap_constituent_daily
# table on 2026-08-25. See module docstring for why this is embedded rather
# than queried at runtime.
MICROCAP_STOCKS = [
    "AARTIDRUGS", "AARTIPHARM", "ACI", "ADVENZYMES", "AEQUS", "AETHER",
    "AGARWALEYE", "AHLUCONT", "AKUMS", "ALIVUS", "ALKYLAMINE", "ALOKINDS",
    "ANUP", "APLLTD", "APOLLO", "ARVIND", "ARVINDFASN", "ASHAPURMIN",
    "ASHOKA", "ASKAUTOLTD", "ASTRAMICRO", "ATLANTAELE", "AURIONPRO",
    "AVALON", "AVANTIFEED", "AVL", "AWFIS", "AXISCADES", "AZAD",
    "BAJAJELEC", "BALAMINES", "BALUFORGE", "BANCOINDIA", "BBOX",
    "BECTORFOOD", "BIRLACORPN", "BLACKBUCK", "BLUESTONE", "BORORENEW",
    "CAMPUS", "CAPILLARY", "CCAVENUE", "CELLO", "CENTURYPLY", "CERA",
    "CMSINFO", "CORONA", "CRAMC", "CRIZAC", "CSBBANK", "CUPID",
    "DATAMATICS", "DBL", "DBREALTY", "DCBBANK", "DIACABS", "DYNAMATECH",
    "EDELWEISS", "EIEL", "ELECTCAST", "ELLEN", "EMBDL", "EMIL", "ENTERO",
    "EPL", "EQUITASBNK", "ETHOSLTD", "EUREKAFORB", "FEDFINA", "FIEMIND",
    "FINPIPE", "GAEL", "GHCL", "GMMPFAUDLR", "GMRP&UI", "GNFC",
    "GODREJAGRO", "GOKEX", "GOKULAGRO", "GPPL", "GREAVESCOT", "GRINDWELL",
    "GRWRHITECH", "GSFC", "HAPPSTMNDS", "HCC", "HCG", "HEMIPROP",
    "HERITGFOOD", "HGINFRA", "ICIL", "IFBIND", "IIFLCAPS", "IMFA",
    "INDIAGLYCO", "INDIASHLTR", "INDIGOPNTS", "INOXGREEN", "INOXINDIA",
    "IONEXCHANG", "IXIGO", "JAIBALAJI", "JAMNAAUTO", "JAYNECOIND",
    "JKLAKSHMI", "JKPAPER", "JLHL", "JSFB", "JSLL", "JUSTDIAL",
    "JYOTHYLAB", "KANSAINER", "KIRLOSBROS", "KIRLPNU", "KITEX", "KNRCON",
    "KPIGREEN", "KRBL", "KRN", "KSB", "KSCL", "KTKBANK", "LLOYDSENGG",
    "LLOYDSENT", "LOTUSDEV", "LUMAXTECH", "LXCHEM", "MAHSCOOTER",
    "MAHSEAMLES", "MANORAMA", "MANYAVAR", "MARKSANS", "MASTEK", "MEDPLUS",
    "METROPOLIS", "MIDHANI", "MOIL", "MSTCLTD", "MTARTECH", "NAZARA",
    "NEOGEN", "NESCO", "NETWORK18", "NFL", "OPTIEMUS", "ORIENTCEM",
    "ORKLAINDIA", "OSWALPUMPS", "PARAS", "PARKHOSPS", "PCJEWELLER",
    "PGIL", "PICCADIL", "PNCINFRA", "PNGJL", "POWERMECH", "PRAJIND",
    "PRICOLLTD", "PRIVISCL", "PRSMJOHNSN", "PRUDENT", "PTC", "PURVA",
    "QPOWER", "QUESS", "RAIN", "RALLIS", "RATEGAIN", "RATNAMANI",
    "RAYMONDLSL", "RBA", "RCF", "REDTAPE", "REFEX", "RELAXO", "RELIGARE",
    "RENUKA", "ROUTE", "RTNINDIA", "RTNPOWER", "RUBICON", "SAATVIKGL",
    "SAFARI", "SAMHI", "SANDUMA", "SANOFICONR", "SANSERA", "SENCO", "SFL",
    "SHAILY", "SHAKTIPUMP", "SHARDACROP", "SHAREINDIA", "SHILPAMED",
    "SHRIPISTON", "SKFINDIA", "SKFINDUS", "SKIPPER", "SKYGOLD",
    "SMARTWORKS", "SMLMAH", "SOUTHBANK", "SPARC", "STAR", "STARCEMENT",
    "STLTECH", "STYL", "STYRENIX", "SUBROS", "SUDARSCHEM", "SUDEEPPHRM",
    "SUNTECK", "SUPRIYA", "SURYAROSNI", "SWSOLAR", "TANLA", "TARC",
    "TDPOWERSYS", "TEXRAIL", "THANGAMAYL", "THOMASCOOK", "THYROCARE",
    "TI", "TIMETECHNO", "TIPSMUSIC", "TMB", "TRANSRAILL", "TRIVENI",
    "TSFINV", "TVSSCS", "UJJIVANSFB", "UTLSOLAR", "V2RETAIL",
    "VAIBHAVGBL", "VARROC", "VGUARD", "VIKRAMSOLR", "VIPIND", "VIYASH",
    "VMART", "VOLTAMP", "WAAREERTL", "WABAG", "WAKEFIT", "WEBELSOLAR",
    "WELENT", "WESTLIFE", "WEWORK", "YATHARTH", "ZAGGLE",
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
# SIGNAL LOGIC — line-for-line port of stage0_discovery.py / screener.py's
# build_weekly_rebalance_dates(), compute_momentum_panel(), and
# build_target_portfolio(), applied to a REST-fetched panel instead of a
# DuckDB query.
# ══════════════════════════════════════════════════════════════════════════════

def _history_to_frame(raw: list) -> pd.DataFrame:
    """Match vp_swing_screener_daily.py's _history_to_frame() convention.
    Also carries volume (when present) -- used only for the SWP no-turnover
    trim suggestion's liquidity ranking, never for the momentum signal
    itself."""
    if not raw:
        return pd.DataFrame(columns=["date", "close", "volume"])
    hist = pd.DataFrame(raw)
    if "timestamp" in hist.columns:
        idx = (pd.to_datetime(hist["timestamp"], unit="s", utc=True)
               .dt.tz_convert("Asia/Kolkata").dt.tz_localize(None).dt.normalize())
    elif "date" in hist.columns:
        idx = pd.to_datetime(hist["date"]).dt.normalize()
    else:
        return pd.DataFrame(columns=["date", "close", "volume"])
    out = pd.DataFrame({"date": idx, "close": hist["close"].astype(float)})
    out["volume"] = hist["volume"].astype(float) if "volume" in hist.columns else 0.0
    return out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)


def fetch_universe_panel() -> pd.DataFrame:
    """Fetch trailing daily closes for every MICROCAP_STOCKS symbol via the
    OpenAlgo REST API and return a long (date, symbol, close) frame -- the
    live-fetch equivalent of screener.py's load_universe_live()."""
    frames = []
    for symbol in MICROCAP_STOCKS:
        try:
            raw = get_history(API_KEY, symbol, EXCHANGE, "D",
                               duration_days=HISTORY_LOOKBACK_DAYS)
            df = _history_to_frame(raw)
            if not df.empty:
                df["symbol"] = symbol
                frames.append(df)
        except Exception as e:
            logger.exception(f"  {symbol}: history fetch failed: {e}")
        time.sleep(REQUEST_GAP_SEC)

    if not frames:
        return pd.DataFrame(columns=["date", "symbol", "close"])
    return pd.concat(frames, ignore_index=True)


def build_monthly_rebalance_dates(df: pd.DataFrame) -> list[pd.Timestamp]:
    """Line-for-line port of stage2_param_sweep.py's build_monthly_rebalance_dates()
    -- last available trading date in each calendar month. This is the champion
    config's cadence (see TOP_PCT comment above), not stage0_discovery.py's
    weekly build_weekly_rebalance_dates()."""
    all_dates = pd.Series(df["date"].unique()).sort_values()
    tmp = pd.DataFrame({"date": all_dates, "ym": all_dates.dt.to_period("M")})
    return tmp.groupby("ym")["date"].max().sort_values().tolist()


def is_last_trading_day_of_month(d: date) -> bool:
    """Calendar-based month-end proof, ported unchanged from
    nifty50_screener.py: True iff every remaining calendar day in d's month
    (d+1 .. month-end) is a weekend or an NSE holiday, i.e. d is the true
    last trading day of its month. Look-ahead only, so it is safe to call on
    today's date at scan time without tomorrow's data existing yet."""
    month_end = (pd.Timestamp(d) + pd.offsets.MonthEnd(0)).date()
    cursor = d + timedelta(days=1)
    while cursor <= month_end:
        if cursor.weekday() < 5 and not is_market_holiday(API_KEY, cursor.strftime("%Y-%m-%d")):
            return False
        cursor += timedelta(days=1)
    return True


def compute_momentum_panel(df: pd.DataFrame) -> pd.DataFrame:
    """Line-for-line port of stage0_discovery.py's compute_momentum_panel()."""
    wide = df.pivot(index="date", columns="symbol", values="close").sort_index()
    shifted_near = wide.shift(SKIP_DAYS)
    shifted_far = wide.shift(LOOKBACK_DAYS + SKIP_DAYS)
    mom = shifted_near / shifted_far - 1.0
    return mom


def compute_avg_turnover(df: pd.DataFrame, as_of_date: pd.Timestamp,
                          symbols: set[str], window: int = 20) -> pd.Series:
    """Trailing-window average daily rupee turnover (close * volume) per
    symbol, as of as_of_date. Used ONLY to rank held positions by liquidity
    for the SWP no-turnover trim suggestion below -- never for the momentum
    signal itself. Symbols with no volume data fall back to 0 (least liquid,
    so they're never suggested over a symbol we actually have data for)."""
    sub = df[df["symbol"].isin(symbols) & (df["date"] <= as_of_date)]
    if sub.empty:
        return pd.Series(0.0, index=list(symbols))
    sub = sub.sort_values("date").groupby("symbol").tail(window)
    turnover = (sub["close"] * sub["volume"]).groupby(sub["symbol"]).mean()
    return turnover.reindex(list(symbols)).fillna(0.0)


def build_target_portfolio(mom: pd.DataFrame, signal_date: pd.Timestamp,
                            price: pd.Series, capital: float,
                            top_pct: float = TOP_PCT) -> tuple[pd.DataFrame, int, int]:
    """Line-for-line port of screener.py's build_target_portfolio()."""
    row = mom.loc[signal_date].dropna()
    n_eligible = len(row)
    if n_eligible < 10:
        raise ValueError(f"Only {n_eligible} eligible symbols at {signal_date.date()} — need >=10")
    n_top = max(1, int(round(n_eligible * top_pct)))
    ranked = row.sort_values(ascending=False)
    top_syms = list(ranked.index[:n_top])

    target_rupee = capital / n_top
    rows = []
    for sym in top_syms:
        px = price.get(sym, float("nan"))
        if pd.isna(px) or px <= 0:
            continue
        shares = int(target_rupee // px)
        rows.append({
            "symbol": sym,
            "momentum_12m1m_pct": round(ranked[sym] * 100, 2),
            "ref_price": round(float(px), 2),
            "target_rupee": round(target_rupee, 0),
            "shares": shares,
        })
    out = pd.DataFrame(rows).sort_values("momentum_12m1m_pct", ascending=False).reset_index(drop=True)
    return out, n_eligible, n_top


# ── Price anomaly detection (informational only) ────────────────────────────
# Fyers' historical feed already delivers split/bonus-adjusted closes
# (verified empirically 2026-08-25 against CUPID's actual April 2024 and
# March 2026 corporate actions plus TATASTEEL's July 2022 split -- see module
# docstring above), so a flag from this function is NOT evidence the pipeline
# mishandled a split/bonus. It is one of: a genuine large one-day move (the
# common case -- CUPID/STLTECH/MTARTECH/THANGAMAYL in the 2026-08 list all
# checked out this way), a corporate action Fyers does not retroactively
# adjust for (demerger, large special distribution), or a broker data glitch.
# Worth a human glance at that specific name before confirming a fill, not a
# reason to alter the ranking or target list -- this never changes what
# build_target_portfolio() computes, it only surfaces a flag alongside it.
ANOMALY_RATIO_LOW = 0.60
ANOMALY_RATIO_HIGH = 1.67


def detect_price_anomalies(df: pd.DataFrame, window_start: pd.Timestamp,
                            window_end: pd.Timestamp) -> list[dict]:
    """Scan every symbol's daily closes in df across [window_start,
    window_end] (inclusive) for a single-day close ratio outside
    [ANOMALY_RATIO_LOW, ANOMALY_RATIO_HIGH] -- the same threshold
    options_data/research/split_adjustment.py uses to flag split/bonus
    candidates on ITS (unadjusted) data source; reused here purely as a
    general "big one-day move" trip-wire, not as a split detector, since
    this pipeline's data is already adjusted (see module docstring).

    Runs across the FULL fetched universe, not just the top-15% cut that
    made this month's target list -- a distortion in a near-miss name is
    caught too. df must have columns date, symbol, close. Returns one dict
    per flagged day (symbol, date, prev_close, close, ratio_pct), sorted by
    symbol then date."""
    win = df[(df["date"] >= window_start) & (df["date"] <= window_end)].copy()
    win = win.sort_values(["symbol", "date"])
    win["prev_close"] = win.groupby("symbol")["close"].shift(1)
    win["ratio"] = win["close"] / win["prev_close"]
    flagged = win[(win["ratio"] < ANOMALY_RATIO_LOW) | (win["ratio"] > ANOMALY_RATIO_HIGH)].dropna()
    out = [
        {
            "symbol": r["symbol"],
            "date": r["date"].date().isoformat(),
            "prev_close": round(float(r["prev_close"]), 2),
            "close": round(float(r["close"]), 2),
            "ratio_pct": round((float(r["ratio"]) - 1) * 100, 1),
        }
        for _, r in flagged.iterrows()
    ]
    return sorted(out, key=lambda a: (a["symbol"], a["date"]))


# ══════════════════════════════════════════════════════════════════════════════
# SCAN CYCLE
#
# Rebalance is a cross-sectional, calendar-month event, not a daily one -- so
# the scan does NOT treat "today's" running signal as the actionable list
# (that was the earlier bug: a continuously-shifting provisional list invites
# acting on a still-incomplete month). Instead each daily scan checks whether
# the PRIOR calendar month's rebalance date can now be proven final -- i.e.
# whether a later trading day, in a newer month, has already appeared in the
# fetched panel. The moment that's true, that prior month's top-15% list is
# computed once, persisted forever to rebalance_db.monthly_target_lists, and
# never recomputed or overwritten again. Everything the dashboard shows and
# every "Confirm"/"Confirm Exit" action acts against is sourced from that
# permanent record and from rebalance_db.positions (the manually-confirmed
# open/closed ledger) -- not from a daily-shifting number.
# ══════════════════════════════════════════════════════════════════════════════

def run_scan_cycle() -> None:
    logger.info(f"Fetching daily history for {len(MICROCAP_STOCKS)} symbols "
                f"({HISTORY_LOOKBACK_DAYS}-day lookback) ...")
    df = fetch_universe_panel()
    if df.empty:
        logger.error("No history fetched for any symbol -- aborting scan cycle")
        return

    reb_dates = build_monthly_rebalance_dates(df)
    if not reb_dates:
        logger.error("No rebalance dates derivable from fetched history -- aborting")
        return

    rebalance_db.ensure_capital_seeded(SCREEN_CAPITAL)
    book_value = rebalance_db.get_book_value()

    mom = compute_momentum_panel(df)
    wide_close = df.pivot(index="date", columns="symbol", values="close").sort_index()

    # ── Finalize the month's list, once and only once ───────────────────────
    # Primary path (2026-09-28, same as nifty50_screener.py): reb_dates[-1]
    # is today's bar; if is_last_trading_day_of_month() proves every later
    # day this month is a weekend/NSE holiday, today IS the month's close and
    # is finalized this same evening -- so the list exists before the next
    # session's open, the validated execution point. The old data-proof path
    # only finalized once a bar in the FOLLOWING month appeared (2026-09's
    # list landed 2026-09-01 14:56, after the open it was meant for).
    # Catch-up path: reb_dates[-2] is still finalized if it was missed (e.g.
    # the launcher wasn't running on the month's last trading day) --
    # a newer month's bar proves it final. has_target_list_for() keeps both
    # paths idempotent.
    finalize_candidates = []
    if len(reb_dates) >= 2:
        finalize_candidates.append(reb_dates[-2])
    if reb_dates and is_last_trading_day_of_month(reb_dates[-1].date()):
        finalize_candidates.append(reb_dates[-1])
    for finalized_date in finalize_candidates:
        finalized_date_str = finalized_date.date().isoformat()
        if not rebalance_db.has_target_list_for(finalized_date_str):
            if finalized_date in mom.index:
                try:
                    f_target, f_n_eligible, f_n_top = build_target_portfolio(
                        mom, finalized_date, wide_close.loc[finalized_date], book_value)
                    rebalance_month = (finalized_date + pd.offsets.MonthBegin(1)).strftime("%Y-%m")
                    rebalance_db.record_monthly_target_list(
                        rebalance_month=rebalance_month,
                        signal_date=finalized_date_str,
                        rows=f_target.to_dict("records"),
                        n_eligible=f_n_eligible, n_top=f_n_top,
                    )

                    # Universe-wide price-anomaly scan over this month's own
                    # 12-1 momentum window (same trading-day offsets
                    # compute_momentum_panel used) -- informational only,
                    # see detect_price_anomalies() docstring.
                    all_dates = wide_close.index
                    loc = all_dates.get_loc(finalized_date)
                    start_loc = max(0, loc - (LOOKBACK_DAYS + SKIP_DAYS))
                    window_start = all_dates[start_loc]
                    anomalies = detect_price_anomalies(df, window_start, finalized_date)
                    if anomalies:
                        rebalance_db.record_price_anomalies(
                            rebalance_month=rebalance_month,
                            signal_date=finalized_date_str,
                            anomalies=anomalies,
                        )
                        logger.warning(f"Price anomaly scan: {len(anomalies)} flagged single-day "
                                        f"move(s) across the {len(MICROCAP_STOCKS)}-symbol universe "
                                        f"in the {rebalance_month} momentum window -- see "
                                        f"rebalance_db.price_anomalies")
                    else:
                        logger.info(f"Price anomaly scan: 0 flagged moves across the "
                                    f"{len(MICROCAP_STOCKS)}-symbol universe in the "
                                    f"{rebalance_month} momentum window")

                    logger.info(f"FINALIZED month-end list: rebalance_month={rebalance_month} "
                                f"signal_date={finalized_date_str} n_top={f_n_top}")
                    notify_finalized_list(
                        "NIFTY Microcap Screener", rebalance_month, finalized_date_str,
                        f_target.to_dict("records"), rebalance_db.list_open_positions(),
                        top_label="top-15%",
                    )
                except ValueError as e:
                    logger.error(f"Finalized target portfolio build failed: {e}")
            else:
                logger.warning(f"Finalized signal date {finalized_date_str} not in momentum "
                                f"panel -- skipping finalization this cycle")

    # ── Sync dashboard state from the permanent record ───────────────────────
    latest = rebalance_db.get_latest_target_list()
    open_positions = rebalance_db.list_open_positions()

    state = _load_state()
    state["open_positions"] = open_positions
    state["book_value"] = rebalance_db.get_book_value()
    state["swp_amount"] = SWP_MONTHLY_AMOUNT
    state["capital_ledger"] = rebalance_db.list_capital_ledger()
    state["last_scan"] = datetime.now().isoformat(timespec="seconds")

    if latest:
        target_symbols = {c["symbol"] for c in latest["candidates"]}
        held_symbols = {p["symbol"] for p in open_positions}
        state["finalized_target"] = latest
        state["candidates"] = latest["candidates"]
        state["signal_date"] = latest["signal_date"]
        state["n_eligible"] = latest["n_eligible"]
        state["n_top"] = latest["n_top"]
        state["exit_candidates"] = [
            p for p in open_positions if p["symbol"] not in target_symbols
        ]
        state["price_anomalies"] = rebalance_db.get_anomalies_for(latest["rebalance_month"])
        # Never due on the screener's very first-ever finalized month: that
        # cycle is the initial buy-in (no capital has been deployed yet to
        # draw a withdrawal from), so has_withdrawal_for() being False there
        # is expected, not something to flag. SWP only becomes meaningful
        # from the SECOND finalized month onward, once a live portfolio has
        # actually existed for a full cycle.
        is_first_ever_cycle = len(rebalance_db.list_target_list_history()) <= 1
        state["swp_due"] = (not is_first_ever_cycle) and \
            (not rebalance_db.has_withdrawal_for(latest["rebalance_month"]))

        # SWP overdue warning: months (other than the current one) whose list
        # already finalized with no withdrawal ever recorded against them --
        # i.e. an SWP that was skipped, not just "not yet confirmed this
        # month". Purely informational; nothing here withdraws or trims
        # automatically -- see module docstring's manual-confirm philosophy.
        pending_swp_months = rebalance_db.list_pending_swp_months()
        state["swp_overdue_months"] = [
            m for m in pending_swp_months if m != latest["rebalance_month"]
        ]

        # No-turnover trim suggestion: when this month's SWP is still
        # unconfirmed and there are no exit candidates to fund it from
        # (build_target_portfolio() never resizes continuing holdings, so a
        # zero-turnover month has no natural sell-proceeds source for the
        # withdrawal -- see conversation with the user, 2026-08-31), suggest
        # the single most-liquid held position (trailing 20-day avg rupee
        # turnover) as the trim candidate, tie-broken by weakest current
        # momentum. Advisory text only -- the user decides and executes the
        # actual trim manually, same as every other trade in this bot.
        state["suggested_trim_symbol"] = None
        if state["swp_due"] and not state["exit_candidates"] and held_symbols:
            signal_ts = pd.Timestamp(latest["signal_date"])
            liquidity = compute_avg_turnover(df, signal_ts, held_symbols)
            cur_mom = mom.loc[signal_ts] if signal_ts in mom.index else pd.Series(dtype=float)
            held_list = [p for p in open_positions if p["symbol"] in held_symbols]
            held_list.sort(key=lambda p: (
                -liquidity.get(p["symbol"], 0.0),
                cur_mom.get(p["symbol"], float("inf")),
            ))
            if held_list:
                state["suggested_trim_symbol"] = held_list[0]["symbol"]

        new_entries = target_symbols - held_symbols
        logger.info(f"Scan complete: latest finalized rebalance_month={latest['rebalance_month']} "
                    f"signal_date={latest['signal_date']}, {latest['n_top']} in top-15%, "
                    f"{len(new_entries)} not yet held, {len(state['exit_candidates'])} "
                    f"held position(s) fell out of top-15%, book_value=Rs.{state['book_value']:,.0f}")
    else:
        state["finalized_target"] = None
        state["candidates"] = []
        state["exit_candidates"] = []
        state["price_anomalies"] = []
        state["signal_date"] = None
        state["swp_due"] = False
        state["swp_overdue_months"] = []
        state["suggested_trim_symbol"] = None
        logger.info("Scan complete: no month-end has been finalized yet "
                     "(need a trading day in a later month to confirm one)")

    _save_state(state)


def confirm_exit_trade(pos: dict, exit_price: float, exit_reason: str) -> None:
    """Log a manually-confirmed exit to the shared performance.db. Called
    from the dashboard's "Confirm Exit" handler, never from this process's
    own scan loop -- there is no automatic exit for this strategy, see
    module docstring."""
    entry_price = float(pos.get("entry_price") or 0)
    qty = int(pos.get("qty") or 0)
    if entry_price <= 0 or qty <= 0:
        return
    log_trade(
        bot_name=BOT_NAME,
        strategy_type="equity",
        instrument=pos.get("symbol", ""),
        symbol=pos.get("symbol", ""),
        entry_time=pos.get("since"),
        exit_time=datetime.now(),
        entry_price=entry_price,
        exit_price=exit_price,
        exit_reason=exit_reason,
        quantity=qty,
        gross_pnl=(exit_price - entry_price) * qty,
        direction="long",
        source="live",
        notes="NIFTY Microcap Screener — manual confirm, signal-only bot",
    )


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP — wake every 20s, fire one scan per trading day at/after SCAN_TIME
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    _acquire_pid_lock()
    rebalance_db.ensure_capital_seeded(SCREEN_CAPITAL)
    logger.info(f"NIFTY Microcap Screener starting | universe={len(MICROCAP_STOCKS)} stocks | "
                f"scan={SCAN_TIME} IST | book_value=Rs.{rebalance_db.get_book_value():,.0f} "
                f"(NAV-style, SWP=Rs.{SWP_MONTHLY_AMOUNT:,.0f}/mo) | "
                f"SIGNAL-ONLY, no order placement")

    scanned_today = False
    current_date = date.today()

    while True:
        now = datetime.now()
        if now.date() != current_date:
            current_date = now.date()
            scanned_today = False

        if now.weekday() < 5:
            hhmm = now.strftime("%H:%M")

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

        time.sleep(20)


if __name__ == "__main__":
    main()
