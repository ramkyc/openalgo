#!/usr/bin/env python3
"""
live_trading/performance_review.py
====================================
Bot performance review — reads from live_trading/logs/performance.db
and prints a rich terminal report.

Usage:
    uv run live_trading/performance_review.py              # last 30 days, all bots
    uv run live_trading/performance_review.py --days 7
    uv run live_trading/performance_review.py --days 90
    uv run live_trading/performance_review.py --bot ha_options_bot
    uv run live_trading/performance_review.py --from 2026-04-01 --to 2026-04-17
    uv run live_trading/performance_review.py --bot ha_options_bot --days 60
    uv run live_trading/performance_review.py --recent 20
    uv run live_trading/performance_review.py --notify
    uv run live_trading/performance_review.py --days 1 --notify
    uv run live_trading/performance_review.py --reconcile
    uv run live_trading/performance_review.py --reconcile --dry-run
    uv run live_trading/performance_review.py /?

Switches:
  --days N          Look-back window in calendar days (default: 30).
  --bot BOT_NAME    Filter report to a single bot. Use the internal bot name,
                    e.g. ha_options_bot, banknifty_bb_options_bot.
  --from YYYY-MM-DD Start date for the reporting period (overrides --days).
  --to   YYYY-MM-DD End date for the reporting period (default: today).
  --recent N        Number of recent trades to show in the trades table (default: 15).
  --no-chart        Skip the daily P&L bar chart section.
  --no-reasons      Skip the exit reason breakdown section.
  --notify          Send a Telegram summary after printing the report.
                    Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env.
  --reconcile       Scan all bot state files against the live OpenAlgo
                    positionbook. Any position the broker shows as flat but
                    the bot never logged gets written to performance.db
                    as a synthetic exit (exit_reason = "state stale (broker: flat)").
                    Requires OpenAlgo to be running (positionbook API call).
                    After reconciliation the normal report is printed.
  --dry-run         Used with --reconcile. Prints what would be written
                    without actually modifying the DB.
  /?  or  /help     Windows-style alias for --help.

Sections printed:
  1. Portfolio Summary          — overall numbers across all bots in period
  2. Per-Bot Breakdown          — one row per bot with trades / WR / P&L / avg
  3. Stage 11 Gate Progress     — session count vs 20-session target
  4. Daily P&L Bar Chart        — last 14 days (or full period if shorter)
  5. Exit Reason Breakdown      — win/loss counts by exit type
  6. Recent Trades              — last --recent trades (default 15)

Why --reconcile exists:
  When a bot's position is closed externally (broker risk action, manual
  close, or crash during the exit order) the bot never calls _close_trade(),
  so log_trade_to_db() is never called.  The streamlit dashboard detects
  this via "state stale (broker: flat)" but performance.db stays blind
  to it, causing a large positive bias in the P&L report.
  --reconcile fixes this by replaying those exits from the positionbook.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, date, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live_trading.shared.performance_db import get_db_path

# ── Env / Telegram ────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass  # dotenv optional — env vars may already be set

_TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")
_TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_inr(val: float | None, sign: bool = True) -> str:
    """Format a rupee amount with ₹ prefix and commas."""
    if val is None:
        return "—"
    s = f"₹{abs(val):,.0f}"
    if sign:
        return f"+{s}" if val >= 0 else f"-{s}"
    return s


def _pct(val: float | None, decimals: int = 1) -> str:
    if val is None:
        return "—"
    return f"{val:.{decimals}f}%"


def _bar(value: float, max_value: float, width: int = 20, positive: bool = True) -> str:
    """Return an ASCII progress bar."""
    if max_value == 0:
        return " " * width
    filled = int(abs(value) / max_value * width)
    filled = min(filled, width)
    char = "█" if positive else "░"
    return char * filled + " " * (width - filled)


def _stage11_bar(sessions: int, target: int = 20, width: int = 16) -> str:
    filled = int(sessions / target * width)
    filled = min(filled, width)
    return "█" * filled + "░" * (width - filled)


# ── Bot metadata ──────────────────────────────────────────────────────────────

BOT_META = {
    "ha_options_bot":              {"label": "HA Options Bot",          "type": "Options", "universe": "NIFTY/BNF/SENSEX"},
    "nifty_trend_seller_bot":      {"label": "Nifty Trend Seller",      "type": "Options", "universe": "NIFTY"},
    "sensex_trend_seller_bot":     {"label": "SENSEX Trend Seller",     "type": "Options", "universe": "SENSEX"},
    "htf_po3_bot":                 {"label": "HTF PO3 Bot",             "type": "Options", "universe": "NIFTY/BANKNIFTY"},
    "banknifty_bb_options_bot":    {"label": "BANKNIFTY BB Options",    "type": "Options", "universe": "BANKNIFTY"},
    "nifty_bb_overbought_bot":     {"label": "Nifty BB Overbought",     "type": "Options", "universe": "NIFTY"},
    "nifty_macd_map_bot":          {"label": "NIFTY MACD Map",          "type": "Options", "universe": "NIFTY"},
    "nifty_eod_hold_bot":          {"label": "NIFTY EOD Hold",          "type": "Options", "universe": "NIFTY"},
    "iron_fly_weekly_bot":         {"label": "NIFTY Iron Fly Weekly",   "type": "Options", "universe": "NIFTY"},
    "sensex_iron_fly_weekly_bot":  {"label": "SENSEX Iron Fly Weekly",  "type": "Options", "universe": "SENSEX"},
    "banknifty_iron_fly_monthly_bot": {"label": "BANKNIFTY Iron Fly Monthly", "type": "Options", "universe": "BANKNIFTY"},
    "flat_blue_line_monthly_bot":     {"label": "Flat Blue Line Monthly",    "type": "Options", "universe": "NIFTY+BANKNIFTY"},
    "preopen_gap_fade_bot":        {"label": "Pre-Open Gap Fade",       "type": "Equity",  "universe": "NIFTY50"},
    # "equity_obi" RETIRED 2026-06-19 — WR 38.3%, P&L −₹4,122 over 149 trades / 23 sessions
    # "equity_obi":                  {"label": "Equity OBI Bot",          "type": "Equity",  "universe": "RELIANCE/HDFCBANK"},
    # "gap_fade_eod_bot":          RETIRED 2026-06-04 — WR 25%, P&L −₹7,739 over 4 trades (May–Jun 2026)
    # "ema_swing_scanner":         RETIRED 2026-06-04 — WR 0%, P&L −₹8,820 over 2 trades; no formal research study
    # NTS_OBI is the STRATEGY_NAME used inside nifty_trend_seller_obi_bot.py — must match exactly
    "NTS_OBI":                     {"label": "NTS + OBI Gate",          "type": "Options", "universe": "NIFTY"},
    "macd_m2_sell_options_bot":    {"label": "MACD M2 Sell Options",    "type": "Options", "universe": "NIFTY/BANKNIFTY"},
}

# Stage 11 target: 20 sessions
STAGE11_TARGET = 20


# ── Data fetchers ─────────────────────────────────────────────────────────────

def _connect(db_path: Path):
    """Return a SQLite connection or raise."""
    import sqlite3
    if not db_path.exists():
        raise FileNotFoundError(
            f"Performance DB not found at {db_path}\n"
            "Run at least one bot session first, or run import_historical_trades.py."
        )
    con = sqlite3.connect(str(db_path), timeout=30, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    return con


def _date_filter(start: date, end: date) -> tuple[str, list]:
    # Pass ISO strings, not date objects, to avoid the Python 3.12 deprecation
    # warning for the built-in sqlite3 date adapter.
    return "trade_date BETWEEN ? AND ?", [start.isoformat(), end.isoformat()]


def fetch_portfolio_summary(con, start: date, end: date, bot_filter: str | None = None) -> dict:
    where, params = _date_filter(start, end)
    if bot_filter:
        where += " AND bot_name = ?"
        params.append(bot_filter)
    row = con.execute(
        f"""
        SELECT
            COUNT(*)                                 AS total_trades,
            SUM(CASE WHEN won THEN 1 ELSE 0 END)     AS wins,
            SUM(gross_pnl)                           AS gross_pnl,
            AVG(gross_pnl)                           AS avg_pnl,
            COUNT(DISTINCT bot_name)                 AS bot_count,
            COUNT(DISTINCT trade_date)               AS trading_days,
            MIN(trade_date)                          AS first_date,
            MAX(trade_date)                          AS last_date
        FROM trades
        WHERE {where}
        """,
        params,
    ).fetchone()
    if not row or row[0] == 0:
        return {}
    total, wins, gpnl, apnl, bots, days, fd, ld = row
    # Best and worst single day (across all bots combined)
    daily = con.execute(
        f"""
        SELECT trade_date, SUM(gross_pnl) AS day_pnl
        FROM trades WHERE {where}
        GROUP BY trade_date ORDER BY day_pnl DESC
        """,
        params,
    ).fetchall()
    best_day = daily[0]  if daily else None
    worst_day = daily[-1] if daily else None
    return {
        "total_trades":  total,
        "wins":          wins or 0,
        "gross_pnl":     gpnl,
        "avg_pnl":       apnl,
        "bot_count":     bots,
        "trading_days":  days,
        "first_date":    fd,
        "last_date":     ld,
        "best_day":      best_day,
        "worst_day":     worst_day,
        "daily_rows":    daily,
    }


def fetch_per_bot(con, start: date, end: date, bot_filter: str | None = None) -> list[dict]:
    where, params = _date_filter(start, end)
    if bot_filter:
        where += " AND bot_name = ?"
        params.append(bot_filter)

    rows = con.execute(
        f"""
        SELECT
            bot_name,
            COUNT(*)                                  AS total_trades,
            SUM(CASE WHEN won THEN 1 ELSE 0 END)      AS wins,
            SUM(gross_pnl)                            AS gross_pnl,
            AVG(gross_pnl)                            AS avg_pnl,
            MAX(gross_pnl)                            AS best_trade,
            MIN(gross_pnl)                            AS worst_trade,
            AVG(hold_duration_mins)                   AS avg_hold,
            AVG(decay_pct)                            AS avg_decay,
            MAX(trade_date)                           AS last_trade,
            strategy_type
        FROM trades
        WHERE {where}
        GROUP BY bot_name, strategy_type
        ORDER BY gross_pnl DESC NULLS LAST
        """,
        params,
    ).fetchall()

    result = []
    for r in rows:
        (bn, total, wins, gpnl, apnl, best, worst, ahold, adecay, last_dt, stype) = r
        wr = (wins / total * 100) if total else None
        result.append({
            "bot_name":    bn,
            "total":       total,
            "wins":        wins or 0,
            "losses":      total - (wins or 0),
            "win_rate":    wr,
            "gross_pnl":   gpnl,
            "avg_pnl":     apnl,
            "best_trade":  best,
            "worst_trade": worst,
            "avg_hold":    ahold,
            "avg_decay":   adecay,
            "last_trade":  last_dt,
            "type":        stype,
        })
    return result


def fetch_stage11_sessions(con) -> dict[str, int]:
    """Return {bot_name: session_count} from trading_sessions table (all time)."""
    try:
        rows = con.execute(
            """
            SELECT bot_name, COUNT(*) AS sessions
            FROM trading_sessions
            GROUP BY bot_name
            """
        ).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


def fetch_daily_pnl(con, start: date, end: date, bot_filter: str | None = None) -> list[tuple]:
    """Returns list of (trade_date, total_pnl, trade_count) ordered by date."""
    where, params = _date_filter(start, end)
    if bot_filter:
        where += " AND bot_name = ?"
        params.append(bot_filter)
    return con.execute(
        f"""
        SELECT trade_date, SUM(gross_pnl) AS day_pnl, COUNT(*) AS trade_count
        FROM trades WHERE {where}
        GROUP BY trade_date ORDER BY trade_date
        """,
        params,
    ).fetchall()


def fetch_exit_reasons(con, start: date, end: date, bot_filter: str | None = None) -> list[tuple]:
    where, params = _date_filter(start, end)
    if bot_filter:
        where += " AND bot_name = ?"
        params.append(bot_filter)
    return con.execute(
        f"""
        SELECT
            exit_reason,
            COUNT(*)                              AS total,
            SUM(CASE WHEN won THEN 1 ELSE 0 END)  AS wins,
            SUM(gross_pnl)                        AS pnl
        FROM trades WHERE {where}
        GROUP BY exit_reason ORDER BY total DESC
        """,
        params,
    ).fetchall()


def fetch_recent_trades(con, n: int, start: date, end: date,
                        bot_filter: str | None = None) -> list[tuple]:
    where, params = _date_filter(start, end)
    if bot_filter:
        where += " AND bot_name = ?"
        params.append(bot_filter)
    return con.execute(
        f"""
        SELECT entry_time, bot_name, instrument, symbol,
               option_type, entry_price, exit_price,
               gross_pnl, exit_reason, won, hold_duration_mins
        FROM trades
        WHERE {where}
        ORDER BY entry_time DESC NULLS LAST
        LIMIT {n}
        """,
        params,
    ).fetchall()


# ── Rendering helpers ─────────────────────────────────────────────────────────

def _col(text: str, width: int, align: str = "left") -> str:
    text = str(text)
    if len(text) > width:
        text = text[:width - 1] + "…"
    if align == "right":
        return text.rjust(width)
    if align == "center":
        return text.center(width)
    return text.ljust(width)


def _divider(widths: list[int], char: str = "─") -> str:
    return "─┼─".join(char * w for w in widths)


def _header_row(cols: list[tuple[str, int, str]]) -> str:
    return " │ ".join(_col(label, w, align) for label, w, align in cols)


def _data_row(values: list, cols: list[tuple[str, int, str]]) -> str:
    return " │ ".join(_col(str(v), w, align) for v, (_, w, align) in zip(values, cols))


# ── Print sections ────────────────────────────────────────────────────────────

SEPARATOR = "═" * 80


def print_header(start: date, end: date, bot_filter: str | None):
    print()
    print(SEPARATOR)
    title = "  OPENALGO — BOT PERFORMANCE REVIEW"
    period = f"  {start.strftime('%d %b %Y')} → {end.strftime('%d %b %Y')}"
    if bot_filter:
        period += f"  [filter: {bot_filter}]"
    print(title)
    print(period)
    print(SEPARATOR)


def print_portfolio_summary(summary: dict):
    if not summary:
        print("\n  ⚠  No trades found in the selected period.\n")
        return

    total   = summary["total_trades"]
    wins    = summary["wins"]
    wr      = wins / total * 100 if total else 0
    gpnl    = summary["gross_pnl"] or 0
    days    = summary["trading_days"] or 1
    avg_day = gpnl / days

    print("\n┌─ PORTFOLIO SUMMARY " + "─" * 59 + "┐")

    best_day_str  = ""
    worst_day_str = ""
    if summary.get("best_day"):
        bd = summary["best_day"]
        best_day_str = f"  Best day:   {_fmt_inr(bd[1])} ({bd[0]})"
    if summary.get("worst_day"):
        wd = summary["worst_day"]
        worst_day_str = f"  Worst day:  {_fmt_inr(wd[1])} ({wd[0]})"

    lines = [
        f"  Total trades : {total:>5}         Active bots : {summary['bot_count']}",
        f"  Win rate     : {wr:>5.1f}%        Trading days: {summary['trading_days']}",
        f"  Gross P&L    : {_fmt_inr(gpnl, sign=True):>12}   Avg per day : {_fmt_inr(avg_day)}",
        best_day_str,
        worst_day_str,
    ]
    for l in lines:
        if l:
            print(f"│ {l:<78}│")
    print("└" + "─" * 79 + "┘")


def print_per_bot(bots: list[dict]):
    if not bots:
        return
    print("\n┌─ PER-BOT BREAKDOWN " + "─" * 59 + "┐")

    COLS = [
        ("Bot",         28, "left"),
        ("Type",         7, "left"),
        ("Trades",       6, "right"),
        ("Win%",         5, "right"),
        ("Gross P&L",   12, "right"),
        ("Avg/Trade",   11, "right"),
        ("Best",        10, "right"),
        ("Worst",       10, "right"),
    ]

    header = " │ ".join(_col(label, w, align) for label, w, align in COLS)
    div    = "─┼─".join("─" * w for _, w, _ in COLS)

    print(f"│ {header} │")
    print(f"├─{div}─┤")

    for b in bots:
        bn    = b["bot_name"]
        label = BOT_META.get(bn, {}).get("label", bn)
        stype = (b["type"] or "").capitalize()[:7]
        wr    = f"{b['win_rate']:.0f}%" if b["win_rate"] is not None else "—"
        gpnl  = _fmt_inr(b["gross_pnl"])
        apnl  = _fmt_inr(b["avg_pnl"])
        best  = _fmt_inr(b["best_trade"])
        worst = _fmt_inr(b["worst_trade"])

        row = [label, stype, b["total"], wr, gpnl, apnl, best, worst]
        line = " │ ".join(_col(str(v), w, align) for v, (_, w, align) in zip(row, COLS))
        print(f"│ {line} │")

    print("└" + "─" * 97 + "┘")


def print_stage11(sessions: dict[str, int], bots: list[dict]):
    """Show Stage 11 gate progress for all bots that appear in the period."""
    if not sessions and not bots:
        return

    # Collect all known bot names from both period bots and session tracker
    known = {b["bot_name"] for b in bots} | set(sessions.keys())
    # Filter to active trading bots only (skip infra)
    active = [bn for bn in known if bn in BOT_META]
    if not active:
        return

    print("\n┌─ STAGE 11 GATE PROGRESS (target: 20 sessions) " + "─" * 31 + "┐")
    print(f"│  {'Bot':<30} {'Sessions':>9}  {'Progress Bar':<18}  {'Status':<22} │")
    print(f"│  {'─'*30}─{'─'*9}──{'─'*18}──{'─'*22} │")

    for bn in sorted(active):
        label = BOT_META.get(bn, {}).get("label", bn)
        s = sessions.get(bn, 0)
        bar = _stage11_bar(s, STAGE11_TARGET)
        if s >= STAGE11_TARGET:
            status = "✅ Gate PASSED"
        elif s >= 15:
            status = f"🟡 {s}/20 — Nearly there"
        elif s >= 10:
            status = f"🔬 {s}/20 — In progress"
        else:
            status = f"🔬 {s}/20 — Early stage"
        print(f"│  {label:<30} {s:>4}/{STAGE11_TARGET:<4}  {bar:<18}  {status:<22} │")

    print("└" + "─" * 79 + "┘")


def print_daily_chart(daily: list[tuple]):
    if not daily:
        return

    # Show last 20 days max
    rows = daily[-20:]
    max_abs = max(abs(r[1] or 0) for r in rows) if rows else 1
    if max_abs == 0:
        max_abs = 1

    BAR_WIDTH = 24

    print("\n┌─ DAILY P&L — ALL BOTS COMBINED " + "─" * 46 + "┐")
    print(f"│  {'Date':<10}  {'Bar':<{BAR_WIDTH}}  {'P&L':>12}  {'Trades':>6} │")
    print(f"│  {'─'*10}  {'─'*BAR_WIDTH}  {'─'*12}  {'─'*6} │")

    for trade_date, day_pnl, trade_count in rows:
        day_pnl = day_pnl or 0
        pos     = day_pnl >= 0
        bar_str = _bar(day_pnl, max_abs, BAR_WIDTH, positive=pos)
        pnl_str = _fmt_inr(day_pnl, sign=True)
        dt_str  = trade_date.strftime("%d %b") if hasattr(trade_date, "strftime") else str(trade_date)
        bar_display = (bar_str if pos else ("░" * int(abs(day_pnl) / max_abs * BAR_WIDTH))).ljust(BAR_WIDTH)
        print(f"│  {dt_str:<10}  {bar_display}  {pnl_str:>12}  {trade_count:>6} │")

    print("└" + "─" * 66 + "┘")


def print_exit_reasons(reasons: list[tuple]):
    if not reasons:
        return
    print("\n┌─ EXIT REASON BREAKDOWN " + "─" * 55 + "┐")
    print(f"│  {'Exit Reason':<28} {'Count':>5}  {'Wins':>5}  {'Win%':>5}  {'Gross P&L':>12} │")
    print(f"│  {'─'*28} {'─'*5}  {'─'*5}  {'─'*5}  {'─'*12} │")
    for reason, total, wins, pnl in reasons:
        wr  = f"{wins/total*100:.0f}%" if total else "—"
        reason_str = (reason or "unknown")[:28]
        print(f"│  {reason_str:<28} {total:>5}  {wins or 0:>5}  {wr:>5}  {_fmt_inr(pnl):>12} │")
    print("└" + "─" * 66 + "┘")


def print_recent_trades(trades: list[tuple]):
    if not trades:
        return
    print("\n┌─ RECENT TRADES " + "─" * 63 + "┐")

    COLS = [
        ("Date/Time", 14, "left"),
        ("Bot",       20, "left"),
        ("Symbol",    20, "left"),
        ("P&L",       12, "right"),
        ("Exit Reason", 16, "left"),
        ("Hold",      6, "right"),
    ]
    header = " │ ".join(_col(label, w, align) for label, w, align in COLS)
    div    = "─┼─".join("─" * w for _, w, _ in COLS)
    print(f"│ {header} │")
    print(f"├─{div}─┤")

    for row in trades:
        # Expected row from fetch_recent_trades:
        # 0: entry_time, 1: bot_name, 2: instrument, 3: symbol, 4: option_type, 
        # 5: entry_price, 6: exit_price, 7: gross_pnl, 8: exit_reason, 9: won, 10: hold_duration_mins
        
        raw_ts      = str(row[0])
        bot_name    = str(row[1])
        symbol      = str(row[3])
        instrument  = str(row[2])
        gross_pnl   = row[7]
        exit_reason = str(row[8])
        won         = row[9]
        hold_mins   = row[10]

        # Manual Robust Parsing: "2026-05-15 09:30:00" -> "15 May 09:30"
        months = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        dt_str = raw_ts
        try:
            if '-' in raw_ts and len(raw_ts) >= 10:
                y = raw_ts[0:4]
                m = int(raw_ts[5:7])
                d = raw_ts[8:10]
                time_part = raw_ts[11:16] if len(raw_ts) >= 16 else ""
                dt_str = f"{d} {months[m]} {time_part}".strip()
        except:
            dt_str = raw_ts[:14]

        bn_lbl  = BOT_META.get(bot_name, {}).get("label", bot_name)
        sym_str = symbol if (symbol and symbol != 'None') else (instrument if (instrument and instrument != 'None') else "—")
        pnl_val = _fmt_inr(gross_pnl)
        rsn_str = (exit_reason if (exit_reason and exit_reason != 'None') else "—")[:16]
        hold_str = f"{int(hold_mins)}m" if hold_mins else "—"

        win_flag = "🟢" if won else "🔴"
        pnl_display = f"{win_flag} {pnl_val}"
        
        values = [dt_str, bn_lbl, sym_str, pnl_display, rsn_str, hold_str]
        line = " │ ".join(_col(str(v), w, align) for v, (_, w, align) in zip(values, COLS))
        print(f"│ {line} │")

    print("└" + "─" * 99 + "┘")


def print_footer(db_path: Path):
    print()
    print(f"  DB: {db_path}")
    print(f"  Generated: {datetime.now().strftime('%d %b %Y %H:%M:%S')}")
    print()


# ── Telegram notification ─────────────────────────────────────────────────────

def compose_telegram_message(
    start: date,
    end: date,
    summary: dict,
    bots: list[dict],
    sessions: dict[str, int],
    bot_filter: str | None,
) -> str:
    """Build a concise Telegram summary from already-fetched data."""
    lines: list[str] = []

    period_str = f"{start.strftime('%d %b')} → {end.strftime('%d %b %Y')}"
    if bot_filter:
        lines.append(f"📊 *PERFORMANCE REVIEW* — {period_str} \\[{bot_filter}\\]")
    else:
        lines.append(f"📊 *PERFORMANCE REVIEW* — {period_str}")
    lines.append("")

    # Portfolio summary
    if summary:
        total = summary["total_trades"] or 0
        wins  = summary["wins"] or 0
        wr    = wins / total * 100 if total else 0
        gpnl  = summary["gross_pnl"] or 0
        days  = summary["trading_days"] or 1
        sign  = "+" if gpnl >= 0 else ""
        lines.append(
            f"💰 *Fleet total*: {total} trades | WR {wr:.0f}% | "
            f"Gross {sign}₹{gpnl:,.0f} over {days}d"
        )
        if summary.get("best_day"):
            bd = summary["best_day"]
            lines.append(f"   Best day: +₹{bd[1]:,.0f} ({bd[0]})")
        if summary.get("worst_day"):
            wd = summary["worst_day"]
            lines.append(f"   Worst day: ₹{wd[1]:,.0f} ({wd[0]})")
    else:
        lines.append("⚠️ No trades in selected period.")

    # Per-bot breakdown
    if bots:
        lines.append("")
        lines.append("*Per-bot breakdown:*")
        for b in bots:
            bn    = b["bot_name"]
            label = BOT_META.get(bn, {}).get("label", bn)
            wr_s  = f"{b['win_rate']:.0f}%" if b["win_rate"] is not None else "—"
            gpnl  = b["gross_pnl"] or 0
            sign  = "+" if gpnl >= 0 else ""
            lines.append(
                f"  {'🟢' if gpnl >= 0 else '🔴'} *{label}*: "
                f"{sign}₹{gpnl:,.0f} | WR {wr_s} | {b['total']} trades"
            )

    # Stage 11 gate
    active_in_sessions = {
        bn: s for bn, s in sessions.items()
        if bn in BOT_META and (not bot_filter or bn == bot_filter)
    }
    if active_in_sessions:
        lines.append("")
        lines.append("*Stage 11 gate (target: 20 sessions):*")
        for bn, s in sorted(active_in_sessions.items()):
            label = BOT_META[bn]["label"]
            if s >= STAGE11_TARGET:
                icon = "✅"
            elif s >= 15:
                icon = "🟡"
            else:
                icon = "🔬"
            lines.append(f"  {icon} {label}: {s}/{STAGE11_TARGET}")

    lines.append("")
    lines.append(f"_Generated {datetime.now().strftime('%d %b %Y %H:%M IST')}_")
    return "\n".join(lines)


def send_telegram(message: str) -> None:
    """Post message to Telegram. Silent on failure."""
    if not _TG_TOKEN or not _TG_CHAT_ID:
        print("  ⚠  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set — skipping notification.")
        return
    try:
        import requests
        url  = f"https://api.telegram.org/bot{_TG_TOKEN}/sendMessage"
        resp = requests.post(
            url,
            data={"chat_id": _TG_CHAT_ID, "text": message, "parse_mode": "Markdown"},
            timeout=10,
        )
        if resp.ok:
            print("  ✅ Telegram notification sent.")
        else:
            print(f"  ⚠  Telegram returned {resp.status_code}: {resp.text[:120]}")
    except Exception as exc:
        print(f"  ⚠  Telegram send failed: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  RECONCILIATION — stale-state trade recovery
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_positionbook(api_key: str, host: str) -> dict:
    """
    Fetch the full positionbook from OpenAlgo and return a keyed dict.

    Returns:
        {symbol: {"quantity", "pnl", "sell_avg", "buy_avg", "sell_qty", "buy_qty"}}
    Empty dict on any error.
    """
    try:
        import requests as _req
        resp = _req.post(
            f"{host}/api/v1/positionbook",
            json={"apikey": api_key},
            timeout=4,
        )
        data = resp.json()
        if data.get("status") != "success":
            return {}
        result = {}
        for pos in data.get("data", []):
            sym = pos.get("symbol", "")
            if not sym:
                continue
            result[sym] = {
                "quantity":      int(pos.get("quantity", 0)),
                "average_price": float(pos.get("average_price") or 0),
                "ltp":           float(pos.get("ltp") or 0),
                "pnl":           float(pos.get("pnl") or 0),
                "buy_qty":       int(pos.get("buy_qty", 0) or 0),
                "sell_qty":      int(pos.get("sell_qty", 0) or 0),
                "buy_avg":       float(pos.get("buy_avg", 0) or 0),
                "sell_avg":      float(pos.get("sell_avg", 0) or 0),
            }
        return result
    except Exception as exc:
        logger.debug(f"[reconcile] positionbook fetch failed: {exc}")
        return {}


def _infer_instrument(symbol: str) -> str:
    """Derive the underlying instrument name from a full option/equity symbol."""
    for prefix in ("BANKNIFTY", "BANKEX", "SENSEX", "FINNIFTY", "MIDCPNIFTY", "NIFTY"):
        if symbol.upper().startswith(prefix):
            return prefix
    return symbol[:10]   # equity ticker


def _resolve_bot_name(stem: str) -> str:
    """
    Convert a state-file stem to an internal bot_name key used in performance.db.

    e.g. 'banknifty_bb_options' → 'banknifty_bb_options_bot'
         'ema_swing_scanner'    → 'ema_swing_scanner'   (no _bot suffix)
    """
    if stem in BOT_META:
        return stem
    candidate = stem + "_bot"
    if candidate in BOT_META:
        return candidate
    # Fuzzy: first key that starts with stem
    for key in BOT_META:
        if key.startswith(stem):
            return key
    return stem   # unknown — preserve as-is


def _extract_active_trades_from_state(state: dict) -> list[dict]:
    """
    Extract all active-trade dicts from a bot state file, regardless of layout.

    Four layouts handled:
      A  state["active_trade"]                        single-instrument bots
                                                      (BNF BB, Nifty TS leg, etc.)
      B  state["active_trades"]  dict leg→trade       multi-leg bots (Nifty TS)
      C  state["instruments"][inst]["active_trade"]   HA Options Bot (per-instrument)
      D  state["positions"]      dict sym→pos_dict    equity bots (gap_fade_eod)
      E  state[instrument]["active"]                  HTF PO3 style
      F  state["symbols"][sym]["positions"]           Equity OBI style
    """
    trades: list[dict] = []

    # Layout A — single active_trade
    t = state.get("active_trade")
    if isinstance(t, dict) and t.get("symbol"):
        trades.append(t)

    # Layout B — active_trades dict (CE / PE legs)
    at = state.get("active_trades")
    if isinstance(at, dict):
        for _leg, t in at.items():
            if isinstance(t, dict) and t.get("symbol"):
                trades.append(t)

    # Layout C & E — per-instrument dict
    for key, val in state.items():
        if key in ("instruments", "symbols", "active_trades") or not isinstance(val, dict):
            continue
        # Layout E (HTF PO3)
        t = val.get("active")
        if isinstance(t, dict) and t.get("symbol"):
            trades.append(t)
        # Layout C (HA Options style)
        t = val.get("active_trade")
        if isinstance(t, dict) and t.get("symbol"):
            trades.append(t)

    # Layout C explicit check (for nested instruments key)
    instruments = state.get("instruments")
    if isinstance(instruments, dict):
        for _inst, inst_state in instruments.items():
            if not isinstance(inst_state, dict):
                continue
            t = inst_state.get("active_trade")
            if isinstance(t, dict) and t.get("symbol"):
                trades.append(t)

    # Layout F — Equity OBI style
    syms = state.get("symbols")
    if isinstance(syms, dict):
        for sym, sym_state in syms.items():
            if not isinstance(sym_state, dict):
                continue
            pos_list = sym_state.get("positions", [])
            if isinstance(pos_list, list):
                for p in pos_list:
                    if isinstance(p, dict) and p.get("entry_price"):
                        trades.append({
                            "symbol":      sym,
                            "entry_prem":  p.get("entry_price", 0),
                            "entry_price": p.get("entry_price", 0),
                            "qty":         p.get("shares", 0),
                            "shares":      p.get("shares", 0),
                            "direction":   p.get("direction", "long"),
                            "_equity":     True,
                        })

    # Layout D — equity positions dict
    positions = state.get("positions")
    if isinstance(positions, dict):
        for sym, p in positions.items():
            if isinstance(p, dict) and p.get("entry_price"):
                # Normalise equity position into active_trade shape
                trades.append({
                    "symbol":     sym,
                    "entry_prem": p.get("entry_price", 0),
                    "qty":        p.get("quantity", 0),
                    "entry_time": p.get("entry_time", ""),
                    "lot_size":   1,
                    "_equity":    True,
                })

    return trades


def reconcile_stale_trades(dry_run: bool = False) -> list[dict]:
    """
    Compare bot state files with the live OpenAlgo positionbook and write
    synthetic exit records to performance.db for any positions that:

      1.  The bot state file still shows as 'active_trade' (bot never logged exit)
      2.  The OpenAlgo positionbook shows as flat (quantity == 0)
      3.  performance.db has no matching row for (bot_name, symbol, trade_date)

    The root cause this fixes: when a position is closed externally (broker
    risk-action, manual close, or bot crash before the exit order is placed)
    _close_trade() is never called, so log_trade_to_db() is never called.
    The streamlit dashboard shows these as "state stale (broker: flat)" and
    computes P&L from the positionbook; this function ports that same logic
    into performance.db so the numbers agree.

    Entry/exit prices are taken from positionbook sell_avg / buy_avg, which
    are the authoritative broker fill averages.  The gross_pnl comes directly
    from the broker's computed pnl field.

    dry_run=True → prints findings without writing to the DB.
    Returns list of reconciled record dicts (empty if nothing to do).
    """
    import sqlite3

    api_key = os.getenv("OPENALGO_API_KEY")
    host    = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")

    W = 79   # inner box width

    print("\n┌─ RECONCILING STALE-STATE TRADES " + "─" * (W - 33) + "┐")

    if not api_key:
        msg = "  ⚠  OPENALGO_API_KEY not set — positionbook unavailable."
        print(f"│{msg:<{W}}│")
        print("└" + "─" * W + "┘")
        return []

    # 1. Fetch positionbook
    pb = _fetch_positionbook(api_key, host)
    if not pb:
        msg = "  ⚠  Positionbook empty or OpenAlgo unreachable — is the server running?"
        print(f"│{msg:<{W}}│")
        print("└" + "─" * W + "┘")
        return []

    flat_syms = {sym for sym, e in pb.items() if e["quantity"] == 0}
    info_line = f"  Positionbook: {len(pb)} positions  ({len(flat_syms)} flat/closed today)"
    print(f"│{info_line:<{W}}│")

    # 2. Locate all state files
    db_path  = get_db_path()
    logs_dir = db_path.parent          # live_trading/logs/
    root_lt  = logs_dir.parent         # live_trading/

    state_dirs: list[Path] = [
        logs_dir,
        # root_lt / "ema_swing_scanner" / "logs",  # RETIRED 2026-06-04 — archived to live_trading/retired/
        # root_lt / "equity_obi"          / "logs",  # RETIRED 2026-06-19
        root_lt / "nifty_trend_seller_obi" / "logs",
    ]

    # 3. Open DB read-only for dup-checks
    if not db_path.exists():
        msg = f"  ⚠  Performance DB not found at {db_path}"
        print(f"│{msg:<{W}}│")
        print("└" + "─" * W + "┘")
        return []

    try:
        con = sqlite3.connect(str(db_path), timeout=30, check_same_thread=False)
        con.execute("PRAGMA journal_mode=WAL")
    except Exception as exc:
        msg = f"  ⚠  Cannot open performance DB: {exc}"
        print(f"│{msg:<{W}}│")
        print("└" + "─" * W + "┘")
        return []

    # 4. Walk state files, cross-reference positionbook
    reconciled: list[dict] = []

    for sdir in state_dirs:
        if not sdir.exists():
            continue
        for state_path in sorted(sdir.glob("*_state.json")):
            try:
                state = json.loads(state_path.read_text())
            except Exception:
                continue

            stem     = state_path.stem.replace("_state", "")
            bot_name = _resolve_bot_name(stem)
            trades   = _extract_active_trades_from_state(state)

            if not trades:
                continue

            for trade in trades:
                symbol = trade.get("symbol", "")
                if not symbol or symbol not in flat_syms:
                    continue   # not flat in broker → not a stale exit

                pb_entry = pb[symbol]

                # Parse entry timestamp from state file
                entry_time_str = str(trade.get("entry_time", ""))
                entry_dt: datetime | None = None
                if entry_time_str:
                    try:
                        entry_dt = datetime.fromisoformat(entry_time_str)
                    except Exception:
                        pass

                trade_date = entry_dt.date() if entry_dt else date.today()

                # Dup check: already in performance.db?
                try:
                    dup = con.execute(
                        "SELECT COUNT(*) FROM trades "
                        "WHERE bot_name=? AND symbol=? AND trade_date=?",
                        [bot_name, symbol, trade_date],
                    ).fetchone()[0]
                except Exception:
                    dup = 0

                if dup > 0:
                    continue   # already logged (normal exit or prior reconcile)

                # Reconstruct entry/exit prices from broker fill averages
                is_equity   = bool(trade.get("_equity"))
                entry_state = float(trade.get("entry_prem") or trade.get("entry_price") or 0)
                qty         = int(trade.get("qty") or pb_entry["sell_qty"] or pb_entry["buy_qty"] or 0)
                lot_size    = int(trade.get("lot_size") or 1)

                if is_equity:
                    # Equity LONG: bot bought (buy_avg) then sold (sell_avg)
                    entry_price = pb_entry["buy_avg"]  if pb_entry["buy_avg"]  > 0 else entry_state
                    exit_price  = pb_entry["sell_avg"] if pb_entry["sell_avg"] > 0 else 0.0
                else:
                    # Options SELL: bot sold (sell_avg) then bought back (buy_avg)
                    entry_price = pb_entry["sell_avg"] if pb_entry["sell_avg"] > 0 else entry_state
                    exit_price  = pb_entry["buy_avg"]  if pb_entry["buy_avg"]  > 0 else 0.0

                gross_pnl = pb_entry["pnl"]

                # If buy_avg / sell_avg were zero, derive exit_price from broker pnl
                if exit_price == 0.0 and qty > 0 and entry_price > 0 and gross_pnl is not None:
                    if is_equity:
                        exit_price = entry_price + gross_pnl / qty
                    else:
                        exit_price = entry_price - gross_pnl / qty

                instrument = _infer_instrument(symbol)
                sym_u      = symbol.upper()
                if sym_u.endswith("CE"):
                    opt_type, stype, direction = "CE", "options", "sell"
                elif sym_u.endswith("PE"):
                    opt_type, stype, direction = "PE", "options", "sell"
                else:
                    opt_type, stype, direction = None, "equity", "long"

                lots = qty // lot_size if lot_size > 1 else None

                record: dict = {
                    "bot_name":      bot_name,
                    "strategy_type": stype,
                    "instrument":    instrument,
                    "symbol":        symbol,
                    "entry_time":    entry_dt,
                    "exit_time":     datetime.now(),
                    "entry_price":   round(entry_price, 2),
                    "exit_price":    round(exit_price,  2),
                    "exit_reason":   "state stale (broker: flat)",
                    "quantity":      qty,
                    "lots":          lots,
                    "lot_size":      lot_size,
                    "gross_pnl":     round(gross_pnl, 2) if gross_pnl is not None else None,
                    "option_type":   opt_type,
                    "direction":     direction,
                    "notes":         f"auto-reconciled | state_file={state_path.name}",
                    "source":        "paper",
                }
                reconciled.append(record)

                pnl_str  = _fmt_inr(gross_pnl) if gross_pnl is not None else "n/a"
                tag      = " [DRY RUN]" if dry_run else " ✅ written"
                line     = f"  {bot_name:<30} {symbol:<26} {pnl_str:>10}{tag}"
                print(f"│{line:<{W}}│")

                if not dry_run:
                    from live_trading.shared.performance_db import log_trade as _log
                    _log(
                        bot_name      = record["bot_name"],
                        strategy_type = record["strategy_type"],
                        instrument    = record["instrument"],
                        symbol        = record["symbol"],
                        entry_time    = record["entry_time"],
                        exit_time     = record["exit_time"],
                        entry_price   = record["entry_price"],
                        exit_price    = record["exit_price"],
                        exit_reason   = record["exit_reason"],
                        quantity      = record["quantity"],
                        gross_pnl     = record["gross_pnl"],
                        option_type   = record["option_type"],
                        direction     = record["direction"],
                        lots          = record["lots"],
                        lot_size      = record["lot_size"],
                        notes         = record["notes"],
                        source        = record["source"],
                    )

    con.close()

    print(f"│{'':─<{W}}│".replace("─", " "))   # blank separator

    if not reconciled:
        ok_line = "  ✅ Nothing to reconcile — performance.db is in sync with broker."
        print(f"│{ok_line:<{W}}│")
    else:
        total_pnl = sum(r["gross_pnl"] for r in reconciled if r["gross_pnl"] is not None)
        count_line = (
            f"  {'DRY RUN — ' if dry_run else ''}Reconciled {len(reconciled)} trade(s)  "
            f"P&L impact: {_fmt_inr(total_pnl)}"
        )
        print(f"│{count_line:<{W}}│")
        if dry_run:
            hint = "  Re-run without --dry-run to write these records to performance.db."
            print(f"│{hint:<{W}}│")

    print("└" + "─" * W + "┘")
    return reconciled


# ── CLI entry point ───────────────────────────────────────────────────────────

def _format_bot_list() -> str:
    """Generate formatted bot list from BOT_META (auto-updates when bots are added/removed)."""
    lines = []
    # Sort bots by type (options first, then equity) then by name
    sorted_bots = sorted(
        BOT_META.items(),
        key=lambda x: (x[1]["type"], x[0])
    )
    for internal_name, meta in sorted_bots:
        label = meta.get("label", internal_name)
        bot_type = meta.get("type", "")
        universe = meta.get("universe", "")
        lines.append(f"    {internal_name:<28} {label:<30} [{bot_type:<7} {universe}]")
    return "\n".join(lines)


def parse_args():
    bot_list = _format_bot_list()
    p = argparse.ArgumentParser(
        prog="performance_review.py",
        description=(
            "OpenAlgo bot performance review.\n"
            "Reads live_trading/logs/performance.db and prints a terminal report.\n"
            "Use /? or /help as a Windows-style alias for --help."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
SWITCHES
  --days N          Look-back window in calendar days (default: 30).
  --bot BOT_NAME    Filter to one bot. Internal names and metadata:
{bot_list}
  --from YYYY-MM-DD Start date (overrides --days).
  --to   YYYY-MM-DD End date (default: today).
  --recent N        Recent trades to show in the table (default: 15).
  --no-chart        Skip the daily P&L bar chart.
  --no-reasons      Skip the exit reason breakdown.
  --notify          Send Telegram summary after report.
                    Requires TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID in .env.
  --reconcile       Scan bot state files vs live positionbook. Writes
                    synthetic exit records for stale-state positions
                    (those the broker closed but the bot never logged).
                    Requires OpenAlgo to be running.
  --dry-run         With --reconcile: show what would be written, no DB changes.
  /?  /help         Alias for --help (Windows-style).

EXAMPLES
  uv run live_trading/performance_review.py
  uv run live_trading/performance_review.py --days 7
  uv run live_trading/performance_review.py --bot ha_options_bot --days 60
  uv run live_trading/performance_review.py --from 2026-04-01 --to 2026-04-30
  uv run live_trading/performance_review.py --days 1 --notify
  uv run live_trading/performance_review.py --reconcile
  uv run live_trading/performance_review.py --reconcile --dry-run
        """,
    )
    p.add_argument("--days",        type=int, default=30,   help="Look-back window in calendar days (default: 30)")
    p.add_argument("--bot",         type=str, default=None, help="Filter to a single bot name")
    p.add_argument("--from",        dest="date_from", type=str, default=None, help="Start date YYYY-MM-DD")
    p.add_argument("--to",          dest="date_to",   type=str, default=None, help="End date YYYY-MM-DD (default: today)")
    p.add_argument("--recent",      type=int, default=15,   help="Number of recent trades to show (default: 15)")
    p.add_argument("--no-chart",    action="store_true",    help="Skip the daily P&L bar chart section")
    p.add_argument("--no-reasons",  action="store_true",    help="Skip the exit reason breakdown section")
    p.add_argument("--notify",      action="store_true",    help="Send Telegram summary after printing the report")
    p.add_argument("--reconcile",   action="store_true",    help="Detect and write stale-state exits from positionbook")
    p.add_argument("--dry-run",     action="store_true",    help="With --reconcile: show findings without writing to DB")
    return p.parse_args()


def main():
    # ── Support Windows-style /? and /help as aliases for --help ──────────────
    if len(sys.argv) > 1 and sys.argv[1] in ("/?", "/help", "/HELP", "/?"):
        sys.argv[1] = "--help"

    args = parse_args()

    # Resolve date range
    end_dt   = date.today()
    start_dt = end_dt - timedelta(days=args.days - 1)

    if args.date_from:
        start_dt = date.fromisoformat(args.date_from)
    if args.date_to:
        end_dt = date.fromisoformat(args.date_to)

    db_path = get_db_path()

    # ── Reconcile stale-state trades first (before opening read-only con) ─────
    if args.reconcile:
        reconcile_stale_trades(dry_run=args.dry_run)
        print()   # blank line before report

    # ── Open DB for report ────────────────────────────────────────────────────
    try:
        con = _connect(db_path)
    except FileNotFoundError as e:
        print(f"\n  ❌  {e}\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n  ❌  Cannot open performance DB: {e}\n")
        sys.exit(1)

    # Fetch data
    summary  = fetch_portfolio_summary(con, start_dt, end_dt, args.bot)
    bots     = fetch_per_bot(con, start_dt, end_dt, args.bot)
    sessions = fetch_stage11_sessions(con)
    daily    = fetch_daily_pnl(con, start_dt, end_dt, args.bot)
    reasons  = fetch_exit_reasons(con, start_dt, end_dt, args.bot)
    recent   = fetch_recent_trades(con, args.recent, start_dt, end_dt, args.bot)
    con.close()

    # Render
    print_header(start_dt, end_dt, args.bot)
    print_portfolio_summary(summary)
    print_per_bot(bots)

    if not args.bot:
        print_stage11(sessions, bots)

    if not args.no_chart:
        print_daily_chart(daily)

    if not args.no_reasons:
        print_exit_reasons(reasons)

    print_recent_trades(recent)
    print_footer(db_path)

    if args.notify:
        msg = compose_telegram_message(start_dt, end_dt, summary, bots, sessions, args.bot)
        send_telegram(msg)


if __name__ == "__main__":
    main()
