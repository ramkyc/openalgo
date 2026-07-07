"""
live_trading/shared/performance_db.py
======================================
Unified local performance database for ALL live bots.

Writes to live_trading/logs/performance.db  (SQLite, WAL mode).
WAL mode allows unlimited concurrent readers and serialises writers without
lock errors — safe for all bots running simultaneously.

Two tables:
  • trades        — one row per completed trade (options + equity)
  • daily_summary — one aggregated row per (bot_name, trade_date)

Usage in any bot:
    from live_trading.shared.performance_db import log_trade

    log_trade(
        bot_name      = "ha_options_bot",
        strategy_type = "options",        # "options" | "equity"
        instrument    = "NIFTY",
        symbol        = "NIFTY21APR2624200PE",
        option_type   = "PE",             # None for equity
        entry_time    = datetime(...),
        exit_time     = datetime(...),
        entry_price   = 150.0,
        exit_price    = 105.0,
        exit_reason   = "HA Reversal",
        quantity      = 65,
        lots          = 1,
        lot_size      = 65,
        gross_pnl     = 2925.0,
        source        = "paper",
    )

Design principles:
  • Silent on failure — never raises, never crashes a bot
  • Deduplicates on (bot_name, entry_time, symbol) — safe to replay
  • daily_summary is re-computed (upserted) after every trade insert
  • DB path: live_trading/logs/performance.db
              or env var PERFORMANCE_DB
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Resolve DB path ────────────────────────────────────────────────────────────
_THIS_DIR = Path(__file__).parent          # live_trading/shared/
_LOGS_DIR = _THIS_DIR.parent / "logs"     # live_trading/logs/
_LOGS_DIR.mkdir(parents=True, exist_ok=True)

_DB_PATH: Path = Path(
    os.environ.get(
        "PERFORMANCE_DB",
        str(_LOGS_DIR / "performance.db"),
    )
)

# ── DDL ────────────────────────────────────────────────────────────────────────
_DDL_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_name           TEXT    NOT NULL,
    strategy_type      TEXT,              -- 'options' | 'equity'
    instrument         TEXT,              -- NIFTY, BANKNIFTY, SENSEX, or stock sym
    trade_date         TEXT,              -- ISO date YYYY-MM-DD
    symbol             TEXT,              -- full option symbol or stock ticker
    direction          TEXT DEFAULT 'sell', -- 'sell' (option seller) | 'long' (equity)
    option_type        TEXT,              -- 'CE' | 'PE' | NULL
    entry_time         TEXT,              -- ISO datetime
    exit_time          TEXT,              -- ISO datetime
    entry_price        REAL,              -- premium (options) or stock price (equity)
    exit_price         REAL,
    exit_reason        TEXT,
    quantity           INTEGER,           -- shares or option quantity
    lots               INTEGER,
    lot_size           INTEGER,
    gross_pnl          REAL,
    net_pnl            REAL,              -- gross_pnl minus Fyers F&O round-trip costs
    decay_pct          REAL,              -- option decay % (NULL for equity)
    won                INTEGER,           -- 1 = win, 0 = loss, NULL = unknown
    hold_duration_mins INTEGER,           -- minutes between entry and exit
    notes              TEXT,
    source             TEXT DEFAULT 'paper', -- 'paper' | 'live'
    created_at         TEXT DEFAULT (datetime('now'))
)
"""

_DDL_DAILY_SUMMARY = """
CREATE TABLE IF NOT EXISTS daily_summary (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_name         TEXT NOT NULL,
    trade_date       TEXT NOT NULL,       -- ISO date YYYY-MM-DD
    total_trades     INTEGER DEFAULT 0,
    winning_trades   INTEGER DEFAULT 0,
    losing_trades    INTEGER DEFAULT 0,
    win_rate         REAL,
    gross_pnl        REAL  DEFAULT 0,
    avg_pnl_per_trade REAL,
    best_trade_pnl   REAL,
    worst_trade_pnl  REAL,
    avg_hold_mins    REAL,
    avg_entry_price  REAL,
    avg_decay_pct    REAL,
    instruments      TEXT,
    created_at       TEXT DEFAULT (datetime('now')),
    updated_at       TEXT DEFAULT (datetime('now')),
    UNIQUE (bot_name, trade_date)
)
"""

_DDL_SESSIONS = """
CREATE TABLE IF NOT EXISTS trading_sessions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_name     TEXT NOT NULL,
    session_date TEXT NOT NULL,           -- ISO date YYYY-MM-DD
    gross_pnl    REAL  DEFAULT 0,
    trade_count  INTEGER DEFAULT 0,
    UNIQUE (bot_name, session_date)
)
"""


def _get_connection() -> sqlite3.Connection:
    """Open (or create) the performance SQLite DB with WAL mode."""
    con = sqlite3.connect(str(_DB_PATH), timeout=30, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    con.execute(_DDL_TRADES)
    con.execute(_DDL_DAILY_SUMMARY)
    con.execute(_DDL_SESSIONS)
    # Migration: add net_pnl column if it does not exist yet
    cols = {r[1] for r in con.execute("PRAGMA table_info(trades)").fetchall()}
    if "net_pnl" not in cols:
        con.execute("ALTER TABLE trades ADD COLUMN net_pnl REAL")
        con.commit()
    return con


def _upsert_daily_summary(con: sqlite3.Connection, bot_name: str, trade_date) -> None:
    """Recompute and upsert daily_summary row for (bot_name, trade_date)."""
    td_str = str(trade_date) if trade_date else None
    row = con.execute(
        """
        SELECT
            COUNT(*)                                         AS total_trades,
            SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END)        AS winning_trades,
            SUM(CASE WHEN won = 0 THEN 1 ELSE 0 END)        AS losing_trades,
            SUM(gross_pnl)                                   AS gross_pnl,
            AVG(gross_pnl)                                   AS avg_pnl,
            MAX(gross_pnl)                                   AS best_trade,
            MIN(gross_pnl)                                   AS worst_trade,
            AVG(hold_duration_mins)                          AS avg_hold,
            AVG(entry_price)                                 AS avg_entry,
            AVG(decay_pct)                                   AS avg_decay,
            GROUP_CONCAT(DISTINCT instrument)                AS instruments
        FROM trades
        WHERE bot_name = ? AND trade_date = ?
        """,
        [bot_name, td_str],
    ).fetchone()

    if not row or row[0] == 0:
        return

    total, wins, losses, gpnl, apnl, best, worst, ahold, aentry, adecay, instrs = row
    win_rate = (wins / total * 100) if total else None
    now_str  = datetime.now().isoformat()

    exists = con.execute(
        "SELECT COUNT(*) FROM daily_summary WHERE bot_name=? AND trade_date=?",
        [bot_name, td_str],
    ).fetchone()[0]

    if exists:
        con.execute(
            """
            UPDATE daily_summary SET
                total_trades=?, winning_trades=?, losing_trades=?,
                win_rate=?, gross_pnl=?, avg_pnl_per_trade=?,
                best_trade_pnl=?, worst_trade_pnl=?,
                avg_hold_mins=?, avg_entry_price=?, avg_decay_pct=?,
                instruments=?, updated_at=?
            WHERE bot_name=? AND trade_date=?
            """,
            [total, wins, losses, win_rate, gpnl, apnl,
             best, worst, ahold, aentry, adecay, instrs, now_str,
             bot_name, td_str],
        )
    else:
        con.execute(
            """
            INSERT INTO daily_summary
              (bot_name, trade_date, total_trades, winning_trades, losing_trades,
               win_rate, gross_pnl, avg_pnl_per_trade,
               best_trade_pnl, worst_trade_pnl,
               avg_hold_mins, avg_entry_price, avg_decay_pct, instruments)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [bot_name, td_str, total, wins, losses, win_rate,
             gpnl, apnl, best, worst, ahold, aentry, adecay, instrs],
        )

    # Also upsert trading_sessions
    sess_exists = con.execute(
        "SELECT COUNT(*) FROM trading_sessions WHERE bot_name=? AND session_date=?",
        [bot_name, td_str],
    ).fetchone()[0]
    if sess_exists:
        con.execute(
            "UPDATE trading_sessions SET gross_pnl=?, trade_count=? "
            "WHERE bot_name=? AND session_date=?",
            [gpnl or 0, total or 0, bot_name, td_str],
        )
    else:
        con.execute(
            "INSERT INTO trading_sessions (bot_name, session_date, gross_pnl, trade_count) "
            "VALUES (?,?,?,?)",
            [bot_name, td_str, gpnl or 0, total or 0],
        )


# ── Public API ─────────────────────────────────────────────────────────────────

def log_trade(
    bot_name:      str,
    strategy_type: str,              # "options" | "equity"
    instrument:    str,
    symbol:        str,
    entry_time:    datetime | str | None,
    exit_time:     datetime | str | None,
    entry_price:   float | None,
    exit_price:    float | None,
    exit_reason:   str,
    quantity:      int | None,
    gross_pnl:     float | None,
    net_pnl:       float | None = None,
    option_type:   str | None  = None,
    direction:     str         = "sell",
    lots:          int | None  = None,
    lot_size:      int | None  = None,
    notes:         str | None  = None,
    source:        str         = "paper",
) -> None:
    """
    Append one completed trade to the local performance.db (SQLite).
    Silent on failure — never raises, never crashes a bot.
    """
    try:
        # Normalise datetimes
        if isinstance(entry_time, str):
            entry_time = datetime.fromisoformat(entry_time)
        if isinstance(exit_time, str):
            exit_time = datetime.fromisoformat(exit_time)

        # Use exit date, not entry date — a trade booked/realised today must
        # show up in "today's trades" even if it was entered on a prior day
        # (multi-day holds, e.g. EMA spread bots held over a weekend).
        trade_date = (
            exit_time.date() if exit_time else
            (entry_time.date() if entry_time else None)
        )
        trade_date_str = str(trade_date) if trade_date else None
        entry_str      = entry_time.isoformat() if entry_time else None
        exit_str       = exit_time.isoformat()  if exit_time  else None

        # Compute derived fields
        won = int((net_pnl if net_pnl is not None else gross_pnl) > 0) if (net_pnl or gross_pnl) is not None else None

        decay_pct: float | None = None
        if strategy_type == "options" and entry_price and entry_price > 0 and exit_price is not None:
            decay_pct = round((entry_price - exit_price) / entry_price * 100, 2)

        hold_mins: int | None = None
        if entry_time and exit_time:
            hold_mins = int((exit_time - entry_time).total_seconds() / 60)

        con = _get_connection()

        # Deduplicate by (bot_name, entry_time, symbol)
        if entry_str and symbol:
            dup = con.execute(
                "SELECT COUNT(*) FROM trades WHERE bot_name=? AND entry_time=? AND symbol=?",
                [bot_name, entry_str, symbol],
            ).fetchone()[0]
            if dup > 0:
                con.close()
                return

        con.execute(
            """
            INSERT INTO trades
              (bot_name, strategy_type, instrument, trade_date, symbol,
               direction, option_type,
               entry_time, exit_time, entry_price, exit_price,
               exit_reason, quantity, lots, lot_size,
               gross_pnl, net_pnl, decay_pct, won, hold_duration_mins,
               notes, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                bot_name, strategy_type, instrument, trade_date_str, symbol,
                direction, option_type,
                entry_str, exit_str, entry_price, exit_price,
                exit_reason, quantity, lots, lot_size,
                round(gross_pnl, 2) if gross_pnl is not None else None,
                round(net_pnl, 2) if net_pnl is not None else None,
                decay_pct, won, hold_mins,
                notes, source,
            ],
        )
        _upsert_daily_summary(con, bot_name, trade_date_str)
        con.commit()
        con.close()

        pnl_str = f"₹{gross_pnl:+,.0f}" if gross_pnl is not None else "n/a"
        logger.debug(f"[perf_db] {bot_name} | {symbol} | {pnl_str}")

    except Exception as exc:
        logger.warning(f"[perf_db] Write failed for {bot_name}: {exc}")


def rebuild_daily_summary() -> None:
    """
    Recompute ALL daily_summary rows from scratch.
    Safe to call at any time — clears and rebuilds from trades table.
    Useful after importing historical trades.
    """
    try:
        con = _get_connection()
        pairs = con.execute(
            "SELECT DISTINCT bot_name, trade_date FROM trades WHERE trade_date IS NOT NULL"
        ).fetchall()
        for bot_name, trade_date in pairs:
            _upsert_daily_summary(con, bot_name, trade_date)
        con.commit()
        con.close()
        logger.info(f"[perf_db] daily_summary rebuilt for {len(pairs)} (bot, date) pairs.")
    except Exception as exc:
        logger.warning(f"[perf_db] rebuild_daily_summary failed: {exc}")


def get_db_path() -> Path:
    """Return the path to the performance SQLite DB (for inspection / reporting)."""
    return _DB_PATH
