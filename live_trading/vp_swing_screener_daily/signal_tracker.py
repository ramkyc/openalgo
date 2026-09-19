"""
live_trading/vp_swing_screener_daily/signal_tracker.py
========================================================
Signal-history tracking for the VP Swing Screener (Daily) -- records every
candidate the screener detects, both the 15:45 close-confirmed touch and the
15:30 intraday heads-up, whether or not the user ever clicked Confirm. This
is what makes "how did today's candidates perform" answerable later without
grepping the text log, and lets an unconfirmed candidate's hypothetical
stop/target outcome be reviewed too, not just the ones that became real
positions.

Lives in the same SQLite file as live_trading/shared/performance_db.py
(own table, `vp_swing_signals` -- a signal here is a detected touch, not
necessarily an executed trade, so it stays separate from that module's
`trades` table).

Silent on failure -- never raises, never crashes the bot, mirroring
performance_db.py's own convention.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime

from live_trading.shared.performance_db import get_db_path

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS vp_swing_signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_name        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    source          TEXT NOT NULL,        -- 'final_scan' | 'intraday_headsup'
    detected_at     TEXT NOT NULL,        -- ISO datetime
    signal_price    REAL,                 -- price a Confirm click enters at
    touch_price     REAL,                 -- rolling-low touch (bar_low / day_low_so_far)
    poc_target      REAL,
    stop_price      REAL,
    reference_price REAL,                 -- close (final_scan) or ltp (intraday_headsup) at detection
    confirmed       INTEGER DEFAULT 0,
    outcome         TEXT,                 -- 'stop' | 'target' | NULL (unresolved)
    outcome_price   REAL,
    outcome_at      TEXT,
    UNIQUE (bot_name, symbol, source, detected_at)
)
"""


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(get_db_path()), timeout=30, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    con.execute(_DDL)
    return con


def log_signal(
    bot_name: str,
    symbol: str,
    source: str,
    detected_at: str,
    signal_price: float,
    touch_price: float,
    poc_target: float,
    stop_price: float,
    reference_price: float,
) -> None:
    """Record a newly detected candidate. Safe to call even if it was
    already logged this run (e.g. a restart re-detecting the same
    detected_at) -- the UNIQUE constraint makes this a no-op via INSERT OR
    IGNORE rather than a duplicate row."""
    try:
        con = _connect()
        con.execute(
            """
            INSERT OR IGNORE INTO vp_swing_signals
              (bot_name, symbol, source, detected_at, signal_price,
               touch_price, poc_target, stop_price, reference_price)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            [bot_name, symbol, source, detected_at, signal_price,
             touch_price, poc_target, stop_price, reference_price],
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"[signal_tracker] log_signal failed for {symbol}: {e}")


def mark_confirmed(bot_name: str, symbol: str, source: str, detected_at: str) -> None:
    """Called from the dashboard when the user clicks Confirm on a
    candidate -- flags the matching signal row so later review can separate
    'I acted on this' from 'the strategy just flagged it'."""
    try:
        con = _connect()
        con.execute(
            """
            UPDATE vp_swing_signals SET confirmed = 1
            WHERE bot_name=? AND symbol=? AND source=? AND detected_at=?
            """,
            [bot_name, symbol, source, detected_at],
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"[signal_tracker] mark_confirmed failed for {symbol}: {e}")


def fetch_pending(bot_name: str) -> list[dict]:
    """All signals (confirmed or not) with no outcome yet, for the daily
    forward-resolution pass to re-check against fresh bars."""
    try:
        con = _connect()
        con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT * FROM vp_swing_signals WHERE bot_name=? AND outcome IS NULL",
            [bot_name],
        ).fetchall()
        con.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.warning(f"[signal_tracker] fetch_pending failed: {e}")
        return []


def resolve_signal(row_id: int, outcome: str, outcome_price: float,
                    outcome_at: str | None = None) -> None:
    try:
        con = _connect()
        con.execute(
            "UPDATE vp_swing_signals SET outcome=?, outcome_price=?, outcome_at=? WHERE id=?",
            [outcome, outcome_price,
             outcome_at or datetime.now().isoformat(timespec="seconds"), row_id],
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"[signal_tracker] resolve_signal failed for id={row_id}: {e}")
