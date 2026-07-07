#!/usr/bin/env python3
"""
live_trading/migrate_perf_db.py
================================
One-shot migration: performance.duckdb  →  performance.db  (SQLite, WAL).

Run once:
    uv run live_trading/migrate_perf_db.py

Safe to re-run — deduplicates on (bot_name, entry_time, symbol).
After a successful migration, performance.duckdb is renamed to
performance.duckdb.bak so it is not deleted but won't be opened again.
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

LOGS_DIR   = Path(__file__).parent / "logs"
DUCKDB_PATH = LOGS_DIR / "performance.duckdb"
SQLITE_PATH = LOGS_DIR / "performance.db"

# ─────────────────────────────────────────────────────────────────────────────

def _open_sqlite(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(path), timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    return con


def _ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        id                 INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_name           TEXT    NOT NULL,
        strategy_type      TEXT,
        instrument         TEXT,
        trade_date         TEXT,
        symbol             TEXT,
        direction          TEXT DEFAULT 'sell',
        option_type        TEXT,
        entry_time         TEXT,
        exit_time          TEXT,
        entry_price        REAL,
        exit_price         REAL,
        exit_reason        TEXT,
        quantity           INTEGER,
        lots               INTEGER,
        lot_size           INTEGER,
        gross_pnl          REAL,
        decay_pct          REAL,
        won                INTEGER,
        hold_duration_mins INTEGER,
        notes              TEXT,
        source             TEXT DEFAULT 'paper',
        created_at         TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS daily_summary (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_name         TEXT NOT NULL,
        trade_date       TEXT NOT NULL,
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
    );

    CREATE TABLE IF NOT EXISTS trading_sessions (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_name     TEXT NOT NULL,
        session_date TEXT NOT NULL,
        gross_pnl    REAL  DEFAULT 0,
        trade_count  INTEGER DEFAULT 0,
        UNIQUE (bot_name, session_date)
    );
    """)
    con.commit()


def _safe_str(v) -> str | None:
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _safe_int(v) -> int | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    return int(v)


def migrate():
    if not DUCKDB_PATH.exists():
        print(f"❌ Source not found: {DUCKDB_PATH}")
        sys.exit(1)

    try:
        import duckdb
    except ImportError:
        print("❌ duckdb not importable in this environment. Run: pip install duckdb")
        sys.exit(1)

    print(f"📂 Source : {DUCKDB_PATH}")
    print(f"📂 Target : {SQLITE_PATH}")
    print()

    # ── Open source ───────────────────────────────────────────────────────────
    src = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    # ── Open / create target ──────────────────────────────────────────────────
    dst = _open_sqlite(SQLITE_PATH)
    _ensure_schema(dst)

    # ── Migrate trades ────────────────────────────────────────────────────────
    trade_rows = src.execute("SELECT * FROM trades ORDER BY id").fetchall()
    trade_cols = [d[0] for d in src.description]
    print(f"  trades        : {len(trade_rows)} rows")

    inserted = skipped = 0
    for row in trade_rows:
        r = dict(zip(trade_cols, row))

        # Dedup check
        dup = dst.execute(
            "SELECT COUNT(*) FROM trades WHERE bot_name=? AND entry_time=? AND symbol=?",
            [r["bot_name"], _safe_str(r.get("entry_time")), r.get("symbol")],
        ).fetchone()[0]
        if dup:
            skipped += 1
            continue

        dst.execute(
            """
            INSERT INTO trades
              (bot_name, strategy_type, instrument, trade_date, symbol,
               direction, option_type, entry_time, exit_time,
               entry_price, exit_price, exit_reason,
               quantity, lots, lot_size,
               gross_pnl, decay_pct, won, hold_duration_mins,
               notes, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                r.get("bot_name"), r.get("strategy_type"), r.get("instrument"),
                _safe_str(r.get("trade_date")), r.get("symbol"),
                r.get("direction", "sell"), r.get("option_type"),
                _safe_str(r.get("entry_time")), _safe_str(r.get("exit_time")),
                r.get("entry_price"), r.get("exit_price"), r.get("exit_reason"),
                r.get("quantity"), r.get("lots"), r.get("lot_size"),
                r.get("gross_pnl"), r.get("decay_pct"),
                _safe_int(r.get("won")),
                r.get("hold_duration_mins"),
                r.get("notes"), r.get("source", "paper"),
            ],
        )
        inserted += 1

    dst.commit()
    print(f"            → inserted={inserted}, skipped(dup)={skipped}")

    # ── Migrate daily_summary ─────────────────────────────────────────────────
    ds_rows = src.execute("SELECT * FROM daily_summary ORDER BY id").fetchall()
    ds_cols = [d[0] for d in src.description]
    print(f"  daily_summary : {len(ds_rows)} rows")

    ds_ins = ds_skip = 0
    for row in ds_rows:
        r = dict(zip(ds_cols, row))
        dup = dst.execute(
            "SELECT COUNT(*) FROM daily_summary WHERE bot_name=? AND trade_date=?",
            [r["bot_name"], _safe_str(r.get("trade_date"))],
        ).fetchone()[0]
        if dup:
            ds_skip += 1
            continue
        dst.execute(
            """
            INSERT INTO daily_summary
              (bot_name, trade_date, total_trades, winning_trades, losing_trades,
               win_rate, gross_pnl, avg_pnl_per_trade,
               best_trade_pnl, worst_trade_pnl,
               avg_hold_mins, avg_entry_price, avg_decay_pct, instruments)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                r.get("bot_name"), _safe_str(r.get("trade_date")),
                r.get("total_trades"), r.get("winning_trades"), r.get("losing_trades"),
                r.get("win_rate"), r.get("gross_pnl"), r.get("avg_pnl_per_trade"),
                r.get("best_trade_pnl"), r.get("worst_trade_pnl"),
                r.get("avg_hold_mins"), r.get("avg_entry_price"), r.get("avg_decay_pct"),
                r.get("instruments"),
            ],
        )
        ds_ins += 1

    dst.commit()
    print(f"            → inserted={ds_ins}, skipped(dup)={ds_skip}")

    # ── Migrate trading_sessions ──────────────────────────────────────────────
    ss_rows = src.execute("SELECT * FROM trading_sessions ORDER BY id").fetchall()
    ss_cols = [d[0] for d in src.description]
    print(f"  trading_sessions: {len(ss_rows)} rows")

    ss_ins = ss_skip = 0
    for row in ss_rows:
        r = dict(zip(ss_cols, row))
        dup = dst.execute(
            "SELECT COUNT(*) FROM trading_sessions WHERE bot_name=? AND session_date=?",
            [r["bot_name"], _safe_str(r.get("session_date"))],
        ).fetchone()[0]
        if dup:
            ss_skip += 1
            continue
        dst.execute(
            "INSERT INTO trading_sessions (bot_name, session_date, gross_pnl, trade_count) "
            "VALUES (?,?,?,?)",
            [r.get("bot_name"), _safe_str(r.get("session_date")),
             r.get("gross_pnl", 0), r.get("trade_count", 0)],
        )
        ss_ins += 1

    dst.commit()
    print(f"            → inserted={ss_ins}, skipped(dup)={ss_skip}")

    src.close()
    dst.close()

    # ── Verify ────────────────────────────────────────────────────────────────
    verify = _open_sqlite(SQLITE_PATH)
    t = verify.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    d = verify.execute("SELECT COUNT(*) FROM daily_summary").fetchone()[0]
    s = verify.execute("SELECT COUNT(*) FROM trading_sessions").fetchone()[0]
    verify.close()

    print()
    print(f"✅ Verification — performance.db: trades={t}, daily_summary={d}, trading_sessions={s}")

    # ── Rename old DuckDB ────────────────────────────────────────────────────
    bak = DUCKDB_PATH.with_suffix(".duckdb.bak")
    DUCKDB_PATH.rename(bak)
    wal = DUCKDB_PATH.with_suffix(".duckdb.wal")
    if wal.exists():
        wal.rename(bak.with_suffix(".wal"))
    print(f"📦 Old DuckDB archived → {bak.name}")
    print()
    print("Migration complete. All bots will now write to performance.db (SQLite).")


if __name__ == "__main__":
    migrate()
