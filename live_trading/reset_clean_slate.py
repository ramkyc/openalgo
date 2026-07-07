#!/usr/bin/env python3
"""
reset_clean_slate.py
====================
One-shot script to wipe all paper-trading history and start fresh.

Run from the fyers_crk/openalgo root:
    uv run live_trading/reset_clean_slate.py

What it does
------------
1. Clears all rows from performance.db  (trades, daily_summary, trading_sessions)
2. Clears all rows from paper_trades.db (paper_trades_options, paper_trades_stocks)
3. Deletes sandbox.db + sandbox.db-journal   (OpenAlgo recreates on next startup)
4. Deletes all bot state JSON files in live_trading/logs/
   (so bots start with a blank slate — no ghost "active_trade" from before)

What it does NOT touch
-----------------------
- Live broker credentials / sessions
- openalgo.db (orders, settings, users)
- logs.db / latency.db / health.db
- DuckDB historical data
- Bot Python source files

Stage-11 sign-offs have been recorded in live_trading/active_trading_bots.md
before running this script (2026-05-18):
  ✅ BANKNIFTY BB Options Bot  — 32 sessions, net +₹84,156
  ✅ HA Options Bot            — 31 sessions, net +₹1,00,949
"""

import sqlite3
import sys
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).parent.parent          # openalgo/
LT_DIR    = ROOT / "live_trading"
LOGS_DIR  = LT_DIR / "logs"
DB_DIR    = ROOT / "db"

PERF_DB   = LOGS_DIR / "performance.db"
PAPER_DB  = LOGS_DIR / "paper_trades.db"
SANDBOX   = DB_DIR / "sandbox.db"
SANDBOX_J = DB_DIR / "sandbox.db-journal"
SANDBOX_W = DB_DIR / "sandbox.db-wal"
SANDBOX_S = DB_DIR / "sandbox.db-shm"

# All bot state JSON files (keyed by display name → path relative to LOGS_DIR)
BOT_STATE_FILES = [
    "banknifty_bb_options_state.json",
    "ha_options_state.json",
    "htf_po3_state.json",
    "nifty_trend_seller_state.json",
    "sensex_trend_seller_state.json",
    "nifty_macd_map_state.json",
    "nifty_eod_hold_state.json",
    "nifty_bb_overbought_state.json",
    "iron_fly_weekly_state.json",
    "sensex_iron_fly_weekly_state.json",
    "banknifty_iron_fly_monthly_state.json",
    "preopen_gap_fade_state.json",
    "gap_fade_eod_state.json",
    "cb_live_state.json",
    "bb_signals_state.json",
]

# ── Helpers ────────────────────────────────────────────────────────────────────

def confirm(prompt: str) -> bool:
    answer = input(f"{prompt} [yes/no]: ").strip().lower()
    return answer == "yes"


def clear_sqlite_tables(db_path: Path, tables: list[str]) -> None:
    if not db_path.exists():
        print(f"  ⚠️  {db_path.name} not found — skipping")
        return
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        for table in tables:
            try:
                cur.execute(f"DELETE FROM {table}")  # noqa: S608
                count = cur.rowcount
                print(f"  ✓  {db_path.name} → {table}: deleted {count} rows")
            except sqlite3.OperationalError as e:
                print(f"  ⚠️  {db_path.name} → {table}: {e}")
        conn.commit()
    finally:
        conn.close()


def delete_file(path: Path, label: str) -> None:
    if path.exists():
        path.unlink()
        print(f"  ✓  Deleted {label}: {path.name}")
    else:
        print(f"  –  {label} not found — skipping: {path.name}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print()
    print("=" * 65)
    print("  reset_clean_slate.py — Paper Trading Database Reset")
    print("=" * 65)
    print()
    print("Stage-11 sign-offs preserved in active_trading_bots.md:")
    print("  ✅ BANKNIFTY BB Options Bot  — 32 sessions, net +₹84,156")
    print("  ✅ HA Options Bot            — 31 sessions, net +₹1,00,949")
    print()
    print("This will PERMANENTLY DELETE all paper trading history:")
    print("  • performance.db  (all trades, sessions, daily summaries)")
    print("  • paper_trades.db (all logged paper trades)")
    print("  • sandbox.db      (all sandbox positions, funds, orders)")
    print("  • All bot state JSON files in live_trading/logs/")
    print()
    print("⚠️  Make sure OpenAlgo is NOT running before proceeding.")
    print()

    if not confirm("Are you sure you want to reset everything?"):
        print("Aborted.")
        sys.exit(0)

    if not confirm("Double-check: this cannot be undone. Proceed?"):
        print("Aborted.")
        sys.exit(0)

    print()
    print("── Step 1: Clear performance.db ──────────────────────────────")
    clear_sqlite_tables(PERF_DB, ["trades", "daily_summary", "trading_sessions"])

    print()
    print("── Step 2: Clear paper_trades.db ─────────────────────────────")
    clear_sqlite_tables(PAPER_DB, ["paper_trades_options"])

    print()
    print("── Step 3: Delete sandbox.db (OpenAlgo recreates on startup) ─")
    for f in [SANDBOX, SANDBOX_J, SANDBOX_W, SANDBOX_S]:
        delete_file(f, f.suffix or f.name)

    print()
    print("── Step 4: Delete bot state JSON files ───────────────────────")
    for fname in BOT_STATE_FILES:
        delete_file(LOGS_DIR / fname, fname)

    print()
    print("=" * 65)
    print("  ✅  Reset complete.")
    print()
    print("Next steps:")
    print("  1. Start OpenAlgo (uv run app.py)  — sandbox.db will be")
    print("     recreated with the new schema (strategy column + correct")
    print("     UNIQUE constraint) and ₹1 Crore virtual capital.")
    print("  2. Start bots normally — they will begin fresh sessions.")
    print("=" * 65)
    print()


if __name__ == "__main__":
    main()
