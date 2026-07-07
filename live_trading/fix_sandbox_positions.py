"""
fix_sandbox_positions.py
========================
Fixes two issues with sandbox.db:

ISSUE 1 (immediate) — Stale zero-qty rows blocking equity_obi
  PREOPEN_GAP_FADE has left closed (qty=0) MIS rows for ~41 NIFTY50 stocks.
  Because sandbox_positions has UNIQUE(user_id, symbol, exchange, product)
  WITHOUT strategy, equity_obi's INSERT for those same symbols fails with
  a UNIQUE constraint violation. OpenAlgo returns status=success to the bot
  but internally rejects the order — creating phantom positions.
  Fix: delete the zero-qty MIS PREOPEN_GAP_FADE rows.

ISSUE 2 (structural) — Missing 'strategy' in the UNIQUE constraint
  The SQLAlchemy model defines UNIQUE(user_id, symbol, exchange, product, strategy)
  but the actual database was created before that code change, so it still has
  the old UNIQUE(user_id, symbol, exchange, product). This means only ONE bot
  can ever hold a position in a given symbol at a time.
  Fix: recreate sandbox_positions with the correct constraint.

  ⚠️  Run Issue 2 fix AFTER market hours only. It is safe but rebuilds the
      table so you don't want it running while bots are actively writing.

Usage:
    # Fix 1 only (safe during market hours — takes <1s):
    uv run live_trading/fix_sandbox_positions.py --issue 1

    # Fix 2 only (run after 15:30):
    uv run live_trading/fix_sandbox_positions.py --issue 2

    # Both fixes (run after 15:30):
    uv run live_trading/fix_sandbox_positions.py --issue both
"""

import argparse
import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "db" / "sandbox.db"


def connect_with_retry(db_path: Path, retries: int = 10, delay: float = 0.5):
    """Open a connection, retrying if the DB is briefly locked."""
    for attempt in range(retries):
        try:
            con = sqlite3.connect(str(db_path), timeout=10)
            con.execute("PRAGMA journal_mode=WAL")
            return con
        except sqlite3.OperationalError as e:
            if attempt < retries - 1:
                print(f"  DB busy, retrying ({attempt+1}/{retries})...")
                time.sleep(delay)
            else:
                raise


def fix_issue_1(con: sqlite3.Connection) -> None:
    """Delete stale zero-qty PREOPEN_GAP_FADE MIS rows that block other bots."""
    cur = con.cursor()

    # Preview
    cur.execute(
        "SELECT symbol FROM sandbox_positions "
        "WHERE quantity = 0 AND product = 'MIS' AND strategy = 'PREOPEN_GAP_FADE' "
        "ORDER BY symbol"
    )
    rows = cur.fetchall()
    print(f"  Found {len(rows)} stale PREOPEN_GAP_FADE zero-qty MIS rows:")
    for (sym,) in rows:
        print(f"    {sym}")

    if not rows:
        print("  Nothing to delete — already clean.")
        return

    cur.execute(
        "DELETE FROM sandbox_positions "
        "WHERE quantity = 0 AND product = 'MIS' AND strategy = 'PREOPEN_GAP_FADE'"
    )
    deleted = cur.rowcount
    con.commit()
    print(f"\n  ✅ Deleted {deleted} rows. equity_obi can now trade those symbols.")


def fix_issue_2(con: sqlite3.Connection) -> None:
    """
    Recreate sandbox_positions with the correct UNIQUE constraint
    (user_id, symbol, exchange, product, strategy).

    This is safe but rebuilds the entire table — run after market hours.
    """
    cur = con.cursor()

    # Show current state
    cur.execute("SELECT COUNT(*) FROM sandbox_positions")
    total = cur.fetchone()[0]
    print(f"  sandbox_positions has {total} rows (will all be preserved).")

    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='sandbox_positions'")
    old_ddl = cur.fetchone()[0]
    print(f"  Current constraint: ", end="")
    for line in old_ddl.splitlines():
        if "UNIQUE" in line or "unique_position" in line:
            print(line.strip())

    # Build the new table
    print("\n  Rebuilding table with UNIQUE(user_id, symbol, exchange, product, strategy)...")
    cur.executescript("""
        BEGIN;

        CREATE TABLE sandbox_positions_new (
            id                      INTEGER NOT NULL,
            user_id                 VARCHAR(50) NOT NULL,
            symbol                  VARCHAR(50) NOT NULL,
            exchange                VARCHAR(20) NOT NULL,
            product                 VARCHAR(20) NOT NULL,
            quantity                INTEGER NOT NULL,
            average_price           DECIMAL(10,2) NOT NULL,
            ltp                     DECIMAL(10,2),
            pnl                     DECIMAL(10,2),
            pnl_percent             DECIMAL(10,4),
            accumulated_realized_pnl DECIMAL(10,2),
            today_realized_pnl      DECIMAL(10,2),
            margin_blocked          DECIMAL(15,2),
            created_at              DATETIME NOT NULL,
            updated_at              DATETIME NOT NULL,
            strategy                VARCHAR(100),
            sl                      DECIMAL(10,2),
            target                  DECIMAL(10,2),
            PRIMARY KEY (id),
            CONSTRAINT unique_position UNIQUE (user_id, symbol, exchange, product, strategy)
        );

        INSERT INTO sandbox_positions_new
            SELECT id, user_id, symbol, exchange, product, quantity, average_price,
                   ltp, pnl, pnl_percent, accumulated_realized_pnl, today_realized_pnl,
                   margin_blocked, created_at, updated_at, strategy, sl, target
            FROM sandbox_positions;

        DROP TABLE sandbox_positions;

        ALTER TABLE sandbox_positions_new RENAME TO sandbox_positions;

        CREATE INDEX IF NOT EXISTS ix_sandbox_positions_user_id
            ON sandbox_positions (user_id);
        CREATE INDEX IF NOT EXISTS ix_sandbox_positions_symbol
            ON sandbox_positions (symbol);
        CREATE INDEX IF NOT EXISTS ix_sandbox_positions_exchange
            ON sandbox_positions (exchange);
        CREATE INDEX IF NOT EXISTS ix_sandbox_positions_strategy
            ON sandbox_positions (strategy);
        CREATE INDEX IF NOT EXISTS idx_user_product
            ON sandbox_positions (user_id, product);

        COMMIT;
    """)
    print("  ✅ Table rebuilt. New constraint: UNIQUE(user_id, symbol, exchange, product, strategy)")
    print("  ✅ All existing rows preserved.")
    print()
    print("  ⚠️  Restart OpenAlgo (uv run app.py) so SQLAlchemy picks up the new schema.")


def main():
    parser = argparse.ArgumentParser(description="Fix sandbox_positions issues")
    parser.add_argument(
        "--issue",
        choices=["1", "2", "both"],
        required=True,
        help="Which fix to apply (1=stale rows, 2=unique constraint, both=both)",
    )
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"❌ Cannot find sandbox.db at {DB_PATH}")
        return

    print(f"Connecting to {DB_PATH} ...")
    con = connect_with_retry(DB_PATH)

    if args.issue in ("1", "both"):
        print("\n── Fix 1: Remove stale zero-qty PREOPEN_GAP_FADE rows ──")
        fix_issue_1(con)

    if args.issue in ("2", "both"):
        print("\n── Fix 2: Rebuild sandbox_positions with correct UNIQUE constraint ──")
        fix_issue_2(con)

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
