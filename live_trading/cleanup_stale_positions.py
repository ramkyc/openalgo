"""
One-shot script to delete the 3 stale HA_OPTIONS ghost positions from 2026-03-27.
Run from the openalgo project root:

    uv run live_trading/cleanup_stale_positions.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Bootstrap the Flask app context so SQLAlchemy models are available
from app import app
from database.sandbox_db import SandboxPositions

STALE_IDS = [31, 34, 38]
STALE_SYMBOLS = [
    "SENSEX02APR2674100PE",
    "BANKNIFTY28APR2652500PE",
    "BANKNIFTY28APR2652500CE",
]

with app.app_context():
    from database.sandbox_db import db_session

    deleted = (
        db_session.query(SandboxPositions)
        .filter(SandboxPositions.id.in_(STALE_IDS))
        .all()
    )

    if not deleted:
        print("Nothing to delete — positions may have already been removed.")
        sys.exit(0)

    print(f"Found {len(deleted)} stale position(s) to remove:")
    for p in deleted:
        print(f"  id={p.id}  {p.symbol:30s}  qty={p.quantity:+d}  strategy={p.strategy}")

    confirm = input("\nDelete these? [y/N] ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        sys.exit(0)

    for p in deleted:
        db_session.delete(p)
    db_session.commit()

    remaining = (
        db_session.query(SandboxPositions)
        .filter(SandboxPositions.quantity != 0)
        .count()
    )
    print(f"\n✅ Done. Open positions remaining in sandbox: {remaining}")
