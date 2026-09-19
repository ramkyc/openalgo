"""
live_trading/nifty_microcap_screener/rebalance_db.py
======================================================
Permanent local ledger for the NIFTY Microcap Screener's monthly rotation.

Separate from performance.db (completed-trade P&L ledger, shared by every
bot) and from nifty_microcap_screener_state.json (mutable day-to-day cache
consumed by the dashboard). This DB is the source of truth for two things
that must survive forever, per the strategy's actual monthly cross-sectional
mechanics:

  1. monthly_target_lists — the FULL top-15% list as computed on each
     confirmed month-end signal date, one row per (rebalance_month, symbol).
     Written exactly once per month, the moment the scan cycle can prove the
     prior month is over (see nifty_microcap_screener.py's finalize-on-
     rollover logic). Never overwritten afterwards.

  2. positions — one row per holding period (open -> closed). A row is
     opened only when the user clicks "Confirm" on the dashboard with a
     manually-entered actual fill price/qty, and closed only when the user
     clicks "Confirm Exit" with a manually-entered actual exit price. This
     is the record consulted at the NEXT month-end to determine which
     currently-open names dropped out of the new top-15% (exit candidates)
     and which new top-15% names are not yet held (entry candidates) --
     the two-list diff described in the module docstring of
     nifty_microcap_screener.py.

  3. price_anomalies — one row per single-day close-ratio outlier flagged by
     nifty_microcap_screener.py's detect_price_anomalies(), scanned across
     the FULL 250-symbol universe (not just the top-15% cut) over each
     finalized month's 12-1 momentum window. Written once per month
     alongside monthly_target_lists, at the same finalize-on-rollover point.
     Informational only -- see that function's docstring for what a flag
     does and does not mean.

  4. capital_ledger — the NAV-style book_value tracker. One 'seed' row
     (SCREEN_CAPITAL, inserted once via ensure_capital_seeded()), one
     'realized_pnl' row per confirmed exit (booked inside confirm_exit(),
     signed +gain/-loss), and one 'withdrawal' row per confirmed SWP
     payout (signed negative, at most one per rebalance_month). book_value
     at any moment is simply the running sum -- get_book_value() -- so
     gains/losses compound into next month's target_rupee sizing and
     withdrawals shrink it, unlike the old hardcoded SCREEN_CAPITAL reset.

SQLite, WAL mode -- same pattern as live_trading/shared/performance_db.py.
DB path: live_trading/logs/nifty_microcap_screener.db
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_DB_PATH = Path(__file__).parent.parent / "logs" / "nifty_microcap_screener.db"
_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

_DDL_TARGET_LISTS = """
CREATE TABLE IF NOT EXISTS monthly_target_lists (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    rebalance_month     TEXT NOT NULL,   -- 'YYYY-MM' -- month positions are ENTERED in
    signal_date         TEXT NOT NULL,   -- 'YYYY-MM-DD' -- the confirmed month-end close this list was ranked on
    finalized_at        TEXT NOT NULL,   -- ISO datetime this row was first persisted
    symbol              TEXT NOT NULL,
    momentum_12m1m_pct  REAL,
    ref_price           REAL,
    target_rupee        REAL,
    shares              INTEGER,
    n_eligible          INTEGER,
    n_top               INTEGER,
    UNIQUE (rebalance_month, symbol)
)
"""

_DDL_ANOMALIES = """
CREATE TABLE IF NOT EXISTS price_anomalies (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    rebalance_month     TEXT NOT NULL,   -- 'YYYY-MM' -- same month key as monthly_target_lists
    signal_date         TEXT NOT NULL,   -- 'YYYY-MM-DD' -- month-end this scan was run against
    detected_at         TEXT NOT NULL,   -- ISO datetime this row was first persisted
    symbol              TEXT NOT NULL,
    anomaly_date        TEXT NOT NULL,   -- 'YYYY-MM-DD' -- the flagged single-day move
    prev_close          REAL,
    close                REAL,
    ratio_pct           REAL,            -- e.g. +250.0 for a 3.5x jump, -70.0 for a crash
    UNIQUE (rebalance_month, symbol, anomaly_date)
)
"""

_DDL_POSITIONS = """
CREATE TABLE IF NOT EXISTS positions (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                 TEXT NOT NULL,
    status                 TEXT NOT NULL DEFAULT 'open',  -- 'open' | 'closed'
    entry_month            TEXT NOT NULL,   -- 'YYYY-MM' rebalance month this entry belongs to
    entry_signal_date      TEXT,            -- month-end signal date behind this entry
    entry_date             TEXT,            -- date user actually confirmed the fill
    entry_price            REAL,            -- MANUALLY entered actual fill price
    qty                    INTEGER,
    momentum_at_entry_pct  REAL,
    exit_month             TEXT,            -- 'YYYY-MM' rebalance month this position dropped out / was closed
    exit_signal_date       TEXT,
    exit_date              TEXT,
    exit_price             REAL,            -- MANUALLY entered actual fill price
    exit_reason            TEXT,
    confirmed_at           TEXT,            -- entry confirm timestamp
    closed_at              TEXT,            -- exit confirm timestamp
    created_at             TEXT DEFAULT (datetime('now'))
)
"""

_DDL_CAPITAL_LEDGER = """
CREATE TABLE IF NOT EXISTS capital_ledger (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type      TEXT NOT NULL,   -- 'seed' | 'realized_pnl' | 'withdrawal'
    amount          REAL NOT NULL,   -- signed delta to book_value: +gain, -loss, -withdrawal
    symbol          TEXT,            -- realized_pnl rows only
    rebalance_month TEXT,            -- withdrawal rows only, 'YYYY-MM'
    note            TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
)
"""


_ddl_ready = False


def _get_connection() -> sqlite3.Connection:
    global _ddl_ready
    con = sqlite3.connect(str(_DB_PATH), timeout=30, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    if not _ddl_ready:
        con.execute(_DDL_TARGET_LISTS)
        con.execute(_DDL_ANOMALIES)
        con.execute(_DDL_POSITIONS)
        con.execute(_DDL_CAPITAL_LEDGER)
        _ddl_ready = True
    return con


def get_db_path() -> Path:
    return _DB_PATH


# ══════════════════════════════════════════════════════════════════════════════
# monthly_target_lists — write-once-per-month, forever
# ══════════════════════════════════════════════════════════════════════════════

def has_target_list_for(signal_date: str) -> bool:
    """signal_date: 'YYYY-MM-DD'. True if this month-end has already been persisted."""
    con = _get_connection()
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM monthly_target_lists WHERE signal_date = ?",
            [signal_date],
        ).fetchone()[0]
        return n > 0
    finally:
        con.close()


def record_monthly_target_list(rebalance_month: str, signal_date: str,
                                rows: list[dict], n_eligible: int, n_top: int) -> bool:
    """Persist the full finalized top-15% list. Idempotent: if signal_date is
    already recorded, does nothing and returns False. Returns True on a fresh
    insert."""
    if has_target_list_for(signal_date):
        return False
    con = _get_connection()
    try:
        finalized_at = datetime.now().isoformat(timespec="seconds")
        con.executemany(
            """
            INSERT OR IGNORE INTO monthly_target_lists
              (rebalance_month, signal_date, finalized_at, symbol,
               momentum_12m1m_pct, ref_price, target_rupee, shares,
               n_eligible, n_top)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            [
                (rebalance_month, signal_date, finalized_at, r["symbol"],
                 r.get("momentum_12m1m_pct"), r.get("ref_price"),
                 r.get("target_rupee"), r.get("shares"), n_eligible, n_top)
                for r in rows
            ],
        )
        con.commit()
        logger.info(f"[rebalance_db] Persisted finalized target list: "
                    f"rebalance_month={rebalance_month} signal_date={signal_date} "
                    f"n_top={n_top}")
        return True
    finally:
        con.close()


def get_latest_target_list() -> dict | None:
    """Most recently finalized month-end list, or None if none persisted yet."""
    con = _get_connection()
    try:
        head = con.execute(
            """
            SELECT rebalance_month, signal_date, finalized_at, n_eligible, n_top
            FROM monthly_target_lists
            ORDER BY signal_date DESC, id DESC LIMIT 1
            """
        ).fetchone()
        if not head:
            return None
        rebalance_month, signal_date, finalized_at, n_eligible, n_top = head
        rows = con.execute(
            """
            SELECT symbol, momentum_12m1m_pct, ref_price, target_rupee, shares
            FROM monthly_target_lists
            WHERE rebalance_month = ? AND signal_date = ?
            ORDER BY momentum_12m1m_pct DESC
            """,
            [rebalance_month, signal_date],
        ).fetchall()
        candidates = [
            {
                "symbol": r[0], "momentum_12m1m_pct": r[1], "ref_price": r[2],
                "target_rupee": r[3], "shares": r[4],
            }
            for r in rows
        ]
        return {
            "rebalance_month": rebalance_month,
            "signal_date": signal_date,
            "finalized_at": finalized_at,
            "n_eligible": n_eligible,
            "n_top": n_top,
            "candidates": candidates,
        }
    finally:
        con.close()


def list_target_list_history() -> list[dict]:
    """One summary row per finalized rebalance_month, oldest first."""
    con = _get_connection()
    try:
        rows = con.execute(
            """
            SELECT rebalance_month, signal_date, finalized_at,
                   COUNT(*) AS n_names, n_eligible, n_top
            FROM monthly_target_lists
            GROUP BY rebalance_month, signal_date
            ORDER BY signal_date ASC
            """
        ).fetchall()
        return [
            {
                "rebalance_month": r[0], "signal_date": r[1], "finalized_at": r[2],
                "n_names": r[3], "n_eligible": r[4], "n_top": r[5],
            }
            for r in rows
        ]
    finally:
        con.close()


# ══════════════════════════════════════════════════════════════════════════════
# price_anomalies — universe-wide outlier scan, write-once-per-month, forever
# ══════════════════════════════════════════════════════════════════════════════

def record_price_anomalies(rebalance_month: str, signal_date: str,
                            anomalies: list[dict]) -> int:
    """Persist flagged single-day price moves for this month's finalized
    scan. Idempotent per (rebalance_month, symbol, anomaly_date) -- safe to
    call even if this month was already recorded. Returns rows inserted
    (0 if anomalies is empty or all were already present)."""
    if not anomalies:
        return 0
    con = _get_connection()
    try:
        before = con.total_changes
        detected_at = datetime.now().isoformat(timespec="seconds")
        con.executemany(
            """
            INSERT OR IGNORE INTO price_anomalies
              (rebalance_month, signal_date, detected_at, symbol, anomaly_date,
               prev_close, close, ratio_pct)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            [
                (rebalance_month, signal_date, detected_at, a["symbol"], a["date"],
                 a.get("prev_close"), a.get("close"), a.get("ratio_pct"))
                for a in anomalies
            ],
        )
        con.commit()
        inserted = con.total_changes - before
        logger.info(f"[rebalance_db] Persisted {inserted} price anomaly row(s): "
                    f"rebalance_month={rebalance_month} signal_date={signal_date}")
        return inserted
    finally:
        con.close()


def get_anomalies_for(rebalance_month: str) -> list[dict]:
    """All flagged single-day moves recorded for this rebalance month."""
    con = _get_connection()
    try:
        rows = con.execute(
            """
            SELECT symbol, anomaly_date, prev_close, close, ratio_pct
            FROM price_anomalies
            WHERE rebalance_month = ?
            ORDER BY symbol ASC, anomaly_date ASC
            """,
            [rebalance_month],
        ).fetchall()
        return [
            {"symbol": r[0], "date": r[1], "prev_close": r[2], "close": r[3], "ratio_pct": r[4]}
            for r in rows
        ]
    finally:
        con.close()


# ══════════════════════════════════════════════════════════════════════════════
# positions — open/closed ledger, forever
# ══════════════════════════════════════════════════════════════════════════════

def list_open_positions() -> list[dict]:
    con = _get_connection()
    try:
        rows = con.execute(
            """
            SELECT symbol, entry_price, qty, entry_date, entry_month,
                   entry_signal_date, momentum_at_entry_pct
            FROM positions WHERE status = 'open'
            ORDER BY entry_date ASC
            """
        ).fetchall()
        return [
            {
                "symbol": r[0], "entry_price": r[1], "qty": r[2],
                "since": r[3],   # kept as "since" for dashboard-render compatibility
                "entry_month": r[4], "entry_signal_date": r[5],
                "momentum_at_entry_pct": r[6],
            }
            for r in rows
        ]
    finally:
        con.close()


def is_open(symbol: str) -> bool:
    con = _get_connection()
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM positions WHERE symbol = ? AND status = 'open'",
            [symbol],
        ).fetchone()[0]
        return n > 0
    finally:
        con.close()


def confirm_entry(symbol: str, entry_month: str, entry_signal_date: str,
                   entry_price: float, qty: int,
                   momentum_at_entry_pct: float | None = None) -> bool:
    """Open a new position with a MANUALLY entered actual fill price/qty.
    No-op (returns False) if this symbol already has an open position --
    a name that stays in the top-15% across months is never re-entered."""
    if entry_price <= 0 or qty <= 0:
        return False
    if is_open(symbol):
        return False
    con = _get_connection()
    try:
        now = datetime.now()
        con.execute(
            """
            INSERT INTO positions
              (symbol, status, entry_month, entry_signal_date, entry_date,
               entry_price, qty, momentum_at_entry_pct, confirmed_at)
            VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?)
            """,
            [symbol, entry_month, entry_signal_date,
             now.strftime("%Y-%m-%d"), entry_price, qty,
             momentum_at_entry_pct, now.isoformat(timespec="seconds")],
        )
        con.commit()
        logger.info(f"[rebalance_db] Confirmed entry: {symbol} qty={qty} "
                    f"price={entry_price} month={entry_month}")
        return True
    finally:
        con.close()


def confirm_exit(symbol: str, exit_month: str, exit_signal_date: str,
                  exit_price: float, exit_reason: str) -> dict | None:
    """Close the open position for symbol with a MANUALLY entered actual
    exit fill price. Returns the closed row's entry data (for performance.db
    logging) or None if there was no open position to close."""
    con = _get_connection()
    try:
        row = con.execute(
            "SELECT id, entry_price, qty, entry_date FROM positions "
            "WHERE symbol = ? AND status = 'open' ORDER BY id DESC LIMIT 1",
            [symbol],
        ).fetchone()
        if not row:
            return None
        pos_id, entry_price, qty, entry_date = row
        now = datetime.now()
        con.execute(
            """
            UPDATE positions SET
                status = 'closed', exit_month = ?, exit_signal_date = ?,
                exit_date = ?, exit_price = ?, exit_reason = ?, closed_at = ?
            WHERE id = ?
            """,
            [exit_month, exit_signal_date, now.strftime("%Y-%m-%d"),
             exit_price, exit_reason, now.isoformat(timespec="seconds"), pos_id],
        )
        realized = (exit_price - entry_price) * qty
        con.execute(
            """
            INSERT INTO capital_ledger (event_type, amount, symbol, note)
            VALUES ('realized_pnl', ?, ?, ?)
            """,
            [realized, symbol,
             f"closed {symbol}: entry Rs.{entry_price} -> exit Rs.{exit_price} x{qty}"],
        )
        con.commit()
        logger.info(f"[rebalance_db] Confirmed exit: {symbol} price={exit_price} "
                    f"month={exit_month} realized_pnl={realized:,.0f}")
        return {"symbol": symbol, "entry_price": entry_price, "qty": qty,
                "entry_date": entry_date}
    finally:
        con.close()


def list_position_history(symbol: str | None = None) -> list[dict]:
    """Full open+closed ledger, most recent first. Optionally filtered to one symbol."""
    con = _get_connection()
    try:
        q = """
            SELECT symbol, status, entry_month, entry_date, entry_price, qty,
                   exit_month, exit_date, exit_price, exit_reason
            FROM positions
            """
        params: list = []
        if symbol:
            q += " WHERE symbol = ?"
            params.append(symbol)
        q += " ORDER BY id DESC"
        rows = con.execute(q, params).fetchall()
        return [
            {
                "symbol": r[0], "status": r[1], "entry_month": r[2],
                "entry_date": r[3], "entry_price": r[4], "qty": r[5],
                "exit_month": r[6], "exit_date": r[7], "exit_price": r[8],
                "exit_reason": r[9],
            }
            for r in rows
        ]
    finally:
        con.close()


# ══════════════════════════════════════════════════════════════════════════════
# capital_ledger — NAV-style book_value: seed + realized P&L - withdrawals
# ══════════════════════════════════════════════════════════════════════════════

def ensure_capital_seeded(seed_amount: float) -> bool:
    """Insert the one-time 'seed' row if the ledger is empty. Idempotent --
    safe to call on every scan cycle. Returns True if a seed row was just
    inserted, False if the ledger was already seeded."""
    con = _get_connection()
    try:
        n = con.execute("SELECT COUNT(*) FROM capital_ledger").fetchone()[0]
        if n > 0:
            return False
        con.execute(
            "INSERT INTO capital_ledger (event_type, amount, note) "
            "VALUES ('seed', ?, 'initial book value')",
            [seed_amount],
        )
        con.commit()
        logger.info(f"[rebalance_db] Capital ledger seeded with Rs.{seed_amount:,.0f}")
        return True
    finally:
        con.close()


def get_book_value() -> float:
    """Current NAV -- seed + all realized P&L - all withdrawals to date."""
    con = _get_connection()
    try:
        row = con.execute("SELECT COALESCE(SUM(amount), 0) FROM capital_ledger").fetchone()
        return float(row[0])
    finally:
        con.close()


def has_withdrawal_for(rebalance_month: str) -> bool:
    """rebalance_month: 'YYYY-MM'. True if an SWP withdrawal is already recorded
    for this month."""
    con = _get_connection()
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM capital_ledger "
            "WHERE event_type = 'withdrawal' AND rebalance_month = ?",
            [rebalance_month],
        ).fetchone()[0]
        return n > 0
    finally:
        con.close()


def record_withdrawal(rebalance_month: str, amount: float, note: str | None = None) -> bool:
    """Log a manually-confirmed SWP withdrawal against book_value. Idempotent
    per rebalance_month -- a second call for the same month is a no-op
    (returns False). amount should be positive; stored as a negative delta."""
    if amount <= 0:
        return False
    if has_withdrawal_for(rebalance_month):
        return False
    con = _get_connection()
    try:
        con.execute(
            """
            INSERT INTO capital_ledger (event_type, amount, rebalance_month, note)
            VALUES ('withdrawal', ?, ?, ?)
            """,
            [-abs(amount), rebalance_month, note or f"SWP withdrawal for {rebalance_month}"],
        )
        con.commit()
        logger.info(f"[rebalance_db] Recorded SWP withdrawal: Rs.{amount:,.0f} "
                    f"month={rebalance_month}")
        return True
    finally:
        con.close()


def list_pending_swp_months() -> list[str]:
    """Every rebalance_month with a finalized target list but no recorded
    withdrawal yet, oldest first. The oldest entries here (if more than one)
    are genuinely overdue -- a later month finalized while an earlier one's
    SWP was never confirmed -- as distinct from the current month's routine
    "not yet confirmed" status, which is expected until the user actually
    withdraws."""
    con = _get_connection()
    try:
        rows = con.execute(
            """
            SELECT DISTINCT rebalance_month FROM monthly_target_lists
            WHERE rebalance_month NOT IN (
                SELECT rebalance_month FROM capital_ledger
                WHERE event_type = 'withdrawal' AND rebalance_month IS NOT NULL
            )
            ORDER BY rebalance_month ASC
            """
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


def list_capital_ledger() -> list[dict]:
    """Full capital_ledger history, oldest first -- seed + every realized P&L
    and withdrawal event, for a NAV-history display."""
    con = _get_connection()
    try:
        rows = con.execute(
            """
            SELECT event_type, amount, symbol, rebalance_month, note, created_at
            FROM capital_ledger
            ORDER BY id ASC
            """
        ).fetchall()
        return [
            {
                "event_type": r[0], "amount": r[1], "symbol": r[2],
                "rebalance_month": r[3], "note": r[4], "created_at": r[5],
            }
            for r in rows
        ]
    finally:
        con.close()
