"""ledger_sync: executor fills -> rebalance_db positions/capital_ledger (synthetic DB)."""
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from live_trading.shared import ledger_sync as ls  # noqa: E402

SCHEMA = """
CREATE TABLE monthly_target_lists (id INTEGER PRIMARY KEY, rebalance_month TEXT, signal_date TEXT, finalized_at TEXT,
  symbol TEXT, momentum_12m1m_pct REAL, ref_price REAL, target_rupee REAL, shares INTEGER, n_eligible INTEGER, n_top INTEGER);
CREATE TABLE positions (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, status TEXT DEFAULT 'open', entry_month TEXT,
  entry_signal_date TEXT, entry_date TEXT, entry_price REAL, qty INTEGER, momentum_at_entry_pct REAL, exit_month TEXT,
  exit_signal_date TEXT, exit_date TEXT, exit_price REAL, exit_reason TEXT, confirmed_at TEXT, closed_at TEXT, created_at TEXT);
CREATE TABLE capital_ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT, amount REAL, symbol TEXT,
  rebalance_month TEXT, note TEXT, created_at TEXT);
"""


def _setup(tmp_path, legs):
    db = tmp_path / "s.db"
    con = sqlite3.connect(db)
    con.executescript(SCHEMA)
    con.execute("INSERT INTO monthly_target_lists (rebalance_month, signal_date, symbol, momentum_12m1m_pct) VALUES ('2026-10','2026-09-30','NEW',50)")
    for sym, px, q in (("OUT", 100.0, 10), ("TRIM", 200.0, 10), ("KEEP", 50.0, 100), ("BAJAJ-AUTO", 9000.0, 1)):
        con.execute("INSERT INTO positions (symbol, entry_month, entry_date, entry_price, qty) VALUES (?, '2026-09', '2026-09-01', ?, ?)", (sym, px, q))
    con.commit()
    con.close()
    st = tmp_path / "2026-10-01_zerodha.json"
    st.write_text(json.dumps({"phase": "BUY", "legs": legs}))
    return db, st


def _leg(sym, side, qty, value, status="DONE", cur=0):
    return {"symbol": sym, "side": side, "qty": qty, "done": qty if not cur else 0, "cur_filled": cur,
            "status": status, "fill_value": value}


def test_apply_and_idempotent(tmp_path):
    legs = [_leg("OUT", "SELL", 10, 1200.0), _leg("TRIM", "SELL", 4, 1000.0),
            _leg("KEEP-BE", "BUY", 100, 6000.0), _leg("NEW", "BUY", 5, 500.0)]
    db, st = _setup(tmp_path, legs)
    s = ls.apply_executor_state(st, "zerodha", db, log_performance=False)
    assert len(s["closed"]) == 1 and len(s["trimmed"]) == 1 and len(s["opened"]) == 1 and len(s["topped_up"]) == 1
    assert round(s["realized"]) == 200 + 200          # OUT +20x10, TRIM +50x4
    con = sqlite3.connect(db)
    q = lambda sql: con.execute(sql).fetchall()
    assert q("SELECT qty, entry_price FROM positions WHERE symbol='KEEP' AND status='open'") == [(200, 55.0)]
    assert q("SELECT qty FROM positions WHERE symbol='TRIM' AND status='open'") == [(6,)]
    assert q("SELECT qty, exit_price FROM positions WHERE symbol='TRIM' AND status='closed'") == [(4, 250.0)]
    assert q("SELECT qty, entry_price, momentum_at_entry_pct FROM positions WHERE symbol='NEW'") == [(5, 100.0, 50.0)]
    assert q("SELECT qty FROM positions WHERE symbol='BAJAJ-AUTO'") == [(1,)]
    assert q("SELECT SUM(amount) FROM capital_ledger")[0][0] == 400.0
    s2 = ls.apply_executor_state(st, "zerodha", db, log_performance=False)
    assert s2["skipped"] == 4 and not s2["closed"] and not s2["opened"]
    assert q("SELECT COUNT(*) FROM positions")[0][0] == 6


def test_resumed_partial_applies_delta_only(tmp_path):
    db, st = _setup(tmp_path, [_leg("OUT", "SELL", 10, 500.0)])
    st.write_text(json.dumps({"phase": "SELL", "legs": [{**_leg("OUT", "SELL", 10, 500.0), "done": 5}]}))
    ls.apply_executor_state(st, "zerodha", db, log_performance=False)
    st.write_text(json.dumps({"phase": "SELL", "legs": [_leg("OUT", "SELL", 10, 1100.0)]}))
    ls.apply_executor_state(st, "zerodha", db, log_performance=False)
    con = sqlite3.connect(db)
    assert con.execute("SELECT qty FROM positions WHERE symbol='OUT' AND status='open'").fetchall() == []
    assert round(con.execute("SELECT SUM(amount) FROM capital_ledger").fetchone()[0]) == 100   # (100-100)*5 + (120-100)*5
    assert con.execute("SELECT COUNT(*) FROM fills").fetchone()[0] == 1


def test_dry_run_and_unfinished_order_not_applied(tmp_path):
    db, st = _setup(tmp_path, [_leg("OUT", "SELL", 10, 0.0, status="OPEN", cur=3)])
    s = ls.apply_executor_state(st, "zerodha", db, log_performance=False)
    assert not s["closed"] and s["warnings"]
    st.write_text(json.dumps({"phase": "SELL", "legs": [_leg("OUT", "SELL", 10, 1200.0)]}))
    ls.apply_executor_state(st, "zerodha", db, log_performance=False, dry_run=True)
    con = sqlite3.connect(db)
    assert con.execute("SELECT COUNT(*) FROM capital_ledger").fetchone()[0] == 0


def test_normalise():
    assert ls.normalise("DIACABS-BE") == "DIACABS" and ls.normalise("BAJAJ-AUTO") == "BAJAJ-AUTO"
