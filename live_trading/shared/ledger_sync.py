"""
live_trading/shared/ledger_sync.py
==================================
Apply the rebalance executor's real fills to a momentum screener's rebalance_db
ledger (positions + capital_ledger), so the screener MTM/NAV reflects what was
actually traded instead of waiting for manual Confirm clicks.

Source of truth is the executor state file written by portfolio_tracker
(`data/rebalance/<YYYY-MM-DD>_<broker>.json`: per leg `filled = done + cur_filled`,
`fill_value` = sum of filled x avg over finished orders). Stdlib only, so
portfolio_tracker can call it with any interpreter.

  python3 ledger_sync.py --state <executor.json> [--broker zerodha|dhan] [--dry-run]
  python3 ledger_sync.py --check zerodha          # ledger vs broker snapshot drift

Semantics
  * Idempotent. `fills` holds, per (run_date, broker, symbol, side), the qty/value
    already applied; a re-run applies only the delta (a resumed partial leg).
  * SELL: closes the open position, or splits off a closed child row for the sold
    part and shrinks the open row (trims). Realised P&L = (sell avg - entry price)
    x qty sold -> capital_ledger 'realized_pnl'. Brokerage/STT are not included,
    same as the manual Confirm Exit.
  * BUY: opens a position, or tops up an open one (qty added, entry_price becomes
    the weighted average).
  * SELLs apply before BUYs. Executor symbols are normalised (series suffix
    such as -BE/-BZ stripped; BAJAJ-AUTO etc. untouched).
  * Never touches a position this run did not trade. Whole run is one transaction.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_LOGS = Path(__file__).resolve().parent.parent / "logs"
SCREENERS = {
    # broker -> (screener key, db file, performance-db bot name)
    "zerodha": ("nifty_microcap_screener", _LOGS / "nifty_microcap_screener.db"),
    "dhan": ("nifty50_screener", _LOGS / "nifty50_screener.db"),
}
_PORTFOLIO_DB = Path.home() / "Developer" / "portfolio_tracker" / "data" / "portfolio.db"

_SERIES = re.compile(r"-(BE|BZ|SM|ST|X1|XT|IL|GB|E1|N1|N2|W1|P1)$")

DDL_FILLS = """
CREATE TABLE IF NOT EXISTS fills (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date    TEXT NOT NULL,     -- executor run date 'YYYY-MM-DD'
    broker      TEXT NOT NULL,
    symbol      TEXT NOT NULL,     -- normalised (series suffix stripped)
    raw_symbol  TEXT,              -- as sent to the broker
    side        TEXT NOT NULL,     -- 'BUY' | 'SELL'
    qty         INTEGER NOT NULL,  -- cumulative shares applied to the ledger
    fill_value  REAL NOT NULL,     -- cumulative rupee value applied
    avg_price   REAL NOT NULL,
    applied_at  TEXT NOT NULL,
    note        TEXT,
    UNIQUE (run_date, broker, symbol, side)
)
"""


def normalise(symbol: str) -> str:
    return _SERIES.sub("", symbol.strip())


def _run_date(state_path: Path) -> str:
    m = re.match(r"(\d{4}-\d{2}-\d{2})_", state_path.name)
    if not m:
        raise ValueError(f"cannot read run date from state file name {state_path.name!r}")
    return m.group(1)


def _target_meta(con: sqlite3.Connection, month: str) -> tuple[str | None, dict]:
    rows = con.execute(
        "SELECT signal_date, symbol, momentum_12m1m_pct FROM monthly_target_lists WHERE rebalance_month = ?",
        [month]).fetchall()
    sig = rows[0][0] if rows else None
    return sig, {r[1]: r[2] for r in rows}


def apply_executor_state(state_path: str | Path, broker: str, db_path: str | Path | None = None,
                         log_performance: bool = True, dry_run: bool = False) -> dict:
    """Apply one executor state file. Returns a summary dict (also on dry_run,
    in which case nothing is committed)."""
    state_path = Path(state_path)
    screener, default_db = SCREENERS[broker]
    db_path = Path(db_path) if db_path else default_db
    run_date = _run_date(state_path)
    month = run_date[:7]
    legs = json.loads(state_path.read_text())["legs"]

    con = sqlite3.connect(str(db_path), timeout=30)
    con.execute("PRAGMA busy_timeout=10000")
    con.execute(DDL_FILLS)
    summary = {"run_date": run_date, "broker": broker, "closed": [], "trimmed": [], "opened": [],
               "topped_up": [], "realized": 0.0, "warnings": [], "skipped": 0}
    perf_rows: list[dict] = []
    try:
        con.execute("BEGIN IMMEDIATE")
        signal_date, momentum = _target_meta(con, month)
        # aggregate legs per (normalised symbol, side) -- a name may appear under two raw symbols
        agg: dict[tuple[str, str], dict] = {}
        for l in legs:
            filled = int(l.get("done", 0)) + int(l.get("cur_filled", 0))
            if filled <= 0:
                continue
            avg = float(l["fill_value"]) / int(l["done"]) if l.get("done") else 0.0
            value = float(l["fill_value"]) if l.get("done") else 0.0
            if int(l.get("cur_filled", 0)):
                summary["warnings"].append(f"{l['symbol']} {l['side']}: {l['cur_filled']} sh on an unfinished order not applied")
                filled = int(l["done"])
                if filled <= 0:
                    continue
            key = (normalise(l["symbol"]), l["side"])
            a = agg.setdefault(key, {"qty": 0, "value": 0.0, "raw": l["symbol"]})
            a["qty"] += filled
            a["value"] += value
        order = sorted(agg.items(), key=lambda kv: (kv[0][1] != "SELL", kv[0][0]))
        now_s = datetime.now().isoformat(timespec="seconds")
        for (sym, side), a in order:
            prev = con.execute("SELECT qty, fill_value FROM fills WHERE run_date=? AND broker=? AND symbol=? AND side=?",
                               [run_date, broker, sym, side]).fetchone()
            pq, pv = (prev or (0, 0.0))
            dq, dv = a["qty"] - pq, a["value"] - pv
            if dq <= 0:
                summary["skipped"] += 1
                continue
            px = dv / dq
            pos = con.execute("SELECT id, entry_price, qty, entry_date, entry_month, entry_signal_date, "
                              "momentum_at_entry_pct FROM positions WHERE symbol=? AND status='open' "
                              "ORDER BY id DESC LIMIT 1", [sym]).fetchone()
            if side == "SELL":
                if not pos:
                    summary["warnings"].append(f"SELL {sym} x{dq}: no open position in ledger (not applied)")
                    continue
                pid, epx, pqty, edate, emonth, esig, emom = pos
                sold = min(dq, pqty)
                if dq > pqty:
                    summary["warnings"].append(f"SELL {sym}: sold {dq} > ledger qty {pqty}; extra {dq - pqty} ignored")
                realized = (px - epx) * sold
                if sold >= pqty:
                    con.execute("UPDATE positions SET status='closed', exit_month=?, exit_signal_date=?, exit_date=?, "
                                "exit_price=?, exit_reason=?, closed_at=? WHERE id=?",
                                [month, signal_date, run_date, px, "Rebalance (executor fill)", now_s, pid])
                    summary["closed"].append((sym, sold, round(px, 2)))
                else:
                    con.execute("INSERT INTO positions (symbol, status, entry_month, entry_signal_date, entry_date, "
                                "entry_price, qty, momentum_at_entry_pct, exit_month, exit_signal_date, exit_date, "
                                "exit_price, exit_reason, confirmed_at, closed_at) "
                                "VALUES (?, 'closed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                [sym, emonth, esig, edate, epx, sold, emom, month, signal_date, run_date, px,
                                 "Trim to target size (executor fill)", now_s, now_s])
                    con.execute("UPDATE positions SET qty = qty - ? WHERE id = ?", [sold, pid])
                    summary["trimmed"].append((sym, sold, round(px, 2)))
                con.execute("INSERT INTO capital_ledger (event_type, amount, symbol, note) VALUES ('realized_pnl', ?, ?, ?)",
                            [realized, sym, f"executor {run_date} {broker}: sold {sold} {sym} @ {px:.2f}, entry {epx:.2f}"])
                summary["realized"] += realized
                perf_rows.append({"symbol": sym, "entry_date": edate, "entry_price": epx, "exit_price": px,
                                  "qty": sold, "exit_date": run_date, "screener": screener})
            else:  # BUY
                if pos:
                    pid, epx, pqty = pos[0], pos[1], pos[2]
                    nq = pqty + dq
                    navg = (epx * pqty + dv) / nq
                    con.execute("UPDATE positions SET qty=?, entry_price=? WHERE id=?", [nq, navg, pid])
                    summary["topped_up"].append((sym, dq, round(px, 2)))
                else:
                    con.execute("INSERT INTO positions (symbol, status, entry_month, entry_signal_date, entry_date, "
                                "entry_price, qty, momentum_at_entry_pct, confirmed_at) VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?)",
                                [sym, month, signal_date, run_date, px, dq, momentum.get(sym), now_s])
                    summary["opened"].append((sym, dq, round(px, 2)))
            con.execute("INSERT INTO fills (run_date, broker, symbol, raw_symbol, side, qty, fill_value, avg_price, applied_at) "
                        "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(run_date, broker, symbol, side) DO UPDATE SET "
                        "qty=excluded.qty, fill_value=excluded.fill_value, avg_price=excluded.fill_value*1.0/excluded.qty, "
                        "applied_at=excluded.applied_at",
                        [run_date, broker, sym, a["raw"], side, a["qty"], a["value"], a["value"] / a["qty"], now_s])
        if dry_run:
            con.rollback()
        else:
            con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

    if log_performance and not dry_run:
        _log_performance(perf_rows, summary)
    return summary


def _log_performance(rows: list[dict], summary: dict) -> None:
    """Mirror the manual Confirm Exit: one performance.db trade per sold lot."""
    if not rows:
        return
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
        from live_trading.shared.performance_db import log_trade
        for r in rows:
            log_trade(bot_name=r["screener"], strategy_type="equity", instrument=r["symbol"], symbol=r["symbol"],
                      entry_time=r["entry_date"], exit_time=r["exit_date"], entry_price=r["entry_price"],
                      exit_price=r["exit_price"], exit_reason="Rebalance (executor fill)", quantity=r["qty"],
                      gross_pnl=(r["exit_price"] - r["entry_price"]) * r["qty"], direction="long", source="live",
                      notes=f"{r['screener']} - executor fill synced")
    except Exception as e:  # never break the sync over the shared P&L log
        summary["warnings"].append(f"performance_db log failed: {e}")


def check_drift(broker: str, db_path: str | Path | None = None,
                portfolio_db: str | Path | None = None) -> dict:
    """Ledger open qty vs the latest portfolio.db broker snapshot (equity rows).
    Returns {'ok': bool, 'snapshot': ts, 'diffs': [(symbol, ledger_qty, broker_qty)]}."""
    _, default_db = SCREENERS[broker]
    con = sqlite3.connect(f"file:{Path(db_path or default_db)}?mode=ro", uri=True)
    ledger: dict[str, int] = {}
    for sym, q in con.execute("SELECT symbol, SUM(qty) FROM positions WHERE status='open' GROUP BY symbol"):
        ledger[sym] = int(q)
    con.close()
    p = sqlite3.connect(f"file:{Path(portfolio_db or _PORTFOLIO_DB)}?mode=ro", uri=True)
    ts = p.execute("SELECT MAX(snap_ts) FROM holdings WHERE broker=?", [broker]).fetchone()[0]
    held: dict[str, int] = {}
    for sym, q in p.execute("SELECT norm_symbol, qty FROM holdings WHERE broker=? AND snap_ts=? AND kind='equity'", [broker, ts]):
        held[normalise(sym)] = held.get(normalise(sym), 0) + int(q)
    p.close()
    diffs = [(s, ledger.get(s, 0), held.get(s, 0)) for s in sorted(set(ledger) | set(held))
             if ledger.get(s, 0) != held.get(s, 0)]
    return {"ok": not diffs, "snapshot": ts, "diffs": diffs}


def _alert(msg: str) -> None:
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
        from live_trading.shared.telegram_notifier import send_sync
        send_sync(msg.replace("*", "").replace("_", " ").replace("`", "").replace("[", "("))
    except Exception:
        pass


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--state", help="executor state JSON (<date>_<broker>.json)")
    ap.add_argument("--broker", choices=sorted(SCREENERS))
    ap.add_argument("--db", help="override screener DB path (testing on a copy)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-perf", action="store_true", help="skip performance.db logging")
    ap.add_argument("--check", choices=sorted(SCREENERS), help="report ledger-vs-broker quantity drift")
    a = ap.parse_args(argv)
    if a.check:
        r = check_drift(a.check, a.db)
        print(f"{a.check}: snapshot {r['snapshot']}  {'IN SYNC' if r['ok'] else 'DRIFT'}")
        for s, lq, bq in r["diffs"]:
            print(f"  {s:14s} ledger {lq:>7d}  broker {bq:>7d}")
        return 0 if r["ok"] else 1
    if not a.state:
        ap.error("--state or --check required")
    broker = a.broker or Path(a.state).stem.split("_", 1)[1]
    if broker not in SCREENERS:
        print(f"no screener ledger for broker {broker!r}; nothing to do")
        return 0
    try:
        s = apply_executor_state(a.state, broker, a.db, log_performance=not a.no_perf, dry_run=a.dry_run)
    except Exception as e:
        logger.exception("ledger sync failed")
        _alert(f"Ledger sync FAILED for {broker} ({Path(a.state).name}): {e}. "
               f"Rerun: python3 live_trading/shared/ledger_sync.py --state {a.state}")
        return 1
    tag = "DRY-RUN " if a.dry_run else ""
    print(f"{tag}{broker} {s['run_date']}: closed {len(s['closed'])}, trimmed {len(s['trimmed'])}, "
          f"opened {len(s['opened'])}, topped-up {len(s['topped_up'])}, realised Rs.{s['realized']:,.0f}, "
          f"already-applied {s['skipped']}")
    for w in s["warnings"]:
        print("  WARN", w)
    return 0


if __name__ == "__main__":
    sys.exit(main())
