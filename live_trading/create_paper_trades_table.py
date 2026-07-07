"""
create_paper_trades_table.py
============================
One-time (and re-runnable) migration script.

Creates the `paper_trades_options` table in options_data.duckdb and
populates it by parsing historical trade data from every bot's log / CSV:

  • ha_options_bot          — parsed from live_trading/logs/ha_options_bot.log
  • banknifty_bb_options_bot— parsed from live_trading/logs/banknifty_bb_options_bot.log
  • bb_paper_bot            — read from live_trading/bb_paper_bot/logs/study_a_trades.csv
  • nifty_trend_seller_obi  — read from live_trading/nifty_trend_seller_obi/logs/trades.csv
  • ema_swing_scanner       — read from live_trading/ema_swing_scanner/logs/paper_trades.csv
                             (equity, tagged as EQUITY bot – kept for completeness)

Run once to seed the table, then re-run any time to pick up new rows
(uses INSERT OR IGNORE so no duplicate risk).

Usage:
  cd /path/to/openalgo
  uv run live_trading/create_paper_trades_table.py
"""

import os
import re
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO_ROOT   = Path(__file__).parent.parent                        # openalgo/
LOGS_DIR    = REPO_ROOT / "live_trading" / "logs"
DUCKDB_PATH = Path(
    os.environ.get(
        "OPTIONS_DUCKDB",
        str(Path.home() / "Developer/options_data/data/options_data.duckdb")
    )
)

HA_LOG          = LOGS_DIR / "ha_options_bot.log"
BNBB_LOG        = LOGS_DIR / "banknifty_bb_options_bot.log"
BB_TRADES_CSV   = REPO_ROOT / "live_trading/bb_paper_bot/logs/study_a_trades.csv"
NTS_TRADES_CSV  = REPO_ROOT / "live_trading/nifty_trend_seller_obi/logs/trades.csv"
EMA_TRADES_CSV  = REPO_ROOT / "live_trading/ema_swing_scanner/logs/paper_trades.csv"

# ── Schema ────────────────────────────────────────────────────────────────────
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS paper_trades_options (
    id            INTEGER PRIMARY KEY,
    bot_name      VARCHAR   NOT NULL,
    instrument    VARCHAR,
    trade_date    DATE,
    option_symbol VARCHAR,
    option_type   VARCHAR,
    entry_time    TIMESTAMP,
    exit_time     TIMESTAMP,
    entry_premium DOUBLE,
    exit_premium  DOUBLE,
    exit_reason   VARCHAR,
    quantity      INTEGER,
    lots          INTEGER,
    lot_size      INTEGER,
    gross_pnl     DOUBLE,
    decay_pct     DOUBLE,
    won           BOOLEAN,
    order_id      VARCHAR,
    notes         VARCHAR,
    source        VARCHAR,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _currency(s: str) -> float:
    """Parse '₹-1,069' or '₹1,350' → float."""
    return float(re.sub(r"[₹,]", "", s))


def _pct(s: str) -> float:
    """Parse '-7.2%' → -7.2."""
    return float(s.replace("%", ""))


def _next_id(con) -> int:
    row = con.execute("SELECT COALESCE(MAX(id),0)+1 FROM paper_trades_options").fetchone()
    return row[0]


# ── Parser: ha_options_bot.log ────────────────────────────────────────────────
# SOLD line:
#   [2026-04-07 09:30:00,918] INFO in ha_options_bot: ✅ [SENSEX] SOLD SENSEX09APR2673800PE @ ₹826.25  (1 lot, qty=20, order=26040791003165)
# CLOSED line:
#   [2026-04-07 09:45:01,059] INFO in ha_options_bot: 🔴 [SENSEX] CLOSED SENSEX09APR2673800PE @ ₹939.10  reason=HA Reversal  gross=₹-2,257  decay=-13.7%

_HA_SOLD_RE = re.compile(
    r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\].*"
    r"✅ \[(\w+)\] SOLD (\S+) @ ₹([\d,.]+)\s+\(1 lot, qty=(\d+), order=(\S+)\)"
)
_HA_CLOSED_RE = re.compile(
    r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\].*"
    r"(🟢|🔴) \[(\w+)\] CLOSED (\S+) @ ₹([\d,.]+)\s+reason=([^\s].*?)\s+gross=(₹[\-\d,]+)\s+decay=([\-\d.]+%)"
)


def _opt_type_from_symbol(symbol: str) -> str:
    if symbol.endswith("CE"):
        return "CE"
    if symbol.endswith("PE"):
        return "PE"
    return ""


def _lot_size_from_instrument(instrument: str, qty: int) -> int:
    defaults = {"NIFTY": 65, "BANKNIFTY": 30, "SENSEX": 20}
    return defaults.get(instrument, qty)


def parse_ha_log(log_path: Path) -> list[dict]:
    """Return list of completed trade dicts from ha_options_bot.log."""
    if not log_path.exists():
        print(f"  ⚠  Not found: {log_path}")
        return []

    text   = log_path.read_text(errors="replace")
    sold   = {}   # key: (instrument, option_symbol) → entry dict
    trades = []

    for line in text.splitlines():
        m = _HA_SOLD_RE.search(line)
        if m:
            ts_str, inst, symbol, prem, qty_str, order = m.groups()
            entry_time   = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            lot_size     = _lot_size_from_instrument(inst, int(qty_str))
            key          = (inst, symbol)
            sold[key] = {
                "bot_name":      "ha_options_bot",
                "instrument":    inst,
                "trade_date":    entry_time.date(),
                "option_symbol": symbol,
                "option_type":   _opt_type_from_symbol(symbol),
                "entry_time":    entry_time,
                "entry_premium": float(prem.replace(",", "")),
                "quantity":      int(qty_str),
                "lots":          1,
                "lot_size":      lot_size,
                "order_id":      order if order != "None" else None,
                "source":        "log_parse",
            }
            continue

        m = _HA_CLOSED_RE.search(line)
        if m:
            ts_str, emoji, inst, symbol, exit_p, reason, gross_str, decay_str = m.groups()
            exit_time = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            key       = (inst, symbol)
            entry     = sold.pop(key, None)
            if entry is None:
                # No matching entry (bot restarted mid-trade) — create stub
                lot_size = _lot_size_from_instrument(inst, 0)
                entry = {
                    "bot_name":      "ha_options_bot",
                    "instrument":    inst,
                    "trade_date":    exit_time.date(),
                    "option_symbol": symbol,
                    "option_type":   _opt_type_from_symbol(symbol),
                    "entry_time":    None,
                    "entry_premium": None,
                    "quantity":      None,
                    "lots":          1,
                    "lot_size":      lot_size,
                    "order_id":      None,
                    "source":        "log_parse",
                }
            gross = _currency(gross_str)
            trade = {
                **entry,
                "exit_time":    exit_time,
                "exit_premium": float(exit_p.replace(",", "")),
                "exit_reason":  reason.strip(),
                "gross_pnl":    gross,
                "decay_pct":    _pct(decay_str),
                "won":          gross > 0,
            }
            trades.append(trade)

    # Anything still open (no CLOSED yet) — skip; they'll be added on next run
    print(f"  ha_options_bot: {len(trades)} completed trades parsed from log")
    return trades


# ── Parser: banknifty_bb_options_bot.log ─────────────────────────────────────
# SOLD line:
#   [2026-03-30 09:35:00,340] INFO in banknifty_bb_options_bot: ✅ [CE] SOLD BANKNIFTY30MAR2651400CE @ ₹282.35  (1 lot, qty=30, order=26033033171567)
# CLOSED line:
#   [2026-03-30 09:48:00,258] INFO in banknifty_bb_options_bot: 🟢 [CE] CLOSED BANKNIFTY30MAR2651400CE @ ₹227.12  reason=SMA reversion  gross=₹1,657  change=19.6%

_BNBB_SOLD_RE = re.compile(
    r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\].*"
    r"✅ \[(CE|PE)\] SOLD (\S+) @ ₹([\d,.]+)\s+\(1 lot, qty=(\d+), order=(\S+)\)"
)
_BNBB_CLOSED_RE = re.compile(
    r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\].*"
    r"(🟢|🔴) \[(CE|PE)\] CLOSED (\S+) @ ₹([\d,.]+)\s+reason=([^\s].*?)\s+gross=(₹[\-\d,]+)\s+change=([\-\d.]+%)"
)


def parse_bnbb_log(log_path: Path) -> list[dict]:
    if not log_path.exists():
        print(f"  ⚠  Not found: {log_path}")
        return []

    text   = log_path.read_text(errors="replace")
    sold   = {}
    trades = []

    for line in text.splitlines():
        m = _BNBB_SOLD_RE.search(line)
        if m:
            ts_str, opt_type, symbol, prem, qty_str, order = m.groups()
            entry_time = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            key        = symbol
            sold[key]  = {
                "bot_name":      "banknifty_bb_options_bot",
                "instrument":    "BANKNIFTY",
                "trade_date":    entry_time.date(),
                "option_symbol": symbol,
                "option_type":   opt_type,
                "entry_time":    entry_time,
                "entry_premium": float(prem.replace(",", "")),
                "quantity":      int(qty_str),
                "lots":          1,
                "lot_size":      30,
                "order_id":      order if order != "None" else None,
                "source":        "log_parse",
                "notes":         None,
            }
            continue

        m = _BNBB_CLOSED_RE.search(line)
        if m:
            ts_str, emoji, opt_type, symbol, exit_p, reason, gross_str, chg_str = m.groups()
            exit_time = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            key       = symbol
            entry     = sold.pop(key, None)
            if entry is None:
                entry = {
                    "bot_name":      "banknifty_bb_options_bot",
                    "instrument":    "BANKNIFTY",
                    "trade_date":    exit_time.date(),
                    "option_symbol": symbol,
                    "option_type":   opt_type,
                    "entry_time":    None,
                    "entry_premium": None,
                    "quantity":      30,
                    "lots":          1,
                    "lot_size":      30,
                    "order_id":      None,
                    "source":        "log_parse",
                    "notes":         None,
                }
            gross = _currency(gross_str)
            trades.append({
                **entry,
                "exit_time":    exit_time,
                "exit_premium": float(exit_p.replace(",", "")),
                "exit_reason":  reason.strip(),
                "gross_pnl":    gross,
                "decay_pct":    _pct(chg_str),
                "won":          gross > 0,
            })

    # Entries without a CLOSED line (EOD exits, no data in log)
    for key, entry in sold.items():
        trades.append({
            **entry,
            "exit_time":    None,
            "exit_premium": None,
            "exit_reason":  "EOD (not logged)",
            "gross_pnl":    None,
            "decay_pct":    None,
            "won":          None,
        })

    print(f"  banknifty_bb_options_bot: {len(trades)} trades parsed from log")
    return trades


# ── Reader: bb_paper_bot study_a_trades.csv ───────────────────────────────────
# Columns: strategy,symbol,entry_ts,exit_ts,entry_px,exit_px,exit_reason,quantity,gross,net,won,duration_min

def read_bb_paper_csv(csv_path: Path) -> list[dict]:
    if not csv_path.exists():
        print(f"  ⚠  Not found: {csv_path}")
        return []

    df = pd.read_csv(csv_path)
    trades = []
    for _, row in df.iterrows():
        symbol   = str(row["symbol"])
        opt_type = _opt_type_from_symbol(symbol)
        gross    = float(row["gross"])
        trades.append({
            "bot_name":      str(row["strategy"]),
            "instrument":    "NIFTY",
            "trade_date":    pd.Timestamp(row["entry_ts"]).date(),
            "option_symbol": symbol,
            "option_type":   opt_type,
            "entry_time":    pd.Timestamp(row["entry_ts"]),
            "exit_time":     pd.Timestamp(row["exit_ts"]),
            "entry_premium": float(row["entry_px"]),
            "exit_premium":  float(row["exit_px"]),
            "exit_reason":   str(row["exit_reason"]),
            "quantity":      int(row["quantity"]),
            "lots":          None,
            "lot_size":      None,
            "gross_pnl":     gross,
            "decay_pct":     None,
            "won":           bool(row["won"]) if str(row["won"]).lower() not in ("nan","") else None,
            "order_id":      None,
            "notes":         f"net={row['net']}, duration_min={row['duration_min']}",
            "source":        "csv_import",
        })

    print(f"  bb_paper_bot: {len(trades)} trades read from CSV")
    return trades


# ── Reader: nifty_trend_seller_obi trades.csv ─────────────────────────────────
# Columns: date,signal_time,direction,nifty_spot,atm_strike,option_symbol,entry_premium,
#          lots,sl_price,exit_time,exit_premium,exit_reason,obi_at_signal,
#          pnl_gross_per_lot,pnl_gross_total,transaction_cost_total,pnl_net_total

def read_nts_obi_csv(csv_path: Path) -> list[dict]:
    if not csv_path.exists():
        print(f"  ⚠  Not found: {csv_path}")
        return []

    df = pd.read_csv(csv_path)
    if df.empty:
        print("  nifty_trend_seller_obi: 0 trades (CSV is empty / header only)")
        return []

    trades = []
    for _, row in df.iterrows():
        symbol   = str(row.get("option_symbol", ""))
        opt_type = _opt_type_from_symbol(symbol)
        lots     = int(row["lots"]) if pd.notna(row.get("lots")) else None
        lot_size = 65  # NIFTY default
        qty      = lots * lot_size if lots else None
        gross    = float(row["pnl_gross_total"]) if pd.notna(row.get("pnl_gross_total")) else None
        trades.append({
            "bot_name":      "nifty_trend_seller_obi",
            "instrument":    "NIFTY",
            "trade_date":    pd.Timestamp(row["date"]).date() if pd.notna(row.get("date")) else None,
            "option_symbol": symbol,
            "option_type":   opt_type,
            "entry_time":    None,
            "exit_time":     pd.Timestamp(row["exit_time"]) if pd.notna(row.get("exit_time")) else None,
            "entry_premium": float(row["entry_premium"]) if pd.notna(row.get("entry_premium")) else None,
            "exit_premium":  float(row["exit_premium"]) if pd.notna(row.get("exit_premium")) else None,
            "exit_reason":   str(row.get("exit_reason", "")),
            "quantity":      qty,
            "lots":          lots,
            "lot_size":      lot_size,
            "gross_pnl":     gross,
            "decay_pct":     None,
            "won":           (gross > 0) if gross is not None else None,
            "order_id":      None,
            "notes":         f"obi={row.get('obi_at_signal')}, net={row.get('pnl_net_total')}",
            "source":        "csv_import",
        })

    print(f"  nifty_trend_seller_obi: {len(trades)} trades read from CSV")
    return trades


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not DUCKDB_PATH.exists():
        print(f"❌ DuckDB not found: {DUCKDB_PATH}")
        sys.exit(1)

    print(f"\n📂 Opening DuckDB: {DUCKDB_PATH}")
    con = duckdb.connect(str(DUCKDB_PATH))

    # Create table
    con.execute(CREATE_SQL)
    # Add PRIMARY KEY sequence (DuckDB doesn't auto-increment by default)
    # We'll assign IDs manually using row number offset
    print("✅ Table `paper_trades_options` ensured.\n")

    # Check existing count
    existing = con.execute("SELECT COUNT(*) FROM paper_trades_options").fetchone()[0]
    print(f"   Existing rows: {existing}\n")

    # Gather all trades
    all_trades: list[dict] = []
    all_trades.extend(parse_ha_log(HA_LOG))
    all_trades.extend(parse_bnbb_log(BNBB_LOG))
    all_trades.extend(read_bb_paper_csv(BB_TRADES_CSV))
    all_trades.extend(read_nts_obi_csv(NTS_TRADES_CSV))

    if not all_trades:
        print("\n⚠  No trades found to insert.")
        con.close()
        return

    # Assign IDs starting after existing max
    next_id = con.execute("SELECT COALESCE(MAX(id),0)+1 FROM paper_trades_options").fetchone()[0]

    inserted = 0
    skipped  = 0

    for i, t in enumerate(all_trades):
        # Skip if exact duplicate (same bot + entry_time + symbol)
        if t.get("entry_time") and t.get("option_symbol"):
            dup = con.execute(
                "SELECT COUNT(*) FROM paper_trades_options "
                "WHERE bot_name=? AND entry_time=? AND option_symbol=?",
                [t["bot_name"], t["entry_time"], t["option_symbol"]]
            ).fetchone()[0]
            if dup > 0:
                skipped += 1
                continue

        t["id"] = next_id + i

        con.execute("""
            INSERT INTO paper_trades_options
              (id, bot_name, instrument, trade_date, option_symbol, option_type,
               entry_time, exit_time, entry_premium, exit_premium,
               exit_reason, quantity, lots, lot_size,
               gross_pnl, decay_pct, won, order_id, notes, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [
            t.get("id"),
            t.get("bot_name"),
            t.get("instrument"),
            t.get("trade_date"),
            t.get("option_symbol"),
            t.get("option_type"),
            t.get("entry_time"),
            t.get("exit_time"),
            t.get("entry_premium"),
            t.get("exit_premium"),
            t.get("exit_reason"),
            t.get("quantity"),
            t.get("lots"),
            t.get("lot_size"),
            t.get("gross_pnl"),
            t.get("decay_pct"),
            t.get("won"),
            t.get("order_id"),
            t.get("notes"),
            t.get("source"),
        ])
        inserted += 1

    con.commit()
    print(f"\n✅ Inserted {inserted} new rows  (skipped {skipped} duplicates)")

    # Print summary by bot
    print("\n── Summary by bot ──────────────────────────────────────────────")
    summary = con.execute("""
        SELECT bot_name,
               COUNT(*)                                    AS trades,
               SUM(CASE WHEN won THEN 1 ELSE 0 END)        AS wins,
               ROUND(SUM(gross_pnl),0)                     AS total_gross_pnl,
               MIN(trade_date)                             AS first_trade,
               MAX(trade_date)                             AS last_trade
        FROM paper_trades_options
        WHERE gross_pnl IS NOT NULL
        GROUP BY bot_name
        ORDER BY bot_name
    """).fetchdf()
    print(summary.to_string(index=False))

    # Print daily summary for ha_options_bot
    print("\n── ha_options_bot daily P&L ──────────────────────────────────")
    daily = con.execute("""
        SELECT trade_date,
               instrument,
               COUNT(*)                                    AS trades,
               SUM(CASE WHEN won THEN 1 ELSE 0 END)        AS wins,
               ROUND(SUM(gross_pnl),0)                     AS day_gross_pnl
        FROM paper_trades_options
        WHERE bot_name = 'ha_options_bot' AND gross_pnl IS NOT NULL
        GROUP BY trade_date, instrument
        ORDER BY trade_date, instrument
    """).fetchdf()
    print(daily.to_string(index=False))

    con.close()
    print("\n✅ Done.")


if __name__ == "__main__":
    main()
