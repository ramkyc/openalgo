#!/usr/bin/env python3
"""
Backfill Equity OBI trades from trades.csv to performance.duckdb
"""
import csv
import sys
import os
from datetime import datetime, date
from pathlib import Path

# Add project root to path
ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from live_trading.shared.performance_db import log_trade as log_trade_to_db

LOG_DIR = Path(__file__).parent / "logs"
TRADES_CSV = LOG_DIR / "trades.csv"
STRATEGY_NAME = "equity_obi"

def backfill():
    if not TRADES_CSV.exists():
        print(f"File not found: {TRADES_CSV}")
        return

    today = date.today().isoformat()
    count = 0
    
    with open(TRADES_CSV, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["date"] == today:
                try:
                    entry_dt = datetime.combine(
                        date.fromisoformat(row["date"]),
                        datetime.strptime(row["signal_time"], "%H:%M:%S").time()
                    )
                    exit_dt = datetime.combine(
                        date.fromisoformat(row["date"]),
                        datetime.strptime(row["exit_time"], "%H:%M:%S").time()
                    )

                    log_trade_to_db(
                        bot_name      = STRATEGY_NAME,
                        strategy_type = "equity",
                        instrument    = row["symbol"],
                        symbol        = row["symbol"],
                        entry_time    = entry_dt,
                        exit_time     = exit_dt,
                        entry_price   = float(row["entry_price"]),
                        exit_price    = float(row["exit_price"]),
                        exit_reason   = row["exit_reason"],
                        quantity      = int(row["shares"]),
                        gross_pnl     = float(row["pnl_gross"]),
                        direction     = row["direction"].lower(),
                        source        = "paper", # Defaulting to paper
                    )
                    print(f"Backfilled: {row['symbol']} {row['direction']} | PnL: {row['pnl_gross']}")
                    count += 1
                except Exception as e:
                    print(f"Error backfilling row: {e}")

    print(f"\nSuccessfully backfilled {count} trades for {today}.")

if __name__ == "__main__":
    backfill()
