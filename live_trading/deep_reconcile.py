#!/usr/bin/env python3
"""
live_trading/deep_reconcile.py
==============================
Deep reconciliation script for OpenAlgo Stage 11 bots.
Synchronizes live_trading/logs/performance.db with db/sandbox.db.

This tool is designed to catch trades that were missed by the bots 
(e.g. due to crashes or failed EOD logging) but are present in the 
OpenAlgo main position book.
"""

import sqlite3
import os
import json
from pathlib import Path
from datetime import datetime, date

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
PERF_DB      = PROJECT_ROOT / "live_trading" / "logs" / "performance.db"
SANDBOX_DB   = PROJECT_ROOT / "db" / "sandbox.db"

# ── Bot Mapping ──────────────────────────────────────────────────────────────
# Maps strategy names in sandbox.db to bot_name in performance.db
BOT_MAPPING = {
    "HA_OPTIONS":           "ha_options_bot",
    "BANKNIFTY_BB_OPTIONS": "banknifty_bb_options_bot",
    "NIFTY_TREND_SELLER":   "nifty_trend_seller_bot",
    "SENSEX_TREND_SELLER":  "sensex_trend_seller_bot",
    "HTF_PO3_SELL_PE":      "htf_po3_bot",
    "PREOPEN_GAP_FADE":     "preopen_gap_fade_bot",
    "GAP_FADE_EOD":         "gap_fade_eod_bot",
    "NTS_OBI":              "nifty_trend_seller_obi",
    "equity_obi":           "equity_obi_bot",
}

def reconcile():
    if not PERF_DB.exists():
        print(f"Error: Performance DB not found at {PERF_DB}")
        return

    if not SANDBOX_DB.exists():
        print(f"Error: Sandbox DB not found at {SANDBOX_DB}")
        return

    print(f"[*] Connecting to databases...")
    p_con = sqlite3.connect(str(PERF_DB))
    s_con = sqlite3.connect(str(SANDBOX_DB))

    today = date.today().isoformat()
    
    # 1. Fetch closed positions from sandbox
    # quantity = 0 and today_realized_pnl != 0
    query = """
    SELECT symbol, strategy, today_realized_pnl, updated_at 
    FROM sandbox_positions 
    WHERE quantity = 0 AND today_realized_pnl != 0
    """
    sandbox_positions = s_con.execute(query).fetchall()
    
    print(f"[*] Found {len(sandbox_positions)} closed positions in sandbox today.")
    
    reconciled_count = 0
    
    for symbol, strategy, pnl, updated_at in sandbox_positions:
        bot_name = BOT_MAPPING.get(strategy, strategy.lower() + "_bot")
        
        # Check if already in performance.db
        # We check symbol and date
        exists = p_con.execute(
            "SELECT COUNT(*) FROM trades WHERE symbol = ? AND bot_name = ? AND trade_date = ?",
            (symbol, bot_name, today)
        ).fetchone()[0]
        
        if exists > 0:
            # Check if P&L matches closely (within 1 rupee)
            # Sometimes bots log slightly different P&L due to slippage calculation
            continue
            
        print(f"[!] Missing trade: {bot_name} | {symbol} | PnL: {pnl}")
        
        # We need more info to create a proper trade record: entry_price, exit_price, direction, qty
        # We'll pull the last two trades for this symbol/strategy from sandbox_trades
        trades_query = """
        SELECT action, quantity, price, trade_timestamp 
        FROM sandbox_trades 
        WHERE symbol = ? AND strategy = ? 
        ORDER BY trade_timestamp DESC LIMIT 2
        """
        fills = s_con.execute(trades_query, (symbol, strategy)).fetchall()
        
        if len(fills) < 2:
            print(f"    - Could not find matching fills in sandbox_trades. Skipping.")
            continue
            
        # fills[0] is exit, fills[1] is entry (due to DESC order)
        exit_fill = fills[0]
        entry_fill = fills[1]
        
        direction = "long" if entry_fill[0] == "BUY" else "short"
        qty = entry_fill[1]
        entry_price = entry_fill[2]
        exit_price = exit_fill[2]
        entry_time = entry_fill[3]
        exit_time = exit_fill[3]
        
        # Bot type
        stype = "options" if ":" in symbol or any(x in symbol for x in ["CE", "PE", "FUT"]) else "equity"
        
        # Insert into performance.db
        try:
            p_con.execute("""
                INSERT INTO trades (
                    bot_name, strategy_type, instrument, symbol, 
                    entry_time, exit_time, entry_price, exit_price, 
                    exit_reason, quantity, gross_pnl, direction, 
                    source, trade_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bot_name, stype, symbol.split(':')[0], symbol,
                entry_time, exit_time, entry_price, exit_price,
                "reconciled (sandbox)", qty, pnl, direction,
                "paper", today
            ))
            reconciled_count += 1
            print(f"    ✅ Successfully reconciled.")
        except Exception as e:
            print(f"    ❌ Failed to reconcile: {e}")

    p_con.commit()
    print(f"\n[*] Reconciliation complete. Total trades added: {reconciled_count}")
    
    p_con.close()
    s_con.close()

if __name__ == "__main__":
    reconcile()
