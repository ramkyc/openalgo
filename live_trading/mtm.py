#!/usr/bin/env python3
import sqlite3
import json
import os
import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich import box

# Paths — derived from this file's location so the script works on any machine
_LT_DIR      = Path(__file__).resolve().parent          # live_trading/
PROJECT_ROOT  = _LT_DIR.parent                          # openalgo root
DB_PATH       = PROJECT_ROOT / "db" / "sandbox.db"
CB_STATE_FILE = _LT_DIR / "logs" / "cb_live_state.json"

console = Console()

def get_cb_state():
    if CB_STATE_FILE.exists():
        try:
            with open(CB_STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def main():
    if not DB_PATH.exists():
        console.print(f"[red]Error: Database not found at {DB_PATH}[/red]")
        return

    cb_state = get_cb_state()
    active_cb_trades = cb_state.get("active_trades", {})
    
    # Map symbols to SL/Target from CB state
    cb_lookup = {}
    for idx, types in active_cb_trades.items():
        for t, data in types.items():
            if data and data.get("symbol"):
                sym_key = data["symbol"].upper().strip()
                cb_lookup[sym_key] = {
                    "sl": data.get("sl"),
                    "target": data.get("target")
                }
    
    # Also fallback to strategy state setup for symbols that were trigger symbols
    cb_setups = {}
    state_data = cb_state.get("state", {})
    for idx, idx_data in state_data.items():
        for t in ["CE", "PE"]:
            opt = idx_data.get(t, {})
            if opt and opt.get("symbol"):
                sym_key = opt["symbol"].upper().strip()
                cb_setups[sym_key] = {
                    "sl": opt.get("prev_low"),
                    "target": None
                }

    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        cursor = conn.cursor()
        
        query = """
        SELECT 
            strategy, 
            symbol, 
            quantity, 
            average_price, 
            ltp, 
            CASE WHEN quantity = 0 THEN 0 ELSE pnl END as open_pnl, 
            today_realized_pnl
        FROM sandbox_positions 
        WHERE quantity != 0 OR today_realized_pnl != 0 
        ORDER BY strategy, symbol
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        table = Table(
            box=box.SIMPLE, 
            show_header=True, 
            header_style="bold cyan",
            expand=True
        )
        
        table.add_column("S", style="dim", width=4)
        table.add_column("Symbol", style="bold yellow", ratio=4, no_wrap=True)
        table.add_column("Q", justify="right", width=6)
        table.add_column("Avg", justify="right", width=8)
        table.add_column("SL", justify="right", style="bright_red", width=8)
        table.add_column("Tg", justify="right", style="bright_green", width=8)
        table.add_column("LTP", justify="right", width=8)
        table.add_column("OpPnL", justify="right", width=10)
        table.add_column("Real", justify="right", width=10)
        table.add_column("Tot", justify="right", style="bold", width=11)

        total_pnl = 0
        
        for row in rows:
            strat, sym, qty, avg, ltp, open_pnl, real_pnl = row
            total_row_pnl = open_pnl + real_pnl
            total_pnl += total_row_pnl
            
            # Shorten strategy name for display
            disp_strat = "CB" if "CANDLE_BREAKER" in strat.upper() else "ORB"
            
            # SL/Target detection logic
            sl = "-"
            target = "-"
            clean_sym = sym.strip().upper()
            
            if "CANDLE_BREAKER" in strat.upper():
                if clean_sym in cb_lookup:
                    d = cb_lookup[clean_sym]
                    sl = f"{d['sl']:.2f}" if d['sl'] else "-"
                    target = f"{d['target']:.2f}" if d['target'] else "-"
                elif clean_sym in cb_setups:
                    d = cb_setups[clean_sym]
                    sl = f"{d['sl']:.2f}" if d['sl'] else "-"
            
            elif "ORB_CHAMPION" in strat.upper():
                if qty != 0:
                    sl_val = avg * 0.8 if qty > 0 else avg * 1.2
                    sl = f"{sl_val:.2f}"
                    target = "EOD"
            
            # P&L and Color calculation
            pnl_style = "green" if total_row_pnl >= 0 else "red"
            
            table.add_row(
                disp_strat,
                sym,
                str(int(qty)),
                f"{avg:,.2f}",
                sl,
                target,
                f"{ltp:,.2f}",
                f"{open_pnl:,.0f}",
                f"{real_pnl:,.0f}",
                f"[{pnl_style}]{total_row_pnl:,.0f}[/]"
            )
            
        console.print(table)
        
        summary_color = "green" if total_pnl >= 0 else "red"
        console.print(f" [bold]TOTAL NET PnL: [{summary_color}]₹{total_pnl:,.2f}[/][/]\n")
        
        conn.close()
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")

if __name__ == "__main__":
    main()
