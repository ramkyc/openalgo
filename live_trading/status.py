import os
import re
import json
import time
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

_ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')

_STRATEGY_SHORT = {
    "ORB_CHAMPION_15M":    "ORB_15M",
    "ORB_CHAMPION_30M":    "ORB_30M",
    "CANDLE_BREAKER_LIVE": "CB_LIVE",
    "BB_OPTIONS_LIVE":     "BB_LIVE",
    "SUPERTREND_5S_LIVE":  "ST_5S",
    "NIFTY_TREND_SELLER":  "NTS",
}

# ── Gap Fade state file path ──────────────────────────────────────────────────
GAP_FADE_STATE_FILE = Path(__file__).parent / "logs" / "preopen_gap_fade_state.json"

def _shorten(name):
    return _STRATEGY_SHORT.get(name, name[:10])

def _visible_len(s):
    """Length of string excluding ANSI escape codes."""
    return len(_ANSI_RE.sub('', str(s)))

def _col(val, width, align='left'):
    """Pad val to width based on visible length, respecting ANSI codes."""
    s = str(val)
    pad = max(0, width - _visible_len(s))
    return (' ' * pad + s) if align == 'right' else (s + ' ' * pad)

STRATEGIES = {
    "ORB_CHAMPION_15M":   "live_trading/logs/orb_state.json",
    "ORB_CHAMPION_30M":   "live_trading/logs/orb_30m_state.json",
    "BB_OPTIONS_LIVE":    "live_trading/logs/bb_state.json",
    "CANDLE_BREAKER_LIVE":"live_trading/logs/cb_live_state.json",
    "DAILY_SNIPER":       "live_trading/logs/daily_sniper_state.json",
    "NIFTY_TREND_SELLER": "live_trading/logs/nifty_trend_seller_state.json",
}

NTS_STATE_FILE = Path(__file__).parent / "logs" / "nifty_trend_seller_state.json"

def load_state(file_path):
    path = Path(file_path)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except:
        return None

def format_pnl(pnl):
    color = Fore.GREEN if pnl > 0 else (Fore.RED if pnl < 0 else Fore.WHITE)
    return f"{color}{pnl:+,.2f}{Style.RESET_ALL}"

def _load_sandbox_ltps():
    """Return {symbol: ltp} from sandbox_positions for all open positions."""
    try:
        conn = sqlite3.connect('db/sandbox.db')
        cur = conn.cursor()
        cur.execute("SELECT symbol, ltp FROM sandbox_positions WHERE quantity != 0 AND ltp IS NOT NULL")
        result = {row[0]: float(row[1]) for row in cur.fetchall()}
        conn.close()
        return result
    except Exception:
        return {}

def _fetch_live_ltps(symbols: set) -> dict:
    """
    Fetch live LTPs for a set of equity symbols from OpenAlgo positionbook.
    Falls back silently to {} on any error so the display still works.
    """
    if not symbols:
        return {}
    try:
        api_key = os.getenv("OPENALGO_API_KEY")
        host    = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
        if not api_key:
            return {}
        resp = requests.post(
            f"{host}/api/v1/positionbook",
            json={"apikey": api_key},
            timeout=3,
        )
        data = resp.json()
        if data.get("status") != "success":
            return {}
        return {
            pos["symbol"]: float(pos["ltp"])
            for pos in data.get("data", [])
            if pos.get("symbol") in symbols and pos.get("ltp") not in (None, "", 0)
        }
    except Exception:
        return {}

def load_gap_fade_positions() -> list:
    """
    Load Gap Fade positions from the bot's JSON state file.

    Returns a list of row dicts, one per position, with keys:
      symbol, qty, side, entry, sl, target, ltp, mtm, exit_price, pnl
    """
    if not GAP_FADE_STATE_FILE.exists():
        return []
    try:
        state = json.loads(GAP_FADE_STATE_FILE.read_text())
    except Exception:
        return []

    positions = state.get("positions", {})

    # For positions still open, fetch live LTPs from OpenAlgo positionbook
    open_syms = {
        sym for sym, p in positions.items()
        if p.get("exit_price") is None and not p.get("sl_hit")
    }
    live_ltps = _fetch_live_ltps(open_syms)

    rows = []
    for sym, p in positions.items():
        direction   = p.get("direction", "")
        entry_price = float(p.get("entry_price") or 0)
        sl_price    = float(p.get("sl_price")    or 0)
        quantity    = int(p.get("quantity")       or 0)
        # Prefer live LTP from positionbook; fall back to state file's current_price
        current_ltp = live_ltps.get(sym) or float(p.get("current_price") or entry_price)
        exit_price  = p.get("exit_price")   # None when still open
        net_pnl     = p.get("net_pnl")      # None until trade logged
        sl_hit      = p.get("sl_hit", False)

        # MTM: unrealised P&L using live LTP (or exit/SL price when closed)
        effective_ltp = float(exit_price) if (exit_price is not None) else current_ltp
        if entry_price > 0:
            if direction == "SHORT":
                mtm = (entry_price - effective_ltp) * quantity
            else:
                mtm = (effective_ltp - entry_price) * quantity
        else:
            mtm = 0.0

        # Target = time-based exit (10:00 AM), no price target
        target = "10:00"

        rows.append({
            "symbol":      sym,
            "qty":         quantity,
            "side":        direction,
            "entry":       entry_price,
            "sl":          sl_price,
            "target":      target,
            "ltp":         effective_ltp,
            "mtm":         mtm,
            "exit_price":  exit_price,
            "pnl":         net_pnl,
            "sl_hit":      sl_hit,
            "is_closed":   (exit_price is not None),
        })

    # Sort: open positions first, then closed; within each group sort by |mtm| desc
    rows.sort(key=lambda r: (r["is_closed"], -abs(r["mtm"])))
    return rows


def get_status_table():
    sandbox_ltp = _load_sandbox_ltps()
    rows = []
    meters = []
    cb_monitoring = []
    cb_history = {}
    
    for name, path in STRATEGIES.items():
        state = load_state(path)
        if not state:
            continue
            
        strategy = _shorten(state.get('strategy', name))
        
        # --- Strategy Meters (Health/Signal status) ---
        if name == "CANDLE_BREAKER_LIVE":
            idx_state = state.get('state', {})
            for idx, sdata in idx_state.items():
                # Store detailed monitoring for a separate table
                ce = sdata.get('CE', {})
                pe = sdata.get('PE', {})
                cb_monitoring.append([
                    idx, 
                    str(sdata.get('timeframe', '?')) + "m",
                    f"{sdata.get('up_cross_count', 0)} / {sdata.get('down_cross_count', 0)}",
                    sdata.get('last_state', 'N/A'),
                    f"{float(ce.get('open', 0)):.1f} ({float(ce.get('current_low', 0)):.1f})",
                    f"{float(pe.get('open', 0)):.1f} ({float(pe.get('current_low', 0)):.1f})"
                ])
                # Synthesize Live Candle Data
                ltp = state.get('index_prices', {}).get(idx, 0)
                tf = sdata.get('timeframe', 15)
                
                # Calculate current candle start
                now = datetime.now()
                baseline = now.replace(hour=9, minute=15, second=0, microsecond=0)
                market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
                if baseline <= now <= market_close:
                    elapsed = int((now - baseline).total_seconds() // 60)
                    start_mins = (elapsed // tf) * tf
                    c_start_dt = baseline + timedelta(minutes=start_mins)
                    c_start_str = c_start_dt.strftime("%H:%M")
                    
                    live_candle = {
                        't': f"{c_start_str}*",
                        'o': sdata.get('index_open', ltp),
                        'h': sdata.get('index_high', ltp),
                        'l': sdata.get('index_low', ltp),
                        'c': ltp,
                        'up': sdata.get('up_cross_count', 0),
                        'down': sdata.get('down_cross_count', 0)
                    }
                    hist = sdata.get('ohlc_history', [])
                    # Append live candle if it's not already the last one (rare edge case during roll)
                    if not hist or hist[-1].get('t') != c_start_str:
                        hist = hist + [live_candle]
                    cb_history[idx] = hist
                else:
                    cb_history[idx] = sdata.get('ohlc_history', [])
        elif name == "BB_OPTIONS_LIVE":
            gaps = state.get('daily_gaps', {})
            for idx, gap in gaps.items():
                mode = "REVERSION" if abs(gap) < 0.8 else "TREND"
                meters.append(f"{idx}: Gap {gap:+.2f}% ({mode})")
        elif "ORB_CHAMPION" in name:
            meters.append(f"{state.get('bot_id', 'ORB')}: High {state.get('range_high', 0)} | Low {state.get('range_low', 0)} | Ready: {state.get('range_ready', False)}")

        # --- Active Positions ---
        if "ORB_CHAMPION" in name:
            pos = state.get('active_position')
            if pos:
                sym   = pos.get('symbol')
                entry = float(pos.get('entry_price', 0))
                ltp   = sandbox_ltp.get(sym, float(pos.get('current_price', 0)))
                qty   = float(pos.get('quantity', 0))
                pnl   = (ltp - entry) * qty if ltp and entry else 0
                orig_sl  = float(pos.get('initial_sl', entry * (1 - 0.20)))
                curr_sl  = float(pos.get('sl_price', orig_sl))
                rows.append([
                    strategy, sym, f"{entry:.2f}", f"{ltp:.2f}",
                    format_pnl(pnl), f"{orig_sl:.2f}",
                    f"{curr_sl:.2f}", "Open", "Shield&Trail"
                ])

        elif name == "BB_OPTIONS_LIVE":
            trades = state.get('active_trades', {})
            for idx, types in trades.items():
                for t_type, trade in types.items():
                    if trade:
                        sym   = trade.get('opt_symbol')
                        entry = float(trade.get('entry_price', 0))
                        ltp   = sandbox_ltp.get(sym, float(trade.get('current_price', 0)))
                        qty   = float(trade.get('quantity', 0))
                        pnl   = (ltp - entry) * qty if ltp and entry else 0
                        rows.append([
                            strategy, sym, f"{entry:.2f}", f"{ltp:.2f}",
                            format_pnl(pnl), f"{float(trade.get('sl_price', 0)):.2f}",
                            "SMA", "SMA", "Inactive"
                        ])

        elif name == "SUPERTREND_5S_LIVE":
            trades = state.get('active_trades', {})
            for idx, trade in trades.items():
                if trade:
                    sym   = trade.get('opt_symbol')
                    entry = float(trade.get('entry_price', 0))
                    ltp   = sandbox_ltp.get(sym, float(trade.get('current_price', 0)))
                    qty   = float(trade.get('quantity', 0))
                    pnl   = (ltp - entry) * qty if ltp and entry else 0
                    rows.append([
                        strategy, sym, f"{entry:.2f}", f"{ltp:.2f}",
                        format_pnl(pnl), f"{float(trade.get('sl_price', 0)):.2f}",
                        "Trend", "Trend", "Inactive"
                    ])

        elif name == "CANDLE_BREAKER_LIVE":
            trades = state.get('active_trades', {})
            last_prices = state.get('last_prices', {})
            for idx, types in trades.items():
                for t_type, trade in types.items():
                    if trade:
                        sym   = trade.get('symbol')
                        entry = float(trade.get('entry_price', 0))
                        ltp   = sandbox_ltp.get(sym, float(last_prices.get(sym, trade.get('current_price', 0))))
                        qty   = float(trade.get('qty', trade.get('quantity', 0)))
                        pnl   = (ltp - entry) * qty if ltp and entry else 0
                        rows.append([
                            strategy, sym, f"{entry:.2f}", f"{ltp:.2f}",
                            format_pnl(pnl), f"{float(trade.get('initial_sl', 0)):.2f}",
                            f"{float(trade.get('sl', 0)):.2f}", f"{float(trade.get('target', 0)):.2f}", "Inactive"
                        ])

        elif name == "NIFTY_TREND_SELLER":
            # Short option seller: P&L = (entry_prem - ltp) * qty  (profit when premium decays)
            active = state.get('active_trades', {})
            for opt_type, trade in active.items():
                if trade:
                    sym   = trade.get('symbol', '')
                    entry = float(trade.get('entry_prem', 0))
                    sl    = float(trade.get('sl_prem', 0))
                    qty   = float(trade.get('qty', 0))
                    # LTP from sandbox (Analyze mode) or positionbook; fall back to entry
                    ltp   = sandbox_ltp.get(sym, entry)
                    pnl   = (entry - ltp) * qty   # seller: profit when ltp falls
                    rows.append([
                        strategy, sym, f"{entry:.2f}", f"{ltp:.2f}",
                        format_pnl(pnl), f"{sl:.2f}", f"{sl:.2f}", "EOD 15:20", "EOD"
                    ])

    return rows, meters, cb_monitoring, cb_history

def print_dashboard():
    os.system('clear')

    rows, meters, cb_mon, cb_hist = get_status_table()

    # ── SECTION 1: 10-Candle History (large, rendered first so it scrolls off top) ──
    if cb_mon:
        print(f"{Fore.CYAN}📊 10-CANDLE HISTORY")
        print("-" * 105)
        for idx in ['NIFTY', 'BANKNIFTY', 'SENSEX']:
            hist = cb_hist.get(idx, [])[-10:]
            if not hist: continue
            print(f"{Fore.YELLOW}[{idx}]")
            h_headers = ["Time", "Open", "High", "Low", "Close", "Up Cross", "Dn Cross"]
            h_widths = [10, 10, 10, 10, 10, 12, 12]
            h_str = "".join([f"{h:<{w}} " for h, w in zip(h_headers, h_widths)])
            print(h_str)
            for c in hist:
                line = (f"{c.get('t'):<10} {float(c.get('o', 0)):<10.2f} {float(c.get('h', 0)):<10.2f} {float(c.get('l', 0)):<10.2f} "
                        f"{float(c.get('c', 0)):<10.2f} {c.get('up'):<12} {c.get('down'):<12}")
                print(line)

        # ── SECTION 2: Candle Breaker signal monitoring ──
        print("\n" + f"{Fore.CYAN}{'='*120}")
        print(f"{Fore.CYAN}🕯️ CANDLE BREAKER MONITORING")
        print(f"{Fore.CYAN}{'='*120}")
        mon_headers = ["Index", "TF", "Up/Dn Cross", "Status", "CE Baseline (Low)", "PE Baseline (Low)"]
        mon_widths = [15, 8, 15, 12, 25, 25]
        h_str = ""
        for h, w in zip(mon_headers, mon_widths):
            h_str += f"{h:<{w}} "
        print(f"{Fore.YELLOW}{h_str}")
        print("-" * 105)
        for row in cb_mon:
            line = ""
            for val, w in zip(row, mon_widths):
                line += f"{str(val):<{w}} "
            print(line)

    # ── SECTION 3: Strategy Meters ──
    if meters:
        print("\n" + "-" * 120)
        print(f"{Fore.YELLOW}STRATEGY METERS:")
        for m in meters:
            print(f"  ⚡ {m}")

    # ── SECTION 4: Active Positions (always visible at bottom) ──
    print("\n" + f"{Fore.CYAN}{'='*120}")
    print(f"{Fore.CYAN}🚀 UNIFIED LIVE TRADING DASHBOARD | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{Fore.CYAN}{'='*120}")

    # col index:   0        1       2       3      4      5        6        7        8
    headers    = ["Strat",  "Symbol","Entry","LTP",  "PnL", "Orig SL","Curr SL","Target","Trailing"]
    col_widths = [  10,      28,      10,     10,    14,     10,       10,       10,      13       ]
    # right-align numeric columns: Entry(2) LTP(3) PnL(4) Orig SL(5) Curr SL(6) Target(7)
    col_align  = ['left','left','right','right','right','right','right','right','left']

    header_str = " ".join(_col(h, w, a) for h, w, a in zip(headers, col_widths, col_align))
    print(f"{Fore.YELLOW}{header_str}")
    print("-" * 119)

    if not rows:
        print(f"{Fore.WHITE}  No active positions.")
    else:
        total_pnl = 0.0
        for row in rows:
            line = " ".join(_col(val, w, a) for val, w, a in zip(row, col_widths, col_align))
            print(line)
            # Extract raw PnL from the formatted string (strip ANSI, commas, + sign)
            try:
                raw = _ANSI_RE.sub('', str(row[4])).replace(',', '').replace('+', '')
                total_pnl += float(raw)
            except (ValueError, IndexError):
                pass
        print("-" * 119)
        total_str = format_pnl(total_pnl)
        # Right-justify label to exactly pnl_col_offset chars, then PnL value with no extra space
        pnl_col_offset = sum(w + 1 for w in col_widths[:4])  # 62 chars: cols 0-3 + 4 separators
        label = "TOTAL PnL:"
        print(f"{Fore.YELLOW}{label.rjust(pnl_col_offset)}{_col(total_str, col_widths[4], 'right')}")

    # ── SECTION 5: Pre-Open Gap Fade Positions ───────────────────────────────
    gap_rows = load_gap_fade_positions()

    print("\n" + f"{Fore.CYAN}{'='*120}")
    print(f"{Fore.CYAN}📈 PRE-OPEN GAP FADE — POSITIONS  "
          f"{Fore.WHITE}(paper / analyzer mode)")
    print(f"{Fore.CYAN}{'='*120}")

    # Column definitions
    # Stock Name | Qty | Buy/Sell | Entry Price | SL | Target | LTP | MTM | Exit Price | PnL
    gf_headers = ["Stock",   "Qty", "Side",  "Entry",  "SL",     "Target", "LTP",    "MTM",    "Exit",   "PnL"]
    gf_widths  = [  13,        6,     7,       9,        9,         7,       9,        12,        9,       12  ]
    gf_align   = ["left", "right", "left", "right",  "right",   "center", "right",  "right",  "right",  "right"]

    hdr_str = " ".join(_col(h, w, a) for h, w, a in zip(gf_headers, gf_widths, gf_align))
    print(f"{Fore.YELLOW}{hdr_str}")
    print("-" * 120)

    if not gap_rows:
        # Try to show state info if file exists but no positions yet
        if GAP_FADE_STATE_FILE.exists():
            try:
                st = json.loads(GAP_FADE_STATE_FILE.read_text())
                n_sig  = st.get("n_signals", 0)
                t_date = st.get("trade_date", "?")
                lu     = st.get("last_update", "")
                try:
                    diff = (datetime.now() - datetime.fromisoformat(lu)).total_seconds()
                    age  = f"{diff:.0f}s ago"
                except Exception:
                    age  = "?"
                print(f"{Fore.WHITE}  Bot active for {t_date} | {n_sig} signal(s) queued | "
                      f"No positions yet | updated {age}")
            except Exception:
                print(f"{Fore.WHITE}  No Gap Fade positions today.")
        else:
            print(f"{Fore.WHITE}  Bot not running (state file absent).")
    else:
        gf_total_mtm = 0.0
        gf_total_pnl = 0.0
        for r in gap_rows:
            sym  = r["symbol"]
            qty  = str(r["qty"])
            side = r["side"]
            side_color = Fore.RED if side == "SHORT" else Fore.GREEN
            side_fmt   = f"{side_color}{side}{Style.RESET_ALL}"

            entry_s  = f"{r['entry']:.2f}"
            sl_s     = f"{r['sl']:.2f}"
            target_s = str(r["target"])
            ltp_s    = f"{r['ltp']:.2f}"

            # MTM colouring
            mtm = r["mtm"]
            gf_total_mtm += mtm
            mtm_s = format_pnl(mtm)

            # Exit price — show "–" if still open
            exit_s = f"{float(r['exit_price']):.2f}" if r["exit_price"] is not None else "–"

            # PnL — show "–" if not yet logged; colour when known
            if r["pnl"] is not None:
                pnl_val = float(r["pnl"])
                gf_total_pnl += pnl_val
                pnl_s = format_pnl(pnl_val)
            else:
                pnl_s = f"{Fore.WHITE}–{Style.RESET_ALL}"

            # Annotate SL-hit rows
            if r["sl_hit"]:
                sym = f"{Fore.MAGENTA}{sym}✂{Style.RESET_ALL}"

            row_vals = [sym, qty, side_fmt, entry_s, sl_s, target_s, ltp_s, mtm_s, exit_s, pnl_s]
            line = " ".join(_col(v, w, a) for v, w, a in zip(row_vals, gf_widths, gf_align))
            print(line)

        # Totals row
        print("-" * 100)
        mtm_label_offset = sum(w + 1 for w in gf_widths[:7])   # align under MTM column
        pnl_label_offset = sum(w + 1 for w in gf_widths[:8])   # align under PnL column
        total_mtm_s = format_pnl(gf_total_mtm)

        # Net P&L: use logged net_pnl when available (after 10:05 on_log_pnl runs).
        # Gross MTM doesn't deduct transaction costs (~₹700 per 10 trades at ₹1L each);
        # Net P&L does — always use Net P&L as the headline figure once available.
        all_pnl_known = all(r["pnl"] is not None for r in gap_rows)
        if all_pnl_known and gf_total_pnl != 0.0:
            total_pnl_s = format_pnl(gf_total_pnl)
            print(f"{Fore.WHITE}{'Gross MTM:'.rjust(mtm_label_offset)}"
                  f"{_col(total_mtm_s, gf_widths[7], 'right')}"
                  f"   {Fore.YELLOW}Net P&L (after costs): {total_pnl_s}{Style.RESET_ALL}")
        else:
            # Trades still open or P&L not yet logged — show MTM only with a note
            print(f"{Fore.YELLOW}{'Total MTM:'.rjust(mtm_label_offset)}"
                  f"{_col(total_mtm_s, gf_widths[7], 'right')}"
                  f"   {Fore.WHITE}(gross · costs deducted at 10:05){Style.RESET_ALL}")

    # ── SECTION 6: Nifty Trend Seller — bot status panel ────────────────────
    print("\n" + f"{Fore.CYAN}{'='*120}")
    print(f"{Fore.CYAN}📉 NIFTY TREND SELLER  "
          f"{Fore.WHITE}(ADX>30↑ + RSI + MACD confluence | sell ATM counter option | EOD exit)")
    print(f"{Fore.CYAN}{'='*120}")

    if NTS_STATE_FILE.exists():
        try:
            nts = json.loads(NTS_STATE_FILE.read_text())
            nifty_ltp  = nts.get("nifty_ltp", 0)
            vix_ltp    = nts.get("vix_ltp", 0)
            expiry     = nts.get("expiry", "—")
            lot_size   = nts.get("lot_size", "—")
            n_lots     = nts.get("n_lots", 10)
            win_str    = nts.get("entry_window", "10:00–13:00")
            vix_filter = nts.get("vix_filter", f"≤{22.0}")
            bars       = nts.get("bars_loaded", 0)
            lu         = nts.get("last_update", "")

            # VIX colour: green ≤ 22, yellow 22–25, red > 25
            vix_color = Fore.GREEN if vix_ltp <= 22 else (Fore.YELLOW if vix_ltp <= 25 else Fore.RED)

            print(
                f"  NIFTY: {Fore.CYAN}{nifty_ltp:.2f}{Style.RESET_ALL}  |  "
                f"VIX: {vix_color}{vix_ltp:.2f}{Style.RESET_ALL} (filter {vix_filter})  |  "
                f"Expiry: {Fore.YELLOW}{expiry}{Style.RESET_ALL}  |  "
                f"Lot size: {lot_size}  |  Lots: {n_lots}  |  "
                f"Window: {win_str}  |  Bars loaded: {bars}"
            )

            active = nts.get("active_trades", {})
            if active:
                print(f"\n  {Fore.YELLOW}Active legs:{Style.RESET_ALL}")
                nt_headers = ["Leg", "Symbol", "Entry ₹", "SL ₹", "Qty", "Since"]
                nt_widths  = [6, 28, 10, 10, 8, 12]
                print("  " + "  ".join(f"{h:<{w}}" for h, w in zip(nt_headers, nt_widths)))
                print("  " + "-" * 80)
                for leg, t in active.items():
                    entry_t = t.get("entry_time", "")
                    try:
                        since = datetime.fromisoformat(entry_t).strftime("%H:%M:%S")
                    except Exception:
                        since = entry_t[:8] if entry_t else "—"
                    print("  " + "  ".join(f"{str(v):<{w}}" for v, w in zip(
                        [leg, t.get("symbol",""), f"{t.get('entry_prem',0):.2f}",
                         f"{t.get('sl_prem',0):.2f}", t.get("qty",""), since],
                        nt_widths
                    )))
            else:
                print(f"  {Fore.WHITE}No open legs.")
        except Exception as e:
            print(f"  {Fore.RED}Error reading NTS state: {e}")
    else:
        print(f"  {Fore.RED}Bot not running (state file absent).")

    # ── SECTION 7: Bot Heartbeats (always visible at very bottom) ──
    print("-" * 119)
    print(f"{Fore.WHITE}BOT HEARTBEATS:")
    for name, path in STRATEGIES.items():
        state = load_state(path)
        if state:
            lu = state.get('last_update', 'N/A')
            try:
                dt = datetime.fromisoformat(lu)
                # Ensure local now is aware if dt is aware
                now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
                diff = (now - dt).total_seconds()
                status = f"{Fore.GREEN}LIVE ({diff:.1f}s ago)" if diff < 30 else f"{Fore.RED}STALE ({diff:.1f}s ago)"
            except:
                status = f"{Fore.RED}INVALID TIME"
            print(f"  - {name:<25}: {status}")
        else:
            print(f"  - {name:<25}: {Fore.RED}OFFLINE")

    # Gap Fade heartbeat
    if GAP_FADE_STATE_FILE.exists():
        try:
            gf_state = json.loads(GAP_FADE_STATE_FILE.read_text())
            lu = gf_state.get("last_update", "")
            dt = datetime.fromisoformat(lu)
            # Ensure local now is aware if dt is aware (IST)
            now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
            diff = (now - dt).total_seconds()
            gf_status = f"{Fore.GREEN}LIVE ({diff:.1f}s ago)" if diff < 120 else f"{Fore.YELLOW}IDLE ({diff:.0f}s ago)"
        except Exception:
            gf_status = f"{Fore.RED}INVALID STATE"
    else:
        gf_status = f"{Fore.RED}OFFLINE"
    print(f"  - {'PREOPEN_GAP_FADE':<25}: {gf_status}")

if __name__ == "__main__":
    while True:
        try:
            print_dashboard()
            time.sleep(2)
        except KeyboardInterrupt:
            print("\nExiting dashboard...")
            break
        except Exception as e:
            print(f"Dashboard Error: {e}")
            time.sleep(5)
