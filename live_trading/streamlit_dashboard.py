"""
OpenAlgo Live Trading — Streamlit Web Dashboard
================================================
live_trading/streamlit_dashboard.py

Reads JSON state files written by each bot every 2 seconds and presents
a live, auto-refreshing web dashboard showing strategy signals, conditions,
and active positions with live MTM for all running bots.

Active bots tracked:
  ── Options Bots (intraday) ──
  1. Nifty BB Overbought Bot      — BB(30,3σ) 5-min → sell ATM PE (paper)
  2. Nifty Trend Seller Bot       — ADX+RSI+MACD 1-min → sell ATM CE/PE (analyze)
  3. SENSEX Trend Seller Bot      — SHORT-ONLY ADX+RSI+MACD → sell ATM CE (analyze)
  4. HTF PO3 Bot                  — 60-min PO3 fractal → sell ATM PE on CISD (paper)
  5. BANKNIFTY BB Options Bot     — BB(20,2σ) 1-min option premium → sell ATM CE/PE (paper)
  6. HA Options Bot               — Heiken Ashi flip → sell ATM CE/PE on NIFTY/BANKNIFTY/SENSEX (paper)
  7. NIFTY MACD Map Bot           — MACD(5,13,3) 15-min histogram cross → sell ATM PE/CE (paper)
  8. NIFTY EOD Hold Bot           — ADX+MACD+hammer/SS reversal, 1-min, 09:15–09:44 window, EOD 15:29 (paper)
  9. NTS + OBI Gate Bot           — NTS champion + depth-50 OBI gate (15-session experiment)
  ── Stock Bots ──
 10. Pre-Open Gap Fade Bot        — gap ≥2% equities fade, exit 10:00 (paper)
 11. Gap Fade EOD Bot             — gap-down 2–5% → long, full-day hold, exit 15:25 (paper)
 12. EMA Swing Scanner            — daily EMA pullback + RSI on 16 NIFTY50 stocks (paper)
 13. Equity OBI Bot               — RETIRED 2026-06-19 (WR 38.3%, P&L −₹4,122 / 149 trades)
  ── Weekly Positions (NRML multi-day) ──
 14. NIFTY Iron Fly Weekly Bot    — Short Iron Fly 4-leg NRML, 10 lots, VIX≥12+MA20, Wed entry (paper)
 15. SENSEX Iron Fly Weekly Bot   — Short Iron Fly 4-leg NRML, 1 lot (20 units), MA20 only, Fri entry (paper)
 16. BANKNIFTY Iron Fly Monthly Bot — Short Iron Fly 4-leg NRML, 10 lots, ADX<25+VIX≥12, monthly cycle (paper)

Run:
    cd ~/Developer/fyers_crk/openalgo
    streamlit run live_trading/streamlit_dashboard.py

Opens automatically at http://localhost:8501
Refreshes every 3 seconds.
"""

import asyncio
from collections import OrderedDict
import json
import os
import queue
import re
import threading
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv, dotenv_values

import pandas as pd
import requests
import streamlit as st

st.set_page_config(
    page_title="OpenAlgo Unified Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Workspaces ───────────────────────────────────────────────────────────────
WORKSPACES = {
    "CRK": {"name": "Ramakrishna (CRK)", "root": Path("/Users/ramakrishna/Developer/fyers_crk/openalgo")},
    "CS":  {"name": "Customer Support (CS)", "root": Path("/Users/ramakrishna/Developer/fyers_cs/openalgo")}
}

def get_ws_env(ws_id: str):
    return dotenv_values(WORKSPACES[ws_id]["root"] / ".env")

def get_workspace_paths(ws_id: str):
    root = WORKSPACES[ws_id]["root"]
    live_trading = root / "live_trading"
    logs = live_trading / "logs"
    
    # ema_logs retired 2026-06-04 (EMA Swing Scanner retired)
    nts_obi_logs = live_trading / "nifty_trend_seller_obi" / "logs"
    # equity_obi_logs retired 2026-06-19 (Equity OBI Bot retired — WR 38.3%, P&L −₹4,122)

    state_files = {
        "NIFTY_BB_OB": logs / "nifty_bb_overbought_state.json",
        "NIFTY_TS":    logs / "nifty_trend_seller_state.json",
        "SENSEX_TS":   logs / "sensex_trend_seller_state.json",
        "GAP_FADE":    logs / "preopen_gap_fade_state.json",
        # "GAP_FADE_EOD": retired 2026-06-04
        "HTF_PO3":     logs / "htf_po3_state.json",
        "BNF_BB_OPT":  logs / "banknifty_bb_options_state.json",
        "BB_MEAN_REV": logs / "bb_mean_reversion_state.json",
        "HA_OPTIONS":  logs / "ha_options_state.json",
        # "EMA_SWING":   retired 2026-06-04
        "NTS_OBI":     nts_obi_logs / "nts_obi_state.json",
        "NIFTY_MACD_MAP": logs / "nifty_macd_map_state.json",
        "NIFTY_EOD_HOLD": logs / "nifty_eod_hold_state.json",
        # "EQUITY_OBI":     retired 2026-06-19
        "IRON_FLY_WEEKLY": logs / "iron_fly_weekly_state.json",
        "SENSEX_IRON_FLY_WEEKLY": logs / "sensex_iron_fly_weekly_state.json",
        "BNF_IRON_FLY_MONTHLY": logs / "banknifty_iron_fly_monthly_state.json",
        "FLAT_BLUE_LINE_MONTHLY": logs / "flat_blue_line_monthly_state.json",
        "NIFTY_MA_CROSS_SELLER": logs / "nifty_ma_cross_seller_state.json",
        "MACD_M2_SELL": logs / "macd_m2_sell_options_state.json",
    }
    
    return {
        "ROOT": root,
        "LOGS_DIR": logs,
        "NTS_OBI_LOGS_DIR": nts_obi_logs,
        # "EQUITY_OBI_LOGS_DIR": retired 2026-06-19
        "REGISTRY_FILE": logs / "launcher_registry.json",
        "COMMANDS_FILE": logs / "bot_commands.json",
        "STATE_FILES": state_files
    }

# Default globals (initially CS; updated in main() based on selection)
WS_PATHS = get_workspace_paths("CS")
WS_ENV   = get_ws_env("CS")
LOGS_DIR      = WS_PATHS["LOGS_DIR"]
NTS_OBI_LOGS_DIR   = WS_PATHS["NTS_OBI_LOGS_DIR"]
# EQUITY_OBI_LOGS_DIR removed — RETIRED 2026-06-19
REGISTRY_FILE = WS_PATHS["REGISTRY_FILE"]
COMMANDS_FILE = WS_PATHS["COMMANDS_FILE"]
STATE_FILES   = WS_PATHS["STATE_FILES"]

def _get_api_key(): 
    return WS_ENV.get("OPENALGO_API_KEY", "")

def _get_host():    
    return WS_ENV.get("HOST_SERVER", "http://127.0.0.1:5000")

# Setup the singleton price cache logic
_WS_LTP_CACHE:    dict[str, float] = {}       # "SYMBOL" -> ltp
_WS_CACHE_LOCK    = threading.Lock()
_WS_THREAD_STARTED = False
_GLOBAL_SUB_LIST: list[tuple[str, str]] = []   # shared across connections

def _ws_cache_thread():
    async def _manage_ws_connection(ws_id: str, url: str, key: str):
        import websockets
        retry = 5
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
                    await ws.send(json.dumps({"action": "authenticate", "api_key": key}))
                    await asyncio.sleep(0.5)
                    subscribed = set()
                    while True:
                        global _GLOBAL_SUB_LIST
                        with _WS_CACHE_LOCK:
                            to_add = [s for s in _GLOBAL_SUB_LIST if s not in subscribed]
                        if to_add:
                            batch = [{"symbol": s[0], "exchange": s[1]} for s in to_add]
                            await ws.send(json.dumps({"action": "subscribe", "mode": "LTP", "symbols": batch}))
                            for s in to_add: subscribed.add(s)
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                            data = json.loads(raw)
                            if data.get("type") == "market_data":
                                sym = data.get("symbol", "")
                                ltp = (data.get("data") or {}).get("ltp", 0)
                                if sym and ltp and ltp > 0:
                                    with _WS_CACHE_LOCK: _WS_LTP_CACHE[sym] = float(ltp)
                        except asyncio.TimeoutError: continue
            except Exception: await asyncio.sleep(retry)

    async def _main_loop():
        ws_tasks = []
        for ws_id in WORKSPACES:
            env = get_ws_env(ws_id)
            key = env.get("OPENALGO_API_KEY", "")
            url = env.get("WEBSOCKET_URL")
            if not url:
                host = env.get("HOST_SERVER", "http://127.0.0.1:5000")
                url = host.replace("http", "ws") + "/ws"
            ws_tasks.append(_manage_ws_connection(ws_id, url, key))
        await asyncio.gather(*ws_tasks)
    asyncio.run(_main_loop())

def _ensure_ws_cache_thread():
    """Start the background WS thread once per process."""
    global _WS_THREAD_STARTED
    if not _WS_THREAD_STARTED:
        t = threading.Thread(target=_ws_cache_thread, daemon=True, name="DashboardWSCache")
        t.start()
        _WS_THREAD_STARTED = True

# Overwrite _WS_SUB_QUEUE behavior to use our list
def _subscribe_symbol(symbol: str, exchange: str):
    global _GLOBAL_SUB_LIST
    item = (symbol, exchange)
    with _WS_CACHE_LOCK:
        if item not in _GLOBAL_SUB_LIST:
            _GLOBAL_SUB_LIST.append(item)



# ── Custom CSS — Cyber Navy Theme ─────────────────────────────────────────────
st.markdown("""
<style>
  /* ── Global background ── */
  .stApp,
  [data-testid="stAppViewContainer"],
  [data-testid="stMain"] {
    background: #0b0e1a !important;
  }

  /* ── Hide Streamlit's top toolbar ── */
  header[data-testid="stHeader"] { display: none !important; }
  .stMainBlockContainer, div.block-container { padding-top: 1.5rem !important; }

  /* ── Sidebar ── */
  section[data-testid="stSidebar"] {
    background: #080b15 !important;
    border-right: 1px solid #1e2a45 !important;
  }
  section[data-testid="stSidebar"] > div:first-child { background: transparent !important; }
  section[data-testid="stSidebar"] * { color: #7fa8cc !important; }
  section[data-testid="stSidebar"] .stSelectbox label,
  section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {
    color: #00d4ff !important;
    font-size: 0.77rem !important;
    font-weight: 700 !important;
    text-transform: uppercase;
    letter-spacing: 0.07em;
  }
  section[data-testid="stSidebar"] hr { border-color: #1e2a45 !important; }
  section[data-testid="stSidebar"] .stRadio label { font-size: 0.87rem !important; font-weight: 400; }
  section[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) p {
    color: #00d4ff !important;
    font-weight: 600 !important;
  }
  section[data-testid="stSidebar"] .stButton > button {
    background: #0d1829 !important;
    border: 1px solid #1e3a5a !important;
    color: #00d4ff !important;
    width: 100%;
  }
  section[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {
    color: #3f5a80 !important;
    font-size: 0.78rem !important;
  }

  /* ── Metric cards ── */
  [data-testid="stMetric"] {
    background: #0d1120 !important;
    border: 1px solid #1e2a45 !important;
    border-radius: 8px !important;
    padding: 10px 14px !important;
  }
  [data-testid="stMetricValue"] {
    font-size: 1.3rem !important;
    font-weight: 700 !important;
    color: #c8dff0 !important;
  }
  [data-testid="stMetricLabel"] p {
    font-size: 0.73rem !important;
    font-weight: 700 !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: #3f5a80 !important;
  }
  [data-testid="stMetricDelta"] { font-size: 0.8rem !important; }

  /* ── Headings ── */
  .stMainBlockContainer h1, div.block-container h1 {
    color: #00d4ff !important; font-size: 1.45rem !important; font-weight: 700 !important;
    letter-spacing: 0.04em; border-bottom: 1px solid #1e2a45; padding-bottom: 6px;
  }
  .stMainBlockContainer h2, div.block-container h2 {
    color: #00d4ff !important; font-size: 1.2rem !important; font-weight: 700 !important;
  }
  .stMainBlockContainer h3, div.block-container h3 {
    color: #00d4ff !important; font-size: 1.05rem !important; font-weight: 700 !important;
    letter-spacing: 0.03em;
  }
  .stMainBlockContainer h4, div.block-container h4 {
    color: #7fa8cc !important; font-size: 0.92rem !important; font-weight: 600 !important;
  }

  /* ── Body & markdown text ── */
  .stMarkdown p, .stMarkdown li { color: #7fa8cc !important; }
  .stMarkdown strong, .stMarkdown b { color: #c8dff0 !important; }
  p { color: #7fa8cc !important; }
  small { color: #3f5a80 !important; }

  /* ── Containers & expanders ── */
  [data-testid="stExpander"] {
    background: #0d1120 !important;
    border: 1px solid #1e2a45 !important;
    border-radius: 8px !important;
  }
  [data-testid="stExpander"] summary p { color: #7fa8cc !important; font-size: 0.9rem !important; }
  div[data-testid="stVerticalBlockBorderWrapper"] > div {
    background: #0d1120 !important;
    border: 1px solid #1e2a45 !important;
    border-radius: 10px !important;
    padding: 14px !important;
  }

  /* ── Dataframe ── */
  [data-testid="stDataFrame"] { border: 1px solid #1e2a45 !important; border-radius: 8px !important; }

  /* ── Buttons (main area) ── */
  .stButton > button {
    background: #0d1829 !important;
    border: 1px solid #1e3a5a !important;
    color: #00d4ff !important;
    border-radius: 6px !important;
    font-size: 0.85rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.03em;
  }
  .stButton > button:hover {
    background: #1a2a4a !important;
    border-color: #00d4ff !important;
  }
  .stButton > button:disabled {
    color: #1e2a45 !important;
    border-color: #111829 !important;
    background: #080b15 !important;
  }

  /* ── Select / dropdowns ── */
  [data-baseweb="select"] > div,
  [data-baseweb="input"] > div {
    background: #0d1120 !important;
    border-color: #1e2a45 !important;
    color: #7fa8cc !important;
  }

  /* ── Dividers ── */
  hr { border-color: #1e2a45 !important; opacity: 0.8 !important; }

  /* ── Progress bar ── */
  [data-testid="stProgressBar"] > div { background: #0d1120 !important; border: 1px solid #1e2a45 !important; border-radius: 6px !important; }
  [data-testid="stProgressBar"] > div > div { background: linear-gradient(90deg, #00d4ff, #00e599) !important; border-radius: 6px !important; }

  /* ── Alerts ── */
  [data-testid="stAlert"] { border-radius: 8px !important; border-width: 1px !important; }
  div[data-testid="stAlert"][data-baseweb="notification"] { background: #0d1120 !important; }

  /* ── Captions ── */
  [data-testid="stCaptionContainer"] p { color: #3f5a80 !important; font-size: 0.78rem !important; }

  /* ── Signal banners ── */
  .signal-banner-on {
    background: #031a0c; border: 1px solid #00e599; border-radius: 8px;
    padding: 12px 18px; color: #00e599; font-weight: 700; font-size: 1.0rem;
  }
  .signal-banner-off {
    background: #0d1120; border: 1px solid #1e2a45; border-radius: 8px;
    padding: 12px 18px; color: #5a7ba0; font-size: 0.92rem;
  }
  .signal-banner-wait {
    background: #170e00; border: 1px solid #f5a623; border-radius: 8px;
    padding: 12px 18px; color: #f5a623; font-size: 0.92rem;
  }

  /* ── Research badge ── */
  .research-badge {
    font-size: 0.79rem; color: #3f5a80; font-style: italic;
    margin-bottom: 8px; padding-bottom: 6px; border-bottom: 1px solid #1e2a45;
  }

  /* ── Condition rows ── */
  .condition-row { font-size: 0.91rem; line-height: 1.85; color: #7fa8cc; }

  /* ── Scrollbar ── */
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-track { background: #080b15; }
  ::-webkit-scrollbar-thumb { background: #1e2a45; border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: #2a3a5a; }
</style>
""", unsafe_allow_html=True)

# ── Paths ──────────────────────────────────────────────────────────────────────
# Bot metadata (common across all workspaces)
BOT_META = {
    "NIFTY_BB_OB": {
        "name":     "Nifty BB Overbought",
        "script":   "nifty_bb_overbought_bot.py",
        "research": "IS +3.63 | OOS +10.65 | MC 100%",
    },
    "NIFTY_TS": {
        "name":     "Nifty Trend Seller",
        "script":   "nifty_trend_seller_bot.py",
        "research": "OOS +1.735 | MC 99.2% | 10/10 stages",
    },
    "SENSEX_TS": {
        "name":     "SENSEX Trend Seller",
        "script":   "sensex_trend_seller_bot.py",
        "research": "OOS +2.181 | WR 73.5% | SHORT-ONLY",
    },
    "GAP_FADE": {
        "name":     "Pre-Open Gap Fade",
        "script":   "preopen_gap_fade_bot.py",
        "research": "OOS +7.26 | Gap ≥2% equities",
    },
    # "GAP_FADE_EOD": RETIRED 2026-06-04
    "HTF_PO3": {
        "name":     "HTF PO3 Bot",
        "script":   "htf_po3_bot.py",
        "research": "IS NIFTY +7.70 / BNF +5.96 | OOS +4.92/+4.96 | 10/10 stages",
    },
    "BNF_BB_OPT": {
        "name":     "BANKNIFTY BB Options",
        "script":   "banknifty_bb_options_bot.py",
        "research": "IS +2.20 | OOS +2.69 | WR 86% | 10/10 stages",
    },
    "BB_MEAN_REV": {
        "name":     "BB Mean Reversion",
        "script":   "bb_mean_reversion_bot.py",
        "research": "IS +1.008 | OOS +2.425 | MC 97.3% | NatRR≥1.25 | 10/10 stages",
    },
    "HA_OPTIONS": {
        "name":     "HA Options Bot",
        "script":   "ha_options_bot.py",
        "research": "OOS +10.14/+9.95/+9.85 | MC 100% | 10/10 stages | 3 instruments",
    },
    # "EMA_SWING": RETIRED 2026-06-04
    "NTS_OBI": {
        "name":     "NTS + OBI Gate",
        "script":   "nifty_trend_seller_obi_bot.py",
        "research": "NTS OOS +1.735 | OBI depth-50 gate | 15-session experiment",
    },
    "NIFTY_MACD_MAP": {
        "name":     "Nifty MACD Map",
        "script":   "nifty_macd_map_bot/nifty_macd_map_bot.py",
        "research": "OOS Sharpe 8.99 | WR 78.9% | 38 trades | 10/10 stages",
    },
    "NIFTY_EOD_HOLD": {
        "name":     "NIFTY EOD Hold Bot",
        "script":   "nifty_eod_hold_bot.py",
        "research": "OOS +2.867/+2.353 | WR 73%/75% | 10/10 stages",
    },
    # "EQUITY_OBI": retired 2026-06-19 — WR 38.3%, P&L −₹4,122 (149 trades, 23 sessions)
    "IRON_FLY_WEEKLY": {
        "name":     "NIFTY Iron Fly Weekly",
        "script":   "iron_fly_weekly_bot.py",
        "research": "OOS +1.87 | WR 65.2% | SL ₹20K | 10 lots NRML | 10/10 stages",
    },
    "SENSEX_IRON_FLY_WEEKLY": {
        "name":     "SENSEX Iron Fly Weekly",
        "script":   "sensex_iron_fly_weekly_bot.py",
        "research": "OOS +4.227 | WR 81.0% | PT 50% | 1 lot BFO | 10/10 stages",
    },
    "BNF_IRON_FLY_MONTHLY": {
        "name":     "BANKNIFTY Iron Fly Monthly",
        "script":   "banknifty_iron_fly_monthly_bot/main.py",
        "research": "OOS +1.555 | WR 85.7% | 10 lots NRML monthly | 10/10 stages",
    },
    "FLAT_BLUE_LINE_MONTHLY": {
        "name":     "Flat Blue Line Monthly",
        "script":   "flat_blue_line_monthly_bot/flat_blue_line_monthly_bot.py",
        "research": "NIFTY OOS +1.77 WR 71% | BN OOS +2.86 WR 85% | 6-leg NRML monthly | 10/10 stages",
    },
    "NIFTY_MA_CROSS_SELLER": {
        "name":     "NIFTY MA Cross Seller",
        "script":   "nifty_ma_cross_seller_bot/nifty_ma_cross_seller_bot.py",
        "research": "OOS +2.00 (w/ Week-2 filter) | WR 58.6% | SL 3× | NRML overnight | 10/10 stages",
    },
    "MACD_M2_SELL": {
        "name":     "MACD M2 Sell Options",
        "script":   "macd_m2_sell_options_bot/macd_m2_sell_options_bot.py",
        "research": "IS +9.42 OOS +6.16 | WR 71% | SL 1.5× | WF 10/10 | 8/8 gates | NIFTY+BANKNIFTY",
    },
}

NIFTY_LOT_SIZE    = 65   # used in NTS_OBI MTM calculation (updated Dec 2025 revision)
LOTS              = 10   # default number of lots per options trade

# ══════════════════════════════════════════════════════════════════════════════
#  BOT CONTROL — registry reader + command sender
# ══════════════════════════════════════════════════════════════════════════════



# ══════════════════════════════════════════════════════════════════════════════
#  BOT CONTROL — registry reader + command sender
# ──────────────────────────────────────────────────────────────────────────────

def _load_registry() -> dict:
    """Load registry for the currently selected workspace."""
    data = _load(REGISTRY_FILE) if REGISTRY_FILE.exists() else None
    return data or {}


def _send_command(action: str, bot_name: str = "") -> None:
    """Send command to the currently selected workspace."""
    try:
        existing: list = []
        if COMMANDS_FILE.exists():
            try:
                existing = json.loads(COMMANDS_FILE.read_text()) or []
            except Exception:
                existing = []
        from datetime import datetime as _dt
        existing.append({
            "action": action,
            "bot":    bot_name,
            "ts":     _dt.now().isoformat(),
        })
        COMMANDS_FILE.write_text(json.dumps(existing, indent=2))
    except Exception as exc:
        st.error(f"Failed to write command: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  BOT CONTROL — render panel
# ══════════════════════════════════════════════════════════════════════════════

def render_bot_controls() -> None:
    st.subheader("⚙️ Bot Controls")
    registry = _load_registry()
    if not registry:
        st.error("⚠️ Launcher registry not found for selected account.")
        return
        
    launcher_pid = registry.get("launcher_pid", "—")
    updated_at   = registry.get("updated_at", "")
    bots_status  = registry.get("bots", {})
    
    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("Launcher PID", str(launcher_pid))
    mc2.metric("Registry Updated", updated_at[11:19] if len(updated_at) > 18 else updated_at)
    mc3.metric("Running Bots", f"{sum(1 for b in bots_status.values() if b.get('status') == 'running')} / {len(bots_status)}")

    bc1, bc2, _bc3 = st.columns([2, 2, 6])
    if bc1.button("▶ Start All", key="start_all_btn"):
        _send_command("start_all")
        st.rerun()
    if bc2.button("■ Stop All", key="stop_all_btn"):
        _send_command("stop_all")
        st.rerun()
    
    st.markdown("---")

    # ── Per-bot rows ──────────────────────────────────────────────────────────
    st.markdown("**Per-Bot Controls**")

    # Header row
    hdr = st.columns([4, 2, 1, 1, 2])
    hdr[0].markdown("**Bot**")
    hdr[1].markdown("**Status**")
    hdr[2].markdown("**Start**")
    hdr[3].markdown("**Stop**")
    hdr[4].markdown("**PID**")
    st.markdown("<hr style='margin:4px 0'>", unsafe_allow_html=True)

    for bot_name, bot_info in bots_status.items():
        status     = bot_info.get("status", "unknown")
        pid        = bot_info.get("pid")
        is_infra   = bot_info.get("is_infra", False)
        is_sched   = bot_info.get("is_daily_scheduler", False)

        is_running = (status == "running")
        status_dot = "🟢" if is_running else "🔴"
        tag        = " *(infra)*" if is_infra else (" *(daily)*" if is_sched else "")

        row = st.columns([4, 2, 1, 1, 2])
        row[0].markdown(f"{status_dot} **{bot_name}**{tag}")
        row[1].markdown(
            f"<span style='color:{'#4ade80' if is_running else '#f87171'}'>"
            f"{status.upper()}</span>",
            unsafe_allow_html=True,
        )

        safe_name = bot_name.replace(" ", "_").replace("+", "plus")

        with row[2]:
            if st.button(
                "▶",
                key=f"ctrl_start_{safe_name}",
                disabled=is_running,
                help=f"Start {bot_name}",
                width='stretch',
            ):
                _send_command("start", bot_name)
                st.toast(f"▶ Starting {bot_name}…", icon="🚀")
                time.sleep(0.5)
                st.rerun()

        with row[3]:
            if st.button(
                "■",
                key=f"ctrl_stop_{safe_name}",
                disabled=not is_running,
                help=f"Stop {bot_name}",
                width='stretch',
            ):
                _send_command("stop", bot_name)
                st.toast(f"■ Stopping {bot_name}…", icon="🛑")
                time.sleep(0.5)
                st.rerun()

        row[4].markdown(f"<small>{pid or '—'}</small>", unsafe_allow_html=True)

    st.markdown("---")
    st.caption(
        "Commands are written to `live_trading/logs/bot_commands.json` and picked up "
        "by the launcher within ~5 seconds.  The registry refreshes every 5 seconds."
    )


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _load(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _staleness(last_update_str: str) -> tuple[float, str]:
    """Return (seconds_ago, human_label)."""
    try:
        dt  = datetime.fromisoformat(last_update_str)
        now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
        diff = (now - dt).total_seconds()
        if diff < 10:
            return diff, f"🟢 {diff:.1f}s ago"
        if diff < 30:
            return diff, f"🟡 {diff:.0f}s ago"
        return diff, f"🔴 {diff:.0f}s ago (STALE)"
    except Exception:
        return 9999, "⚫ unknown"


def _process_running(script_name: str) -> bool:
    """Check if a bot script is running via psutil."""
    try:
        import psutil
        for proc in psutil.process_iter(["cmdline"]):
            try:
                cmd = " ".join(proc.info.get("cmdline") or [])
                if script_name in cmd:
                    return True
            except Exception:
                pass
    except ImportError:
        pass
    return False


def _positionbook_ltps(symbols: set, pb: dict | None = None) -> dict:
    """Fetch live LTPs from OpenAlgo positionbook for the given symbol set.

    Accepts an optional pre-fetched positionbook to avoid a redundant HTTP call.
    """
    if pb is None:
        pb = _fetch_positionbook_full()
    return {
        sym: d["ltp"]
        for sym, d in pb.items()
        if sym in symbols and d["ltp"]
    }


def _fetch_positionbook_full() -> dict:
    """Fetch ALL positions from OpenAlgo positionbook for the selected workspace."""
    try:
        api_key = _get_api_key()
        host    = _get_host()
        if not api_key:
            return {}
        resp = requests.post(
            f"{host}/api/v1/positionbook",
            json={"apikey": api_key},
            timeout=4,
        )
        data = resp.json()
        if data.get("status") != "success":
            return {}
        result = {}
        for pos in data.get("data", []):
            sym = pos.get("symbol", "")
            if not sym:
                continue
            result[sym] = {
                "quantity":      int(pos.get("quantity", 0)),
                "average_price": float(pos.get("average_price") or 0),
                "ltp":           float(pos.get("ltp") or 0),
                "pnl":           float(pos.get("pnl") or 0),
                "exchange":      pos.get("exchange", ""),
                "product":       pos.get("product", ""),
                # Buy/sell breakdown — non-zero even for flat positions
                "buy_qty":       int(pos.get("buy_qty", 0) or 0),
                "sell_qty":      int(pos.get("sell_qty", 0) or 0),
                "buy_avg":       float(pos.get("buy_avg", 0) or 0),
                "sell_avg":      float(pos.get("sell_avg", 0) or 0),
            }
        return result
    except Exception:
        return {}


def _option_exchange(symbol: str) -> str:
    """Derive the correct exchange from an option symbol name.

    BSE derivatives (BFO): symbols starting with SENSEX or BANKEX.
    NSE derivatives (NFO): NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, etc.
    """
    s = symbol.upper()
    if s.startswith(("SENSEX", "BANKEX")):
        return "BFO"
    return "NFO"


def _quotes_ltps(sym_exchange: dict) -> dict:
    """Return live LTPs for the given symbols.

    Strategy (fastest → slowest, stops when all symbols are satisfied):
      1. WebSocket LTP cache — populated by the background thread in real-time.
         Any symbol not yet in the cache is enqueued for WS subscription so it
         will appear on subsequent refreshes.
      2. REST fallback via /api/v1/multiquotes — used only for symbols not yet
         in the cache (typically only on the very first refresh after startup).

    This eliminates the 429 rate-limit errors caused by polling the Fyers REST
    quotes API on every 3-second dashboard refresh.
    """
    if not sym_exchange:
        return {}

    # Ensure background WS thread is running
    _ensure_ws_cache_thread()

    result: dict[str, float] = {}
    need_rest: dict[str, str] = {}

    with _WS_CACHE_LOCK:
        cache_snapshot = dict(_WS_LTP_CACHE)

    for sym, exch in sym_exchange.items():
        if not sym:
            continue
        if sym in cache_snapshot:
            result[sym] = cache_snapshot[sym]
        else:
            # Not in cache yet — enqueue for WS subscription
            _subscribe_symbol(sym, exch)
            need_rest[sym] = exch

    # REST fallback for symbols not yet in cache (cold-start only)
    if need_rest:
        try:
            api_key = _get_api_key()
            host    = _get_host()

            if api_key:
                symbols_list = [
                    {"symbol": s, "exchange": e}
                    for s, e in need_rest.items()
                ]
                resp = requests.post(
                    f"{host}/api/v1/multiquotes",
                    json={"apikey": api_key, "symbols": symbols_list},
                    timeout=5,
                )
                data = resp.json()
                if data.get("status") == "success":
                    for item in data.get("results", []):
                        s   = item.get("symbol", "")
                        ltp = (item.get("data") or {}).get("ltp")
                        if s and ltp not in (None, "", 0):
                            result[s] = float(ltp)
                            # Seed the cache so next refresh is WS-only
                            with _WS_CACHE_LOCK:
                                _WS_LTP_CACHE[s] = float(ltp)
        except Exception:
            pass

    return result


def _all_ltps(sym_exchange: dict, pb: dict | None = None) -> dict:
    """Get LTPs from positionbook first; fall back to quotes API for missing symbols.

    Args:
        sym_exchange: {symbol: exchange} mapping from _collect_all_open_symbols()
        pb: optional pre-fetched positionbook to avoid a redundant HTTP call
    """
    symbols  = set(sym_exchange.keys())
    pb_ltps  = _positionbook_ltps(symbols, pb=pb)
    missing  = symbols - set(pb_ltps.keys())
    if missing:
        missing_map = {s: sym_exchange[s] for s in missing}
        pb_ltps.update(_quotes_ltps(missing_map))
    return pb_ltps


def _collect_all_open_symbols() -> dict:
    """Collect every open position symbol across ALL bots.

    Returns a {symbol: exchange} dict used by _all_ltps() for a single
    multiquotes call that covers options (NFO/BFO), equities (NSE), etc.

    Exchange rules:
      • Options: derived from symbol prefix via _option_exchange()
            SENSEX/BANKEX → BFO   |   everything else → NFO
      • Gap Fade equities  → NSE
      • EMA Swing equities → NSE
    """
    sym_exchange: dict[str, str] = {}

    def _add_opt(symbol: str) -> None:
        if symbol:
            sym_exchange[symbol] = _option_exchange(symbol)

    def _add_eq(symbol: str) -> None:
        if symbol:
            sym_exchange[symbol] = "NSE"

    # ── Options bots ─────────────────────────────────────────────────────────
    for key in ("NIFTY_BB_OB", "NIFTY_TS", "SENSEX_TS"):
        state = _load(STATE_FILES[key])
        if not state:
            continue
        if key == "NIFTY_BB_OB":
            t = state.get("active_trade")
            if t:
                _add_opt(t.get("symbol", ""))
        else:
            for leg_dict in state.get("active_trades", {}).values():
                if leg_dict and isinstance(leg_dict, dict):
                    _add_opt(leg_dict.get("symbol", ""))

    # HTF PO3 bot — two independent state machines keyed by index symbol
    po3_state = _load(STATE_FILES["HTF_PO3"])
    if po3_state:
        for sym_key in ("NIFTY", "BANKNIFTY"):
            instrument = po3_state.get(sym_key, {})
            if isinstance(instrument, dict):
                active = instrument.get("active")
                if active and isinstance(active, dict):
                    _add_opt(active.get("symbol", ""))

    # BankNifty BB Options bot
    bnf_state = _load(STATE_FILES["BNF_BB_OPT"])
    if bnf_state:
        t = bnf_state.get("active_trade")
        if t and isinstance(t, dict):
            _add_opt(t.get("symbol", ""))

    # BB Mean Reversion bot
    bbmr_state = _load(STATE_FILES["BB_MEAN_REV"])
    if bbmr_state:
        t = bbmr_state.get("active_trade")
        if t and isinstance(t, dict):
            _add_opt(t.get("symbol", ""))

    # HA Options bot — multi-instrument, each keyed by symbol name
    ha_state = _load(STATE_FILES["HA_OPTIONS"])
    if ha_state:
        for inst_dict in ha_state.get("instruments", {}).values():
            if isinstance(inst_dict, dict):
                t = inst_dict.get("active_trade")
                if t and isinstance(t, dict):
                    _add_opt(t.get("symbol", ""))

    # NTS + OBI Gate bot — single option leg (ATM CE)
    nts_obi_state = _load(STATE_FILES["NTS_OBI"])
    if nts_obi_state:
        trade = nts_obi_state.get("active_trade")
        if trade and isinstance(trade, dict):
            _add_opt(trade.get("symbol", ""))

    # NIFTY MACD Map bot — independent PE and CE legs
    macd_map_state = _load(STATE_FILES["NIFTY_MACD_MAP"])
    if macd_map_state:
        for leg_key in ("active_pe", "active_ce"):
            t = macd_map_state.get(leg_key)
            if t and isinstance(t, dict):
                _add_opt(t.get("symbol", ""))

    # ── Equity bots ───────────────────────────────────────────────────────────
    # Pre-Open Gap Fade — equity positions on NSE (open between 09:15–10:00)
    gap_state = _load(STATE_FILES["GAP_FADE"])
    if gap_state:
        for sym, pos in gap_state.get("positions", {}).items():
            if not pos.get("exit_price"):    # still open
                _add_eq(sym)

    # Gap Fade EOD — RETIRED 2026-06-04
    # EMA Swing Scanner — RETIRED 2026-06-04

    # Equity OBI bot — active positions and ghosts
    # equity_obi_state removed — RETIRED 2026-06-19

    # ── NIFTY Iron Fly Weekly — 4 NRML legs (SELL CE, SELL PE, BUY CE, BUY PE) ─
    iron_fly_state = _load(STATE_FILES["IRON_FLY_WEEKLY"])
    if iron_fly_state and not iron_fly_state.get("closed", True):
        legs = iron_fly_state.get("legs", {})
        for leg_key in ("sell_ce", "sell_pe", "buy_ce", "buy_pe"):
            leg = legs.get(leg_key, {})
            _add_opt(leg.get("symbol", ""))

    # ── SENSEX Iron Fly Weekly — 4 NRML legs (BFO; _add_opt routes SENSEX* → BFO) ─
    sensex_if_state = _load(STATE_FILES["SENSEX_IRON_FLY_WEEKLY"])
    if sensex_if_state and not sensex_if_state.get("closed", True):
        legs = sensex_if_state.get("legs", {})
        for leg_key in ("sell_ce", "sell_pe", "buy_ce", "buy_pe"):
            leg = legs.get(leg_key, {})
            _add_opt(leg.get("symbol", ""))

    # ── BANKNIFTY Iron Fly Monthly — 4 NRML legs ──────────────────────────────
    bnf_if_state = _load(STATE_FILES["BNF_IRON_FLY_MONTHLY"])
    if bnf_if_state and not bnf_if_state.get("closed", True):
        legs = bnf_if_state.get("legs", {})
        for leg_key in ("sell_ce", "sell_pe", "buy_ce", "buy_pe"):
            leg = legs.get(leg_key, {})
            _add_opt(leg.get("symbol", ""))

    # ── Flat Blue Line Monthly — 6 NRML legs per instrument (NIFTY + BANKNIFTY) ─
    fbl_state = _load(STATE_FILES["FLAT_BLUE_LINE_MONTHLY"])
    if fbl_state:
        for inst in ("NIFTY", "BANKNIFTY"):
            inst_state = fbl_state.get(inst, {})
            if not inst_state.get("closed", True):
                legs = inst_state.get("legs", {})
                for leg_key in ("atm_c", "atm_p", "sc", "sp", "hc", "hp"):
                    leg = legs.get(leg_key, {})
                    _add_opt(leg.get("symbol", ""))

    # ── NIFTY MA Cross Seller — 1 NRML leg (overnight ATM CE or PE) ───────────
    ma_cross_state = _load(STATE_FILES.get("NIFTY_MA_CROSS_SELLER"))
    if ma_cross_state:
        t = ma_cross_state.get("active_trade")
        if t and isinstance(t, dict):
            _add_opt(t.get("symbol", ""))

    return sym_exchange


def _tick(ok: bool) -> str:
    return "✅" if ok else "❌"


def _pnl_str(val: float, show_sign: bool = True) -> str:
    sign  = "+" if val > 0 and show_sign else ""
    color = "#00e599" if val > 0 else ("#f87171" if val < 0 else "#5a7ba0")
    return f'<span style="color:{color};font-weight:700">{sign}₹{val:,.0f}</span>'


def _entry_window_open(window_str: str) -> bool:
    """Parse 'HH:MM–HH:MM' and check if now is within it."""
    try:
        parts   = window_str.replace("–", "-").split("-")
        sh, sm  = map(int, parts[0].strip().split(":"))
        eh, em  = map(int, parts[1].strip().split(":"))
        now_min = datetime.now().hour * 60 + datetime.now().minute
        return (sh * 60 + sm) <= now_min <= (eh * 60 + em)
    except Exception:
        return False


def _bb_progress(close: float, lower: float, upper: float) -> tuple[float, str]:
    """Return (0–1 progress value, band description) for the BB visual."""
    span = upper - lower
    if span <= 0:
        return 0.5, "—"
    pct = (close - lower) / span
    pct = max(0.0, min(1.0, pct))
    label_zone = (
        "🔴 Above upper (OVERBOUGHT)" if close > upper else
        "🟢 Below lower (OVERSOLD)"   if close < lower else
        "⚪ Inside bands"
    )
    return pct, label_zone


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — BOT HEARTBEATS
# ══════════════════════════════════════════════════════════════════════════════

def render_heartbeats():
    st.subheader("🤖 Bot Heartbeats")

    keys_in_order = [
        # Options bots (intraday)
        "NIFTY_BB_OB", "NIFTY_TS", "SENSEX_TS", "HTF_PO3",
        "BNF_BB_OPT", "BB_MEAN_REV", "HA_OPTIONS", "NIFTY_MACD_MAP", "NIFTY_EOD_HOLD", "NTS_OBI",
        "IRON_FLY_WEEKLY", "SENSEX_IRON_FLY_WEEKLY", "BNF_IRON_FLY_MONTHLY",
        "MACD_M2_SELL",
        # Stock bots
        "GAP_FADE",
        # "GAP_FADE_EOD", "EMA_SWING",  # RETIRED 2026-06-04
        # "EQUITY_OBI",  # RETIRED 2026-06-19
    ]

    # ── Header ────────────────────────────────────────────────────────────────
    hdr = st.columns([0.4, 3.2, 2.2, 5.2])
    hdr[0].markdown("<small>**●**</small>", unsafe_allow_html=True)
    hdr[1].markdown("<small>**Bot**</small>", unsafe_allow_html=True)
    hdr[2].markdown("<small>**Status**</small>", unsafe_allow_html=True)
    hdr[3].markdown("<small>**Research / Notes**</small>", unsafe_allow_html=True)
    st.markdown("<hr style='margin:2px 0 6px 0; opacity:0.3'>", unsafe_allow_html=True)

    for key in keys_in_order:
        meta = BOT_META[key]

        # ── Determine status for each bot type ────────────────────────────────
        state = _load(STATE_FILES[key])
        if not state:
            dot, status, color = "⚫", "OFFLINE", "#f87171"
        else:
            ts_key = "last_update" if "last_update" in state else "updated_at"
            _, label = _staleness(state.get(ts_key, ""))
            # Strip the leading emoji from the label — we render it separately
            time_part = label.split(" ", 1)[1] if " " in label else label
            if "🟢" in label:
                dot, status, color = "🟢", time_part, "#4ade80"
            elif "🟡" in label:
                dot, status, color = "🟡", time_part, "#fbbf24"
            else:
                dot, status, color = "🔴", time_part, "#f87171"

        # ── Render one row ────────────────────────────────────────────────────
        row = st.columns([0.4, 3.2, 2.2, 5.2])
        row[0].markdown(dot)
        row[1].markdown(f"**{meta['name']}**")
        row[2].markdown(
            f"<span style='color:{color}; font-size:0.88em'>{status}</span>",
            unsafe_allow_html=True,
        )
        row[3].markdown(
            f"<span style='color:#9ca3af; font-size:0.82em'>{meta['research']}</span>",
            unsafe_allow_html=True,
        )
        st.markdown("<hr style='margin:3px 0; opacity:0.12'>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS — today's-trades DB loader and rich card renderer
# ══════════════════════════════════════════════════════════════════════════════

def _load_today_trades(bot_name: str) -> list[dict]:
    """Return today's completed trades for *bot_name* from performance.db.

    Columns returned (all present; missing DB columns return None):
        symbol, direction, option_type, entry_time, exit_time,
        entry_price, exit_price, quantity, lots, lot_size,
        gross_pnl, exit_reason, hold_duration_mins, decay_pct, won, source
    """
    import sqlite3
    db_path = LOGS_DIR / "performance.db"
    if not db_path.exists():
        return []
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cur  = conn.cursor()
        cur.execute(
            """SELECT symbol, direction, option_type, entry_time, exit_time,
                      entry_price, exit_price, quantity, lots, lot_size,
                      gross_pnl, exit_reason, hold_duration_mins, decay_pct, won, source
               FROM trades
               WHERE bot_name = ? AND trade_date = date('now', 'localtime')
               ORDER BY entry_time""",
            (bot_name,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


def _render_today_trades_detail(trades: list[dict], compact: bool = False) -> None:
    """Render today's completed trades from performance.db as a styled dashboard section.

    compact=True  → summary table (equity / multi-trade bots, 3+ trades).
    compact=False → metric cards (single/dual-trade options bots, ≤2 trades).

    Renders nothing when *trades* is empty.
    """
    if not trades:
        return

    n = len(trades)
    label = "Today's completed trades" if n > 1 else "Today's completed trade"
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:8px;margin:14px 0 10px;">'
        f'<span style="font-size:12px;font-weight:500;color:var(--secondary-text-color,#6b7280);'
        f'text-transform:uppercase;letter-spacing:0.05em;">{label}</span>'
        f'<hr style="flex:1;border:none;border-top:1px solid rgba(128,128,128,0.2);margin:0 0 0 8px;">'
        f'</div>',
        unsafe_allow_html=True,
    )

    if compact or n > 2:
        # ── Compact table view (equity / high-frequency bots) ──────────────────
        rows = []
        for t in trades:
            gross   = float(t.get("gross_pnl") or 0)
            opt     = (t.get("option_type") or "").upper()
            dir_str = (t.get("direction") or "").upper()
            side    = f"{dir_str} {opt}".strip() if opt else dir_str
            rows.append({
                "Symbol":   t.get("symbol", ""),
                "Side":     side,
                "Entry ₹":  round(float(t.get("entry_price") or 0), 2),
                "Exit ₹":   round(float(t.get("exit_price") or 0), 2),
                "Qty":      t.get("quantity") or 0,
                "Dur (min)": t.get("hold_duration_mins") or 0,
                "Gross P&L": round(gross, 0),
                "Reason":   t.get("exit_reason") or "—",
            })
        import pandas as pd
        df = pd.DataFrame(rows)
        st.dataframe(
            df.style
              .map(lambda v: "color: #15803d; font-weight:600" if v > 0
                             else ("color: #dc2626; font-weight:600" if v < 0 else ""),
                   subset=["Gross P&L"])
              .format({"Entry ₹": "₹{:.2f}", "Exit ₹": "₹{:.2f}",
                       "Gross P&L": lambda v: f"+₹{v:,.0f}" if v >= 0 else f"₹{v:,.0f}"}),
            use_container_width=True, hide_index=True,
        )
        total = sum(float(t.get("gross_pnl") or 0) for t in trades)
        wins  = sum(1 for t in trades if t.get("won", 0))
        sc1, sc2, sc3 = st.columns(3)
        pnl_dir = "normal" if total >= 0 else "inverse"
        sc1.metric("Total Gross P&L",
                   f"{'+'if total>=0 else ''}₹{total:,.0f}", delta_color=pnl_dir)
        sc2.metric("Trades Today", n)
        sc3.metric("Win / Loss", f"{wins} / {n - wins}")
    else:
        # ── Rich card view (single-trade or dual-leg options bots) ────────────
        for t in trades:
            won       = bool(t.get("won", 0))
            gross     = float(t.get("gross_pnl") or 0)
            entry_p   = float(t.get("entry_price") or 0)
            exit_p    = float(t.get("exit_price") or 0)
            qty       = int(t.get("quantity") or 0)
            lots      = t.get("lots")
            lot_size  = t.get("lot_size")
            dur       = t.get("hold_duration_mins") or 0
            decay     = t.get("decay_pct")
            reason    = t.get("exit_reason") or "—"
            opt_type  = (t.get("option_type") or "").upper()
            direction = (t.get("direction") or "").upper()
            symbol    = t.get("symbol", "")
            source    = t.get("source") or "paper"
            entry_t   = (t.get("entry_time") or "")[:16].replace("T", " ")
            exit_t    = (t.get("exit_time") or "")[:16].replace("T", " ")
            pnl_sign  = "+" if gross >= 0 else ""
            side_lbl  = f"{direction} {opt_type}".strip() if opt_type else direction
            outcome   = "🟢 WIN" if won else "🔴 LOSS"

            with st.container(border=True):
                h1, h2, h3 = st.columns([4, 1, 1])
                h1.markdown(f"**{symbol}**")
                h2.markdown(f"`{side_lbl}`")
                h3.markdown(f"**{outcome}**")

                t1, t2, t3, t4, t5, t6 = st.columns(6)
                t1.metric("Entry ₹", f"{entry_p:.2f}", help=f"at {entry_t}")
                t2.metric("Exit ₹",  f"{exit_p:.2f}",  help=f"at {exit_t}")
                qty_help = f"{lots}L × {lot_size}" if lots and lot_size else str(qty)
                t3.metric("Qty", f"{qty:,}", help=qty_help)
                if decay is not None:
                    t4.metric("Decay %",  f"{decay:+.2f}%")
                else:
                    move_pct = (exit_p - entry_p) / entry_p * 100 if entry_p else 0
                    t4.metric("Move %", f"{move_pct:+.2f}%")
                t5.metric("Duration", f"{dur} min")
                t6.metric(
                    "Gross P&L", f"{pnl_sign}₹{gross:,.0f}",
                    delta_color="normal" if gross >= 0 else "inverse",
                )
                st.caption(
                    f"Exit reason: **{reason}** &nbsp;|&nbsp; "
                    f"entry {entry_t} → exit {exit_t} &nbsp;|&nbsp; "
                    f"source: {source}"
                )


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — PORTFOLIO SNAPSHOT (all bots combined)
# ══════════════════════════════════════════════════════════════════════════════

def render_portfolio_snapshot(ltps: dict, positionbook: dict | None = None):
    st.subheader("💰 Portfolio Snapshot")

    # ── Ground truth: use pre-fetched positionbook (avoids duplicate HTTP call) ─
    if positionbook is None:
        positionbook = _fetch_positionbook_full()
    pb_available = bool(positionbook)

    state_symbols: set[str] = set()
    open_rows   = []   # live positions — show LTP, SL, TGT, live MTM
    closed_rows = []   # realised positions — show Exit ₹, Reason, Net P&L

    def _pb_is_open(sym: str) -> bool:
        if not pb_available: return True
        entry = positionbook.get(sym)
        return entry is not None and entry["quantity"] != 0

    def _pb_ltp(sym: str, fallback: float) -> float:
        entry = positionbook.get(sym)
        if entry and entry["ltp"]: return entry["ltp"]
        return ltps.get(sym, fallback)

    def _pb_pnl(sym: str) -> float | None:
        entry = positionbook.get(sym)
        if entry and entry["quantity"] == 0: return entry["pnl"]
        return None

    def _pb_avg(sym: str) -> float:
        entry = positionbook.get(sym)
        return entry["average_price"] if entry else 0.0

    def _add_open(bot, sym, side, entry, ltp, sl, tgt, qty, pnl, since):
        state_symbols.add(sym)
        if pb_available and sym in positionbook and positionbook[sym]["quantity"] == 0:
            broker_pnl = positionbook[sym]["pnl"]
            broker_avg = positionbook[sym]["average_price"]
            exit_px = broker_avg if broker_avg else ltp
            closed_rows.append({
                "Bot": bot, "Symbol": sym, "Side": side,
                "Entry ₹": entry, "Exit ₹": exit_px,
                "Qty": qty, "Net P&L ₹": broker_pnl,
                "Reason": "⚠️ state stale (broker: flat)",
            })
            return
        open_rows.append({
            "Bot": bot, "Symbol": sym, "Side": side,
            "Entry ₹": entry, "LTP ₹": ltp, "SL ₹": sl, "TGT ₹": tgt,
            "Qty": qty, "MTM ₹": pnl, "Since": since,
        })

    def _add_closed(bot, sym, side, entry, exit_px, qty, net_pnl, reason):
        state_symbols.add(sym)
        broker_pnl = _pb_pnl(sym)
        final_pnl  = broker_pnl if broker_pnl is not None else net_pnl
        closed_rows.append({
            "Bot": bot, "Symbol": sym, "Side": side,
            "Entry ₹": entry, "Exit ₹": exit_px,
            "Qty": qty, "Net P&L ₹": final_pnl, "Reason": reason,
        })

    def _add_closed_from_db(bot, sym, side, entry, exit_px, qty, pnl, reason):
        """Like _add_closed but trusts the passed pnl directly (no positionbook override).
        Use when the source is performance_db, which is always more reliable than sandbox P&L."""
        state_symbols.add(sym)
        closed_rows.append({
            "Bot": bot, "Symbol": sym, "Side": side,
            "Entry ₹": entry, "Exit ₹": exit_px,
            "Qty": qty, "Net P&L ₹": pnl, "Reason": reason,
        })

    def _strategy_key(bot: str) -> str:
        """Collapse a per-leg 'Bot' label down to its parent strategy name.

        Multi-leg bots label each leg as "{strategy} ({leg_id})", e.g.
        "FBL NIFTY (atm_c)", "BNF IF (sc)", "Iron Fly (sell_ce)". Stripping the
        trailing parenthetical groups simultaneous legs back into one strategy
        so combined MTM can be shown instead of a row per leg. Single-leg bots
        (no parenthetical suffix) pass through unchanged and form size-1 groups.
        """
        return re.sub(r"\s*\([^)]*\)\s*$", "", bot).strip()

    def _group_open_rows(rows: list[dict]) -> "OrderedDict[str, list[dict]]":
        """Bucket open_rows by parent strategy, preserving first-seen order."""
        groups: "OrderedDict[str, list[dict]]" = OrderedDict()
        for r in rows:
            groups.setdefault(_strategy_key(r["Bot"]), []).append(r)
        return groups

    def _summarize_strategy(strategy: str, legs: list[dict]) -> dict:
        """Combined-MTM summary row for one strategy's legs."""
        since_set = {r["Since"] for r in legs}
        since = next(iter(since_set)) if len(since_set) == 1 else "multiple"
        return {
            "Strategy": strategy, "Legs": len(legs),
            "Combined MTM ₹": sum(r["MTM ₹"] for r in legs), "Since": since,
        }

    # (Rest of the bot-specific logic follows below)
    # _load_today_trades is now a module-level function above render_portfolio_snapshot.

    # ── BB Overbought ──────────────────────────────────────────────────────────
    state = _load(STATE_FILES.get("NIFTY_BB_OB"))
    if state:
        t = state.get("active_trade")
        if t:
            sym   = t.get("symbol", "")
            entry = float(t.get("entry_prem", 0))
            sl    = float(t.get("sl_prem", 0))
            e4    = float(t.get("e4_target", entry * 0.7))
            qty   = int(t.get("qty", 0))
            ltp   = ltps.get(sym, entry)
            pnl   = (entry - ltp) * qty
            since = t.get("entry_time", "")[:16].replace("T", " ")
            _add_open("BB Overbought", sym, "SELL PE", entry, ltp, sl, e4, qty, pnl, since)

    # ── Nifty Trend Seller ─────────────────────────────────────────────────────
    state = _load(STATE_FILES["NIFTY_TS"])
    if state:
        for leg, t in state.get("active_trades", {}).items():
            if not t:
                continue
            sym   = t.get("symbol", "")
            entry = float(t.get("entry_prem", 0))
            sl    = float(t.get("sl_prem", 0))
            qty   = int(t.get("qty", 0))
            ltp   = ltps.get(sym, entry)
            pnl   = (entry - ltp) * qty
            since = t.get("entry_time", "")[:16].replace("T", " ")
            _add_open(f"Nifty TS ({leg})", sym, f"SELL {leg}", entry, ltp, sl, sl / 2, qty, pnl, since)

    # ── SENSEX Trend Seller ────────────────────────────────────────────────────
    state = _load(STATE_FILES["SENSEX_TS"])
    if state:
        ce = state.get("active_trades", {}).get("CE")
        if ce:
            sym   = ce.get("symbol", "")
            entry = float(ce.get("entry_prem", 0))
            sl    = float(ce.get("sl_prem", 0))
            qty   = int(ce.get("qty", 0))
            ltp   = ltps.get(sym, entry)
            pnl   = (entry - ltp) * qty
            since = ce.get("entry_time", "")[:16].replace("T", " ")
            _add_open("SENSEX TS (CE)", sym, "SELL CE", entry, ltp, sl, sl / 2, qty, pnl, since)

    # ── HTF PO3 ────────────────────────────────────────────────────────────────
    state = _load(STATE_FILES["HTF_PO3"])
    if state:
        _htf_db_loaded = False  # load perf_db trades at most once per refresh
        for sym_key, label in (("NIFTY", "NIFTY PE"), ("BANKNIFTY", "BNIFTY PE")):
            instrument = state.get(sym_key, {})
            if not isinstance(instrument, dict):
                continue
            active = instrument.get("active")
            if active:
                # Position still open — show as live
                sym   = active.get("symbol", "")
                entry = float(active.get("entry_prem", 0))
                sl    = float(active.get("sl_prem", entry * 2))
                tgt   = float(active.get("target_prem", entry * 0.7))
                qty   = int(active.get("qty", 0))
                ltp   = ltps.get(sym, entry)
                pnl   = (entry - ltp) * qty
                since = active.get("entry_time", "")[:16].replace("T", " ")
                _add_open(f"HTF PO3 ({label})", sym, "SELL PE", entry, ltp, sl, tgt, qty, pnl, since)
            elif instrument.get("session_traded") and not _htf_db_loaded:
                # Traded and closed today — state doesn't retain the symbol after
                # close, so read from performance_db (authoritative P&L).
                _htf_db_loaded = True  # prevent duplicate rows if both NIFTY & BNF were traded
                for t in _load_today_trades("htf_po3_bot"):
                    sym = t["symbol"]
                    lbl = "BNIFTY PE" if "BANKNIFTY" in sym.upper() else "NIFTY PE"
                    _add_closed_from_db(
                        f"HTF PO3 ({lbl})", sym, "SELL",
                        t["entry_price"], t["exit_price"],
                        t["quantity"], t["gross_pnl"], t["exit_reason"],
                    )

    # ── BankNifty BB Options ───────────────────────────────────────────────────
    state = _load(STATE_FILES["BNF_BB_OPT"])
    if state:
        t = state.get("active_trade")
        if t and isinstance(t, dict):
            # Position still open
            sym      = t.get("symbol", "")
            entry    = float(t.get("entry_prem", 0))
            sl       = float(t.get("sl_prem", 0))
            sma_t    = float(t.get("sma_target", 0))
            qty      = int(t.get("qty", 0))
            opt_type = t.get("opt_type", "?")
            ltp      = ltps.get(sym, entry)
            pnl      = (entry - ltp) * qty
            since    = t.get("entry_time", "")[:16].replace("T", " ")
            _add_open(f"BNF BB ({opt_type})", sym, f"SELL {opt_type}", entry, ltp, sl, sma_t, qty, pnl, since)
        elif state.get("signal_fired"):
            # Trade completed today — state clears active_trade on close so we
            # use performance_db as the authoritative source.
            for t in _load_today_trades("banknifty_bb_options_bot"):
                sym      = t["symbol"]
                opt_type = "PE" if sym.upper().endswith("PE") else "CE"
                _add_closed_from_db(
                    f"BNF BB ({opt_type})", sym, "SELL",
                    t["entry_price"], t["exit_price"],
                    t["quantity"], t["gross_pnl"], t["exit_reason"],
                )

    # ── HA Options Bot ─────────────────────────────────────────────────────────
    state = _load(STATE_FILES["HA_OPTIONS"])
    if state:
        for sym_key, inst_dict in state.get("instruments", {}).items():
            if not isinstance(inst_dict, dict):
                continue
            t = inst_dict.get("active_trade")
            if not t:
                continue
            sym   = t.get("symbol", "")
            side  = t.get("side", "?")
            entry = float(t.get("entry_prem", 0))
            sl    = float(t.get("sl_index", 0))   # index-level SL
            qty   = int(t.get("qty", 0))
            ltp   = ltps.get(sym, entry)
            pnl   = (entry - ltp) * qty
            since = t.get("entry_time", "")[:16].replace("T", " ")
            _add_open(f"HA Options ({sym_key})", sym, f"SELL {side}", entry, ltp, sl, 0.0, qty, pnl, since)

    # ── NIFTY MACD Map ─────────────────────────────────────────────────────────
    state = _load(STATE_FILES["NIFTY_MACD_MAP"])
    if state:
        for leg_key, label in (("active_pe", "SELL PE"), ("active_ce", "SELL CE")):
            t = state.get(leg_key)
            if not t or not isinstance(t, dict):
                continue
            sym   = t.get("symbol", "")
            entry = float(t.get("entry_prem", 0))
            sl    = float(t.get("sl_prem", 0))
            qty   = int(t.get("qty", 0))
            ltp   = ltps.get(sym, entry)
            pnl   = (entry - ltp) * qty
            since = t.get("entry_time", "")[:16].replace("T", " ")
            _add_open(f"MACD Map ({leg_key[-2:].upper()})", sym, label, entry, ltp, sl, 0.0, qty, pnl, since)

    # ── NIFTY Iron Fly Weekly (NRML 4-leg short iron fly) ─────────────────────
    state = _load(STATE_FILES["IRON_FLY_WEEKLY"])
    if state and state.get("legs"):
        qty      = int(state.get("qty", 0))
        legs     = state.get("legs", {})
        entry_t  = (state.get("entry_time") or "")[:16].replace("T", " ")
        sl_total = float(state.get("stop_loss_total", 20000))
        net_cr   = float(state.get("net_credit_per_unit", 0))
        closed   = state.get("closed", False)
        exit_rsn = state.get("exit_reason")

        for leg_key, label, side in (
            ("sell_ce", "Iron Fly (Sell CE)", "SELL CE"),
            ("sell_pe", "Iron Fly (Sell PE)", "SELL PE"),
            ("buy_ce",  "Iron Fly (Buy CE)",  "BUY  CE"),
            ("buy_pe",  "Iron Fly (Buy PE)",  "BUY  PE"),
        ):
            leg = legs.get(leg_key, {})
            if not leg:
                continue
            sym   = leg.get("symbol", "")
            entry = float(leg.get("entry_prem", 0))
            ltp   = ltps.get(sym, entry)

            # MTM: sell legs profit when ltp falls, buy legs profit when ltp rises
            if "sell" in leg_key:
                pnl = (entry - ltp) * qty
                sl  = entry * 2.0   # rough per-leg reference (combined SL managed by bot)
                tgt = 0.0
            else:
                pnl = (ltp - entry) * qty
                sl  = 0.0
                tgt = 0.0

            if closed:
                # NRML weekly — state persists; only show as closed today if exit was today
                exit_time_str = (state.get("exit_time") or "")
                exited_today  = exit_time_str[:10] == datetime.now().date().isoformat()
                if exited_today:
                    exit_px = ltp  # best available when closed
                    reason  = exit_rsn or "exited"
                    _add_closed(f"Iron Fly ({leg_key})", sym, side, entry, exit_px, qty, pnl, reason)
            else:
                _add_open(f"Iron Fly ({leg_key})", sym, side, entry, ltp, sl, tgt, qty, pnl, entry_t)

    # ── BANKNIFTY Iron Fly Monthly (NRML 4-leg short iron fly) ────────────────
    state = _load(STATE_FILES["BNF_IRON_FLY_MONTHLY"])
    if state and state.get("legs"):
        qty      = int(state.get("qty", 0))
        legs     = state.get("legs", {})
        entry_t  = (state.get("entry_time") or "")[:16].replace("T", " ")
        net_cr   = float(state.get("net_credit_per_unit", 0))
        closed   = state.get("closed", False)
        exit_rsn = state.get("exit_reason")

        for leg_key, label, side in (
            ("sell_ce", "BNF IF (Sell CE)", "SELL CE"),
            ("sell_pe", "BNF IF (Sell PE)", "SELL PE"),
            ("buy_ce",  "BNF IF (Buy CE)",  "BUY  CE"),
            ("buy_pe",  "BNF IF (Buy PE)",  "BUY  PE"),
        ):
            leg = legs.get(leg_key, {})
            if not leg:
                continue
            sym   = leg.get("symbol", "")
            entry = float(leg.get("entry_prem", 0))
            ltp   = ltps.get(sym, entry)

            if "sell" in leg_key:
                pnl = (entry - ltp) * qty
                sl  = entry * 2.0
                tgt = net_cr * 0.5 * qty if not closed else 0.0
            else:
                pnl = (ltp - entry) * qty
                sl  = 0.0
                tgt = 0.0

            if closed:
                # NRML monthly — state persists; only show as closed today if exit was today
                exit_time_str = (state.get("exit_time") or "")
                exited_today  = exit_time_str[:10] == datetime.now().date().isoformat()
                if exited_today:
                    _add_closed(f"BNF IF ({leg_key})", sym, side, entry, ltp, qty, pnl, exit_rsn or "exited")
            else:
                _add_open(f"BNF IF ({leg_key})", sym, side, entry, ltp, sl, tgt, qty, pnl, entry_t)

    # ── Flat Blue Line Monthly (NRML 6-leg modified iron fly: NIFTY + BANKNIFTY)
    #    NOTE: previously missing from this aggregator entirely — its open
    #    positions fell through to the generic "📊 Broker" catch-all instead
    #    of showing under the bot's own name. Per-leg SL/TGT aren't meaningful
    #    here (the strategy manages combined breakevens/profit-target across
    #    all 6 legs — see be_lower/be_upper/profit_target in the state), so
    #    those columns are passed as 0.0, matching the Iron Fly/BNF IF pattern
    #    for non-primary legs.
    fbl_state = _load(STATE_FILES["FLAT_BLUE_LINE_MONTHLY"])
    if fbl_state:
        for instrument in ("NIFTY", "BANKNIFTY"):
            inst_state = fbl_state.get(instrument) if isinstance(fbl_state, dict) else None
            if not inst_state or not inst_state.get("legs"):
                continue
            legs     = inst_state.get("legs", {})
            entry_t  = (inst_state.get("entry_time") or "")[:16].replace("T", " ")
            closed   = inst_state.get("closed", False)
            exit_rsn = inst_state.get("exit_reason")

            for leg_key in ("atm_c", "atm_p", "sc", "sp", "hc", "hp"):
                leg = legs.get(leg_key, {})
                if not leg:
                    continue
                sym    = leg.get("symbol", "")
                entry  = float(leg.get("entry_prem", 0))
                qty    = int(leg.get("qty", 0))
                action = leg.get("action", "BUY")
                opt    = "CE" if leg_key.endswith("_c") else "PE"
                side   = f"{action} {opt}"
                ltp    = ltps.get(sym, entry)
                label  = f"FBL {instrument} ({leg_key})"

                if action == "SELL":
                    pnl = (entry - ltp) * qty
                else:
                    pnl = (ltp - entry) * qty

                if closed:
                    # Only add to today's closed panel if the exit happened today.
                    # FBL is NRML monthly — the state file persists across days, so a
                    # position closed last week would otherwise reappear in "Closed Today"
                    # every morning until the bot is next restarted.
                    exit_time_str = inst_state.get("exit_time", "") or ""
                    exited_today  = exit_time_str[:10] == datetime.now().date().isoformat()
                    if exited_today:
                        _add_closed(label, sym, side, entry, ltp, qty, pnl, exit_rsn or "exited")
                    # else: closed on a previous day — skip from today's portfolio view
                else:
                    _add_open(label, sym, side, entry, ltp, 0.0, 0.0, qty, pnl, entry_t)

    # ── Gap Fade Pre-Open (09:15–10:00) ───────────────────────────────────────
    state = _load(STATE_FILES["GAP_FADE"])
    if state:
        for sym, p in state.get("positions", {}).items():
            entry     = float(p.get("entry_price") or 0)
            qty       = int(p.get("quantity") or 0)
            side      = p.get("direction", "SHORT")
            sl        = float(p.get("sl_price") or 0)
            exit_p    = p.get("exit_price")
            net_pnl   = p.get("net_pnl")
            state_ltp = float(p.get("current_price") or entry)
            if exit_p:
                ep     = float(exit_p)
                real   = float(net_pnl) if net_pnl is not None else (
                    (entry - ep) * qty if side == "SHORT" else (ep - entry) * qty
                )
                reason = "🔴 SL hit" if p.get("sl_hit") else "⏰ time exit"
                _add_closed("Gap Fade", sym, side, entry, ep, qty, real, reason)
            else:
                ltp = ltps.get(sym, state_ltp)
                pnl = (entry - ltp) * qty if side == "SHORT" else (ltp - entry) * qty
                _add_open("Gap Fade", sym, side, entry, ltp, sl, 0.0, qty, pnl, "09:15 entry")

    # Gap Fade EOD — RETIRED 2026-06-04

    # ── NIFTY MA Cross Seller (NRML overnight, 1 leg: ATM CE or PE) ───────────
    ma_cross_state = _load(STATE_FILES.get("NIFTY_MA_CROSS_SELLER"))
    if ma_cross_state:
        t = ma_cross_state.get("active_trade")
        if t and isinstance(t, dict):
            sym      = t.get("symbol", "")
            opt_type = t.get("opt_type", "CE")
            entry    = float(t.get("entry_prem", 0))
            sl       = float(t.get("sl_prem", 0))
            qty      = int(t.get("qty", 0))
            ltp      = ltps.get(sym, entry)
            pnl      = (entry - ltp) * qty
            since    = t.get("entry_ts", "")[:16].replace("T", " ")
            _add_open(
                f"MA Cross ({opt_type})", sym, f"SELL {opt_type}",
                entry, ltp, sl, 0.0, qty, pnl, since,
            )

    # ── Equity OBI Bot — RETIRED 2026-06-19 ───────────────────────────────────

    # ── Untracked positions (positionbook entries with no matching state file) ──
    # Appears when: position placed manually from OpenAlgo UI, bot crashed before
    # writing state, or a bot not yet integrated into the dashboard.
    if pb_available:
        for sym, pb in positionbook.items():
            if sym in state_symbols:
                continue  # already accounted for above
            qty = pb["quantity"]
            ltp = pb["ltp"]
            pnl = pb["pnl"]
            avg = pb["average_price"]
            if qty != 0:
                # Still open — show as an untracked open position.
                # Derive side from net qty sign (positive = long/buy, negative = short/sell)
                open_side = "SELL" if qty < 0 else "BUY"
                # Entry price: use sell_avg if short, buy_avg if long, fall back to avg
                buy_avg_open  = pb.get("buy_avg", 0.0)
                sell_avg_open = pb.get("sell_avg", 0.0)
                if open_side == "SELL" and sell_avg_open:
                    entry_open = sell_avg_open
                elif open_side == "BUY" and buy_avg_open:
                    entry_open = buy_avg_open
                else:
                    entry_open = avg
                open_rows.append({
                    "Bot": "📊 Broker", "Symbol": sym, "Side": open_side,
                    "Entry ₹": entry_open, "LTP ₹": ltp,
                    "SL ₹": 0.0, "TGT ₹": 0.0,
                    "Qty": abs(qty), "MTM ₹": pnl, "Since": "—",
                })
            elif pnl != 0:
                # Flat with non-zero realized P&L — show as closed broker position.
                # Derive Side, Entry ₹, and Qty from the gross buy/sell breakdown.
                buy_qty   = pb.get("buy_qty", 0)
                sell_qty  = pb.get("sell_qty", 0)
                buy_avg   = pb.get("buy_avg", 0.0)
                sell_avg  = pb.get("sell_avg", 0.0)

                # Side: whichever leg came first is the "entry" side.
                # All option bots are short (sell to open → buy to close), so
                # sell_qty is the opening leg.  For long equity bots it's the
                # opposite.  A simple heuristic: if both legs are equal, prefer
                # SELL for options (CE/PE suffix) and BUY for everything else.
                is_option = sym.upper().endswith(("CE", "PE"))
                if sell_qty > buy_qty:
                    side    = "SELL"
                    entry_px = sell_avg
                    traded_qty = sell_qty
                elif buy_qty > sell_qty:
                    side    = "BUY"
                    entry_px = buy_avg
                    traded_qty = buy_qty
                else:
                    # Equal (balanced round-trip): use symbol type as hint
                    if is_option:
                        side, entry_px, traded_qty = "SELL", sell_avg, sell_qty
                    else:
                        side, entry_px, traded_qty = "BUY", buy_avg, buy_qty

                # Fall back to broker avg/ltp if buy/sell breakdown unavailable
                if traded_qty == 0:
                    side, entry_px, traded_qty = "?", avg, 0

                closed_rows.append({
                    "Bot": "📊 Broker", "Symbol": sym, "Side": side,
                    "Entry ₹": entry_px, "Exit ₹": ltp,
                    "Qty": traded_qty, "Net P&L ₹": pnl, "Reason": "broker record",
                })

    # ── Nothing at all ─────────────────────────────────────────────────────────
    if not open_rows and not closed_rows:
        st.info("No positions today across any bot.")
        return

    # ── Compute totals for summary cards ──────────────────────────────────────
    live_mtm   = sum(r["MTM ₹"]     for r in open_rows)
    realized   = sum(r["Net P&L ₹"] for r in closed_rows)
    grand_total = live_mtm + realized

    wins_closed = sum(1 for r in closed_rows if r["Net P&L ₹"] > 0)
    wr_str = (f"{wins_closed}/{len(closed_rows)} wins"
              if closed_rows else "—")

    # ── Summary metric cards ───────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🟢 Open Positions",  len(open_rows))
    c2.metric("📁 Closed Today",    len(closed_rows), help=wr_str)
    c3.metric("Live MTM",
              f"{'+'if live_mtm>=0 else ''}₹{live_mtm:,.0f}",
              delta=None)
    c4.metric("Realized P&L",
              f"{'+'if realized>=0 else ''}₹{realized:,.0f}",
              delta=None)

    st.markdown("---")

    # ── Shared stylers ─────────────────────────────────────────────────────────
    def _color_pnl(v):
        return ("color:#00e599;font-weight:700" if v > 0
                else "color:#f87171;font-weight:700" if v < 0 else "color:#5a7ba0")

    def _fmt_pnl(v):
        return f"{'+'if v>0 else ''}₹{v:,.0f}"

    def _fmt_px(v):
        return f"₹{v:.2f}" if v else "—"

    def _fmt_tgt(v):
        return f"₹{v:.2f}" if v and v > 0 else "—"

    def _subtotal_html(label: str, value: float) -> str:
        color = "#00e599" if value >= 0 else "#f87171"
        sign  = "+" if value >= 0 else ""
        return (
            f'<div style="text-align:right;padding:4px 8px;margin-top:2px;'
            f'font-size:0.88em;color:{color};font-weight:700;letter-spacing:0.02em;">'
            f'<span style="color:#3f5a80;font-weight:400">{label}:</span>'
            f'&nbsp;&nbsp;{sign}₹{value:,.0f}</div>'
        )

    # ── Open positions (grouped by parent strategy, with per-strategy drill-down) ─
    if open_rows:
        leg_groups   = _group_open_rows(open_rows)
        grouped_open = [_summarize_strategy(name, legs) for name, legs in leg_groups.items()]
        st.markdown(
            f"#### 🟢 Open Positions &nbsp; "
            f"<span style='font-size:0.8em;color:#3f5a80'>"
            f"({len(grouped_open)} {'strategy' if len(grouped_open)==1 else 'strategies'} · "
            f"{len(open_rows)} {'leg' if len(open_rows)==1 else 'legs'})</span>",
            unsafe_allow_html=True)
        df_grouped = pd.DataFrame(grouped_open)
        styled_grouped = (
            df_grouped.style
            .map(_color_pnl, subset=["Combined MTM ₹"])
            .format({"Combined MTM ₹": _fmt_pnl})
        )
        st.dataframe(styled_grouped, width='stretch', hide_index=True)
        st.markdown(_subtotal_html("Live MTM subtotal", live_mtm), unsafe_allow_html=True)

        # One expander per multi-leg strategy — click to reveal just its own legs.
        # Single-leg "strategies" already show their full info in the summary row
        # above, so they're skipped here to avoid a wall of one-row expanders.
        leg_cols = ["Bot", "Symbol", "Side", "Entry ₹", "LTP ₹", "SL ₹", "TGT ₹", "Qty", "MTM ₹", "Since"]
        for name, legs in leg_groups.items():
            if len(legs) <= 1:
                continue
            combined = sum(r["MTM ₹"] for r in legs)
            sign = "+" if combined >= 0 else ""
            with st.expander(f"{name} — {len(legs)} legs — {sign}₹{combined:,.0f}"):
                df_leg = pd.DataFrame(legs)[leg_cols]
                styled_leg = (
                    df_leg.style
                    .map(_color_pnl, subset=["MTM ₹"])
                    .format({
                        "Entry ₹": _fmt_px,
                        "LTP ₹":   _fmt_px,
                        "SL ₹":    _fmt_px,
                        "TGT ₹":   _fmt_tgt,
                        "MTM ₹":   _fmt_pnl,
                    })
                )
                st.dataframe(styled_leg, width='stretch', hide_index=True)
    else:
        st.markdown("#### 🟢 Open Positions")
        st.caption("No live positions right now.")

    st.markdown("---")

    # ── Closed positions ───────────────────────────────────────────────────────
    if closed_rows:
        n_wins  = sum(1 for r in closed_rows if r["Net P&L ₹"] > 0)
        n_total = len(closed_rows)
        wr_pct  = n_wins / n_total * 100
        st.markdown(
            f"#### 📁 Closed Positions &nbsp; "
            f"<span style='font-size:0.8em;color:#3f5a80'>({n_total} trades · "
            f"{n_wins}W / {n_total-n_wins}L · WR {wr_pct:.0f}%)</span>",
            unsafe_allow_html=True,
        )
        df_closed = pd.DataFrame(closed_rows)
        styled_closed = (
            df_closed.style
            .map(_color_pnl, subset=["Net P&L ₹"])
            .format({
                "Entry ₹":  _fmt_px,
                "Exit ₹":   _fmt_px,
                "Net P&L ₹": _fmt_pnl,
            })
        )
        st.dataframe(styled_closed, width='stretch', hide_index=True)
        st.markdown(_subtotal_html("Realized subtotal", realized), unsafe_allow_html=True)
    else:
        st.markdown("#### 📁 Closed Positions")
        st.caption("No closed positions yet today.")

    # ── Grand total ────────────────────────────────────────────────────────────
    gt_color = "#00e599" if grand_total >= 0 else "#f87171"
    gt_bg    = "#031a0c"  if grand_total >= 0 else "#1a0505"
    gt_sign  = "+" if grand_total >= 0 else ""
    st.markdown(
        f'<div style="margin-top:16px;padding:12px 18px;border-radius:8px;'
        f'background:{gt_bg};border:1px solid {gt_color};border-left:3px solid {gt_color};">'
        f'<span style="font-size:0.82em;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:0.07em;color:#3f5a80;">Total Gross MTM (all bots)</span>'
        f'&nbsp;&nbsp;&nbsp;'
        f'<span style="font-size:1.35em;font-weight:700;color:{gt_color};">'
        f'{gt_sign}₹{grand_total:,.0f}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — NIFTY BB OVERBOUGHT BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_bb_overbought_panel(ltps: dict):
    state = _load(STATE_FILES.get("NIFTY_BB_OB"))

    with st.container(border=True):
        st.subheader("📊 Nifty BB Overbought Bot")
        
        with st.expander("📖 Strategy & Research Details"):
            st.markdown(
                '<div class="research-badge">'
                'Research: BB(30,3σ) on 5-min NIFTY bars — OVERBOUGHT → Sell ATM PE  |  '
                'IS Sharpe +3.63  |  OOS +10.65  |  MC 100% stable  |  NIFTY-only (SENSEX/BANKNIFTY fail Stage 9)'
                '</div>',
                unsafe_allow_html=True,
            )
            
            sch1, sch2 = st.columns(2)
            with sch1:
                st.markdown("**Entry Conditions**")
                st.markdown(
                    '<div class="condition-row">'
                    '🔍 BB(30,3σ) 5-min upper band touch/cross<br>'
                    '✅ Overbought confluence (Nifty level > Upper BB)<br>'
                    '✅ RSI > 70 preferred (momentum confirmation)<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with sch2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🛑 Stop-loss: 1.5x entry premium<br>'
                    '🎯 Target: Mean reversion to middle BB SMA<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )

        if not state:
            st.error("Bot not running — state file absent. Check `live_trading/logs/nifty_bb_overbought_state.json`.")
            return

        nifty      = state.get("nifty_ltp", 0)
        vix        = state.get("vix_ltp", 0)
        daily_adx  = state.get("daily_adx")   # may be None
        adx_ok     = (daily_adx is not None) and (daily_adx < 25.0)
        expiry     = state.get("expiry", "—")
        is_exp_day = state.get("is_expiry_day", False)
        lot_size   = state.get("lot_size", "—")
        n_lots     = state.get("n_lots", 1)
        entry_win  = state.get("entry_window", "09:15–10:30")
        bb_params  = state.get("bb_params", "BB(30, 3σ)")
        bars       = state.get("bars_loaded", 0)
        bb         = state.get("bb_snapshot", {})
        active     = state.get("active_trade")

        # ── Row 1: key metrics ─────────────────────────────────────────────────
        _in_win_bb = _entry_window_open(entry_win)
        c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
        c1.metric("NIFTY", f"{nifty:,.2f}")
        c2.metric(
            "VIX", f"{vix:.2f}",
            delta="—",
            help="VIX is tracked but not used as a filter in this bot"
        )
        if daily_adx is not None:
            c3.metric(
                "Daily ADX-14",
                f"{daily_adx:.1f}",
                delta="✅ < 25 — MR regime" if adx_ok else "❌ ≥ 25 — trending",
                delta_color="normal" if adx_ok else "inverse",
                help="Key filter: ADX ≥ 25 → trending day → entries blocked",
            )
        else:
            c3.metric("Daily ADX-14", "Fetching…", help="Loaded once at session open")

        c4.metric(
            "Expiry",
            expiry,
            delta="⚡ Expiry Day" if is_exp_day else "",
            delta_color="inverse" if is_exp_day else "normal",
            help="Min premium ₹200 on expiry day (vs ₹150 normal)",
        )
        c5.metric("Lot Size", lot_size, help="Fetched dynamically from token DB")
        c6.metric("Lots", n_lots)
        c7.metric(f"{bb_params} Bars", bars, help="5-min bars loaded (need ≥35)")
        c8.metric("Window", entry_win,
                  delta="🟢 OPEN" if _in_win_bb else "🔴 CLOSED",
                  delta_color="off",
                  help="IST — signals only checked inside this window")

        # ── BB Snapshot ────────────────────────────────────────────────────────
        if bb:
            st.markdown("---")
            close = bb.get("close", 0)
            sma   = bb.get("sma", 0)
            upper = bb.get("upper", 0)
            lower = bb.get("lower", 0)
            std   = bb.get("std", 0)
            dist  = bb.get("distance_pct", 0)   # % above upper (positive = overbought)

            b1, b2, b3, b4, b5 = st.columns(5)
            b1.metric("NIFTY Close (5m)", f"{close:,.1f}")
            b2.metric("BB SMA", f"{sma:,.1f}")
            b3.metric(
                "BB Upper (3σ)", f"{upper:,.1f}",
                delta=f"{close - upper:+.1f} pts",
                delta_color="inverse" if close > upper else "normal",
            )
            b4.metric("BB Lower (3σ)", f"{lower:,.1f}")
            b5.metric(
                "Std Dev", f"{std:.1f}",
                help="1σ of 5-min closes over the last 30 bars"
            )

            # Visual progress bar: where is close relative to lower…upper
            prog, zone_label = _bb_progress(close, lower, upper)
            st.caption(f"**Band Position:** {zone_label}  |  Distance from upper: {dist:+.3f}%")
            st.progress(prog)

            # Signal banner
            is_overbought = close > upper
            in_window     = _entry_window_open(entry_win)
            min_prem      = 200.0 if is_exp_day else 150.0

            if active:
                st.markdown(
                    '<div class="signal-banner-on">'
                    '🟢 POSITION ACTIVE — ATM PE Sold | Monitoring E4 target & SL'
                    '</div>',
                    unsafe_allow_html=True,
                )
            elif is_overbought and adx_ok:
                st.markdown(
                    '<div class="signal-banner-wait">'
                    f'🟡 OVERBOUGHT SIGNAL LIVE — waiting for ATM PE ≥ ₹{min_prem:.0f} '
                    f'{"and entry window" if not in_window else "| In window ✅"}'
                    '</div>',
                    unsafe_allow_html=True,
                )
            elif is_overbought and not adx_ok:
                st.markdown(
                    '<div class="signal-banner-off">'
                    '🔴 OVERBOUGHT but ADX filter BLOCKED — trending day (daily ADX ≥ 25), no entry'
                    '</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div class="signal-banner-off">'
                    '⚪ Normal — NIFTY within BB bands. Monitoring for OVERBOUGHT breach.'
                    '</div>',
                    unsafe_allow_html=True,
                )

        # ── Conditions checklist ───────────────────────────────────────────────
        st.markdown("---")
        ch1, ch2 = st.columns(2)

        with ch1:
            st.markdown("**Entry Conditions**")
            if bb:
                is_ob    = bb.get("close", 0) > bb.get("upper", float("inf"))
                in_win   = _entry_window_open(entry_win)
                min_p    = 200.0 if is_exp_day else 150.0
                adx_disp = f"{daily_adx:.1f}" if daily_adx is not None else "?"
                exp_note = "(expiry day)" if is_exp_day else "(normal day)"
                st.markdown(
                    f'<div class="condition-row">'
                    f'{_tick(is_ob)} Close ({bb.get("close",0):,.1f}) > BB Upper ({bb.get("upper",0):,.1f})<br>'
                    f'{_tick(adx_ok)} Daily ADX-14 ({adx_disp}) &lt; 25 (mean-reversion regime)<br>'
                    f'{_tick(in_win)} Entry window open ({entry_win})<br>'
                    f'{_tick(not active)} No existing position (one trade per session)<br>'
                    f'Min premium: ₹{min_p:.0f} {exp_note}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.info("Warming up — no BB snapshot yet.")

        with ch2:
            st.markdown("**Exit Rules**")
            st.markdown(
                '<div class="condition-row">'
                '🎯 <b>E4 Profit target</b>: premium drops −30% from entry<br>'
                '🛑 <b>Safety SL</b>: premium rises to 2× entry (never triggered in 60 IS+OOS trades at ≥₹150)<br>'
                '⏰ <b>EOD exit</b>: 15:15 IST unconditional close<br>'
                '📦 <b>Sizing</b>: 1 lot flat (scale up after 15+ live trades observed)'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── Active trade ───────────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("**Active Position**")
        if not active:
            st.success("No open position — waiting for OVERBOUGHT signal in window.")
        else:
            sym   = active.get("symbol", "")
            entry = float(active.get("entry_prem", 0))
            e4    = float(active.get("e4_target", entry * 0.7))
            sl    = float(active.get("sl_prem", entry * 2))
            qty   = int(active.get("qty", 0))
            since = active.get("entry_time", "")[:19].replace("T", " ")
            ltp   = ltps.get(sym, entry)
            pnl   = (entry - ltp) * qty

            t1, t2, t3, t4, t5, t6 = st.columns(6)
            t1.metric("Symbol", sym)
            t2.metric("Entry ₹", f"{entry:.2f}")
            t3.metric("E4 Target ₹", f"{e4:.2f}", help="−30% from entry premium")
            t4.metric("SL ₹", f"{sl:.2f}", help="2× entry premium")
            t5.metric("LTP ₹", f"{ltp:.2f}", delta=f"{ltp - entry:+.2f}")
            pnl_color = "normal" if pnl > 0 else "inverse"
            t6.metric("MTM", f"{'+'if pnl>0 else ''}₹{pnl:,.0f}",
                      delta_color=pnl_color, help=f"Qty: {qty}  |  Entered: {since}")

        if not active:
            _render_today_trades_detail(_load_today_trades("nifty_bb_overbought_bot"))


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — NIFTY TREND SELLER BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_nts_panel(ltps: dict):
    state = _load(STATE_FILES["NIFTY_TS"])

    with st.container(border=True):
        st.subheader("📉 Nifty Trend Seller Bot")
        st.markdown(
            '<div class="research-badge">'
            'Research: ADX(14)+RSI(14)+MACD(5,13,3) confluence on 1-min NIFTY bars  |  '
            'OOS Sharpe +1.735 (short-only)  |  MC 99.2% stable  |  10/10 stages  |  '
            'Live bot: combined legs, ADX>30, RSI>55/<45, 5-bar ADX-D  |  '
            'Research-optimal: SHORT-ONLY, ADX>25, RSI<50, 7-bar ADX-D (pending update)'
            '</div>',
            unsafe_allow_html=True,
        )

        if not state:
            st.error("Bot not running — state file absent.")
            return

        nifty   = state.get("nifty_ltp", 0)
        vix     = state.get("vix_ltp", 0)
        expiry  = state.get("expiry", "—")
        lot_sz  = state.get("lot_size", "—")
        n_lots  = state.get("n_lots", 10)
        window  = state.get("entry_window", "10:00–13:00")
        bars    = state.get("bars_loaded", 0)
        indic   = state.get("indicators", {})
        active  = state.get("active_trades", {})

        # ── Row 1: metrics ─────────────────────────────────────────────────────
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("NIFTY", f"{nifty:,.2f}")
        c2.metric(
            "VIX", f"{vix:.2f}",
            delta="OK ≤22" if vix <= 22 else f"⚠ {vix:.1f} > 22",
            delta_color="normal" if vix <= 22 else "inverse",
        )
        c3.metric("Expiry", expiry)
        c4.metric("Lot Size", lot_sz)
        c5.metric("Lots", n_lots, help="Per leg (CE or PE)")
        c6.metric("Bars Loaded", bars, help="1-min NIFTY bars in rolling window")

        # ── Row 2: live indicator values ───────────────────────────────────────
        if indic:
            st.markdown("---")
            adx, adx_old = indic.get("adx", 0), indic.get("adx_old", 0)
            rsi, ema, close = indic.get("rsi", 0), indic.get("ema", 0), indic.get("close", 0)
            conds = indic.get("conditions", {})
            in_win = _entry_window_open(window)

            i1, i2, i3, i4 = st.columns(4)
            i1.metric("ADX(14)", f"{adx:.1f}", delta=f"{adx - adx_old:+.2f}")
            i2.metric("RSI(14)", f"{rsi:.1f}", delta=("Bull" if rsi > 55 else ("Bear" if rsi < 45 else "Neut")), delta_color="normal" if rsi > 55 else "inverse")
            i3.metric("EMA(20)", f"{ema:.1f}", delta=f"{close - ema:+.1f} (close)")
            i4.metric("Window", window,
                      delta="🟢 OPEN" if in_win else "🔴 CLOSED",
                      delta_color="off",
                      help="IST — signals only accepted inside this window")

            with st.expander("📖 Indicator & Entry Checklist Details"):
                st.markdown(
                    '<div class="condition-row">'
                    '🔍 Live Params: ADX > 30 | RSI > 55 or < 45 | MACD Cross | VIX ≤ 22'
                    '</div>',
                    unsafe_allow_html=True,
                )
                ch1, ch2 = st.columns(2)
                with ch1:
                    st.markdown("**Entry Checklist**")
                    st.markdown(f'<div class="condition-row">Short: {conds.get("ema_ok_short")} EMA | {conds.get("adx_strong")} ADX | {conds.get("rsi_bear")} RSI | {conds.get("macd_cross_dn")} MACD</div>', unsafe_allow_html=True)
                with ch2:
                    st.markdown("**Exit Rules**")
                    st.markdown('<div class="condition-row">🛑 SL: 2x entry premium | 🎯 Target: Uncapped</div>', unsafe_allow_html=True)
        else:
            st.info("Indicator snapshot warming up...")

        # ── Active trades ──────────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("**Active Positions**")

        open_legs = {k: v for k, v in active.items() if v is not None}
        if not open_legs:
            st.success("No open positions.")
        else:
            rows = []
            for leg, t in open_legs.items():
                sym   = t.get("symbol", "")
                entry = float(t.get("entry_prem", 0))
                sl    = float(t.get("sl_prem", 0))
                qty   = int(t.get("qty", 0))
                ltp   = ltps.get(sym, entry)
                pnl   = (entry - ltp) * qty
                since = t.get("entry_time", "")[:19].replace("T", " ")
                rows.append({
                    "Leg": leg, "Symbol": sym,
                    "Entry ₹": f"{entry:.2f}", "LTP ₹": f"{ltp:.2f}",
                    "SL ₹": f"{sl:.2f}", "Qty": qty,
                    "MTM": f"{'+'if pnl>0 else ''}₹{pnl:,.0f}",
                    "Entered": since,
                })
            df = pd.DataFrame(rows)

            def _style(v):
                try:
                    num = float(v.replace("₹", "").replace(",", "").replace("+", ""))
                    return "color:#00e599;font-weight:700" if num > 0 else ("color:#f87171;font-weight:700" if num < 0 else "color:#5a7ba0")
                except Exception:
                    return ""

            st.dataframe(
                df.style.map(_style, subset=["MTM"]),
                width="stretch", hide_index=True,
            )

        _render_today_trades_detail(_load_today_trades("nifty_trend_seller_bot"))


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 5 — SENSEX TREND SELLER BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_sensex_ts_panel(ltps: dict):
    state = _load(STATE_FILES["SENSEX_TS"])

    with st.container(border=True):
        st.subheader("📉 SENSEX Trend Seller Bot")
        st.markdown(
            '<div class="research-badge">'
            'Research: Stage 9 multi-instrument (2026-03-21)  |  SHORT-ONLY — sell CE on bearish SENSEX signal  |  '
            'OOS Sharpe +2.181  |  WR 73.5%  |  ADX>25, RSI<50, 7-bar ADX-D  |  BFO (BSE F&O)  |  '
            'BANKNIFTY excluded — destructive (OOS −1.225, WR 42.9%)'
            '</div>',
            unsafe_allow_html=True,
        )

        if not state:
            st.error("Bot not running — state file absent.")
            return

        sensex  = state.get("sensex_ltp", 0)
        vix     = state.get("vix_ltp", 0)
        expiry  = state.get("expiry") or "—"
        lot_sz  = state.get("lot_size", "—")
        n_lots  = state.get("n_lots", 10)
        window  = state.get("entry_window", "10:00–13:00")
        bars    = state.get("bars_loaded", 0)
        indic   = state.get("indicators", {})
        active  = state.get("active_trades", {})
        ce_pos  = active.get("CE")

        # ── Row 1: metrics ─────────────────────────────────────────────────────
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("SENSEX", f"{sensex:,.2f}")
        c2.metric(
            "VIX", f"{vix:.2f}",
            delta="OK ≤22" if vix <= 22 else f"⚠ {vix:.1f} > 22",
            delta_color="normal" if vix <= 22 else "inverse",
        )
        c3.metric("Expiry", expiry, help="BSE weekly — Thursday from Sep 2025")
        c4.metric("Lot Size", lot_sz, help="BSE lot (currently 20 per lot)")
        c5.metric("Lots", n_lots)
        c6.metric("Bars Loaded", bars, help="1-min SENSEX bars in rolling window")

        # ── Row 2: live indicators ─────────────────────────────────────────────
        if indic:
            st.markdown("---")
            st.markdown("**Live Indicator Snapshot** *(updated on each 1-min bar close)*")

            adx      = indic.get("adx", 0)
            adx_old  = indic.get("adx_old", 0)
            rsi      = indic.get("rsi", 0)
            ema      = indic.get("ema", 0)
            close    = indic.get("close", 0)
            macd_l   = indic.get("macd_line", 0)
            macd_s   = indic.get("macd_signal", 0)
            conds    = indic.get("conditions", {})
            macd_diff = macd_l - macd_s
            in_win   = _entry_window_open(window)

            i1, i2, i3, i4, i5 = st.columns(5)
            i1.metric(
                "ADX(14)", f"{adx:.1f}",
                delta=f"{adx - adx_old:+.2f} vs 7b ago",
                delta_color="normal" if adx > adx_old else "inverse",
                help="Research-optimal threshold: >25 (vs NIFTY live-bot's >30)",
            )
            i2.metric(
                "RSI(14)", f"{rsi:.1f}",
                delta="bearish" if rsi < 50 else "neutral",
                delta_color="inverse" if rsi < 50 else "off",
                help="Short threshold: RSI < 50 (research-optimal)",
            )
            i3.metric(
                "EMA(20)", f"{ema:.1f}",
                delta=f"{close - ema:+.1f} (close)",
                delta_color="normal" if close > ema else "inverse",
            )
            i4.metric(
                "MACD(5,13,3)", f"{macd_l:.3f}",
                delta=f"vs signal: {macd_diff:+.3f}",
                delta_color="normal" if macd_diff > 0 else "inverse",
                help=f"Signal line: {macd_s:.3f}",
            )
            i5.metric(
                "Entry Window",
                window,
                delta="🟢 OPEN" if in_win else "🔴 CLOSED",
                delta_color="off",
                help="IST — signals only accepted inside this window",
            )

            # ── Conditions checklist (5 conditions, SHORT-ONLY) ────────────────
            st.markdown("---")
            st.markdown("**SHORT Entry Conditions** *(all 5 required → Sell ATM CE)*")

            c_bear = conds.get("ema_bearish", False)
            c_adx  = conds.get("adx_strong",  False)
            c_rise = conds.get("adx_rising",  False)
            c_rsi  = conds.get("rsi_bearish", False)
            c_macd = conds.get("macd_cross_down", False)
            met    = sum([c_bear, c_adx, c_rise, c_rsi, c_macd])

        # ── Row 2: live indicators ─────────────────────────────────────────────
        if indic:
            st.markdown("---")
            adx, adx_old = indic.get("adx", 0), indic.get("adx_old", 0)
            rsi, ema, close = indic.get("rsi", 0), indic.get("ema", 0), indic.get("close", 0)
            conds = indic.get("conditions", {})
            in_win = _entry_window_open(window)

            i1, i2, i3, i4 = st.columns(4)
            i1.metric("ADX(14)", f"{adx:.1f}", delta=f"{adx - adx_old:+.2f}")
            i2.metric("RSI(14)", f"{rsi:.1f}", delta=("Bearish" if rsi < 50 else "Neutral"), delta_color="inverse" if rsi < 50 else "off")
            i3.metric("EMA(20)", f"{ema:.1f}", delta=f"{close - ema:+.1f} (close)")
            i4.metric("Window", window,
                      delta="🟢 OPEN" if in_win else "🔴 CLOSED",
                      delta_color="off",
                      help="IST — signals only accepted inside this window")

            with st.expander("📖 Indicator & Entry Checklist Details"):
                st.markdown(f'<div class="condition-row">Short Conditions (ADX>25, RSI<50): {conds.get("adx_strong")} ADX | {conds.get("rsi_bearish")} RSI | {conds.get("macd_cross_down")} MACD</div>', unsafe_allow_html=True)
                st.markdown('<div class="condition-row">🛑 SL: 2x entry premium | 🎯 Target: Uncapped</div>', unsafe_allow_html=True)
        else:
            st.info("Indicator snapshot warming up...")

        # ── Active CE position ─────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("**Active CE Position**")
        if not ce_pos:
            st.success("No open CE position — waiting for bearish confluence signal.")
        else:
            sym   = ce_pos.get("symbol", "")
            entry = float(ce_pos.get("entry_prem", 0))
            sl    = float(ce_pos.get("sl_prem", 0))
            qty   = int(ce_pos.get("qty", 0))
            since = ce_pos.get("entry_time", "")[:19].replace("T", " ")
            ltp   = ltps.get(sym, entry)
            pnl   = (entry - ltp) * qty

            t1, t2, t3, t4, t5 = st.columns(5)
            t1.metric("Symbol", sym)
            t2.metric("Entry ₹", f"{entry:.2f}")
            t3.metric("SL ₹", f"{sl:.2f}", help="2× entry premium")
            t4.metric("LTP ₹", f"{ltp:.2f}", delta=f"{ltp - entry:+.2f}")
            t5.metric(
                "MTM", f"{'+'if pnl>0 else ''}₹{pnl:,.0f}",
                delta_color="normal" if pnl > 0 else "inverse",
                help=f"Qty: {qty}  |  Entered: {since}",
            )

        _render_today_trades_detail(_load_today_trades("sensex_trend_seller_bot"))


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 6 — PRE-OPEN GAP FADE BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_gap_fade_panel(ltps: dict):
    state = _load(STATE_FILES["GAP_FADE"])

    with st.container(border=True):
        st.subheader("📈 Pre-Open Gap Fade Bot")
        st.markdown(
            '<div class="research-badge">'
            'Research: Nifty 50 stocks gapping ≥2% at open → fade with market SHORT  |  '
            'SL 0.5%  |  Exit 10:00 AM  |  IS Sharpe +3.86  |  OOS Sharpe +7.26  |  9/10 stages pass'
            '</div>',
            unsafe_allow_html=True,
        )

        if not state:
            st.error("Bot not running — state file absent.")
            return

        trade_date = state.get("trade_date", "—")
        n_signals  = state.get("n_signals", 0)
        positions  = state.get("positions", {})
        _, age_label = _staleness(state.get("last_update", ""))

        # Summary row
        _gf_in_win = _entry_window_open("09:15–10:00")
        s1, s2, s3, s4, s5, s6 = st.columns(6)
        s1.metric("Trade Date", trade_date)
        s2.metric("Gap Signals", n_signals)
        s3.metric("Open Positions", sum(1 for p in positions.values() if not p.get("exit_price")))
        s4.metric("State Age", age_label.split(" ", 1)[-1] if " " in age_label else age_label)
        s5.metric("Entry", "09:15 IST", help="Market open — first tick")
        s6.metric("Exit", "10:00 IST",
                  delta="🟢 ACTIVE" if _gf_in_win else "🔴 CLOSED",
                  delta_color="off",
                  help="Hard time stop — all positions closed unconditionally")

        with st.expander("📖 Strategy & Research Details"):
            st.markdown(
                '<div class="research-badge">'
                'Nifty 50 stocks gapping ≥2% at open → fade with market SHORT  |  '
                'SL 0.5%  |  Exit 10:00 AM  |  IS Sharpe +3.86  |  OOS Sharpe +7.26'
                '</div>',
                unsafe_allow_html=True,
            )
            gf_ch1, gf_ch2 = st.columns(2)
            with gf_ch1:
                st.markdown("**Entry Conditions**")
                st.markdown(
                    '<div class="condition-row">'
                    '📌 Nifty50 stock gaps ≥2% at pre-open auction vs previous close<br>'
                    '📉 Fade direction: SHORT (sell the gap-up, buy the gap-down)<br>'
                    '⏰ Entry: 09:15 IST at market open (first tick)<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with gf_ch2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🛑 <b>Hard SL</b>: 0.5% from entry price<br>'
                    '⏰ <b>Time Stop</b>: 10:00 AM — all positions closed unconditionally<br>'
                    '🎯 <b>Profit Target</b>: Uncapped (rides the gap fill)<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )

        if not positions:
            st.info("No positions today.")
            return

        rows = []
        for sym, p in positions.items():
            entry    = float(p.get("entry_price") or 0)
            # Live LTP from multiquotes; fall back to bot's cached current_price
            state_ltp = float(p.get("current_price") or entry)
            ltp       = ltps.get(sym, state_ltp)
            qty      = int(p.get("quantity") or 0)
            side     = p.get("direction", "SHORT")
            sl       = float(p.get("sl_price") or 0)
            exit_p   = p.get("exit_price")
            net_pnl  = p.get("net_pnl")
            if exit_p:
                if net_pnl is not None:
                    # post-10:05: on_log_pnl has computed net P&L (cost-adjusted)
                    pnl = float(net_pnl)
                else:
                    # intraday: on_log_pnl hasn't run yet — compute gross realized P&L
                    # from the actual exit price (= sl_price for SL-hits, LTP for time-exit)
                    ep  = float(exit_p)
                    pnl = (entry - ep) * qty if side == "SHORT" else (ep - entry) * qty
                status = "Closed (SL)" if p.get("sl_hit") else "Closed"
            else:
                pnl  = (entry - ltp) * qty if side == "SHORT" else (ltp - entry) * qty
                status = "Open"
            rows.append({
                "Symbol": sym, "Side": side, "Qty": qty,
                "Entry ₹": entry, "SL ₹": sl, "LTP ₹": ltp,
                "MTM ₹": pnl, "Status": status,
            })

        df = pd.DataFrame(rows)
        total = df["MTM ₹"].sum()

        def _color_mtm(v):
            return ("color:#00e599;font-weight:700" if v > 0
                    else "color:#f87171;font-weight:700" if v < 0 else "color:#5a7ba0")

        styled = (
            df.style
            .map(_color_mtm, subset=["MTM ₹"])
            .format({
                "Entry ₹": "₹{:.2f}", "SL ₹": "₹{:.2f}", "LTP ₹": "₹{:.2f}",
                "MTM ₹":  lambda v: f"{'+'if v>0 else ''}₹{v:,.0f}",
            })
        )
        st.dataframe(styled, width="stretch", hide_index=True)

        sign  = "+" if total > 0 else ""
        color = "green" if total > 0 else ("red" if total < 0 else "gray")
        st.markdown(
            f'**Gap Fade Total MTM: '
            f'<span style="color:{color};font-weight:bold">{sign}₹{total:,.0f}</span>**',
            unsafe_allow_html=True,
        )

        _render_today_trades_detail(
            _load_today_trades("preopen_gap_fade_bot"), compact=True
        )


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 6b — GAP FADE EOD BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_gap_fade_eod_panel(ltps: dict):
    """RETIRED 2026-06-04 — Gap Fade EOD Bot. Function kept for historical reference only."""
    st.warning("⚠️ Gap Fade EOD Bot was retired on 2026-06-04.")
    return
    state = _load(STATE_FILES["GAP_FADE_EOD"])  # noqa: unreachable

    with st.container(border=True):
        st.subheader("📈 Gap Fade EOD Bot")
        st.markdown(
            '<div class="research-badge">'
            'Research: Nifty50 stock gaps down 2–5% → LONG at open, hold all day, sell at 15:25  |  '
            'No SL (full-day recovery mechanism)  |  IS Sharpe +2.624  |  OOS Sharpe +2.336  |  '
            'WR 59.7% OOS  |  10/10 stages pass  |  Skip NIFTY expiry days'
            '</div>',
            unsafe_allow_html=True,
        )

        if not state:
            st.error("Bot not running — state file absent. "
                     "Check `live_trading/logs/gap_fade_eod_state.json`.")
            return

        trade_date = state.get("trade_date", "—")
        n_signals  = state.get("n_signals", 0)
        positions  = state.get("positions", {})
        _, age_label = _staleness(state.get("last_update", ""))

        # Summary row
        _gfe_in_win = _entry_window_open("09:15–15:25")
        s1, s2, s3, s4, s5, s6 = st.columns(6)
        s1.metric("Trade Date",    trade_date)
        s2.metric("Gap Signals",   n_signals)
        s3.metric("Open Positions", sum(1 for p in positions.values() if not p.get("exit_price")))
        s4.metric("State Age",     age_label.split(" ", 1)[-1] if " " in age_label else age_label)
        s5.metric("Entry", "09:15 IST", help="Market open — first tick on gap-down signal")
        s6.metric("Exit", "15:25 IST",
                  delta="🟢 ACTIVE" if _gfe_in_win else "🔴 CLOSED",
                  delta_color="off",
                  help="Full-day hold — unconditional close at 15:25")

        with st.expander("📖 Strategy & Research Details"):
            gf_ch1, gf_ch2 = st.columns(2)
            with gf_ch1:
                st.markdown("**Entry Conditions**")
                st.markdown(
                    '<div class="condition-row">'
                    '📌 Nifty50 stock gaps DOWN 2%–5% at pre-open IEP vs previous close<br>'
                    '📈 Direction: LONG only (buy the gap-down, expect intraday recovery)<br>'
                    '⏰ Entry: 09:15 IST market open (first tick)<br>'
                    '⛔ Skip NIFTY expiry days (WR drops 62% → 46% on expiry)<br>'
                    '🚫 Gap cap at 5%: excludes circuit-breaker / extreme illiquid opens<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with gf_ch2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🚫 <b>No Stop-Loss</b> — full-day institutional absorption mechanism<br>'
                    '&nbsp;&nbsp;&nbsp;&nbsp;(tight SL would be stopped by normal intraday noise)<br>'
                    '⏰ <b>Time Exit</b>: 15:25 PM — all positions closed unconditionally<br>'
                    '🎯 <b>Profit Target</b>: Uncapped (rides full intraday gap fill)<br>'
                    '📊 <b>Expected WR</b>: ~59–62% (non-expiry days)<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )

        if not positions:
            st.info("No positions today — either no gap-down signals or expiry day filter triggered.")
            return

        rows = []
        for sym, p in positions.items():
            entry     = float(p.get("entry_price") or 0)
            state_ltp = float(p.get("current_price") or entry)
            ltp       = ltps.get(sym, state_ltp)   # live LTP first, bot-cached fallback
            qty       = int(p.get("quantity") or 0)
            gap_pct   = float(p.get("gap_pct") or 0)
            exit_p    = p.get("exit_price")
            net_pnl   = p.get("net_pnl")

            if exit_p:
                ep  = float(exit_p)
                pnl = float(net_pnl) if net_pnl is not None else (ep - entry) * qty
                display_ltp = ep
                status = "Closed (15:25)"
            else:
                pnl = (ltp - entry) * qty   # long: profit when price rises
                display_ltp = ltp
                status = "Open"

            rows.append({
                "Symbol":   sym,
                "Gap %":    gap_pct,
                "Side":     "LONG",
                "Qty":      qty,
                "Entry ₹":  entry,
                "LTP ₹":    display_ltp,
                "MTM ₹":    pnl,
                "Status":   status,
            })

        df = pd.DataFrame(rows)
        total = df["MTM ₹"].sum()

        def _color_mtm(v):
            return ("color:#00e599;font-weight:700" if v > 0
                    else "color:#f87171;font-weight:700" if v < 0 else "color:#5a7ba0")

        def _color_gap(v):
            return "color:#f87171;font-weight:bold"   # gap-down is always negative/red

        styled = (
            df.style
            .map(_color_mtm, subset=["MTM ₹"])
            .map(_color_gap, subset=["Gap %"])
            .format({
                "Gap %":   "{:+.2f}%",
                "Entry ₹": "₹{:.2f}",
                "LTP ₹":   "₹{:.2f}",
                "MTM ₹":   lambda v: f"{'+'if v>0 else ''}₹{v:,.0f}",
            })
        )
        st.dataframe(styled, width="stretch", hide_index=True)

        sign  = "+" if total > 0 else ""
        color = "green" if total > 0 else ("red" if total < 0 else "gray")
        st.markdown(
            f'**Gap Fade EOD Total MTM: '
            f'<span style="color:{color};font-weight:bold">{sign}₹{total:,.0f}</span>**',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 7 — HTF PO3 BOT
# ══════════════════════════════════════════════════════════════════════════════

_PO3_PHASES = {
    "ACCUM":      ("⏳", "Accumulation",  "Waiting for 60-min bar open + accum window to complete"),
    "WATCH":      ("👀", "Watch",          "Accum done — monitoring for manipulation (price below accum_low)"),
    "MANIP":      ("🎯", "Manipulation",   "Below accum_low — FVG detected, waiting for CISD confirmation"),
    "WAIT_CISD":  ("⚡", "Wait CISD",      "FVG mitigation zone — watching for 1-min close above FVG top"),
    "SIGNAL":     ("🔥", "SIGNAL FIRED",   "CISD confirmed — entry triggered"),
    "TRADED":     ("✅", "Traded",          "Session trade placed — monitoring position"),
}


def _render_po3_instrument(sym_key: str, cfg: dict, instrument: dict, ltps: dict):
    """Render a single instrument's PO3 state inside a bordered column."""
    phase_raw  = instrument.get("phase", "ACCUM")
    phase_icon, phase_name, phase_desc = _PO3_PHASES.get(
        phase_raw, ("❓", phase_raw, "")
    )
    traded     = instrument.get("session_traded", False)
    expiry     = instrument.get("expiry", "—")
    ltp        = instrument.get("ltp", 0)
    accum_high = instrument.get("accum_high")
    accum_low  = instrument.get("accum_low")
    fvg_top    = instrument.get("fvg_top")
    fvg_bot    = instrument.get("fvg_bot")
    active     = instrument.get("active")

    # ── Instrument header ─────────────────────────────────────────────────────
    lot_size = cfg.get("lot_size", "?")
    accum_m  = cfg.get("accum_minutes", "?")
    fvg_min  = cfg.get("fvg_min_size", 20)
    sl_mult  = cfg.get("sl_mult", "?")
    tgt_pct  = cfg.get("target_pct", "?")

    st.markdown(
        f'<div class="research-badge">'
        f'{sym_key} — accum {accum_m}m | FVG ≥{fvg_min}pts | '
        f'SL {sl_mult}× | Tgt {tgt_pct}× | Lot {lot_size} | Expiry {expiry}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Phase banner ─────────────────────────────────────────────────────────
    banner_class = (
        "signal-banner-on"   if phase_raw in ("SIGNAL", "TRADED") else
        "signal-banner-wait" if phase_raw in ("MANIP", "WAIT_CISD") else
        "signal-banner-off"
    )
    st.markdown(
        f'<div class="{banner_class}">'
        f'{phase_icon} <b>{phase_name}</b>  —  {phase_desc}'
        f'{"  |  ⛔ Session trade complete" if traded and phase_raw != "TRADED" else ""}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Key metrics row ───────────────────────────────────────────────────────
    m1, m2, m3, m4 = st.columns(4)
    m1.metric(f"{sym_key} LTP", f"{ltp:,.1f}" if ltp else "—")
    m2.metric(
        "Accum High",
        f"{accum_high:,.1f}" if accum_high else "—",
        help="Top of accumulation range (first accum_minutes of 60-min HTF bar)",
    )
    m3.metric(
        "Accum Low",
        f"{accum_low:,.1f}" if accum_low else "—",
        help="Bottom of accumulation range — price must break below this for manipulation phase",
    )
    if fvg_top:
        m4.metric(
            "FVG Zone",
            f"{fvg_bot:,.1f} – {fvg_top:,.1f}",
            help="Detected bullish FVG (3-bar gap ≥ fvg_min_size). CISD = 1-min close above fvg_top",
        )
    else:
        m4.metric("FVG Zone", "Not yet detected", help="FVG forms just before manipulation dip")

    # ── Active position ───────────────────────────────────────────────────────
    st.markdown("**Active Position**")
    if not active:
        if traded:
            st.info("Session trade already executed — no new entries for the rest of this session.")
            _render_today_trades_detail(
                [t for t in _load_today_trades("htf_po3_bot")
                 if sym_key.upper() in (t.get("symbol") or "").upper()
                    or sym_key.upper() in (t.get("instrument") or "").upper()]
            )
        else:
            st.success(f"No open position — monitoring PO3 phases.")
    else:
        sym    = active.get("symbol", "")
        entry  = float(active.get("entry_prem", 0))
        sl     = float(active.get("sl_prem", entry * 2))
        tgt    = float(active.get("target_prem", entry * 0.7))
        qty    = int(active.get("qty", 0))
        since  = active.get("entry_time", "")[:19].replace("T", " ")
        ltp_op = ltps.get(sym, entry)
        pnl    = (entry - ltp_op) * qty

        p1, p2, p3, p4, p5, p6 = st.columns(6)
        p1.metric("Symbol", sym)
        p2.metric("Entry ₹", f"{entry:.2f}")
        p3.metric(
            "Target ₹", f"{tgt:.2f}",
            help=f"−{(1-float(cfg.get('target_pct',0.7)))*100:.0f}% from entry",
        )
        p4.metric("SL ₹", f"{sl:.2f}", help=f"{cfg.get('sl_mult','?')}× entry premium")
        p5.metric("LTP ₹", f"{ltp_op:.2f}", delta=f"{ltp_op - entry:+.2f}")
        p6.metric(
            "MTM", f"{'+'if pnl>0 else ''}₹{pnl:,.0f}",
            delta_color="normal" if pnl > 0 else "inverse",
            help=f"Qty: {qty}  |  Entered: {since}",
        )


def render_htf_po3_panel(ltps: dict):
    state = _load(STATE_FILES["HTF_PO3"])

    with st.container(border=True):
        st.subheader("🔱 HTF Power of 3 Bot  (NIFTY + BANKNIFTY)")
        st.markdown(
            '<div class="research-badge">'
            'Research: 60-min PO3 fractal (Accum → Manipulation FVG → CISD) → Sell ATM PE  |  '
            'NIFTY: accum=30m fvg≥20pts SL=2× tgt=0.7 weekly  |  '
            'BANKNIFTY: accum=15m fvg≥20pts SL=1.5× tgt=0.3 monthly  |  '
            'Entry 09:45–14:30 IST  |  EOD 15:20  |  1 lot flat  |  ALL 10 pipeline stages ✅'
            '</div>',
            unsafe_allow_html=True,
        )

        if not state:
            st.error(
                "Bot not running — state file absent. "
                "Check `live_trading/logs/htf_po3_state.json`."
            )
            return

        _, age_label = _staleness(state.get("last_update", ""))
        st.caption(f"State file: {age_label}")

        # ── Window summary row ─────────────────────────────────────────────────
        _po3_win = "09:45–14:30"
        _po3_in_win = _entry_window_open(_po3_win)
        pw1, pw2, pw3 = st.columns(3)
        pw1.metric("Entry Window", f"{_po3_win} IST",
                   delta="🟢 OPEN" if _po3_in_win else "🔴 CLOSED",
                   delta_color="off",
                   help="Signals only accepted inside this window")
        pw2.metric("EOD Exit", "15:20 IST", help="All positions closed unconditionally")
        pw3.metric("State Age", age_label.split(" ", 1)[-1] if " " in age_label else age_label)

        with st.expander("📖 Strategy & HTF PO3 Sequence Details"):
            po3_ch1, po3_ch2 = st.columns(2)
            with po3_ch1:
                st.markdown("**Entry Conditions — 5-Stage PO3 Sequence**")
                st.markdown(
                    '<div class="condition-row">'
                    '1️⃣ Accumulation window completes (30m NIFTY / 15m BANKNIFTY)<br>'
                    '2️⃣ Price breaks below accumulation low (Manipulation begins)<br>'
                    '3️⃣ Bullish FVG detected ≥ 20 pts in manipulation leg<br>'
                    '4️⃣ CISD: 1-min close above FVG top (displacement confirmation)<br>'
                    '5️⃣ Entry window: 09:45–14:30 IST'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with po3_ch2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🎯 <b>E4 Target</b>: NIFTY −30% | BANKNIFTY −70% from entry<br>'
                    '🛑 <b>Safety SL</b>: NIFTY 2× | BANKNIFTY 1.5× entry<br>'
                    '⏰ <b>EOD exit</b>: 15:20 IST unconditional close<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )

        # ── Two instruments side by side ───────────────────────────────────────
        INSTRUMENT_CONFIGS = {
            "NIFTY": {
                "lot_size": 65, "accum_minutes": 30, "fvg_min_size": 20,
                "sl_mult": "2.0", "target_pct": "0.7",
            },
            "BANKNIFTY": {
                "lot_size": 30, "accum_minutes": 15, "fvg_min_size": 20,
                "sl_mult": "1.5", "target_pct": "0.3",
            },
        }

        col_nifty, col_bnf = st.columns(2)
        for col, sym_key in ((col_nifty, "NIFTY"), (col_bnf, "BANKNIFTY")):
            instrument = state.get(sym_key, {})
            cfg        = INSTRUMENT_CONFIGS[sym_key]
            with col:
                with st.container(border=True):
                    _render_po3_instrument(sym_key, cfg, instrument, ltps)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 8 — EMA SWING SCANNER (daily, paper)
# ══════════════════════════════════════════════════════════════════════════════

def render_ema_swing_panel(ltps: dict):
    """RETIRED 2026-06-04 — EMA Swing Scanner. Function kept for historical reference only."""
    st.warning("⚠️ EMA Swing Scanner was retired on 2026-06-04.")
    return
    state = _load(STATE_FILES["EMA_SWING"])  # noqa: unreachable

    with st.container(border=True):
        st.subheader("📊 EMA Swing Scanner — Daily Paper Trader")
        with st.expander("📖 Strategy & Regime Details"):
            st.markdown(
                '<div class="research-badge">'
                'Strategy: EMA Pullback + RSI Confirmation on 16 NIFTY50 stocks (Daily)  |  '
                'V3 Backtest Jan 2023–Mar 2026: 423 trades | WR 52.3% | PF 1.83 | '
                'Sharpe 2.48 | MaxDD −7.75% | CAGR 24.99% | ₹10L → ₹20.47L  |  '
                'Regime filter: NIFTY50-INDEX > 50 EMA  |  Fires daily at 15:30 IST  |  PAPER MODE'
                '</div>',
                unsafe_allow_html=True,
            )
            ec1, ec2 = st.columns(2)
            with ec1:
                st.markdown("**Entry Conditions**")
                st.markdown(
                    '<div class="condition-row">'
                    '📈 Regime: NIFTY50 > EMA(50) on daily chart<br>'
                    '🔍 Signal: Close crosses below EMA(20) but stays above EMA(50)<br>'
                    '✅ RSI(14) ≥ 50 for bullish momentum<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with ec2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🛑 Stop-loss: 2-ATR fixed stop from entry<br>'
                    '🎯 Exit on Daily Profit Target or Regime Flip<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )

        # ── Top metrics row ────────────────────────────────────────────────────
        portfolio_val   = state.get("portfolio_value", 1_000_000)
        starting_cap    = state.get("starting_capital", 1_000_000)
        total_ret_pct   = state.get("total_return_pct", 0.0)
        regime_active   = state.get("regime_active", False)
        last_scan       = state.get("last_scan", "")
        open_count      = state.get("open_count", 0)
        pending_count   = len(state.get("pending_signals", []))
        closed_today    = state.get("closed_today", [])
        index_info      = state.get("index", {})

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Portfolio Value", f"₹{portfolio_val:,.0f}",
                  delta=f"{total_ret_pct:+.2f}% from ₹{starting_cap/1e5:.0f}L",
                  delta_color="normal" if total_ret_pct >= 0 else "inverse")
        c2.metric("Open Positions", open_count, help="Max 5 concurrent")
        c3.metric("Pending (next open)", pending_count, help="Signals queued for tomorrow's open")
        c4.metric("Closed Today", len(closed_today))

        # Regime metric
        regime_emoji = "✅ ON" if regime_active else "❌ OFF"
        idx_close = index_info.get("close", 0)
        idx_ema   = index_info.get("ema50", 0)
        c5.metric("Regime (NIFTY50 > EMA50)", regime_emoji,
                  delta=f"{idx_close:,.0f} vs {idx_ema:,.0f}" if idx_close else "—",
                  delta_color="normal" if regime_active else "inverse")
        c6.metric("Last Scan", last_scan[:10] if last_scan else "never")

        # ── Today's signals ────────────────────────────────────────────────────
        signals_today = state.get("signals_today", [])
        st.markdown("---")
        if signals_today:
            st.markdown(f"**🎯 Today's Signals ({len(signals_today)}) — Entry Tomorrow Open**")
            sig_rows = []
            for s in signals_today:
                tier_label = {"tier1": "T1★ (1.5×)", "tier2": "T2 (1.0×)", "tier3": "T3 (0.75×)"}.get(
                    s.get("tier", ""), s.get("tier", ""))
                sig_rows.append({
                    "Symbol":        s.get("symbol", ""),
                    "Tier":          tier_label,
                    "Close ₹":       s.get("close", 0),
                    "RSI(14)":       round(s.get("rsi", 0), 1),
                    "ATR(14) ₹":     round(s.get("atr", 0), 2),
                    "Indicative Stop ₹": round(s.get("suggested_stop", 0), 2),
                    "Indicative Cap ₹":  round(s.get("suggested_hard_cap", 0), 2),
                })
            df_sig = pd.DataFrame(sig_rows)
            st.dataframe(df_sig, hide_index=True, width='stretch')
        elif regime_active:
            st.info("🔍 No signals found today.")
        else:
            st.warning("⏸ Regime OFF — NIFTY50 below 50 EMA. No new entries permitted.")

        # ── Open positions ─────────────────────────────────────────────────────
        open_positions = state.get("open_positions", [])
        st.markdown("---")
        if open_positions:
            st.markdown(f"**📂 Open Paper Positions ({len(open_positions)})**")
            pos_rows = []
            for pos in open_positions:
                sym        = pos.get("symbol", "")
                entry_px   = float(pos.get("entry_price") or 0)
                shares     = int(pos.get("shares") or 0)
                ltp        = ltps.get(sym, entry_px)   # live NSE equity LTP
                mtm        = (ltp - entry_px) * shares  # long equity
                be_label   = "✅ Active" if pos.get("breakeven_active") else "—"
                pos_rows.append({
                    "Symbol":      sym,
                    "Tier":        pos.get("tier", ""),
                    "Entry Date":  pos.get("entry_date", ""),
                    "Entry ₹":     entry_px,
                    "LTP ₹":       ltp,
                    "MTM ₹":       mtm,
                    "Shares":      shares,
                    "Stop ₹":      float(pos.get("stop_eod") or 0),
                    "Hard Cap ₹":  float(pos.get("hard_cap") or 0),
                    "Day #":       pos.get("days_held", 0),
                    "Breakeven":   be_label,
                })

            def _color_be(v):
                return "color:#00e599;font-weight:700" if v == "✅ Active" else "color:#5a7ba0"

            def _color_mtm_str(v):
                try:
                    n = float(str(v).replace("₹", "").replace(",", "").replace("+", ""))
                    return "color:#00e599;font-weight:700" if n > 0 else ("color:#f87171;font-weight:700" if n < 0 else "color:#5a7ba0")
                except Exception:
                    return ""

            df_pos = pd.DataFrame(pos_rows)
            st.dataframe(
                df_pos.style
                    .map(_color_be, subset=["Breakeven"])
                    .map(lambda v: "color:#00e599;font-weight:700" if v > 0 else ("color:#f87171;font-weight:700" if v < 0 else "color:#5a7ba0"), subset=["MTM ₹"])
                    .format({
                        "Entry ₹":    "₹{:.2f}",
                        "LTP ₹":      "₹{:.2f}",
                        "MTM ₹":      lambda v: f"{'+'if v>0 else ''}₹{v:,.0f}",
                        "Stop ₹":     "₹{:.2f}",
                        "Hard Cap ₹": "₹{:.2f}",
                    }),
                hide_index=True,
                width='stretch',
            )

            # Total open MTM
            total_mtm = sum(r["MTM ₹"] for r in pos_rows)
            sign  = "+" if total_mtm > 0 else ""
            color = "green" if total_mtm > 0 else ("red" if total_mtm < 0 else "gray")
            st.markdown(
                f'**EMA Swing Open MTM: '
                f'<span style="color:{color};font-weight:bold">{sign}₹{total_mtm:,.0f}</span>**',
                unsafe_allow_html=True,
            )
        else:
            st.info("📂 No open paper positions.")

        # ── Pending signals ────────────────────────────────────────────────────
        pending = state.get("pending_signals", [])
        if pending:
            st.markdown("---")
            st.markdown(f"**⏳ Pending (open at tomorrow's open) — {len(pending)}**")
            pend_rows = [
                {
                    "Symbol":      p.get("symbol", ""),
                    "Tier":        p.get("tier", ""),
                    "Signal Date": p.get("signal_date", ""),
                    "Close ₹":     p.get("close", 0),
                    "ATR(14) ₹":   round(p.get("atr", 0), 2),
                    "RSI(14)":     round(p.get("rsi", 0), 1),
                }
                for p in pending
            ]
            st.dataframe(pd.DataFrame(pend_rows), hide_index=True, width='stretch')

        # ── Closed today ───────────────────────────────────────────────────────
        if closed_today:
            st.markdown("---")
            st.markdown(f"**🏁 Closed Today ({len(closed_today)})**")
            closed_rows = []
            for t in closed_today:
                pnl = t.get("pnl_inr", 0) or 0
                closed_rows.append({
                    "Symbol":      t.get("symbol", ""),
                    "Exit Reason": t.get("exit_reason", ""),
                    "Entry ₹":     t.get("entry_price", 0),
                    "Exit ₹":      t.get("exit_price", 0),
                    "Shares":      t.get("shares", 0),
                    "P&L ₹":       round(pnl, 0),
                    "P&L %":       round(t.get("pnl_pct", 0) or 0, 2),
                    "Days Held":   t.get("days_held", 0),
                    "W/L":         "🟢 Win" if t.get("win") else "🔴 Loss",
                })

            def _color_pnl(v):
                return "color:#00e599;font-weight:700" if v > 0 else ("color:#f87171;font-weight:700" if v < 0 else "color:#5a7ba0")

            df_closed = pd.DataFrame(closed_rows)
            st.dataframe(
                df_closed.style
                    .map(_color_pnl, subset=["P&L ₹", "P&L %"])
                    .format({"Entry ₹": "₹{:.2f}", "Exit ₹": "₹{:.2f}",
                             "P&L ₹": lambda v: f"{'+'if v>0 else ''}₹{v:,.0f}",
                             "P&L %":  lambda v: f"{v:+.2f}%"}),
                hide_index=True,
                width='stretch',
            )


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 8 — BANKNIFTY BB OPTIONS BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_bnf_bb_options_panel(ltps: dict):
    state = _load(STATE_FILES["BNF_BB_OPT"])

    with st.container(border=True):
        st.subheader("📉 BANKNIFTY BB Options Bot")
        st.markdown(
            '<div class="research-badge">'
            'Research: BB(20,2σ) on 1-min ATM option premium — premium closes ≥ upper BB → SELL that option  |  '
            'IS Sharpe +2.20 (non-expiry)  |  OOS Sharpe +2.69  |  WR 86%  |  10/10 stages PASS  |  '
            'Monthly expiry-day SKIP  |  Entry 09:30–14:00 IST  |  SL 1.5× entry  |  Exit on SMA reversion'
            '</div>',
            unsafe_allow_html=True,
        )

        if not state:
            st.error(
                "Bot not running — state file absent. "
                "Check `live_trading/logs/banknifty_bb_options_state.json`."
            )
            return

        bnf_ltp      = state.get("bnf_ltp", 0)
        vix_ltp      = state.get("vix_ltp", 0)
        expiry       = state.get("expiry", "—")
        is_exp_day   = state.get("is_expiry_day", False)
        lot_size     = state.get("lot_size", "—")
        n_lots       = state.get("n_lots", 1)
        entry_win    = state.get("entry_window", "09:30–14:00")
        phase        = state.get("phase", "unknown")
        signal_fired = state.get("signal_fired", False)
        ce_symbol    = state.get("ce_symbol", "—")
        pe_symbol    = state.get("pe_symbol", "—")
        bb_ce        = state.get("ce_bb", {})
        bb_pe        = state.get("pe_bb", {})
        active       = state.get("active_trade")

        with st.expander("📖 Strategy & Research Details"):
            st.markdown(
                '<div class="research-badge">'
                'Research: BB(20,2σ) on 1-min ATM option premium — premium closes ≥ upper BB → SELL that option  |  '
                'Monthly expiry-day SKIP  |  Entry 09:30–14:00 IST  |  SL 1.5× entry  |  Exit on SMA reversion'
                '</div>',
                unsafe_allow_html=True,
            )
            ch1, ch2 = st.columns(2)
            with ch1:
                st.markdown("**Entry Conditions**")
                st.markdown(
                    '<div class="condition-row">'
                    '🔍 BB(20,2σ) 1-min upper band touch/cross<br>'
                    '✅ Option premium ≥ upper BB<br>'
                    '✅ Entry window open (09:30–14:00 IST)<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with ch2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🛑 Stop-loss: 1.5x entry premium<br>'
                    '🎯 Mean-reversion exit: 1-min close ≤ SMA (middle band)<br>'
                    '⏰ EOD exit: 15:20 IST unconditional close<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )

        # ── Row 1: key metrics ─────────────────────────────────────────────────
        _bnf_in_win = _entry_window_open(entry_win)
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("BANKNIFTY", f"{bnf_ltp:,.2f}" if bnf_ltp else "—")
        c2.metric("VIX", f"{vix_ltp:.2f}" if vix_ltp else "—")
        c3.metric("Expiry", expiry, delta="⚡ EXPIRY DAY" if is_exp_day else "")
        c4.metric("Phase", phase.replace("_", " ").title())
        c5.metric("Window", entry_win,
                  delta="🟢 OPEN" if _bnf_in_win else "🔴 CLOSED",
                  delta_color="off",
                  help="IST — signals only accepted inside this window")

        if is_exp_day:
            st.markdown('<div class="signal-banner-off">🚫 BANKNIFTY MONTHLY EXPIRY DAY — No trades today</div>', unsafe_allow_html=True)
            return

        # ── Option symbols resolved ────────────────────────────────────────────
        st.markdown(f"**ATM CE**: `{ce_symbol}` &nbsp;|&nbsp; **ATM PE**: `{pe_symbol}`")

        # ── BB Snapshots (CE and PE side by side) ─────────────────────────────
        if bb_ce or bb_pe:
            st.markdown("---")
            st.markdown("**BB(20, 2σ) Premium Snapshots — 1-min bars**")
            left_bb, right_bb = st.columns(2)

            for col, bb, label, sym in [
                (left_bb,  bb_ce, "CE Premium", ce_symbol),
                (right_bb, bb_pe, "PE Premium", pe_symbol),
            ]:
                with col:
                    if not bb:
                        st.info(f"{label}: warming up…")
                        continue

                    close = bb.get("close", 0)
                    sma   = bb.get("sma", 0)
                    upper = bb.get("upper", 0)
                    lower = bb.get("lower", 0)
                    bars  = bb.get("bars", 0)

                    st.caption(f"**{label}** — {bars} bars loaded")
                    b1, b2, b3 = col.columns(3)
                    b1.metric("Close ₹", f"{close:.2f}")
                    b2.metric("SMA (mid) ₹", f"{sma:.2f}")
                    b3.metric(
                        "Upper BB ₹", f"{upper:.2f}",
                        delta=f"{close - upper:+.2f}",
                        delta_color="inverse" if close > upper else "normal",
                    )

                    prog, zone = _bb_progress(close, lower, upper)
                    col.caption(f"{zone}")
                    col.progress(prog)

                    is_triggered = close > upper
                    if is_triggered:
                        col.markdown(
                            '<div class="signal-banner-wait">'
                            f'🟡 {label} ABOVE UPPER BB — SELL signal candidate'
                            '</div>',
                            unsafe_allow_html=True,
                        )

        # ── Signal / Position Banner ───────────────────────────────────────────
        st.markdown("---")
        in_window = _entry_window_open(entry_win)

        if active:
            sym       = active.get("symbol", "")
            side      = active.get("side", "")
            entry_p   = float(active.get("entry_prem", 0))
            sl_p      = float(active.get("sl_prem", 0))
            sma_tgt   = float(active.get("sma_target", 0))
            qty       = int(active.get("qty", 0))
            since     = active.get("entry_time", "")[:19].replace("T", " ")
            ltp       = ltps.get(sym, entry_p)
            pnl       = (entry_p - ltp) * qty   # seller: profit when premium falls

            st.markdown(
                '<div class="signal-banner-on">'
                f'🟢 POSITION ACTIVE — {side} {sym} | SL {sl_p:.2f} | Target SMA {sma_tgt:.2f}'
                '</div>',
                unsafe_allow_html=True,
            )

            st.markdown("**Active Position**")
            t1, t2, t3, t4, t5, t6 = st.columns(6)
            t1.metric("Symbol", sym)
            t2.metric("Entry ₹", f"{entry_p:.2f}")
            t3.metric("SL ₹", f"{sl_p:.2f}", help="1.5× entry premium")
            t4.metric("SMA Target ₹", f"{sma_tgt:.2f}", help="Exit when premium closes ≤ SMA")
            t5.metric("LTP ₹", f"{ltp:.2f}", delta=f"{ltp - entry_p:+.2f}")
            pnl_dir = "normal" if pnl > 0 else "inverse"
            t6.metric("MTM", f"{'+'if pnl>0 else ''}₹{pnl:,.0f}",
                      delta_color=pnl_dir, help=f"Qty: {qty} | Entered: {since}")

        elif signal_fired:
            st.markdown(
                '<div class="signal-banner-wait">'
                '🟡 SIGNAL FIRED — trade already taken today (one trade per session)'
                '</div>',
                unsafe_allow_html=True,
            )
            _render_today_trades_detail(_load_today_trades("banknifty_bb_options_bot"))
        elif not in_window:
            st.markdown(
                '<div class="signal-banner-off">'
                f'⏸ Entry window CLOSED ({entry_win} IST) — monitoring paused'
                '</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="signal-banner-off">'
                f'⚪ Monitoring — no signal yet | Entry window: {entry_win} IST ✅'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── Conditions checklist ───────────────────────────────────────────────
        st.markdown("---")
        ch1, ch2 = st.columns(2)

        with ch1:
            st.markdown("**Entry Conditions**")
            ce_triggered = bb_ce.get("close", 0) > bb_ce.get("upper", float("inf")) if bb_ce else False
            pe_triggered = bb_pe.get("close", 0) > bb_pe.get("upper", float("inf")) if bb_pe else False
            ce_bars_ok   = bb_ce.get("bars", 0) >= 20 if bb_ce else False
            pe_bars_ok   = bb_pe.get("bars", 0) >= 20 if bb_pe else False
            no_trade     = not signal_fired and not active
            st.markdown(
                f'<div class="condition-row">'
                f'{_tick(ce_triggered or pe_triggered)} Premium (CE or PE) closes ≥ upper BB(20,2σ)<br>'
                f'&nbsp;&nbsp;CE: {"✅" if ce_triggered else "⚪"} ({bb_ce.get("close",0):.2f} vs {bb_ce.get("upper",0):.2f}) '
                f'[{bb_ce.get("bars",0)} bars]<br>'
                f'&nbsp;&nbsp;PE: {"✅" if pe_triggered else "⚪"} ({bb_pe.get("close",0):.2f} vs {bb_pe.get("upper",0):.2f}) '
                f'[{bb_pe.get("bars",0)} bars]<br>'
                f'{_tick(in_window)} Entry window open ({entry_win} IST)<br>'
                f'{_tick(no_trade)} No trade taken today (one signal per session)<br>'
                f'{_tick(not is_exp_day)} Not a BANKNIFTY monthly expiry day'
                f'</div>',
                unsafe_allow_html=True,
            )

        with ch2:
            st.markdown("**Exit Rules**")
            st.markdown(
                '<div class="condition-row">'
                '🛑 <b>Stop-loss</b>: tick-level SL — exit if LTP ≥ 1.5× entry premium<br>'
                '🎯 <b>Mean-reversion exit</b>: 1-min close ≤ SMA (middle band) → exit<br>'
                '⏰ <b>EOD exit</b>: 15:20 IST unconditional close<br>'
                '📦 <b>Sizing</b>: 1 lot (30 units) — SELL CE or PE whichever triggered first<br>'
                '⛔ <b>Expiry filter</b>: all BANKNIFTY monthly expiry days skipped'
                '</div>',
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 9b — BB MEAN REVERSION BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_bb_mean_reversion_panel(ltps: dict):
    state = _load(STATE_FILES["BB_MEAN_REV"])

    with st.container(border=True):
        st.subheader("📉 BB Mean Reversion Bot")
        st.markdown(
            '<div class="research-badge">'
            'Research: BB(20,2σ) on 1-min BANKNIFTY INDEX — red candle + high > upper BB → BUY ATM monthly PE  |  '
            'IS Sharpe +1.008  |  OOS Sharpe +2.425  |  MC 97.3%  |  10/10 stages PASS  |  '
            'Trend filter (Bear/Sideways only)  |  NatRR ≥ 1.25 gate  |  DTE exclusion 8–14  |  DEBIT trade'
            '</div>',
            unsafe_allow_html=True,
        )

        if not state:
            st.error(
                "Bot not running — state file absent. "
                "Check `live_trading/logs/bb_mean_reversion_state.json`."
            )
            return

        bnf_ltp      = state.get("bnf_ltp", 0)
        pe_ltp       = state.get("pe_ltp", 0)
        pe_symbol    = state.get("pe_symbol", "—")
        expiry       = state.get("expiry", "—")
        phase        = state.get("phase", "unknown")
        trend_ok     = state.get("trend_ok", False)
        dte_ok       = state.get("dte_ok", False)
        signal_fired = state.get("signal_fired", False)
        paper_mode   = state.get("paper_mode", True)
        n_lots       = state.get("n_lots", 1)
        lot_size     = state.get("lot_size", 30)
        bars_loaded  = state.get("bars_loaded", 0)
        bb           = state.get("bb", {})
        htf_open     = state.get("htf_open", 0)
        active       = state.get("active_trade")
        entry_win    = "09:30–14:45"

        with st.expander("📖 Strategy & Gate Details"):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**5-Gate Signal Check**")
                st.markdown(
                    '<div class="condition-row">'
                    '1️⃣ <b>Trend</b>: prev-day close ≤ 20-day SMA (Bear/Sideways)<br>'
                    '2️⃣ <b>Trigger</b>: 1-min bar RED AND high > upper BB(20,2σ)<br>'
                    '3️⃣ <b>HTF</b>: trigger_close < current 5-min candle open<br>'
                    '4️⃣ <b>Nat R:R</b>: (trigger_close−SMA) / (trigger_high−trigger_close) ≥ 1.25<br>'
                    '5️⃣ <b>DTE</b>: monthly expiry NOT in 8–14 day band'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with c2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🛑 <b>SL (primary)</b>: BANKNIFTY spot ≥ trigger_high (index tick-level)<br>'
                    '🛑 <b>SL (secondary)</b>: PE LTP ≤ sl_opt (belt-and-suspenders)<br>'
                    '🎯 <b>Target</b>: PE LTP ≥ entry + 4× risk_opt<br>'
                    '⏰ <b>EOD</b>: 15:15 IST unconditional close<br>'
                    '📦 <b>Position</b>: BUY PE (DEBIT, no margin) | 1 lot (30 units)'
                    '</div>',
                    unsafe_allow_html=True,
                )

        # ── Row 1: key metrics ─────────────────────────────────────────────────
        _win_open = _entry_window_open(entry_win)
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("BANKNIFTY", f"{bnf_ltp:,.0f}" if bnf_ltp else "—")
        c2.metric("PE LTP", f"₹{pe_ltp:.2f}" if pe_ltp else "—")
        c3.metric("Expiry", expiry if expiry != "—" else "—")
        c4.metric("Phase", phase.replace("_", " ").title())
        c5.metric("Window", entry_win,
                  delta="🟢 OPEN" if _win_open else "🔴 CLOSED",
                  delta_color="off")

        # ── Gate status row ────────────────────────────────────────────────────
        g1, g2, g3 = st.columns(3)
        with g1:
            if not trend_ok:
                st.markdown(
                    '<div class="signal-banner-off">⛔ Gate 1 (Trend): Bull day — bot INACTIVE</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div class="signal-banner-wait">✅ Gate 1 (Trend): Bear/Sideways — entries enabled</div>',
                    unsafe_allow_html=True,
                )
        with g2:
            if not dte_ok:
                st.markdown(
                    '<div class="signal-banner-off">⛔ Gate 5 (DTE): 8–14 day band — entries disabled</div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div class="signal-banner-wait">✅ Gate 5 (DTE): DTE outside exclusion band</div>',
                    unsafe_allow_html=True,
                )
        with g3:
            mode_label = "PAPER" if paper_mode else "⚠️ LIVE"
            st.markdown(
                f'<div class="signal-banner-wait">'
                f'Mode: {mode_label} | {n_lots} lot × {lot_size} units | Bars: {bars_loaded}'
                f'</div>',
                unsafe_allow_html=True,
            )

        # ── BB Snapshot ────────────────────────────────────────────────────────
        if bb:
            st.markdown("---")
            st.markdown("**BB(20, 2σ) Index Snapshot — 1-min BANKNIFTY bars**")
            b1, b2, b3, b4, b5 = st.columns(5)
            b1.metric("Close", f"{bb.get('close', 0):.0f}")
            b2.metric("Open", f"{bb.get('open', 0):.0f}")
            b3.metric("High", f"{bb.get('high', 0):.0f}")
            b4.metric("SMA (mid)", f"{bb.get('sma', 0):.0f}")
            b5.metric(
                "Upper BB", f"{bb.get('upper', 0):.0f}",
                delta=f"{bb.get('close',0) - bb.get('upper',0):+.0f}",
                delta_color="inverse" if bb.get('close', 0) > bb.get('upper', 0) else "normal",
            )
            prog, zone = _bb_progress(bb.get('close', 0), bb.get('lower', 0), bb.get('upper', 0))
            st.caption(f"HTF 5-min open: {htf_open:.0f}  |  {zone}")
            st.progress(prog)

            is_red         = bb.get('close', 1) < bb.get('open', 0)
            above_upper    = bb.get('close', 0) > bb.get('upper', float('inf'))
            trigger_active = is_red and above_upper and trend_ok and dte_ok and _win_open
            if trigger_active:
                risk_index = bb.get('high', 0) - bb.get('close', 0)
                dist_sma   = bb.get('close', 0) - bb.get('sma', 0)
                nat_rr     = (dist_sma / risk_index) if risk_index > 0 else 0
                rr_ok      = nat_rr >= 1.25
                htf_ok     = bb.get('close', 0) < htf_open if htf_open > 0 else False
                st.markdown(
                    '<div class="signal-banner-wait">'
                    f'🔔 TRIGGER ACTIVE — Red candle + high > upper BB  |  '
                    f'HTF: {"✅" if htf_ok else "❌"} ({bb.get("close",0):.0f} vs 5-min {htf_open:.0f})  |  '
                    f'NatRR: {"✅" if rr_ok else "❌"} {nat_rr:.2f} (need ≥ 1.25)'
                    '</div>',
                    unsafe_allow_html=True,
                )

        # ── Active trade / position banner ─────────────────────────────────────
        st.markdown("---")
        if active:
            entry_opt  = float(active.get("entry_opt", 0))
            sl_opt     = float(active.get("sl_opt", 0))
            tp_opt     = float(active.get("tp_opt", 0))
            sl_index   = float(active.get("sl_index", 0))
            risk_opt   = float(active.get("risk_opt", 0))
            nat_rr_v   = float(active.get("nat_rr", 0))
            qty        = int(active.get("qty", 0))
            entry_time = active.get("entry_time", "")[:19].replace("T", " ")
            sym        = active.get("symbol", "")
            ltp        = pe_ltp if pe_ltp > 0 else entry_opt
            pnl        = (ltp - entry_opt) * qty   # buyer: profit when premium rises

            st.markdown(
                '<div class="signal-banner-on">'
                f'🟢 LONG PE ACTIVE — {sym}  |  SL index: {sl_index:.0f}  |  Target: ₹{tp_opt:.2f}'
                '</div>',
                unsafe_allow_html=True,
            )
            t1, t2, t3, t4, t5, t6 = st.columns(6)
            t1.metric("Symbol", sym)
            t2.metric("Entry ₹", f"{entry_opt:.2f}")
            t3.metric("SL opt ₹", f"{sl_opt:.2f}", help=f"Index SL: {sl_index:.0f}")
            t4.metric("Target ₹", f"{tp_opt:.2f}", help=f"4× risk = {risk_opt:.2f} pts")
            t5.metric("LTP ₹", f"{ltp:.2f}", delta=f"{ltp - entry_opt:+.2f}")
            pnl_dir = "normal" if pnl >= 0 else "inverse"
            t6.metric("MTM", f"{'+'if pnl>=0 else ''}₹{pnl:,.0f}",
                      delta_color=pnl_dir, help=f"NatRR at entry: {nat_rr_v:.2f} | {entry_time}")

        elif signal_fired:
            st.markdown(
                '<div class="signal-banner-wait">'
                '🟡 Signal taken today — one trade per session (monitoring only)'
                '</div>',
                unsafe_allow_html=True,
            )
            _render_today_trades_detail(_load_today_trades("bb_mean_reversion_bot"))
        elif not trend_ok:
            st.markdown(
                '<div class="signal-banner-off">'
                '⛔ Inactive today — BANKNIFTY is in a Bull trend (Gate 1)'
                '</div>',
                unsafe_allow_html=True,
            )
        elif not dte_ok:
            st.markdown(
                '<div class="signal-banner-off">'
                '⛔ Inactive today — Monthly expiry in 8–14 DTE exclusion band (Gate 5)'
                '</div>',
                unsafe_allow_html=True,
            )
        elif not _win_open:
            st.markdown(
                '<div class="signal-banner-off">'
                f'⏸ Entry window closed ({entry_win} IST) — monitoring paused'
                '</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="signal-banner-off">'
                f'⚪ Monitoring — no signal yet | ATM PE: {pe_symbol}'
                '</div>',
                unsafe_allow_html=True,
            )



# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 10 — HA OPTIONS BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_ha_options_panel(ltps: dict):
    state = _load(STATE_FILES["HA_OPTIONS"])

    with st.container(border=True):
        st.subheader("🕯 HA Options Bot")

        if not state:
            st.error("Bot not running — state file absent.")
            return

        vix_ltp         = state.get("vix_ltp", 0)
        entry_win       = state.get("entry_window", "09:30–14:30")
        in_window       = _entry_window_open(entry_win)
        instruments     = state.get("instruments", {})
        stage11_start   = state.get("stage_11_start", "—")
        vol_thresh_pct  = state.get("vol_threshold_pct", 0.83)

        with st.expander("📖 Strategy & Research Details"):
            st.markdown(
                '<div class="research-badge">'
                'Research: HA candle flip (per-day reset) → sell ATM CE/PE  |  '
                'OOS combined Sharpe +12.37  |  ALL 10/10 stages PASS  |  '
                'Entry 09:30–14:30 IST  |  Exit: HA reversal OR swing SL (5-bar lookback)  |  '
                f'Stage 8 vol filter: NIFTY prior-day range ≥ {vol_thresh_pct:.2f}%  |  '
                f'Stage 11 Run 2: reset {stage11_start}'
                '</div>',
                unsafe_allow_html=True,
            )
            
            hc1, hc2 = st.columns(2)
            with hc1:
                st.markdown("**Entry Conditions**")
                st.markdown(
                    f'<div class="condition-row">'
                    f'Entry window open ({entry_win} IST)<br>'
                    f'&bull; NIFTY: HA flip (5m) &nbsp;&bull; BANKNIFTY: HA flip (15m) &nbsp;&bull; SENSEX: HA flip (5m)<br>'
                    f'Bullish flip &rarr; Sell CE &nbsp;|&nbsp; Bearish flip &rarr; Sell PE'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            with hc2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🛑 <b>HA Reversal</b>: exit if HA direction flips against position<br>'
                    '📌 <b>Swing Stop</b>: 5-bar protective stop on index level<br>'
                    '⏰ <b>EOD exit</b>: 15:20 IST close<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )

        # ── Row 1: VIX + per-instrument LTPs + Window ─────────────────────────
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("VIX", f"{vix_ltp:.2f}" if vix_ltp else "—")

        for col, sym in zip([c2, c3, c4], ["NIFTY", "BANKNIFTY", "SENSEX"]):
            inst_d = instruments.get(sym, {})
            ltp_v  = inst_d.get("ltp", 0)
            tf     = inst_d.get("tf", "?")
            expiry = inst_d.get("expiry") or "—"
            ha_sig = inst_d.get("ha_signal", 0)
            ha_label = "📈 Bull" if ha_sig == 1 else ("📉 Bear" if ha_sig == -1 else "⚪ Neut")
            col.metric(
                f"{sym}",
                f"{ltp_v:,.2f}" if ltp_v else "—",
                delta=f"{ha_label} | {tf}m | exp {expiry}",
                delta_color="off",
            )

        c5.metric("Window", entry_win,
                  delta="🟢 OPEN" if in_window else "🔴 CLOSED",
                  delta_color="off",
                  help="IST — signals only accepted inside this window")

        st.markdown("---")
        # Render active positions for HA Options (logic remains same but more compact)
        for sym_key, inst_dict in instruments.items():
            active = inst_dict.get("active_trade")
            if active:
                sym       = active.get("symbol", "")
                side      = active.get("side", "")
                entry_p   = float(active.get("entry_prem", 0))
                sl_p      = float(active.get("sl_prem", 0))
                qty       = int(active.get("qty", 0))
                since     = active.get("entry_time", "")[:19].replace("T", " ")
                ltp       = ltps.get(sym, entry_p)
                pnl       = (entry_p - ltp) * qty
                
                t1, t2, t3, t4, t5 = st.columns(5)
                t1.metric(f"Active {sym_key}", sym)
                t2.metric("Entry", f"{entry_p:.2f}")
                t3.metric("SL", f"{sl_p:.2f}")
                t4.metric("LTP", f"{ltp:.2f}", delta=f"{ltp-entry_p:+.2f}")
                t5.metric("PnL", f"₹{pnl:,.0f}", delta_color="normal" if pnl>0 else "inverse")

        st.markdown("---")

        # ── Per-instrument panels ──────────────────────────────────────────────
        inst_cols = st.columns(3)

        for col, sym in zip(inst_cols, ["NIFTY", "BANKNIFTY", "SENSEX"]):
            inst_d = instruments.get(sym, {})
            if not inst_d:
                col.info(f"**{sym}** — not initialised")
                continue

            tf           = inst_d.get("tf", "?")
            bars         = inst_d.get("bars_loaded", 0)
            ha_sig       = inst_d.get("ha_signal", 0)
            expiry       = inst_d.get("expiry") or "—"
            is_exp_day   = inst_d.get("is_expiry_day", False)
            lot_size     = inst_d.get("lot_size", "—")
            trade_done   = inst_d.get("trade_done_today", False)
            active       = inst_d.get("active_trade")
            vol_pass     = inst_d.get("vol_filter_pass", True)

            with col:
                st.markdown(f"**{sym} — {tf}-min HA**")

                ha_arrow = "📈 Bullish" if ha_sig == 1 else ("📉 Bearish" if ha_sig == -1 else "⚪ Neutral")
                m1, m2, m3 = col.columns(3)
                m1.metric("HA Dir", ha_arrow.split()[1] if ha_sig != 0 else "—")
                m2.metric("Bars", bars)
                m3.metric("Expiry", expiry)

                if is_exp_day:
                    st.markdown(
                        '<div class="signal-banner-wait">'
                        f'⚠️ {sym} EXPIRY DAY — caution: shorter theta, faster moves'
                        '</div>',
                        unsafe_allow_html=True,
                    )

                if not vol_pass:
                    st.markdown(
                        '<div class="signal-banner-off">'
                        f'⛔ VOL FILTER BLOCKED — low-vol day (NIFTY prior-day range &lt; {vol_thresh_pct:.2f}%), no entries for {sym}'
                        '</div>',
                        unsafe_allow_html=True,
                    )

                if active:
                    sym_opt  = active.get("symbol", "")
                    side     = active.get("side", "?")
                    entry_p  = float(active.get("entry_prem", 0))
                    sl_idx   = float(active.get("sl_index", 0))
                    qty      = int(active.get("qty", 0))
                    since    = active.get("entry_time", "")[:16].replace("T", " ")
                    ltp_opt  = ltps.get(sym_opt, entry_p)
                    pnl      = (entry_p - ltp_opt) * qty

                    st.markdown(
                        '<div class="signal-banner-on">'
                        f'🟢 ACTIVE — SELL {side} {sym_opt}'
                        '</div>',
                        unsafe_allow_html=True,
                    )
                    a1, a2 = st.columns(2)
                    a1.metric("Entry ₹", f"{entry_p:.2f}")
                    a2.metric("LTP ₹", f"{ltp_opt:.2f}", delta=f"{ltp_opt - entry_p:+.2f}")
                    b1, b2 = st.columns(2)
                    b1.metric("SL Index", f"{sl_idx:.0f}", help="Index level; SL fires on tick cross")
                    pnl_dir = "normal" if pnl > 0 else "inverse"
                    b2.metric(
                        "MTM",
                        f"{'+'if pnl>0 else ''}₹{pnl:,.0f}",
                        delta_color=pnl_dir,
                        help=f"Qty: {qty} | Entered: {since}",
                    )
                    st.caption(f"Exit: HA reversal or SL  |  1 lot ({lot_size} units)")

                elif trade_done:
                    st.markdown(
                        '<div class="signal-banner-wait">'
                        f'🟡 TRADE DONE TODAY — waiting for next session'
                        '</div>',
                        unsafe_allow_html=True,
                    )
                    _render_today_trades_detail(
                        [t for t in _load_today_trades("ha_options_bot")
                         if sym.upper() in (t.get("symbol") or "").upper()]
                    )
                elif not in_window:
                    st.markdown(
                        '<div class="signal-banner-off">'
                        f'⏸ Entry window CLOSED ({entry_win} IST)'
                        '</div>',
                        unsafe_allow_html=True,
                    )
                elif ha_sig == 0:
                    st.markdown(
                        '<div class="signal-banner-off">'
                        '⚪ Warming up — no HA signal yet'
                        '</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    ha_label_full = "BULLISH → watching for PE sell" if ha_sig == 1 else "BEARISH → watching for CE sell"
                    st.markdown(
                        '<div class="signal-banner-off">'
                        f'⚪ HA {ha_label_full} | awaiting next HA flip for entry'
                        '</div>',
                        unsafe_allow_html=True,
                    )

        # ── Conditions summary ─────────────────────────────────────────────────
        st.markdown("---")
        ch1, ch2 = st.columns(2)

        all_vol_pass = all(
            instruments.get(s, {}).get("vol_filter_pass", True)
            for s in ["NIFTY", "BANKNIFTY", "SENSEX"]
        )

        with ch1:
            st.markdown("**Entry Conditions (per instrument)**")
            st.markdown(
                '<div class="condition-row">'
                '✅ HA candle flips direction (daily restart — each day independent)<br>'
                f'{_tick(in_window)} Entry window open ({entry_win} IST)<br>'
                f'{_tick(all_vol_pass)} Stage 8 vol filter: NIFTY prior-day range ≥ {vol_thresh_pct:.2f}% (SENSEX blocked on low-vol days)<br>'
                '✅ ATM premium ≥ ₹15 at entry<br>'
                '✅ SL risk ≥ 20 index points (swing high/low of previous 5 bars)<br>'
                '✅ No trade already taken today for that instrument (one trade per day)'
                '</div>',
                unsafe_allow_html=True,
            )

        with ch2:
            st.markdown("**Exit Rules**")
            st.markdown(
                '<div class="condition-row">'
                '🔄 <b>HA reversal</b>: index bar closes in opposite HA direction → BUY to close<br>'
                '🛑 <b>Swing SL</b>: index tick crosses swing HA high/low → BUY to close<br>'
                '⏰ <b>EOD exit</b>: 15:20 IST unconditional close<br>'
                '📦 <b>Sizing</b>: 1 lot flat per instrument (NIFTY 75 / BANKNIFTY 35 / SENSEX 20)<br>'
                '📊 <b>Instruments</b>: NIFTY 5min + BANKNIFTY 15min + SENSEX 5min  (max 3 concurrent)'
                '</div>',
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION — EQUITY OBI BOT — RETIRED 2026-06-19
#  Dead code — call site removed. Kept for archive reference only.
# ══════════════════════════════════════════════════════════════════════════════

def render_equity_obi_panel(ltps: dict):  # RETIRED — do not call
    state = _load(STATE_FILES["EQUITY_OBI"])

    with st.container(border=True):
        st.subheader("🔬 Equity OBI Bot  *(15-session forward experiment)*")

        with st.expander("📖 Strategy & Experiment Details"):
            st.markdown(
                '<div class="research-badge">'
                'Symbols: RELIANCE + HDFCBANK (NSE MIS equity, long AND short)<br>'
                'LONG: rolling w_OBI &gt; +20 (3 ticks) + VWMP &lt; LTP + close &gt; EMA(20) → BUY<br>'
                'SHORT: rolling w_OBI &lt; -20 (3 ticks) + VWMP &gt; LTP + close &lt; EMA(20) → SELL<br>'
                'Capital: ₹1L/trade | SL: 0.4% | Target: 0.6% | EOD: 15:20 IST<br>'
                'Dual logs: trades.csv (OBI-approved) vs ghosts.csv (OBI-blocked counterfactual)'
                '</div>',
                unsafe_allow_html=True,
            )

        if not state:
            st.warning("Bot not running — state file absent. "
                       "Check `live_trading/equity_obi/logs/equity_obi_state.json`.")
            return

        session_active = state.get("session_active", False)
        paper_mode     = state.get("paper_mode", True)
        obi_ticks      = state.get("obi_ticks", 0)
        timestamp      = state.get("timestamp", "—")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Session", "🟢 Active" if session_active else "🔴 Inactive")
        col2.metric("Mode", "📝 Paper" if paper_mode else "🔴 LIVE")
        col3.metric("OBI Ticks", f"{obi_ticks:,}")

        # ── Load Trades for Summary ───────────────────────────────────────────
        trades_df = pd.DataFrame()
        realized_pnl = 0.0
        trades_csv = EQUITY_OBI_LOGS_DIR / "trades.csv"
        if trades_csv.exists():
            try:
                trades_df = pd.read_csv(trades_csv)
                today = datetime.now().strftime("%Y-%m-%d")
                trades_df = trades_df[trades_df["date"] == today]
                if not trades_df.empty:
                    realized_pnl = trades_df["pnl_gross"].sum()
            except Exception as e:
                st.error(f"Error loading trades.csv: {e}")

        col4.metric("Today's Realized PnL", f"₹{realized_pnl:+,.2f}")
        st.caption(f"State updated: {timestamp}")

        symbols_state = state.get("symbols", {})

        # ── Table 1: Active Positions ─────────────────────────────────────────
        st.markdown("### 📥 Active Positions")
        active_rows = []
        for sym, sym_data in symbols_state.items():
            # The bot now stores positions in a list
            positions = sym_data.get("positions", [])
            for pos in positions:
                # Fuzzy LTP lookup: try base, NSE equity, and NSE index
                ltp_now = ltps.get(sym)
                if ltp_now is None:
                    ltp_now = ltps.get(f"NSE:{sym}-EQ")
                if ltp_now is None:
                    ltp_now = ltps.get(f"NSE:{sym}-INDEX")
                
                ltp_now = ltp_now or 0.0
                pnl = 0.0
                if ltp_now > 0:
                    entry = pos.get("entry_price", 0)
                    shares = pos.get("shares", 0)
                    direction = pos.get("direction", "LONG")
                    pnl = (ltp_now - entry) * shares if direction == "LONG" else (entry - ltp_now) * shares

                active_rows.append({
                    "Symbol": sym,
                    "Dir": "🟢 LONG" if pos.get("direction") == "LONG" else "🔴 SHORT",
                    "Qty": pos.get("shares"),
                    "Entry Time": pos.get("signal_time"),
                    "Entry Price": f"₹{pos.get('entry_price', 0):.2f}",
                    "SL": f"₹{pos.get('sl_price', 0):.2f}",
                    "Target": f"₹{pos.get('target_price', 0):.2f}",
                    "LTP": f"₹{ltp_now:.2f}",
                    "Unrealized PnL": pnl
                })

        if active_rows:
            adf = pd.DataFrame(active_rows)
            st.dataframe(
                adf.style.format({"Unrealized PnL": "₹{:+,.2f}"})
                .map(lambda x: "color: #00ff00" if x > 0 else ("color: #ff4b4b" if x < 0 else ""), subset=["Unrealized PnL"]),
                hide_index=True,
                width="stretch"
            )
        else:
            st.info("No active positions.")

        # ── Table 2: Closed Trades ────────────────────────────────────────────
        st.markdown("### 🏁 Today's Closed Trades")
        if not trades_df.empty:
            display_cols = ["signal_time", "symbol", "direction", "entry_price", "exit_price", "exit_reason", "pnl_gross"]
            tdf = trades_df[display_cols].copy()
            tdf.columns = ["Time", "Symbol", "Dir", "Entry", "Exit", "Reason", "PnL"]
            tdf["Dir"] = tdf["Dir"].apply(lambda x: "🟢 LONG" if x == "LONG" else "🔴 SHORT")
            
            st.dataframe(
                tdf.style.format({"Entry": "₹{:.2f}", "Exit": "₹{:.2f}", "PnL": "₹{:+,.2f}"})
                .map(lambda x: "color: #00ff00" if x > 0 else ("color: #ff4b4b" if x < 0 else ""), subset=["PnL"]),
                hide_index=True,
                width="stretch"
            )
        else:
            st.info("No trades closed yet today.")

        # ── Table 3: Ghost Tracking ───────────────────────────────────────────
        st.markdown("### 👻 Ghost Tracking (OBI-Blocked)")
        ghost_rows = []
        for sym, sym_data in symbols_state.items():
            ghost = sym_data.get("ghost")
            if ghost:
                # Fuzzy LTP lookup: try base, NSE equity, and NSE index
                ltp_now = ltps.get(sym)
                if ltp_now is None:
                    ltp_now = ltps.get(f"NSE:{sym}-EQ")
                if ltp_now is None:
                    ltp_now = ltps.get(f"NSE:{sym}-INDEX")
                
                ltp_now = ltp_now or 0.0
                pnl = 0.0
                if ltp_now > 0:
                    entry = ghost.get("entry_price", 0)
                    shares = ghost.get("shares", 0)
                    direction = ghost.get("direction", "LONG")
                    pnl = (ltp_now - entry) * shares if direction == "LONG" else (entry - ltp_now) * shares

                ghost_rows.append({
                    "Symbol": sym,
                    "Dir": "👻🟢 LONG" if ghost.get("direction") == "LONG" else "👻🔴 SHORT",
                    "Entry Time": ghost.get("signal_time"),
                    "Entry Price": f"₹{ghost.get('entry_price', 0):.2f}",
                    "SL": f"₹{ghost.get('sl_price', 0):.2f}",
                    "Target": f"₹{ghost.get('target_price', 0):.2f}",
                    "LTP": f"₹{ltp_now:.2f}",
                    "Ghost PnL": pnl
                })
        
        if ghost_rows:
            gdf = pd.DataFrame(ghost_rows)
            st.dataframe(
                gdf.style.format({"Ghost PnL": "₹{:+,.2f}"})
                .map(lambda x: "color: #aaaaaa"), # Ghosts stay greyed out style-wise
                hide_index=True,
                width="stretch"
            )
        else:
            st.caption("No ghost tracking active.")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 10 — NTS + OBI GATE BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_nts_obi_panel(ltps: dict):
    state = _load(STATE_FILES["NTS_OBI"])

    with st.container(border=True):
        st.subheader("🔬 NTS + OBI Gate Bot  *(15-session forward experiment)*")

        with st.expander("📖 Strategy & Experiment Details"):
            st.markdown(
                '<div class="research-badge">'
                'Base: NTS champion (ADX>25, RSI<50, ADX-D 7b, SL 2×, SHORT-ONLY) — OOS Sharpe +1.735, WR 66%, MC 99.2%<br>'
                'OBI Gate: weighted depth-50 imbalance of ATM CE &lt; 0 → TRADE | ≥ 0 → ghost-track (counterfactual)<br>'
                'Cannot backtest (no historical L2 data). Two parallel CSV streams compared after 15 sessions.'
                '</div>',
                unsafe_allow_html=True,
            )
            dc1, dc2 = st.columns(2)
            with dc1:
                st.markdown("**NTS Signal (5 conditions)**")
                st.markdown(
                    '<div class="condition-row">'
                    '1️⃣ Close &lt; EMA(20) — bearish trend<br>'
                    '2️⃣ ADX(14) &gt; 25 — strong momentum<br>'
                    '3️⃣ ADX accelerating (7-bar window)<br>'
                    '4️⃣ RSI(14) &lt; 50 — downward pressure<br>'
                    '5️⃣ MACD(5,13,3) line crosses below signal<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with dc2:
                st.markdown("**OBI Gate + Exit**")
                st.markdown(
                    '<div class="condition-row">'
                    '📊 Subscribe depth-50 for ATM CE via WebSocket<br>'
                    '✅ OBI &lt; 0 → net sellers on CE → TRADE<br>'
                    '🚫 OBI ≥ 0 → bid pressure → ghost-track only<br>'
                    '🛑 SL: premium rises to 2× entry<br>'
                    '⏰ EOD: 15:20 IST force-exit<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )

            st.markdown("---")
            # ── Session parameters row (live from state where available) ─────
            _win  = (state or {}).get("entry_window", "10:00–13:00")
            _vix  = (state or {}).get("vix_threshold", 22.0)
            _thr  = (state or {}).get("obi_threshold", 0.0)
            _drift = 100   # ATM re-roll trigger (hardcoded in bot)
            _warm  = 50    # OBI warmup ticks (hardcoded in bot)
            pw1, pw2, pw3, pw4, pw5 = st.columns(5)
            pw1.metric("Entry Window", _win, help="IST — signals only checked inside this window")
            pw2.metric("VIX Filter", f"≤{_vix:.0f}", help="Day skipped if INDIAVIX > threshold at 09:40")
            pw3.metric("OBI Gate", f"< {_thr:+.1f}", help="Weighted OBI of ATM CE must be below this to enter")
            pw4.metric("ATM Re-roll", f"±{_drift} pts", help="ATM CE re-resolved when spot drifts this far from morning ATM")
            pw5.metric("OBI Warmup", f"{_warm} ticks", help="OBI gate inactive until this many depth ticks received")

        if not state:
            st.error("Bot not running — state file absent. Check `nifty_trend_seller_obi/logs/nts_obi_state.json`.")
            return

        session_active = state.get("session_active", False)
        vix_ok         = state.get("vix_ok", True)
        atm_sym        = state.get("atm_symbol") or "—"
        atm_strike     = state.get("atm_strike") or "—"
        obi_now        = state.get("obi_current")
        obi_levels     = state.get("obi_n_levels", 0)
        obi_ticks      = state.get("obi_ticks_today", 0)
        obi_thresh     = state.get("obi_threshold", 0.0)
        sigs_today     = state.get("signals_today", 0)
        sigs_blocked   = state.get("signals_blocked", 0)
        sigs_taken     = sigs_today - sigs_blocked
        trade_taken    = state.get("trade_taken_today", False)
        nifty_spot     = state.get("nifty_spot", 0)
        window         = state.get("entry_window", "10:00–13:00")
        active         = state.get("active_trade")
        ghost          = state.get("ghost_trade")
        params         = state.get("params", {})

        # ── Row 1: session health ─────────────────────────────────────────────
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Session", "🟢 ACTIVE" if session_active else "⚫ IDLE")
        c2.metric("NIFTY Spot", f"{nifty_spot:,.0f}" if nifty_spot else "—")
        c3.metric("ATM Strike", str(atm_strike))
        c4.metric(
            "VIX Filter",
            "✅ OK" if vix_ok else "❌ SKIP",
            delta="≤22" if vix_ok else ">22 — day skipped",
            delta_color="normal" if vix_ok else "inverse",
        )
        c5.metric("Window", window,
                  delta="🟢 OPEN" if _entry_window_open(window) else "🔴 CLOSED",
                  delta_color="off",
                  help="IST — signals only accepted inside this window")
        c6.metric("Trade Taken", "✅ Yes" if trade_taken else "⬜ No")

        # ── Row 2: OBI live readout ───────────────────────────────────────────
        st.markdown("---")
        o1, o2, o3, o4, o5 = st.columns(5)

        if obi_now is not None:
            obi_color = "green" if obi_now < 0 else "red"
            obi_label = "🟢 BEARISH (gate passes)" if obi_now < obi_thresh else "🔴 BULLISH (gate blocks)"
            o1.metric(
                "Live OBI (weighted)",
                f"{obi_now:+.1f}",
                delta=obi_label,
                delta_color="normal" if obi_now < 0 else "inverse",
                help="Negative = net selling pressure on CE → confirms short"
            )
        else:
            o1.metric("Live OBI", "Warming up…")

        o2.metric("OBI Threshold", f"{obi_thresh:+.1f}", help="OBI must be below this to trade")
        o3.metric("Depth Levels", f"{obi_levels}/50", help="50-level depth = maximum resolution")
        o4.metric("OBI Ticks Today", f"{obi_ticks:,}", help="WebSocket depth updates received")
        o5.metric("ATM CE Symbol", atm_sym[:22] if atm_sym != "—" else "—")

        # ── Row 2b: NTS Signal Conditions ─────────────────────────────────────
        st.markdown("---")
        st.markdown("**📊 NTS Signal Conditions**")

        # Get current indicator values from state
        indicators = state.get("indicators", {})

        # Compute pass/fail for each condition
        ind_close = indicators.get("close", 0)
        ind_ema = indicators.get("ema20", 0)
        ind_adx = indicators.get("adx", 0)
        ind_rsi = indicators.get("rsi", 0)
        ind_macd = indicators.get("macd", 0)
        ind_macds = indicators.get("macds", 0)

        # Conditions check (same logic as bot)
        cond_ema = ind_close < ind_ema if ind_close and ind_ema else None  # Close < EMA
        cond_adx = ind_adx > 25 if ind_adx else None  # ADX > 25
        cond_rsi = ind_rsi < 50 if ind_rsi else None  # RSI < 50
        # MACD cross - this is computed from prev bar, so we can't show current state easily
        # Just show values

        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
        sc1.metric(
            "1️⃣ Close < EMA(20)",
            f"{ind_close:.0f} < {ind_ema:.0f}" if ind_close and ind_ema else "—",
            delta="✅ PASS" if cond_ema else "❌ FAIL" if cond_ema is not None else "—",
            delta_color="normal" if cond_ema else "inverse" if cond_ema is False else "off",
        )
        sc2.metric(
            "2️⃣ ADX > 25",
            f"{ind_adx:.1f}" if ind_adx else "—",
            delta="✅ PASS" if cond_adx else "❌ FAIL" if cond_adx is not None else "—",
            delta_color="normal" if cond_adx else "inverse" if cond_adx is False else "off",
        )
        sc3.metric(
            "3️⃣ RSI < 50",
            f"{ind_rsi:.1f}" if ind_rsi else "—",
            delta="✅ PASS" if cond_rsi else "❌ FAIL" if cond_rsi is not None else "—",
            delta_color="normal" if cond_rsi else "inverse" if cond_rsi is False else "off",
        )
        sc4.metric(
            "4️⃣ ADX Rising",
            f"{ind_adx:.1f}" if ind_adx else "—",
            help="Compare with 7 bars ago - check log for status",
        )
        sc5.metric(
            "5️⃣ MACD Cross",
            f"{ind_macd:.4f}/{ind_macds:.4f}" if ind_macd and ind_macds else "—",
            help="Check log for cross detection",
        )

        st.caption("ℹ️ Conditions 1-3 shown live. For ADX rising & MACD cross, check bot log `[SIG]` entries.")

        # ── Row 3: signal scorecard ───────────────────────────────────────────
        st.markdown("---")
        s1, s2, s3 = st.columns(3)
        s1.metric("Signals Today", sigs_today)
        s2.metric("OBI Approved", sigs_taken, help="Trades actually taken")
        s3.metric("OBI Blocked (ghosts)", sigs_blocked, help="Counterfactual stream — tracked to EOD")

        # ── Active trade ──────────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("**Active Position (paper)**")

        if active:
            sym     = active.get("symbol", "—")
            entry   = active.get("entry_premium", 0)
            sl      = active.get("sl_price", 0)
            obi_ent = active.get("obi_at_signal")
            ltp     = ltps.get(sym, 0)
            mtm_per = (entry - ltp) * NIFTY_LOT_SIZE if ltp else 0
            mtm_tot = mtm_per * active.get("lots", LOTS)

            ac1, ac2, ac3, ac4, ac5 = st.columns(5)
            ac1.metric("Symbol", sym[:22])
            ac2.metric("Entry ₹", f"{entry:.2f}")
            ac3.metric("SL ₹", f"{sl:.2f}", delta=f"{(sl/entry - 1)*100:+.1f}% of entry")
            if ltp:
                ac4.metric("LTP ₹", f"{ltp:.2f}", delta=f"{ltp - entry:+.2f}")
                color = "green" if mtm_tot >= 0 else "red"
                sign  = "+" if mtm_tot >= 0 else ""
                st.markdown(
                    f"**Gross MTM: <span style='color:{color};font-size:1.2em'>"
                    f"{sign}₹{mtm_tot:,.0f}</span>** "
                    f"(₹{mtm_per:,.0f}/lot × {active.get('lots', LOTS)} lots)",
                    unsafe_allow_html=True,
                )
            else:
                ac4.metric("LTP ₹", "Fetching…")
            if obi_ent is not None:
                ac5.metric("OBI at Entry", f"{obi_ent:+.1f}")
        else:
            st.success("No open position.")

        # ── Ghost trade (counterfactual) ──────────────────────────────────────
        if ghost:
            st.markdown("---")
            st.markdown("**Ghost Trade (OBI blocked — counterfactual tracking)**")
            g1, g2, g3, g4 = st.columns(4)
            g_sym   = ghost.get("symbol", "—")
            g_entry = ghost.get("entry_premium", 0)
            g_sl    = ghost.get("sl_price", 0)
            g_ltp   = ltps.get(g_sym, 0)
            g_pnl   = (g_entry - g_ltp) * NIFTY_LOT_SIZE * ghost.get("lots", LOTS) if g_ltp else 0

            g1.metric("Ghost Symbol", g_sym[:22])
            g2.metric("Ghost Entry ₹", f"{g_entry:.2f}")
            g3.metric("Ghost SL ₹", f"{g_sl:.2f}")
            if g_ltp:
                color = "green" if g_pnl >= 0 else "red"
                sign  = "+" if g_pnl >= 0 else ""
                g4.metric("Ghost LTP ₹", f"{g_ltp:.2f}", delta=f"{g_ltp - g_entry:+.2f}")
                st.markdown(
                    f"*Ghost gross MTM: "
                    f"<span style='color:{color}'>{sign}₹{g_pnl:,.0f}</span> "
                    f"(this is what OBI blocked — positive = gate saved a winner, "
                    f"negative = gate correctly avoided a loser)*",
                    unsafe_allow_html=True,
                )

        _render_today_trades_detail(_load_today_trades("nts_obi_bot"))

# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 11 — NIFTY MACD MAP BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_nifty_macd_map_panel(ltps: dict):
    """
    Three-tab panel for the NIFTY MACD Map Bot:
      Tab 1 — Overview (position cards + key metrics)
      Tab 2 — Strategy Flowchart (rich colour SVG)
      Tab 3 — Live Decision State (indicator snapshot + filter checklist)
    """
    state = _load(STATE_FILES["NIFTY_MACD_MAP"])

    st.markdown("## 📊 Nifty MACD Map Bot")
    st.caption(
        "MACD(5,13,3) 15-min histogram zero-cross · dist ≥ 1.5σ · delay 1 bar · "
        "1 lot MIS · SL 2× · EOD 15:15 · ALL 10 stages pass"
    )

    tab_overview, tab_flow, tab_state = st.tabs([
        "📊 Overview",
        "🗺️ Strategy Flowchart",
        "🧠 Live Decision State",
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════════
    with tab_overview:
        if not state:
            st.warning("Bot not running — state file absent. Start the bot to see live data.")
        else:
            updated_at  = state.get("updated_at", "")
            expiry      = state.get("expiry") or "—"
            bars_loaded = state.get("bars_loaded", 0)
            hist_std    = state.get("hist_std", 0.0)
            indicators  = state.get("indicators", {})
            active_pe   = state.get("active_pe")
            active_ce   = state.get("active_ce")

            nifty_raw  = indicators.get("nifty_ltp", 0)
            # Bot stores WebSocket tick in paise (×100) — divide for display
            nifty_ltp  = nifty_raw / 100.0 if nifty_raw and nifty_raw > 100_000 else nifty_raw
            bar_time   = indicators.get("bar_time", "—")
            in_window  = indicators.get("in_window", False)
            dist_ratio = indicators.get("dist_ratio", 0.0)
            ml_cur     = indicators.get("ml_cur", 0.0)
            hist_cur   = indicators.get("hist_cur", 0.0)
            hist_prev  = indicators.get("hist_prev", 0.0)
            cross_up   = indicators.get("prev_cross_up", False)
            cross_dn   = indicators.get("prev_cross_dn", False)

            # Status banner
            has_pos = bool(active_pe or active_ce)
            if has_pos:
                banner_icon, banner_msg, banner_col = "📌", "IN POSITION", "#7b61ff"
            elif in_window:
                banner_icon, banner_msg, banner_col = "🟢", "IN WINDOW — scanning for signal", "#00c875"
            else:
                banner_icon, banner_msg, banner_col = "⏸", "OUT OF WINDOW (09:30–14:00 only)", "#94a3b8"

            updated_fmt = updated_at[:19].replace("T", " ")
            st.markdown(
                f'<div style="background:{banner_col}22;border-left:4px solid {banner_col};'
                f'padding:10px 16px;border-radius:6px;margin-bottom:12px;">'
                f'<span style="font-size:1.3em">{banner_icon}</span> '
                f'<strong style="color:{banner_col};font-size:1.05em">{banner_msg}</strong>'
                f'<span style="float:right;opacity:.6;font-size:.85em">Updated {updated_fmt}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

            # Top metrics row
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("NIFTY", f"{nifty_ltp:,.1f}" if nifty_ltp else "—")
            c2.metric("Expiry", expiry)
            c3.metric("Bars Loaded", bars_loaded)
            c4.metric("Hist σ", f"{hist_std:,.0f}" if hist_std else "—")
            c5.metric("Last Bar", bar_time,
                      delta="✅ In Window" if in_window else "⏸ Out of Window",
                      delta_color="off")
            c6.metric("Window", "09:30–14:00 IST",
                      delta="🟢 OPEN" if in_window else "🔴 CLOSED",
                      delta_color="off",
                      help="Signals only accepted inside this window")

            st.markdown("---")

            # MACD indicator row
            mi1, mi2, mi3, mi4, mi5 = st.columns(5)
            mi1.metric("Histogram (cur)", f"{hist_cur:,.1f}" if hist_cur else "—",
                       delta=f"{hist_cur - hist_prev:+,.1f}" if hist_prev else None)
            mi2.metric("MACD Line", f"{ml_cur:,.1f}" if ml_cur else "—",
                       delta="Bullish" if ml_cur > 0 else ("Bearish" if ml_cur < 0 else "Flat"),
                       delta_color="off")
            mi3.metric("Dist Ratio (×σ)", f"{dist_ratio:.2f}×",
                       delta="✅ ≥ 1.5 threshold" if dist_ratio >= 1.5 else "❌ < threshold",
                       delta_color="off")
            mi4.metric("Prior Cross Up", "🟢 YES" if cross_up else "—",
                       help="Histogram crossed 0 upward on previous bar → next bar may SELL PE")
            mi5.metric("Prior Cross Dn", "🔴 YES" if cross_dn else "—",
                       help="Histogram crossed 0 downward on previous bar → next bar may SELL CE")

            st.markdown("---")

            # Active positions
            legs_with_position = [(k, v) for k, v in (
                ("PE leg (SELL PE)", active_pe),
                ("CE leg (SELL CE)", active_ce),
            ) if v]

            if legs_with_position:
                st.markdown("### 📌 Open Positions")
                for leg_label, trade in legs_with_position:
                    sym      = trade.get("symbol", "")
                    entry_p  = float(trade.get("entry_prem", 0))
                    sl_p     = float(trade.get("sl_prem", 0))
                    qty      = int(trade.get("qty", 0))
                    lot_size = trade.get("lot_size", "—")
                    since    = trade.get("entry_time", "")[:19].replace("T", " ")
                    ltp      = ltps.get(sym, entry_p)
                    pnl      = (entry_p - ltp) * qty
                    sl_dist  = ltp / entry_p if entry_p else 0

                    t1, t2, t3, t4, t5 = st.columns(5)
                    t1.metric(f"🟠 {leg_label}", sym)
                    t2.metric("Entry ₹", f"{entry_p:.2f}")
                    t3.metric("SL ₹ (2×)", f"{sl_p:.2f}")
                    t4.metric("LTP ₹", f"{ltp:.2f}", delta=f"{ltp - entry_p:+.2f}")
                    t5.metric("MTM ₹", f"₹{pnl:+,.0f}",
                              delta_color="normal" if pnl > 0 else "inverse")

                    sl_pct = min(sl_dist / 2.0 * 100, 100) if entry_p else 0
                    bar_color = "#00c875" if sl_dist < 1.5 else ("#fbbf24" if sl_dist < 1.8 else "#f87171")
                    st.markdown(
                        f'<div style="margin:4px 0 2px;font-size:.82em;color:#94a3b8">'
                        f'SL proximity ({sl_dist:.2f}× entry · trigger at 2.0×) &nbsp;·&nbsp; '
                        f'Qty {qty} ({lot_size} lot) &nbsp;·&nbsp; Entry {since}</div>'
                        f'<div style="background:#1e293b;border-radius:4px;height:7px;overflow:hidden;margin-bottom:12px">'
                        f'<div style="background:{bar_color};width:{sl_pct:.0f}%;height:100%"></div></div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("No active positions — waiting for next MACD zero-cross signal.")

            _render_today_trades_detail(_load_today_trades("nifty_macd_map_bot"))

        with st.expander("📋 Raw state"):
            st.json(state or {})

        if state and state.get("updated_at"):
            age_sec, age_label = _staleness(state["updated_at"])
            st.caption(f"State file: {age_label} · updated_at {state['updated_at'][11:19]}")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — STRATEGY FLOWCHART
    # ══════════════════════════════════════════════════════════════════════════
    with tab_flow:
        st.markdown("#### NIFTY MACD Map Bot — Execution Logic")
        st.caption(
            "How the bot decides on every completed 15-minute bar. "
            "Follow the path from Session Start to ORDER PLACED."
        )

        flowchart_html = """
<style>
  .fc-wrap { font-family: 'Inter', 'Segoe UI', sans-serif; padding: 8px 0; }
  .fc-node {
    display: inline-flex; align-items: center; justify-content: center;
    text-align: center; border-radius: 10px; font-weight: 600;
    font-size: 13px; line-height: 1.35; padding: 10px 16px;
    box-shadow: 0 2px 8px rgba(0,0,0,.25);
  }
  .fc-start  { background: #7b61ff; color: #fff; border-radius: 24px; padding: 10px 24px; }
  .fc-action { background: #1e40af; color: #dbeafe; }
  .fc-check  { background: #064e3b; color: #6ee7b7; border-radius: 50px; }
  .fc-block  { background: #7f1d1d; color: #fca5a5; }
  .fc-entry  { background: #14532d; color: #86efac; }
  .fc-exit   { background: #78350f; color: #fde68a; }
  .fc-monitor{ background: #1e3a5f; color: #93c5fd; }
  .fc-arrow  { color: #64748b; font-size: 20px; line-height: 1; user-select: none; }
  .fc-label  { font-size: 11px; color: #94a3b8; font-weight: 500; }
  .fc-row    { display: flex; align-items: center; gap: 6px; margin: 4px 0; }
  .fc-col    { display: flex; flex-direction: column; align-items: center; gap: 0; }
  .fc-split  { display: flex; gap: 16px; align-items: flex-start; justify-content: center; }
  .fc-branch { display: flex; flex-direction: column; align-items: center; gap: 4px; }
  .fc-yes    { color: #4ade80; font-size: 11px; font-weight: 700; }
  .fc-no     { color: #f87171; font-size: 11px; font-weight: 700; }
</style>

<div class="fc-wrap">

<!-- ── SESSION START ─────────────────────────────────────────────────────── -->
<div class="fc-col">

  <div class="fc-row">
    <div class="fc-node fc-start">☀️ Session Start (09:15)</div>
  </div>

  <div class="fc-arrow">↓</div>

  <div class="fc-row">
    <div class="fc-node fc-action">📚 Load 5 days of 1m history<br>
      <span style="font-weight:400;font-size:11px">Resample → 15m bars · Warm MACD(5,13,3)</span>
    </div>
  </div>

  <div class="fc-arrow">↓</div>

  <div class="fc-row">
    <div class="fc-node fc-action">🔌 Subscribe WebSocket<br>
      <span style="font-weight:400;font-size:11px">NIFTY index ticks (NSE_INDEX)</span>
    </div>
  </div>

  <div class="fc-arrow">↓</div>

<!-- ── TICK LOOP ──────────────────────────────────────────────────────────── -->

  <div class="fc-row">
    <div class="fc-node fc-action" style="background:#1e293b;color:#94a3b8">⚡ On every NIFTY tick<br>
      <span style="font-weight:400;font-size:11px">Track last known LTP · Detect 15m boundary</span>
    </div>
  </div>

  <div class="fc-arrow">↓</div>

  <div class="fc-row">
    <div class="fc-node fc-check">New 15-min bar boundary crossed?</div>
  </div>

  <div class="fc-split">
    <div class="fc-branch">
      <div class="fc-no">NO</div>
      <div class="fc-arrow">↓</div>
      <div class="fc-node fc-block" style="font-size:11px">Wait for more ticks</div>
    </div>
    <div class="fc-branch">
      <div class="fc-yes">YES — bar closed</div>
      <div class="fc-arrow">↓</div>
      <div class="fc-node fc-action">📊 Fetch 1m history · Resample to 15m<br>
        <span style="font-weight:400;font-size:11px">Recompute MACD(5,13,3) · Update hist σ (rolling)</span>
      </div>
    </div>
  </div>

  <div style="height:12px"></div>
  <div class="fc-arrow">↓ (YES path continues)</div>
  <div style="height:4px"></div>

<!-- ── POSITION MONITORING ─────────────────────────────────────────────────── -->

  <div class="fc-row">
    <div class="fc-node fc-check">Any active leg (PE or CE)?</div>
  </div>

  <div class="fc-split">

    <div class="fc-branch">
      <div class="fc-yes">YES → MONITOR</div>
      <div class="fc-arrow">↓</div>
      <div class="fc-node fc-monitor">🔍 Poll LTP for each active leg</div>
      <div class="fc-arrow">↓</div>

      <div style="display:flex;flex-direction:column;gap:6px;align-items:center">

        <div class="fc-row">
          <div class="fc-node fc-check" style="font-size:11px">Premium ≥ 2× entry?</div>
          <div style="width:6px"></div>
          <div class="fc-yes">YES</div>
          <div class="fc-arrow">→</div>
          <div class="fc-node fc-exit">🛑 EXIT<br><span style="font-size:10px">SL_2X</span></div>
        </div>

        <div class="fc-row">
          <div class="fc-node fc-check" style="font-size:11px">15:15 IST?</div>
          <div style="width:6px"></div>
          <div class="fc-yes">YES</div>
          <div class="fc-arrow">→</div>
          <div class="fc-node fc-exit">⏰ EXIT<br><span style="font-size:10px">EOD</span></div>
        </div>

        <div class="fc-no" style="margin-top:4px">ALL NO → Hold position 🕐</div>
      </div>
    </div>

    <div style="width:2px;background:#334155;min-height:200px;margin:0 8px"></div>

    <div class="fc-branch">
      <div class="fc-no">NO → SCAN FOR ENTRY</div>
      <div class="fc-arrow">↓</div>

<!-- ── ENTRY FILTERS ─────────────────────────────────────────────────────── -->

      <div style="display:flex;flex-direction:column;gap:5px;align-items:center">

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px">Time in 09:30–14:00?</div>
          <div class="fc-no">NO →</div>
          <div class="fc-node fc-block" style="font-size:11px">⏰ Outside window</div>
        </div>

        <div class="fc-arrow">↓ YES</div>

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px">MACD histogram crossed zero<br>on the <em>previous</em> bar?</div>
          <div class="fc-no">NO →</div>
          <div class="fc-node fc-block" style="font-size:11px">No cross detected</div>
        </div>

        <div class="fc-arrow">↓ YES (delay = 1 bar)</div>

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px">|hist_cur| ≥ 1.5 × rolling σ?<br>
            <span style="font-size:10px">(strength filter — avoids weak crosses)</span>
          </div>
          <div class="fc-no">NO →</div>
          <div class="fc-node fc-block" style="font-size:11px">📉 Weak signal<br>dist &lt; 1.5σ</div>
        </div>

        <div class="fc-arrow">↓ YES</div>

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px">MACD line confirms direction?<br>
            <span style="font-size:10px">Bull cross → ml &gt; 0 | Bear cross → ml &lt; 0</span>
          </div>
          <div class="fc-no">NO →</div>
          <div class="fc-node fc-block" style="font-size:11px">❌ MACD line mismatch</div>
        </div>

        <div class="fc-arrow">↓ YES</div>

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px">That leg already open today?</div>
          <div class="fc-yes">YES →</div>
          <div class="fc-node fc-block" style="font-size:11px">🔁 Max 1 per leg<br>per session</div>
        </div>

        <div class="fc-arrow">↓ NO (all filters clear)</div>

<!-- ── CROSS DIRECTION → ORDER ─────────────────────────────────────────────── -->

        <div class="fc-node fc-check" style="font-size:12px">Cross direction?</div>

        <div class="fc-split" style="gap:24px;margin-top:8px">
          <div class="fc-branch">
            <div style="color:#f87171;font-size:11px;font-weight:700">BEARISH<br>(hist ↓ through zero)</div>
            <div class="fc-arrow">↓</div>
            <div class="fc-node fc-exit" style="background:#1a3a2a;color:#86efac">
              📉 SELL ATM CE<br>
              <span style="font-size:10px;font-weight:400">MIS · 1 lot · SL 2×</span>
            </div>
          </div>
          <div class="fc-branch">
            <div style="color:#4ade80;font-size:11px;font-weight:700">BULLISH<br>(hist ↑ through zero)</div>
            <div class="fc-arrow">↓</div>
            <div class="fc-node fc-entry">
              📈 SELL ATM PE<br>
              <span style="font-size:10px;font-weight:400">MIS · 1 lot · SL 2×</span>
            </div>
          </div>
        </div>

        <div style="height:10px"></div>
        <div class="fc-node" style="background:#7b61ff22;border:1px solid #7b61ff;color:#c4b5fd;font-size:12px">
          ⚡ Exit monitor polls every 30s until SL or EOD
        </div>

      </div>
    </div>

  </div>

</div>

<!-- ── LEGEND ─────────────────────────────────────────────────────────────── -->
<div style="margin-top:24px;padding:12px 16px;background:#0f172a;border-radius:8px;
            display:flex;gap:20px;flex-wrap:wrap;font-size:11px;">
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#064e3b;margin-right:6px"></span><span style="color:#94a3b8">Decision check</span></div>
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#1e40af;margin-right:6px"></span><span style="color:#94a3b8">Action / process</span></div>
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#7f1d1d;margin-right:6px"></span><span style="color:#94a3b8">Blocked / skip</span></div>
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#14532d;margin-right:6px"></span><span style="color:#94a3b8">Entry order</span></div>
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#78350f;margin-right:6px"></span><span style="color:#94a3b8">Exit order</span></div>
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#1e3a5f;margin-right:6px"></span><span style="color:#94a3b8">Monitor loop</span></div>
</div>

</div>
"""
        st.components.v1.html(flowchart_html, height=1400, scrolling=True)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — LIVE DECISION STATE
    # ══════════════════════════════════════════════════════════════════════════
    with tab_state:
        if not state:
            st.warning("State file not found. Start the bot to see live decision data.")
        else:
            indicators  = state.get("indicators", {})
            updated_at  = state.get("updated_at", "")
            updated_fmt = updated_at[:19].replace("T", " ")
            st.caption(f"State file last written: **{updated_fmt}** · Updates on each 15-min bar close")

            col_refresh = st.columns([1, 4])[0]
            with col_refresh:
                if st.button("🔄 Refresh Now"):
                    st.rerun()

            st.markdown("---")

            # ── MACD Signal Engine ──────────────────────────────────────────
            st.markdown("### 📡 MACD Signal Engine")

            nifty_raw  = indicators.get("nifty_ltp", 0)
            nifty_disp = nifty_raw / 100.0 if nifty_raw and nifty_raw > 100_000 else nifty_raw
            hist_cur   = indicators.get("hist_cur", 0.0)
            hist_prev  = indicators.get("hist_prev", 0.0)
            hist_std   = indicators.get("hist_std", state.get("hist_std", 0.0))
            ml_cur     = indicators.get("ml_cur", 0.0)
            dist_ratio = indicators.get("dist_ratio", 0.0)
            bars_total = indicators.get("bars_total", state.get("bars_loaded", 0))
            cross_up   = indicators.get("prev_cross_up", False)
            cross_dn   = indicators.get("prev_cross_dn", False)
            in_window  = indicators.get("in_window", False)
            bar_time   = indicators.get("bar_time", "—")

            se1, se2, se3 = st.columns(3)
            se1.metric("NIFTY (index)", f"{nifty_disp:,.1f}" if nifty_disp else "—")
            se2.metric("15m Bars Loaded", f"{bars_total}",
                       delta="✅ MACD warmed" if bars_total >= 30 else "⏳ Warming…",
                       delta_color="off")
            se3.metric("Last Bar Time", bar_time)

            se4, se5, se6 = st.columns(3)
            se4.metric("Histogram (cur)", f"{hist_cur:,.1f}" if hist_cur else "—",
                       delta=f"{hist_cur - hist_prev:+,.1f}" if hist_prev else None)
            se5.metric("MACD Line", f"{ml_cur:,.1f}" if ml_cur else "—",
                       delta="▲ Bullish" if ml_cur > 0 else "▼ Bearish",
                       delta_color="normal" if ml_cur > 0 else "inverse")
            se6.metric("Rolling σ (hist)", f"{hist_std:,.1f}" if hist_std else "—")

            # Dist ratio bar
            threshold = 1.5
            pct_done  = min(dist_ratio / threshold * 100, 100) if threshold else 100
            bar_color = "#4ade80" if dist_ratio >= threshold else "#fb923c"
            st.markdown(
                f'<div style="margin:8px 0 2px;font-size:.82em;color:#64748b">'
                f'Signal strength: dist ratio {dist_ratio:.2f}× σ &nbsp;(threshold = {threshold}×)</div>'
                f'<div style="background:#1e293b;border-radius:4px;height:10px;overflow:hidden">'
                f'<div style="background:{bar_color};width:{pct_done:.0f}%;height:100%;'
                f'transition:width .4s"></div></div>'
                f'<div style="font-size:.75em;color:#64748b;margin-top:2px">'
                f'{"✅ STRONG — exceeds 1.5σ threshold" if dist_ratio >= threshold else f"⚠️ WEAK — {dist_ratio:.2f}σ is below 1.5σ threshold; signal will be skipped"}'
                f'</div>',
                unsafe_allow_html=True,
            )

            st.markdown("---")

            # ── Entry Filter Checklist ──────────────────────────────────────
            st.markdown("### 🔍 Entry Filter Checklist")
            st.caption("All filters must be GREEN for a signal to fire")

            def _frow(icon, name, ok, note=""):
                colour = "#00c875" if ok else "#f87171"
                badge  = "✅ PASS" if ok else "❌ BLOCK"
                st.markdown(
                    f'<div style="display:flex;align-items:center;padding:7px 12px;'
                    f'margin:3px 0;background:#0f172a;border-radius:7px;gap:10px;">'
                    f'<span style="font-size:1.2em">{icon}</span>'
                    f'<span style="flex:1;color:#e2e8f0;font-size:.9em">{name}</span>'
                    f'<span style="font-size:.8em;color:#64748b">{note}</span>'
                    f'<span style="background:{colour}22;color:{colour};font-size:.75em;'
                    f'font-weight:700;padding:2px 8px;border-radius:4px">{badge}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            _frow("⏰", "Entry window (09:30–14:00 IST)", in_window,
                  f"Last bar: {bar_time}")
            _frow("📊", "MACD histogram zero-cross on previous bar",
                  cross_up or cross_dn,
                  "🟢 Bull cross (→sell PE)" if cross_up else ("🔴 Bear cross (→sell CE)" if cross_dn else "No cross yet"))
            _frow("📏", f"Signal strength ≥ 1.5σ (dist = {dist_ratio:.2f}×)",
                  dist_ratio >= threshold,
                  f"hist={hist_cur:,.0f} vs σ={hist_std:,.0f}")
            _frow("🧭", "MACD line confirms direction",
                  (cross_up and ml_cur > 0) or (cross_dn and ml_cur < 0) or (not cross_up and not cross_dn),
                  f"ml={ml_cur:,.0f} · {'OK' if ml_cur != 0 else 'Flat'}")
            _frow("📅", "Expiry resolved (DTE ≥ 2)",
                  bool(state.get("expiry")),
                  state.get("expiry") or "Not resolved")
            _frow("🔁", "PE leg slot free",
                  not bool(state.get("active_pe")),
                  "Open" if not state.get("active_pe") else "Already has position today")
            _frow("🔁", "CE leg slot free",
                  not bool(state.get("active_ce")),
                  "Open" if not state.get("active_ce") else "Already has position today")

            st.markdown("---")

            # ── Overall Verdict ─────────────────────────────────────────────
            st.markdown("### 🎯 Signal Readiness")
            has_pos = bool(state.get("active_pe") or state.get("active_ce"))
            if has_pos:
                icon_, msg_, col_ = "📌", "IN POSITION — monitoring open leg(s) for SL or EOD exit", "#7b61ff"
            elif not in_window:
                icon_, msg_, col_ = "⏸", f"OUT OF WINDOW — last bar {bar_time} · signals resume at 09:30", "#94a3b8"
            elif (cross_up or cross_dn) and dist_ratio >= threshold:
                icon_, msg_, col_ = "🟢", "SIGNAL PENDING — cross detected + strength confirmed · order will fire next bar", "#00c875"
            elif cross_up or cross_dn:
                icon_, msg_, col_ = "⚠️", f"CROSS DETECTED but strength too low ({dist_ratio:.2f}σ < 1.5σ) — waiting", "#fbbf24"
            else:
                icon_, msg_, col_ = "🔍", "SCANNING — in window, no cross yet", "#60a5fa"

            st.markdown(
                f'<div style="background:{col_}22;border:1.5px solid {col_};'
                f'border-radius:10px;padding:16px 20px;font-size:1em;">'
                f'<span style="font-size:1.5em">{icon_}</span> '
                f'<strong style="color:{col_}">{msg_}</strong>'
                f'</div>',
                unsafe_allow_html=True,
            )


# (Tick Stasher page removed 2026-04-27 — tick_stasher.py retired; no active bot
#  reads live_ticks.duckdb. Only retired candle_breaker_bot consumed it.)


def render_nifty_eod_hold_panel(ltps: dict):
    state = _load(STATE_FILES["NIFTY_EOD_HOLD"])

    with st.container(border=True):
        st.subheader("📊 NIFTY EOD Hold Bot")

        if not state:
            st.error("Bot not running — state file absent.")
            return

        updated_at   = state.get("updated_at", "")
        expiry       = state.get("expiry") or "—"
        bars_loaded  = state.get("bars_loaded", 0)
        indicators   = state.get("indicators", {})
        active_trade = state.get("active_trade")
        signal_taken = state.get("signal_taken", False)
        vix_skip     = state.get("vix_skip", False)
        vix_ltp      = state.get("vix_ltp", 0.0)
        pending      = state.get("pending_entries", [])

        # ── Strategy & Research expander ──────────────────────────────────────
        with st.expander("📖 Strategy & Research Details"):
            st.markdown(
                '<div class="research-badge">'
                'ADX≥25 + MACD/hist slope + hammer/shooting-star + EMA-20 — 1-min NIFTY bars  |  '
                'Signal window: 09:15–09:44  |  DELAY=1 bar entry  |  '
                'macd_slope OOS Sharpe +2.867 (WR 73.3%, 30 trades)  |  '
                'hist_slope OOS Sharpe +2.353 (WR 75.0%, 24 trades)  |  '
                'No SL — hold unconditionally to 15:29 EOD  |  VIX soft filter ≥17  |  '
                '10 lots × lot_size=65  |  10/10 stages PASS'
                '</div>',
                unsafe_allow_html=True,
            )
            ec1, ec2 = st.columns(2)
            with ec1:
                st.markdown("**Entry Conditions**")
                st.markdown(
                    '<div class="condition-row">'
                    'ADX(14) ≥ 25<br>'
                    'Combo 1 (macd_slope): MACD-line direction bullish/bearish<br>'
                    'Combo 2 (hist_slope): histogram direction bullish/bearish<br>'
                    'Bearish + shooting-star + close > EMA-20 → <b>SELL ATM CE</b><br>'
                    'Bullish + hammer + close &lt; EMA-20 → <b>SELL ATM PE</b>'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with ec2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '⏰ <b>EOD exit</b>: 15:29 IST — unconditional close<br>'
                    '🚫 <b>No stop-loss</b> — full session hold<br>'
                    '🌡️ <b>VIX filter</b>: skip session if INDIAVIX ≥ 17 at open<br>'
                    '✅ One signal per session (first wins, both combos evaluated)'
                    '</div>',
                    unsafe_allow_html=True,
                )

        # ── Top metrics row ───────────────────────────────────────────────────
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        nifty_ltp  = indicators.get("nifty_ltp", 0)
        bar_time   = indicators.get("bar_time", "—")
        in_window  = indicators.get("in_window", False)

        c1.metric("NIFTY", f"{nifty_ltp:,.2f}" if nifty_ltp else "—")
        c2.metric("INDIAVIX", f"{vix_ltp:.2f}" if vix_ltp else "—",
                  delta="⚠️ SKIP SESSION" if vix_skip else "✅ OK",
                  delta_color="off")
        c3.metric("Expiry", expiry)
        c4.metric("Bars Loaded", bars_loaded)
        c5.metric("Last Bar", bar_time,
                  delta="✅ In Window" if in_window else "⏸ Out of Window",
                  delta_color="off")
        c6.metric("Window", "09:15–09:44 IST",
                  delta="🟢 OPEN" if in_window else "🔴 CLOSED",
                  delta_color="off",
                  help="IST — signal window for 1-min opening range reversal")

        st.markdown("---")

        # ── Indicator row ─────────────────────────────────────────────────────
        i1, i2, i3, i4, i5 = st.columns(5)
        adx_v      = indicators.get("adx", 0.0)
        ema20_v    = indicators.get("ema20", 0.0)
        macd_v     = indicators.get("macd", 0.0)
        hist_v     = indicators.get("hist", 0.0)
        prev_macd  = indicators.get("prev_macd", 0.0)
        prev_hist  = indicators.get("prev_hist", 0.0)

        i1.metric("ADX(14)", f"{adx_v:.1f}" if adx_v else "—",
                  delta="✅ ≥ 25" if adx_v >= 25 else "❌ < 25",
                  delta_color="off")
        i2.metric("EMA(20)", f"{ema20_v:,.2f}" if ema20_v else "—")
        i3.metric("MACD Line", f"{macd_v:.4f}" if macd_v else "—",
                  delta=f"{macd_v - prev_macd:+.4f}" if prev_macd else None)
        i4.metric("Histogram", f"{hist_v:.4f}" if hist_v else "—",
                  delta=f"{hist_v - prev_hist:+.4f}" if prev_hist else None)
        i5.metric("Signal Taken", "🟢 YES" if signal_taken else "⬜ NO",
                  delta=f"Pending: {pending}" if pending else None,
                  delta_color="off")

        st.markdown("---")

        # ── Active position ────────────────────────────────────────────────────
        if active_trade:
            sym       = active_trade.get("symbol", "")
            opt_type  = active_trade.get("opt_type", "")
            entry_p   = float(active_trade.get("entry_prem", 0))
            qty       = int(active_trade.get("qty", 0))
            lot_size  = active_trade.get("lot_size", "—")
            since     = active_trade.get("entry_time", "")[:19].replace("T", " ")
            ltp       = ltps.get(sym, entry_p)
            pnl       = (entry_p - ltp) * qty   # seller: decay = profit

            t1, t2, t3, t4, t5 = st.columns(5)
            t1.metric(f"🟠 SELL {opt_type}", sym)
            t2.metric("Entry ₹", f"{entry_p:.2f}")
            t3.metric("LTP ₹", f"{ltp:.2f}", delta=f"{ltp - entry_p:+.2f}")
            t4.metric("MTM ₹", f"₹{pnl:,.0f}",
                      delta_color="normal" if pnl > 0 else "inverse")
            t5.metric(f"Qty ({lot_size} lot)", qty,
                      delta=f"Entry {since}", delta_color="off")
        else:
            if vix_skip:
                st.warning("⚠️ Session skipped — INDIAVIX ≥ 17 at open.")
            elif signal_taken:
                st.success("✅ Position closed (EOD exit completed).")
                _render_today_trades_detail(_load_today_trades("nifty_eod_hold_bot"))
            else:
                st.info("No active position — waiting for 09:15–09:44 reversal signal.")

        # ── Staleness footer ───────────────────────────────────────────────────
        if updated_at:
            age_sec, age_label = _staleness(updated_at)
            st.caption(f"State file: {age_label} · updated_at {updated_at[11:19]}")









# ══════════════════════════════════════════════════════════════════════════════
#  NIFTY IRON FLY WEEKLY BOT PANEL
# ══════════════════════════════════════════════════════════════════════════════

def render_iron_fly_panel(ltps: dict):
    """Dashboard panel for the NIFTY Weekly Short Iron Fly bot."""
    state = _load(STATE_FILES["IRON_FLY_WEEKLY"])

    with st.container(border=True):
        st.subheader("🦋 NIFTY Iron Fly Weekly Bot")

        with st.expander("📖 Strategy & Research Details"):
            st.markdown(
                '<div class="research-badge">'
                'Research: Short Iron Fly — sell ATM CE+PE straddle, buy OTM CE+PE hedges (Δ≈0.10) &nbsp;|&nbsp; '
                'Champion C3: entry Wed 10:00 IST, VIX≥12 + NIFTY≥MA20 &nbsp;|&nbsp; '
                'IS Sharpe +2.34 · OOS +1.87 &nbsp;|&nbsp; WR 65.2% OOS &nbsp;|&nbsp; '
                'SL ₹20,000 combined · Exit Mon 15:15 &nbsp;|&nbsp; 10 lots NRML &nbsp;|&nbsp; 10/10 stages ✅'
                '</div>',
                unsafe_allow_html=True,
            )
            ec1, ec2 = st.columns(2)
            with ec1:
                st.markdown("**Entry Conditions (Wed 10:00 IST)**")
                st.markdown(
                    '<div class="condition-row">'
                    '📅 Entry day: Wednesday (Tue expiry − 6 days)<br>'
                    '✅ INDIAVIX ≥ 12 at entry time<br>'
                    '✅ NIFTY spot ≥ 20-day MA (trend filter)<br>'
                    '📌 Sell ATM CE + ATM PE (straddle)<br>'
                    '📌 Buy OTM CE + OTM PE (hedge, delta≈0.10)<br>'
                    '📌 Product: NRML — held 3–4 days overnight'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with ec2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🛑 SL: Combined MTM ≤ −₹20,000 (₹2,000/lot × 10 lots)<br>'
                    '⏰ Scheduled exit: Monday 15:15 IST (day before Tuesday expiry)<br>'
                    '📋 Order sequence (close): BUY sell_ce → BUY sell_pe → SELL buy_ce → SELL buy_pe<br>'
                    '📋 Order sequence (entry): BUY buy_ce → BUY buy_pe → SELL sell_ce → SELL sell_pe'
                    '</div>',
                    unsafe_allow_html=True,
                )

        if not state:
            st.warning(
                "State file absent — bot is either not running or has not entered yet. "
                f"Check `live_trading/logs/iron_fly_weekly_state.json`."
            )
            return

        closed      = state.get("closed", False)
        paper_mode  = state.get("paper_mode", True)
        trade_date  = state.get("trade_date", "—")
        entry_time  = (state.get("entry_time") or "")[:16].replace("T", " ")
        expiry_str  = state.get("expiry_str", "—")
        exit_date   = state.get("exit_date", "—")
        atm_strike  = state.get("atm_strike", 0)
        vix_entry   = state.get("vix_at_entry", 0)
        ma20_entry  = state.get("ma20_at_entry", 0)
        net_cr_unit = state.get("net_credit_per_unit", 0)
        qty         = int(state.get("qty", 0))
        lot_size    = int(state.get("lot_size", 65))
        n_lots      = int(state.get("n_lots", 10))
        sl_total    = float(state.get("stop_loss_total", 20000))
        current_mtm = float(state.get("current_mtm", 0))
        sl_hit      = state.get("sl_hit", False)
        exit_reason = state.get("exit_reason")
        last_update = state.get("last_update", "")
        legs        = state.get("legs", {})

        # ── Header: status banner ──────────────────────────────────────────────
        if closed:
            rsn = exit_reason or "exited"
            if sl_hit:
                st.error(f"🛑 Position CLOSED — SL hit ({rsn})")
            else:
                st.success(f"✅ Position CLOSED — {rsn}")
        elif legs:
            mode_tag = "📝 PAPER" if paper_mode else "🔴 LIVE"
            st.info(f"{mode_tag} · Active 4-leg iron fly · ATM {atm_strike} · Expiry {expiry_str}")
        else:
            st.info(f"💤 Waiting for entry signal (next Wed 10:00 IST) · Expiry {expiry_str or '—'}")

        # ── State freshness ────────────────────────────────────────────────────
        if last_update:
            age_sec, age_label = _staleness(last_update)
            st.caption(f"State: {age_label}  ·  trade_date {trade_date}  ·  qty {qty} ({n_lots} lots × {lot_size})")

        if not legs:
            return   # no position yet — nothing more to show

        # ── Entry context metrics ──────────────────────────────────────────────
        st.markdown("---")
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("ATM Strike",       f"{atm_strike}")
        m2.metric("Expiry",           expiry_str)
        m3.metric("VIX at Entry",     f"{vix_entry:.2f}")
        m4.metric("MA20 at Entry",    f"₹{ma20_entry:,.0f}")
        m5.metric("Net Credit/unit",  f"₹{net_cr_unit:.1f}")
        m6.metric("Combined SL",      f"₹{sl_total:,.0f}")

        # ── Live MTM card ──────────────────────────────────────────────────────
        st.markdown("---")
        mtm_color = "#00e599" if current_mtm >= 0 else "#f87171"
        mtm_bg    = "#031a0c"  if current_mtm >= 0 else "#1a0505"
        mtm_sign  = "+" if current_mtm >= 0 else ""
        sl_pct    = abs(current_mtm) / sl_total * 100 if sl_total else 0
        st.markdown(
            f'<div style="padding:14px 20px;border-radius:10px;'
            f'background:{mtm_bg};border:1px solid {mtm_color};border-left:4px solid {mtm_color};">'
            f'<span style="font-size:0.8em;font-weight:700;text-transform:uppercase;letter-spacing:0.07em;color:#3f5a80;">Combined MTM (all 4 legs)</span>'
            f'<br><span style="font-size:1.55em;font-weight:700;color:{mtm_color};">'
            f'{mtm_sign}₹{current_mtm:,.0f}</span>'
            f'&nbsp;&nbsp;<span style="font-size:0.85em;color:#3f5a80;">'
            f'({sl_pct:.0f}% of SL ₹{sl_total:,.0f})</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        if not closed:
            sl_remaining = sl_total + current_mtm   # how much room left before SL fires
            st.progress(
                min(1.0, max(0.0, sl_pct / 100)),
                text=f"SL usage {sl_pct:.0f}%  ·  Room remaining: ₹{sl_remaining:,.0f}"
            )

        # ── 4-leg table ────────────────────────────────────────────────────────
        st.markdown("#### 📋 Position Legs")
        leg_rows = []
        for leg_key, display_action, display_type in (
            ("sell_ce", "SELL", "CE 🔴"),
            ("sell_pe", "SELL", "PE 🔴"),
            ("buy_ce",  "BUY",  "CE 🟢"),
            ("buy_pe",  "BUY",  "PE 🟢"),
        ):
            leg = legs.get(leg_key, {})
            if not leg:
                continue
            sym         = leg.get("symbol", "—")
            strike      = int(leg.get("strike", 0))
            entry_prem  = float(leg.get("entry_prem", 0))
            ltp         = ltps.get(sym, 0.0) or entry_prem
            if "sell" in leg_key:
                leg_mtm = (entry_prem - ltp) * qty
            else:
                leg_mtm = (ltp - entry_prem) * qty
            leg_rows.append({
                "Leg":     display_type,
                "Action":  display_action,
                "Strike":  strike,
                "Symbol":  sym,
                "Entry ₹": entry_prem,
                "LTP ₹":   ltp,
                "Leg MTM ₹": leg_mtm,
            })

        if leg_rows:
            df_legs = pd.DataFrame(leg_rows)
            def _color_leg_pnl(v):
                return "color:#00e599;font-weight:700" if v > 0 else (
                       "color:#f87171;font-weight:700" if v < 0 else "color:#5a7ba0")
            styled = (
                df_legs.style
                .map(_color_leg_pnl, subset=["Leg MTM ₹"])
                .format({
                    "Entry ₹":   lambda v: f"₹{v:.2f}",
                    "LTP ₹":     lambda v: f"₹{v:.2f}",
                    "Leg MTM ₹": lambda v: f"{'+'if v>0 else ''}₹{v:,.0f}",
                })
            )
            st.dataframe(styled, width="stretch", hide_index=True)

        # ── Exit schedule ──────────────────────────────────────────────────────
        if not closed:
            st.markdown("---")
            st.caption(
                f"📅 Scheduled exit: **{exit_date} 15:15 IST** (Monday before {expiry_str} expiry)  ·  "
                f"Entry: {entry_time or '—'}"
            )

        _render_today_trades_detail(
            _load_today_trades("nifty_iron_fly_bot"), compact=True
        )


# ══════════════════════════════════════════════════════════════════════════════
#  SENSEX IRON FLY WEEKLY BOT PANEL
# ══════════════════════════════════════════════════════════════════════════════

def render_sensex_iron_fly_panel(ltps: dict):
    """Dashboard panel for the SENSEX Weekly Short Iron Fly bot."""
    state = _load(STATE_FILES["SENSEX_IRON_FLY_WEEKLY"])

    with st.container(border=True):
        st.subheader("🦋 SENSEX Iron Fly Weekly Bot")

        with st.expander("📖 Strategy & Research Details"):
            st.markdown(
                '<div class="research-badge">'
                'Research: Short Iron Fly — sell ATM CE+PE straddle, buy OTM CE+PE hedges (Δ≈0.10) &nbsp;|&nbsp; '
                'Champion C4: entry Fri 10:00 IST, SENSEX≥MA20 (no VIX filter) &nbsp;|&nbsp; '
                'IS Sharpe +4.95 · OOS +4.227 &nbsp;|&nbsp; WR 81.0% OOS &nbsp;|&nbsp; '
                'PT 50% of net credit · Exit Wed 15:15 (day-before-expiry) &nbsp;|&nbsp; '
                '1 lot (20 units) NRML BFO &nbsp;|&nbsp; 10/10 stages ✅'
                '</div>',
                unsafe_allow_html=True,
            )
            ec1, ec2 = st.columns(2)
            with ec1:
                st.markdown("**Entry Conditions (Fri 10:00 IST)**")
                st.markdown(
                    '<div class="condition-row">'
                    '📅 Entry day: Friday (Thursday expiry − 6 days)<br>'
                    '✅ SENSEX spot ≥ 20-day MA (sole regime filter)<br>'
                    '🚫 No VIX filter (Stage 8: VIX≤14 was overfitting)<br>'
                    '📌 Sell ATM CE + ATM PE (straddle) · strike step 100<br>'
                    '📌 Buy OTM CE + OTM PE (hedge, delta≈0.10 via Black-76)<br>'
                    '📌 Product: NRML BFO — held 5–6 days overnight'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with ec2:
                st.markdown("**Exit Rules & Adjustments**")
                st.markdown(
                    '<div class="condition-row">'
                    '🎯 Profit target: 50% of net credit collected → exit all legs<br>'
                    '⏰ Scheduled exit: Wednesday 15:15 IST (day before Thursday expiry)<br>'
                    '⚙️ Adjustment: if short leg |delta| exits [0.20–0.70] for 2 polls → '
                    'close that leg + re-open at current ATM (buy hedges unchanged)<br>'
                    '🛑 No stop-loss — PT and adjustments manage risk'
                    '</div>',
                    unsafe_allow_html=True,
                )

        if not state:
            st.warning(
                "State file absent — bot is either not running or has not entered yet. "
                "Check `live_trading/logs/sensex_iron_fly_weekly_state.json`."
            )
            return

        closed      = state.get("closed", False)
        paper_mode  = state.get("paper_mode", True)
        trade_date  = state.get("trade_date", "—")
        entry_time  = (state.get("entry_time") or "")[:16].replace("T", " ")
        expiry_str  = state.get("expiry_str", "—")
        exit_day    = state.get("exit_day", "—")
        atm_strike  = state.get("atm_strike", 0)
        vix_entry   = state.get("vix_at_entry", 0)
        ma20_entry  = state.get("ma20_at_entry", 0)
        spot_entry  = state.get("spot_at_entry", 0)
        net_cr_unit = float(state.get("net_credit_per_unit", 0))
        premium     = float(state.get("premium_collected", 0))
        qty         = int(state.get("qty", 0))
        lot_size    = int(state.get("lot_size", 20))
        n_lots      = int(state.get("n_lots", 1))
        current_mtm = float(state.get("current_mtm", 0))
        exit_reason = state.get("exit_reason")
        n_adj       = int(state.get("n_adjustments", 0))
        last_update = state.get("last_update", "")
        legs        = state.get("legs", {})

        # ── Header: status banner ──────────────────────────────────────────────
        if closed:
            rsn = exit_reason or "exited"
            emoji = "🎯" if rsn == "profit_target" else "✅"
            st.success(f"{emoji} Position CLOSED — {rsn}")
        elif legs:
            mode_tag = "📝 PAPER" if paper_mode else "🔴 LIVE"
            st.info(
                f"{mode_tag} · Active 4-leg iron fly · ATM {atm_strike} · "
                f"Expiry {expiry_str} · Spot {spot_entry:.0f}"
            )
        else:
            st.info(f"💤 Waiting for entry signal (next Fri 10:00 IST) · Expiry {expiry_str or '—'}")

        # ── State freshness ────────────────────────────────────────────────────
        if last_update:
            age_sec, age_label = _staleness(last_update)
            st.caption(
                f"State: {age_label}  ·  trade_date {trade_date}  ·  "
                f"qty {qty} ({n_lots} lot × {lot_size} units)"
            )

        if not legs:
            return   # no position yet — nothing more to show

        # ── Entry context metrics ──────────────────────────────────────────────
        st.markdown("---")
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("ATM Strike",      f"{atm_strike}")
        m2.metric("Expiry",          expiry_str)
        m3.metric("VIX at Entry",    f"{vix_entry:.2f}")
        m4.metric("MA20 at Entry",   f"₹{ma20_entry:,.0f}" if ma20_entry else "—")
        m5.metric("Net Credit/unit", f"₹{net_cr_unit:.1f}")
        m6.metric("Adjustments",     n_adj)

        # ── Live MTM + PT progress ─────────────────────────────────────────────
        st.markdown("---")
        pt_target   = premium * 0.5
        mtm_color   = "#00e599" if current_mtm >= 0 else "#f87171"
        mtm_bg      = "#031a0c"  if current_mtm >= 0 else "#1a0505"
        mtm_sign    = "+" if current_mtm >= 0 else ""
        st.markdown(
            f'<div style="padding:14px 20px;border-radius:10px;'
            f'background:{mtm_bg};border:1px solid {mtm_color};border-left:4px solid {mtm_color};">'
            f'<span style="font-size:0.8em;font-weight:700;text-transform:uppercase;letter-spacing:0.07em;color:#3f5a80;">Combined MTM (all 4 legs)</span>'
            f'<br><span style="font-size:1.55em;font-weight:700;color:{mtm_color};">'
            f'{mtm_sign}₹{current_mtm:,.0f}</span>'
            f'&nbsp;&nbsp;<span style="font-size:0.85em;color:#3f5a80;">'
            f'PT target: ₹{pt_target:,.0f} (50% of ₹{premium:,.0f})</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

        if not closed and premium > 0:
            pt_pct = min(max(current_mtm / pt_target, 0.0), 1.0) if pt_target > 0 else 0.0
            st.progress(
                pt_pct,
                text=f"PT progress {pt_pct*100:.0f}%  ·  MTM ₹{current_mtm:+,.0f} / PT ₹{pt_target:,.0f}"
            )

        # ── 4-leg table ────────────────────────────────────────────────────────
        st.markdown("#### 📋 Position Legs")
        leg_rows = []
        for leg_key, display_action, display_type in (
            ("sell_ce", "SELL", "CE 🔴"),
            ("sell_pe", "SELL", "PE 🔴"),
            ("buy_ce",  "BUY",  "CE 🟢"),
            ("buy_pe",  "BUY",  "PE 🟢"),
        ):
            leg = legs.get(leg_key, {})
            if not leg:
                continue
            sym        = leg.get("symbol", "—")
            strike     = int(leg.get("strike", 0))
            entry_prem = float(leg.get("entry_prem", 0))
            ltp        = ltps.get(sym, 0.0) or entry_prem
            if "sell" in leg_key:
                leg_mtm = (entry_prem - ltp) * qty
            else:
                leg_mtm = (ltp - entry_prem) * qty
            leg_rows.append({
                "Leg":       display_type,
                "Action":    display_action,
                "Strike":    strike,
                "Symbol":    sym,
                "Entry ₹":  entry_prem,
                "LTP ₹":    ltp,
                "Leg MTM ₹": leg_mtm,
            })

        if leg_rows:
            df_legs = pd.DataFrame(leg_rows)
            def _color_leg_pnl(v):
                return "color:#00e599;font-weight:700" if v > 0 else (
                       "color:#f87171;font-weight:700" if v < 0 else "color:#5a7ba0")
            styled = (
                df_legs.style
                .map(_color_leg_pnl, subset=["Leg MTM ₹"])
                .format({
                    "Entry ₹":   lambda v: f"₹{v:.2f}",
                    "LTP ₹":     lambda v: f"₹{v:.2f}",
                    "Leg MTM ₹": lambda v: f"{'+'if v>0 else ''}₹{v:,.0f}",
                })
            )
            st.dataframe(styled, width="stretch", hide_index=True)

        # ── Closed trade summary ───────────────────────────────────────────────
        if closed:
            total_pnl = float(state.get("total_pnl", current_mtm))
            color = "green" if total_pnl >= 0 else "red"
            st.markdown(
                f"**Final P&L: <span style='color:{color}'>₹{total_pnl:+,.2f}</span>**"
                f"  (exit reason: {exit_reason or '—'})",
                unsafe_allow_html=True,
            )

        # ── Exit schedule ──────────────────────────────────────────────────────
        if not closed:
            st.markdown("---")
            st.caption(
                f"📅 Scheduled exit: **{exit_day} 15:15 IST** (Wed before {expiry_str} expiry)  ·  "
                f"Entry: {entry_time or '—'}"
            )

        _render_today_trades_detail(
            _load_today_trades("sensex_iron_fly_bot"), compact=True
        )

        # ── Raw state ─────────────────────────────────────────────────────────
        with st.expander("📋 Raw state"):
            st.json(state)


# ══════════════════════════════════════════════════════════════════════════════
#  FLAT BLUE LINE MONTHLY BOT PANEL
# ══════════════════════════════════════════════════════════════════════════════

def render_flat_blue_line_monthly_panel(ltps: dict):
    """Dashboard panel for the Flat Blue Line Monthly (Double Fly) bot."""
    full_state = _load(STATE_FILES["FLAT_BLUE_LINE_MONTHLY"])

    with st.container(border=True):
        st.subheader("🔵 Flat Blue Line Monthly Bot")

        with st.expander("📖 Strategy & Research Details"):
            st.markdown(
                '<div class="research-badge">'
                'Research: 6-leg Double Fly (long ATM straddle + short strangle ×2 + long wings Δ≈0.10) &nbsp;|&nbsp; '
                'NIFTY N_C=3/N_P=2: OOS Sharpe +1.77, WR 71% &nbsp;|&nbsp; '
                'BN N_C=1/N_P=3: OOS Sharpe +2.86, WR 85% &nbsp;|&nbsp; '
                'ALL 10 stages ✅'
                '</div>',
                unsafe_allow_html=True,
            )
            ec1, ec2 = st.columns(2)
            with ec1:
                st.markdown("**Entry (10:00 IST, first tday after prior monthly expiry)**")
                st.markdown(
                    '<div class="condition-row">'
                    '📅 Entry: 10:00 IST · first trading day after prior monthly expiry<br>'
                    '📌 IV filter: ATM Call Black-76 IV ≥ 14% required<br>'
                    '📌 Leg 1+2: BUY 5× ATM Call + 5× ATM Put (long straddle)<br>'
                    '📌 Leg 3+4: BUY N_C×5 OTM Call hedge + N_P×5 OTM Put hedge (Δ≈0.10)<br>'
                    '📌 Leg 5+6: SELL 10× OTM Call (ATM+D) + 10× OTM Put (ATM−D)<br>'
                    '📌 D = round((ATM_C + ATM_P) / 2, strike_step) &nbsp;|&nbsp; Product: NRML'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with ec2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🎯 Exit 1 — Profit target: MTM ≥ ₹22,500 (NIFTY) / dynamic (BN)<br>'
                    '🛑 Exit 2 — Breakeven stop: spot crosses BE_L or BE_U (expiry payoff)<br>'
                    '⏰ Exit 3 — Pre-expiry: 3 trading days before expiry at 15:15 IST<br>'
                    '📌 No mid-trade adjustments — enter and monitor only'
                    '</div>',
                    unsafe_allow_html=True,
                )

        if not full_state:
            st.warning(
                "State file absent — bot is either not running or has not entered yet this month. "
                "Check `live_trading/logs/flat_blue_line_monthly_state.json`."
            )
            return

        # ── Per-instrument display ─────────────────────────────────────────────
        for inst in ("NIFTY", "BANKNIFTY"):
            state = full_state.get(inst, {})
            closed = state.get("closed", True)

            st.markdown(f"#### {inst}")

            if closed:
                reason = state.get("exit_reason", "")
                month  = state.get("month_key", "—")
                if reason == "low_vol":
                    st.info(f"📵 {month}: Skipped — IV < 14% at entry.")
                elif reason == "entry_window_expired":
                    st.info(f"⏩ {month}: Entry window expired — waiting for next cycle.")
                elif reason in ("target", "be_stop", "pre_expiry", "forced_expiry"):
                    pnl = state.get("total_pnl", 0)
                    emoji = "🎯" if reason == "target" else ("🛑" if reason == "be_stop" else "⏰")
                    st.success(f"{emoji} {month}: Exited ({reason}) · P&L ₹{pnl:,.0f}")
                else:
                    st.info(f"⏳ {inst}: No open position — awaiting next entry day.")
                continue

            # ── Active position ────────────────────────────────────────────────
            entry_time  = (state.get("entry_time") or "")[:16].replace("T", " ")
            expiry_str  = state.get("expiry_str", "—")
            pre_exit    = state.get("pre_exit_date", "—")
            atm         = int(state.get("atm_strike", 0))
            sc_k        = int(state.get("sc_strike", 0))
            sp_k        = int(state.get("sp_strike", 0))
            hc_k        = int(state.get("hc_strike", 0))
            hp_k        = int(state.get("hp_strike", 0))
            n_c         = int(state.get("n_c", 0))
            n_p         = int(state.get("n_p", 0))
            be_lo       = float(state.get("be_lower", 0))
            be_hi       = float(state.get("be_upper", 0))
            tgt         = float(state.get("profit_target", 0))
            straddle    = float(state.get("straddle_pts", 0))
            lot_size    = int(state.get("lot_size", 0))
            current_mtm = float(state.get("current_mtm", 0))
            current_spot = float(state.get("current_spot", 0))
            legs        = state.get("legs", {})

            # Live MTM from ltps if available
            def _leg_ltp(key):
                sym = legs.get(key, {}).get("symbol", "")
                return ltps.get(sym, legs.get(key, {}).get("entry_prem", 0))

            if ltps and legs:
                unit = lot_size * 5
                live_mtm = unit * (
                    (_leg_ltp("atm_c") - legs["atm_c"]["entry_prem"])
                    + (_leg_ltp("atm_p") - legs["atm_p"]["entry_prem"])
                    + 2 * (legs["sc"]["entry_prem"] - _leg_ltp("sc"))
                    + 2 * (legs["sp"]["entry_prem"] - _leg_ltp("sp"))
                    + n_c * (_leg_ltp("hc") - legs["hc"]["entry_prem"])
                    + n_p * (_leg_ltp("hp") - legs["hp"]["entry_prem"])
                ) if all(k in legs for k in ("atm_c","atm_p","sc","sp","hc","hp")) else current_mtm
            else:
                live_mtm = current_mtm

            mtm_color = "#00e599" if live_mtm >= 0 else "#f87171"

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("MTM (gross)", f"₹{live_mtm:+,.0f}", delta=None)
            m2.metric("Target", f"₹{tgt:,.0f}")
            m3.metric("Spot", f"{current_spot:.0f}" if current_spot else "—")
            m4.metric("Expiry", expiry_str)

            st.markdown(
                f'<div class="condition-row">'
                f'📅 Entry: {entry_time} &nbsp;|&nbsp; Pre-exit: {pre_exit} 15:15<br>'
                f'ATM={atm} &nbsp;·&nbsp; SC={sc_k}/SP={sp_k} &nbsp;·&nbsp; HC={hc_k}/HP={hp_k}<br>'
                f'N_C={n_c}  N_P={n_p} &nbsp;|&nbsp; Straddle={straddle:.1f} &nbsp;|&nbsp; LS={lot_size}<br>'
                f'BE=[{be_lo:.0f}, {be_hi:.0f}]'
                f'</div>',
                unsafe_allow_html=True,
            )

            # Leg table
            if legs:
                import pandas as pd
                rows = []
                label_map = {
                    "atm_c": f"Long ATM Call ({atm})",
                    "atm_p": f"Long ATM Put  ({atm})",
                    "sc":    f"Short Call    ({sc_k})",
                    "sp":    f"Short Put     ({sp_k})",
                    "hc":    f"Long HC       ({hc_k})",
                    "hp":    f"Long HP       ({hp_k})",
                }
                for key, label in label_map.items():
                    leg = legs.get(key, {})
                    ep  = leg.get("entry_prem", 0)
                    ltp = _leg_ltp(key)
                    rows.append({
                        "Leg": label,
                        "Symbol": leg.get("symbol", ""),
                        "Entry ₹": f"{ep:.1f}",
                        "LTP ₹":   f"{ltp:.1f}" if ltp else "—",
                        "Qty":     leg.get("qty", 0),
                    })
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

            with st.expander("📋 Raw state"):
                st.json(state)

            _render_today_trades_detail(
                _load_today_trades("flat_blue_line_bot"), compact=True
            )

            st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
#  BANKNIFTY IRON FLY MONTHLY BOT PANEL
# ══════════════════════════════════════════════════════════════════════════════

def render_bnf_iron_fly_monthly_panel(ltps: dict):
    """Dashboard panel for the BANKNIFTY Monthly Short Iron Fly bot."""
    state = _load(STATE_FILES["BNF_IRON_FLY_MONTHLY"])

    with st.container(border=True):
        st.subheader("🦋 BANKNIFTY Iron Fly Monthly Bot")

        with st.expander("📖 Strategy & Research Details"):
            st.markdown(
                '<div class="research-badge">'
                'Research: Short ATM straddle + long OTM wings (Δ≈0.15) &nbsp;|&nbsp; '
                'Champion (OOS): adj [0.20–0.70], PT 50% &nbsp;|&nbsp; '
                'IS Sharpe +2.355 · OOS +1.555 &nbsp;|&nbsp; WR 85.7% OOS &nbsp;|&nbsp; '
                'Delta adj every 2 bars · 10 lots NRML · ALL 10 stages ✅'
                '</div>',
                unsafe_allow_html=True,
            )
            ec1, ec2 = st.columns(2)
            with ec1:
                st.markdown("**Entry Conditions (10:00 IST)**")
                st.markdown(
                    '<div class="condition-row">'
                    '📅 Entry: first trading day after prior monthly expiry at 10:00 IST<br>'
                    '📌 Sell ATM CE + ATM PE (short straddle)<br>'
                    '📌 Buy OTM CE + OTM PE (hedge, delta≈0.15 via Black-76)<br>'
                    '📌 Product: NRML — held ~20 days to day-before-expiry<br>'
                    '📌 DTE ≥ 2 at entry'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with ec2:
                st.markdown("**Exit Rules & Adjustments**")
                st.markdown(
                    '<div class="condition-row">'
                    '🎯 Profit target: 50% of net premium collected → buy to close all legs<br>'
                    '⏰ Scheduled exit: day-before-expiry 15:15 IST<br>'
                    '⚙️ Adjustment: if short leg delta exits [0.20–0.70] for 2 bars → '
                    'close that leg + re-open at current ATM (same expiry, same lot size)<br>'
                    '🛑 No stop-loss — adjustments handle adverse moves'
                    '</div>',
                    unsafe_allow_html=True,
                )

        if not state:
            st.warning(
                "State file absent — bot is either not running or has not entered yet. "
                f"Check `live_trading/logs/banknifty_iron_fly_monthly_state.json`."
            )
            return

        closed      = state.get("closed", False)
        paper_mode  = state.get("paper_mode", True)
        trade_date  = state.get("trade_date", "—")
        entry_time  = (state.get("entry_time") or "")[:16].replace("T", " ")
        expiry_str = state.get("expiry_str", "—")
        exit_day   = state.get("exit_day", "—")
        atm_strike = state.get("atm_strike", 0)
        vix_entry  = state.get("vix_at_entry", 0)
        spot_entry = state.get("spot_at_entry", 0)
        net_cr_unit = float(state.get("net_credit_per_unit", 0))
        premium    = float(state.get("premium_collected", 0))
        qty        = int(state.get("qty", 0))
        lot_size   = int(state.get("lot_size", 30))
        n_lots     = int(state.get("n_lots", 10))
        current_mtm = float(state.get("current_mtm", 0))
        exit_reason = state.get("exit_reason")
        n_adj      = int(state.get("n_adjustments", 0))
        last_update = state.get("last_update", "")
        legs        = state.get("legs", {})

        # ── Header: status banner ──────────────────────────────────────────────
        if closed:
            rsn = exit_reason or "exited"
            st.success(f"✅ Position CLOSED — {rsn}")
        elif legs:
            mode_tag = "📝 PAPER" if paper_mode else "🔴 LIVE"
            st.info(
                f"{mode_tag} · Active 4-leg iron fly · ATM {atm_strike} · "
                f"Expiry {expiry_str} · Spot {spot_entry:.0f} · VIX {vix_entry:.1f}"
            )
        else:
            st.info("Bot is running — waiting for next entry day.")
            return

        # ── MTM + stats ──────────────────────────────────────────────────────
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Entry", entry_time[:10] if entry_time else "—", trade_date)
        c2.metric("Expiry", expiry_str, f"Exit: {exit_day} 15:15")
        c3.metric("MTM (₹)", f"{current_mtm:+,.0f}")
        c4.metric("Adjustments", n_adj, f"PT: ₹{premium * 0.5:,.0f}")

        # ── 4 legs ─────────────────────────────────────────────────────────────
        st.markdown("**Legs**")
        headers = ["Leg", "Symbol", "Entry (₹)", "LTP (₹)", "MTM (₹)", "Strike"]
        row_data = []
        for leg_key, leg_label in [
            ("sell_ce", "SELL CE ATM"),
            ("sell_pe", "SELL PE ATM"),
            ("buy_ce",  "BUY CE OTM"),
            ("buy_pe",  "BUY PE OTM"),
        ]:
            leg = legs.get(leg_key, {})
            if not leg:
                continue
            sym   = leg.get("symbol", "—")
            entry = float(leg.get("entry_prem", 0))
            ltp   = ltps.get(sym, entry)
            if "sell" in leg_key:
                pnl = (entry - ltp) * qty
            else:
                pnl = (ltp - entry) * qty
            strike = leg.get("strike", 0)
            row_data.append([leg_label, sym, f"{entry:.2f}", f"{ltp:.2f}",
                             f"{pnl:+,.0f}", f"{strike:,}"])

        if row_data:
            st.dataframe(
                pd.DataFrame(row_data, columns=headers),
                use_container_width=True, hide_index=True,
            )

        # ── Closed trade summary ───────────────────────────────────────────────
        if closed:
            total_pnl = float(state.get("total_pnl", 0))
            color = "green" if total_pnl >= 0 else "red"
            st.markdown(
                f"**Final P&L: <span style='color:{color}'>₹{total_pnl:+,.2f}</span>**"
                f"  (exit reason: {exit_reason or '—'})",
                unsafe_allow_html=True,
            )
        else:
            # ── Profit target indicator ────────────────────────────────────────
            if premium > 0:
                pt_target = premium * 0.5
                pct = min(current_mtm / pt_target * 100, 100)
                st.progress(
                    min(max(current_mtm / pt_target, 0), 1.0),
                    text=f"MTM ₹{current_mtm:+,.0f} / PT ₹{pt_target:,.0f} "
                         f"(50% of ₹{premium:,.0f} premium)"
                )

        _render_today_trades_detail(
            _load_today_trades("bnf_iron_fly_monthly_bot"), compact=True
        )

        # ── Raw state ─────────────────────────────────────────────────────────
        with st.expander("📋 Raw state"):
            st.json(state)


# ══════════════════════════════════════════════════════════════════════════════
#  NIFTY MA CROSS SELLER BOT PANEL
# ══════════════════════════════════════════════════════════════════════════════

def render_nifty_ma_cross_panel(ltps: dict):
    """
    Three-tab panel for the NIFTY MA Cross Seller Bot:
      Tab 1 — Overview (position card + key metrics)
      Tab 2 — Strategy Flowchart (rich colour SVG)
      Tab 3 — Live Decision State (auto-refreshing indicator snapshot)
    """
    state = _load(STATE_FILES.get("NIFTY_MA_CROSS_SELLER"))

    st.markdown("## 🤖 NIFTY MA Cross Seller")
    st.caption(
        "SMA(15) / SMA(225) on 3m NIFTY bars · Trend variant · NRML overnight · "
        "10 lots · SL 3× · Week-2 blocked · ALL 10 stages pass"
    )

    tab_overview, tab_flow, tab_state = st.tabs([
        "📊 Overview",
        "🗺️ Strategy Flowchart",
        "🧠 Live Decision State",
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════════
    with tab_overview:
        if not state:
            st.warning("Bot not running — state file absent. Start the bot to see live data.")
        else:
            active = state.get("active_trade")
            nifty  = state.get("nifty_ltp", 0.0)
            vix    = state.get("vix_ltp", 0.0)
            expiry = state.get("expiry_str", "—")
            dte    = state.get("dte")
            updated = state.get("last_update", "")[:19].replace("T", " ")
            readiness = state.get("readiness", "UNKNOWN")
            bars   = state.get("bars_3m_count", 0)
            warmed = state.get("sma_warmed", False)

            # Readiness banner
            _banner = {
                "READY":        ("🟢", "READY TO TRADE", "#00c875"),
                "IN_POSITION":  ("📉", "IN POSITION",    "#7b61ff"),
                "WEEK2_BLOCKED":("🚫", "WEEK-2 BLOCKED", "#f87171"),
                "CUTOFF_PASSED":("⏰", "ENTRY CUTOFF PASSED", "#fbbf24"),
                "LOCKOUT":      ("🔒", "LOCKED OUT",     "#fb923c"),
                "WARMING_UP":   ("⏳", "WARMING UP",     "#60a5fa"),
                "DANGER_ZONE":  ("⚠️", "DANGER ZONE (VIX<13+ADX≥25)", "#f87171"),
                "UNKNOWN":      ("❓", "UNKNOWN",        "#94a3b8"),
            }
            icon, label, colour = _banner.get(readiness, _banner["UNKNOWN"])
            st.markdown(
                f'<div style="background:{colour}22;border-left:4px solid {colour};'
                f'padding:10px 16px;border-radius:6px;margin-bottom:12px;">'
                f'<span style="font-size:1.3em">{icon}</span> '
                f'<strong style="color:{colour};font-size:1.05em">{label}</strong>'
                f'<span style="float:right;opacity:.6;font-size:.85em">Updated {updated}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

            # Top metrics
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("NIFTY", f"{nifty:,.1f}" if nifty else "—")
            c2.metric("INDIAVIX", f"{vix:.2f}" if vix else "—")
            c3.metric("Expiry", expiry or "—")
            c4.metric("DTE", str(dte) if dte is not None else "—")
            c5.metric("3m Bars", f"{bars}" + ("" if warmed else " ⏳"))

            st.markdown("---")

            # Active position card
            if active:
                sym      = active.get("symbol", "")
                opt_type = active.get("opt_type", "")
                entry_p  = float(active.get("entry_prem", 0))
                sl_p     = float(active.get("sl_prem", 0))
                qty      = int(active.get("qty", 0))
                entry_ts = active.get("entry_ts", "")[:16].replace("T", " ")
                ltp_now  = ltps.get(sym, entry_p)
                pnl      = (entry_p - ltp_now) * qty
                pnl_pct  = ((entry_p - ltp_now) / entry_p * 100) if entry_p else 0
                sl_dist  = ltp_now / entry_p if entry_p else 0

                pnl_color = "#00c875" if pnl >= 0 else "#f87171"

                st.markdown("### 📌 Open Position")
                pc1, pc2, pc3, pc4 = st.columns(4)
                pc1.metric("Symbol", sym)
                pc2.metric("Type", f"SELL {opt_type}")
                pc3.metric("Entry Premium", f"₹{entry_p:.2f}")
                pc4.metric("Current Premium", f"₹{ltp_now:.2f}",
                           delta=f"{pnl_pct:+.1f}%",
                           delta_color="normal" if pnl >= 0 else "inverse")

                pc5, pc6, pc7, pc8 = st.columns(4)
                pc5.metric("Qty", str(qty))
                pc6.metric("SL (3×)", f"₹{sl_p:.2f}")
                pc7.metric("SL Buffer", f"{(sl_p - ltp_now):.1f} pts remaining")
                pc8.metric("MTM P&L", f"₹{pnl:+,.0f}")

                # SL proximity bar
                sl_pct = min(sl_dist * 100, 100) if entry_p else 0
                bar_color = "#00c875" if sl_dist < 1.5 else ("#fbbf24" if sl_dist < 2.5 else "#f87171")
                st.markdown(
                    f'<div style="margin:8px 0 4px;font-size:.85em;color:#94a3b8">SL proximity '
                    f'({sl_dist:.2f}× entry · trigger at 3.0×)</div>'
                    f'<div style="background:#1e293b;border-radius:4px;height:8px;overflow:hidden">'
                    f'<div style="background:{bar_color};width:{min(sl_dist/3*100,100):.0f}%;height:100%"></div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                st.caption(f"Position opened: {entry_ts} · Product: NRML (overnight)")
            else:
                st.info("No open position. Bot is flat and scanning for signals.")

            # Today's closed trades from perf DB
            today_trades = _load_today_trades("nifty_ma_cross_seller_bot")
            if today_trades:
                st.markdown("### 📋 Today's Closed Trades")
                _render_today_trades_detail(today_trades)

        with st.expander("📋 Raw state"):
            st.json(state or {})

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — STRATEGY FLOWCHART
    # ══════════════════════════════════════════════════════════════════════════
    with tab_flow:
        st.markdown("#### NIFTY MA Cross Seller — Execution Logic")
        st.caption(
            "How the bot decides on every completed 3-minute bar. "
            "Follow the path from Session Start to ORDER PLACED."
        )

        flowchart_html = """
<style>
  .fc-wrap { font-family: 'Inter', 'Segoe UI', sans-serif; padding: 8px 0; }
  .fc-node {
    display: inline-flex; align-items: center; justify-content: center;
    text-align: center; border-radius: 10px; font-weight: 600;
    font-size: 13px; line-height: 1.35; padding: 10px 16px;
    box-shadow: 0 2px 8px rgba(0,0,0,.25);
  }
  .fc-start  { background: #7b61ff; color: #fff; border-radius: 24px; padding: 10px 24px; }
  .fc-action { background: #1e40af; color: #dbeafe; }
  .fc-check  { background: #064e3b; color: #6ee7b7; border-radius: 50px; }
  .fc-block  { background: #7f1d1d; color: #fca5a5; }
  .fc-entry  { background: #14532d; color: #86efac; }
  .fc-exit   { background: #78350f; color: #fde68a; }
  .fc-monitor{ background: #1e3a5f; color: #93c5fd; }
  .fc-arrow  { color: #64748b; font-size: 20px; line-height: 1; user-select: none; }
  .fc-label  { font-size: 11px; color: #94a3b8; font-weight: 500; }
  .fc-row    { display: flex; align-items: center; gap: 6px; margin: 4px 0; }
  .fc-col    { display: flex; flex-direction: column; align-items: center; gap: 0; }
  .fc-split  { display: flex; gap: 16px; align-items: flex-start; justify-content: center; }
  .fc-branch { display: flex; flex-direction: column; align-items: center; gap: 4px; }
  .fc-yes    { color: #4ade80; font-size: 11px; font-weight: 700; }
  .fc-no     { color: #f87171; font-size: 11px; font-weight: 700; }
</style>

<div class="fc-wrap">

<!-- ── SESSION START ─────────────────────────────────────────────────────── -->
<div class="fc-col">

  <div class="fc-row">
    <div class="fc-node fc-start">☀️ Session Start (09:15)</div>
  </div>

  <div class="fc-arrow">↓</div>

  <div class="fc-row">
    <div class="fc-node fc-action">📚 Load 15 days of 1m history<br><span style="font-weight:400;font-size:11px">Convert → 3m bars · Warm SMA(15) & SMA(225)</span></div>
  </div>

  <div class="fc-arrow">↓</div>

  <div class="fc-row">
    <div class="fc-node fc-action">🔌 Subscribe WebSocket<br><span style="font-weight:400;font-size:11px">NIFTY index ticks + INDIA VIX</span></div>
  </div>

  <div class="fc-arrow">↓</div>

<!-- ── TICK LOOP ──────────────────────────────────────────────────────────── -->

  <div class="fc-row">
    <div class="fc-node fc-action" style="background:#1e293b;color:#94a3b8">⚡ Every 1-min bar close<br><span style="font-weight:400;font-size:11px">Accumulate ticks → OHLC</span></div>
  </div>

  <div class="fc-arrow">↓</div>

  <div class="fc-row">
    <div class="fc-node fc-check">3 × 1m bars accumulated?</div>
  </div>

  <div class="fc-split">
    <div class="fc-branch">
      <div class="fc-no">NO</div>
      <div class="fc-arrow">↓</div>
      <div class="fc-node fc-block" style="font-size:11px">Wait for more ticks</div>
    </div>
    <div class="fc-branch">
      <div class="fc-yes">YES</div>
      <div class="fc-arrow">↓</div>
      <div class="fc-node fc-action">🕒 3m bar complete<br><span style="font-weight:400;font-size:11px">Append to bars deque · Compute SMA(15)/SMA(225)</span></div>
    </div>
  </div>

  <div style="height:12px"></div>
  <div class="fc-arrow">↓ (YES path continues)</div>
  <div style="height:4px"></div>

<!-- ── POSITION CHECK ─────────────────────────────────────────────────────── -->

  <div class="fc-row">
    <div class="fc-node fc-check">Active NRML position open?</div>
  </div>

  <div class="fc-split">

    <div class="fc-branch">
      <div class="fc-yes">YES → MONITOR</div>
      <div class="fc-arrow">↓</div>
      <div class="fc-node fc-monitor">🔍 Monitor Position</div>
      <div class="fc-arrow">↓</div>

      <div style="display:flex;flex-direction:column;gap:6px;align-items:center">

        <div class="fc-row">
          <div class="fc-node fc-check" style="font-size:11px">Premium ≥ 3× entry?</div>
          <div style="width:6px"></div>
          <div class="fc-yes">YES</div>
          <div class="fc-arrow">→</div>
          <div class="fc-node fc-exit">🛑 EXIT<br><span style="font-size:10px">SL_3X</span></div>
        </div>

        <div class="fc-row">
          <div class="fc-node fc-check" style="font-size:11px">14:30 on expiry day?</div>
          <div style="width:6px"></div>
          <div class="fc-yes">YES</div>
          <div class="fc-arrow">→</div>
          <div class="fc-node fc-exit">🛑 EXIT<br><span style="font-size:10px">EXPIRY_GATE</span></div>
        </div>

        <div class="fc-row">
          <div class="fc-node fc-check" style="font-size:11px">15:15 on expiry day?</div>
          <div style="width:6px"></div>
          <div class="fc-yes">YES</div>
          <div class="fc-arrow">→</div>
          <div class="fc-node fc-exit">🛑 EXIT<br><span style="font-size:10px">EOD_EXPIRY</span></div>
        </div>

        <div class="fc-row">
          <div class="fc-node fc-check" style="font-size:11px">Opposite SMA cross?</div>
          <div style="width:6px"></div>
          <div class="fc-yes">YES</div>
          <div class="fc-arrow">→</div>
          <div class="fc-node fc-exit">🛑 EXIT<br><span style="font-size:10px">REVERSAL</span></div>
        </div>

        <div class="fc-no" style="margin-top:4px">ALL NO → Hold overnight (NRML) 🌙</div>
      </div>
    </div>

    <div style="width:2px;background:#334155;min-height:200px;margin:0 8px"></div>

    <div class="fc-branch">
      <div class="fc-no">NO → SCAN FOR ENTRY</div>
      <div class="fc-arrow">↓</div>

<!-- ── ENTRY FILTERS (SEQUENTIAL) ─────────────────────────────────────────── -->

      <div style="display:flex;flex-direction:column;gap:5px;align-items:center">

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px">≥ 230 bars warmed?</div>
          <div class="fc-no">NO →</div>
          <div class="fc-node fc-block" style="font-size:11px">⏳ Warming up</div>
        </div>

        <div class="fc-arrow">↓ YES</div>

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px">SMA cross detected?</div>
          <div class="fc-no">NO →</div>
          <div class="fc-node fc-block" style="font-size:11px">No signal</div>
        </div>

        <div class="fc-arrow">↓ YES</div>

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px;background:#7c3aed;color:#ede9fe">Calendar day 8–14?<br><span style="font-size:10px">(Week-2 filter)</span></div>
          <div class="fc-yes">YES →</div>
          <div class="fc-node fc-block" style="font-size:11px;background:#4c1d95;color:#c4b5fd">🚫 WEEK-2<br>BLOCKED</div>
        </div>

        <div class="fc-arrow">↓ NO</div>

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px">Time ≥ 14:00?</div>
          <div class="fc-yes">YES →</div>
          <div class="fc-node fc-block" style="font-size:11px">⏰ Entry cutoff</div>
        </div>

        <div class="fc-arrow">↓ NO</div>

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px">Lockout &lt; 450 bars?</div>
          <div class="fc-yes">YES →</div>
          <div class="fc-node fc-block" style="font-size:11px">🔒 Too soon<br>after last cross</div>
        </div>

        <div class="fc-arrow">↓ NO (≥ 450 bars)</div>

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px">DTE &lt; 2?</div>
          <div class="fc-yes">YES →</div>
          <div class="fc-node fc-block" style="font-size:11px">📅 Too close<br>to expiry</div>
        </div>

        <div class="fc-arrow">↓ NO</div>

        <div class="fc-row" style="gap:4px">
          <div class="fc-node fc-check" style="font-size:11px">VIX&lt;13 AND ADX≥25?</div>
          <div class="fc-yes">YES →</div>
          <div class="fc-node fc-block" style="font-size:11px">⚠️ Danger zone<br>(skip)</div>
        </div>

        <div class="fc-arrow">↓ NO (all filters clear)</div>

<!-- ── CROSS DIRECTION → ORDER ─────────────────────────────────────────────── -->

        <div class="fc-node fc-check" style="font-size:12px">Cross direction?</div>

        <div class="fc-split" style="gap:24px;margin-top:8px">
          <div class="fc-branch">
            <div style="color:#f87171;font-size:11px;font-weight:700">BEARISH<br>(fast ↓ below slow)</div>
            <div class="fc-arrow">↓</div>
            <div class="fc-node fc-entry" style="background:#1a3a2a">
              📉 SELL ATM CE<br>
              <span style="font-size:10px;font-weight:400">NRML · 10 lots · SL 3×</span>
            </div>
          </div>
          <div class="fc-branch">
            <div style="color:#4ade80;font-size:11px;font-weight:700">BULLISH<br>(fast ↑ above slow)</div>
            <div class="fc-arrow">↓</div>
            <div class="fc-node fc-entry">
              📈 SELL ATM PE<br>
              <span style="font-size:10px;font-weight:400">NRML · 10 lots · SL 3×</span>
            </div>
          </div>
        </div>

        <div style="height:10px"></div>
        <div class="fc-node" style="background:#7b61ff22;border:1px solid #7b61ff;color:#c4b5fd;font-size:12px">
          🌙 Hold overnight until exit trigger
        </div>

      </div>
    </div>

  </div>

</div>

<!-- ── LEGEND ─────────────────────────────────────────────────────────────── -->
<div style="margin-top:24px;padding:12px 16px;background:#0f172a;border-radius:8px;
            display:flex;gap:20px;flex-wrap:wrap;font-size:11px;">
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#064e3b;margin-right:6px"></span><span style="color:#94a3b8">Decision check</span></div>
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#1e40af;margin-right:6px"></span><span style="color:#94a3b8">Action / process</span></div>
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#7f1d1d;margin-right:6px"></span><span style="color:#94a3b8">Blocked / skip</span></div>
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#14532d;margin-right:6px"></span><span style="color:#94a3b8">Entry order</span></div>
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#78350f;margin-right:6px"></span><span style="color:#94a3b8">Exit order</span></div>
  <div><span style="display:inline-block;width:12px;height:12px;border-radius:3px;
       background:#1e3a5f;margin-right:6px"></span><span style="color:#94a3b8">Monitor loop</span></div>
</div>

</div>
"""
        st.components.v1.html(flowchart_html, height=1500, scrolling=True)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — LIVE DECISION STATE
    # ══════════════════════════════════════════════════════════════════════════
    with tab_state:
        if not state:
            st.warning("State file not found. Start the bot to see live decision data.")
        else:
            updated = state.get("last_update", "")[:19].replace("T", " ")
            st.caption(f"State file last written: **{updated}** · Updates every 3 minutes (on bar close)")

            col_refresh = st.columns([1, 4])[0]
            with col_refresh:
                if st.button("🔄 Refresh Now"):
                    st.rerun()

            st.markdown("---")

            # ── Signal Engine ─────────────────────────────────────────────────
            st.markdown("### 📡 Signal Engine")

            sma_fast   = state.get("sma_fast")
            sma_slow   = state.get("sma_slow")
            sma_gap    = state.get("sma_gap")
            bars_count = state.get("bars_3m_count", 0)
            warmed     = state.get("sma_warmed", False)
            last_cross = state.get("last_cross_dir", "—")
            cross_ago  = state.get("last_cross_bars_ago", "—")
            filters    = state.get("filters", {})
            bars_since = filters.get("bars_since_cross", 0)
            lockout_total   = filters.get("lockout_total", 450)
            lockout_remaining = filters.get("lockout_remaining", 0)
            locked     = filters.get("locked", False)

            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                if sma_fast is not None:
                    st.metric("SMA Fast (15)", f"{sma_fast:,.2f}")
                else:
                    st.metric("SMA Fast (15)", "warming…")
            with sc2:
                if sma_slow is not None:
                    st.metric("SMA Slow (225)", f"{sma_slow:,.2f}")
                else:
                    st.metric("SMA Slow (225)", "warming…")
            with sc3:
                if sma_gap is not None:
                    gap_dir = "▲ Bullish" if sma_gap > 0 else "▼ Bearish"
                    st.metric("Fast − Slow", f"{sma_gap:+.2f}", delta=gap_dir,
                              delta_color="normal" if sma_gap > 0 else "inverse")
                else:
                    st.metric("Fast − Slow", "—")

            sc4, sc5, sc6 = st.columns(3)
            sc4.metric("3m Bars Loaded", f"{bars_count} / 230 min",
                       delta="✅ Warmed" if warmed else "⏳ Warming up",
                       delta_color="off")

            # Lockout progress
            pct_done = (bars_since / lockout_total * 100) if lockout_total else 100
            sc5.metric(
                "Lockout Progress",
                f"{bars_since} / {lockout_total} bars",
                delta=f"🔒 {lockout_remaining} to go" if locked else "✅ Unlocked",
                delta_color="off",
            )
            sc6.metric("Last Cross", str(last_cross) if last_cross else "—",
                       delta=f"{cross_ago} bars ago" if cross_ago and cross_ago != "—" else None,
                       delta_color="off")

            # Lockout bar
            bar_color = "#4ade80" if not locked else "#fb923c"
            st.markdown(
                f'<div style="margin:6px 0 2px;font-size:.8em;color:#64748b">'
                f'Anti-whipsaw lockout ({bars_since}/{lockout_total} bars completed)</div>'
                f'<div style="background:#1e293b;border-radius:4px;height:10px;overflow:hidden">'
                f'<div style="background:{bar_color};width:{min(pct_done,100):.0f}%;height:100%;'
                f'transition:width .4s"></div></div>'
                f'<div style="font-size:.75em;color:#64748b;margin-top:2px">'
                f'{"🔒 LOCKED — " + str(lockout_remaining) + " bars remaining before next entry allowed" if locked else "✅ UNLOCKED — cross accepted if all other filters clear"}'
                f'</div>',
                unsafe_allow_html=True,
            )

            st.markdown("---")

            # ── Today's Filters ───────────────────────────────────────────────
            st.markdown("### 🔍 Today's Entry Filters")

            cal_day   = filters.get("calendar_day", "?")
            w2        = filters.get("week2_blocked", False)
            cutoff    = filters.get("after_entry_cutoff", False)
            dz        = filters.get("danger_zone_vix", False)
            dte_ok    = filters.get("dte_ok", False)
            min_dte   = filters.get("min_dte", 2)
            dte_val   = state.get("dte")
            vix_thr   = filters.get("vix_threshold", 13.0)
            vix_val   = state.get("vix_ltp", 0.0)

            def _frow(icon, name, ok, note=""):
                colour = "#00c875" if ok else "#f87171"
                badge  = "✅ PASS" if ok else "❌ BLOCK"
                st.markdown(
                    f'<div style="display:flex;align-items:center;padding:7px 12px;'
                    f'margin:3px 0;background:#0f172a;border-radius:7px;gap:10px;">'
                    f'<span style="font-size:1.2em">{icon}</span>'
                    f'<span style="flex:1;color:#e2e8f0;font-size:.9em">{name}</span>'
                    f'<span style="font-size:.8em;color:#64748b">{note}</span>'
                    f'<span style="background:{colour}22;color:{colour};font-size:.75em;'
                    f'font-weight:700;padding:2px 8px;border-radius:4px">{badge}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            _frow("📅", f"Week-2 block (day {cal_day})",
                  not w2,
                  f"Days 8–14 blocked · Today is day {cal_day}")
            _frow("⏰", "Entry cutoff < 14:00 IST",
                  not cutoff,
                  "No new entries after 14:00")
            _frow("🔒", "Anti-whipsaw lockout",
                  not locked,
                  f"{bars_since}/{lockout_total} bars elapsed")
            _frow("📆", f"DTE ≥ {min_dte}",
                  dte_ok,
                  f"DTE = {dte_val}" if dte_val is not None else "No expiry resolved")
            _frow("⏳", "SMA warm-up complete",
                  warmed,
                  f"{bars_count}/230 bars loaded")
            _frow("⚠️", f"Danger zone clear (VIX < {vix_thr} AND ADX ≥ 25)",
                  not dz,
                  f"VIX = {vix_val:.2f}" if vix_val else "VIX unknown")

            st.markdown("---")

            # ── Overall Verdict ───────────────────────────────────────────────
            st.markdown("### 🎯 Signal Readiness")
            readiness = state.get("readiness", "UNKNOWN")
            _banner = {
                "READY":        ("🟢", "READY — next valid SMA cross will trigger an order", "#00c875"),
                "IN_POSITION":  ("📉", "IN POSITION — monitoring for exit trigger, no new entries", "#7b61ff"),
                "WEEK2_BLOCKED":("🚫", "WEEK-2 BLOCKED — no entries on calendar days 8–14", "#f87171"),
                "CUTOFF_PASSED":("⏰", "ENTRY CUTOFF PASSED — 14:00 IST has elapsed", "#fbbf24"),
                "LOCKOUT":      ("🔒", f"LOCKED OUT — {lockout_remaining} bars until next entry allowed", "#fb923c"),
                "WARMING_UP":   ("⏳", f"WARMING UP — {bars_count}/230 3m bars loaded, need {230-bars_count} more", "#60a5fa"),
                "DANGER_ZONE":  ("⚠️", f"DANGER ZONE — VIX={vix_val:.1f} < {vix_thr} (ADX check at entry time)", "#f87171"),
                "UNKNOWN":      ("❓", "State unknown", "#94a3b8"),
            }
            icon, msg, colour = _banner.get(readiness, _banner["UNKNOWN"])
            st.markdown(
                f'<div style="background:{colour}22;border:1.5px solid {colour};'
                f'border-radius:10px;padding:16px 20px;font-size:1em;">'
                f'<span style="font-size:1.5em">{icon}</span> '
                f'<strong style="color:{colour}">{msg}</strong>'
                f'</div>',
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    # ── Sidebar Account Selector ─────────────────────────────────────────────
    # This must be at the very top of the sidebar to be visible
    selected_ws_id = st.sidebar.selectbox(
        "📂 Select Account",
        options=list(WORKSPACES.keys()),
        format_func=lambda x: WORKSPACES[x]["name"],
        index=0,
        key="selected_workspace_top"
    )
    st.sidebar.markdown('---')

    # Re-setup globals based on the selection for the current page run
    global WS_PATHS, WS_ENV, LOGS_DIR, NTS_OBI_LOGS_DIR, REGISTRY_FILE, COMMANDS_FILE, STATE_FILES
    WS_PATHS = get_workspace_paths(selected_ws_id)
    WS_ENV   = get_ws_env(selected_ws_id)

    LOGS_DIR            = WS_PATHS["LOGS_DIR"]
    NTS_OBI_LOGS_DIR    = WS_PATHS["NTS_OBI_LOGS_DIR"]
    # EQUITY_OBI_LOGS_DIR removed — RETIRED 2026-06-19
    REGISTRY_FILE       = WS_PATHS["REGISTRY_FILE"]
    COMMANDS_FILE       = WS_PATHS["COMMANDS_FILE"]
    STATE_FILES         = WS_PATHS["STATE_FILES"]

    # This ensures bot logic pointing to these globals uses the selected workspace
    load_dotenv(WS_PATHS["ROOT"] / ".env")


    # Session-state-backed navigation: each section is its own radio widget,
    # with styled markdown headers between them. on_change callbacks sync the
    # selected item into st.session_state["nav_view"] so all sections stay
    # in agreement about which view is active.
    if "nav_view" not in st.session_state:
        st.session_state["nav_view"] = "🏠 Dashboard Overview"

    _cur = st.session_state["nav_view"]

    def _nav_changed(radio_key: str) -> None:
        val = st.session_state.get(radio_key)
        if val:
            st.session_state["nav_view"] = val

    _GRP_OV  = ["🏠 Dashboard Overview"]
    _GRP_OPT = [
        "🤖 Nifty BB OB", "🤖 Nifty Trend Seller", "🤖 SENSEX Trend Seller",
        "🤖 HTF PO3 Bot", "🤖 BANKNIFTY BB Options", "🤖 BB Mean Reversion", "🤖 HA Options Bot",
        "🤖 NIFTY MACD Map", "🤖 NIFTY EOD Hold", "🤖 MA Cross Seller", "🔬 NTS + OBI Gate",
    ]
    _GRP_STK = [
        "🤖 Pre-Open Gap Fade",
        # "🔬 Equity OBI",  # RETIRED 2026-06-19
        # "🤖 Gap Fade EOD",   # RETIRED 2026-06-04
        # "🤖 EMA Swing Scanner",  # RETIRED 2026-06-04
    ]
    _GRP_WK  = ["📅 NIFTY Iron Fly Weekly", "📅 SENSEX Iron Fly Weekly", "📅 BANKNIFTY Iron Fly Monthly"]
    _GRP_MO  = ["📆 Flat Blue Line Monthly"]
    _GRP_SYS = ["⚙️ Infrastructure", "🎛️ Bot Controls"]

    # Pre-set each radio's session_state key to the currently active view
    # (or None if the view lives in a different section). This ensures only
    # the correct section shows a highlighted item.
    for _key, _items in [
        ("nav_ov", _GRP_OV), ("nav_opt", _GRP_OPT), ("nav_stk", _GRP_STK),
        ("nav_wk", _GRP_WK), ("nav_mo", _GRP_MO), ("nav_sys", _GRP_SYS),
    ]:
        st.session_state[_key] = _cur if _cur in _items else None

    def _sec_hdr(label: str) -> None:
        st.sidebar.markdown(
            f'<div style="color:#00d4ff;font-weight:700;font-size:0.72em;'
            f'letter-spacing:0.10em;padding:10px 4px 3px 4px;'
            f'border-top:1px solid #1e2a45;margin-top:4px">'
            f'{label}</div>',
            unsafe_allow_html=True,
        )

    st.sidebar.radio(" ", _GRP_OV,  key="nav_ov",
        label_visibility="collapsed",
        on_change=_nav_changed, args=("nav_ov",))

    _sec_hdr("⚡ OPTIONS BOTS")
    st.sidebar.radio(" ", _GRP_OPT, key="nav_opt",
        label_visibility="collapsed",
        on_change=_nav_changed, args=("nav_opt",))

    _sec_hdr("📈 STOCK BOTS")
    st.sidebar.radio(" ", _GRP_STK, key="nav_stk",
        label_visibility="collapsed",
        on_change=_nav_changed, args=("nav_stk",))

    _sec_hdr("📅 WEEKLY POSITIONS")
    st.sidebar.radio(" ", _GRP_WK,  key="nav_wk",
        label_visibility="collapsed",
        on_change=_nav_changed, args=("nav_wk",))

    _sec_hdr("📆 MONTHLY POSITIONS")
    st.sidebar.radio(" ", _GRP_MO,  key="nav_mo",
        label_visibility="collapsed",
        on_change=_nav_changed, args=("nav_mo",))

    _sec_hdr("⚙️ SYSTEM")
    st.sidebar.radio(" ", _GRP_SYS, key="nav_sys",
        label_visibility="collapsed",
        on_change=_nav_changed, args=("nav_sys",))

    view = st.session_state["nav_view"]

    st.sidebar.markdown("---")

    # ── Global Data Fetching ────────────────────────────────────────────────
    # Fetch positionbook once and share it across LTP resolution + overview page
    sym_exchange = _collect_all_open_symbols()
    positionbook = _fetch_positionbook_full()
    ltps         = _all_ltps(sym_exchange, pb=positionbook)   # positionbook first, multiquotes fallback

    # ── View Routing ────────────────────────────────────────────────────────
    if view == "🏠 Dashboard Overview":
        st.subheader("🏠 Portfolio & System Overview")
        render_portfolio_snapshot(ltps, positionbook=positionbook)

    # ── Options Bots ────────────────────────────────────────────────────────
    elif view == "🤖 Nifty BB OB":
        render_bb_overbought_panel(ltps)

    elif view == "🤖 Nifty Trend Seller":
        render_nts_panel(ltps)

    elif view == "🤖 SENSEX Trend Seller":
        render_sensex_ts_panel(ltps)

    elif view == "🤖 HTF PO3 Bot":
        render_htf_po3_panel(ltps)

    elif view == "🤖 BANKNIFTY BB Options":
        render_bnf_bb_options_panel(ltps)

    elif view == "🤖 BB Mean Reversion":
        render_bb_mean_reversion_panel(ltps)

    elif view == "🤖 HA Options Bot":
        render_ha_options_panel(ltps)

    elif view == "🤖 NIFTY MACD Map":
        render_nifty_macd_map_panel(ltps)

    elif view == "🤖 NIFTY EOD Hold":
        render_nifty_eod_hold_panel(ltps)

    elif view == "🤖 MA Cross Seller":
        render_nifty_ma_cross_panel(ltps)

    elif view == "🔬 NTS + OBI Gate":
        render_nts_obi_panel(ltps)

    # ── Stock Bots ───────────────────────────────────────────────────────────
    elif view == "🤖 Pre-Open Gap Fade":
        render_gap_fade_panel(ltps)

    # "🤖 Gap Fade EOD" — RETIRED 2026-06-04
    # "🤖 EMA Swing Scanner" — RETIRED 2026-06-04

    # "🔬 Equity OBI" — RETIRED 2026-06-19

    # ── Weekly Positions ─────────────────────────────────────────────────────
    elif view == "📅 NIFTY Iron Fly Weekly":
        render_iron_fly_panel(ltps)

    elif view == "📅 SENSEX Iron Fly Weekly":
        render_sensex_iron_fly_panel(ltps)

    elif view == "📅 BANKNIFTY Iron Fly Monthly":
        render_bnf_iron_fly_monthly_panel(ltps)

    elif view == "📆 Flat Blue Line Monthly":
        render_flat_blue_line_monthly_panel(ltps)

    # ── System ───────────────────────────────────────────────────────────────
    elif view == "⚙️ Infrastructure":
        st.subheader("⚙️ System Infrastructure")
        render_heartbeats()

    elif view == "🎛️ Bot Controls":
        render_bot_controls()

    # ── Sidebar: Refresh Controls ────────────────────────────────────────────
    refresh_options = {
        "Off (manual)": None,
        "3 seconds":    3,
        "5 seconds":    5,
        "10 seconds":   10,
        "30 seconds":   30,
        "60 seconds":   60,
    }
    refresh_label = st.sidebar.selectbox(
        "🔄 Auto-Refresh Rate",
        options=list(refresh_options.keys()),
        index=0,   # default: Off
        key="refresh_rate",
    )
    refresh_secs = refresh_options[refresh_label]

    st.sidebar.markdown("---")
    st.sidebar.caption(f"Last Refresh: {datetime.now().strftime('%H:%M:%S')}")
    if refresh_secs is None:
        st.sidebar.caption("⏸ Auto-refresh is **OFF**")
        if st.sidebar.button("⚡ Refresh Now", width='stretch'):
            st.rerun()
    else:
        st.sidebar.caption(f"🔄 Refreshes every **{refresh_secs}s**")

    # ── Auto-refresh (only when enabled) ────────────────────────────────────
    if refresh_secs is not None:
        time.sleep(refresh_secs)
        st.rerun()


if __name__ == "__main__":
    main()
