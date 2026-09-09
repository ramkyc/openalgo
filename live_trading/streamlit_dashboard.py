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
  6. HA Options Bot               — RETIRED 2026-07-10 (failed Stage 11 paper-trading gate)
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
from collections import OrderedDict, deque
import json
import logging
import os
import queue
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv, dotenv_values

import pandas as pd
import requests
import streamlit as st

# ── Logging ───────────────────────────────────────────────────────────────────
# live_trading/-layer code, not core OpenAlgo — follows the bot logging
# convention (see _template_bot.py), not utils/logging.py's Flask-request-scoped
# centralized logger. Path(__file__) is not symlink-resolved, so this correctly
# writes to each workspace's own live_trading/logs/ dir even though fyers_cs's
# copy of this file is a symlink into fyers_crk.
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "streamlit_dashboard.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="OpenAlgo Unified Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Workspaces ───────────────────────────────────────────────────────────────
WORKSPACES = {
    "CRK": {"name": "Ramakrishna (CRK)", "root": Path("/Users/ramakrishna/Developer/fyers_crk/openalgo")},
    "CS":  {"name": "Sumana (CS)", "root": Path("/Users/ramakrishna/Developer/fyers_cs/openalgo")}
}

@st.cache_data(ttl="5m")
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
        # "GAP_FADE":  retired 2026-06-24 — no post-cost edge on equity intraday
        # "GAP_FADE_EOD": retired 2026-06-04
        "HTF_PO3":     logs / "htf_po3_state.json",
        "BNF_BB_OPT":  logs / "banknifty_bb_options_state.json",
        "BNF_BB_OC":   logs / "banknifty_bb_opening_candle_state.json",
        "BB_MEAN_REV": logs / "bb_mean_reversion_state.json",
        # "HA_OPTIONS":  retired 2026-07-10 — failed Stage 11 paper-trading gate
        # "EMA_SWING":   retired 2026-06-04
        "NTS_OBI":     nts_obi_logs / "nts_obi_state.json",
        "NIFTY_MACD_MAP": logs / "nifty_macd_map_state.json",
        "NIFTY_EMA_SPREAD":    logs / "nifty_ema_spread_state.json",
        "BANKNIFTY_EMA_SPREAD": logs / "banknifty_ema_spread_state.json",
        "SENSEX_EMA_SPREAD":   logs / "sensex_ema_spread_state.json",
        "NIFTY_EOD_HOLD": logs / "nifty_eod_hold_state.json",
        # "EQUITY_OBI":     retired 2026-06-19
        "IRON_FLY_WEEKLY": logs / "nifty_iron_fly_weekly_state.json",
        "SENSEX_IRON_FLY_WEEKLY": logs / "sensex_iron_fly_weekly_state.json",
        "BNF_IRON_FLY_MONTHLY": logs / "banknifty_iron_fly_monthly_state.json",
        "FLAT_BLUE_LINE_MONTHLY": logs / "flat_blue_line_monthly_state.json",
        "NIFTY_MA_CROSS_SELLER": logs / "nifty_ma_cross_seller_state.json",
        "MACD_M2_SELL": logs / "macd_m2_sell_options_state.json",
        "BANKNIFTY_TREND_PULLBACK_POSITIONAL": logs / "banknifty_trend_pullback_positional_state.json",
        "NIFTY_GEX_ICT_V2": logs / "nifty_gex_ict_v2_bot_state.json",
        "NIFTY_ATM_STRADDLE": logs / "nifty_atm_straddle_scalp_state.json",
        "VP_SWING_SCREENER": logs / "vp_swing_screener_state.json",
        "VP_SWING_SCREENER_DAILY": logs / "vp_swing_screener_daily_state.json",
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

def render_research_findings_tab(research_subpath: str) -> None:
    """Render a research results_summary.md (or equivalent) inside a styled container."""
    path = Path("/Users/ramakrishna/Developer/options_data/research") / research_subpath
    if not path.exists():
        st.warning(f"Research file not found: {research_subpath}")
        return
    try:
        content = path.read_text(encoding="utf-8")
    except Exception as e:
        logger.exception(f"Could not read research file: {research_subpath}")
        st.error(f"Could not read research file: {e}")
        return
    st.markdown(
        '<div style="background:#0d1117;border:1px solid #21262d;border-radius:10px;'
        'padding:24px 28px;line-height:1.7;font-size:0.92em;">',
        unsafe_allow_html=True,
    )
    st.markdown(content)
    st.markdown("</div>", unsafe_allow_html=True)

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
    # "GAP_FADE": RETIRED 2026-06-24 — no post-cost edge on equity intraday
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
    "BNF_BB_OC": {
        "name":     "BNF BB Opening Candle",
        "script":   "banknifty_bb_opening_candle_bot.py",
        "research": "OOS +5.52 pts (BNF) · MC 100% · WF 11/13 | 8/8 stages",
    },
    "BB_MEAN_REV": {
        "name":     "BB Mean Reversion",
        "script":   "bb_mean_reversion_bot.py",
        "research": "IS +1.008 | OOS +2.425 | MC 97.3% | NatRR≥1.25 | 10/10 stages",
    },
    # "HA_OPTIONS": retired 2026-07-10 — failed Stage 11 paper-trading gate
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
    "NIFTY_EMA_SPREAD": {
        "name":     "NIFTY EMA Spread Bot",
        "script":   "nifty_ema_spread_bot/nifty_ema_spread_bot.py",
        "research": "OOS Sharpe 6.14 | WR 57.9% | 428 trades | 10/10 stages | 13/13 WF windows",
    },
    "BANKNIFTY_EMA_SPREAD": {
        "name":     "BANKNIFTY EMA Spread Bot",
        "script":   "banknifty_ema_spread_bot/banknifty_ema_spread_bot.py",
        "research": "OOS Sharpe 3.00 | WR 60.1% | 193 trades | Stage 9 confirm",
    },
    "SENSEX_EMA_SPREAD": {
        "name":     "SENSEX EMA Spread Bot",
        "script":   "sensex_ema_spread_bot/sensex_ema_spread_bot.py",
        "research": "OOS Sharpe 2.90 | WR 58.1% | 353 trades | Stage 9 confirm",
    },
    "NIFTY_EOD_HOLD": {
        "name":     "NIFTY EOD Hold Bot",
        "script":   "nifty_eod_hold_bot.py",
        "research": "OOS +2.867/+2.353 | WR 73%/75% | 10/10 stages",
    },
    # "EQUITY_OBI": retired 2026-06-19 — WR 38.3%, P&L −₹4,122 (149 trades, 23 sessions)
    "IRON_FLY_WEEKLY": {
        "name":     "NIFTY Iron Fly Weekly",
        "script":   "nifty_iron_fly_weekly_bot.py",
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
    "BANKNIFTY_TREND_PULLBACK_POSITIONAL": {
        "name":     "BANKNIFTY Trend Pullback Positional",
        "script":   "banknifty_trend_pullback_positional_bot/banknifty_trend_pullback_positional_bot.py",
        "research": "IS+OOS Sharpe 1.850 WR 60.0% | Net +₹1.01M | SL 2.5× keep50% | NRML positional | 9/10 stages (Stage 9 skipped)",
    },
    "NIFTY_GEX_ICT_V2": {
        "name":     "Nifty GEX ICT V2",
        "script":   "nifty_gex_ict_v2_bot/nifty_gex_ict_v2_bot.py",
        "research": "IS+OOS Sharpe 2.48 | n=239 | Net +₹143,993 | MC 100% | Bootstrap 2.51 | WF avg OOS 4.01 | NIFTY-only 9/9 stages",
    },
    "NIFTY_ATM_STRADDLE": {
        "name":     "NIFTY ATM Straddle Scalp",
        "script":   "nifty_atm_straddle_scalp_bot/nifty_atm_straddle_scalp_bot.py",
        "research": "Champion 10:30_sl20_tgt0.75 | SL 20% breakeven trail | Target 0.75% margin | 10 lots | ALL 0-11 stages pass",
    },
    "VP_SWING_SCREENER": {
        "name":     "VP Swing Screener",
        "script":   "vp_swing_screener/vp_swing_screener.py",
        "research": "IS+OOS Sharpe 4.0/5.2 | WR ~69% | n=3,907 | 53-stock NIFTY50 | 60min swing, target=POC, SL=3% | SIGNAL-ONLY, no live orders",
    },
    "VP_SWING_SCREENER_DAILY": {
        "name":     "VP Swing Screener (Daily)",
        "script":   "vp_swing_screener_daily/vp_swing_screener_daily.py",
        "research": "IS+OOS Sharpe 6.27/6.65 | WR 74.0% | n=2,937 | 53-stock NIFTY50 | daily-bar swing, target=POC, SL=3%, PD=10 | Stage 13 capital Rs.50L | SIGNAL-ONLY, no live orders",
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
        logger.exception("Failed to write command")
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


def _read_jsonl_tail(path: Path, limit: int = 500) -> list[dict]:
    """Return up to the last `limit` parsed records from a JSONL file, oldest→newest."""
    if not path.exists():
        return []
    try:
        with path.open("r") as f:
            lines = deque(f, maxlen=limit)
    except Exception:
        return []
    out = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


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
            entry = {
                "quantity":      int(pos.get("quantity", 0)),
                "average_price": float(pos.get("average_price") or 0),
                "ltp":           float(pos.get("ltp") or 0),
                "pnl":           float(pos.get("pnl") or 0),
                "exchange":      pos.get("exchange", ""),
                "product":       pos.get("product", ""),
                "strategy":      pos.get("strategy", ""),
                # Buy/sell breakdown — non-zero even for flat positions
                "buy_qty":       int(pos.get("buy_qty", 0) or 0),
                "sell_qty":      int(pos.get("sell_qty", 0) or 0),
                "buy_avg":       float(pos.get("buy_avg", 0) or 0),
                "sell_avg":      float(pos.get("sell_avg", 0) or 0),
            }
            if sym not in result:
                result[sym] = entry
                result[sym]["_all"] = [entry]
            else:
                # Same symbol, different strategy — accumulate for flat catch-all
                result[sym]["_all"].append(entry)
                # Keep open position as primary (qty != 0 takes precedence)
                if entry["quantity"] != 0 and result[sym]["quantity"] == 0:
                    strategy_saved = result[sym]["strategy"]
                    all_saved      = result[sym]["_all"]
                    result[sym]    = entry
                    result[sym]["strategy"] = strategy_saved or entry["strategy"]
                    result[sym]["_all"]     = all_saved
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

    # BNF BB Opening Candle bot — three simultaneous legs (BNF CE, BNF PE, SENSEX PE)
    bnf_oc_state = _load(STATE_FILES["BNF_BB_OC"])
    if bnf_oc_state:
        for leg_data in bnf_oc_state.get("legs", {}).values():
            if isinstance(leg_data, dict) and leg_data.get("status") == "ACTIVE":
                _add_opt(leg_data.get("symbol", ""))

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

    # HA Options bot — RETIRED 2026-07-10, failed Stage 11 paper-trading gate

    # NTS + OBI Gate bot — single option leg (ATM CE)
    nts_obi_state = _load(STATE_FILES["NTS_OBI"])
    if nts_obi_state:
        trade = nts_obi_state.get("active_trade")
        if trade and isinstance(trade, dict):
            _add_opt(trade.get("symbol", ""))

    # NIFTY EMA Spread — long + short legs (2-leg debit spread)
    nifty_ema_spread_state = _load(STATE_FILES["NIFTY_EMA_SPREAD"])
    if nifty_ema_spread_state:
        pos = nifty_ema_spread_state.get("position")
        if pos and isinstance(pos, dict):
            _add_opt(pos.get("long_sym",  ""))
            _add_opt(pos.get("short_sym", ""))

    # BANKNIFTY EMA Spread — long + short legs
    bnf_ema_spread_state = _load(STATE_FILES["BANKNIFTY_EMA_SPREAD"])
    if bnf_ema_spread_state:
        pos = bnf_ema_spread_state.get("position")
        if pos and isinstance(pos, dict):
            _add_opt(pos.get("long_sym",  ""))
            _add_opt(pos.get("short_sym", ""))

    # SENSEX EMA Spread — long + short legs
    sensex_ema_spread_state = _load(STATE_FILES["SENSEX_EMA_SPREAD"])
    if sensex_ema_spread_state:
        pos = sensex_ema_spread_state.get("position")
        if pos and isinstance(pos, dict):
            _add_opt(pos.get("long_sym",  ""))
            _add_opt(pos.get("short_sym", ""))

    # NIFTY MACD Map bot — independent PE and CE legs
    macd_map_state = _load(STATE_FILES["NIFTY_MACD_MAP"])
    if macd_map_state:
        for leg_key in ("active_pe", "active_ce"):
            t = macd_map_state.get(leg_key)
            if t and isinstance(t, dict):
                _add_opt(t.get("symbol", ""))

    # MACD M2 Sell Options bot — multi-instrument, keyed by instrument name
    macd_m2_state = _load(STATE_FILES.get("MACD_M2_SELL"))
    if macd_m2_state:
        for pos in macd_m2_state.get("positions", {}).values():
            if isinstance(pos, dict):
                _add_opt(pos.get("opt_symbol", ""))

    # ── Equity bots ───────────────────────────────────────────────────────────
    # Pre-Open Gap Fade — equity positions on NSE (open between 09:15–10:00)
    # Gap Fade Pre-Open — RETIRED 2026-06-24
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

    # ── NIFTY ATM Straddle Scalp — 2 MIS legs (SELL CE, SELL PE) ──────────────
    straddle_state = _load(STATE_FILES["NIFTY_ATM_STRADDLE"])
    if straddle_state and not straddle_state.get("closed", True):
        legs = straddle_state.get("legs", {})
        for leg_key in ("sell_ce", "sell_pe"):
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

    # ── NIFTY GEX ICT V2 — 1 MIS leg (breakout ATM CE or PE) ──────────────────
    gex_ict_v2_state = _load(STATE_FILES["NIFTY_GEX_ICT_V2"])
    if gex_ict_v2_state:
        t = gex_ict_v2_state.get("active_trade")
        if t and isinstance(t, dict):
            _add_opt(t.get("symbol", ""))

    # ── BANKNIFTY Trend Pullback Positional — 1 NRML leg (SELL ATM CE/PE) ─────
    bnf_tpp_state = _load(STATE_FILES["BANKNIFTY_TREND_PULLBACK_POSITIONAL"])
    if bnf_tpp_state:
        pos = bnf_tpp_state.get("position")
        if pos and isinstance(pos, dict):
            _add_opt(pos.get("opt_symbol", ""))

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
        "BNF_BB_OPT", "BNF_BB_OC", "BB_MEAN_REV", "NIFTY_MACD_MAP", "NIFTY_EOD_HOLD", "NTS_OBI",
        "NIFTY_EMA_SPREAD", "BANKNIFTY_EMA_SPREAD", "SENSEX_EMA_SPREAD",
        "IRON_FLY_WEEKLY", "SENSEX_IRON_FLY_WEEKLY", "BNF_IRON_FLY_MONTHLY",
        "MACD_M2_SELL", "NIFTY_ATM_STRADDLE",
        # Stock bots
        # "GAP_FADE",  # RETIRED 2026-06-24
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

@st.cache_data(ttl="2s")
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
                      gross_pnl, net_pnl, exit_reason, hold_duration_mins, decay_pct, won, source
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
            net     = t.get("net_pnl")
            net_val = float(net) if net is not None else None
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
                "Net P&L":  round(net_val, 0) if net_val is not None else round(gross, 0),
                "Reason":   t.get("exit_reason") or "—",
            })
        import pandas as pd
        df = pd.DataFrame(rows)
        pnl_col = "Net P&L"
        st.dataframe(
            df.style
              .map(lambda v: "color: #15803d; font-weight:600" if v > 0
                             else ("color: #dc2626; font-weight:600" if v < 0 else ""),
                   subset=["Gross P&L", "Net P&L"])
              .format({"Entry ₹": "₹{:.2f}", "Exit ₹": "₹{:.2f}",
                       "Gross P&L": lambda v: f"+₹{v:,.0f}" if v >= 0 else f"-₹{abs(v):,.0f}",
                       "Net P&L":   lambda v: f"+₹{v:,.0f}" if v >= 0 else f"-₹{abs(v):,.0f}"}),
            width="stretch", hide_index=True,
        )
        net_total   = sum(float(t.get("net_pnl") or t.get("gross_pnl") or 0) for t in trades)
        gross_total = sum(float(t.get("gross_pnl") or 0) for t in trades)
        wins  = sum(1 for t in trades if t.get("won", 0))
        sc1, sc2, sc3 = st.columns(3)
        pnl_dir = "normal" if net_total >= 0 else "inverse"
        cost_total = gross_total - net_total
        cost_str   = f"  (cost ₹{cost_total:,.0f})" if abs(cost_total) > 1 else ""
        sc1.metric("Total Net P&L",
                   f"{'+'if net_total>=0 else ''}₹{net_total:,.0f}",
                   delta=f"gross ₹{gross_total:+,.0f}{cost_str}",
                   delta_color=pnl_dir)
        sc2.metric("Trades Today", n)
        sc3.metric("Win / Loss", f"{wins} / {n - wins}")
    else:
        # ── Rich card view (single-trade or dual-leg options bots) ────────────
        for t in trades:
            won       = bool(t.get("won", 0))
            gross     = float(t.get("gross_pnl") or 0)
            _net      = t.get("net_pnl")
            net       = float(_net) if _net is not None else None
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
                if net is not None:
                    cost = gross - net
                    net_sign = "+" if net >= 0 else "-"
                    t6.metric(
                        "Net P&L", f"{net_sign}₹{abs(net):,.0f}",
                        delta=f"gross ₹{gross:+,.0f}  cost ₹{cost:,.0f}",
                        delta_color="normal" if net >= 0 else "inverse",
                    )
                else:
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
    open_state_symbols: set[str] = set()  # symbols with active open state (per-bot guard for closed fallback)
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
        open_state_symbols.add(sym)
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

    def _group_rows_by_strategy(rows: list[dict]) -> "OrderedDict[str, list[dict]]":
        """Bucket rows (open or closed) by parent strategy, preserving first-seen order."""
        groups: "OrderedDict[str, list[dict]]" = OrderedDict()
        for r in rows:
            groups.setdefault(_strategy_key(r["Bot"]), []).append(r)
        return groups

    def _summarize_strategy(strategy: str, legs: list[dict]) -> dict:
        """Combined-MTM summary row for one strategy's open legs."""
        since_set = {r["Since"] for r in legs}
        since = next(iter(since_set)) if len(since_set) == 1 else "multiple"
        return {
            "Strategy": strategy, "Legs": len(legs),
            "Combined MTM ₹": sum(r["MTM ₹"] for r in legs), "Since": since,
        }

    def _summarize_closed_strategy(strategy: str, legs: list[dict]) -> dict:
        """Combined-P&L summary row for one strategy's closed legs."""
        reason_set = {r["Reason"] for r in legs}
        reason = next(iter(reason_set)) if len(reason_set) == 1 else "multiple"
        return {
            "Strategy": strategy, "Legs": len(legs),
            "Combined P&L ₹": sum(r["Net P&L ₹"] for r in legs), "Reason": reason,
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
        # Closed today — state clears active_trades on exit; fall back to performance_db
        for t in _load_today_trades("nifty_trend_seller_bot"):
            sym = t["symbol"]
            if sym not in open_state_symbols:
                opt = "PE" if sym.upper().endswith("PE") else "CE"
                _add_closed_from_db(
                    f"Nifty TS ({opt})", sym, f"SELL {opt}",
                    t["entry_price"], t["exit_price"],
                    t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                )

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
        # Closed today — fall back to performance_db
        for t in _load_today_trades("sensex_trend_seller_bot"):
            sym = t["symbol"]
            if sym not in open_state_symbols:
                _add_closed_from_db(
                    "SENSEX TS (CE)", sym, "SELL CE",
                    t["entry_price"], t["exit_price"],
                    t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                )

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
                tgt   = float(active.get("tgt_prem", entry * 0.7))
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
                    t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                )

    # ── BankNifty BB Opening Candle (3 independent simultaneous legs) ──────────
    state = _load(STATE_FILES["BNF_BB_OC"])
    if state and state.get("legs"):
        legs = state["legs"]
        any_closed_today = False
        for leg_key, leg_label, opt_type in (
            ("BNF_CE",    "BANKNIFTY CE", "CE"),
            ("BNF_PE",    "BANKNIFTY PE", "PE"),
            ("SENSEX_PE", "SENSEX PE",    "PE"),
        ):
            leg = legs.get(leg_key, {})
            if not isinstance(leg, dict):
                continue
            status = leg.get("status", "")
            if status == "ACTIVE":
                sym   = leg.get("symbol", "")
                entry = float(leg.get("fill_price", 0))
                sl    = float(leg.get("sl_price", 0))
                tgt   = float((leg.get("bb_now") or {}).get("sma", 0))
                qty   = int(leg.get("qty", 0))
                ltp   = ltps.get(sym, entry)
                pnl   = (entry - ltp) * qty
                since = (leg.get("entry_time") or "")[:16].replace("T", " ")
                _add_open(f"BNF BB OC ({leg_label})", sym, f"SELL {opt_type}", entry, ltp, sl, tgt, qty, pnl, since)
            elif status == "CLOSED":
                any_closed_today = True
        if any_closed_today:
            # Legs close independently and the state doesn't retain exit price/time
            # once CLOSED — use performance_db as the authoritative source, same as
            # every other bot's "closed today" fallback.
            for t in _load_today_trades("banknifty_bb_opening_candle_bot"):
                sym      = t["symbol"]
                opt_type = "PE" if sym.upper().endswith("PE") else "CE"
                _add_closed_from_db(
                    f"BNF BB OC ({opt_type})", sym, "SELL",
                    t["entry_price"], t["exit_price"],
                    t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                )

    # ── HA Options Bot — RETIRED 2026-07-10, failed Stage 11 paper-trading gate ─

    # ── BB Mean Reversion ─────────────────────────────────────────────────────
    state = _load(STATE_FILES["BB_MEAN_REV"])
    if state:
        t = state.get("active_trade")
        if t and isinstance(t, dict):
            sym   = t.get("symbol", "")
            entry = float(t.get("entry_prem", 0))
            sl    = float(t.get("sl_prem", 0))
            qty   = int(t.get("qty", 0))
            ltp   = ltps.get(sym, entry)
            pnl   = (entry - ltp) * qty
            since = t.get("entry_time", "")[:16].replace("T", " ")
            _add_open("BB Mean Rev", sym, "SELL PE", entry, ltp, sl, 0.0, qty, pnl, since)
        # Closed today — fall back to performance_db
        for t in _load_today_trades("bb_mean_reversion_bot"):
            sym = t["symbol"]
            if sym not in open_state_symbols:
                opt = "PE" if sym.upper().endswith("PE") else "CE"
                _add_closed_from_db(
                    f"BB Mean Rev ({opt})", sym, f"SELL {opt}",
                    t["entry_price"], t["exit_price"],
                    t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                )

    # ── NTS + OBI Gate ────────────────────────────────────────────────────────
    state = _load(STATE_FILES["NTS_OBI"])
    if state:
        t = state.get("active_trade")
        if t and isinstance(t, dict):
            sym   = t.get("symbol", "")
            entry = float(t.get("entry_prem", 0))
            sl    = float(t.get("sl_prem", 0))
            qty   = int(t.get("qty", 0))
            ltp   = ltps.get(sym, entry)
            pnl   = (entry - ltp) * qty
            since = t.get("entry_time", "")[:16].replace("T", " ")
            _add_open("NTS+OBI", sym, "SELL CE", entry, ltp, sl, 0.0, qty, pnl, since)
        # Closed today — fall back to performance_db
        for t in _load_today_trades("NTS_OBI"):
            sym = t["symbol"]
            if sym not in open_state_symbols:
                _add_closed_from_db(
                    "NTS+OBI", sym, "SELL CE",
                    t["entry_price"], t["exit_price"],
                    t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                )

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
        # Closed today — state clears active_pe/active_ce on exit; fall back to performance_db
        for t in _load_today_trades("nifty_macd_map_bot"):
            sym = t["symbol"]
            if sym not in open_state_symbols:
                opt = "PE" if sym.upper().endswith("PE") else "CE"
                _add_closed_from_db(
                    f"MACD Map ({opt})", sym, f"SELL {opt}",
                    t["entry_price"], t["exit_price"],
                    t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                )

    # ── BANKNIFTY Trend Pullback Positional (NRML positional, single ATM leg) ──
    state = _load(STATE_FILES["BANKNIFTY_TREND_PULLBACK_POSITIONAL"])
    if state:
        pos = state.get("position")
        if pos and isinstance(pos, dict):
            sym      = pos.get("opt_symbol", "")
            opt_type = pos.get("opt_type", "?")
            entry    = float(pos.get("credit", 0))
            sl       = float(pos.get("sl_level", 0))
            tgt      = float(pos.get("tgt_level", 0))
            qty      = int(pos.get("quantity", 0))
            ltp      = ltps.get(sym, entry)
            pnl      = (entry - ltp) * qty
            since    = (pos.get("entry_time") or "")[:16].replace("T", " ")
            _add_open(f"BNF Trend Pullback ({opt_type})", sym, f"SELL {opt_type}",
                      entry, ltp, sl, tgt, qty, pnl, since)
        # Closed today — state clears position on exit; fall back to performance_db
        for t in _load_today_trades("banknifty_trend_pullback_positional_bot"):
            sym = t["symbol"]
            if sym not in open_state_symbols:
                opt = "PE" if sym.upper().endswith("PE") else "CE"
                _add_closed_from_db(
                    f"BNF Trend Pullback ({opt})", sym, f"SELL {opt}",
                    t["entry_price"], t["exit_price"],
                    t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                )

    # ── EMA Spread Bots (NRML 2-leg debit spread — long ATM + short OTM) ───────
    for _ema_key, _ema_label, _ema_bot in (
        ("NIFTY_EMA_SPREAD",    "NIFTY EMA Spread",    "nifty_ema_spread_bot"),
        ("BANKNIFTY_EMA_SPREAD","BANKNIFTY EMA Spread","banknifty_ema_spread_bot"),
        ("SENSEX_EMA_SPREAD",   "SENSEX EMA Spread",   "sensex_ema_spread_bot"),
    ):
        state = _load(STATE_FILES[_ema_key])
        if state:
            pos = state.get("position")
            if pos and isinstance(pos, dict):
                long_sym  = pos.get("long_sym", "")
                short_sym = pos.get("short_sym", "")
                entry_deb = float(pos.get("entry_debit", 0))
                qty       = int(pos.get("qty", 0))
                entry_t   = (pos.get("entry_time") or "")[:16].replace("T", " ")
                long_ltp  = ltps.get(long_sym, 0.0)
                short_ltp = ltps.get(short_sym, 0.0)
                spread_val= long_ltp - short_ltp
                pnl       = (spread_val - entry_deb) * qty
                _add_open(f"{_ema_label} (Long)", long_sym, "BUY  CE/PE",
                          entry_deb, long_ltp, 0.0, 0.0, qty, pnl, entry_t)
                _add_open(f"{_ema_label} (Short)", short_sym, "SELL CE/PE",
                          0.0, short_ltp, 0.0, 0.0, qty, 0.0, entry_t)
            # Closed today — fall back to performance_db
            for t in _load_today_trades(_ema_bot):
                sym = t["symbol"]
                if sym not in open_state_symbols:
                    opt = "PE" if sym.upper().endswith("PE") else "CE"
                    _add_closed_from_db(
                        _ema_label, sym, f"SPREAD {opt}",
                        t["entry_price"], t["exit_price"],
                        t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                    )

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
            ("sell_ce", "NIFTY Iron Fly (Sell CE)", "SELL CE"),
            ("sell_pe", "NIFTY Iron Fly (Sell PE)", "SELL PE"),
            ("buy_ce",  "NIFTY Iron Fly (Buy CE)",  "BUY  CE"),
            ("buy_pe",  "NIFTY Iron Fly (Buy PE)",  "BUY  PE"),
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
                    _add_closed(f"NIFTY Iron Fly ({leg_key})", sym, side, entry, exit_px, qty, pnl, reason)
            else:
                _add_open(f"NIFTY Iron Fly ({leg_key})", sym, side, entry, ltp, sl, tgt, qty, pnl, entry_t)

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

    # ── NIFTY ATM Straddle Scalp (MIS 2-leg short straddle, breakeven trail) ────
    state = _load(STATE_FILES["NIFTY_ATM_STRADDLE"])
    if state and state.get("legs"):
        qty       = int(state.get("qty", 0))
        legs      = state.get("legs", {})
        entry_t   = (state.get("entry_time") or "")[:16].replace("T", " ")
        target_rs = float(state.get("target_rs", 0))
        closed    = state.get("closed", False)
        exit_rsn  = state.get("exit_reason")

        for leg_key, side in (("sell_ce", "SELL CE"), ("sell_pe", "SELL PE")):
            leg = legs.get(leg_key, {})
            if not leg:
                continue
            sym        = leg.get("symbol", "")
            entry      = float(leg.get("entry_prem", 0))
            leg_closed = leg.get("closed", False)

            if leg_closed:
                exit_px = float(leg.get("exit_prem", entry))
                pnl     = (entry - exit_px) * qty
                _add_closed(f"NIFTY ATM Straddle ({leg_key})", sym, side, entry, exit_px, qty, pnl,
                            leg.get("exit_reason") or exit_rsn or "exited")
            else:
                ltp = ltps.get(sym, entry)
                pnl = (entry - ltp) * qty
                sl  = float(leg.get("sl_level", entry * 1.20))
                _add_open(f"NIFTY ATM Straddle ({leg_key})", sym, side, entry, ltp, sl, target_rs, qty, pnl, entry_t)

    # ── SENSEX Iron Fly Weekly (NRML 4-leg short iron fly, BFO) ─────────────────
    state = _load(STATE_FILES["SENSEX_IRON_FLY_WEEKLY"])
    if state and state.get("legs"):
        qty      = int(state.get("qty", 0))
        legs     = state.get("legs", {})
        entry_t  = (state.get("entry_time") or "")[:16].replace("T", " ")
        net_cr   = float(state.get("net_credit_per_unit", 0))
        closed   = state.get("closed", False)
        exit_rsn = state.get("exit_reason")

        for leg_key, label, side in (
            ("sell_ce", "SENSEX IF (sell_ce)", "SELL CE"),
            ("sell_pe", "SENSEX IF (sell_pe)", "SELL PE"),
            ("buy_ce",  "SENSEX IF (buy_ce)",  "BUY  CE"),
            ("buy_pe",  "SENSEX IF (buy_pe)",  "BUY  PE"),
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
                exit_time_str = (state.get("exit_time") or "")
                exited_today  = exit_time_str[:10] == datetime.now().date().isoformat()
                if exited_today:
                    _add_closed(label, sym, side, entry, ltp, qty, pnl, exit_rsn or "exited")
            else:
                _add_open(label, sym, side, entry, ltp, sl, tgt, qty, pnl, entry_t)

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

    # Gap Fade Pre-Open — RETIRED 2026-06-24 (no post-cost edge on equity intraday)
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
        # Closed today — fall back to performance_db
        for t in _load_today_trades("nifty_ma_cross_seller_bot"):
            sym = t["symbol"]
            if sym not in open_state_symbols:
                opt = t.get("option_type") or ("PE" if sym.upper().endswith("PE") else "CE")
                _add_closed_from_db(
                    f"MA Cross ({opt})", sym, f"SELL {opt}",
                    t["entry_price"], t["exit_price"],
                    t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                )

    # ── MACD M2 Sell Options (multi-instrument, keyed by instrument name) ────
    macd_m2_state = _load(STATE_FILES.get("MACD_M2_SELL"))
    if macd_m2_state:
        for inst, pos in macd_m2_state.get("positions", {}).items():
            if not isinstance(pos, dict):
                continue
            sym      = pos.get("opt_symbol", "")
            opt_type = pos.get("opt_type", "?")
            entry    = float(pos.get("credit", 0))
            sl       = float(pos.get("sl_level", 0))
            tgt      = float(pos.get("tgt_level", 0))
            qty      = int(pos.get("quantity", 0))
            ltp      = ltps.get(sym, entry)
            pnl      = (entry - ltp) * qty
            since    = pos.get("entry_time", "")[:16].replace("T", " ")
            _add_open(
                f"MACD M2 Sell ({inst})", sym, f"SELL {opt_type}",
                entry, ltp, sl, tgt, qty, pnl, since,
            )
        # Closed today — fall back to performance_db
        for t in _load_today_trades("macd_m2_sell_options_bot"):
            sym = t["symbol"]
            if sym not in open_state_symbols:
                opt = t.get("option_type") or ("PE" if sym.upper().endswith("PE") else "CE")
                _add_closed_from_db(
                    f"MACD M2 Sell ({opt})", sym, f"SELL {opt}",
                    t["entry_price"], t["exit_price"],
                    t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                )

    # ── NIFTY GEX ICT V2 (breakout-only, sell-side, 1 leg: ATM CE or PE) ─────
    gex_ict_v2_state = _load(STATE_FILES["NIFTY_GEX_ICT_V2"])
    if gex_ict_v2_state:
        t = gex_ict_v2_state.get("active_trade")
        if t and isinstance(t, dict):
            sym      = t.get("symbol", "")
            opt_type = t.get("opt_type", "CE")
            entry    = float(t.get("entry_prem", 0))
            sl       = float(t.get("spot_stop", 0) or 0)
            tgt      = float(t.get("spot_target1", 0) or 0) if t.get("spot_target1") is not None else 0.0
            qty      = int(t.get("qty", 0))
            ltp      = ltps.get(sym, entry)
            pnl      = (entry - ltp) * qty
            since    = t.get("entry_time", "")[:16].replace("T", " ")
            _add_open(
                f"GEX ICT V2 ({opt_type})", sym, f"SELL {opt_type}",
                entry, ltp, sl, tgt, qty, pnl, since,
            )
        # Closed today — fall back to performance_db
        for t in _load_today_trades("nifty_gex_ict_v2_bot"):
            sym = t["symbol"]
            if sym not in open_state_symbols:
                opt = t.get("option_type") or ("PE" if sym.upper().endswith("PE") else "CE")
                _add_closed_from_db(
                    f"GEX ICT V2 ({opt})", sym, f"SELL {opt}",
                    t["entry_price"], t["exit_price"],
                    t["quantity"], t.get("net_pnl") or t["gross_pnl"], t["exit_reason"],
                )

    # ── Equity OBI Bot — RETIRED 2026-06-19 ───────────────────────────────────

    # ── VP Swing Screener (signal-only — positions reflect manually-executed
    #    candidates, reconciled against the real broker positionbook by symbol
    #    exactly like every order-placing bot's block above; the screener
    #    itself never calls placeorder()) ────────────────────────────────────
    vp_state = _load(STATE_FILES.get("VP_SWING_SCREENER"))
    if vp_state:
        for pos in vp_state.get("open_positions", []):
            sym    = pos.get("symbol", "")
            entry  = float(pos.get("entry_price") or 0)
            stop   = float(pos.get("stop") or 0)
            target = float(pos.get("target_poc") or 0)
            qty    = int(pos.get("qty") or 0)
            ltp    = ltps.get(sym, entry)
            pnl    = (ltp - entry) * qty  # long equity
            since  = pos.get("since", "")
            _add_open("VP Swing Screener", sym, "BUY", entry, ltp, stop, target, qty, pnl, since)
        # No performance_db fallback here: the screener never logs trades via
        # log_trade_to_db() (see live_trading/shared/bot_registry.py's own
        # docstring re: bots with zero performance.db rows) — _add_open's
        # built-in stale-state detection (positionbook flat, state still
        # listing the symbol) is the only "closed" signal available for it.

    # ── VP Swing Screener (Daily) — same signal-only reconciliation pattern
    #    as the 60-min screener above, just against its own state file. ────
    vp_daily_state = _load(STATE_FILES.get("VP_SWING_SCREENER_DAILY"))
    if vp_daily_state:
        for pos in vp_daily_state.get("open_positions", []):
            sym    = pos.get("symbol", "")
            entry  = float(pos.get("entry_price") or 0)
            stop   = float(pos.get("stop") or 0)
            target = float(pos.get("target_poc") or 0)
            qty    = int(pos.get("qty") or 0)
            ltp    = ltps.get(sym, entry)
            pnl    = (ltp - entry) * qty  # long equity
            since  = pos.get("since", "")
            _add_open("VP Swing Screener (Daily)", sym, "BUY", entry, ltp, stop, target, qty, pnl, since)

    # ── Untracked positions (positionbook entries with no matching state file) ──
    # Appears when: position placed manually from OpenAlgo UI, bot crashed before
    # writing state, or a bot not yet integrated into the dashboard.
    _STRATEGY_LABEL = {
        "HTF_PO3_SELL_PE":          "HTF PO3",
        "HTF_PO3_SELL_CE":          "HTF PO3",
        "HTF_PO3_MANUAL_CLOSE":     "HTF PO3 (manual)",
        "HA_OPTIONS":               "HA Options",
        "NIFTY_IRON_FLY_WEEKLY":    "NIFTY Iron Fly",
        "SENSEX_IRON_FLY_WEEKLY":   "SENSEX Iron Fly",
        "BNF_IRON_FLY_MONTHLY":     "BNF Iron Fly",
        "SENSEX_TREND_SELLER":      "SENSEX TS",
        "NIFTY_TREND_SELLER":       "Nifty TS",
        "BB_OVERBOUGHT":            "BB Overbought",
        "BNF_BB_OC":                "BNF BB OC",
        "MACD_M2_SELL_OPTIONS":     "MACD M2 Sell",
        "NIFTY_GEX_ICT_V2":         "NIFTY GEX ICT V2",
        "NIFTY_ATM_STRADDLE_SCALP": "NIFTY ATM Straddle",
    }

    def _pb_bot_label(pb_entry: dict) -> str:
        strat = (pb_entry.get("strategy") or "").strip()
        return _STRATEGY_LABEL.get(strat, f"📊 Broker" if not strat else f"📊 {strat}")

    if pb_available:
        for sym, pb in positionbook.items():
            if sym in state_symbols:
                continue  # already accounted for above
            # Iterate all per-strategy entries for this symbol (handles same symbol
            # appearing under multiple strategies, e.g. an orphaned trade + manual close)
            for pb_entry in pb.get("_all", [pb]):
                qty = pb_entry["quantity"]
                ltp = pb_entry["ltp"]
                pnl = pb_entry["pnl"]
                avg = pb_entry["average_price"]
                bot_label = _pb_bot_label(pb_entry)
                if qty != 0:
                    # Still open — show as an untracked open position.
                    open_side     = "SELL" if qty < 0 else "BUY"
                    buy_avg_open  = pb_entry.get("buy_avg", 0.0)
                    sell_avg_open = pb_entry.get("sell_avg", 0.0)
                    if open_side == "SELL" and sell_avg_open:
                        entry_open = sell_avg_open
                    elif open_side == "BUY" and buy_avg_open:
                        entry_open = buy_avg_open
                    else:
                        entry_open = avg
                    open_rows.append({
                        "Bot": bot_label, "Symbol": sym, "Side": open_side,
                        "Entry ₹": entry_open, "LTP ₹": ltp,
                        "SL ₹": 0.0, "TGT ₹": 0.0,
                        "Qty": abs(qty), "MTM ₹": pnl, "Since": "—",
                    })
                elif pnl != 0:
                    # Flat with non-zero realized P&L — show as closed broker position.
                    buy_qty   = pb_entry.get("buy_qty", 0)
                    sell_qty  = pb_entry.get("sell_qty", 0)
                    buy_avg   = pb_entry.get("buy_avg", 0.0)
                    sell_avg  = pb_entry.get("sell_avg", 0.0)
                    is_option = sym.upper().endswith(("CE", "PE"))
                    if sell_qty > buy_qty:
                        side       = "SELL"
                        entry_px   = sell_avg
                        traded_qty = sell_qty
                    elif buy_qty > sell_qty:
                        side       = "BUY"
                        entry_px   = buy_avg
                        traded_qty = buy_qty
                    else:
                        if is_option:
                            side, entry_px, traded_qty = "SELL", sell_avg, sell_qty
                        else:
                            side, entry_px, traded_qty = "BUY", buy_avg, buy_qty
                    if traded_qty == 0:
                        continue  # round-trip with no qty breakdown — skip (noise)
                    closed_rows.append({
                        "Bot": bot_label, "Symbol": sym, "Side": side,
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
        leg_groups   = _group_rows_by_strategy(open_rows)
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
        closed_leg_groups = _group_rows_by_strategy(closed_rows)
        grouped_closed    = [_summarize_closed_strategy(name, legs) for name, legs in closed_leg_groups.items()]
        st.markdown(
            f"#### 📁 Closed Positions &nbsp; "
            f"<span style='font-size:0.8em;color:#3f5a80'>"
            f"({len(grouped_closed)} {'strategy' if len(grouped_closed)==1 else 'strategies'} · "
            f"{n_total} {'trade' if n_total==1 else 'trades'} · {n_wins}W / {n_total-n_wins}L · WR {wr_pct:.0f}%)</span>",
            unsafe_allow_html=True,
        )
        df_grouped_closed = pd.DataFrame(grouped_closed)
        styled_grouped_closed = (
            df_grouped_closed.style
            .map(_color_pnl, subset=["Combined P&L ₹"])
            .format({"Combined P&L ₹": _fmt_pnl})
        )
        st.dataframe(styled_grouped_closed, width='stretch', hide_index=True)
        st.markdown(_subtotal_html("Realized subtotal", realized), unsafe_allow_html=True)

        # One expander per multi-leg strategy — click to reveal just its own trades.
        closed_leg_cols = ["Bot", "Symbol", "Side", "Entry ₹", "Exit ₹", "Qty", "Net P&L ₹", "Reason"]
        for name, legs in closed_leg_groups.items():
            if len(legs) <= 1:
                continue
            combined = sum(r["Net P&L ₹"] for r in legs)
            sign = "+" if combined >= 0 else ""
            with st.expander(f"{name} — {len(legs)} legs — {sign}₹{combined:,.0f}"):
                df_leg = pd.DataFrame(legs)[closed_leg_cols]
                styled_leg = (
                    df_leg.style
                    .map(_color_pnl, subset=["Net P&L ₹"])
                    .format({
                        "Entry ₹":   _fmt_px,
                        "Exit ₹":    _fmt_px,
                        "Net P&L ₹": _fmt_pnl,
                    })
                )
                st.dataframe(styled_leg, width='stretch', hide_index=True)

        with st.expander(f"All closed trades ({n_total})"):
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
#  SHARED STRATEGY-TAB FRAMEWORK
#  ----------------------------------------------------------------------------
#  Every bot panel exposes three tabs — Overview · Strategy Flowchart · Live
#  Decision State. The flowchart and decision-state tabs are generated from a
#  compact per-bot spec by the helpers below so all bots stay visually
#  consistent with the NIFTY MACD Map / MA Cross reference panels and there is
#  ONE place to evolve the styling. Do not hand-roll per-bot HTML for these two
#  tabs — feed a spec to render_strategy_flowchart() / render_decision_state().
# ══════════════════════════════════════════════════════════════════════════════

_FLOW_CSS = """
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
  .fc-row    { display: flex; align-items: center; gap: 6px; margin: 4px 0; justify-content: center; }
  .fc-col    { display: flex; flex-direction: column; align-items: center; gap: 0; }
  .fc-split  { display: flex; gap: 24px; align-items: flex-start; justify-content: center; margin-top: 8px; }
  .fc-branch { display: flex; flex-direction: column; align-items: center; gap: 4px; }
  .fc-yes    { color: #4ade80; font-size: 11px; font-weight: 700; }
  .fc-no     { color: #f87171; font-size: 11px; font-weight: 700; }
</style>
"""

_FLOW_LEGEND = """
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
"""


def _fc_sub(sub: str) -> str:
    return f'<br><span style="font-weight:400;font-size:11px">{sub}</span>' if sub else ""


# --- Flowchart step builders. Each returns (row_html, outgoing_arrow_label). ---
def fc_start(text: str):
    return (f'<div class="fc-node fc-start">{text}</div>', "↓")

def fc_action(text: str, sub: str = ""):
    return (f'<div class="fc-node fc-action">{text}{_fc_sub(sub)}</div>', "↓")

def fc_check(text: str, sub: str = ""):
    return (f'<div class="fc-node fc-check">{text}{_fc_sub(sub)}</div>', "↓")

def fc_monitor(text: str, sub: str = ""):
    return (f'<div class="fc-node fc-monitor">{text}{_fc_sub(sub)}</div>', "↓")

def fc_entry(text: str, sub: str = ""):
    return (f'<div class="fc-node fc-entry">{text}{_fc_sub(sub)}</div>', "↓")

def fc_exit(text: str, sub: str = ""):
    return (f'<div class="fc-node fc-exit">{text}{_fc_sub(sub)}</div>', "↓")

def fc_filter(text: str, no_label: str = "Skip / no trade"):
    """A gating check with an inline 'NO → blocked' branch; flow continues on YES."""
    html = (
        f'<div class="fc-node fc-check" style="font-size:11px">{text}</div>'
        f'<div class="fc-no" style="margin-left:6px">NO →</div>'
        f'<div class="fc-node fc-block" style="font-size:11px;margin-left:6px">{no_label}</div>'
    )
    return (html, "↓ YES")

def fc_split(left_title: str, left_node: str, right_title: str, right_node: str):
    """Terminal two-way branch, e.g. bullish→SELL PE / bearish→SELL CE."""
    html = (
        '<div class="fc-split">'
        f'<div class="fc-branch"><div class="fc-no">{left_title}</div>'
        f'<div class="fc-arrow">↓</div>{left_node}</div>'
        f'<div class="fc-branch"><div class="fc-yes">{right_title}</div>'
        f'<div class="fc-arrow">↓</div>{right_node}</div>'
        '</div>'
    )
    return (html, "↓")

def fc_node_exit(text: str, sub: str = ""):
    return f'<div class="fc-node fc-exit">{text}{_fc_sub(sub)}</div>'

def fc_node_entry(text: str, sub: str = ""):
    return f'<div class="fc-node fc-entry">{text}{_fc_sub(sub)}</div>'

def fc_note(text: str):
    html = (
        '<div class="fc-node" style="background:#7b61ff22;border:1px solid #7b61ff;'
        f'color:#c4b5fd;font-size:12px">{text}</div>'
    )
    return (html, "↓")


def render_strategy_flowchart(title: str, caption: str, steps: list):
    """Render a vertical flowchart from a list of (html, arrow) step tuples."""
    rows = []
    for i, (html, arrow) in enumerate(steps):
        rows.append(f'<div class="fc-row">{html}</div>')
        if i < len(steps) - 1:
            rows.append(f'<div class="fc-arrow">{arrow}</div>')
    body = "".join(rows)
    full = f'{_FLOW_CSS}<div class="fc-wrap"><div class="fc-col">{body}</div>{_FLOW_LEGEND}</div>'
    st.markdown(f"#### {title}")
    st.caption(caption)
    st.iframe(full, height="content")


def _decision_frow(icon: str, name: str, ok: bool, note: str = ""):
    colour = "#00c875" if ok else "#f87171"
    badge = "✅ PASS" if ok else "❌ BLOCK"
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


def render_readiness(icon: str, msg: str, colour: str):
    st.markdown(
        f'<div style="background:{colour}22;border:1.5px solid {colour};'
        f'border-radius:10px;padding:16px 20px;font-size:1em;">'
        f'<span style="font-size:1.5em">{icon}</span> '
        f'<strong style="color:{colour}">{msg}</strong>'
        f'</div>',
        unsafe_allow_html=True,
    )


def render_decision_state(
    state: dict,
    *,
    key: str,
    updates_note: str,
    metrics: list,
    filters: list,
    readiness: tuple,
    checklist_title: str = "🔍 Entry Filter Checklist",
    checklist_caption: str = "All gates must be GREEN for a signal to fire",
):
    """Render the Live Decision State tab from a per-bot spec.

    metrics  : list of (label, value) | (label, value, delta) | (label, value, delta, delta_color)
    filters  : list of (icon, name, ok_bool, note)
    readiness: (icon, msg, colour)
    """
    if not state:
        st.error("🔌 Bot not running — state file absent. Start the bot to see live decision data.")
        return

    updated_at = state.get("updated_at") or state.get("last_update", "")
    updated_fmt = updated_at[:19].replace("T", " ") if updated_at else "—"
    st.caption(f"State file last written: **{updated_fmt}** · {updates_note}")

    if st.button("🔄 Refresh Now", key=f"refresh_{key}"):
        st.rerun()

    st.markdown("---")

    if metrics:
        for i in range(0, len(metrics), 4):
            chunk = metrics[i:i + 4]
            cols = st.columns(len(chunk))
            for col, m in zip(cols, chunk):
                label, value = m[0], m[1]
                delta = m[2] if len(m) > 2 else None
                dcolor = m[3] if len(m) > 3 else "off"
                if delta is None:
                    col.metric(label, value)
                else:
                    col.metric(label, value, delta=delta, delta_color=dcolor)
        st.markdown("---")

    st.markdown(f"### {checklist_title}")
    if checklist_caption:
        st.caption(checklist_caption)
    for icon, name, ok, note in filters:
        _decision_frow(icon, name, ok, note)

    st.markdown("---")
    st.markdown("### 🎯 Signal Readiness")
    render_readiness(*readiness)


# ══════════════════════════════════════════════════════════════════════════════
#  PERFORMANCE HUB — shared config, fleet status, per-bot tab, hub page
# ══════════════════════════════════════════════════════════════════════════════

# Bot lifecycle config — single source of truth is live_trading/shared/bot_registry.py,
# shared with performance_review.py. Do not hand-maintain a separate list here; edit
# the registry instead. This derives the "retired" display string from status_date.
import sys as _sys
_sys.path.insert(0, str(Path(__file__).parent.parent))
from live_trading.shared.bot_registry import (
    BOT_REGISTRY, BOT_META as _REGISTRY_BOT_META, WORKSPACE as _REGISTRY_HOME_WS,
    status_in_workspace as _status_in_workspace,
)

def _fmt_registry_date(iso: str | None) -> str:
    if not iso:
        return ""
    y, m, d = iso.split("-")
    months = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    return f"{d} {months[int(m)]} {y}"

BOT_LIFECYCLE: list[dict] = [
    {**b, "retired": _fmt_registry_date(b.get("status_date"))} if b["status"] == "retired" else dict(b)
    for b in BOT_REGISTRY
]

STAGE11_TARGET = 20


@st.cache_data(ttl=300)
def _load_perf_fleet(ws: str, days: int = 30):
    """Return (sessions_dict, bots_dict) from the selected workspace's performance.db; cached 5 min."""
    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).parent.parent))
        from live_trading.performance_review import (
            fetch_stage11_sessions, fetch_per_bot, _connect,
        )
        from datetime import date, timedelta
        db_path = WORKSPACES[ws]["root"] / "live_trading" / "logs" / "performance.db"
        con = _connect(db_path)
        sessions = fetch_stage11_sessions(con)
        end = date.today()
        start = end - timedelta(days=days - 1)
        bots = {b["bot_name"]: b for b in fetch_per_bot(con, start, end)}
        con.close()
        return sessions, bots
    except Exception:
        return {}, {}


@st.cache_data(ttl=300)
def _load_perf_bot(ws: str, bot_name: str, days: int):
    """Per-bot perf data for the Performance tab; cached 5 min."""
    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).parent.parent))
        from live_trading.performance_review import (
            fetch_portfolio_summary, fetch_daily_pnl,
            fetch_exit_reasons, fetch_recent_trades, _connect,
        )
        from datetime import date, timedelta
        db_path = WORKSPACES[ws]["root"] / "live_trading" / "logs" / "performance.db"
        con = _connect(db_path)
        end = date.today()
        start = end - timedelta(days=days - 1)
        summary = fetch_portfolio_summary(con, start, end, bot_filter=bot_name)
        daily   = fetch_daily_pnl(con, start, end, bot_filter=bot_name)
        reasons = fetch_exit_reasons(con, start, end, bot_filter=bot_name)
        recent  = fetch_recent_trades(con, 500, start, end, bot_filter=bot_name)
        con.close()
        return summary, daily, reasons, recent
    except Exception:
        return {}, [], [], []


def _fmt_pnl(val, sign=True):
    if val is None:
        return "—"
    if val >= 0:
        prefix = "+" if sign else ""
        return f"{prefix}₹{val:,.0f}"
    return f"-₹{abs(val):,.0f}"


def _fmt_hold(mins):
    """Convert hold_duration_mins to human-readable string.
    Short (intraday): '47m' or '1h 23m'.
    Long (positional): '3d 4h' or '12d 6h'.
    """
    if not mins:
        return "—"
    mins = int(mins)
    if mins < 60:
        return f"{mins}m"
    hours, rem_m = divmod(mins, 60)
    if hours < 24:
        return f"{hours}h {rem_m}m" if rem_m else f"{hours}h"
    days, rem_h = divmod(hours, 24)
    return f"{days}d {rem_h}h" if rem_h else f"{days}d"


# Bots that hold positions for multiple days — trade table grouped by session date.
_POSITIONAL_BOTS = {
    "nifty_iron_fly_weekly_bot",
    "sensex_iron_fly_weekly_bot",
    "banknifty_iron_fly_monthly_bot",
    "flat_blue_line_monthly_bot",
}


def render_fleet_status():
    """Fleet status swimlane rendered on the Dashboard Overview page."""
    ws = st.session_state.get("selected_workspace_top", "CRK")
    sessions, bots_30d = _load_perf_fleet(ws, days=30)

    live  = [b for b in BOT_LIFECYCLE if _status_in_workspace(b, ws) == "live"]
    paper = [b for b in BOT_LIFECYCLE if _status_in_workspace(b, ws) == "paper"]

    col_l, col_p = st.columns(2)

    with col_l:
        st.markdown("##### 🟢 Live — real money")
        if not live:
            st.caption("No bots deployed live in this account yet.")
        for b in live:
            s    = sessions.get(b["bot"], 0)
            perf = bots_30d.get(b["bot"], {})
            gpnl = perf.get("gross_pnl")
            wr   = perf.get("win_rate")
            with st.container(border=True):
                if b.get("paused"):
                    st.markdown(f"**{b['label']}** &nbsp;⏸️ <span style='color:#f0a020;font-size:0.8em'>PAUSED</span>",
                                unsafe_allow_html=True)
                    if b.get("paused_reason"):
                        st.caption(f"⏸️ {b['paused_reason']}" + (f" ({b['paused_date']})" if b.get("paused_date") else ""))
                else:
                    st.markdown(f"**{b['label']}**")
                c1, c2, c3 = st.columns(3)
                c1.metric("30d P&L",   _fmt_pnl(gpnl))
                c2.metric("Win rate",  f"{wr:.0f}%" if wr is not None else "—")
                c3.metric("Sessions",  f"{s} ✓")

    with col_p:
        st.markdown("##### 🔬 Paper — Stage 11")
        for b in paper:
            s    = sessions.get(b["bot"], 0)
            perf = bots_30d.get(b["bot"], {})
            gpnl = perf.get("gross_pnl")
            with st.container(border=True):
                st.markdown(f"**{b['label']}**")
                c1, c2 = st.columns([3, 2])
                c1.progress(min(s / STAGE11_TARGET, 1.0), text=f"{s}/{STAGE11_TARGET}")
                c2.markdown(
                    f"<small style='color:gray'>{_fmt_pnl(gpnl)}</small>",
                    unsafe_allow_html=True,
                )


def render_bot_performance_tab(bot_name: str | None):
    """Content for the 📈 Performance tab on any bot panel page."""
    if bot_name is None:
        st.info("No performance tracking configured for this bot yet.")
        return

    days_opt = st.radio(
        "Period", ["7 days", "30 days", "90 days"], index=1,
        horizontal=True, key=f"perf_days_{bot_name}",
    )
    days = {"7 days": 7, "30 days": 30, "90 days": 90}[days_opt]

    ws = st.session_state.get("selected_workspace_top", "CRK")
    summary, daily, reasons, recent = _load_perf_bot(ws, bot_name, days)

    if not summary:
        st.info(f"No trades recorded for **{bot_name}** in the last {days} days.")
        return

    total   = summary.get("total_trades", 0) or 0
    wins    = summary.get("wins", 0) or 0
    gpnl    = summary.get("gross_pnl") or 0
    apnl    = summary.get("avg_pnl") or 0
    best_d  = summary.get("best_day")
    worst_d = summary.get("worst_day")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Gross P&L",  _fmt_pnl(gpnl))
    c2.metric("Win rate",   f"{wins/total*100:.1f}%" if total else "—")
    c3.metric("Trades",     total)
    c4.metric("Avg/trade",  _fmt_pnl(apnl))
    c5.metric("Best day",   _fmt_pnl(best_d[1]  if best_d  else None))
    c6.metric("Worst day",  _fmt_pnl(worst_d[1] if worst_d else None))

    if daily:
        st.markdown("**Daily P&L**")
        df_d = pd.DataFrame(daily, columns=["date", "pnl", "trades"])
        df_d["date"] = pd.to_datetime(df_d["date"])
        st.bar_chart(df_d.set_index("date")[["pnl"]], height=200)

    if reasons:
        st.markdown("**Exit reasons**")
        df_r = pd.DataFrame(reasons, columns=["Reason", "Count", "Wins", "P&L"])
        df_r["Win %"] = (df_r["Wins"] / df_r["Count"] * 100).round(1).astype(str) + "%"
        df_r["P&L"]   = df_r["P&L"].apply(_fmt_pnl)
        st.dataframe(df_r[["Reason", "Count", "Win %", "P&L"]], width="stretch", hide_index=True)

    if recent:
        MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        def _dt(raw_ts):
            raw_ts = str(raw_ts)
            try:
                m = int(raw_ts[5:7])
                return f"{raw_ts[8:10]} {MONTHS[m]} {raw_ts[11:16]}"
            except Exception:
                return raw_ts[:16]

        def _date_only(raw_ts):
            raw_ts = str(raw_ts)
            try:
                m = int(raw_ts[5:7])
                return f"{raw_ts[8:10]} {MONTHS[m]} {raw_ts[0:4]}"
            except Exception:
                return raw_ts[:10]

        if bot_name in _POSITIONAL_BOTS:
            # ── Positional bots: group legs by session date ───────────────────
            st.markdown("**Position log** (grouped by session)")
            from collections import defaultdict
            sessions_map = defaultdict(list)
            for r in recent:
                date_key = str(r[0])[:10]   # YYYY-MM-DD
                sessions_map[date_key].append(r)

            for date_key in sorted(sessions_map, reverse=True):
                legs = sessions_map[date_key]
                session_pnl = sum((r[7] or 0) for r in legs)
                won_session  = session_pnl >= 0
                pnl_str      = _fmt_pnl(session_pnl)
                outcome_icon = "🟢" if won_session else "🔴"

                # Hold: from earliest entry to latest exit (or max hold)
                max_hold = max((r[10] or 0) for r in legs)
                hold_str = _fmt_hold(max_hold)

                # Entry date display
                raw_date = legs[0][0]
                date_disp = _date_only(raw_date)

                with st.expander(
                    f"{outcome_icon} {date_disp}  ·  {pnl_str}  ·  held {hold_str}",
                    expanded=False,
                ):
                    leg_rows = []
                    for r in sorted(legs, key=lambda x: str(x[0])):
                        # Exit time — prefer DB exit_time (r[11]), fall back to computed
                        exit_dt_str = "—"
                        try:
                            raw_exit = str(r[11]) if (len(r) > 11 and r[11]) else None
                            if raw_exit and raw_exit != "None":
                                em = int(raw_exit[5:7])
                                exit_dt_str = f"{raw_exit[8:10]} {MONTHS[em]} {raw_exit[11:16]}"
                            elif r[0] and r[10]:
                                from datetime import datetime, timedelta
                                entry_dt = datetime.fromisoformat(str(r[0]))
                                exit_dt  = entry_dt + timedelta(minutes=int(r[10]))
                                exit_dt_str = f"{exit_dt.day:02d} {MONTHS[exit_dt.month]} {exit_dt.strftime('%H:%M')}"
                        except Exception:
                            pass

                        entry_str = _dt(r[0])
                        hold_str_leg = _fmt_hold(r[10])
                        exit_reason  = str(r[8]) if r[8] else "—"

                        # Try to parse per-leg details from notes field (r[14])
                        # Format: "sell_ce=SYM@PRICE | sell_pe=SYM@PRICE | buy_ce=SYM@PRICE | buy_pe=SYM@PRICE"
                        notes = str(r[14]) if (len(r) > 14 and r[14]) else ""
                        parsed_legs = []
                        if "|" in notes and "@" in notes:
                            try:
                                for part in notes.split("|"):
                                    part = part.strip()
                                    if "=" in part and "@" in part:
                                        role_part, rest = part.split("=", 1)
                                        sym_part, px_part = rest.rsplit("@", 1)
                                        role = role_part.strip().upper().replace("_", " ")
                                        sym  = sym_part.strip()
                                        px   = float(px_part.strip())
                                        parsed_legs.append((role, sym, px))
                            except Exception:
                                parsed_legs = []

                        if parsed_legs:
                            # Expand into one row per leg; combined P&L on last row
                            for i, (role, sym, entry_px) in enumerate(parsed_legs):
                                row_pnl = _fmt_pnl(r[7]) if i == len(parsed_legs) - 1 else "—"
                                leg_rows.append({
                                    "Entry":       entry_str if i == 0 else "",
                                    "Exit":        exit_dt_str if i == 0 else "",
                                    "Leg":         role,
                                    "Symbol":      sym,
                                    "Entry ₹":     f"{entry_px:,.2f}",
                                    "Exit ₹":      "—",
                                    "Combined P&L": row_pnl,
                                    "Exit reason": exit_reason if i == 0 else "",
                                    "Hold":        hold_str_leg if i == 0 else "",
                                })
                        else:
                            # Fallback: single row with whatever data we have
                            sym = str(r[3]) if (r[3] and str(r[3]) != "None") else str(r[2])
                            exit_px = r[6]
                            if not exit_px:
                                try:
                                    entry_px = float(r[5] or 0)
                                    pnl      = float(r[7] or 0)
                                    qty      = int(r[12]) if (len(r) > 12 and r[12]) else 0
                                    if qty and entry_px:
                                        exit_px = entry_px - pnl / qty
                                except Exception:
                                    pass
                            leg_rows.append({
                                "Entry":       entry_str,
                                "Exit":        exit_dt_str,
                                "Leg":         "—",
                                "Symbol":      sym,
                                "Entry ₹":     f"{r[5]:,.2f}" if r[5] else "—",
                                "Exit ₹":      f"{exit_px:,.2f}" if exit_px else "—",
                                "Combined P&L": _fmt_pnl(r[7]),
                                "Exit reason": exit_reason,
                                "Hold":        hold_str_leg,
                            })
                    st.dataframe(
                        pd.DataFrame(leg_rows),
                        width="stretch", hide_index=True,
                    )
        else:
            # ── Intraday bots: flat table ─────────────────────────────────────
            st.markdown("**Recent trades**")
            rows = []
            for r in recent:
                sym = str(r[3]) if (r[3] and str(r[3]) != "None") else str(r[2])
                rows.append({
                    "Date/Time":   _dt(r[0]),
                    "Symbol":      sym,
                    "P&L":         _fmt_pnl(r[7]),
                    "Outcome":     "WIN" if r[9] else "LOSS",
                    "Exit reason": str(r[8]) if r[8] else "—",
                    "Hold":        _fmt_hold(r[10]),
                })
            st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)


def render_performance_hub():
    """📊 Performance Hub — full fleet-level report page."""
    from datetime import date, timedelta

    st.markdown("## 📊 Performance Hub")

    # ── Controls ──────────────────────────────────────────────────────────────
    c_period, c_bot, c_btn = st.columns([2, 2, 1])
    with c_period:
        period_opt = st.selectbox(
            "Period", ["Last 7 days", "Last 30 days", "Last 90 days", "Custom range"],
            index=1, label_visibility="collapsed",
        )
    with c_bot:
        active_bots = [b for b in BOT_LIFECYCLE if b["status"] != "retired"]
        bot_labels  = ["All bots"] + [b["label"] for b in active_bots]
        bot_names   = [None]       + [b["bot"]   for b in active_bots]
        sel_label   = st.selectbox("Bot", bot_labels, index=0, label_visibility="collapsed")
        selected_bot = bot_names[bot_labels.index(sel_label)]
    with c_btn:
        if st.button("↻ Refresh", use_container_width=True):
            _load_perf_fleet.clear()
            _load_perf_bot.clear()
            st.rerun()

    end_dt = date.today()
    if period_opt == "Last 7 days":
        start_dt = end_dt - timedelta(days=6)
    elif period_opt == "Last 30 days":
        start_dt = end_dt - timedelta(days=29)
    elif period_opt == "Last 90 days":
        start_dt = end_dt - timedelta(days=89)
    else:
        cc1, cc2 = st.columns(2)
        start_dt = cc1.date_input("From", value=end_dt - timedelta(days=29))
        end_dt   = cc2.date_input("To",   value=end_dt)

    days = (end_dt - start_dt).days + 1

    # ── Load data ─────────────────────────────────────────────────────────────
    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).parent.parent))
        from live_trading.performance_review import (
            fetch_portfolio_summary, fetch_per_bot, fetch_stage11_sessions,
            fetch_daily_pnl, fetch_exit_reasons, fetch_recent_trades,
            _connect, BOT_META, is_active,
        )
        from live_trading.shared.performance_db import get_db_path
        con = _connect(get_db_path())
        summary  = fetch_portfolio_summary(con, start_dt, end_dt, bot_filter=selected_bot)
        # Dashboard shows active bots only — retired-bot history stays in performance_review.py.
        bots_raw = [b for b in fetch_per_bot(con, start_dt, end_dt, bot_filter=selected_bot)
                    if is_active(b["bot_name"])]
        sessions = fetch_stage11_sessions(con)
        daily    = fetch_daily_pnl(con, start_dt, end_dt, bot_filter=selected_bot)
        reasons  = fetch_exit_reasons(con, start_dt, end_dt, bot_filter=selected_bot)
        recent   = [r for r in fetch_recent_trades(con, 30, start_dt, end_dt, bot_filter=selected_bot)
                    if is_active(str(r[1]))]
        con.close()
    except Exception as exc:
        logger.exception("Could not load performance data")
        st.error(f"Could not load performance data: {exc}")
        return

    period_str = f"{start_dt.strftime('%d %b')} → {end_dt.strftime('%d %b %Y')}"
    st.caption(f"Period: {period_str}")

    # ── Section tabs ──────────────────────────────────────────────────────────
    t_summary, t_bots, t_fleet, t_gate, t_chart, t_reasons, t_trades, t_reconcile = st.tabs([
        "📋 Summary", "🤖 Per-bot", "🏭 Fleet status",
        "🚦 Stage 11", "📈 Daily P&L", "🚪 Exit reasons",
        "📜 Trades", "🔧 Reconcile",
    ], on_change="rerun")

    if t_summary.open:
        with t_summary:
            if not summary:
                st.warning("No trades found for the selected period / bot.")
            else:
                total   = summary.get("total_trades", 0) or 0
                wins    = summary.get("wins", 0) or 0
                gpnl    = summary.get("gross_pnl") or 0
                tdays   = summary.get("trading_days", 1) or 1
                best_d  = summary.get("best_day")
                worst_d = summary.get("worst_day")
                c1, c2, c3, c4, c5, c6 = st.columns(6)
                c1.metric("Gross P&L",  _fmt_pnl(gpnl))
                c2.metric("Win rate",   f"{wins/total*100:.1f}%" if total else "—")
                c3.metric("Trades",     total)
                c4.metric("Avg/day",    _fmt_pnl(gpnl / tdays if tdays else 0))
                c5.metric("Best day",   _fmt_pnl(best_d[1]  if best_d  else None))
                c6.metric("Worst day",  _fmt_pnl(worst_d[1] if worst_d else None))

    if t_bots.open:
        with t_bots:
            if bots_raw:
                rows = []
                for b in bots_raw:
                    label = BOT_META.get(b["bot_name"], {}).get("label", b["bot_name"])
                    rows.append({
                        "Bot":       label,
                        "Trades":    b["total"],
                        "Win %":     f"{b['win_rate']:.0f}%" if b["win_rate"] else "—",
                        "Gross P&L": _fmt_pnl(b["gross_pnl"]),
                        "Avg/trade": _fmt_pnl(b["avg_pnl"]),
                        "Best":      _fmt_pnl(b["best_trade"]),
                        "Worst":     _fmt_pnl(b["worst_trade"]),
                    })
                st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
            else:
                st.info("No per-bot data for this period.")

    if t_fleet.open:
        with t_fleet:
            render_fleet_status()

    if t_gate.open:
        with t_gate:
            active = [b for b in BOT_LIFECYCLE if b["status"] in ("live", "paper")]
            cols3  = st.columns(3)
            for i, b in enumerate(active):
                s = sessions.get(b["bot"], 0)
                status = "✅ Passed" if s >= STAGE11_TARGET else (
                    f"🟡 {s}/20" if s >= 15 else f"🔬 {s}/20"
                )
                with cols3[i % 3]:
                    st.markdown(f"<small><b>{b['label']}</b> — {status}</small>",
                                unsafe_allow_html=True)
                    st.progress(min(s / STAGE11_TARGET, 1.0))

    if t_chart.open:
        with t_chart:
            if daily:
                df_d = pd.DataFrame(daily, columns=["date", "pnl", "trades"])
                df_d["date"] = pd.to_datetime(df_d["date"])
                st.bar_chart(df_d.set_index("date")[["pnl"]], height=300)
            else:
                st.info("No daily P&L data for this period.")

    if t_reasons.open:
        with t_reasons:
            if reasons:
                df_r = pd.DataFrame(reasons, columns=["Reason", "Count", "Wins", "P&L"])
                df_r["Win %"] = (df_r["Wins"] / df_r["Count"] * 100).round(1).astype(str) + "%"
                df_r["P&L"]   = df_r["P&L"].apply(_fmt_pnl)
                st.dataframe(df_r[["Reason", "Count", "Win %", "P&L"]],
                             width="stretch", hide_index=True)
            else:
                st.info("No exit reason data for this period.")

    if t_trades.open:
        with t_trades:
            if recent:
                MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

                def _dt_hub(raw_ts):
                    raw_ts = str(raw_ts)
                    try:
                        m = int(raw_ts[5:7])
                        return f"{raw_ts[8:10]} {MONTHS[m]} {raw_ts[11:16]}"
                    except Exception:
                        return raw_ts[:16]

                rows = []
                for r in recent:
                    bn    = str(r[1])
                    label = BOT_META.get(bn, {}).get("label", bn)
                    sym   = str(r[3]) if (r[3] and str(r[3]) != "None") else str(r[2])
                    rows.append({
                        "Date/Time":   _dt_hub(r[0]),
                        "Bot":         label,
                        "Symbol":      sym,
                        "P&L":         _fmt_pnl(r[7]),
                        "Outcome":     "WIN" if r[9] else "LOSS",
                        "Exit reason": str(r[8]) if r[8] else "—",
                        "Hold":        _fmt_hold(r[10]),
                    })
                st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
            else:
                st.info("No trades in this period.")

    if t_reconcile.open:
        with t_reconcile:
            st.caption(
                "Scans bot state files vs the live positionbook. Writes synthetic exit records "
                "for positions the broker closed that the bot never logged."
            )
            dry = st.checkbox("Dry run (preview only — no DB writes)", value=True)
            if st.button("Run reconcile", key="hub_reconcile"):
                with st.spinner("Reconciling…"):
                    try:
                        from live_trading.performance_review import reconcile_stale_trades
                        result = reconcile_stale_trades(dry_run=dry)
                        if result:
                            df_rec = pd.DataFrame([{
                                "Bot":    r["bot_name"],
                                "Symbol": r["symbol"],
                                "P&L":    _fmt_pnl(r.get("gross_pnl")),
                                "Note":   r.get("notes", ""),
                            } for r in result])
                            st.dataframe(df_rec, width="stretch", hide_index=True)
                            if dry:
                                st.info("Dry run — uncheck to write these records.")
                            else:
                                st.success(f"Reconciled {len(result)} trade(s) into performance.db.")
                        else:
                            st.success("Nothing to reconcile — performance.db is in sync.")
                    except Exception as exc:
                        logger.exception("Reconcile failed")
                        st.error(f"Reconcile failed: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — NIFTY BB OVERBOUGHT BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_bb_overbought_panel(ltps: dict):
    state = _load(STATE_FILES.get("NIFTY_BB_OB"))
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _bb_overbought_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "NIFTY BB Overbought Bot — Execution Logic",
                "Sell ATM PE when NIFTY closes above its 3σ upper band in a mean-revert regime.",
                [
                    fc_start("☀️ Session Start (09:15)"),
                    fc_action("📚 Load 5-min NIFTY history + Daily ADX-14",
                              "Warm BB(30, 3σ) · need ≥ 35 bars"),
                    fc_action("⚡ On each completed 5-min bar"),
                    fc_filter("Bar close in 09:15–10:30 IST?", "⏰ Outside window"),
                    fc_filter("Close &gt; BB(30, 3σ) upper band?", "Within bands — wait"),
                    fc_filter("Daily ADX-14 &lt; 25 (mean-revert regime)?", "📈 Trending day"),
                    fc_filter("ATM PE premium ≥ ₹150 (₹200 expiry day)?", "Premium too low"),
                    fc_filter("No position opened yet today?", "🔁 One trade / session"),
                    fc_entry("📉 SELL ATM weekly PE", "MIS · 1 lot"),
                    fc_monitor("🔍 Monitor PE premium each poll"),
                    fc_exit("🎯 E4 target — premium ≤ 0.70× entry → EXIT"),
                    fc_exit("🛑 Safety SL — premium ≥ 2× entry → EXIT"),
                    fc_exit("⏰ 15:15 IST → EOD EXIT (unconditional)"),
                    fc_note("Whichever exit triggers first closes the position"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _bb         = state.get("bb_snapshot", {}) if state else {}
            _entry_win  = state.get("entry_window", "09:15–10:30") if state else "09:15–10:30"
            _daily_adx  = state.get("daily_adx") if state else None
            _adx_ok     = (_daily_adx is not None) and (_daily_adx < 25.0)
            _close      = _bb.get("close", 0)
            _upper      = _bb.get("upper", 0)
            _is_ob      = bool(_bb) and _close > _upper
            _in_win     = _entry_window_open(_entry_win)
            _active     = state.get("active_trade") if state else None
            _is_exp     = state.get("is_expiry_day", False) if state else False
            _bars       = state.get("bars_loaded", 0) if state else 0
            _adx_disp   = f"{_daily_adx:.1f}" if _daily_adx is not None else "—"

            if _active:
                _ready = ("📌", "IN POSITION — monitoring ATM PE for E4 target / SL / EOD exit", "#7b61ff")
            elif not _in_win:
                _ready = ("⏸", "OUT OF WINDOW — signals only 09:15–10:30 IST", "#94a3b8")
            elif _is_ob and _adx_ok:
                _ready = ("🟢", "OVERBOUGHT + regime OK — waiting for ATM PE premium ≥ threshold", "#00c875")
            elif _is_ob and not _adx_ok:
                _ready = ("🔴", f"OVERBOUGHT but ADX filter BLOCKS — trending day (ADX {_adx_disp} ≥ 25)", "#f87171")
            else:
                _ready = ("🔍", "SCANNING — NIFTY within bands, watching for 3σ breach", "#60a5fa")

            render_decision_state(
                state,
                key="bb_ob",
                updates_note="Updates on each 5-min bar close",
                metrics=[
                    ("NIFTY", f"{state.get('nifty_ltp', 0):,.1f}" if state else "—"),
                    ("Daily ADX-14", _adx_disp,
                     "✅ < 25" if _adx_ok else ("❌ ≥ 25" if _daily_adx is not None else None),
                     "normal" if _adx_ok else "inverse"),
                    ("5m Bars", f"{_bars}", "✅ warmed" if _bars >= 35 else "⏳ warming", "off"),
                    ("Window", _entry_win, "🟢 OPEN" if _in_win else "🔴 CLOSED", "off"),
                    ("BB Upper (3σ)", f"{_upper:,.1f}" if _upper else "—"),
                    ("Close vs Upper", f"{_close - _upper:+.1f}" if _bb else "—",
                     "overbought" if _is_ob else "inside", "off"),
                ],
                filters=[
                    ("⏰", "Entry window 09:15–10:30 IST", _in_win, _entry_win),
                    ("📊", "NIFTY 5m close > BB(30,3σ) upper", _is_ob,
                     f"{_close:,.0f} vs {_upper:,.0f}" if _bb else "no bar yet"),
                    ("📈", "Daily ADX-14 < 25 (mean-revert)", _adx_ok, f"ADX = {_adx_disp}"),
                    ("📅", "Weekly expiry resolved", bool(state and state.get("expiry")),
                     (state.get("expiry") if state else "") or "—"),
                    ("🔁", "No position open today", not bool(_active),
                     "free" if not _active else "already traded"),
                ],
                readiness=_ready,
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("bb_deep_study/study_a_report/STUDY_A_RESULTS.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("nifty_bb_overbought_bot")


def _bb_overbought_entry_decision_trail(entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows at/before entry_time — the phase path leading
    into the currently-open trade."""
    records = _read_jsonl_tail(LOGS_DIR / "nifty_bb_overbought_decisions.jsonl", limit=3000)
    matched = [r for r in records if not entry_time or r.get("ts", "") <= entry_time]
    return matched[-n:]


def _bb_overbought_overview(ltps: dict, state: dict):
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
            st.error("🔌 Bot not running — state file absent. Start the bot to see live data. Check `live_trading/logs/nifty_bb_overbought_state.json`.")
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
            sym        = active.get("symbol", "")
            entry      = float(active.get("entry_prem", 0))
            e4         = float(active.get("e4_target", entry * 0.7))
            sl         = float(active.get("sl_prem", entry * 2))
            qty        = int(active.get("qty", 0))
            order_id   = active.get("order_id", "")
            entry_time = active.get("entry_time", "")
            ltp        = ltps.get(sym, entry)

            _render_active_position_lifecycle(
                symbol=sym,
                order_id=order_id,
                entry_price=entry,
                sl_price=sl,
                target_price=e4,
                qty=qty,
                entry_time=entry_time,
                ltp=ltp,
                eod_exit_time="15:14",
                decision_trail=_bb_overbought_entry_decision_trail(entry_time),
            )

        if not active:
            _render_today_trades_detail(_load_today_trades("nifty_bb_overbought_bot"))


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — NIFTY TREND SELLER BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_nts_panel(ltps: dict):
    state = _load(STATE_FILES["NIFTY_TS"])
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _nts_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "Nifty Trend Seller Bot — Execution Logic",
                "Sell the counter option when 5-condition trend confluence fires on 1-min NIFTY bars.",
                [
                    fc_start("☀️ Session Start"),
                    fc_action("📚 Load 1-min NIFTY history",
                              "Warm EMA(20) · ADX(14) · RSI(14) · MACD(5,13,3)"),
                    fc_action("⚡ On each completed 1-min bar"),
                    fc_filter("Time inside entry window?", "⏰ Outside window"),
                    fc_filter("VIX ≤ 22?", "🌡️ Volatility too high"),
                    fc_filter("ADX(14) &gt; 30 (strong trend)?", "Trend too weak"),
                    fc_filter("ADX rising vs 5 bars ago?", "Not accelerating"),
                    fc_check("Trend direction + momentum?",
                             "EMA-20 side · RSI · MACD(5,13,3) cross"),
                    fc_split(
                        "BEARISH<br>close&lt;EMA · RSI&lt;45 · MACD↓",
                        fc_node_exit("📉 SELL ATM CE", "MIS · SL 2× entry"),
                        "BULLISH<br>close&gt;EMA · RSI&gt;55 · MACD↑",
                        fc_node_entry("📈 SELL ATM PE", "MIS · SL 2× entry"),
                    ),
                    fc_monitor("🔍 Monitor each leg — SL 2× entry premium"),
                    fc_exit("⏰ 15:14 IST → EOD EXIT (all legs, unconditional)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _ind     = state.get("indicators", {}) if state else {}
            _win     = state.get("entry_window", "10:00–13:00") if state else "10:00–13:00"
            _in_win  = _entry_window_open(_win)
            _vix     = state.get("vix_ltp", 0) if state else 0
            _adx     = _ind.get("adx", 0)
            _adx_old = _ind.get("adx_old", 0)
            _rsi     = _ind.get("rsi", 0)
            _ema     = _ind.get("ema", 0)
            _close   = _ind.get("close", 0)
            _bars    = state.get("bars_loaded", 0) if state else 0
            _active  = state.get("active_trades", {}) if state else {}
            _open    = {k: v for k, v in _active.items() if v}
            _vix_ok  = _vix <= 22
            _adx_ok  = _adx > 30
            _adx_rise = _adx > _adx_old

            if _open:
                _ready = ("📌", f"IN POSITION — {len(_open)} leg(s) open · monitoring SL / EOD", "#7b61ff")
            elif not _in_win:
                _ready = ("⏸", f"OUT OF WINDOW — entries only {_win} IST", "#94a3b8")
            elif _adx_ok and _adx_rise and _vix_ok:
                _ready = ("🟢", "TREND CONFIRMED — awaiting RSI + MACD direction alignment", "#00c875")
            elif not _vix_ok:
                _ready = ("🔴", f"BLOCKED — VIX {_vix:.1f} > 22", "#f87171")
            else:
                _ready = ("🔍", "SCANNING — waiting for strong, accelerating trend", "#60a5fa")

            render_decision_state(
                state,
                key="nts",
                updates_note="Updates on each 1-min bar close",
                metrics=[
                    ("NIFTY", f"{state.get('nifty_ltp', 0):,.1f}" if state else "—"),
                    ("VIX", f"{_vix:.2f}", "✅ ≤ 22" if _vix_ok else "⚠ > 22",
                     "normal" if _vix_ok else "inverse"),
                    ("ADX(14)", f"{_adx:.1f}", f"{_adx - _adx_old:+.2f}",
                     "normal" if _adx_rise else "inverse"),
                    ("RSI(14)", f"{_rsi:.1f}",
                     "Bull" if _rsi > 55 else ("Bear" if _rsi < 45 else "Neut"), "off"),
                    ("EMA(20)", f"{_ema:,.1f}" if _ema else "—",
                     f"{_close - _ema:+.1f} close" if _ema else None, "off"),
                    ("Window", _win, "🟢 OPEN" if _in_win else "🔴 CLOSED", "off"),
                    ("Bars", f"{_bars}"),
                ],
                filters=[
                    ("⏰", "Entry window open", _in_win, _win),
                    ("🌡️", "VIX ≤ 22", _vix_ok, f"VIX = {_vix:.1f}"),
                    ("💪", "ADX(14) > 30 (strong trend)", _adx_ok, f"ADX = {_adx:.1f}"),
                    ("📈", "ADX rising (vs 5 bars ago)", _adx_rise, f"Δ {_adx - _adx_old:+.2f}"),
                    ("🧭", "Momentum aligned (RSI > 55 or < 45)", (_rsi > 55 or _rsi < 45),
                     f"RSI = {_rsi:.1f}"),
                    ("🔁", "PE leg free", not bool(_active.get("PE")),
                     "open" if not _active.get("PE") else "in position"),
                    ("🔁", "CE leg free", not bool(_active.get("CE")),
                     "open" if not _active.get("CE") else "in position"),
                ],
                readiness=_ready,
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("nifty_trend_seller_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("nifty_trend_seller_bot")


def _nts_entry_decision_trail(entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows at/before entry_time — the per-bar verdict path
    (confluence build-up) leading into the currently-open leg."""
    records = _read_jsonl_tail(LOGS_DIR / "nifty_trend_seller_decisions.jsonl", limit=3000)
    matched = [r for r in records if not entry_time or r.get("ts", "") <= entry_time]
    return matched[-n:]


def _nts_overview(ltps: dict, state: dict):
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
            st.error("🔌 Bot not running — state file absent. Start the bot to see live data.")
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
            for leg, t in open_legs.items():
                sym        = t.get("symbol", "")
                entry      = float(t.get("entry_prem", 0))
                sl         = float(t.get("sl_prem", 0))
                qty        = int(t.get("qty", 0))
                order_id   = t.get("order_id", "")
                entry_time = t.get("entry_time", "")
                ltp        = ltps.get(sym, entry)

                st.caption(f"Leg: {leg}")
                _render_active_position_lifecycle(
                    symbol=sym,
                    order_id=order_id,
                    entry_price=entry,
                    sl_price=sl,
                    target_price=None,
                    qty=qty,
                    entry_time=entry_time,
                    ltp=ltp,
                    eod_exit_time="15:14",
                    decision_trail=_nts_entry_decision_trail(entry_time),
                    trail_phase_key="verdict",
                )

        _render_today_trades_detail(_load_today_trades("nifty_trend_seller_bot"))


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 5 — SENSEX TREND SELLER BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_sensex_ts_panel(ltps: dict):
    state = _load(STATE_FILES["SENSEX_TS"])
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _sensex_ts_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "SENSEX Trend Seller Bot — Execution Logic (SHORT-ONLY)",
                "Sell ATM CE only when the 6-condition bearish confluence fires on 1-min SENSEX bars.",
                [
                    fc_start("☀️ Session Start"),
                    fc_action("📚 Load 1-min SENSEX history",
                              "Warm EMA(20) · ADX(14) · RSI(14) · MACD(5,13,3)"),
                    fc_action("⚡ On each completed 1-min bar"),
                    fc_filter("Time inside entry window?", "⏰ Outside window"),
                    fc_filter("VIX ≤ 22?", "🌡️ Volatility too high"),
                    fc_filter("Close &lt; EMA(20) (bearish trend)?", "Not bearish"),
                    fc_filter("ADX(14) &gt; 25 (strong trend)?", "Trend too weak"),
                    fc_filter("ADX rising vs 7 bars ago?", "Not accelerating"),
                    fc_filter("RSI(14) &lt; 50 (bearish momentum)?", "Momentum not bearish"),
                    fc_filter("MACD(5,13,3) bearish (line &lt; signal)?", "MACD not bearish"),
                    fc_filter("CE leg free today?", "🔁 One CE / session"),
                    fc_entry("📉 SELL ATM CE (BFO)", "MIS · 10 lots · SL 2× entry"),
                    fc_monitor("🔍 Monitor CE premium — SL 2× entry"),
                    fc_exit("⏰ 15:14 IST → EOD EXIT (unconditional)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _ind     = state.get("indicators", {}) if state else {}
            _win     = state.get("entry_window", "10:00–13:00") if state else "10:00–13:00"
            _in_win  = _entry_window_open(_win)
            _vix     = state.get("vix_ltp", 0) if state else 0
            _adx     = _ind.get("adx", 0)
            _adx_old = _ind.get("adx_old", 0)
            _rsi     = _ind.get("rsi", 0)
            _ema     = _ind.get("ema", 0)
            _close   = _ind.get("close", 0)
            _macd_l  = _ind.get("macd_line", 0)
            _macd_s  = _ind.get("macd_signal", 0)
            _bars    = state.get("bars_loaded", 0) if state else 0
            _ce      = (state.get("active_trades", {}) if state else {}).get("CE")
            _vix_ok    = _vix <= 22
            _bearish   = bool(_ema) and _close < _ema
            _adx_ok    = _adx > 25
            _adx_rise  = _adx > _adx_old
            _rsi_bear  = _rsi < 50
            _macd_bear = _macd_l < _macd_s

            if _ce:
                _ready = ("📌", "IN POSITION — short ATM CE open · monitoring SL / EOD", "#7b61ff")
            elif not _in_win:
                _ready = ("⏸", f"OUT OF WINDOW — entries only {_win} IST", "#94a3b8")
            elif not _vix_ok:
                _ready = ("🔴", f"BLOCKED — VIX {_vix:.1f} > 22", "#f87171")
            elif _bearish and _adx_ok and _adx_rise:
                _ready = ("🟢", "BEARISH TREND CONFIRMED — awaiting RSI + MACD alignment", "#00c875")
            else:
                _ready = ("🔍", "SCANNING — waiting for strong bearish confluence", "#60a5fa")

            render_decision_state(
                state,
                key="sensex_ts",
                updates_note="Updates on each 1-min bar close",
                metrics=[
                    ("SENSEX", f"{state.get('sensex_ltp', 0):,.1f}" if state else "—"),
                    ("VIX", f"{_vix:.2f}", "✅ ≤ 22" if _vix_ok else "⚠ > 22",
                     "normal" if _vix_ok else "inverse"),
                    ("ADX(14)", f"{_adx:.1f}", f"{_adx - _adx_old:+.2f} vs 7b",
                     "normal" if _adx_rise else "inverse"),
                    ("RSI(14)", f"{_rsi:.1f}", "Bear" if _rsi_bear else "Neut",
                     "inverse" if _rsi_bear else "off"),
                    ("EMA(20)", f"{_ema:,.1f}" if _ema else "—",
                     f"{_close - _ema:+.1f} close" if _ema else None, "off"),
                    ("MACD diff", f"{_macd_l - _macd_s:+.1f}",
                     "Bearish" if _macd_bear else "Bullish", "off"),
                    ("Window", _win, "🟢 OPEN" if _in_win else "🔴 CLOSED", "off"),
                    ("Bars", f"{_bars}"),
                ],
                filters=[
                    ("⏰", "Entry window open", _in_win, _win),
                    ("🌡️", "VIX ≤ 22", _vix_ok, f"VIX = {_vix:.1f}"),
                    ("📉", "Close < EMA(20) — bearish", _bearish,
                     f"{_close:,.0f} vs {_ema:,.0f}" if _ema else "—"),
                    ("💪", "ADX(14) > 25 — strong", _adx_ok, f"ADX = {_adx:.1f}"),
                    ("📈", "ADX rising (vs 7 bars ago)", _adx_rise, f"Δ {_adx - _adx_old:+.2f}"),
                    ("🧭", "RSI(14) < 50 — bearish momentum", _rsi_bear, f"RSI = {_rsi:.1f}"),
                    ("🔀", "MACD bearish (line < signal)", _macd_bear,
                     f"{_macd_l - _macd_s:+.1f}"),
                    ("🔁", "CE leg free today", not bool(_ce),
                     "free" if not _ce else "in position"),
                ],
                readiness=_ready,
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("adx_rsi_macd_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("sensex_trend_seller_bot")


def _sensex_ts_entry_decision_trail(entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows at/before entry_time — the per-bar phase path
    leading into the currently-open CE leg."""
    records = _read_jsonl_tail(LOGS_DIR / "sensex_trend_seller_decisions.jsonl", limit=3000)
    matched = [r for r in records if not entry_time or r.get("ts", "") <= entry_time]
    return matched[-n:]


def _sensex_ts_overview(ltps: dict, state: dict):
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
            st.error("🔌 Bot not running — state file absent. Start the bot to see live data.")
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
            sym        = ce_pos.get("symbol", "")
            entry      = float(ce_pos.get("entry_prem", 0))
            sl         = float(ce_pos.get("sl_prem", 0))
            qty        = int(ce_pos.get("qty", 0))
            order_id   = ce_pos.get("order_id", "")
            entry_time = ce_pos.get("entry_time", "")
            ltp        = ltps.get(sym, entry)

            _render_active_position_lifecycle(
                symbol=sym,
                order_id=order_id,
                entry_price=entry,
                sl_price=sl,
                target_price=None,
                qty=qty,
                entry_time=entry_time,
                ltp=ltp,
                eod_exit_time="15:14",
                decision_trail=_sensex_ts_entry_decision_trail(entry_time),
            )

        _render_today_trades_detail(_load_today_trades("sensex_trend_seller_bot"))


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 6 — PRE-OPEN GAP FADE BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_gap_fade_panel(ltps: dict):
    state = _load(STATE_FILES["GAP_FADE"]) if "GAP_FADE" in STATE_FILES else {}
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _gap_fade_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "Pre-Open Gap Fade Bot — Execution Logic",
                "Fade ≥2% pre-open gaps on Nifty-50 stocks at the open; hard time-stop at 10:00.",
                [
                    fc_start("🔔 Pre-open auction (09:08–09:15)"),
                    fc_action("📊 Read IEP for 49 Nifty-50 stocks",
                              "Compute gap % vs previous close"),
                    fc_filter("Any stock gaps ≥ 2%?", "No gap — flat day"),
                    fc_check("Gap direction?"),
                    fc_split(
                        "GAP UP ≥ 2%",
                        fc_node_exit("📉 SHORT (fade up)", "MARKET MIS @ 09:15"),
                        "GAP DOWN ≥ 2%",
                        fc_node_entry("📈 LONG (fade down)", "MARKET MIS @ 09:15"),
                    ),
                    fc_action("⚡ Enter 09:15:05 · place SL-M 0.5% immediately"),
                    fc_monitor("🔍 Monitor each position"),
                    fc_exit("🛑 SL 0.5% against entry → EXIT"),
                    fc_exit("⏰ 10:00 IST → force-close ALL (hard time stop)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _positions = state.get("positions", {}) if state else {}
            _open      = sum(1 for p in _positions.values() if not p.get("exit_price"))
            _n_sig     = state.get("n_signals", 0) if state else 0
            _win_act   = _entry_window_open("09:15–10:00")

            if _open > 0:
                _ready = ("📌", f"IN POSITION — {_open} open · monitoring SL 0.5% / 10:00 time-stop", "#7b61ff")
            elif not _win_act:
                _ready = ("⏸", "SESSION CLOSED — bot trades 09:15–10:00 IST only", "#94a3b8")
            elif _n_sig > 0:
                _ready = ("✅", f"{_n_sig} gap signal(s) handled — positions exited or none filled", "#00c875")
            else:
                _ready = ("🔍", "AWAITING 09:15 open scan — no ≥2% gap detected yet", "#60a5fa")

            render_decision_state(
                state,
                key="gap_fade",
                updates_note="Updates through the 09:15–10:00 trading window",
                metrics=[
                    ("Trade Date", state.get("trade_date", "—") if state else "—"),
                    ("Gap Signals", f"{_n_sig}"),
                    ("Open Positions", f"{_open}"),
                    ("Window", "09:15–10:00", "🟢 ACTIVE" if _win_act else "🔴 CLOSED", "off"),
                ],
                filters=[
                    ("⏰", "Trading window 09:15–10:00 IST", _win_act, "active" if _win_act else "closed"),
                    ("📊", "≥2% pre-open gap detected", _n_sig > 0, f"{_n_sig} signal(s)"),
                    ("📈", "Positions currently open", _open > 0, f"{_open} open"),
                    ("🛑", "Per-position SL-M at 0.5%", True, "placed on entry"),
                ],
                readiness=_ready,
                checklist_title="🔍 Session State",
                checklist_caption="Gap fade is a one-shot open scanner; gates reflect today's session.",
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("preopen_gap_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("preopen_gap_fade_bot")


def _gap_fade_overview(ltps: dict, state: dict):
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
            st.error("🔌 Bot not running — state file absent. Start the bot to see live data.")
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
    "ACCUM":         ("⏳", "Accumulation",       "Waiting for 60-min bar open + accum window to complete"),
    "SKIPPED_RANGE": ("⛔", "Skipped — Range Filter", "Accumulation range exceeded accum_range_cap — no entries this HTF bar"),
    "WATCH":         ("👀", "Watch",               "Accum done — monitoring for manipulation (price below accum_low)"),
    "MANIP":         ("🎯", "Manipulation",        "Below accum_low — FVG detected, waiting for CISD confirmation"),
    "WAIT_CISD":     ("⚡", "Wait CISD",           "FVG mitigation zone — watching for 1-min close above FVG top"),
    "SIGNAL":        ("🔥", "SIGNAL FIRED",        "CISD confirmed — entry triggered"),
    "TRADED":        ("✅", "Traded",              "Session trade placed — monitoring position"),
}


def _render_active_position_lifecycle(
    *,
    symbol: str,
    order_id: str,
    entry_price: float,
    sl_price: float | None,
    target_price: float | None,
    qty: int,
    entry_time: str,
    ltp: float,
    eod_exit_time: str,
    decision_trail: list[dict] | None = None,
    trail_phase_key: str = "phase",
    direction: str = "short",
):
    """Standardized post-entry card: order id, live SL/target distance, EOD countdown,
    and the decision trail that led into the trade.

    direction="short" (default, sell-to-open): SL sits above entry, target below.
    direction="long" (buy-to-open / debit): SL sits below entry, target above.
    Pilot component for the dashboard redesign (concern #2); HTF PO3 is wired first,
    intended to be reused by the other bots once field-name mapping is added per bot.
    """
    since = entry_time[:19].replace("T", " ") if entry_time else "—"
    pnl = (ltp - entry_price) * qty if direction == "long" else (entry_price - ltp) * qty

    p1, p2, p3, p4, p5, p6 = st.columns(6)
    p1.metric("Symbol", symbol)
    p2.metric("Order ID", order_id or "—")
    p3.metric("Entry ₹", f"{entry_price:.2f}", help=f"Entered {since}")
    p4.metric("LTP ₹", f"{ltp:.2f}", delta=f"{ltp - entry_price:+.2f}")
    p5.metric(
        "MTM", f"{'+' if pnl > 0 else ''}₹{pnl:,.0f}",
        delta_color="normal" if pnl > 0 else "inverse",
        help=f"Qty: {qty}",
    )

    mins_to_eod = None
    if eod_exit_time:
        try:
            eh, em = map(int, eod_exit_time.split(":"))
            eod_dt = datetime.now().replace(hour=eh, minute=em, second=0, microsecond=0)
            mins_to_eod = (eod_dt - datetime.now()).total_seconds() / 60
        except Exception:
            mins_to_eod = None
    if mins_to_eod is None:
        p6.metric("EOD Exit", eod_exit_time or "—")
    elif mins_to_eod > 0:
        p6.metric("EOD Exit", eod_exit_time, delta=f"in {mins_to_eod:.0f} min", delta_color="off")
    else:
        p6.metric("EOD Exit", eod_exit_time, delta="⏰ DUE NOW", delta_color="off")

    # ── Live SL / Target distance ───────────────────────────────────────────
    g1, g2 = st.columns(2)
    with g1:
        if sl_price is None:
            st.caption("🛑 No stop-loss — held unconditionally to EOD")
            st.progress(0.0)
        else:
            if direction == "long":
                sl_span = entry_price - sl_price
                sl_prog = max(0.0, min(1.0, (entry_price - ltp) / sl_span)) if sl_span else 0.0
                sl_room = ltp - sl_price
            else:
                sl_span = sl_price - entry_price
                sl_prog = max(0.0, min(1.0, (ltp - entry_price) / sl_span)) if sl_span else 0.0
                sl_room = sl_price - ltp
            st.caption(f"🛑 SL ₹{sl_price:.2f}  —  ₹{sl_room:.2f} away ({(1 - sl_prog) * 100:.0f}% of room left)")
            st.progress(sl_prog)
    with g2:
        if target_price is None:
            st.caption("🎯 No profit target — exits via SL or EOD only (theta decay to close)")
            st.progress(0.0)
        elif direction == "long":
            tgt_span = target_price - entry_price
            tgt_prog = max(0.0, min(1.0, (ltp - entry_price) / tgt_span)) if tgt_span else 0.0
            tgt_room = target_price - ltp
            st.caption(f"🎯 Target ₹{target_price:.2f}  —  ₹{tgt_room:.2f} away ({tgt_prog * 100:.0f}% there)")
            st.progress(tgt_prog)
        else:
            tgt_span = entry_price - target_price
            tgt_prog = max(0.0, min(1.0, (entry_price - ltp) / tgt_span)) if tgt_span else 0.0
            tgt_room = ltp - target_price
            st.caption(f"🎯 Target ₹{target_price:.2f}  —  ₹{tgt_room:.2f} away ({tgt_prog * 100:.0f}% there)")
            st.progress(tgt_prog)

    # ── Entry decision trail ────────────────────────────────────────────────
    if decision_trail:
        with st.expander(f"🕵️ How we got here — entry decision trail ({len(decision_trail)} steps)"):
            for rec in decision_trail:
                ts_raw = rec.get("ts", "")
                ts = ts_raw[11:19] if len(ts_raw) >= 19 else (ts_raw or "—")
                phase = rec.get(trail_phase_key, "—")
                extra_bits = []
                lo, hi = rec.get("accum_low"), rec.get("accum_high")
                if lo is not None and hi is not None and abs(lo) < 1e9 and abs(hi) < 1e9:
                    extra_bits.append(f"accum {lo:.1f}–{hi:.1f}")
                flo, fhi = rec.get("fvg_bottom"), rec.get("fvg_top")
                if flo is not None and fhi is not None and abs(flo) < 1e9 and abs(fhi) < 1e9:
                    extra_bits.append(f"FVG {flo:.1f}–{fhi:.1f}")
                extra = ("  ·  " + "  ·  ".join(extra_bits)) if extra_bits else ""
                st.markdown(f"`{ts}` **{phase}**{extra}")


def _po3_entry_decision_trail(instrument_key: str, entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows for `instrument_key` at/before `entry_time` —
    the phase path (ACCUM → ... → TRADED) that led into the currently-open trade."""
    records = _read_jsonl_tail(LOGS_DIR / "htf_po3_decisions.jsonl", limit=3000)
    matched = [
        r for r in records
        if r.get("instrument") == instrument_key and (not entry_time or r.get("ts", "") <= entry_time)
    ]
    return matched[-n:]


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
    range_cap  = instrument.get("accum_range_cap", cfg.get("accum_range_cap"))
    ltp_fails  = instrument.get("ltp_fail_streak", 0)

    # ── Instrument header ─────────────────────────────────────────────────────
    lot_size = cfg.get("lot_size", "?")
    accum_m  = cfg.get("accum_minutes", "?")
    fvg_min  = cfg.get("fvg_min_size", 20)
    sl_mult  = cfg.get("sl_mult", "?")
    tgt_pct  = cfg.get("target_pct", "?")

    st.markdown(
        f'<div class="research-badge">'
        f'{sym_key} — accum {accum_m}m | range cap {range_cap if range_cap else "—"}pts | '
        f'FVG ≥{fvg_min}pts | SL {sl_mult}× | Tgt {tgt_pct}× | Lot {lot_size} | Expiry {expiry}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Phase banner ─────────────────────────────────────────────────────────
    banner_class = (
        "signal-banner-on"   if phase_raw in ("SIGNAL", "TRADED") else
        "signal-banner-wait" if phase_raw in ("MANIP", "WAIT_CISD") else
        "signal-banner-off"  # includes ACCUM, SKIPPED_RANGE, and unknown phases
    )
    st.markdown(
        f'<div class="{banner_class}">'
        f'{phase_icon} <b>{phase_name}</b>  —  {phase_desc}'
        f'{"  |  ⛔ Session trade complete" if traded and phase_raw != "TRADED" else ""}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── LTP-polling reliability warning ─────────────────────────────────────────
    if ltp_fails >= 3:
        st.markdown(
            f'<div class="signal-banner-off">'
            f'⚠️ LTP polling has failed {ltp_fails}× in a row for this instrument — '
            f'SL/target monitoring may be stale. Check bot log / Telegram alerts.'
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
                 if (t.get("symbol") or "").upper().startswith(sym_key.upper())]
            )
        else:
            st.success(f"No open position — monitoring PO3 phases.")
    else:
        sym        = active.get("symbol", "")
        entry      = float(active.get("entry_prem", 0))
        sl         = float(active.get("sl_prem", entry * 2))
        tgt        = float(active.get("tgt_prem", entry * 0.7))
        qty        = int(active.get("qty", 0))
        order_id   = active.get("order_id", "")
        entry_time = active.get("entry_time", "")
        ltp_op     = ltps.get(sym, entry)

        _render_active_position_lifecycle(
            symbol=sym,
            order_id=order_id,
            entry_price=entry,
            sl_price=sl,
            target_price=tgt,
            qty=qty,
            entry_time=entry_time,
            ltp=ltp_op,
            eod_exit_time="15:20",
            decision_trail=_po3_entry_decision_trail(sym_key, entry_time),
        )


def render_htf_po3_panel(ltps: dict):
    state = _load(STATE_FILES["HTF_PO3"])
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _htf_po3_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "HTF Power-of-3 Bot — Execution Logic",
                "Sell ATM PE on a bullish 60-min PO3 fractal: Accumulation → Manipulation FVG → CISD.",
                [
                    fc_start("☀️ Session Start"),
                    fc_action("📊 Accumulation window of each 60-min bar",
                              "BANKNIFTY 15m → record accum high / low"),
                    fc_filter("Accumulation range ≤ cap? (300pt)",
                              "⛔ Range filter tripped — SKIPPED_RANGE, no entries this bar"),
                    fc_filter("Price dips below accum low? (Manipulation)", "No manipulation"),
                    fc_filter("Bullish FVG ≥ 20 pts in the dip?", "No valid FVG"),
                    fc_filter("CISD — 1-min close above FVG top?", "No displacement"),
                    fc_filter("Time in 09:45–14:30 IST?", "⏰ Outside window"),
                    fc_filter("Instrument not yet traded today?", "🔁 One trade / instrument"),
                    fc_entry("📈 SELL ATM PE", "BANKNIFTY monthly · 1 lot"),
                    fc_monitor("🔍 Monitor PE premium"),
                    fc_exit("🎯 Target — −70% from entry → EXIT"),
                    fc_exit("🛑 SL — 1.5× entry → EXIT"),
                    fc_exit("⏰ 15:20 IST → EOD EXIT (unconditional)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _bn      = state.get("BANKNIFTY", {}) if state else {}
            _win     = "09:45–14:30"
            _in_win  = _entry_window_open(_win)
            _bn_done = bool(_bn.get("session_traded"))
            _bn_skip = _bn.get("phase") == "SKIPPED_RANGE"

            if not _in_win:
                _ready = ("⏸", f"OUT OF WINDOW — signals only {_win} IST", "#94a3b8")
            elif _bn_done:
                _ready = ("📌", "BANKNIFTY traded today — monitoring / awaiting EOD", "#7b61ff")
            else:
                _ready = ("🔍", "SCANNING — building 60-min PO3 fractal (accum → manipulation → CISD)", "#60a5fa")

            render_decision_state(
                state,
                key="htf_po3",
                updates_note="Updates on each 1-min / 60-min bar evaluation",
                metrics=[
                    ("Window", _win, "🟢 OPEN" if _in_win else "🔴 CLOSED", "off"),
                    ("BANKNIFTY", f"{_bn.get('ltp', 0):,.1f}" if _bn.get("ltp") else "—",
                     "traded ✓" if _bn_done else "scanning", "off"),
                    ("EOD Exit", "15:20 IST"),
                ],
                filters=[
                    ("⏰", "Entry window 09:45–14:30 IST", _in_win, _win),
                    ("🟧", "BANKNIFTY trade slot free today", not _bn_done,
                     "free" if not _bn_done else "already traded"),
                    ("⛔", "BANKNIFTY current bar not range-filtered", not _bn_skip,
                     "ok" if not _bn_skip else "SKIPPED_RANGE — cap tripped this bar"),
                ],
                readiness=_ready,
                checklist_title="🔍 Per-Instrument State",
                checklist_caption="PO3 fractal stages (accum/FVG/CISD) advance intrabar — see bot log [SIG] entries.",
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("htf_po3_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("htf_po3_bot")


def _htf_po3_overview(ltps: dict, state: dict):
    with st.container(border=True):
        st.subheader("🔱 HTF Power of 3 Bot  (BANKNIFTY)")
        st.markdown(
            '<div class="research-badge">'
            'Research: 60-min PO3 fractal (Accum → Manipulation FVG → CISD) → Sell ATM PE  |  '
            'BANKNIFTY: accum=15m fvg≥20pts SL=1.5× tgt=0.3 monthly  |  '
            'Entry 09:45–14:30 IST  |  EOD 15:20  |  1 lot flat  |  ALL 10 pipeline stages ✅'
            '</div>',
            unsafe_allow_html=True,
        )
        st.caption("ℹ️ NIFTY disabled 2026-07-15 (per trading decision) — BANKNIFTY only. "
                   "Config preserved commented-out in htf_po3_bot.py for re-enable.")

        if not state:
            st.error(
                "🔌 Bot not running — state file absent. Start the bot to see live data. "
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
                st.markdown("**Entry Conditions — 6-Stage PO3 Sequence**")
                st.markdown(
                    '<div class="condition-row">'
                    '1️⃣ Accumulation window completes (15m BANKNIFTY)<br>'
                    '2️⃣ Accum range ≤ cap (300pt) — else SKIPPED_RANGE, no entry this bar<br>'
                    '3️⃣ Price breaks below accumulation low (Manipulation begins)<br>'
                    '4️⃣ Bullish FVG detected ≥ 20 pts in manipulation leg<br>'
                    '5️⃣ CISD: 1-min close above FVG top (displacement confirmation)<br>'
                    '6️⃣ Entry window: 09:45–14:30 IST'
                    '</div>',
                    unsafe_allow_html=True,
                )
                st.caption(
                    "Range-cap added 2026-07-10 after the 2026-07-08 BANKNIFTY loss day "
                    "(367.3pt accum range, >2× the max ever seen in 106 backtested trades)."
                )
            with po3_ch2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🎯 <b>E4 Target</b>: −70% from entry<br>'
                    '🛑 <b>Safety SL</b>: 1.5× entry<br>'
                    '⏰ <b>EOD exit</b>: 15:20 IST unconditional close<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )

        # ── BANKNIFTY (NIFTY disabled 2026-07-15) ───────────────────────────────
        INSTRUMENT_CONFIGS = {
            "BANKNIFTY": {
                "lot_size": 30, "accum_minutes": 15, "fvg_min_size": 20,
                "sl_mult": "1.5", "target_pct": "0.3", "accum_range_cap": 300.0,
            },
        }

        instrument = state.get("BANKNIFTY", {})
        cfg        = INSTRUMENT_CONFIGS["BANKNIFTY"]
        with st.container(border=True):
            _render_po3_instrument("BANKNIFTY", cfg, instrument, ltps)



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
# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 7b — VP SWING REVERSION SCREENER (stocks, signal-only — no live orders)
# ══════════════════════════════════════════════════════════════════════════════

_VP_CAPITAL_PER_TRADE = 100_000  # mirrors vp_swing_screener.py CAPITAL_PER_TRADE


def _confirm_vp_candidate(candidate: dict) -> None:
    """Promote a detected candidate into open_positions in the screener's own
    state file. Called when the user clicks "Confirm" after manually
    executing the trade through their own broker terminal -- the screener
    never places orders itself (see render_vp_swing_screener_panel's
    docstring), so this manual action is the only way a candidate becomes a
    tracked position. The next scan cycle (vp_swing_screener.py) reads
    open_positions fresh from this same file, so it picks the position up
    and starts tracking its stop/target from the following scan onward."""
    path = STATE_FILES.get("VP_SWING_SCREENER")
    if not path:
        return
    touch_price = float(candidate.get("touch_price") or 0)
    if touch_price <= 0:
        return
    state = _load(path) or {}
    new_pos = {
        "symbol":      candidate.get("symbol", ""),
        "entry_price": touch_price,
        "stop":        float(candidate.get("stop") or 0),
        "target_poc":  float(candidate.get("poc") or 0),
        "qty":         int(_VP_CAPITAL_PER_TRADE // touch_price),
        "since":       datetime.now().strftime("%Y-%m-%d"),
    }
    positions = [p for p in state.get("open_positions", []) if p.get("symbol") != new_pos["symbol"]]
    positions.append(new_pos)
    state["open_positions"] = positions
    state["candidates"] = [c for c in state.get("candidates", []) if c.get("symbol") != new_pos["symbol"]]
    try:
        path.write_text(json.dumps(state, indent=2))
    except Exception as e:
        st.error(f"Failed to save confirmed position: {e}")


def render_vp_swing_screener_panel(ltps: dict):
    """Volume-Profile Swing Reversion screener — 60-min bars, touch of rolling
    10-day lower profile extreme -> long candidate, target = rolling POC.
    Research: options_data/research/vp_swing_reversion_study/ (10/10 stages,
    Stage 12 overnight-gap tail risk accepted 2026-08-09).

    This is a SCREENER, not an order-placing bot: it never calls placeorder().
    The "open position tracker" below reflects positions the state file has
    been told about (e.g. after you manually execute a candidate through your
    own broker terminal) — reconciled against live LTP the same way every
    other bot's positions are, via `ltps`/positionbook lookup by symbol.
    """
    state = _load(STATE_FILES.get("VP_SWING_SCREENER")) or {}

    with st.container(border=True):
        st.subheader("🔬 VP Swing Screener — Volume-Profile Reversion (60-min, long-only)")
        with st.expander("📖 Strategy Details"):
            st.markdown(
                '<div class="research-badge">'
                'Strategy: touch of the lower extreme of a rolling 10-trading-day '
                'volume profile (60-min bars) → long candidate, target = rolling POC, '
                'hard 3% stop-loss, no forced EOD close, no pyramiding  |  '
                'Full IS+OOS (2023-11-30→2026-08-07): 3,907 trades, WR ~69%, '
                'Sharpe 4.0(IS)/5.2(OOS)  |  '
                'Stage 12 overnight-gap tail risk (-5.44% worst 1%ile) accepted as a '
                'documented cost  |  Stage 13 capital deployment 79.5% peak on a '
                '₹46L book  |  Universe: 53 NIFTY50 stocks  |  '
                'SIGNAL-ONLY — no automatic order placement'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── Top metrics row ────────────────────────────────────────────────────
        open_positions = state.get("open_positions", [])
        candidates     = state.get("candidates", [])
        book_value     = state.get("book_value", 4_600_000)
        last_scan      = state.get("last_scan", "")
        last_pos_check = state.get("last_position_check", "")

        deployed_value = sum(
            float(p.get("entry_price") or 0) * float(p.get("qty") or 0)
            for p in open_positions
        )
        deployed_pct = (deployed_value / book_value * 100) if book_value else 0.0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Book Value", f"₹{book_value/1e5:.1f}L")
        c2.metric("Open Positions", len(open_positions))
        c3.metric("Capital Deployed", f"{deployed_pct:.1f}%",
                   delta=f"₹{deployed_value:,.0f}")
        c4.metric("Last Scan (hourly)", last_scan[11:16] if len(last_scan) >= 16 else (last_scan or "never"))
        c5.metric(
            "Last Position Check (5-min)",
            last_pos_check[11:16] if len(last_pos_check) >= 16 else ("—" if not open_positions else "pending"),
        )

        # ── New candidates this scan ────────────────────────────────────────────
        st.markdown("---")
        if candidates:
            st.markdown(f"**🎯 New Candidates This Hour ({len(candidates)})**")
            st.caption("Confirm assumes you've already executed the trade through your own "
                       "broker terminal — clicking it does not place an order, it only starts "
                       "tracking the position here.")
            hdr = st.columns([1.3, 1, 1, 1, 1, 0.9])
            for col, label in zip(hdr, ["Symbol", "Touch ₹", "Rolling POC ₹", "Stop ₹", "Detected", ""]):
                col.markdown(f"**{label}**")
            for idx, c in enumerate(candidates):
                row = st.columns([1.3, 1, 1, 1, 1, 0.9])
                row[0].write(c.get("symbol", ""))
                row[1].write(f"₹{float(c.get('touch_price') or 0):.2f}")
                row[2].write(f"₹{float(c.get('poc') or 0):.2f}")
                row[3].write(f"₹{float(c.get('stop') or 0):.2f}")
                detected = c.get("detected_at", "")
                row[4].write(detected[11:16] if len(detected) >= 16 else detected)
                if row[5].button("Confirm", key=f"vp_confirm_{c.get('symbol', '')}_{idx}"):
                    _confirm_vp_candidate(c)
                    st.rerun()
        else:
            st.info("🔍 No new candidates this scan.")

        # ── Open position tracker ───────────────────────────────────────────────
        st.markdown("---")
        if open_positions:
            st.markdown(f"**📂 Open Position Tracker ({len(open_positions)})**")
            pos_rows = []
            for pos in open_positions:
                sym      = pos.get("symbol", "")
                entry_px = float(pos.get("entry_price") or 0)
                qty      = int(pos.get("qty") or 0)
                stop     = float(pos.get("stop") or 0)
                target   = float(pos.get("target_poc") or 0)
                ltp      = ltps.get(sym, entry_px)
                mtm      = (ltp - entry_px) * qty
                since    = pos.get("since", "")
                try:
                    day_n = (pd.Timestamp.now().normalize() - pd.Timestamp(since).normalize()).days + 1 if since else 0
                except Exception:
                    day_n = 0
                pos_rows.append({
                    "Symbol":            sym,
                    "Entry ₹":           entry_px,
                    "LTP ₹":             ltp,
                    "Stop ₹":            stop,
                    "Target (POC) ₹":    target,
                    "Qty":               qty,
                    "MTM ₹":             mtm,
                    "Day":               day_n,
                })

            def _color_mtm(v):
                return "color:#00e599;font-weight:700" if v > 0 else ("color:#f87171;font-weight:700" if v < 0 else "color:#5a7ba0")

            df_pos = pd.DataFrame(pos_rows)
            st.dataframe(
                df_pos.style
                    .map(_color_mtm, subset=["MTM ₹"])
                    .format({
                        "Entry ₹":        "₹{:.2f}",
                        "LTP ₹":          "₹{:.2f}",
                        "Stop ₹":         "₹{:.2f}",
                        "Target (POC) ₹": "₹{:.2f}",
                        "MTM ₹":          lambda v: f"{'+' if v > 0 else ''}₹{v:,.0f}",
                    }),
                hide_index=True,
                width='stretch',
            )

            total_mtm = sum(r["MTM ₹"] for r in pos_rows)
            sign  = "+" if total_mtm > 0 else ""
            color = "green" if total_mtm > 0 else ("red" if total_mtm < 0 else "gray")
            st.markdown(
                f'**VP Swing Screener Open MTM: '
                f'<span style="color:{color};font-weight:bold">{sign}₹{total_mtm:,.0f}</span>**',
                unsafe_allow_html=True,
            )
        else:
            st.info("📂 No open positions tracked.")


# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 7c — VP SWING REVERSION SCREENER (DAILY) — stocks, signal-only
# ══════════════════════════════════════════════════════════════════════════════

_VP_DAILY_CAPITAL_PER_TRADE = 100_000  # mirrors vp_swing_screener_daily.py CAPITAL_PER_TRADE


def _confirm_vp_daily_candidate(candidate: dict) -> None:
    """Promote a detected candidate into open_positions in the daily
    screener's own state file. Same manual-confirm workflow as
    _confirm_vp_candidate() for the 60-min screener — the daily screener
    never places orders itself, so this is the only way a candidate becomes
    a tracked position."""
    path = STATE_FILES.get("VP_SWING_SCREENER_DAILY")
    if not path:
        return
    touch_price = float(candidate.get("touch_price") or 0)
    if touch_price <= 0:
        return
    state = _load(path) or {}
    new_pos = {
        "symbol":      candidate.get("symbol", ""),
        "entry_price": touch_price,
        "stop":        float(candidate.get("stop") or 0),
        "target_poc":  float(candidate.get("poc") or 0),
        "qty":         int(_VP_DAILY_CAPITAL_PER_TRADE // touch_price),
        "since":       datetime.now().strftime("%Y-%m-%d"),
    }
    positions = [p for p in state.get("open_positions", []) if p.get("symbol") != new_pos["symbol"]]
    positions.append(new_pos)
    state["open_positions"] = positions
    state["candidates"] = [c for c in state.get("candidates", []) if c.get("symbol") != new_pos["symbol"]]
    try:
        path.write_text(json.dumps(state, indent=2))
    except Exception as e:
        st.error(f"Failed to save confirmed position: {e}")


def render_vp_swing_daily_screener_panel(ltps: dict):
    """Volume-Profile Swing Reversion screener — DAILY bars, touch of rolling
    10-day lower profile extreme -> long candidate, target = rolling POC.
    Research: options_data/research/vp_swing_reversion_daily_study/ (10/10
    stages, Stage 12 overnight-gap tail risk and Stage 13 G-13-C stress
    both accepted as documented costs, 2026-08-12; final capital basis
    Rs.50,00,000, DECISIONS.md #6).

    Because a daily bar only completes at session close, this screener's
    authoritative scan runs ONCE per trading day (~15:45 IST) — a signal
    confirmed there is only actionable tomorrow at the earliest. A separate,
    optional heads-up pass at ~15:30 IST evaluates the same touch condition
    against today's still-forming bar and surfaces it as a "provisional"
    candidate (day low so far, LTP, % off low), letting you choose to enter
    near today's close instead of waiting for tomorrow's open — see that
    section's caption for what is/isn't final about it. This is a SCREENER,
    not an order-placing bot: it never calls placeorder(). The "open
    position tracker" below reflects positions the state file has been told
    about (e.g. after you manually execute a candidate through your own
    broker terminal) — reconciled against live LTP the same way every other
    bot's positions are.
    """
    state = _load(STATE_FILES.get("VP_SWING_SCREENER_DAILY")) or {}

    with st.container(border=True):
        st.subheader("🔬 VP Swing Screener (Daily) — Volume-Profile Reversion (daily bars, long-only)")
        with st.expander("📖 Strategy Details"):
            st.markdown(
                '<div class="research-badge">'
                'Strategy: touch of the lower extreme of a rolling 10-trading-day '
                'volume profile (daily bars) → long candidate, target = rolling POC, '
                'hard 3% stop-loss, no forced EOD close, no pyramiding  |  '
                'Full IS+OOS (2023-11-17→2026-08-07): 2,937 trades, WR 74.0%, '
                'Sharpe 6.27(IS)/6.65(OOS)  |  '
                'Stage 12 overnight-gap tail risk (-5.63% worst 1%ile) accepted as a '
                'documented cost  |  Stage 13 capital deployment 87.1% peak / 130.6% '
                'stressed on a ₹50L book (stressed-scenario gate accepted as a '
                'documented cost)  |  Universe: 53 NIFTY50 stocks  |  '
                'Scans once/day (~15:35 IST, after close) — a signal today is only '
                'actionable tomorrow at the earliest  |  '
                'SIGNAL-ONLY — no automatic order placement'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── Top metrics row ────────────────────────────────────────────────────
        open_positions = state.get("open_positions", [])
        candidates     = state.get("candidates", [])
        book_value     = state.get("book_value", 5_000_000)
        last_scan      = state.get("last_scan", "")
        last_pos_check = state.get("last_position_check", "")

        deployed_value = sum(
            float(p.get("entry_price") or 0) * float(p.get("qty") or 0)
            for p in open_positions
        )
        deployed_pct = (deployed_value / book_value * 100) if book_value else 0.0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Book Value", f"₹{book_value/1e5:.1f}L")
        c2.metric("Open Positions", len(open_positions))
        c3.metric("Capital Deployed", f"{deployed_pct:.1f}%",
                   delta=f"₹{deployed_value:,.0f}")
        c4.metric("Last Scan (daily)", last_scan[11:16] if len(last_scan) >= 16 else (last_scan or "never"))
        c5.metric(
            "Last Position Check (5-min)",
            last_pos_check[11:16] if len(last_pos_check) >= 16 else ("—" if not open_positions else "pending"),
        )

        # ── Intraday heads-up (provisional, ~15:30, pre-close) ──────────────────
        intraday_candidates = state.get("intraday_candidates", [])
        last_intraday_check = state.get("last_intraday_check", "")
        if intraday_candidates:
            st.markdown("---")
            st.markdown(f"**⏱️ Same-Day Provisional Touches ({len(intraday_candidates)})**")
            st.caption(
                "Heads-up check (~15:30 IST, ~10 min before close) — the touch itself is "
                "locked in (today's low can only fall further by close), but the exact low "
                "and whether this recovery holds through the final minutes are NOT final. "
                "Confirming here enters near today's close instead of waiting for tomorrow's "
                "open — same 'you already executed it yourself' semantics as Confirm below."
            )
            hdr = st.columns([1.2, 1, 1, 1, 1, 1, 0.8])
            for col, label in zip(hdr, ["Symbol", "Day Low ₹", "LTP ₹", "Off Low", "Prov. POC ₹", "Prov. Stop ₹", ""]):
                col.markdown(f"**{label}**")
            for idx, c in enumerate(intraday_candidates):
                row = st.columns([1.2, 1, 1, 1, 1, 1, 0.8])
                row[0].write(c.get("symbol", ""))
                row[1].write(f"₹{float(c.get('day_low_so_far') or 0):.2f}")
                row[2].write(f"₹{float(c.get('ltp') or 0):.2f}")
                off_low = float(c.get("pct_off_low") or 0)
                row[3].markdown(f":green[+{off_low:.2f}%]" if off_low > 0 else f"{off_low:.2f}%")
                row[4].write(f"₹{float(c.get('poc') or 0):.2f}")
                row[5].write(f"₹{float(c.get('stop') or 0):.2f}")
                if row[6].button("Confirm", key=f"vp_daily_intraday_confirm_{c.get('symbol', '')}_{idx}"):
                    _confirm_vp_daily_candidate(c)
                    st.rerun()
            if last_intraday_check:
                st.caption(f"Last intraday check: {last_intraday_check[11:16] if len(last_intraday_check) >= 16 else last_intraday_check}")

        # ── New candidates this scan ────────────────────────────────────────────
        st.markdown("---")
        if candidates:
            st.markdown(f"**🎯 New Candidates (Today's Scan) ({len(candidates)})**")
            st.caption("Confirm assumes you've already executed the trade through your own "
                       "broker terminal — clicking it does not place an order, it only starts "
                       "tracking the position here.")
            hdr = st.columns([1.3, 1, 1, 1, 1, 0.9])
            for col, label in zip(hdr, ["Symbol", "Touch ₹", "Rolling POC ₹", "Stop ₹", "Detected", ""]):
                col.markdown(f"**{label}**")
            for idx, c in enumerate(candidates):
                row = st.columns([1.3, 1, 1, 1, 1, 0.9])
                row[0].write(c.get("symbol", ""))
                row[1].write(f"₹{float(c.get('touch_price') or 0):.2f}")
                row[2].write(f"₹{float(c.get('poc') or 0):.2f}")
                row[3].write(f"₹{float(c.get('stop') or 0):.2f}")
                detected = c.get("detected_at", "")
                row[4].write(detected[11:16] if len(detected) >= 16 else detected)
                if row[5].button("Confirm", key=f"vp_daily_confirm_{c.get('symbol', '')}_{idx}"):
                    _confirm_vp_daily_candidate(c)
                    st.rerun()
        else:
            st.info("🔍 No new candidates from today's scan.")

        # ── Open position tracker ───────────────────────────────────────────────
        st.markdown("---")
        if open_positions:
            st.markdown(f"**📂 Open Position Tracker ({len(open_positions)})**")
            pos_rows = []
            for pos in open_positions:
                sym      = pos.get("symbol", "")
                entry_px = float(pos.get("entry_price") or 0)
                qty      = int(pos.get("qty") or 0)
                stop     = float(pos.get("stop") or 0)
                target   = float(pos.get("target_poc") or 0)
                ltp      = ltps.get(sym, entry_px)
                mtm      = (ltp - entry_px) * qty
                since    = pos.get("since", "")
                try:
                    day_n = (pd.Timestamp.now().normalize() - pd.Timestamp(since).normalize()).days + 1 if since else 0
                except Exception:
                    day_n = 0
                pos_rows.append({
                    "Symbol":            sym,
                    "Entry ₹":           entry_px,
                    "LTP ₹":             ltp,
                    "Stop ₹":            stop,
                    "Target (POC) ₹":    target,
                    "Qty":               qty,
                    "MTM ₹":             mtm,
                    "Day":               day_n,
                })

            def _color_mtm(v):
                return "color:#00e599;font-weight:700" if v > 0 else ("color:#f87171;font-weight:700" if v < 0 else "color:#5a7ba0")

            df_pos = pd.DataFrame(pos_rows)
            st.dataframe(
                df_pos.style
                    .map(_color_mtm, subset=["MTM ₹"])
                    .format({
                        "Entry ₹":        "₹{:.2f}",
                        "LTP ₹":          "₹{:.2f}",
                        "Stop ₹":         "₹{:.2f}",
                        "Target (POC) ₹": "₹{:.2f}",
                        "MTM ₹":          lambda v: f"{'+' if v > 0 else ''}₹{v:,.0f}",
                    }),
                hide_index=True,
                width='stretch',
            )

            total_mtm = sum(r["MTM ₹"] for r in pos_rows)
            sign  = "+" if total_mtm > 0 else ""
            color = "green" if total_mtm > 0 else ("red" if total_mtm < 0 else "gray")
            st.markdown(
                f'**VP Swing Screener (Daily) Open MTM: '
                f'<span style="color:{color};font-weight:bold">{sign}₹{total_mtm:,.0f}</span>**',
                unsafe_allow_html=True,
            )
        else:
            st.info("📂 No open positions tracked.")


# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 8a — BNF BB OPENING CANDLE BOT
# ══════════════════════════════════════════════════════════════════════════════

def _bnf_oc_entry_decision_trail(leg_key: str, entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows for `leg_key` at/before entry_time — the
    per-bar status path (WARMUP → ... → ACTIVE) leading into the currently-open leg."""
    records = _read_jsonl_tail(LOGS_DIR / "banknifty_bb_opening_candle_decisions.jsonl", limit=3000)
    matched = [
        r for r in records
        if r.get("leg") == leg_key and (not entry_time or r.get("ts", "") <= entry_time)
    ]
    return matched[-n:]


def render_bnf_bb_opening_candle_panel(ltps: dict):
    state = _load(STATE_FILES["BNF_BB_OC"])
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State",
        "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    legs_raw = (state or {}).get("legs", {})

    if tab_overview.open:
        with tab_overview:
            st.subheader("📉 BNF BB Opening Candle Bot")
            st.caption(
                "09:15 ATM option High > BB(20,2σ) → SELL LIMIT at (Close+High)/2 @ 09:16. "
                "Three legs: **BANKNIFTY CE · BANKNIFTY PE · SENSEX PE**. SL=10 pts, target=evolving SMA."
            )

            if not state:
                st.error("🔌 Bot not running — state file absent. Start the bot to see live data. "
                         "Check `live_trading/logs/banknifty_bb_opening_candle_state.json`.")

            if state:
                skip_day    = (state or {}).get("skip_day", False)
                skip_reason = (state or {}).get("skip_reason", "")
                daily_adx   = (state or {}).get("daily_adx")
                vix_ltp     = (state or {}).get("vix_ltp", 0)
                bnf_ltp     = (state or {}).get("bnf_ltp", 0)
                sensex_ltp  = (state or {}).get("sensex_ltp", 0)

                if skip_day:
                    st.warning(f"⛔ **Day skipped — {skip_reason}**")

                # Market snapshot row
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("BANKNIFTY", f"{bnf_ltp:,.1f}" if bnf_ltp else "—")
                c2.metric("SENSEX", f"{sensex_ltp:,.1f}" if sensex_ltp else "—")
                c3.metric("VIX", f"{vix_ltp:.2f}" if vix_ltp else "—",
                          "≥18 SKIP" if vix_ltp >= 18 else "OK",
                          delta_color="inverse" if vix_ltp >= 18 else "off")
                c4.metric("Daily ADX(14)", f"{daily_adx:.1f}" if daily_adx else "—",
                          ">35 SKIP" if (daily_adx and daily_adx > 35) else "OK",
                          delta_color="inverse" if (daily_adx and daily_adx > 35) else "off")

                st.markdown("---")

                # Per-leg cards
                leg_keys = ["BNF_CE", "BNF_PE", "SENSEX_PE"]
                leg_labels = {"BNF_CE": "BANKNIFTY CE", "BNF_PE": "BANKNIFTY PE", "SENSEX_PE": "SENSEX PE"}
                cols = st.columns(3)
                for col, key in zip(cols, leg_keys):
                    leg = legs_raw.get(key, {})
                    status = leg.get("status", "WARMUP")
                    sym    = leg.get("symbol", "—")
                    exp    = leg.get("expiry", "—")
                    ltp    = leg.get("ltp", 0)
                    bb     = leg.get("bb_now", {})
                    fill_p = leg.get("fill_price", 0)
                    sl_p   = leg.get("sl_price", 0)
                    mid    = leg.get("midpoint", 0)
                    pnl    = leg.get("gross_pnl", 0)

                    STATUS_COLOR = {
                        "WARMUP": "#5a7ba0", "READY": "#60a5fa", "SKIP_DAY": "#94a3b8",
                        "CHECKED": "#94a3b8", "LIMIT_PLACED": "#f59e0b", "ACTIVE": "#7b61ff",
                        "CLOSED": "#00c875",
                    }
                    color = STATUS_COLOR.get(status, "#5a7ba0")
                    with col:
                        with st.container(border=True):
                            st.markdown(f"**{leg_labels[key]}**")
                            st.markdown(
                                f'<span style="color:{color};font-weight:700;">{status}</span>',
                                unsafe_allow_html=True,
                            )
                            st.caption(f"{sym}  ·  Expiry: {exp}")
                            if ltp:
                                st.metric("LTP", f"₹{ltp:.2f}")
                            if bb:
                                st.metric("BB Upper", f"₹{bb.get('upper', 0):.2f}",
                                          "SIGNAL ✅" if bb.get("signal") else "below band")
                                st.metric("BB SMA", f"₹{bb.get('sma', 0):.2f}")
                            if mid:
                                st.metric("Midpoint", f"₹{mid:.2f}")
                            if status == "CLOSED" and pnl:
                                emoji = "🟢" if pnl > 0 else "🔴"
                                st.metric("Gross P&L", f"{emoji} ₹{pnl:,.0f}")

                # ── Active position detail (full-width — 3-col snapshot above is
                #    too narrow for the standard lifecycle card's 6-up metrics row) ──
                active_leg_items = [
                    (k, legs_raw.get(k, {})) for k in leg_keys
                    if legs_raw.get(k, {}).get("status") == "ACTIVE"
                ]
                if active_leg_items:
                    st.markdown("---")
                    st.markdown("**Active Position Detail**")
                    for key, leg in active_leg_items:
                        sym        = leg.get("symbol", "")
                        fill_p     = leg.get("fill_price", 0)
                        sl_p       = leg.get("sl_price", 0)
                        bb_now     = leg.get("bb_now", {}) or {}
                        target     = bb_now.get("sma") or (fill_p * 0.99 if fill_p else 0)
                        qty        = leg.get("qty", 0)
                        order_id   = leg.get("order_id", "")
                        entry_time = leg.get("entry_time", "") or ""
                        leg_ltp    = leg.get("ltp", 0) or ltps.get(sym, fill_p)
                        with st.container(border=True):
                            st.markdown(f"**{leg_labels[key]}**")
                            _render_active_position_lifecycle(
                                symbol=sym,
                                order_id=order_id,
                                entry_price=fill_p,
                                sl_price=sl_p,
                                target_price=target,
                                qty=qty,
                                entry_time=entry_time,
                                ltp=leg_ltp,
                                eod_exit_time="15:14",
                                decision_trail=_bnf_oc_entry_decision_trail(key, entry_time),
                                trail_phase_key="status",
                            )

                # Today's trades
                st.markdown("---")
                st.subheader("📋 Today's Trades")
                trades = _load_today_trades("banknifty_bb_opening_candle_bot")
                if trades:
                    _render_today_trades_detail(trades)
                else:
                    st.info("No trades logged yet today.")

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "BNF BB Opening Candle Bot — Execution Logic",
                "09:15 ATM option 1-min candle High > BB(20,2σ) → sell at midpoint via limit order at 09:16.",
                [
                    fc_start("☀️ Session Start (09:15)"),
                    fc_action("📡 Seed BB(20,2σ) from last 20 bars of prior day",
                              "BANKNIFTY CE · BANKNIFTY PE · SENSEX PE"),
                    fc_filter("Daily ADX(14) ≤ 35?", "⚠️ Strong trend — skip all legs today"),
                    fc_filter("India VIX at 09:15 < 18?", "⚠️ VIX elevated — skip (advisory)"),
                    fc_check("09:15 candle HIGH > BB(20,2σ) upper band?", "for each of the 3 legs independently"),
                    fc_action("📋 Place SELL LIMIT at (Close + High)/2", "at 09:16 open — simultaneously for each leg with signal"),
                    fc_check("09:16 bar closes — limit order filled?", "check orderbook at 09:17"),
                    fc_split(
                        "Filled",
                        fc_node_entry("🎯 Activate: SL = fill + 10 pts", "monitor SMA exit"),
                        "Not filled",
                        fc_node_exit("🚫 Cancel order", "no trade for this leg today"),
                    ),
                    fc_monitor("🔍 Monitor sold option: SL at tick level + SMA at bar level"),
                    fc_exit("🎯 Close ≤ evolving 20-bar SMA → EXIT (reversion)"),
                    fc_exit("🛑 LTP ≥ fill + 10 pts → EXIT (stop loss)"),
                    fc_exit("⏰ 15:14 IST → EOD EXIT (unconditional for all active legs)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            skip_day   = (state or {}).get("skip_day", False)
            daily_adx  = (state or {}).get("daily_adx")
            vix_ltp    = (state or {}).get("vix_ltp", 0)
            adx_ok     = (daily_adx is None) or (daily_adx <= 35)
            vix_ok     = (vix_ltp == 0) or (vix_ltp < 18)

            if skip_day:
                skip_reason = (state or {}).get("skip_reason", "")
                readiness = ("⛔", f"DAY SKIPPED — {skip_reason}", "#f87171")
            else:
                active_legs  = [k for k, v in legs_raw.items() if v.get("status") == "ACTIVE"]
                pending_legs = [k for k, v in legs_raw.items() if v.get("status") == "LIMIT_PLACED"]
                closed_legs  = [k for k, v in legs_raw.items() if v.get("status") == "CLOSED"]
                if active_legs:
                    readiness = ("📌", f"IN POSITION — {', '.join(active_legs)}", "#7b61ff")
                elif pending_legs:
                    readiness = ("⏳", f"AWAITING FILL — {', '.join(pending_legs)}", "#f59e0b")
                elif len(closed_legs) == 3:
                    readiness = ("✅", "All legs done for today", "#00c875")
                else:
                    readiness = ("🔍", "SCANNING — watching 09:15 bar for signal", "#60a5fa")

            render_decision_state(
                state,
                key="bnf_bb_oc",
                updates_note="Updates at each 1-min bar close (9:15 signal check · 9:16 fill check · ongoing SMA exit)",
                metrics=[
                    ("BANKNIFTY", f"{(state or {}).get('bnf_ltp', 0):,.1f}" if state else "—"),
                    ("SENSEX", f"{(state or {}).get('sensex_ltp', 0):,.1f}" if state else "—"),
                    ("VIX", f"{vix_ltp:.2f}" if vix_ltp else "—"),
                    ("Daily ADX(14)", f"{daily_adx:.1f}" if daily_adx else "—",
                     ">35 SKIP" if (daily_adx and daily_adx > 35) else "OK", "inverse" if (daily_adx and daily_adx > 35) else "off"),
                ],
                filters=[
                    ("📊", "Daily ADX(14) ≤ 35", adx_ok,
                     f"{daily_adx:.1f}" if daily_adx else "not computed"),
                    ("📈", "India VIX < 18 (advisory)", vix_ok,
                     f"{vix_ltp:.2f}" if vix_ltp else "no data"),
                    ("🤖", "3 legs initialized", all(leg.get("symbol") for leg in legs_raw.values()),
                     "BNF CE · BNF PE · SENSEX PE"),
                ],
                readiness=readiness,
            )

            # Per-leg status table
            st.markdown("---")
            st.markdown("**Leg Status**")
            for key in ["BNF_CE", "BNF_PE", "SENSEX_PE"]:
                leg = legs_raw.get(key, {})
                st.markdown(
                    f"**{key}** — `{leg.get('status','?')}`  "
                    f"symbol=`{leg.get('symbol','—')}`  "
                    f"fill=₹{leg.get('fill_price',0):.2f}  "
                    f"sl=₹{leg.get('sl_price',0):.2f}"
                )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("bb_opening_candle_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("banknifty_bb_opening_candle_bot")


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 8 — BANKNIFTY BB OPTIONS BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_bnf_bb_options_panel(ltps: dict):
    state = _load(STATE_FILES["BNF_BB_OPT"])

    # This bot migrated to live trading on fyers_cs (2026-06-29) — see bot_registry.py.
    # Any state file still sitting in another workspace's logs/ dir is a stale leftover
    # from before the migration; warn instead of silently rendering it as if live.
    _reg_entry = next((b for b in BOT_REGISTRY if b["bot"] == "banknifty_bb_options_bot"), None)
    _bot_ws    = (_reg_entry or {}).get("workspace", _REGISTRY_HOME_WS)
    _cur_ws    = st.session_state.get("selected_workspace_top", "CRK")
    if _reg_entry and _bot_ws != _cur_ws:
        _ws_name = WORKSPACES.get(_bot_ws, {}).get("name", _bot_ws)
        st.warning(
            f"⚠️ **{_reg_entry['label']}** now runs live on the **{_ws_name}** account "
            f"({_reg_entry.get('reason', 'migrated')}, {_reg_entry.get('status_date', '')}). "
            f"This isn't the account it runs on — any data shown below is a stale leftover "
            f"from before the migration, not a live feed."
        )

    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _bnf_bb_options_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "BANKNIFTY BB Options Bot — Execution Logic",
                "Sell the ATM option whose 1-min premium closes above its BB(20,2σ) upper band — re-arms for another signal after each exit (Tier-3 multi-trade).",
                [
                    fc_start("☀️ Session Start"),
                    fc_action("📡 Subscribe BANKNIFTY ATM CE + PE premium",
                              "1-min premium bars · warm BB(20, 2σ) on each · 15-min EMA(9,26) regime"),
                    fc_filter("Not a monthly expiry day?", "⚡ Expiry day — skip"),
                    fc_filter("Time in 09:30–14:00 IST?", "⏰ Outside window"),
                    fc_filter("Outside dead zone 11:00–12:30?", "⏸ Dead zone — skip (Stage 12 finding)"),
                    fc_filter("No position open right now?", "🔁 Slot busy — re-arms automatically once the open trade exits (Tier-3 multi-trade)"),
                    fc_check("A premium closes ≥ its BB(20,2σ) upper band AND ≥ normalized floor (1.3% of session-start spot)?",
                             "expensive → mean-reversion edge"),
                    fc_filter("CE only: 15-min EMA(9,26) regime bullish?",
                              "⛔ Bearish/not-warm — skip CE this bar (PE ungated, re-checks next bar)"),
                    fc_filter("Entry distance within normalized band above session mean (≥ ₹5 floor, ≤ 0.205% of spot)?",
                              "↔ Outside band — skip (no edge too close / oversized SL too far)"),
                    fc_filter("Market depth passes (spread ≤3%, imbalance ≤60%, min qty 300)?",
                              "📊 Thin/imbalanced book — skip (fail-open if no depth data)"),
                    fc_split(
                        "CE premium ≥ upper BB",
                        fc_node_exit("📉 SELL ATM CE", "MIS · SL symmetry (1.05× floor)"),
                        "PE premium ≥ upper BB",
                        fc_node_entry("📉 SELL ATM PE", "MIS · SL symmetry (1.05× floor)"),
                    ),
                    fc_monitor("🔍 Monitor sold option premium"),
                    fc_exit("🎯 Session-mean reversion — 1m close ≤ cumulative mean since 09:15 → EXIT"),
                    fc_exit("🛑 SL — premium ≥ symmetry SL (entry + entry−mean, floor 1.05×) → EXIT"),
                    fc_exit("⏰ 15:14 IST → EOD EXIT (unconditional)"),
                    fc_note("🔄 Tier-3 multi-trade: after any exit (target/SL/EOD), the signal slot re-arms and the bot watches for another entry the same session — NOT first-signal-only."),
                ],
            )

    if tab_state.open:
        with tab_state:
            _entry_win = state.get("entry_window", "09:30–14:00") if state else "09:30–14:00"
            _in_win    = _entry_window_open(_entry_win)
            _is_exp    = state.get("is_expiry_day", False) if state else False
            _fired     = state.get("signal_fired", False) if state else False
            _active    = state.get("active_trade") if state else None
            _ce_bb     = state.get("ce_bb", {}) if state else {}
            _pe_bb     = state.get("pe_bb", {}) if state else {}
            _ce_close  = _ce_bb.get("close", 0)
            _ce_upper  = _ce_bb.get("upper", 0)
            _pe_close  = _pe_bb.get("close", 0)
            _pe_upper  = _pe_bb.get("upper", 0)
            _ce_breach = bool(_ce_bb) and _ce_upper and _ce_close >= _ce_upper
            _pe_breach = bool(_pe_bb) and _pe_upper and _pe_close >= _pe_upper
            _signal    = _ce_breach or _pe_breach
            _multi     = state.get("multi_trade_active", False) if state else False
            _slot_free = (not _active) if _multi else (not _fired and not _active)

            # Dead zone 11:00–12:30 — signals in this window are hard-skipped (Stage 12 finding)
            _in_dead_zone = _entry_window_open("11:00–12:30")

            # Tier-3 normalized premium floor + entry-distance band (1.3% of session-start
            # spot / percent-of-spot band) — falls back to the legacy absolute values
            # (₹10 floor, ₹5–100 band) if an older bot build hasn't written these fields yet.
            _norm_active = state.get("normalized_filters_active", False) if state else False
            _floor       = state.get("premium_floor", 10.0) if state else 10.0
            _floor_pct   = state.get("premium_floor_pct") if state else None
            _dmin        = state.get("dist_min", 5.0) if state else 5.0
            _dmax        = state.get("dist_max", 100.0) if state else 100.0
            _ema_regime  = state.get("ema15_regime") if state else None

            # Entry distance — only meaningful once a breach exists
            _ce_sess_mean = (state.get("ce_session_mean") or 0) if state else 0
            _pe_sess_mean = (state.get("pe_session_mean") or 0) if state else 0
            _active_dist = None
            _active_ltp  = None
            if _ce_breach and _ce_sess_mean:
                _active_dist = _ce_close - _ce_sess_mean
                _active_ltp  = _ce_close
            elif _pe_breach and _pe_sess_mean:
                _active_dist = _pe_close - _pe_sess_mean
                _active_ltp  = _pe_close
            _dist_ok  = True if _active_dist is None else (_dmin <= _active_dist <= _dmax)
            _floor_ok = True if _active_ltp is None else (_active_ltp >= _floor)

            if _active:
                _ready = ("📌", "IN POSITION — monitoring sold option for SMA reversion / SL / EOD", "#7b61ff")
            elif _is_exp:
                _ready = ("🔴", "EXPIRY DAY — bot skips trading today", "#f87171")
            elif not _in_win:
                _ready = ("⏸", f"OUT OF WINDOW — signals only {_entry_win} IST", "#94a3b8")
            elif _fired and not _multi:
                _ready = ("✅", "First signal already taken today — done scanning", "#00c875")
            elif _fired and _multi:
                _ready = ("⚠️", "Signal fired but no open position — entry attempt likely failed "
                                "(re-arms only after a trade actually closes)", "#f59e0b")
            elif _signal and _floor_ok and _dist_ok:
                _ready = ("🟢", "SIGNAL LIVE — a premium breached its upper BB · order firing", "#00c875")
            elif _signal:
                _blocked_by = " & ".join(
                    n for n, ok in (("floor", _floor_ok), ("distance band", _dist_ok)) if not ok
                )
                _ready = ("🟡", f"BB breach detected but BLOCKED by {_blocked_by} filter — no entry", "#f59e0b")
            else:
                _ready = ("🔍", "SCANNING — watching CE & PE premiums vs upper BB", "#60a5fa")

            render_decision_state(
                state,
                key="bnf_bb",
                updates_note="Updates on each 1-min premium bar close",
                metrics=[
                    ("BANKNIFTY", f"{state.get('bnf_ltp', 0):,.1f}" if state and state.get("bnf_ltp") else "—"),
                    ("VIX", f"{state.get('vix_ltp', 0):.2f}" if state and state.get("vix_ltp") else "—"),
                    ("Expiry", (state.get("expiry") if state else "") or "—",
                     "⚡ EXPIRY DAY" if _is_exp else None, "inverse"),
                    ("Window", _entry_win, "🟢 OPEN" if _in_win else "🔴 CLOSED", "off"),
                    ("CE prem vs upper", f"{_ce_close:.1f}/{_ce_upper:.1f}" if _ce_bb else "—",
                     "≥ band" if _ce_breach else "below", "off"),
                    ("PE prem vs upper", f"{_pe_close:.1f}/{_pe_upper:.1f}" if _pe_bb else "—",
                     "≥ band" if _pe_breach else "below", "off"),
                    ("Premium floor", f"₹{_floor:,.0f}" + (f" ({_floor_pct:.1%} spot)" if _floor_pct else " (fixed)"),
                     "normalized" if _norm_active else "legacy", "off"),
                    ("15m EMA regime", (_ema_regime or "not warm").title(),
                     "CE gate only" if _ema_regime else None, "off"),
                ],
                filters=[
                    ("📅", "Not a monthly expiry day", not _is_exp, "expiry" if _is_exp else "ok"),
                    ("⏰", "Entry window 09:30–14:00 IST", _in_win, _entry_win),
                    ("⏸", "Outside dead zone (11:00–12:30 excluded)", not _in_dead_zone,
                     "in dead zone" if _in_dead_zone else "ok"),
                    ("🔁", "Signal slot free" + (" (multi-trade re-arms after exit)" if _multi else " (first signal only)"),
                     _slot_free, "free" if _slot_free else "used / in position"),
                    ("📊", "A premium ≥ its BB(20,2σ) upper", bool(_signal),
                     ("CE breach" if _ce_breach else "") + (" PE breach" if _pe_breach else "") or "none"),
                    ("💰", f"Premium ≥ floor ₹{_floor:,.0f}" + (" (normalized, 1.3% of spot)" if _norm_active else " (fixed ₹10)"),
                     _floor_ok, f"₹{_active_ltp:.1f}" if _active_ltp is not None else "n/a — no breach yet"),
                    ("↔", f"Entry distance ₹{_dmin:.1f}–₹{_dmax:.1f} above session mean" + (" (normalized band)" if _norm_active else ""),
                     _dist_ok, f"₹{_active_dist:.1f}" if _active_dist is not None else "n/a — no breach yet"),
                ],
                readiness=_ready,
            )
            st.caption(
                "ℹ️ Not reflected above — bot enforces this but doesn't yet expose live status in "
                "the state file: the 3-layer market depth filter (spread/imbalance/liquidity). "
                "Check the bot log for live verdicts on this filter."
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("bb_options_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("banknifty_bb_options_bot")


def _bnf_bb_options_entry_decision_trail(entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows at/before entry_time — the funnel state
    (MONITORING → ACTIVE) leading into the currently-open trade."""
    records = _read_jsonl_tail(LOGS_DIR / "banknifty_bb_options_decisions.jsonl", limit=3000)
    matched = [r for r in records if not entry_time or r.get("ts", "") <= entry_time]
    return matched[-n:]


def _bnf_bb_options_overview(ltps: dict, state: dict):
    with st.container(border=True):
        st.subheader("📉 BANKNIFTY BB Options Bot")
        st.markdown(
            '<div class="research-badge">'
            'Research: BB(20,2σ) on 1-min ATM option premium — premium closes ≥ upper BB → SELL that option  |  '
            'IS Sharpe +2.20 (non-expiry)  |  OOS Sharpe +2.69  |  WR 86%  |  10/10 stages PASS  |  '
            'Monthly expiry-day SKIP  |  Entry 09:30–14:00 IST  |  SL symmetry (1:1 R:R, floor 1.05×)  |  Exit on session-mean reversion'
            '</div>',
            unsafe_allow_html=True,
        )

        if not state:
            st.error(
                "🔌 Bot not running — state file absent. Start the bot to see live data. "
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

        _norm_active_ov = state.get("normalized_filters_active", False)
        _multi_active_ov = state.get("multi_trade_active", False)
        _floor_ov     = state.get("premium_floor", 10.0)
        _floor_pct_ov = state.get("premium_floor_pct")
        _dmin_ov      = state.get("dist_min", 5.0)
        _dmax_ov      = state.get("dist_max", 100.0)
        _floor_label  = (f"≥ ₹{_floor_ov:,.0f} (normalized, {_floor_pct_ov:.1%} of session-start spot)"
                         if _norm_active_ov and _floor_pct_ov else f"min ₹{_floor_ov:,.0f}")
        _dist_label   = (f"₹{_dmin_ov:,.0f}–₹{_dmax_ov:,.0f} from session mean (normalized band)"
                         if _norm_active_ov else f"₹{_dmin_ov:,.0f}–₹{_dmax_ov:,.0f} from session mean")

        with st.expander("📖 Strategy & Research Details"):
            st.markdown(
                '<div class="research-badge">'
                'Research: BB(20,2σ) on 1-min ATM option premium — premium closes ≥ upper BB → SELL that option  |  '
                'Monthly expiry-day SKIP  |  Entry 09:30–14:00 IST (dead zone 11:00–12:30 excluded)  |  '
                'SL symmetry (1:1 R:R, floor 1.05×)  |  Exit on session-mean reversion  |  ' +
                ('Tier-3 multi-trade: re-arms after each exit' if _multi_active_ov else 'First-signal-only per session') +
                '</div>',
                unsafe_allow_html=True,
            )
            ch1, ch2 = st.columns(2)
            with ch1:
                st.markdown("**Entry Conditions**")
                st.markdown(
                    '<div class="condition-row">'
                    '🔍 BB(20,2σ) 1-min upper band touch/cross<br>'
                    f'✅ Option premium {_floor_label}<br>'
                    '✅ Entry window open (09:30–14:00 IST, excl. 11:00–12:30 dead zone)<br>'
                    f'✅ Entry distance {_dist_label}<br>'
                    '✅ CE only: 15-min EMA(9,26) regime bullish (PE ungated)<br>'
                    '✅ Market depth: spread/imbalance/liquidity pass (fail-open if no data)<br>'
                    '✅ ' + ('No position currently open (multi-trade: re-arms after each exit)' if _multi_active_ov
                             else 'No trade taken yet today (first signal only)') + '<br>'
                    '</div>',
                    unsafe_allow_html=True,
                )
            with ch2:
                st.markdown("**Exit Rules**")
                st.markdown(
                    '<div class="condition-row">'
                    '🛑 Stop-loss: symmetry SL — entry + (entry − session mean), floor 1.05× entry<br>'
                    '🎯 Mean-reversion exit: 1-min close ≤ cumulative session mean (since 09:15)<br>'
                    '⏰ EOD exit: 15:14 IST unconditional close<br>'
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
            sym        = active.get("symbol", "")
            side       = active.get("opt_type", "")
            entry_p    = float(active.get("entry_prem", 0))
            sl_p       = float(active.get("sl_prem", 0))
            sma_tgt    = float(active.get("sma_target", 0))
            qty        = int(active.get("qty", 0))
            order_id   = active.get("order_id", "")
            entry_time = active.get("entry_time", "")
            ltp        = ltps.get(sym, entry_p)

            st.markdown(
                '<div class="signal-banner-on">'
                f'🟢 POSITION ACTIVE — {side} {sym} | SL {sl_p:.2f} | Target SMA {sma_tgt:.2f}'
                '</div>',
                unsafe_allow_html=True,
            )

            _render_active_position_lifecycle(
                symbol=sym,
                order_id=order_id,
                entry_price=entry_p,
                sl_price=sl_p,
                target_price=sma_tgt,
                qty=qty,
                entry_time=entry_time,
                ltp=ltp,
                eod_exit_time="15:14",
                decision_trail=_bnf_bb_options_entry_decision_trail(entry_time),
                trail_phase_key="phase",
            )

        elif signal_fired:
            st.markdown(
                '<div class="signal-banner-wait">'
                '🟡 SIGNAL FIRED — no open position right now. ' +
                ('Slot re-arms automatically once a trade closes (multi-trade mode) — '
                 'this state means the last entry attempt likely failed.' if _multi_active_ov
                 else 'Trade already taken today (one trade per session).') +
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
            no_trade     = not active if _multi_active_ov else (not signal_fired and not active)
            _slot_note   = "no position open (multi-trade: re-arms after each exit)" if _multi_active_ov else "one signal per session"
            if ce_triggered or pe_triggered:
                _breach_ltp  = bb_ce.get("close", 0) if ce_triggered else bb_pe.get("close", 0)
                _floor_row   = f'{_tick(_breach_ltp >= _floor_ov)} Premium ≥ floor {_floor_label} (breach @ ₹{_breach_ltp:.2f})<br>'
            else:
                _floor_row   = f'⚪ Premium ≥ floor {_floor_label} — n/a, no breach yet<br>'
            st.markdown(
                f'<div class="condition-row">'
                f'{_tick(ce_triggered or pe_triggered)} Premium (CE or PE) closes ≥ upper BB(20,2σ)<br>'
                f'&nbsp;&nbsp;CE: {"✅" if ce_triggered else "⚪"} ({bb_ce.get("close",0):.2f} vs {bb_ce.get("upper",0):.2f}) '
                f'[{bb_ce.get("bars",0)} bars]<br>'
                f'&nbsp;&nbsp;PE: {"✅" if pe_triggered else "⚪"} ({bb_pe.get("close",0):.2f} vs {bb_pe.get("upper",0):.2f}) '
                f'[{bb_pe.get("bars",0)} bars]<br>'
                f'{_tick(in_window)} Entry window open ({entry_win} IST)<br>'
                f'{_floor_row}'
                f'{_tick(no_trade)} No trade currently open ({_slot_note})<br>'
                f'{_tick(not is_exp_day)} Not a BANKNIFTY monthly expiry day'
                f'</div>',
                unsafe_allow_html=True,
            )

        with ch2:
            st.markdown("**Exit Rules**")
            _sizing = (f"{n_lots} lot(s) ({n_lots * lot_size} units)"
                       if isinstance(lot_size, int) else f"{n_lots} lot(s)")
            st.markdown(
                '<div class="condition-row">'
                '🛑 <b>Stop-loss</b>: tick-level SL — exit if LTP ≥ symmetry SL '
                '(entry + (entry − session mean), floor 1.05× entry)<br>'
                '🎯 <b>Mean-reversion exit</b>: 1-min close ≤ cumulative session mean (since 09:15) → exit<br>'
                '⏰ <b>EOD exit</b>: 15:14 IST unconditional close<br>'
                f'📦 <b>Sizing</b>: {_sizing} — SELL CE or PE whichever triggered first<br>'
                '⛔ <b>Expiry filter</b>: all BANKNIFTY monthly expiry days skipped'
                '</div>',
                unsafe_allow_html=True,
            )



# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 9b — BB MEAN REVERSION BOT
# ══════════════════════════════════════════════════════════════════════════════

def render_bb_mean_reversion_panel(ltps: dict):
    state = _load(STATE_FILES["BB_MEAN_REV"])
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _bb_mean_reversion_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "BB Mean Reversion Bot — Execution Logic",
                "BUY ATM monthly PE when a red 1-min BANKNIFTY candle is rejected at the 2σ upper band (5 gates).",
                [
                    fc_start("☀️ Session Start"),
                    fc_action("📡 1-min BANKNIFTY index bars",
                              "warm BB(20,2σ) · daily 20-SMA trend · monthly DTE"),
                    fc_filter("Trend: prev close ≤ 20-day SMA?", "📈 Bull regime — skip"),
                    fc_filter("Monthly DTE outside 8–14 day band?", "DTE in dead band"),
                    fc_filter("Time in 09:30–14:45 IST?", "⏰ Outside window"),
                    fc_filter("1-min bar RED and high &gt; upper BB?", "No spike rejection"),
                    fc_filter("HTF: trigger close &lt; current 5-min open?", "HTF not aligned"),
                    fc_filter("Natural R:R ≥ 1.25?", "R:R too low"),
                    fc_filter("No signal taken yet today?", "🔁 First signal only"),
                    fc_entry("🟢 BUY ATM monthly PE (DEBIT)", "1 lot · no margin"),
                    fc_monitor("🔍 Monitor spot + PE LTP"),
                    fc_exit("🎯 Target — PE ≥ entry + 4× risk → EXIT"),
                    fc_exit("🛑 SL — spot ≥ trigger_high (or PE ≤ sl_opt) → EXIT"),
                    fc_exit("⏰ 15:15 IST → EOD EXIT (unconditional)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _win      = "09:30–14:45"
            _in_win   = _entry_window_open(_win)
            _trend_ok = state.get("trend_ok", False) if state else False
            _dte_ok   = state.get("dte_ok", False) if state else False
            _fired    = state.get("signal_fired", False) if state else False
            _active   = state.get("active_trade") if state else None
            _bars     = state.get("bars_loaded", 0) if state else 0
            _phase    = (state.get("phase", "unknown") if state else "unknown").replace("_", " ").title()
            _slot_free = not _fired and not _active

            if _active:
                _ready = ("📌", "IN POSITION — long PE · monitoring target / spot-SL / EOD", "#7b61ff")
            elif not _in_win:
                _ready = ("⏸", f"OUT OF WINDOW — signals only {_win} IST", "#94a3b8")
            elif not _trend_ok:
                _ready = ("🔴", "BLOCKED — bull regime (prev close > 20-day SMA)", "#f87171")
            elif not _dte_ok:
                _ready = ("🔴", "BLOCKED — monthly DTE inside 8–14 day dead band", "#f87171")
            elif _fired:
                _ready = ("✅", "First signal already taken today — done scanning", "#00c875")
            else:
                _ready = ("🔍", "SCANNING — watching for red candle rejected at 2σ upper band", "#60a5fa")

            render_decision_state(
                state,
                key="bb_mean_rev",
                updates_note="Updates on each 1-min bar close",
                metrics=[
                    ("BANKNIFTY", f"{state.get('bnf_ltp', 0):,.0f}" if state and state.get("bnf_ltp") else "—"),
                    ("PE LTP", f"₹{state.get('pe_ltp', 0):.2f}" if state and state.get("pe_ltp") else "—"),
                    ("Expiry", (state.get("expiry") if state else "") or "—"),
                    ("Phase", _phase),
                    ("Window", _win, "🟢 OPEN" if _in_win else "🔴 CLOSED", "off"),
                    ("Bars", f"{_bars}"),
                ],
                filters=[
                    ("📉", "Trend gate — Bear/Sideways (prev close ≤ 20d SMA)", _trend_ok,
                     "ok" if _trend_ok else "bull regime"),
                    ("📅", "DTE gate — monthly expiry outside 8–14 days", _dte_ok,
                     "ok" if _dte_ok else "dead band"),
                    ("⏰", "Entry window 09:30–14:45 IST", _in_win, _win),
                    ("🔁", "First signal slot free today", _slot_free,
                     "free" if _slot_free else "used / in position"),
                ],
                readiness=_ready,
                checklist_caption="Gates 1/2/5 shown live; trigger/HTF/R:R evaluate intrabar (see bot log).",
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("bb_mean_reversion_candle_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("bb_mean_reversion_bot")


def _bb_mean_reversion_entry_decision_trail(entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows at/before entry_time — the per-bar phase path
    leading into the currently-open PE leg."""
    records = _read_jsonl_tail(LOGS_DIR / "bb_mean_reversion_decisions.jsonl", limit=3000)
    matched = [r for r in records if not entry_time or r.get("ts", "") <= entry_time]
    return matched[-n:]


def _bb_mean_reversion_overview(ltps: dict, state: dict):
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
                "🔌 Bot not running — state file absent. Start the bot to see live data. "
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
            entry_opt      = float(active.get("entry_opt", 0))
            sl_opt         = float(active.get("sl_opt", 0))
            tp_opt         = float(active.get("tp_opt", 0))
            sl_index       = float(active.get("sl_index", 0))
            risk_opt       = float(active.get("risk_opt", 0))
            nat_rr_v       = float(active.get("nat_rr", 0))
            qty            = int(active.get("qty", 0))
            order_id       = active.get("order_id", "")
            entry_time_raw = active.get("entry_time", "")
            sym            = active.get("symbol", "")
            ltp            = pe_ltp if pe_ltp > 0 else entry_opt

            st.caption(f"Index SL: {sl_index:.0f}  |  NatRR at entry: {nat_rr_v:.2f}  |  Risk: {risk_opt:.2f} pts")
            _render_active_position_lifecycle(
                symbol=sym,
                order_id=order_id,
                entry_price=entry_opt,
                sl_price=sl_opt,
                target_price=tp_opt,
                qty=qty,
                entry_time=entry_time_raw,
                ltp=ltp,
                eod_exit_time="15:14",
                decision_trail=_bb_mean_reversion_entry_decision_trail(entry_time_raw),
                direction="long",
            )

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
        elif signal_fired:
            st.markdown(
                '<div class="signal-banner-wait">'
                '🟡 Signal taken today — one trade per session (monitoring only)'
                '</div>',
                unsafe_allow_html=True,
            )
            _render_today_trades_detail(_load_today_trades("bb_mean_reversion_bot"))
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
#  SECTION 10 — HA OPTIONS BOT — RETIRED 2026-07-10
#  Dead code — call site removed. Kept for archive reference only.
# ══════════════════════════════════════════════════════════════════════════════

def render_ha_options_panel(ltps: dict):  # RETIRED — do not call
    state = _load(STATE_FILES["HA_OPTIONS"])
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _ha_options_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "HA Options Bot — Execution Logic (NIFTY · BANKNIFTY · SENSEX)",
                "Sell the ATM option on a Heiken-Ashi candle flip; HA candles reset fresh each day.",
                [
                    fc_start("☀️ Session Start"),
                    fc_action("📊 Compute Heiken-Ashi candles (reset daily)",
                              "NIFTY 5m · BANKNIFTY 15m · SENSEX 5m"),
                    fc_filter("Time in 09:30–14:30 IST?", "⏰ Outside window"),
                    fc_filter("Stage-8 vol filter pass? (prior-day range ≥ thr)", "📉 Too quiet"),
                    fc_filter("Instrument not yet traded today?", "🔁 One trade / instrument"),
                    fc_check("HA candle flips this bar?"),
                    fc_split(
                        "BULLISH HA flip",
                        fc_node_exit("📉 SELL ATM CE", "MIS · 1 lot"),
                        "BEARISH HA flip",
                        fc_node_entry("📉 SELL ATM PE", "MIS · 1 lot"),
                    ),
                    fc_monitor("🔍 Monitor HA direction + swing stop"),
                    fc_exit("🔄 HA reversal against position → EXIT"),
                    fc_exit("📌 Swing stop — 5-bar protective index level → EXIT"),
                    fc_exit("⏰ 15:20 IST → EOD EXIT (unconditional)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _insts   = state.get("instruments", {}) if state else {}
            _win     = state.get("entry_window", "09:30–14:30") if state else "09:30–14:30"
            _in_win  = _entry_window_open(_win)
            _vix     = state.get("vix_ltp", 0) if state else 0
            _any_act = any(i.get("active_trade") for i in _insts.values())

            def _ha_label(sig):
                return "📈 Bull" if sig == 1 else ("📉 Bear" if sig == -1 else "⚪ Neut")

            _inst_metrics = []
            _inst_filters = []
            for _sym, _emoji in (("NIFTY", "🟦"), ("BANKNIFTY", "🟧"), ("SENSEX", "🟥")):
                _d = _insts.get(_sym, {})
                _ltp = _d.get("ltp", 0)
                _sig = _d.get("ha_signal", 0)
                _done = _d.get("trade_done_today", False)
                _vol = _d.get("vol_filter_pass", True)
                _act = bool(_d.get("active_trade"))
                _ready_scan = _vol and not _done and not _act
                _inst_metrics.append(
                    (_sym, f"{_ltp:,.1f}" if _ltp else "—",
                     f"{_ha_label(_sig)} · {_d.get('tf','?')}m", "off")
                )
                _note = ("in position" if _act else
                         ("traded today" if _done else
                          ("vol filter fail" if not _vol else f"scanning · HA {_ha_label(_sig)}")))
                _inst_filters.append((_emoji, f"{_sym} ready to scan", _ready_scan, _note))

            if _any_act:
                _ready = ("📌", "IN POSITION — monitoring HA reversal / swing stop / EOD", "#7b61ff")
            elif not _in_win:
                _ready = ("⏸", f"OUT OF WINDOW — signals only {_win} IST", "#94a3b8")
            else:
                _ready = ("🔍", "SCANNING — awaiting Heiken-Ashi flip on a ready instrument", "#60a5fa")

            render_decision_state(
                state,
                key="ha_options",
                updates_note="Updates on each instrument's HA bar close",
                metrics=[("VIX", f"{_vix:.2f}" if _vix else "—"),
                         ("Window", _win, "🟢 OPEN" if _in_win else "🔴 CLOSED", "off")]
                        + _inst_metrics,
                filters=[("⏰", "Entry window 09:30–14:30 IST", _in_win, _win)] + _inst_filters,
                readiness=_ready,
                checklist_title="🔍 Per-Instrument Readiness",
                checklist_caption="Each instrument trades once/day; the HA flip itself fires on the bar close.",
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("ha_options_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("ha_options_bot")


def _ha_options_overview(ltps: dict, state: dict):
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
                         if (t.get("symbol") or "").upper().startswith(sym.upper())]
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
                logger.exception("Error loading trades.csv")
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
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _nts_obi_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "NTS + OBI Gate Bot — Execution Logic (SHORT-ONLY experiment)",
                "On a bearish NTS signal, gate the sell-CE entry by the ATM CE order-book imbalance.",
                [
                    fc_start("☀️ Session Start"),
                    fc_action("📚 1-min NIFTY: warm EMA/ADX/RSI/MACD",
                              "+ subscribe ATM CE depth-50 over WebSocket"),
                    fc_filter("VIX ≤ threshold at 09:40?", "🌡️ Day skipped"),
                    fc_filter("Time inside entry window?", "⏰ Outside window"),
                    fc_filter("NTS bearish signal (5 conditions)?", "No signal"),
                    fc_filter("OBI warmup complete (≥ 50 ticks)?", "⏳ OBI warming"),
                    fc_check("Weighted OBI of ATM CE < 0?",
                             "net sellers on the CE we want to short"),
                    fc_split(
                        "OBI ≥ 0 (bid pressure)",
                        '<div class="fc-node fc-block">👻 Ghost-track (counterfactual)</div>',
                        "OBI < 0 (net sellers)",
                        fc_node_entry("📉 SELL ATM CE (paper)", "MIS · SL 2× entry"),
                    ),
                    fc_monitor("🔍 Monitor CE premium — SL 2× entry"),
                    fc_exit("⏰ 15:20 IST → EOD EXIT (unconditional)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _win      = state.get("entry_window", "10:00–13:00") if state else "10:00–13:00"
            _in_win   = _entry_window_open(_win)
            _vix_ok   = state.get("vix_ok", True) if state else True
            _obi      = state.get("obi_current") if state else None
            _obi_thr  = state.get("obi_threshold", 0.0) if state else 0.0
            _obi_ticks = state.get("obi_ticks_today", 0) if state else 0
            _warm_ok  = _obi_ticks >= 50
            _gate_open = (_obi is not None) and (_obi < _obi_thr)
            _taken    = state.get("trade_taken_today", False) if state else False
            _active   = state.get("active_trade") if state else None
            _ghost    = state.get("ghost_trade") if state else None
            _sigs     = state.get("signals_today", 0) if state else 0
            _blocked  = state.get("signals_blocked", 0) if state else 0

            if _active:
                _ready = ("📌", "IN POSITION — short ATM CE (OBI gate opened) · monitoring SL / EOD", "#7b61ff")
            elif _ghost:
                _ready = ("👻", "GHOST-TRACKING — signal fired but OBI gate blocked the real entry", "#a78bfa")
            elif not _vix_ok:
                _ready = ("🔴", "DAY SKIPPED — VIX above threshold at 09:40", "#f87171")
            elif not _in_win:
                _ready = ("⏸", f"OUT OF WINDOW — signals only {_win} IST", "#94a3b8")
            elif not _warm_ok:
                _ready = ("⏳", f"OBI WARMING — {_obi_ticks}/50 depth ticks received", "#fbbf24")
            else:
                _ready = ("🔍", "SCANNING — waiting for bearish NTS signal, then OBI gate", "#60a5fa")

            render_decision_state(
                state,
                key="nts_obi",
                updates_note="Updates on each 1-min bar / depth tick",
                metrics=[
                    ("NIFTY spot", f"{state.get('nifty_spot', 0):,.1f}" if state and state.get("nifty_spot") else "—"),
                    ("ATM CE", (state.get("atm_symbol") if state else "") or "—"),
                    ("OBI now", f"{_obi:+.3f}" if _obi is not None else "—",
                     "gate open" if _gate_open else "gate shut", "off"),
                    ("OBI ticks", f"{_obi_ticks}", "✅ warm" if _warm_ok else "⏳ warming", "off"),
                    ("Signals today", f"{_sigs}", f"{_blocked} blocked", "off"),
                    ("Window", _win, "🟢 OPEN" if _in_win else "🔴 CLOSED", "off"),
                ],
                filters=[
                    ("🌡️", "VIX filter OK (≤ threshold at 09:40)", _vix_ok, "ok" if _vix_ok else "skipped"),
                    ("⏰", "Entry window open", _in_win, _win),
                    ("📊", "OBI warmup complete (≥ 50 ticks)", _warm_ok, f"{_obi_ticks} ticks"),
                    ("✅", "OBI gate open (OBI < threshold)", _gate_open,
                     f"OBI {_obi:+.3f} vs {_obi_thr:+.1f}" if _obi is not None else "no OBI yet"),
                    ("🔁", "No trade taken today", not _taken, "free" if not _taken else "done"),
                ],
                readiness=_ready,
            )

    if tab_research.open:
        with tab_research:
            st.info("No dedicated research study for NTS + OBI Gate — this is a live experiment combining two existing signals.")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("NTS_OBI")


def _nts_obi_overview(ltps: dict, state: dict):
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
            st.error("🔌 Bot not running — state file absent. Start the bot to see live data. Check `nifty_trend_seller_obi/logs/nts_obi_state.json`.")
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
            qty     = active.get("qty", NIFTY_LOT_SIZE * active.get("lots", LOTS))

            if obi_ent is not None:
                st.caption(f"OBI at entry: {obi_ent:+.1f}")

            _render_active_position_lifecycle(
                symbol=sym,
                order_id="",
                entry_price=entry,
                sl_price=sl,
                target_price=None,
                qty=qty,
                entry_time=active.get("entry_time", ""),
                ltp=ltp or entry,
                eod_exit_time="15:14",
                decision_trail=None,
            )
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

def _nifty_macd_map_entry_decision_trail(entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows at/before entry_time — the per-bar phase path
    leading into the currently-open leg."""
    records = _read_jsonl_tail(LOGS_DIR / "nifty_macd_map_decisions.jsonl", limit=3000)
    matched = [r for r in records if not entry_time or r.get("ts", "") <= entry_time]
    return matched[-n:]


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

    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview",
        "🗺️ Strategy Flowchart",
        "🧠 Live Decision State",
        "📖 Research Findings",
        "📈 Performance",
    ], on_change="rerun")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════════
    if tab_overview.open:
        with tab_overview:
            if not state:
                st.error("🔌 Bot not running — state file absent. Start the bot to see live data.")
            else:
                updated_at  = state.get("updated_at", "")
                expiry      = state.get("expiry") or "—"
                bars_loaded = state.get("bars_loaded", 0)
                hist_std    = state.get("hist_std", 0.0)
                indicators  = state.get("indicators", {})
                active_pe   = state.get("active_pe")
                active_ce   = state.get("active_ce")

                # Index LTP arrives already paise-converted from the WS layer
                # (broker/fyers/streaming/fyers_mapping.hsm_price_to_rupees).
                nifty_ltp  = indicators.get("nifty_ltp", 0)
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
                        sym        = trade.get("symbol", "")
                        entry_p    = float(trade.get("entry_prem", 0))
                        sl_p       = float(trade.get("sl_prem", 0))
                        qty        = int(trade.get("qty", 0))
                        order_id   = trade.get("order_id", "")
                        entry_time = trade.get("entry_time", "")
                        ltp        = ltps.get(sym, entry_p)

                        st.caption(leg_label)
                        _render_active_position_lifecycle(
                            symbol=sym,
                            order_id=order_id,
                            entry_price=entry_p,
                            sl_price=sl_p,
                            target_price=None,
                            qty=qty,
                            entry_time=entry_time,
                            ltp=ltp,
                            eod_exit_time="15:14",
                            decision_trail=_nifty_macd_map_entry_decision_trail(entry_time),
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
    if tab_flow.open:
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
            # st.iframe auto-detects a raw HTML string and sandboxes it in an iframe
            # (replaces the deprecated st.components.v1.html); height="content"
            # auto-sizes to the flowchart instead of a fixed-height scroll box.
            st.iframe(flowchart_html, height="content")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — LIVE DECISION STATE
    # ══════════════════════════════════════════════════════════════════════════
    if tab_state.open:
        with tab_state:
            if not state:
                st.error("🔌 Bot not running — state file absent. Start the bot to see live decision data.")
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

                nifty_disp = indicators.get("nifty_ltp", 0)
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

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("macd_money_map_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("nifty_macd_map_bot")


# (Tick Stasher page removed 2026-04-27 — tick_stasher.py retired; no active bot
#  reads live_ticks.duckdb. Only retired candle_breaker_bot consumed it.)


def render_macd_m2_sell_panel(ltps: dict):
    """
    Panel for the MACD M2 Sell Options Bot (NIFTY + BANKNIFTY):
      Tab 1 — Overview (position cards + key metrics)
      Tab 2 — Strategy Flowchart
      Tab 3 — Research Findings
      Tab 4 — Performance
    """
    state = _load(STATE_FILES["MACD_M2_SELL"])

    st.markdown("## 📉 MACD M2 Sell Options Bot")
    st.caption(
        "MACD(12,26,9) zero-line crossover (M2) + SR3 pivot ±0.2% · 15-min bars · "
        "Bull → SELL ATM PE · Bear → SELL ATM CE · NIFTY + BANKNIFTY, 5 lots each · "
        "SL 1.5× credit · Target keep 50% · EOD 15:14 · Status: Paper trading"
    )

    tab_overview, tab_flow, tab_research, tab_perf = st.tabs([
        "📊 Overview",
        "🗺️ Strategy Flowchart",
        "📖 Research Findings",
        "📈 Performance",
    ], on_change="rerun")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════════
    if tab_overview.open:
        with tab_overview:
            if not state:
                st.error("🔌 Bot not running — state file absent. Start the bot to see live data.")
            else:
                positions  = state.get("positions", {}) or {}
                state_ltp  = state.get("ltp", {}) or {}
                indicators = state.get("indicators", {}) or {}
                last_update = state.get("last_update", "")

                has_pos = any(positions.get(sym) for sym in ("NIFTY", "BANKNIFTY"))
                if has_pos:
                    banner_icon, banner_msg, banner_col = "📌", "IN POSITION", "#7b61ff"
                else:
                    banner_icon, banner_msg, banner_col = "🔍", "SCANNING — awaiting M2 + SR3 signal", "#60a5fa"

                updated_fmt = last_update[:19].replace("T", " ") if last_update else "—"
                st.markdown(
                    f'<div style="background:{banner_col}22;border-left:4px solid {banner_col};'
                    f'padding:10px 16px;border-radius:6px;margin-bottom:12px;">'
                    f'<span style="font-size:1.3em">{banner_icon}</span> '
                    f'<strong style="color:{banner_col};font-size:1.05em">{banner_msg}</strong>'
                    f'<span style="float:right;opacity:.6;font-size:.85em">Updated {updated_fmt}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                with st.expander("📖 Strategy & Research Details"):
                    st.markdown(
                        '<div style="background:#1e293b;border-radius:6px;padding:10px 14px;'
                        'font-size:.85em;color:#cbd5e1;margin-bottom:10px">'
                        'MACD(12,26,9) zero-line cross (M2) + SR3 pivot ±0.2% → sell ATM CE/PE &nbsp;|&nbsp; '
                        'IS Sharpe 9.42, OOS Sharpe 6.16 &nbsp;|&nbsp; Walk-forward 10/10 windows positive &nbsp;|&nbsp; '
                        'Entry 09:15–14:30 IST &nbsp;|&nbsp; SL 1.5× credit, target keep 50%'
                        '</div>',
                        unsafe_allow_html=True,
                    )
                    ec1, ec2 = st.columns(2)
                    with ec1:
                        st.markdown("**Entry Conditions**")
                        st.markdown(
                            '<div style="font-size:.85em;color:#94a3b8;line-height:1.7">'
                            'Entry window open (09:15–14:30 IST)<br>'
                            'NIFTY + BANKNIFTY, 15-min bars<br>'
                            'Bull M2 + SR3 → Sell ATM PE &nbsp;|&nbsp; Bear M2 + SR3 → Sell ATM CE<br>'
                            'DTE ≥ 2 &nbsp;·&nbsp; Credit ≥ ₹10'
                            '</div>',
                            unsafe_allow_html=True,
                        )
                    with ec2:
                        st.markdown("**Exit Rules**")
                        st.markdown(
                            '<div style="font-size:.85em;color:#94a3b8;line-height:1.7">'
                            'Stop loss: premium ≥ 1.5× credit<br>'
                            'Target: premium ≤ 50% of credit<br>'
                            'EOD exit: 15:14 IST, unconditional<br>'
                            'Lots: NIFTY 125 qty &nbsp;·&nbsp; BANKNIFTY 75 qty'
                            '</div>',
                            unsafe_allow_html=True,
                        )

                st.markdown("### 📌 NIFTY / BANKNIFTY")
                col_nifty, col_bnf = st.columns(2)
                for col, sym in ((col_nifty, "NIFTY"), (col_bnf, "BANKNIFTY")):
                    ind        = indicators.get(sym, {}) or {}
                    pos        = positions.get(sym)
                    ltp_val    = state_ltp.get(sym, 0)
                    macd_line  = ind.get("macd_line")
                    cross      = ind.get("cross", "none")
                    pivot      = ind.get("pivot")
                    dist_pct   = ind.get("pivot_dist_pct")
                    sr3_ok     = ind.get("sr3_ok")
                    bar_ts     = ind.get("bar_ts", "")[11:16] if ind.get("bar_ts") else "—"

                    if cross == "bull":
                        badge_txt, badge_bg, badge_fg = "BULL M2", "#00c87533", "#00c875"
                    elif cross == "bear":
                        badge_txt, badge_bg, badge_fg = "BEAR M2", "#f8717133", "#f87171"
                    else:
                        badge_txt, badge_bg, badge_fg = "NO CROSS", "#94a3b833", "#94a3b8"

                    macd_color = "#94a3b8"
                    macd_txt   = "—"
                    if macd_line is not None:
                        macd_color = "#00c875" if macd_line >= 0 else "#f87171"
                        macd_txt   = f"{macd_line:+.2f}"

                    if sr3_ok is None:
                        sr3_icon, sr3_color, sr3_txt = "—", "#94a3b8", "no data yet"
                    elif sr3_ok:
                        sr3_icon, sr3_color, sr3_txt = "✅", "#00c875", f"Yes, {dist_pct:.2f}%"
                    else:
                        sr3_icon, sr3_color, sr3_txt = "❌", "#f87171", f"No, {dist_pct:.2f}%" if dist_pct is not None else "No"

                    cross_icon = {"bull": "✅", "bear": "✅"}.get(cross, "➖")

                    def _row(label, value, color="#e2e8f0"):
                        return (
                            f'<div style="display:flex;justify-content:space-between;padding:6px 0;'
                            f'border-bottom:1px solid #1e293b;font-size:.86em">'
                            f'<span style="color:#94a3b8">{label}</span>'
                            f'<span style="color:{color}">{value}</span></div>'
                        )

                    card = (
                        f'<div style="border:1px solid #1e293b;border-radius:10px;padding:14px;margin-bottom:8px">'
                        f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">'
                        f'<span style="font-weight:600;font-size:1.02em">{sym}</span>'
                        f'<span style="background:{badge_bg};color:{badge_fg};font-size:.72em;'
                        f'padding:2px 8px;border-radius:8px">{badge_txt}</span></div>'
                        + _row("LTP", f"{ltp_val:,.2f}" if ltp_val else "—")
                        + _row("MACD (15m)", macd_txt, macd_color)
                        + _row("Zero-line cross", f"{cross_icon} {cross.upper()} · bar {bar_ts}" if cross != "none" else f"{cross_icon} none")
                        + _row("Within ±0.2% of pivot", f"{sr3_icon} {sr3_txt}" + (f" (PP {pivot:,.1f})" if pivot else ""), sr3_color)
                        + '</div>'
                    )
                    col.markdown(card, unsafe_allow_html=True)

                    with col:
                        if pos:
                            opt_sym    = pos.get("opt_symbol", "")
                            entry_prem = float(pos.get("credit", 0))
                            sl_prem    = float(pos.get("sl_level", 0))
                            tgt_prem   = float(pos.get("tgt_level", 0))
                            qty        = int(pos.get("quantity", 0))
                            entry_time = pos.get("entry_time", "")
                            opt_ltp    = ltps.get(opt_sym, entry_prem)

                            st.caption(f"SOLD {pos.get('opt_type','')} ({pos.get('direction','').upper()})")
                            _render_active_position_lifecycle(
                                symbol=opt_sym,
                                order_id="",
                                entry_price=entry_prem,
                                sl_price=sl_prem,
                                target_price=tgt_prem,
                                qty=qty,
                                entry_time=entry_time,
                                ltp=opt_ltp,
                                eod_exit_time="15:14",
                                decision_trail=None,
                            )
                        else:
                            st.caption("No open position — scanning.")

                if last_update:
                    age_sec, age_label = _staleness(last_update)
                    st.caption(f"State freshness: {age_label}")

                _render_today_trades_detail(_load_today_trades("macd_m2_sell_options_bot"))

            with st.expander("📋 Raw state"):
                st.json(state or {})

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — STRATEGY FLOWCHART
    # ══════════════════════════════════════════════════════════════════════════
    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "MACD M2 Sell Options Bot — Execution Logic (NIFTY · BANKNIFTY)",
                "How the bot decides on every completed 15-minute bar.",
                [
                    fc_start("☀️ Session Start"),
                    fc_action("📊 Compute MACD(12,26,9) on 15-min bars", "NIFTY + BANKNIFTY independently"),
                    fc_filter("Time in 09:15–14:30 IST?", "⏰ Outside window"),
                    fc_check("MACD line crosses zero this bar? (M2)"),
                    fc_filter("Index within ±0.2% of prior-day pivot? (SR3)", "📍 No S/R confluence"),
                    fc_split(
                        "BULL M2 + SR3",
                        fc_node_exit("📉 SELL ATM PE", "MIS · 5 lots"),
                        "BEAR M2 + SR3",
                        fc_node_entry("📉 SELL ATM CE", "MIS · 5 lots"),
                    ),
                    fc_filter("DTE ≥ 2 and option LTP ≥ ₹10?", "🚫 Skip entry"),
                    fc_monitor("🔍 Monitor premium vs SL (1.5× credit) / Target (keep 50%)"),
                    fc_exit("🛑 SL — premium ≥ 1.5× credit → BUY BACK"),
                    fc_exit("🎯 Target — premium ≤ 50% of credit → BUY BACK"),
                    fc_exit("⏰ 15:14 IST → EOD EXIT (unconditional)"),
                ],
            )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — RESEARCH FINDINGS
    # ══════════════════════════════════════════════════════════════════════════
    if tab_research.open:
        with tab_research:
            render_research_findings_tab("macd_price_action_sr_study/sell_strategy_spec.txt")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — PERFORMANCE
    # ══════════════════════════════════════════════════════════════════════════
    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("macd_m2_sell_options_bot")


def _bnf_tpp_entry_decision_trail(entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows at/before entry_time — the per-bar phase path
    leading into the currently-open position."""
    records = _read_jsonl_tail(LOGS_DIR / "banknifty_trend_pullback_positional_decisions.jsonl", limit=3000)
    matched = [r for r in records if not entry_time or r.get("ts", "") <= entry_time]
    return matched[-n:]


def render_bnf_trend_pullback_panel(ltps: dict):
    """
    Panel for the BANKNIFTY Trend Pullback Positional Bot:
      Tab 1 — Overview (regime/pierce/confirmation state + position card)
      Tab 2 — Strategy Flowchart
      Tab 3 — Live Decision State
      Tab 4 — Research Findings
      Tab 5 — Performance
    """
    state = _load(STATE_FILES["BANKNIFTY_TREND_PULLBACK_POSITIONAL"])

    st.markdown("## 📉 BANKNIFTY Trend Pullback Positional Bot")
    st.caption(
        "EMA(9,26) 15-min regime vs SMA(50) basis + BB(20,2σ) pullback pierce + 1-min candle "
        "confirmation · Bullish regime → SELL ATM PE · Bearish regime → SELL ATM CE · "
        "BANKNIFTY only, 10 lots, NRML (positional) · SL 2.5× credit · Target keep 50% · "
        "Exit priority: regime_end → expiry_force_exit(15:14) → target → SL · Status: Paper trading"
    )

    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview",
        "🗺️ Strategy Flowchart",
        "🧠 Live Decision State",
        "📖 Research Findings",
        "📈 Performance",
    ], on_change="rerun")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════════
    if tab_overview.open:
        with tab_overview:
            if not state:
                st.error("🔌 Bot not running — state file absent. Start the bot to see live data.")
            else:
                position   = state.get("position")
                regime     = state.get("regime") or {}
                pierce     = state.get("pierce_info") or {}
                awaiting   = bool(state.get("awaiting_confirm", False))
                no_trade   = bool(state.get("no_trade_this_regime", False))
                ltp_val    = state.get("ltp", 0)
                last_update = state.get("last_update", "")

                if position:
                    banner_icon, banner_msg, banner_col = "📌", "IN POSITION", "#7b61ff"
                elif awaiting:
                    banner_icon, banner_msg, banner_col = "⏳", "AWAITING 1-MIN CANDLE CONFIRMATION", "#f59e0b"
                elif no_trade and regime:
                    banner_icon, banner_msg, banner_col = "🚫", "NO TRADE THIS REGIME — pierce missed/stale", "#94a3b8"
                elif regime:
                    banner_icon, banner_msg, banner_col = "🔍", f"SCANNING — {regime.get('direction','?').upper()} regime, awaiting pullback pierce", "#60a5fa"
                else:
                    banner_icon, banner_msg, banner_col = "⏸", "WAITING — no aligned regime established yet", "#94a3b8"

                updated_fmt = last_update[:19].replace("T", " ") if last_update else "—"
                st.markdown(
                    f'<div style="background:{banner_col}22;border-left:4px solid {banner_col};'
                    f'padding:10px 16px;border-radius:6px;margin-bottom:12px;">'
                    f'<span style="font-size:1.3em">{banner_icon}</span> '
                    f'<strong style="color:{banner_col};font-size:1.05em">{banner_msg}</strong>'
                    f'<span style="float:right;opacity:.6;font-size:.85em">Updated {updated_fmt}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                with st.expander("📖 Strategy & Research Details"):
                    st.markdown(
                        '<div style="background:#1e293b;border-radius:6px;padding:10px 14px;'
                        'font-size:.85em;color:#cbd5e1;margin-bottom:10px">'
                        'EMA(9,26) regime cross vs SMA(50) basis + BB(20,2σ) pullback pierce → '
                        '1-min candle confirmation → sell ATM option &nbsp;|&nbsp; '
                        'IS+OOS Sharpe 1.850, WR 60.0%, Net P&amp;L +₹1,013,689 &nbsp;|&nbsp; '
                        'Entry cutoff 15:10 IST &nbsp;|&nbsp; SL 2.5× credit, target keep 50%'
                        '</div>',
                        unsafe_allow_html=True,
                    )
                    ec1, ec2 = st.columns(2)
                    with ec1:
                        st.markdown("**Entry Conditions**")
                        st.markdown(
                            '<div style="font-size:.85em;color:#94a3b8;line-height:1.7">'
                            'Aligned EMA(9,26) cross vs SMA(50) basis (15-min)<br>'
                            'First BB(20,2σ) close-pierce opposite regime direction<br>'
                            '1-min reversal candle confirms within 30 min of pierce<br>'
                            'Confirmation before 15:10 IST &nbsp;·&nbsp; DTE ≥ 2'
                            '</div>',
                            unsafe_allow_html=True,
                        )
                    with ec2:
                        st.markdown("**Exit Rules (priority order)**")
                        st.markdown(
                            '<div style="font-size:.85em;color:#94a3b8;line-height:1.7">'
                            '1. Regime end (new aligned cross fires)<br>'
                            '2. Expiry force-exit: ≥ expiry date and ≥ 15:14 IST<br>'
                            '3. Target: premium ≤ 50% of credit<br>'
                            '4. Stop loss: premium ≥ 2.5× credit'
                            '</div>',
                            unsafe_allow_html=True,
                        )

                st.markdown("### 📌 BANKNIFTY")
                regime_dir  = regime.get("direction", "none")
                since_ts    = (regime.get("since_ts") or "")[:16].replace("T", " ") or "—"
                pierce_ts   = (pierce.get("pierce_ts") or "")[:16].replace("T", " ") if pierce else "—"
                trade_dir   = pierce.get("trade_direction", "—") if pierce else "—"

                if regime_dir == "bullish":
                    badge_txt, badge_bg, badge_fg = "BULLISH REGIME", "#00c87533", "#00c875"
                elif regime_dir == "bearish":
                    badge_txt, badge_bg, badge_fg = "BEARISH REGIME", "#f8717133", "#f87171"
                else:
                    badge_txt, badge_bg, badge_fg = "NO REGIME", "#94a3b833", "#94a3b8"

                if position:
                    pos_txt   = f"SOLD {position.get('opt_type','')} — {position.get('opt_symbol','')}"
                    pos_color = "#e2e8f0"
                else:
                    pos_txt    = "none — scanning"
                    pos_color  = "#94a3b8"

                def _row(label, value, color="#e2e8f0"):
                    return (
                        f'<div style="display:flex;justify-content:space-between;padding:6px 0;'
                        f'border-bottom:1px solid #1e293b;font-size:.86em">'
                        f'<span style="color:#94a3b8">{label}</span>'
                        f'<span style="color:{color}">{value}</span></div>'
                    )

                card = (
                    f'<div style="border:1px solid #1e293b;border-radius:10px;padding:14px;margin-bottom:8px">'
                    f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">'
                    f'<span style="font-weight:600;font-size:1.02em">BANKNIFTY</span>'
                    f'<span style="background:{badge_bg};color:{badge_fg};font-size:.72em;'
                    f'padding:2px 8px;border-radius:8px">{badge_txt}</span></div>'
                    + _row("LTP", f"{ltp_val:,.2f}" if ltp_val else "—")
                    + _row("Regime since", since_ts)
                    + _row("Pullback pierce", f"{pierce_ts} → {trade_dir}" if pierce else "—")
                    + _row("Awaiting confirmation", "✅ Yes" if awaiting else "➖ No")
                    + _row("Position", pos_txt, pos_color)
                    + '</div>'
                )
                st.markdown(card, unsafe_allow_html=True)

                if last_update:
                    age_sec, age_label = _staleness(last_update)
                    st.caption(f"State freshness: {age_label}")

                if position:
                    st.markdown("### 📌 Open Position")
                    st.caption(
                        f"SELL {position.get('opt_type','')} · NRML positional hold — exits on regime_end, "
                        "target (keep 50%), 2.5× SL, or expiry-day 15:14 IST cutoff (not a daily EOD exit)"
                    )
                    opt_sym = position.get("opt_symbol", "—")
                    entry_credit = float(position.get("credit", 0))
                    _render_active_position_lifecycle(
                        symbol=opt_sym,
                        order_id=position.get("order_id", ""),
                        entry_price=entry_credit,
                        sl_price=position.get("sl_level"),
                        target_price=position.get("tgt_level"),
                        qty=position.get("quantity", 0),
                        entry_time=position.get("entry_time", ""),
                        ltp=ltps.get(opt_sym, entry_credit),
                        eod_exit_time="",
                        decision_trail=_bnf_tpp_entry_decision_trail(position.get("entry_time", "")),
                    )

                _render_today_trades_detail(_load_today_trades("banknifty_trend_pullback_positional_bot"))

            with st.expander("📋 Raw state"):
                st.json(state or {})

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — STRATEGY FLOWCHART
    # ══════════════════════════════════════════════════════════════════════════
    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "BANKNIFTY Trend Pullback Positional Bot — Execution Logic",
                "How the bot decides on every completed 15-minute bar, then confirms on 1-min bars.",
                [
                    fc_start("☀️ Session Start"),
                    fc_action("📊 Compute EMA(9)/EMA(26) + SMA(50) basis + BB(20,2σ) on 15-min bars", "BANKNIFTY"),
                    fc_check("EMA(9)/EMA(26) cross this bar, aligned with SMA(50) basis?"),
                    fc_action("🔀 New regime starts (bullish or bearish)", "prior open position exits on regime_end"),
                    fc_filter("First close-beyond-BB(20,2σ) pierce opposite regime direction?", "🔍 Still scanning regime"),
                    fc_action("⏳ Drop to 1-min bars, watch for reversal candle", "window = 30 min from pierce bar close"),
                    fc_filter("Reversal candle confirms (engulfing/hammer/shooting star) before 15:10 IST?", "🚫 No trade this regime"),
                    fc_split(
                        "BULLISH regime confirmed",
                        fc_node_exit("📈 SELL ATM PE", "NRML · 10 lots"),
                        "BEARISH regime confirmed",
                        fc_node_entry("📉 SELL ATM CE", "NRML · 10 lots"),
                    ),
                    fc_monitor("🔍 Monitor: regime_end → expiry_force_exit(15:14) → target(keep 50%) → SL(2.5×)"),
                    fc_exit("🔀 Regime end — new aligned cross fires → EXIT"),
                    fc_exit("⏰ Expiry force-exit — ≥ expiry date and ≥ 15:14 IST → EXIT"),
                    fc_exit("🎯 Target — premium ≤ 50% of credit → BUY BACK"),
                    fc_exit("🛑 SL — premium ≥ 2.5× credit → BUY BACK"),
                ],
            )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — LIVE DECISION STATE
    # ══════════════════════════════════════════════════════════════════════════
    if tab_state.open:
        with tab_state:
            _regime      = state.get("regime") or {} if state else {}
            _regime_dir  = _regime.get("direction")
            _has_regime  = bool(_regime_dir)
            _pierce      = state.get("pierce_info") or {} if state else {}
            _pierce_found = bool(state.get("pierce_found", False)) if state else False
            _awaiting    = bool(state.get("awaiting_confirm", False)) if state else False
            _no_trade    = bool(state.get("no_trade_this_regime", False)) if state else False
            _position    = state.get("position") if state else None
            _deadline    = (state.get("confirm_deadline") or "")[:16].replace("T", " ") if state else ""
            _ltp_val     = state.get("ltp", 0) if state else 0

            if _position:
                _ready = ("📌", "IN POSITION — monitoring for regime_end / expiry_force_exit / target / SL", "#7b61ff")
            elif _awaiting:
                _ready = ("⏳", "AWAITING 1-MIN CANDLE CONFIRMATION", "#f59e0b")
            elif _no_trade and _has_regime:
                _ready = ("🚫", "NO TRADE THIS REGIME — pierce missed/stale", "#94a3b8")
            elif _has_regime:
                _ready = ("🔍", f"SCANNING — {_regime_dir.upper()} regime, awaiting pullback pierce", "#60a5fa")
            else:
                _ready = ("⏸", "WAITING — no aligned regime established yet", "#94a3b8")

            render_decision_state(
                state,
                key="bnf_trend_pullback",
                updates_note="Updates on each completed 15-min bar, then 1-min bars during confirmation window",
                metrics=[
                    ("BANKNIFTY", f"{_ltp_val:,.2f}" if _ltp_val else "—"),
                    ("Regime", _regime_dir.upper() if _has_regime else "—",
                     "✅ established" if _has_regime else None, "off"),
                    ("Regime since", (_regime.get("since_ts") or "")[:16].replace("T", " ") or "—"),
                    ("Pullback pierce", (_pierce.get("trade_direction") or "—") if _pierce_found else "—",
                     "✅ found" if _pierce_found else "⏳ none yet", "off"),
                    ("Confirm deadline", _deadline or "—", "⏳ pending" if _awaiting else None, "off"),
                    ("Position", "SOLD " + _position.get("opt_type", "") if _position else "none",
                     "📌 open" if _position else None, "off"),
                ],
                filters=[
                    ("📊", "Aligned EMA(9,26) cross vs SMA(50) basis (15-min)", _has_regime,
                     f"{_regime_dir} regime" if _has_regime else "no regime yet"),
                    ("🔻", "First BB(20,2σ) close-pierce opposite regime direction", _pierce_found,
                     (_pierce.get("pierce_ts") or "")[:16].replace("T", " ") if _pierce_found else "waiting for pierce"),
                    ("🕯️", "1-min reversal candle confirms within 30 min of pierce", bool(_position) or (not _awaiting and _pierce_found and not _no_trade),
                     "confirmed" if _position else ("awaiting confirmation" if _awaiting else ("missed/stale" if _no_trade else "n/a"))),
                    ("⏰", "Confirmation before 15:10 IST cutoff", not _no_trade,
                     "within cutoff" if not _no_trade else "window expired"),
                    ("🔁", "No position already open this regime", not bool(_position),
                     "free" if not _position else "in position"),
                ],
                readiness=_ready,
            )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — RESEARCH FINDINGS
    # ══════════════════════════════════════════════════════════════════════════
    if tab_research.open:
        with tab_research:
            render_research_findings_tab("trend_pullback_positional_study/results_summary.md")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 5 — PERFORMANCE
    # ══════════════════════════════════════════════════════════════════════════
    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("banknifty_trend_pullback_positional_bot")


def render_nifty_eod_hold_panel(ltps: dict):
    state = _load(STATE_FILES["NIFTY_EOD_HOLD"])
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _nifty_eod_hold_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "NIFTY EOD Hold Bot — Execution Logic",
                "Sell an ATM option on an early candle-reversal signal, then hold to EOD with no stop-loss.",
                [
                    fc_start("☀️ Session Start"),
                    fc_action("📚 Load 1-min NIFTY history",
                              "warm ADX(14) · MACD(5,13,3) · EMA(20)"),
                    fc_filter("INDIAVIX < 17 at open?", "🌡️ Session skipped"),
                    fc_action("⚡ On each 1-min bar in 09:15–09:44"),
                    fc_filter("Signal window 09:15–09:44 IST?", "⏰ Window closed"),
                    fc_filter("ADX(14) ≥ 25?", "Trend too weak"),
                    fc_filter("Hammer / shooting-star candle?", "No reversal candle"),
                    fc_filter("No signal taken yet today?", "🔁 One signal / session"),
                    fc_check("Direction — candle + MACD + EMA-20?"),
                    fc_split(
                        "BEARISH<br>shooting-star · close&gt;EMA",
                        fc_node_exit("📉 SELL ATM CE", "entry = next bar open"),
                        "BULLISH<br>hammer · close&lt;EMA",
                        fc_node_entry("📈 SELL ATM PE", "entry = next bar open"),
                    ),
                    fc_monitor("🤝 Hold unconditionally — NO stop-loss"),
                    fc_exit("⏰ 15:29 IST → EOD EXIT (unconditional)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _ind     = state.get("indicators", {}) if state else {}
            _in_win  = _entry_window_open("09:15–09:44")
            _vix     = state.get("vix_ltp", 0.0) if state else 0.0
            _vix_skip = state.get("vix_skip", False) if state else False
            _adx     = _ind.get("adx", 0)
            _adx_ok  = _adx >= 25
            _taken   = state.get("signal_taken", False) if state else False
            _active  = state.get("active_trade") if state else None
            _bars    = state.get("bars_loaded", 0) if state else 0
            _bar_t   = _ind.get("bar_time", "—")

            if _active:
                _ready = ("📌", "IN POSITION — holding to 15:29 EOD (no stop-loss)", "#7b61ff")
            elif _vix_skip:
                _ready = ("🔴", f"SESSION SKIPPED — INDIAVIX {_vix:.1f} ≥ 17 at open", "#f87171")
            elif _taken:
                _ready = ("✅", "Signal already taken today — one per session", "#00c875")
            elif not _in_win:
                _ready = ("⏸", "OUT OF WINDOW — signal window is 09:15–09:44 IST only", "#94a3b8")
            else:
                _ready = ("🔍", "SCANNING — watching for ADX≥25 + reversal candle", "#60a5fa")

            render_decision_state(
                state,
                key="eod_hold",
                updates_note="Updates on each 1-min bar close (signal window only)",
                metrics=[
                    ("NIFTY", f"{_ind.get('nifty_ltp', 0):,.1f}" if _ind.get("nifty_ltp") else "—"),
                    ("INDIAVIX", f"{_vix:.2f}" if _vix else "—",
                     "⚠️ SKIP" if _vix_skip else "✅ OK", "inverse" if _vix_skip else "off"),
                    ("ADX(14)", f"{_adx:.1f}" if _adx else "—",
                     "✅ ≥ 25" if _adx_ok else "❌ < 25", "normal" if _adx_ok else "inverse"),
                    ("Expiry", (state.get("expiry") if state else "") or "—"),
                    ("Last Bar", _bar_t),
                    ("Window", "09:15–09:44", "🟢 OPEN" if _in_win else "🔴 CLOSED", "off"),
                    ("Bars", f"{_bars}"),
                ],
                filters=[
                    ("🌡️", "INDIAVIX < 17 (session not skipped)", not _vix_skip,
                     f"VIX = {_vix:.1f}"),
                    ("⏰", "Signal window 09:15–09:44 IST", _in_win, _bar_t),
                    ("💪", "ADX(14) ≥ 25", _adx_ok, f"ADX = {_adx:.1f}"),
                    ("🔁", "No signal taken yet today", not _taken,
                     "free" if not _taken else "already fired"),
                ],
                readiness=_ready,
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("atm_options_eod_hold_1min_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("nifty_eod_hold_bot")


def _nifty_eod_hold_entry_decision_trail(entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows at/before entry_time — the per-bar phase path
    leading into the currently-open trade."""
    records = _read_jsonl_tail(LOGS_DIR / "nifty_eod_hold_decisions.jsonl", limit=3000)
    matched = [r for r in records if not entry_time or r.get("ts", "") <= entry_time]
    return matched[-n:]


def _nifty_eod_hold_overview(ltps: dict, state: dict):
    with st.container(border=True):
        st.subheader("📊 NIFTY EOD Hold Bot")

        if not state:
            st.error("🔌 Bot not running — state file absent. Start the bot to see live data.")
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
            sym        = active_trade.get("symbol", "")
            opt_type   = active_trade.get("opt_type", "")
            entry_p    = float(active_trade.get("entry_prem", 0))
            qty        = int(active_trade.get("qty", 0))
            entry_time = active_trade.get("entry_time", "")
            ltp        = ltps.get(sym, entry_p)

            st.caption(f"SOLD {opt_type}")
            _render_active_position_lifecycle(
                symbol=sym,
                order_id="",
                entry_price=entry_p,
                sl_price=None,
                target_price=None,
                qty=qty,
                entry_time=entry_time,
                ltp=ltp,
                eod_exit_time="15:14",
                decision_trail=_nifty_eod_hold_entry_decision_trail(entry_time),
            )
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
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _iron_fly_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "NIFTY Iron Fly Weekly Bot — Execution Logic",
                "Short iron fly (ATM straddle + OTM hedges), entered Wed 10:00, held NRML to Mon exit.",
                [
                    fc_start("📅 Entry day — Wednesday 10:00 IST"),
                    fc_filter("INDIAVIX ≥ 12 at entry?", "🌡️ Vol too low — skip"),
                    fc_filter("NIFTY spot ≥ 20-day MA?", "📉 Below trend — skip"),
                    fc_action("🦋 Build short iron fly",
                              "BUY OTM CE → BUY OTM PE → SELL ATM CE → SELL ATM PE (Δ≈0.10 hedges)"),
                    fc_entry("📌 4 legs NRML — held 3–4 days overnight", "10 lots"),
                    fc_monitor("🔍 Poll combined MTM every 30s"),
                    fc_exit("🛑 SL — combined MTM ≤ −₹20,000 (₹2k/lot) → close all"),
                    fc_exit("⏰ Monday 15:15 IST → scheduled exit (theta harvested)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _legs    = state.get("legs", {}) if state else {}
            _closed  = state.get("closed", False) if state else False
            _sl_hit  = state.get("sl_hit", False) if state else False
            _open    = bool(_legs) and not _closed
            _vix_e   = state.get("vix_at_entry", 0) if state else 0
            _ma20_e  = state.get("ma20_at_entry", 0) if state else 0
            _mtm     = float(state.get("current_mtm", 0)) if state else 0
            _sl_tot  = float(state.get("stop_loss_total", 20000)) if state else 20000
            _sl_ok   = _mtm > -_sl_tot
            _net_cr  = state.get("net_credit_per_unit", 0) if state else 0

            if _closed and _sl_hit:
                _ready = ("🛑", f"CLOSED — SL hit ({state.get('exit_reason') or 'stop'})", "#f87171")
            elif _closed:
                _ready = ("✅", f"CLOSED — {state.get('exit_reason') or 'scheduled exit'} (theta harvested)", "#00c875")
            elif _open:
                _col = "#00c875" if _mtm >= 0 else ("#fbbf24" if _sl_ok else "#f87171")
                _ready = ("📌", f"IN POSITION — combined MTM ₹{_mtm:,.0f} · SL at −₹{_sl_tot:,.0f}", _col)
            else:
                _ready = ("🔍", "FLAT — awaiting Wednesday 10:00 entry (VIX≥12 + NIFTY≥MA20)", "#60a5fa")

            render_decision_state(
                state,
                key="iron_fly_wk",
                updates_note="Updates every 30s while a position is open",
                metrics=[
                    ("Status", "OPEN" if _open else ("CLOSED" if _closed else "FLAT")),
                    ("Trade Date", state.get("trade_date", "—") if state else "—"),
                    ("Expiry", state.get("expiry_str", "—") if state else "—"),
                    ("ATM Strike", f"{state.get('atm_strike', 0):,.0f}" if state and state.get("atm_strike") else "—"),
                    ("VIX @ entry", f"{_vix_e:.2f}" if _vix_e else "—"),
                    ("Net Credit/unit", f"₹{_net_cr:.2f}" if _net_cr else "—"),
                    ("Combined MTM", f"₹{_mtm:,.0f}", "🟢" if _mtm >= 0 else "🔴", "off"),
                    ("SL threshold", f"−₹{_sl_tot:,.0f}"),
                ],
                filters=[
                    ("🦋", "Iron fly position open this cycle", _open,
                     "open" if _open else ("closed" if _closed else "flat")),
                    ("🌡️", "VIX ≥ 12 at entry", _vix_e >= 12 if _vix_e else False,
                     f"VIX = {_vix_e:.1f}" if _vix_e else "no entry yet"),
                    ("📈", "NIFTY ≥ 20-day MA at entry", bool(_legs),
                     f"MA20 = {_ma20_e:,.0f}" if _ma20_e else "no entry yet"),
                    ("🛑", "Combined MTM above SL (−₹20,000)", _sl_ok if _open else True,
                     f"MTM ₹{_mtm:,.0f}" if _open else "n/a"),
                ],
                readiness=_ready,
                checklist_title="🔍 Position & Entry Gates",
                checklist_caption="Iron fly is positional — entry gates are evaluated once at Wed 10:00 and recorded.",
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("iron_fly_weekly_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("nifty_iron_fly_weekly_bot")


def _iron_fly_overview(ltps: dict, state: dict):
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
                "🔌 State file absent — bot is either not running or has not entered a position yet this week. "
                "Check `live_trading/logs/nifty_iron_fly_weekly_state.json`."
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
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _sensex_iron_fly_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "SENSEX Iron Fly Weekly Bot — Execution Logic",
                "Short iron fly entered Fri 10:00 (SENSEX≥MA20, no VIX), 50% profit target, delta-based adjustments.",
                [
                    fc_start("📅 Entry day — Friday 10:00 IST"),
                    fc_filter("SENSEX spot ≥ 20-day MA?", "📉 Below trend — skip"),
                    fc_action("🦋 Build short iron fly (strike step 100)",
                              "BUY OTM CE → BUY OTM PE → SELL ATM CE → SELL ATM PE (Δ≈0.10)"),
                    fc_entry("📌 4 legs NRML BFO — held 5–6 days", "1 lot (20 units)"),
                    fc_monitor("🔍 Poll MTM + short-leg deltas every 30s"),
                    fc_monitor("⚙️ Adjust — re-center short leg if |Δ| ∉ [0.20–0.70] for 2 polls"),
                    fc_exit("🎯 Profit target — MTM ≥ 50% of net credit → close all"),
                    fc_exit("⏰ Wednesday 15:15 IST → scheduled exit (no stop-loss)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _legs    = state.get("legs", {}) if state else {}
            _closed  = state.get("closed", False) if state else False
            _open    = bool(_legs) and not _closed
            _spot_e  = state.get("spot_at_entry", 0) if state else 0
            _ma20_e  = state.get("ma20_at_entry", 0) if state else 0
            _mtm     = float(state.get("current_mtm", 0)) if state else 0
            _prem    = float(state.get("premium_collected", 0)) if state else 0
            _pt      = 0.5 * _prem
            _pt_hit  = _prem > 0 and _mtm >= _pt
            _n_adj   = int(state.get("n_adjustments", 0)) if state else 0
            _gate_ok = bool(_spot_e) and bool(_ma20_e) and _spot_e >= _ma20_e

            if _closed:
                _ready = ("✅", f"CLOSED — {state.get('exit_reason') or 'scheduled exit'}", "#00c875")
            elif _open and _pt_hit:
                _ready = ("🎯", f"PROFIT TARGET HIT — MTM ₹{_mtm:,.0f} ≥ 50% credit · exiting", "#00c875")
            elif _open:
                _col = "#00c875" if _mtm >= 0 else "#fbbf24"
                _ready = ("📌", f"IN POSITION — MTM ₹{_mtm:,.0f} · PT ₹{_pt:,.0f} · {_n_adj} adj", _col)
            else:
                _ready = ("🔍", "FLAT — awaiting Friday 10:00 entry (SENSEX ≥ MA20)", "#60a5fa")

            _pt_pct = (_mtm / _pt * 100) if _pt else 0
            render_decision_state(
                state,
                key="sensex_iron_fly",
                updates_note="Updates every 30s while a position is open",
                metrics=[
                    ("Status", "OPEN" if _open else ("CLOSED" if _closed else "FLAT")),
                    ("Trade Date", state.get("trade_date", "—") if state else "—"),
                    ("Expiry", state.get("expiry_str", "—") if state else "—"),
                    ("ATM Strike", f"{state.get('atm_strike', 0):,.0f}" if state and state.get("atm_strike") else "—"),
                    ("Spot @ entry", f"{_spot_e:,.0f}" if _spot_e else "—"),
                    ("Net Credit/unit", f"₹{state.get('net_credit_per_unit', 0):.2f}" if state and state.get("net_credit_per_unit") else "—"),
                    ("Combined MTM", f"₹{_mtm:,.0f}", f"{_pt_pct:+.0f}% of PT" if _pt else None, "off"),
                    ("Adjustments", f"{_n_adj}"),
                ],
                filters=[
                    ("🦋", "Iron fly position open this cycle", _open,
                     "open" if _open else ("closed" if _closed else "flat")),
                    ("📈", "SENSEX ≥ 20-day MA at entry", _gate_ok,
                     f"{_spot_e:,.0f} vs {_ma20_e:,.0f}" if _ma20_e else "no entry yet"),
                    ("🎯", "Profit target (50% credit) reached", _pt_hit,
                     f"MTM ₹{_mtm:,.0f} / PT ₹{_pt:,.0f}" if _pt else "no entry yet"),
                ],
                readiness=_ready,
                checklist_title="🔍 Position & Entry Gates",
                checklist_caption="Positional iron fly — sole entry gate is SENSEX ≥ MA20; risk managed by PT + adjustments.",
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("sensex_iron_fly_weekly_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("sensex_iron_fly_weekly_bot")


def _sensex_iron_fly_overview(ltps: dict, state: dict):
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
                "🔌 State file absent — bot is either not running or has not entered a position yet this week. "
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
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _flat_blue_line_overview(ltps, full_state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "Flat Blue Line Monthly Bot — Execution Logic (NIFTY + BANKNIFTY)",
                "6-leg Double Fly entered the first day after monthly expiry; enter-and-monitor, no adjustments.",
                [
                    fc_start("📅 Entry — 10:00 IST, first day after prior monthly expiry"),
                    fc_filter("ATM Call Black-76 IV ≥ 14%?", "📉 Low vol — skip month"),
                    fc_action("🔵 Build 6-leg Double Fly",
                              "BUY ATM straddle → BUY OTM wings (Δ0.10) → SELL 2× OTM strangle at ATM±D"),
                    fc_entry("📌 6 legs NRML — held into the month",
                             "NIFTY N_C=3/N_P=2 · BANKNIFTY N_C=1/N_P=3"),
                    fc_monitor("🔍 Poll MTM + spot vs breakevens"),
                    fc_exit("🎯 Profit target — MTM ≥ ₹22,500 (NIFTY) / dynamic (BN) → close all"),
                    fc_exit("🛑 Breakeven stop — spot crosses BE_L / BE_U → close all"),
                    fc_exit("⏰ 3 trading days before expiry @ 15:15 → close all"),
                ],
            )

    if tab_state.open:
        with tab_state:
            def _inst_status(inst):
                s = (full_state or {}).get(inst, {})
                return {
                    "open": not s.get("closed", True),
                    "pnl": s.get("total_pnl", 0),
                    "reason": s.get("exit_reason", ""),
                    "month": s.get("month_key", "—"),
                }

            _ni = _inst_status("NIFTY")
            _bn = _inst_status("BANKNIFTY")
            _any_open = _ni["open"] or _bn["open"]

            def _inst_metric(label, d):
                if d["open"]:
                    return (label, f"₹{d['pnl']:,.0f}", "OPEN", "off")
                if d["reason"] in ("target", "be_stop", "pre_expiry", "forced_expiry"):
                    return (label, f"₹{d['pnl']:,.0f}", f"closed ({d['reason']})", "off")
                if d["reason"] == "low_vol":
                    return (label, "skipped", "IV < 14%", "off")
                return (label, "flat", "awaiting entry", "off")

            if _any_open:
                _tot = (_ni["pnl"] if _ni["open"] else 0) + (_bn["pnl"] if _bn["open"] else 0)
                _ready = ("📌", f"IN POSITION — combined open MTM ₹{_tot:,.0f} · monitoring PT / BE / pre-expiry", "#7b61ff")
            else:
                _ready = ("🔍", "FLAT — awaiting first trading day after monthly expiry (IV ≥ 14%)", "#60a5fa")

            render_decision_state(
                full_state,
                key="flat_blue_line",
                updates_note="Updates every 30s while a position is open",
                metrics=[
                    _inst_metric("NIFTY Double Fly", _ni),
                    _inst_metric("BANKNIFTY Double Fly", _bn),
                    ("Month", _ni["month"] if _ni["month"] != "—" else _bn["month"]),
                ],
                filters=[
                    ("🟦", "NIFTY position open this cycle", _ni["open"],
                     "open" if _ni["open"] else (_ni["reason"] or "flat")),
                    ("🟧", "BANKNIFTY position open this cycle", _bn["open"],
                     "open" if _bn["open"] else (_bn["reason"] or "flat")),
                ],
                readiness=_ready,
                checklist_title="🔍 Per-Instrument Position State",
                checklist_caption="Monthly positional double-fly — entry IV gate is evaluated once per cycle.",
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("flat_blue_line_monthly/FINAL_REPORT.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("flat_blue_line_monthly_bot")


def _flat_blue_line_overview(ltps: dict, full_state: dict):
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
                "🔌 State file absent — bot is either not running or has not entered a position yet this month. "
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
                    msg = f"{emoji} {month}: Exited ({reason}) · P&L ₹{pnl:,.0f}"
                    if pnl >= 0:
                        st.success(msg)
                    else:
                        st.error(msg)
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
                st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

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
    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview", "🗺️ Strategy Flowchart", "🧠 Live Decision State", "📖 Research Findings", "📈 Performance",
    ], on_change="rerun")

    if tab_overview.open:
        with tab_overview:
            _bnf_iron_fly_monthly_overview(ltps, state)

    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "BANKNIFTY Iron Fly Monthly Bot — Execution Logic",
                "Short ATM straddle + OTM wings (Δ≈0.15), 50% profit target, delta-range adjustments.",
                [
                    fc_start("📅 Entry — 10:00 IST, first day after prior monthly expiry"),
                    fc_filter("DTE ≥ 2 at entry?", "Too close to expiry"),
                    fc_action("🦋 Build 4-leg iron fly",
                              "SELL ATM CE → SELL ATM PE → BUY OTM CE → BUY OTM PE (Δ≈0.15)"),
                    fc_entry("📌 4 legs NRML — held ~20 days", "10 lots"),
                    fc_monitor("🔍 Poll MTM + short-leg deltas every 2 bars"),
                    fc_monitor("⚙️ Adjust — re-centre short leg if |Δ| ∉ [0.20–0.70] for 2 bars"),
                    fc_exit("🎯 Profit target — MTM ≥ 50% of net premium → close all"),
                    fc_exit("⏰ Day-before-expiry 15:15 IST → scheduled exit (no stop-loss)"),
                ],
            )

    if tab_state.open:
        with tab_state:
            _legs     = state.get("legs", {}) if state else {}
            _closed   = state.get("closed", False) if state else False
            _open     = bool(_legs) and not _closed
            _mtm      = float(state.get("current_mtm", 0)) if state else 0
            _prem     = float(state.get("premium_collected", 0)) if state else 0
            _pt       = 0.5 * _prem
            _pt_hit   = _prem > 0 and _mtm >= _pt
            _n_adj    = int(state.get("n_adjustments", 0)) if state else 0
            _exit_day = state.get("exit_day", "—") if state else "—"

            if _closed:
                _ready = ("✅", f"CLOSED — {state.get('exit_reason') or 'scheduled exit'}", "#00c875")
            elif _open and _pt_hit:
                _ready = ("🎯", f"PROFIT TARGET HIT — MTM ₹{_mtm:,.0f} ≥ 50% · exiting", "#00c875")
            elif _open:
                _col = "#00c875" if _mtm >= 0 else "#fbbf24"
                _ready = ("📌", f"IN POSITION — MTM ₹{_mtm:,.0f} · PT ₹{_pt:,.0f} · {_n_adj} adj", _col)
            else:
                _ready = ("🔍", "FLAT — awaiting first day after monthly expiry (DTE ≥ 2)", "#60a5fa")

            render_decision_state(
                state,
                key="bnf_iron_fly_monthly",
                updates_note="Updates every poll while a position is open",
                metrics=[
                    ("Status", "OPEN" if _open else ("CLOSED" if _closed else "FLAT")),
                    ("Trade Date", state.get("trade_date", "—") if state else "—"),
                    ("Expiry", state.get("expiry_str", "—") if state else "—"),
                    ("ATM Strike", f"{state.get('atm_strike', 0):,.0f}" if state and state.get("atm_strike") else "—"),
                    ("Net Credit/unit", f"₹{state.get('net_credit_per_unit', 0):.2f}" if state and state.get("net_credit_per_unit") else "—"),
                    ("Combined MTM", f"₹{_mtm:,.0f}", f"PT ₹{_pt:,.0f}" if _pt else None, "off"),
                    ("Adjustments", f"{_n_adj}"),
                    ("Sched. Exit", _exit_day),
                ],
                filters=[
                    ("🦋", "Iron fly position open this cycle", _open,
                     "open" if _open else ("closed" if _closed else "flat")),
                    ("🎯", "Profit target (50% premium) reached", _pt_hit,
                     f"MTM ₹{_mtm:,.0f} / PT ₹{_pt:,.0f}" if _pt else "no entry yet"),
                ],
                readiness=_ready,
                checklist_title="🔍 Position State",
                checklist_caption="Monthly positional — entry is conditional only on DTE; risk managed by PT + delta adjustments.",
            )

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("banknifty_iron_fly_monthly_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("banknifty_iron_fly_monthly_bot")


def _bnf_iron_fly_monthly_overview(ltps: dict, state: dict):
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
                "🔌 State file absent — bot is either not running or has not entered a position yet this month. "
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
                width="stretch", hide_index=True,
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

def _nifty_ma_cross_entry_decision_trail(entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows at/before entry_time — the per-bar phase path
    leading into the currently-open trade."""
    records = _read_jsonl_tail(LOGS_DIR / "nifty_ma_cross_seller_decisions.jsonl", limit=3000)
    matched = [r for r in records if not entry_time or r.get("ts", "") <= entry_time]
    return matched[-n:]


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

    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview",
        "🗺️ Strategy Flowchart",
        "🧠 Live Decision State",
        "📖 Research Findings",
        "📈 Performance",
    ], on_change="rerun")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════════
    if tab_overview.open:
        with tab_overview:
            if not state:
                st.error("🔌 Bot not running — state file absent. Start the bot to see live data.")
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
                    sym        = active.get("symbol", "")
                    opt_type   = active.get("opt_type", "")
                    entry_p    = float(active.get("entry_prem", 0))
                    sl_p       = float(active.get("sl_prem", 0))
                    qty        = int(active.get("qty", 0))
                    entry_time = active.get("entry_ts", "")
                    ltp_now    = ltps.get(sym, entry_p)

                    st.markdown("### 📌 Open Position")
                    st.caption(
                        f"SELL {opt_type} · NRML overnight hold — exits on reversal cross, "
                        f"3× SL, or expiry-day 15:14 IST cutoff (not a daily EOD exit)"
                    )
                    _render_active_position_lifecycle(
                        symbol=sym,
                        order_id="",
                        entry_price=entry_p,
                        sl_price=sl_p,
                        target_price=None,
                        qty=qty,
                        entry_time=entry_time,
                        ltp=ltp_now,
                        eod_exit_time="",
                        decision_trail=_nifty_ma_cross_entry_decision_trail(entry_time),
                    )
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
    if tab_flow.open:
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
            # st.iframe auto-detects a raw HTML string and sandboxes it in an iframe
            # (replaces the deprecated st.components.v1.html); height="content"
            # auto-sizes to the flowchart instead of a fixed-height scroll box.
            st.iframe(flowchart_html, height="content")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — LIVE DECISION STATE
    # ══════════════════════════════════════════════════════════════════════════
    if tab_state.open:
        with tab_state:
            if not state:
                st.error("🔌 Bot not running — state file absent. Start the bot to see live decision data.")
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

    if tab_research.open:
        with tab_research:
            render_research_findings_tab("ma_cross_options_seller_study/results_summary.md")

    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("nifty_ma_cross_seller_bot")


# ══════════════════════════════════════════════════════════════════════════════
#  EMA SPREAD PANELS  (NIFTY · BANKNIFTY · SENSEX)
# ══════════════════════════════════════════════════════════════════════════════

def _ema_spread_entry_decision_trail(bot_name: str, entry_time: str, n: int = 6) -> list[dict]:
    """Last n decision-log rows at/before entry_time — the per-bar phase path
    leading into the currently-open spread. bot_name e.g. "nifty_ema_spread_bot"."""
    log_stem = bot_name[:-len("_bot")] if bot_name.endswith("_bot") else bot_name
    records = _read_jsonl_tail(LOGS_DIR / f"{log_stem}_decisions.jsonl", limit=3000)
    matched = [r for r in records if not entry_time or r.get("ts", "") <= entry_time]
    return matched[-n:]


def _render_ema_spread_panel(
    ltps: dict,
    state_key: str,
    title: str,
    caption: str,
    underlying: str,
    spread_width: int,
    exchange: str,
    research_path: str,
    bot_name: str,
):
    """Generic 5-tab panel for all three EMA spread bots."""
    state = _load(STATE_FILES[state_key])

    st.markdown(f"## 📅 {title}")
    st.caption(caption)

    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview",
        "🗺️ Strategy Flowchart",
        "🧠 Live Decision State",
        "📖 Research Findings",
        "📈 Performance",
    ], on_change="rerun")

    # ── TAB 1 — OVERVIEW ─────────────────────────────────────────────────────
    if tab_overview.open:
        with tab_overview:
            if not state:
                st.error("🔌 Bot not running — state file absent. Start the bot to see live data.")
            else:
                pos    = state.get("position")
                signal = state.get("signal", 0)
                emas   = state.get("ema_state", {})
                ema5   = emas.get("ema5")
                ema13  = emas.get("ema13")
                spot   = emas.get("spot", 0)
                last_bar_time = emas.get("last_bar_time", "—")
                bars_loaded   = emas.get("bars_loaded", 0)

                sig_label = {1: "🟢 BULL (sell bear put spread)", -1: "🔴 BEAR (sell bull call spread)", 0: "⚪ FLAT"}.get(signal, "⚪ FLAT")

                # Status banner
                if pos:
                    banner_icon, banner_msg, banner_col = "📌", "IN POSITION", "#7b61ff"
                elif ema5 is not None and ema13 is not None:
                    banner_icon, banner_msg, banner_col = "🟢", "SCANNING — waiting for EMA crossover", "#00c875"
                else:
                    banner_icon, banner_msg, banner_col = "⏳", "WARMING UP — loading EMA history", "#94a3b8"

                updated_fmt = (state.get("updated", "") or "")[:19].replace("T", " ")
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
                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric(underlying, f"{spot:,.1f}" if spot else "—")
                c2.metric("EMA(5)", f"{ema5:,.1f}" if ema5 is not None else "—")
                c3.metric("EMA(13)", f"{ema13:,.1f}" if ema13 is not None else "—")
                c4.metric("Bars Loaded", bars_loaded)
                c5.metric("Last Bar", last_bar_time)

                st.markdown("---")

                # Signal / cross row
                s1, s2, s3 = st.columns(3)
                s1.metric("Current EMA Signal", sig_label)
                if ema5 is not None and ema13 is not None:
                    s2.metric("EMA Cross", "EMA5 > EMA13" if ema5 > ema13 else "EMA5 < EMA13",
                              delta="Bullish" if ema5 > ema13 else "Bearish", delta_color="off")
                else:
                    s2.metric("EMA Cross", "—")
                s3.metric("Spread Width", f"{spread_width} pts")

                st.markdown("---")

                if not pos:
                    st.info("No open position — flat.")
                else:
                    long_sym  = pos.get("long_sym", "—")
                    short_sym = pos.get("short_sym", "—")
                    direction = pos.get("direction", "—")
                    entry_deb = float(pos.get("entry_debit", 0))
                    qty       = int(pos.get("qty", 0))
                    entry_t   = (pos.get("entry_time") or "")[:16].replace("T", " ")
                    expiry    = pos.get("expiry", "—")

                    long_ltp  = ltps.get(long_sym,  entry_deb)
                    short_ltp = ltps.get(short_sym, 0.0)
                    spread_val = long_ltp - short_ltp
                    pnl        = (spread_val - entry_deb) * qty
                    tp_val     = round(entry_deb * 1.5, 2)
                    sl_val     = round(entry_deb * 0.05, 2)

                    st.markdown("### 📌 Open Position")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Direction", direction)
                    col2.metric("Entry Debit (R)", f"₹{entry_deb:.2f}")
                    col3.metric("Spread Value Now", f"₹{spread_val:.2f}",
                                delta=f"₹{spread_val - entry_deb:+.2f}")

                    col4, col5, col6 = st.columns(3)
                    col4.metric("Net P&L", f"₹{pnl:,.0f}",
                                delta_color="normal")
                    col5.metric("TP @ +0.5R", f"₹{tp_val:.2f}")
                    col6.metric("SL @ −0.95R", f"₹{sl_val:.2f}")

                    st.markdown("---")
                    st.markdown("**Leg symbols**")
                    lc1, lc2 = st.columns(2)
                    lc1.markdown(f"🟢 **Long (BUY):** `{long_sym}`  \nLTP: ₹{long_ltp:.2f}")
                    lc2.markdown(f"🔴 **Short (SELL):** `{short_sym}`  \nLTP: ₹{short_ltp:.2f}")
                    st.caption(f"Entry: {entry_t}  ·  Expiry: {expiry}  ·  Qty: {qty}")

                    # ── Live TP / SL distance (spread value vs entry debit R) ──────
                    pg1, pg2 = st.columns(2)
                    with pg1:
                        sl_span = entry_deb - sl_val
                        sl_prog = max(0.0, min(1.0, (entry_deb - spread_val) / sl_span)) if sl_span else 0.0
                        st.caption(f"🛑 SL ₹{sl_val:.2f}  —  ₹{(spread_val - sl_val):.2f} away ({(1 - sl_prog) * 100:.0f}% of room left)")
                        st.progress(sl_prog)
                    with pg2:
                        tgt_span = tp_val - entry_deb
                        tgt_prog = max(0.0, min(1.0, (spread_val - entry_deb) / tgt_span)) if tgt_span else 0.0
                        st.caption(f"🎯 TP ₹{tp_val:.2f}  —  ₹{(tp_val - spread_val):.2f} away ({tgt_prog * 100:.0f}% there)")
                        st.progress(tgt_prog)

                    entry_time_raw = pos.get("entry_time") or ""
                    trail = _ema_spread_entry_decision_trail(bot_name, entry_time_raw)
                    if trail:
                        with st.expander(f"🕵️ How we got here — entry decision trail ({len(trail)} steps)"):
                            for rec in trail:
                                ts_raw = rec.get("ts", "")
                                ts = ts_raw[11:19] if len(ts_raw) >= 19 else (ts_raw or "—")
                                st.markdown(f"`{ts}` **{rec.get('phase', '—')}**")

                _render_today_trades_detail(_load_today_trades(bot_name))

            with st.expander("📋 Raw state"):
                st.json(state or {})

            if state and state.get("updated"):
                age_sec, age_label = _staleness(state["updated"])
                st.caption(f"State file: {age_label} · updated {state['updated'][11:19]}")

    # ── TAB 2 — STRATEGY FLOWCHART ───────────────────────────────────────────
    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                f"{title} — Execution Logic",
                f"EMA(5,13) crossover on 15-min {underlying} index bars → {spread_width}pt ATM debit spread (NRML).",
                [
                    fc_start("☀️ Market Open"),
                    fc_action("📚 Pre-load 2 days of 1-min history",
                              "resample → 15-min closes → seed EMA(5,13)"),
                    fc_action("⚡ WebSocket tick stream — 15-min bar builder"),
                    fc_filter("Bar closed (15-min boundary)?", "⏳ Accumulate ticks"),
                    fc_action("Recompute EMA(5,13) on new close"),
                    fc_filter("EMA crossover detected?", "🔍 Continue scanning"),
                    fc_check("Direction: BULL (EMA5 > EMA13) or BEAR?"),
                    fc_split(
                        "🟢 BULL crossover",
                        fc_node_entry(f"Buy ATM CE + Sell OTM +{spread_width}pt CE", "bull call spread"),
                        "🔴 BEAR crossover",
                        fc_node_entry(f"Buy ATM PE + Sell OTM −{spread_width}pt PE", "bear put spread"),
                    ),
                    fc_action("Record entry debit = R  ·  TP = 1.5R  ·  SL = 0.05R"),
                    fc_filter("On each bar: spread_val ≥ TP?", "⬇️ Continue"),
                    fc_exit("✅ PROFIT EXIT — spread reached TP (1.5R)"),
                    fc_filter("spread_val ≤ SL?", "⬇️ Continue"),
                    fc_exit("🛑 STOP-LOSS EXIT — spread below 0.05R"),
                    fc_filter("Opposite crossover (signal flip)?", "⬇️ Continue scanning"),
                    fc_action("🔄 REVERSAL — close current spread, open opposite"),
                    fc_exit("🔚 End of session"),
                ],
            )

    # ── TAB 3 — LIVE DECISION STATE ──────────────────────────────────────────
    if tab_state.open:
        with tab_state:
            if not state:
                st.error("🔌 Bot not running — state file absent. Start the bot to see live decision data.")
            else:
                pos    = state.get("position")
                signal = state.get("signal", 0)
                emas   = state.get("ema_state", {})
                ema5   = emas.get("ema5")
                ema13  = emas.get("ema13")
                last_bar = emas.get("last_bar_time", "—")

                st.markdown("### 📡 EMA Signal State")

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

                bull = signal == 1
                bear = signal == -1
                has_pos = pos is not None

                if ema5 is not None and ema13 is not None:
                    _frow("📈", f"EMA(5) = {ema5:.2f}", True, "")
                    _frow("📉", f"EMA(13) = {ema13:.2f}", True, "")
                    _frow("📊", f"EMA(5) > EMA(13)", ema5 > ema13,
                          "BULL signal" if ema5 > ema13 else "BEAR signal")
                else:
                    st.info("EMA values not yet available — warming up history.")

                _frow("🕐", f"Last 15-min bar", True, last_bar)
                _frow("📌", "In position", has_pos,
                      pos.get("direction", "—") if has_pos else "Flat")
                _frow("🔄", "Signal active", bull or bear,
                      {1: "BULL", -1: "BEAR", 0: "FLAT"}.get(signal, "FLAT"))

                if has_pos:
                    st.markdown("---")
                    st.markdown("### 📋 Open Position")
                    st.json({
                        "direction":   pos.get("direction"),
                        "long_sym":    pos.get("long_sym"),
                        "short_sym":   pos.get("short_sym"),
                        "entry_debit": pos.get("entry_debit"),
                        "expiry":      pos.get("expiry"),
                        "qty":         pos.get("qty"),
                        "entry_time":  pos.get("entry_time", "")[:16],
                    })

    # ── TAB 4 — RESEARCH FINDINGS ────────────────────────────────────────────
    if tab_research.open:
        with tab_research:
            render_research_findings_tab(research_path)

    # ── TAB 5 — PERFORMANCE ──────────────────────────────────────────────────
    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab(bot_name)


def render_nifty_gex_ict_v2_panel(ltps: dict):
    """
    Five-tab panel for the NIFTY GEX ICT V2 Bot:
      Tab 1 — Overview (position card + key metrics)
      Tab 2 — Strategy Flowchart
      Tab 3 — Live Decision State (VA break / GEX level reach / regime / ICT confirm pipeline)
      Tab 4 — Research Findings
      Tab 5 — Performance
    """
    state = _load(STATE_FILES.get("NIFTY_GEX_ICT_V2"))

    st.markdown("## 🤖 NIFTY GEX ICT V2 Bot")
    st.caption(
        "Prior-day futures VA break → GEX level reach (09:20 snapshot) → 5-min regime refresh → "
        "breakout-only, any-of MSS/IFVG confirm · SELL ATM PE/CE · buffer 1.5% · EOD 15:14 · "
        "10 lots · NIFTY-only · IS+OOS Sharpe 2.48"
    )

    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview",
        "🗺️ Strategy Flowchart",
        "🧠 Live Decision State",
        "📖 Research Findings",
        "📈 Performance",
    ], on_change="rerun")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════════
    if tab_overview.open:
        with tab_overview:
            if not state:
                st.error("🔌 Bot not running — state file absent. Start the bot to see live data.")
            else:
                active   = state.get("active_trade")
                nifty    = state.get("nifty_ltp", 0.0)
                expiry   = state.get("expiry") or "—"
                lot_size = state.get("lot_size", "—")
                vah      = state.get("vah")
                val      = state.get("val")
                regime   = state.get("current_regime") or "—"
                break_dir = state.get("break_dir")
                module   = state.get("module")
                signal_done = state.get("signal_done_today", False)
                updated  = state.get("last_update", "")[:19].replace("T", " ")
                trades_today = state.get("trades_today", 0)
                wins_today   = state.get("wins_today", 0)
                pnl_today    = state.get("pnl_today", 0.0)

                if active:
                    banner_icon, banner_msg, banner_col = "📌", "IN POSITION", "#7b61ff"
                elif signal_done:
                    banner_icon, banner_msg, banner_col = "⏸", "SIGNAL DONE FOR TODAY", "#94a3b8"
                elif break_dir:
                    banner_icon, banner_msg, banner_col = "🟡", f"VA BROKEN {break_dir.upper()} — watching for confirm", "#fbbf24"
                else:
                    banner_icon, banner_msg, banner_col = "🟢", "SCANNING — waiting for VA break", "#00c875"

                st.markdown(
                    f'<div style="background:{banner_col}22;border-left:4px solid {banner_col};'
                    f'padding:10px 16px;border-radius:6px;margin-bottom:12px;">'
                    f'<span style="font-size:1.3em">{banner_icon}</span> '
                    f'<strong style="color:{banner_col};font-size:1.05em">{banner_msg}</strong>'
                    f'<span style="float:right;opacity:.6;font-size:.85em">Updated {updated}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("NIFTY", f"{nifty:,.1f}" if nifty else "—")
                c2.metric("Expiry", expiry)
                c3.metric("Lot Size", lot_size)
                c4.metric("VAH / VAL", f"{vah:.0f} / {val:.0f}" if vah and val else "—")
                c5.metric("Regime", "🔴 Negative Γ" if regime == "negative_gamma"
                           else ("🟢 Positive Γ" if regime == "positive_gamma" else "—"))

                d1, d2, d3, d4 = st.columns(4)
                d1.metric("Trades Today", trades_today)
                d2.metric("Wins Today", f"{wins_today}/{trades_today}" if trades_today else "—")
                d3.metric("P&L Today", f"₹{pnl_today:+,.0f}")
                d4.metric("Module", (module or "—").upper())

                st.markdown("---")

                if active:
                    sym       = active.get("symbol", "")
                    opt_type  = active.get("opt_type", "")
                    entry_p   = float(active.get("entry_prem", 0))
                    stop_lvl  = active.get("spot_stop")
                    tgt_lvl   = active.get("spot_target1")
                    qty       = int(active.get("qty", 0))
                    since     = active.get("entry_time", "")[:16].replace("T", " ")
                    confirm_kind = active.get("confirm_kind", "—")
                    ltp_now   = ltps.get(sym, entry_p)
                    pnl       = (entry_p - ltp_now) * qty

                    st.markdown("### 📌 Open Position")
                    t1, t2, t3, t4, t5 = st.columns(5)
                    t1.metric(f"🟠 SELL {opt_type}", sym)
                    t2.metric("Entry ₹", f"{entry_p:.2f}")
                    t3.metric("LTP ₹", f"{ltp_now:.2f}", delta=f"{ltp_now - entry_p:+.2f}")
                    t4.metric("MTM ₹", f"₹{pnl:+,.0f}", delta_color="normal" if pnl > 0 else "inverse")
                    t5.metric("Confirm", confirm_kind)

                    s1, s2, s3 = st.columns(3)
                    s1.metric("Spot Stop", f"{stop_lvl:.1f}" if stop_lvl else "—")
                    s2.metric("Spot Target1", f"{tgt_lvl:.1f}" if tgt_lvl else "—")
                    s3.metric("Qty / Since", f"{qty} · {since}")
                else:
                    st.info("No active position — waiting for next VA break + GEX reach + ICT confirm.")

                _render_today_trades_detail(_load_today_trades("nifty_gex_ict_v2_bot"))

            with st.expander("📋 Raw state"):
                st.json(state or {})

            if state and state.get("last_update"):
                age_sec, age_label = _staleness(state["last_update"])
                st.caption(f"State file: {age_label} · last_update {state['last_update'][11:19]}")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — STRATEGY FLOWCHART
    # ══════════════════════════════════════════════════════════════════════════
    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "NIFTY GEX ICT V2 Bot — Execution Logic",
                "How the bot decides on every completed 1-minute bar (09:15–15:14 IST).",
                [
                    fc_start("📅 09:15 — Session Open"),
                    fc_action("📚 Resolve weekly expiry · lot size",
                              "Prior-day futures value area (VAH/VAL) from 10pt-bin volume profile"),
                    fc_action("📊 09:20 — GEX Snapshot",
                              "call_resistance / put_support / top-3 |GEX| levels (frozen for the day)"),
                    fc_check("🔍 First VA Break?",
                             "Spot high &gt; VAH (up) or low &lt; VAL (down) — sticky, first break only"),
                    fc_action("🎯 Candidate GEX Levels",
                              "Priority order by distance from first candidate; cumulative touch-tracking"),
                    fc_check("📍 GEX Level Reached?",
                             "First level in priority order whose touch condition fires"),
                    fc_action("🔄 5-min Regime Refresh",
                              "Recompute GEX regime at the 5-min boundary ≤ reach time"),
                    fc_filter("⚖️ Regime = Negative Gamma? (breakout)",
                              no_label='Positive Γ → "fade" module — EXCLUDED, no trade'),
                    fc_check("✅ ICT Confirm (MSS or IFVG)",
                             "Continuation direction, within 375-min confirmation window (5-min bars)"),
                    fc_entry("💰 ORDER PLACED",
                             "Bullish → SELL ATM PE · Bearish → SELL ATM CE · 10 lots"),
                    fc_exit("🏁 Exit",
                            "Spot target1 / spot stop (buffer 1.5%) / EOD 15:14 IST — first to fire"),
                ],
            )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — LIVE DECISION STATE
    # ══════════════════════════════════════════════════════════════════════════
    if tab_state.open:
        with tab_state:
            if state:
                vah = state.get("vah")
                val = state.get("val")
                gex = state.get("gex_0920") or {}
                metrics = [
                    ("NIFTY", f"{state.get('nifty_ltp', 0):,.1f}" if state.get("nifty_ltp") else "—"),
                    ("VAH", f"{vah:.1f}" if vah else "—"),
                    ("VAL", f"{val:.1f}" if val else "—"),
                    ("HVL", f"{gex.get('hvl'):.1f}" if gex.get("hvl") else "—"),
                    ("Call Resistance", gex.get("call_resistance") or "—"),
                    ("Put Support", gex.get("put_support") or "—"),
                    ("GEX Levels", ", ".join(str(x) for x in gex.get("gex_levels", [])) or "—"),
                    ("Current Regime", state.get("current_regime") or "—"),
                ]
                reach_ts = state.get("reach_ts")
                window_end = state.get("window_end")
                filters = [
                    ("🔍", "First VA break detected", bool(state.get("break_dir")),
                     f"dir={state.get('break_dir') or '—'} @ {str(state.get('break_ts') or '—')[:16]}"),
                    ("📍", "GEX level reached", state.get("reached_level") is not None,
                     f"level={state.get('reached_level') or '—'} @ {str(reach_ts or '—')[:16]}"),
                    ("⚖️", "Module = breakout (not fade)", state.get("module") == "breakout",
                     f"module={state.get('module') or '—'}"),
                    ("⏱️", "Within confirmation window", bool(window_end) and not state.get("signal_done_today", False),
                     f"window_end={str(window_end or '—')[:16]}"),
                    ("✅", "ICT confirm fired (entry taken)", state.get("active_trade") is not None,
                     "MSS or IFVG" if state.get("active_trade") else "—"),
                ]
                has_pos = state.get("active_trade") is not None
                if has_pos:
                    readiness = ("📌", "IN POSITION — monitoring stop/target/EOD", "#7b61ff")
                elif state.get("signal_done_today"):
                    readiness = ("⏸", "SIGNAL DONE FOR TODAY — no further entries", "#94a3b8")
                elif state.get("break_dir"):
                    readiness = ("🟡", "VA BROKEN — awaiting GEX level reach + ICT confirm", "#fbbf24")
                else:
                    readiness = ("🟢", "SCANNING — waiting for first value-area break", "#00c875")

                render_decision_state(
                    state, key="gex_ict_v2",
                    updates_note="written every ~2s while the bot is running",
                    metrics=metrics,
                    filters=filters,
                    readiness=readiness,
                )
            else:
                st.error("🔌 Bot not running — state file absent. Start the bot to see live decision data.")

    # ── TAB 4 — RESEARCH FINDINGS ────────────────────────────────────────────
    if tab_research.open:
        with tab_research:
            render_research_findings_tab("gex_ict_v2_study/results_summary.md")

    # ── TAB 5 — PERFORMANCE ──────────────────────────────────────────────────
    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("nifty_gex_ict_v2_bot")


def render_nifty_atm_straddle_scalp_panel(ltps: dict):
    """
    Five-tab panel for the NIFTY ATM Straddle Scalp Bot:
      Tab 1 — Overview (position card + key metrics)
      Tab 2 — Strategy Flowchart
      Tab 3 — Live Decision State (entry window / SL / breakeven trail / target pipeline)
      Tab 4 — Research Findings
      Tab 5 — Performance
    """
    state = _load(STATE_FILES.get("NIFTY_ATM_STRADDLE"))
    today_iso = datetime.now().date().isoformat()
    is_today  = bool(state) and state.get("trade_date") == today_iso

    st.markdown("## 🤖 NIFTY ATM Straddle Scalp Bot")
    st.caption(
        "Single fixed entry 10:30 IST → SELL ATM CE+PE (short straddle) · per-leg SL 20% "
        "(broker SL-M) · survivor trailed to breakeven on sibling stop · target 0.75% of "
        "margin · EOD 15:14 · 10 lots/leg · no DTE floor · champion 10:30_sl20_tgt0.75, "
        "ALL 0-11 stages pass"
    )

    tab_overview, tab_flow, tab_state, tab_research, tab_perf = st.tabs([
        "📊 Overview",
        "🗺️ Strategy Flowchart",
        "🧠 Live Decision State",
        "📖 Research Findings",
        "📈 Performance",
    ], on_change="rerun")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════════
    if tab_overview.open:
        with tab_overview:
            if not state or not is_today:
                st.info("🟢 SCANNING — no position today yet. Waiting for the 10:30-10:35 IST entry window.")
            else:
                legs      = state.get("legs", {})
                closed    = state.get("closed", False)
                qty       = int(state.get("qty", 0))
                margin    = float(state.get("margin", 0))
                target_rs = float(state.get("target_rs", 0))
                strike    = state.get("atm_strike", "—")
                expiry    = state.get("expiry_str") or "—"
                spot0     = state.get("spot_at_entry")
                breakeven = state.get("breakeven_active", False)
                entry_t   = (state.get("entry_time") or "")[:16].replace("T", " ")
                updated   = state.get("last_update", "")[:19].replace("T", " ")

                if closed:
                    total = state.get("total_pnl")
                    banner_icon, banner_msg, banner_col = (
                        ("✅", f"CLOSED — {state.get('exit_reason', '?')} — P&L ₹{total:+,.0f}", "#00c875")
                        if total is not None and total >= 0 else
                        ("❌", f"CLOSED — {state.get('exit_reason', '?')} — P&L ₹{total:+,.0f}" if total is not None
                         else f"CLOSED — {state.get('exit_reason', '?')}", "#f87171")
                    )
                elif breakeven:
                    banner_icon, banner_msg, banner_col = "🛡️", "IN POSITION — one leg stopped, survivor at breakeven", "#fbbf24"
                else:
                    banner_icon, banner_msg, banner_col = "📌", "IN POSITION — both legs live", "#7b61ff"

                st.markdown(
                    f'<div style="background:{banner_col}22;border-left:4px solid {banner_col};'
                    f'padding:10px 16px;border-radius:6px;margin-bottom:12px;">'
                    f'<span style="font-size:1.3em">{banner_icon}</span> '
                    f'<strong style="color:{banner_col};font-size:1.05em">{banner_msg}</strong>'
                    f'<span style="float:right;opacity:.6;font-size:.85em">Updated {updated}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("NIFTY @ Entry", f"{spot0:,.1f}" if spot0 else "—")
                c2.metric("ATM Strike", strike)
                c3.metric("Expiry", expiry)
                c4.metric("Qty / Leg", qty or "—")
                c5.metric("Margin", f"₹{margin:,.0f}" if margin else "—")

                d1, d2, d3 = st.columns(3)
                d1.metric("Target", f"₹{target_rs:,.0f}" if target_rs else "—")
                mtm = state.get("total_pnl") if closed else state.get("current_mtm", 0.0)
                d2.metric("P&L" if closed else "MTM", f"₹{mtm:+,.0f}" if mtm is not None else "—")
                d3.metric("Breakeven Trail", "🛡️ Active" if breakeven else "— Not triggered")

                st.markdown("---")
                st.markdown("### 📌 Legs")
                for leg_key, side in (("sell_ce", "SELL CE"), ("sell_pe", "SELL PE")):
                    leg = legs.get(leg_key, {})
                    if not leg:
                        continue
                    sym       = leg.get("symbol", "")
                    entry     = float(leg.get("entry_prem", 0))
                    leg_closed = leg.get("closed", False)
                    sl_lvl    = leg.get("sl_level")
                    t1, t2, t3, t4, t5 = st.columns(5)
                    t1.metric(side, sym)
                    t2.metric("Entry ₹", f"{entry:.2f}")
                    if leg_closed:
                        exit_p = float(leg.get("exit_prem") or 0)
                        t3.metric("Exit ₹", f"{exit_p:.2f}", delta=f"{exit_p - entry:+.2f}", delta_color="inverse")
                        t4.metric("Leg P&L", f"₹{(entry - exit_p) * qty:+,.0f}")
                        t5.metric("Reason", leg.get("exit_reason") or "—")
                    else:
                        ltp = ltps.get(sym, entry)
                        t3.metric("LTP ₹", f"{ltp:.2f}", delta=f"{ltp - entry:+.2f}", delta_color="inverse")
                        t4.metric("Leg MTM", f"₹{(entry - ltp) * qty:+,.0f}")
                        t5.metric("SL Level", f"₹{sl_lvl:.2f}" if sl_lvl else "—")

                _render_today_trades_detail(_load_today_trades("nifty_atm_straddle_scalp_bot"))

            with st.expander("📋 Raw state"):
                st.json(state or {})

            if state and state.get("last_update"):
                age_sec, age_label = _staleness(state["last_update"])
                st.caption(f"State file: {age_label} · last_update {state['last_update'][11:19]}")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — STRATEGY FLOWCHART
    # ══════════════════════════════════════════════════════════════════════════
    if tab_flow.open:
        with tab_flow:
            render_strategy_flowchart(
                "NIFTY ATM Straddle Scalp Bot — Execution Logic",
                "Single fixed daily entry — no indicator/signal gate. Exit rules evaluated every 30s.",
                [
                    fc_start("📅 09:15 — Session Open"),
                    fc_check("⏱️ 10:30-10:35 IST window reached, no position taken today?",
                             "Single fixed entry time — no other signal"),
                    fc_action("📚 Resolve NIFTY spot → ATM strike → nearest weekly expiry",
                              "No DTE floor (min_dte=0) — expiry day included, Stage 10 validated"),
                    fc_check("✅ Both ATM CE/PE quotes resolve (non-zero premium)?"),
                    fc_entry("💰 SELL ATM CE + SELL ATM PE",
                             "10 lots/leg · broker-side SL-M placed immediately at entry×1.20"),
                    fc_check("🛑 Either leg's SL-M fills (120% of its own entry)?"),
                    fc_action("🛡️ Trail survivor's SL-M to its own entry price (breakeven)",
                              "Cancel resting SL-M, replace at trigger = survivor's entry premium"),
                    fc_check("🎯 Combined straddle P&L ≥ 0.75% of margin utilized?"),
                    fc_exit("🏁 Target — close remaining leg(s) at market"),
                    fc_exit("🏁 EOD 15:14 IST — close remaining leg(s) at market, no exceptions"),
                ],
            )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — LIVE DECISION STATE
    # ══════════════════════════════════════════════════════════════════════════
    if tab_state.open:
        with tab_state:
            if state and is_today:
                legs = state.get("legs", {})
                ce, pe = legs.get("sell_ce", {}), legs.get("sell_pe", {})
                metrics = [
                    ("NIFTY @ Entry", f"{state.get('spot_at_entry', 0):,.1f}" if state.get("spot_at_entry") else "—"),
                    ("ATM Strike", state.get("atm_strike") or "—"),
                    ("Expiry", state.get("expiry_str") or "—"),
                    ("Qty / Leg", state.get("qty") or "—"),
                    ("Margin", f"₹{state.get('margin', 0):,.0f}"),
                    ("Target ₹", f"₹{state.get('target_rs', 0):,.0f}"),
                    ("Combined MTM", f"₹{state.get('current_mtm', 0):+,.0f}"),
                    ("Breakeven Trail", "🛡️ Active" if state.get("breakeven_active") else "— Not triggered"),
                ]
                closed = state.get("closed", False)
                entry_taken = bool(ce) and bool(pe)
                any_leg_stopped = ce.get("closed") or pe.get("closed")
                filters = [
                    ("⏱️", "10:30-10:35 entry window reached", entry_taken or closed,
                     (state.get("entry_time") or "—")[:16].replace("T", " ")),
                    ("📚", "NIFTY spot + ATM CE/PE resolved", entry_taken,
                     f"strike={state.get('atm_strike') or '—'}"),
                    ("💰", "Position entered (SELL CE + SELL PE)", entry_taken,
                     f"qty/leg={state.get('qty') or '—'}"),
                    ("🛑", "Per-leg SL-M armed on both legs", bool(ce.get("sl_order_id")) and bool(pe.get("sl_order_id")) if entry_taken and not closed else entry_taken,
                     f"CE sl={ce.get('sl_level') or '—'}  PE sl={pe.get('sl_level') or '—'}"),
                    ("🛡️", "Breakeven trail triggered (one leg stopped)", bool(state.get("breakeven_active")),
                     f"any_leg_stopped={any_leg_stopped}" if entry_taken else "—"),
                    ("🎯", "Target or EOD close fired", closed,
                     state.get("exit_reason") or "—"),
                ]
                if closed:
                    total = state.get("total_pnl")
                    readiness = ("✅" if (total or 0) >= 0 else "❌",
                                 f"CLOSED — {state.get('exit_reason', '?')} — P&L ₹{total:+,.0f}" if total is not None
                                 else f"CLOSED — {state.get('exit_reason', '?')}",
                                 "#00c875" if (total or 0) >= 0 else "#f87171")
                elif state.get("breakeven_active"):
                    readiness = ("🛡️", "IN POSITION — survivor trailed to breakeven, monitoring target/EOD", "#fbbf24")
                elif entry_taken:
                    readiness = ("📌", "IN POSITION — both legs live, monitoring SL/target/EOD", "#7b61ff")
                else:
                    readiness = ("🟢", "SCANNING — waiting for 10:30 IST entry window", "#00c875")

                render_decision_state(
                    state, key="atm_straddle_scalp",
                    updates_note="written every ~5s while the bot is running",
                    metrics=metrics,
                    filters=filters,
                    readiness=readiness,
                )
            else:
                st.info("🟢 SCANNING — no position today yet. Waiting for the 10:30-10:35 IST entry window.")

    # ── TAB 4 — RESEARCH FINDINGS ────────────────────────────────────────────
    if tab_research.open:
        with tab_research:
            render_research_findings_tab("atm_short_straddle_scalp_study/results_summary.md")

    # ── TAB 5 — PERFORMANCE ──────────────────────────────────────────────────
    if tab_perf.open:
        with tab_perf:
            render_bot_performance_tab("nifty_atm_straddle_scalp_bot")


def render_nifty_ema_spread_panel(ltps: dict):
    _render_ema_spread_panel(
        ltps,
        state_key     = "NIFTY_EMA_SPREAD",
        title         = "NIFTY EMA Spread Bot",
        caption       = ("EMA(5,13) crossover · 15-min NIFTY index bars · 50pt ATM debit spread "
                         "· NRML positional · TP 1.5R · SL 0.05R · 10/10 stages · OOS Sharpe 6.14"),
        underlying    = "NIFTY",
        spread_width  = 50,
        exchange      = "NFO",
        research_path = "index_spread_study/results_summary.md",
        bot_name      = "nifty_ema_spread_bot",
    )


def render_banknifty_ema_spread_panel(ltps: dict):
    _render_ema_spread_panel(
        ltps,
        state_key     = "BANKNIFTY_EMA_SPREAD",
        title         = "BANKNIFTY EMA Spread Bot",
        caption       = ("EMA(5,13) crossover · 15-min BANKNIFTY index bars · 100pt ATM debit spread "
                         "· NRML positional · TP 1.5R · SL 0.05R · Stage 9 confirm · OOS Sharpe 3.00"),
        underlying    = "BANKNIFTY",
        spread_width  = 100,
        exchange      = "NFO",
        research_path = "index_spread_study/results_summary.md",
        bot_name      = "banknifty_ema_spread_bot",
    )


def render_sensex_ema_spread_panel(ltps: dict):
    _render_ema_spread_panel(
        ltps,
        state_key     = "SENSEX_EMA_SPREAD",
        title         = "SENSEX EMA Spread Bot",
        caption       = ("EMA(5,13) crossover · 15-min SENSEX index bars · 100pt ATM debit spread "
                         "· NRML positional · TP 1.5R · SL 0.05R · Stage 9 confirm · OOS Sharpe 2.90"),
        underlying    = "SENSEX",
        spread_width  = 100,
        exchange      = "BFO",
        research_path = "index_spread_study/results_summary.md",
        bot_name      = "sensex_ema_spread_bot",
    )


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every="5s")
def _render_portfolio_fragment(sym_exchange):
    """Self-refreshing Portfolio Snapshot. Must be called unconditionally at a
    fixed script position in main() — a run_every fragment called only inside a
    conditional branch keeps ticking after nav_view moves away from it, and its
    stale reruns then bleed into whatever page is showing (see incident 2026-09-08).
    """
    if st.session_state.get("nav_view") != "🏠 Dashboard Overview":
        return
    positionbook = _fetch_positionbook_full()
    ltps         = _all_ltps(sym_exchange, pb=positionbook)
    render_portfolio_snapshot(ltps, positionbook=positionbook)


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
        "🤖 HTF PO3 Bot", "🤖 BANKNIFTY BB Options", "🤖 BNF BB Opening Candle",
        "🤖 BB Mean Reversion",
        # "🤖 HA Options Bot",  # RETIRED 2026-07-10
        "🤖 NIFTY MACD Map", "🤖 NIFTY EOD Hold", "🤖 MA Cross Seller", "🔬 NTS + OBI Gate",
        "🤖 MACD M2 Sell Options", "🤖 BNF Trend Pullback Positional", "🤖 GEX ICT V2",
        "🤖 NIFTY ATM Straddle Scalp",
    ]
    _GRP_STK = [
        # "🤖 Pre-Open Gap Fade",  # RETIRED 2026-06-24
        # "🔬 Equity OBI",  # RETIRED 2026-06-19
        # "🤖 Gap Fade EOD",   # RETIRED 2026-06-04
        # "🤖 EMA Swing Scanner",  # RETIRED 2026-06-04
        "🔬 VP Swing Screener",
        "🔬 VP Swing Screener (Daily)",
    ]
    _GRP_WK  = ["📅 NIFTY Iron Fly Weekly", "📅 SENSEX Iron Fly Weekly",
                "📅 NIFTY EMA Spread", "📅 SENSEX EMA Spread"]
    _GRP_MO  = ["📅 BANKNIFTY Iron Fly Monthly", "📆 Flat Blue Line Monthly",
                "📆 BANKNIFTY EMA Spread"]
    _GRP_SYS = ["⚙️ Infrastructure", "🎛️ Bot Controls"]
    _GRP_ANA = ["📊 Performance Hub"]

    # Nav label -> bot_registry.py "bot" key, so we can filter each bot nav
    # group down to only the bots that actually run in the selected workspace.
    # Labels with no entry here (system/analytics/non-bot items) always show.
    _NAV_LABEL_TO_BOT: dict[str, str] = {
        "🤖 Nifty BB OB":                     "nifty_bb_overbought_bot",
        "🤖 Nifty Trend Seller":              "nifty_trend_seller_bot",
        "🤖 SENSEX Trend Seller":             "sensex_trend_seller_bot",
        "🤖 HTF PO3 Bot":                     "htf_po3_bot",
        "🤖 BANKNIFTY BB Options":            "banknifty_bb_options_bot",
        "🤖 BNF BB Opening Candle":           "banknifty_bb_opening_candle_bot",
        "🤖 BB Mean Reversion":               "bb_mean_reversion_bot",
        "🤖 NIFTY MACD Map":                  "nifty_macd_map_bot",
        "🤖 NIFTY EOD Hold":                  "nifty_eod_hold_bot",
        "🤖 MA Cross Seller":                 "nifty_ma_cross_seller_bot",
        "🔬 NTS + OBI Gate":                  "NTS_OBI",
        "🤖 MACD M2 Sell Options":            "macd_m2_sell_options_bot",
        "🤖 BNF Trend Pullback Positional":   "banknifty_trend_pullback_positional_bot",
        "🤖 GEX ICT V2":                      "nifty_gex_ict_v2_bot",
        "🤖 NIFTY ATM Straddle Scalp":        "nifty_atm_straddle_scalp_bot",
        "📅 NIFTY Iron Fly Weekly":           "nifty_iron_fly_weekly_bot",
        "📅 SENSEX Iron Fly Weekly":          "sensex_iron_fly_weekly_bot",
        "📅 NIFTY EMA Spread":                "nifty_ema_spread_bot",
        "📅 SENSEX EMA Spread":               "sensex_ema_spread_bot",
        "📅 BANKNIFTY Iron Fly Monthly":      "banknifty_iron_fly_monthly_bot",
        "📆 Flat Blue Line Monthly":          "flat_blue_line_monthly_bot",
        "📆 BANKNIFTY EMA Spread":            "banknifty_ema_spread_bot",
    }

    def _shown_in_workspace(label: str) -> bool:
        bot_name = _NAV_LABEL_TO_BOT.get(label)
        if bot_name is None:
            return True
        meta = _REGISTRY_BOT_META.get(bot_name)
        if meta is None:
            return True
        return _status_in_workspace(meta, selected_ws_id) is not None

    _GRP_OPT = [l for l in _GRP_OPT if _shown_in_workspace(l)]
    _GRP_STK = [l for l in _GRP_STK if _shown_in_workspace(l)]
    _GRP_WK  = [l for l in _GRP_WK  if _shown_in_workspace(l)]
    _GRP_MO  = [l for l in _GRP_MO  if _shown_in_workspace(l)]

    # Pre-set each radio's session_state key to the currently active view
    # (or None if the view lives in a different section). This ensures only
    # the correct section shows a highlighted item.
    for _key, _items in [
        ("nav_ov", _GRP_OV), ("nav_opt", _GRP_OPT), ("nav_stk", _GRP_STK),
        ("nav_wk", _GRP_WK), ("nav_mo", _GRP_MO), ("nav_sys", _GRP_SYS),
        ("nav_ana", _GRP_ANA),
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

    _sec_hdr("📊 ANALYTICS")
    st.sidebar.radio(" ", _GRP_ANA, key="nav_ana",
        label_visibility="collapsed",
        on_change=_nav_changed, args=("nav_ana",))

    view = st.session_state["nav_view"]

    st.sidebar.markdown("---")

    # ── Global Data Fetching ────────────────────────────────────────────────
    # Fetch positionbook once and share it across LTP resolution + overview page
    sym_exchange = _collect_all_open_symbols()
    if view == "🏠 Dashboard Overview":
        # Dashboard Overview only uses the self-refreshing fragment below, which
        # fetches its own fresh positionbook/ltps internally — skip the duplicate
        # fetch here since nothing in this view's branch uses these values.
        positionbook, ltps = None, {}
    else:
        positionbook = _fetch_positionbook_full()
        ltps         = _all_ltps(sym_exchange, pb=positionbook)   # positionbook first, multiquotes fallback

    # ── Portfolio Snapshot (self-refreshing fragment) ───────────────────────
    # Called unconditionally at a fixed script position so it never becomes an
    # orphaned run_every fragment when nav_view switches to another page.
    if view == "🏠 Dashboard Overview":
        st.subheader("🏠 Portfolio & System Overview")
    _render_portfolio_fragment(sym_exchange)

    # ── View Routing ────────────────────────────────────────────────────────
    if view == "🏠 Dashboard Overview":
        st.markdown("---")
        st.subheader("🏭 Bot Fleet Status")
        render_fleet_status()

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

    elif view == "🤖 BNF BB Opening Candle":
        render_bnf_bb_opening_candle_panel(ltps)

    elif view == "🤖 BB Mean Reversion":
        render_bb_mean_reversion_panel(ltps)

    elif view == "🤖 NIFTY MACD Map":
        render_nifty_macd_map_panel(ltps)

    elif view == "🤖 NIFTY EOD Hold":
        render_nifty_eod_hold_panel(ltps)

    elif view == "🤖 MA Cross Seller":
        render_nifty_ma_cross_panel(ltps)

    elif view == "🤖 GEX ICT V2":
        render_nifty_gex_ict_v2_panel(ltps)

    elif view == "🤖 NIFTY ATM Straddle Scalp":
        render_nifty_atm_straddle_scalp_panel(ltps)

    elif view == "🔬 NTS + OBI Gate":
        render_nts_obi_panel(ltps)

    elif view == "🤖 MACD M2 Sell Options":
        render_macd_m2_sell_panel(ltps)

    elif view == "🤖 BNF Trend Pullback Positional":
        render_bnf_trend_pullback_panel(ltps)

    # ── Stock Bots ───────────────────────────────────────────────────────────
    elif view == "🤖 Pre-Open Gap Fade":
        render_gap_fade_panel(ltps)

    elif view == "🔬 VP Swing Screener":
        render_vp_swing_screener_panel(ltps)

    elif view == "🔬 VP Swing Screener (Daily)":
        render_vp_swing_daily_screener_panel(ltps)

    # "🤖 Gap Fade EOD" — RETIRED 2026-06-04
    # "🤖 EMA Swing Scanner" — RETIRED 2026-06-04

    # "🔬 Equity OBI" — RETIRED 2026-06-19
    # "🤖 HA Options Bot" — RETIRED 2026-07-10

    # ── Weekly Positions ─────────────────────────────────────────────────────
    elif view == "📅 NIFTY Iron Fly Weekly":
        render_iron_fly_panel(ltps)

    elif view == "📅 SENSEX Iron Fly Weekly":
        render_sensex_iron_fly_panel(ltps)

    elif view == "📅 BANKNIFTY Iron Fly Monthly":
        render_bnf_iron_fly_monthly_panel(ltps)

    elif view == "📆 Flat Blue Line Monthly":
        render_flat_blue_line_monthly_panel(ltps)

    elif view == "📅 NIFTY EMA Spread":
        render_nifty_ema_spread_panel(ltps)

    elif view == "📆 BANKNIFTY EMA Spread":
        render_banknifty_ema_spread_panel(ltps)

    elif view == "📅 SENSEX EMA Spread":
        render_sensex_ema_spread_panel(ltps)

    # ── System ───────────────────────────────────────────────────────────────
    elif view == "⚙️ Infrastructure":
        st.subheader("⚙️ System Infrastructure")
        render_heartbeats()

    elif view == "🎛️ Bot Controls":
        render_bot_controls()

    # ── Analytics ────────────────────────────────────────────────────────────
    elif view == "📊 Performance Hub":
        render_performance_hub()

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
