#!/usr/bin/env python3
"""
_template_bot.py — START HERE for any new live/paper trading bot
================================================================================
Copy this file (and rename it) as the starting point for a new bot instead of
copy-pasting a sibling bot. Siblings carry whatever bugs they shipped with —
this template exists specifically because three classes of bug have shipped
repeatedly across bots (most recently in flat_blue_line_monthly_bot.py on
2026-06-08): fake "paper mode" flags that silently swallow orders, misuse of
`strike_int` in symbol resolution, and missing dashboard registration. Every
section below that matters for those three is marked ⚠️ — read them before
writing a line of strategy logic.

Read first, every time, no exceptions:
  - docs/trading/bot-pipeline.md          — Stage 11–13 doctrine, technical rules
  - docs/trading/deployment-checklist.md  — final gate before declaring done
  - directives/live_bot_websocket.md      — WS auth, canonical symbols, get_history
  - directives/bot_code_qa.md             — pre-finalize QA checklist

Replace every <ALL_CAPS_PLACEHOLDER> below, delete sections you don't need,
and DELETE THIS DOCSTRING'S META-COMMENTARY once your bot is real.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, time as dt_time
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Path / env — copy verbatim, this is the standard ROOT pattern ────────────
PROJECT_ROOT = Path(__file__).parent.parent   # → openalgo/   (adjust if nested
                                              #   one level deeper, e.g. inside
                                              #   live_trading/<strategy>/)
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from openalgo import api                                                       # noqa
from live_trading.api_utils import HOST, get_expiry_dates                      # noqa
from live_trading.shared.telegram_notifier import send_async                   # noqa
from live_trading.shared.trade_logger import log_trade_to_db                   # noqa
# ⚠️  SYMBOL RESOLUTION — use the shared resolver, not a hand-rolled one:
from live_trading.shared.atm_resolver import resolve_atm_option, get_atm_strike  # noqa

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "<STRATEGY_NAME>_bot.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

API_KEY = os.getenv("OPENALGO_API_KEY")

# ──────────────────────────────────────────────────────────────────────────────
# ⚠️  DO NOT ADD A PAPER_MODE / DRY_RUN / SIMULATE FLAG. NONE. EVER.
#
# Per project doctrine (CLAUDE.md → "How paper trading actually works" and
# docs/trading/bot-pipeline.md Stage 12/13): bots ALWAYS fire real OpenAlgo
# REST orders via placeorder(). OpenAlgo's own Sandbox/Analyze Mode — a UI-level
# toggle, set by Ramakrishna, NOT a bot setting — transparently intercepts and
# simulates the fill. That is the ONLY thing that distinguishes "paper" from
# "live": nothing in this file should know or care which mode OpenAlgo is in.
#
# An internal flag here is not a safety net — it actively breaks the pipeline.
# It lets _order() fabricate a fake {"status": "ok"} without ever reaching
# OpenAlgo, so the trade never appears in the Positions/Orderbook UI and never
# gets logged to performance.db. This EXACT bug shipped in
# flat_blue_line_monthly_bot.py on 2026-06-08 — entries were logged locally as
# "PAPER BUY/SELL" yet were completely invisible in OpenAlgo. `PAPER_MODE`
# currently appears in 8 separate bot files. Don't make it 9.
#
# Going live = Ramakrishna flips OpenAlgo's UI mode toggle. Full stop.
# ──────────────────────────────────────────────────────────────────────────────

STRATEGY_NAME = "<STRATEGY_NAME>"          # e.g. "FLAT_BLUE_LINE_MONTHLY" — used in
                                           # placeorder(strategy=...), Telegram alerts,
                                           # and performance_db strategy attribution
OPT_EXCHANGE  = "NFO"                      # NFO for NIFTY/BANKNIFTY, BFO for SENSEX/BANKEX
IDX_EXCHANGE  = "NSE_INDEX"                # canonical index quote exchange — see
                                           # directives/live_bot_websocket.md for the
                                           # authoritative index symbol/exchange table

POLL_SECS   = 60     # monitoring interval
ORDER_DELAY = 1.5    # seconds between sequential leg orders

# ⚠️  MIS/intraday EOD exit hard limit — 15:14 IST MAXIMUM.
# The sandbox auto-squaresoff ALL MIS positions at 15:15 IST; any close order
# arriving after 15:15 silently fails (position already flat, never logged).
# NRML bots (positional/overnight, e.g. iron-fly style) are exempt from this.
EOD_EXIT_TIME = dt_time(15, 14)            # MIS bots: must be <= 15:14. Delete
                                           # this line entirely for NRML bots.

STATE_FILE       = LOG_DIR / "<strategy_name>_state.json"
PAPER_TRADES_CSV = LOG_DIR / "<strategy_name>_paper_trades.csv"


# ══════════════════════════════════════════════════════════════════════════════
# QUOTES — always batch via /api/v1/multiquotes, never loop single /api/v1/quotes
# ══════════════════════════════════════════════════════════════════════════════
#
# ⚠️  Why this matters: OpenAlgo runs as a SINGLE eventlet worker (gunicorn -w 1)
# shared by ~16 concurrent bots. A bot that fires N sequential single-symbol
# /api/v1/quotes calls per monitoring tick (6-14 calls is typical for a
# multi-leg strategy) WILL intermittently hit "Read timed out" under load —
# this happened repeatedly in flat_blue_line_monthly_bot.py until every
# quote fetch (including the easy-to-miss spot-price fetch needed BEFORE
# symbol resolution) was routed through _multiquote(). Use the helper below
# for every quote fetch, including the very first one in your entry path.

def _multiquote(items: list[tuple[str, str]],
                retries: int = 3, delay: float = 3.0) -> dict[str, float]:
    """
    Batch-fetch LTPs for multiple (symbol, exchange) pairs in ONE
    /api/v1/multiquotes round-trip, with retry/backoff baked in.
    Returns {symbol: ltp} for whatever resolved with ltp > 0; missing/zero/
    error entries are simply absent — callers should .get(sym, fallback).
    """
    pending = list(dict.fromkeys(items))   # de-dupe, preserve order
    out: dict[str, float] = {}
    for attempt in range(1, retries + 1):
        if not pending:
            break
        payload = {
            "apikey":  API_KEY,
            "symbols": [{"symbol": s, "exchange": e} for s, e in pending],
        }
        try:
            res = requests.post(f"{HOST}/api/v1/multiquotes", json=payload, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data.get("status") == "success":
                    for row in data.get("results", []):
                        sym = row.get("symbol")
                        qd  = row.get("data") or {}
                        ltp = (qd.get("ltp") or qd.get("last_price")
                               or qd.get("close") or qd.get("c") or 0)
                        try:
                            price = float(ltp)
                        except (TypeError, ValueError):
                            price = 0.0
                        if sym and price > 0:
                            out[sym] = price
        except Exception as e:
            logger.error(f"multiquotes error: {e}")
        pending = [(s, e) for s, e in pending if s not in out]
        if pending and attempt < retries:
            time.sleep(delay)
    if pending:
        logger.warning(f"  multiquotes: no LTP for {[s for s, _ in pending]}")
    return out


# ══════════════════════════════════════════════════════════════════════════════
# SYMBOL RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════
#
# ⚠️  `strike_int` in /api/v1/optionsymbol (and api_utils.get_option_symbol) is
# the STRIKE INTERVAL (e.g. 50, 100) — NOT an absolute strike. Passing a
# computed absolute strike (e.g. 23200) silently produces a bogus/404 symbol.
# This exact misuse caused the recurring "ATM symbols unresolved" failure in
# flat_blue_line_monthly_bot.py and appears in `strike_int` form across half a
# dozen bot files. TWO correct options — pick whichever fits your strategy:
#
#   (a) PREFERRED — use the shared resolver (handles NIFTY/BANKNIFTY/SENSEX/etc,
#       parameterized by index; extend INDEX_CONFIG there if yours is missing):
#
#         from live_trading.shared.atm_resolver import resolve_atm_option
#         info = resolve_atm_option(spot, opt_type="CE", index="BANKNIFTY")
#         symbol = info["symbol"]
#
#   (b) Direct canonical-symbol string construction (no API round-trip,
#       what flat_blue_line_monthly_bot now does for its 6 non-ATM legs):
#
#         symbol = f"{underlying}{expiry_str}{strike}{opt_type}"
#         # e.g. "BANKNIFTY30JUN2654200CE" — symtoken master-contract mapping
#         # translates this to broker-native format (e.g. Fyers monthly-only
#         # NSE:BANKNIFTY26JUN54200CE) transparently. Verify the mapping exists
#         # in the DB before relying on it for a new underlying/expiry pattern.
#
# Whichever you pick, NEVER do: get_option_symbol(..., strike_int=<absolute_strike>)


# ══════════════════════════════════════════════════════════════════════════════
# ORDER PLACEMENT — the ONE function that talks to OpenAlgo's order API
# ══════════════════════════════════════════════════════════════════════════════

def _order(client, symbol: str, action: str, qty: int,
           product: str = "MIS") -> dict:
    """
    ALWAYS fires a real OpenAlgo REST order — never short-circuit this, never
    gate it behind a flag. OpenAlgo's own Sandbox/Analyze Mode (UI toggle, not
    a bot setting) transparently intercepts and simulates the fill — that's
    what makes it show up correctly in the Positions/Orderbook UI and get
    logged to performance.db. See the doctrine block near the top of this file.
    """
    try:
        resp = client.placeorder(
            strategy=STRATEGY_NAME, symbol=symbol, action=action,
            exchange=OPT_EXCHANGE, price_type="MARKET",
            product=product, quantity=qty,
        )
        logger.info(f"  {action} {symbol} x{qty}: {resp}")
        return resp if isinstance(resp, dict) else {"status": "ok"}
    except Exception as e:
        logger.error(f"  placeorder failed ({action} {symbol}): {e}")
        return {"status": "error", "message": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# STATE PERSISTENCE — crash-safe restart, no re-entry on relaunch
# ══════════════════════════════════════════════════════════════════════════════

def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
    return {}


def _save_state(state: dict) -> None:
    try:
        state["last_update"] = datetime.now().isoformat()
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        logger.error(f"Failed to save state: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TODO: STRATEGY LOGIC GOES HERE
# ══════════════════════════════════════════════════════════════════════════════
#
# Translate research/<study>/backtest_oos.py 1-for-1 — no "improvements" without
# re-running OOS validation. Typical shape:
#
#   class <StrategyName>Bot:
#       def __init__(self): ...
#       async def _check_entry(self): ...   # signal detection + entry-window gate
#       async def _enter(self, ...): ...    # resolve symbols, batch-quote, place orders
#       async def _monitor(self): ...       # batch-quote legs+spot, check exits
#       async def _exit(self, reason): ...  # unwind in correct order, log, alert
#       async def run(self): ...            # main poll loop
#
# Session init rule (incident 2026-07-07): never latch a "_session_started"
# flag before the init fetches (history/expiry) actually succeeded. Latch only
# on success and re-attempt failed init on a cooldown (e.g. 120s) — otherwise a
# transient OpenAlgo outage at open silently disables the bot for the day.
# api_utils already retries transport errors (~17s); the bot-level retry
# covers longer outages.


# ══════════════════════════════════════════════════════════════════════════════
# ⚠️  REGISTER ME — before this bot can go anywhere near Stage 11, wire it into
# ALL FOUR of these (see docs/trading/deployment-checklist.md "Registration"):
#
#   [ ] live_trading/start_all_bots.py
#         → add to the BOTS list
#
#   [ ] live_trading/streamlit_dashboard.py   — THREE separate places, all of them:
#         [ ] STATE_FILES dict   (bot key → state JSON path)
#         [ ] BOT_META dict      (display name + research notes)
#         [ ] render_portfolio_snapshot() — add an _add_open()/_add_closed()
#             block per leg/symbol (copy the nearest multi-leg bot's block,
#             e.g. search for "FBL" or "BNF IF"). ⚠️ THIS THIRD ONE IS THE
#             MOST COMMONLY MISSED — skip it and your positions will render
#             under the generic "📊 Broker" catch-all instead of your bot's
#             name (this exact thing happened to flat_blue_line_monthly_bot
#             on 2026-06-08, hours after its first live trade).
#
#   [ ] live_trading/performance_review.py
#         → add the bot's strategy name / state file mapping
#
#   [ ] (equity bots only) live_trading/market_review.py
#         → AND pass strategy_type="equity" + explicit direction to
#           log_trade_to_db() — defaults silently misclassify equity trades
#
#   [ ] live_trading/active_trading_bots.md
#         → document status, params, and current stage
#
# Then run `python3 live_trading/sanity_check.py` — it cross-checks the
# launcher registry against dashboard registration and will flag anything
# still missing before you find out the hard way from the dashboard.
# ══════════════════════════════════════════════════════════════════════════════


if __name__ == "__main__":
    logger.info(f"🚀  {STRATEGY_NAME}  |  orders fire via OpenAlgo "
                f"(paper/live mode is set in the OpenAlgo UI, not here)")
    # TODO: instantiate and run your bot's main loop, e.g.:
    #   bot = <StrategyName>Bot()
    #   asyncio.run(bot.run())
