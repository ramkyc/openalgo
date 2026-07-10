"""
live_trading/shared/bot_registry.py
=====================================
Single source of truth for bot metadata and lifecycle status.

Both performance_review.py and streamlit_dashboard.py import BOT_META /
ACTIVE_BOTS / RETIRED_BOTS from here instead of keeping their own copies.
Before this consolidation there were three independently-maintained
registries (performance_review.py::BOT_META, streamlit_dashboard.py::BOT_META,
streamlit_dashboard.py::BOT_LIFECYCLE) plus prose comments in
start_all_bots.py — they drifted: preopen_gap_fade_bot was retired in the
launcher on 2026-06-24 but kept showing as active in performance_review.py's
report until this fix.

status:
    "live"    — real money deployed (see workspace)
    "paper"   — paper trading, accumulating toward the Stage 11 (20-session) gate
    "retired" — no longer running; historical performance only

workspace:
    Which OpenAlgo instance currently runs this bot. Defaults to WORKSPACE
    (this repo, "CRK") when omitted. Only differs for a bot that migrated to
    another instance (e.g. banknifty_bb_options_bot -> fyers_cs).

Bots with zero rows in performance.db (killed before or without ever logging
a trade — e.g. bollinger_options_bot, candle_breaker_bot, orb_champion_15m_bot,
daily_sniper_bot, bb_paper_bot, bb_5m_scanner, tick_stasher, supertrend_5s_bot)
are intentionally omitted — they can never appear in a report keyed off the
trades table, so there is nothing to register.
"""

from __future__ import annotations

WORKSPACE = "CRK"

BOT_REGISTRY: list[dict] = [
    # ── Live (real money) ────────────────────────────────────────────────────
    {"bot": "banknifty_bb_options_bot", "label": "BANKNIFTY BB Options", "type": "Options", "universe": "BANKNIFTY",
     "status": "live", "workspace": "CS",
     "reason": "Migrated to live deployment on fyers_cs", "status_date": "2026-06-29"},

    # ── Paper (Stage 11 accumulation) ────────────────────────────────────────
    {"bot": "nifty_trend_seller_bot",      "label": "Nifty Trend Seller",          "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "sensex_trend_seller_bot",     "label": "SENSEX Trend Seller",         "type": "Options", "universe": "SENSEX",           "status": "paper"},
    {"bot": "htf_po3_bot",                 "label": "HTF PO3 Bot",                 "type": "Options", "universe": "NIFTY/BANKNIFTY",  "status": "paper"},
    {"bot": "banknifty_bb_opening_candle_bot", "label": "BANKNIFTY BB Opening Candle", "type": "Options", "universe": "BANKNIFTY/SENSEX", "status": "paper"},
    {"bot": "nifty_bb_overbought_bot",     "label": "Nifty BB Overbought",         "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "nifty_macd_map_bot",          "label": "NIFTY MACD Map",              "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "nifty_eod_hold_bot",          "label": "NIFTY EOD Hold",              "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "nifty_iron_fly_weekly_bot",   "label": "NIFTY Iron Fly Weekly",       "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "sensex_iron_fly_weekly_bot",  "label": "SENSEX Iron Fly Weekly",      "type": "Options", "universe": "SENSEX",           "status": "paper"},
    {"bot": "banknifty_iron_fly_monthly_bot", "label": "BANKNIFTY Iron Fly Monthly", "type": "Options", "universe": "BANKNIFTY",      "status": "paper"},
    {"bot": "flat_blue_line_monthly_bot",  "label": "Flat Blue Line Monthly",      "type": "Options", "universe": "NIFTY+BANKNIFTY",  "status": "paper"},
    {"bot": "NTS_OBI",                     "label": "NTS + OBI Gate",              "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "macd_m2_sell_options_bot",    "label": "MACD M2 Sell Options",        "type": "Options", "universe": "NIFTY/BANKNIFTY",  "status": "paper"},
    {"bot": "banknifty_trend_pullback_positional_bot", "label": "BNF Trend Pullback Positional", "type": "Options", "universe": "BANKNIFTY", "status": "paper"},
    {"bot": "nifty_ema_spread_bot",        "label": "NIFTY EMA Spread",            "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "banknifty_ema_spread_bot",    "label": "BANKNIFTY EMA Spread",        "type": "Options", "universe": "BANKNIFTY",         "status": "paper"},
    {"bot": "sensex_ema_spread_bot",       "label": "SENSEX EMA Spread",           "type": "Options", "universe": "SENSEX",           "status": "paper"},
    {"bot": "bb_mean_reversion_bot",       "label": "BB Mean Reversion",           "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "nifty_ma_cross_seller_bot",   "label": "Nifty MA Cross Seller",       "type": "Options", "universe": "NIFTY",            "status": "paper"},

    # ── Retired ───────────────────────────────────────────────────────────────
    {"bot": "preopen_gap_fade_bot", "label": "Pre-Open Gap Fade", "type": "Equity", "universe": "NIFTY50",
     "status": "retired", "reason": "No post-cost edge on equity intraday", "status_date": "2026-06-24"},
    {"bot": "equity_obi", "label": "Equity OBI Bot", "type": "Equity", "universe": "RELIANCE/HDFCBANK",
     "status": "retired", "reason": "WR 38.3%, P&L -4,122 over 149 trades / 23 sessions", "status_date": "2026-06-19"},
    {"bot": "gap_fade_eod_bot", "label": "Gap Fade EOD", "type": "Equity", "universe": "NIFTY50",
     "status": "retired", "reason": "WR 25%, P&L -7,739 over 4 trades", "status_date": "2026-06-04"},
    {"bot": "ema_swing_scanner", "label": "EMA Swing Scanner", "type": "Equity", "universe": "NIFTY50",
     "status": "retired", "reason": "WR 0%, P&L -8,820 over 2 trades - no research study", "status_date": "2026-06-04"},
    {"bot": "iron_fly_weekly_bot", "label": "Iron Fly Weekly (legacy)", "type": "Options", "universe": "NIFTY/SENSEX",
     "status": "retired", "reason": "Legacy pre-split name, superseded by nifty_iron_fly_weekly_bot / sensex_iron_fly_weekly_bot", "status_date": None},
    {"bot": "ha_options_bot", "label": "HA Options Bot", "type": "Options", "universe": "NIFTY/BNF/SENSEX",
     "status": "retired", "reason": "Failed Stage 11 paper-trading gate", "status_date": "2026-07-10"},
]

BOT_META: dict[str, dict] = {b["bot"]: b for b in BOT_REGISTRY}

# "Active" for this workspace = live/paper AND actually running here (not migrated elsewhere).
ACTIVE_BOTS  = [b for b in BOT_REGISTRY if b["status"] != "retired" and b.get("workspace", WORKSPACE) == WORKSPACE]
RETIRED_BOTS = [b for b in BOT_REGISTRY if b["status"] == "retired" or b.get("workspace", WORKSPACE) != WORKSPACE]


def is_active(bot_name: str) -> bool:
    """True if bot_name is live/paper and running in this workspace. Unregistered bots default to active
    so a newly-added bot's data is never silently hidden pending a registry update."""
    meta = BOT_META.get(bot_name)
    if meta is None:
        return True
    return meta["status"] != "retired" and meta.get("workspace", WORKSPACE) == WORKSPACE
