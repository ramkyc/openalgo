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
    (this repo, "CS") when omitted. Only differs for a bot that migrated to
    another instance.

workspace_status (optional):
    For a bot that runs simultaneously in more than one instance with a
    DIFFERENT status in each (e.g. banknifty_bb_opening_candle_bot: paper
    accumulation continues in CRK at N_LOTS=10 while a separate N_LOTS=1
    copy trades live on CS) — a {workspace: status} dict overriding the
    flat status/workspace pair above for that workspace only. Use
    status_in_workspace() to read the effective status; every bot without
    this key keeps behaving exactly as the flat status/workspace pair says.

paused (optional):
    True if this bot's live/paper status above is still accurate but its
    launcher entry is currently commented out / not being spawned (e.g. a
    live bot paused after a losing streak, kept registered so its history
    stays visible). See paused_reason / paused_date.

Bots with zero rows in performance.db (killed before or without ever logging
a trade — e.g. bollinger_options_bot, candle_breaker_bot, orb_champion_15m_bot,
daily_sniper_bot, bb_paper_bot, bb_5m_scanner, tick_stasher, supertrend_5s_bot)
are intentionally omitted — they can never appear in a report keyed off the
trades table, so there is nothing to register.
"""

from __future__ import annotations

WORKSPACE = "CS"

BOT_REGISTRY: list[dict] = [
    # ── Live (real money) ────────────────────────────────────────────────────
    {"bot": "htf_po3_bot", "label": "HTF PO3 Bot", "type": "Options", "universe": "NIFTY/BANKNIFTY",
     "status": "live", "workspace": "CS",
     "reason": "Migrated to live deployment on fyers_cs", "status_date": "2026-07-11",
     "paused": True, "paused_reason": "User directive after ~₹1.82L cumulative loss across all 9 live trades to date, 0 wins",
     "paused_date": "2026-07-16"},

    # ── Paper (Stage 11 accumulation) ────────────────────────────────────────
    {"bot": "nifty_trend_seller_bot",      "label": "Nifty Trend Seller",          "type": "Options", "universe": "NIFTY",            "status": "paper",
     "reason": "Reconfigured to short-only (research-optimal ADX>25/RSI<50/ADX-D 7b) after fleet review — combined long+short was flat (-₹2,210/41 trades); own research flagged the long leg as weak IS/OOS",
     "status_date": "2026-08-31"},
    {"bot": "banknifty_bb_opening_candle_bot", "label": "BNF BB Opening Candle", "type": "Options", "universe": "BANKNIFTY/SENSEX",
     "status": "live", "workspace": "CS",
     "workspace_status": {"CRK": "paper", "CS": "live"},
     "reason": "Live on fyers_cs (1 lot) since 2026-07-16, alongside CRK paper accumulation (10 lots) that continued until CRK's decommissioning/consolidation into this instance on 2026-09-26 — workspace_status kept as historical record of the dual-instance period",
     "status_date": "2026-07-16",
     "paused": True, "paused_reason": "User directive (fyers_cs instance) — kept commented out through the 2026-09-26 consolidation pending explicit re-enable",
     "paused_date": "2026-08-11"},
    {"bot": "nifty_bb_overbought_bot",     "label": "Nifty BB Overbought",         "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "nifty_macd_map_bot",          "label": "NIFTY MACD Map",              "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "banknifty_iron_fly_monthly_bot", "label": "BANKNIFTY Iron Fly Monthly", "type": "Options", "universe": "BANKNIFTY",      "status": "paper"},
    {"bot": "NTS_OBI",                     "label": "NTS + OBI Gate",              "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "banknifty_trend_pullback_positional_bot", "label": "BNF Trend Pullback Positional", "type": "Options", "universe": "BANKNIFTY", "status": "paper"},
    {"bot": "sensex_ema_spread_bot",       "label": "SENSEX EMA Spread",           "type": "Options", "universe": "SENSEX",           "status": "paper"},
    {"bot": "bb_mean_reversion_bot",       "label": "BB Mean Reversion",           "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "nifty_ma_cross_seller_bot",   "label": "Nifty MA Cross Seller",       "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "nifty_gex_ict_v2_bot",        "label": "Nifty GEX ICT V2",            "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "nifty_atm_straddle_scalp_bot", "label": "NIFTY ATM Straddle Scalp",   "type": "Options", "universe": "NIFTY",            "status": "paper"},
    {"bot": "atm_poc_reversion_bot",       "label": "ATM POC Reversion",           "type": "Options", "universe": "NIFTY",            "status": "paper"},

    # ── Retired ───────────────────────────────────────────────────────────────
    {"bot": "banknifty_bb_options_bot", "label": "BANKNIFTY BB Options", "type": "Options", "universe": "BANKNIFTY",
     "status": "retired", "reason": "No longer needed (paused since 2026-07-29 after a trade needed manual broker intervention)",
     "status_date": "2026-09-26"},
    {"bot": "preopen_gap_fade_bot", "label": "Pre-Open Gap Fade", "type": "Equity", "universe": "NIFTY50",
     "status": "retired", "reason": "No post-cost edge on equity intraday", "status_date": "2026-06-24"},
    {"bot": "equity_obi", "label": "Equity OBI Bot", "type": "Equity", "universe": "RELIANCE/HDFCBANK",
     "status": "retired", "reason": "WR 38.3%, P&L -4,122 over 149 trades / 23 sessions", "status_date": "2026-06-19"},
    {"bot": "gap_fade_eod_bot", "label": "Gap Fade EOD", "type": "Equity", "universe": "NIFTY50",
     "status": "retired", "reason": "WR 25%, P&L -7,739 over 4 trades", "status_date": "2026-06-04"},
    {"bot": "ema_swing_scanner", "label": "EMA Swing Scanner", "type": "Equity", "universe": "NIFTY50",
     "status": "retired", "reason": "WR 0%, P&L -8,820 over 2 trades - no research study", "status_date": "2026-06-04"},
    {"bot": "sensex_iron_fly_weekly_bot", "label": "SENSEX Iron Fly Weekly", "type": "Options", "universe": "SENSEX",
     "status": "retired", "reason": "2026-10-03 re-run on repaired data: edge was a data artefact, all re-optimised configs fail OOS (Sharpe -0.56..-2.38)", "status_date": "2026-10-03"},
    {"bot": "iron_fly_weekly_bot", "label": "Iron Fly Weekly (legacy)", "type": "Options", "universe": "NIFTY/SENSEX",
     "status": "retired", "reason": "Legacy pre-split name, superseded by nifty_iron_fly_weekly_bot / sensex_iron_fly_weekly_bot", "status_date": None},
    {"bot": "ha_options_bot", "label": "HA Options Bot", "type": "Options", "universe": "NIFTY/BNF/SENSEX",
     "status": "retired", "reason": "Failed Stage 11 paper-trading gate", "status_date": "2026-07-10"},
    {"bot": "flat_blue_line_monthly_bot",  "label": "Flat Blue Line Monthly",      "type": "Options", "universe": "NIFTY+BANKNIFTY",
     "status": "retired", "reason": "Worst avg loss in the fleet: -₹53,467/trade, -₹2,13,868 over just 4 trades", "status_date": "2026-08-31"},
    {"bot": "sensex_trend_seller_bot",     "label": "SENSEX Trend Seller",         "type": "Options", "universe": "SENSEX",
     "status": "retired", "reason": "Stage 11 gate essentially complete (19/20); -₹1,68,010 over 19 trades incl. one -₹1,36,410 tail-risk blowup", "status_date": "2026-08-31"},
    {"bot": "nifty_iron_fly_weekly_bot",   "label": "NIFTY Iron Fly Weekly",       "type": "Options", "universe": "NIFTY",
     "status": "retired", "reason": "0% win rate over 5 trades, -₹1,52,652 (-₹30,530/trade avg)", "status_date": "2026-08-31"},
    {"bot": "banknifty_ema_spread_bot",    "label": "BANKNIFTY EMA Spread",        "type": "Options", "universe": "BANKNIFTY",
     "status": "retired", "reason": "Stage 11 gate passed (21/20) but lifetime net just +₹3,330 over 29 trades (31% WR); last 15 consecutive trades bled -₹13,470, nearly all signal_reversal exits — edge decayed to noise post-gate", "status_date": "2026-09-04"},
    {"bot": "macd_m2_sell_options_bot",    "label": "MACD M2 Sell Options",        "type": "Options", "universe": "NIFTY/BANKNIFTY",
     "status": "retired", "reason": "Stage 11 gate passed (20/20) but worst ₹ loser in the fleet: -₹1,04,165 over 29 trades", "status_date": "2026-08-31"},
    {"bot": "nifty_ema_spread_bot",        "label": "NIFTY EMA Spread",            "type": "Options", "universe": "NIFTY",
     "status": "retired", "reason": "Stage 11 gate passed (20/20); worst win rate of any gated bot (29%), -₹38,155 over 28 trades", "status_date": "2026-08-31"},
    {"bot": "nifty_eod_hold_bot",          "label": "NIFTY EOD Hold",              "type": "Options", "universe": "NIFTY",
     "status": "retired", "reason": "90 trading sessions since 2026-04-30 launch, ZERO trades ever placed, vs. OOS backtest expecting ~39 signals over that span — ADX≥25 condition fires constantly but candle+MACD/hist-slope+EMA-polarity confluence essentially never aligns live", "status_date": "2026-09-04"},
    {"bot": "vp_swing_screener",           "label": "VP Swing Screener",           "type": "Equity", "universe": "NIFTY50 subset (53 stocks)",
     "status": "retired", "reason": "121 scans since 2026-08-09, only 3 candidates ever flagged, zero converted to positions — backtest (3,907 trades/2.7yr) implies a live universe/implementation gap, never debugged", "status_date": "2026-09-04"},
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


def status_in_workspace(meta: dict, ws: str) -> str | None:
    """Effective status ("live"/"paper"/"retired") of a bot's registry entry
    in a specific workspace, or None if it doesn't run there at all. Honors
    the per-workspace `workspace_status` override for bots running
    simultaneously in more than one instance with a different status in
    each; every other bot falls back to the flat status/workspace pair."""
    ws_status = meta.get("workspace_status")
    if ws_status is not None:
        return ws_status.get(ws)
    return meta["status"] if meta.get("workspace", WORKSPACE) == ws else None
