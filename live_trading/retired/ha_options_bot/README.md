# HA Options Bot

**Strategy**: Heiken Ashi candle flip → sell ATM CE/PE on NIFTY (5-min), BANKNIFTY (15-min), SENSEX (5-min)
**Research**: `options_data/research/ha_options_study/` — all 10 pipeline stages PASS
**Mode**: Paper (Analyze mode) — sandbox, no real money

## Champion Configs (from results_summary.md Stage 3/4)

| Instrument | TF | Strike | OOS Sharpe | OOS WR |
|-----------|-----|--------|------------|--------|
| NIFTY | 5-min | ATM | 10.14 | 43.4% |
| BANKNIFTY | 15-min | ATM | 9.95 | 53.5% |
| SENSEX | 5-min | ATM | 9.85 | 42.0% |

Combined OOS Sharpe: 12.37 | MC stability: 100% of 10,000 bootstrap runs profitable

## Stage 11 — Paper Trading Log

### Run 1 (INVALIDATED — bugs found)
- **Period**: 2026-03-24 → 2026-04-07 (7 sessions)
- **Result**: WR 18.5%, Gross P&L −₹30,728
- **Invalidated because**:
  1. HA computation used multi-day continuous chain (not per-day reset as in research)
  2. `_trade_done_today` not restored on restart → multiple trades/instrument/day
  3. Apr 6–7 US tariff shock (confounding factor)
- **Analysis**: See `PERFORMANCE_ANALYSIS.md`

### Run 2 (ACTIVE)
- **Reset date**: 2026-04-07
- **Fixes applied**:
  - ✅ HA daily reset: `compute_ha_signal()` and `compute_swing_sl()` now use today's bars only
  - ✅ State restoration: `_restore_state()` reads `trade_done_today` and `vol_filter_pass` from JSON on restart
  - ✅ Stage 8 vol filter: skips SENSEX (and optionally all) on low-vol days (NIFTY range < 0.83%)
- **Sessions completed**: 0 / 20 required
- **Win rate so far**: —
- **Status**: 🔬 Paper trading — awaiting 20 sessions

## Stage 11 Gate Criteria (per CLAUDE.md)

| Gate | Threshold | Current |
|------|-----------|---------|
| Sessions completed | ≥ 20 | 0 |
| Win rate vs OOS | Within ±10% of 43–53% | — |
| No session loss > 2× ATR SL | ✓ | — |
| Net P&L over 20 sessions | Positive | — |

**Do NOT promote to live (Stage 13) until all gates pass AND Ramakrishna gives explicit sign-off.**

## Key Files

| File | Purpose |
|------|---------|
| `ha_options_bot.py` | Main bot |
| `PERFORMANCE_ANALYSIS.md` | Deep analysis of Run 1 underperformance |
| `logs/ha_options_bot.log` | Full trade log |
| `logs/ha_options_state.json` | Live state (updated every 2s) |
| DuckDB `paper_trades_options` | Centralized trade journal (all bots) |

## Running the Bot

```bash
cd ~/Developer/fyers_crk/openalgo
uv run python -m live_trading.ha_options_bot.ha_options_bot
```

Startup order: OpenAlgo app running → Fyers broker logged in → start bot.

## Vol Filter (Stage 8)

At session open the bot fetches yesterday's NIFTY daily range.
If range < 0.83% of close → SENSEX entries are blocked for the day (low-vol HA signals on SENSEX are noisy).
This is logged in `ha_options_state.json` as `vol_threshold_pct` and per-instrument `vol_filter_pass`.
