# BANKNIFTY Trend Pullback Positional Bot

Stage: **11 (Paper Trading)** — not yet launched. Requires explicit go-ahead before starting the process, and separately, explicit Stage 12 sign-off before OpenAlgo's Sandbox/Analyze Mode toggle is switched to live.

## Research basis

Validated in `options_data/research/trend_pullback_positional_study/results_summary.md`.
Full 10-stage pipeline run (Stages 0, 1, 2b, 4, 5, 6, 7, 8, 10 — Stage 9 explicitly
skipped by user agreement, since the line was already BANKNIFTY-only going in).

Combined IS+OOS track record (145 trades, 2022-07-01 → 2026-07-02):

| Metric | Value |
|---|---|
| Win Rate | 60.0% |
| Sharpe | 1.850 |
| Net P&L | +₹1,013,689 |
| Max DD | -13.4% |

## Strategy summary

1. **Regime** (15-min BANKNIFTY index bars): EMA(9)/EMA(26) cross, aligned against
   SMA(50) basis (bullish cross only counts if close > SMA50 at the cross bar,
   and vice versa). A regime persists until the next aligned cross of either direction.
2. **Pullback pierce**: within an aligned regime, the first close-beyond-BB(20, 2σ)
   pierce *opposite* the regime direction.
3. **Confirmation** (1-min bars, within 30 minutes of the pierce bar's close):
   reversal candle pattern (engulfing/hammer/shooting star) confirming the pullback
   is over and the regime is resuming.
4. **Entry**: sell ATM option in the regime's continuation direction (SELL ATM CE
   for a short/bearish regime, SELL ATM PE for a long/bullish regime). No entries
   confirmed at/after 15:10 IST.
5. **Exit priority**: `regime_end → expiry_force_exit (15:14 IST on/after the
   contract's own expiry date) → target (keep 50% of credit) → SL (2.5x credit)`.

Product: **NRML** (multi-day hold — this bot does NOT flatten daily at 15:14/15:15;
that MIS-only rule from `_template_bot.py` does not apply here). Sizing: 10 lots.

## Known live-vs-backtest divergences

- **Exit-priority ordering is a best-effort approximation, not a strict guarantee.**
  The backtest checks `regime_end → expiry_force_exit → target → SL` sequentially
  on the *same* bar. Live, `regime_end` is detected only on 15-min bar-boundary
  closes (`_scan_regime`, driven by the WS 15-min boundary tracker), while
  `expiry_force_exit`/`target`/`SL` are checked independently every 30 seconds
  (`_exit_monitor_loop`). It is possible, in a fast-moving 15-minute window, for
  target or SL to fire a few seconds before a regime-end that would otherwise
  have taken exit priority in the backtest. An `asyncio.Lock` (`_exit_lock`)
  prevents a double-close race between the two loops, but does not reorder which
  one wins a genuine near-simultaneous trigger.
- **Cold-start / restart mid-regime**: on startup (or restart), the bot replays
  the current regime and looks for a pierce across the *entire* regime-to-date
  history. If a pierce is found whose 30-minute confirmation window has already
  elapsed, the bot does **not** retroactively enter — it marks
  `no_trade_this_regime = True`, logs a warning, and waits for the next regime.
  This is a deliberate conservative choice: a live bot must never chase a stale
  signal it only just noticed after a restart.
- **One trade per aligned-regime episode.** Once a trade is entered (or a setup
  is abandoned/missed) within a given regime, the bot does not look for a second
  pullback in the same still-open regime. This matches
  `find_first_pullback_setup()`'s "first pierce only" semantics in the backtest.

## Registration status

- [ ] `live_trading/start_all_bots.py` (BOTS + AVAILABLE_BOTS)
- [ ] `live_trading/streamlit_dashboard.py` (STATE_FILES, BOT_META, render_portfolio_snapshot())
- [ ] `live_trading/performance_review.py` (BOTS_TO_TRACK)
- [ ] `live_trading/active_trading_bots.md`
- [ ] `python3 live_trading/sanity_check.py` passes

## Stage 11 gate (before requesting Stage 12 sign-off)

- ≥ 20 trading sessions in Sandbox/Analyze Mode
- Win rate within ±10% of the 60.0% OOS backtest expectation
- No single session loss > 2× the expected SL-based loss (2.5x credit)
- Net P&L positive over the full 20-session window
- Summary report comparing paper results to the OOS backtest, prepared before requesting sign-off

## Running

```bash
cd ~/Developer/fyers_crk/openalgo
uv run python3 live_trading/banknifty_trend_pullback_positional_bot/banknifty_trend_pullback_positional_bot.py
```

State persists to `live_trading/logs/banknifty_trend_pullback_positional_state.json`
(survives restarts — includes any open position, current regime, and
pierce/confirmation progress). PID lock at
`live_trading/logs/banknifty_trend_pullback_positional_bot.pid`.
