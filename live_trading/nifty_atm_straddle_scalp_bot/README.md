# NIFTY ATM Straddle Scalp Bot

**Status:** 📝 Paper trading (OpenAlgo Sandbox/Analyze mode)
**Champion config:** `10:30_sl20_tgt0.75`
**Research reference:** `options_data/research/atm_short_straddle_scalp_study/` — `FINDINGS.md`, `DECISIONS.md`. Verdict: **APPROVED FOR PAPER TRADING** (all stages 0–11 PASS after margin recalibration).

## Strategy

Sell an ATM NIFTY straddle (SELL 1 lot ATM CE + SELL 1 lot ATM PE, same strike) at a single
fixed daily entry time. Same-day only — no overnight carry.

- **Entry:** 10:30 IST (window 10:30–10:35). Resolve NIFTY spot → ATM strike → nearest weekly
  expiry with **no DTE floor** (`min_dte=0` — expiry-day trades included; Stage 10 expiry
  segmentation explicitly validated DTE=0 for this config). SELL ATM CE + SELL ATM PE, 10 lots
  each.
- **Per-leg stop-loss:** broker-side SL-M at `entry_premium × 1.20` (20% adverse move on that
  leg). Placed immediately after entry fill.
- **Breakeven trail:** once one leg's SL-M fills, the survivor's resting SL-M is cancelled and
  replaced at `trigger = survivor's own entry premium` (breakeven). Held from there for the
  straddle-level target or EOD.
- **Target:** combined straddle P&L ≥ 0.75% of margin utilized → close remaining leg(s) at
  market.
- **EOD:** unconditional close at **15:14 IST** (MIS sandbox hard cutoff — square-off fires at
  15:15; see `_eod_guard_loop()` for the redundant safety-net check from 15:05 onward).

## Margin / target formula (static, mirrors backtest exactly)

```
units     = lot_size * N_LOTS(10)
notional  = spot_at_entry * units
margin    = MARGIN_PCT_OF_NOTIONAL(0.1326) * notional
target_rs = TARGET_PCT(0.0075) * margin
```

This is a fixed constant, not a live margin API call — deliberate signal-parity choice with the
backtest (see `margin_calibration` D13 in the research study).

## Exit reason codes

Logged per leg, combined with `+` for the overall trade `exit_reason` (e.g. `SL+Breakeven`,
`SL+EOD`, `Target`, `EOD`):

| Code | Meaning |
|---|---|
| `SL` | Broker-side SL-M filled at 120% of that leg's entry premium |
| `Breakeven` (implicit, tracked via trailed SL trigger) | Survivor's stop trailed to its own entry after sibling stopped |
| `Target` | Combined P&L reached 0.75% of margin — both/remaining legs closed at market |
| `EOD` | 15:14 IST hard exit |
| `EOD_GUARD` | Safety-net exit fired by the redundant 15:05+ guard loop |

## Sizing

- 10 lots per leg (`N_LOTS = 10`)
- Lot size resolved per-contract via `database.token_db.get_symbol_info()`, falling back to 65
  (current NIFTY lot size) on lookup failure

## Architecture

- No `PAPER_MODE` flag — always fires real `placeorder()` calls. OpenAlgo's Sandbox/Analyze mode
  UI toggle is what simulates fills; going live means flipping that toggle, not this code.
- Polling-based, no WebSocket — single fixed daily entry time, 30s MTM/SL poll.
- `PollWatchdog` (`shared/poll_watchdog.py`) monitors quote-feed health on both legs.
- One combined `log_trade_to_db()` call per full exit event, `option_type="SHORT_STRADDLE"`
  (added to `trade_logger.py`'s `_MULTI_LEG_TYPES`), `net_pnl` left unset — dashboard falls back
  to `gross_pnl`.
- State file: `live_trading/logs/nifty_atm_straddle_scalp_state.json`
- PID lock: `live_trading/logs/nifty_atm_straddle_scalp_bot.pid`
- Log file: `live_trading/logs/nifty_atm_straddle_scalp_bot.log`

## Deployment stage

This is **Stage 11 (paper trading under OpenAlgo Sandbox mode)** — not Stage 13 (real-money live
trading), which requires a separate, explicit, future written sign-off (Stage 12) not yet given.
