# BB Mean Reversion Bot

**Source study:** `~/Developer/options_data/research/bb_mean_reversion_candle_study/`
**Deployed:** 2026-06-01
**Stage:** Stage 11 — Paper Trading (1 lot)

---

## Strategy

When BANKNIFTY forms a **red candle (close < open) whose high pierces the upper
Bollinger Band (20, 2σ)**, the spike has been rejected by close — a mean-reversion
signal. We **BUY an ATM PE** expecting the index to continue falling toward the
BB midband (SMA). This is a **long option / debit trade** — no margin required.

### Position type

**BUY ATM PE (long put, debit)** — NOT selling CE.  
Maximum loss = premium paid. No unlimited risk.

---

## Signal Gates (ALL five must pass)

| # | Gate | Condition |
|---|------|-----------|
| 1 | **Trend** | Previous-day BANKNIFTY close ≤ 20-day SMA of daily closes |
| 2 | **Trigger** | 1-min bar is red (close < open) AND high > upper BB(20,2) |
| 3 | **HTF** | trigger_close < open of the containing 5-min candle |
| 4 | **Natural R:R** | `(trigger_close − bb_mid) / (trigger_high − trigger_close) ≥ 1.25` |
| 5 | **DTE** | Monthly expiry DTE is NOT in range 8–14 days |

Gate 1 is checked once at session open. Gates 2–5 evaluated at each 1-min bar close.

---

## Champion Parameters

| Parameter | Value |
|-----------|-------|
| Instrument | BANKNIFTY |
| Option type | ATM PE (long put) |
| Expiry | Monthly (nearest ≥ 7 DTE, not in 8–14 DTE band) |
| Signal TF | 1 minute (BANKNIFTY index price) |
| HTF TF | 5 minutes |
| BB | Period 20, Std Dev 2.0 |
| Trend SMA | 20-day SMA of BANKNIFTY daily closes |
| Natural R:R gate | ≥ 1.25 |
| Stop loss | BANKNIFTY spot ≥ trigger_high (index level) |
| Target | 4.0 × risk (in option price terms) |
| Entry window | 09:30–14:45 IST |
| EOD exit | 15:15 IST |
| Lot size | 30 units/lot |
| Paper lots | **6 lots** (180 units — research-validated quantity) |
| Live lots | 6 lots (same — flip `BB_MEAN_REVERSION_PAPER_MODE=false` to go live) |

---

## Stage Gate History

| Stage | Gate | Result |
|-------|------|--------|
| 2 — IS Sweep | IS Sharpe > 1.0 | ✅ PASS (1.008, BANKNIFTY 1/5 RR4) |
| 2 — OOS Sweep | OOS Sharpe > 0.8 | ✅ PASS (2.425) |
| 3 — Regime Analysis | IS/OOS divergence explained | ✅ Bull trend = signal killer; trend filter added |
| 4 — Monte Carlo | Stability ≥ 60% | ✅ PASS (97.3%, 10,000 runs) |
| 5 — Bootstrap | Median Sharpe > 0.8 | ✅ PASS (1.51, monthly block resampling) |
| 9 — Expiry Segmentation | No segment > 60% P&L | ✅ PASS (WoM gate structural for monthly options) |
| 10 — Distance Analysis | Natural R:R sweet spot | ✅ PASS (threshold 1.25, avg ₹446/day at 1 lot) |
| **11 — Paper Trading** | ≥ 20 sessions, net P&L positive | ⏳ In progress |

---

## Expected Performance (OOS-derived, not a guarantee)

| Metric | Value |
|--------|-------|
| Win rate | ~28–32% |
| Expected P&L/trade (1 lot) | ~₹198 |
| Avg trades per active day | ~2.25 |
| Daily hit rate | ~41% |
| OOS Sharpe | 2.43 |
| Bootstrap median Sharpe | 1.51 |
| Typical max drawdown | ~10–13% |

---

## Files

| File | Purpose |
|------|---------|
| `bb_mean_reversion_bot.py` | Bot: signal detection, order placement, exit monitoring |
| `logs/bb_mean_reversion_bot.log` | Session log |
| `logs/bb_mean_reversion_state.json` | Dashboard heartbeat (written every 2s) |
| `logs/paper_trades.db` | Shared SQLite trade log (via shared/trade_logger.py) |

---

## Stage 11 Gate

Minimum **20 paper sessions** before requesting live sign-off.

Pass criteria:
- Net P&L positive over 20+ sessions
- Live win rate within ±10% of OOS win rate (28–32%)
- No single session loss > 3× average daily P&L at 1 lot
- Trend filter functioning correctly (no trades on Bull-trend days)

---

## Operating Notes

- **Bot is inactive on Bull-trend days** (trend gate fails) — this is correct behaviour,
  not an error. Expect 40–50% of trading days to be idle.
- **DTE exclusion**: skips monthly options with 8–14 days to expiry. Bot will log
  "DTE exclusion active" and wait for a suitable expiry.
- **One trade per session**: once a signal fires (or is passed by any gate), no new
  entries until the next session.
- **Product type**: MIS (intraday squareoff). EOD exit is unconditional at 15:15.
