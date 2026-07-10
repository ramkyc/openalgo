# HA Options Bot — Performance Analysis vs Research Expectations
**Date**: 2026-04-07
**Period covered**: 2026-03-24 → 2026-04-07 (7 trading days, 27 trades)
**Research baseline**: `options_data/research/ha_options_study/results_summary.md`

---

## 1. Headline Numbers

| Metric | Live (7 days) | OOS Research Expectation |
|--------|--------------|--------------------------|
| Total trades | 27 | ~35 (5/day avg) |
| Win rate | **18.5%** | **~43% (NIFTY/SENSEX 5-min), 53% (BANKNIFTY 15-min)** |
| Gross P&L | **−₹30,728** | +₹35,000/day expected |
| Avg P&L/trade | −₹1,138 | +₹1,400–₹2,600/trade |
| Wins/Losses | 5 wins, 22 losses | ~43 wins per 100 trades |
| Best trade | +₹2,998 | — |
| Worst trade | −₹6,991 (BANKNIFTY Apr 6) | — |

**Gate criteria check (Stage 11)**: Win rate of 18.5% vs expected 43% = deviation of −24.5 percentage points. Gate allows ±10% deviation. **Gate FAILED.**

---

## 2. Per-Instrument Breakdown

| Instrument | Trades | WR | Gross P&L | Expected WR |
|-----------|--------|-----|-----------|-------------|
| NIFTY | 13 | 15.4% | −₹12,177 | 43.4% |
| BANKNIFTY | 7 | 28.6% | −₹12,000 | 53.5% |
| SENSEX | 7 | 14.3% | −₹6,551 | 42.0% |

All three instruments are significantly below their OOS win-rate expectations.

---

## 3. Critical Bugs Found

### Bug #1 — HA Computation Mismatch (MOST CRITICAL)

**What the research does** (`run_oos_simulation.py`):
`compute_ha(o, h, l, c)` is called **once per day** with only that day's bars.
Each trading day starts with a fresh HA seed: `hao[0] = (o[0] + c[0]) / 2`.

**What the bot does** (`ha_options_bot.py` → `_compute_ha`):
The bot maintains a rolling `deque(maxlen=500)` that spans **multiple trading days**.
When `compute_ha_signal()` is called, it passes ALL accumulated bars (up to 5 days of history) to `_compute_ha`. The HA chain runs **continuously** across day boundaries — `hao[today_bar_1]` depends on `hac[yesterday_last_bar]`.

**Numerical proof** (computed above):
Using realistic NIFTY values, the first 3 bars of Day 2 showed **complete signal mismatch**:
- Research: `-1, -1, -1` (bearish → expects CE sell signal)
- Bot: `+1, +1, +1` (bullish → expects PE sell signal)

The docstring of `_compute_ha` claims "Resets HA seed at each new trading day", but the implementation does NOT. There is no `dates` column grouping or day-boundary reset in the actual loop.

**Impact**: The bot enters trades that the research never validated. Signals are fundamentally different from the research baseline, especially at session open when the HA state inherited from yesterday may contradict today's actual price action.

**Fix required**: Modify `_compute_ha` (or `compute_ha_signal`) to only use bars from **today's date** when computing the current signal, seeding from the first bar of each day.

---

### Bug #2 — `_trade_done_today` State Not Restored on Restart

**What happens**: `InstrumentState.__init__` always sets `self._trade_done_today = False`. The state JSON file is written (`STATE_FILE`) but the bot never reads it back on startup to restore `_trade_done_today`.

**Evidence**: March 27 log shows **8 trades** across 3 instruments (3 NIFTY, 3 SENSEX, 2 BANKNIFTY) in a single day — far exceeding the 1-trade-per-instrument-per-day limit. This happened because the bot was restarted mid-session after a trade was already taken, and `_trade_done_today` reset to `False`.

**Impact**: Multiple trades per instrument per day → more loss events, higher transaction costs, compounded drawdown.

**Fix required**: On startup, if `STATE_FILE` exists and `trade_date == today`, restore `_trade_done_today = True` for any instrument whose `trade_done_today` field is `True` in the JSON.

---

### Bug #3 — BANKNIFTY Lot Size Discrepancy

Research uses `lot=35` for BANKNIFTY (unchanged). Bot uses `default_lot=35` but then calls `_get_lot_size()` from the OpenAlgo token DB. Live trades show `qty=30` consistently (lot size 30 × 1 lot). The actual NSE BANKNIFTY lot size may have changed, or the DB lookup is returning a different value.

**Impact**: Bot is sized at 30/35 = 85.7% of research expectation on BANKNIFTY. Minor, but creates P&L comparability gap.

---

## 4. Market Context (Crucial)

The 7-day live period (Mar 24 – Apr 7) coincided with:

- **March 24–27**: Mild whipsaw; market consolidating
- **April 6**: US "Liberation Day" tariff shock — markets crashed violently. NIFTY fell ~700 pts. Bot generated bullish HA flip → sold PE → market crashed → SL hit. BANKNIFTY CE sold on bearish flip, but the crash was faster than SL could be managed.
- **April 7**: Continued crash / extreme volatility

The OOS research period (Jul 2025 – Mar 2026) had DIFFERENT volatility regimes. The bot is seeing its worst-case scenario: a sustained crash where HA keeps generating bullish flips as the market briefly bounces before resuming the fall ("dead cat bounces").

**Stage 8 finding**: Strategy works but performs better in high-volatility environments — HOWEVER, it underperforms when HA generates false flips due to extreme trending. A VIX > 22 filter (or NIFTY ADR > 2%) would filter out these extreme days.

---

## 5. Signal Analysis

### Exit reason breakdown:
| Exit Reason | Trades | Wins | Total P&L |
|------------|--------|------|-----------|
| Swing SL | 8 | 0 | −₹17,954 |
| HA Reversal | 18 | 4 | −₹15,772 |
| EOD 15:20 | 1 | 1 | +₹2,998 |

Observations:
- **SL hit = 100% loss rate**: All 8 SL-hit trades lost. SL was correctly calibrated but the market ran through it in every case.
- **HA Reversal = 22% win rate**: Many trades that exited on HA reversal were also losers — the trade direction was wrong from the start.
- **Average hold time**: Reversal exits 27.5 min, SL hits 14.4 min — very short holds signal poor signal quality.
- **13 of 27 trades (48%) held < 15 min**: Immediate reversals suggest the bot is entering on very early HA flips that do not sustain.

### Option type analysis:
- **PE sells: 21 trades, WR 14.3%** — predominantly betting market goes up, consistently wrong
- **CE sells: 6 trades, WR 33.3%** — better, but still below 43% expected
- The PE heavy bias coincides with the market's broader downtrend; brief upward HA flips were immediately reversed

---

## 6. Comparison with OOS Signal Logic

The OOS simulation (`run_oos_simulation.py`) enters at the **open of bar `ni = i+1`** (next bar after the flip bar). The bot enters at the **option LTP fetched after the flip bar closes** — which is approximately the same bar open, but the bot uses `get_option_ltp()` which fetches a live quote, not necessarily the bar open. During volatile opens this can be a worse fill.

The OOS simulation also uses `offset=0` (ATM strike computed as `round(ic/step)*step`). The bot also uses ATM. These match.

---

## 7. Assessment Summary

| Issue | Severity | Status |
|-------|---------|--------|
| HA continuous vs daily reset (signal mismatch) | 🔴 CRITICAL | **Bug — not matching research** |
| `_trade_done_today` not restored on restart | 🔴 CRITICAL | **Bug — multiple trades/day** |
| Adverse market period (tariff crash Apr 6–7) | 🟡 CONTEXTUAL | External — not a bug |
| BANKNIFTY lot size 30 vs 35 | 🟡 MINOR | Needs investigation |
| Vol filter not implemented | 🟡 MINOR | Research-recommended, not enforced |
| Sample size too small (27 trades / 7 days) | 🟡 NOTE | Cannot statistically conclude strategy is broken |

---

## 8. Recommendations

### Immediate fixes (before continuing paper trading):

**Fix 1: Daily HA reset** — modify `compute_ha_signal()` to only use bars from today:
```python
def compute_ha_signal(self) -> int | None:
    if len(self.bars) < 3:
        return None

    df = pd.DataFrame(list(self.bars))

    # KEY FIX: use only today's bars for HA computation
    today_str = datetime.now().strftime("%Y-%m-%d")
    today_bars = df[df["date"] == today_str]
    if len(today_bars) < 3:
        return None  # not enough bars today to compute HA

    df_today = today_bars.copy().reset_index(drop=True)
    df_today = _compute_ha(df_today)

    df_today["direction"] = np.where(df_today["ha_close"] > df_today["ha_open"], 1, -1)
    last_dir = int(df_today["direction"].iloc[-1])
    prev_dir = int(df_today["direction"].iloc[-2])
    self.ha_signal = last_dir
    if last_dir != prev_dir:
        return last_dir
    return 0
```

Similarly, `compute_swing_sl()` should also filter to today's bars only.

**Fix 2: State restoration** — on startup load `_trade_done_today` from state JSON:
```python
async def _restore_state(self) -> None:
    """Restore _trade_done_today from state JSON if today's session."""
    if not STATE_FILE.exists():
        return
    try:
        saved = json.loads(STATE_FILE.read_text())
        today = datetime.now().strftime("%Y-%m-%d")
        last_update = saved.get("last_update", "")
        if not last_update.startswith(today):
            return  # stale state from yesterday
        for sym, inst_state in saved.get("instruments", {}).items():
            if sym in self._inst:
                self._inst[sym]._trade_done_today = inst_state.get("trade_done_today", False)
                logger.info(f"  [{sym}] Restored trade_done_today={self._inst[sym]._trade_done_today}")
    except Exception as e:
        logger.warning(f"  State restore failed: {e}")
```
Call `await self._restore_state()` at the top of `main_loop()`.

### Consider (after fixes verified):

- **Implement the Stage 8 vol filter**: Skip SENSEX on days where NIFTY's prior-day range was < 0.83%. This is especially important given the current volatile environment.
- **BANKNIFTY lot size**: Investigate why `_get_lot_size()` is returning 30 instead of 35 for BANKNIFTY.
- **Extend paper trading window**: After applying fixes, restart the Stage 11 clock. The current 7-session window is confounded by both bugs and extreme market conditions — the results are not statistically meaningful.

### Do NOT promote to live until:
1. Both bugs above are fixed and verified
2. The bot runs for a minimum of 20 sessions (reset after fix date)
3. Win rate is within ±10% of the 43–53% OOS expectation across those 20 sessions

---

## 9. Key Question

Even after fixing both bugs, the 43–53% win rate from research may not manifest immediately in live because:
- The OOS period (Jul 2025 – Mar 2026) had different regime characteristics
- The April 2026 market (US tariff shock, extreme volatility, sustained downtrend) is a new regime not present in OOS data
- HA strategies struggle when market is in a sustained trend with brief counter-trend bounces that generate false HA flips

This is not evidence that the strategy is permanently broken — it may be a regime mismatch. The Stage 8 analysis showed the strategy DOES work in high-volatility environments, but only when the volatility is two-directional (whipsawing), not unidirectional (trending crash).

**Recommendation**: Apply the fixes, add the Stage 8 vol/trend filter, and resume paper trading for another 20 sessions before drawing conclusions.

---
*Analysis generated: 2026-04-07*
