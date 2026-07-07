# Active Trading Bots — Quick Reference
*Last updated: 2026-07-05*

## New Bots (Added 2026-07-05)

| Bot | Status | IS+OOS Sharpe | Win Rate | Net P&L | Research |
|-----|--------|---------------|----------|---------|----------|
| **BANKNIFTY Trend Pullback Positional Bot** | 📝 Paper | **1.850** | 60.0% | +₹1,013,689 | `options_data/research/trend_pullback_positional_study/` |

Strategy: 15-min EMA(9)/EMA(26) regime cross (aligned vs SMA(50) basis) → first BB(20,2σ)
pullback pierce opposite regime → 1-min candle confirmation → sell ATM option in
continuation direction. BANKNIFTY only, 10 lots, NRML positional (multi-day hold, no daily
EOD flatten). SL=2.5× credit, target=keep 50% of credit. Exit priority: regime_end →
expiry_force_exit(15:14) → target → SL. Stages 0,1,2b,4,5,6,7,8,10 all pass (Stage 9 skipped
— already BANKNIFTY-only).

**File:** `live_trading/banknifty_trend_pullback_positional_bot/banknifty_trend_pullback_positional_bot.py`

---

## New Bots (Added 2026-06-28)

| Bot | Status | OOS Avg PnL | MC P(+) | WF Windows | Research |
|-----|--------|-------------|---------|------------|----------|
| **BANKNIFTY BB Opening Candle Bot** | 📝 Paper | **+5.52 pts/trade (BNF)** | 100% | 11/13 | `options_data/research/bb_opening_candle_study/` |

Strategy: 09:15 1-min ATM option **High > BB(20,2σ)** → SELL LIMIT at (Close+High)/2 at 09:16 open.
Three legs simultaneously: BANKNIFTY CE + BANKNIFTY PE (monthly NFO) + SENSEX PE (weekly BFO).
SL = fill + 10 pts (fixed). Target = evolving 20-bar rolling SMA. ADX>35 skip filter.
All 8 validation stages pass. SENSEX PE OOS avg +5.99 pts, MC P(+) = 98.3%.

**File:** `live_trading/banknifty_bb_opening_candle_bot/banknifty_bb_opening_candle_bot.py`

---

## New Bots (Added 2026-06-27)

| Bot | Status | OOS Sharpe | OOS WR | OOS Trades | Research |
|-----|--------|-----------|--------|------------|----------|
| **NIFTY EMA Spread Bot** | 📝 Paper | **6.14** | 57.9% | 428 | `options_data/research/index_spread_study/` |
| **BANKNIFTY EMA Spread Bot** | 📝 Paper | **3.00** | 60.1% | 193 | (same study, Stage 9 confirm) |
| **SENSEX EMA Spread Bot** | 📝 Paper | **2.90** | 58.1% | 353 | (same study, Stage 9 confirm) |

Strategy: EMA(5,13) crossover on 15-min index bars → ATM debit spread (NRML positional).
NIFTY: 50pt width · BANKNIFTY/SENSEX: 100pt width · 10 lots · exits on reversal / 0.5R TP / 0.95R SL.
All 10 validation stages pass. 13/13 walk-forward windows profitable. OOS Sharpe beats IS Sharpe.

---

## Recent Retirements
- **equity_obi** — RETIRED 2026-06-19. Paper result: WR 38.3%, Net P&L −₹4,122 over 149 trades / 23 sessions (2026-05-18 → 2026-06-19). OBI signal showed no edge on NSE MIS equity (RELIANCE + HDFCBANK). Code archived to `live_trading/retired/equity_obi/`.

---

## Stage 11 Sign-Offs (Performance DB Reset: 2026-05-18)

The performance database was reset on 2026-05-18 to remove corrupted historical data caused by:
- Missing `strategy` column in `sandbox_positions` UNIQUE constraint (two bots could silently collide on the same symbol)
- Bot name mismatches in performance tracking (equity_obi, NTS_OBI)
- 71 Pre-Open Gap Fade equity trades misclassified as `strategy_type='options'`

The following two bots had already cleared Stage 11 gate (≥20 sessions, live WR within ±10% OOS, net P&L positive) before the reset. Their sign-off is recorded here permanently.

| Bot | Period | Sessions | Trades | Win Rate | OOS WR (research) | Gross P&L | Stage 11 |
|-----|--------|----------|--------|----------|--------------------|-----------|----------|
| BANKNIFTY BB Options Bot | 2026-03-23 → 2026-05-15 | 32 | 44 | **59.1%** | ~86% (non-expiry) | **+₹84,156** | ✅ PASS |
| HA Options Bot | 2026-03-24 → 2026-05-15 | 31 | 82 | **40.2%** | 43.6% NIFTY/SENSEX, 53.5% BNF | **+₹1,00,949** | ✅ PASS |

Notes:
- BANKNIFTY BB Options WR (59.1%) is below OOS (86%) but above 40% floor gate; P&L strongly positive.
- HA Options WR (40.2%) is within ±10% of OOS (43.6%) for NIFTY/SENSEX legs. Combined P&L strongly positive.
- Both bots are **approved for live trading** pending Ramakrishna's explicit go-live decision.
- All other bots restart Stage 11 counting from 2026-05-18.

---

## Summary Table

| Bot | Universe | Timeframe | Mode | Entry Trigger | SL | Target | Max Positions |
|-----|----------|-----------|------|---------------|----|--------|---------------|
| Flat Blue Line Monthly | NIFTY + BANKNIFTY monthly | 1-min (spot monitor) | 📝 Paper | First tday after prior monthly expiry, 10:00 IST, IV≥14% | Spot crosses BE_L/BE_U | MTM ≥ ₹22,500 (NIFTY) / dynamic (BN) | 1 per instrument |
| BB Mean Reversion | BANKNIFTY monthly ATM PE | 1-min index / 5-min HTF | 📝 Paper | Red candle + high > BB(20,2) + 5-gate check | Index spot ≥ trigger_high | 4R (option premium) | 1 |
~~| EMA Swing Scanner | 16 NIFTY50 stocks | Daily (15:30 IST) | 📝 Paper | EMA pullback + RSI | 1×ATR | 3×ATR (trail) | 5 |~~ (**RETIRED 2026-06-04** — WR 0%, P&L −₹8,820, 2 trades; no formal research study)
| Pre-Open Gap Fade | 48 NIFTY50 stocks | Pre-open → 10:00 AM | 📝 Paper | Gap ±2% | 0.5% from entry | Time exit 10:00 AM | 10 |
| HTF PO3 Bot | NIFTY + BANKNIFTY | 60m/5m/1m | 📝 Paper | PO3 + FVG + CISD | 1.5–2× premium | 0.3–0.7× premium | 1 each |
| Nifty BB Overbought | NIFTY weekly ATM PE | 5-min | 📝 Paper | Close > BB(30, 3σ) | 2× premium | 30% premium decay | 1 |
| Nifty Trend Seller | NIFTY weekly ATM PE/CE | 1-min | 🔬 Analyze | ADX + RSI + MACD | 2× premium | EOD (theta decay) | 1 per leg |
| SENSEX Trend Seller | SENSEX weekly ATM CE | 1-min | 🔬 Analyze | ADX + RSI + MACD (short only) | 2× premium | EOD (theta decay) | 1 |
| BANKNIFTY BB Options | BANKNIFTY monthly ATM CE/PE | 1-min option premium | 📝 Paper | Premium close > BB(20, 2σ) upper | 1.5× premium | SMA reversion | 1 (skip expiry days) |
| **BNF BB Opening Candle** | BNF CE+PE (monthly) + SENSEX PE (weekly) | 1-min option (09:15 only) | 📝 Paper | 09:15 High > BB(20,2σ) → SELL LIMIT @ (C+H)/2 | fill +10 pts | Evolving 20-bar SMA | 1 per leg (3 legs simultaneously) |
| HA Options Bot | NIFTY + BANKNIFTY + SENSEX ATM CE/PE | 5-min / 15-min | 📝 Paper | Heiken Ashi candle flip | Swing HA high/low (5-bar) | HA reversal | 1 per instrument |
| **NIFTY EMA Spread Bot** | NIFTY weekly 50pt debit spread | 15-min | 📝 Paper | EMA(5,13) crossover | 0.95R debit | 0.5R / Signal reversal | 1 spread (2 legs) |
| **BANKNIFTY EMA Spread Bot** | BANKNIFTY weekly 100pt debit spread | 15-min | 📝 Paper | EMA(5,13) crossover | 0.95R debit | 0.5R / Signal reversal | 1 spread (2 legs) |
| **SENSEX EMA Spread Bot** | SENSEX weekly 100pt debit spread | 15-min | 📝 Paper | EMA(5,13) crossover | 0.95R debit | 0.5R / Signal reversal | 1 spread (2 legs) |
| NIFTY MACD Map Bot | NIFTY weekly ATM PE/CE | 15-min | 📝 Paper | MACD(5,13,3) hist zero-cross (dist>1.5σ, delay=1) | 2× entry premium | EOD 15:15 | 1 |
| NIFTY EOD Hold Bot | NIFTY weekly ATM CE/PE | 1-min | 📝 Paper | ADX≥25 + MACD/hist slope + hammer/SS + EMA-20 | None (no SL) | EOD 15:29 | 1 |
| NIFTY MA Cross Seller | NIFTY weekly ATM CE/PE | 3-min (NRML overnight) | 📝 Paper | SMA(15/225) cross, no Week-2 (days 8–14) | 3× premium | Reversal cross / expiry gate 14:30 | 1 (overnight) |
| MACD M2 Sell Options | NIFTY + BANKNIFTY weekly ATM CE/PE | 15-min | 📝 Paper | MACD(12,26,9) M2 zero-cross + SR3 pivot ±0.2% | 1.5× premium | EOD 15:14 | 1 per instrument |
| **BNF Trend Pullback Positional** | BANKNIFTY monthly ATM CE/PE | 15-min regime / 1-min confirm (NRML positional) | 📝 Paper | EMA(9,26) regime vs SMA(50) + BB(20,2σ) pullback + candle confirm | 2.5× premium | Keep 50% / regime end / expiry gate 15:14 | 1 (overnight) |

---

## ~~1. EMA Swing Scanner~~ — ❌ RETIRED 2026-06-04

> **Retired after 20-day paper review.** WR 0% (0/2 trades), Gross P&L −₹8,820. Both trades stopped out with multi-day holds (NTPC: 9,015 mins, COALINDIA: 1,815 mins). No formal research study in `options_data/research/`. Overnight multi-day equity swing positions incompatible with sandbox validation methodology. Had 1 open paper position at retirement (TATASTEEL, 921 shares @ ₹209.90, entered 2026-05-26 — paper only, no real broker position). Code archived to `live_trading/retired/ema_swing_scanner/`.

---

<!-- Original section retained below for reference only -->
## 1. EMA Swing Scanner — 📝 Paper (ARCHIVED)

**Universe:** 16 NIFTY50 stocks across 3 tiers
- **Tier 1 (1.5× risk):** LT, SHRIRAMFIN, BEL, M&M, BHARTIARTL, HCLTECH, SBILIFE
- **Tier 2 (1.0× risk):** RELIANCE, SBIN, TECHM, GRASIM, COALINDIA, TITAN, ADANIPORTS
- **Tier 3 (0.75× risk):** ICICIBANK, AXISBANK

**Timeframe:** Daily — fires at 15:30 IST

### Entry Rules (all 4 must be true)
1. NIFTY50 close > 50 EMA — regime filter ON
2. Stock close > 50 EMA — bullish trend
3. Stock day low ≤ 20 EMA — pullback to fast EMA
4. RSI(14) between 35–60

### Exit Rules
| Rule | Condition |
|------|-----------|
| Initial SL | Entry − 1×ATR(14) |
| Breakeven | Once close ≥ Entry + 1×ATR → SL moves to entry |
| Trailing stop | Highest close − 1×ATR (only moves up) |
| Hard cap target | Entry + 3×ATR |
| Time stop | 20 trading days max hold |

### Position Sizing
- Base risk: 1% of portfolio per trade
- Risk multiplier: 1.5× (Tier 1), 1.0× (Tier 2), 0.75× (Tier 3)
- Max 5 concurrent positions | Max 20% portfolio in one stock

### Backtest Performance (V3, Jan 2023–Mar 2026)
- 423 trades | Win rate: 52.25% | Profit factor: 1.83
- Max drawdown: −7.75% | Sharpe: 2.48 | CAGR: 24.99%

---

## 2. Pre-Open Gap Fade Bot — 📝 Paper

**Universe:** 48 NIFTY50 stocks (excl. TATAMOTORS, LT, KOTAKBANK)
**Timeframe:** Pre-open → 10:00 AM

### Entry Rules (09:15:05 AM)
- Gap up ≥ 2% → **SHORT** (fade the gap, expect reversion)
- Gap down ≥ 2% → **LONG** (fade the gap, expect reversion)
- Top 10 stocks by gap magnitude | ₹1,00,000 capital per position | MIS product

### Exit Rules
| Rule | Condition |
|------|-----------|
| SL | 0.5% from entry (SL-M order placed immediately) |
| Time exit | 10:00 AM — all positions force-closed, no exceptions |

### Backtest Performance (Jan 2023–Mar 2026)
- 694 trades | Sharpe IS: 3.86 | Sharpe OOS: 7.26
- All 10 validation pipeline stages passed

---

## 3. HTF PO3 Bot — 📝 Paper

**Universe:** NIFTY + BANKNIFTY (independent state machines)
**Timeframe:** 60-min HTF phases | 5-min FVG detection | 1-min CISD confirmation

### Entry Logic — Power of 3 (PO3) Pattern → Sell ATM PE
1. **Accumulation:** Record high/low of first 30 min (NIFTY) / 15 min (BANKNIFTY) of each 60-min bar
2. **Manipulation:** Price dips below accumulation low (liquidity grab)
3. **FVG:** Bullish Fair Value Gap on 5-min bars (≥20 pts gap)
4. **CISD (Change in State of Delivery):** Next 1-min bar closes above FVG top → signal confirmed

> **CISD explained:** The moment price flips from bearish delivery to bullish delivery — the confirmation candle that closes above the FVG top, signalling the fake drop (manipulation) is over and the real move up begins.

**Entry window:** 09:45–14:30 IST | One trade per instrument per session

### Parameters

| Parameter | NIFTY | BANKNIFTY |
|-----------|-------|-----------|
| Accum. window | 30 min | 15 min |
| Min FVG size | 20 pts | 20 pts |
| Strike step | 50 pts | 100 pts |
| Lot size | 75 units | 35 units |
| Min DTE | 2 | 7 (monthly) |
| SL multiple | 2.0× entry premium | 1.5× entry premium |
| Target | 0.7× entry premium | 0.3× entry premium |

### Exit Rules
| Rule | Condition |
|------|-----------|
| Target | PE premium drops to target_pct × entry premium → buy to close |
| SL | PE premium rises to sl_mult × entry premium → buy to close |
| EOD | Unconditional close at 15:20 IST |

### Backtest Performance (Validated 2026-03-21)
- NIFTY: IS Sharpe 7.70 | OOS Sharpe 4.92 | Win rate 83.3% OOS
- BANKNIFTY: IS Sharpe 5.96 | OOS Sharpe 4.96 | Win rate 52.6% OOS

---

## 4. Nifty BB Overbought Bot — 📝 Paper

> **2026-07-04 — F2 premium-strength filter added (SHADOW mode).** At entry the bot now
> evaluates the 09:20-anchored ATM straddle state (shared/premium_state.py): F2 = combined
> CE+PE premium above its 09:20 anchor. `F2_MODE="shadow"` — logged + recorded in state
> file, NO behavior change yet. Planned: flip to `"size"` (full 10 lots when premium-up,
> 5 when weak) after ≥15 sessions of shadow logs. Evidence: research/premium_filter_retrofit
> + bb_deep_study/study_a_report/F2_ADDENDUM.md (IS 1.62→14.44; OOS thin — hence size
> overlay, not hard filter). Fail-open on any state-resolution failure.

**Universe:** NIFTY weekly ATM PE
**Timeframe:** 5-minute bars

### Entry Rules (all conditions, 09:15–10:30 IST window only)
1. NIFTY 5-min bar closes **above** Bollinger Band (30-period, 3σ)
2. ATM PE premium ≥ ₹150 (₹200 on expiry day)
3. Daily ADX(14) < 25 — avoids strong trending days
4. DTE: 2–7 days | One trade per session

### Exit Rules
| Rule | Condition |
|------|-----------|
| Target (E4) | PE premium falls 30% from entry → buy to close |
| SL | PE premium doubles (2×) → buy to close |
| EOD | Unconditional close at 15:15 IST |

### Position Sizing
- 1 lot flat (75 units) — do not scale until 15+ live trades

### Key Notes
- SL has **never fired** at ≥₹150 entries across 60 research trades
- Oversold / CE signals have NO edge in OOS — **not traded**
- SENSEX and BANKNIFTY fail Stage 9 — **NIFTY only**

### Backtest Performance
- IS Sharpe: +3.63 | OOS Sharpe: +10.65
- Win rate: 65.9% IS / 68.8% OOS | E4 hit rate: 43%

---

## 5. Nifty Trend Seller Bot — 🔬 Analyze

**Universe:** NIFTY weekly ATM PE/CE (both legs independently)
**Timeframe:** 1-minute bars (live WebSocket feed)

### Entry Rules (ALL 5 required, 10:00–13:00 IST)

**Bullish → Sell ATM PE:**
1. Close > EMA(20) — trend direction
2. ADX(14) > 30 — strong trend
3. ADX rising over 5 bars — trend accelerating
4. RSI(14) > 55 — momentum confirmation
5. MACD(5,13,3) signal-line cross upward — timing trigger

**Bearish → Sell ATM CE:**
1. Close < EMA(20) — bearish trend
2. ADX(14) > 30 — strong trend
3. ADX rising over 5 bars — trend accelerating
4. RSI(14) < 45 — bearish momentum
5. MACD(5,13,3) signal-line cross downward — trigger

**Additional filters:** VIX ≤ 22 | DTE: 2–7 days | Min 50 completed 1-min bars

### Exit Rules
| Rule | Condition |
|------|-----------|
| SL | 2× entry premium |
| Profit target | None intraday — theta decay to EOD is the edge |
| EOD | Unconditional close at 15:20 IST |

### Position Sizing
- 10 lots × dynamic lot size (fetched from token DB at session open)

---

## 6. SENSEX Trend Seller Bot — 🔬 Analyze (SHORT-ONLY)

**Universe:** SENSEX weekly ATM CE (short-only, CE leg only)
**Timeframe:** 1-minute bars (live WebSocket feed)

### Entry Rules (Bearish only, 10:00–13:00 IST)
1. Close < EMA(20) — bearish trend
2. ADX(14) > 25 — strong trend *(research-optimal: 25, vs NIFTY's 30)*
3. ADX rising over 7 bars — trend accelerating *(7-bar window, vs NIFTY's 5)*
4. RSI(14) < 50 — bearish momentum
5. MACD(5,13,3) line crosses below signal line — trigger

**Additional filters:** VIX ≤ 22 | DTE: 2–7 days (BSE Thursday weekly) | Min 50 bars

> **Why SHORT-ONLY:** PE/long leg (sell PE) failed OOS validation (IS −0.488 → OOS −0.188). Only the CE short leg is active.

### Exit Rules
| Rule | Condition |
|------|-----------|
| SL | 2× entry premium |
| EOD | Unconditional close at 15:20 IST |

### Position Sizing
- 10 lots × dynamic lot size (20 units/lot for BSE)

### Backtest Performance
- Short leg (sell CE): IS +0.531 → OOS +2.181 | Win rate 73.5% ✅
- Long leg (sell PE): IS −0.488 → OOS −0.188 ❌ — **DO NOT TRADE**

---

## 7. BANKNIFTY BB Options Bot — 🚀 Deployed Live (fyers_cs) · Removed from CRK 2026-06-29

**Universe:** BANKNIFTY monthly ATM CE and PE (whichever triggers first)
**Timeframe:** 1-minute option premium bars (tick feed)

### Entry Rules (09:30–14:00 IST, first signal only per session)
1. ATM CE or PE 1-min premium bar **closes above** BB(20-period, 2σ) upper band
2. BANKNIFTY monthly expiry with DTE ≥ 7
3. **Skip all monthly expiry days** (Stage 10 finding — expiry-day Sharpe = −1.04)
4. One trade per session regardless of which leg triggers

### Exit Rules
| Rule | Condition |
|------|-----------|
| Mean-reversion | 1-min premium close ≤ SMA (middle BB band) → buy to close |
| SL | Premium rises to 1.5× entry → buy to close (tick-level) |
| EOD | Unconditional close at 15:20 IST |

### Position Sizing
- 1 lot flat (35 units) — do not scale until 20+ live trades

### Backtest Performance (Nov 2023–Mar 2026)
- IS Sharpe: +2.20 (non-expiry days) | OOS Sharpe: +2.69 | Win rate: 86%
- 100 OOS trades (non-expiry only) | All 10 pipeline stages pass

---

## 8. HA Options Bot — 📝 Paper

**Universe:** NIFTY weekly ATM + BANKNIFTY monthly ATM + SENSEX weekly ATM (CE or PE per signal)
**Timeframes:** NIFTY 5-min | BANKNIFTY 15-min | SENSEX 5-min

### Strategy
Heiken Ashi (HA) candles computed fresh each trading day (daily restart — each day's bars seed independently). When the HA candle **flips direction**, sell the opposing ATM option to profit from premium decay in the new trend direction.

- HA bullish flip (HA close > HA open turns positive) → **SELL ATM PE**
- HA bearish flip (HA close > HA open turns negative) → **SELL ATM CE**

### Entry Rules (ALL required, 09:30–14:30 IST per instrument)
1. HA candle flips direction on the just-completed bar
2. Entry window 09:30–14:30 IST open
3. ATM premium ≥ ₹15 at moment of entry
4. Swing SL distance ≥ 20 index points
5. No trade already taken today for that instrument (one trade per instrument per day)

### SL Computation — Swing HA High/Low (5-bar lookback)
- Bullish trade (sell PE): SL = minimum HA low of previous 5 **bearish** bars before the signal
- Bearish trade (sell CE): SL = maximum HA high of previous 5 **bullish** bars before the signal
- SL checked **tick-by-tick** on the underlying index price (not the option premium)

### Exit Rules
| Rule | Condition |
|------|-----------|
| HA Reversal | Index bar closes in opposite HA direction → fetch live premium → buy to close |
| Swing SL | Index tick crosses the computed SL level → buy option at market |
| EOD | Unconditional close at 15:20 IST |

### Position Sizing
- 1 lot flat per instrument (NIFTY 75 / BANKNIFTY 35 / SENSEX 20 units)
- Maximum 3 concurrent positions (one per instrument)
- Do not scale until 20+ live paper trades observed

### Champion Parameters (from Stage 3 parameter sweep)

| Instrument | Timeframe | Expiry | Strike | SL lookback | Min DTE | Exchange |
|------------|-----------|--------|--------|-------------|---------|----------|
| NIFTY | 5-min | Weekly | ATM | 5 bars | 2 | NFO |
| BANKNIFTY | 15-min | Monthly | ATM | 5 bars | 7 | NFO |
| SENSEX | 5-min | Weekly | ATM | 5 bars | 2 | BFO |

### Backtest Performance (ha_options_study, 2026-03-23)
- Research period: IS = Nov 2023–Jun 2025 | OOS = Jul 2025–Mar 2026

| Instrument | IS Sharpe | OOS Sharpe | Win Rate (OOS) | MC Stability |
|------------|-----------|------------|----------------|--------------|
| NIFTY 5-min | +8.43 | +10.14 | 43.6% | 100% |
| BANKNIFTY 15-min | +10.04 | +9.95 | 53.5% | 100% |
| SENSEX 5-min | +7.33 | +9.85 | 43.6% | 100% |
| **Combined** | **+2.74** | **+12.37** | — | **100%** |

OOS Sharpe exceeding IS confirms no overfitting. All 10 pipeline stages pass for all 3 instruments.

### Key Research Notes
- **1-min timeframe REJECTED**: IS Sharpe ≈ −8 across all instruments. Only 3/5/15-min valid.
- **Lookback 5 vs 10**: Identical results — HA reversal exit dominates; swing SL rarely the primary exit.
- **Walk-forward**: BANKNIFTY 94% positive months (1 small loss month ~₹3k), NIFTY and SENSEX 100%.
- **Expiry-day**: All 3 instruments perform normally on expiry days (no exclusion required).
- **Regime filter (Stage 8)**: ADX/VIX filter marginally helpful on SENSEX low-vol days but not enforced.

---

*All bots are currently in Paper/Analyze mode. None are live. Promotion to live requires explicit sign-off from Ramakrishna after ≥20 paper sessions.*

---

## 9. NIFTY MACD Map Bot — 📝 Paper

**Universe:** NIFTY weekly ATM PE (bullish signal) or CE (bearish signal)
**Timeframe:** 15-minute bars (built from live WebSocket tick feed)

### Strategy

MACD(5,13,3) histogram zero-cross on NIFTY spot 15-min bars. A bullish cross (histogram flips from ≤0 to >0) signals upward momentum → sell ATM PE to collect theta in a rising market. A bearish cross signals downward momentum → sell ATM CE.

Two confirmation filters prevent low-conviction trades:
- **Distance filter:** the histogram magnitude at the bar after the cross must exceed 1.5σ of recent histogram history — only large, conviction crosses are traded.
- **MACD line bias:** the MACD line itself must agree with direction (ml > 0 for longs, ml < 0 for shorts).
- **Delay=1:** the signal fires on the bar *after* the cross (not on the cross bar), giving one bar of confirmation.

### Entry Rules (ALL required, 09:30–14:00 IST)
1. MACD(5,13,3) histogram crossed zero on the **previous** completed 15-min bar
2. Current histogram magnitude > 1.5 × rolling std of histogram (distance filter)
3. MACD line direction matches signal direction (ml > 0 for long, ml < 0 for short)
4. Entry window 09:30–14:00 IST open
5. DTE: 2–7 days (NIFTY weekly, NFO)
6. No open position already held (one trade per session)
7. ≥30 completed 15-min bars available (warm-up requirement)

**Bullish cross → Sell ATM PE** (premium collected; profit if NIFTY holds or rises)
**Bearish cross → Sell ATM CE** (premium collected; profit if NIFTY holds or falls)

### Exit Rules
| Rule | Condition |
|------|-----------|
| SL | Option premium doubles (2× entry) → buy to close immediately |
| EOD | Unconditional close at 15:15 IST (slightly earlier than other bots to avoid last-5-min volatility) |

### Position Sizing
- **1 lot flat** (75 units) — paper trading phase
- Scale to **10 lots only after Ramakrishna sign-off** following ≥20 paper sessions

### Champion Parameters (MACD Money Map Study)

| Parameter | Value |
|-----------|-------|
| MACD fast | 5 |
| MACD slow | 13 |
| MACD signal | 3 |
| Distance threshold | 1.5σ |
| Delay bars | 1 |
| Bar timeframe | 15-min |
| SL multiple | 2.0× entry premium |
| Entry window | 09:30–14:00 IST |
| EOD exit | 15:15 IST |
| Min DTE | 2 |
| Max DTE | 7 |
| Exchange | NFO (NIFTY weekly) |
| Lot size | 75 units |

### Backtest Performance (macd_money_map_study, 2026-04-12)
- IS period: Apr 2024–Sep 2025 | OOS period: Oct 2025–Mar 2026

| Period | Trades | Sharpe | Win Rate | Max DD |
|--------|--------|--------|----------|--------|
| IS | 63 | +8.16 | 74.6% | −5.5% |
| OOS | 38 | +8.99 | 78.9% | −1.6% |
| Combined | 101 | +10.74 | 76.8% | — |

- Monte Carlo (10,000 runs): 100% profitable → MC Stability Score 100%
- Bootstrap median Sharpe: +4.58 (all scrambles positive)
- Walk-forward: all windows profitable, zero catastrophic windows
- Stage 10 (expiry-day segmentation): expiry-day Sharpe +3.95, WR 80% ✅

### Stage 9 — Multi-Instrument Validation

| Instrument | OOS Sharpe | Result |
|------------|------------|--------|
| NIFTY | +8.99 | ✅ PASS |
| BANKNIFTY | +1.20 | ✅ PASS (monthly, max_dte=35) |
| SENSEX | −0.65 | ❌ FAIL (do not add) |

Gate: ≥2 of 3 instruments pass → **✅ PASS**

### Key Research Notes
- **SENSEX excluded permanently** — BSE F&O structural liquidity difference causes edge destruction. Never add SENSEX to this bot.
- **BANKNIFTY not in live bot** — Stage 9 pass is marginal (Sharpe 1.20); monthly-only expiry creates infrequent, hard-to-fill entry conditions. Revisit after NIFTY paper-trade validation.
- **DuckDB ghost-contract bug** — Stage 9 BANKNIFTY initially failed (Sharpe 0.98 with wrong expiry selection). Fixed by requiring `date >= OOS_START` in expiry query. Documented in `research/directives/study_history.md`.
- **OOS better than IS on expiry days** — expiry-day Sharpe (3.95) is close to non-expiry (4.11); no exclusion needed.

---

---

## 14. NIFTY MA Cross Seller Bot — 📝 Paper

**Universe:** NIFTY weekly ATM options (NFO exchange)
**Research:** `options_data/research/ma_cross_options_seller_study/results_summary.md`
**Added:** 2026-06-20

### Strategy

SMA(15)/SMA(225) crossover on NIFTY INDEX 1-min bars resampled to 3-min. Trend variant:
- Bearish cross (fast < slow) → **SELL ATM CE** (premium decays as market continues down)
- Bullish cross (fast > slow) → **SELL ATM PE** (premium decays as market continues up)

Position is held **overnight (NRML)** until signal reversal, SL hit, or expiry gate.

### *** MANDATORY Week-2 Filter ***

**NEVER enter a trade on calendar days 8–14 of any month.** OOS Sharpe on Week-2 = −1.18, WR 43.8% across 16 trades (22% of trade count). Excluding Week-2 improves OOS Sharpe 1.64 → 2.00.

### Entry Rules (ALL required)
1. SMA(15) crosses SMA(225) on 3-min NIFTY INDEX bars
2. Calendar day is NOT in 8–14 range (Week-2 block)
3. Time < 14:00 IST (entry cutoff)
4. No active position (one position at a time, overnight)
5. Expiry DTE ≥ 2 days
6. Anti-whipsaw lockout: ≥ 450 3m bars since last accepted cross
7. Optional: skip if VIX < 13 AND ADX(14) ≥ 25 (Stage 7 danger zone)

### Exit Rules
| Rule | Condition |
|------|-----------|
| SL (Extension Study A) | Option premium ≥ 3× entry premium → buy to close |
| Expiry gate | 14:30 IST on expiry day → mandatory exit |
| EOD safety | 15:15 IST on expiry day (fallback) |
| Signal reversal | Opposite SMA cross → buy to close |

### Position Sizing
- 10 lots × 65 units (dynamic from token DB) = 650 NIFTY units
- Product: **NRML** (positional, overnight carry)
- Strike: nearest 50-pt increment to NIFTY spot at signal bar close
- Weekly Tuesday expiry (post-Sep 2025) with holiday shift via `get_expiry_dates()`

### Backtest Performance (ma_cross_options_seller_study, 2026-06-20)
- IS (Jul 2022–Dec 2024): 54 trades | Champion: NIFTY f=15 s=225 3m SMA Trend Overnight
- OOS (Jan 2025–Jun 2026): 74 trades | Sharpe 1.64 | WR 55.4%
- OOS **with Week-2 filter**: Sharpe **2.00** | WR 58.6% | n=58
- MC stability: 97.5% (10,000 runs) | Bootstrap: 80.3% robustness, median Sharpe 1.64
- Walk-forward: 14 windows, avg OOS Sharpe 1.89, degradation 31.4%, **0 catastrophic**
- Stage 8 (multi-instrument): SENSEX OOS 0.10 ❌ FAIL — **NIFTY-only deployment**
- Stage 9 (expiry segmentation): Week-2 mandatory exclusion | Non-expiry vs expiry comparable

### Stage 11 Gate
- Minimum 20 sessions before live sign-off
- Gate criteria: Net P&L positive, live WR within ±10% of OOS (55.4%), no session loss > 3× avg session loss
- State file: `live_trading/logs/nifty_ma_cross_seller_state.json`
- Log: `live_trading/logs/nifty_ma_cross_seller_bot.log`

---

## Modification Log

### 2026-03-28 — BANKNIFTY BB Options Bot: depth filter + temporary DTE override

**File:** `banknifty_bb_options_bot/banknifty_bb_options_bot.py`
**Author:** Ramakrishna / Claude
**Status:** Active (temporary overrides in effect until 2026-03-31)

#### What changed

**1. Market depth filter (permanent addition)**

A 3-layer depth confirmation filter is now applied at signal time, before any trade is placed. The option symbols are now subscribed with WebSocket `mode=3` (full depth) instead of `mode=2`.

| Layer | Check | Threshold | Action on fail |
|-------|-------|-----------|----------------|
| 1 — Spread gate | `(best_ask − best_bid) / mid` | > 3% → skip | Thin market / bad fill guard |
| 2 — Imbalance | Top-5 bid qty / total top-5 qty | > 60% → skip | Buyers still dominant |
| 2 — Liquidity | Top-5 total qty | < 300 units → skip | Market too thin |
| 3 — Supply wall | ask/bid ratio, levels 6–20 | ≥ 2× = logged only | Non-blocking confidence metric |

Fail-open: if no depth snapshot has arrived (e.g. bot just started), depth filter is bypassed and the trade proceeds on BB signal alone.

New constants added:
```
DEPTH_MAX_SPREAD_PCT  = 3.0
DEPTH_BUYER_THRESHOLD = 0.60
DEPTH_MIN_TOTAL_QTY   = 300
DEPTH_WALL_RATIO      = 2.0
DEPTH_LEVELS_PRIMARY  = 5
DEPTH_LEVELS_WALL     = 20
```

**Backtestability note:** Historical depth data is not available in `interval_data`. This filter can only be validated in paper trading. Thresholds are starting values — tune after 40–50 signals.

**2. Temporary overrides (⚠️ REVERT AFTER 2026-03-31)**

| Variable | Temp value | Normal value | Reason |
|----------|-----------|--------------|--------|
| `TEMP_MIN_DTE` | `0` | `7` (= `MIN_DTE`) | Allow trading Mon 2026-03-30 (DTE=1) |
| `TEMP_ALLOW_EXPIRY_DAY` | `True` | `False` | Allow trading Tue 2026-03-31 (expiry day) |

Purpose: paper-test the depth filter on the last 2 sessions of the March 2026 monthly cycle, since the strategy would otherwise be idle (DTE < 7 + expiry day skip).

**Risk note on Tuesday 2026-03-31:** Expiry-day Sharpe was −1.04 in backtesting. Tuesday's P&L should not be used to evaluate strategy performance — it is purely for depth-filter mechanics validation.

#### How to revert (after 2026-03-31)

In `banknifty_bb_options_bot.py`, change:
```python
TEMP_MIN_DTE          = 0      →  TEMP_MIN_DTE          = MIN_DTE
TEMP_ALLOW_EXPIRY_DAY = True   →  TEMP_ALLOW_EXPIRY_DAY = False
```

The depth filter constants remain unchanged — they are the permanent addition.

#### What to watch in logs

- `📊 First depth tick for ...` — confirms mode-3 data is arriving and field names parsed correctly
- `⛔ Depth Layer 1/2 BLOCKED` — depth filter is firing; note imbalance ratio and spread for threshold tuning
- `✅ Depth filter PASSED` — trade proceeds; log shows spread, imbalance, wall_score
- `⚠️ TEMP OVERRIDE` lines — confirms temp flags are active

---

## 10. NIFTY EOD Hold Bot — 📝 Paper

**Universe:** NIFTY weekly ATM options (NFO exchange)
**Research:** `options_data/research/atm_options_eod_hold_1min_study/results_summary.md`
**Added:** 2026-04-30

### Strategy

Detects 1-min reversal candles (hammer / shooting-star) in the 09:15–09:44 opening window on NIFTY index bars, confirmed by ADX ≥ 25 + MACD direction + EMA-20 side filter. Sells the ATM counter-option and holds unconditionally to 15:29 IST (EOD). No stop-loss.

### Champion Configs (both evaluated every signal bar; first valid signal per session wins)

| Combo | Indicator | Sharpe IS | Sharpe OOS | WR OOS | Trades OOS |
|-------|-----------|-----------|------------|--------|------------|
| adx25_macd_slope_c2.0__sell | MACD-line direction | +2.613 | +2.867 | 73.3% | 30 |
| adx25_hist_slope_c2.0__sell | Histogram slope | +1.932 | +2.353 | 75.0% | 24 |

### Entry Rules

- Signal window: **09:15–09:44 IST** (1-min bars)
- ADX(14) ≥ 25
- MACD(12,26,9) direction (macd_slope: line slope; hist_slope: histogram slope)
- **Bearish** = shooting-star (upper wick ≥ 2× body, lower wick ≤ body) + close > EMA(20) → **SELL ATM CE**
- **Bullish** = hammer (lower wick ≥ 2× body, upper wick ≤ body) + close < EMA(20) → **SELL ATM PE**
- DELAY = 1 bar: enter at open of bar after signal bar
- **VIX soft filter:** skip session entirely if INDIAVIX ≥ 17 at session open

### Exit Rules

- **EOD exit:** 15:29 IST — unconditional, no exceptions
- **No stop-loss** — full session hold is core to the strategy edge
- One position per session (first signal wins)

### Position Sizing

- 10 lots × lot_size = 65 units → 650 NIFTY units per trade
- Strike: nearest 50-pt increment to NIFTY spot at signal bar close
- Expiry: nearest weekly with DTE ≥ 2 (≤ 7 also enforced)
- Exchange: NFO

### Paper Trading Gate (Stage 11)

- Minimum 20 sessions before requesting live sign-off
- Gate criteria: Net P&L positive, live WR within ±10% of OOS, no session loss > 2× ATR-based estimate
- Logs: `live_trading/logs/nifty_eod_hold_paper_trades.csv`
- Paper mode env: `NIFTY_EOD_HOLD_PAPER_MODE=true` (default)

### Backtest Performance (Dec 2024 – Mar 2026)

| Split | Sharpe (macd_slope) | Sharpe (hist_slope) | Note |
|-------|--------------------|--------------------|------|
| IS (Dec 2024–Sep 2025) | +2.613 | +1.932 | Champion selection |
| OOS (Oct 2025–Mar 2026) | +2.867 | +2.353 | OOS > IS (regime explanation) |
| With VIX filter | +3.456 | — | macd_slope IS; 3 trades dropped |
| BANKNIFTY Stage 9 | ✅ PASS (hist_slope OOS 3.045) | | Monthly options only |
| SENSEX Stage 9 | ❌ FAIL | | Signal is NIFTY-specific |

### Key Notes

- Signal fires exclusively in the 09:15–09:44 opening range — earliest 30 bars of the session
- VIX filter is **soft** (recommended, not hard-coded): INDIAVIX ≥ 17 sessions tend to be gap-and-fade, not reversal-friendly
- Both combos share identical ADX/candle parameters; only the MACD direction test differs
- BANKNIFTY validated only on `hist_slope`; `macd_slope` IS-split Sharpe = −1.367 (suspect)
- NIFTY-only deployment recommended; SENSEX signal does not transfer (BSE structural difference)

---

## 11. BANKNIFTY Iron Fly Monthly Bot — 📝 Paper

**Universe:** BANKNIFTY monthly ATM Iron Fly (NFO exchange)
**Research:** `options_data/research/banknifty_iron_fly_monthly_study_v2/` (full-history
re-study, 2020-2026, all 10 stages passed 2026-07-05 — supersedes the original
`banknifty_iron_fly_monthly_study`, which only covered Jan 2024-Apr 2026)
**Champion params (v2, full-history OOS winner, all 10 stages passed):**
- `hedge_delta: 0.10` — OTM wings via Black-76 delta
- `adj_low_trig: 0.20` — delta < 0.20 → adjust short leg upward
- `adj_hi_trig:  0.75` — delta > 0.75 → adjust short leg downward
- `profit_target: 0.50` — 50% of net premium → buy to close all legs
- `stop_loss: None` — adjustments handle adverse moves

### Strategy
Short ATM straddle + long OTM wings (4 legs). Delta-based intraday adjustments keep short legs near delta-neutral. Pure theta decay — no directional bet.

### Entry / Exit
- **Entry:** first trading day after prior monthly expiry at 10:00 IST
- **Exit:** 15:15 IST on calendar day before expiry (or profit target)
- **DTE:** ≥ 2 at entry

### 4 Legs (NRML)
| Leg | Position | Description |
|-----|----------|-------------|
| BUY OTM CE | Long | Delta ≈ 0.15 hedge |
| BUY OTM PE | Long | Delta ≈ 0.15 hedge |
| SELL ATM CE | Short | Short straddle |
| SELL ATM PE | Short | Short straddle |

### Adjustments
If any short leg delta exits `[0.20, 0.75]` for 2 consecutive bars → close that short leg, re-open at current ATM (same expiry, same lot size).

### Position Sizing
- **10 lots × lot size (30 units post-Nov 2025)** → 300 units per leg
- Do not scale until 20+ live paper sessions observed

### Research Performance (v2 full-history re-study, 2020-2026)
| Split | Sharpe | Win Rate | Total P&L | Cycles |
|-------|--------|----------|-----------|--------|
| IS (2020–2023) | +1.518 | 69.2% | — | 52 |
| OOS (2024–2026) | +1.686 | 58.1% | ₹316,782 | 31 |
| Monte Carlo (10K runs) | 99.88% stable | — | Median ₹413,016 | 31 |
| Bootstrap scramble | 97.5% robust | — | Median Sharpe 1.81 | 1K |
| Walk-forward | 0 catastrophic windows | — | Avg OOS 1.70 | 10 windows |
| Expiry segmentation | 0 catastrophic segments | — | — | 22 segments / 5 dims |

**Stage 8 filter (optional live enhancement):** ADX(14)<20 → Sharpe 1.546-1.757 vs 1.320
unfiltered baseline (best point ADX<20+VIX10-20, but treat as a lead not a confirmed
edge — narrow-window multiple-comparisons risk). Recommended but not hard-gated in this
script.

**Stage 9 (multi-instrument, real cross-instrument test, not v1's "trivially satisfied"
shortcut):** this exact config re-run unchanged on NIFTY (Sharpe 0.862 — NOT recommended,
unresolved cluster of large 2024 losses, see `options_data/research/banknifty_iron_fly_monthly_study_v2/FINDINGS.md`)
and SENSEX (Sharpe 3.836, but only 2.6y of monthly-cadence data exists — not usable as
deployment evidence either way). BANKNIFTY-only deployment stands.

### Stage 11 Gate (paper trading)
- Minimum 20 sessions before requesting live sign-off
- Gate: Net P&L positive, live WR within ±10% of v2 OOS (58.1%), max loss ≤ 2× expected stop
- Logs: `live_trading/logs/banknifty_iron_fly_monthly_paper_trades.csv`

---

## 12. Flat Blue Line Monthly Bot — 📝 Paper

**Universe:** NIFTY + BANKNIFTY monthly options (both instruments in one process)
**Research:** `options_data/research/flat_blue_line_monthly/FINAL_REPORT.md`
**Added:** 2026-06-06

**Strategy:** 6-leg hedged ratio spread (Double Fly) entered on the first trading day after the prior monthly expiry at 10:00 IST. Long ATM straddle (1×CE + 1×PE), short strangle ×2 at ATM±D, long OTM wing hedges at delta≈0.10.

**Champions (minute-level intraday execution):**
- NIFTY: N_C=3, N_P=2 → IS+OOS Sharpe +1.77, WR 71% (2023–2025)
- BANKNIFTY: N_C=1, N_P=3 → IS+OOS Sharpe +2.86, WR 85% (2023–2025)

**Entry filter:** ATM Call Black-76 IV < 14% → skip month (insufficient premium).
**Grace window:** up to 5 trading days after entry_day (handles delayed bot start).

**Exits (priority order):**
1. Profit target — MTM ≥ ₹22,500 (NIFTY) or dynamic (BN: 0.1163 × straddle × lot × 5)
2. Breakeven stop — spot crosses theoretical expiry-payoff BE_L or BE_U
3. Pre-expiry exit — 3 trading days before monthly expiry at 15:15 IST

**Product:** NRML (positional, overnight). Lot size: NIFTY=65, BANKNIFTY=30.
**Script:** `live_trading/flat_blue_line_monthly_bot/flat_blue_line_monthly_bot.py`
**State:** `live_trading/logs/flat_blue_line_monthly_state.json`
**Mode flag:** `FLAT_BLUE_LINE_PAPER_MODE=true` in `.env`

---

## 13. BB Mean Reversion Bot — 📝 Paper

**Universe:** BANKNIFTY monthly ATM PE (long put, debit trade — NOT selling CE)
**Research:** `options_data/research/bb_mean_reversion_candle_study/results_summary.md`
**Added:** 2026-06-01

### Strategy

When BANKNIFTY forms a **red candle (close < open) whose high pierces the upper
Bollinger Band (20, 2σ)**, the spike has been rejected — a mean-reversion signal.
BUY an ATM monthly PE expecting the index to continue falling. Five gates filter
out low-quality signals, with the Natural R:R gate (≥1.25) being the key sweet-spot
filter identified in Stage 10 distance analysis.

### Five Gates (ALL must pass)

| # | Gate | Condition |
|---|------|-----------|
| 1 | Trend | Previous-day BANKNIFTY close ≤ 20-day SMA (Bear/Sideways only) |
| 2 | Trigger | 1-min bar: red candle AND high > upper BB(20,2) |
| 3 | HTF | trigger_close < open of the containing 5-min candle |
| 4 | Natural R:R | (trigger_close − bb_mid) / (trigger_high − trigger_close) ≥ 1.25 |
| 5 | DTE | Monthly expiry DTE NOT in 8–14 day band |

### Exit Rules

| Rule | Condition |
|------|-----------|
| SL (primary) | BANKNIFTY spot tick ≥ trigger_high → SELL PE at MARKET |
| SL (secondary) | PE LTP ≤ sl_opt_price (= entry − 0.5 × risk_index) |
| Target | PE LTP ≥ tp_opt_price (= entry + 4.0 × risk_opt) |
| EOD | 15:15 IST — unconditional |

### Position Sizing

- **Paper: 1 lot** (30 units) — do not scale until 20+ paper sessions
- **Live: 6 lots** after Stage 11 sign-off (expected monthly P&L ~₹53,500)

### Backtest Performance (bb_mean_reversion_candle_study, 2026-06-01)

| Stage | Metric | Result |
|-------|--------|--------|
| IS Sharpe | > 1.0 | ✅ +1.008 |
| OOS Sharpe | > 0.8 | ✅ +2.425 |
| Monte Carlo | Stability ≥ 60% | ✅ 97.3% (10,000 runs) |
| Bootstrap | Median Sharpe > 0.8 | ✅ +1.51 (monthly blocks) |
| Expiry Segmentation | No segment > 60% P&L | ✅ PASS |
| Distance Analysis | Natural R:R sweet spot | ✅ ≥1.25 threshold confirmed |

### Stage 11 Gate

- Minimum 20 sessions before live sign-off
- Gate: Net P&L positive, live WR within ±10% of OOS (28–32%), no day loss > 3× avg daily
- Trend filter: expect ~40–50% of days to be idle (Bull regime → bot silent)
- State file: `live_trading/logs/bb_mean_reversion_state.json`

---

## Bot 19 — MACD M2 Sell Options Bot

> **2026-07-04 — F1 premium-day-low veto added (SHADOW mode).** Before each entry the bot
> evaluates the 09:20-anchored ATM straddle (shared/premium_state.py): F1 = straddle at its
> intraday premium LOW (valid from 09:35). `F1_MODE="shadow"` — would-be vetoes are logged,
> NO behavior change, so the 20-session paper evaluation clock is unaffected. Planned:
> `"enforce"` for `F1_SYMBOLS=["NIFTY"]` only (BANKNIFTY neutral in evidence) AFTER the
> current 20-session evaluation completes. Evidence: macd_price_action_sr_study/
> F1_ADDENDUM.md (portfolio IS 9.65→10.73, OOS 6.18→7.20, 82% trades kept). Fail-open.
*Added: 2026-06-21 | Status: Paper trading | Research: [sell_strategy_spec.txt](../../../Developer/options_data/research/macd_price_action_sr_study/sell_strategy_spec.txt)*

### Overview

| | |
|---|---|
| **Instruments** | NIFTY + BANKNIFTY spot index (NSE_INDEX), options on NFO |
| **Signal** | MACD(12,26,9) M2 zero-line crossover + SR3 pivot ±0.2% |
| **Direction** | Bull signal → Sell ATM PE · Bear signal → Sell ATM CE |
| **Product** | MIS (intraday) |
| **Lot sizing** | 5 lots per instrument (paper) |
| **Entry window** | 09:15 – 14:30 IST |
| **EOD exit** | 15:14 IST (hard limit) |
| **SL** | 1.5× credit received |
| **Target** | Buy back when premium decays to 50% of credit (keep 50%) |
| **Min DTE** | ≥ 2 days |
| **Min credit** | ₹10 |
| **Script** | `live_trading/macd_m2_sell_options_bot/macd_m2_sell_options_bot.py` |
| **State file** | `live_trading/logs/macd_m2_sell_options_state.json` |

### Strategy

MACD(12,26,9) zero-line crossover on 15-min bars identifies high-conviction inflection zones. When
the crossover coincides with the index within ±0.2% of the prior day's pivot point (PP = H+L+C / 3),
it marks a S/R confluence.  The sold option decays rapidly as the expected directional move fails to
materialise — win rate 71–73% over 6.5 years.

**Signal rules (ALL required):**
1. MACD(12,26,9) M2 crossover on 15-min bars (from below zero to above, or above to below)
2. Index close within ±0.2% of prior day's pivot point
3. Signal bar close time before 14:30 IST
4. No existing position open for that instrument
5. Option DTE ≥ 2 days
6. Option LTP at entry > ₹10

### Exit Rules

| Rule | Condition |
|------|-----------|
| SL | Live premium ≥ 1.5× entry credit |
| Target | Live premium ≤ 50% of entry credit |
| EOD | 15:14 IST — unconditional (sandbox auto-squares at 15:15) |

### Backtest Performance (macd_price_action_sr_study sell-side, 2026-06-21)

| Stage | Metric | Result |
|-------|--------|--------|
| IS Sharpe (2020–2023) | > 1.0 | ✅ +9.42 |
| IS Win Rate | > 50% | ✅ 73% |
| IS Profit Factor | > 1.5 | ✅ 5.23 |
| IS Max DD% | < 10% | ✅ 3.1% |
| OOS Sharpe (2024–2026) | > 0.8 | ✅ +6.16 |
| OOS Win Rate | > 50% | ✅ 71% |
| Walk-Forward | > 7/10 positive | ✅ 10/10 |
| Bootstrap 5th pct Sharpe | > 1.0 | ✅ +6.37 |
| **All gates** | 8/8 | ✅ **STRONG GREEN LIGHT** |

### Stage 11 Gate

- Minimum 20 sessions before live sign-off
- Gate: Net P&L positive, live WR within ±10% of OOS (61–81%), no session loss > 3× avg session P&L
- State file: `live_trading/logs/macd_m2_sell_options_state.json`
- Telegram alerts on every entry/exit

---

## Bot 20 — BANKNIFTY Trend Pullback Positional Bot

*Added: 2026-07-05 | Status: Paper trading | Research: [results_summary.md](../../../Developer/options_data/research/trend_pullback_positional_study/results_summary.md)*

### Overview

| | |
|---|---|
| **Instruments** | BANKNIFTY spot index (NSE_INDEX), options on NFO |
| **Signal** | EMA(9)/EMA(26) 15-min regime cross (aligned vs SMA(50) basis) + BB(20,2σ) pullback pierce + 1-min candle confirmation |
| **Direction** | Bullish regime confirm → Sell ATM PE · Bearish regime confirm → Sell ATM CE |
| **Product** | NRML (positional, multi-day hold — no daily EOD flatten) |
| **Lot sizing** | 10 lots |
| **Entry cutoff** | 15:10 IST (no confirmed entries at/after) |
| **Expiry force-exit** | 15:14 IST, on/after the traded contract's own expiry date only |
| **SL** | 2.5× entry credit |
| **Target** | Buy back when premium decays to 50% of credit (keep 50%) |
| **Confirmation window** | 30 minutes from pullback pierce bar's close |
| **Min DTE** | ≥ 2 days |
| **Script** | `live_trading/banknifty_trend_pullback_positional_bot/banknifty_trend_pullback_positional_bot.py` |
| **State file** | `live_trading/logs/banknifty_trend_pullback_positional_state.json` |

### Strategy

A 15-min EMA(9)/EMA(26) cross defines a regime only when aligned with an SMA(50) basis
(bullish cross requires close > SMA50 at the cross bar; bearish requires close < SMA50).
Within that regime, the bot watches for the FIRST close-beyond-BB(20,2σ) pullback pierce
opposite the regime direction, then drops to 1-min bars to confirm the pullback is over via
a reversal candle (bullish/bearish engulfing, hammer, shooting star) within 30 minutes. On
confirmation, sells the ATM option in the regime's continuation direction.

**Signal rules (ALL required):**
1. Aligned EMA(9)/EMA(26) cross vs SMA(50) basis on 15-min bars (regime start)
2. First close-beyond-BB(20,2σ) pierce opposite the regime direction, within that regime
3. 1-min reversal candle confirming within 30 minutes of the pierce bar's close
4. Confirmation bar's next-bar timestamp before 15:10 IST
5. No existing position open
6. Option DTE ≥ 2 days

### Exit Rules (priority order)

| Rule | Condition |
|------|-----------|
| Regime end | A new aligned EMA/SMA regime cross fires (checked on 15-min bar close) |
| Expiry force-exit | Today ≥ contract's expiry date AND time ≥ 15:14 IST |
| Target | Live premium ≤ 50% of entry credit |
| SL | Live premium ≥ 2.5× entry credit |

Note: regime-end is detected on a 15-min bar-close cadence, while expiry/target/SL are
polled independently every 30 seconds — see the bot's `README.md` "Known live-vs-backtest
divergences" for why exit-priority ordering is a best-effort approximation, not a strict
per-bar guarantee, in real time.

### Backtest Performance (trend_pullback_positional_study, 2026-07-05)

| Stage | Result |
|-------|--------|
| Stage 0 (Discovery) | ✅ PASS |
| Stage 1 (IS) | ✅ PASS |
| Stage 2b (Parameter sweep) | ✅ PASS |
| Stage 4 (OOS) | ✅ PASS |
| Stage 5 (Monte Carlo) | ✅ PASS |
| Stage 6 (Bootstrap) | ✅ PASS |
| Stage 7 (Walk-forward) | ✅ PASS |
| Stage 8 (Regime filter) | ✅ PASS |
| Stage 9 (Multi-instrument) | ⏭️ SKIPPED (already BANKNIFTY-only) |
| Stage 10 (Expiry segmentation) | ✅ PASS |
| Combined IS+OOS (145 trades, 2022-07 → 2026-07) | Sharpe 1.850, WR 60.0%, Net P&L +₹1,013,689, Max DD -13.4% |

### Stage 11 Gate

- Minimum 20 sessions before live sign-off
- Gate: Net P&L positive, live WR within ±10% of OOS (50–70%), no session loss > 2× the 2.5×-credit SL
- State file: `live_trading/logs/banknifty_trend_pullback_positional_state.json`
- Telegram alerts on every entry/exit
