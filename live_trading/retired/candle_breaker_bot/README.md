# Candle Breaker Bot

## What It Does

Trades **NIFTY and SENSEX options** on a "candle reclaim" pattern. The idea: if the option price falls below the open of its current candle bar and then **climbs back above it** (reclaims the open), it signals bullish momentum for a continuation move. The bot targets 2.5× the initial pullback as profit.

This runs simultaneously on two instruments with different timeframes:
- **NIFTY** → 15-minute candles → Monthly expiry CE/PE options (NFO)
- **SENSEX** → 30-minute candles → Weekly expiry CE/PE options (BFO)

---

## Strategy Logic (per instrument)

### Setup — on each new candle bar (every 15m for NIFTY / every 30m for SENSEX)
When a new candle starts, the bot records the option's **opening price** and **previous candle's low**. These two levels define the trade setup.

### Entry — "Down then Up" reclaim
During the candle's life, the bot tracks the option LTP:
- It counts how many times the price crosses **below the open** (down-crosses) and **back above the open** (up-crosses)
- Entry fires when **up-cross count ≥ target** (currently set to 3 crosses = down, up, down → up again), confirming the reclaim is real and not just a wick
- This up-cross counting prevents entering on the very first dip-and-recovery (noise filter)

### Exit levels — set at entry
| Level | Calculation | Description |
|---|---|---|
| Target | `entry - (entry - prev_low) × RR` | 2.5× the initial pullback below open |
| Stop Loss | `prev_low` | Previous candle's low — if broken, trade is invalid |

*(For a PUT option, falling LTP is profit, so target is below entry and SL is above.)*

### Exit conditions
1. **Target hit** — option LTP reaches the calculated target
2. **Stop loss hit** — option LTP breaches the previous candle's low
3. **EOD shutdown** — 15:25 PM, all positions closed
4. **Entry cutoff** — no new entries after 14:45 (leaves ≥40 min for the trade)
5. **Lunch break** — no new entries between 12:00 and 13:30

---

## Key Parameters

| Parameter | Value | Notes |
|---|---|---|
| **NIFTY** | | |
| Timeframe | 15 minutes | Candle granularity |
| Expiry type | Monthly | NFO monthly options |
| Quantity | 1800 | NIFTY freeze lot |
| **SENSEX** | | |
| Timeframe | 30 minutes | Candle granularity |
| Expiry type | Weekly | BFO weekly options |
| Quantity | 1000 | SENSEX freeze lot |
| **Shared** | | |
| Risk:Reward | 2.5× | Target = 2.5 × (entry to prev-low distance) |
| Entry cutoff | 14:45 | No new positions after 2:45 PM |
| Lunch break | 12:00 – 13:30 | No entries during this window |
| Morning wait | 09:45 | Bot does not take entries before 9:45 AM |
| EOD exit | 15:25 | Force-close all open trades |
| Daily Portfolio SL | ₹50,000 | Bot halts for the day if cumulative loss exceeds this |
| Up-cross target | 3 crosses | Number of reclaim crossovers before entry fires |
| Strategy name | `CANDLE_BREAKER_LIVE` | OpenAlgo position tag |

---

## State Persistence
The bot saves its full state to `candle_breaker_state.json` every few seconds. On restart, it:
1. Loads the saved state
2. Calls `positionbook` to re-link any live positions
3. Reconstructs SL levels from order history

---

## Transaction Cost Tracking
The bot uses `calc_friction.py` to compute actual Indian F&O costs (STT, exchange charges, GST, brokerage) for each trade. Net P&L (after all costs) is logged and sent via Telegram.

---

## Files
| File | Purpose |
|---|---|
| `candle_breaker_bot.py` | Main bot script |
| `candle_breaker_state.json` (in `logs/`) | Persistent state (positions, counts, SL levels) |
| `cb_live_state.json` (in `logs/`) | Live snapshot of active trades (refreshed every 2s) |
