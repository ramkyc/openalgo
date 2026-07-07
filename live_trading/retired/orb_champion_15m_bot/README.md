# ORB Champion — 15-Minute Opening Range Breakout Bot

## What It Does

Trades **BANKNIFTY options** based on a breakout of the first 15-minute price range of the day. When BANKNIFTY breaks above the morning high, it buys the ATM Call. When it breaks below the morning low, it buys the ATM Put. Only one trade is taken per day.

---

## Strategy Logic

### Step 1 — Build the Opening Range (09:15 – 09:30)
The bot watches every tick from 09:15 to 09:30. The highest tick seen becomes `range_high`, the lowest tick becomes `range_low`. This is the "Opening Range."

### Step 2 — Monitor for Breakout (09:30 – 15:00)
After 09:30, the bot waits for BANKNIFTY to **cross** through either boundary:
- Price crosses **above `range_high`** → BUY ATM **Call (CE)**
- Price crosses **below `range_low`** → BUY ATM **Put (PE)**

The crossover logic is strict — the previous tick must have been *inside or at* the range boundary, and the current tick must be *outside*. This prevents entering on a price that was already past the boundary when the bot started.

**Entry cutoff: 15:00.** No new trades after 3 PM.

### Step 3 — Manage the Trade
Once in a position, the bot monitors the option LTP every tick with a 3-stage exit system:

| Stage | Condition | Action |
|---|---|---|
| Initial SL | Option drops 20% from entry | Exit immediately |
| Profit Shield | Option gains 15% from entry | Move SL to break-even (entry price) |
| Trailing Stop | Option gains 25% from entry | Activate 10% trailing SL from peak |

### Step 4 — EOD Exit
If still in a position at **15:25**, the bot closes it at market regardless.

---

## Key Parameters

| Parameter | Value | Notes |
|---|---|---|
| Underlying | BANKNIFTY | NSE_INDEX |
| Options Exchange | NFO | NRML product |
| Quantity | 900 | Freeze lot for BANKNIFTY (SEBI limit per order) |
| Opening Range Window | 09:15 – 09:30 | 15-minute range |
| Entry Cutoff | 15:00 | No fresh trades after 3 PM |
| EOD Exit | 15:25 | Force-close all positions |
| Stop Loss | 20% of entry premium | Hard initial SL |
| Profit Shield trigger | +15% from entry | Moves SL to break-even |
| Trailing SL trigger | +25% from entry | Activates 10% TSL from peak |
| Trailing SL distance | 10% from peak | Locks in profits on big moves |
| Max trades per day | 1 | One trade only |
| Strategy name in OpenAlgo | `ORB_CHAMPION_15M` | Used for position tagging |

---

## Mid-Day Restart Recovery
If the bot crashes and restarts mid-day, it:
1. Fetches the morning's 1-min candles from OpenAlgo history to reconstruct the range
2. Checks `positionbook` for any existing `ORB_CHAMPION_15M` position and **adopts it** (re-attaches the SL monitoring)
3. Continues as if nothing happened

---

## Files
| File | Purpose |
|---|---|
| `orb_champion_15m_bot.py` | Main bot script |
| `orb_state.json` (in `logs/`) | Live state dump (refreshed every 2s) |

---

## Why 15M Was Chosen Over 30M
Both 15M and 30M versions traded the same symbol (BANKNIFTY ATM options) and on most days the breakout happens before the 30M range even forms, meaning both bots entered the same trade. The 15M version is faster-reacting and sufficient — the 30M bot was retired to avoid doubling up on the same position.
