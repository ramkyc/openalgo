# Bollinger Bands Options Bot

## What It Does

A **mean-reversion bot** that trades ATM options on NIFTY, BANKNIFTY, and SENSEX. When an option's premium drops to or below the lower Bollinger Band, it buys that option expecting the premium to revert back to the SMA (middle band). It exits when the premium returns to the SMA, or earlier if SL is hit.

---

## Strategy Logic

### Signal — "Premium Touches Lower BB"
For each index, the bot maintains a rolling 5-minute OHLC history of the ATM Call and Put option premiums. It computes Bollinger Bands on the **closing premium** of each 5-min bar:
- **Entry**: Premium closes **at or below the lower band** → BUY that option
- **Exit target**: Premium reverts back to the **SMA** (middle band)

The idea is that an extreme drop in option premium is temporary — implied volatility mean-reverts and theta decay is offset by the IV spike reverting.

### Filters applied before entry
- Premium must be between **₹5 and ₹600** (avoids near-zero junk and deep ITM)
- Entry window: **09:30 AM to 15:10 PM** (no late entries)
- Only **one active trade per index per side** (e.g. one NIFTY CE and one NIFTY PE simultaneously if both signal)
- **Gap filter**: If the daily gap for an index is extreme (>1%), the bot becomes more cautious

### Exit conditions
| Trigger | Detail |
|---|---|
| **Target** | Premium returns to SMA (middle BB) |
| **Stop Loss %** | 30% drop from entry premium |
| **Stop Loss ₹** | ₹20,000 gross loss on that position (whichever is hit first) |
| **Max holding** | 3 hours — exits even if target/SL not hit (theta trap prevention) |
| **EOD** | 15:25 PM force-close |

---

## Key Parameters

| Parameter | Value | Notes |
|---|---|---|
| **NIFTY** | | |
| BB Period | 45 bars (5-min) | ~3.75 hours of history |
| BB Std Dev | 1.5σ | Tighter band = more frequent signals |
| Quantity | 1,560 | 24 lots × 65 (lot size changed from 75 → 65) |
| Exchange | NFO | |
| **BANKNIFTY** | | |
| BB Period | 45 bars (5-min) | |
| BB Std Dev | 1.5σ | |
| Quantity | 900 | Freeze lot |
| Exchange | NFO | |
| **SENSEX** | | |
| BB Period | 45 bars (5-min) | |
| BB Std Dev | 1.5σ | |
| Quantity | 1,000 | Freeze lot |
| Exchange | BFO | |
| **Shared** | | |
| Entry window | 09:30 – 15:10 | |
| EOD exit | 15:25 | |
| Stop Loss % | 30% | Per option premium |
| Stop Loss ₹ | ₹20,000 | Per position gross loss |
| Max holding | 3 hours | |
| Premium range | ₹5 – ₹600 | Filter to avoid garbage strikes |
| Strategy name | `BB_OPTIONS_LIVE` | OpenAlgo tag |

---

## Mid-Day Recovery
On startup, the bot checks `positionbook` for any open `BB_OPTIONS_LIVE` positions and re-attaches them with the appropriate SL levels reconstructed from the average entry price.

---

## Backtest Basis
This bot was built from a backtest result of ₹4.57M profit and ~70% win rate over historical data. The parameters (BB 45/1.5σ, SL 30%/₹20k, 3hr max hold) were the champion configuration from that study.

---

## Files
| File | Purpose |
|---|---|
| `bollinger_options_bot.py` | Main bot script |
