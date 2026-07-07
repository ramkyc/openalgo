# Daily Sniper Bot — CNC Stock Mean Reversion

## What It Does

A **once-daily swing trading bot** that buys Nifty 50 stocks using CNC (delivery) when their price touches the lower Bollinger Band on a **daily timeframe**. It holds the stock overnight and exits when the price returns to the BB middle (SMA) or hits a stop loss. All activity happens at 3:20 PM.

---

## Strategy Logic

### When It Runs
The bot is scheduled to run **once at 3:20 PM every trading day** via APScheduler. It does two things on each run:

**1. Check existing holdings for exit**
For each stock currently held in the portfolio, it checks:
- If current price ≥ BB middle (SMA-20 on daily) → **EXIT** (target reached)
- If current price ≤ entry price × (1 − 3%) → **EXIT** (stop loss hit)

**2. Scan whitelist for new entries**
For each stock in the watchlist, it fetches 60 days of daily price history and computes BB(20, 2σ). If today's close is **at or below the lower band**, it places a **BUY CNC** market order.

### Position Sizing
`CAPITAL_PER_TRADE = ₹1,00,000` — quantity = floor(1,00,000 / current_price). So at ₹5,968 per share (e.g. DIVISLAB), it buys 16 shares.

### CNC vs Holdings
Stocks bought via CNC are in **same-day positions** (visible in Positions tab in OpenAlgo). After T+1 settlement overnight, they move to **Holdings** tab. The bot correctly checks both `positionbook()` (same-day buys) and `holdings()` (previously bought stock) when deciding whether to exit.

---

## Key Parameters

| Parameter | Value | Notes |
|---|---|---|
| Run time | 3:20 PM daily | Via APScheduler cron |
| BB Period | 20 bars (daily) | ~1 month of trading days |
| BB Std Dev | 2.0σ | Standard BB setting |
| Entry signal | Close ≤ lower BB | Daily close required |
| Exit target | Price ≥ SMA (middle band) | Mean reversion complete |
| Stop Loss | 3% below entry price | Protective SL |
| Capital per trade | ₹1,00,000 | Quantity = floor(100k / price) |
| Product type | CNC | Delivery (overnight holding) |
| Exchange | NSE | Equity stocks |
| Strategy name | `DAILY_SNIPER` | OpenAlgo tag |
| History fetch | 60 calendar days | Ensures ≥20 trading days for BB |

---

## Watchlist (10 stocks)

| Sector | Stocks |
|---|---|
| Pharma | DIVISLAB, CIPLA, DRREDDY, SUNPHARMA |
| Banking | ICICIBANK, KOTAKBANK |
| Auto | BAJAJ-AUTO, M&M |
| Heavyweights | RELIANCE, LT |

---

## Important Notes

- **Holdings vs Positions**: After market close, CNC buys from yesterday show up under **Holdings** (not Positions) in the OpenAlgo UI. Use the Avatar → Holdings link to see them.
- **No intraday trading**: This bot only places orders at 3:20 PM and only in CNC. It is not connected to any WebSocket feed.
- **Paper mode**: Running in OpenAlgo Analyzer (Sandbox) mode — no real broker orders.

---

## Files
| File | Purpose |
|---|---|
| `daily_sniper_bot_v2.py` | Main bot script |
| `daily_sniper_state.json` (in `logs/`) | Tracks which stocks are currently held |
