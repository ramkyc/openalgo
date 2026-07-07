
# 🏆 Strategy Playbook: ORB Champion (Live)

This document serves as the official operational reference for the **ORB Champion** trading system.

## 📡 1. Core Logic: Opening Range Breakout (ORB)

The strategy exploits the "Opening Drive" of the Indian markets, specifically targeting **BANKNIFTY** which exhibits the highest intraday trend expectancy.

- **Range Window**: 09:15 AM - 09:45 AM (First 30 Minutes).
- **The Trigger**: The bot identifies the Highest High and Lowest Low of this window.
- **Entry Condition**:
  - **Long (Buy Call)**: If Index Close > 09:45 High.
  - **Short (Buy Put)**: If Index Close < 09:45 Low.
- **Instrument**: ATM (At-The-Money) Option for the current weekly expiry.
- **Frequency**: Exactly **ONE** trade per day. No chasing, no over-trading.

## 🛡️ 2. The "Shield & Trail" Safety System

This is our custom risk-management engine designed to protect capital from intraday "V-Reversals."

| Stage | Trigger Condition | Bot Action |
| :--- | :--- | :--- |
| **Initial SL** | Entry | Sets a hard stop loss at **-20%** of premium. |
| **Stage 1: SHIELD** | **+15% Profit** | Moves Stop Loss to **Break-Even (Cost Price)**. |
| **Stage 2: TRAIL** | **+25% Profit** | Activates **10% Trailing SL** from the peak. |
The TRAIL feature is a "smart" exit that follows the price up as you make money. It is definitely the first option you mentioned: it exits if the price drops by 10% from its highest peak.

Here is the step-by-step logic with a concrete example to make it crystal clear:

🏦 Example Trade:
Entry Price: ₹1,000
Initial SL: ₹800 (-20%)
Trailing Threshold (+25%): ₹1,250
Trailing Percent: 10%
Phase 1: The Activation
The "Trail" logic stays OFF until your profit hits +25%.

If the price is at ₹1,100, the bot is only using the Shield (SL at ₹1,000).
The moment the price touches ₹1,250, the Trailing engine wakes up. ⚡
Phase 2: The "Peak Tracking"
Once active, the bot remembers the highest price it has seen since the trade started (The Peak). Your Stop Loss is then set at 10% below that Peak.

Price hits ₹1,250 (Peak):

Stop Loss moves to ₹1,125 (₹1,250 minus 10%).
You have now locked in a +12.5% profit.
Price climbs to ₹1,500 (New Peak):

The bot automatically recalculates: 10% below ₹1,500.
Stop Loss moves up to ₹1,350. 📈
You have now locked in a +35% profit.
Market Reverses (Price drops to ₹1,350):

The bot sees the price is equal to or below your Trailing SL.
EXIT! 🏁

> [!TIP]
> **Why this works**: In our 14-month backtest, the "Shield" saved **13.7%** of our trades from turning into losses, effectively boosting our win rate to **62.7%**.

## ⏱️ 3. Exit Conditions

A trade is closed if ANY of the following occur:

1. **Stop Loss Hit**: Initial -20% stop reached.
2. **Trailing SL Hit**: Price pulls back 10% from its highest profit peak.
3. **EOD Shutdown**: If the trade is still open at **03:25 PM**, it is square-off regardless of PnL.

---

# 📉 Strategy Playbook: Bollinger Options Bot

This document serves as the operational reference for the **BB 15M Bot**.

## 📡 1. Core Logic: Mean Reversion (BANKNIFTY)

The strategy exploits the volatility of **BANKNIFTY**, which exhibits the strongest statistical snap-back tendency when reaching extreme Bollinger Deviation on a 15-minute chart.

- **Primary Instrument**: **BANKNIFTY** only.
- **Indicator**: Bollinger Bands (Length: **45**, StdDev: **1.5**).
- **The Concept**: Price has a strong statistical tendency to return to the **45-period SMA (Middle Band)**.
- **Trigger Conditions**:
  - **Call Entry**: ATM option premium touches the **Lower Bollinger Band** on the 15M chart.
  - **Put Entry**: ATM option premium touches the **Upper Bollinger Band** on the 15M chart.
- **Observation Window**: 12:00 PM - 03:10 PM.

## 🛡️ 2. Risk Management

Unlike the ORB bot, the Bollinger bot uses a fixed target exit to capture the mean reversion.

1. **Target**: Automatic exit when the option premium returns to the **SMA (Middle Band)**.
2. **Stop Loss**:
   - **Hard SL**: -30% of entry premium.
   - **Financial Cap**: Max ₹20,000 loss per trade.
3. **Time Exit**: Maximum holding period of 180 minutes to avoid theta decay traps.

---

---

# 🏹 Strategy Playbook: Nifty Heavyweight Sniper (Daily)

This strategy is a long-term **Mean Reversion** system targeting the 10 most influential stocks in the Nifty 50. It uses institutional "buy the dip" levels on the Daily timeframe.

## 📡 1. Core Logic: Daily Mean Reversion

Unlike intraday strategies, the Sniper exploits the reliable price "elasticity" of blue-chip heavyweights.

- **The "Golden Basket"**:
  - **Pharma**: DIVISLAB, CIPLA, DRREDDY, SUNPHARMA.
  - **Banks**: ICICIBANK, KOTAKBANK.
  - **Auto**: BAJAJ-AUTO, M&M.
  - **Heavyweights**: RELIANCE, LT.
- **Indicator**: Bollinger Bands (Length: **20**, StdDev: **2.0**).
- **Timeframe**: Daily (Checked at 15:20 PM IST).
- **Trigger Conditions**:
  - **ENTRY**: If Current Price <= **Daily Lower Band** (Buying the extreme dip).
  - **EXIT**: If Current Price >= **Daily Middle Band (SMA)** (Reversion to mean).
- **Product Type**: **CNC / Delivery** (NRML).

## 🛡️ 2. Risk Management & Performance

- **Stop Loss**: **3.0% Protective Stop Loss** (Enabled). This protects against "Band Walking" in downward trends.
- **Costs**: Extremely low due to zero intraday churn.
- **Backtest Result**: **70% Success Ratio** across Nifty 50; Top 10 Winners averaged **+15% return** in the 2025-2026 cycle.
- **Audit Reference**: [View Full Nifty 50 Audit Logs](file:///Users/ramakrishna/.gemini/antigravity/brain/23eb86d6-e7b7-4ee6-871c-40dd2be3ebf6/nifty50_full_bb_audit.md)

## ⚙️ 3. Persistence & Automation

This bot is designed to be "Set and Forget." It runs on a persistent macOS LaunchAgent.

- **Auto-Start**: The script starts automatically on Mac login/reboot.
- **Persistence**: If the process crashes or is killed, macOS will automatically restart it (**KeepAlive**).
- **Logs**:
  - Output: `live_trading/daily_sniper.log`
  - Errors: `live_trading/daily_sniper_error.log`

---

# 🔥 Strategy Playbook: Candle Breaker (Reclaim)

This strategy exploits intraday volatility by identifying bullish reclaim levels. It enters when a candle's price dips below its open and then crosses back above it.

## 📡 1. Core Logic: Candle Reclaim

The "Candle Breaker" targets high-liquidity indices where price dips are often aggressively bought back by institutional players.

- **Indices & Configurations**:
  - **NIFTY**: 15-Min Timeframe, **Monthly Options** (to avoid high theta decay).
  - **SENSEX**: 30-Min Timeframe, **Weekly Options** (higher yield/churn).
  - *(Note: BANKNIFTY is explicitly excluded from this strategy based on backtesting results).*

- **Trade Concurrency Rules**:
  - **Simultaneous Trades Allowed**: NIFTY and SENSEX operate independently.
  - **Mutual Exclusivity**: CE and PE entries are mutually exclusive within a single candle. If a CE trade is taken, no PE trade can be taken in that same candle (and vice versa), even if the first trade hits SL or Target.
  - **Strict Per-Index Lock**: Maximum of **ONE** trade attempt per candle per index. Once an entry is attempted, the index is locked until the next candle roll.
  - **Persistence**: Trading state (locks) are persisted to disk. Restarting the bot will NOT reset the candle lockout.

- **The Trigger**:
  - Price crosses from **Below Open** to **Above Open**.

- **Filter: Crossover Density**:
  - Entry is only made on the **Nth** crossover, where N = average crossover count of the last 10 candles + 1.
  - This filters out "noise" and chop.
- **Entry Type**: **LIMIT Order** at the candle's Open price.
- **Risk-Reward**: **2.0 RR**.

## 🛡️ 2. Risk Management

- **Stop Loss**:
  - Dynamic SL = `min(Current Candle Low, Previous Candle Low)`.
  - This ensures the stop is placed below the most recent "dip" structure.
- **Freeze Quantities**:
  - **NIFTY**: 1800 units.
  - **SENSEX**: 1000 units.
- **EOD Square-off**: 03:25 PM IST.

---

## 🚀 Deployment & Management

### The All-In-One Startup

1. Open Terminal.
2. Ensure the Daily Sniper LaunchAgent is loaded:
   `launchctl load ~/Library/LaunchAgents/com.antigravity.daily_sniper.plist`
3. Start Intraday Bots:
   `python live_trading/start_all_bots.py`

---

# 📊 Data Infrastructure: Automated Daily Sync

To ensure our backtesting and mining databases are always current, a unified synchronization script runs every evening.

## 📡 1. Automated Daily Tasks

Every trading day at **08:00 PM IST**, the following tasks are executed automatically:

1. **Stock Data Refresh**: Updates the 1-minute historical data for all NIFTY 50 stocks in the Mining DB (`options_data.duckdb`).
2. **Options Data Backfill**: Automatically triggers the Telegram pipeline to download and process the day's Options and Futures files into the local SQLite database.

## ⚙️ 2. Persistence & Logs

The sync is managed by a macOS LaunchAgent to ensure it runs even if the machine reboots.

- **Automation Plist**: `~/Library/LaunchAgents/com.antigravity.daily_data_sync.plist`
- **Master Script**: `~/Developer/options_data/daily_sync.sh`
- **Activity Log**: `~/Developer/options_data/daily_sync.log`

---

## 🚀 Deployment & Management

### The All-In-One Startup

1. Open Terminal.
2. Ensure the Daily Sniper LaunchAgent (Long-only Bot) is loaded:
   `launchctl load ~/Library/LaunchAgents/com.antigravity.daily_sniper.plist`
3. Ensure the Daily Data Sync is scheduled:
   `launchctl load ~/Library/LaunchAgents/com.antigravity.daily_data_sync.plist`
4. Start Intraday Bots:
   `python live_trading/start_all_bots.py`

**Stay Disciplined. Trust the Math. Control the Risk.** 🛡️⚖️📈
