# BB Paper Bot — Study A + Study B

## What It Does

A **paper-trading research bot** that runs two independent Bollinger Band strategies on NIFTY simultaneously. It observes whether selling the ATM Put after an extreme overbought signal is profitable. No real money is at risk — all orders go through OpenAlgo Sandbox.

---

## Two Studies Running in Parallel

### Study A — BB on the Index Price
**Signal**: NIFTY 5-min bar closes **above** its Bollinger Band (30 bars, 3σ)

The logic: When the index is extremely overbought (3 standard deviations above a 30-bar mean), the ATM Put is likely cheap due to low IV on the call side. Sell the ATM PE expecting a pullback.

| Parameter | Value |
|---|---|
| Data source | NIFTY index 5-min OHLC |
| BB Period | 30 bars |
| BB Std Dev | 3.0σ |
| Signal | Close > upper band |
| Hysteresis | 1.0 × std (to clear signal) |
| Entry window | 09:15 – 10:30 AM only |
| Min premium | ₹150 (option must be worth selling) |

### Study B — BB on the Option's Own Price
**Signal**: ATM NIFTY PE 5-min bar closes **above** its own Bollinger Band (20 bars, 2.5σ)

The logic: When the PE premium itself spikes to an extreme high (panic buying), it is likely to mean-revert. Sell the PE at the spike.

| Parameter | Value |
|---|---|
| Data source | ATM NIFTY PE 5-min option price |
| BB Period | 20 bars |
| BB Std Dev | 2.5σ |
| Signal | Option close > upper band |
| Entry window | 09:15 – 10:30 AM only |
| Min premium | ₹150 |
| Max signals | 1 per day (fires only on first qualifying bar) |

---

## Shared Parameters

| Parameter | Value | Notes |
|---|---|---|
| Instrument | NIFTY ATM Put (PE) | Both studies sell PE |
| Product | NRML | |
| Exchange | NFO | |
| EOD exit | 15:15 PM | Force-close all positions |
| SL for Study A | 2.0 × (entry std band width) | Wider SL for index-based signal |
| SL for Study B | 1.5 × (entry std band width) | Tighter SL for option-based signal |
| Strategy A tag | `BB_PAPER_A` | OpenAlgo tag |
| Strategy B tag | `BB_PAPER_B` | OpenAlgo tag |
| Paper mode | `true` by default | Set `BB_PAPER_MODE=false` in .env for live |

---

## Architecture

The bot runs as a single `asyncio` process with one shared WebSocket connection:
- One WS feed handles both the NIFTY index ticks (for Study A) and ATM PE option ticks (for Study B)
- `StudyAEngine` and `StudyBEngine` are independent classes maintaining their own OHLC histories and signal states
- `PositionManager` tracks open positions for both strategies independently
- ATM strike is resolved once at market open and re-checked on each candle close

---

## Files
| File | Purpose |
|---|---|
| `main.py` | Entry point — WebSocket loop, ATM resolution, orchestration |
| `strategy_a.py` | Study A signal engine (index BB) |
| `strategy_b.py` | Study B signal engine (option BB) |
| `position_manager.py` | Tracks open/closed positions, P&L |
| `logs/bb_paper_bot.log` | Trade log |
