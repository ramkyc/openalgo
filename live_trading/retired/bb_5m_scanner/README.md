# BB 5M Scanner — Discretionary Alert Bot

## What It Does

This is **not a trading bot** — it places no orders. It is a **signal scanner** that monitors NIFTY, BANKNIFTY, and SENSEX on 5-minute bars and sends Telegram alerts when any index moves into overbought or oversold territory based on Bollinger Bands. You decide whether to act on the signal.

---

## How It Works

### Indicator
Bollinger Bands computed on the **closing price of each completed 5-minute bar**:
- **Band Period**: 45 bars
- **Std Dev multiplier**: 1.5σ

### Signal States (per symbol)
| State | Condition |
|---|---|
| **OVERBOUGHT** | 5-min close > upper BB |
| **OVERSOLD** | 5-min close < lower BB |
| **Neutral** | Price inside bands |

### Alert Logic — Fires Only on State *Change*
Alerts do **not** fire continuously while the signal persists. They fire exactly **once when the state changes**:
- Neutral → OVERBOUGHT: sends Telegram alert
- Neutral → OVERSOLD: sends Telegram alert
- OVERBOUGHT/OVERSOLD → Neutral (price returns inside bands): sends "signal cleared" alert

**Hysteresis**: To avoid rapid back-and-forth alerts when price hovers right at the band edge, the signal only clears after the price has pulled back **1.0 × std** inside the band.

**Cooldown**: Even if state toggles rapidly, at most **one alert fires per symbol per 15 minutes**.

---

## Key Parameters

| Parameter | Value |
|---|---|
| Symbols monitored | NIFTY, BANKNIFTY, SENSEX |
| Bar timeframe | 5 minutes |
| BB Period | 45 bars |
| BB Std Dev | 1.5σ |
| Hysteresis | 1.0 × σ (before signal clears) |
| Alert cooldown | 15 minutes per symbol |

---

## Shared State File
The scanner writes its current signal state to `logs/bb_signals_state.json`. The Telegram `/signals` command (handled by `telegram_status.py`) reads this file and reports the current status for all three indices on demand.

---

## Files
| File | Purpose |
|---|---|
| `bb_5m_scanner.py` | Main scanner script |
| `bb_signals_state.json` (in `logs/`) | Live signal state (read by `/signals` Telegram command) |
