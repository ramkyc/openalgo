# Pre-Open Gap Fade Bot — Paper Trading

## What It Does

Fades (trades against) large pre-market gaps in **Nifty 50 stocks**. When a stock opens with a gap ≥ 2% at 09:15, it bets that the gap will partially close by 10:00 AM. All trading is in **paper mode** (OpenAlgo Analyzer/Sandbox) — no real orders go to the broker.

---

## Strategy Logic

### Timing
| Time | Action |
|---|---|
| 08:55 AM | Bot starts, fetches previous close prices for all 48 stocks |
| 09:14:50 AM | Scans pre-open IEP (or LTP) to compute gap % |
| 09:15:05 AM | Places entry + SL orders for qualifying stocks |
| 09:16 – 09:59 | Monitors positions via multiquotes every 60s |
| 10:00:00 AM | Force-closes all remaining open positions |
| 10:05:00 AM | Logs final P&L to DuckDB |

### Entry Rules
- Stock gaps **UP ≥ 2%** → **SHORT** (fade the gap, expect price to fall)
- Stock gaps **DOWN ≥ 2%** → **LONG** (fade the gap, expect price to rise)
- Entry at **09:15:05 AM** as a MARKET order
- Immediately after entry, a **SL-M order at 0.5% against entry** is placed

### Exit Rules
- **Stop Loss**: 0.5% SL-M order placed at entry — auto-triggered by the broker
- **Time Exit**: 10:00 AM — all remaining open positions closed with MARKET orders regardless of P&L

### Position Sizing
- ₹1,00,000 per stock, max 10 simultaneous positions
- Quantity = floor(1,00,000 / entry_price)
- If 20 stocks qualify, only the **top 10 by gap magnitude** are traded

---

## Key Parameters

| Parameter | Value | Notes |
|---|---|---|
| Gap threshold | ≥ 2.0% | Absolute gap (up or down) |
| Entry time | 09:15:05 AM | MARKET MIS order |
| Stop Loss | 0.5% from entry | SL-M order placed immediately after entry |
| Exit time | 10:00:00 AM | Force close all |
| Capital per trade | ₹1,00,000 | |
| Max positions | 10 | Top 10 gaps by magnitude |
| Product type | MIS | Intraday only |
| Exchange | NSE | Equity |
| Strategy name | `PREOPEN_GAP_FADE` | OpenAlgo tag |
| Order throttle | 0.3s between pairs | Stays under 10 orders/second rate limit |
| Round-trip cost | ~0.07% | Used for net P&L calculation |

---

## Universe — 48 Nifty 50 Stocks

All Nifty 50 EQ stocks **except**:
- **TATAMOTORS** — excluded due to demerger complexity
- **LT** — excluded (Sharpe –1.76 in backtest)
- **KOTAKBANK** — excluded (Sharpe –4.32 in backtest)

---

## Backtest Results (Jan 2023 – Mar 2026)
- **Sharpe Ratio (IS)**: 3.86
- **Sharpe Ratio (OOS)**: 7.26
- **694 trades** across the 3-year period
- All 10 stages of the strategy validation pipeline passed

---

## P&L Logging
Trade records are written to the `paper_trades_stocks` table in `options_data.duckdb` for later analysis.

---

## Files
| File | Purpose |
|---|---|
| `preopen_gap_fade_bot.py` | Main bot script |
| `preopen_gap_fade_state.json` (in `logs/`) | Daily state snapshot |
