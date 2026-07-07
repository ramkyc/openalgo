# EMA Swing Scanner — Paper Trading Bot

## What It Does

A **paper-trading daily scanner** that runs the EMA Pullback + RSI Confirmation swing strategy across 19 NIFTY50 stocks. Fires once per trading day at 15:30 IST after the daily candle closes.

**This bot is paper-only until explicitly promoted to live.** All signals are logged and simulated. No real orders are placed.

---

## Strategy (from V3 Backtest)

| Parameter | Value |
|---|---|
| Universe | 19 NIFTY50 stocks (Tier 1/2/3 — see below) |
| Timeframe | Daily (D) |
| Trend filter | Stock Close > 50 EMA |
| Entry signal | Day Low ≤ 20 EMA AND RSI(14) between 35–60 |
| Regime filter | NIFTY50-INDEX Close > 50 EMA |
| Entry | Next day's open (simulated) |
| Stop | Entry − 1×ATR(14) |
| Breakeven | Once close ≥ Entry + 1×ATR, stop → Entry |
| Trailing | Stop = (highest_close − 1×ATR), only moves up |
| Hard cap | Entry + 3×ATR |
| Time stop | 20 trading days |
| Max positions | 5 concurrent |

## Stock Universe & Risk Tiers

| Tier | Risk Mult | Stocks |
|---|---|---|
| Tier 1 (1.5×) | LT, SHRIRAMFIN, BEL, M&M, BHARTIARTL, HCLTECH, SBILIFE |
| Tier 2 (1.0×) | RELIANCE, SBIN, TECHM, GRASIM, COALINDIA, TITAN, ADANIPORTS, TATASTEEL |
| Tier 3 (0.75×) | ICICIBANK, AXISBANK, NTPC, HINDALCO |

*Note: JSWSTEEL excluded — negative V3 expectancy in regime-on periods.*

## Backtest Performance (V3, Jan 2023 – Mar 2026)

| Metric | Value |
|---|---|
| Total trades | 423 |
| Win rate | 52.25% |
| Avg return/trade | 0.89% |
| Profit factor | 1.83 |
| Max drawdown | -7.75% |
| Sharpe ratio | 2.48 |
| CAGR (₹10L capital) | 24.99% |
| Total return | 104.7% (₹10L → ₹20.47L) |

---

## Paper Trading → Live Promotion Process

1. Run in **paper mode** (default) for a minimum of **20 trading sessions**
2. Compare paper results to V3 backtest expectations:
   - Win rate within ±10% of 52%
   - No single position loss > 2× expected ATR stop
3. Review with Ramakrishna → explicit sign-off required
4. Set `EMA_SWING_PAPER_MODE=false` in `.env` to enable live orders

---

## Files

| File | Purpose |
|---|---|
| `main.py` | Entry point — daily scheduler loop |
| `scanner.py` | Signal detection (indicators + regime check) |
| `paper_trader.py` | Paper position tracking, trailing stop updates, P&L |
| `logs/ema_swing_scanner.log` | Full activity log |
| `logs/ema_swing_scanner_state.json` | Live state for dashboard |
| `logs/paper_trades.csv` | All simulated trades (entry, exit, P&L) |

## Run

```bash
# From openalgo root
uv run -m live_trading.ema_swing_scanner.main

# Or directly
python live_trading/ema_swing_scanner/main.py
```

Fires daily at 15:30 IST. Safe to leave running overnight — it skips weekends and market holidays automatically.
