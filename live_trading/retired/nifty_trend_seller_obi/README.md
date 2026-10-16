# Nifty Trend Seller + OBI Gate — Paper Trade Bot

## What this is

A 15-day paper trading experiment to measure whether real-time 50-level order book
imbalance (OBI) data adds edge to the Nifty Trend Seller strategy.

**Base strategy**: Nifty Trend Seller (NTS) — all 10 research pipeline stages PASSED.
OOS Sharpe +1.735, WR 66%, MC stability 99.2%.
Reference: `research/nifty_trend_seller_study/results_summary.md`

**OBI gate**: At the moment an NTS short signal fires, read the weighted OBI of the
NIFTY ATM CE we intend to sell (via depth-50 WebSocket). Negative OBI = net selling
pressure = confirms our bearish view. Positive OBI = buyers dominating CE = skip.

**Why we can't backtest this**: No historical depth-50 data exists locally or cheaply
anywhere. Forward paper-trading is the only way to measure the gate's impact.

---

## Files

```
nifty_trend_seller_obi/
├── nifty_trend_seller_obi_bot.py  ← main runner
├── obi_engine.py                  ← WebSocket depth-50 OBI subscriber
├── session_logger.py              ← CSV logger (signals / trades / ghosts)
├── README.md                      ← this file
└── logs/
    ├── nts_obi_bot.log            ← running log
    ├── signals.csv                ← every NTS signal (gate pass or block)
    ├── trades.csv                 ← OBI-approved paper trades
    └── ghosts.csv                 ← OBI-blocked signals tracked to EOD
```

---

## Pre-requisites

1. OpenAlgo is running: `uv run app.py` (in the openalgo directory)
2. Fyers broker is logged in (OAuth token active)
3. OpenAlgo is in **Analyze Mode** (the paper trading sandbox)
4. `.env` has valid `OPENALGO_API_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
5. WebSocket is reachable at `ws://127.0.0.1:5001/ws` (check `WEBSOCKET_URL` in .env)

---

## How to run

```bash
cd ~/Developer/fyers_crk
uv run live_trading/nifty_trend_seller_obi/nifty_trend_seller_obi_bot.py
```

Start it by **09:35 IST** so the 09:40 startup job runs on time.

---

## Session timeline (all IST)

| Time  | Event |
|-------|-------|
| 09:40 | Startup — holiday check, VIX check, resolve ATM CE, subscribe OBI |
| 10:00 | Entry window opens — indicator loop begins every minute |
| 10:00–13:00 | Every minute: fetch 1-min bars → NTS signal check → OBI gate |
| 13:00 | Entry window closes (no new positions, existing trade still monitored) |
| 15:20 | EOD force-exit (if position still open) |
| 15:25 | Session summary logged + Telegram |

---

## Champion parameters

| Parameter     | Value  | Source |
|---------------|--------|--------|
| ADX threshold | 25     | IS sweep — more trades, same WR vs ADX>30 |
| RSI threshold | 50     | RSI < 50 for short signal |
| ADX rising bars | 7    | 7-bar window filters noise better than 3 or 5 |
| SL multiplier | 2.0×   | Entry premium × 2.0 = SL price |
| Direction     | SHORT only (sell CE) | Long leg has negligible IS edge |
| Entry window  | 10:00–13:00 IST | |
| VIX filter    | ≤ 22   | Skip day if VIX exceeded |
| DTE filter    | ≥ 2    | Handled by ATM resolver |
| Lots          | 10     | Standard per CLAUDE.md |

---

## OBI gate parameters

| Parameter            | Default | Notes |
|----------------------|---------|-------|
| `OBI_GATE_THRESHOLD` | 0.0     | OBI < 0 = any net selling pressure on CE → TRADE |
| `OBI_STALE_SECONDS`  | 10      | If no depth update in 10s, gate is bypassed and stale=True logged |
| `STRIKE_DRIFT_TRIGGER` | 100 pts | Re-resolve ATM if spot drifts >100 pts from morning ATM |

The threshold starts at 0.0 (permissive). After 15 sessions, if OBI shows clear
positive edge, tighten to -10 or -20 for the next phase.

---

## What gets logged

### signals.csv — every NTS signal

Every time all 5 NTS conditions fire simultaneously during 10:00–13:00:

| Column | Description |
|--------|-------------|
| signal_time | Bar close time (HH:MM) |
| adx, rsi_val, macd_cross | Indicator values at signal bar |
| entry_premium | Option LTP at signal moment |
| obi_at_signal | Weighted OBI (-100 to +100) |
| obi_n_levels | Depth levels received (ideally 50) |
| obi_gate_pass | True (TRADE) / False (GHOST) |

### trades.csv — completed paper trades (OBI approved)

| Column | Description |
|--------|-------------|
| exit_reason | SL_HIT / EOD_EXIT |
| pnl_net_total | Net P&L after ₹65/lot transaction costs |

### ghosts.csv — OBI-blocked signals (counterfactual)

| Column | Description |
|--------|-------------|
| ghost_exit_reason | GHOST_SL_HIT / GHOST_EOD |
| ghost_pnl_gross_total | What we would have made/lost (pre-cost) |

---

## 15-day retrospective analysis

After 15 sessions, open a Python notebook and run:

```python
import pandas as pd

trades = pd.read_csv("logs/trades.csv")
ghosts = pd.read_csv("logs/ghosts.csv")
signals = pd.read_csv("logs/signals.csv")

# Compare WR: OBI-filtered vs unfiltered
print("=== TRADE STREAM (OBI gate ON) ===")
t_wr = (trades["pnl_gross_per_lot"] > 0).mean() * 100
print(f"WR: {t_wr:.1f}%  |  Trades: {len(trades)}")
print(f"Net P&L: ₹{trades['pnl_net_total'].sum():,.0f}")

print("\n=== GHOST STREAM (OBI gate OFF) ===")
g_wr = (ghosts["ghost_pnl_gross_per_lot"] > 0).mean() * 100
print(f"WR: {g_wr:.1f}%  |  Ghosts: {len(ghosts)}")
print(f"Gross P&L: ₹{ghosts['ghost_pnl_gross_total'].sum():,.0f}")

print("\n=== OBI GATE COST/BENEFIT ===")
# How many signals did the gate block?
total_signals = len(signals)
blocked = (~signals["obi_gate_pass"]).sum()
print(f"Total signals: {total_signals}  |  Blocked: {blocked} ({blocked/total_signals*100:.0f}%)")

# Was the gate directionally correct? (blocked trades that would have lost)
print("\nOBI value distribution at signal time:")
print(signals.groupby("obi_gate_pass")["obi_at_signal"].describe())
```

**Gate is adding edge if:**
- `trades` WR > `ghosts` WR (gate is keeping the good trades and blocking the bad ones)
- `trades` net P&L > `ghosts` gross P&L (self-evident if WR is higher)
- OBI at blocked signals skews positive (you blocked CE-bullish moments = correctly avoided)

**Gate is neutral/harmful if:**
- WR is similar between streams (OBI is random noise relative to NTS outcomes)
- Ghosts WR is higher (you blocked good trades — tighten or remove the gate)

---

## Troubleshooting

**"No depth snapshot yet for NIFTY..."**
The WebSocket hasn't received a depth tick yet. This is normal for the first 30–60
seconds after subscription. If it persists > 2 minutes, check OpenAlgo WebSocket
connection and that the ATM CE symbol format is correct.

**"Could not resolve ATM CE"**
OpenAlgo's `/api/v1/expiry` endpoint returned no dates. Ensure:
1. OpenAlgo is running and broker is logged in
2. NSE has started trading (not pre-open)
3. The DTE filter (≥2) has available expiries

**"VIX=xx.x > 22 — SKIPPING today"**
The VIX filter triggered. No trades today. This is expected behaviour.

**OBI always shows stale=True**
Check the WebSocket URL in `.env`. Try restarting OpenAlgo app and re-running the bot.

---

## Architecture notes

The bot uses a **two-thread design**:
- **Main thread**: APScheduler (signal detection, position management) — polls every minute
- **OBI engine thread**: OpenAlgo WebSocket SDK — receives depth ticks continuously

The OBI cache is protected by `threading.Lock()` inside `OBIEngine`. The main thread
reads from it safely via `obi.get_snapshot(symbol)`. No async/await required.

---

*Research: options_data/research/nifty_trend_seller_study/results_summary.md*
*Strategy approved: 2026-03-21 | OBI bot created: 2026-03-28*
