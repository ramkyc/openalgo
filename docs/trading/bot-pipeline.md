# Trading Bot Pipeline

**Authoritative research reference:** `~/Developer/options_data/CLAUDE.md` — read before any research or bot work.

## The Path from Research to Live Trading

Every strategy must travel through these exact stages in order. No shortcuts. No exceptions.

```
STAGES 1–10 — Research & Backtesting  (done in ~/Developer/options_data/)
  ↓ results_summary.md shows all 10 stages PASSED
  ↓ receive_validated_strategy.md checklist completed
STAGE 11 — Paper Trading (minimum 20 sessions in this repo, sandbox mode)
  ↓ all Stage 11 gate criteria met
STAGE 12 — Explicit sign-off from Ramakrishna
  ↓
STAGE 13 — Live Trading (real money)
```

## Stages 1–10 — Research & Backtesting (not done here)

All backtesting is in `~/Developer/options_data/` using local historical data from `data/options_data.duckdb`. This repo does NOT contain research scripts.

When a strategy arrives from `options_data`, the handoff artifacts are:
- `research/<strategy_name>/results_summary.md` — stage-by-stage pass/fail with metrics
- `research/<strategy_name>/backtest_oos.py` — OOS script whose logic is translated 1-for-1 into the live bot

Complete `directives/receive_validated_strategy.md` before writing any bot code.

## Stage 11 — Paper Trading

> **⚠️ Read these two directives first, every time, no exceptions:**
> - `directives/live_bot_websocket.md` — WebSocket auth sequence, canonical index symbol names & exchanges, `get_history` signature, state heartbeat format
> - `directives/bot_code_qa.md` — Pre-finalize QA checklist (syntax, function signatures, auth, symbols, state file, logging)
>
> Commonly skipped sections that cause silent bugs:
> - **Section 5b** — Every new bot must be registered in `start_all_bots.py`, `streamlit_dashboard.py`, `performance_review.py`, and (for equity bots) `market_review.py`. Missing any one means the bot is invisible to monitoring.
> - **Section 5c** — Equity bots MUST pass `strategy_type="equity"` and an explicit `direction` to `log_trade_to_db()`. Defaults silently misclassify equity trades.

Bot directory structure under `live_trading/`:

```
live_trading/
  {strategy_name}/
    __init__.py
    README.md
    scanner.py        ← signal detection (uses OpenAlgo REST API, NOT DuckDB)
    paper_trader.py   ← position tracking, trailing stops, P&L
    main.py           ← scheduler loop
    logs/             ← state JSON + CSV trades (auto-created)
```

## Stage 12 — Explicit Sign-Off

**NEVER promote a bot to live without Ramakrishna's explicit written confirmation.**

When Stage 11 gate criteria are met:
1. Share the paper trading summary (trades CSV + key metrics from `paper_trades.csv`)
2. Compare to OOS backtest expectations from `results_summary.md`
3. Wait for explicit "go live" from Ramakrishna
4. Only then flip OpenAlgo's own **Sandbox/Analyze Mode toggle in the UI** to live

## Stage 13 — Live Trading

- Live orders via OpenAlgo API (`/api/v1/order/`)
- Monitor via the Streamlit dashboard
- Any strategy degradation → flip OpenAlgo's UI mode toggle back to Sandbox/Analyze immediately

> **⚠️ NO INTERNAL PAPER-MODE FLAG — EVER.** Do not write a `{BOT_NAME}_PAPER_MODE`
> (or any `PAPER_MODE`-style) env var or constant into bot code, and do not gate
> `placeorder()` behind one. Per OpenAlgo's documented architecture: **bots ALWAYS
> fire real REST orders via `placeorder()`**; OpenAlgo's own Sandbox/Analyze Mode
> (a UI-level toggle, not a bot setting) transparently intercepts and simulates
> the fill — that's what makes trades show up correctly in the Positions/Orderbook
> UI and get logged to `performance.db`.
>
> An internal flag here is not a harmless safety net — it actively breaks the
> pipeline: it lets `_order()` fabricate a `{"status": "ok"}` response without
> ever calling `placeorder()`, so the trade never reaches OpenAlgo, never appears
> in the UI, and never gets logged. This exact bug shipped in
> `flat_blue_line_monthly_bot.py` on 2026-06-08 (entries logged locally as
> `"PAPER BUY/SELL"` but invisible in the Positions UI) and has recurred across
> multiple bots — `PAPER_MODE` appears in 8 separate bot files today. **Going live
> = flip the OpenAlgo UI mode toggle. Nothing in bot code should know or care
> whether it's "paper" or "live."**
>
> Paper-trading happens by simply running the bot — unmodified — while OpenAlgo
> itself is in Sandbox/Analyze Mode. There is no separate "paper" code path to keep
> in sync, no flag to flip in `.env`, and nothing to "revert" in the bot when
> degradation is spotted — only the OpenAlgo UI toggle changes.

## Bot Deployment Checklist

1. Strategies must pass all 10 research stages in `~/Developer/options_data/research/<study>/` first
2. Champion params from `options_data/research/<study>/results_summary.md`
3. Create `live_trading/<strategy_name>/<bot_name>.py`
4. **ROOT path pattern:**
   ```python
   ROOT = Path(__file__).parent.parent.parent   # → openalgo/
   load_dotenv(ROOT / ".env")
   sys.path.insert(0, str(ROOT))
   ```
5. Import from shared utilities in `live_trading/shared/` and `live_trading/api_utils.py`
6. Register in `live_trading/start_all_bots.py` BOTS list
7. Document in `live_trading/active_trading_bots.md`

## Key Technical Rules for All Bots

- **Daily bar trailing stops**: Use `stop_eod` pattern — test today's low against yesterday's stop, then update stop at END of day. Never update and test on the same bar (causes phantom stop-outs).
- **No Flask in live bots**: Scanners use the OpenAlgo REST API. Never `from app import app` in a scanner.
- **No DuckDB in live bots**: DuckDB is for research only. Live bots fetch all data via OpenAlgo REST API.
- **No local DB for expiry**: Expiry dates must be fetched from `GET /api/v1/expiry`. Never query `interval_data` from a live bot.
- **`uv run` always**: Never use global Python.
- **Max concurrent positions**: Respect `MAX_CONCURRENT` limits in each bot's `paper_trader.py`.
- **Regime filter**: Always check market regime (index > long EMA) before entering new positions.
- **Signal logic fidelity**: Bot signal logic must match `backtest_oos.py` exactly — no modifications without re-running OOS validation.
- **Pre-flight before starting**: Run `execution/check_system_health.py` first. Startup order: OpenAlgo app → broker login → bots.
- **Self-annealing**: When you discover a new constraint or timing edge case, update the relevant directive in `directives/` immediately.
- **Option-symbol resolution — use the shared resolver, never `strike_int`**: `live_trading/shared/atm_resolver.py` is the ONE correct, shared path for resolving ATM/OTM option symbols (it correctly omits `strike_int` and resolves via `offset="ATM"` against the live underlying price). **`strike_int` in `/api/v1/optionsymbol` (and `get_option_symbol()`) means the STRIKE INTERVAL (e.g. `50`, `100`), NOT an absolute strike** — passing an absolute strike (e.g. `23200`) silently produces a bogus/404'd symbol. This exact misuse caused the recurring "ATM symbols unresolved" failure in `flat_blue_line_monthly_bot.py` and appears in `strike_int` form across 5 active bot files. If your index isn't covered by `atm_resolver.py` yet, extend it (it's parameterized by strike step) — do not hand-roll resolution in the bot file. When in doubt, the safest pattern is direct canonical-symbol string construction: `f"{underlying}{expiry_str}{strike}{opt_type}"` (e.g. `BANKNIFTY30JUN2654200CE`) — the `symtoken` master-contract mapping handles broker translation transparently.
- **Net P&L — positionbook is the single source of truth**: After every trade close, `log_trade_to_db()` automatically fetches the Fyers-computed net P&L for the closed symbol from the OpenAlgo positionbook (`/api/v1/positionbook`) and stores it as `net_pnl`. This is the authoritative figure — it reflects actual sandbox fill prices and all Fyers charges (STT, exchange, SEBI, brokerage, GST, stamp) as computed by Fyers themselves. Do NOT hardcode a cost rate table in a new bot or try to compute costs manually; the logger handles it. If the positionbook fetch fails, the logger falls back to an embedded Fyers charge formula automatically. The dashboard always shows `net_pnl` where available.
  > **Multi-leg technical debt (iron fly / flat blue line):** The positionbook returns P&L per symbol; multi-leg strategies need to sum across all legs. Until this is resolved, multi-leg bots use the formula fallback. New multi-leg bots follow the same pattern. See: [open tech debt in this file → § Technical Debt].

- **MIS/intraday EOD exit ≤ 15:14 IST — hard rule, no exceptions**: The OpenAlgo sandbox auto-squaresoff ALL MIS positions at exactly 15:15 IST. Any close order that arrives after 15:15 finds the position already flat — the order silently fails and the trade is never logged to `performance.db`. Maximum allowed EOD exit constant: `dt_time(15, 14)` (one minute of buffer before the 15:15 cutoff). This rule applies to every bot that uses `product="MIS"`. Exception: NRML positions (e.g. iron fly bots) are not subject to the 15:15 sandbox cutoff and may exit later.

## Technical Debt

| # | Item | Filed | Impact |
|---|------|-------|--------|
| TD-001 | **Multi-leg net P&L via positionbook** — Iron fly (4 legs) and Flat Blue Line (6 legs) bots log one combined record to `performance.db` but the positionbook returns P&L per individual symbol. `fetch_closed_pnl()` in `shared/positionbook_pnl.py` handles only single-symbol positions. Until resolved, multi-leg bots use the embedded Fyers charge formula as the `net_pnl` fallback. To fix: after all leg close orders fill, sum `fetch_closed_pnl(leg_symbol)` for each leg and pass the total as `net_pnl` to `log_trade_to_db()`. | 2026-06-23 | Medium — net P&L for iron fly / flat blue line is formula-estimated, not Fyers-exact. Acceptable for paper trading; revisit before Stage 13. |
