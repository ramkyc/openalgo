# Bot Deployment Checklist

Use this as a final gate before declaring any bot code complete or requesting Stage 12 sign-off.

## Pre-Code (Research Handoff)

- [ ] All 10 research pipeline stages PASSED in `~/Developer/options_data/research/<study>/results_summary.md`
- [ ] `directives/receive_validated_strategy.md` checklist completed
- [ ] Champion parameters noted from `results_summary.md` Stage 3 section
- [ ] `backtest_oos.py` reviewed — bot signal logic will match it exactly

## Code Quality (from `directives/bot_code_qa.md`)

- [ ] Syntax checked — no import errors
- [ ] Function signatures match OpenAlgo REST API (not DuckDB schema)
- [ ] WebSocket auth sequence follows `directives/live_bot_websocket.md`
- [ ] Correct canonical index symbol names and exchanges
- [ ] State file written to `live_trading/{strategy}/logs/{strategy}_state.json`
- [ ] All paper trades logged to `live_trading/{strategy}/logs/paper_trades.csv`
- [ ] Telegram alerts via `live_trading/shared/telegram_notifier.py`
- [ ] **MIS/intraday bots only**: EOD exit constant ≤ `dt_time(15, 14)` — sandbox auto-squaresoff all MIS positions at 15:15; any close order after that silently fails and the trade is never logged. NRML bots are exempt.
- [ ] **NO internal `PAPER_MODE`-style flag anywhere in the bot** — `_order()` (or equivalent) must call `placeorder()` unconditionally, every time, with no env-var/flag gate. Paper vs. live is controlled exclusively by OpenAlgo's UI Sandbox/Analyze toggle — never by bot code. Grep your new file for `PAPER` before declaring it done; it should return nothing. (This exact bug shipped in `flat_blue_line_monthly_bot.py` on 2026-06-08 — see `docs/trading/bot-pipeline.md` Stage 12/13 for the full doctrine.)
- [ ] **Option-symbol resolution goes through `live_trading/shared/atm_resolver.py`** (or direct canonical-symbol string construction `f"{underlying}{expiry_str}{strike}{opt_type}"`) — never via `/api/v1/optionsymbol`'s `strike_int` passed an absolute strike. `strike_int` = strike INTERVAL (e.g. `50`/`100`), not an absolute strike. Grep your new file for `strike_int`; if present, verify it's `None` or an actual interval, not a computed ATM strike.

## Registration (Section 5b)

- [ ] Registered in `live_trading/start_all_bots.py` BOTS list
- [ ] Registered in `live_trading/streamlit_dashboard.py` — **all three** of:
  - [ ] `STATE_FILES` dict (maps bot key → state JSON path)
  - [ ] `BOT_META` dict (display name + research notes for the bot-status panel)
  - [ ] `render_portfolio_snapshot()` — add an `_add_open()`/`_add_closed()` block for each leg/symbol the bot trades (copy the nearest multi-leg bot's block, e.g. "BNF IF"). **Skipping this one is the most common miss** — the bot still "looks registered" via `STATE_FILES`/`BOT_META`, but its positions silently fall through to the generic "📊 Broker" catch-all instead of showing under the bot's name (shipped in `flat_blue_line_monthly_bot` on 2026-06-08).
- [ ] Registered in `live_trading/performance_review.py`
- [ ] (Equity bots only) Registered in `live_trading/market_review.py`
- [ ] Added to `live_trading/shared/bot_registry.py` `BOT_REGISTRY` (status=`"paper"`)
- [ ] **Dedicated decision-state sidebar page built — MANDATORY, not optional** (see full spec below)
- [ ] Documented in `live_trading/active_trading_bots.md`
- [ ] Run `live_trading/sanity_check.py` — it cross-checks the launcher registry against dashboard registration and will flag anything still missing

### Decision-State Sidebar Page — MANDATORY for every bot

Every bot gets its own dedicated multi-tab panel in `streamlit_dashboard.py` before it can be considered deployed. This is not an optional polish item — a bot without one does not pass this checklist, regardless of whether its trades otherwise show up correctly elsewhere in the dashboard. **Note to AI agents: when building or deploying a new bot, building this page is part of the bot's definition of done, in the same tier as registration and P&L logging — do not treat it as a follow-up or leave it for later.**

Template to copy: `render_nifty_gex_ict_v2_panel()` in `streamlit_dashboard.py` (most recent reference implementation). Build `render_<bot_name>_panel(ltps: dict)` with exactly five `st.tabs()`:

1. **📊 Overview** — position/leg cards, key metrics, banner reflecting current state (scanning / in position / closed), raw-state JSON expander
2. **🗺️ Strategy Flowchart** — `render_strategy_flowchart(title, caption, steps)` built from `fc_start()`, `fc_action()`, `fc_check()`, `fc_filter()`, `fc_entry()`, `fc_exit()`, `fc_split()`, `fc_note()` step builders, one step per actual decision point in the bot's logic
3. **🧠 Live Decision State** — shared `render_decision_state(state, key=..., updates_note=..., metrics=..., filters=..., readiness=...)` helper: metric cards, a pass/fail filter checklist mirroring the bot's actual gate conditions, and a colored readiness banner
4. **📖 Research Findings** — `render_research_findings_tab("<study_folder>/results_summary.md")`
5. **📈 Performance** — `render_bot_performance_tab("<bot_name>")` (no separate registration needed here beyond the `bot_registry.py` entry)

Wiring (three spots, all required):
- [ ] Nav label added to the relevant `_GRP_*` sidebar group list (`_GRP_OPT`/`_GRP_STK`/`_GRP_WK`/`_GRP_MO`)
- [ ] Nav label → bot-name mapping added to `_NAV_LABEL_TO_BOT`
- [ ] Routing `elif view == "<nav label>": render_<bot_name>_panel(ltps)` added to the view dispatch chain

Verify by starting the dashboard (`uv run streamlit run live_trading/streamlit_dashboard.py`), clicking the new nav item, and clicking through all 5 tabs to confirm none throw an error — do this before declaring the bot's dashboard work done, not after.

## P&L Logging (Section 5d)

- [ ] `log_trade_to_db()` called after every exit — no exceptions. The function auto-fetches `net_pnl` from the Fyers positionbook; bots do NOT need to compute costs manually.
- [ ] For **multi-leg bots** (iron fly, flat blue line): pass `net_pnl` explicitly by summing `fetch_closed_pnl(leg_symbol)` across all legs. Do not leave it as `None` — the formula fallback will be used, which is less accurate. (See TD-001 in `docs/trading/bot-pipeline.md`.)
- [ ] Never hardcode STT / exchange / brokerage rates in a bot. The shared logger owns that computation.

## Equity Bot Specifics (Section 5c)

- [ ] `log_trade_to_db()` called with `strategy_type="equity"` and explicit `direction`
- [ ] (Default `"options"` / `"sell"` would silently misclassify trades)

## Stage 11 Gate (before requesting sign-off)

- [ ] ≥ 20 trading sessions completed in sandbox mode
- [ ] Win rate within ±10% of OOS backtest expectation
- [ ] No single session loss > 2× expected ATR-based stop
- [ ] Net P&L positive over the full 20-session window
- [ ] Summary report comparing paper results to OOS backtest prepared

## Stage 12 — Go-Live

- [ ] Summary shared with Ramakrishna (trades CSV + key metrics)
- [ ] Explicit "go live" received in writing
- [ ] Flip OpenAlgo's own **Sandbox/Analyze Mode toggle in the UI** to live — bot code does not change, does not need redeploying, and has no flag to flip

---

## Bot Retirement Checklist

Use this whenever a bot is being decommissioned — whether rejected at research stage, underperforming in paper trading, or superseded by a better strategy. Skipping these steps leaves zombie processes and orphaned LaunchAgents that silently consume resources and create confusion (see: daily_sniper incident, 2026-05-23).

### Step 1 — Stop the running process

- [ ] If running via `start_all_bots.py`: comment out the bot entry and restart the launcher
- [ ] If the bot has its own LaunchAgent, stop it immediately:
  ```bash
  launchctl stop com.openalgo.<bot-name>
  ```

### Step 2 — Unload and remove the LaunchAgent (if one exists)

Check `~/Library/LaunchAgents/` for any plist referencing the bot:

```bash
ls ~/Library/LaunchAgents/ | grep openalgo
```

For each matching plist:

```bash
launchctl unload ~/Library/LaunchAgents/com.openalgo.<bot-name>.plist
rm ~/Library/LaunchAgents/com.openalgo.<bot-name>.plist
```

Also remove the source plist from the repo (or leave it archived under `live_trading/retired/`):

```bash
mv live_trading/com.openalgo.<bot-name>.plist live_trading/retired/
```

> **Why this matters:** If the LaunchAgent is not unloaded, macOS will keep restarting the bot at login. If the bot's working directory or log paths no longer exist, launchd will recreate those directories on every restart attempt — potentially after folder renames, drive remounts, or fresh clones.

### Step 3 — Deregister from all registration points

Mirror the deployment Registration checklist in reverse:

- [ ] Comment out (do **not** delete) the entry in `live_trading/start_all_bots.py` — add inline note: `# RETIRED YYYY-MM-DD — reason`
- [ ] Remove from `live_trading/streamlit_dashboard.py`
- [ ] Remove from `live_trading/performance_review.py`
- [ ] (Equity bots) Remove from `live_trading/market_review.py`
- [ ] Update status in `live_trading/active_trading_bots.md` — mark as **RETIRED** with date and reason

### Step 4 — Archive the bot code

Move the bot's folder and standalone `.py` file to `live_trading/retired/`:

```bash
mv live_trading/<bot_name>_bot/  live_trading/retired/
mv live_trading/<bot_name>_bot.py  live_trading/retired/
```

### Step 5 — Clean up oversized logs

Logs from rejected bots accumulate quickly (the daily_sniper error log reached 85 MB in two months). Before archiving:

```bash
# Check log sizes first
ls -lh live_trading/logs/<bot_name>*

# Truncate or remove logs that are no longer needed
rm live_trading/logs/<bot_name>_bot.log
rm live_trading/<bot_name>*.log          # catch any root-level log files
rm live_trading/logs/<bot_name>_state.json
```

### Step 6 — Archive research study (if applicable)

If the retirement is due to failed research validation:

- [ ] Confirm `results_summary.md` in the study folder documents the failure verdict and root cause
- [ ] If not already there, move the study folder to `options_data/research/rejected/`:
  ```bash
  mv ~/Developer/options_data/research/<study>/ \
     ~/Developer/options_data/research/rejected/
  ```
- [ ] The `rejected/` copy is the canonical archive — remove duplicates in `research/` root

### Step 7 — Verify nothing recreates the folder

After completing all steps above, confirm the folder stays gone:

```bash
# Delete any ghost folder
rm -rf ~/Developer/fyers_crk/openalgo/live_trading/<bot_name>_ghost_dir/

# Wait ~60 seconds, then check it hasn't come back
sleep 60 && ls ~/Developer/fyers_crk/openalgo/live_trading/ | grep <bot_name>
# Should return nothing
```

If the folder reappears, there is still a running process — recheck `launchctl list | grep openalgo` and `ps aux | grep <bot_name>`.

---

### Quick Reference — Retirement Commands

```bash
BOT=daily_sniper_bot   # change to target bot name

# 1. Stop & unload LaunchAgent
launchctl unload ~/Library/LaunchAgents/com.openalgo.${BOT}.plist 2>/dev/null
rm -f ~/Library/LaunchAgents/com.openalgo.${BOT}.plist

# 2. Archive code
mv live_trading/${BOT}/    live_trading/retired/ 2>/dev/null
mv live_trading/${BOT}.py  live_trading/retired/ 2>/dev/null
mv live_trading/com.openalgo.${BOT}.plist  live_trading/retired/ 2>/dev/null

# 3. Clean logs
rm -f live_trading/logs/${BOT}.log
rm -f live_trading/logs/${BOT%_bot}_state.json
rm -f live_trading/${BOT%_bot}*.log

# 4. Verify
sleep 60 && ls live_trading/ | grep ${BOT%%_*} || echo "Clean — no ghost folders"
```
