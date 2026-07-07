# Temporary changes — Flat Blue Line Monthly Bot (REVERT TRACKER)

Single source of truth for every temporary relaxation/override applied during
the 2026-06-08 live test, so none get forgotten. Check this file before/after
the test and revert each row once its "Revert when" condition is met.

| # | What was changed | Where | Revert when | Status |
|---|---|---|---|---|
| 1 | Entry window widened from 10:00–10:20 to 10:00–13:00 IST, **date-guarded to `date(2026, 6, 8)` only** | `flat_blue_line_monthly_bot.py` → `_check_entry()` | Once today's test trades are confirmed live in OpenAlgo Positions | ✅ **Reverted 2026-06-08 ~12:25** — both NIFTY (6 legs, entered ~12:16-12:17) and BANKNIFTY (6 legs, entered ~12:18-12:19) confirmed live in `flat_blue_line_monthly_state.json` with real `entry_prem` values and in the CSV with real order timestamps. Block deleted; restored single-line check `if not (t.hour == ENTRY_HOUR and t.minute < ENTRY_WINDOW_MINS): return`. Compile verified OK. |
| 2 | Bot's auto-restart **disabled** by stopping it via the dashboard command path (`bot_commands.json` → `{"action":"stop","bot":"Flat Blue Line Monthly Bot"}`) instead of `kill <pid>`, so the launcher's `monitor_bots()` doesn't treat it as a crash and respawn it | `live_trading/logs/bot_commands.json` (consumed/cleared by launcher within ~5s; registry showed `status: "stopped"`) | Once you're ready to resume | ✅ Done — user restarted via dashboard; bot is running normally again (PID 17746+ → now whatever PID the latest restart assigned), normal auto-restart-on-crash behaviour is back in effect |
| 3 | Reset `flat_blue_line_monthly_state.json` to `{}` (was holding phantom `closed:false` entries for NIFTY+BANKNIFTY from the morning's fake "PAPER" trades that never reached OpenAlgo) | `live_trading/logs/flat_blue_line_monthly_state.json` | N/A — this is a one-time cleanup, not a relaxation. No revert needed; the bot will rebuild state correctly from its next real entry. | ✅ Done |
| 4 | Cleared `flat_blue_line_monthly_paper_trades.csv` (every row was a phantom entry from the same failed run) | `live_trading/logs/flat_blue_line_monthly_paper_trades.csv` | N/A — one-time cleanup. New header (without the now-removed `paper_mode` column) will be written automatically on the next real trade. | ✅ Done |

---

## Permanent fixes applied today (NOT temporary — do not revert)

For context/cross-reference, these were also fixed today in the same file but are
permanent corrections, not temporary relaxations:

- `_get_monthly_expiries()` — replaced gap-based heuristic with calendar-month grouping (fixes NIFTY weekly/monthly misdetection)
- `_resolve_symbol()` — now builds the OpenAlgo canonical symbol directly instead of misusing `/api/v1/optionsymbol`'s `strike_int` (which is a strike INTERVAL, not an absolute strike) — this was the root cause of "ATM symbols unresolved"
- Removed the internal `PAPER_MODE` flag from `_order()` entirely — bots must always fire real OpenAlgo REST orders; OpenAlgo's own Sandbox/Analyze mode (UI toggle) handles simulation. The flag was silently faking "success" without ever calling `placeorder()`, so trades never reached OpenAlgo
- Added `_multiquote()` and batched all leg-price/spot fetches (entry + monitoring) into single `/api/v1/multiquotes` round-trips instead of 6-14 sequential `/api/v1/quotes` calls — fixes the "Read timed out" errors

## Restart sequence (once you're ready to re-test)

1. Confirm bot is stopped: registry shows `status: "stopped"`, `pid: null` ✅ (already true)
2. Restart via dashboard "Start" (or `{"action":"start", ...}` command) — it will load the corrected code AND the cleaned state, and re-attempt entry within the relaxed 10:00–13:00 window (#1 above)
3. Watch `flat_blue_line_monthly_bot.log` for `"PAPER BUY/SELL"` lines — **these should be GONE now**; you should instead see `"BUY <symbol> x<qty>: {...}"` with a real order response, and the trade should appear in OpenAlgo Positions within seconds
4. Once confirmed in Positions — come back and revert #1 (delete the temporary date-guard block)
