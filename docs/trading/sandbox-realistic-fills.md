# Sandbox realistic fills + gross/net reporting (flag `sandbox_realistic_fills`)

## What changed
- Dashboard (fleet cards, Performance tab, Hub, recent trades) now shows gross AND net P&L from performance.db. Net shows "n/a" where a bot never logged it (multi-leg DEBIT_SPREAD / IRON_FLY) and "≈" when only some trades have net.
- `sandbox/realistic_fills.py`, behind sandbox_config `sandbox_realistic_fills` (default **false**; behaviour unchanged until set to true):
  - MARKET fills at the far touch; if bid==ask==ltp (WebSocket path) -> LTP ± slippage tier (0.3% 09:15-09:45, 0.2% 15:00-15:40, else 0.1%, mirrors research lib/costs), tick-rounded.
  - SL-M fills at the far touch if already beyond trigger, else trigger ± slippage overshoot.
  - Stop watchdog (`sandbox_stop_watchdog_interval`, default 2s, WS engine thread + polling path): re-checks "trigger pending" orders against the quote's day high/low. Highs/lows only widen, so a stop is flagged crossed iff the extreme passed the trigger after the order was first seen. No broker history call needed.
- Tests: `test/sandbox/test_realistic_fills.py` (9) incl. 14 Oct PE replay (trigger 167.88, spike to 171.8 between polls: flag off = missed, flag on = fires at >= trigger).

## Reconciliation, nifty_atm_straddle_scalp_bot (sandbox) vs cs_fyers paper, 2026-09-28..10-15
| date | cs_fyers paper | sandbox gross | sandbox net | gross − cs |
|---|---:|---:|---:|---:|
| 2026-09-28 | 1,488 | 14,918 | 14,618 | 13,429 |
| 2026-09-29 | -894 | -8,482 | -8,722 | -7,589 |
| 2026-09-30 | -16,380 | -16,802 | -17,244 | -422 |
| 2026-10-01 | 16,120 | 15,112 | 14,727 | -1,008 |
| 2026-10-05 | -12,188 | -10,920 | -11,253 | 1,268 |
| 2026-10-06 | 14,722 | 15,275 | 15,062 | 552 |
| 2026-10-07 | -20,345 | -19,695 | -20,150 | 650 |
| 2026-10-08 | 14,690 | 16,088 | 15,696 | 1,398 |
| 2026-10-09 | -16,478 | -15,015 | -15,413 | 1,462 |
| 2026-10-12 | 14,592 | 15,795 | 15,486 | 1,202 |
| 2026-10-13 | -8,678 | -9,490 | -9,719 | -812 |
| 2026-10-14 | -21,028 | 15,308 | 14,882 | 36,335 |
| 2026-10-15 | 14,722 | 15,015 | 14,618 | 292 |
| **total** | **-19,653** | **27,105** | **22,587** | **46,758** |

Reading: excluding 09-28, 09-29 (cs_fyers paper start-up days) and 10-14, per-day gross differences are ±0.3-1.5k (~4-8% of a win/loss day); sandbox is systematically slightly better on target days. 10-14 alone is +36k and is the missed-stop case (engine never fired SL-M; bot's own 30s SL GUARD later cancelled it). Net−gross is ~Rs 300-450/day. This is a model reconciliation of existing data: the watchdog has not been run live; enabling by default awaits the user's go-ahead.

## Remaining differences
- 09-28/09-29: cs_fyers paper start-up days (partial sessions / different entry), not fill model.
- Watchdog fills at trigger+overshoot, not the true intra-poll print; cs_fyers fills at the polling-cycle ask.
- Baseline day-extreme is taken at first sighting; crossings before it (or engine restart mid-order) fall back to the tick path.
- WS path carries LTP only (no depth), so MARKET fills use a slippage tier, not real spread.
- Net is unavailable for multi-leg bots (per-leg premiums not logged); sandbox_orders/trades is not costed by the engine itself, net comes from trade_logger's cost model.
- Sandbox engine logging is debug-level, so why SL-M missed on 13 Oct (x2) / 14 Oct could not be proven from logs; day-extreme watchdog covers it regardless of cause.
