# ORB Champion — 30-Minute Opening Range Breakout Bot

## STATUS: RETIRED (2026-03-19)

This bot has been removed from the launcher (`start_all_bots.py`). The **15M version** (`orb_champion_15m_bot/`) is the active one.

**Reason for retirement**: Both the 15M and 30M bots traded the same symbol (BANKNIFTY ATM options). On most days, the breakout occurs within the first 15-minute range anyway, so the 30-minute range formed later gave the same (or worse) signal at the same entry price. Running both just doubled the position size unintentionally on the same trade.

---

## What It Did (for reference)

Same logic as the 15M bot, except:

| Parameter | 30M Bot | 15M Bot (Active) |
|---|---|---|
| Opening Range Window | 09:15 – 09:45 | 09:15 – 09:30 |
| Strategy name | `ORB_CHAMPION` | `ORB_CHAMPION_15M` |
| State file | `orb_30m_state.json` | `orb_state.json` |

All other parameters (BANKNIFTY, 900 qty, 20% SL, 15%/25% shield/trail, 15:00 entry cutoff, 15:25 EOD exit) were identical.

See `orb_champion_15m_bot/README.md` for the full parameter reference.
