# Equity OBI Bot - Modifications Log

## Date: 2026-05-01

## Changes Made

### Entry Trading Window Adjustment
**Previous Configuration:**
- Entry Start: 10:00 AM
- Entry End: 13:00 PM (1:00 PM)
- EOD Exit Time: 15:20 (3:20 PM)

**New Configuration:**
- Entry Start: 09:25 AM (9:25 AM)
- Entry End: 15:15 (3:15 PM)
- EOD Exit Time: 15:25 (3:25 PM)

### Rationale
- Extended entry window to capture more intraday opportunities starting earlier in the session
- Added 55 minutes of trading time for entries (15:15 instead of 13:00)
- Maintained 10-minute buffer before broker auto-square-off (15:25 instead of 15:20)

### Files Modified
- `equity_obi_bot.py`:
  - `ENTRY_START` changed from `dt_time(10, 0)` to `dt_time(9, 25)`
  - `ENTRY_END` changed from `dt_time(13, 0)` to `dt_time(15, 15)`
  - Removed redundant PM signal check cron job (merged into 10:00-13:00+14:00+15:00 window)
  - `EOD_EXIT_TIME` changed from `dt_time(15, 20)` to `dt_time(15, 25)`

### Unchanged Behavior
- Existing open positions are automatically closed by 15:25 (EOD exit)
- All stop-loss and target logic remains unchanged
- Paper mode configuration unchanged
- Telegram notifications still active

---
*Note: This bot operates in paper trading mode (Stage 11)*
