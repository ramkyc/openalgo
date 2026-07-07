import os
import sys
from datetime import datetime, timezone
import logging

try:
    from services.historify_scheduler_service import get_historify_scheduler
    from database.historify_db import get_connection, get_watchlist
except ImportError:
    # Add project root to path if running manually
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from services.historify_scheduler_service import get_historify_scheduler
    from database.historify_db import get_connection, get_watchlist

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_today_expiry():
    """
    Checks if any symbol in the watchlist expires today.
    """
    # Expiry format in symbols is often DDMMMYY (e.g., 16FEB26)
    today_str = datetime.now().strftime("%d%b%y").upper()
    
    watchlist = get_watchlist()
    for item in watchlist:
        symbol = item.get('symbol', '')
        if today_str in symbol:
            logger.info(f"Found today's expiry symbol in watchlist: {symbol}")
            return True
    return False

def update_eod_schedule():
    scheduler = get_historify_scheduler()
    # Note: Scheduler requires an API key for initial initialization if not already initialized
    api_key = os.getenv("OPENALGO_API_KEY")
    scheduler.init(api_key=api_key)
    
    schedule_id = "daily_options_1m"
    
    if is_today_expiry():
        download_time = "15:25"
        reason = "Expiry Day detected"
    else:
        download_time = "15:35"
        reason = "Non-Expiry Day"
        
    logger.info(f"Calculated EOD Download Time: {download_time} ({reason})")
    
    # Update schedule
    success, msg = scheduler.update_schedule(
        schedule_id, 
        time_of_day=download_time,
        is_enabled=True,
        is_paused=False
    )
    
    if success:
        logger.info(f"✅ Schedule 'daily_options_1m' updated to {download_time} IST. Enabled: True")
    else:
        logger.error(f"❌ Failed to update schedule: {msg}")

if __name__ == "__main__":
    update_eod_schedule()
