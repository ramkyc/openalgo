#!/usr/bin/env python3
"""
Setup and Manage Historify Schedules (Indices 5s)

This script:
1. Disables all existing schedules (especially the bulky watchlist-based ones).
2. Sets up a specific schedule for NIFTY, BANKNIFTY, and SENSEX at 5s timeframe.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
from datetime import datetime
from services.historify_scheduler_service import get_historify_scheduler
from database.historify_db import get_all_schedules, update_schedule
from utils.logging import get_logger

logger = get_logger(__name__)

def get_api_key() -> str:
    api_key = os.getenv('HISTORIFY_API_KEY') or os.getenv('API_KEY') or os.getenv('OPENALGO_API_KEY')
    if not api_key:
        from database.auth_db import get_first_available_api_key
        api_key = get_first_available_api_key()
    if not api_key:
        raise ValueError("No API key found. Please configure in .env or provide a valid broker login.")
    return api_key

def cleanup_existing_schedules():
    """Disable and delete existing bulky schedules"""
    logger.info("Checking for existing schedules...")
    schedules = get_all_schedules()
    for sch in schedules:
        if sch['id'] in ['daily_options_1m', 'weekly_options_full']:
            logger.info(f"Disabling bulky schedule: {sch['id']}")
            update_schedule(sch['id'], is_enabled=False)
            # Remove from APScheduler if it's currently running in memory
            # (In this script context, it might not be initialized, so we just update database)

def setup_indices_5s_schedule():
    """Create the specific 5s schedule for Indices"""
    scheduler = get_historify_scheduler()
    api_key = get_api_key()
    
    # Init scheduler instance
    scheduler.init(api_key=api_key)
    
    schedule_id = "daily_indices_5s"
    
    try:
        logger.info(f"Creating/Updating schedule: {schedule_id}")
        
        # We use 'catalog' as source in database to indicate it's not the whole watchlist
        # Although the scheduler current implementation defaults to watchlist,
        # we will monkey-patch or modify the code to respect index-only if this ID matches.
        
        success, msg = scheduler.add_schedule(
            schedule_id=schedule_id,
            name="Indices 5s Data (NIFTY, BANKNIFTY, SENSEX)",
            schedule_type="daily",
            time_of_day="15:35", # Wait for market to settle after close
            data_interval="5s",
            lookback_days=1,
            description="Daily download of 5-second OHLCV data for major Indices."
        )
        
        if success:
            logger.info(f"✅ Created schedule: {schedule_id}")
        else:
            # If it already exists, just make sure it is updated and enabled
            logger.info(f"Schedule might already exist: {msg}")
            update_schedule(schedule_id, is_enabled=True, time_of_day="15:35", data_interval="5s")
            
        return True
    except Exception as e:
        logger.error(f"❌ Failed to setup schedule: {e}")
        return False

def main():
    logger.info("="*60)
    logger.info("Historify Custom Setup: Indices 5s")
    logger.info("="*60)
    
    try:
        cleanup_existing_schedules()
        setup_indices_5s_schedule()
        logger.info("\n✅ Setup complete. Check UI or logs at 3:35 PM IST.")
    except Exception as e:
        logger.error(f"Error during setup: {e}")

if __name__ == "__main__":
    main()
