"""
Shared Telegram notification helper for live_trading bots.
Provides both sync and async send, with 2-attempt retry.
Also provides wait_for_confirm() for interactive late-start confirmation.
"""

import asyncio
import logging
import os
import time
import requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent.parent / ".env")

TG_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

logger = logging.getLogger(__name__)


async def send_async(message: str, chat_id: str = None) -> bool:
    """Send a Telegram message asynchronously (non-blocking)."""
    if not TG_TOKEN:
        return False
    target = chat_id or TG_CHAT_ID
    if not target:
        return False
    url     = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": target, "text": message, "parse_mode": "Markdown"}
    for attempt in range(2):
        try:
            await asyncio.to_thread(requests.post, url, json=payload, timeout=10)
            return True
        except Exception as e:
            if attempt == 0:
                logger.warning(f"Telegram attempt 1 failed: {e}. Retrying…")
                await asyncio.sleep(1)
            else:
                logger.error(f"Telegram failed after 2 attempts: {e}")
    return False


def send_sync(message: str, chat_id: str = None) -> bool:
    """Send a Telegram message synchronously."""
    if not TG_TOKEN:
        return False
    target = chat_id or TG_CHAT_ID
    if not target:
        return False
    url     = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    payload = {"chat_id": target, "text": message, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        return r.status_code == 200
    except Exception as e:
        logger.error(f"Telegram send error: {e}")
        return False


def wait_for_confirm(command: str, timeout_secs: int = 600) -> bool:
    """
    Wait for a confirmation flag file to appear.

    telegram_status.py's polling loop writes a flag file when it receives
    `command` from Telegram.  This function simply watches for that file —
    no second getUpdates call, no Telegram 409 Conflict.

    Flag file location:
        live_trading/logs/{command_without_slash}.flag
    Example:
        '/confirm_preopen'  →  live_trading/logs/confirm_preopen.flag
        '/confirm_gapeod'   →  live_trading/logs/confirm_gapeod.flag

    Call send_sync() to notify the user BEFORE calling this so they know
    what to reply with.
    """
    flag_name = command.strip().lstrip("/").lower() + ".flag"
    # __file__ is live_trading/shared/telegram_notifier.py
    #   .parent      → live_trading/shared/
    #   .parent.parent → live_trading/
    flag_path = Path(__file__).parent.parent / "logs" / flag_name

    # Remove any stale flag left over from a previous session
    if flag_path.exists():
        try:
            flag_path.unlink()
            logger.info(f"🗑️  Removed stale flag: {flag_path.name}")
        except Exception as e:
            logger.warning(f"Could not remove stale flag {flag_path.name}: {e}")

    deadline = time.time() + timeout_secs
    logger.info(
        f"⏳ Waiting up to {timeout_secs}s for Telegram command '{command}' "
        f"(watching flag: {flag_path.name}) …"
    )

    while time.time() < deadline:
        if flag_path.exists():
            try:
                flag_path.unlink()
            except Exception:
                pass
            logger.info(f"✅ Telegram confirm received: '{command}'")
            return True
        time.sleep(2)   # poll every 2 s — no API calls, no 409 risk

    logger.info(f"⏰ Confirm window expired after {timeout_secs}s — no '{command}' received")
    return False
