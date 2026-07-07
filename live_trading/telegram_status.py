"""
Telegram /status Command Handler for Paper Trading Bots

Listens for /status commands via Telegram Bot API long polling.
Queries OpenAlgo API and returns live MTM status for all active strategies.

Usage:
    Runs as a background thread/process alongside the bots.
    Send /status or /s to your Telegram bot to get a report.
"""

import os
import sys
import json
import requests
import logging
import time
import threading
import random
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("live_trading/telegram_status.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment
load_dotenv()
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")

# BB Scanner shared state file (written by bb_5m_scanner.py)
BB_STATE_FILE = Path(__file__).parent / "logs" / "bb_signals_state.json"

# Confirm flag directory
CONFIRM_FLAGS_DIR = Path(__file__).parent / "logs"

# Friendly names for strategies
STRATEGY_MAP = {
    "PREOPEN_GAP_FADE":      "🎯 Pre-Open Gap Fade",
    "GAP_FADE_EOD":         "📉 Gap Fade EOD",
    "NIFTY_BB_OVERBOUGHT":   "🔵 Nifty BB Overbought",
    "NIFTY_TREND_SELLER":    "📈 Nifty Trend Seller",
    "SENSEX_TREND_SELLER":   "📉 Sensex Trend Seller",
    "HTF_PO3_SELL_PE":       "🧱 HTF PO3 (Short PE)",
    "EMA_SWING":             "🔄 EMA Swing Scanner",
    "BANKNIFTY_BB_OPTIONS":  "⚡ BankNifty BB Options",
    "HA_OPTIONS":            "🕯️ HA Options Bot",
    "NTS_OBI":               "🔬 NTS + OBI Gate",
    "NIFTY_MACD_MAP":        "🗺️ Nifty MACD Map",
    "NIFTY_EOD_HOLD":        "⌛ Nifty EOD Hold",
    "NIFTY_IRON_FLY_WEEKLY": "🦋 Nifty Iron Fly Weekly",
    "SENSEX_IRON_FLY_WEEKLY": "🦋 Sensex Iron Fly Weekly",
    "equity_obi":            "⚖️ Equity OBI Bot",
}


def _redact_token(text: str) -> str:
    """Redact the Telegram bot token if it appears inside exception text."""
    if not text:
        return text
    if TG_TOKEN:
        return text.replace(TG_TOKEN, "<redacted-token>")
    return text


def _safe_exc(exc: Exception) -> str:
    """Stringify exceptions without leaking Telegram credentials in URLs."""
    return _redact_token(str(exc))

def get_all_positions() -> List[Dict[str, Any]]:
    """Fetch all positions from OpenAlgo platform"""
    try:
        url = f"{HOST}/api/v1/positionbook"
        res = requests.post(url, json={"apikey": API_KEY}, timeout=10)
        if res.status_code == 200:
            data = res.json()
            if data.get("status") == "success":
                return data.get("data", [])
    except Exception as e:
        logger.error(f"Error fetching positions: {e}")
    return []

def format_status_message():
    """Build a dynamic status message grouping positions by strategy"""
    now = datetime.now().strftime("%d-%b-%Y %I:%M %p")
    positions = get_all_positions()
    
    # Group by strategy
    strat_data = {}
    
    for pos in positions:
        strat = pos.get("strategy", "UNKNOWN")
        if strat not in strat_data:
            strat_data[strat] = {"open": [], "realized": 0.0}
        
        # Track realized PnL
        strat_data[strat]["realized"] += float(pos.get("today_realized_pnl", 0))
        
        # Track open positions
        qty = float(pos.get("quantity") or pos.get("netqty") or 0)
        if qty != 0:
            # Use unrealized_pnl specifically for open positions to avoid double counting
            unrealized = float(pos.get("unrealized_pnl", 0))
            strat_data[strat]["open"].append({
                "symbol": pos.get("symbol"),
                "qty": qty,
                "avg": float(pos.get("avgprice") or pos.get("average_price") or 0),
                "ltp": float(pos.get("ltp", 0)),
                "pnl": unrealized,
                "product": pos.get("product")
            })

    if not strat_data:
        return f"📊 *LIVE TRADING STATUS*\n🕐 {now}\n\n📭 No bot activity recorded today."

    lines = [f"📊 *LIVE TRADING STATUS*", f"🕐 {now}", ""]
    
    total_unrealized = 0.0
    total_realized = 0.0

    # Sort strategies: those with open positions first, then by name
    sorted_strats = sorted(strat_data.keys(), key=lambda s: (len(strat_data[s]["open"]) == 0, s))

    for s_id in sorted_strats:
        data = strat_data[s_id]
        name = STRATEGY_MAP.get(s_id, f"🤖 {s_id}")
        
        lines.append(f"*{name}*")
        lines.append("━━━━━━━━━━━━━━━━━━")
        
        if not data["open"]:
            lines.append("   _No open positions_")
        else:
            for p in data["open"]:
                pnl = p["pnl"]
                total_unrealized += pnl
                pnl_emoji = "🟢" if pnl > 0 else "🔴" if pnl < 0 else "⚪"
                lines.append(f"📌 *{p['symbol']}* ({p['product']})")
                lines.append(f"   Avg: ₹{p['avg']:.2f} | LTP: ₹{p['ltp']:.2f}")
                lines.append(f"   Qty: {p['qty']:.0f} | {pnl_emoji} P&L: ₹{pnl:,.0f}")
        
        realized = data["realized"]
        total_realized += realized
        rel_emoji = "🟢" if realized > 0 else "🔴" if realized < 0 else "⚪"
        lines.append(f"   Today Realized: {rel_emoji} ₹{realized:,.0f}")
        lines.append("")

    # Summary
    total_pnl = total_unrealized + total_realized
    total_emoji = "🟢" if total_pnl > 0 else "🔴" if total_pnl < 0 else "⚪"
    
    lines.append("━━━━━━━━━━━━━━━━━━")
    lines.append(f"Unrealized MTM: ₹{total_unrealized:,.0f}")
    lines.append(f"Realized P&L:  ₹{total_realized:,.0f}")
    lines.append(f"*Total P&L: {total_emoji} ₹{total_pnl:,.0f}*")

    return "\n".join(lines)

def format_signals_message():
    """Read bb_signals_state.json and format a /signals reply."""
    now = datetime.now().strftime("%d-%b-%Y %I:%M %p")
    lines = [f"📡 *BB 5-Min Signal Status*", f"🕐 {now}", ""]

    if not BB_STATE_FILE.exists():
        lines.append("⚠️ Scanner state not found.")
        lines.append("_BB scanner may not be running yet._")
        return "\n".join(lines)

    try:
        payload = json.loads(BB_STATE_FILE.read_text())
    except Exception as e:
        lines.append(f"❌ Could not read state file: {e}")
        return "\n".join(lines)

    symbol_order = ["NIFTY", "BANKNIFTY", "SENSEX"]
    for sym in symbol_order:
        entry = payload.get(sym)
        if entry is None:
            lines.append(f"⚪ *{sym}* — No data")
            lines.append("")
            continue

        state       = entry.get("state")        # None / "OVERBOUGHT" / "OVERSOLD"
        meta        = entry.get("meta", {})
        last_updated = entry.get("last_updated", "—")
        ltp         = meta.get("ltp")

        if state == "OVERBOUGHT":
            upper = meta.get("upper", "—")
            triggered = meta.get("triggered_at", last_updated)
            lines.append(f"🚨 *{sym}* — OVERBOUGHT")
            lines.append(f"   LTP: ₹{ltp:.2f}  |  Upper Band: ₹{upper}")
            lines.append(f"   ⏱ Since {triggered}  |  Action: `Consider PE Entry`")
        elif state == "OVERSOLD":
            lower = meta.get("lower", "—")
            triggered = meta.get("triggered_at", last_updated)
            lines.append(f"🟢 *{sym}* — OVERSOLD")
            lines.append(f"   LTP: ₹{ltp:.2f}  |  Lower Band: ₹{lower}")
            lines.append(f"   ⏱ Since {triggered}  |  Action: `Consider CE Entry`")
        else:
            ltp_str = f"₹{ltp:.2f}" if ltp else "—"
            lines.append(f"⚪ *{sym}* — Inside Bands (Neutral)")
            lines.append(f"   LTP: {ltp_str}  |  Last update: {last_updated}")

        lines.append("")

    lines.append("_BB(45, 1.5) on 5-min bars_")
    return "\n".join(lines)

def send_telegram(message, chat_id=None):
    """Send a message via Telegram"""
    if not TG_TOKEN:
        logger.warning("No TELEGRAM_BOT_TOKEN set, skipping")
        return False
    target_chat = chat_id or TG_CHAT_ID
    if not target_chat:
        logger.warning("No TELEGRAM_CHAT_ID set, skipping")
        return False

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        res = requests.post(url, json={
            "chat_id": target_chat,
            "text": message,
            "parse_mode": "Markdown"
        }, timeout=10)
        return res.status_code == 200
    except Exception as e:
        logger.warning(f"Telegram send error: {_safe_exc(e)}")
        return False

def start_polling():
    """Start polling for Telegram /status commands"""
    if not TG_TOKEN:
        logger.warning("⚠️ TELEGRAM_BOT_TOKEN not set — /status handler disabled")
        return

    logger.info("🤖 Telegram /status handler started. Listening for commands...")
    last_update_id = 0
    network_failures = 0

    while True:
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
            params = {
                "offset": last_update_id + 1,
                "timeout": 30,  # Long polling
                "allowed_updates": ["message"]
            }
            res = requests.get(url, params=params, timeout=35)
            network_failures = 0

            if res.status_code == 409:
                # logger.warning("⚠️ Telegram Conflict (409): Another instance is likely running. Retrying in 30s...")
                time.sleep(30)
                continue
            elif res.status_code != 200:
                logger.warning(f"Telegram API polling returned HTTP {res.status_code}. Retrying shortly.")
                time.sleep(10)
                continue

            data = res.json()
            if not data.get("ok"):
                time.sleep(5)
                continue

            for update in data.get("result", []):
                last_update_id = update["update_id"]
                message = update.get("message", {})
                text = message.get("text", "").strip()
                chat_id = message.get("chat", {}).get("id")

                if text.lower() in ["/status", "/s"]:
                    logger.info(f"📨 /status command received from chat {chat_id}")
                    try:
                        status_msg = format_status_message()
                        send_telegram(status_msg, chat_id=str(chat_id))
                        logger.info("✅ Status sent successfully")
                    except Exception as e:
                        logger.error(f"Error generating status: {e}")
                        send_telegram(f"❌ Error generating status: {e}", chat_id=str(chat_id))

                elif text.lower() in ["/signals", "/sig"]:
                    logger.info(f"📨 /signals command received from chat {chat_id}")
                    try:
                        sig_msg = format_signals_message()
                        send_telegram(sig_msg, chat_id=str(chat_id))
                        logger.info("✅ Signals status sent successfully")
                    except Exception as e:
                        logger.error(f"Error generating signals status: {e}")
                        send_telegram(f"❌ Error reading signals: {e}", chat_id=str(chat_id))

                elif text.lower() == "/confirm_preopen":
                    flag_path = CONFIRM_FLAGS_DIR / "confirm_preopen.flag"
                    CONFIRM_FLAGS_DIR.mkdir(parents=True, exist_ok=True)
                    flag_path.touch()
                    logger.info(f"✅ /confirm_preopen flag written → {flag_path}")
                    send_telegram(
                        "✅ *Pre-Open Gap Fade* — Entry confirmed! Placing trades now…",
                        chat_id=str(chat_id),
                    )

                elif text.lower() == "/confirm_gapeod":
                    flag_path = CONFIRM_FLAGS_DIR / "confirm_gapeod.flag"
                    CONFIRM_FLAGS_DIR.mkdir(parents=True, exist_ok=True)
                    flag_path.touch()
                    logger.info(f"✅ /confirm_gapeod flag written → {flag_path}")
                    send_telegram(
                        "✅ *Gap Fade EOD* — Entry confirmed! Placing trades now…",
                        chat_id=str(chat_id),
                    )

                elif text.lower() == "/confirm_nifty_if":
                    flag_path = CONFIRM_FLAGS_DIR / "confirm_nifty_if.flag"
                    CONFIRM_FLAGS_DIR.mkdir(parents=True, exist_ok=True)
                    flag_path.touch()
                    logger.info(f"✅ /confirm_nifty_if flag written → {flag_path}")
                    send_telegram(
                        "✅ *Nifty Iron Fly Weekly* — Manual entry request received! Processing entry now…",
                        chat_id=str(chat_id),
                    )

                elif text.lower() in ["/help", "/start"]:
                    help_msg = (
                        "🤖 *Paper Trading Bot Commands*\n\n"
                        "/status or /s — View open positions & MTM\n"
                        "/signals or /sig — Live BB 5-min signal state\n"
                        "/confirm_preopen — Confirm late-start entry for Pre-Open Gap Fade bot\n"
                        "/confirm_gapeod — Confirm late-start entry for Gap Fade EOD bot\n"
                        "/confirm_nifty_if — Manually force-trigger entry for NIFTY Weekly Iron Fly\n"
                        "/help — Show this help message"
                    )
                    send_telegram(help_msg, chat_id=str(chat_id))

        except requests.exceptions.Timeout:
            continue  # Normal for long polling
        except requests.exceptions.RequestException as e:
            network_failures += 1
            backoff = min(60, 5 * network_failures) + random.uniform(0, 1)
            logger.warning(
                f"Polling warning: transient Telegram network failure "
                f"({type(e).__name__}: {_safe_exc(e)}). Retrying in {backoff:.1f}s."
            )
            time.sleep(backoff)
        except Exception as e:
            logger.error(f"Polling error: {_safe_exc(e)}")
            time.sleep(5)

def start_in_thread():
    """Start the Telegram polling in a daemon thread"""
    thread = threading.Thread(target=start_polling, daemon=True, name="TelegramStatusHandler")
    thread.start()
    return thread

if __name__ == "__main__":
    start_polling()
