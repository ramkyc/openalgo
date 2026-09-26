"""
Fyers 50-Level Market Depth Monitor (Dynamic Alpha Engine)

Features:
1. Continuous 50-level analysis for Dynamic Entry (no fixed time).
2. Patterns: OBI Extremes, VWMP Divergence, and Institutional Wall Support.
3. Virtual Trade Tracker: Marks entries live and tracks them to SL/Target.
4. Telegram Notifications for Virtual Entry, Exit, and PnL.

Usage:
    uv run live_trading/depth_monitor.py
"""

import asyncio
import os
import sys
import logging
import numpy as np
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, List, Any

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from openalgo import api

# --- Configuration ---
load_dotenv()
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST = os.getenv("HOST_SERVER", "http://127.0.0.1:8080")
WS_URL = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:8765")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Dynamic Entry Constants
# Index-Specific Configurations
SYMBOL_CONFIGS = {
    "NIFTY": {
        "obi_threshold": 35.0,
        "wall_strength": 6.0,
        "sl_buffer": 0.996  # 0.4%
    },
    "SENSEX": {
        "obi_threshold": 45.0,  # Increased to 45% because we only have 5-level depth
        "wall_strength": 15.0,  # Increased to 15x to ensure it is a real wall in 5-level data
        "sl_buffer": 0.995      # 0.5% (slightly wider for SENSEX volatility)
    },
    "DEFAULT": {
        "obi_threshold": 40.0,
        "wall_strength": 10.0,
        "sl_buffer": 0.996
    }
}

COOLDOWN_SECONDS = 300 # 5 mins between trades per symbol

# Current active expiring options (May 2026)
# Adjust these dates based on when you want depth monitoring
MONITOR_SYMBOLS = [
    {"exchange": "NSE", "symbol": "RELIANCE:50"},              # Equity - always available
    {"exchange": "NFO", "symbol": "NIFTY21MAY2625000CE:50"},  # NIFTY ATM CE
    {"exchange": "NFO", "symbol": "NIFTY21MAY2625000PE:50"},  # NIFTY ATM PE
    {"exchange": "BFO", "symbol": "SENSEX21MAY268000CE:50"},  # SENSEX ATM CE
    {"exchange": "BFO", "symbol": "SENSEX21MAY268000PE:50"},  # SENSEX ATM PE
]

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("live_trading/depth_monitor.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DynamicDepthEngine:
    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST, ws_url=WS_URL, verbose=True)
        self.active_trades = {}  # symbol -> trade_data
        self.last_trade_time = {} # symbol -> timestamp
        self.wall_alerts_sent = {}
        self.last_m = {} # symbol -> previous metrics for confirmation
        self.loop = None
        
    async def send_telegram(self, message):
        if not TG_TOKEN or not TG_CHAT_ID: return
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        try:
            await asyncio.to_thread(
                requests.post, url, 
                json={"chat_id": TG_CHAT_ID, "text": f"💠 *L3 DYNAMIC ENGINE*\n{message}", "parse_mode": "Markdown"}, 
                timeout=5
            )
        except Exception as e:
            logger.error(f"Telegram error: {e}")

    def calculate_metrics(self, data: Dict[str, Any]):
        # OpenAlgo SDK wraps the actual depth data in a 'data' field
        payload = data.get('data', {})
        symbol = data.get('symbol', 'Unknown')
        depth = payload.get('depth', {})
        bids = depth.get('buy', [])
        asks = depth.get('sell', [])
        ltp = payload.get('ltp', 0)
        
        if not bids or not asks: return None

        total_bid_qty = sum(b['quantity'] for b in bids)
        total_ask_qty = sum(a['quantity'] for a in asks)
        
        # Weighted Imbalance (L3 OBI)
        weighted_bid = sum(b['quantity'] / (i+1) for i, b in enumerate(bids))
        weighted_ask = sum(a['quantity'] / (i+1) for i, a in enumerate(asks))
        obi = (weighted_bid - weighted_ask) / (weighted_bid + weighted_ask) * 100 if (weighted_bid + weighted_ask) > 0 else 0
        
        # VWMP (Volume Weighted Mid-Price)
        vwmp = (sum(b['price']*b['quantity'] for b in bids) + sum(a['price']*a['quantity'] for a in asks)) / (total_bid_qty + total_ask_qty)
        
        # Get config for this symbol
        matching_key = "DEFAULT"
        for key in SYMBOL_CONFIGS:
            if key in symbol:
                matching_key = key
                break
        cfg = SYMBOL_CONFIGS[matching_key]

        # Wall Discovery
        median_qty = np.median([b['quantity'] for b in bids] + [a['quantity'] for a in asks])
        walls = [{'side': 'BID' if l in bids else 'ASK', 'price': l['price'], 'qty': l['quantity'], 'lvl': i+1} 
                 for i, l in enumerate(bids+asks) if l['quantity'] > (median_qty * cfg['wall_strength'])]

        return {
            'symbol': symbol, 'ltp': ltp, 'vwmp': vwmp, 'obi': obi, 
            'walls': walls, 'total_bid': total_bid_qty, 'total_ask': total_ask_qty
        }

    async def _process_depth_update(self, data):
        symbol = data.get('symbol', 'Unknown')
        logger.info(f"Received depth update for {symbol}")
        m = self.calculate_metrics(data)
        if not m: return

        # 1. Update Active Virtual Trades
        symbol = m['symbol']
        if symbol in self.active_trades:
            await self.track_virtual_trade(m)
        else:
            # 2. Check for Dynamic Entry Signal
            await self.check_entry_signal(m)

        # 3. Store current metrics for next tick confirmation
        self.last_m[symbol] = m
        self.display_dashboard(m)

    def on_depth_update(self, data):
        """Thread-safe sync wrapper for the async depth processor"""
        if self.loop and self.loop.is_running():
            asyncio.run_coroutine_threadsafe(self._process_depth_update(data), self.loop)
        else:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._process_depth_update(data))
                else:
                    asyncio.run(self._process_depth_update(data))
            except Exception as e:
                logger.error(f"Error scheduling depth update: {e}")

    async def check_entry_signal(self, m):
        now = datetime.now()
        symbol = m['symbol']
        
        # Cooldown check
        if symbol in self.last_trade_time and (now - self.last_trade_time[symbol]).total_seconds() < COOLDOWN_SECONDS:
            return

        signal = None
        sl = 0
        target = 0
        reason = ""

        # Get config for this symbol
        matching_key = "DEFAULT"
        for key in SYMBOL_CONFIGS:
            if key in symbol:
                matching_key = key
                break
        cfg = SYMBOL_CONFIGS[matching_key]

        # Optimization 1: 2-Tick OBI Momentum Confirmation
        prev_m = self.last_m.get(symbol)
        if not prev_m: return

        # Entry Hypothesis
        # 1. LONG: OBI High + VWMP Support + Bid Wall Proximity
        if m['obi'] > cfg['obi_threshold'] and prev_m['obi'] > cfg['obi_threshold'] and m['vwmp'] > m['ltp']:
            bid_walls = [w for w in m['walls'] if w['side'] == 'BID' and w['price'] < m['ltp'] and w['lvl'] <= 10]
            if bid_walls:
                signal = "LONG"
                # Use symbol-specific SL buffer
                sl = bid_walls[0]['price'] * cfg['sl_buffer']
                target = m['ltp'] + (m['ltp'] - sl) * 2
                reason = f"L3 Bullish Imbalance ({m['obi']:.1f}%) + Wall at lvl {bid_walls[0]['lvl']}"

        if signal:
            self.active_trades[symbol] = {
                'side': signal, 'entry': m['ltp'], 'sl': sl, 'target': target,
                'start_time': now, 'reason': reason
            }
            self.last_trade_time[symbol] = now
            msg = (f"🚀 *DYNAMIC ENTRY TRIGGERED*\n"
                   f"Symbol: {symbol}\nAction: {signal}\nEntry: ₹{m['ltp']:.2f}\n"
                   f"SL: ₹{sl:.2f}\nTarget: ₹{target:.2f}\n"
                   f"Reason: {reason}")
            await self.send_telegram(msg)
            logger.info(f"VIRTUAL {signal} ENTRY: {symbol} at {m['ltp']}")

    async def track_virtual_trade(self, m):
        trade = self.active_trades[m['symbol']]
        symbol = m['symbol']
        ltp = m['ltp']
        exit_triggered = False
        result = ""

        if trade['side'] == 'LONG':
            if ltp <= trade['sl']:
                exit_triggered, result = True, "❌ STOP LOSS HIT"
            elif ltp >= trade['target']:
                exit_triggered, result = True, "✅ TARGET ACHIEVED"
        else: # SHORT
            if ltp >= trade['sl']:
                exit_triggered, result = True, "❌ STOP LOSS HIT"
            elif ltp <= trade['target']:
                exit_triggered, result = True, "✅ TARGET ACHIEVED"

        if exit_triggered:
            # Atomic pop to prevent duplicate triggers from high-frequency updates
            trade = self.active_trades.pop(symbol, None)
            if not trade: return 

            pnl = (m['ltp'] - trade['entry']) if trade['side'] == 'LONG' else (trade['entry'] - m['ltp'])
            msg = (f"🏁 *VIRTUAL TRADE COMPLETED*\n"
                   f"Symbol: {symbol}\nResult: {result}\n"
                   f"Exit Price: ₹{m['ltp']:.2f} | PnL: ₹{pnl:.2f}\n"
                   f"Duration: {(datetime.now() - trade['start_time']).total_seconds()/60:.1f} mins")
            await self.send_telegram(msg)
            logger.info(f"VIRTUAL EXIT: {symbol} Result: {result}")

    def display_dashboard(self, m):
        sentiment = "🟢 BULLISH" if m['obi'] > 25 else ("🔴 BEARISH" if m['obi'] < -25 else "⚪ NEUTRAL")
        vwmp_diff = m['vwmp'] - m['ltp']
        
        dashboard = (
            f"\n{'='*50}\n"
            f"📊 DYNAMIC L3: {m['symbol']} | {datetime.now().strftime('%H:%M:%S')}\n"
            f"Sentiment: {sentiment} ({m['obi']:.1f}% OBI)\n"
            f"LTP: {m['ltp']:.2f} | VWMP: {m['vwmp']:.2f} (Diff: {vwmp_diff:.4f})\n"
        )
        
        if m['symbol'] in self.active_trades:
            t = self.active_trades[m['symbol']]
            dashboard += f"🎯 ACTIVE TRADE: {t['side']} @ {t['entry']} | Tgt: {t['target']} | SL: {t['sl']}\n"
        
        if m['walls']:
            dashboard += f"{'-' * 15} LIQUIDITY WALLS {'-' * 18}\n"
            for wall in m['walls'][:2]:
                dashboard += f"🧱 {wall['side']} Wall at {wall['price']:.2f} (x{int(wall['qty'])} units)\n"
        dashboard += "="*50
        
        logger.info(dashboard)

    async def run(self):
        self.loop = asyncio.get_running_loop()
        logger.info("Starting Dynamic Level 3 Alpha Engine...")
        self.client.connect()
        self.client.subscribe_depth(MONITOR_SYMBOLS, on_data_received=self.on_depth_update)
        try:
            last_heartbeat = 0
            while True:
                now = time.time()
                if now - last_heartbeat > 60:
                    logger.info("💓 Engine Heartbeat: Listening for L3 Depth updates...")
                    last_heartbeat = now
                await asyncio.sleep(1)
        finally: self.client.disconnect()

if __name__ == "__main__":
    try: asyncio.run(DynamicDepthEngine().run())
    except KeyboardInterrupt: pass
