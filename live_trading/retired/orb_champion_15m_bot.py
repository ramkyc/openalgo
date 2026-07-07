#!/usr/bin/env python3
"""
ORB Champion: 30-Min Opening Range Breakout Live Bot
Logic:
- Entry: Index (BANKNIFTY) breaks above/below the first 30-min range (09:15-09:45).
- Instrument: Buy 1 Lot (15 Qty) of ATM Call/Put based on breakout direction.
- Exit: EOD (15:15) or manual override.
- Safety: Only ONE trade per day.
"""

import asyncio
import websockets
import json
import os
import sys
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, time, timedelta
from dotenv import load_dotenv
from typing import Optional, Dict

# Ensure we can import from project root
PROJECT_ROOT = "/Users/ramakrishna/Developer/fyers_crk/openalgo"
sys.path.append(PROJECT_ROOT)

from openalgo import api
from live_trading.api_utils import (
    get_expiry_dates, 
    get_option_symbol, 
    get_appropriate_expiry_for_day,
    get_history,
    is_market_holiday
)

# --- Configuration ---
STRATEGY_NAME = "ORB_CHAMPION_15M"
SYMBOL = "BANKNIFTY"
EXCHANGE = "NSE_INDEX"
OPT_EXCHANGE = "NFO"
QUANTITY = 900  # Freeze Quantity for BANKNIFTY

RANGE_START = time(9, 15)
RANGE_END = time(9, 30)
EXIT_TIME = time(15, 25)
STOP_LOSS_PCT = 0.20  

# Profit Locking (Shield & Trail)
SHIELD_THRESHOLD_PCT = 0.15   # 15% Profit -> Move SL to Break-Even
TRAIL_THRESHOLD_PCT = 0.25    # 25% Profit -> Activate 10% TSL
TRAIL_SL_PCT = 0.10           # 10% Trailing from Peak

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Environment
load_dotenv()
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
WS_URL = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

class ORBChampionBot:
    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.strategy = "ORB_CHAMPION_15M"
        self.bot_id = "15-Min"
        self.range_high = None
        self.range_low = None
        self.range_ready = False
        self.trade_taken = False
        self.active_position = None
        self.index_ltp = 0.0
        self.last_index_ltp = None
        self.websocket = None
        
    async def check_for_existing_positions(self):
        """Adopt existing ORB positions from the broker if found"""
        try:
            # Using direct request to match the successful manual check
            payload = {"apikey": API_KEY}
            res = await asyncio.to_thread(requests.post, f"{HOST}/api/v1/positionbook", json=payload, timeout=10)
            data = res.json()
            
            if data.get('status') != 'success':
                return False
            
            positions = data.get('data', [])
            for pos in positions:
                # Check for active position belonging to this strategy
                if pos.get('strategy') == STRATEGY_NAME and float(pos.get('quantity', 0)) != 0:
                    symbol = pos.get('symbol')
                    avg_price = float(pos.get('average_price', 0))
                    qty = min(abs(float(pos.get('quantity', 0))), QUANTITY)
                    
                    # Calculate SL based on established entry
                    sl_price = avg_price * (1 - STOP_LOSS_PCT)
                    
                    self.active_position = {
                        'symbol': symbol,
                        'entry_price': avg_price,
                        'sl_price': sl_price,
                        'initial_sl': sl_price, # Set initial SL during adoption
                        'quantity': qty,
                        'current_price': float(pos.get('ltp', 0)),
                        'max_price': float(pos.get('ltp', 0)),
                        'shield_active': False,
                        'trail_active': False
                    }
                    self.trade_taken = True
                    logger.info(f"🛡️ Adopted existing position: {symbol} @ {avg_price}. SL: {sl_price}")
                    await self.send_telegram(f"🛡️ *{self.bot_id}* Adopted existing position: `{symbol}`\nAvg Price: ₹{avg_price:.2f}\nStop Loss: ₹{sl_price:.2f}")
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking existing positions: {e}")
            return False

    async def send_telegram(self, message):
        if not TG_TOKEN or not TG_CHAT_ID: return
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
            payload = {"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}
            await asyncio.to_thread(requests.post, url, json=payload, timeout=5)
        except Exception as e:
            logger.error(f"Telegram alert failed: {e}")

    async def initialize_range_from_candles(self):
        """Warm up the morning range if bot is started mid-day"""
        now = datetime.now().time()
        if now <= RANGE_START:
            logger.info("Bot started before market open. Waiting for live data...")
            return True
        
        logger.info(f"Bot started at {now}. Fetching available candles to reconstruct range since 09:15...")
        history = await asyncio.to_thread(get_history, API_KEY, SYMBOL, EXCHANGE, "1m", duration_days=1)
        
        if not history:
            logger.error("❌ Failed to fetch history for range reconstruction!")
            return False
            
        df = pd.DataFrame(history)
        if 'timestamp' in df.columns:
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
        elif 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
        elif 'time' in df.columns:
            df['datetime'] = pd.to_datetime(df['time'])
        else:
            logger.error(f"❌ Unexpected data format in history. Missing timestamp column. Columns: {df.columns}")
            return False
        today = datetime.now().date()
        
        # Filter for today's 09:15-09:45 window
        df_today = df[df['datetime'].dt.date == today]
        mask_range = (df_today['datetime'].dt.time >= RANGE_START) & \
                     (df_today['datetime'].dt.time <= RANGE_END)
        
        day_range = df_today.loc[mask_range]
        if not day_range.empty:
            self.range_high = day_range['high'].max()
            self.range_low = day_range['low'].min()
            # range_ready is ONLY set to True if it's actually after 09:45
            if now > RANGE_END:
                self.range_ready = True
            
            logger.info(f"🎯 Range Initialized: Low {self.range_low} - High {self.range_high}")
            return True
        else:
            logger.warning("No data found in history for today's morning range.")
            return False

    async def execute_entry(self, opt_type: str):
        if self.trade_taken: return
        
        logger.info(f"🔎 Breakout Detected! Direction: {opt_type} | Index LTP: {self.index_ltp}")
        self.trade_taken = True
        
        try:
            # 1. Fetch Dynamic Expiry and ATM Symbol (with retry)
            expiry = await asyncio.to_thread(get_appropriate_expiry_for_day, API_KEY, SYMBOL, EXCHANGE, OPT_EXCHANGE)
            strike = int(round(self.index_ltp / 100) * 100)
            
            opt_symbol = None
            for attempt in range(3):
                opt_symbol = await asyncio.to_thread(get_option_symbol, API_KEY, SYMBOL, OPT_EXCHANGE, expiry, opt_type, offset="ATM", strike_int=None, underlying_ltp=self.index_ltp)
                if opt_symbol:
                    break
                logger.warning(f"⚠️ Symbol resolution attempt {attempt+1}/3 failed for strike {strike}. Retrying in 1s...")
                await asyncio.sleep(1)
            
            if not opt_symbol:
                logger.error(f"❌ Failed to resolve {opt_type} symbol for strike {strike} after 3 attempts. Abortion.")
                await self.send_telegram(f"⚠️ *{self.bot_id} ORB ERROR*: Symbol fetch failed for strike `{strike}`")
                self.trade_taken = False
                return

            # 2. Market Buy
            logger.info(f"Placing Order: BUY 1 Lot {opt_symbol}")
            res = self.client.placesmartorder(
                strategy=STRATEGY_NAME,
                symbol=opt_symbol,
                action="BUY",
                exchange=OPT_EXCHANGE,
                price_type="MARKET",
                product="NRML",
                quantity=QUANTITY,
                position_size=QUANTITY
            )
            
            if res.get('status') == 'success':
                # Fetch entry price for SL calculation
                entry_price = 0.0
                q_res = self.client.quotes(symbol=opt_symbol, exchange=OPT_EXCHANGE)
                if q_res.get('status') == 'success':
                    entry_price = float(q_res['data'].get('ltp', 0))
                
                sl_price = entry_price * (1 - STOP_LOSS_PCT)
                
                self.active_position = {
                    "symbol": opt_symbol, 
                    "type": opt_type, 
                    "entry_price": entry_price,
                    "sl_price": sl_price,
                    "initial_sl": sl_price,
                    "quantity": QUANTITY,
                    "current_price": entry_price,
                    "max_price": entry_price,
                    "shield_active": False,
                    "trail_active": False
                }
                
                # Subscribe to option symbol for live SL monitoring
                await self.websocket.send(json.dumps({
                    "action": "subscribe", 
                    "symbol": opt_symbol, 
                    "exchange": OPT_EXCHANGE, 
                    "mode": 2
                }))
                
                await self.send_telegram(f"⚡ *{self.bot_id} ORB TRADE ACTIVE*\nInstrument: `{opt_symbol}`\nEntry: ₹{entry_price:.2f} | SL: ₹{sl_price:.2f}\nIndex Breakout: {self.index_ltp}\nRange: [{self.range_low}, {self.range_high}]")
            else:
                logger.error(f"Order rejected by OpenAlgo: {res}")
                # Wait a bit to prevent 429 storm if it's a persistent error
                await asyncio.sleep(5)
                self.trade_taken = False
        except Exception as e:
            logger.error(f"Critical execution error: {e}")
            await asyncio.sleep(5)
            self.trade_taken = False

    async def execute_exit(self, reason: str):
        if not self.active_position: return
        
        symbol = self.active_position['symbol']
        logger.info(f"🛒 Closing Position: {symbol} | Reason: {reason}")
        
        try:
            res = self.client.placesmartorder(
                strategy=STRATEGY_NAME,
                symbol=symbol,
                action="SELL",
                exchange=OPT_EXCHANGE,
                price_type="MARKET",
                product="NRML",
                quantity=QUANTITY,
                position_size=0
            )
            
            if res.get('status') == 'success':
                logger.info(f"✅ Success: {symbol} closed.")
                await self.send_telegram(f"🏁 *{self.bot_id} ORB POSITION CLOSED*\nSymbol: `{symbol}`\nReason: {reason}")
                self.active_position = None
            else:
                logger.error(f"Exit fail: {res}")
        except Exception as e:
            logger.error(f"Critical exit error: {e}")

    async def main_loop(self):
        # 1. Verification
        if is_market_holiday(API_KEY):
            logger.info("Market is closed today. Hibernating.")
            return

        # 2. Warm-up
        await self.initialize_range_from_candles()
        
        # 2.5 Check for existing positions (Adoption logic)
        await self.check_for_existing_positions()

        # 3. Connection with auto-reconnect
        asyncio.create_task(self.dump_live_state())
        first_connect = True
        retry_delay = 5
        while True:
            if datetime.now().time() >= EXIT_TIME:
                logger.info("Past EXIT_TIME. Not reconnecting.")
                break
            try:
                logger.info("Establishing WebSocket handshake...")
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60) as ws:
                    self.websocket = ws
                    await ws.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))

                    # 1. Subscribe to Index
                    await ws.send(json.dumps({"action": "subscribe", "symbol": SYMBOL, "exchange": EXCHANGE, "mode": 2}))

                    # 2. Subscribe to active position (on every connect, not just first)
                    if self.active_position:
                        await ws.send(json.dumps({
                            "action": "subscribe", "symbol": self.active_position['symbol'],
                            "exchange": OPT_EXCHANGE, "mode": 2
                        }))
                        logger.info(f"📡 Subscribed to adopted position: {self.active_position['symbol']}")

                    if first_connect:
                        status_msg = f"🤖 *{self.bot_id} ORB Bot Online*\nInstrument: `{SYMBOL}`\nStatus: Monitoring for breakout..."
                        if self.range_ready:
                            status_msg += f"\nRange: [{self.range_low}, {self.range_high}]"
                        await self.send_telegram(status_msg)
                        now = datetime.now().time()
                        if self.range_ready and now > RANGE_END:
                            await self.send_telegram(f"🔥 *FINAL {self.bot_id} RANGE (Restored)*\nHigh: `{self.range_high}`\nLow: `{self.range_low}`\nMonitoring for breakout...")
                        first_connect = False
                    else:
                        logger.info("WebSocket reconnected successfully.")

                    retry_delay = 5  # reset backoff on successful connection

                    async for message in ws:
                        data = json.loads(message)
                        if data.get("type") == "market_data":
                            m_data = data.get("data", {})
                            sym = m_data.get("symbol")
                            ltp = m_data.get("ltp", 0) or m_data.get("lp", 0)

                            # Robust symbol check for indices (handles both friendly and broker formats)
                            is_index_tick = (sym == SYMBOL) or (":" in str(sym) and str(sym).split(":")[-1].replace("-INDEX", "") == SYMBOL)

                            if is_index_tick:
                                self.index_ltp = ltp
                                logger.info(f"Tick: {sym} @ {ltp}")
                            elif self.active_position and sym == self.active_position['symbol']:
                                self.active_position['current_price'] = ltp

                            if self.index_ltp > 0:
                                await self.tick_logic()

            except Exception as e:
                logger.warning(f"WebSocket disconnected: {e}. Reconnecting in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)

    async def tick_logic(self):
        now = datetime.now().time()
        
        # Scenario A: Defining Range
        if not self.range_ready:
            if RANGE_START <= now <= RANGE_END:
                if self.range_high is None or self.index_ltp > self.range_high:
                    self.range_high = float(self.index_ltp)
                if self.range_low is None or self.index_ltp < self.range_low:
                    self.range_low = float(self.index_ltp)
            elif now > RANGE_END:
                if self.range_high is not None and self.range_low is not None:
                    self.range_ready = True
                    logger.info(f"🔥 Range Formed -> High: {self.range_high}, Low: {self.range_low}")
                    await self.send_telegram(f"🔥 *FINAL {self.bot_id} RANGE FORMED*\nHigh: `{self.range_high}`\nLow: `{self.range_low}`\nMonitoring for breakout crossover...")

        # Scenario B: Monitoring Breakout (Post-Observation Window)
        # Fix: Only trigger on CROSSOVER to prevent late entry/chasing
        if self.range_ready and not self.trade_taken and now < EXIT_TIME:
            if self.last_index_ltp is not None:
                # CE: Price was below or at high, now is above high
                if self.range_high is not None and self.last_index_ltp <= self.range_high and self.index_ltp > self.range_high:
                    await self.execute_entry("CE")
                # PE: Price was above or at low, now is below low
                elif self.range_low is not None and self.last_index_ltp >= self.range_low and self.index_ltp < self.range_low:
                    await self.execute_entry("PE")
            else:
                # First tick received, just initialize for next crossover check
                logger.debug(f"Monitoring started at {self.index_ltp}. Waiting for breakout crossover...")

        # Update last price for next crossover check
        self.last_index_ltp = self.index_ltp

        # Scenario C: SL & Profit Locking Monitoring
        if self.active_position:
            curr_p = float(self.active_position.get('current_price', 0))
            entry_p = float(self.active_position.get('entry_price', 0))
            sl_p = float(self.active_position.get('sl_price', 0))
            
            if curr_p > 0:
                # 1. Update Max Price
                if curr_p > self.active_position['max_price']:
                    self.active_position['max_price'] = curr_p
                
                max_p = self.active_position['max_price']
                current_profit_pct = (curr_p - entry_p) / entry_p
                
                # 2. Stage 1: Profit Shield (+15% -> BE)
                if not self.active_position['shield_active'] and current_profit_pct >= SHIELD_THRESHOLD_PCT:
                    self.active_position['shield_active'] = True
                    self.active_position['sl_price'] = max(self.active_position['sl_price'], entry_p)
                    logger.info(f"🛡️ Profit Shield Activated! SL moved to Break-Even (₹{entry_p:.2f})")
                    await self.send_telegram(f"🛡️ *PROFIT SHIELD ACTIVE*\nSL moved to Break-Even: ₹{entry_p:.2f}")

                # 3. Stage 2: Trailing Exit (+25% -> 10% TSL)
                if current_profit_pct >= TRAIL_THRESHOLD_PCT:
                    if not self.active_position['trail_active']:
                        self.active_position['trail_active'] = True
                        logger.info("⚡ Trailing Stop Loss Activated!")
                    
                    new_sl = max_p * (1 - TRAIL_SL_PCT)
                    if new_sl > self.active_position['sl_price']:
                        self.active_position['sl_price'] = new_sl
                        logger.info(f"📈 Trailing SL Updated: ₹{new_sl:.2f}")

                # 4. Check Final SL Execution
                sl_p = self.active_position['sl_price'] # Re-fetch updated SL
                if curr_p <= sl_p:
                    reason = "Stop Loss Hit"
                    if self.active_position['trail_active']: reason = "Trailing SL Hit"
                    elif self.active_position['shield_active']: reason = "Shield (BE) SL Hit"
                    
                    await self.execute_exit(f"{reason} (Price: {curr_p:.2f} <= SL: {sl_p:.2f})")
                    return

        # Scenario D: Square-off
        if self.active_position and now >= EXIT_TIME:
            await self.execute_exit("EOD Shutdown (03:25 PM)")
            await asyncio.sleep(5)
            logger.info("Bot session complete for the day.")
            sys.exit(0)
            
    async def dump_live_state(self):
        state_file = Path("live_trading/logs/orb_state.json")
        while True:
            try:
                dump_data = {
                    "last_update": datetime.now().isoformat(),
                    "strategy": STRATEGY_NAME,
                    "active_position": self.active_position,
                    "index_ltp": self.index_ltp,
                    "range_high": self.range_high,
                    "range_low": self.range_low,
                    "range_ready": self.range_ready,
                    "trade_taken": self.trade_taken
                }
                state_file.write_text(json.dumps(dump_data, default=str))
            except: pass
            await asyncio.sleep(2)

if __name__ == "__main__":
    bot = ORBChampionBot()
    try:
        asyncio.run(bot.main_loop())
    except KeyboardInterrupt:
        logger.info("Stop signal received.")
