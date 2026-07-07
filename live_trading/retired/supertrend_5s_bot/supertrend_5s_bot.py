"""
Supertrend 5s Scalping Bot (Paper Trading)

Monitors NIFTY and SENSEX indices on 5-second timeframe.
Executes ATM option entries on trend reversal with ADX filter.
"""

import os
import json
import logging
import asyncio
import websockets
import pandas as pd
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional
from collections import deque
from pydantic import BaseModel, field_validator
from pathlib import Path
from dotenv import load_dotenv
import requests

# Import OpenAlgo components
from openalgo import api
from openalgo.indicators import TechnicalAnalysis
from live_trading.api_utils import get_expiry_dates, get_option_symbol, get_appropriate_expiry_for_day

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("live_trading/supertrend_5s_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment
load_dotenv()
API_KEY = os.getenv("OPENALGO_API_KEY")
WS_URL = os.getenv("WS_URL", "ws://127.0.0.1:5001/ws")
HOST = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Strategy Parameters
STRATEGY_NAME = "SUPERTREND_5S_LIVE"
ST_LEN = 10
ST_MULT = 6.0
ADX_LEN = 14
ADX_TH = 25
SL_PCT = 0.30
HOT_ZONES = [
    (time(9, 15), time(11, 0)),
    (time(13, 0), time(15, 30))
]

# Index configurations
INDEX_CONFIGS = {
    "NIFTY": {
        "symbol": "NIFTY",
        "exchange": "NSE_INDEX",
        "opt_exchange": "NFO",
        "freeze": 1300
    },
    "SENSEX": {
        "symbol": "SENSEX",
        "exchange": "BSE_INDEX",
        "opt_exchange": "BFO",
        "freeze": 1000
    }
}


# --- Utility Functions ---
def is_in_hot_zone(dt):
    t = dt.time()
    for start, end in HOT_ZONES:
        if start <= t <= end:
            return True
    return False

# --- Pydantic Models ---
class SearchMatch(BaseModel):
    symbol: str
    name: str = ""
    expiry: str = ""
    strike: float = 0.0
    instrumenttype: Optional[str] = None
    optiontype: Optional[str] = None

    @field_validator('strike', mode='before')
    @classmethod
    def parse_strike(cls, v):
        if v is None or v == "": return 0.0
        try: return float(v)
        except: return 0.0

class MarketData(BaseModel):
    ltp: float = 0.0
    lp: float = 0.0
    symbol: Optional[str] = None
    
    @property
    def price(self) -> float:
        return self.ltp if self.ltp > 0 else self.lp

# --- Bot Class ---
class Supertrend5sBot:
    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.history = {idx: deque(maxlen=200) for idx in INDEX_CONFIGS}
        self.current_bar = {idx: None for idx in INDEX_CONFIGS}
        self.last_ltp = {idx: 0.0 for idx in INDEX_CONFIGS}
        self.active_trades = {} 
        self.active_subscriptions = set()
        self.websocket = None
        self._last_log: Dict[str, datetime] = {}
        self.sustenance_buffer = {idx: [] for idx in INDEX_CONFIGS}
        self.exp_prefixes = {idx: "" for idx in INDEX_CONFIGS}
        self._expiry_cache = {}
        self._startup_sent = False
        
        # Load active trades on startup
        self._load_active_trades()

    def _load_active_trades(self):
        """Restore active trades from OpenAlgo platform on startup"""
        try:
            logger.info("📡 Fetching active positions from OpenAlgo...")
            res = self.client.positionbook()
            if res.get('status') != 'success':
                logger.error(f"Failed to fetch positionbook: {res.get('message')}")
                return

            positions = res.get('data', [])
            for pos in positions:
                # Filter by strategy and open quantity
                if pos.get('strategy') == STRATEGY_NAME and pos.get('quantity', 0) != 0:
                    opt_symbol = pos['symbol']
                    qty = pos['quantity']
                    avg_price = pos['average_price']
                    
                    # Determine which index this belongs to
                    idx_name = None
                    for name, config in INDEX_CONFIGS.items():
                        if opt_symbol.startswith(name): # Basic heuristic
                            idx_name = name
                            break
                    
                    if not idx_name:
                        # Fallback heuristic: check if it's NIFTY or SENSEX option
                        if 'NIFTY' in opt_symbol: idx_name = 'NIFTY'
                        elif 'SENSEX' in opt_symbol: idx_name = 'SENSEX'
                    
                    if idx_name:
                        sig = 'CE' if 'CE' in opt_symbol else 'PE'
                        direction = -1 if sig == 'CE' else 1
                        sl = avg_price * (1 - SL_PCT)
                        
                        trade = {
                            'symbol': idx_name,
                            'type': sig,
                            'opt_symbol': opt_symbol,
                            'entry_time': datetime.now(), # Estimate since positionbook doesn't have entry_time
                            'entry_price': avg_price,
                            'sl_price': sl,
                            'quantity': abs(qty),
                            'dir': direction
                        }
                        self.active_trades[idx_name] = trade
                        logger.info(f"💾 Restored active trade from platform: {opt_symbol} (Qty: {qty}, Avg: ₹{avg_price:.2f})")
        except Exception as e:
            logger.error(f"Error loading active trades from API: {e}")


    async def send_telegram(self, message):
        if not TG_TOKEN or not TG_CHAT_ID: return
        try:
            url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
            await asyncio.to_thread(requests.post, url, json={"chat_id": TG_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
        except: pass

    async def start(self):
        logger.info("Initializing Expiry Dates via API...")
        for idx in INDEX_CONFIGS:
             try:
                 expiry = await self.get_cached_expiry(idx)
                 if expiry:
                     self.exp_prefixes[idx] = expiry
                     logger.info(f"[{idx}] 🚀 Selected Expiry: {expiry}")
                 else:
                     logger.error(f"[{idx}] ❌ No expiry found!")
             except Exception as e:
                 logger.error(f"[{idx}] Expiry Error: {e}")
        
        if not self._startup_sent:
            await self.send_telegram("Multi-Index Bot Started!\n" + "\n".join([f"{k}: {v}" for k, v in self.exp_prefixes.items()]))
            self._startup_sent = True

        self._bar_timer_started = getattr(self, '_bar_timer_started', False)
        if not self._bar_timer_started:
            asyncio.create_task(self.bar_timer_loop())
            self._bar_timer_started = True

        self._last_ws_data = datetime.now()
        asyncio.create_task(self._stale_data_watchdog())
        asyncio.create_task(self.dump_live_state())

        retry_delay = 5
        while True:
            try:
                logger.info(f"🔌 Connecting to WebSocket: {WS_URL}")
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60) as websocket:
                    self.websocket = websocket
                    await websocket.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))
                    logger.info("✅ WebSocket connected, authenticating...")
                    retry_delay = 5

                    async for message in websocket:
                        self._last_ws_data = datetime.now()
                        data = json.loads(message)
                        if data.get("type") == "auth" and data.get("status") == "success":
                            logger.info("✅ WebSocket authenticated, subscribing...")
                            for idx, config in INDEX_CONFIGS.items():
                                await websocket.send(json.dumps({"action": "subscribe", "symbol": config['symbol'], "exchange": config['exchange'], "mode": 2}))
                                await asyncio.sleep(1) # Prevent race condition
                        elif data.get("type") == "market_data":
                            symbol = data.get("symbol")
                            idx_name = next((k for k, v in INDEX_CONFIGS.items() if v['symbol'] == symbol), None)
                            if idx_name:
                                try:
                                    md = MarketData(**data.get("data", {}))
                                    if idx_name not in self._last_log or (datetime.now() - self._last_log[idx_name]).total_seconds() > 30:
                                        logger.info(f"Ingesting {idx_name}: LTP {md.price}")
                                        self._last_log[idx_name] = datetime.now()
                                    self.last_ltp[idx_name] = md.price
                                    self.update_current_bar(idx_name, md.price)
                                except Exception as e:
                                    logger.error(f"Error parsing market data: {e}")
                            else:
                                # This is market data for an active option position
                                for idx, trade in self.active_trades.items():
                                    if trade and trade.get('opt_symbol') == symbol:
                                        try:
                                            md = MarketData(**data.get("data", {}))
                                            trade['current_price'] = md.price
                                        except: pass

                logger.warning("⚠️ WebSocket connection closed cleanly. Reconnecting...")
            except Exception as e:
                logger.error(f"❌ WebSocket error: {e}. Reconnecting in {retry_delay}s...")

            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 1.5, 60)

    async def _stale_data_watchdog(self):
        while True:
            await asyncio.sleep(30)
            elapsed = (datetime.now() - self._last_ws_data).total_seconds()
            if elapsed > 120:
                logger.warning(f"⚠️ STALE DATA: No WebSocket data for {elapsed:.0f}s.")

    def update_current_bar(self, idx_name, price):
        now = datetime.now()
        bar_start = now.replace(second=(now.second // 5) * 5, microsecond=0)
        cb = self.current_bar[idx_name]
        if cb is None or bar_start > cb['date']:
            if cb: self.history[idx_name].append(cb)
            self.current_bar[idx_name] = {'date': bar_start, 'open': price, 'high': price, 'low': price, 'close': price}
        else:
            cb['high'] = max(cb['high'], price); cb['low'] = min(cb['low'], price); cb['close'] = price

    async def bar_timer_loop(self):
        while True:
            await asyncio.sleep(1)
            for idx_name in INDEX_CONFIGS:
                cb = self.current_bar[idx_name]
                if cb and datetime.now() >= cb['date'] + timedelta(seconds=5):
                    self.update_current_bar(idx_name, self.last_ltp[idx_name])
                    await self.process_signals(idx_name)

    async def process_signals(self, idx_name):
        hist = self.history[idx_name]
        if len(hist) < 30: return
        
        ta_lib = TechnicalAnalysis()
        df = pd.DataFrame(list(hist))
        st_vals, st_dir = ta_lib.supertrend(df['high'], df['low'], df['close'], period=ST_LEN, multiplier=ST_MULT)
        _, _, adx = ta_lib.adx(df['high'], df['low'], df['close'], period=ADX_LEN)
        
        if st_vals is None or adx is None or len(st_vals) < 2: return
        st_dir_vals = st_dir.values if hasattr(st_dir, 'values') else st_dir
        adx_vals = adx.values if hasattr(adx, 'values') else adx
        curr_dir, prev_dir = st_dir_vals[-1], st_dir_vals[-2]
        curr_adx = adx_vals[-1]
        
        active = self.active_trades.get(idx_name)
        if active:
            opt_ltp = await self.get_ltp_async(active['opt_symbol'], INDEX_CONFIGS[idx_name]['opt_exchange'])
            if opt_ltp > 0:
                exit_p, reason = None, ""
                if opt_ltp <= active['sl_price']: exit_p, reason = active['sl_price'], "STOP LOSS"
                elif curr_dir != active['dir']: exit_p, reason = opt_ltp, "ST REVERSAL"
                elif curr_adx < 18: exit_p, reason = opt_ltp, "ADX DECAY"
                elif datetime.now().time() >= time(15, 25): exit_p, reason = opt_ltp, "EOD"
                if exit_p: await self.close_trade(idx_name, exit_p, reason)
            
            # Use WebSocket price if available for risk monitoring
            opt_ltp = active.get('current_price', 0)
            if opt_ltp > 0:
                exit_p, reason = None, ""
                if opt_ltp <= active['sl_price']: exit_p, reason = active['sl_price'], "STOP LOSS"
                elif curr_dir != active['dir']: exit_p, reason = opt_ltp, "ST REVERSAL"
                elif curr_adx < 18: exit_p, reason = opt_ltp, "ADX DECAY"
                elif datetime.now().time() >= time(15, 25): exit_p, reason = opt_ltp, "EOD"
                if exit_p: await self.close_trade(idx_name, exit_p, reason)
            else:
                # Fallback to API quote ONLY if WebSocket data hasn't arrived yet
                opt_ltp = await self.get_ltp_async(active['opt_symbol'], INDEX_CONFIGS[idx_name]['opt_exchange'])
                # ... check risk (omitted for brevity, will implement below) ...
            
            # If trade was closed, allow checking for new signals in the same bar
            if self.active_trades.get(idx_name):
                return

        if not is_in_hot_zone(datetime.now()): return
        # Signal Detection: -1 is Bullish (Green), 1 is Bearish (Red/Yellow)
        reversal = 'CE' if curr_dir == -1 and prev_dir == 1 else ('PE' if curr_dir == 1 and prev_dir == -1 else None)
        
        if reversal:
            self.sustenance_buffer[idx_name] = [-1 if reversal == 'CE' else 1]
            logger.info(f"🔄 [{idx_name}] Trend flip to {reversal} detected (ADX: {curr_adx:.2f}). Waiting for sustenance bars...")
        elif len(self.sustenance_buffer[idx_name]) > 0:
            if curr_dir == self.sustenance_buffer[idx_name][0]:
                self.sustenance_buffer[idx_name].append(curr_dir)
            else:
                self.sustenance_buffer[idx_name] = []
        
        if len(self.sustenance_buffer[idx_name]) >= 2:
            sig = 'PE' if self.sustenance_buffer[idx_name][0] == 1 else 'CE'
            if curr_adx >= ADX_TH:
                logger.info(f"🚀 [{idx_name}] Sustenance confirmed! Executing entry for {sig} (ADX: {curr_adx:.2f})")
                
                # Resolve Option Symbol with Dynamic Expiry
                config = INDEX_CONFIGS[idx_name]
                expiry_date = await self.get_cached_expiry(idx_name)
                if not expiry_date:
                    logger.error(f"[{idx_name}] No valid expiry date found. Skipping entry.")
                    return

                try:
                    current_ltp = self.last_ltp.get(idx_name)
                    opt_symbol = await asyncio.to_thread(get_option_symbol, API_KEY, idx_name, config['exchange'],
                                                 expiry=expiry_date, option_type=sig, offset="ATM",
                                                 underlying_ltp=current_ltp)
                    if opt_symbol:
                        # Fire actual order to OpenAlgo
                        res = self.client.placesmartorder(
                            strategy=STRATEGY_NAME,
                            symbol=opt_symbol,
                            action="BUY",
                            exchange=config['opt_exchange'],
                            price_type="MARKET",
                            product="NRML",
                            quantity=config['freeze'],
                            position_size=config['freeze']
                        )
                        if res.get('status') == 'success':
                            logger.info(f"✅ Order placed successfully: {res.get('orderid')}")
                            await self.execute_entry(idx_name, sig, curr_dir, opt_symbol, res.get('orderid'))
                        else:
                            logger.error(f"❌ Failed to place order: {res.get('message')}")
                except Exception as e:
                    logger.error(f"Order placement error: {e}")
            else:
                logger.info(f"⚠️ [{idx_name}] Sustenance {sig} confirmed but ADX {curr_adx:.2f} < {ADX_TH}. Skipping.")
            self.sustenance_buffer[idx_name] = []

    async def get_ltp_async(self, symbol, exchange):
        """Get last traded price for a symbol (non-blocking)"""
        try:
            # Use a watchdog timeout for the sync call in thread
            res = await asyncio.wait_for(
                asyncio.to_thread(self.client.quotes, symbol=symbol, exchange=exchange),
                timeout=10
            )
            if isinstance(res, dict) and 'data' in res:
                data = res['data']
                if isinstance(data, dict):
                    val = data.get('ltp', data.get('last_price', 0))
                    return float(val or 0)
                elif isinstance(data, list) and len(data) > 0:
                    val = data[0].get('ltp', data[0].get('last_price', 0))
                    return float(val or 0)
        except Exception as e:
            logger.error(f"Error fetching LTP for {symbol}: {e}")
        return 0.0

    async def execute_entry(self, idx_name, sig, direction, opt_symbol, order_id=None):
        config = INDEX_CONFIGS[idx_name]
        expiry_date = self.exp_prefixes[idx_name]
        if not expiry_date: return

        entry_price = await self.get_ltp_async(opt_symbol, config['opt_exchange'])
        if entry_price > 0:
            sl = entry_price * (1 - SL_PCT)
            trade = {
                'symbol': idx_name, 'type': sig, 'dir': direction,
                'opt_symbol': opt_symbol, 'entry_price': entry_price, 'sl_price': sl,
                'entry_time': datetime.now().isoformat(), 'quantity': config['freeze'],
                'order_id': order_id
            }
            self.active_trades[idx_name] = trade
            await self.send_telegram(f"🔔 *LIVE ENTRY: {idx_name}*\n{opt_symbol}\nPrice: ₹{entry_price}\nSL: ₹{sl:.2f}\nOrder ID: {order_id}")

    async def close_trade(self, idx_name, exit_price, reason):
        active = self.active_trades.get(idx_name)
        if not active: return
        
        # Fire actual exit order
        try:
            res = self.client.placesmartorder(
                strategy=STRATEGY_NAME,
                symbol=active['opt_symbol'],
                action="SELL",
                exchange=INDEX_CONFIGS[idx_name]['opt_exchange'],
                price_type="MARKET",
                product="NRML",
                quantity=active['quantity'],
                position_size=0
            )
            if res.get('status') == 'success':
                logger.info(f"✅ Exit order placed successfully: {res.get('orderid')}")
            else:
                logger.error(f"❌ Failed to place exit order: {res.get('message')}")
        except Exception as e:
            logger.error(f"Exit order error: {e}")

        pnl = (exit_price - active['entry_price']) * active['quantity']
        msg = f"🏁 *LIVE EXIT: {idx_name}*\nSymbol: {active['opt_symbol']}\nEntry: ₹{active['entry_price']}\nExit: ₹{exit_price}\nPnL: ₹{pnl:,.2f}\nReason: {reason}"
        logger.info(msg); await self.send_telegram(msg)
        self.active_trades[idx_name] = None

    async def get_cached_expiry(self, idx_name):
        """Get expiry based on user rules, cached per day to minimize API calls"""
        today = datetime.now().date()
        if idx_name not in self._expiry_cache or self._expiry_cache[idx_name]['date'] != today:
            config = INDEX_CONFIGS[idx_name]
            expiry = await asyncio.to_thread(
                get_appropriate_expiry_for_day, 
                API_KEY, idx_name, config['exchange'], config['opt_exchange']
            )
            if expiry:
                self._expiry_cache[idx_name] = {'date': today, 'expiry': expiry}
                self.exp_prefixes[idx_name] = expiry
        
        return self._expiry_cache.get(idx_name, {}).get('expiry')

    async def dump_live_state(self):
        state_file = Path("live_trading/logs/st_state.json")
        while True:
            try:
                dump_data = {
                    "last_update": datetime.now().isoformat(),
                    "strategy": STRATEGY_NAME,
                    "active_trades": self.active_trades,
                    "index_prices": self.last_ltp
                }
                state_file.write_text(json.dumps(dump_data, default=str))
            except: pass
            await asyncio.sleep(2)

if __name__ == "__main__":
    import requests
    bot = Supertrend5sBot()
    asyncio.run(bot.start())
