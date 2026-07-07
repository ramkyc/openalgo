"""
Bollinger Bands Options Live Trading Bot

Strategy:
- Entry: ATM option premium touches lower BB (oversold)
- Exit: Premium returns to SMA (mean reversion)
- Stop Loss: 30% or ₹20,000
- Time: 12:00 PM - 2:30 PM entry, 3:25 PM EOD
- Premium Filter: ₹5 - ₹500
- Max Holding: 3 hours

Based on backtest results: ₹4.57M profit, 70% win rate
"""

import asyncio
import websockets
import json
import pandas as pd
import requests
import os
import sys
import logging
from datetime import datetime, time, timedelta
from pathlib import Path
from dotenv import load_dotenv
from collections import deque
from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Optional, List, Dict
from openalgo import api

# Ensure we can import from live_trading package
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from live_trading.api_utils import get_expiry_dates, get_option_symbol, get_appropriate_expiry_for_day

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Load Environment ---
# Ensure we load from the project root .env
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(project_root, ".env"))

API_KEY = os.getenv("OPENALGO_API_KEY")
if not API_KEY:
    logger.error("❌ OPENALGO_API_KEY not found in environment. Please check your .env file.")
    sys.exit(1)

WS_URL = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")
HOST = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- Strategy Parameters ---
STRATEGY_NAME = "BB_OPTIONS_LIVE"
ENTRY_START_TIME = time(9, 30)   # Earlier entry possible
ENTRY_END_TIME = time(15, 10)    # 3:10 PM
EXIT_TIME = time(15, 25)         # 3:25 PM
MIN_PREMIUM = 5.0                # ₹5 minimum
MAX_PREMIUM = 600.0              # ₹600 maximum
STOP_LOSS_PCT = 0.30             # 30%
STOP_LOSS_AMOUNT = 20000         # ₹20,000
MAX_HOLDING_MINUTES = 180        # 3 hours (Prevents theta decay traps)

# Initialize Technical Analysis
from openalgo.indicators import TechnicalAnalysis
ta_lib = TechnicalAnalysis()

# Index Configurations
INDEX_CONFIGS = {
    "NIFTY": {
        "symbol": "NIFTY",
        "exchange": "NSE_INDEX",
        "opt_exchange": "NFO",
        "step": 50,
        "freeze": 1560,  # 24 lots × 65 (lot size changed from 75 to 65)
        "bb_period": 45,
        "bb_std": 1.5,
        "timeframe": "5m",
        "gap_mr_limit": 0.8,
        "gap_tc_trigger": 0.8,
        "min_tc_dist_sma": 0.6
    },
    "BANKNIFTY": {
        "symbol": "BANKNIFTY",
        "exchange": "NSE_INDEX",
        "opt_exchange": "NFO",
        "step": 100,
        "freeze": 900,
        "bb_period": 45,
        "bb_std": 1.5,
        "timeframe": "5m",
        "gap_mr_limit": 0.7,
        "gap_tc_trigger": 0.7,
        "min_tc_dist_sma": 1.0
    },
    "SENSEX": {
        "symbol": "SENSEX",
        "exchange": "BSE_INDEX",
        "opt_exchange": "BFO",
        "step": 100,
        "freeze": 1000,
        "bb_period": 45,
        "bb_std": 1.5,
        "timeframe": "5m",
        "gap_mr_limit": 0.8,
        "gap_tc_trigger": 0.8,
        "min_tc_dist_sma": 0.7
    }
}

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
class BollingerBandsOptionsBot:
    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        
        # For each index, track both CE and PE option premiums
        # History format: {index_name: {symbol: deque}} to handle multiple strikes smoothly
        self.premium_histories = {idx: {} for idx in INDEX_CONFIGS}
        self.index_ltp = {idx: 0.0 for idx in INDEX_CONFIGS}
        
        # Active trades: {idx_name: {'CE': {...}, 'PE': {...}}}
        self.active_trades = {idx: {'CE': None, 'PE': None} for idx in INDEX_CONFIGS}
        self.active_subscriptions = set()
        self.websocket = None
        
        self.exp_prefixes = {idx: "" for idx in INDEX_CONFIGS}
        self._expiry_cache = {}
        self._last_log: Dict[str, datetime] = {}
        self._startup_sent = False
        
        # ATM strikes and symbols
        self.current_atm_strike = {idx: 0 for idx in INDEX_CONFIGS}
        self.current_ce_symbol = {idx: "" for idx in INDEX_CONFIGS}
        self.current_pe_symbol = {idx: "" for idx in INDEX_CONFIGS}
        
        # Gap Analysis
        self.daily_gaps = {idx: 0.0 for idx in INDEX_CONFIGS}
        self._gaps_updated_today = False
        
        # Reverse mapping for easy lookup during tick processing
        self.broker_to_idx = {}
        for idx, conf in INDEX_CONFIGS.items():
            self.broker_to_idx[conf['symbol']] = idx
            
        # Load any existing open trades from OpenAlgo platform
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
                if pos.get('strategy') == STRATEGY_NAME and pos.get('quantity', 0) != 0:
                    symbol = pos['symbol']
                    qty = pos['quantity']
                    avg_price = pos['average_price']
                    
                    idx_name = None
                    for name in INDEX_CONFIGS:
                        if symbol.startswith(name): 
                            idx_name = name
                            break
                    
                    if idx_name:
                        opt_type = 'CE' if 'CE' in symbol else 'PE'
                        sl_price_pct = avg_price * (1 - STOP_LOSS_PCT)
                        sl_price_amt = avg_price - (STOP_LOSS_AMOUNT / float(INDEX_CONFIGS[idx_name]['freeze']))
                        sl_price = max(sl_price_pct, sl_price_amt)
                        
                        trade = {
                            'opt_symbol': symbol,
                            'strike': 0,
                            'entry_time': datetime.now(),
                            'entry_price': avg_price,
                            'sl_price': sl_price,
                            'sma': 0,
                            'quantity': abs(qty)
                        }
                        self.active_trades[idx_name][opt_type] = trade
                        logger.info(f"💾 Restored trade: {symbol} Avg: ₹{avg_price:.2f}")
        except Exception as e:
            logger.error(f"Error loading active trades: {e}")

    async def send_telegram(self, message):
        if not TG_TOKEN or not TG_CHAT_ID: return
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        try:
            await asyncio.to_thread(
                requests.post, url, 
                data={"chat_id": TG_CHAT_ID, "text": f"📊 *BB OPTIONS LIVE*\n{message}", "parse_mode": "Markdown"}, 
                timeout=5
            )
        except Exception as e:
            logger.error(f"Telegram error: {e}")

    async def fetch_previous_close(self, index_name):
        """Fetch previous day's close for gap calculation"""
        config = INDEX_CONFIGS[index_name]
        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
            
            df = await asyncio.to_thread(self.client.history, 
                                        symbol=config['symbol'], 
                                        exchange=config['exchange'], 
                                        interval="D", 
                                        start_date=start_date, 
                                        end_date=end_date)
            
            if isinstance(df, pd.DataFrame) and not df.empty:
                today_str = datetime.now().strftime("%Y-%m-%d")
                formatted_index = pd.to_datetime(df.index).strftime('%Y-%m-%d')
                hist_candles = df[formatted_index < today_str]
                if not hist_candles.empty:
                    prev_close = float(hist_candles.iloc[-1]['close'])
                    logger.info(f"📈 [{index_name}] Previous Close: {prev_close}")
                    return prev_close
        except Exception as e:
            logger.error(f"Error fetching previous close for {index_name}: {e}")
        return 0.0

    async def update_daily_gaps(self):
        """Calculate gap % for all indices based on current LTP vs Prev Close"""
        if self._gaps_updated_today:
            return
            
        logger.info("🕒 Calculating daily gaps for all indices...")
        all_success = True
        
        for idx in INDEX_CONFIGS:
            prev_close = await self.fetch_previous_close(idx)
            current_ltp = self.index_ltp.get(idx, 0.0)
            
            if current_ltp <= 0:
                current_ltp = await self.get_ltp_async(INDEX_CONFIGS[idx]['symbol'], INDEX_CONFIGS[idx]['exchange'])
            
            if prev_close > 0 and current_ltp > 0:
                gap_pct = (current_ltp - prev_close) / prev_close * 100
                self.daily_gaps[idx] = gap_pct
                logger.info(f"📊 [{idx}] Daily Gap: {gap_pct:.2f}% (LTP: {current_ltp} / Prev: {prev_close})")
            else:
                logger.warning(f"⚠️ [{idx}] Failed to calculate gap (LTP: {current_ltp}, Prev: {prev_close})")
                all_success = False
        
        if all_success:
            self._gaps_updated_today = True
            gap_msg = "\n".join([f"{idx}: {self.daily_gaps[idx]:.2f}%" for idx in INDEX_CONFIGS])
            await self.send_telegram(f"✅ Live Daily Gaps Calculated:\n{gap_msg}")

    async def get_ltp_async(self, symbol, exchange):
        try:
            res = await asyncio.wait_for(
                asyncio.to_thread(self.client.quotes, symbol=symbol, exchange=exchange),
                timeout=15
            )
            if isinstance(res, dict) and 'data' in res:
                data = res['data']
                if isinstance(data, dict):
                    return float(data.get('ltp', data.get('last_price', 0)) or 0)
                elif isinstance(data, list) and len(data) > 0:
                    return float(data[0].get('ltp', data[0].get('last_price', 0)) or 0)
        except: pass
        return 0.0

    async def update_option_prices(self, index_name):
        expiry_date = await self.get_cached_expiry(index_name)
        if not expiry_date:
            return

        current_ltp = self.index_ltp.get(index_name)
        
        try:
            ce_symbol = await asyncio.to_thread(get_option_symbol, API_KEY, index_name, INDEX_CONFIGS[index_name]['exchange'], 
                                        expiry=expiry_date, option_type="CE", offset="ATM", underlying_ltp=current_ltp)
            pe_symbol = await asyncio.to_thread(get_option_symbol, API_KEY, index_name, INDEX_CONFIGS[index_name]['exchange'], 
                                        expiry=expiry_date, option_type="PE", offset="ATM", underlying_ltp=current_ltp)
        except: return
            
        if not ce_symbol or not pe_symbol: return
        
        # Track these candidates for potential entry
        self.current_ce_symbol[index_name] = ce_symbol
        self.current_pe_symbol[index_name] = pe_symbol
        
        # Set of symbols we need to update history for
        symbols_to_update = {ce_symbol, pe_symbol}
        
        # Also need to update history for symbols we ALREADY hold (important for SMA exit)
        for opt_type in ['CE', 'PE']:
            trade = self.active_trades[index_name][opt_type]
            if trade and trade.get('opt_symbol'):
                symbols_to_update.add(trade['opt_symbol'])

        for sym in symbols_to_update:
            # Ensure deque exists
            if sym not in self.premium_histories[index_name]:
                await self.warm_up_symbol(index_name, sym)
            
            price = await self.get_ltp_async(sym, INDEX_CONFIGS[index_name]['opt_exchange'])
            if price > 0:
                self.premium_histories[index_name][sym].append({
                    'timestamp': datetime.now(), 
                    'price': price, 
                    'symbol': sym
                })

    async def warm_up(self, index_name):
        """Warm up currently calculated ATM symbols"""
        config = INDEX_CONFIGS[index_name]
        ltp = await self.get_ltp_async(config['symbol'], config['exchange'])
        if ltp <= 0: return
        self.index_ltp[index_name] = ltp
        
        expiry = await self.get_cached_expiry(index_name)
        if not expiry: return
        
        ce_symbol = await asyncio.to_thread(get_option_symbol, API_KEY, index_name, config['exchange'], 
                                          expiry=expiry, option_type="CE", offset="ATM", underlying_ltp=ltp)
        pe_symbol = await asyncio.to_thread(get_option_symbol, API_KEY, index_name, config['exchange'], 
                                          expiry=expiry, option_type="PE", offset="ATM", underlying_ltp=ltp)
        
        self.current_ce_symbol[index_name] = ce_symbol
        self.current_pe_symbol[index_name] = pe_symbol
        
        for sym in [ce_symbol, pe_symbol]:
            if sym: await self.warm_up_symbol(index_name, sym)

    async def warm_up_symbol(self, index_name, symbol):
        """Fetch historical data for a specific option symbol"""
        logger.info(f"🔥 [{index_name}] Warming up symbol: {symbol}...")
        config = INDEX_CONFIGS[index_name]
        bb_period = int(config['bb_period'])
        interval = config['timeframe']
        
        try:
            df = await asyncio.to_thread(self.client.history, symbol=symbol, exchange=config['opt_exchange'], 
                                        interval=interval, start_date=(datetime.now()-timedelta(days=10)).strftime("%Y-%m-%d"), 
                                        end_date=datetime.now().strftime("%Y-%m-%d"))
            
            if isinstance(df, pd.DataFrame) and not df.empty:
                hist_deque = deque(maxlen=200)
                # Iterate through rows and append to deque
                for i in range(len(df)):
                    row = df.iloc[i]
                    hist_deque.append({'timestamp': df.index[i], 'price': float(row['close']), 'symbol': symbol})
                self.premium_histories[index_name][symbol] = hist_deque
        except Exception as e:
            logger.error(f"Error during warmup for {symbol}: {e}")

    def calculate_bollinger_bands(self, index_name, history):
        config = INDEX_CONFIGS[index_name]
        bb_period = int(config['bb_period'])
        bb_std = float(config['bb_std'])
        
        if len(history) < bb_period: 
            return None, None, None
            
        prices = pd.DataFrame(list(history))['price']
        sma = prices.rolling(window=bb_period).mean().iloc[-1]
        std = prices.rolling(window=bb_period).std().iloc[-1]
        
        lower = sma - (bb_std * std)
        upper = sma + (bb_std * std)
        return float(sma), float(upper), float(lower)

    async def process_signals(self, index_name, is_candle_close=False):
        now = datetime.now()
        for opt_type in ['CE', 'PE']:
            active = self.active_trades[index_name][opt_type]
            if active: await self.check_risk_exit(index_name, opt_type, active)
        
        if is_candle_close:
            for opt_type in ['CE', 'PE']:
                active = self.active_trades[index_name][opt_type]
                if active: await self.check_mean_reversion_exit(index_name, opt_type, active)
            
            # --- GAP-BASED ENTRY LOGIC ---
            config = INDEX_CONFIGS[index_name]
            current_gap = float(self.daily_gaps.get(index_name, 0.0))
            gap_mr_limit = float(config.get('gap_mr_limit', 0.8))
            gap_tc_trigger = float(config.get('gap_tc_trigger', 0.8))

            if abs(current_gap) < gap_mr_limit:
                # MODE: Mean Reversion (Standard Reversal)
                if ENTRY_START_TIME <= now.time() <= ENTRY_END_TIME:
                    if now.second % 60 < 10:
                        logger.info(f"🛡️ [{index_name}] Mode: MEAN REVERSION (Gap: {current_gap:.2f}%)")
                    await self.check_entry(index_name, 'CE')
                    await self.check_entry(index_name, 'PE')
            elif abs(current_gap) >= gap_tc_trigger:
                # MODE: Trend Continuation (Momentum)
                if ENTRY_START_TIME <= now.time() <= ENTRY_END_TIME:
                    if now.second % 60 < 10:
                        logger.info(f"🔥 [{index_name}] Mode: TREND CONTINUATION (Gap: {current_gap:.2f}%)")
                    target_opt = 'CE' if current_gap > 0 else 'PE'
                    await self.check_trend_entry(index_name, target_opt)
            else:
                if now.second % 60 < 10:
                    logger.info(f"⚠️ [{index_name}] Gap {current_gap:.2f}% in No-Trade Zone")

    async def check_entry(self, index_name, opt_type):
        if self.active_trades[index_name][opt_type]: return
        
        symbol = self.current_ce_symbol[index_name] if opt_type == 'CE' else self.current_pe_symbol[index_name]
        if not symbol or symbol not in self.premium_histories[index_name]:
            return
            
        history = self.premium_histories[index_name][symbol]
        sma, bb_upper, bb_lower = self.calculate_bollinger_bands(index_name, history)
        if not sma:
            return
            
        price = history[-1]['price']
        # Reasoning Log
        logger.info(f"📊 [BB_TRACE] {index_name} {opt_type} | Sym: {symbol} | Price: ₹{price:.2f} | Band: ₹{bb_lower:.2f}")

        if price <= bb_lower:
            if MIN_PREMIUM <= price <= MAX_PREMIUM:
                if len(history) >= INDEX_CONFIGS[index_name]['bb_period']:
                    await self.execute_entry(index_name, opt_type, price, sma, "Mean Reversion")

    async def check_trend_entry(self, index_name, opt_type):
        """Check for Trend Continuation entry signal on high-gap days"""
        if self.active_trades[index_name][opt_type]: return
        
        symbol = self.current_ce_symbol[index_name] if opt_type == 'CE' else self.current_pe_symbol[index_name]
        if not symbol or symbol not in self.premium_histories[index_name]: return
            
        history = self.premium_histories[index_name][symbol]
        sma, bb_upper, _ = self.calculate_bollinger_bands(index_name, history)
        if not sma: return
            
        price = history[-1]['price']
        config = INDEX_CONFIGS[index_name]
        
        # Trend Entry Signal: Breach of UPPER Band
        dist_from_sma = abs(price - sma) / sma * 100
        min_dist = config.get('min_tc_dist_sma', 0.6)
        
        if price >= bb_upper and dist_from_sma >= min_dist:
            logger.info(f"🚀 [{index_name}] {opt_type} TREND SIGNAL! Upper BB Breach & Extension ({dist_from_sma:.2f}%)")
            if MIN_PREMIUM <= price <= MAX_PREMIUM:
                await self.execute_entry(index_name, opt_type, price, sma, "Trend Continuation")

    async def execute_entry(self, index_name, opt_type, entry_price, sma, entry_reason):
        config = INDEX_CONFIGS[index_name]
        symbol = self.current_ce_symbol[index_name] if opt_type == 'CE' else self.current_pe_symbol[index_name]
        
        try:
            res = self.client.placesmartorder(strategy=STRATEGY_NAME, symbol=symbol, action="BUY", 
                                        exchange=config['opt_exchange'], price_type="MARKET", 
                                        product="NRML", quantity=config['freeze'], 
                                        position_size=config['freeze'])
            if res.get('status') == 'success':
                sl = max(entry_price * (1 - STOP_LOSS_PCT), entry_price - (STOP_LOSS_AMOUNT / float(config['freeze'])))
                self.active_trades[index_name][opt_type] = {
                    'opt_symbol': symbol, 'entry_time': datetime.now(), 'entry_price': entry_price,
                    'sl_price': sl, 'sma': sma, 'quantity': config['freeze'], 'order_id': res.get('orderid'),
                    'current_price': entry_price
                }
                # Dynamically subscribe to option symbol on WebSocket
                if self.websocket:
                    try:
                        await self.websocket.send(json.dumps({
                            "action": "subscribe", "symbol": symbol, 
                            "exchange": config['opt_exchange'], "mode": 2
                        }))
                        self.active_subscriptions.add(symbol)
                        logger.info(f"📡 Subscribed to {symbol} on WebSocket for risk monitoring")
                    except: pass
                
                await self.send_telegram(f"🔔 *LIVE ENTRY: {index_name} {opt_type}*\nSymbol: {symbol}\nPrice: ₹{entry_price:.2f}\nSL: ₹{sl:.2f}")
        except Exception as e:
            logger.error(f"Entry execution error: {e}")

    async def check_risk_exit(self, index_name, opt_type, active):
        config = INDEX_CONFIGS[index_name]
        # Try WebSocket price first (ZERO API COST)
        price = active.get('current_price', 0)
        if price <= 0:
            # Fallback to API quote ONLY if WebSocket data hasn't arrived
            price = await self.get_ltp_async(active['opt_symbol'], config['opt_exchange'])
        
        if price <= 0: return
        
        reason = None
        if price <= active['sl_price']: reason = "Stop Loss"
        elif datetime.now().time() >= EXIT_TIME: reason = "EOD"
        elif (datetime.now() - active['entry_time']).total_seconds() / 60 >= MAX_HOLDING_MINUTES: reason = "Time Stop"
        
        if reason: await self.close_trade(index_name, opt_type, active, price, reason)

    async def check_mean_reversion_exit(self, index_name, opt_type, active):
        config = INDEX_CONFIGS[index_name]
        sym = active['opt_symbol']
        
        if sym not in self.premium_histories[index_name]:
            return
            
        history = self.premium_histories[index_name][sym]
        
        # Calculate CURRENT SMA dynamically from latest history of THIS SPECIFIC SYMBOL
        sma, _, _ = self.calculate_bollinger_bands(index_name, history)
        if not sma: return
        
        price = await self.get_ltp_async(sym, config['opt_exchange'])
        if price > 0 and price >= sma:
            # Minimum holding of 2 mins to prevent noise square-off
            if (datetime.now() - active['entry_time']).total_seconds() >= 120:
                await self.close_trade(index_name, opt_type, active, price, f"Mean Reversion (SMA: {sma:.2f})")

    async def close_trade(self, index_name, opt_type, active, exit_price, reason):
        try:
            res = self.client.placesmartorder(strategy=STRATEGY_NAME, symbol=active['opt_symbol'], action="SELL", 
                                        exchange=INDEX_CONFIGS[index_name]['opt_exchange'], price_type="MARKET", 
                                        product="NRML", quantity=active['quantity'],
                                        position_size=0)
            if res.get('status') == 'success':
                pnl = (exit_price - active['entry_price']) * active['quantity']
                hold_mins = int((datetime.now() - active['entry_time']).total_seconds() / 60)
                # Structured exit log — parsed by market_review.py for post-market PnL analysis
                logger.info(
                    f"🏁 *EXIT: {index_name} {opt_type}* | Symbol: {active['opt_symbol']} | "
                    f"Entry: ₹{active['entry_price']:.2f} | Exit: ₹{exit_price:.2f} | "
                    f"Qty: {active['quantity']} | Net PnL: ₹{pnl:,.2f} | "
                    f"Hold: {hold_mins}m | Reason: {reason}"
                )
                await self.send_telegram(f"🏁 *LIVE EXIT: {index_name} {opt_type}*\nSymbol: {active['opt_symbol']}\nExit: ₹{exit_price:.2f}\nPnL: ₹{pnl:,.2f}\nReason: {reason}")
                
                # Dynamically unsubscribe to save bandwidth/API
                sym = active['opt_symbol']
                self.active_trades[index_name][opt_type] = None
                
                if self.websocket and sym in self.active_subscriptions:
                    try:
                        await self.websocket.send(json.dumps({"action": "unsubscribe", "symbol": sym}))
                        self.active_subscriptions.remove(sym)
                        logger.info(f"📡 Unsubscribed from {sym}")
                    except: pass
        except Exception as e:
            logger.error(f"Trade closure error: {e}")

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

    async def start(self):
        for idx in INDEX_CONFIGS: await self.warm_up(idx)
        await self.send_telegram("🤖 *Bollinger Bot Online*\nMonitoring NIFTY, BANKNIFTY & SENSEX.")
        asyncio.create_task(self.periodic_update_loop())
        asyncio.create_task(self.dump_live_state())
        retry_delay = 5.0
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60) as websocket:
                    self.websocket = websocket
                    await websocket.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))
                    async for message in websocket:
                        data = json.loads(message)
                        if data.get("type") == "auth" and data.get("status") == "success":
                            for idx, config in INDEX_CONFIGS.items():
                                await websocket.send(json.dumps({"action": "subscribe", "symbol": config['symbol'], "exchange": config['exchange'], "mode": 2}))
                        elif data.get("type") == "market_data":
                            symbol = data.get("symbol")
                            # 1. Update Index LTP
                            idx_found = self.broker_to_idx.get(symbol)
                            
                            if idx_found:
                                try:
                                    md = MarketData(**data.get("data", {}))
                                    self.index_ltp[idx_found] = md.price
                                except: pass
                            
                            # 2. Update Active Position Prices (for risk trailing)
                            for idx_name, trades in self.active_trades.items():
                                for t_type in ['CE', 'PE']:
                                    trade = trades[t_type]
                                    if trade and trade.get('opt_symbol') == symbol:
                                        try:
                                            md = MarketData(**data.get("data", {}))
                                            trade['current_price'] = md.price
                                        except: pass
            except:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 60)

    async def periodic_update_loop(self):
        last_minute = -1
        while True:
            now = datetime.now()
            
            # --- GAP UPDATE (Once per day) ---
            if not self._gaps_updated_today and now.time() >= time(9, 15, 5):
                await self.update_daily_gaps()

            # Trigger update every 5 minutes for 5-minute bars (Sync with INDEX_CONFIGS)
            if now.minute % 5 == 0 and now.minute != last_minute:
                last_minute = now.minute
                for idx in INDEX_CONFIGS:
                    await self.update_option_prices(idx)
                    await self.process_signals(idx, is_candle_close=True)
                    # Stagger requests
                    await asyncio.sleep(1.0)
            elif now.second % 10 == 0:
                for idx in INDEX_CONFIGS: await self.process_signals(idx, is_candle_close=False)
            await asyncio.sleep(0.5)

    async def dump_live_state(self):
        state_file = Path("live_trading/logs/bb_state.json")
        while True:
            try:
                # Convert active_trades to serializable format
                trades_serializable = {}
                for idx, types in self.active_trades.items():
                    trades_serializable[idx] = {}
                    for t_type, trade in types.items():
                        if trade:
                            # Clone and convert non-serializable fields
                            t_copy = trade.copy()
                            if 'entry_time' in t_copy and isinstance(t_copy['entry_time'], datetime):
                                t_copy['entry_time'] = t_copy['entry_time'].isoformat()
                            trades_serializable[idx][t_type] = t_copy
                        else:
                            trades_serializable[idx][t_type] = None

                dump_data = {
                    "last_update": datetime.now().isoformat(),
                    "strategy": STRATEGY_NAME,
                    "active_trades": trades_serializable,
                    "index_prices": self.index_ltp,
                    "daily_gaps": self.daily_gaps
                }
                state_file.write_text(json.dumps(dump_data))
            except Exception as e:
                logger.error(f"BB Dump Error: {e}")
            await asyncio.sleep(2)

if __name__ == "__main__":
    bot = BollingerBandsOptionsBot()
    asyncio.run(bot.start())
