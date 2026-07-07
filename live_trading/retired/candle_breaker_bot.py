import asyncio
import websockets
import json
import pandas as pd
import numpy as np
import requests
import os
import sys
import logging
from datetime import datetime, time, timedelta
from dotenv import load_dotenv
from collections import deque
from pathlib import Path

# Ensure we can import from project root
project_root = "/Users/ramakrishna/Developer/fyers_crk/openalgo"
sys.path.append(project_root)

from openalgo import api
from live_trading.api_utils import (
    get_expiry_dates, 
    get_option_symbol, 
    get_appropriate_expiry_for_day,
    get_history,
    is_market_holiday
)
from live_trading import live_risk_check
from live_trading import calc_friction

# Setup Logging
logger = logging.getLogger("candle_breaker_bot")
logger.setLevel(logging.INFO)
handler = logging.FileHandler("live_trading/logs/candle_breaker_bot.log")
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler())

# Environment
load_dotenv(os.path.join(project_root, ".env"))

API_KEY = os.getenv("OPENALGO_API_KEY")
if not API_KEY:
    logger.error("❌ OPENALGO_API_KEY not found in environment. Please check your .env file.")
    sys.exit(1)

WS_URL = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")
HOST = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- Configuration ---
STRATEGY_NAME = "CANDLE_BREAKER_LIVE"
SHUTDOWN_TIME = time(15, 25)
ENTRY_CUTOFF_TIME = time(14, 45)
DAILY_PORTFOLIO_SL = 50000 
LUNCH_BREAK_START = time(12, 0)
LUNCH_BREAK_END = time(13, 30)
MORNING_WAIT_TIME = time(9, 45)

CONFIGS = {
    "NIFTY": {
        "symbol": "NIFTY",
        "broker_symbol": "NSE:NIFTY50-INDEX",
        "exchange": "NSE_INDEX",
        "opt_exchange": "NFO",
        "timeframe": 15,  # 15 minutes
        "expiry_type": "monthly",
        "freeze": 1800,
        "rr": 2.5
    },
    "SENSEX": {
        "symbol": "SENSEX",
        "broker_symbol": "BSE:SENSEX-INDEX",
        "exchange": "BSE_INDEX",
        "opt_exchange": "BFO",
        "timeframe": 30,  # 30 minutes
        "expiry_type": "weekly",
        "freeze": 1000,
        "rr": 2.5
    }
}

class CandleBreakerBot:
    def __init__(self):
        self.client = api(api_key=API_KEY, host=HOST)
        self.websocket = None
        self.active_trades = {idx: {'CE': None, 'PE': None} for idx in CONFIGS}
        self.state = {idx: {
            'up_history': deque(maxlen=10),
            'down_history': deque(maxlen=10),
            'ohlc_history': deque(maxlen=10),
            'up_cross_count': 0,
            'down_cross_count': 0,
            'last_state': 'unknown',
            'index_open': 0,
            'index_high': 0,
            'index_low': 9999999.0,
            'CE': {'symbol': None, 'open': 0, 'prev_low': 0, 'current_low': 999999.0},
            'PE': {'symbol': None, 'open': 0, 'prev_low': 0, 'current_low': 999999.0}
        } for idx in CONFIGS}
        
        self.index_prices = {idx: 0.0 for idx in CONFIGS}
        self.last_prices = {} # Store latest tick for all symbols (indices AND options)
        self.exiting_trades = set() # Prevent duplicate exit orders
        self.active_subscriptions = set()
        self.daily_mtm = 0.0
        self.bot_halted = False
        self.candle_trade_taken = {idx: False for idx in CONFIGS}
        self.last_tick_time = {}   # symbol -> datetime of last WS tick
        self.state_file = Path("live_trading/logs/candle_breaker_state.json")
        self.initialized = False
        
        # Reverse mapping for easy lookup during tick processing
        # Maps both friendly symbol and broker symbol to the index key
        self.broker_to_idx = {}
        for idx, conf in CONFIGS.items():
            self.broker_to_idx[conf['symbol']] = idx
            self.broker_to_idx[conf['broker_symbol']] = idx
        
        self.load_state()

    async def save_state(self):
        """Persist index crossover counts and history to local file"""
        try:
            ser_state = {
                'candle_trade_taken': self.candle_trade_taken
            }
            for idx, sdata in self.state.items():
                ser_state[idx] = {
                    'up_history': list(sdata['up_history']),
                    'down_history': list(sdata['down_history']),
                    'ohlc_history': list(sdata['ohlc_history']),
                    'up_cross_count': sdata['up_cross_count'],
                    'down_cross_count': sdata['down_cross_count'],
                    'last_state': sdata['last_state']
                }
            self.state_file.write_text(json.dumps(ser_state, indent=4))
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def load_state(self):
        """Load persisted state from local file"""
        if not self.state_file.exists(): return
        try:
            data = json.loads(self.state_file.read_text())
            
            # Restore trade-taken flags
            if 'candle_trade_taken' in data:
                self.candle_trade_taken.update(data['candle_trade_taken'])
                
            for idx, sdata in data.items():
                if idx not in self.state: continue
                self.state[idx]['up_cross_count'] = sdata.get('up_cross_count', sdata.get('cross_count', 0)) # Fallback
                self.state[idx]['down_cross_count'] = sdata.get('down_cross_count', 0)
                self.state[idx]['last_state'] = sdata.get('last_state', 'unknown')
                self.state[idx]['up_history'] = deque(sdata.get('up_history', []), maxlen=25)
                self.state[idx]['down_history'] = deque(sdata.get('down_history', []), maxlen=25)
                self.state[idx]['ohlc_history'] = deque(sdata.get('ohlc_history', []), maxlen=25)
            logger.info("📁 Loaded cross-count state from local file.")
        except Exception as e:
            logger.error(f"Failed to load state: {e}")

    async def backfill_index_crossovers(self, index_name):
        """Calculate Index crossovers for the last 10 candles using local tick data + API fallback"""
        conf = CONFIGS[index_name]
        timeframe = int(conf['timeframe'])
        
        token_map = {"NIFTY": 256265, "BANKNIFTY": 260105, "SENSEX": 265}
        token = token_map.get(index_name)
        if not token: return
        
        lookback_days = 3
        start_date = (datetime.now() - timedelta(days=lookback_days)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        try:
            # 1. Fetch from DuckDB (historical parquet + interval_data + today's live_ticks from tick_stasher)
            def _query_duckdb():
                import duckdb
                master_db = Path('/Users/ramakrishna/Developer/options_data/data/options_data.duckdb')
                db_path = str(master_db) if master_db.exists() else 'db/options_data.duckdb'
                if not Path(db_path).exists(): return pd.DataFrame()
                
                con = duckdb.connect(db_path, read_only=True)
                parquet_path = '/Users/ramakrishna/Developer/options_data/data/tick_store/**/*.parquet'
                
                # Check for dedicated live_ticks database (written by tick_stasher.py)
                live_ticks_db = Path('/Users/ramakrishna/Developer/options_data/data/live_ticks.duckdb')
                live_ticks_union = ""
                if live_ticks_db.exists():
                    try:
                        lt_con = duckdb.connect(str(live_ticks_db), read_only=True)
                        try:
                            lt_df = lt_con.execute(f"""
                                SELECT
                                    date_trunc('minute', date_min) as timestamp,
                                    first(last_price) as open,
                                    max(last_price) as high,
                                    min(last_price) as low,
                                    last(last_price) as close
                                FROM live_ticks
                                WHERE instrument_token = {token} AND date_min >= '{start_date.strftime("%Y-%m-%d %H:%M:%S")}'
                                GROUP BY date_trunc('minute', date_min)
                                ORDER BY timestamp
                            """).df()
                        finally:
                            lt_con.close()
                        # Return the live ticks as a separate df to be merged later
                        lt_df['_source'] = 'live_ticks'
                    except Exception:
                        lt_df = pd.DataFrame()
                else:
                    lt_df = None
                
                query = f"""
                WITH combined_ticks AS (
                    SELECT 
                        date_trunc('minute', date_min) as timestamp, 
                        first(last_price) as open, 
                        max(last_price) as high, 
                        min(last_price) as low, 
                        last(last_price) as close
                    FROM read_parquet('{parquet_path}', union_by_name=True)
                    WHERE instrument_token = {token} AND date_min >= '{start_date.strftime("%Y-%m-%d %H:%M:%S")}'
                    GROUP BY date_trunc('minute', date_min)
                    UNION ALL
                    SELECT date as timestamp, open, high, low, close
                    FROM interval_data
                    WHERE instrument_token = {token} AND date >= '{start_date.strftime("%Y-%m-%d %H:%M:%S")}'
                )
                SELECT timestamp, open, high, low, close FROM combined_ticks ORDER BY timestamp
                """
                try:
                   df = con.execute(query).df()
                except:
                   df = pd.DataFrame()
                con.close()
                
                # Merge live ticks if available
                if lt_df is not None and not lt_df.empty:
                    lt_clean = lt_df[['timestamp', 'open', 'high', 'low', 'close']]
                    df = pd.concat([df, lt_clean], sort=False)
                
                return df
                
            db_df = await asyncio.to_thread(_query_duckdb)
            
            # 2. Fetch from API (using get_history utility from api_utils)
            api_symbol = conf['symbol']
            api_exchange = conf['exchange']
            
            api_df = pd.DataFrame()
            try:
                # Use the utility function get_history which is proven in other bots
                history_data = await asyncio.to_thread(get_history, API_KEY, api_symbol, api_exchange, "1m", duration_days=lookback_days)
                
                if history_data:
                    api_df = pd.DataFrame(history_data)
                    # Standardize columns
                    if 'timestamp' in api_df.columns:
                        api_df['timestamp'] = pd.to_datetime(api_df['timestamp'], unit='s', utc=True)
                    elif 'datetime' in api_df.columns:
                        api_df['timestamp'] = pd.to_datetime(api_df['datetime'], utc=True)
                    
                    api_df['timestamp'] = api_df['timestamp'].dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
            except Exception as api_e:
                logger.error(f"Fallback API fetch failed for {index_name} ({api_symbol}): {api_e}")

            # 3. Merge and Deduplicate
            if db_df.empty and api_df.empty:
                logger.warning(f"⚠️ No historical data available for {index_name} (API/DB both empty)")
                return
            
            # Normalize DB data timestamps to naive if needed
            if not db_df.empty and 'timestamp' in db_df.columns:
                db_df['timestamp'] = pd.to_datetime(db_df['timestamp']).dt.tz_localize('Asia/Kolkata', ambiguous='infer').dt.tz_localize(None)

            df = pd.concat([db_df, api_df], sort=False).drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            
            # Final safety check: ensure naive
            if not df.empty and 'timestamp' in df.columns and df['timestamp'].dt.tz is not None:
                df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
            
            # 4. Market-Aware Resampling
            m1_df = df.copy().set_index('timestamp')
            # Resample to timeframe candles (OHLC)
            # Anchor all candles (15m and 30m) to 09:15 by using offset='15min'
            rs_offset = '15min'  # Align all candles (15m and 30m) to 09:15 market open
            resampled = m1_df.resample(f'{timeframe}min', offset=rs_offset, label='left', closed='left').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last'
            })
            
            # Drop rows without complete OHLC data
            valid_candles = resampled.dropna()
            # Filter to market hours only (09:15 to 15:30) to exclude pre/post-market candles
            _idx_times = pd.DatetimeIndex(valid_candles.index).time
            valid_candles = valid_candles[
                (_idx_times >= time(9, 15)) & (_idx_times <= time(15, 30))
            ]
            # Current candle start to exclude
            curr_start = self.get_current_candle_start(timeframe).replace(tzinfo=None)
            # Filter out any candles that are in the future
            now_naive = datetime.now().replace(tzinfo=None)
            valid_candles = valid_candles[valid_candles.index <= now_naive]
            # Get last 25 valid candles BEFORE current
            history_candles = valid_candles[valid_candles.index < curr_start].tail(25).copy()
            
            new_up_history = deque(maxlen=25)
            new_down_history = deque(maxlen=25)
            new_ohlc_history = deque(maxlen=25)
            
            for b_idx, cdata in history_candles.iterrows():
                b_start = pd.to_datetime(b_idx)
                b_end = b_start + timedelta(minutes=float(timeframe))
                subset = m1_df[(m1_df.index >= b_start) & (m1_df.index < b_end)]
                
                c_open = float(cdata['open'])
                c_high = float(cdata['high'])
                c_low = float(cdata['low'])
                c_close = float(cdata['close'])
                
                up_crosses = 0
                down_crosses = 0
                state = 'unknown'
                # Determine initial state based on first row
                first_row = next(iter(subset.iterrows()))[1]
                state = 'above' if float(first_row.get('close', first_row.get('open', c_open))) >= c_open else 'below'
                # Iterate through rows to count crossovers using low/high
                for _, row in subset.iterrows():
                    low = float(row.get('low', c_open))
                    high = float(row.get('high', c_open))
                    if state == 'above' and low <= c_open:
                        down_crosses += 1
                        state = 'below'
                    elif state == 'below' and high >= c_open:
                        up_crosses += 1
                        state = 'above'

                
                new_up_history.append(up_crosses)
                new_down_history.append(down_crosses)
                new_ohlc_history.append({
                    't': b_start.strftime("%H:%M"),
                    'o': round(c_open, 2), 'h': round(c_high, 2), 'l': round(c_low, 2), 'c': round(c_close, 2),
                    'up': up_crosses, 'down': down_crosses
                })

            if len(new_up_history) > 0:
                self.state[index_name].update({
                    'up_history': new_up_history,
                    'down_history': new_down_history,
                    'ohlc_history': new_ohlc_history
                })
                logger.info(f"📊 Backfilled index history for {index_name}. UP: {list(new_up_history)}, DOWN: {list(new_down_history)}")

            # Current candle catch‑up using minute‑level high/low to detect crosses
            subset_curr = df[df['timestamp'] >= curr_start]
            if not subset_curr.empty:
                c_open = float(subset_curr.iloc[0]['open'])
                up_crosses = down_crosses = 0
                # Initialise state from the first minute's high/low relative to open
                first_min = subset_curr.iloc[0]
                state = 'above' if float(first_min.get('high', c_open)) >= c_open else 'below'

                for _, row in subset_curr.iterrows():
                    low = float(row.get('low', c_open))
                    high = float(row.get('high', c_open))
                    if state == 'above' and low <= c_open:
                        down_crosses += 1
                        state = 'below'
                    elif state == 'below' and high >= c_open:
                        up_crosses += 1
                        state = 'above'

                # Update state with the latest candle stats
                c_high = float(subset_curr['high'].max()) if 'high' in subset_curr.columns else c_open
                c_low = float(subset_curr['low'].min()) if 'low' in subset_curr.columns else c_open
                self.state[index_name].update({
                    'up_cross_count': up_crosses,
                    'down_cross_count': down_crosses,
                    'last_state': state,
                    'index_open': c_open,
                    'index_high': max(float(self.state[index_name].get('index_high', 0)), c_high),
                    'index_low': min(float(self.state[index_name].get('index_low', 9999999)), c_low)
                })
                logger.info(f"🔄 Mid-candle catchup for {index_name}: Open ₹{c_open:.2f}, Up: {up_crosses}, Down: {down_crosses}")
                
        except Exception as e:
            logger.error(f"Index Backfill error for {index_name}: {e}")

    def get_current_candle_start(self, timeframe):
        """Calculate the start time of the current candle based on market open 9:15 AM"""
        now = datetime.now()
        baseline = now.replace(hour=9, minute=15, second=0, microsecond=0)
        
        if now < baseline:
            return baseline
            
        elapsed_mins = int((now - baseline).total_seconds() // 60)
        start_mins = (elapsed_mins // timeframe) * timeframe
        return baseline + timedelta(minutes=start_mins)

    async def sync_positions(self):
        """Sync open positions from broker/sandbox to avoid duplicate entries on restart"""
        logger.info("⚖️ Syncing open positions from broker...")
        try:
            res = await asyncio.to_thread(self.client.positionbook)
            if res.get('status') == 'success':
                data = res.get('data', [])
                for pos in data:
                    if pos.get('strategy') == STRATEGY_NAME and float(pos.get('quantity', 0)) != 0:
                        symbol = pos.get('symbol')
                        qty = float(pos.get('quantity'))
                        avg_p = float(pos.get('average_price', 0))
                        
                        idx_name = None
                        for name, conf in CONFIGS.items():
                            if symbol.startswith(name):
                                idx_name = name
                                break
                        
                        if idx_name:
                            conf = CONFIGS[idx_name]
                            opt_type = 'CE' if symbol.upper().endswith('CE') else 'PE'
                            sdata = self.state[idx_name][opt_type]
                            
                            logger.info(f"✅ Re-linking active trade: {idx_name} {opt_type} -> {symbol} ({qty} qty at ₹{avg_p})")
                            
                            # Attempt to find sl/target from existing state or fetch history for THIS symbol
                            sl = sdata.get('prev_low', 0)
                            
                            # If symbol matches current setup symbol, sl is already populated by setup_candle
                            # If not, we should ideally fetch history for THIS specific symbol
                            if sl == 0 or sdata.get('symbol') != symbol:
                                try:
                                    logger.info(f"🔍 Fetching history for synced symbol {symbol} to find SL...")
                                    h_res = await asyncio.to_thread(self.client.history, symbol=symbol, exchange=conf['opt_exchange'], 
                                                                 interval=f"{conf['timeframe']}m", 
                                                                 start_date=(datetime.now() - timedelta(hours=5)).strftime("%Y-%m-%d"), 
                                                                 end_date=datetime.now().strftime("%Y-%m-%d"))
                                    h_data = h_res['data'] if isinstance(h_res, dict) and 'status' in h_res else h_res
                                    if h_data is not None and len(h_data) >= 2:
                                        h_df = pd.DataFrame(h_data)
                                        sl = float(h_df.iloc[-2]['low'])
                                        logger.info(f"✅ Found SL for {symbol} from history: ₹{sl}")
                                except: pass

                            # Ensure risk is positive and meaningful for SL/Target (Buying Options)
                            if sl >= avg_p or sl <= 0:
                                risk = avg_p * 0.05 # Default 5% risk if structure is broken
                                sl = avg_p - risk
                                logger.warning(f"⚠️ {symbol} SL (₹{sl:.2f}) structure invalid or broken. Using default 5% risk.")
                            else:
                                risk = avg_p - sl
                                
                            target = avg_p + (risk * float(CONFIGS[idx_name]['rr']))
                            
                            self.active_trades[idx_name][opt_type] = {
                                'symbol': symbol,
                                'entry_price': avg_p,
                                'entry_friction': 0,
                                'sl': round(sl, 2),
                                'initial_sl': round(sl, 2), # Maintain original SL
                                'target': round(target, 2),
                                'qty': abs(int(qty)),
                                'order_id': 'SYNCED',
                                'entry_time': datetime.now().isoformat()
                            }
                            # Also ensure the setup matches the active trade for monitoring
                            sdata['symbol'] = symbol
                            sdata['prev_low'] = sl
                            self.candle_trade_taken[idx_name] = True
        except Exception as e:
            logger.error(f"Failed to sync positions: {e}")

    async def send_telegram(self, message):
        if not TG_TOKEN or not TG_CHAT_ID: return
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        try:
            await asyncio.to_thread(requests.post, url, data={"chat_id": TG_CHAT_ID, "text": f"🔥 *CANDLE BREAKER*\n{message}", "parse_mode": "Markdown"}, timeout=5)
        except Exception as e:
            logger.error(f"Telegram error: {e}")

    def get_monthly_expiry(self, symbol, opt_exchange):
        dates = get_expiry_dates(API_KEY, symbol, opt_exchange)
        if not dates: return None
        today = datetime.now()
        this_month = today.strftime("%b").upper()
        month_dates = [d for d in dates if this_month in d.upper()]
        if month_dates: return month_dates[-1]
        return dates[0]

    async def get_index_ltp(self, index_name):
        """Fetch current LTP for an index, prioritizing cached WebSocket value"""
        # 1. Check local cache (WebSocket)
        cached_ltp = self.index_prices.get(index_name, 0.0)
        if cached_ltp > 0:
            return cached_ltp

        # 2. Fallback to REST API
        try:
            conf = CONFIGS[index_name]
            logger.info(f"🔄 Fetching LTP for {index_name} via REST API fallback...")
            res = await asyncio.to_thread(self.client.quotes, symbol=conf['symbol'], exchange=conf['exchange'])
            if isinstance(res, dict) and res.get('status') == 'success':
                q_data = res.get('data', [])
                # Handle both list and dictionary results for portability
                if isinstance(q_data, list) and len(q_data) > 0:
                    lp = q_data[0].get('lp') or q_data[0].get('ltp', 0)
                    return float(lp)
                elif isinstance(q_data, dict):
                    lp = q_data.get('lp') or q_data.get('ltp', 0)
                    return float(lp)
            return 0.0
        except Exception as e:
            logger.error(f"Error fetching LTP for {index_name}: {e}")
            return 0.0

    async def setup_candle(self, index_name):
        conf = CONFIGS[index_name]
        ltp = self.index_prices.get(index_name, 0.0)
        
        if ltp <= 0:
            logger.info(f"🔍 Fetching initial LTP for {index_name} via REST...")
            ltp = await self.get_index_ltp(index_name)
            if ltp > 0:
                self.index_prices[index_name] = ltp
                logger.info(f"✅ Initial LTP for {index_name}: ₹{ltp:.2f}")

        if ltp <= 0:
            logger.warning(f"⚠️ setup_candle skipping {index_name} because LTP is still 0")
            return

        if conf['expiry_type'] == 'monthly':
            expiry = self.get_monthly_expiry(index_name, conf['opt_exchange'])
        else:
            expiry = get_appropriate_expiry_for_day(API_KEY, index_name, conf['exchange'], conf['opt_exchange'])

        if not expiry:
            logger.warning(f"⚠️ Could not find expiry for {index_name}")
            return

        for opt_type in ['CE', 'PE']:
            symbol = get_option_symbol(API_KEY, index_name, conf['exchange'], expiry, opt_type, offset="ATM", underlying_ltp=ltp)
            if not symbol: continue

            try:
                hist_res = await asyncio.to_thread(self.client.history, symbol=symbol, exchange=conf['opt_exchange'], interval=f"{conf['timeframe']}m", 
                                          start_date=(datetime.now() - timedelta(hours=5)).strftime("%Y-%m-%d"), 
                                          end_date=datetime.now().strftime("%Y-%m-%d"))
                
                h_data = hist_res['data'] if isinstance(hist_res, dict) and 'data' in hist_res else hist_res
                is_empty = (h_data is None) or (isinstance(h_data, (list, deque)) and len(h_data) == 0) or (isinstance(h_data, pd.DataFrame) and h_data.empty)
                
                if not is_empty:
                    df = pd.DataFrame(h_data) if not isinstance(h_data, pd.DataFrame) else h_data
                    c_start = self.get_current_candle_start(conf['timeframe']).replace(tzinfo=None)
                    
                    current_c = None
                    prev_c = None
                    for i in range(len(df)-1, -1, -1):
                        try:
                            ts = df.index[i] if 'timestamp' not in df.columns else pd.to_datetime(df.iloc[i]['timestamp'])
                            if hasattr(ts, 'to_pydatetime'): ts = ts.to_pydatetime()
                            ts = ts.replace(tzinfo=None)
                        except: continue
                        if ts <= c_start:
                            current_c = df.iloc[i]
                            if i > 0: prev_c = df.iloc[i-1]
                            break
                    
                    # Store old_sym before overwriting 
                    old_sym = self.state[index_name][opt_type]['symbol']
                    
                    if current_c is not None:
                        open_p = float(current_c['open'])
                        p_low = float(prev_c['low']) if prev_c is not None else float(current_c['low'])
                        self.state[index_name][opt_type]['open'] = open_p
                        self.state[index_name][opt_type]['prev_low'] = p_low
                        self.state[index_name][opt_type]['symbol'] = symbol
                        self.state[index_name][opt_type]['current_low'] = 999999.0
                        logger.info(f"📡 {index_name} {opt_type} Setup: {symbol} | Open: ₹{open_p:.2f} | Prev Low: ₹{p_low:.2f}")

                    active_sym = None
                    if self.active_trades[index_name][opt_type]:
                        active_sym = self.active_trades[index_name][opt_type]['symbol']

                    if old_sym and old_sym != symbol and old_sym != active_sym and self.websocket:
                        try: await self.websocket.send(json.dumps({"action": "unsubscribe", "symbol": old_sym}))
                        except: pass
                    
                    if self.websocket:
                        try:
                            await self.websocket.send(json.dumps({"action": "subscribe", "symbol": symbol, "exchange": conf['opt_exchange'], "mode": 2}))
                            self.active_subscriptions.add(symbol)
                            logger.info(f"📡 Subscribed to setup symbol: {symbol}")
                        except: pass
            except Exception as e:
                logger.error(f"Error setting up candle for {index_name} {opt_type}: {e}")
                
        # Backfill removed from here, moved to start()

    async def handle_tick(self, symbol, price):
        now_time = datetime.now().time()
        self.last_prices[symbol] = float(price)
        self.last_tick_time[symbol] = datetime.now()
        for idx_name, conf in CONFIGS.items():
            if conf['symbol'] == symbol:
                # Safety Check: Prevent index pollution if broker mis-maps an option to index ticker
                # If Nifty (23000) suddenly gets an update for 900 (option price), ignore it.
                current_val = float(price)
                old_val = self.index_prices.get(idx_name, 0)
                
                if old_val > 0:
                    change_pct = abs(current_val - old_val) / old_val
                    if change_pct > 0.10: # 10% jump in ticks is highly suspicious for an index
                        logger.warning(f"⚠️ SUSPICIOUS INDEX JUMP for {idx_name}: ₹{old_val:.2f} -> ₹{current_val:.2f}. IGNORED.")
                        continue

                # Update the index price cache immediately
                self.index_prices[idx_name] = current_val
                
                sdata = self.state[idx_name]
                if sdata.get('index_open', 0) == 0:
                    sdata['index_open'] = current_val
                    logger.info(f"🟢 INDEX CANDLE OPEN for {idx_name}: ₹{current_val:.2f}")
                    continue
                
                if self.bot_halted: continue
                is_nifty = (idx_name == 'NIFTY')
                if not is_nifty and now_time < MORNING_WAIT_TIME: continue
                if is_nifty and LUNCH_BREAK_START <= now_time <= LUNCH_BREAK_END: continue

                o_c = sdata['index_open']
                sdata['index_high'] = max(sdata['index_high'], float(price))
                sdata['index_low'] = min(sdata['index_low'], float(price))

                prev_state = sdata['last_state']
                current_state = 'above' if float(price) >= float(o_c) else 'below'
                
                if prev_state != current_state and prev_state != 'unknown':
                    if current_state == 'above':
                        sdata['up_cross_count'] += 1
                        avg_cross = np.mean(list(sdata['up_history'])) if len(sdata['up_history']) >= 10 else 0
                        target_cross = int(np.floor(float(avg_cross))) + 1
                        logger.info(f"🔄 Up-Cross #{sdata['up_cross_count']} for {idx_name} | Target: {target_cross}")
                        if sdata['up_cross_count'] == target_cross:
                            opt_data = self.state[idx_name]['CE']
                            if opt_data['symbol']: await self.trigger_entry(idx_name, 'CE', opt_data['symbol'], float(price))
                    else:
                        sdata['down_cross_count'] += 1
                        avg_cross = np.mean(list(sdata['down_history'])) if len(sdata['down_history']) >= 10 else 0
                        target_cross = int(np.floor(float(avg_cross))) + 1
                        logger.info(f"🔄 Down-Cross #{sdata['down_cross_count']} for {idx_name} | Target: {target_cross}")
                        if sdata['down_cross_count'] == target_cross:
                            opt_data = self.state[idx_name]['PE']
                            if opt_data['symbol']: await self.trigger_entry(idx_name, 'PE', opt_data['symbol'], float(price))

                sdata['last_state'] = current_state

            for opt_type in ['CE', 'PE']:
                opt_sdata = self.state[idx_name][opt_type]
                is_active_trade = (self.active_trades[idx_name][opt_type] is not None and self.active_trades[idx_name][opt_type]['symbol'] == symbol)
                if is_active_trade: await self.monitor_trade(idx_name, opt_type, price)
                if opt_sdata.get('symbol') == symbol:
                    opt_sdata['current_low'] = min(float(opt_sdata.get('current_low', 999999.0)), float(price))

    async def trigger_entry(self, index_name: str, opt_type: str, symbol: str, current_price: float):
        # 0. BLOCK entries after Shutdown or Cutoff
        now_time = datetime.now().time()
        if now_time >= SHUTDOWN_TIME:
            logger.warning(f"⚠️ {index_name} Entry Blocked: Past SHUTDOWN_TIME")
            return
        if now_time >= ENTRY_CUTOFF_TIME:
            logger.warning(f"⚠️ {index_name} Entry Blocked: Past ENTRY_CUTOFF_TIME (2:45 PM)")
            return

        # 1. TRUTH: Max 1 Trade per Candle per Index (Strictly enforced)
        if self.candle_trade_taken[index_name]: return

        # Double check: Is there already an active trade for this index?
        if any(trade is not None for trade in self.active_trades[index_name].values()):
            logger.warning(f"⚠️ {index_name} Entry Blocked: Another trade is already active for this index.")
            self.candle_trade_taken[index_name] = True # Lock it down
            await self.save_state()
            return

        # Set flag IMMEDIATELY to prevent race conditions during async execution
        self.candle_trade_taken[index_name] = True
        await self.save_state() # Persist the lock
        
        sdata = self.state[index_name][opt_type]
        conf = CONFIGS[index_name]
        entry_p = float(sdata['open'])
        sl = float(min(float(sdata['prev_low']), float(sdata['current_low'])))
        risk = entry_p - sl
        if risk <= 0:
            logger.warning(f"⚠️ {symbol} Entry Blocked: SL Invalid")
            return
        
        target_p = entry_p + (risk * float(conf['rr']))
        
        # Quantity calculation
        lot_size = 1
        try:
            from database.token_db import get_symbol_info
            si = get_symbol_info(symbol, conf['opt_exchange'])
            if si and getattr(si, 'lotsize', None): lot_size = int(si.lotsize)
        except: pass
            
        base_qty = int(conf['freeze'])
        qty = (base_qty // lot_size) * lot_size if lot_size > 0 else base_qty
        
        logger.info(f"🚀 TRIGGER: {symbol} at ₹{current_price:.2f} (Limit Entry ₹{entry_p:.2f}, Qty: {qty})")
        
        risk_report = live_risk_check.check_live_vitals(symbol=symbol, exchange=conf['opt_exchange'])
        if not risk_report.get("safe_to_trade", False):
            msg = f"🛑 *SHIELD ACTIVE: ENTRY BLOCKED* for {symbol}"
            logger.warning(msg)
            await self.send_telegram(msg)
            return

        try:
             res = self.client.placesmartorder(strategy=STRATEGY_NAME, symbol=symbol, action="BUY", 
                                         exchange=conf['opt_exchange'], price_type="MARKET",
                                         product="NRML", quantity=qty, position_size=qty)
             if res.get('status') == 'success':
                 entry_friction = calc_friction.calculate_friction(entry_p, qty, is_buy=True)
                 self.active_trades[index_name][opt_type] = {
                     'symbol': symbol, 'entry_price': entry_p, 'entry_friction': float(entry_friction['total']),
                     'sl': sl, 'initial_sl': sl, 'target': target_p, 'qty': qty, 'order_id': str(res.get('orderid')),
                     'entry_time': datetime.now().isoformat()
                 }
                 logger.info(f"🚀 ENTRY SUCCESS: {symbol} at ₹{entry_p}")
                 # Ensure we are subscribed to this symbol for monitoring
                 if self.websocket:
                     try:
                         await self.websocket.send(json.dumps({"action": "subscribe", "symbol": symbol, "exchange": conf['opt_exchange'], "mode": 2}))
                         self.active_subscriptions.add(symbol)
                     except: pass
                 await self.send_telegram(f"🚀 *ENTRY: {index_name} {opt_type}*\nSymbol: {symbol}\nLimit: ₹{entry_p:.2f}\nTgt: ₹{target_p:.2f}\nSL: ₹{sl:.2f}")
             else:
                  logger.error(f"❌ ENTRY FAILED for {symbol}: {res.get('message', 'Unknown Error')}")
        except Exception as e:
            logger.error(f"Entry error for {symbol}: {e}")

    async def monitor_trade(self, index_name, opt_type, current_price):
        trade = self.active_trades[index_name][opt_type]
        if not trade or trade['symbol'] in self.exiting_trades: return
        
        reason = None
        if current_price >= trade['target']: reason = "TARGET"
        elif current_price <= trade['sl']: reason = "STOP LOSS"
        elif datetime.now().time() >= SHUTDOWN_TIME: reason = "EOD"
        
        if reason:
            self.exiting_trades.add(trade['symbol'])
            try:
                conf = CONFIGS[index_name]
                res = self.client.placesmartorder(strategy=STRATEGY_NAME, symbol=trade['symbol'], action="SELL", 
                                            exchange=conf['opt_exchange'], price_type="MARKET", 
                                            product="NRML", quantity=trade['qty'], position_size=0)
                if res.get('status') == 'success':
                    exit_friction = calc_friction.calculate_friction(current_price, trade['qty'], is_buy=False)
                    total_friction = trade.get('entry_friction', 0) + exit_friction['total']
                    gross_pnl = (current_price - trade['entry_price']) * trade['qty']
                    net_pnl = gross_pnl - total_friction
                    self.daily_mtm += net_pnl
                    msg = f"🏁 *EXIT: {index_name} {opt_type}* | Net PnL: ₹{net_pnl:,.2f} | Reason: {reason}"
                    await self.send_telegram(msg)
                    logger.info(msg)
                    self.active_trades[index_name][opt_type] = None
                    if self.daily_mtm <= -DAILY_PORTFOLIO_SL:
                        self.bot_halted = True
                        await self.send_telegram("🛑 *CRITICAL: DAILY PORTFOLIO SL HIT*")
                else:
                    err_msg = res.get('message', str(res))
                    logger.error(f"❌ EXIT ORDER REJECTED for {trade['symbol']} ({reason}): {err_msg}")
            except Exception as e:
                logger.error(f"Exit error for {trade['symbol']}: {e}")
            finally:
                if trade and trade['symbol'] in self.exiting_trades:
                    self.exiting_trades.remove(trade['symbol'])

    async def scheduler(self):
        """Task to handle candle rolls and setup"""
        last_rolled = {idx: None for idx in CONFIGS}
        last_rest_check = datetime.min   # throttle REST staleness checks
        while True:
            try:
                now = datetime.now()
                now_time = now.time()

                # --- GLOBAL EOD EXIT CHECK ---
                if now_time >= SHUTDOWN_TIME:
                    for idx_name in CONFIGS:
                        for opt_type in ['CE', 'PE']:
                            trade = self.active_trades[idx_name][opt_type]
                            if trade:
                                price = self.last_prices.get(trade['symbol'], trade['entry_price'])
                                logger.info(f"⏰ SHUTDOWN TIMER HIT! Actively exiting {trade['symbol']}...")
                                await self.monitor_trade(idx_name, opt_type, price)

                # --- REST STALENESS CHECK (every 60s, only for active trades with stale WS ticks) ---
                if (now - last_rest_check).total_seconds() >= 60:
                    last_rest_check = now
                    stale_trades = []   # [(idx_name, opt_type, trade, exchange)]
                    for idx_name, conf in CONFIGS.items():
                        for opt_type in ['CE', 'PE']:
                            trade = self.active_trades[idx_name][opt_type]
                            if not trade or trade['symbol'] in self.exiting_trades:
                                continue
                            last_tick = self.last_tick_time.get(trade['symbol'])
                            age = (now - last_tick).total_seconds() if last_tick else 999
                            if age > 60:
                                stale_trades.append((idx_name, opt_type, trade, conf['opt_exchange']))
                    if stale_trades:
                        symbols_req = [{"symbol": t[2]['symbol'], "exchange": t[3]} for t in stale_trades]
                        try:
                            res = await asyncio.to_thread(self.client.multiquotes, symbols=symbols_req)
                            if res.get('status') == 'success':
                                quotes = {q['symbol']: float(q.get('ltp', 0)) for q in res.get('data', []) if q.get('ltp')}
                                for idx_name, opt_type, trade, _ in stale_trades:
                                    ltp = quotes.get(trade['symbol'])
                                    if ltp and ltp > 0:
                                        self.last_prices[trade['symbol']] = ltp
                                        self.last_tick_time[trade['symbol']] = now
                                        logger.info(f"📡 REST fallback price for {trade['symbol']}: ₹{ltp:.2f} (WS was stale)")
                                        await self.monitor_trade(idx_name, opt_type, ltp)
                        except Exception as e:
                            logger.warning(f"REST staleness check failed: {e}")

                # --- CANDLE ROLL LOGIC ---
                for idx_name, conf in CONFIGS.items():
                    c_start = self.get_current_candle_start(conf['timeframe']).replace(tzinfo=None)
                    if last_rolled[idx_name] is None:
                        last_rolled[idx_name] = c_start
                        continue # First run setup already handled by __init__
                        
                    if c_start > last_rolled[idx_name]:
                        logger.info(f"⏰ Candle Roll detected for {idx_name}: {last_rolled[idx_name]} -> {c_start}")
                        # Store candle OHLC and counts
                        s_state = self.state[idx_name]
                        ltp = self.index_prices[idx_name]
                        
                        s_state['up_history'].append(s_state['up_cross_count'])
                        s_state['down_history'].append(s_state['down_cross_count'])
                        s_state['ohlc_history'].append({
                            't': last_rolled[idx_name].strftime("%H:%M"),
                            'o': s_state['index_open'],
                            'h': s_state['index_high'],
                            'l': s_state['index_low'],
                            'c': ltp,
                            'up': s_state['up_cross_count'],
                            'down': s_state['down_cross_count']
                        })
                        
                        # Reset counters
                        s_state['up_cross_count'] = 0
                        s_state['down_cross_count'] = 0
                        s_state['index_open'] = ltp
                        s_state['index_high'] = ltp
                        s_state['index_low'] = ltp
                        s_state['last_state'] = 'unknown'
                        self.candle_trade_taken[idx_name] = False
                        
                        await self.save_state()
                        last_rolled[idx_name] = c_start
                        await self.setup_candle(idx_name)
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
            await asyncio.sleep(1)

    async def dump_live_state(self):
        state_file = Path("live_trading/logs/cb_live_state.json")
        while True:
            try:
                dump_data = {
                    "last_update": datetime.now().isoformat(),
                    "halted": self.bot_halted,
                    "candle_trade_taken": self.candle_trade_taken,
                    "index_prices": self.index_prices,
                    "last_prices": self.last_prices,
                    "active_trades": {idx: {"CE": self.active_trades[idx]["CE"], "PE": self.active_trades[idx]["PE"]} for idx in self.active_trades},
                    "state": {idx: {
                        "timeframe": CONFIGS[idx]["timeframe"],
                        "up_cross_count": self.state[idx]["up_cross_count"],
                        "down_cross_count": self.state[idx]["down_cross_count"],
                        "up_history": list(self.state[idx]["up_history"]),
                        "down_history": list(self.state[idx]["down_history"]),
                        "ohlc_history": list(self.state[idx]["ohlc_history"]),
                        "last_state": self.state[idx]["last_state"],
                        "index_open": self.state[idx]["index_open"],
                        "index_high": self.state[idx]["index_high"],
                        "index_low": self.state[idx]["index_low"],
                        "CE": self.state[idx]["CE"],
                        "PE": self.state[idx]["PE"]
                    } for idx in self.state}
                }
                state_file.write_text(json.dumps(dump_data))
            except: pass
            await asyncio.sleep(2)

    async def report_status(self):
        while True:
            await asyncio.sleep(300)
            logger.info("📊 --- BOT HEARTBEAT ---")

    async def start(self):
        logger.info(" Candle Breaker Bot Starting...")
        
        # 1. Setup indices first (populate index_prices and setup_candle)
        for idx in CONFIGS:
            logger.info(f"⚙️ Setting up {idx}...")
            await self.setup_candle(idx)
            await self.backfill_index_crossovers(idx)

        # 2. Sync positions AFTER setup (so sdata['prev_low'] might be available)
        await self.sync_positions()

        async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60) as ws:
            self.websocket = ws
            await ws.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))
            for idx in CONFIGS:
                await ws.send(json.dumps({"action": "subscribe", "symbol": CONFIGS[idx]['symbol'], "exchange": CONFIGS[idx]['exchange'], "mode": 2}))
            
            # Subscribe to adopted/existing option trades
            for idx, trades in self.active_trades.items():
                for t_type, trade in trades.items():
                    if trade and trade.get('symbol'):
                        await ws.send(json.dumps({
                            "action": "subscribe", "symbol": trade['symbol'], 
                            "exchange": CONFIGS[idx]['opt_exchange'], "mode": 2
                        }))
                        self.active_subscriptions.add(trade['symbol'])
                        logger.info(f"📡 Subscribed to adopted option: {trade['symbol']}")

            # 4. Subscribe to setup symbols (Atm symbols resolved by setup_candle)
            for idx in CONFIGS:
                for t in ['CE', 'PE']:
                    sym = self.state[idx][t]['symbol']
                    if sym and sym not in self.active_subscriptions:
                        await ws.send(json.dumps({
                            "action": "subscribe", "symbol": sym, 
                            "exchange": CONFIGS[idx]['opt_exchange'], "mode": 2
                        }))
                        self.active_subscriptions.add(sym)
                        logger.info(f"📡 Subscribed to startup setup symbol: {sym}")
            
            asyncio.create_task(self.scheduler())
            asyncio.create_task(self.dump_live_state())
            asyncio.create_task(self.report_status())
            
            async for message in ws:
                data = json.loads(message)
                if data.get("type") == "market_data":
                    m_data = data.get("data", {})
                    # Correctly extract symbol from top-level to avoid exchange prefixes (consistent with internal tracking)
                    sym = data.get("symbol")
                    if sym and ("CE" in sym or "PE" in sym):
                        logger.debug(f"🔍 WS TICK: {sym}")
                    ltp_val = m_data.get("ltp") or m_data.get("lp")
                    
                    if sym and ltp_val is not None:
                        ltp = float(ltp_val)
                        
                        # Update index_prices based on broker symbol mapping
                        if sym in self.broker_to_idx:
                            idx_name = self.broker_to_idx[sym]
                            self.index_prices[idx_name] = ltp
                            if datetime.now().second % 30 == 0:
                                logger.debug(f"📉 Socket Tick -> {idx_name} ({sym}): {ltp}")
                        
                        self.last_prices[sym] = ltp
                        await self.handle_tick(sym, ltp)
                elif data.get("type") == "error":
                    logger.error(f"❌ WS Server Error: {data.get('message')}")

if __name__ == "__main__":
    bot = CandleBreakerBot()
    asyncio.run(bot.start())
