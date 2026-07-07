
import asyncio
import websockets
import json
import os
import sys
import logging
import pandas as pd
import requests
from datetime import datetime, time, timedelta
from pathlib import Path
from dotenv import load_dotenv
from openalgo import api

# --- Configuration ---
STRATEGY_NAME = "BB_5M_SCANNER"
SYMBOLS = ["NIFTY", "BANKNIFTY", "SENSEX"]
BB_PERIOD = 45
BB_STD = 1.5
# Hysteresis: price must pull back 1.0 × σ inside the band before the signal
# clears.  This prevents rapid OVERBOUGHT↔CLEARED chattering when LTP hovers
# right at the band boundary.
BB_HYSTERESIS = 1.0    # 1.0 × σ  ≈ 40-160 pts for NIFTY

# Hard cooldown: even if state oscillates, at most ONE Telegram alert fires
# per symbol within this window. Eliminates flood on choppy markets.
ALERT_COOLDOWN_MINUTES = 15

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("live_trading/logs/bb_scanner.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Environment ---
load_dotenv()
API_KEY   = os.getenv("OPENALGO_API_KEY")
HOST      = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
WS_URL    = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")
TG_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Shared state file — telegram_status.py reads this for /signals command
STATE_FILE = Path(__file__).parent.parent / "logs" / "bb_signals_state.json"


class BBScanner:
    def __init__(self):
        self.ohlc_data    = {sym: pd.DataFrame() for sym in SYMBOLS}

        # Tracks the last ALERTED state per symbol.
        # Alerts fire only on state CHANGE — not repeatedly while signal persists.
        #   None         = neutral (inside bands)
        #   "OVERBOUGHT" = price > upper band
        #   "OVERSOLD"   = price < lower band
        self.signal_state = {sym: None for sym in SYMBOLS}
        self.signal_meta  = {sym: {} for sym in SYMBOLS}   # ltp, band value, triggered_at
        self.last_alert_time = {sym: None for sym in SYMBOLS}  # cooldown tracking
        self.client       = api(api_key=API_KEY, host=HOST)

    # ── State persistence ─────────────────────────────────────────────────────

    def _save_signal_state(self):
        """Write current signal state to shared file for /signals command."""
        try:
            STATE_FILE.parent.mkdir(exist_ok=True)
            payload = {
                sym: {
                    "state":        self.signal_state[sym],
                    "meta":         self.signal_meta[sym],
                    "last_updated": datetime.now().strftime("%H:%M:%S"),
                }
                for sym in SYMBOLS
            }
            STATE_FILE.write_text(json.dumps(payload, indent=2))
        except Exception as e:
            logger.warning(f"Could not save signal state: {e}")

    # ── Telegram helper ───────────────────────────────────────────────────────

    async def send_telegram(self, message, chat_id=None):
        if not TG_TOKEN: return
        target = chat_id or TG_CHAT_ID
        if not target: return
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        payload = {"chat_id": target, "text": message, "parse_mode": "Markdown"}
        for attempt in range(2):
            try:
                await asyncio.to_thread(requests.post, url, json=payload, timeout=10)
                return
            except Exception as e:
                if attempt == 0:
                    logger.warning(f"Telegram alert attempt 1 failed: {e}. Retrying...")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"Telegram alert failed after 2 attempts: {e}")

    # ── History warm-up ───────────────────────────────────────────────────────

    async def initialize_history(self):
        """Pre-load 5m candles to have BB immediately ready"""
        logger.info(f"📡 Warming up 5-minute history for BB({BB_PERIOD}, {BB_STD})...")
        for sym in SYMBOLS:
            try:
                exchange = "NSE_INDEX" if sym in ["NIFTY", "BANKNIFTY"] else "BSE_INDEX"
                end_dt   = datetime.now()
                start_dt = end_dt - timedelta(days=5)
                hist_query = {
                    "symbol": sym, "exchange": exchange, "interval": "1m",
                    "start_date": start_dt.strftime("%Y-%m-%d"),
                    "end_date":   end_dt.strftime("%Y-%m-%d"),
                    "source": "api"
                }
                res  = await asyncio.to_thread(
                    requests.post,
                    f"{HOST}/api/v1/history",
                    json={"apikey": API_KEY, **hist_query},
                    timeout=15
                )
                data = res.json()

                if data.get('status') == 'success' and data.get('data'):
                    df = pd.DataFrame(data['data'])
                    if 'timestamp' in df.columns:
                        df['datetime'] = (
                            pd.to_datetime(df['timestamp'], unit='s', utc=True)
                            .dt.tz_convert('Asia/Kolkata')
                            .dt.tz_localize(None)
                        )
                    elif 'date' in df.columns:
                        df['datetime'] = pd.to_datetime(df['date'])
                    else:
                        df['datetime'] = pd.to_datetime(df.index)
                    df.set_index('datetime', inplace=True)

                    resampled = df.resample('5min', offset='15min').agg(
                        {'high': 'max', 'low': 'min', 'close': 'last'}
                    ).dropna()
                    self.ohlc_data[sym] = resampled
                    logger.info(f"✅ {sym} history loaded ({len(resampled)} candles)")
                else:
                    logger.warning(f"⚠️ Could not fetch history for {sym}. Waiting for live ticks...")
            except Exception as e:
                logger.error(f"Error warming up {sym}: {e}")

        # Initialise shared state file so /signals works even before first alert
        self._save_signal_state()

    # ── BB calculation ────────────────────────────────────────────────────────

    def calculate_bb(self, sym):
        if len(self.ohlc_data[sym]) < BB_PERIOD:
            return None, None, None
        df    = self.ohlc_data[sym]
        sma   = df['close'].rolling(window=BB_PERIOD).mean().iloc[-1]
        std   = df['close'].rolling(window=BB_PERIOD).std().iloc[-1]
        return sma + BB_STD * std, sma - BB_STD * std, std

    # ── Tick handler ──────────────────────────────────────────────────────────

    async def on_tick(self, sym, ltp, timestamp):
        # Update 5m OHLC window
        dt = (pd.to_datetime(timestamp, unit='s', utc=True)
              .tz_convert('Asia/Kolkata').tz_localize(None))
        window = dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)

        if window not in self.ohlc_data[sym].index:
            new_row = pd.DataFrame({'high': [ltp], 'low': [ltp], 'close': [ltp]}, index=[window])
            self.ohlc_data[sym] = pd.concat([self.ohlc_data[sym], new_row])
        else:
            self.ohlc_data[sym].at[window, 'high']  = max(self.ohlc_data[sym].at[window, 'high'], ltp)
            self.ohlc_data[sym].at[window, 'low']   = min(self.ohlc_data[sym].at[window, 'low'],  ltp)
            self.ohlc_data[sym].at[window, 'close'] = ltp

        upper, lower, std = self.calculate_bb(sym)
        if upper is None:
            return

        prev_state = self.signal_state[sym]

        # ── Determine new state WITH HYSTERESIS ───────────────────────────────
        # Entry: price crosses outside the band (unchanged).
        # Clearance: price must pull back BB_HYSTERESIS × σ BELOW/ABOVE the
        # band before the signal is cleared.  This prevents rapid chattering
        # when LTP hovers right at the band boundary (1-2 pt oscillations).
        if ltp > upper:
            new_state = "OVERBOUGHT"
        elif ltp < lower:
            new_state = "OVERSOLD"
        elif prev_state == "OVERBOUGHT":
            # Stay OVERBOUGHT until LTP drops sufficiently below upper band
            if ltp < (upper - BB_HYSTERESIS * std):
                new_state = None
            else:
                new_state = "OVERBOUGHT"   # suppress: still inside hysteresis zone
        elif prev_state == "OVERSOLD":
            # Stay OVERSOLD until LTP rises sufficiently above lower band
            if ltp > (lower + BB_HYSTERESIS * std):
                new_state = None
            else:
                new_state = "OVERSOLD"     # suppress: still inside hysteresis zone
        else:
            new_state = None

        # ── Fire alert ONLY on state change AND outside cooldown ─────────────
        if new_state != prev_state:
            self.signal_state[sym] = new_state
            now_dt  = datetime.now()
            now_str = now_dt.strftime("%H:%M")

            # Cooldown guard: skip alert if we alerted this symbol recently
            last = self.last_alert_time[sym]
            in_cooldown = (
                last is not None
                and (now_dt - last).total_seconds() < ALERT_COOLDOWN_MINUTES * 60
            )

            if new_state == "OVERBOUGHT":
                self.signal_meta[sym] = {"ltp": ltp, "upper": round(upper, 2), "triggered_at": now_str}
                if not in_cooldown:
                    msg = (f"🚨 *BB OVERBOUGHT (5M)*\n"
                           f"Index: `{sym}`\nLTP: ₹{ltp:.2f}\n"
                           f"Upper Band: ₹{upper:.2f}\nAction: `Consider PE Entry`")
                    logger.info(f"BB Signal ON: {sym} OVERBOUGHT @ {ltp:.2f}")
                    await self.send_telegram(msg)
                    self.last_alert_time[sym] = now_dt
                else:
                    logger.debug(f"BB Signal ON: {sym} OVERBOUGHT @ {ltp:.2f} [cooldown — suppressed]")

            elif new_state == "OVERSOLD":
                self.signal_meta[sym] = {"ltp": ltp, "lower": round(lower, 2), "triggered_at": now_str}
                if not in_cooldown:
                    msg = (f"🟢 *BB OVERSOLD (5M)*\n"
                           f"Index: `{sym}`\nLTP: ₹{ltp:.2f}\n"
                           f"Lower Band: ₹{lower:.2f}\nAction: `Consider CE Entry`")
                    logger.info(f"BB Signal ON: {sym} OVERSOLD @ {ltp:.2f}")
                    await self.send_telegram(msg)
                    self.last_alert_time[sym] = now_dt
                else:
                    logger.debug(f"BB Signal ON: {sym} OVERSOLD @ {ltp:.2f} [cooldown — suppressed]")

            else:
                # Signal cleared — price returned inside bands
                prev_label = "OVERBOUGHT" if prev_state == "OVERBOUGHT" else "OVERSOLD"
                self.signal_meta[sym] = {"ltp": ltp, "triggered_at": now_str}
                if not in_cooldown:
                    msg = (f"✅ *BB Signal Cleared (5M)*\n"
                           f"Index: `{sym}`\n"
                           f"LTP: ₹{ltp:.2f} back inside bands\n"
                           f"_(was {prev_label})_")
                    logger.info(f"BB Signal OFF: {sym} returned inside bands @ {ltp:.2f}")
                    await self.send_telegram(msg)
                    self.last_alert_time[sym] = now_dt
                else:
                    logger.debug(f"BB Signal OFF: {sym} cleared [cooldown — suppressed]")

            # Persist new state for /signals command
            self._save_signal_state()

    # ── Main WebSocket loop ───────────────────────────────────────────────────

    async def main_loop(self):
        await self.initialize_history()

        first_connect = True
        retry_delay   = 5

        while True:
            if datetime.now().time() >= time(15, 30):
                logger.info("Past 15:30 IST. Shutting down scanner.")
                break

            try:
                logger.info(f"🔌 Connecting to {WS_URL} for indices...")
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60) as ws:
                    await ws.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))

                    for sym in SYMBOLS:
                        exchange = "NSE_INDEX" if sym in ["NIFTY", "BANKNIFTY"] else "BSE_INDEX"
                        await ws.send(json.dumps({
                            "action": "subscribe", "symbol": sym,
                            "exchange": exchange, "mode": 2
                        }))
                        logger.info(f"✅ Subscribed to {sym}")

                    if first_connect:
                        await self.send_telegram(
                            "📡 *5-Min Bollinger Scanner Online*\n"
                            "Monitoring: `NIFTY, BANKNIFTY, SENSEX`\n"
                            "_Alerts fire once on signal entry & once on clearance.\n"
                            "Type /signals anytime to check live state._"
                        )
                        first_connect = False
                    else:
                        logger.info("WebSocket reconnected successfully.")

                    retry_delay = 5

                    async for message in ws:
                        data = json.loads(message)
                        if data.get("type") == "market_data":
                            sym    = data.get("symbol")
                            m_data = data.get("data", {})
                            ltp    = m_data.get("ltp", 0) or m_data.get("lp", 0)
                            ts     = m_data.get("t") or datetime.now().timestamp()
                            if sym in SYMBOLS and ltp > 0:
                                await self.on_tick(sym, float(ltp), ts)

            except Exception as e:
                logger.warning(f"WebSocket disconnected: {e}. Reconnecting in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


if __name__ == "__main__":
    scanner = BBScanner()
    try:
        asyncio.run(scanner.main_loop())
    except KeyboardInterrupt:
        logger.info("Stop signal received.")
