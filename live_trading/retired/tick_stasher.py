
import asyncio
import json
import logging
import os
import websockets
import duckdb
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Configure logging - simplified for launcher compatibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("OPENALGO_API_KEY")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:8765")
# Use a SEPARATE dedicated file to avoid DuckDB write-lock conflicts with main database
# Override with LIVE_TICKS_DB env var for portability across machines
DB_PATH = os.getenv(
    "LIVE_TICKS_DB",
    str(Path.home() / "Developer" / "options_data" / "data" / "live_ticks.duckdb")
)

# Flush buffered ticks to DuckDB every N seconds.
# This releases the write lock between flushes so the dashboard can read.
# 5 s ≈ 12 opens/minute — far below the per-tick rate that caused OOM kills.
FLUSH_INTERVAL_SECS = 5

# Symbol config: maps (symbol, exchange) -> instrument_token
SYMBOL_CONFIGS = [
    {"symbol": "NIFTY",     "exchange": "NSE_INDEX", "token": 256265},
    {"symbol": "SENSEX",    "exchange": "BSE_INDEX",  "token": 265},
    {"symbol": "BANKNIFTY", "exchange": "NSE_INDEX",  "token": 260105},
]

# Map symbol string to token — support both bare and "EXCHANGE:SYMBOL" formats
SYMBOL_TO_TOKEN: dict[str, int] = {}
for _cfg in SYMBOL_CONFIGS:
    SYMBOL_TO_TOKEN[_cfg["symbol"]] = _cfg["token"]
    SYMBOL_TO_TOKEN[f"{_cfg['exchange']}:{_cfg['symbol']}"] = _cfg["token"]


class TickStasher:
    def __init__(self):
        self.db_path   = DB_PATH
        self._buf: list[tuple] = []   # in-memory tick buffer
        self._total    = 0            # lifetime tick counter
        self._ensure_db()

    # ── DB init ───────────────────────────────────────────────────────────────

    def _ensure_db(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(self.db_path)
        con.execute("""
            CREATE TABLE IF NOT EXISTS live_ticks (
                instrument_token BIGINT,
                date_min         TIMESTAMP,
                last_price       DOUBLE,
                volume_min       BIGINT,
                oi               BIGINT,
                day_high         DOUBLE,
                day_low          DOUBLE
            )
        """)
        for col, dtype in [("oi", "BIGINT"), ("day_high", "DOUBLE"), ("day_low", "DOUBLE")]:
            try:
                con.execute(f"ALTER TABLE live_ticks ADD COLUMN {col} {dtype}")
                logger.info(f"✅ Migrated: added column {col} to live_ticks")
            except Exception:
                pass
        con.close()
        logger.info(f"✅ DB ready at {self.db_path}")

    # ── Flush buffer to DuckDB ────────────────────────────────────────────────

    def _flush(self) -> int:
        """Write all buffered ticks to DuckDB and clear the buffer.
        Opens and immediately closes the connection so the dashboard can read.
        Returns the number of rows written."""
        if not self._buf:
            return 0
        rows = list(self._buf)
        self._buf.clear()
        try:
            con = duckdb.connect(self.db_path)
            con.executemany(
                "INSERT INTO live_ticks VALUES (?, ?, ?, ?, ?, ?, ?)", rows
            )
            con.close()
            return len(rows)
        except Exception as e:
            logger.error(f"DB flush error: {e}")
            # Put rows back so we don't lose them; will retry next flush
            self._buf[:0] = rows
            return 0

    # ── Periodic flush coroutine ───────────────────────────────────────────────

    async def _flush_loop(self):
        """Runs forever alongside the WS loop, flushing every FLUSH_INTERVAL_SECS."""
        while True:
            await asyncio.sleep(FLUSH_INTERVAL_SECS)
            written = self._flush()
            if written:
                logger.debug(f"💽 Flushed {written} ticks to DuckDB (total: {self._total})")

    # ── Main WS loop ──────────────────────────────────────────────────────────

    async def start(self):
        logger.info(
            f"📡 Tick Stasher starting "
            f"(buffered flush every {FLUSH_INTERVAL_SECS}s — LTP + volume + OI + day range)..."
        )
        retry_delay = 5

        # Run the WS listener and the flush timer concurrently
        await asyncio.gather(
            self._ws_loop(retry_delay),
            self._flush_loop(),
        )

    async def _ws_loop(self, retry_delay: int):
        while True:
            try:
                async with websockets.connect(
                    WS_URL, ping_interval=30, ping_timeout=60
                ) as ws:
                    logger.info("✅ Connected to WebSocket")
                    await ws.send(json.dumps({"action": "authenticate", "api_key": API_KEY}))
                    await asyncio.sleep(0.5)

                    symbols_payload = [
                        {"exchange": cfg["exchange"], "symbol": cfg["symbol"]}
                        for cfg in SYMBOL_CONFIGS
                    ]
                    # Use "LTP" mode (mode=1).
                    # Fyers HSM for NSE_INDEX (NIFTY/BANKNIFTY) only returns LTP-mode data,
                    # so the ZMQ topic is NSE_INDEX_NIFTY_LTP → proxy broadcasts at mode=1.
                    # Subscribing with Quote (mode=2) means the proxy lookup
                    # ("NIFTY", "NSE_INDEX", 2) finds no clients → no ticks delivered.
                    # LTP subscriptions match the mode=1 broadcast and receive all 3 symbols.
                    sub_msg = {
                        "action": "subscribe",
                        "mode":   "LTP",
                        "symbols": symbols_payload,
                    }
                    await ws.send(json.dumps(sub_msg))
                    sub_labels = [s["exchange"] + ":" + s["symbol"] for s in symbols_payload]
                    logger.info(
                        f"📊 Subscribed (LTP mode) to "
                        f"{len(symbols_payload)} symbols: {sub_labels}"
                    )

                    async for message in ws:
                        data     = json.loads(message)
                        msg_type = data.get("type")

                        if msg_type == "error":
                            logger.error(f"WS server error: {data.get('message', data)}")
                            continue
                        if msg_type not in ["market_data", "quote"]:
                            logger.debug(f"Non-data message: {msg_type} — {data}")
                            continue

                        # Resolve symbol — top-level or inside data dict
                        sym = data.get("symbol")
                        if not sym:
                            m = data.get("data", {})
                            sym = (m.get("exchange", "") + ":" + m.get("symbol", "")).strip(":")

                        m_data = data.get("data", {})
                        logger.debug(f"📥 Received: {msg_type} for {sym}")

                        ltp = m_data.get("ltp") or m_data.get("lp")
                        if sym in SYMBOL_TO_TOKEN and ltp is not None:
                            self._total += 1
                            self._buf.append((
                                SYMBOL_TO_TOKEN[sym],
                                datetime.now(),
                                float(ltp),
                                int(m_data.get("volume", 0) or 0),
                                int(m_data.get("oi",     0) or 0),
                                float(m_data.get("high", 0) or 0),
                                float(m_data.get("low",  0) or 0),
                            ))
                            if ltp > 0:
                                logger.info(
                                    f"📥 Tick #{self._total} {sym}: ltp={ltp} "
                                    f"(buf={len(self._buf)})"
                                )
                        else:
                            logger.debug(
                                f"⏭️ Skipping sym={sym!r} (LTP: {ltp}) — not in token map"
                            )

            except Exception as e:
                logger.error(f"WebSocket error: {e}. Retrying in {retry_delay}s...")
                # Flush whatever is buffered before reconnecting
                self._flush()
                await asyncio.sleep(retry_delay)


if __name__ == "__main__":
    stasher = TickStasher()
    asyncio.run(stasher.start())
