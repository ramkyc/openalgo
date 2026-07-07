"""
Depth Recorder v2 — Normalized 50-Level Depth Data Collector
=============================================================
Collects real-time depth-50 data for the top 20 liquid NIFTY50 stocks.

Two tables written per monthly DuckDB file:

  depth_levels(timestamp, symbol, ltp, side, level, price, quantity, orders)
    — one row per level per tick (up to 100 rows/tick for depth-50)
    — use this for raw-book queries, wall detection, imbalance backtests

  derived_metrics(timestamp, symbol, ltp, raw_obi, w_obi, vwmp, bid_tot, ask_tot, n_levels)
    — one summary row per tick, pre-computed
    — use this for fast time-series OBI queries

Storage layout:
    db/depth_data/YYYY_MM.duckdb   (monthly rotation, auto-created)

Market hours gate: 09:15–15:35 IST (no data written outside this window).

Auto-reconnect: on WebSocket drop, retries every RECONNECT_BACKOFF seconds
    (up to 5 attempts, then waits 1 minute before next cycle).

Usage:
    uv run live_trading/depth_recorder.py
"""

import asyncio
import os
import sys
import logging
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from dotenv import load_dotenv

# ── Project root on path ───────────────────────────────────────────────────────
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from openalgo import api

# ── Environment ────────────────────────────────────────────────────────────────
load_dotenv()
API_KEY = os.getenv("OPENALGO_API_KEY")
HOST    = os.getenv("HOST_SERVER",    "http://127.0.0.1:5001")
WS_URL  = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")

# ── Constants ──────────────────────────────────────────────────────────────────
DB_BASE_DIR          = Path("db/depth_data")
BATCH_FLUSH_INTERVAL = 30       # seconds between periodic flushes
BATCH_FLUSH_SIZE     = 500      # ticks that trigger an immediate flush
RECONNECT_BACKOFF    = 30       # seconds between reconnect attempts
HEARTBEAT_INTERVAL   = 60       # seconds between heartbeat log lines

MARKET_OPEN_H,  MARKET_OPEN_M  = 9,  15
MARKET_CLOSE_H, MARKET_CLOSE_M = 15, 35

# Top 20 liquid NIFTY50 stocks (NSE equity, depth-50)
TOP20_STOCKS = [
    "RELIANCE", "HDFCBANK",   "ICICIBANK", "INFY",       "TCS",
    "KOTAKBANK", "SBIN",      "AXISBANK",  "BAJFINANCE", "LT",
    "BHARTIARTL","HINDUNILVR","TMPV",      "WIPRO",       "HCLTECH",
    "SUNPHARMA", "TITAN",     "MARUTI",    "ADANIPORTS",  "NTPC",
]

SUBSCRIBE_SYMBOLS = [
    {"exchange": "NSE", "symbol": f"{s}:50"} for s in TOP20_STOCKS
]

# ── Logging ────────────────────────────────────────────────────────────────────
Path("live_trading").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("live_trading/depth_recorder.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _is_market_hours() -> bool:
    """Return True if current IST time is within 09:15–15:35."""
    now = datetime.now()
    t        = now.hour * 60 + now.minute
    open_t   = MARKET_OPEN_H  * 60 + MARKET_OPEN_M
    close_t  = MARKET_CLOSE_H * 60 + MARKET_CLOSE_M
    return open_t <= t <= close_t


def _get_db_path() -> Path:
    """Monthly DuckDB path: db/depth_data/YYYY_MM.duckdb"""
    now = datetime.now()
    DB_BASE_DIR.mkdir(parents=True, exist_ok=True)
    return DB_BASE_DIR / f"{now.year}_{now.month:02d}.duckdb"


def _init_db(db: duckdb.DuckDBPyConnection) -> None:
    """Create tables and indexes if they don't exist."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS depth_levels (
            timestamp  TIMESTAMPTZ,
            symbol     VARCHAR,
            ltp        DOUBLE,
            side       VARCHAR,       -- 'bid' | 'ask'
            level      TINYINT,       -- 1 = best, 50 = worst
            price      DOUBLE,
            quantity   INTEGER,
            orders     SMALLINT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS derived_metrics (
            timestamp  TIMESTAMPTZ,
            symbol     VARCHAR,
            ltp        DOUBLE,
            raw_obi    DOUBLE,        -- unweighted OBI (-100 to +100)
            w_obi      DOUBLE,        -- proximity-weighted OBI
            vwmp       DOUBLE,        -- volume-weighted mid-price
            bid_tot    INTEGER,
            ask_tot    INTEGER,
            n_levels   SMALLINT       -- actual levels received (≤50)
        )
    """)
    # Composite indexes for time-series and per-symbol queries
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_levels_sym_ts
        ON depth_levels(symbol, timestamp)
    """)
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_sym_ts
        ON derived_metrics(symbol, timestamp)
    """)
    logger.info("[DB] Schema ready.")


def _compute_metrics(bids: list, asks: list, ltp: float) -> dict:
    """Compute raw OBI, weighted OBI, and VWMP from raw depth lists."""
    tb = sum(b["quantity"] for b in bids)
    ta = sum(a["quantity"] for a in asks)
    total = tb + ta

    raw_obi = (tb - ta) / total * 100.0 if total > 0 else 0.0

    wb = sum(b["quantity"] / (i + 1) for i, b in enumerate(bids))
    wa = sum(a["quantity"] / (i + 1) for i, a in enumerate(asks))
    denom = wb + wa
    w_obi = (wb - wa) / denom * 100.0 if denom > 0 else 0.0

    vwmp_num = (
        sum(b["price"] * b["quantity"] for b in bids)
        + sum(a["price"] * a["quantity"] for a in asks)
    )
    vwmp = vwmp_num / total if total > 0 else ltp

    return {
        "raw_obi":  raw_obi,
        "w_obi":    w_obi,
        "vwmp":     vwmp,
        "bid_tot":  tb,
        "ask_tot":  ta,
        "n_levels": len(bids),
    }


# ── Main Recorder Class ────────────────────────────────────────────────────────

class DepthRecorder:
    """
    Thread-safe depth-50 recorder.

    The OpenAlgo SDK fires on_depth_update() on its own thread.
    We use list.append (GIL-safe in CPython) to batch rows and
    asyncio tasks to flush to DuckDB periodically.
    """

    def __init__(self):
        self._levels_batch:  list[dict] = []
        self._metrics_batch: list[dict] = []
        self._tick_count  = 0
        self._last_flush_ticks = 0
        self._connected   = False
        self._running     = True
        self._client      = None
        self._db: duckdb.DuckDBPyConnection | None = None
        self._current_db_path: Path | None = None
        self.loop: asyncio.AbstractEventLoop | None = None

    # ── DB ─────────────────────────────────────────────────────────────────────

    def _ensure_db(self) -> None:
        """Open (or rotate to) the correct monthly DuckDB file."""
        path = _get_db_path()
        if path != self._current_db_path or self._db is None:
            if self._db is not None:
                try:
                    self._db.close()
                except Exception:
                    pass
            self._db = duckdb.connect(str(path))
            _init_db(self._db)
            self._current_db_path = path
            logger.info(f"[DB] Opened → {path}")

    # ── Flush ──────────────────────────────────────────────────────────────────

    async def _flush_loop(self) -> None:
        """Periodic flush every BATCH_FLUSH_INTERVAL seconds."""
        while self._running:
            await asyncio.sleep(BATCH_FLUSH_INTERVAL)
            await self._flush()

    async def _flush(self) -> None:
        """Drain both batches into the monthly DuckDB file."""
        # Swap out current batches atomically (list assignment is GIL-safe)
        levels_snap  = self._levels_batch[:]
        metrics_snap = self._metrics_batch[:]
        del self._levels_batch[:]
        del self._metrics_batch[:]

        if not levels_snap and not metrics_snap:
            return

        try:
            self._ensure_db()
            if levels_snap:
                df_l = pd.DataFrame(levels_snap)
                self._db.execute("INSERT INTO depth_levels SELECT * FROM df_l")
            if metrics_snap:
                df_m = pd.DataFrame(metrics_snap)
                self._db.execute("INSERT INTO derived_metrics SELECT * FROM df_m")
            n_ticks = len(metrics_snap)
            logger.info(
                f"[DB] Flushed {len(levels_snap):,} level rows + "
                f"{n_ticks:,} metric rows → {self._current_db_path.name}"
            )
        except Exception as e:
            logger.error(f"[DB] Flush error: {e}")

    # ── WebSocket callback ─────────────────────────────────────────────────────

    def on_depth_update(self, data: dict) -> None:
        """
        Called by OpenAlgo SDK on every depth tick (SDK thread).
        Fast path: parse → batch append → done.
        Heavy work (DB writes) happens in the async flush loop.
        """
        if not _is_market_hours():
            return

        payload = data.get("data", {})
        symbol  = data.get("symbol", "").replace(":50", "")
        depth   = payload.get("depth", {})
        bids    = depth.get("buy",  [])
        asks    = depth.get("sell", [])
        ltp     = float(payload.get("ltp", 0) or 0)

        if not bids or not asks:
            return

        ts = datetime.now(timezone.utc)

        # Level rows (up to 100 per tick for depth-50)
        for i, b in enumerate(bids):
            self._levels_batch.append({
                "timestamp": ts,
                "symbol":    symbol,
                "ltp":       ltp,
                "side":      "bid",
                "level":     i + 1,
                "price":     float(b.get("price",    0)),
                "quantity":  int(b.get("quantity",   0)),
                "orders":    int(b.get("orders",     0)),
            })
        for i, a in enumerate(asks):
            self._levels_batch.append({
                "timestamp": ts,
                "symbol":    symbol,
                "ltp":       ltp,
                "side":      "ask",
                "level":     i + 1,
                "price":     float(a.get("price",    0)),
                "quantity":  int(a.get("quantity",   0)),
                "orders":    int(a.get("orders",     0)),
            })

        # Derived metrics row
        m = _compute_metrics(bids, asks, ltp)
        self._metrics_batch.append({
            "timestamp": ts,
            "symbol":    symbol,
            "ltp":       ltp,
            **m,
        })

        self._tick_count += 1

        # Force-flush if batch has grown large (schedule on event loop)
        if (self._tick_count - self._last_flush_ticks >= BATCH_FLUSH_SIZE
                and self.loop and self.loop.is_running()):
            self._last_flush_ticks = self._tick_count
            asyncio.run_coroutine_threadsafe(self._flush(), self.loop)

        if self._tick_count % 1000 == 0:
            logger.info(
                f"[REC] {self._tick_count:,} ticks | last: {symbol} "
                f"w_obi={m['w_obi']:+.1f} LTP={ltp:.2f} n={m['n_levels']}"
            )

    # ── Connection management ──────────────────────────────────────────────────

    def _connect(self) -> bool:
        """Attempt WS connect + subscribe. Returns True on success."""
        try:
            self._client = api(api_key=API_KEY, host=HOST, ws_url=WS_URL)
            self._client.connect()
            self._client.subscribe_depth(
                SUBSCRIBE_SYMBOLS,
                on_data_received=self.on_depth_update,
            )
            self._connected = True
            logger.info(f"[WS] Connected. Subscribed {len(TOP20_STOCKS)} stocks.")
            return True
        except Exception as e:
            logger.error(f"[WS] Connection failed: {e}")
            self._connected = False
            return False

    def _disconnect(self) -> None:
        if self._client:
            try:
                self._client.disconnect()
            except Exception:
                pass
            self._client = None
        self._connected = False

    async def _reconnect_loop(self) -> None:
        """
        Monitors connection health.  If the SDK drops (e.g. broker timeout),
        the flag goes False and this loop re-establishes the subscription.
        """
        while self._running:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            if not self._running:
                break
            if not self._connected:
                logger.warning("[WS] Not connected — starting reconnect sequence…")
                self._disconnect()
                connected = False
                for attempt in range(1, 6):
                    if not self._running:
                        break
                    logger.info(f"[WS] Reconnect attempt {attempt}/5…")
                    if self._connect():
                        connected = True
                        break
                    logger.warning(
                        f"[WS] Attempt {attempt} failed, "
                        f"waiting {RECONNECT_BACKOFF}s…"
                    )
                    await asyncio.sleep(RECONNECT_BACKOFF)
                if not connected:
                    logger.error(
                        "[WS] All reconnect attempts failed. "
                        "Will retry in 60s…"
                    )

    # ── Main run loop ──────────────────────────────────────────────────────────

    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()
        self._ensure_db()

        # Graceful shutdown on SIGINT / SIGTERM
        def _shutdown(*_):
            logger.info("[SYS] Shutdown signal received.")
            self._running = False
            self._connected = False

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                self.loop.add_signal_handler(sig, _shutdown)
            except NotImplementedError:
                pass  # Windows

        # ── Wait for market open ─────────────────────────────────────────────
        if not _is_market_hours():
            logger.info("[SYS] Outside market hours — waiting for 09:15 IST…")
            while self._running:
                if _is_market_hours():
                    break
                await asyncio.sleep(30)

        if not self._running:
            return

        # ── Initial connect ──────────────────────────────────────────────────
        if not self._connect():
            logger.error("[WS] Initial connection failed. Exiting.")
            return

        # ── Background tasks ─────────────────────────────────────────────────
        flush_task     = asyncio.create_task(self._flush_loop(),     name="flush")
        reconnect_task = asyncio.create_task(self._reconnect_loop(), name="reconnect")

        logger.info("[SYS] Depth recorder running. Ctrl-C to stop.")

        # ── Main heartbeat loop ───────────────────────────────────────────────
        last_hb = 0.0
        try:
            while self._running:
                if not _is_market_hours():
                    logger.info("[SYS] 15:35 reached — stopping collection.")
                    break
                now = time.monotonic()
                if now - last_hb >= HEARTBEAT_INTERVAL:
                    logger.info(
                        f"[HB] Running | ticks={self._tick_count:,} | "
                        f"batch_l={len(self._levels_batch):,} | "
                        f"batch_m={len(self._metrics_batch):,} | "
                        f"connected={self._connected}"
                    )
                    last_hb = now
                await asyncio.sleep(10)
        finally:
            self._running = False
            flush_task.cancel()
            reconnect_task.cancel()
            # Final flush to capture any remaining rows
            logger.info("[SYS] Final flush…")
            await self._flush()
            self._disconnect()
            if self._db:
                self._db.close()
            logger.info(
                f"[SYS] Recorder stopped. "
                f"Total ticks: {self._tick_count:,} | "
                f"DB: {self._current_db_path}"
            )


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        asyncio.run(DepthRecorder().run())
    except KeyboardInterrupt:
        pass
