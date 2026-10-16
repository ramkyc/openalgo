"""
OBI Engine — Real-time 50-Level Order Book Imbalance Subscriber
================================================================
Subscribes to depth-50 for NIFTY ATM option symbols via OpenAlgo WebSocket.
Maintains a thread-safe, continuously updated OBI snapshot per symbol.

Weighted OBI formula (proximity-weighting: nearest levels get highest weight):
    weighted_bid = Σ qty_i / rank_i  for i in 1..N (bids, best price first)
    weighted_ask = Σ qty_i / rank_i  for i in 1..N (asks, best price first)
    OBI = (weighted_bid - weighted_ask) / (weighted_bid + weighted_ask) × 100

Range: −100 (pure ask/selling pressure) → +100 (pure bid/buying pressure)
Interpretation for sell-CE strategy:
    OBI < 0   → net sellers dominate CE → confirms bearish view → TRADE
    OBI ≥ 0   → bid pressure on CE → uncertain direction → SKIP (ghost track)

Depth subscription design:
    Symbols are subscribed with ':50' suffix (e.g. 'NIFTY12MAY2624000CE:50').
    The Fyers adapter treats this as a TBT (50-level) request, falling back
    to standard 5-level HSM depth if TBT is unavailable.  n_levels in each
    snapshot reflects what was actually delivered (50 or 5).

Usage:
    engine = OBIEngine(api_key, host, ws_url)
    engine.start([{"exchange": "NFO", "symbol": "NIFTY27MAR2624000CE"}])
    ...
    snap = engine.get_snapshot("NIFTY27MAR2624000CE")
    # snap = {"obi": -32.4, "ltp": 147.5, "bid_tot": 1200, "ask_tot": 3800, "n_levels": 50}
"""

import threading
import logging
import sys
from collections import deque
from pathlib import Path
from typing import Optional

# Ensure project root is on sys.path so 'from openalgo import api' works
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from openalgo import api as OpenAlgoAPI

logger = logging.getLogger(__name__)


# ── Core math ─────────────────────────────────────────────────────────────────

def weighted_obi(bids: list, asks: list) -> float:
    """
    Compute proximity-weighted Order Book Imbalance.
    Bids and asks are lists of dicts with 'price' and 'quantity' keys,
    sorted from best price inward (as delivered by OpenAlgo depth callback).
    """
    wb = sum(b["quantity"] / (i + 1) for i, b in enumerate(bids))
    wa = sum(a["quantity"] / (i + 1) for i, a in enumerate(asks))
    denom = wb + wa
    return (wb - wa) / denom * 100.0 if denom > 0 else 0.0


def raw_obi(bids: list, asks: list) -> float:
    """
    Simple unweighted OBI (total bid qty vs total ask qty).
    Kept as a secondary diagnostic metric.
    """
    tb = sum(b["quantity"] for b in bids)
    ta = sum(a["quantity"] for a in asks)
    denom = tb + ta
    return (tb - ta) / denom * 100.0 if denom > 0 else 0.0


# ── OBIEngine ─────────────────────────────────────────────────────────────────

class OBIEngine:
    """
    Thread-safe WebSocket depth-50 subscriber.
    Holds the latest OBI snapshot for each subscribed symbol.

    The depth callback runs on the OpenAlgo SDK's internal thread.
    All reads/writes to _cache are protected by a threading.Lock so the
    main APScheduler thread can safely call get_snapshot() at any time.
    """

    # Maximum OBI history kept per symbol for rolling average
    OBI_HISTORY_MAXLEN = 10

    def __init__(self, api_key: str, host: str, ws_url: str, use_depth_50: bool = False):
        """
        Args:
            api_key:       OpenAlgo API key
            host:          OpenAlgo host (e.g. http://127.0.0.1:8080)
            ws_url:        OpenAlgo WebSocket URL
            use_depth_50:  If True, append ':50' suffix to request TBT 50-level depth.
                           ⚠️  Fyers TBT has a hard limit of 5 symbols per account.
                           If all 5 slots are taken by another bot (e.g. equity_obi
                           subscribes 20 symbols first), the `:50` subscription silently
                           falls back to HSM inside OpenAlgo — but the ZMQ topic is still
                           registered as `symbol:50` while HSM publishes without the suffix,
                           so the callback never fires and tick_count stays 0 permanently.
                           Default False: use standard 5-level HSM depth, which always
                           works and avoids the TBT slot-limit / topic-mismatch issue.
        """
        self._api_key      = api_key
        self._host         = host
        self._ws_url       = ws_url
        self._use_depth_50 = use_depth_50
        self._client       = None
        self._lock         = threading.Lock()
        # cache: clean_symbol (no ':50') → snapshot dict
        self._cache: dict[str, dict] = {}
        # rolling OBI history: clean_symbol → deque of weighted OBI floats
        self._obi_history: dict[str, deque] = {}
        self._subscribed: list[dict] = []
        self._connected = False
        self._tick_count = 0    # diagnostic counter

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self, symbols: list[dict]) -> None:
        """
        Connect and subscribe to depth-50 for the given symbols.

        symbols format: [{"exchange": "NFO", "symbol": "NIFTY27MAR2624000CE"}, ...]
        The ':50' depth suffix is added automatically.
        """
        self._client = OpenAlgoAPI(
            api_key=self._api_key,
            host=self._host,
            ws_url=self._ws_url,
        )
        self._subscribed = self._normalize(symbols)
        self._client.connect()
        self._client.subscribe_depth(
            self._subscribed,
            on_data_received=self._on_depth,
        )
        self._connected = True
        syms  = [s["symbol"] for s in self._subscribed]
        depth = "depth-50 (TBT)" if self._use_depth_50 else "depth-5 (HSM)"
        logger.info(f"[OBI] Connected. Subscribed {depth} for: {syms}")

    def update_symbols(self, new_symbols: list[dict]) -> None:
        """
        Replace current depth subscription with a new set of symbols.
        Called when the ATM strike shifts by ≥ 1 step during the session.
        """
        if not self._connected:
            self.start(new_symbols)
            return
        try:
            self._client.unsubscribe_depth(self._subscribed)
        except Exception as e:
            logger.warning(f"[OBI] unsubscribe warning: {e}")
        self._subscribed = self._normalize(new_symbols)
        self._client.subscribe_depth(
            self._subscribed,
            on_data_received=self._on_depth,
        )
        new_syms = [s["symbol"] for s in self._subscribed]
        logger.info(f"[OBI] Re-subscribed: {new_syms}")

    def get_snapshot(self, symbol: str) -> Optional[dict]:
        """
        Return the latest depth snapshot for a symbol (case-sensitive, no ':50').
        Returns None if no update has been received yet.

        Snapshot dict keys:
            obi      — proximity-weighted OBI (-100 to +100)
            raw_obi  — unweighted OBI (diagnostic)
            ltp      — last traded price
            vwmp     — volume-weighted mid-price (centre of gravity of book)
            bid_tot  — total bid quantity across all levels
            ask_tot  — total ask quantity across all levels
            n_levels — number of depth levels received (max 50)
        """
        clean = symbol.replace(":50", "")
        with self._lock:
            snap = self._cache.get(clean)
            return dict(snap) if snap else None

    def get_obi(self, symbol: str) -> Optional[float]:
        """Convenience: return just the weighted OBI float, or None."""
        snap = self.get_snapshot(symbol)
        return snap["obi"] if snap else None

    def get_rolling_obi(self, symbol: str, n: int = 3) -> Optional[float]:
        """
        Return the mean of the last *n* weighted OBI values for *symbol*.
        Returns None if fewer than *n* values have been received yet.

        Use this instead of get_obi() at signal-check time to filter
        single-tick noise.  n=3 means all three of the most recent ticks
        must have sustained the directional pressure.

        Example:
            rolling = engine.get_rolling_obi("NIFTY25MAY24000CE", n=3)
            gate_passes = rolling is not None and rolling < OBI_THRESHOLD
        """
        clean = symbol.replace(":50", "")
        with self._lock:
            hist = self._obi_history.get(clean)
            if hist is None or len(hist) < n:
                return None
            # Use the most recent n values
            recent = list(hist)[-n:]
        return sum(recent) / len(recent)

    def is_stale(self, symbol: str, max_age_seconds: float = 5.0) -> bool:
        """
        Returns True if the last depth update for this symbol is older than
        max_age_seconds (or no update has been received yet).
        Used to detect WebSocket drops or subscription gaps.
        """
        import time
        snap = self.get_snapshot(symbol)
        if snap is None:
            return True
        age = time.time() - snap.get("_ts", 0)
        return age > max_age_seconds

    @property
    def tick_count(self) -> int:
        """Total depth updates received since start (diagnostic)."""
        return self._tick_count

    def stop(self) -> None:
        """Cleanly disconnect the WebSocket."""
        if self._client and self._connected:
            try:
                self._client.disconnect()
            except Exception:
                pass
            self._connected = False
            logger.info("[OBI] Disconnected.")

    # ── Internal ──────────────────────────────────────────────────────────────

    def _normalize(self, symbols: list[dict]) -> list[dict]:
        """
        Optionally append ':50' suffix for TBT 50-level depth.

        When use_depth_50=True:
            The Fyers adapter routes the subscription through TBT WebSocket.
            ⚠️  Fyers TBT is limited to 5 symbols per account. If the limit is
            already reached (e.g. equity_obi subscribes 20 symbols first), the
            OpenAlgo adapter falls back to HSM internally — but the ZMQ topic is
            still registered under the ':50' key while HSM data is published
            without it.  This topic mismatch means the callback never fires and
            tick_count stays 0 permanently ("Warming up" all day).

        When use_depth_50=False (default):
            Uses standard 5-level HSM depth.  The ZMQ topic and HSM publish key
            both use the plain symbol — no mismatch.  Always works, independent
            of TBT slot availability.  Verified working for NFO options via
            banknifty_bb_options_bot (5-level depth).
        """
        result = []
        for s in symbols:
            base = s["symbol"].replace(":50", "")
            sym  = base + ":50" if self._use_depth_50 else base
            result.append({"exchange": s["exchange"], "symbol": sym})
        return result

    def _on_depth(self, data: dict) -> None:
        """
        Callback invoked by OpenAlgo SDK on every depth tick (runs on SDK thread).
        Parses depth payload, computes OBI metrics + VWMP, and updates _cache and
        _obi_history thread-safely.
        """
        import time
        payload = data.get("data", {})
        raw_sym = data.get("symbol", "")
        clean   = raw_sym.replace(":50", "")

        depth = payload.get("depth", {})
        bids  = depth.get("buy",  [])
        asks  = depth.get("sell", [])
        ltp   = float(payload.get("ltp", 0) or 0)

        if not bids or not asks:
            return

        obi  = weighted_obi(bids, asks)
        robi = raw_obi(bids, asks)
        tb   = sum(b["quantity"] for b in bids)
        ta   = sum(a["quantity"] for a in asks)
        total = tb + ta

        # Volume-Weighted Mid-Price: centre-of-gravity of the entire order book
        vwmp = (
            (
                sum(b["price"] * b["quantity"] for b in bids)
                + sum(a["price"] * a["quantity"] for a in asks)
            ) / total
            if total > 0 else ltp
        )

        with self._lock:
            self._cache[clean] = {
                "obi":      obi,
                "raw_obi":  robi,
                "ltp":      ltp,
                "vwmp":     vwmp,
                "bid_tot":  tb,
                "ask_tot":  ta,
                "n_levels": len(bids),
                "_ts":      time.time(),
            }
            # Rolling OBI history (for get_rolling_obi)
            if clean not in self._obi_history:
                self._obi_history[clean] = deque(maxlen=self.OBI_HISTORY_MAXLEN)
            self._obi_history[clean].append(obi)

        self._tick_count += 1

        if self._tick_count % 50 == 0:
            logger.debug(
                f"[OBI] {clean} | OBI={obi:+.1f} raw={robi:+.1f} "
                f"VWMP={vwmp:.2f} LTP={ltp} bids={tb} asks={ta} ticks={self._tick_count}"
            )
