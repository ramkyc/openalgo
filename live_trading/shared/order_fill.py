"""
live_trading/shared/order_fill.py
==================================
Fetch the actual filled price for a placed order via OpenAlgo's
/api/v1/orderstatus endpoint.

Works identically in analyze (sandbox) and live broker modes — both return
the same `average_price` field once the order's status is "complete". This
is the authoritative execution price for P&L bookkeeping: LTP snapshots
taken at signal time do not reflect the bid/ask spread (sandbox) or slippage
(live) actually incurred at fill. Use LTP for live decisions (SL/target/
signal triggers); use fetch_fill_price() for anything written to
performance.db / paper_trades.db.

Usage:
    from live_trading.shared.order_fill import fetch_fill_price

    price = fetch_fill_price(order_id, strategy="NIFTY_EMA_SPREAD")
    # Returns float (actual average fill price) or None if unavailable —
    # callers should fall back to their own LTP snapshot when None.
"""

from __future__ import annotations

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

_DEFAULT_HOST = "http://127.0.0.1:5001"


def fetch_fill_price(
    order_id: str,
    strategy: str,
    api_key: str | None = None,
    host: str | None = None,
    timeout: float = 3.0,
    retries: int = 3,
    retry_delay: float = 0.5,
) -> float | None:
    """
    Return the actual average fill price for *order_id*, or None if the
    order isn't complete yet or the lookup fails for any reason.

    Silent on all errors — never raises. Callers must fall back to their
    own LTP-based price snapshot when this returns None.
    """
    api_key = api_key or os.getenv("OPENALGO_API_KEY")
    host = host or os.getenv("HOST_SERVER", _DEFAULT_HOST)

    if not api_key or not order_id:
        return None

    for attempt in range(retries):
        try:
            resp = requests.post(
                f"{host}/api/v1/orderstatus",
                json={"apikey": api_key, "strategy": strategy, "orderid": str(order_id)},
                timeout=timeout,
            )
            data = resp.json()
            if data.get("status") != "success":
                return None

            order = data.get("data", {})
            if (order.get("order_status") or "").lower() != "complete":
                if attempt < retries - 1:
                    time.sleep(retry_delay)
                    continue
                return None

            avg = order.get("average_price")
            return float(avg) if avg else None

        except Exception as exc:
            logger.debug(f"[order_fill] fetch failed for order {order_id}: {exc}")
            if attempt < retries - 1:
                time.sleep(retry_delay)

    return None
