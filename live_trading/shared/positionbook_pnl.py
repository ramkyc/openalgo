"""
live_trading/shared/positionbook_pnl.py
========================================
Fetch the Fyers-computed net P&L for a fully-closed position from the
OpenAlgo positionbook API.

This is the authoritative source for net P&L — it uses actual sandbox fill
prices and Fyers' own charge computation, not a hardcoded rate table.

Usage:
    from live_trading.shared.positionbook_pnl import fetch_closed_pnl

    net = fetch_closed_pnl("NIFTY26JUN2524000CE")
    # Returns float (Fyers net P&L) or None if position not found / still open

Architecture note (multi-leg technical debt):
    This function returns P&L for a SINGLE symbol only. Iron fly and other
    multi-leg strategies store one combined record in performance.db but trade
    4+ symbols. For multi-leg bots the positionbook P&L must be summed across
    all leg symbols. This is not yet implemented — the embedded cost-formula
    fallback in trade_logger.py is used for those bots instead.
    Tracked: docs/trading/bot-pipeline.md § Technical Debt
"""

from __future__ import annotations

import logging
import os

import requests

logger = logging.getLogger(__name__)

_DEFAULT_HOST = "http://127.0.0.1:5001"


def fetch_closed_pnl(
    symbol:  str,
    api_key: str | None = None,
    host:    str | None = None,
    timeout: float = 3.0,
) -> float | None:
    """
    Return the Fyers-computed net P&L for *symbol* if the position is fully
    closed (quantity == 0) in the OpenAlgo positionbook.

    Returns None if:
    - Position is still open (quantity != 0)
    - Symbol not found in positionbook
    - API call fails for any reason

    Silent on all errors — never raises.
    """
    api_key = api_key or os.getenv("OPENALGO_API_KEY")
    host    = host    or os.getenv("HOST_SERVER", _DEFAULT_HOST)

    if not api_key:
        return None

    try:
        resp = requests.post(
            f"{host}/api/v1/positionbook",
            json={"apikey": api_key},
            timeout=timeout,
        )
        data = resp.json()
        if data.get("status") != "success":
            return None

        for pos in data.get("data", []):
            if pos.get("symbol") != symbol:
                continue
            qty = int(pos.get("quantity", 1))
            if qty != 0:
                return None          # position still open — P&L is unrealised
            pnl = pos.get("pnl")
            return float(pnl) if pnl is not None else None

    except Exception as exc:
        logger.debug(f"[positionbook_pnl] fetch failed for {symbol}: {exc}")

    return None
