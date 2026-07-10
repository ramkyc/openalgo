"""
diagnose_nifty.py — Run from the openalgo directory to diagnose NIFTY LTP=0.

Usage:
    cd ~/Developer/fyers_crk/openalgo
    uv run python diagnose_nifty.py

Checks:
  1. DB lookup — what brsymbol is stored for NIFTY / NIFTYBANK / INDIAVIX
  2. Fyers symbol-token API — which symbols are valid vs invalid
  3. HSM token built for each symbol
  4. ZMQ bus — whether NIFTY topics are being published (requires app running)
"""
from __future__ import annotations

import os, sys, time, json
sys.path.insert(0, os.path.dirname(__file__))

# ── 1. ENV / auth ──────────────────────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv()

from database.token_db import get_br_symbol

print("=" * 60)
print("NIFTY WebSocket Diagnostic")
print("=" * 60)

# ── 2. DB lookups ──────────────────────────────────────────────────────────────
print("\n── 1. DB brsymbol lookup ──")
checks = [
    ("NIFTY",     "NSE_INDEX"),
    ("BANKNIFTY", "NSE_INDEX"),
    ("INDIAVIX",  "NSE_INDEX"),
]
for sym, exch in checks:
    br = get_br_symbol(sym, exch)
    print(f"  {sym}@{exch} → brsymbol = {br!r}")

# ── 3. Fyers API token check ────────────────────────────────────────────────────
print("\n── 2. Fyers /data/symbol-token API ──")
import requests
from database.auth_db import get_auth_token_broker

openalgo_api_key = os.getenv("OPENALGO_API_KEY", "")
if not openalgo_api_key:
    print("  ERROR: OPENALGO_API_KEY not in .env")
    sys.exit(1)

result = get_auth_token_broker(openalgo_api_key)
# Returns (auth_token, broker) or (auth_token, feed_token, broker)
access_token = result[0] if result else None
if not access_token:
    print("  ERROR: Could not retrieve access token from DB")
    sys.exit(1)

print(f"  Access token (first 20 chars): {str(access_token)[:20]}…")

brsymbols = [get_br_symbol(sym, exch) for sym, exch in checks]
brsymbols = [b for b in brsymbols if b]

try:
    resp = requests.post(
        "https://api-t1.fyers.in/data/symbol-token",
        headers={"Authorization": access_token, "Content-Type": "application/json"},
        json={"symbols": brsymbols},
        timeout=10,
    )
    data = resp.json()
    print(f"  API status: {data.get('s')}")
    valid   = data.get("validSymbol",   {})
    invalid = data.get("invalidSymbol", [])
    print(f"  Valid symbols ({len(valid)}):   {list(valid.keys())}")
    print(f"  Invalid symbols ({len(invalid)}): {invalid}")

    print("\n── 3. HSM token derivation ──")
    from broker.fyers.streaming.fyers_token_converter import FyersTokenConverter
    conv = FyersTokenConverter(access_token)
    for brsym in brsymbols:
        fytoken = valid.get(brsym, "<INVALID>")
        if fytoken == "<INVALID>":
            print(f"  {brsym} → INVALID (Fyers rejected) — NO TICKS POSSIBLE")
        else:
            hsm = conv._convert_to_hsm_token(brsym, fytoken, "SymbolUpdate")
            print(f"  {brsym} → fytoken={fytoken} → hsm_token={hsm!r}")

except Exception as e:
    print(f"  ERROR calling Fyers API: {e}")

# ── 4. ZMQ check ───────────────────────────────────────────────────────────────
print("\n── 4. ZMQ topic check (5s) ──")
try:
    import zmq
    ctx = zmq.Context()

    # 4a: NIFTY-specific topics
    sub = ctx.socket(zmq.SUB)
    sub.connect("tcp://127.0.0.1:5556")
    sub.setsockopt(zmq.SUBSCRIBE, b"NSE_INDEX_NIFTY")
    sub.setsockopt(zmq.SUBSCRIBE, b"NSE_INDEX_NIFTYBANK")
    sub.setsockopt(zmq.SUBSCRIBE, b"NSE_INDEX_INDIAVIX")
    sub.setsockopt(zmq.RCVTIMEO, 500)
    seen = {}
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            parts = sub.recv_multipart()
            topic = parts[0].decode()
            seen[topic] = seen.get(topic, 0) + 1
        except zmq.Again:
            pass
    sub.close()

    if seen:
        for t, n in sorted(seen.items()):
            print(f"  {t}: {n} ticks")
    else:
        print("  No NSE_INDEX NIFTY/BANKNIFTY/VIX ticks in 5s")

        # 4b: Subscribe to ALL topics to see what IS flowing
        print("  → Checking what topics ARE being published (5s any-topic probe)…")
        sub2 = ctx.socket(zmq.SUB)
        sub2.connect("tcp://127.0.0.1:5556")
        sub2.setsockopt(zmq.SUBSCRIBE, b"")  # wildcard — catch everything
        sub2.setsockopt(zmq.RCVTIMEO, 500)
        all_seen = {}
        deadline2 = time.time() + 5
        while time.time() < deadline2:
            try:
                parts = sub2.recv_multipart()
                topic = parts[0].decode()
                all_seen[topic] = all_seen.get(topic, 0) + 1
            except zmq.Again:
                pass
        sub2.close()

        if all_seen:
            print(f"  ZMQ IS publishing — topics seen ({len(all_seen)} unique):")
            for t, n in sorted(all_seen.items()):
                print(f"    {t}: {n} ticks")
        else:
            print("  ZMQ bus is SILENT — app may not be running or ZMQ pub is down")

    ctx.term()
except ImportError:
    print("  pyzmq not installed — skip ZMQ check")
except Exception as e:
    print(f"  ZMQ error: {e}")

# ── 5. WebSocket proxy test ────────────────────────────────────────────────────
print("\n── 5. WebSocket proxy subscribe test (5s) ──")
try:
    import asyncio
    import websockets as _ws

    OPENALGO_API_KEY = os.getenv("OPENALGO_API_KEY", "")
    WS_URL = "ws://127.0.0.1:8766"

    async def _ws_test():
        seen = {}
        try:
            async with _ws.connect(WS_URL, open_timeout=5) as ws:
                # Authenticate
                await ws.send(json.dumps({"action": "authenticate", "api_key": OPENALGO_API_KEY}))
                # Subscribe NIFTY and BANKNIFTY in Quote mode
                for sym, exch in [("NIFTY","NSE_INDEX"), ("BANKNIFTY","NSE_INDEX"), ("INDIAVIX","NSE_INDEX")]:
                    await ws.send(json.dumps({"action":"subscribe","symbol":sym,"exchange":exch}))

                deadline = time.time() + 5
                while time.time() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                        msg = json.loads(raw)
                        if msg.get("type") == "market_data":
                            sym = msg.get("symbol","?")
                            ltp = msg.get("data",{}).get("ltp", 0)
                            seen[sym] = seen.get(sym, 0) + 1
                            if seen[sym] == 1:
                                print(f"  ✓ {sym}: first tick LTP={ltp}")
                    except asyncio.TimeoutError:
                        pass
        except Exception as e:
            print(f"  WS error: {e}")
        return seen

    ws_seen = asyncio.run(_ws_test())
    if not ws_seen:
        print("  No market_data messages received via WebSocket in 5s")
    else:
        for sym, n in sorted(ws_seen.items()):
            print(f"  {sym}: {n} ticks via WebSocket")
except ImportError:
    print("  websockets package not installed — skip WS test")
except Exception as e:
    print(f"  WS test error: {e}")

print("\n" + "=" * 60)
print("Diagnosis complete.")
print("=" * 60)
