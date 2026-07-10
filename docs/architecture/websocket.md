# WebSocket Architecture

Real-time market data flows through a three-layer pipeline:

## Layer 1 — Broker WebSocket Adapters (`broker/*/streaming/`)

Each broker has a WebSocket adapter that connects to the broker's proprietary feed and normalizes data into OpenAlgo's internal format.

Connection pooling per broker:
- `MAX_SYMBOLS_PER_WEBSOCKET` (default: 1000)
- `MAX_WEBSOCKET_CONNECTIONS` (default: 3)
- Total capacity: **3000 symbols**

**Fyers streaming** uses a proprietary HSM binary protocol (`fyers_hsm_websocket.py`) rather than a standard WebSocket API. Adapter components: `fyers_adapter.py` → `fyers_hsm_websocket.py` + `fyers_mapping.py` + `fyers_token_converter.py`.

## Layer 2 — ZeroMQ Message Bus (port 5556)

Broker adapters publish normalized tick data to a ZeroMQ PUB socket. This decouples the broker feed from client delivery — the broker adapter runs independently and never blocks on slow clients. Also used for cache invalidation events across modules.

## Layer 3 — Unified WebSocket Proxy Server (port 8766)

`websocket_proxy/server.py` subscribes to ZeroMQ, manages client WebSocket connections, handles symbol subscriptions/unsubscriptions, and delivers filtered ticks to each connected client. Includes per-symbol throttling to prevent flooding slow clients.

## Frontend Real-Time Data (MarketDataManager)

`frontend/src/lib/MarketDataManager.ts` is a singleton managing all WebSocket subscriptions across React components:

- Ref-counted subscriptions (unsubscribes only when last consumer leaves)
- Callback fan-out to multiple simultaneous subscribers
- Auto-fallback to REST polling (`/api/v1/multiquotes` every 5s) after 3 WebSocket failures

Hooks: `useLivePrice`, `useMarketData`, `useMarketStatus` wrap the singleton.

## Flask-SocketIO (UI Events)

Separate from market data — Flask-SocketIO handles real-time UI updates:
- `order_update` — order placement, modification, cancellation
- `analyzer_update` — sandbox trade results
- `cache_loaded` — master contract cache ready

## Port Reference (this instance)

| Service | Port |
|---------|------|
| ZeroMQ message bus | 5556 |
| WebSocket proxy | 8766 |
| Flask app | 5001 |
