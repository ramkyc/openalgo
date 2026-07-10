# Architecture Overview

OpenAlgo is four products in a single self-hosted instance, all sharing one broker session and WebSocket feed.

| Surface | Route | Purpose |
|---------|-------|---------|
| Unified Broker API | `/api/v1/` | External platforms: TradingView, Amibroker, ChartInk, Excel, Python, MCP |
| Python Strategy Host | `/python` | In-browser CodeMirror editor — paste scripts, schedule on IST times, run parallel strategies with process isolation and live logs |
| Flow (No-Code Builder) | `/flow` | Drag-and-drop nodes: market data → indicators → conditions → order execution; JSON import/export |
| Options Trading Suite | `/tools` | 12 analytical tools: Strategy Builder, Option Chain, IV Smile, Max Pain, Vol Surface, GEX, OI Tracker, Straddle Chart, etc. |

All surfaces share the Sandbox engine (₹1 Crore virtual capital, exchange-aligned auto square-off) and support Telegram alerts.

## Backend Structure

- `app.py` — Entry point; loads env, initializes all databases, registers all blueprints
- `blueprints/` — Flask route handlers (UI pages + webhooks)
- `restx_api/` — REST API endpoints (`/api/v1/`) with auto-generated Swagger docs
- `broker/` — 30+ broker integrations (plugin system, loaded dynamically)
- `services/` — Business logic layer (one file per feature area)
- `database/` — SQLAlchemy models and DB initialization functions
- `utils/` — Shared utilities package
- `websocket_proxy/` — Unified WebSocket server (port 8766)

## Request Processing Pipeline

WSGI middleware wraps in reverse order — last registered is outermost:

```
Incoming Request
  → TrafficLoggerMiddleware (logs method, path, duration, status code)
    → SecurityMiddleware (checks IP ban list, blocks banned IPs with 403)
      → CSP Middleware (sets Content-Security-Policy headers)
        → Flask app (routing, blueprints, CSRF, session)
          → API key auth (for /api/v1/ endpoints)
            → Service layer → Broker API
```

Registered in `app.py:319-323`: security middleware first, then traffic logging (so traffic wraps outside security). Session cleanup in `teardown_appcontext` after each response.

## Event-Driven Architecture

State changes are broadcast to the UI in real-time:

- **Order placed** → `order_router_service.py` → broker API → `socketio.emit("order_update")` → UI
- **Market data tick** → broker WebSocket adapter → ZeroMQ PUB → WebSocket proxy → client browser
- **Master contract loaded** → `master_contract_cache_hook.py` → `socketio.emit("cache_loaded")` → UI
- **Analyzer trade** → `sandbox_service.py` → `socketio.emit("analyzer_update")` → sandbox UI

## Security and Deployment Model

- **Single user per deployment** — no multi-user, no privilege escalation. One user, one broker session per instance.
- **Self-hosted** — server access = full control. No SaaS component.
- All install scripts auto-generate unique `APP_KEY` and `API_KEY_PEPPER` via `secrets.token_hex(32)`.
- **SEBI static IP mandate** (effective April 1, 2026): All transactional API orders require broker-side static IP whitelisting. Stolen credentials cannot be used from an attacker's machine — broker rejects requests from non-registered IPs. Attacks routed *through* the OpenAlgo server (which has the registered IP) are still viable.
- External platforms (TradingView, GoCharting, Chartink) send API keys in JSON body or URL query params — they cannot set custom HTTP headers. Accepted architectural trade-off.
- The MCP server (`mcp/mcpserver.py`) is local-only, communicates via stdio. NOT remotely exposed.
- Indian broker tokens expire daily at ~3:00 AM IST. Session management is aligned to this schedule.

## Key Feature Areas

| Feature | Blueprint | Service | Notes |
|---------|-----------|---------|-------|
| Order routing | `blueprints/orders.py` | `services/place_order_service.py` | Auto + Semi-Auto (Action Center) modes |
| Analyzer/Paper trading | `blueprints/analyzer.py` | `services/analyzer_service.py` | Uses `db/sandbox.db`, ₹1Cr virtual capital |
| Historical data | `blueprints/historify.py` | `services/historify_service.py` | DuckDB backend, scheduler service |
| Options tools | `restx_api/` | `services/option_*.py` | Chain, Greeks, synthetic futures |
| Telegram bot | `blueprints/telegram.py` | `services/telegram_bot_service.py` | Trade alerts + bot interface |
| Flow editor | `blueprints/flow.py` | `services/flow_*.py` | Visual strategy builder |

## MCP Integration

Two MCP endpoints:
- `blueprints/mcp_http.py` — streamable HTTP transport for MCP
- `blueprints/mcp_oauth.py` — OAuth2 authorization for remote MCP clients; state stored in `database/oauth_db.py`
- `mcp/mcpserver.py` — local-only stdio MCP server (not remotely exposed)

## This Instance

| Setting | Value |
|---------|-------|
| Flask port | 5001 |
| WebSocket port | 8766 |
| ZeroMQ port | 5556 |
| Session cookie | `openalgo_crk` |
| Broker | Fyers (primary account) |
