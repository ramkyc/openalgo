# Database Architecture

OpenAlgo uses 6 separate SQLite databases for isolation. Each has its own `init_db()` in `/database/`, called at startup from `app.py`.

| File | Purpose |
|------|---------|
| `db/openalgo.db` | Main: users, orders, positions, settings |
| `db/logs.db` | Traffic and API logs |
| `db/latency.db` | Latency monitoring |
| `db/health.db` | Health monitoring data |
| `db/sandbox.db` | Sandbox/Analyzer trading mode (isolated from live trading) |
| `db/historify.duckdb` | Historical market data (DuckDB) |

## SQLite Connection Pooling — NullPool Only

All SQLite databases use `NullPool` — each operation gets a fresh connection, closed immediately after use.

**Do NOT use `StaticPool`** (single shared connection) — it causes:
- `"bad parameter or other API misuse"` errors
- `"cannot commit - SQL statements in progress"` errors

Root cause: concurrent requests corrupt the shared connection's cursor state under both eventlet green threads and standard threading. This applies to all platforms (Windows, Mac, Linux).

## FD Leak Prevention (5 layers)

Session cleanup is enforced at 5 points to prevent file descriptor leaks:

1. `app.py` `teardown_appcontext` — removes all scoped sessions after every request
2. `traffic_logger.py` — explicit `logs_session.remove()` in finally block
3. `security_middleware.py` — explicit cleanup for banned-IP WSGI path
4. `blueprints/traffic.py` — teardown handler
5. `blueprints/security.py` — teardown handler

## HTTP Client Pooling

Broker API calls use `httpx` with HTTP/2 connection pooling (`utils/httpx_client.py`). A single shared client instance per broker session maintains persistent connections to the broker's API servers, avoiding TCP/TLS handshake overhead on every order or data request.

## Always Use SQLAlchemy ORM

Never use raw SQL. Each database module has its own engine and session:

```python
from database.auth_db import User

user = User.query.filter_by(username='admin').first()
```
