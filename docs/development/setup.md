# Development Setup

## Prerequisites

- Python 3.12+ (required per pyproject.toml)
- Node.js 20/22/24 (only if editing React frontend)
- **uv package manager** — never use global Python

## Initial Setup

```bash
# Configure environment
cp .sample.env .env

# Generate new APP_KEY and API_KEY_PEPPER:
uv run python -c "import secrets; print(secrets.token_hex(32))"

# Run application
# Note: frontend/dist/ is force-committed by CI on main
# You only need Node.js if you are actively editing React code
uv run app.py
```

Access points:
- Main app: http://127.0.0.1:5001
- API docs: http://127.0.0.1:5001/api/docs
- React frontend: http://127.0.0.1:5001/react

## Always Use UV

```bash
uv run app.py              # run the app
uv run python script.py    # run any Python script
uv add package_name        # add new package (updates pyproject.toml)
uv sync                    # sync after pulling changes
```

Never use global Python or manually manage virtual environments.

## Production (Linux only)

```bash
uv run gunicorn --worker-class eventlet -w 1 app:app
# -w 1 is mandatory for WebSocket compatibility
```

## Environment Variables (.env)

Critical variables:

| Variable | Purpose |
|----------|---------|
| `APP_KEY` | Flask secret key — generate with `secrets.token_hex(32)` |
| `API_KEY_PEPPER` | Encryption pepper — generate with `secrets.token_hex(32)` |
| `VALID_BROKERS` | Comma-separated list of enabled broker names |
| `BROKER_API_KEY` / `BROKER_API_SECRET` | Active broker credentials |
| `FLASK_DEBUG` | Development only |
| `WEBSOCKET_HOST` / `WEBSOCKET_PORT` | WebSocket server config (this instance: 8766) |
| `MAX_SYMBOLS_PER_WEBSOCKET` | Symbol limit per WS connection (default 1000) |
| `MAX_WEBSOCKET_CONNECTIONS` | Max WS connections (default 3) |
