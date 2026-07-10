# Eventlet Runtime Constraints

## Production Runtime

Production deployments run under **Gunicorn with eventlet worker**:

```bash
uv run gunicorn --worker-class eventlet -w 1 app:app
```

### Critical Constraints

- **No `asyncio`**: eventlet monkey-patches the stdlib and is incompatible with `asyncio.run()`, `async/await`, and `asyncio.get_event_loop()`. Any code that needs async behavior must use eventlet green threads or run async work on a separate real OS thread (see `telegram_bot_service.py:_render_plotly_png` for the pattern).
- **Single worker (`-w 1`)**: Required for WebSocket and SocketIO compatibility. Flask-SocketIO state is in-process and cannot be shared across workers.
- **`threading.local()` maps to green threads**: eventlet monkey-patches `threading.local()` so each green thread gets its own session. This is why `scoped_session` works correctly under eventlet.

## Development Runtime (Mac/Windows)

The Flask development server (`uv run app.py`) uses standard threading, not eventlet. Code must work in both environments.

Key differences from production:
- No monkey-patching — standard `threading` and `socket` modules
- `asyncio` works normally on dev server but will **break under eventlet in production**
- SQLite concurrency behavior differs (Windows is more restrictive with file locking)

## Rule

Never write code that uses `asyncio` at the module level — it will silently work in dev and silently fail in production under eventlet.
