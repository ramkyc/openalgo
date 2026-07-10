# Troubleshooting

## Start Here: `log/errors.jsonl`

Always read `log/errors.jsonl` first. Each line is structured JSON with full traceback, source file:line, and Flask request context.

## WebSocket Connection Issues

1. Ensure WebSocket server started (it starts automatically with `app.py`)
2. Check `WEBSOCKET_HOST` and `WEBSOCKET_PORT` in `.env` (this instance: port 8766)
3. For Gunicorn: confirm you are using `-w 1` (single worker)
4. Check firewall for port 8766

## Database Locked Errors

1. SQLite doesn't handle high concurrency well
2. Close all connections and restart the app
3. Confirm you are using `NullPool` — never `StaticPool` (see `docs/architecture/databases.md`)

## Broker Integration Not Loading

1. Check broker name in `VALID_BROKERS` (.env)
2. Verify `plugin.json` exists in `broker/{name}/`
3. Check broker module structure matches the pattern (see `docs/conventions/broker-patterns.md`)
4. Restart application to reload plugins

## React Frontend Build Errors

1. Ensure Node.js version matches `frontend/package.json` engines field
2. Delete `frontend/node_modules` and run `npm install`
3. Check for TypeScript errors: `npm run build`

## Eventlet / asyncio Errors in Production

Code that uses `asyncio` runs fine in dev but breaks under eventlet in production. See `docs/architecture/eventlet-runtime.md` for the full constraint list and the correct pattern for async work.
