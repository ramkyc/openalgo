# Logging Conventions

## Always Use `logger.exception()`

```python
from utils.logging import get_logger
logger = get_logger(__name__)

try:
    result = broker_module.place_order(data, token)
    return {'status': 'success', 'data': result}
except Exception as e:
    logger.exception(f"Error placing order: {e}")   # auto-captures full traceback
    return {'status': 'error', 'message': str(e)}
```

**Never use:**
- `traceback.print_exc()`
- `traceback.format_exc()`
- `import traceback` at all

These bypass centralized logging and won't appear in the JSON error log.

## Centralized Logging Architecture (`utils/logging.py`)

All logging flows through Python's standard `logging` module, configured in `setup_logging()` at import time. Every module uses `logger = get_logger(__name__)`.

Three output handlers (all share the same `SensitiveDataFilter` to redact API keys/tokens):

1. **Console** (always active): Colored output via `ColoredFormatter`, level controlled by `LOG_LEVEL` env var.
2. **File** (if `LOG_TO_FILE=True`): Daily-rotated text logs in `log/openalgo_YYYY-MM-DD.log`, retained for `LOG_RETENTION` days.
3. **JSON error log** (always active): `log/errors.jsonl` — structured JSON Lines, ERROR+ only.

## Debugging: Read `log/errors.jsonl` First

Each line is a JSON object with: timestamp, logger name, module, source file:line, error message, full exception traceback (if any), and Flask request context (method, path, IP) when available. Auto-truncated to the last 1000 entries on app startup.
