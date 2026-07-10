# Python Style

## Formatter / Linter: Ruff

```bash
uv run ruff check .          # lint (errors + warnings)
uv run ruff check . --fix    # auto-fix safe issues
uv run ruff format .         # format (replaces Black)
```

Ruff rules enabled: `E`, `F`, `W` (pycodestyle/pyflakes), `I` (isort), `B` (bugbear), `C4` (comprehensions), `UP` (pyupgrade).
Line-length: 100. Target: Python 3.12.
Excluded: `.venv`, `frontend`, `db`, `log`, `strategies`.

## Security Tooling (dev group)

```bash
uv run --group dev bandit -r . -x .venv,frontend   # security scan
uv run --group dev pip-audit                        # CVE check on deps
uv run --group dev detect-secrets scan              # secret leak scan
```

## Conventions

- 4 spaces for indentation
- Google-style docstrings
- Imports: Standard library → Third-party → Local

## Git Commit Messages (Conventional Commits)

- `feat:` New features
- `fix:` Bug fixes
- `docs:` Documentation changes
- `refactor:` Code refactoring

## React / TypeScript

- Follow Biome.js linting rules (`frontend/biome.json`)
- Functional components with hooks
- Component files use PascalCase: `MyComponent.tsx`

## API Authentication Pattern

```python
# In request body (recommended):
{"apikey": "YOUR_API_KEY", "symbol": "NSE:SBIN-EQ", ...}
# Or header: X-API-KEY: YOUR_API_KEY
```

## Standard JSON Response Pattern

```python
return {'status': 'success' | 'error', 'message': '...', 'data': {...}}
```

## React Data Fetching

Use TanStack Query for server state, `MarketDataManager` hooks for live prices:

```typescript
import { useQuery } from '@tanstack/react-query';

const { data, isLoading, error } = useQuery({
  queryKey: ['positions'],
  queryFn: () => api.getPositions()
});
```
