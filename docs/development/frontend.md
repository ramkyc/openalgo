# Frontend Build Notes

## CI/CD — You Usually Don't Need to Build

`frontend/dist/` is force-committed to `main` by CI (`commit-dist` job in `.github/workflows/ci.yml`) after every successful push. The CI workflow runs `git add -f` to override the `.gitignore` on `main` only.

**Who needs to build locally:**
- **Backend-only contributors**: No — just `git pull` from main.
- **Production servers**: No — `git pull` from main already has the latest UI.
- **React developers**: Yes — `cd frontend && npm install && npm run build` (or `npm run dev` for hot reload) to test your own changes.
- **Feature branch devs**: May need a local build if the branch predates the latest CI build.

## Frontend Commands

```bash
cd frontend

npm install            # install dependencies
npm run dev            # hot-reload dev server
npm run build          # production build (output: frontend/dist/)
npm run lint           # Biome.js linting
npm run format         # Biome.js formatting
npm test               # run tests (CI only, not needed locally)
npm run e2e            # end-to-end tests
```

**When building locally: `npm run build` only — tests run in CI, not locally.**

## Version Bumping

There are **two independent versions** in this repo. Do not confuse them.

### 1. Platform version (e.g. `2.0.1.0`)

Source of truth: `utils/version.py`. Bumping touches **two files** and regenerates the lockfile:

1. `utils/version.py` — `VERSION = "x.y.z.w"`
2. `pyproject.toml` — `version = "x.y.z.w"` (line 4)
3. `uv sync` to regenerate `uv.lock`

```bash
# Verify after bump:
uv run python -c "from utils.version import get_version; print(get_version())"
```

Surfaces: UI footer / about page, API responses, Docker image tags.

### 2. OpenAlgo Python SDK pin (e.g. `openalgo==1.0.49`)

Separate client library on PyPI. Bumping touches:

1. `pyproject.toml` — update `openalgo==X.Y.Z` in `dependencies`
2. `requirements.txt` — update `openalgo==X.Y.Z`
3. `requirements-nginx.txt` — update `openalgo==X.Y.Z`
4. `uv sync` to regenerate `uv.lock`

**Rule:** Releasing OpenAlgo → bump #1. New SDK on PyPI → bump #2. They are unrelated.
