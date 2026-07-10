# Testing

## Backend Tests

```bash
uv run pytest test/ -v                              # all tests
uv run pytest test/test_broker.py -v               # specific file
uv run pytest test/test_broker.py::test_fn -v      # single test
uv run pytest test/ --cov                          # with coverage
```

## Frontend Tests

```bash
cd frontend
npm test                   # run all tests
npm run test:coverage      # with coverage
npm run e2e                # end-to-end tests
```

## Manual Testing

Most testing is done manually via:
- Web UI: http://127.0.0.1:5001
- Swagger API: http://127.0.0.1:5001/api/docs (full interactive API docs)
- API Analyzer: http://127.0.0.1:5001/analyzer

See also: `docs/test/MANUAL_TESTING_GUIDE.md` and `docs/test/QUICK_TEST_CHECKLIST.md`
