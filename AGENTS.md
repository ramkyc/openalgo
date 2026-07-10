## graphify Knowledge Graph

This project has a graphify knowledge graph at `graphify-out/`.

**Rules:**
- Before answering architecture or codebase questions, read `graphify-out/GRAPH_REPORT.md` for god nodes and community structure
- For specific questions, run: `/graphify query "<question>"` to traverse the graph
- After modifying code files in this session, run:
  `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"`

## Related Projects

| Project | Path | Purpose |
|---|---|---|
| **options_data** | `~/Developer/options_data/` | Research pipeline, 11-stage backtesting, DuckDB data store |
| **fyers_cs** | `~/Developer/fyers_cs/openalgo/` | Secondary account OpenAlgo instance (port 5000) |
| **fyers_crk** | `~/Developer/fyers_crk/openalgo/` | Primary account OpenAlgo instance (port 5001) — THIS repo |

**When working on a bot or strategy:** always read `~/Developer/options_data/CLAUDE.md` first.
