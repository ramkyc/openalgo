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

## Bot journal & stats data — MANDATORY for every new bot (any AI or human)
Applies to Claude Code, Antigravity/Gemini, Codex, Cursor, or anyone else writing a bot. A bot is NOT done until it records, per closed trade, a queryable row with: bot_name (literal registry key), date, symbol/strike/expiry/side/lots, entry+exit timestamps and fill prices, the intended signal price (slippage), an `exit_reason` enum, gross + costs + **net** P&L, and the **signal context at entry** (indicator/filter values, DTE, VIX, regime). Skipped signals go to a decisions/events record. The dashboard must have a "📒 Journal & stats" tab fed by this data (reference: `render_time_value_sell_panel`), and cs_fyers bots must be in `_CSF_FLEET`. Full spec: `~/Developer/fyers_cs/openalgo/docs/trading/deployment-checklist.md` → "Journal & Stats Data Contract". Read it before writing any bot; reject/redo work that lacks it.
