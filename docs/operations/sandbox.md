# Sandbox / Analyzer Mode

## What It Is

Sandbox mode is the paper-trading engine built into OpenAlgo. It intercepts orders and simulates fills against ₹1 Crore virtual capital.

- Isolated database (`db/sandbox.db`) — completely separate from live trading
- Realistic margin system with leverage
- Auto square-off at exchange timings
- Toggle via `/analyzer` blueprint in the UI (`blueprints/analyzer.py`)
- Sandbox controls (capital, leverage, reset schedule) at `/sandbox` (`blueprints/sandbox.py`)

## How Live Bots Use It

Bots fire **real OpenAlgo REST API orders** — identical code path to live trading. When OpenAlgo is in Analyzer/Sandbox mode, it intercepts those orders and simulates fills. **There is no paper-trading flag in bot code.** Going live = flip the OpenAlgo mode toggle in the UI. Zero bot code changes needed.

## Stage 11 Paper Trading

During Stage 11 validation, the Fyers broker connection is kept in Analyze mode. All orders are real API calls; sandbox intercepts them. No real money is at risk.

Gate criteria before requesting sign-off:
- ≥ 20 trading sessions completed in sandbox mode
- Win rate within ±10% of OOS backtest expectation
- No single session loss > 2× expected ATR-based stop
- Net P&L positive over the full 20-session window
- Paper results reviewed against OOS backtest in a summary report
