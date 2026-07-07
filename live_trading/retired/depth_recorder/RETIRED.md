# Depth Recorder — RETIRED 2026-07-02

## What it was
LaunchAgent-managed infra bot (`com.openalgo.depth-recorder`, RunAtLoad +
KeepAlive) that recorded order-book depth for the top-20 liquid NIFTY50
stocks into monthly DuckDB files (`db/depth_data/YYYY_MM.duckdb`), 09:15–15:35
IST daily. Only 3–4 symbols ever received true 50-level TBT depth (Fyers
hard-limits the TBT WebSocket; adapter reserves 4 slots) — the other 16 fell
back to 5-level HSM depth and logged daily ERROR/WARNING noise.

## Why retired
1. **Sole consumer already retired.** The only strategy that used stock depth
   was the Equity OBI Bot, retired 2026-06-19 (149 trades, WR 38.3%,
   net −₹4,122).
2. **The wall hypothesis tested negative.** Pilot study (2026-07-02, 10
   sessions Jun 18→Jul 2, HDFCBANK/RELIANCE/ICICIBANK/INFY, ~1.5M ticks):
   - Strong walls (level qty ≥5× median) were touched 1,457 times:
     **74% pierced, 26% rejected** — visible walls do not act as
     support/resistance in these stocks.
   - Wall-filtered variants of the Equity OBI rules improved avg net P&L by
     only ~₹30–40/trade over baseline (≈3 trades of variance at n≈130–141) —
     statistically insignificant.
3. **Freed resources.** All 4 TBT depth-50 slots now available (the NTS+OBI
   Gate bot's ATM option can never be bumped to 5-level); ~1 GB/month disk
   growth stopped; 16 daily TBT-limit ERROR/WARNING pairs eliminated.

## What is kept
- Recorded data retained: `db/depth_data/2026_06.duckdb` (3.1 GB, Jun 18–30)
  and `2026_07.duckdb` (0.9 GB, Jul 1–2). Tables: `depth_levels` (per-level
  ticks) and `derived_metrics` (per-tick raw_obi/w_obi/vwmp — the Equity OBI
  bot's exact inputs).
- Pilot study script + trade CSVs were produced in the 2026-07-02 session
  scratchpad (`equity_obi_wall_pilot.py`, `pilot_trades_V{1,2,3}.csv`).

## How to resurrect
Copy `com.openalgo.depth-recorder.plist` back to `~/Library/LaunchAgents/`,
move `depth_recorder.py` back to `live_trading/`, then
`launchctl load ~/Library/LaunchAgents/com.openalgo.depth-recorder.plist`.
