# Broker Integration Pattern

All 30+ brokers follow a standardized structure in `broker/{broker_name}/`:

```
broker/{name}/
  api/
    auth_api.py         # OAuth2 or API key based authentication
    order_api.py        # Place, modify, cancel orders
    data.py             # Quotes, depth, historical data
    funds.py            # Account balance and margins
  mapping/              # Transform OpenAlgo format ↔ broker format
  streaming/            # WebSocket adapter for real-time data
  database/
    master_contract_db.py   # Symbol mapping
  plugin.json           # Broker metadata
```

Reference implementations: `/broker/fyers/`, `/broker/zerodha/`, `/broker/dhan/`

## Adding a New Broker

1. Create `broker/new_broker/` with the structure above
2. Add broker name to `VALID_BROKERS` in `.env`
3. Restart application to reload plugins

Brokers are dynamically loaded via `utils/plugin_loader.py` from `broker/*/plugin.json`.

## Symbol Format

OpenAlgo uses a standardized symbol format across all 30+ brokers:

| Type | Format | Example |
|------|--------|---------|
| Equity | Base symbol | `INFY`, `SBIN`, `TATAMOTORS` |
| Futures | `[Base][Expiry]FUT` | `BANKNIFTY24APR24FUT` |
| Options | `[Base][Expiry][Strike][CE/PE]` | `NIFTY28MAR2420800CE` |

Exchange codes: `NSE`, `BSE`, `NFO`, `BFO`, `CDS`, `BCD`, `MCX`, `NCDEX`, `NSE_INDEX`, `BSE_INDEX`, `GLOBAL_INDEX`

Order constants:
- **Product:** `CNC` (delivery), `NRML` (F&O carry), `MIS` (intraday)
- **Price type:** `MARKET`, `LIMIT`, `SL`, `SL-M`
- **Action:** `BUY`, `SELL`

Broker-specific symbols ↔ OpenAlgo format conversion happens in `broker/*/mapping/`.

Database schema (`SymToken`): `symbol`, `brsymbol`, `exchange`, `brexchange`, `token`, `expiry`, `strike`, `lotsize`, `instrumenttype`, `tick_size`
