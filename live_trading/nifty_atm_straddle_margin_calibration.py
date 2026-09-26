"""
nifty_atm_straddle_margin_calibration.py — one-off margin calibration utility.

Not a bot. Queries Fyers' real margin calculator (via this project's own
OpenAlgo REST API, same pattern every live bot already uses) for a 10-lot
NIFTY ATM short straddle (sell ATM CE + sell ATM PE, product=MIS) across
every currently available weekly expiry, and compares the result against
the flat 9%-of-notional margin approximation used by the research study
at ~/Developer/options_data/research/atm_short_straddle_scalp_study/
(MARGIN_PCT_OF_NOTIONAL = 0.09, see that study's DECISIONS.md D1/D12).

Limitation: Fyers' margin API returns CURRENT margin only — there is no
historical margin endpoint, so this calibrates "what margin looks like
today for this structure," not a per-trade historical recompute across
the study's ~573 backtested trades. Still run across multiple expiries
(different DTE) since that is the one axis of variation the live market
actually offers today, and it happens to be the same axis the study's
own Stage 10 (expiry segmentation) found mildly sensitive.

Run (with the OpenAlgo app already running on HOST_SERVER, default
http://127.0.0.1:8080, and this project's own broker session already
logged in — see this project's CLAUDE.md "Broker Token Boundaries"):

    uv run python live_trading/nifty_atm_straddle_margin_calibration.py

Output:
    live_trading/margin_calibration_results.csv
"""

from __future__ import annotations

import csv
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

from live_trading.api_utils import HOST, get_expiry_dates, get_option_symbol  # noqa: E402

API_KEY = os.getenv("OPENALGO_API_KEY")
IDX_SYMBOL = "NIFTY"
IDX_EXCHANGE = "NSE_INDEX"
OPT_EXCHANGE = "NFO"
LOTS_PER_TRADE = 10          # matches options_data study's LOTS_PER_TRADE
DEFAULT_LOT_SIZE = 65        # post-Jan-2026 NIFTY lot size, fallback only
MARGIN_PCT_ASSUMED = 0.09    # options_data study's MARGIN_PCT_OF_NOTIONAL (D1)
N_EXPIRIES = 5               # how many upcoming weekly expiries to sample

OUT_CSV = Path(__file__).parent / "margin_calibration_results.csv"


def get_spot() -> float:
    payload = {"apikey": API_KEY, "symbol": IDX_SYMBOL, "exchange": IDX_EXCHANGE}
    r = requests.post(f"{HOST}/api/v1/quotes", json=payload, timeout=10)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(f"Quote fetch failed: {data}")
    return float(data["data"]["ltp"])


def get_lot_size(symbol: str) -> int:
    try:
        from database.token_db import get_symbol_info
        si = get_symbol_info(symbol, OPT_EXCHANGE)
        if si and getattr(si, "lotsize", None):
            return int(si.lotsize)
    except Exception as e:
        print(f"  Lot size lookup failed ({symbol}): {e}. Using {DEFAULT_LOT_SIZE}.")
    return DEFAULT_LOT_SIZE


def calc_margin(ce_symbol: str, pe_symbol: str, qty: int) -> dict:
    payload = {
        "apikey": API_KEY,
        "positions": [
            {
                "symbol": ce_symbol, "exchange": OPT_EXCHANGE, "action": "SELL",
                "quantity": str(qty), "product": "MIS", "pricetype": "MARKET",
                "price": "0", "trigger_price": "0",
            },
            {
                "symbol": pe_symbol, "exchange": OPT_EXCHANGE, "action": "SELL",
                "quantity": str(qty), "product": "MIS", "pricetype": "MARKET",
                "price": "0", "trigger_price": "0",
            },
        ],
    }
    r = requests.post(f"{HOST}/api/v1/margin", json=payload, timeout=20)
    r.raise_for_status()
    return r.json()


def main():
    if not API_KEY:
        print("ERROR: OPENALGO_API_KEY not set in .env"); sys.exit(1)

    print("=" * 78)
    print("NIFTY ATM Short Straddle — Margin Calibration (live Fyers query)")
    print(f"Comparison baseline: options_data study's flat "
          f"{MARGIN_PCT_ASSUMED*100:.0f}%-of-notional assumption (D1)")
    print("=" * 78)

    spot = get_spot()
    print(f"\nNIFTY spot: {spot:.2f}")

    expiries = get_expiry_dates(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, "options")
    if not expiries:
        print("ERROR: no expiry dates returned"); sys.exit(1)

    parsed = sorted(
        (datetime.strptime(e, "%d%b%y").date(), e) for e in expiries
    )[:N_EXPIRIES]
    print(f"Sampling {len(parsed)} nearest weekly expiries: {[e for _, e in parsed]}\n")

    today = datetime.now().date()
    rows = []

    for expiry_date, expiry_str in parsed:
        dte = (expiry_date - today).days
        ce_symbol = get_option_symbol(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, expiry_str, "CE", offset="ATM")
        pe_symbol = get_option_symbol(API_KEY, IDX_SYMBOL, OPT_EXCHANGE, expiry_str, "PE", offset="ATM")
        if not ce_symbol or not pe_symbol:
            print(f"  [{expiry_str}] SKIP — could not resolve ATM symbols "
                  f"(ce={ce_symbol}, pe={pe_symbol})")
            continue

        lot_size = get_lot_size(ce_symbol)
        qty = lot_size * LOTS_PER_TRADE
        notional = spot * lot_size * LOTS_PER_TRADE

        resp = calc_margin(ce_symbol, pe_symbol, qty)
        if resp.get("status") != "success":
            print(f"  [{expiry_str}] SKIP — margin API error: {resp.get('message')}")
            continue

        real_margin = float(resp["data"]["total_margin_required"])
        real_pct = real_margin / notional
        assumed_margin = MARGIN_PCT_ASSUMED * notional

        print(f"  [{expiry_str}] DTE={dte:>2}  CE={ce_symbol}  PE={pe_symbol}  "
              f"lot_size={lot_size}")
        print(f"      Notional (spot x lot_size x {LOTS_PER_TRADE} lots): Rs.{notional:>14,.0f}")
        print(f"      Real Fyers margin (MIS, 10 lots each leg):          Rs.{real_margin:>14,.0f}  "
              f"({real_pct*100:.2f}% of notional)")
        print(f"      Study's assumed margin (9% flat):                  Rs.{assumed_margin:>14,.0f}")
        print(f"      Delta (real - assumed):                            Rs.{real_margin-assumed_margin:>14,.0f}  "
              f"({(real_pct/MARGIN_PCT_ASSUMED - 1)*100:+.1f}% vs. assumption)\n")

        rows.append(dict(
            expiry=expiry_str, dte=dte, spot=round(spot, 2),
            ce_symbol=ce_symbol, pe_symbol=pe_symbol, lot_size=lot_size,
            notional=round(notional, 0), real_margin=round(real_margin, 0),
            real_pct_of_notional=round(real_pct, 4),
            assumed_margin_9pct=round(assumed_margin, 0),
        ))

    if not rows:
        print("No usable rows — nothing to summarize."); sys.exit(1)

    avg_pct = sum(r["real_pct_of_notional"] for r in rows) / len(rows)
    print("=" * 78)
    print(f"SUMMARY  (n={len(rows)} expiries)")
    print(f"  Real margin / notional — avg: {avg_pct*100:.2f}%   "
          f"min: {min(r['real_pct_of_notional'] for r in rows)*100:.2f}%   "
          f"max: {max(r['real_pct_of_notional'] for r in rows)*100:.2f}%")
    print(f"  Study's flat assumption: {MARGIN_PCT_ASSUMED*100:.2f}%")
    print("=" * 78)

    with open(OUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nSaved -> {OUT_CSV}")


if __name__ == "__main__":
    main()
