#!/usr/bin/env python3
"""
Liquidity check for a microcap target list at the current (or a hypothetical) book value.
READ-ONLY: fetches ~30d daily bars per symbol via OpenAlgo, writes nothing.

For each name in the latest finalized target list: 20-day average daily rupee
turnover (close*volume), the per-name target at book/n_top, that target as a % of
turnover, and a capped order size (default cap 2% of ADV).

Usage (from openalgo/): uv run python -m live_trading.nifty_microcap_screener.liquidity_check [--book 2590000] [--cap 0.02]
"""
from __future__ import annotations
import argparse, sys, time
import pandas as pd
from live_trading.nifty_microcap_screener.nifty_microcap_screener import (
    API_KEY, EXCHANGE, _history_to_frame, get_history, rebalance_db)

ap = argparse.ArgumentParser()
ap.add_argument("--book", type=float, default=None, help="book value; default = ledger NAV")
ap.add_argument("--cap", type=float, default=0.02, help="max order as fraction of 20d ADV turnover")
a = ap.parse_args()

tl = rebalance_db.get_latest_target_list()
df = pd.DataFrame(tl["candidates"])
book = a.book or rebalance_db.get_book_value()
n_top = len(df)
target = book / n_top
print(f"list {tl.get('rebalance_month')} signal {tl.get('signal_date')} | n={n_top} book=Rs.{book:,.0f} target/name=Rs.{target:,.0f} cap={a.cap:.1%} ADV")

out = []
for sym, px in zip(df["symbol"], df["ref_price"]):
    try:
        h = _history_to_frame(get_history(API_KEY, sym, EXCHANGE, "D", duration_days=40)).tail(20)
        adv = float((h["close"] * h["volume"]).mean()) if len(h) else 0.0
    except Exception as e:
        adv = 0.0
    time.sleep(1.0)
    cap_rs = a.cap * adv
    buy_rs = min(target, cap_rs)
    out.append(dict(symbol=sym, px=px, adv_lakh=adv / 1e5, target_lakh=target / 1e5,
                    pct_adv=100 * target / adv if adv else float("inf"),
                    order_rs=buy_rs, shares=int(buy_rs // px), short_rs=target - buy_rs))
r = pd.DataFrame(out).sort_values("pct_adv", ascending=False)
pd.set_option("display.width", 200)
print(r.round(2).to_string(index=False))
print(f"\nflagged (> cap): {int((r.short_rs > 0).sum())} names | total order Rs.{r.order_rs.sum():,.0f} of Rs.{book:,.0f} | shortfall Rs.{r.short_rs.sum():,.0f}")
