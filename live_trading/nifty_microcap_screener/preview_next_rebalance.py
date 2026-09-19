#!/usr/bin/env python3
"""
NIFTY Microcap Screener — next-rebalance PREVIEW (dry run, read-only)
live_trading/nifty_microcap_screener/preview_next_rebalance.py

Rehearses what the September finalization will look like, using the most
recently available daily close as a stand-in "as-of" date -- run this any
evening after market close to get a same-day-fresh look at which of the
currently-held 34 names would drop out of the top-15% and which new names
would enter, WITHOUT waiting for the real month-end event.

The real finalization only fires once reb_dates[-1] (today's month) proves
reb_dates[-2] (the prior calendar month) is fully closed -- see
run_scan_cycle()'s finalize block. That means the real September list is
only computed automatically once a scan runs on/after the first trading day
of September, using 2026-08-31's close as the true signal date. Until then,
this script fakes that moment early: it treats the LATEST available close
(today's, if run after ~16:00 IST) as if it were the finalized signal date,
reruns the exact same ranking logic, and diffs the result against the real
positions already open in rebalance_db -- so you can see the mechanics of a
rotation before the real one happens.

WRITES NOTHING. No rebalance_db insert, no state.json mutation, no dashboard
impact. Console output only. Safe to run any number of times, any day.

Caveat: the "as-of" date here is whatever today's fetch returns, not
necessarily August's true month-end (2026-08-31) -- the momentum window
shifts by a few trading days versus the real September signal, so the
exact top-15% cut can differ slightly from what actually finalizes on
2026-08-31. Useful for rehearsing the UI/DB flow, not a guarantee of the
real list.

Usage: uv run python live_trading/nifty_microcap_screener/preview_next_rebalance.py
"""
from __future__ import annotations

from nifty_microcap_screener import (
    SCREEN_CAPITAL,
    build_target_portfolio,
    compute_momentum_panel,
    fetch_universe_panel,
    rebalance_db,
)


def main() -> None:
    print("=" * 78)
    print("NIFTY Microcap Screener — NEXT REBALANCE PREVIEW (dry run, writes nothing)")
    print("=" * 78)

    print("\nFetching fresh daily history for the full universe (same fetch as the "
          "real scan) ...")
    df = fetch_universe_panel()
    if df.empty:
        print("No history fetched -- aborting preview.")
        return

    as_of = df["date"].max()
    print(f"Latest available close across the universe: {as_of.date()}")
    print("(If this isn't today's date, the market may not have closed yet, or "
          "today's close hasn't posted to Fyers' history API -- re-run after "
          "~16:00 IST for a same-day read.)")

    mom = compute_momentum_panel(df)
    if as_of not in mom.index:
        print(f"\n{as_of.date()} not in the momentum panel (not enough trailing "
              "history yet) -- aborting preview.")
        return

    rebalance_db.ensure_capital_seeded(SCREEN_CAPITAL)
    book_value = rebalance_db.get_book_value()
    print(f"Current NAV-style book value (seed + realized P&L - withdrawals): "
          f"Rs.{book_value:,.0f}")

    wide_close = df.pivot(index="date", columns="symbol", values="close").sort_index()
    try:
        target, n_eligible, n_top = build_target_portfolio(
            mom, as_of, wide_close.loc[as_of], book_value)
    except ValueError as e:
        print(f"\nCould not build a target portfolio as of {as_of.date()}: {e}")
        return

    preview_symbols = {r["symbol"] for r in target.to_dict("records")}
    print(f"\n{n_eligible} eligible symbols, top-15% cut = {n_top} names, "
          f"as of {as_of.date()}'s close")

    held = rebalance_db.list_open_positions()
    held_by_symbol = {p["symbol"]: p for p in held}
    held_symbols = set(held_by_symbol)

    would_exit = sorted(held_symbols - preview_symbols)
    would_stay = sorted(held_symbols & preview_symbols)
    would_enter = sorted(preview_symbols - held_symbols)

    print(f"\nCurrently held (real, confirmed): {len(held_symbols)}")
    print(f"  Would STAY in top-15%:  {len(would_stay)}")
    print(f"  Would DROP OUT (exit candidates): {len(would_exit)}")
    print(f"  Would be NEW entries (not yet held): {len(would_enter)}")

    if would_exit:
        print(f"\n{'─'*60}\nWOULD DROP OUT -- \"Dropped From Top 15%\" section preview\n{'─'*60}")
        for sym in would_exit:
            pos = held_by_symbol[sym]
            entry_px = float(pos.get("entry_price") or 0)
            qty = int(pos.get("qty") or 0)
            ltp = float(wide_close.loc[as_of].get(sym, entry_px))
            mtm = (ltp - entry_px) * qty if entry_px > 0 and qty > 0 else 0.0
            print(f"  {sym:<14} entry ₹{entry_px:>9.2f}  as-of close ₹{ltp:>9.2f}  "
                  f"qty {qty:>4}  MTM ₹{mtm:>10,.0f}  (since {pos.get('since', '')})")
    else:
        print("\nNo held names would drop out as of this date -- all 34 still rank "
              "in the top-15%.")

    if would_enter:
        print(f"\n{'─'*60}\nWOULD ENTER -- new top-15% names not yet held\n{'─'*60}")
        rows_by_symbol = {r["symbol"]: r for r in target.to_dict("records")}
        for sym in would_enter:
            r = rows_by_symbol[sym]
            print(f"  {sym:<14} mom {r['momentum_12m1m_pct']:>7.1f}%  "
                  f"ref ₹{r['ref_price']:>9.2f}  shares {r['shares']:>5}")

    print(f"\n{'='*78}")
    print("Nothing was written anywhere -- rebalance_db, state.json, and the "
          "dashboard are all untouched by this run.")
    print("The REAL September finalization happens automatically, once, the first "
          "time the daily scan cycle runs on/after the first trading day of "
          "September, using 2026-08-31's actual close as the signal date.")
    print("=" * 78)


if __name__ == "__main__":
    main()
