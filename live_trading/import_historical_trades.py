#!/usr/bin/env python3
"""
live_trading/import_historical_trades.py
==========================================
One-time (and safe-to-rerun) backfill script that pulls historical trades
from all existing sources into live_trading/logs/performance.db.

Sources imported:
  1. ~/Developer/options_data/data/options_data.duckdb
       → table: paper_trades_options  (all option bots)
  2. live_trading/ema_swing_scanner/logs/paper_trades.csv
       → EMA Swing Scanner equity trades
  3. live_trading/nifty_trend_seller_obi/logs/trades.csv
       → NTS+OBI Gate Bot trades (bot intentionally logs to CSV only,
         for OBI-gate A/B research — this backfill is how its trades
         reach performance.db / performance_review.py)

Deduplication: (bot_name, entry_time, symbol) — identical to the
runtime logger, so running this script multiple times is safe.

Usage:
    uv run live_trading/import_historical_trades.py
    uv run live_trading/import_historical_trades.py --dry-run   # count only, no writes
    uv run live_trading/import_historical_trades.py --rebuild-summary  # rebuild daily_summary only
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live_trading.shared.performance_db import (
    _get_connection, _upsert_daily_summary, get_db_path, rebuild_daily_summary
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _already_exists(perf_con, bot_name: str, entry_time, symbol: str) -> bool:
    if not entry_time or not symbol:
        return False
    return perf_con.execute(
        "SELECT COUNT(*) FROM trades WHERE bot_name=? AND entry_time=? AND symbol=?",
        [bot_name, entry_time, symbol],
    ).fetchone()[0] > 0


def _next_id(perf_con) -> int:
    return perf_con.execute(
        "SELECT COALESCE(MAX(id), 0) + 1 FROM trades"
    ).fetchone()[0]


# ── Source 1: paper_trades_options (DuckDB) ───────────────────────────────────

def import_options_duckdb(perf_con, dry_run: bool = False) -> int:
    """Import from ~/Developer options_data.duckdb → paper_trades_options."""
    import os
    src_path = Path(
        os.environ.get(
            "OPTIONS_DUCKDB",
            str(Path.home() / "Developer/options_data/data/options_data.duckdb"),
        )
    )
    if not src_path.exists():
        logger.warning(f"Source DB not found: {src_path} — skipping.")
        return 0

    try:
        import duckdb
        src_con = duckdb.connect(str(src_path), read_only=True)
    except Exception as e:
        logger.error(f"Cannot open source DuckDB: {e}")
        return 0

    # Check table exists
    tables = src_con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_name='paper_trades_options'"
    ).fetchall()
    if not tables:
        logger.warning("paper_trades_options table not found in source DB — skipping.")
        src_con.close()
        return 0

    rows = src_con.execute(
        """
        SELECT bot_name, instrument, trade_date, option_symbol, option_type,
               entry_time, exit_time, entry_premium, exit_premium,
               exit_reason, quantity, lots, lot_size,
               gross_pnl, decay_pct, won, order_id, notes, source
        FROM paper_trades_options
        ORDER BY entry_time
        """
    ).fetchall()
    src_con.close()

    logger.info(f"Source has {len(rows)} rows in paper_trades_options.")
    imported = 0
    skipped  = 0

    for r in rows:
        (bot_name, instrument, trade_date, option_symbol, option_type,
         entry_time, exit_time, entry_premium, exit_premium,
         exit_reason, quantity, lots, lot_size,
         gross_pnl, decay_pct, won, order_id, notes, source) = r

        if _already_exists(perf_con, bot_name, entry_time, option_symbol or ""):
            skipped += 1
            continue

        if dry_run:
            imported += 1
            continue

        # Compute hold_duration_mins
        hold_mins = None
        if entry_time and exit_time:
            try:
                hold_mins = int((exit_time - entry_time).total_seconds() / 60)
            except Exception:
                pass

        row_id = _next_id(perf_con)
        perf_con.execute(
            """
            INSERT INTO trades
              (id, bot_name, strategy_type, instrument, trade_date, symbol,
               direction, option_type,
               entry_time, exit_time, entry_price, exit_price,
               exit_reason, quantity, lots, lot_size,
               gross_pnl, decay_pct, won, hold_duration_mins,
               notes, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                row_id, bot_name, "options", instrument, trade_date,
                option_symbol, "sell", option_type,
                entry_time, exit_time, entry_premium, exit_premium,
                exit_reason, quantity, lots, lot_size,
                gross_pnl, decay_pct, won, hold_mins,
                notes, source or "paper",
            ],
        )
        _upsert_daily_summary(perf_con, bot_name, trade_date)
        imported += 1

    if not dry_run:
        perf_con.commit()

    logger.info(f"Options DuckDB: imported={imported} skipped(dups)={skipped}")
    return imported


# ── Source 2: EMA Swing Scanner CSV ───────────────────────────────────────────

EMA_CSV_COLS = [
    "symbol", "tier", "signal_date", "entry_date", "entry_price",
    "shares", "position_value", "risk_inr", "atr_at_entry",
    "rsi_at_signal", "exit_date", "exit_price", "exit_reason",
    "pnl_inr", "pnl_pct", "win", "days_held", "breakeven_activated",
]


def import_ema_csv(perf_con, dry_run: bool = False) -> int:
    csv_path = (
        Path(__file__).parent
        / "ema_swing_scanner" / "logs" / "paper_trades.csv"
    )
    if not csv_path.exists():
        logger.info(f"EMA CSV not found at {csv_path} — nothing to import.")
        return 0

    import csv
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    logger.info(f"EMA CSV has {len(rows)} rows.")
    imported = 0
    skipped  = 0
    errors   = 0

    for r in rows:
        try:
            symbol     = r.get("symbol", "").strip()
            entry_date = r.get("entry_date", "").strip()
            exit_date  = r.get("exit_date", "").strip()
            entry_price = float(r["entry_price"]) if r.get("entry_price") else None
            exit_price  = float(r["exit_price"])  if r.get("exit_price")  else None
            pnl_inr     = float(r["pnl_inr"])     if r.get("pnl_inr")     else None
            shares      = int(float(r["shares"]))  if r.get("shares")      else None
            won_raw     = str(r.get("win", "")).strip().lower()
            won         = won_raw in ("true", "1", "yes") if won_raw else (pnl_inr > 0 if pnl_inr else None)
            tier        = r.get("tier", "")
            atr         = r.get("atr_at_entry", "")
            rsi         = r.get("rsi_at_signal", "")
            exit_reason = r.get("exit_reason", "")
            days_held   = int(float(r["days_held"])) if r.get("days_held") else None

            if not entry_date or not symbol:
                errors += 1
                continue

            # Synthetic timestamps: 09:15 open, 15:30 close (equity daily bar)
            entry_time = datetime.strptime(entry_date, "%Y-%m-%d").replace(hour=9,  minute=15)
            exit_time  = datetime.strptime(exit_date,  "%Y-%m-%d").replace(hour=15, minute=30) if exit_date else None
            trade_date = entry_time.date()
            hold_mins  = days_held * 375 if days_held else None   # approx: 1 trading day ≈ 375 min

            if _already_exists(perf_con, "ema_swing_scanner", entry_time, symbol):
                skipped += 1
                continue

            if dry_run:
                imported += 1
                continue

            row_id = _next_id(perf_con)
            notes  = f"tier={tier} atr={atr} rsi={rsi}"
            perf_con.execute(
                """
                INSERT INTO trades
                  (id, bot_name, strategy_type, instrument, trade_date, symbol,
                   direction, option_type,
                   entry_time, exit_time, entry_price, exit_price,
                   exit_reason, quantity, lots, lot_size,
                   gross_pnl, decay_pct, won, hold_duration_mins,
                   notes, source)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    row_id, "ema_swing_scanner", "equity", symbol,
                    trade_date, symbol, "long", None,
                    entry_time, exit_time, entry_price, exit_price,
                    exit_reason, shares, None, None,
                    pnl_inr, None, won, hold_mins,
                    notes, "paper",
                ],
            )
            _upsert_daily_summary(perf_con, "ema_swing_scanner", trade_date)
            imported += 1

        except Exception as e:
            logger.debug(f"  Row error ({r.get('symbol','?')}): {e}")
            errors += 1

    if not dry_run:
        perf_con.commit()

    logger.info(f"EMA CSV: imported={imported} skipped(dups)={skipped} errors={errors}")
    return imported


# ── Source 3: NTS+OBI Gate Bot CSV ────────────────────────────────────────────

NTS_OBI_BOT_NAME  = "NTS_OBI"     # must match STRATEGY_NAME in nifty_trend_seller_obi_bot.py
NTS_OBI_LOT_SIZE  = 65            # NIFTY_LOT_SIZE in nifty_trend_seller_obi_bot.py


def import_obi_csv(perf_con, dry_run: bool = False) -> int:
    """Import NTS+OBI Gate Bot trades from its session_logger.py trades.csv."""
    csv_path = (
        Path(__file__).parent
        / "nifty_trend_seller_obi" / "logs" / "trades.csv"
    )
    if not csv_path.exists():
        logger.info(f"NTS+OBI CSV not found at {csv_path} — nothing to import.")
        return 0

    import csv
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    logger.info(f"NTS+OBI CSV has {len(rows)} rows.")
    imported = 0
    skipped  = 0
    errors   = 0

    for r in rows:
        try:
            date_str      = r.get("date", "").strip()
            signal_time   = r.get("signal_time", "").strip()
            exit_time_str = r.get("exit_time", "").strip()
            option_symbol = r.get("option_symbol", "").strip()
            entry_premium = float(r["entry_premium"]) if r.get("entry_premium") else None
            exit_premium  = float(r["exit_premium"])  if r.get("exit_premium")  else None
            lots          = int(float(r["lots"]))      if r.get("lots")         else None
            exit_reason   = r.get("exit_reason", "")
            gross_pnl     = float(r["pnl_gross_total"]) if r.get("pnl_gross_total") else None
            net_pnl       = float(r["pnl_net_total"])   if r.get("pnl_net_total")   else None
            obi_val       = r.get("obi_at_signal", "")

            if not date_str or not option_symbol:
                errors += 1
                continue

            entry_time = (
                datetime.strptime(f"{date_str} {signal_time}", "%Y-%m-%d %H:%M")
                if signal_time else datetime.strptime(date_str, "%Y-%m-%d")
            )
            exit_time = (
                datetime.strptime(f"{date_str} {exit_time_str}", "%Y-%m-%d %H:%M")
                if exit_time_str else None
            )
            trade_date = entry_time.date()
            hold_mins  = (
                int((exit_time - entry_time).total_seconds() / 60) if exit_time else None
            )

            sym_u       = option_symbol.upper()
            option_type = "CE" if sym_u.endswith("CE") else ("PE" if sym_u.endswith("PE") else None)
            quantity    = lots * NTS_OBI_LOT_SIZE if lots else None
            won = (
                (net_pnl > 0) if net_pnl is not None
                else ((gross_pnl > 0) if gross_pnl is not None else None)
            )

            if _already_exists(perf_con, NTS_OBI_BOT_NAME, entry_time, option_symbol):
                skipped += 1
                continue

            if dry_run:
                imported += 1
                continue

            row_id = _next_id(perf_con)
            perf_con.execute(
                """
                INSERT INTO trades
                  (id, bot_name, strategy_type, instrument, trade_date, symbol,
                   direction, option_type,
                   entry_time, exit_time, entry_price, exit_price,
                   exit_reason, quantity, lots, lot_size,
                   gross_pnl, net_pnl, decay_pct, won, hold_duration_mins,
                   notes, source)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    row_id, NTS_OBI_BOT_NAME, "options", "NIFTY",
                    trade_date, option_symbol, "sell", option_type,
                    entry_time, exit_time, entry_premium, exit_premium,
                    exit_reason, quantity, lots, NTS_OBI_LOT_SIZE,
                    gross_pnl, net_pnl, None, won, hold_mins,
                    f"obi_at_signal={obi_val}", "paper",
                ],
            )
            _upsert_daily_summary(perf_con, NTS_OBI_BOT_NAME, trade_date)
            imported += 1

        except Exception as e:
            logger.debug(f"  Row error ({r.get('option_symbol','?')}): {e}")
            errors += 1

    if not dry_run:
        perf_con.commit()

    logger.info(f"NTS+OBI CSV: imported={imported} skipped(dups)={skipped} errors={errors}")
    return imported


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill performance.duckdb from all trade sources")
    parser.add_argument("--dry-run", action="store_true", help="Count rows to import without writing")
    parser.add_argument("--rebuild-summary", action="store_true",
                        help="Only rebuild daily_summary from existing trades table — no import")
    args = parser.parse_args()

    db_path = get_db_path()
    logger.info(f"Performance DB: {db_path}")

    if args.rebuild_summary:
        logger.info("Rebuilding daily_summary…")
        rebuild_daily_summary()
        logger.info("Done.")
        return

    if args.dry_run:
        logger.info("DRY RUN — no writes will be made.\n")

    perf_con = _get_connection()

    # Current counts
    existing = perf_con.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    logger.info(f"Current trades in performance.duckdb: {existing}")

    total_imported = 0
    total_imported += import_options_duckdb(perf_con, dry_run=args.dry_run)
    total_imported += import_ema_csv(perf_con,        dry_run=args.dry_run)
    total_imported += import_obi_csv(perf_con,        dry_run=args.dry_run)

    if not args.dry_run:
        # Final rebuild of all daily summaries to ensure consistency
        logger.info("Rebuilding all daily summaries…")
        pairs = perf_con.execute(
            "SELECT DISTINCT bot_name, trade_date FROM trades WHERE trade_date IS NOT NULL"
        ).fetchall()
        for bn, td in pairs:
            _upsert_daily_summary(perf_con, bn, td)
        perf_con.commit()

    perf_con.close()

    after = 0
    if not args.dry_run:
        check_con = _get_connection()
        after = check_con.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        check_con.close()

    if args.dry_run:
        logger.info(f"\n  Would import ~{total_imported} new trades.")
    else:
        logger.info(f"\n  Import complete. Trades before: {existing}  After: {after}  (+{after - existing})")
        logger.info(f"  Run performance_review.py to see the report.")


if __name__ == "__main__":
    main()
