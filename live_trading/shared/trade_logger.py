"""
live_trading/shared/trade_logger.py
====================================
Centralised SQLite trade-logging utility shared by ALL live bots.

Writes to live_trading/logs/paper_trades.db  (SQLite, WAL mode).
WAL mode allows unlimited concurrent readers and serialises writers without
producing lock errors — safe for all bots running simultaneously.

Usage (in any bot's close/exit method):
    from live_trading.shared.trade_logger import log_trade_to_db

    log_trade_to_db(
        bot_name      = "ha_options_bot",
        instrument    = "NIFTY",
        option_symbol = "NIFTY13APR2622900PE",
        option_type   = "PE",                  # None for equity bots
        entry_time    = datetime(2026, 4, 7, 9, 30, 0),
        exit_time     = datetime(2026, 4, 7, 9, 45, 0),
        entry_premium = 362.65,
        exit_premium  = 406.90,
        exit_reason   = "HA Reversal",
        quantity      = 65,
        lots          = 1,
        lot_size      = 65,
        gross_pnl     = -2876.0,
        order_id      = "26040780654199",       # optional
        notes         = None,                   # optional
    )

The function:
  • Writes to live_trading/logs/paper_trades.db (SQLite, WAL, busy_timeout=10s)
  • Creates the paper_trades_options table if it does not already exist
  • Silently deduplicates by (bot_name, entry_time, option_symbol) so restarts
    cannot double-insert the same trade
  • Swallows all exceptions (logs a warning) so a DB failure NEVER crashes
    the bot
  • Dual-writes to performance_db for the live performance dashboard
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Embedded transaction cost calculator ──────────────────────────────────────
# Rates verified 2026-03-28 against fyers.in/charges-list (Fyers flat-fee plan)
_STT_RATE      = 0.0005           # 0.05% on SELL-side turnover only
_EXC_RATE      = {"NSE": 0.00053, "BSE": 0.00050}
_SEBI_RATE     = 0.0000001        # ₹10 per crore
_STAMP_RATE    = 0.00003          # 0.003% on BUY-side turnover only
_GST_RATE      = 0.18             # on brokerage + exchange + SEBI
_BROKERAGE     = 20.0             # ₹20 flat per order
_EXCHANGE_MAP  = {"NIFTY": "NSE", "BANKNIFTY": "NSE", "SENSEX": "BSE", "BANKEX": "BSE"}


def _single_order_cost(premium: float, lot_size: int, lots: int,
                        side: str, exchange: str) -> float:
    turnover = premium * lot_size * lots
    exc  = _EXC_RATE.get(exchange, _EXC_RATE["NSE"]) * turnover
    sebi = _SEBI_RATE * turnover
    stt  = _STT_RATE * turnover if side == "sell" else 0.0
    stamp = _STAMP_RATE * turnover if side == "buy" else 0.0
    gst  = _GST_RATE * (_BROKERAGE + exc + sebi)
    return stt + exc + sebi + stamp + _BROKERAGE + gst


def _cost_round_trip(entry_premium: float, exit_premium: float,
                     lot_size: int, lots: int,
                     instrument: str, direction: str) -> float:
    """Total Fyers F&O round-trip cost for one completed options trade."""
    exc = _EXCHANGE_MAP.get((instrument or "").upper(), "NSE")
    if direction == "sell":
        return (
            _single_order_cost(entry_premium, lot_size, lots, "sell", exc) +
            _single_order_cost(exit_premium,  lot_size, lots, "buy",  exc)
        )
    return (
        _single_order_cost(entry_premium, lot_size, lots, "buy",  exc) +
        _single_order_cost(exit_premium,  lot_size, lots, "sell", exc)
    )

# ── Positionbook P&L lookup ────────────────────────────────────────────────────
try:
    from live_trading.shared.positionbook_pnl import fetch_closed_pnl as _fetch_pb_pnl
    _PB_AVAILABLE = True
except Exception:
    _PB_AVAILABLE = False

# ── Local performance DB (dual-write) ─────────────────────────────────────────
try:
    from live_trading.shared.performance_db import log_trade as _log_perf_trade
    _PERF_DB_AVAILABLE = True
except Exception:
    _PERF_DB_AVAILABLE = False

# ── DB path ────────────────────────────────────────────────────────────────────
_THIS_DIR = Path(__file__).parent          # live_trading/shared/
_LOGS_DIR = _THIS_DIR.parent / "logs"     # live_trading/logs/
_LOGS_DIR.mkdir(parents=True, exist_ok=True)

_DB_PATH: Path = Path(
    os.environ.get(
        "PAPER_TRADES_DB",
        str(_LOGS_DIR / "paper_trades.db"),
    )
)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS paper_trades_options (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_name      TEXT    NOT NULL,
    instrument    TEXT,
    trade_date    TEXT,
    option_symbol TEXT,
    option_type   TEXT,
    entry_time    TEXT,
    exit_time     TEXT,
    entry_premium REAL,
    exit_premium  REAL,
    exit_reason   TEXT,
    quantity      INTEGER,
    lots          INTEGER,
    lot_size      INTEGER,
    gross_pnl     REAL,
    net_pnl       REAL,
    decay_pct     REAL,
    won           INTEGER,
    order_id      TEXT,
    notes         TEXT,
    source        TEXT,
    created_at    TEXT DEFAULT (datetime('now'))
)
"""


_MIGRATIONS = [
    "ALTER TABLE paper_trades_options ADD COLUMN net_pnl REAL",
]


def _get_connection() -> sqlite3.Connection:
    """Open (or create) the paper_trades SQLite DB with WAL mode."""
    con = sqlite3.connect(str(_DB_PATH), timeout=30, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    con.execute(_CREATE_TABLE_SQL)
    existing = {r[1] for r in con.execute("PRAGMA table_info(paper_trades_options)").fetchall()}
    for sql in _MIGRATIONS:
        col = sql.split("ADD COLUMN")[1].strip().split()[0]
        if col not in existing:
            con.execute(sql)
    con.commit()
    return con


def log_trade_to_db(
    bot_name:      str,
    instrument:    str,
    option_symbol: str,
    option_type:   str | None,
    entry_time:    datetime | str | None,
    exit_time:     datetime | str | None,
    entry_premium: float | None,
    exit_premium:  float | None,
    exit_reason:   str,
    quantity:      int | None,
    lots:          int | None,
    lot_size:      int | None,
    gross_pnl:     float | None,
    net_pnl:       float | None = None,
    order_id:      str | None = None,
    notes:         str | None = None,
    strategy_type: str        = "options",   # "options" | "equity"
    direction:     str        = "sell",      # "sell" | "long" | "short"
) -> None:
    """
    Append one completed trade to paper_trades_options in SQLite.
    Silent on failure — never raises an exception.

    strategy_type / direction — passed through to performance_db for correct
    classification in performance_review.py.  Equity long bots MUST pass
    strategy_type="equity" and direction="long" (or "short").  The defaults
    ("options" / "sell") exist only for backward-compatibility.
    """
    try:
        # Normalise datetime strings
        if isinstance(entry_time, str):
            entry_time = datetime.fromisoformat(entry_time)
        if isinstance(exit_time, str):
            exit_time = datetime.fromisoformat(exit_time)

        # Use exit date, not entry date — a trade booked/realised today must
        # show up in "today's trades" even if it was entered on a prior day
        # (multi-day holds, e.g. EMA spread bots held over a weekend).
        trade_date = exit_time.date() if exit_time else (
            entry_time.date() if entry_time else None
        )

        # Resolve net_pnl when not explicitly provided by the caller.
        # Priority: (1) Fyers positionbook P&L  (2) embedded cost formula
        # Multi-leg strategies are skipped — positionbook_pnl.fetch_closed_pnl()
        # only returns P&L for the ONE symbol passed in (option_symbol), which
        # for a spread/fly is just one leg. Using that as "net_pnl" silently
        # replaces the bot's own correctly-computed combined-leg gross_pnl with
        # a wildly wrong single-leg number (confirmed live 2026-07-06 on the
        # EMA_SPREAD bots — e.g. a real -8,547 spread P&L was overwritten with
        # -46,897, the long leg's standalone P&L). The cost formula has the
        # same problem (single entry/exit premium pair, not per-leg), so it is
        # skipped too. Dashboards already fall back to gross_pnl when net_pnl
        # is None (see streamlit_dashboard.py `t.get("net_pnl") or t["gross_pnl"]`),
        # so leaving net_pnl unset here is the correct (if cost-exclusive)
        # display for any multi-leg type until per-leg cost attribution exists.
        _MULTI_LEG_TYPES = {"IRON_FLY", "DEBIT_SPREAD", "CREDIT_SPREAD", "DOUBLE_FLY", "SHORT_STRADDLE"}
        if net_pnl is None and gross_pnl is not None and (option_type or "").upper() not in _MULTI_LEG_TYPES:
            # Try positionbook first (actual Fyers fills + charges)
            if _PB_AVAILABLE and option_symbol:
                try:
                    pb_pnl = _fetch_pb_pnl(option_symbol)
                    if pb_pnl is not None:
                        net_pnl = round(pb_pnl, 2)
                        logger.debug(f"[trade_logger] net_pnl from positionbook: ₹{net_pnl:,.2f}")
                except Exception:
                    pass
            # Fall back to embedded Fyers charge formula
            if net_pnl is None and entry_premium and exit_premium is not None and lots and lot_size:
                try:
                    cost = _cost_round_trip(
                        entry_premium, exit_premium, int(lot_size), int(lots),
                        instrument or "", direction,
                    )
                    net_pnl = round(gross_pnl - cost, 2)
                    logger.debug(f"[trade_logger] net_pnl from formula: ₹{net_pnl:,.2f}")
                except Exception:
                    pass

        won = int((net_pnl if net_pnl is not None else gross_pnl) > 0) if (net_pnl or gross_pnl) is not None else None

        decay_pct: float | None = None
        if entry_premium and entry_premium > 0 and exit_premium is not None:
            decay_pct = round(
                (entry_premium - exit_premium) / entry_premium * 100, 2
            )

        entry_str  = entry_time.isoformat() if entry_time  else None
        exit_str   = exit_time.isoformat()  if exit_time   else None
        date_str   = str(trade_date)        if trade_date  else None

        con = _get_connection()

        # Deduplicate by (bot_name, entry_time, option_symbol)
        if entry_str and option_symbol:
            dup = con.execute(
                "SELECT COUNT(*) FROM paper_trades_options "
                "WHERE bot_name=? AND entry_time=? AND option_symbol=?",
                [bot_name, entry_str, option_symbol],
            ).fetchone()[0]
            if dup > 0:
                con.close()
                return

        con.execute(
            """
            INSERT INTO paper_trades_options
              (bot_name, instrument, trade_date, option_symbol, option_type,
               entry_time, exit_time, entry_premium, exit_premium,
               exit_reason, quantity, lots, lot_size,
               gross_pnl, net_pnl, decay_pct, won, order_id, notes, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                bot_name, instrument, date_str,
                option_symbol, option_type,
                entry_str, exit_str,
                entry_premium, exit_premium,
                exit_reason,
                quantity, lots, lot_size,
                round(gross_pnl, 2) if gross_pnl is not None else None,
                net_pnl,
                decay_pct, won,
                order_id, notes, "live",
            ],
        )
        con.commit()
        con.close()
        net_str = f"  net=₹{net_pnl:,.0f}" if net_pnl is not None else ""
        logger.info(
            f"[trade_logger] 📊 {bot_name} | {option_symbol} | "
            f"gross=₹{gross_pnl:,.0f}{net_str}" if gross_pnl is not None
            else f"[trade_logger] 📊 {bot_name} | {option_symbol} logged"
        )
    except Exception as exc:
        logger.warning(f"[trade_logger] SQLite write failed: {exc}")

    # ── Dual-write to local performance DB ────────────────────────────────────
    if _PERF_DB_AVAILABLE:
        try:
            _log_perf_trade(
                bot_name      = bot_name,
                strategy_type = strategy_type,
                instrument    = instrument,
                symbol        = option_symbol,
                option_type   = option_type,
                entry_time    = entry_time,
                exit_time     = exit_time,
                entry_price   = entry_premium,
                exit_price    = exit_premium,
                exit_reason   = exit_reason,
                quantity      = quantity,
                lots          = lots,
                lot_size      = lot_size,
                gross_pnl     = gross_pnl,
                net_pnl       = net_pnl,
                notes         = notes,
                source        = "paper",
                direction     = direction,
            )
        except Exception as exc:
            logger.debug(f"[trade_logger] perf_db dual-write skipped: {exc}")
