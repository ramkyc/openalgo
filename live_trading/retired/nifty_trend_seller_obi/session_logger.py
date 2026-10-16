"""
Session Logger — NTS+OBI Paper Trade Analysis Logger
=====================================================
Writes three CSV files to the bot's logs/ directory:

  signals.csv  — Every signal that fires (before OBI gate decision).
                 Captures: indicators, OBI value, whether gate passed.
                 Written for EVERY qualifying NTS signal regardless of OBI.

  trades.csv   — Completed paper trades (only OBI-approved entries).
                 Captures: entry/exit premium, reason, gross & net PnL.

  ghosts.csv   — OBI-blocked signals tracked to EOD for counterfactual PnL.
                 Answers: "what would have happened if we ignored the OBI gate?"

After 15 paper sessions, compare:
    WR(trades.csv)     vs WR(signals.csv where obi_gate_pass=False + ghost exit)
    Sharpe(trades.csv) vs Sharpe(all signals without OBI gate)

These two streams reveal whether the OBI gate is adding genuine edge.
"""

import csv
import logging
import threading
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class SessionLogger:
    """Thread-safe append-only CSV logger with three separate output streams."""

    # ── Column definitions ────────────────────────────────────────────────────

    SIGNAL_FIELDS = [
        "date", "signal_time", "direction",
        "nifty_spot", "atm_strike", "option_symbol",
        "adx", "rsi_val", "macd_cross",          # indicator values at signal bar
        "entry_premium",                           # option LTP at signal moment
        "obi_at_signal",                           # weighted OBI of ATM CE
        "obi_raw_at_signal",                       # unweighted OBI (diagnostic)
        "obi_bid_tot", "obi_ask_tot",             # raw qty totals (diagnostic)
        "obi_n_levels",                            # depth levels received (max 50)
        "obi_gate_pass",                           # True / False
        "obi_threshold",                           # configured threshold value
        "obi_stale",                               # True if depth was stale at read
    ]

    TRADE_FIELDS = [
        "date", "signal_time", "direction",
        "nifty_spot", "atm_strike", "option_symbol",
        "entry_premium", "lots",
        "sl_price",                                # = entry_premium × sl_mult
        "exit_time", "exit_premium", "exit_reason",# SL_HIT / EOD_EXIT
        "obi_at_signal",
        "pnl_gross_per_lot",                       # (entry - exit) × lot_size
        "pnl_gross_total",                         # × lots
        "transaction_cost_total",                  # from transaction_costs module
        "pnl_net_total",                           # gross - costs
    ]

    GHOST_FIELDS = [
        "date", "signal_time",
        "atm_strike", "option_symbol",
        "entry_premium",                           # premium at blocked signal time
        "ghost_exit_time", "ghost_exit_premium", "ghost_exit_reason",
        "obi_at_signal",
        "lots",
        "ghost_pnl_gross_per_lot",
        "ghost_pnl_gross_total",
    ]

    def __init__(self, log_dir: Path):
        self._dir  = log_dir
        self._lock = threading.Lock()
        self._dir.mkdir(parents=True, exist_ok=True)
        self._init_files()

    # ── Public write methods ──────────────────────────────────────────────────

    def log_signal(self, **kwargs) -> None:
        """
        Call for EVERY NTS signal that fires (OBI gate check happens after).
        Required keys match SIGNAL_FIELDS above.
        """
        kwargs.setdefault("date", datetime.now().strftime("%Y-%m-%d"))
        self._append("signals.csv", self.SIGNAL_FIELDS, kwargs)

    def log_trade(self, **kwargs) -> None:
        """
        Call when a paper trade is completed (OBI gate passed, exit triggered).
        """
        kwargs.setdefault("date", datetime.now().strftime("%Y-%m-%d"))
        self._append("trades.csv", self.TRADE_FIELDS, kwargs)

    def log_ghost(self, **kwargs) -> None:
        """
        Call when a ghost (OBI-blocked) trade reaches EOD exit or SL in ghost mode.
        """
        kwargs.setdefault("date", datetime.now().strftime("%Y-%m-%d"))
        self._append("ghosts.csv", self.GHOST_FIELDS, kwargs)

    def session_summary(self) -> dict:
        """
        Read trades.csv and ghosts.csv for today, compute per-session stats.
        Returns a dict suitable for Telegram summary.
        """
        today = datetime.now().strftime("%Y-%m-%d")
        trades = self._read_today("trades.csv", today)
        ghosts = self._read_today("ghosts.csv", today)

        t_count = len(trades)
        g_count = len(ghosts)

        # Trade PnL
        t_pnl = sum(float(r.get("pnl_net_total", 0) or 0) for r in trades)
        t_wins = sum(
            1 for r in trades
            if float(r.get("pnl_gross_per_lot", 0) or 0) > 0
        )
        t_wr = (t_wins / t_count * 100) if t_count > 0 else 0.0

        # Ghost PnL (counterfactual)
        g_pnl = sum(float(r.get("ghost_pnl_gross_total", 0) or 0) for r in ghosts)
        g_wins = sum(
            1 for r in ghosts
            if float(r.get("ghost_pnl_gross_per_lot", 0) or 0) > 0
        )
        g_wr = (g_wins / g_count * 100) if g_count > 0 else 0.0

        return {
            "date":             today,
            "trades_taken":     t_count,
            "trades_wr_pct":    round(t_wr, 1),
            "trades_pnl_net":   round(t_pnl, 2),
            "signals_blocked":  g_count,
            "ghost_wr_pct":     round(g_wr, 1),
            "ghost_pnl_gross":  round(g_pnl, 2),
            "obi_edge_pnl":     round(t_pnl - g_pnl, 2),  # positive = OBI helped
        }

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _init_files(self) -> None:
        """Create CSV files with headers if they don't exist yet."""
        for fname, fields in [
            ("signals.csv", self.SIGNAL_FIELDS),
            ("trades.csv",  self.TRADE_FIELDS),
            ("ghosts.csv",  self.GHOST_FIELDS),
        ]:
            path = self._dir / fname
            if not path.exists():
                with open(path, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader()
                logger.info(f"[LOG] Created {path}")

    def _append(self, fname: str, fields: list, row: dict) -> None:
        path = self._dir / fname
        with self._lock:
            with open(path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                writer.writerow(row)
        logger.info(f"[LOG] {fname} ← {row}")

    def _read_today(self, fname: str, today: str) -> list[dict]:
        path = self._dir / fname
        if not path.exists():
            return []
        rows = []
        try:
            with open(path, "r") as f:
                for row in csv.DictReader(f):
                    if row.get("date") == today:
                        rows.append(row)
        except Exception as e:
            logger.error(f"[LOG] Error reading {fname}: {e}")
        return rows
