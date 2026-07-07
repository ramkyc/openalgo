"""
Position manager for BB Paper Bot.

Tracks one open position per strategy (Study A / Study B) independently.
Handles entry, exit-condition checking, forced EOD close, and trade logging.
"""

import csv
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

LOT_SIZE   = 65          # NIFTY lot size (updated Dec 2025 revision)
N_LOTS     = 10          # standardised: 10 lots per CLAUDE.md position-size rule
TRADE_COST = 65          # ₹ flat round-trip transaction cost per lot (approx)


@dataclass
class Position:
    strategy:          str
    symbol:            str
    exchange:          str
    entry_px:          float
    entry_ts:          datetime
    quantity:          int
    sl_multiplier:     float    # 2.0 for Study A, 1.5 for Study B
    target_multiplier: float    # 0.70 (E4 = −30% from entry)

    @property
    def sl_price(self) -> float:
        return round(self.entry_px * self.sl_multiplier, 2)

    @property
    def target_price(self) -> float:
        return round(self.entry_px * self.target_multiplier, 2)


class PositionManager:
    """
    Manages open positions for Study A and Study B independently.
    Both strategies sell the same ATM PE; each has its own entry price,
    SL level, and target — and their own trade log CSV.
    """

    def __init__(self, client):
        """
        client: openalgo.api instance (used for order placement).
        """
        self.client     = client
        self.positions: dict[str, Position] = {}   # keyed by strategy name
        self._csv_files: dict[str, Path]    = {}

    # ── Entry ────────────────────────────────────────────────────────────

    def enter(self,
              strategy: str,
              symbol:   str,
              exchange: str,
              entry_px: float,
              sl_mult:  float,
              target_mult: float = 0.70,
              quantity: int = N_LOTS * LOT_SIZE,
              paper_mode: bool = True) -> bool:
        """
        Open a new position for `strategy`. Places a SELL order via OpenAlgo.
        Returns True if order was placed successfully (or logged in paper mode).
        """
        if strategy in self.positions:
            logger.warning(f"[{strategy}] Already has an open position — ignoring entry signal.")
            return False

        if not paper_mode:
            try:
                res = self.client.placesmartorder(
                    strategy=strategy,
                    symbol=symbol,
                    action="SELL",
                    exchange=exchange,
                    product="MIS",
                    pricetype="MARKET",
                    quantity=str(quantity),
                )
                if not res or res.get("status") != "success":
                    logger.error(f"[{strategy}] Order placement failed: {res}")
                    return False
                logger.info(f"[{strategy}] SELL order placed: {symbol} @ {entry_px:.2f}")
            except Exception as e:
                logger.error(f"[{strategy}] Order exception: {e}")
                return False

        pos = Position(
            strategy=strategy,
            symbol=symbol,
            exchange=exchange,
            entry_px=entry_px,
            entry_ts=datetime.now(),
            quantity=quantity,
            sl_multiplier=sl_mult,
            target_multiplier=target_mult,
        )
        self.positions[strategy] = pos
        logger.info(
            f"[{strategy}] Position opened — {symbol} SELL @ ₹{entry_px:.2f}  "
            f"Target ₹{pos.target_price:.2f}  SL ₹{pos.sl_price:.2f}"
        )
        return True

    # ── Exit condition check ─────────────────────────────────────────────

    def check_exit(self, strategy: str, ltp: float) -> Optional[str]:
        """
        Check whether ltp triggers E4 (target) or SL for `strategy`.
        Returns "E4", "SL", or None.
        """
        pos = self.positions.get(strategy)
        if pos is None:
            return None
        if ltp <= pos.target_price:
            return "E4"
        if ltp >= pos.sl_price:
            return "SL"
        return None

    # ── Close ────────────────────────────────────────────────────────────

    def close(self,
              strategy:    str,
              exit_px:     float,
              exit_reason: str,
              paper_mode:  bool = True) -> Optional[dict]:
        """
        Close the open position for `strategy`.
        Returns a trade summary dict, or None if no position existed.
        """
        pos = self.positions.pop(strategy, None)
        if pos is None:
            return None

        if not paper_mode:
            try:
                res = self.client.placesmartorder(
                    strategy=strategy,
                    symbol=pos.symbol,
                    action="BUY",
                    exchange=pos.exchange,
                    product="MIS",
                    pricetype="MARKET",
                    quantity=str(pos.quantity),
                )
                logger.info(f"[{strategy}] BUY (close) order placed: {pos.symbol}")
            except Exception as e:
                logger.error(f"[{strategy}] Close order exception: {e}")

        gross   = (pos.entry_px - exit_px) * pos.quantity
        net     = gross - TRADE_COST
        won     = net > 0
        duration_min = (datetime.now() - pos.entry_ts).seconds // 60

        summary = {
            "strategy":    strategy,
            "symbol":      pos.symbol,
            "entry_ts":    pos.entry_ts.strftime("%Y-%m-%d %H:%M"),
            "exit_ts":     datetime.now().strftime("%Y-%m-%d %H:%M"),
            "entry_px":    round(pos.entry_px, 2),
            "exit_px":     round(exit_px, 2),
            "exit_reason": exit_reason,
            "quantity":    pos.quantity,
            "gross":       round(gross, 2),
            "net":         round(net, 2),
            "won":         won,
            "duration_min": duration_min,
        }
        self._log_trade(strategy, summary)
        emoji = "🟢" if won else "🔴"
        logger.info(
            f"[{strategy}] Position closed — {exit_reason}  "
            f"exit ₹{exit_px:.2f}  {emoji} net ₹{net:,.0f}  ({duration_min} min)"
        )
        return summary

    def has_position(self, strategy: str) -> bool:
        return strategy in self.positions

    def get_position(self, strategy: str) -> Optional[Position]:
        return self.positions.get(strategy)

    # ── CSV trade log ────────────────────────────────────────────────────

    def _log_trade(self, strategy: str, trade: dict):
        tag      = "a" if "A" in strategy else "b"
        csv_path = LOGS_DIR / f"study_{tag}_trades.csv"
        is_new   = not csv_path.exists()
        try:
            with open(csv_path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(trade.keys()))
                if is_new:
                    w.writeheader()
                w.writerow(trade)
        except Exception as e:
            logger.error(f"Could not write trade log: {e}")
