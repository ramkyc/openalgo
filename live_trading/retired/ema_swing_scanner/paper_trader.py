"""
paper_trader.py — Position tracking for EMA Swing Scanner

Manages paper + live positions:
  - Opens positions at next-day's first available price (open)
  - Tracks trailing stop (correct daily-bar logic: check yesterday's stop, update tonight)
  - Closes positions on stop / hard cap / time stop
  - Persists state to JSON and trades to CSV
  - When paper_mode=False, fires real CNC orders via OpenAlgo API

Position lifecycle:
  DAY 0 (signal day)  → signal logged as "pending"
  DAY 1 (entry day)   → position opened at today's open price (+ BUY order if live)
  DAY N               → position updated daily until exit (+ SELL order on exit if live)
"""

from __future__ import annotations

import csv
import json
import logging
import os
from dataclasses import dataclass, asdict, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Performance DB (optional — silent if unavailable) ─────────────────────────
try:
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from live_trading.shared.performance_db import log_trade as _log_perf_trade
    _PERF_DB_OK = True
except Exception:
    _PERF_DB_OK = False

# ── Constants (mirror V3 backtest) ────────────────────────────────────────────
BASE_RISK_PCT    = 0.01   # 1% portfolio per trade
MAX_POS_PCT      = 0.20   # max 20% in one stock
MAX_CONCURRENT   = 5
BREAKEVEN_ATR    = 1.0    # close must reach entry + 1×ATR to activate breakeven
TRAIL_ATR        = 1.0    # trail = highest_close − 1×ATR
HARD_CAP_ATR     = 3.0    # hard exit cap
MAX_HOLD_DAYS    = 20

RISK_MULT = {"tier1": 1.5, "tier2": 1.0, "tier3": 0.75}


# ── Position dataclass ────────────────────────────────────────────────────────
@dataclass
class PaperPosition:
    symbol:           str
    exchange:         str
    tier:             str
    signal_date:      str
    entry_date:       str          # date position was opened (next day after signal)
    entry_price:      float
    shares:           int
    position_value:   float
    risk_inr:         float
    atr_at_entry:     float
    rsi_at_signal:    float
    stop_eod:         float        # stop to use tomorrow (updated at EOD)
    hard_cap:         float
    highest_close:    float        # for trailing
    breakeven_active: bool = False
    days_held:        int   = 0
    status:           str   = "open"   # open / closed
    # Filled on close:
    exit_date:     Optional[str]   = None
    exit_price:    Optional[float] = None
    exit_reason:   Optional[str]   = None
    pnl_inr:       Optional[float] = None
    pnl_pct:       Optional[float] = None
    win:           Optional[bool]  = None


# ── PaperTrader ───────────────────────────────────────────────────────────────
STRATEGY  = "EMA_SWING"   # strategy tag sent to OpenAlgo
PRODUCT   = "CNC"         # delivery (multi-day swing)
EXCHANGE  = "NSE"


class PaperTrader:

    def __init__(self, logs_dir: Path, starting_capital: float = 1_000_000.0,
                 paper_mode: bool = True, openalgo_client=None):
        self.logs_dir    = logs_dir
        self.trades_csv  = logs_dir / "paper_trades.csv"
        self.state_json  = logs_dir / "ema_swing_scanner_state.json"

        self.portfolio_value  = starting_capital
        self.starting_capital = starting_capital
        self.paper_mode       = paper_mode
        self.client           = openalgo_client  # None → paper only

        self.open_positions: dict[str, PaperPosition] = {}   # symbol → position
        self.pending_signals: list[dict] = []                 # signals waiting for next open
        self.closed_trades:   list[dict] = []                 # today's exits

        self._load_state()
        self._ensure_csv_header()

    # ── Order helpers ─────────────────────────────────────────────────────────

    def _place_buy(self, symbol: str, qty: int) -> dict:
        """Place a CNC BUY market order. Returns the API response dict."""
        try:
            resp = self.client.placeorder(
                strategy    = STRATEGY,
                symbol      = symbol,
                action      = "BUY",
                exchange     = EXCHANGE,
                price_type  = "MARKET",
                product     = PRODUCT,
                quantity    = str(qty),
            )
            logger.info(f"  [ORDER] BUY {qty} {symbol} → {resp}")
            return resp or {}
        except Exception as e:
            logger.error(f"  [ORDER] BUY {symbol} exception: {e}")
            return {}

    def _place_sell(self, symbol: str, qty: int) -> dict:
        """Place a CNC SELL market order. Returns the API response dict."""
        try:
            resp = self.client.placeorder(
                strategy    = STRATEGY,
                symbol      = symbol,
                action      = "SELL",
                exchange     = EXCHANGE,
                price_type  = "MARKET",
                product     = PRODUCT,
                quantity    = str(qty),
            )
            logger.info(f"  [ORDER] SELL {qty} {symbol} → {resp}")
            return resp or {}
        except Exception as e:
            logger.error(f"  [ORDER] SELL {symbol} exception: {e}")
            return {}

    # ── State persistence ─────────────────────────────────────────────────────

    def _load_state(self):
        if not self.state_json.exists():
            return
        try:
            raw = json.loads(self.state_json.read_text())
            self.portfolio_value = raw.get("portfolio_value", self.starting_capital)
            for p in raw.get("open_positions", []):
                pos = PaperPosition(**p)
                self.open_positions[pos.symbol] = pos
            self.pending_signals = raw.get("pending_signals", [])
            logger.info(
                f"State restored: portfolio ₹{self.portfolio_value:,.0f} | "
                f"{len(self.open_positions)} open | {len(self.pending_signals)} pending"
            )
        except Exception as e:
            logger.error(f"State load error: {e}")

    def save_state(self, index_info: dict, signals_today: list,
                   regime_active: bool, last_scan: str):
        """Write full state JSON for the dashboard to read."""
        open_pos = [asdict(p) for p in self.open_positions.values()]
        total_return = (self.portfolio_value - self.starting_capital) / self.starting_capital * 100

        state = {
            "bot":             "ema_swing_scanner",
            "mode":            "paper" if self.paper_mode else "live",
            "last_update":     datetime.now().isoformat(),
            "last_scan":       last_scan,
            "regime_active":   regime_active,
            "index":           index_info,
            "portfolio_value": round(self.portfolio_value, 2),
            "starting_capital":self.starting_capital,
            "total_return_pct":round(total_return, 2),
            "open_positions":  open_pos,
            "open_count":      len(open_pos),
            "pending_signals": self.pending_signals,
            "signals_today":   [
                {"symbol": s.symbol, "tier": s.tier, "rsi": s.rsi,
                 "atr": s.atr, "close": s.close,
                 "suggested_stop": s.suggested_stop,
                 "suggested_hard_cap": s.suggested_hard_cap}
                for s in signals_today
            ],
            "closed_today":    self.closed_trades,
            "stats": {
                "open_positions":   len(open_pos),
                "pending_signals":  len(self.pending_signals),
                "closed_today":     len(self.closed_trades),
                "portfolio_inr":    round(self.portfolio_value, 0),
                "return_pct":       round(total_return, 2),
            },
        }
        self.state_json.write_text(json.dumps(state, indent=2, default=str))

    def _ensure_csv_header(self):
        if not self.trades_csv.exists():
            with open(self.trades_csv, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow([
                    "symbol", "tier", "signal_date", "entry_date", "entry_price",
                    "shares", "position_value", "risk_inr", "atr_at_entry",
                    "rsi_at_signal", "exit_date", "exit_price", "exit_reason",
                    "pnl_inr", "pnl_pct", "win", "days_held",
                    "breakeven_activated",
                ])

    def _append_csv(self, p: PaperPosition):
        with open(self.trades_csv, "a", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                p.symbol, p.tier, p.signal_date, p.entry_date, p.entry_price,
                p.shares, p.position_value, p.risk_inr, p.atr_at_entry,
                p.rsi_at_signal, p.exit_date, p.exit_price, p.exit_reason,
                p.pnl_inr, p.pnl_pct, p.win, p.days_held,
                p.breakeven_active,
            ])

    # ── Daily update flow ─────────────────────────────────────────────────────

    def process_day(self, today_ohlcv: dict[str, dict], today_date: date):
        """
        Called once per day with today's OHLCV for all universe stocks.
        today_ohlcv: {symbol: {"open": x, "high": x, "low": x, "close": x}}
        """
        self.closed_trades = []

        # ── Step 1: Open pending positions at today's open ────────────────
        still_pending = []
        for sig in self.pending_signals:
            sym   = sig["symbol"]
            ohlcv = today_ohlcv.get(sym)
            if ohlcv is None:
                still_pending.append(sig)
                continue
            if sym in self.open_positions or len(self.open_positions) >= MAX_CONCURRENT:
                still_pending.append(sig)
                continue

            entry_px = ohlcv["open"]
            atr_val  = sig["atr"]
            stop_px  = entry_px - atr_val
            tier     = sig["tier"]
            risk_inr = self.portfolio_value * BASE_RISK_PCT * RISK_MULT.get(tier, 1.0)
            shares   = max(1, int(risk_inr / max(atr_val, 0.01)))
            shares   = min(shares, int(self.portfolio_value * MAX_POS_PCT / entry_px))

            pos = PaperPosition(
                symbol=sym, exchange=EXCHANGE, tier=tier,
                signal_date=sig["signal_date"],
                entry_date=str(today_date),
                entry_price=round(entry_px, 2),
                shares=shares,
                position_value=round(shares * entry_px, 2),
                risk_inr=round(risk_inr, 2),
                atr_at_entry=round(atr_val, 2),
                rsi_at_signal=sig["rsi"],
                stop_eod=round(stop_px, 2),
                hard_cap=round(entry_px + HARD_CAP_ATR * atr_val, 2),
                highest_close=entry_px,
            )
            self.open_positions[sym] = pos
            mode_tag = "📝 paper" if self.paper_mode else "🔴 LIVE"
            logger.info(
                f"📂 [{mode_tag}] Opened position: {sym} | {shares} shares @ ₹{entry_px:.2f} | "
                f"Stop ₹{stop_px:.2f} | Cap ₹{pos.hard_cap:.2f}"
            )

            # ── Fire live BUY order ────────────────────────────────────────
            if not self.paper_mode and self.client is not None:
                self._place_buy(sym, shares)

        self.pending_signals = still_pending

        # ── Step 2: Check exits using YESTERDAY's stop (stop_eod) ─────────
        to_close = []
        for sym, pos in self.open_positions.items():
            ohlcv = today_ohlcv.get(sym)
            if ohlcv is None:
                continue

            day_high  = ohlcv["high"]
            day_low   = ohlcv["low"]
            day_close = ohlcv["close"]
            pos.days_held += 1

            stop_to_test = pos.stop_eod
            hit_cap  = day_high  >= pos.hard_cap
            hit_stop = day_low   <= stop_to_test

            exit_px = exit_reason = None
            if hit_cap and hit_stop:
                exit_px, exit_reason = stop_to_test, "stop"
            elif hit_cap:
                exit_px, exit_reason = pos.hard_cap, "cap"
            elif hit_stop:
                exit_px, exit_reason = stop_to_test, "stop"
            elif pos.days_held >= MAX_HOLD_DAYS:
                exit_px, exit_reason = day_close, "time"

            if exit_px is not None:
                pnl_inr = (exit_px - pos.entry_price) * pos.shares
                pnl_pct = (exit_px - pos.entry_price) / pos.entry_price * 100
                pos.exit_date   = str(today_date)
                pos.exit_price  = round(exit_px, 2)
                pos.exit_reason = exit_reason
                pos.pnl_inr     = round(pnl_inr, 2)
                pos.pnl_pct     = round(pnl_pct, 3)
                pos.win         = pnl_inr > 0
                pos.status      = "closed"
                self.portfolio_value += pnl_inr

                emoji = "🟢" if pos.win else "🔴"
                mode_tag = "📝 paper" if self.paper_mode else "🔴 LIVE"
                logger.info(
                    f"{emoji} [{mode_tag}] Closed: {sym} | {exit_reason} @ ₹{exit_px:.2f} | "
                    f"P&L ₹{pnl_inr:+,.0f} ({pnl_pct:+.2f}%) | "
                    f"Portfolio: ₹{self.portfolio_value:,.0f}"
                )

                # ── Fire live SELL order ───────────────────────────────────
                if not self.paper_mode and self.client is not None:
                    self._place_sell(sym, pos.shares)

                self.closed_trades.append(asdict(pos))
                self._append_csv(pos)
                # ── Log to unified performance DB ──────────────────────────
                if _PERF_DB_OK:
                    try:
                        _log_perf_trade(
                            bot_name      = "ema_swing_scanner",
                            strategy_type = "equity",
                            instrument    = sym,
                            symbol        = sym,
                            option_type   = None,
                            direction     = "long",
                            entry_time    = datetime.strptime(pos.entry_date, "%Y-%m-%d").replace(hour=9, minute=15),
                            exit_time     = datetime.strptime(pos.exit_date,  "%Y-%m-%d").replace(hour=15, minute=30),
                            entry_price   = pos.entry_price,
                            exit_price    = pos.exit_price,
                            exit_reason   = pos.exit_reason,
                            quantity      = pos.shares,
                            lots          = None,
                            lot_size      = None,
                            gross_pnl     = pos.pnl_inr,
                            notes         = f"tier={pos.tier} atr={pos.atr_at_entry} rsi={pos.rsi_at_signal}",
                            source        = "paper" if self.paper_mode else "live",
                        )
                    except Exception as _e:
                        logger.debug(f"[paper_trader] perf_db log failed: {_e}")
                to_close.append(sym)
                continue

            # ── Step 3: Update stop at END of day ─────────────────────────
            if day_close > pos.highest_close:
                pos.highest_close = day_close

            if (not pos.breakeven_active and
                    day_close >= pos.entry_price + BREAKEVEN_ATR * pos.atr_at_entry):
                pos.breakeven_active = True
                logger.info(f"  ✅ {sym} breakeven activated (close {day_close:.2f})")

            if pos.breakeven_active:
                new_stop = pos.highest_close - TRAIL_ATR * pos.atr_at_entry
                new_stop = max(new_stop, pos.entry_price)   # never below entry
                pos.stop_eod = max(pos.stop_eod, new_stop)

        for sym in to_close:
            del self.open_positions[sym]

    # ── Add new signals as pending ────────────────────────────────────────────

    def add_signals(self, signals: list, signal_date: str):
        """Queue today's signals to be opened at tomorrow's open."""
        for sig in signals:
            if sig.symbol in self.open_positions:
                logger.info(f"  {sig.symbol}: already in open positions — skipped")
                continue
            already_pending = any(p["symbol"] == sig.symbol for p in self.pending_signals)
            if already_pending:
                logger.info(f"  {sig.symbol}: already pending — skipped")
                continue
            self.pending_signals.append({
                "symbol":      sig.symbol,
                "tier":        sig.tier,
                "rsi":         sig.rsi,
                "atr":         sig.atr,
                "close":       sig.close,
                "signal_date": signal_date,
            })
            logger.info(f"  📌 Queued for tomorrow: {sig.symbol} | ATR {sig.atr:.2f} | Tier {sig.tier}")
