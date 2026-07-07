"""
Equity OBI Paper Trading Bot — RELIANCE + HDFCBANK
====================================================
Trades NSE MIS equity (intraday long AND short) using Order Book Imbalance
as the primary entry trigger, confirmed by a proximity-EMA trend filter.

Entry logic (per symbol):
  LONG:  rolling w_OBI > +20 (3 consecutive ticks) AND VWMP < LTP AND close > EMA(20)
  SHORT: rolling w_OBI < -20 (3 consecutive ticks) AND VWMP > LTP AND close < EMA(20)

Position sizing:
  Capital per trade: ₹1,00,000
  Shares = floor(100_000 / LTP)

Exit rules:
  Stop-loss: 0.4% adverse move from entry
  Target:    0.6% in-favour move from entry  (R:R ≈ 1.5:1)
  EOD exit:  15:14 IST (MIS square-off before broker auto-exit at 15:20)

Logging:
  trades.csv   — OBI-approved completed trades
  ghosts.csv   — OBI-blocked counterfactual tracking
  signals.csv  — Every signal bar regardless of OBI gate

Stage 11 — paper mode only.
Set EQUITY_OBI_PAPER_MODE=true in .env (default true).

Usage:
    uv run live_trading/equity_obi/equity_obi_bot.py
"""

import csv
import json
import logging
import math
import os
import sys
import threading
import time
from collections import deque
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

# ── Project root on path ───────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from live_trading.nifty_trend_seller_obi.obi_engine import OBIEngine

# ── Environment ────────────────────────────────────────────────────────────────
load_dotenv()
API_KEY      = os.getenv("OPENALGO_API_KEY")
HOST         = os.getenv("HOST_SERVER",         "http://127.0.0.1:5001")
WS_URL       = os.getenv("WEBSOCKET_URL",       "ws://127.0.0.1:5001/ws")
TG_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
PAPER_MODE   = os.getenv("EQUITY_OBI_PAPER_MODE", "true").lower() == "true"

# ── Strategy constants ─────────────────────────────────────────────────────────
STRATEGY_NAME         = "equity_obi"
# Core 4 stocks — always subscribed (use TBT slots 1-4).
# AXISBANK is added at 13:00 when NTS+OBI releases the 5th TBT slot.
# TBT slot allocation:
#   09:40–13:00: equity_obi × 4 (slots 1-4) | NTS+OBI ATM CE (slot 5)
#   13:00–15:30: equity_obi × 5 (slots 1-5) | NTS+OBI OBI engine stopped
CORE_SYMBOLS          = ["HDFCBANK", "RELIANCE", "ICICIBANK", "INFY"]
AXISBANK_SYMBOL       = "AXISBANK"
SYMBOLS               = CORE_SYMBOLS + [AXISBANK_SYMBOL]   # full universe (for state/logging)
CORE_OBI_SYMBOLS      = CORE_SYMBOLS  # only 4 at startup — leaves slot 5 for NTS+OBI

# AXISBANK-specific entry window (starts at 13:01 when NTS+OBI slot is free)
AXISBANK_ENTRY_START  = dt_time(13, 1)
AXISBANK_ENTRY_END    = dt_time(15, 30)
EXCHANGE              = "NSE"

OBI_LONG_THRESHOLD    = +20.0   # w_OBI must exceed this for LONG entry
OBI_SHORT_THRESHOLD   = -20.0   # w_OBI must be below this for SHORT entry
OBI_ROLLING_N         = 3       # consecutive ticks that must sustain the signal
OBI_STALE_SECONDS     = 5.0
OBI_MIN_LEVELS        = 20      # minimum depth levels to trust the OBI reading
OBI_WARMUP_TICKS      = 50      # ticks before any signal is evaluated

EMA_PERIOD            = 20      # 1-min EMA for trend filter
CAPITAL_PER_TRADE     = 100_000 # ₹1,00,000 per position
MAX_TOTAL_POSITIONS   = 5       # Global limit across all symbols (requested: start with 5)
SL_PCT                = 0.004   # 0.4% stop-loss
TARGET_PCT            = 0.006   # 0.6% target
EOD_EXIT_TIME         = dt_time(15, 14)

ENTRY_START           = dt_time(9, 25)
ENTRY_END             = dt_time(15, 15)

IST                   = ZoneInfo("Asia/Kolkata")

LOG_DIR               = Path(__file__).parent / "logs"

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "equity_obi.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


from live_trading.shared.performance_db import log_trade as log_trade_to_db

# ── CSV Logger ─────────────────────────────────────────────────────────────────

class EquityLogger:
    """Thread-safe CSV logger with three streams: signals, trades, ghosts."""

    SIGNAL_FIELDS = [
        "date", "signal_time", "symbol", "direction",
        "ltp", "entry_price", "ema20",
        "obi_rolling", "obi_spot", "vwmp",
        "obi_gate_pass", "obi_threshold", "obi_stale", "obi_n_levels",
    ]
    TRADE_FIELDS = [
        "date", "signal_time", "symbol", "direction",
        "entry_price", "shares",
        "sl_price", "target_price",
        "exit_time", "exit_price", "exit_reason",
        "obi_at_signal",
        "pnl_gross", "pnl_pct",
    ]
    GHOST_FIELDS = [
        "date", "signal_time", "symbol", "direction",
        "entry_price", "shares",
        "ghost_exit_time", "ghost_exit_price", "ghost_exit_reason",
        "obi_at_signal",
        "ghost_pnl_gross",
    ]

    def __init__(self, log_dir: Path):
        self._dir  = log_dir
        self._lock = threading.Lock()
        self._dir.mkdir(parents=True, exist_ok=True)
        self._init_files()

    def _init_files(self):
        for fname, fields in [
            ("signals.csv", self.SIGNAL_FIELDS),
            ("trades.csv",  self.TRADE_FIELDS),
            ("ghosts.csv",  self.GHOST_FIELDS),
        ]:
            path = self._dir / fname
            if not path.exists():
                with open(path, "w", newline="") as f:
                    csv.DictWriter(f, fieldnames=fields).writeheader()

    def _append(self, fname: str, fields: list, row: dict):
        path = self._dir / fname
        with self._lock:
            with open(path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
                writer.writerow(row)

    def log_signal(self, **kw):
        kw.setdefault("date", date.today().isoformat())
        self._append("signals.csv", self.SIGNAL_FIELDS, kw)

    def log_trade(self, **kw):
        kw.setdefault("date", date.today().isoformat())
        # 1. Local CSV
        self._append("trades.csv", self.TRADE_FIELDS, kw)

        # 2. Performance DB (DuckDB)
        try:
            # Convert signal_time and exit_time to datetime objects for the DB
            entry_dt = datetime.combine(
                date.fromisoformat(kw["date"]),
                datetime.strptime(kw["signal_time"], "%H:%M:%S").time()
            )
            exit_dt = datetime.combine(
                date.fromisoformat(kw["date"]),
                datetime.strptime(kw["exit_time"], "%H:%M:%S").time()
            )

            log_trade_to_db(
                bot_name      = STRATEGY_NAME,
                strategy_type = "equity",
                instrument    = kw["symbol"],
                symbol        = kw["symbol"],
                entry_time    = entry_dt,
                exit_time     = exit_dt,
                entry_price   = float(kw["entry_price"]),
                exit_price    = float(kw["exit_price"]),
                exit_reason   = kw["exit_reason"],
                quantity      = int(kw["shares"]),
                gross_pnl     = float(kw["pnl_gross"]),
                direction     = kw["direction"].lower(),
                source        = "paper" if PAPER_MODE else "live",
            )
        except Exception as e:
            logger.error(f"[LOG] Failed to write to Performance DB: {e}")

    def log_ghost(self, **kw):
        kw.setdefault("date", date.today().isoformat())
        self._append("ghosts.csv", self.GHOST_FIELDS, kw)


# ── Main Bot ───────────────────────────────────────────────────────────────────

class EquityOBIBot:
    """
    Paper trading bot: NSE MIS equity, long + short, OBI-gated.
    Multiple positions allowed per symbol with cooldown period.

    Design changes from single-trade mode:
    - Removed trade_done lock: allows multiple trades per day
    - Position limit: max 50% capital deployed
    - Cooldown period: 15 min between new entries
    """

    def __init__(self):
        self.obi     = OBIEngine(api_key=API_KEY, host=HOST, ws_url=WS_URL, use_depth_50=True)
        self.logger  = EquityLogger(LOG_DIR)
        self.session_active = False

        # Per-symbol state
        # positions[sym]  = list[dict] (list of active positions)
        # ghosts[sym]     = dict | None  (OBI-blocked counterfactual)
        # position_last_time[sym] = datetime (for cooldown check)
        self.positions:   dict[str, list[dict]] = {s: [] for s in SYMBOLS}
        self.ghosts:      dict[str, dict | None] = {s: None for s in SYMBOLS}
        self._position_last_time: dict[str, dt_time | None] = {s: None for s in SYMBOLS}

        # EMA state per symbol (Wilder's EMA updated on each 1-min bar)
        self._ema:        dict[str, float | None] = {s: None for s in SYMBOLS}

        # Position configuration
        # One position per symbol — only re-evaluate after SL or Target closes it.
        # After a close, a 15-min cooldown applies before the next entry is considered.
        self._cooldown_minutes = 15   # re-entry cooldown after a position closes
        self._max_positions    = 1    # max active positions per symbol
        self._max_capital_pct  = 0.50 # max 50% capital deployed

        self._scheduler = BackgroundScheduler(timezone=str(IST))
        self._state_file = LOG_DIR / "equity_obi_state.json"

        # Restore any open positions from today's state file (mid-session restart recovery)
        self._restore_state()

    # ── State restore ─────────────────────────────────────────────────────────

    def _restore_state(self) -> None:
        """
        Reload today's active positions from the state file on mid-session restart.

        Only runs if the state file timestamp is from today (date match).
        on_startup() at 09:40 resets everything fresh — this only activates
        for restarts that happen AFTER 09:40 when on_startup has already fired.
        """
        if not self._state_file.exists():
            return
        try:
            saved = json.loads(self._state_file.read_text())
            ts = saved.get("timestamp", "")
            if not ts:
                return
            saved_date = datetime.fromisoformat(ts).date()
            if saved_date != date.today():
                return   # stale — yesterday's state, ignore

            restored = 0
            for sym in SYMBOLS:
                sym_data = saved.get("symbols", {}).get(sym, {})
                positions = sym_data.get("positions") or []
                for pos in positions:
                    # Ensure is_closing is reset so the position can be managed normally
                    pos["is_closing"] = False
                    self.positions[sym].append(pos)
                    restored += 1
                last_entry = sym_data.get("last_entry")
                if last_entry and positions:
                    try:
                        self._position_last_time[sym] = datetime.fromisoformat(
                            str(last_entry)
                        ).time()
                    except Exception:
                        pass

            if restored:
                logger.info(
                    f"[_restore_state] Restored {restored} open position(s) from today's "
                    f"state file (mid-session restart recovery)."
                )
                for sym in SYMBOLS:
                    if self.positions[sym]:
                        for p in self.positions[sym]:
                            logger.info(
                                f"  ↳ {sym} {p.get('direction')} "
                                f"@ ₹{p.get('entry_price')} × {p.get('shares')} shares"
                            )
        except Exception as e:
            logger.warning(f"[_restore_state] Could not read state file: {e}")

    # ── HTTP helpers ───────────────────────────────────────────────────────────

    def _get(self, endpoint: str, params: dict) -> dict:
        url = f"{HOST}/api/v1/{endpoint}"
        try:
            r = requests.get(url, params={**params, "apikey": API_KEY}, timeout=10)
            return r.json()
        except Exception as e:
            logger.error(f"[HTTP] GET {endpoint} failed: {e}")
            return {}

    def _post(self, endpoint: str, payload: dict) -> dict:
        url = f"{HOST}/api/v1/{endpoint}"
        try:
            r = requests.post(url, json={**payload, "apikey": API_KEY}, timeout=60)
            if not r.text.strip():
                logger.error(f"[HTTP] POST {endpoint} → HTTP {r.status_code}, empty body")
                return {}
            return r.json()
        except Exception as e:
            logger.error(f"[HTTP] POST {endpoint} failed: {e}")
            return {}

    def _tg(self, msg: str) -> None:
        if not TG_TOKEN or not TG_CHAT_ID:
            return
        try:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json={"chat_id": TG_CHAT_ID, "text": f"📊 *EQUITY OBI*\n{msg}",
                      "parse_mode": "Markdown"},
                timeout=5,
            )
        except Exception as e:
            logger.error(f"[TG] {e}")

    # ── Data helpers ───────────────────────────────────────────────────────────

    def _fetch_1min(self, symbol: str) -> list[dict]:
        """Fetch recent 1-min OHLCV bars from OpenAlgo history API.

        Uses POST (required by OpenAlgo /api/v1/history).
        5-day lookback so EMA(20) has enough bars even early in the session.
        """
        end_date   = date.today()
        start_date = end_date - timedelta(days=5)
        resp = self._post("history", {
            "symbol":     symbol,
            "exchange":   EXCHANGE,
            "interval":   "1m",
            "start_date": start_date.isoformat(),
            "end_date":   end_date.isoformat(),
            "source":     "api",
        })
        return resp.get("data", [])

    def _get_ltp(self, symbol: str) -> float:
        """Fetch current LTP; prefers fresh OBI snapshot, falls back to REST when stale.

        When the OBI WebSocket goes quiet (broker stops sending TBT ticks), the
        snapshot's ltp freezes at the last tick value. Using that frozen price for
        exit calculations produces ₹0 P&L when the frozen price equals entry price.
        Guard: only trust the snapshot when it is fresh (< OBI_STALE_SECONDS old);
        otherwise fall through to the REST quotes endpoint for an accurate price.
        """
        snap = self.obi.get_snapshot(symbol)
        if snap and snap.get("ltp") and not self.obi.is_stale(symbol, max_age_seconds=OBI_STALE_SECONDS):
            return float(snap["ltp"])

        # OBI snapshot missing, lacks ltp, or is stale — use REST for accurate price
        resp = self._post("quotes", {"symbol": symbol, "exchange": EXCHANGE})
        try:
            return float(resp["data"]["ltp"])
        except Exception:
            # Last resort: return the stale OBI ltp rather than 0 so exits still fire
            if snap and snap.get("ltp"):
                logger.warning(
                    f"[LTP] REST quotes failed for {symbol}; using stale OBI ltp "
                    f"₹{snap['ltp']} — PnL may be inaccurate"
                )
                return float(snap["ltp"])
            return 0.0

    def _compute_ema(self, symbol: str, bars: list[dict]) -> float | None:
        """
        Compute / update EMA(20) using 1-min close prices.
        On the first call (no prior EMA), seeds with the SMA of the first 20 bars.
        Subsequent calls do a single-step Wilder update.
        """
        closes = [float(b["close"]) for b in bars if "close" in b]
        if len(closes) < EMA_PERIOD:
            return None
        k = 2 / (EMA_PERIOD + 1)
        prior = self._ema.get(symbol)
        if prior is None:
            # Seed with SMA of first EMA_PERIOD bars
            ema = sum(closes[:EMA_PERIOD]) / EMA_PERIOD
            for c in closes[EMA_PERIOD:]:
                ema = c * k + ema * (1 - k)
        else:
            # Incremental update using the latest close
            ema = closes[-1] * k + prior * (1 - k)
        self._ema[symbol] = ema
        return ema

    # ── OBI gate ───────────────────────────────────────────────────────────────

    def _check_obi_gate(
        self, symbol: str, direction: str
    ) -> tuple[bool, dict]:
        """
        Evaluate OBI entry gate for *symbol* in *direction* ('LONG'|'SHORT').

        Conditions for LONG:
            rolling_obi  > OBI_LONG_THRESHOLD  (sustained buying pressure)
            vwmp         < ltp                 (book centre-of-gravity is below price →
                                                buyers chasing up, bullish)
        Conditions for SHORT:
            rolling_obi  < OBI_SHORT_THRESHOLD (sustained selling pressure)
            vwmp         > ltp                 (book C-o-G is above price →
                                                sellers leaning down, bearish)

        Returns (gate_pass: bool, info_dict).
        """
        snap = self.obi.get_snapshot(symbol)
        if snap is None:
            return False, {"obi_spot": None, "obi_rolling": None,
                           "vwmp": None, "obi_stale": True, "obi_n_levels": 0,
                           "obi_gate_pass": False,
                           "obi_threshold": OBI_LONG_THRESHOLD if direction == "LONG" else OBI_SHORT_THRESHOLD}

        n_levels = snap.get("n_levels", 0)
        stale    = self.obi.is_stale(symbol, max_age_seconds=OBI_STALE_SECONDS)
        ltp      = snap["ltp"]
        vwmp     = snap.get("vwmp", ltp)
        obi_spot = snap["obi"]
        threshold = OBI_LONG_THRESHOLD if direction == "LONG" else OBI_SHORT_THRESHOLD

        # n_levels guard
        if n_levels < OBI_MIN_LEVELS:
            logger.warning(f"[OBI] {symbol} only {n_levels} levels — treating as stale")
            return False, {"obi_spot": obi_spot, "obi_rolling": None,
                           "vwmp": vwmp, "obi_stale": True, "obi_n_levels": n_levels,
                           "obi_gate_pass": False, "obi_threshold": threshold}

        # Rolling OBI — require 3 ticks of sustained pressure
        rolling = self.obi.get_rolling_obi(symbol, n=OBI_ROLLING_N)
        if rolling is None:
            logger.info(f"[OBI] {symbol} rolling not ready (<{OBI_ROLLING_N} ticks)")
            return False, {"obi_spot": obi_spot, "obi_rolling": None,
                           "vwmp": vwmp, "obi_stale": stale, "obi_n_levels": n_levels,
                           "obi_gate_pass": False, "obi_threshold": threshold}

        if direction == "LONG":
            obi_ok   = rolling > OBI_LONG_THRESHOLD
            vwmp_ok  = vwmp < ltp   # C-o-G below price → buyers pulling price up
        else:
            obi_ok   = rolling < OBI_SHORT_THRESHOLD
            vwmp_ok  = vwmp > ltp   # C-o-G above price → sellers leaning down

        gate_pass = obi_ok and vwmp_ok and (not stale)

        logger.info(
            f"[OBI] {symbol} {direction} | rolling={rolling:+.1f} "
            f"vwmp={vwmp:.2f} ltp={ltp:.2f} n={n_levels} stale={stale} "
            f"→ {'PASS ✅' if gate_pass else 'BLOCK 🚫'}"
        )
        return gate_pass, {
            "obi_spot":    round(obi_spot, 2),
            "obi_rolling": round(rolling,  2),
            "vwmp":        round(vwmp, 2),
            "obi_stale":   stale,
            "obi_n_levels": n_levels,
            "obi_gate_pass": gate_pass,
            "obi_threshold": threshold,
        }

    # ── Position sizing ────────────────────────────────────────────────────────

    def _shares(self, ltp: float) -> int:
        """Number of shares to buy/sell for ₹1L capital."""
        if ltp <= 0:
            return 0
        return max(1, math.floor(CAPITAL_PER_TRADE / ltp))

    # ── Order placement ────────────────────────────────────────────────────────

    def _place_order(
        self, symbol: str, action: str, qty: int
    ) -> bool:
        """Place a paper/live MIS equity order. Returns True on success.

        ⚠️  OpenAlgo Analyzer returns status=success in the HTTP response even
        when the underlying sandbox INSERT is rejected (e.g. UNIQUE constraint
        conflict from a stale row left by another strategy).  We therefore
        verify by checking the orderbook for the returned orderid.
        """
        payload = {
            "strategy":   STRATEGY_NAME,
            "symbol":     symbol,
            "exchange":   EXCHANGE,
            "action":     action,   # "BUY" or "SELL"
            "product":    "MIS",
            "pricetype":  "MARKET",
            "quantity":   qty,
            "price":      0,
            "trigger_price":        0,
            "disclosed_quantity":   0,
        }
        resp = self._post("placeorder", payload)
        logger.info(f"[ORDER] {action} {symbol} qty={qty} paper={PAPER_MODE} → {resp}")

        if resp.get("status") != "success":
            return False

        orderid = resp.get("orderid")
        if not orderid:
            return False

        # Verify the order actually landed (not silently rejected by Analyzer)
        try:
            time.sleep(0.3)  # brief pause for sandbox to finalize
            ob_resp = self._get("orderbook", {"apikey": API_KEY})
            orders = ob_resp.get("data", [])
            for o in orders:
                if str(o.get("orderid")) == str(orderid):
                    status = o.get("status", "").lower()
                    if status in ("rejected", "cancelled"):
                        logger.error(
                            f"[ORDER] {action} {symbol} orderid={orderid} internally "
                            f"{status} — sandbox conflict (stale position row?). "
                            f"Run: uv run live_trading/fix_sandbox_positions.py --issue 1"
                        )
                        return False
                    return True
        except Exception as e:
            logger.warning(f"[ORDER] Could not verify orderid={orderid}: {e}")

        return True  # assume ok if orderbook check fails

    # ── Signal evaluation ──────────────────────────────────────────────────────

    def _evaluate_symbol(self, symbol: str) -> None:
        """
        Run signal logic for one symbol.
        Called every minute by the scheduler inside on_signal_check().

        Multi-trade mode:
        - Removed trade_done lock
        - Check position count limit
        - Check cooldown period
        """
        # Warmup guard
        if self.obi.tick_count < OBI_WARMUP_TICKS:
            return

        # AXISBANK specific time window check (uses 5th TBT slot after NTS+OBI closes at 13:00)
        now = datetime.now(IST).time()
        if symbol == "AXISBANK":
            if not (AXISBANK_ENTRY_START <= now <= AXISBANK_ENTRY_END):
                return

        # Fetch 1-min bars + compute EMA
        bars = self._fetch_1min(symbol)
        if not bars or len(bars) < EMA_PERIOD + 5:
            return

        # Drop the still-forming current bar
        completed = bars[:-1]
        ema = self._compute_ema(symbol, completed)
        if ema is None:
            return

        last_close = float(completed[-1]["close"])
        ltp        = self._get_ltp(symbol)
        if ltp <= 0:
            return

        now_str = datetime.now(IST).strftime("%H:%M:%S")

        # ── Cooldown check ──────────────────────────────────────────────────────
        last_time = self._position_last_time[symbol]
        if last_time is not None:
            cooldown_threshold = (
                datetime.combine(date.today(), last_time).replace(tzinfo=IST)
                + timedelta(minutes=self._cooldown_minutes)
            )
            if datetime.now(IST) < cooldown_threshold:
                logger.info(f"[COOLDOWN] {symbol} cooldown until {cooldown_threshold.strftime('%H:%M')}")
                self.logger.log_signal(
                    signal_time  = now_str,
                    symbol       = symbol,
                    direction    = "N/A",
                    ltp          = round(ltp, 2),
                    entry_price  = round(ltp, 2),
                    ema20        = round(ema, 2),
                    obi_rolling  = "N/A",
                    obi_spot     = "N/A",
                    vwmp         = "N/A",
                    obi_gate_pass = False,
                    obi_stale    = False,
                    obi_n_levels = 0,
                    cooldown     = True,
                )
                return

        # ── Position count check ────────────────────────────────────────────────
        if len(self.positions[symbol]) >= self._max_positions:
            logger.debug(f"[POSITION_LIMIT] {symbol} has {len(self.positions[symbol])} positions (max {self._max_positions})")
            return

        # ── Global total positions check ────────────────────────────────────────
        total_active = sum(len(pos_list) for pos_list in self.positions.values())
        if total_active >= MAX_TOTAL_POSITIONS:
            logger.debug(f"[GLOBAL_LIMIT] Total active positions ({total_active}) reached MAX_TOTAL_POSITIONS ({MAX_TOTAL_POSITIONS})")
            return

        # ── Signal detection ─────────────────────────────────────────────────
        snap = self.obi.get_snapshot(symbol)
        vwmp = snap.get("vwmp", ltp) if snap else ltp

        if last_close > ema and vwmp < ltp:
            direction = "LONG"
        elif last_close < ema and vwmp > ltp:
            direction = "SHORT"
        else:
            return   # no signal bar

        # ── OBI gate ─────────────────────────────────────────────────────────
        gate_pass, obi_info = self._check_obi_gate(symbol, direction)

        # Log signal (regardless of gate)
        self.logger.log_signal(
            signal_time  = now_str,
            symbol       = symbol,
            direction    = direction,
            ltp          = round(ltp, 2),
            entry_price  = round(ltp, 2),
            ema20        = round(ema, 2),
            **obi_info,
        )

        if gate_pass:
            self._enter_position(symbol, direction, ltp, now_str, obi_info)
        else:
            self._enter_ghost(symbol, direction, ltp, now_str, obi_info)

    def _enter_position(
        self, symbol: str, direction: str,
        ltp: float, signal_time: str, obi_info: dict,
    ) -> None:
        shares = self._shares(ltp)
        if shares == 0:
            return

        action = "BUY" if direction == "LONG" else "SELL"
        ok = self._place_order(symbol, action, shares)
        if not ok:
            logger.error(f"[BOT] Order placement failed for {symbol} {direction}")
            return

        if direction == "LONG":
            sl_price     = round(ltp * (1 - SL_PCT),     2)
            target_price = round(ltp * (1 + TARGET_PCT),  2)
        else:
            sl_price     = round(ltp * (1 + SL_PCT),     2)
            target_price = round(ltp * (1 - TARGET_PCT),  2)

        # Multi-trade mode: append to list instead of replacing
        position = {
            "position_id":    len(self.positions[symbol]) + 1,
            "symbol":         symbol,
            "direction":      direction,
            "entry_price":    ltp,
            "shares":         shares,
            "sl_price":       sl_price,
            "target_price":   target_price,
            "signal_time":    signal_time,
            "obi_at_signal":  obi_info.get("obi_spot"),
            "is_closing":     False,   # prevents duplicate exit orders
        }
        self.positions[symbol].append(position)
        self._position_last_time[symbol] = datetime.now(IST).time()

        msg = (
            f"📥 *TRADE ENTERED #{position['position_id']}* {symbol} {direction}\n"
            f"Entry: ₹{ltp:.2f} | SL: ₹{sl_price:.2f} | Tgt: ₹{target_price:.2f}\n"
            f"Shares: {shares} | OBI: {obi_info.get('obi_rolling', 'N/A'):+.1f} | "
            f"Positions: {len(self.positions[symbol])}/{self._max_positions}"
        )
        logger.info(f"[BOT] {msg}")
        self._tg(msg)

    def _enter_ghost(
        self, symbol: str, direction: str,
        ltp: float, signal_time: str, obi_info: dict,
    ) -> None:
        if self.ghosts[symbol] is not None:
            return  # already ghost-tracking this symbol today

        shares = self._shares(ltp)
        if direction == "LONG":
            sl_price     = round(ltp * (1 - SL_PCT),    2)
            target_price = round(ltp * (1 + TARGET_PCT), 2)
        else:
            sl_price     = round(ltp * (1 + SL_PCT),    2)
            target_price = round(ltp * (1 - TARGET_PCT), 2)

        self.ghosts[symbol] = {
            "symbol":        symbol,
            "direction":     direction,
            "entry_price":   ltp,
            "shares":        shares,
            "sl_price":      sl_price,
            "target_price":  target_price,
            "signal_time":   signal_time,
            "obi_at_signal": obi_info.get("obi_spot"),
        }
        logger.info(
            f"[GHOST] OBI blocked {symbol} {direction} @ ₹{ltp:.2f} — "
            f"ghost tracking started"
        )

    # ── Position / ghost monitoring ────────────────────────────────────────────

    def _monitor_positions(self, now: dt_time) -> None:
        for symbol in SYMBOLS:
            positions = self.positions[symbol]
            if not positions:
                continue

            ltp = self._get_ltp(symbol)
            if ltp <= 0:
                continue

            for pos in positions:
                exit_reason = None
                direction   = pos["direction"]

                if direction == "LONG":
                    if ltp <= pos["sl_price"]:
                        exit_reason = "SL_HIT"
                    elif ltp >= pos["target_price"]:
                        exit_reason = "TARGET_HIT"
                else:
                    if ltp >= pos["sl_price"]:
                        exit_reason = "SL_HIT"
                    elif ltp <= pos["target_price"]:
                        exit_reason = "TARGET_HIT"

                if now >= EOD_EXIT_TIME:
                    exit_reason = "EOD_EXIT"

                if exit_reason:
                    self._close_position(symbol, ltp, exit_reason, now, pos)

    def _close_position(
        self, symbol: str, exit_price: float, reason: str, now: dt_time, pos: dict,
    ) -> None:
        if pos is None:
            return

        # Place closing order
        close_action = "SELL" if pos["direction"] == "LONG" else "BUY"
        self._place_order(symbol, close_action, pos["shares"])

        pnl_per_share = (
            (exit_price - pos["entry_price"])
            if pos["direction"] == "LONG"
            else (pos["entry_price"] - exit_price)
        )
        pnl_gross = round(pnl_per_share * pos["shares"], 2)
        pnl_pct   = round(pnl_per_share / pos["entry_price"] * 100, 3)

        self.logger.log_trade(
            signal_time   = pos["signal_time"],
            symbol        = symbol,
            direction     = pos["direction"],
            entry_price   = pos["entry_price"],
            shares        = pos["shares"],
            sl_price      = pos["sl_price"],
            target_price  = pos["target_price"],
            exit_time     = now.strftime("%H:%M:%S"),
            exit_price    = round(exit_price, 2),
            exit_reason   = reason,
            obi_at_signal = pos["obi_at_signal"],
            pnl_gross     = pnl_gross,
            pnl_pct       = pnl_pct,
        )

        # Remove closed position from list by index
        if pos in self.positions[symbol]:
            self.positions[symbol].remove(pos)

        logger.info(f"[BOT] Trade #{pos['position_id']} closed | Exit: ₹{exit_price:.2f} | Reason: {reason} | PnL: ₹{pnl_gross:+,.2f} ({pnl_pct:+.2f}%)")

        # Check if we still have positions open
        if self.positions[symbol]:
            remaining = len(self.positions[symbol])
            logger.info(f"[BOT] {symbol} has {remaining}/{self._max_positions} positions remaining")
        else:
            logger.info(f"[BOT] {symbol} no longer has any open positions")

        emoji = "✅" if pnl_gross > 0 else "❌"
        msg = (
            f"{emoji} *TRADE CLOSED* {symbol} {pos['direction']}\n"
            f"Exit: ₹{exit_price:.2f} | Reason: {reason}\n"
            f"PnL: ₹{pnl_gross:+,.2f} ({pnl_pct:+.2f}%)"
        )
        logger.info(f"[BOT] {msg}")
        self._tg(msg)

    def _monitor_ghosts(self, now: dt_time) -> None:
        for symbol in SYMBOLS:
            ghost = self.ghosts[symbol]
            if ghost is None:
                continue

            ltp = self._get_ltp(symbol)
            if ltp <= 0:
                continue

            exit_reason = None
            direction   = ghost["direction"]

            if direction == "LONG":
                if ltp <= ghost["sl_price"]:
                    exit_reason = "SL_HIT"
                elif ltp >= ghost["target_price"]:
                    exit_reason = "TARGET_HIT"
            else:
                if ltp >= ghost["sl_price"]:
                    exit_reason = "SL_HIT"
                elif ltp <= ghost["target_price"]:
                    exit_reason = "TARGET_HIT"

            if now >= EOD_EXIT_TIME:
                exit_reason = "EOD_EXIT"

            if exit_reason:
                pnl = (
                    (ltp - ghost["entry_price"]) * ghost["shares"]
                    if direction == "LONG"
                    else (ghost["entry_price"] - ltp) * ghost["shares"]
                )
                self.logger.log_ghost(
                    signal_time          = ghost["signal_time"],
                    symbol               = symbol,
                    direction            = direction,
                    entry_price          = ghost["entry_price"],
                    shares               = ghost["shares"],
                    ghost_exit_time      = now.strftime("%H:%M:%S"),
                    ghost_exit_price     = round(ltp, 2),
                    ghost_exit_reason    = exit_reason,
                    obi_at_signal        = ghost["obi_at_signal"],
                    ghost_pnl_gross      = round(pnl, 2),
                )
                logger.info(
                    f"[GHOST] {symbol} {direction} closed | "
                    f"reason={exit_reason} pnl=₹{pnl:+,.2f}"
                )
                self.ghosts[symbol] = None  # Still only one ghost at a time

    # ── State file ────────────────────────────────────────────────────────────

    def _write_state(self) -> None:
        state = {
            "timestamp":    datetime.now(IST).isoformat(),
            "session_active": self.session_active,
            "paper_mode":   PAPER_MODE,
            "obi_ticks":    self.obi.tick_count,
            "symbols": {
                s: {
                    "positions":   self.positions[s],
                    "ghost":       self.ghosts[s],
                    "last_entry":  self._position_last_time[s],
                    "position_count": len(self.positions[s]),
                }
                for s in SYMBOLS
            },
        }
        with open(self._state_file, "w") as f:
            json.dump(state, f, indent=2, default=str)

    # ── Scheduled jobs ────────────────────────────────────────────────────────

    def on_startup(self) -> None:
        """09:40 — Subscribe OBI for core 4 stocks (leaves slot 5 for NTS+OBI), reset daily state."""
        logger.info(f"[BOT] ===== {STRATEGY_NAME} startup {date.today()} =====")

        # Reset daily state
        for s in SYMBOLS:
            self.positions[s]         = []          # empty list for multiple positions
            self.ghosts[s]            = None
            self._position_last_time[s] = None
            self._ema[s]              = None

        # Start OBI WebSocket for the core 4 stocks only (slot 5 is NTS+OBI's)
        obi_syms = [{"exchange": EXCHANGE, "symbol": s} for s in CORE_OBI_SYMBOLS]
        self.obi.start(obi_syms)

        self.session_active = True
        msg = (
            f"✅ {STRATEGY_NAME} ready | "
            f"Core symbols: {len(CORE_OBI_SYMBOLS)} | "
            f"OBI L={OBI_LONG_THRESHOLD:+.0f} S={OBI_SHORT_THRESHOLD:+.0f} | "
            f"Max concurrent={MAX_TOTAL_POSITIONS} | Cooldown={self._cooldown_minutes}min"
        )
        logger.info(f"[BOT] {msg}")
        self._tg(msg)
        self._write_state()

    def _add_axisbank_tbt(self) -> None:
        """
        13:00 — NTS+OBI is now idle (its slot is free). Add AXISBANK to the OBI engine.
        Called by the scheduler; guarded by session_active so it is a no-op after EOD.
        """
        if not self.session_active:
            return
        # AXISBANK entry window check is inside _evaluate_symbol, but this
        # subscription action is fine to take — no harm if signal check hasn't opened yet.
        if AXISBANK_SYMBOL in self.obi._subscribed:
            logger.debug(f"[BOT] AXISBANK already subscribed — skipping")
            return
        logger.info("[BOT] 13:00 — Adding AXISBANK to OBI engine (NTS+OBI slot now free)")
        self.obi.update_symbols(
            self.obi._subscribed + [{"exchange": EXCHANGE, "symbol": AXISBANK_SYMBOL}]
        )

    def on_signal_check(self) -> None:
        """Every minute: evaluate all symbols in the universe."""
        if not self.session_active:
            return

        now = datetime.now(IST).time()

        # Monitor open positions + ghosts (always, inside session)
        self._monitor_positions(now)
        self._monitor_ghosts(now)

        # Entry evaluations only inside the entry window
        if ENTRY_START <= now <= ENTRY_END:
            for symbol in SYMBOLS:
                if not self.positions[symbol]:
                    # No open position — evaluate for a fresh entry.
                    # _evaluate_symbol() applies its own cooldown and GLOBAL position limit checks.
                    self._evaluate_symbol(symbol)
                else:
                    logger.debug(
                        f"[POSITION_OPEN] {symbol} has an open position — "
                        f"waiting for SL/Target before re-evaluating"
                    )

        self._write_state()

    def on_eod_exit(self) -> None:
        """15:14 — Force-close all open positions and ghosts."""
        if not self.session_active:
            return
        logger.info("[BOT] EOD exit sweep")
        now = EOD_EXIT_TIME
        self._monitor_positions(now)
        self._monitor_ghosts(now)

        # Session summary
        summary = self._build_summary()
        logger.info(f"[BOT] EOD summary: {summary}")
        self._tg(self._format_summary(summary))

        # Stop OBI subscription
        self.obi.stop()
        self.session_active = False
        self._write_state()

    def _build_summary(self) -> dict:
        today = date.today().isoformat()
        trades = []
        ghosts = []
        for fname, target in [("trades.csv", trades), ("ghosts.csv", ghosts)]:
            path = LOG_DIR / fname
            if path.exists():
                import csv as _csv
                with open(path) as f:
                    for row in _csv.DictReader(f):
                        if row.get("date") == today:
                            target.append(row)
        t_pnl  = sum(float(r.get("pnl_gross", 0) or 0) for r in trades)
        g_pnl  = sum(float(r.get("ghost_pnl_gross", 0) or 0) for r in ghosts)
        t_wins = sum(1 for r in trades if float(r.get("pnl_gross", 0) or 0) > 0)
        return {
            "date":          today,
            "trades":        len(trades),
            "trade_wins":    t_wins,
            "trade_pnl":     round(t_pnl, 2),
            "ghosts":        len(ghosts),
            "ghost_pnl":     round(g_pnl, 2),
            "obi_edge":      round(t_pnl - g_pnl, 2),
        }

    def _format_summary(self, s: dict) -> str:
        wr = f"{s['trade_wins']}/{s['trades']}" if s["trades"] else "—"
        return (
            f"📋 *EOD Summary* {s['date']}\n"
            f"Trades: {s['trades']} (WR {wr}) | PnL: ₹{s['trade_pnl']:+,.2f}\n"
            f"Blocked: {s['ghosts']} | Ghost PnL: ₹{s['ghost_pnl']:+,.2f}\n"
            f"OBI Edge: ₹{s['obi_edge']:+,.2f}"
        )

    # ── Run ───────────────────────────────────────────────────────────────────

    def run(self) -> None:
        logger.info(
            f"[BOT] Starting {STRATEGY_NAME} | paper={PAPER_MODE} | "
            f"symbols={SYMBOLS}"
        )

        self._scheduler.add_job(
            self.on_startup, "cron",
            hour=9, minute=40, id="startup",
        )
        self._scheduler.add_job(
            self.on_signal_check, "cron",
            hour="9-15", minute="*", id="signal_check",
        )
        self._scheduler.add_job(
            self.on_eod_exit, "cron",
            hour=15, minute=25, id="eod_exit",
        )

        # 13:00 — NTS+OBI entry window closed; add AXISBANK using the now-free 5th TBT slot
        self._scheduler.add_job(
            self._add_axisbank_tbt, "cron",
            hour=13, minute=0, id="add_axisbank",
        )

        self._scheduler.start()
        logger.info("[BOT] Scheduler started. Waiting for 09:40 startup job.")

        # ── Mid-day catch-up ──────────────────────────────────────────────────
        # If the bot is started after the 09:40 startup cron has already fired
        # today (e.g. after a restart mid-session), call on_startup() immediately
        # so the OBI WebSocket connects and the session becomes active.
        now_ist = datetime.now(IST).time()
        STARTUP_TIME = dt_time(9, 40)
        EOD_TIME     = dt_time(15, 14)
        if STARTUP_TIME <= now_ist <= EOD_TIME and not self.session_active:
            logger.info(
                f"[BOT] Started at {now_ist.strftime('%H:%M')} — past 09:40. "
                f"Triggering on_startup() immediately."
            )
            self.on_startup()

        try:
            while True:
                time.sleep(30)
                self._write_state()
        except (KeyboardInterrupt, SystemExit):
            logger.info("[BOT] Shutdown requested.")
        finally:
            self._scheduler.shutdown(wait=False)
            if self.obi._connected:
                self.obi.stop()
            logger.info("[BOT] Stopped.")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    EquityOBIBot().run()
