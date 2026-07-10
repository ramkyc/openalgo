"""
Nifty Trend Seller + OBI Gate — Paper Trade Bot (Analyze Mode)
=================================================================
Source strategy : research/nifty_trend_seller_study/ — ALL 10 STAGES APPROVED
Champion config : Short-only | ADX>25, RSI<50, ADX-D 7b, SL 2.0×
OOS Sharpe      : +1.735 | WR 66% | MC Stability 99.2%

OBI Gate (new — forward-test only, no historical depth data available):
  At the moment a valid NTS short signal fires, read the weighted OBI of the
  NIFTY ATM CE we intend to sell (depth-50 via OpenAlgo WebSocket).
  OBI < OBI_GATE_THRESHOLD → net selling pressure on CE → TRADE (paper order)
  OBI ≥ OBI_GATE_THRESHOLD → bid pressure on CE → SKIP → ghost-track to EOD

Two parallel streams are logged every session:
  trades.csv   — OBI-approved paper trades (the actual test)
  ghosts.csv   — OBI-blocked signals tracked to EOD (counterfactual)

After 15 sessions compare the two streams to measure OBI gate edge.

Schedule (IST):
  09:40   startup — holiday check, VIX check, resolve ATM CE, start OBI subscription
  10:00   entry window opens; indicator loop begins
  10:00–13:00  every minute: fetch bars, compute NTS signal, apply OBI gate
  13:00   entry window closes (no new positions)
  15:14   EOD force-exit
  15:25   session summary logged + Telegram

Run:
  cd ~/Developer/fyers_crk
  uv run live_trading/nifty_trend_seller_obi/nifty_trend_seller_obi_bot.py
"""

import os
import sys
import time
import logging
import threading
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, date, time as dt_time, timedelta
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

# ── Path setup ────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from live_trading.shared import ta_compat as ta
load_dotenv(PROJECT_ROOT / ".env")

from live_trading.api_utils import is_market_holiday, get_expiry_dates, get_option_symbol, get_multiquotes
from live_trading.shared.atm_resolver import get_atm_strike, resolve_atm_option, get_option_ltp
from live_trading.shared.order_fill import fetch_fill_price
from live_trading.nifty_trend_seller_obi.obi_engine import OBIEngine
from live_trading.nifty_trend_seller_obi.session_logger import SessionLogger

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "nts_obi_bot.log"),
        logging.StreamHandler(),
    ],
)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
STRATEGY_NAME = "NTS_OBI"
API_KEY       = os.getenv("OPENALGO_API_KEY")
HOST          = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
WS_URL        = os.getenv("WEBSOCKET_URL", "ws://127.0.0.1:5001/ws")
TG_TOKEN      = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID")
IST           = pytz.timezone("Asia/Kolkata")


def _resolve_fill(resp: dict | None, fallback: float) -> float:
    """Actual order fill price via OpenAlgo orderstatus, falling back to the
    LTP snapshot quoted before the order was placed if the lookup fails."""
    order_id = resp.get("orderid") if isinstance(resp, dict) else None
    if not order_id or order_id == "PAPER":
        return fallback
    fill = fetch_fill_price(order_id, STRATEGY_NAME)
    return fill if fill is not None else fallback

# ── Champion strategy parameters (nifty_trend_seller_study/results_summary.md) ─
ADX_THRESH       = 25          # ADX(14) must exceed this
RSI_THRESH       = 50          # RSI(14) must be < this for short (sell CE)
ADX_RISING_BARS  = 7           # ADX now > ADX N bars ago (trend accelerating)
SL_MULT          = 2.0         # SL fires when premium rises to entry × SL_MULT
EMA_LEN          = 20
ADX_LEN          = 14
MACD_FAST        = 5
MACD_SLOW        = 13
MACD_SIG         = 3
VIX_MAX          = 22.0        # Skip day if India VIX > this at open
LOTS             = 10          # Standard position size per CLAUDE.md
NIFTY_LOT_SIZE   = 65              # updated Dec 2025 revision
NIFTY_STEP       = 50          # ATM strike rounding step
MIN_PREMIUM      = 50.0        # Don't enter if option premium < ₹50 (too OTM)

# ── OBI Gate ──────────────────────────────────────────────────────────────────
# Weighted OBI of NIFTY ATM CE must be BELOW this to proceed.
#
# Conservative start: OBI_GATE_THRESHOLD = 0.0
#   → Any net selling pressure on the CE side confirms our short sell.
#   → This will block ~40-50% of signals (pure market-neutral book).
#
# Tighten after 15 days if OBI edge is confirmed: try -10, -20.
OBI_GATE_THRESHOLD = 0.0

# If depth data is stale (no update in this many seconds), skip OBI gate
# and LOG the trade as "obi_stale=True" for post-session review.
OBI_STALE_SECONDS  = 10.0

# ATM CE strike re-resolution trigger: if spot moves > this many points from
# the morning ATM, re-resolve and re-subscribe OBI to the new ATM strike.
STRIKE_DRIFT_TRIGGER = 100   # pts (2 strike steps)

# ── Timing ────────────────────────────────────────────────────────────────────
ENTRY_START = dt_time(10,  0)
ENTRY_END   = dt_time(13, 0)
EOD_EXIT    = dt_time(15, 14)
EOD_LOG     = dt_time(15, 25)

# ── Transaction costs (from research/transaction_costs.py) ───────────────────
# Approximate per-lot for NIFTY ATM options (premium ~₹150)
# Exact: use compute_round_trip_cost() — imported below when available
APPROX_COST_PER_LOT = 65.0   # ₹65/lot round-trip (entry + exit)


# ── Bot ───────────────────────────────────────────────────────────────────────

class NTSOBIBot:
    """
    Main bot class.
    Dual-thread design:
      Main thread     → APScheduler (signal detection, position management)
      OBI engine thread → OpenAlgo WebSocket SDK (depth updates, updates _cache)
    State shared between threads is protected by threading.Lock() inside OBIEngine.
    """

    def __init__(self):
        self.obi      = OBIEngine(API_KEY, HOST, WS_URL, use_depth_50=True)
        self.logger   = SessionLogger(LOG_DIR)
        self._sched   = BlockingScheduler(timezone=IST)

        # Session state (reset each day by on_startup)
        self.position: dict | None = None     # active paper trade
        self.ghost: dict | None    = None     # OBI-blocked ghost trade
        self.session_active        = False
        self.trade_taken_today     = False
        self.vix_ok                = True
        self.atm_info: dict | None = None     # {symbol, strike, expiry, exchange}
        self.morning_spot          = 0.0      # spot at 09:40 for drift detection
        self._last_indicators      = {}       # last computed NTS indicator values

        # Restore any active trade from today's state file (mid-session restart)
        self._restore_state()

    # ──────────────────────────────────────────────────────────────────────────
    # State restore (mid-session restart recovery)
    # ──────────────────────────────────────────────────────────────────────────

    def _restore_state(self) -> None:
        """
        Reload today's active trade from nts_obi_state.json on mid-session restart.
        on_startup() at 09:40 resets all state fresh — this only activates for
        restarts that happen AFTER 09:40 when on_startup has already fired.
        trade_taken_today=True prevents re-entry after the restore.
        """
        state_path = LOG_DIR / "nts_obi_state.json"
        if not state_path.exists():
            return
        try:
            import json as _json
            state = _json.loads(state_path.read_text())
            last_update = datetime.fromisoformat(state.get("last_update", ""))
            if last_update.date() != date.today():
                return   # stale — yesterday's state, ignore
            at = state.get("active_trade")
            if at:
                self.position = at
                self.trade_taken_today = True
                logger.info(
                    f"[_restore_state] Restored active trade: {at.get('symbol')}  "
                    f"entry=₹{at.get('entry_premium')}  sl=₹{at.get('sl_price')}"
                )
            elif state.get("trade_taken_today"):
                # No open position but trade was already taken — mark as done to prevent re-entry
                self.trade_taken_today = True
                logger.info("[_restore_state] No open position but trade_taken_today=True — re-entry blocked.")
        except Exception as e:
            logger.warning(f"[_restore_state] Could not read state file: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # API helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _post(self, endpoint: str, payload: dict) -> dict:
        try:
            r = requests.post(
                f"{HOST}/api/v1/{endpoint}",
                json={"apikey": API_KEY, **payload},
                timeout=60,  # sandbox LTP fetch can take up to ~3s; use same timeout as equity_obi
            )
            if r.status_code == 200:
                return r.json()
            # Log the actual error body so non-200 failures are visible (not just "→ {}")
            try:
                err_body = r.json()
            except Exception:
                err_body = r.text[:500]
            logger.error(f"[HTTP] POST {endpoint} → HTTP {r.status_code}: {err_body}")
        except Exception as e:
            logger.error(f"API error [{endpoint}]: {e}")
        return {}

    def _tg(self, msg: str) -> None:
        """Send a Telegram notification (fire-and-forget, non-blocking)."""
        if not TG_TOKEN or not TG_CHAT_ID:
            return
        def _send():
            try:
                requests.post(
                    f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                    json={
                        "chat_id": TG_CHAT_ID,
                        "text": f"🎯 *NTS+OBI*\n{msg}",
                        "parse_mode": "Markdown",
                    },
                    timeout=5,
                )
            except Exception:
                pass
        threading.Thread(target=_send, daemon=True).start()

    def _get_nifty_spot(self) -> float:
        # Use centralized bulk fetcher if possible, otherwise this is a fallback
        return self._fetch_bulk_quotes().get("NIFTY", 0.0)

    def _get_vix(self) -> float:
        return self._fetch_bulk_quotes().get("INDIAVIX", 0.0)

    def _get_option_ltp(self, symbol: str, exchange: str = "NFO") -> float:
        return self._fetch_bulk_quotes().get(symbol, 0.0)

    def _fetch_bulk_quotes(self) -> dict:
        """
        Fetch NIFTY, VIX, and ATM Option LTP in a single bulk request.
        Returns a dict mapping symbol -> ltp.
        """
        symbols = [
            {"symbol": "NIFTY", "exchange": "NSE_INDEX"},
            {"symbol": "INDIAVIX", "exchange": "NSE_INDEX"},
        ]
        if self.atm_info:
            symbols.append({"symbol": self.atm_info["symbol"], "exchange": self.atm_info["exchange"]})
        
        results = get_multiquotes(API_KEY, symbols)
        ltp_map = {}
        for r in results:
            sym = r.get("symbol")
            data = r.get("data", {})
            if sym and data:
                ltp_map[sym] = float(data.get("ltp", 0))
        return ltp_map

    def _fetch_nifty_1min(self, days_back: int = 3) -> pd.DataFrame:
        """
        Fetch NIFTY 1-min bars via OpenAlgo history API.
        We look back 'days_back' calendar days to ensure enough bars even after
        weekends/holidays. Returns a DataFrame with columns:
          open, high, low, close, volume  — indexed by IST datetime.
        """
        end_dt   = datetime.now()
        start_dt = end_dt - timedelta(days=days_back)
        data = self._post("history", {
            "symbol":     "NIFTY",
            "exchange":   "NSE_INDEX",
            "interval":   "1m",
            "start_date": start_dt.strftime("%Y-%m-%d"),
            "end_date":   end_dt.strftime("%Y-%m-%d"),
            "source":     "broker",
        })
        if not data:
            logger.warning("[BOT] History API returned empty/false response (network error?)")
            return pd.DataFrame()
        if data.get("status") != "success":
            logger.warning(f"[BOT] History API status={data.get('status')}: {data.get('message', '')}")
            return pd.DataFrame()
        if not data.get("data"):
            logger.warning(f"[BOT] History API returned no data (empty list/None)")
            return pd.DataFrame()

        df = pd.DataFrame(data["data"])

        # Normalise timestamp column (Unix seconds or ISO string)
        if "timestamp" in df.columns:
            df["datetime"] = (
                pd.to_datetime(df["timestamp"], unit="s", utc=True)
                .dt.tz_convert("Asia/Kolkata")
                .dt.tz_localize(None)
            )
        elif "date" in df.columns:
            df["datetime"] = pd.to_datetime(df["date"])
        else:
            logger.error("[BOT] Cannot find timestamp column in history data")
            return pd.DataFrame()

        df = df.set_index("datetime").sort_index()

        # Keep only today's bars (intraday)
        today_str = date.today().strftime("%Y-%m-%d")
        before_filter = len(df)
        df = df[df.index.strftime("%Y-%m-%d") == today_str]
        logger.info(f"[BOT] History raw={before_filter} rows → filtered to today({today_str})={len(df)} rows")

        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df[["open", "high", "low", "close"]].dropna()

    # ──────────────────────────────────────────────────────────────────────────
    # Indicator computation
    # ──────────────────────────────────────────────────────────────────────────

    def _compute_short_signal(self, df: pd.DataFrame) -> dict | None:
        """
        Apply all 5 NTS conditions to the closed bar (df.iloc[-1]).
        Returns a dict of indicator values if a SHORT signal fires, else None.

        Conditions (short = sell CE):
          1. Close < EMA(20)                     [bearish trend]
          2. ADX(14) > ADX_THRESH (25)           [strong trend]
          3. ADX[now] > ADX[ADX_RISING_BARS ago] [trend accelerating]
          4. RSI(14) < RSI_THRESH (50)           [downward momentum]
          5. MACD(5,13,3) line crosses BELOW signal line on this bar [entry trigger]
        """
        MIN_BARS = max(ADX_LEN * 2, MACD_SLOW * 2, ADX_RISING_BARS + 5, 60)
        if len(df) < MIN_BARS:
            logger.debug(f"[SIG] Insufficient bars ({len(df)} < {MIN_BARS})")
            return None

        # ── Indicators via pandas-ta ──────────────────────────────────────────
        df = df.copy()
        df["ema"]  = ta.ema(df["close"], length=EMA_LEN)

        adx_df     = ta.adx(df["high"], df["low"], df["close"], length=ADX_LEN)
        df["adx"]  = adx_df[f"ADX_{ADX_LEN}"]

        df["rsi"]  = ta.rsi(df["close"], length=14)

        macd_df    = ta.macd(df["close"], fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)
        df["macd"] = macd_df[f"MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"]
        df["macds"]= macd_df[f"MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"]

        df = df.dropna()
        if len(df) < ADX_RISING_BARS + 2:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]

        # ── Evaluate conditions ───────────────────────────────────────────────
        adx_n_bars_ago = df.iloc[-(ADX_RISING_BARS + 1)]["adx"]

        c1_trend    = last["close"] < last["ema"]
        c2_strength = last["adx"]   > ADX_THRESH
        c3_accel    = last["adx"]   > adx_n_bars_ago
        c4_rsi      = last["rsi"]   < RSI_THRESH
        c5_macd_x   = (prev["macd"] >= prev["macds"]) and (last["macd"] < last["macds"])

        all_pass = all([c1_trend, c2_strength, c3_accel, c4_rsi, c5_macd_x])

        if not all_pass:
            logger.info(
                f"[SIG] NO SIGNAL - Conditions: "
                f"Trend(close<EMA)={c1_trend}, "
                f"ADX>{ADX_THRESH}={c2_strength}({last['adx']:.1f}), "
                f"ADX_rising={c3_accel}, "
                f"RSI<{RSI_THRESH}={c4_rsi}({last['rsi']:.1f}), "
                f"MACD_cross={c5_macd_x}"
            )
            return None

        return {
            "adx":       round(float(last["adx"]), 2),
            "rsi_val":   round(float(last["rsi"]), 2),
            "macd_cross": True,
            "bar_time":  df.index[-1].strftime("%H:%M"),
        }

    # ──────────────────────────────────────────────────────────────────────────
    # OBI gate
    # ──────────────────────────────────────────────────────────────────────────

    def _check_obi_gate(self) -> tuple[bool, dict]:
        """
        Read the current OBI snapshot for the subscribed ATM CE.
        Returns (gate_passes: bool, obi_info: dict).

        gate_passes = True  → OBI < OBI_GATE_THRESHOLD → TRADE
        gate_passes = False → OBI ≥ threshold or stale  → SKIP
        """
        if self.atm_info is None:
            logger.warning("[OBI] No ATM info resolved — OBI gate cannot check")
            return False, {"obi_at_signal": None, "obi_stale": True}

        symbol = self.atm_info["symbol"]
        snap   = self.obi.get_snapshot(symbol)

        if snap is None:
            logger.warning(f"[OBI] No depth snapshot yet for {symbol} — treating as stale")
            return False, {
                "obi_at_signal": None, "obi_raw_at_signal": None,
                "obi_bid_tot": None, "obi_ask_tot": None,
                "obi_n_levels": 0, "obi_stale": True,
            }

        stale = self.obi.is_stale(symbol, max_age_seconds=OBI_STALE_SECONDS)

        # n_levels guard — need at least 3 levels to compute a meaningful OBI.
        # When TBT (50-level) is working we get n_levels=50; when TBT falls
        # back to HSM 5-level depth we get n_levels=5.  Both are valid OBI
        # inputs.  The time-based is_stale() check handles truly dead feeds.
        n_levels = snap.get("n_levels", 0)
        if n_levels < 3:
            logger.warning(
                f"[OBI] Only {n_levels} depth levels for {symbol} — "
                f"treating as stale (need ≥3)"
            )
            return False, {
                "obi_at_signal": snap["obi"], "obi_raw_at_signal": snap.get("raw_obi", 0),
                "obi_bid_tot": snap.get("bid_tot", 0), "obi_ask_tot": snap.get("ask_tot", 0),
                "obi_n_levels": n_levels, "obi_stale": True,
                "obi_gate_pass": False, "obi_threshold": OBI_GATE_THRESHOLD,
            }

        obi_val = snap["obi"]

        # Fix 1: Rolling gate — require the mean of the last 3 OBI readings to
        # be below threshold, not just a single tick.  Suppresses noise spikes.
        rolling_obi = self.obi.get_rolling_obi(symbol, n=3)
        if rolling_obi is None:
            logger.info(
                f"[OBI] Insufficient OBI history for {symbol} — "
                f"waiting for 3 ticks (rolling gate not ready)"
            )
            return False, {
                "obi_at_signal": round(obi_val, 2), "obi_raw_at_signal": round(snap.get("raw_obi", 0), 2),
                "obi_bid_tot": snap.get("bid_tot", 0), "obi_ask_tot": snap.get("ask_tot", 0),
                "obi_n_levels": n_levels, "obi_stale": stale,
                "obi_gate_pass": False, "obi_threshold": OBI_GATE_THRESHOLD,
            }

        gate_passes = (rolling_obi < OBI_GATE_THRESHOLD) and (not stale)

        info = {
            "obi_at_signal":     round(obi_val, 2),
            "obi_raw_at_signal": round(snap.get("raw_obi", 0), 2),
            "obi_bid_tot":       snap.get("bid_tot", 0),
            "obi_ask_tot":       snap.get("ask_tot", 0),
            "obi_n_levels":      n_levels,
            "obi_stale":         stale,
            "obi_gate_pass":     gate_passes,
            "obi_threshold":     OBI_GATE_THRESHOLD,
        }

        logger.info(
            f"[OBI] Gate check | symbol={symbol} OBI={obi_val:+.1f} "
            f"rolling(3)={rolling_obi:+.1f} n_levels={n_levels} stale={stale} "
            f"→ {'PASS ✅' if gate_passes else 'BLOCK 🚫'}"
        )
        return gate_passes, info

    # ──────────────────────────────────────────────────────────────────────────
    # Position management helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _place_paper_sell(self, symbol: str, exchange: str, qty: int) -> dict:
        """
        Place a paper SELL order via OpenAlgo Analyze mode.
        Returns the raw response dict (status/orderid) for fill resolution.
        """
        data = self._post("placeorder", {
            "strategy":   STRATEGY_NAME,
            "symbol":     symbol,
            "exchange":   exchange,
            "action":     "SELL",
            "product":    "MIS",
            "pricetype":  "MARKET",
            "quantity":   qty,
            "price":      0,
            "trigger_price": 0,
            "disclosed_quantity": 0,
        })
        logger.info(f"[ORDER] SELL {symbol} qty={qty} → {data}")
        return data

    def _place_paper_buy(self, symbol: str, exchange: str, qty: int) -> dict:
        """Close a paper short (buy-to-close). Returns the raw response dict."""
        data = self._post("placeorder", {
            "strategy":   STRATEGY_NAME,
            "symbol":     symbol,
            "exchange":   exchange,
            "action":     "BUY",
            "product":    "MIS",
            "pricetype":  "MARKET",
            "quantity":   qty,
            "price":      0,
            "trigger_price": 0,
            "disclosed_quantity": 0,
        })
        logger.info(f"[ORDER] BUY (close) {symbol} qty={qty} → {data}")
        return data

    def _compute_pnl(self, entry: float, exit_: float, lots: int) -> dict:
        """Compute gross and net PnL for a completed trade."""
        pnl_per_lot   = (entry - exit_) * NIFTY_LOT_SIZE   # sell high, buy low
        pnl_gross     = pnl_per_lot * lots
        cost_total    = APPROX_COST_PER_LOT * lots
        pnl_net       = pnl_gross - cost_total
        return {
            "pnl_gross_per_lot":    round(pnl_per_lot, 2),
            "pnl_gross_total":      round(pnl_gross, 2),
            "transaction_cost_total": round(cost_total, 2),
            "pnl_net_total":        round(pnl_net, 2),
        }

    # ──────────────────────────────────────────────────────────────────────────
    # ATM strike management
    # ──────────────────────────────────────────────────────────────────────────

    def _resolve_and_subscribe(self, spot: float) -> bool:
        """
        Resolve NIFTY ATM CE for the current spot price.
        Updates self.atm_info and re-subscribes OBI engine.
        Returns True on success.
        """
        atm_info = resolve_atm_option(spot, opt_type="CE", api_key=API_KEY, min_dte=2)
        if not atm_info:
            logger.error(f"[ATM] Could not resolve ATM CE for spot={spot}")
            return False

        self.atm_info = atm_info
        symbol = atm_info["symbol"]
        exchange = atm_info["exchange"]

        # Subscribe OBI engine to this CE
        self.obi.update_symbols([{"exchange": exchange, "symbol": symbol}])
        logger.info(f"[ATM] Resolved + subscribed: {symbol} (spot={spot})")
        return True

    def _maybe_reroll_atm(self) -> None:
        """
        If spot has drifted > STRIKE_DRIFT_TRIGGER pts from morning ATM,
        re-resolve the ATM CE and re-subscribe OBI — but only if no active trade.
        """
        if self.position is not None or self.atm_info is None:
            return
        spot = self._get_nifty_spot()
        if spot <= 0:
            return
        drift = abs(spot - self.atm_info["strike"])
        if drift >= STRIKE_DRIFT_TRIGGER:
            logger.info(f"[ATM] Spot drift {drift} pts → re-resolving ATM CE")
            self._resolve_and_subscribe(spot)

    # ──────────────────────────────────────────────────────────────────────────
    # Scheduled jobs
    # ──────────────────────────────────────────────────────────────────────────

    def on_startup(self) -> None:
        """
        09:40 — Pre-market setup.
        - Holiday check
        - VIX check
        - Resolve ATM CE
        - Start OBI WebSocket subscription
        """
        today_str = date.today().isoformat()
        logger.info(f"[BOT] ===== {STRATEGY_NAME} startup {today_str} =====")

        # Holiday check
        if is_market_holiday(API_KEY):
            logger.info("[BOT] Market holiday — bot idle today.")
            self.session_active = False
            return

        # VIX check
        vix = self._get_vix()
        if vix > VIX_MAX:
            msg = f"VIX={vix:.1f} > {VIX_MAX} — SKIPPING today (high volatility filter)"
            logger.warning(f"[BOT] {msg}")
            self._tg(f"⚠️ {msg}")
            self.vix_ok = False
            self.session_active = False
            return

        self.vix_ok = True
        logger.info(f"[BOT] VIX={vix:.1f} OK (≤{VIX_MAX})")

        # Reset daily state
        self.position          = None
        self.ghost             = None
        self.trade_taken_today = False

        # Resolve ATM CE and start OBI subscription
        spot = self._get_nifty_spot()
        if spot <= 0:
            logger.error("[BOT] Could not fetch NIFTY spot — bot idle today.")
            self.session_active = False
            return

        self.morning_spot = spot
        ok = self._resolve_and_subscribe(spot)
        if not ok:
            logger.error("[BOT] ATM resolve failed — bot idle today.")
            self.session_active = False
            return

        self.session_active = True
        atm_sym = self.atm_info["symbol"]
        msg = (
            f"✅ Session ready | VIX={vix:.1f} | Spot={spot:.0f}\n"
            f"ATM CE: {atm_sym} | OBI gate: {OBI_GATE_THRESHOLD}"
        )
        logger.info(f"[BOT] {msg}")
        self._tg(msg)
        self._write_state()

        # Give OBI engine 30s to populate cache before entry window opens
        logger.info("[OBI] Warming up depth subscription (30s)...")

    def on_signal_check(self) -> None:
        """
        Every minute 10:00 – 13:00.
        Fetch 1-min bars → compute NTS indicators → check 5 conditions → OBI gate.
        """
        now_ist = datetime.now(IST).strftime("%H:%M:%S")
        logger.info(f"[DEBUG] on_signal_check fired at {now_ist}, session_active={self.session_active}, vix_ok={self.vix_ok}")
        if not self.session_active or not self.vix_ok:
            logger.info("[DEBUG] session inactive or vix not ok - returning")
            return

        # Fix 3: Hard warmup gate — OBI engine must have received at least 10
        # depth ticks before any signal can be evaluated.  The 09:40 startup
        # fires the subscription; the first signal check is at 10:00, which
        # is typically ~90–120 ticks in.  This guard defends against edge cases
        # where the WebSocket reconnects late and the cache is still cold.
        # Lowered from 50 to 25 to 10 on 2026-05-11 to survive frequent restarts
        # and ensure signals can fire within the entry window.
        if self.obi.tick_count < 10:
            logger.info(
                f"[OBI] Warmup in progress ({self.obi.tick_count}/10 ticks) — "
                f"skipping signal check"
            )
            self._write_state()
            return

        logger.info(f"[DEBUG] warmup passed, tick_count={self.obi.tick_count}")

        now = datetime.now(IST).time()

        # ── Position monitoring (runs even during entry window) ───────────────
        if self.position is not None:
            self._monitor_position(now)
            self._maybe_reroll_atm()  # only acts if no active position
            self._write_state()
            return

        # ── Ghost tracking ─────────────────────────────────────────────────────
        if self.ghost is not None:
            self._monitor_ghost(now)
            self._write_state()
            return

        # ── Entry window guard ────────────────────────────────────────────────
        logger.info(f"[DEBUG] entry window check: now={now}, start={ENTRY_START}, end={ENTRY_END}")
        if not (ENTRY_START <= now <= ENTRY_END):
            logger.info("[DEBUG] outside entry window - returning")
            self._write_state()    # keep dashboard live even outside entry window
            return
        if self.trade_taken_today:
            logger.info("[DEBUG] trade taken today - returning")
            self._write_state()
            return

        # ── ATM drift check ────────────────────────────────────────────────────
        self._maybe_reroll_atm()
        logger.info("[DEBUG] after ATM drift check")

        # ── Fetch bars ────────────────────────────────────────────────────────
        logger.info("[DEBUG] fetching nifty 1min bars...")
        df = self._fetch_nifty_1min()
        if df.empty or len(df) < 60:
            logger.info(f"[SIG] Insufficient bars ({len(df)})")
            self._write_state()
            return
        logger.info(f"[DEBUG] got {len(df)} bars")

        # Only evaluate completed (closed) bars → drop the still-forming last bar
        df = df.iloc[:-1]

        # ── Compute signal ────────────────────────────────────────────────────
        logger.info("[DEBUG] computing signal...")

        # Compute indicators for state file
        df_temp = df.copy()
        df_temp["ema"] = ta.ema(df_temp["close"], length=20)
        adx_df = ta.adx(df_temp["high"], df_temp["low"], df_temp["close"], length=14)
        df_temp["adx"] = adx_df["ADX_14"]
        df_temp["rsi"] = ta.rsi(df_temp["close"], length=14)
        macd_df = ta.macd(df_temp["close"], fast=5, slow=13, signal=3)
        df_temp["macd"] = macd_df["MACD_5_13_3"]
        df_temp["macds"] = macd_df["MACDs_5_13_3"]
        df_temp = df_temp.dropna()

        # Store for state
        self._last_indicators = {}
        if len(df_temp) > 0:
            last = df_temp.iloc[-1]
            self._last_indicators = {
                "close": round(last["close"], 2),
                "ema20": round(last["ema"], 2),
                "adx": round(last["adx"], 2),
                "rsi": round(last["rsi"], 2),
                "macd": round(last["macd"], 4),
                "macds": round(last["macds"], 4),
            }

        sig = self._compute_short_signal(df)
        if sig is None:
            logger.info("[DEBUG] _compute_short_signal returned None - no signal")
            # Update state even when no signal
            self._write_state()
            return
        logger.info(f"[DEBUG] SIGNAL DETECTED! {sig}")

        spot   = float(df["close"].iloc[-1])
        symbol = self.atm_info["symbol"]
        strike = self.atm_info["strike"]

        # Fetch option premium at signal time
        premium = self._get_option_ltp(symbol, self.atm_info["exchange"])
        if premium < MIN_PREMIUM:
            logger.info(f"[SIG] Signal fired but premium ₹{premium:.0f} < ₹{MIN_PREMIUM} — skip")
            self._write_state()
            return

        # ── OBI gate ──────────────────────────────────────────────────────────
        gate_pass, obi_info = self._check_obi_gate()

        # Log the signal event regardless of gate outcome
        self.logger.log_signal(
            signal_time=sig["bar_time"],
            direction="SHORT",
            nifty_spot=round(spot, 2),
            atm_strike=strike,
            option_symbol=symbol,
            adx=sig["adx"],
            rsi_val=sig["rsi_val"],
            macd_cross=sig["macd_cross"],
            entry_premium=premium,
            **obi_info,
        )

        if gate_pass:
            self._enter_trade(symbol, premium, sig, obi_info, spot, strike)
        else:
            self._enter_ghost(symbol, premium, sig, obi_info, spot, strike)

        self._write_state()

    def _enter_trade(
        self, symbol: str, premium: float,
        sig: dict, obi_info: dict, spot: float, strike: int,
    ) -> None:
        """OBI gate passed — place paper SELL order."""
        qty = LOTS * NIFTY_LOT_SIZE
        res = self._place_paper_sell(symbol, self.atm_info["exchange"], qty)
        if res.get("status") != "success":
            logger.error("[BOT] Paper order placement failed — skipping trade")
            return

        # Resolve actual fill price; recompute the SL threshold from it
        # (not the raw LTP snapshot quoted before the order was placed).
        fill_premium = _resolve_fill(res, premium)
        sl_price = round(fill_premium * SL_MULT, 2)
        self.position = {
            "symbol":        symbol,
            "exchange":      self.atm_info["exchange"],
            "entry_premium": fill_premium,
            "entry_prem":    fill_premium,   # dashboard field-name alias
            "sl_price":      sl_price,
            "sl_prem":       sl_price,       # dashboard field-name alias
            "lots":          LOTS,
            "qty":           qty,
            "signal_time":   sig["bar_time"],
            "entry_time":    datetime.now().isoformat(),  # dashboard "Since" column
            "strike":        strike,
            "nifty_spot":    spot,
            "obi_at_signal": obi_info.get("obi_at_signal"),
        }
        self.trade_taken_today = True

        msg = (
            f"📥 *PAPER TRADE ENTERED* (OBI ✅)\n"
            f"Symbol: `{symbol}`\n"
            f"Entry: ₹{fill_premium:.2f} | SL: ₹{sl_price:.2f}\n"
            f"OBI: {obi_info.get('obi_at_signal'):+.1f} | Lots: {LOTS}"
        )
        logger.info(f"[BOT] {msg}")
        self._tg(msg)

    def _enter_ghost(
        self, symbol: str, premium: float,
        sig: dict, obi_info: dict, spot: float, strike: int,
    ) -> None:
        """OBI gate blocked — start ghost tracking."""
        if self.ghost is not None:
            return   # already ghost-tracking another signal today
        sl_price = round(premium * SL_MULT, 2)
        self.ghost = {
            "symbol":        symbol,
            "exchange":      self.atm_info["exchange"],
            "entry_premium": premium,
            "sl_price":      sl_price,
            "signal_time":   sig["bar_time"],
            "strike":        strike,
            "lots":          LOTS,
            "obi_at_signal": obi_info.get("obi_at_signal"),
        }
        logger.info(
            f"[GHOST] OBI blocked signal — ghost tracking {symbol} "
            f"entry=₹{premium:.2f} SL=₹{sl_price:.2f}"
        )

    def _monitor_position(self, now: dt_time) -> None:
        """
        Check SL and EOD exit for the active paper trade.
        Called every minute from on_signal_check.
        """
        pos    = self.position
        symbol = pos["symbol"]
        ltp    = self._get_option_ltp(symbol, pos["exchange"])
        if ltp <= 0:
            logger.warning(f"[MON] Could not fetch LTP for {symbol}")
            return

        exit_reason = None
        if ltp >= pos["sl_price"]:
            exit_reason = "SL_HIT"
        elif now >= EOD_EXIT:
            exit_reason = "EOD_EXIT"

        if exit_reason:
            self._close_position(ltp, exit_reason)

    def _close_position(self, exit_premium: float, reason: str) -> None:
        pos = self.position
        if pos is None:
            return

        qty = pos["qty"]
        res = self._place_paper_buy(pos["symbol"], pos["exchange"], qty)
        if res.get("status") != "success":
            # Position may already be closed by sandbox auto-squareoff (15:15 MIS cutoff).
            # Do NOT return early — always log the trade so the CSV stays accurate.
            logger.warning(
                f"[ORDER] Close order non-success (likely auto-squareoff already "
                f"closed position): {res}  — logging trade with LTP fallback."
            )

        # Resolve actual fill price for the close order (falls back to the
        # LTP snapshot that triggered this exit if the lookup fails).
        exit_fill = _resolve_fill(res, exit_premium)

        pnl = self._compute_pnl(pos["entry_premium"], exit_fill, pos["lots"])

        self.logger.log_trade(
            signal_time=pos["signal_time"],
            direction="SHORT",
            nifty_spot=pos["nifty_spot"],
            atm_strike=pos["strike"],
            option_symbol=pos["symbol"],
            entry_premium=pos["entry_premium"],
            lots=pos["lots"],
            sl_price=pos["sl_price"],
            exit_time=datetime.now().strftime("%H:%M"),
            exit_premium=round(exit_fill, 2),
            exit_reason=reason,
            obi_at_signal=pos["obi_at_signal"],
            **pnl,
        )

        self.position = None
        self._write_state()

        emoji = "✅" if pnl["pnl_net_total"] > 0 else "❌"
        msg = (
            f"{emoji} *TRADE CLOSED* | {reason}\n"
            f"Symbol: `{pos['symbol']}`\n"
            f"Entry: ₹{pos['entry_premium']:.2f} → Exit: ₹{exit_fill:.2f}\n"
            f"Net P&L: ₹{pnl['pnl_net_total']:,.0f} | Gross: ₹{pnl['pnl_gross_total']:,.0f}"
        )
        logger.info(f"[BOT] {msg}")
        self._tg(msg)

    def _monitor_ghost(self, now: dt_time) -> None:
        """Track ghost (OBI-blocked) signal to EOD for counterfactual PnL."""
        g   = self.ghost
        ltp = self._get_option_ltp(g["symbol"], g["exchange"])
        if ltp <= 0:
            return

        ghost_exit_reason = None
        if ltp >= g["sl_price"]:
            ghost_exit_reason = "GHOST_SL_HIT"
        elif now >= EOD_EXIT:
            ghost_exit_reason = "GHOST_EOD"

        if ghost_exit_reason:
            pnl_per_lot   = (g["entry_premium"] - ltp) * NIFTY_LOT_SIZE
            pnl_gross     = pnl_per_lot * g["lots"]
            self.logger.log_ghost(
                signal_time=g["signal_time"],
                atm_strike=g["strike"],
                option_symbol=g["symbol"],
                entry_premium=g["entry_premium"],
                ghost_exit_time=datetime.now().strftime("%H:%M"),
                ghost_exit_premium=round(ltp, 2),
                ghost_exit_reason=ghost_exit_reason,
                obi_at_signal=g["obi_at_signal"],
                lots=g["lots"],
                ghost_pnl_gross_per_lot=round(pnl_per_lot, 2),
                ghost_pnl_gross_total=round(pnl_gross, 2),
            )
            logger.info(
                f"[GHOST] Closed | {ghost_exit_reason} | "
                f"entry=₹{g['entry_premium']:.2f} exit=₹{ltp:.2f} "
                f"pnl=₹{pnl_gross:,.0f}"
            )
            self.ghost = None

    # ──────────────────────────────────────────────────────────────────────────
    # State file writer (for Streamlit dashboard)
    # ──────────────────────────────────────────────────────────────────────────

    def _write_state(self) -> None:
        """
        Write a JSON snapshot to logs/nts_obi_state.json.
        Called every minute from on_signal_check and on key events.
        The Streamlit dashboard reads this file for live display.
        """
        import json as _json
        snap = self.obi.get_snapshot(self.atm_info["symbol"]) if self.atm_info else None
        total_sigs, blocked_sigs = 0, 0
        try:
            import csv as _csv
            today = date.today().isoformat()
            signals_path = LOG_DIR / "signals.csv"
            if signals_path.exists():
                with open(signals_path) as f:
                    for row in _csv.DictReader(f):
                        if row.get("date") == today:
                            total_sigs += 1
                            if row.get("obi_gate_pass") in ("False", "false", "0", ""):
                                blocked_sigs += 1
        except Exception:
            pass

        state = {
            "last_update":         datetime.now().isoformat(),
            "session_active":      self.session_active,
            "vix_ok":              self.vix_ok,
            "trade_taken_today":   self.trade_taken_today,
            "nifty_spot":          round(self.morning_spot, 2),
            "atm_symbol":          self.atm_info["symbol"] if self.atm_info else None,
            "atm_strike":          self.atm_info["strike"]  if self.atm_info else None,
            "obi_current":         round(snap["obi"], 2)      if snap else None,
            "obi_raw_current":     round(snap["raw_obi"], 2)  if snap else None,
            "obi_n_levels":        snap["n_levels"]            if snap else 0,
            "obi_ticks_today":     self.obi.tick_count,
            "obi_threshold":       OBI_GATE_THRESHOLD,
            "signals_today":       total_sigs,
            "signals_blocked":     blocked_sigs,
            "active_trade":        self.position,
            "ghost_trade":         self.ghost,
            "entry_window":        f"{ENTRY_START.strftime('%H:%M')}–{ENTRY_END.strftime('%H:%M')}",
            "vix_threshold":       VIX_MAX,
            "params": {
                "adx_thresh":      ADX_THRESH,
                "rsi_thresh":      RSI_THRESH,
                "adx_rising_bars": ADX_RISING_BARS,
                "sl_mult":         SL_MULT,
                "lots":            LOTS,
            },
            "indicators": self._last_indicators,  # current indicator values for dashboard
        }
        try:
            (LOG_DIR / "nts_obi_state.json").write_text(
                _json.dumps(state, default=str, indent=2)
            )
        except Exception as e:
            logger.debug(f"[STATE] Write error: {e}")

    def on_eod_exit(self) -> None:
        """
        15:14 — Force-close any open position.
        (Redundant safety net; primary close is in on_signal_check monitor loop.)
        """
        if self.position is not None:
            logger.info("[EOD] Force-closing open position...")
            symbol = self.position["symbol"]
            ltp    = self._get_option_ltp(symbol, self.position["exchange"])
            if ltp <= 0:
                ltp = self.position["entry_premium"]   # fallback
            self._close_position(ltp, "EOD_EXIT")

        if self.ghost is not None:
            g   = self.ghost
            ltp = self._get_option_ltp(g["symbol"], g["exchange"])
            if ltp > 0:
                self._monitor_ghost(EOD_EXIT)

    def on_session_summary(self) -> None:
        """15:25 — Log and Telegram the daily session summary."""
        if not self.session_active:
            return
        self.obi.stop()
        self.session_active = False

        summary = self.logger.session_summary()
        msg = (
            f"📊 *SESSION SUMMARY — {summary['date']}*\n"
            f"─────────────────────────\n"
            f"Trades (OBI pass): {summary['trades_taken']} | WR: {summary['trades_wr_pct']}%\n"
            f"Net P&L: ₹{summary['trades_pnl_net']:,.0f}\n"
            f"─────────────────────────\n"
            f"Ghosts (OBI block): {summary['signals_blocked']} | WR: {summary['ghost_wr_pct']}%\n"
            f"Ghost gross P&L: ₹{summary['ghost_pnl_gross']:,.0f}\n"
            f"─────────────────────────\n"
            f"OBI edge: ₹{summary['obi_edge_pnl']:,.0f} "
            f"({'positive ✅' if summary['obi_edge_pnl'] >= 0 else 'negative ❌'})"
        )
        logger.info(f"[BOT] {msg}")
        self._tg(msg)

    # ──────────────────────────────────────────────────────────────────────────
    # Scheduler setup & run
    # ──────────────────────────────────────────────────────────────────────────

    def run(self) -> None:
        logger.info(f"[BOT] Starting {STRATEGY_NAME} scheduler...")

        # ── Startup recovery ──────────────────────────────────────────────────
        # APScheduler CronTrigger on a fresh in-memory store computes the NEXT
        # occurrence of hour=9,minute=40 from "now". If we start at e.g. 09:41
        # the cron sees tomorrow's 09:40 as the next fire time and today's
        # session is silently skipped — on_startup() never runs.
        #
        # Fix: if we start during the active window (09:40–13:00), fire
        # on_startup() immediately before handing off to the scheduler.
        # The scheduled on_startup at 09:40 will be a no-op tomorrow since
        # session_active will already be True from that day's startup.
        now_ist      = datetime.now(IST)
        now_ist_time = now_ist.time()
        if dt_time(9, 40) <= now_ist_time < dt_time(13, 0):
            logger.info(
                f"[BOT] Late start detected ({now_ist_time.strftime('%H:%M:%S')}) — "
                f"firing on_startup() immediately (missed 09:40 cron slot)"
            )
            self.on_startup()

        # 09:40 — startup
        self._sched.add_job(
            self.on_startup,
            CronTrigger(hour=9, minute=40, timezone=IST),
            id="startup", misfire_grace_time=120,
        )

        # Every minute 10:00–13:05 — signal check + position monitor
        self._sched.add_job(
            self.on_signal_check,
            CronTrigger(hour="10-13", minute="*", timezone=IST),
            id="signal_check", misfire_grace_time=30,
        )

        # 15:14 — force EOD exit
        self._sched.add_job(
            self.on_eod_exit,
            CronTrigger(hour=15, minute=14, timezone=IST),
            id="eod_exit", misfire_grace_time=60,
        )

        # 15:25 — session summary
        self._sched.add_job(
            self.on_session_summary,
            CronTrigger(hour=15, minute=25, timezone=IST),
            id="session_summary", misfire_grace_time=120,
        )

        # Write an initial dashboard state snapshot so the bot page knows
        # the scheduler is active even before the 09:40 session startup.
        self._write_state()

        logger.info("[BOT] Scheduler running. Press Ctrl+C to stop.")
        try:
            self._sched.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("[BOT] Shutdown requested.")
        finally:
            self.obi.stop()
            logger.info("[BOT] OBI engine disconnected. Bye.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    bot = NTSOBIBot()
    bot.run()
