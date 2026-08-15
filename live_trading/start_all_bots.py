"""
Multi-Bot Launcher for Live (Analyzer) Trading

Active bots:
1. Nifty BB Overbought Bot (BB(30,3σ) 5-min, NIFTY ATM PE sell, paper trading)
12. BANKNIFTY BB Opening Candle Bot (BB(20,2σ) on 09:15 1-min candle, BNF CE+PE + SENSEX PE, paper trading)
   ↳ Research validated (bb_deep_study Study A, 2026-03-21). IS Sharpe +3.63, OOS +10.65.
   ↳ OVERBOUGHT-only, 09:15–10:30 entry, daily ADX-14 <25 filter, E4 −30% exit.
2. Tick Stasher (infra — continuous tick recording)
3. Pre-Open Gap Fade Bot (Nifty 50 stocks, paper trading)
4. Nifty Trend Seller Bot (ADX+RSI+MACD confluence, NIFTY ATM CE/PE sell, combined)
   ↳ Research validated 10/10 pipeline stages. SHORT-ONLY OOS +1.735. Live-bot
     runs combined (ADX>30 RSI>55 ADX-D 5b); research-optimal params pending update.
5. SENSEX Trend Seller Bot (SHORT-ONLY — sell ATM CE on bearish ADX+RSI+MACD signal)
   ↳ Stage 9 validated: OOS Sharpe +2.181, WR 73.5% (Oct 2025–Mar 2026).
   ↳ Params: ADX>25, RSI<50, ADX-D 7b, SL 2×, BSE weekly options (BFO).
   ↳ BANKNIFTY MUST NOT be added — short-CE leg is destructive (OOS -1.225, WR 42.9%).
6. [DEPLOYED LIVE on fyers_cs 2026-07-11 — HTF PO3 Bot, see live_trading/active_trading_bots.md]
7. EMA Swing Scanner (daily EMA pullback + RSI on 16 NIFTY50 stocks, paper trading)
   ↳ V3 Backtest Jan 2023–Mar 2026: 423 trades, WR 52.3%, PF 1.83, Sharpe 2.48.
   ↳ MaxDD −7.75%, CAGR 24.99%, ₹10L → ₹20.47L. Regime filter: NIFTY50 > 50 EMA.
   ↳ Fires once daily at 15:30 IST (long-running scheduler, not market-hours loop).
   ↳ SENSEX excluded (OOS Sharpe −3.765, structural BSE options liquidity issue).
8. BANKNIFTY BB Options Bot (BB(20,2σ) 1-min option premium, sell ATM CE/PE, paper trading)
   ↳ Research validated ALL 10/10 pipeline stages (bb_options_study extended, 2026-03-22).
   ↳ IS Sharpe +2.198, OOS Sharpe +2.692, WR 86% (non-expiry days, Oct 2025–Mar 2026).
   ↳ Signal: ATM CE or PE 1-min close > upper BB → sell that option (first signal only).
   ↳ Exit: SMA reversion (bar-level) | SL 1.5× entry (tick-level) | EOD 15:20.
   ↳ MANDATORY: skip all BANKNIFTY monthly expiry days (Stage 10 finding).
9. [RETIRED 2026-07-10 — failed paper-trading gate, see live_trading/active_trading_bots.md]
10. NTS + OBI Gate Bot (NTS champion params + depth-50 OBI gate, 15-session forward experiment)
11. NIFTY MACD Map Bot (MACD(5,13,3) histogram cross, 15-min bars, ATM PE/CE sell, paper trading)
12. Gap Fade EOD Bot (Nifty 50 stocks gap-down 2–5%, long-only, full-day hold, exit 15:25, paper trading)
13. NIFTY EOD Hold Bot (ADX+MACD+hammer/SS reversal, 1-min bars, 09:15–09:44 window, EOD exit 15:29, paper trading)
14. NIFTY Iron Fly Weekly Bot (Short Iron Fly — sell ATM CE+PE, buy OTM CE+PE, 10 lots NRML, VIX≥12+MA20, paper trading)
   ↳ Research validated ALL 10/10 pipeline stages (iron_fly_weekly_study, 2026-05-12). Champion C3.
   ↳ OOS Sharpe +1.87, WR 65.2%. Entry Wed 10:00, SL ₹20K combined, exit Mon 15:15.
15. Equity OBI Bot (depth-50 OBI-gated long+short on RELIANCE+HDFCBANK, MIS equity, paper trading)
   ↳ Research validated ALL 10/10 pipeline stages (gap_continuation_study, 2026-04-20).
   ↳ IS Sharpe +2.624, OOS Sharpe +2.336, WR 59.7% OOS. Long only, no SL, skip NIFTY expiry days.
   ↳ Research validated ALL 10/10 pipeline stages (macd_money_map_study, 2026-04-12).
   ↳ OOS Sharpe +8.99, WR 78.9% (Oct 2025–Mar 2026). NIFTY + BANKNIFTY pass Stage 9.
   ↳ Params: dist>1.5σ, delay=1 bar, DTE 2–7, SL 2×, entry 09:30–14:00, EOD 15:15.
16. BB Mean Reversion Bot (BANKNIFTY 1-min index red candle + BB(20,2σ) → BUY ATM monthly PE, 5-gate, paper trading)
   ↳ Research validated ALL 10/10 pipeline stages (bb_mean_reversion_candle_study, 2026-06-01).
   ↳ IS Sharpe +1.008, OOS Sharpe +2.425, MC 97.3%. Trend filter + NatRR≥1.25 gate.
   ↳ DEBIT trade (BUY PE). SL: spot ≥ trigger_high. Target: 4× risk. EOD: 15:15.
17. SENSEX Iron Fly Weekly Bot (Short Iron Fly — sell ATM CE+PE, buy OTM CE+PE, 1 lot 20 units NRML, MA20 filter, paper trading)
   ↳ Research validated ALL 10/10 pipeline stages (sensex_iron_fly_weekly_study, 2026-05-13). Champion C4.
   ↳ OOS Sharpe +4.227, WR 81.0%, 58 cycles. Entry Fri 10:00, PT 50% credit, exit Wed 15:15.
   ↳ Adj trigger: |Δ| < 0.20 or > 0.70 for 2 consecutive polls. No SL. BFO NRML.

PREVIOUSLY PENDING (now built 2026-03-22):
- BANKNIFTY BB Options Bot (approved 2026-03-22, paper trading target):
  Strategy C — sell BANKNIFTY ATM CE/PE when 1-min premium closes ≥ upper BB.
  Champion config: BB(20,2.0) SL=1.5×. SKIP all BANKNIFTY monthly expiry days.
  Extended re-run (Nov 2023–Sep 2025 IS, Oct 2025–Mar 2026 OOS, 539 trading days):
    OOS Sharpe +2.692 (non-expiry only), WR 86%, 100 OOS trades. All 10 stages pass.
  Research: options_data/research/bb_options_study/results_summary.md

RETIRED (signals / alerts only, replaced by live bots):
- BB 5M Scanner (2026-03-21): discretionary alert tool. Replaced by Nifty BB Overbought Bot
  which uses research-optimal BB(30,3σ) vs scanner's BB(45,1.5σ) (IS Sharpe 0.80 < 1.5 gate).

RETIRED:
- Bollinger Bands Options Bot (2026-03-21): Strategy A (BUY lower BB) confirmed losing.
  IS Sharpe -0.64 (NIFTY), -2.06 (SENSEX), -4.85 (BANKNIFTY). OOS negative everywhere.
  Replaced by BANKNIFTY BB Options Bot (Strategy C, SELL upper BB) — pending build.
- Candle Breaker Bot (2026-03-21): Signal has no directional predictive power (OOS Sharpe -5.21).
- ORB Champion 15M Bot (2026-03-21): All variants exhausted — BUY (best IS +0.563), SELL_OPP
  (best IS +0.751), SELL_SAME (best IS +0.856), and filtered variants (range_comp + VIX regime)
  all fail. NIFTY SELL_OPP + range_comp passes IS (Sharpe 2.33) but fails OOS (Sharpe -0.55,
  52 trades). OOS VIX max 14.4 vs IS max 23.2 — regime-filtered configs never fired. Live-bot
  total loss ₹-36,138. No viable option strategy found on 15-min ORB signal.
- Daily Sniper Bot (2026-03-21): BB lower-band mean-reversion on Nifty 50 stocks. Tested all
  50 stocks across 48 param combos. Top-10 IS whitelist: IS Sharpe +3.01 → OOS Sharpe -1.33
  (144% degradation). Win rate collapses 83%→45% OOS. Root cause: IS stock selection is
  overfitting (5-9 trades per stock); low-VIX OOS regime (avg 11.5) doesn't support
  mean-reversion. Live-bot BB(20,2) SL=3%: IS +1.58 → OOS -1.35. Strategy has no edge.
- BB Paper Bot (2026-03-21): Study A (Index BB) fails IS gate (best +1.46) and signal
  frequency collapses OOS (2-4 trades in 6 months vs 20-42 in IS) — index doesn't break
  upper BB in low-VIX regime. Study B (Option BB) top configs pass IS (+1.71) but fail OOS
  gate (best +0.97, gate 1.0). Live-bot Study B BB(20,2.5) SL=1.5×: OOS Sharpe +0.87.
  Both live-bot params failed IS gate to begin with. No viable edge found.

Usage:
    python live_trading/start_all_bots.py
    python live_trading/start_all_bots.py --only "VP Swing Screener,VP Swing Screener (Daily)"

--only takes a comma-separated list of bot names (exact match against BOTS[]['name'],
falling back to a case-insensitive substring match against the name or script path).
It (re)starts just those bots via the same protected launch path as a full run
(start_bot()'s subprocess.Popen(..., preexec_fn=os.setsid)) and exits — it does not
touch the rest of the fleet, does not apply market-hours/pulse gating, and does not
enter the persistent monitor_bots() supervisor loop. Use it for a one-off restart of
a specific bot (e.g. after manually diagnosing a dead process) instead of a raw
`nohup ... &` shell command, which skips os.setsid and leaves the process vulnerable
to being killed as part of the launching shell's process group — see WORKLOG.md
2026-08-13 in vp_swing_reversion_daily_study for the incident this was added for.

Press Ctrl+C to stop all bots gracefully.
"""

import os
import sys
import argparse
import subprocess
import signal
import time
import logging
import json
from pathlib import Path
import asyncio
import websockets
from datetime import datetime, time as dt_time, timedelta
from dotenv import load_dotenv
# Add parent directory to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load Env
load_dotenv()

from live_trading.api_utils import is_market_holiday, is_nse_fo_trading_day_via_fyers

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("live_trading/logs/launcher.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

BOTS = [
    # Bollinger Bands Options Bot — RETIRED 2026-03-21
    # Research (options_data/research/bb_options_study/) showed:
    #   Strategy A (BUY lower BB) BB(45,1.5) SL=30%: IS Sharpe -0.64 (NIFTY), -2.06 (SENSEX),
    #   -4.85 (BANKNIFTY). OOS also negative across all instruments. CONFIRMED LOSING.
    #
    #   Extended re-run (2026-03-22, IS: Nov 2023–Sep 2025, OOS: Oct 2025–Mar 2026):
    #   Champion is Strategy C (SELL upper BB) — BANKNIFTY only (SENSEX fails Stage 7 WF):
    #     BANKNIFTY BB(20,2.0) SL=1.5x: OOS Sharpe=2.692 (non-expiry), WR=86%, 10/10 stages ✅
    #     BANKNIFTY BB(30,1.0) SL=2.0x: OOS Sharpe=2.439 (non-expiry), WR=80%, 10/10 stages ✅
    #     Rule: SKIP all BANKNIFTY monthly expiry days (expiry-day Sharpe = -1.04, destructive).
    #   → BANKNIFTY BB Options Bot to be built and paper-traded (not yet built, 2026-03-22).
    # ORB Champion 15M Bot — RETIRED 2026-03-21
    # Research (options_data/research/orb_study/) showed:
    #   Live-bot config (SL=20%, SHIELD=15%, TRAIL=25%, TSL=10%):
    #   IS Sharpe -1.086, OOS Sharpe -1.753. Total loss ₹-36,138 over full period.
    #   Best ANY config IS Sharpe only +0.563 (BANKNIFTY) — far below 1.5 gate.
    #   Root cause: ORB breakouts have 37–47% win rate; option buyers need >55%.
    #   False breakouts dominate; buying options after breakout means buying overpriced IV.
    # BB 5M Scanner — REPLACED 2026-03-21 by Nifty BB Overbought Bot
    # Scanner used BB(45, 1.5σ) which had IS Sharpe +0.80 (fails >1.5 gate).
    # Research champion BB(30, 3σ): IS Sharpe +3.63, OOS Sharpe +10.65 (OVERBOUGHT-only OOS +14.47).
    # The new bot automates paper trading with research-optimal parameters.
    #
    # BANKNIFTY BB Opening Candle Bot — APPROVED ✅ (research/bb_opening_candle_study, 2026-06-28)
    # Research: 8/8 pipeline stages PASS
    #   Signal: 09:15 1-min ATM option High > BB(20,2σ) → SELL LIMIT at (Close+High)/2 @ 09:16
    #   Instruments: BANKNIFTY CE + BANKNIFTY PE (monthly NFO) + SENSEX PE (weekly BFO)
    #   SL: fill + 10 pts (fixed). Target: evolving 20-bar rolling SMA.
    #   BNF CE+PE OOS avg +5.52 pts · MC P(positive)=100% · Walk-forward: 11/13 pass
    #   SENSEX PE OOS avg +5.99 pts · MC P=98.3%
    #   ADX filter: skip if daily BANKNIFTY ADX(14) > 35 (all other ADX levels positive)
    {
        'name': 'BANKNIFTY BB Opening Candle Bot',
        'script': 'live_trading/banknifty_bb_opening_candle_bot/banknifty_bb_opening_candle_bot.py',
        'description': (
            '09:15 ATM option High > BB(20,2σ) → SELL LIMIT at (C+H)/2 @ 09:16. '
            'BNF CE+PE (monthly NFO) + SENSEX PE (weekly BFO). SL=10pts, target=evolving SMA. '
            'ADX>35 skip filter. OOS avg +5.52 pts (BNF), MC 100%. Paper trading.'
        )
    },
    # Nifty BB Overbought Bot — APPROVED ✅ (research/bb_deep_study Study A, 2026-03-21)
    # Research findings (bb_deep_study/study_a_report/STUDY_A_RESULTS.md):
    #   Champion: BB(30, 3σ), 09:15–10:30 entry window, OVERBOUGHT-only → sell ATM PE
    #   IS Sharpe +3.63, OOS Sharpe +10.65 (OVERBOUGHT-only OOS Sharpe +14.47)
    #   MC: 100% stability (all 10,000 runs profitable)
    #   Bootstrap: median Sharpe +3.90 across 1,000 scrambles
    #   Walk-forward: 7/11 windows profitable, 0 catastrophic
    #   Daily ADX-14 filter (< 25): ADX>25 destroys edge (IS Sharpe drops to -3.04)
    #   SENSEX and BANKNIFTY fail Stage 9 → NIFTY-only
    #   Min premium: ₹150 (₹200 on expiry days); E4 exit at −30% premium decay
    {
        'name': 'Nifty BB Overbought Bot',
        'script': 'live_trading/nifty_bb_overbought_bot/nifty_bb_overbought_bot.py',
        'description': (
            'BB(30,3σ) 5-min OVERBOUGHT → sell NIFTY ATM PE. '
            'Entry 09:15–10:30, daily ADX-14 <25 filter, min premium ₹150, '
            'E4 exit −30% decay, SL 2×, 1 lot, BFO paper trading. '
            'Research: IS Sharpe +3.63, OOS +10.65.'
        )
    },
    # Candle Breaker Bot — RETIRED 2026-03-21
    # Research (options_data/research/candle_breaker_study/) showed:
    #   IS Sharpe 0.82 (FAIL >1.5), OOS Sharpe -5.21 (FAIL >1.0).
    #   Signal has no directional predictive power. Strategy rejected at Stage 4.
    # Tick Stasher — RETIRED 2026-04-27
    # No active bot reads live_ticks.duckdb (only retired candle_breaker_bot did).
    # Freeing resources; tick_stasher.py moved to live_trading/retired/.
    # Pre-Open Gap Fade Bot — RETIRED 2026-06-24
    # Post-cost returns insufficient; no edge after transaction costs on equity intraday.
    # {
    #     'name': 'Pre-Open Gap Fade Bot',
    #     'script': 'live_trading/preopen_gap_fade_bot/preopen_gap_fade_bot.py',
    #     'description': 'Nifty 50 stocks — gap ≥2% fade, SL 0.5%, exit 10:00 (paper trading)'
    # },
    # Daily Sniper Bot — RETIRED 2026-03-21
    # Research (options_data/research/daily_sniper_study/) showed:
    #   IS Sharpe +3.01 (top-10 selected) → OOS Sharpe -1.33 (144% degradation).
    #   Live-bot config IS +1.58 → OOS -1.35. Win rate 83%→45%. No OOS edge.
    #   Root cause: IS stock selection is overfitting; low-VIX OOS regime kills mean-reversion.
    # BB Paper Bot — RETIRED 2026-03-21
    # Research (options_data/research/bb_paper_study/) showed:
    #   Study A (Index BB): fails IS gate (best +1.46); signal collapses OOS (2-4 trades in 6mo).
    #   Study B (Option BB): top configs pass IS (+1.71) but fail OOS gate (best +0.97, gate 1.0).
    #   Live-bot Study B BB(20,2.5) SL=1.5×: OOS Sharpe +0.87. Both live-bot configs failed IS.
    #
    # Nifty Trend Seller Bot — APPROVED ✅ (research/nifty_trend_seller_study, 2026-03-21)
    # All 10 pipeline stages pass. Key findings:
    #   CURRENT live-bot params (ADX>30, RSI>55/<45, ADX-D 5b, SL 2.0×):
    #     IS Sharpe +0.884 (fails IS gate) but OOS Sharpe +2.004 (strong OOS).
    #     MC stability 90.7%, prob_loss 9.3%.  Walk-forward: 0 catastrophic windows.
    #   RESEARCH-OPTIMAL params (ADX>25, RSI<50 (short-only), ADX-D 7b, SL 2.0×):
    #     IS Sharpe +1.984 → OOS +1.735 (12.6% degradation only).
    #     MC stability 99.2%, prob_loss 0.8%.  P5 Sharpe +0.829.
    #   SHORT LEG ONLY has the real edge (sell CE on bearish signal).
    #     Long leg (sell PE on bullish) is weak IS and OOS across all instruments.
    #   STAGE 9 multi-instrument (script: 09_multi_instrument.py):
    #     NIFTY:     OOS +1.735 ✅ (validated)
    #     SENSEX:    OOS +2.369 ✅ short-only OOS +2.181, WR 73.5% — new bot opportunity
    #     BANKNIFTY: OOS -1.163 ❌ DO NOT ADD — short-CE destructive (WR 42.9%)
    #   RECOMMENDED ACTION: Paper-trade Apr–May 2026 to observe long leg.
    #     If long-leg WR ≥55% and Sharpe ≥0.5 → keep combined.
    #     Otherwise → switch to short-only with ADX>25, ADX-D 7b params.
    {
        'name': 'Nifty Trend Seller Bot',
        'script': 'live_trading/nifty_trend_seller_bot/nifty_trend_seller_bot.py',
        'description': (
            'ADX(14)>30↑ + RSI>55/<45 + MACD(5,13,3) — NIFTY ATM sell CE/PE, '
            '10:00–13:00 IST, DTE 2–7, VIX≤22, EOD 15:20. '
            'Research-optimal: ADX>25, RSI<50, ADX-D 7b, SHORT-ONLY (OOS Sharpe +1.735). '
            '[PENDING param update after Apr–May 2026 paper observation]'
        )
    },
    # SENSEX Trend Seller Bot — APPROVED ✅ (Stage 9 multi-instrument, 2026-03-21)
    # Research (options_data/research/nifty_trend_seller_study/09_multi_instrument.py):
    #   SHORT-ONLY (sell CE on bearish SENSEX confluence): OOS Sharpe +2.181, WR 73.5%
    #   Long leg (sell PE) is destructive on SENSEX (OOS -0.188) — NOT traded.
    #   Params: ADX>25, RSI<50, ADX-D 7b, SL 2.0× (research-optimal, not NIFTY's ADX>30)
    #   Exchange: BFO (BSE F&O), Lot size: 20, Weekly expiry ≥2 DTE
    {
        'name': 'SENSEX Trend Seller Bot',
        'script': 'live_trading/sensex_trend_seller_bot/sensex_trend_seller_bot.py',
        'description': (
            'SHORT-ONLY: ADX(14)>25↑(7b) + RSI<50 + MACD(5,13,3)×↓ — '
            'SENSEX ATM sell CE, 10:00–13:00 IST, DTE 2–7, VIX≤22, BFO, EOD 15:20. '
            'Stage 9 validated: OOS Sharpe +2.181, WR 73.5%.'
        )
    },
    # HTF PO3 Bot — DEPLOYED LIVE on fyers_cs 2026-07-11
    # Bot folder moved to live_trading/deployed_live/htf_po3_bot/
    # No longer launched from CRK; monitor via CS workspace dashboard.
    # BANKNIFTY BB Options Bot — DEPLOYED LIVE on fyers_cs 2026-06-29
    # Bot folder moved to live_trading/deployed_live/banknifty_bb_options_bot/
    # No longer launched from CRK; monitor via CS workspace dashboard.
    # HA Options Bot — RETIRED 2026-07-10 — failed its paper-trading gate (underperformed
    # vs. research expectations once live). See live_trading/active_trading_bots.md for the
    # retirement note. Research history kept below for reference; APPROVED ✅ (research/ha_options_study/, ALL 10 pipeline stages, 2026-03-23)
    # Research summary: options_data/research/ha_options_study/results_summary.md
    #   Strategy: Heiken Ashi candle flip → sell ATM CE (bearish) or PE (bullish); daily HA restart.
    #             Entry on open of next bar after flip, 09:30–14:30 IST.
    #             Exit: HA reversal (index bar closes in opposite direction) OR swing SL.
    #             SL = swing HA high/low of previous 5 bars before the signal bar.
    #
    #   NIFTY    (5-min ATM, weekly ≥2 DTE):
    #             IS Sharpe +8.43   |  OOS Sharpe +10.14  |  WR 43.6%
    #             Walk-forward: 100% positive months
    #             MC stability: 100% (all 10,000 runs profitable)
    #
    #   BANKNIFTY (15-min ATM, monthly ≥7 DTE):
    #             IS Sharpe +10.04  |  OOS Sharpe +9.95   |  WR 53.5%  ← HIGHEST IS SHARPE
    #             Walk-forward: 94% positive months (1 slightly negative month < −₹3,000)
    #             MC stability: 100%
    #
    #   SENSEX   (5-min ATM, weekly ≥2 DTE, BFO):
    #             IS Sharpe +7.33   |  OOS Sharpe +9.85   |  WR 43.6%
    #             Walk-forward: 100% positive months
    #             MC stability: 100%
    #
    #   OOS combined Sharpe +12.37 (better than IS +2.74 — not curve-fitted).
    #   3/3 instruments pass all 10 stages. No catastrophic drawdown months.
    #   Entry filters: ATM premium ≥ ₹15, SL risk ≥ 20 index points.
    #   Sizing: 1 lot flat per instrument. Max 2 concurrent positions.
    #   DO NOT add 1-min timeframe (IS Sharpe ≈ −8, conclusively rejected).
    # Nifty Trend Seller + OBI Gate Bot — PAPER TRADE EXPERIMENT (2026-03-28)
    # Forward-testing whether depth-50 OBI data adds edge to the approved NTS strategy.
    # OBI gate: weighted NIFTY ATM CE order book imbalance must be < 0 at signal time.
    # Cannot be backtested — no historical L2 data. 15-session forward paper trade window.
    # Base strategy (research/nifty_trend_seller_study/) ALL 10 STAGES APPROVED:
    #   Champion: SHORT-ONLY | ADX>25, RSI<50, ADX-D 7b, SL 2.0× | OOS Sharpe +1.735
    # Two parallel log streams: trades.csv (OBI on) vs ghosts.csv (OBI off/counterfactual)
    {
        'name': 'NTS + OBI Gate Bot',
        'script': 'live_trading/nifty_trend_seller_obi/nifty_trend_seller_obi_bot.py',
        'description': (
            'NTS champion params + OBI gate: ADX(14)>25↑(7b) + RSI<50 + MACD×↓ — '
            'NIFTY ATM sell CE, 10:00–13:00 IST, SL 2×, VIX≤22, EOD 15:20. '
            'OBI gate: CE depth-50 weighted imbalance < 0 → TRADE; else ghost-track. '
            '15-session forward experiment. Logs: trades.csv + ghosts.csv.'
        )
    },
    # NIFTY MACD Map Bot — APPROVED ✅ (research/macd_money_map_study/, ALL 10 pipeline stages, 2026-04-12)
    # Research summary: options_data/research/macd_money_map_study/results_summary.md
    #   Strategy: MACD(5,13,3) histogram zero-cross on 15-min NIFTY spot bars.
    #             Confirmation filters: dist > 1.5σ (cross magnitude), DELAY=1 bar (signal on bar after cross).
    #             MACD line direction check: hist_cur must be positive and ml>0 (long) or negative and ml<0 (short).
    #             Bullish cross → sell ATM PE (collect premium in rising market).
    #             Bearish cross → sell ATM CE (collect premium in falling market).
    #
    #   NIFTY OOS (Oct 2025–Mar 2026, 6 months):
    #             IS Sharpe +8.16   |  OOS Sharpe +8.99  |  WR 78.9%  |  38 OOS trades
    #             Walk-forward: all windows profitable | MC stability: 100% (10,000 runs)
    #             Bootstrap median Sharpe +4.58 | Stage 10: expiry-day Sharpe +3.95, WR 80% ✅
    #
    #   Stage 9 multi-instrument:
    #             NIFTY:     OOS Sharpe +8.99 ✅ (champion)
    #             BANKNIFTY: OOS Sharpe +1.20 ✅ (monthly expiry, max_dte=35)
    #             SENSEX:    OOS Sharpe −0.65 ❌ (BSE structural issue — do NOT add)
    #             Gate: ≥2 of 3 instruments pass → ✅ PASS
    #
    #   Champion params: MACD(5,13,3), dist_thresh=1.5, delay=1, SL=2×, DTE 2–7, EOD 15:15.
    #   Entry window: 09:30–14:00 IST. 1 lot flat (75 units). NFO weekly options.
    #   Paper trading — scale to 10 lots only after sign-off from Ramakrishna.
    {
        'name': 'NIFTY MACD Map Bot',
        'script': 'live_trading/nifty_macd_map_bot/nifty_macd_map_bot.py',
        'description': (
            'MACD(5,13,3) histogram zero-cross (dist>1.5σ, delay=1 bar, 15-min bars) — '
            'NIFTY ATM sell PE (bullish) / CE (bearish). '
            'Entry 09:30–14:00 IST, DTE 2–7, SL 2×, EOD 15:15. '
            '1 lot flat (scale to 10 after sign-off). ALL 10 stages pass. '
            'OOS Sharpe +8.99, WR 78.9%. Paper trading.'
        )
    },
    # Gap Fade EOD Bot — APPROVED ✅ (research/gap_continuation_study/, ALL 10 pipeline stages, 2026-04-20)
    # Research summary: options_data/research/gap_continuation_study/results_summary.md
    #   Strategy: NIFTY50 stock gaps down 2–5% at open → LONG at open, hold full day, sell at 15:25.
    #             Long-only (gap-up short: IS Sharpe 0.82 < 1.5 gate, excluded).
    #             No stop-loss — full-day mechanism; tight SL would be stopped by intraday noise.
    #             Skip NIFTY expiry days (WR drops 62% → 46% on expiry).
    #   IS:  Sharpe +2.624  |  WR 59.4%  |  n=288 (Jan 2023–Sep 2025)
    #   OOS: Sharpe +2.336  |  WR 59.7%  |  n=119 (Oct 2025–Mar 2026)  |  degradation 11%
    #   ALL 10 pipeline stages PASS (Stage 7 conditional — fix gap_lo=2% in live, do not re-optimise).
    #   Distinct from preopen_gap_fade_bot: different mechanism (full-day recovery vs morning fade),
    #   different hold period (6hrs vs 45min), no SL, long-only, gap cap at 5%.
    # Gap Fade EOD Bot — RETIRED 2026-06-04
    # Live paper-trading result (May 18–Jun 4, 4 trades): WR 25%, Gross P&L −₹7,739.
    # All 3 losing trades held full day via EOD_15:14 exit — thesis never triggered,
    # position held as dead weight for 6hrs with no mid-session stop. Structural flaw:
    # no intraday stop means a trending-against day wipes out weeks of wins.
    # Research validated but live regime (low-gap-frequency, trending market) not supportive.
    # {
    #     'name': 'Gap Fade EOD Bot',
    #     'script': 'live_trading/gap_fade_eod_bot/gap_fade_eod_bot.py',
    #     'description': (
    #         'Nifty50 stocks gap-down 2–5% → LONG at 09:15, sell at 15:25 (full-day). '
    #         'No SL. Skip NIFTY expiry days (currently Tuesdays). Long-only. '
    #         'All 50 stocks, ₹1L/trade, max 10 positions. '
    #         'Research: IS Sharpe +2.624, OOS +2.336, WR 59.7%. ALL 10 stages pass.'
    #     )
    # },
    # NIFTY EOD Hold Bot — APPROVED ✅ (research/atm_options_eod_hold_1min_study/, ALL 10 pipeline stages, 2026-04-30)
    # Research summary: options_data/research/atm_options_eod_hold_1min_study/results_summary.md
    #   Strategy: 1-min reversal signal (ADX≥25 + MACD direction + hammer/shooting-star + EMA-20 context)
    #             in 09:15–09:44 opening window → sell ATM NIFTY weekly option, hold to 15:29 (EOD).
    #             No stop-loss. VIX soft filter: skip session if INDIAVIX ≥ 17 at open.
    #   Combos (both evaluated, first signal wins):
    #     adx25_macd_slope_c2.0__sell  — MACD-line slope direction
    #     adx25_hist_slope_c2.0__sell  — histogram slope direction
    #   IS:  macd_slope Sharpe +2.613 | WR 65.4% | n=52  (Dec 2024–Sep 2025)
    #        hist_slope Sharpe +1.932 | WR 70.8% | n=48
    #   OOS: macd_slope Sharpe +2.867 | WR 73.3% | n=30  (Oct 2025–Mar 2026)
    #        hist_slope Sharpe +2.353 | WR 75.0% | n=24
    #   Stage 9: NIFTY ✅ + BANKNIFTY ✅ (SENSEX ❌ — signal is NIFTY-specific). NIFTY-only deployment.
    #   Params: ADX=25, candle_strict=2.0, MACD(12,26,9), EMA(20), ADX(14)
    #           10 lots × lot_size=65, DTE 2–7, NFO weekly options.
    {
        'name': 'NIFTY EOD Hold Bot',
        'script': 'live_trading/nifty_eod_hold_bot/nifty_eod_hold_bot.py',
        'description': (
            'ADX≥25 + MACD/hist-slope + hammer/SS candle + EMA-20 reversal signal — '
            '1-min NIFTY bars, 09:15–09:44 window. Sell ATM CE (bearish) / PE (bullish). '
            'Hold to 15:29 EOD, no SL. VIX soft filter: skip if INDIAVIX ≥ 17. '
            '10 lots (lot_size=65), DTE 2–7, NFO weekly options. '
            'OOS Sharpe +2.867/+2.353, WR 73%/75%. ALL 10 stages pass. Paper trading.'
        )
    },
    # NIFTY Iron Fly Weekly Bot — APPROVED ✅ (research/iron_fly_weekly_study/, ALL 10 pipeline stages, 2026-05-12)
    # Research summary: options_data/research/iron_fly_weekly_study/results_summary.md
    #   Strategy: Short Iron Fly — sell ATM CE + ATM PE (straddle), buy OTM CE + OTM PE at delta≈0.10.
    #             Entry: Wednesday 10:00 IST (expiry − 6 days, i.e. prior Wed for Tue expiry).
    #             Entry filters: VIX ≥ 12 AND NIFTY spot ≥ 20-day MA (combined C3 config).
    #             Exit: SL ₹2,000/lot combined MTM loss → ₹20,000 for 10 lots, OR 15:15 day-before-expiry.
    #   Champion C3 (entry_style=monday_1000, hedge_delta=0.10, SL=₹2000/lot):
    #     IS:  Sharpe +2.34, WR 68.4%, MaxDD −8.2% (Jan 2023–Sep 2025)
    #     OOS: Sharpe +1.87, WR 65.2%, MaxDD −6.1% (Oct 2025–Mar 2026, 23 trades)
    #     MC stability: 97.3% (10,000 runs). Bootstrap median Sharpe +1.52. WF: 0 catastrophic windows.
    #     Stage 9: NIFTY-only deployment (SENSEX/BANKNIFTY structural liquidity mismatch).
    #     Stage 10: expiry-day segmented (all exits before expiry, no day-of-expiry risk).
    #   Product: NRML (3–4 day hold, overnight positions). Lot size: 65. NFO weekly options.
    #   4-leg entry order: BUY buy_ce → BUY buy_pe → SELL sell_ce → SELL sell_pe (margin benefit).
    #   4-leg exit order: BUY sell_ce → BUY sell_pe → SELL buy_ce → SELL buy_pe.
    #   Weekly expiry: Tuesday (post-Sep 2025). Entry: Wednesday 10:00 IST (6 days before).
    {
        'name': 'NIFTY Iron Fly Weekly Bot',
        'script': 'live_trading/nifty_iron_fly_weekly_bot/nifty_iron_fly_weekly_bot.py',
        'description': (
            'NIFTY Weekly Short Iron Fly — sell ATM CE+PE, buy OTM CE+PE (delta≈0.10). '
            'Entry Wed 10:00 IST, 10 lots NRML, VIX≥12 + NIFTY≥MA20 filter. '
            'SL ₹20,000 combined, exit 15:15 day-before-expiry (Mon). '
            'Champion C3: ALL 10 stages pass. OOS Sharpe +1.87, WR 65.2%. Paper trading.'
        )
    },
    # SENSEX Iron Fly Weekly Bot — APPROVED ✅ (research/sensex_iron_fly_weekly_study/, ALL 10 pipeline stages, 2026-05-13)
    # Research summary: options_data/research/sensex_iron_fly_weekly_study/results_summary.md
    #   Strategy: Short Iron Fly — sell ATM CE + ATM PE (straddle), buy OTM CE + OTM PE at delta≈0.10.
    #             Entry: Friday 10:00 IST (expiry − 6 days for Thursday expiry on BFO).
    #             Entry filter: SENSEX prev_close ≥ SENSEX MA20 on entry day.
    #             No VIX filter — Stage 8 confirmed MA20 alone captures full regime benefit.
    #             PT: exit when combined MTM ≥ 50% of net entry credit.
    #             Adjustment: if short-leg |delta| exits [0.20, 0.70] for 2 consecutive polls
    #                         → buy back that short leg + re-sell ATM in same option type.
    #             No stop-loss — adjustment mechanism manages risk.
    #   Champion C4 (hedge_delta=0.10, PT=50%, adj_low=0.20, adj_hi=0.70):
    #     IS  (Jan 2024–Mar 2025, 60 cycles): Sharpe +5.726, WR 76.7%
    #     OOS (Apr 2025–Apr 2026, 58 cycles): Sharpe +4.227, WR 81.0%
    #     MC stability: 100% (10,000 runs). Bootstrap median Sharpe +4.42. WF: 0 catastrophic windows.
    #     Stage 8: MA20-only filter (VIX≤14 overfitting warning — do NOT add VIX cap in live bot).
    #     Stage 10: High-adj (11+ adj/cycle, N=14) WR=50% — bot logs adj_count for monitoring.
    #   Product: NRML BFO (4 trading day hold). 1 lot × 20 units. Weekly Thursday expiry.
    #   4-leg entry order: BUY buy_ce → BUY buy_pe → SELL sell_ce → SELL sell_pe (margin benefit).
    #   4-leg exit order: BUY sell_ce → BUY sell_pe → SELL buy_ce → SELL buy_pe.
    {
        'name': 'SENSEX Iron Fly Weekly Bot',
        'script': 'live_trading/sensex_iron_fly_weekly_bot/sensex_iron_fly_weekly_bot.py',
        'description': (
            'SENSEX Weekly Short Iron Fly — sell ATM CE+PE, buy OTM CE+PE (delta≈0.10). '
            'Entry Fri 10:00 IST, 1 lot (20 units) NRML BFO, SENSEX≥MA20 filter (no VIX filter). '
            'PT 50% of net credit. Adj trigger: |Δ|<0.20 or >0.70 for 2 polls. No SL. '
            'Exit Wed 15:15 day-before-expiry. '
            'Champion C4: ALL 10 stages pass. OOS Sharpe +4.227, WR 81.0%. Paper trading.'
        )
    },
    # BANKNIFTY Iron Fly Monthly Bot — APPROVED ✅ (research/banknifty_iron_fly_monthly_study/, ALL 10 pipeline stages, 2026-05-12)
    # Research summary: options_data/research/banknifty_iron_fly_monthly_study/results_summary.md
    #   Strategy: Short ATM straddle + long OTM wings (4 legs). Delta-based intraday adjustments.
    #             Entry: first trading day after prior monthly expiry at 10:00 IST.
    #             Exit: 15:15 IST on calendar day before expiry (or profit target 50% of net premium).
    #   Champion (OOS winner, hedge_delta=0.15):
    #     IS:  Sharpe +2.355, WR 81.2%, Total ₹55,732 (16 cycles, Jan 2024–Jun 2025)
    #     OOS: Sharpe +1.555, WR 85.7%, Total ₹43,252 (7 cycles, Jul 2025–Apr 2026)
    #     MC: 99.9% stable. Bootstrap median +1.80. WF: 0 catastrophic windows.
    #     Stage 8 filter: ADX<25+VIX≥12 → Sharpe 3.73 (+2.1 improvement).
    #   Adjustments: if short leg delta exits [0.20, 0.70] for 2 bars → close + re-open at ATM.
    #   Product: NRML (monthly hold, ~20 days). 10 lots × lot_size=30. NFO monthly options.
    {
        'name': 'BANKNIFTY Iron Fly Monthly Bot',
        'script': 'live_trading/banknifty_iron_fly_monthly_bot/main.py',
        'description': (
            'BANKNIFTY Monthly Short Iron Fly — sell ATM CE+PE, buy OTM CE+PE (delta≈0.15). '
            'Entry 10:00 IST first trading day after prior month expiry. '
            'Delta adj [0.20–0.70] for 2 bars, 10 lots NRML monthly. '
            'ALL 10 stages pass. OOS Sharpe +1.555, WR 85.7%. Paper trading.'
        )
    },
    # Flat Blue Line Monthly Bot — APPROVED ✅ (research/flat_blue_line_monthly/, ALL 10 pipeline stages, 2026-06-05)
    # Research summary: options_data/research/flat_blue_line_monthly/FINAL_REPORT.md
    #   Strategy: 6-leg hedged ratio spread (Double Fly) on NIFTY + BANKNIFTY monthly options.
    #             Leg 1+2: long ATM straddle (1×CE + 1×PE). Leg 3+4: short strangle ×2 at ATM±D.
    #             Leg 5+6: long OTM wing hedges at delta≈0.10 (N_C call lots, N_P put lots).
    #             D = round((ATM_C_px + ATM_P_px) / 2, strike_step).
    #             Entry: 10:00 IST on first trading day after prior monthly expiry.
    #             Entry filter: ATM Call Black-76 IV < 14% → skip month.
    #   Champions (minute-level intraday execution):
    #     NIFTY     N_C=3 N_P=2: IS+OOS Sharpe +1.77, WR 71% (2023-2025, 21 cycles)
    #     BANKNIFTY N_C=1 N_P=3: IS+OOS Sharpe +2.86, WR 85% (2023-2025, 20 cycles)
    #   Exits: (1) MTM ≥ profit target (₹22,500 NIFTY / dynamic BN)
    #          (2) Spot crosses expiry-payoff breakeven BE_L or BE_U
    #          (3) 3 trading days before monthly expiry at 15:15 IST
    #   Product: NRML (positional, held overnight). Lot size: NIFTY=65, BANKNIFTY=30.
    #   6-leg entry order: BUY atm_c → BUY atm_p → BUY hc → BUY hp → SELL sc → SELL sp.
    #   6-leg exit order : BUY sc → BUY sp → SELL atm_c → SELL atm_p → SELL hc → SELL hp.
    {
        'name': 'Flat Blue Line Monthly Bot',
        'script': 'live_trading/flat_blue_line_monthly_bot/flat_blue_line_monthly_bot.py',
        'description': (
            'NIFTY+BANKNIFTY Monthly Double Fly — long ATM straddle, short strangle ×2, '
            'long OTM wings (delta≈0.10). Entry 10:00 IST first tday after prior monthly expiry. '
            'NIFTY N_C=3/N_P=2 (Sharpe +1.77, WR 71%), BN N_C=1/N_P=3 (Sharpe +2.86, WR 85%). '
            'IV filter <14%, pre-expiry exit 3 tdays before, BE spot-based stop. '
            'ALL 10 stages pass. Paper trading.'
        )
    },
    # BB Mean Reversion Bot — APPROVED ✅ (research/bb_mean_reversion_candle_study/, ALL 10 pipeline stages, 2026-06-01)
    # Research summary: options_data/research/bb_mean_reversion_candle_study/results_summary.md
    #   Strategy: BUY ATM monthly PE when BANKNIFTY 1-min index candle is RED AND high > upper BB(20,2σ).
    #             5-gate check: Trend (Bear/Sideways) | Trigger | HTF (5-min) | NatRR≥1.25 | DTE not 8-14.
    #             Trend filter: prev-day close ≤ 20-day SMA (Bear/Sideways only — ~50% of days active).
    #             Natural R:R gate: (trigger_close−bb_mid)/(trigger_high−trigger_close) ≥ 1.25
    #             (eliminates the 0–50pt "dead zone" — 69% of signals but loss-making on avg).
    #   IS:  Sharpe +1.008 | WR ~29% | (BANKNIFTY 1/5 RR4.0, trend-filtered)
    #   OOS: Sharpe +2.425 | WR ~28% | MC stability 97.3% (10,000 runs)
    #   Bootstrap: median Sharpe +1.51 (monthly block resampling). ALL 10 pipeline stages PASS.
    #   Stage 10: Natural R:R ≥ 1.25 sweet-spot filter identified (avg ₹446/day at 1 lot).
    #   SL: BANKNIFTY spot ≥ trigger_high (index SL, tick-level) OR PE LTP ≤ sl_opt (belt+suspenders).
    #   Target: 4.0× risk in option premium terms. EOD close: 15:15 IST.
    #   This is a DEBIT (BUY PE) trade — NOT selling CE. Maximum loss = premium paid. No margin required.
    {
        'name': 'BB Mean Reversion Bot',
        'script': 'live_trading/bb_mean_reversion_bot/bb_mean_reversion_bot.py',
        'description': (
            'Red 1-min BANKNIFTY index candle + high > BB(20,2σ) → BUY ATM monthly PE. '
            '5-gate check: Trend(Bear/Sideways) + HTF(5-min) + NatRR≥1.25 + DTE-excl(8-14). '
            'SL: index spot ≥ trigger_high. Target: 4× risk. EOD: 15:15. '
            '1 lot flat (30 units). IS +1.008 | OOS +2.425 | MC 97.3%. Paper trading.'
        )
    },
    # NIFTY MA Crossover Seller Bot — APPROVED ✅ (research/ma_cross_options_seller_study/, ALL 10 stages, 2026-06-20)
    # Research summary: options_data/research/ma_cross_options_seller_study/results_summary.md
    #   Champion A: SMA(15)/SMA(225) on 3m NIFTY INDEX bars, Trend variant, overnight hold.
    #   Signal: bearish cross → SELL ATM CE; bullish cross → SELL ATM PE. Product: NRML.
    #   Key finding: Week-2 (calendar days 8–14) OOS Sharpe −1.18, WR 43.8% — mandatory exclusion.
    #   Danger zone: VIX<13 AND ADX≥25 → optional skip (Stage 7).
    #   OOS (Jan 2025–Jun 2026): 74 trades | Sharpe 1.64 | WR 55.4%
    #   With Week-2 filter: Sharpe 2.00 | WR 58.6% | n=58
    #   MC stability: 97.5% (10,000 runs). Bootstrap: 80.3%, median Sharpe 1.64.
    #   Walk-forward: 14 windows, 0 catastrophic. Stage 8: NIFTY-only (SENSEX fails).
    #   SL: 3× entry premium (Extension Study A validated).
    #   10 lots NRML, lot_size=65, weekly Tue expiry (NFO, post-Sep 2025), DTE ≥2, anti-whipsawlockout 450 3m bars.
    {
        'name': 'NIFTY MA Cross Seller Bot',
        'script': 'live_trading/nifty_ma_cross_seller_bot/nifty_ma_cross_seller_bot.py',
        'description': (
            'SMA(15)/SMA(225) on 3m NIFTY INDEX bars — Trend variant OVERNIGHT sell. '
            'Bearish cross → SELL ATM CE; Bullish cross → SELL ATM PE. '
            'MANDATORY: skip calendar days 8–14 (Week-2 filter). '
            'SL 3× entry premium. Entry cutoff 14:00, expiry gate 14:30. '
            '10 lots NRML, lot_size=65, NFO weekly Tue (post-Sep 2025), DTE≥2, lockout 450 bars. '
            'OOS Sharpe 2.00 (week-2 filtered), WR 58.6%. ALL 10 stages pass. Paper trading.'
        )
    },
    # MACD M2 Sell Options Bot — APPROVED ✅ (research/macd_price_action_sr_study/ sell-side extension, ALL 8 pipeline gates, 2026-06-21)
    # Research: options_data/research/macd_price_action_sr_study/sell_strategy_spec.txt
    #   Signal: MACD(12,26,9) M2 zero-line crossover + price within ±0.2% of prior day's pivot PP=(H+L+C)/3
    #   Bull M2+SR3 → SELL ATM PE | Bear M2+SR3 → SELL ATM CE
    #   IS (2020–2023):  Sharpe +9.42 | WR 73% | PF 5.23 | DD% 3.1% | n=269
    #   OOS (2024–2026): Sharpe +6.16 | WR 71% | PF 3.16 | DD% 3.2% | n=309
    #   Walk-forward: 10/10 windows positive OOS | Bootstrap 5th pct Sharpe=6.37
    #   Sell-side outperforms buy-side — works BETTER in low-ATR environments where buyers bleed
    #   Phase S4: SL=1.5× (tighter beats 2×) | Target=keep 50% of credit
    #   Phase S5: MACD(12,26,9) globally optimal — 0% of 324 configs beat it
    #   8/8 decision gates PASS — STRONG GREEN LIGHT
    {
        'name': 'MACD M2 Sell Options Bot',
        'script': 'live_trading/macd_m2_sell_options_bot/macd_m2_sell_options_bot.py',
        'description': (
            'MACD(12,26,9) M2 zero-line crossover + SR3 pivot ±0.2% → sell ATM options. '
            'Bull M2+SR3 → SELL ATM PE | Bear M2+SR3 → SELL ATM CE. '
            'NIFTY + BANKNIFTY, 5 lots each. MIS. Entry 09:15–14:30, EOD 15:14. '
            'SL 1.5× credit | Target keep 50% of credit | DTE≥2 | MIN_CREDIT ₹10. '
            'IS Sharpe +9.42 WR 73% | OOS Sharpe +6.16 WR 71% | WF 10/10 | 8/8 gates. Paper trading.'
        )
    },
    # BANKNIFTY Trend Pullback Positional Bot — APPROVED ✅ (research/trend_pullback_positional_study/, 10-stage pipeline, 2026-07-05)
    # Research: options_data/research/trend_pullback_positional_study/results_summary.md
    #   Regime: EMA(9)/EMA(26) cross on 15-min bars, aligned vs SMA(50) basis
    #   Pullback: first close-beyond-BB(20,2σ) pierce opposite regime direction
    #   Confirm: 1-min reversal candle (engulfing/hammer/shooting star) within 30 min
    #   Bullish regime confirm → SELL ATM PE | Bearish regime confirm → SELL ATM CE
    #   Combined IS+OOS (145 trades, 2022-07 → 2026-07): Sharpe 1.850 | WR 60.0% | Net P&L +₹1,013,689 | Max DD -13.4%
    #   Stages 0,1,2b,4,5,6,7,8,10 PASS (Stage 9 skipped — already BANKNIFTY-only)
    #   Positional NRML — no daily EOD flatten, exits on regime_end/expiry_force_exit/target/SL
    {
        'name': 'BANKNIFTY Trend Pullback Positional Bot',
        'script': 'live_trading/banknifty_trend_pullback_positional_bot/banknifty_trend_pullback_positional_bot.py',
        'description': (
            'EMA(9)/EMA(26) 15-min regime vs SMA(50) basis + BB(20,2σ) pullback pierce '
            '+ 1-min candle confirmation → sell ATM option in regime direction. '
            'BANKNIFTY only, 10 lots, NRML (positional, multi-day hold). '
            'Exit priority: regime_end → expiry_force_exit(15:14) → target(keep 50%) → SL(2.5x). '
            'Combined IS+OOS Sharpe 1.850 WR 60.0% Net P&L +₹1,013,689. Paper trading.'
        )
    },
    # RETIRED 2026-06-19 — Equity OBI Bot
    # Paper trading result (2026-05-18 → 2026-06-19, 149 trades): WR 38.3%, Net P&L −₹4,122.
    # OBI signal does not translate edge to NSE MIS equity on RELIANCE + HDFCBANK.
    # Avg P&L per trade = −₹28 (slow bleed). No recovery path evident after 23 sessions.
    # Code archived to live_trading/retired/equity_obi/
    # {
    #     'name': 'Equity OBI Bot',
    #     'script': 'live_trading/equity_obi/equity_obi_bot.py',
    #     'description': (
    #         'depth-50 OBI-gated long+short — RELIANCE + HDFCBANK NSE MIS equity. '
    #         'LONG: w_OBI>+20 (3t) + VWMP<LTP + close>EMA(20). '
    #         'SHORT: w_OBI<-20 (3t) + VWMP>LTP + close<EMA(20). '
    #         'SL 0.4%, Target 0.6%, EOD 15:20. ₹1L/trade. Paper trading. '
    #         'Dual logs: trades.csv + ghosts.csv.'
    #     )
    # },
    # EMA Swing Scanner — RETIRED 2026-06-04
    # Live paper-trading result (May 18–Jun 4, 2 trades): WR 0%, Gross P&L −₹8,820.
    # Both trades stopped out: NTPC held 9,015 mins (6.25 days), COALINDIA held 1,815 mins.
    # No formal research study in options_data/research/ (deployed/README flagged ⚠️ No study).
    # Overnight multi-day holds incompatible with sandbox paper-trading validation methodology.
    # NOTE: Had 1 open paper position at retirement — TATASTEEL (921 shares, entry ₹209.90,
    #   entered 2026-05-26). Paper-only; no real broker position. Regime filter was OFF at
    #   retirement (NIFTY below 50 EMA). Position left untracked on retirement.
    # {
    #     'name': 'EMA Swing Scanner',
    #     'script': 'live_trading/ema_swing_scanner/main.py',
    #     'description': (
    #         'Daily EMA Pullback + RSI(14) on 16 NIFTY50 stocks. '
    #         'Regime filter: NIFTY50 > 50 EMA. ATR trailing stop (stop_eod pattern). '
    #         'Fires at 15:30 IST daily. Paper mode. '
    #         'V3 backtest: WR 52.3%, Sharpe 2.48, MaxDD −7.75%, CAGR 24.99%.'
    #     ),
    #     'is_daily_scheduler': True,   # does NOT use market-hours loop — manages own schedule
    # },

    # ─────────────────────────────────────────────────────────────────────────
    # NIFTY EMA Spread Bot — APPROVED ✅ (research/index_spread_study/, ALL 10 pipeline stages, 2026-06-27)
    # Research: options_data/research/index_spread_study/results_summary.md
    #   Strategy: EMA(5,13) crossover on 15-min NIFTY INDEX bars → 50pt ATM debit spread.
    #             Bull crossover: BUY ATM CE + SELL ATM+50 CE (bull call spread).
    #             Bear crossover: BUY ATM PE + SELL ATM-50 PE (bear put spread).
    #             Re-enter opposite on signal reversal. Exit: 0.5R TP / 0.95R SL / signal reversal.
    #             NRML product (positional, holds overnight). Min DTE 1. Last entry 14:00.
    #   IS:  Sharpe +4.539 | WR 53.4% | 311 trades (2022-07 → 2024-06)
    #   OOS: Sharpe +6.144 | WR 57.9% | 428 trades (2024-07 → 2026-06) — OOS BEATS IS
    #   ALL 10 pipeline stages PASS. 13/13 walk-forward windows profitable. 100% MC runs profitable.
    #   Stage 9: NIFTY ✅ + SENSEX ✅ (Sh 2.90) + BANKNIFTY ✅ (Sh 3.00)
    #   Params: EMA(5,13), width=50pt, 10 lots × lot_size=65, NFO weekly options, DTE≥1.
    {
        'name': 'NIFTY EMA Spread Bot',
        'script': 'live_trading/nifty_ema_spread_bot/nifty_ema_spread_bot.py',
        'description': (
            'EMA(5,13) crossover on 15-min NIFTY bars → 50pt ATM debit spread (NRML positional). '
            'Bull call spread on BULL cross, bear put spread on BEAR cross. '
            'Exit: 0.5R profit target | 0.95R SL | signal reversal (primary, 76% of trades). '
            '10 lots, min DTE 1, last entry 14:00. ALL 10 stages pass. '
            'OOS Sharpe +6.14, WR 57.9%, 428 trades. Paper trading.'
        ),
    },

    # BANKNIFTY EMA Spread Bot — APPROVED ✅ (Stage 9 multi-instrument confirm, 2026-06-27)
    # Same config as NIFTY bot. Strike width scaled to 100pt (BANKNIFTY granularity).
    #   OOS: Sharpe +3.00 | WR 60.1% | 193 trades (2024-07 → 2026-06)
    {
        'name': 'BANKNIFTY EMA Spread Bot',
        'script': 'live_trading/banknifty_ema_spread_bot/banknifty_ema_spread_bot.py',
        'description': (
            'EMA(5,13) crossover on 15-min BANKNIFTY bars → 100pt ATM debit spread (NRML). '
            'Same config as NIFTY EMA Spread Bot; strike width scaled to 100pt. '
            '10 lots (lot=30). Exit: 0.5R TP | 0.95R SL | signal reversal. '
            'OOS Sharpe +3.00, WR 60.1%, 193 trades. Paper trading.'
        ),
    },

    # SENSEX EMA Spread Bot — APPROVED ✅ (Stage 9 multi-instrument confirm, 2026-06-27)
    # Same config as NIFTY bot. Exchange: BFO. Strike width scaled to 100pt.
    #   OOS: Sharpe +2.90 | WR 58.1% | 353 trades (2024-07 → 2026-06)
    {
        'name': 'SENSEX EMA Spread Bot',
        'script': 'live_trading/sensex_ema_spread_bot/sensex_ema_spread_bot.py',
        'description': (
            'EMA(5,13) crossover on 15-min SENSEX bars → 100pt ATM debit spread (NRML, BFO). '
            'Same config as NIFTY EMA Spread Bot; exchange BFO, strike width 100pt. '
            '10 lots (lot=10). Exit: 0.5R TP | 0.95R SL | signal reversal. '
            'OOS Sharpe +2.90, WR 58.1%, 353 trades. Paper trading.'
        ),
    },

    # NIFTY GEX+ICT v2 Bot — APPROVED ✅ (research/gex_ict_v2_study/, ALL 9 pipeline stages pass, 2026-07-12)
    # Research summary: options_data/research/gex_ict_v2_study/results_summary.md
    #   Signal: prior-day value area break (futures 10pt-bin volume profile) → GEX candidate level
    #   reached (09:20 snapshot, priority-ordered) → 5-min regime refresh at reach time →
    #   negative_gamma ("breakout" module only; fade module excluded, fails ~30/yr MC floor) →
    #   ICT confirm (MSS or IFVG) within a 375-min window.
    #   Execution: bullish confirm → SELL ATM PE; bearish confirm → SELL ATM CE.
    #   Exit: spot stop/target1 (buffer_pct=1.50% around reached level; stop nearly inert, ~1.7%
    #   fire rate) or EOD 15:14 IST, whichever first.
    #   NIFTY-only (SENSEX dropped at Stage 3 OOS, Sharpe -0.10).
    #   Combined IS+OOS: n=239, Sharpe 2.48, net +Rs.143,993, ~98 trades/yr.
    #   MC stability 100% (10,000 runs). Bootstrap median Sharpe 2.51. Walk-forward avg OOS Sharpe
    #   4.01, 22/22 windows non-catastrophic. Stage 9 expiry segmentation: no catastrophic segment.
    #   10 lots, lot_size from DB, NFO weekly expiry, DTE≥1.
    {
        'name': 'NIFTY GEX ICT V2 Bot',
        'script': 'live_trading/nifty_gex_ict_v2_bot/nifty_gex_ict_v2_bot.py',
        'description': (
            'Prior-day futures value-area break → GEX level reach (09:20 snapshot) → '
            '5-min regime refresh → breakout-only, any-of MSS/IFVG confirm. '
            'Bullish confirm → SELL ATM PE; bearish confirm → SELL ATM CE. '
            'Spot stop/target1 (buffer 1.5%) or EOD 15:14. '
            '10 lots, NFO weekly, DTE≥1. NIFTY-only (SENSEX dropped OOS). '
            'Combined IS+OOS Sharpe 2.48, n=239, MC stability 100%. Paper trading.'
        ),
    },

    # VP Swing Screener — hourly scan engine (research/vp_swing_reversion_study/, 10/10 stages, 2026-08-09)
    # Signal: touch of the lower extreme of a rolling 10-trading-day volume profile
    #   (60-min bars, N=60) → long candidate. Target = rolling POC (recomputed every
    #   bar). Hard 3% stop-loss. No forced EOD close, no pyramiding, one position
    #   per symbol. 53-stock NIFTY50 universe. Full IS+OOS: 3,907 trades, WR ~69%,
    #   Sharpe 4.0(IS)/5.2(OOS). Stage 12 overnight-gap tail risk (-5.44% worst
    #   1%ile) accepted 2026-08-09 as a documented cost.
    # ⚠️  SCREENER, NOT AN ORDER-PLACING BOT — never calls placeorder(). Scans at
    #   6 fixed 60-min bar-close times (10:15…15:15 IST) and writes candidates/
    #   open_positions into logs/vp_swing_screener_state.json for the dashboard.
    #   A candidate becomes a tracked position only via the dashboard's manual
    #   "Confirm" action, never automatically. Not in bot_registry.py or
    #   performance_review.py (zero performance.db rows, per that module's own
    #   documented convention for non-order-placing bots).
    {
        'name': 'VP Swing Screener',
        'script': 'live_trading/vp_swing_screener/vp_swing_screener.py',
        'description': (
            'Touch of rolling 10-day volume-profile lower extreme (60-min bars) → long '
            'candidate, target = rolling POC, SL=3%. No forced EOD close, no pyramiding. '
            '53-stock NIFTY50 universe. IS+OOS Sharpe 4.0/5.2, WR ~69%, n=3,907. '
            'SIGNAL-ONLY — never calls placeorder(); positions tracked only after manual '
            '"Confirm" on the dashboard.'
        ),
    },

    # VP Swing Screener (Daily) — once-per-day scan engine
    # (research/vp_swing_reversion_daily_study/, 10/10 stages, 2026-08-12)
    # Same mechanics as the 60-min screener above, carried over unchanged
    # (SL=3%, PROFILE_DAYS=10) and re-validated on DAILY bars (N_BARS=10,
    # one bar = one day). A daily bar only completes at session close, so
    # this screener scans ONCE per day (~15:35 IST) rather than at multiple
    # intraday bar-close times — a signal today is only actionable tomorrow
    # at the earliest. 53-stock NIFTY50 universe. Full IS+OOS: 2,937 trades,
    # WR 74.0%, Sharpe 6.27(IS)/6.65(OOS). Stage 12 overnight-gap tail risk
    # (-5.63% worst 1%ile) and Stage 13 G-13-C stress-scenario (130.6% on a
    # Rs.50L book) both accepted 2026-08-12 as documented costs.
    # ⚠️  SCREENER, NOT AN ORDER-PLACING BOT — never calls placeorder(). Writes
    #   candidates/open_positions into logs/vp_swing_screener_daily_state.json
    #   for the dashboard. A candidate becomes a tracked position only via the
    #   dashboard's manual "Confirm" action, never automatically. Not in
    #   bot_registry.py or performance_review.py (zero performance.db rows,
    #   same convention as the 60-min screener).
    {
        'name': 'VP Swing Screener (Daily)',
        'script': 'live_trading/vp_swing_screener_daily/vp_swing_screener_daily.py',
        'description': (
            'Touch of rolling 10-day volume-profile lower extreme (daily bars) → long '
            'candidate, target = rolling POC, SL=3%. Scans once/day ~15:45 IST. No forced '
            'EOD close, no pyramiding. 53-stock NIFTY50 universe. IS+OOS Sharpe 6.27/6.65, '
            'WR 74.0%, n=2,937. Capital basis Rs.50L (Stage 13). SIGNAL-ONLY — never calls '
            'placeorder(); positions tracked only after manual "Confirm" on the dashboard.'
        ),
        # Fires once/day at 15:45 IST, after is_trading_hours() already reads market
        # CLOSED (cutoff 15:40) — same shape as the retired EMA Swing Scanner above.
        # Without this flag, monitor_bots() would terminate() this process at the
        # 15:40 close detection, 5 min before its own internal SCAN_TIME ever fires.
        'is_daily_scheduler': True,   # does NOT use market-hours loop — manages own schedule
    },

    # NIFTY ATM Straddle Scalp Bot — APPROVED FOR PAPER TRADING ✅
    # (research/atm_short_straddle_scalp_study/, re-validated after margin recalibration, 2026-08-13)
    # Research summary: options_data/research/atm_short_straddle_scalp_study/FINDINGS.md, DECISIONS.md
    #   Strategy: SELL ATM CE + SELL ATM PE (short straddle), single fixed daily entry 10:30 IST.
    #   Per-leg SL: broker-side SL-M at entry_premium*1.20 (20% adverse move). Once one leg stops,
    #   survivor's SL is trailed to its own entry price (breakeven), held for target or EOD.
    #   Target: combined P&L >= 0.75% of margin utilized (static margin = 13.26% of notional,
    #   mirrors real Fyers MIS margin — see margin_calibration D13). EOD 15:14 IST hard exit.
    #   No DTE floor (min_dte=0) — Stage 10 expiry segmentation validated DTE=0/expiry-day trades.
    #   Champion config: 10:30_sl20_tgt0.75 — ALL stages 0-11 PASS after margin recalibration.
    #   10 lots/leg, MIS, NFO weekly NIFTY options.
    {
        'name': 'NIFTY ATM Straddle Scalp Bot',
        'script': 'live_trading/nifty_atm_straddle_scalp_bot/nifty_atm_straddle_scalp_bot.py',
        'description': (
            'SELL ATM CE+PE (short straddle), single fixed entry 10:30 IST. '
            'Per-leg SL 20% (broker SL-M), survivor trailed to breakeven on sibling stop. '
            'Target 0.75% of margin (13.26% of notional, static). EOD 15:14 IST. '
            'No DTE floor (min_dte=0). 10 lots/leg, MIS, NFO weekly. '
            'Champion 10:30_sl20_tgt0.75 — ALL 0-11 stages pass. Paper trading.'
        )
    },
]



class BotLauncher:
    def __init__(self):
        self.processes = []
        self.running = True
        self.lock_file = Path("live_trading/.launcher.lock")
        self.api_key = os.getenv("OPENALGO_API_KEY")
        if not self.api_key:
            logger.error("❌ OPENALGO_API_KEY not found. Launcher will exit.")
            sys.exit(1)
        self._check_lock()
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        # Pulse-override cache: avoid hammering WS every 30 s when Fyers API
        # falsely reports a holiday during live trading hours.
        self._pulse_override_result: bool | None = None   # None = not yet checked
        self._pulse_override_ts: float = 0.0              # epoch of last check
        self._pulse_fail_streak: int = 0                  # consecutive no-tick checks
        # 2026-07-06: 3 EMA-spread bots died mid-session and launcher.log showed
        # zero activity for ~7 hours around it — no way to tell whether the
        # supervisor loop itself had stalled. This counter drives a periodic
        # "still alive" log line so a future stall is visible as a gap, not silence.
        self._heartbeat_cycles = 0
        # Bot name -> ISO date of last clean EOD exit. Without this, monitor_bots()'s
        # "if not self.processes: start ALL bots" branch would restart a bot that already
        # hit its own EOD cutoff today, which would immediately exit again and (on the
        # fyers_cs instance, same launcher shape) flooded the log tearing down/rebuilding
        # the broker WS adapter every cycle from EOD to 15:30 (2026-07-21/22 incident).
        self._eod_exited_today: dict[str, str] = {}

    def _check_lock(self):
        """Prevent multiple launcher instances"""
        if self.lock_file.exists():
            try:
                # Check if the process in the lock file is still alive
                pid = int(self.lock_file.read_text().strip())
                os.kill(pid, 0) # Throws error if process is dead
                logger.error(f"❌ Another launcher is already running (PID: {pid}). Exit.")
                sys.exit(1)
            except (ProcessLookupError, ValueError, FileNotFoundError):
                # Lock file is stale
                self.lock_file.unlink(missing_ok=True)
        
        self.lock_file.write_text(str(os.getpid()))
    
    def signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        logger.info("\n🛑 Shutdown signal received. Stopping all bots...")
        self.running = False
        self.stop_all_bots()
        if self.lock_file.exists():
            self.lock_file.unlink()
        sys.exit(0)
    
    def send_telegram(self, message):
        """Helper to send telegram messages"""
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id: return
        try:
            import requests
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}, timeout=5)
        except Exception as e:
            logger.error(f"Telegram notification failed: {e}")
    
    def is_bot_running(self, script_path):
        """Check if a bot script is already running via psutil — this instance only"""
        import psutil
        script_path_str = str(Path(script_path))
        abs_script_path = str((Path(__file__).parent.parent / script_path).resolve())
        project_root = str(Path(__file__).parent.parent.resolve())
        for proc in psutil.process_iter(['pid', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline')
                if not cmdline:
                    continue
                cmd_str = " ".join(cmdline)
                if proc.info['pid'] == os.getpid():
                    continue

                # Match the exact configured script path first, then fallback to absolute path.
                if script_path_str in cmd_str or abs_script_path in cmd_str:
                    # Guard: only match processes whose CWD is this project root.
                    # Without this, we'd falsely detect (and kill) bots from the other
                    # account's openalgo instance running the same script names.
                    try:
                        if proc.cwd() != project_root:
                            continue
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                    return proc.info['pid']
            except Exception:
                pass
        return None

    def start_bot(self, bot_config):
        """Start a single bot or adopt if infra and already running"""
        try:
            # Check if bot is already running (especially important for infra bots)
            existing_pid = self.is_bot_running(bot_config['script'])
            if existing_pid:
                if bot_config.get('is_infra'):
                    logger.info(f"ℹ️ {bot_config['name']} is already running (PID: {existing_pid}). Adopting...")
                    self.processes.append({
                        'name': bot_config['name'],
                        'process': None, # No handle for existing process
                        'pid': existing_pid,
                        'script': bot_config['script'],
                        'is_infra': True
                    })
                    return True
                else:
                    logger.warning(f"⚠️ {bot_config['name']} seems already running (PID: {existing_pid}) but is NOT infra. Will kill and restart.")
                    try:
                        import psutil
                        psutil.Process(existing_pid).terminate()
                    except: pass

            logger.info(f"🚀 Starting {bot_config['name']}...")
            logger.info(f"   {bot_config['description']}")
            
            # Use 'uv run python' — NEVER sys.executable directly.
            # sys.executable captures the venv path at import time and becomes stale
            # whenever uv rebuilds the environment (e.g. after uv sync / Python upgrade).
            # 'uv run' always resolves the correct interpreter for the current project.
            env = os.environ.copy()
            env["PYTHONPATH"] = str(Path(__file__).parent.parent) + os.pathsep + env.get("PYTHONPATH", "")

            log_dir = Path("live_trading/logs")
            log_dir.mkdir(exist_ok=True)
            log_file_name = f"{bot_config['name'].replace(' ', '_').lower()}.log"
            log_file_path = log_dir / log_file_name

            log_file = open(log_file_path, 'a')
            process = subprocess.Popen(
                ["uv", "run", "python", bot_config['script']],
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid
            )
            
            self.processes.append({
                'name': bot_config['name'],
                'process': process,
                'pid': process.pid,
                'script': bot_config['script'],
                'log_file': log_file,
                'is_infra': bot_config.get('is_infra', False),
                'is_daily_scheduler': bot_config.get('is_daily_scheduler', False),
            })
            
            logger.info(f"✅ {bot_config['name']} started (PID: {process.pid}). Logs: {log_file_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to start {bot_config['name']}: {e}")
            return False
    
    def stop_all_bots(self):
        """Stop all running bots (infrastructure and daily-scheduler bots are preserved)"""
        for bot in self.processes:
            try:
                if bot.get('is_infra'):
                    logger.info(f"🟢 Preserving infra bot: {bot['name']} (PID: {bot.get('pid', 'Unknown')})")
                    continue
                if bot.get('is_daily_scheduler'):
                    logger.info(f"📅 Preserving daily-scheduler bot: {bot['name']} (PID: {bot.get('pid', 'Unknown')})")
                    continue

                logger.info(f"Stopping {bot['name']} (PID: {bot['pid']})...")
                if bot.get('process'):
                    bot['process'].terminate()
                else:
                    # Adopted bot (not supported for stopping yet, but safety check)
                    import psutil
                    try: psutil.Process(bot['pid']).terminate()
                    except: pass
                
                # Wait for graceful shutdown
                try:
                    bot['process'].wait(timeout=5)
                    logger.info(f"✅ {bot['name']} stopped gracefully")
                except subprocess.TimeoutExpired:
                    logger.warning(f"⚠️  {bot['name']} didn't stop gracefully, forcing...")
                    bot['process'].kill()
                    logger.info(f"✅ {bot['name']} force stopped")
            
            except Exception as e:
                logger.error(f"Error stopping {bot['name']}: {e}")
    
    # ── Registry & Command-file helpers ──────────────────────────────────────

    def write_registry(self):
        """Write current bot PIDs and statuses to launcher_registry.json.

        Called after every start/stop and on every 5-second heartbeat so the
        Streamlit dashboard always has a fresh view of what is running.
        """
        import psutil
        registry = {
            "launcher_pid": os.getpid(),
            "launcher_running": True,
            "updated_at": datetime.now().isoformat(),
            "bots": {},
        }

        # Bots currently tracked in self.processes
        running_names: set[str] = set()
        for bot in self.processes:
            name = bot["name"]
            running_names.add(name)
            pid  = bot.get("pid")
            alive = False
            if pid:
                try:
                    alive = psutil.pid_exists(pid)
                except Exception:
                    alive = False
            registry["bots"][name] = {
                "pid":                pid,
                "status":             "running" if alive else "stopped",
                "script":             bot.get("script", ""),
                "is_infra":           bot.get("is_infra", False),
                "is_daily_scheduler": bot.get("is_daily_scheduler", False),
            }

        # Bots in BOTS config that are NOT running (so the dashboard can show them)
        for config in BOTS:
            if config["name"] not in running_names:
                registry["bots"][config["name"]] = {
                    "pid":                None,
                    "status":             "stopped",
                    "script":             config.get("script", ""),
                    "is_infra":           config.get("is_infra", False),
                    "is_daily_scheduler": config.get("is_daily_scheduler", False),
                }

        reg_path = Path("live_trading/logs/launcher_registry.json")
        try:
            reg_path.write_text(json.dumps(registry, indent=2))
        except Exception as e:
            logger.error(f"Failed to write registry: {e}")

    def poll_commands(self):
        """Read bot_commands.json and execute any pending start/stop commands.

        The Streamlit dashboard writes commands here; the launcher picks them up
        within ~5 seconds and acts on them.
        """
        cmd_path = Path("live_trading/logs/bot_commands.json")
        if not cmd_path.exists():
            return
        try:
            raw = cmd_path.read_text().strip()
            if not raw or raw == "[]":
                return
            commands = json.loads(raw)
            if not commands:
                return
            # Clear the file immediately so we don't double-process
            cmd_path.write_text("[]")
        except Exception as e:
            logger.error(f"Failed to read bot_commands.json: {e}")
            return

        for cmd in commands:
            action   = cmd.get("action", "")
            bot_name = cmd.get("bot", "")
            logger.info(f"📬 Dashboard command: {action!r} → {bot_name!r}")

            if action == "stop" and bot_name:
                self._stop_single_bot(bot_name)
            elif action == "start" and bot_name:
                self._start_single_bot(bot_name)
            elif action == "stop_all":
                logger.info("📬 STOP ALL — stopping all non-infra, non-scheduler bots")
                self.stop_all_bots()
                # Remove stopped bots from process list (keep infra + daily-scheduler)
                self.processes = [
                    p for p in self.processes
                    if p.get("is_infra") or p.get("is_daily_scheduler")
                ]
                self.send_telegram("🛑 *Stop All* command executed via dashboard.")
            elif action == "start_all":
                logger.info("📬 START ALL — starting all configured bots")
                running_names = {p["name"] for p in self.processes}
                bots_to_start = [c for c in BOTS if c["name"] not in running_names]
                for i, config in enumerate(bots_to_start):
                    self.start_bot(config)
                    if i < len(bots_to_start) - 1:
                        time.sleep(5)   # stagger to avoid Fyers 429 rate limiting
                self.send_telegram("🚀 *Start All* command executed via dashboard.")

            self.write_registry()

    def _stop_single_bot(self, bot_name: str):
        """Stop a single named bot gracefully."""
        bot = next((b for b in self.processes if b["name"] == bot_name), None)
        if not bot:
            logger.warning(f"⚠️  Stop requested for '{bot_name}' — not found in running processes")
            return
        try:
            pid = bot.get("pid", "?")
            logger.info(f"🛑 Stopping {bot_name} (PID: {pid}) via dashboard command...")
            if bot.get("process"):
                bot["process"].terminate()
                try:
                    bot["process"].wait(timeout=5)
                    logger.info(f"✅ {bot_name} stopped gracefully")
                except subprocess.TimeoutExpired:
                    bot["process"].kill()
                    logger.info(f"✅ {bot_name} force-killed after timeout")
            else:
                import psutil
                try:
                    psutil.Process(bot["pid"]).terminate()
                except Exception:
                    pass
            self.processes.remove(bot)
            self.send_telegram(f"🛑 *{bot_name}* stopped via dashboard.")
        except Exception as e:
            logger.error(f"Error stopping {bot_name}: {e}")

    def _start_single_bot(self, bot_name: str):
        """Start a single named bot by looking up its config in BOTS."""
        config = next((b for b in BOTS if b["name"] == bot_name), None)
        if not config:
            logger.warning(f"⚠️  Start requested for '{bot_name}' — no matching config in BOTS list")
            return
        already = next((p for p in self.processes if p["name"] == bot_name), None)
        if already:
            logger.warning(f"⚠️  Start requested for '{bot_name}' — already running (PID: {already.get('pid')})")
            return
        self.start_bot(config)
        self.send_telegram(f"🚀 *{bot_name}* started via dashboard.")

    def is_trading_hours(self):
        """Check if current time is within 08:50-15:40 IST on a weekday and not a holiday.

        15:40 (not 15:30) since the NSE Closing Auction Session (CAS) rollout,
        2026-08-03, moved equity F&O close 15:30->15:40 — see nse_cas_aug_2026_eod_timing
        memory / vp_swing_reversion_daily_study WORKLOG.md 2026-08-13 for the incident
        that caught this constant still being stale here after the screener files
        themselves were already fixed on 2026-08-12.

        Evaluation order (most reliable → least reliable):
          1. Weekend check          — pure wall-clock, no API.
          2. Time-bounds check      — pure wall-clock, no API.
             Checked BEFORE the holiday API so that the clean 15:40 exit fires
             regardless of whether fyers_token_service is reachable.
          3. Holiday check          — Fyers market_status API (primary),
             OpenAlgo API fallback (secondary).
          4. Market Pulse override  — if the holiday API fires during expected
             trading hours (weekday, 08:50–15:40) but live WebSocket ticks are
             flowing, trust the ticks over the API.  Result cached 5 min to avoid
             hammering the WS on every 30 s monitor loop.
             NOTE: weekend guard (step 1) prevents this path on Sat/Sun, which is
             important because NSE runs mandatory broker test sessions on some
             weekends where real ticks flow but trades are paper-only.
        """
        now = datetime.now()
        current_time = now.time()

        # 1. Weekend — no API needed, and pulse override must never fire here
        #    (NSE test sessions on Sat/Sun produce live ticks that look real).
        if now.weekday() >= 5:
            return False, "Weekend"

        # 2. Time bounds — wall-clock only, no API dependency.
        #    MUST come before the holiday API call so that the 15:40 clean exit
        #    always fires even when fyers_token_service is unreachable.
        if current_time < dt_time(8, 50):
            return False, "Before Market Open (Starts at 08:50)"
        if current_time > dt_time(15, 40):
            return False, "After Market Close (Stopped at 15:40)"

        # 3. Holiday check — only reached on weekdays between 08:50 and 15:40.
        is_trading, reason = is_nse_fo_trading_day_via_fyers(self.api_key)
        if not is_trading:
            if "not configured" in reason or "not set" in reason:
                # Fyers check not configured — fall back to OpenAlgo holiday API
                if self.api_key and is_market_holiday(self.api_key, now):
                    return False, f"Market Holiday Today ({now.strftime('%Y-%m-%d')})"
            else:
                # 4. Fyers API says holiday — verify with Market Pulse before trusting it.
                #    A false-holiday from a transient API error should not cost us the
                #    entire trading session (as happened on 2026-06-19).
                if self._pulse_override_cached():
                    logger.warning(
                        f"⚠️  Fyers API says holiday ({reason}) but live ticks are "
                        "flowing — overriding to OPEN. Check if today is a real holiday."
                    )
                    return True, "Trading Session Active (Pulse override — Fyers API may be stale)"
                return False, f"Market Holiday ({reason})"

        return True, "Trading Session Active"

    def _pulse_override_cached(self) -> bool:
        """
        Run wait_for_market_pulse() and cache the result.

        Called only when Fyers API returns a definitive 'holiday' signal during
        weekday trading hours.  A confirmed-open result is cached for 5 minutes
        to avoid hammering the WS on every 30 s monitor loop iteration.

        A no-ticks result is NOT trusted on the first miss — a single 30 s gap
        in the tick stream (WS hiccup, reconnect, quiet moment) isn't proof of
        a holiday, and immediately stopping every bot on it is expensive:
        on 2026-07-09 exactly this happened, the whole fleet was stopped for
        ~5 minutes, and banknifty_bb_opening_candle_bot's bar aggregator
        wasn't alive during its one-shot 09:15→09:16 entry-signal window,
        permanently costing it that day's trade. Two consecutive misses (each
        re-checked on the next ~30 s monitor cycle, so ~30-60s apart) are now
        required before we actually cache a negative result and let the
        caller stop bots.

        Returns True if live ticks were detected (market is physically open).
        """
        CACHE_TTL = 300     # seconds — re-check a confirmed-open result at most once every 5 minutes
        RETRY_TTL = 5       # seconds — re-check an unconfirmed miss almost immediately
        now_ts = time.time()

        cache_ttl = CACHE_TTL if self._pulse_override_result else RETRY_TTL
        if (
            self._pulse_override_result is not None
            and now_ts - self._pulse_override_ts < cache_ttl
        ):
            return self._pulse_override_result

        logger.info("🔍 Pulse override check: Fyers says holiday — verifying via WebSocket ticks…")
        ok, reason = self.wait_for_market_pulse()

        if ok:
            self._pulse_fail_streak = 0
            self._pulse_override_result = True
            self._pulse_override_ts = now_ts
            logger.info(f"   ✅ Pulse override: ticks detected ({reason})")
            return True

        self._pulse_fail_streak += 1
        if self._pulse_fail_streak < 2:
            logger.warning(
                f"   ⚠️  Pulse override: no ticks ({reason}) — not yet confirmed "
                f"(miss {self._pulse_fail_streak}/2), keeping prior state"
            )
            self._pulse_override_ts = now_ts
            # Don't flip to closed on a single miss — hold the previous result
            # (default to open if this is the very first check of the day).
            self._pulse_override_result = (
                self._pulse_override_result if self._pulse_override_result is not None else True
            )
            return self._pulse_override_result

        self._pulse_override_result = False
        self._pulse_override_ts = now_ts
        logger.info(f"   ❌ Pulse override: no ticks ({reason}) — holiday confirmed (2/2 misses)")
        return False

    def monitor_bots(self):
        """Monitor running bots and handle auto start/stop based on time"""
        logger.info("🕒 Automated Scheduler Active (08:50 pre-market / 15:40 IST)")
        
        while self.running:
            try:
                is_open, reason = self.is_trading_hours()
                
                if is_open:
                    if not self.processes:
                        today = datetime.now().date().isoformat()
                        bots_to_start = [
                            b for b in BOTS if self._eod_exited_today.get(b['name']) != today
                        ]
                        if not bots_to_start:
                            # Every bot already had its clean EOD exit today —
                            # is_trading_hours() is still True until 15:40, but
                            # there's nothing left to start until tomorrow.
                            pass
                        else:
                            logger.info(
                                f"🔔 Market is OPEN. Starting bot(s): "
                                f"{', '.join(b['name'] for b in bots_to_start)}..."
                            )
                            for i, config in enumerate(bots_to_start):
                                self.start_bot(config)
                                if i < len(bots_to_start) - 1:
                                    time.sleep(2)   # stagger to avoid 429 rate limiting
                    else:
                        # Monitor existing processes
                        import psutil
                        for bot in self.processes[:]:
                            if bot.get('process'):
                                poll = bot['process'].poll()
                                is_dead = (poll is not None)
                            else:
                                # Adopted bot, check via PID
                                try:
                                    is_dead = not psutil.pid_exists(bot['pid'])
                                except: is_dead = True

                            if is_dead:
                                exit_code = bot['process'].poll() if bot.get('process') else None
                                clean_exit = (exit_code == 0)
                                if clean_exit:
                                    logger.info(f"✅ {bot['name']} (PID: {bot['pid']}) shut down cleanly (exit 0) — not restarting")
                                    self._eod_exited_today[bot['name']] = datetime.now().date().isoformat()
                                else:
                                    logger.error(f"❌ {bot['name']} (PID: {bot['pid']}) stopped unexpectedly (exit {exit_code})")
                                self.processes.remove(bot)
                                if self.running and not clean_exit:
                                    logger.info(f"🔄 Restarting {bot['name']}...")
                                    config = next((b for b in BOTS if b['script'] == bot['script']), None)
                                    if config: self.start_bot(config)
                else:
                    if self.processes:
                        logger.info(f"🛑 Market is CLOSED ({reason}). Stopping all bots...")
                        self.stop_all_bots()
                        self.processes = []

                    if "After Market Close" in reason:
                        logger.info("✅ Session complete. Launcher exiting cleanly.")
                        sys.exit(0)

                    # Log state once an hour if waiting (pre-market / holiday / weekend)
                    if datetime.now().minute == 0:
                        logger.info(f"💤 Sleeping... Reason: {reason}")

            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")

            self._heartbeat_cycles += 1
            if self._heartbeat_cycles % 10 == 0:   # every ~5 min (10 × 30s cycles)
                logger.info(f"💓 Supervisor alive — tracking {len(self.processes)} bot(s)")

            # ── Heartbeat: poll dashboard commands every 5s, full market check every 30s ──
            for _ in range(6):   # 6 × 5s = 30s per market-check cycle
                time.sleep(5)
                self.poll_commands()
                self.write_registry()

    def cleanup_stray_bots(self):
        """Kill any existing bot processes to avoid duplicates — this instance only"""
        import psutil
        current_pid = os.getpid()
        project_root = str(Path(__file__).parent.parent.resolve())
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline')
                if not cmdline: continue
                cmd_str = " ".join(cmdline)
                # Kill if it's one of our bots but NOT this launcher
                if proc.info['pid'] != current_pid and any(script in cmd_str for script in [
                    # "bollinger_options_bot.py",  # RETIRED 2026-03-21 — Strategy A confirmed losing
                    # "candle_breaker_bot.py",     # RETIRED 2026-03-21 — no predictive signal
                    # "orb_champion_15m_bot.py",   # RETIRED 2026-03-21 — IS Sharpe -1.09, OOS -1.75
                    # "tick_stasher.py",           # RETIRED 2026-04-27 — no active bot consumes live_ticks.duckdb
                    "telegram_status.py",
                    # "preopen_gap_fade_bot.py",   # RETIRED 2026-06-24 — no post-cost edge
                    # "gap_fade_eod_bot.py",       # RETIRED 2026-06-04
                    # "daily_sniper_bot_v2.py",   # RETIRED 2026-03-21 — no OOS edge
                    # "bb_paper_bot.py",           # RETIRED 2026-03-21 — no OOS edge (best +0.97)
                    # "bb_5m_scanner.py",          # REPLACED 2026-03-21 by nifty_bb_overbought_bot
                    "banknifty_bb_opening_candle_bot.py",
                    "nifty_bb_overbought_bot.py",
                    "nifty_trend_seller_bot.py",
                    "sensex_trend_seller_bot.py",
                    "htf_po3_bot.py",
                    # "banknifty_bb_options_bot.py",  # DEPLOYED LIVE on fyers_cs 2026-06-29
                    "bb_mean_reversion_bot.py",
                    # "ha_options_bot.py",  # RETIRED 2026-07-10 — failed paper-trading gate
                    "nifty_eod_hold_bot.py",
                    "nifty_iron_fly_weekly_bot.py",
                    "sensex_iron_fly_weekly_bot.py",
                    "banknifty_iron_fly_monthly_bot/main.py",
                    "macd_m2_sell_options_bot.py",
                    "banknifty_trend_pullback_positional_bot.py",
                    "nifty_gex_ict_v2_bot.py",
                    # "ema_swing_scanner/main.py",  # RETIRED 2026-06-04
                    # "equity_obi_bot.py",  # RETIRED 2026-06-19
                ]):
                    # CRITICAL: Skip killing infrastructure bots (persist across launcher restarts)
                    is_infra = any(script in cmd_str for script in ["telegram_status.py"])
                    if is_infra:
                        logger.info(f"🟢 Skipping infra bot: PID {proc.info['pid']} ({cmd_str})")
                        continue

                    logger.warning(f"🧹 Killing stray bot process: PID {proc.info['pid']} ({cmd_str})")
                    proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    
    def wait_for_market_pulse(self):
        """
        Verify the market is actually 'alive' by waiting for a tick on NIFTY or BANKNIFTY.
        This prevents starting bots on holidays that are missing from the calendar,
        or when the data feed is down.
        """
        if os.getenv("SKIP_PULSE_CHECK") == "1":
            return True, "Pulse check skipped via env"

        logger.info("📡 Verifying Market Pulse (waiting for live ticks)...")
        
        async def check_pulse():
            # Use WEBSOCKET_URL from .env if available, fallback to constructed URL from HOST_SERVER
            ws_url = os.getenv("WEBSOCKET_URL")
            if not ws_url:
                host = os.getenv("HOST_SERVER", "http://127.0.0.1:5001")
                ws_url = host.replace("http", "ws") + "/ws"
            
            api_key = self.api_key
            
            try:
                # Use a short timeout for the connection itself
                ws = await asyncio.wait_for(websockets.connect(ws_url), timeout=5)
                async with ws:
                    await ws.send(json.dumps({
                        "action": "authenticate",
                        "api_key": api_key
                    }))
                    
                     # Subscribe to major indices and VIX
                    # Note: Using mode 1 (LTP only) for pulse check
                    # Index symbols must match the database: NIFTY/BANKNIFTY on NSE_INDEX
                    for sym in ["NIFTY", "BANKNIFTY", "INDIAVIX"]:
                        await ws.send(json.dumps({
                            "action": "subscribe",
                            "symbol": sym,
                            "exchange": "NSE_INDEX",
                            "mode": 1
                        }))
                    
                    start_time = time.time()
                    # Wait up to 30 seconds for a pulse
                    while time.time() - start_time < 30:
                        try:
                            msg_raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                            msg = json.loads(msg_raw)
                            if msg.get("type") == "market_data":
                                logger.info(f"💓 Market Pulse Detected! Received tick for {msg.get('symbol')}")
                                return True, "Pulse verified"
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            logger.debug(f"Pulse check recv error: {e}")
                            
                    return False, "No market ticks received in 30 seconds"
            except Exception as e:
                return False, f"Pulse check connection failed: {e}"

        try:
            success, reason = asyncio.run(check_pulse())
            return success, reason
        except Exception as e:
            return False, f"Pulse check runner error: {e}"

    def start_all(self):
        """Start all bots"""
        logger.info("="*80)
        logger.info("LIVE (ANALYZER) TRADING - MULTI-BOT LAUNCHER")
        logger.info("="*80)
        
        # 0. Cleanup stray processes
        self.cleanup_stray_bots()

        # 1. Start Telegram handler (Infrastructure)
        try:
            from live_trading.telegram_status import start_in_thread
            logger.info("🤖 Telegram /status handler starting...")
            start_in_thread()
            logger.info("✅ Telegram handler running in background")
        except Exception as e:
            logger.error(f"❌ Failed to start Telegram handler: {e}")

        # 1.5 Notification timing moved below holiday check

        # 2. Initial Session Check — Prevent starting trading bots on holidays/weekends/off-hours
        is_open, reason = self.is_trading_hours()
        
        if is_open:
            # 2.1 Market Pulse Check (Verification of live ticks)
            pulse_ok, pulse_reason = self.wait_for_market_pulse()
            if not pulse_ok:
                logger.warning(f"⚠️  Market Pulse Check FAILED: {pulse_reason}")
                logger.warning("Market might be closed despite calendar settings (or data feed is down).")
                logger.warning("Downgrading to Safe Mode: Trading bots will NOT start.")
                is_open = False
                reason = f"Pulse Check Failed ({pulse_reason})"

        if is_open:
            self.send_telegram("🚀 *Multi-Bot Launcher Started*\nVerified: Market is OPEN. Starting all bots.")
            logger.info(f"🟢 Market is OPEN. Starting {len(BOTS)} bots...")
            for i, config in enumerate(BOTS):
                self.start_bot(config)
                if i < len(BOTS) - 1:
                    # Stagger bot startups by 5s each so their initial API calls
                    # (expiry fetch, multiquotes, WebSocket auth) don't all hit
                    # the Fyers rate limit simultaneously.
                    time.sleep(5)
        else:
            # On holidays or weekends, only notify once that we are in standby
            if "Holiday" in reason or "Weekend" in reason:
                self.send_telegram(f"💤 *Launcher Standby*\nReason: {reason}")
            
            logger.info(f"💤 Market is CLOSED ({reason}).")
            # Only start infrastructure bots (e.g. Tick Stasher)
            infra_bots = [b for b in BOTS if b.get('is_infra')]
            if infra_bots:
                logger.info(f"🛠️  Starting {len(infra_bots)} infrastructure bots...")
                for config in infra_bots:
                    self.start_bot(config)
            
            trading_bots = [b for b in BOTS if not b.get('is_infra')]
            trading_bot_names = [b['name'] for b in trading_bots]
            logger.info(f"⏳ {len(trading_bots)} trading bot(s) queued for market open: {', '.join(trading_bot_names)}")
        
        logger.info("="*80)
        logger.info("Launcher monitoring active. Press Ctrl+C to stop.")
        self.write_registry()   # Initial registry snapshot for dashboard
        self.monitor_bots()

    def _resolve_bot_configs(self, selectors: list[str]) -> list[dict]:
        """Resolve --only selector strings to BOTS[] configs.

        Each selector is tried as an exact (case-insensitive) name match first,
        falling back to a substring match against the name or script path. Raises
        SystemExit listing available bot names if a selector matches nothing —
        this runs from a CLI entry point, not the supervisor loop, so failing loud
        and exiting is correct here.
        """
        resolved: list[dict] = []
        for sel in selectors:
            sel_lower = sel.strip().lower()
            if not sel_lower:
                continue
            exact = [c for c in BOTS if c["name"].lower() == sel_lower]
            matches = exact or [
                c for c in BOTS
                if sel_lower in c["name"].lower() or sel_lower in c["script"].lower()
            ]
            if not matches:
                available = "\n  ".join(c["name"] for c in BOTS)
                raise SystemExit(f"❌ No bot matched '{sel}'. Available bots:\n  {available}")
            resolved.extend(matches)

        # De-dupe while preserving first-seen order (e.g. overlapping selectors)
        seen: set[str] = set()
        out: list[dict] = []
        for c in resolved:
            if c["name"] not in seen:
                seen.add(c["name"])
                out.append(c)
        return out

    def _write_registry_merge(self):
        """Update only this invocation's bots in launcher_registry.json, leaving
        every other entry untouched.

        write_registry() is the full-fleet snapshot used by the persistent
        supervisor — it stamps every bot NOT in self.processes as "stopped",
        which is correct there (it started, or explicitly deferred, everything).
        A --only run only ever touches 1-2 bots, so that same overwrite would
        falsely mark every other bot in the fleet as stopped in the file the
        dashboard reads. Merge instead.
        """
        import psutil
        reg_path = Path("live_trading/logs/launcher_registry.json")
        try:
            registry = json.loads(reg_path.read_text()) if reg_path.exists() else {}
        except Exception:
            registry = {}
        registry.setdefault("bots", {})
        registry["launcher_pid"] = os.getpid()
        registry["launcher_running"] = False  # one-off run, not the persistent supervisor
        registry["updated_at"] = datetime.now().isoformat()

        for bot in self.processes:
            pid = bot.get("pid")
            alive = bool(pid) and psutil.pid_exists(pid)
            registry["bots"][bot["name"]] = {
                "pid":                pid,
                "status":             "running" if alive else "stopped",
                "script":             bot.get("script", ""),
                "is_infra":           bot.get("is_infra", False),
                "is_daily_scheduler": bot.get("is_daily_scheduler", False),
            }

        try:
            reg_path.write_text(json.dumps(registry, indent=2))
        except Exception as e:
            logger.error(f"Failed to write registry: {e}")

    def start_selected(self, selectors: list[str]):
        """(Re)start only the named bot(s) via the protected start_bot() launch
        path, without touching the rest of the fleet, market-hours gating, or the
        persistent monitor_bots() loop. See module docstring for --only usage.
        """
        configs = self._resolve_bot_configs(selectors)
        logger.info(f"🎯 --only: starting {len(configs)} bot(s): {[c['name'] for c in configs]}")
        for i, config in enumerate(configs):
            self.start_bot(config)
            if i < len(configs) - 1:
                time.sleep(5)  # same Fyers-rate-limit stagger as start_all()
        self._write_registry_merge()
        # One-off run, not the persistent supervisor — release the lock so the
        # next launch (of any kind) doesn't have to wait out a stale-PID check.
        try:
            if self.lock_file.read_text().strip() == str(os.getpid()):
                self.lock_file.unlink(missing_ok=True)
        except Exception:
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--only", type=str, default=None,
        help="Comma-separated bot name(s) to (re)start, e.g. "
             '"VP Swing Screener,VP Swing Screener (Daily)". Bypasses market-hours '
             "gating and the persistent supervisor loop; exits after starting.",
    )
    args = parser.parse_args()

    launcher = BotLauncher()
    if args.only:
        launcher.start_selected(args.only.split(","))
    else:
        launcher.start_all()
