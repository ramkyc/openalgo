# BANKNIFTY Monthly Iron Fly — Paper Trading Bot
# =============================================
# Strategy: Short ATM straddle + long OTM hedge wings, delta-based intraday adjustments
# Champion params from research/banknifty_iron_fly_monthly_study_v2/ (v2 full-history
# re-study, 2020-2026, all 10 stages passed 2026-07-05 — supersedes v1's
# banknifty_iron_fly_monthly_study, which only covered Jan 2024-Apr 2026)
#
# Champion config (v2, full-history OOS winner):
#   hedge_delta   : 0.10  (Black-76 delta for OTM wings)
#   adj_low_trig  : 0.20  (delta < 0.20 → adjust short leg upward)
#   adj_hi_trig   : 0.75  (delta > 0.75 → adjust short leg downward)
#   profit_target : 0.50  (50% of net premium → buy to close all legs)
#   stop_loss     : None  (no SL — adjustments handle adverse moves)
#
# Entry   : First trading day after prior monthly expiry at 10:00 IST
# Exit    : 15:15 IST on the calendar day before expiry
# DTE     : ≥ 2 at entry
#
# 4 legs — all NRML:
#   SELL ATM CE  (short)
#   SELL ATM PE  (short)
#   BUY  OTM CE  (delta ≈ 0.10 — hedge)
#   BUY  OTM PE  (delta ≈ 0.10 — hedge)
#
# Adjustments: if any short leg delta exits [0.20, 0.75] for 2 consecutive bars,
#              close that short leg and re-open at current ATM (same expiry).
#              Unadjusted deltas mean position is within acceptable range.
#
# NIFTY / SENSEX: v2 Stage 9 tested this exact config on both as a real
# cross-instrument test. NIFTY is NOT recommended (unresolved 2024 loss
# cluster, Sharpe 0.862 vs BANKNIFTY's own). SENSEX's Sharpe 3.836 is on a
# 2.6-year sample only (monthly options don't exist before Nov 2023) and is
# not usable as deployment evidence either way. BANKNIFTY only for now.
#
# Stage 11 gate (paper trading):
#   ≥ 20 sessions, WR within ±10% of v2 OOS (58.1%), net P&L positive, max loss ≤ 2× expected stop