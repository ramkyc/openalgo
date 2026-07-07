# Equity OBI Bot - Visual Explanation

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      EQUITY OBI BOT ARCHITECTURE                              │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌─────────────┐       ┌──────────────┐       ┌─────────────┐
    │  OBI Engine │       │   REST API   │       │ Market Data  │
    │ (WebSocket) │◄────►│  (OpenAlgo)   │◄────►│ (1-min bars) │
    └─────────────┘       └──────┬───────┘       └─────────────┘
                                 │
                          ┌──────┴─────────┐
                          │  OBI Engine    │
                          │  - get_snapshot│
                          │  - get_rolling │
                          │  - tick_count  │
                          └────────────────┘
                                   │
    ┌──────────────────────────────┼──────────────────────────────┐
    │                              │                                │
    ▼                              ▼                                ▼
┌──────────────┐         ┌─────────────────┐         ┌─────────────────┐
│ TELEGRAM BOT │         │  POSITION MGR   │         │   CSV LOGGER    │
│ - Alerts     │         │  - Enter        │         │  - signals.csv  │
│ - EOD Report │         │  - Exit         │         │  - trades.csv   │
│ - State      │         │  - Ghost Track  │         │  - ghosts.csv   │
└──────────────┘         └─────────────────┘         └─────────────────┘
```

---

## Signal Detection Flow (per symbol)

```
┌─────────────────────────────────────────────────────────────────────┐
│                    ENTRY EVALUATION (Every Minute)                    │
│               (10:00 – 13:00, then 14:00 – 15:15)                    │
└─────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────────────────────────────┐
    │  START                                                        │
    └──────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
    ┌──────────────────────────────────────────────────────────────┐
    │  1. Is daily trade done?                                      │
    │     ┌───────────┐                                           │
    │     │ trade_done│                                          │
    │     │    True   │──┐                                       │
    │     └───────────┘  │ NO                                    │
    │                    ▼ YES                                    │
    └──────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
    ┌──────────────────────────────────────────────────────────────┐
    │  2. Is OBI warmup period passed?                              │
    │     tick_count >= 50                                           │
    │     ┌───────────┐                                           │
    │     │    NO     │──┐ NO (skip evaluation)                   │
    │     └───────────┘  │ YES                                    │
    └──────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
    ┌──────────────────────────────────────────────────────────────┐
    │  3. Fetch 1-min bars & compute EMA(20)                        │
    │     - Close the still-forming bar                             │
    │     - Calculate EMA using Wilder's formula                    │
    └──────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
    ┌──────────────────────────────────────────────────────────────┐
    │  4. Trend Filter Check                                         │
    │     ┌─────────────────────────────────────┐                 │
    │     │ close > EMA(20) AND VWMP < LTP      │──────► LONG     │
    │     │ close < EMA(20) AND VWMP > LTP      │──────► SHORT    │
    │     │ else                                │──────► NO SIGNAL │
    │     └─────────────────────────────────────┘                 │
    └──────────────────────────────────────────────────────────────┘
                                   │
                   ┌───────────────┼───────────────┐
                   │               │               │
                   ▼               ▼               ▼
    ┌────────────────────┐ ┌────────────────┐ ┌────────────────────┐
    │   LONG SIGNAL      │ │   SHORT       │ │    NO SIGNAL        │
    │   detected         │ │   detected    │ │    (exit)           │
    └────────────────────┘ └────────────────┘ └────────────────────┘
                   │               │               │
                   ▼               ▼               │
    ┌────────────────────┐               │         │
    │   OBI Gate Check   │◄──────────────┘         │
    │   ┌───────────────┐│                         │
    │   │ rolling_obi   ││                         │
    │   │ > +20 (LONG)  ││                         │
    │   │ vwmp < ltp    ││                         │
    │   │ n_levels >= 20││                         │
    │   │ !stale        ││                         │
    │   └──────┬─────────┘│                         │
    │          │ YES     ││                         │
    │   ┌──────┴───────┐ └─► Enter Position + Notify    │
    │   │  ALL PASSED  │                                 │
    │   └──────────────┘                                 │
    │          │ NO                                      │
    │   ┌──────┴───────┐                                  │
    │   │ NOT ALL PASS │                                  │
    │   └──────┬───────┘                                  │
    │          │ NO                                       │
    │          ▼ YES                                      │
    │   ┌─────────────────────┐                           │
    │   │   START GHOST TRACK │◄────┐                    │
    │   │   (blocked trade)   │     │                    │
    │   └─────────────────────┘     │                    │
    └───────────────────────────────┘                    │
                   │                                      │
                   ▼                                      │
    ┌──────────────────────────────────────────────────────────────┐
    │  5. Log to CSV                                                 │
    │     - signals.csv (always)                                     │
    │     - trades.csv (if entered)                                  │
    │     - ghosts.csv (if blocked)                                  │
    └──────────────────────────────────────────────────────────────┘
```

---

## Position Lifecycle

```
┌─────────────────────────────────────────────────────────────────────┐
│                        POSITION STATE MACHINE                         │
└─────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐
    │   ENTRY      │ ◄─── Entry window (09:25 – 15:15)
    └──────┬───────┘
           │ Order placed (paper mode)
           │
    ┌──────▼────────────┐
    │  OPEN POSITION    │
    │  ┌───────────────┐│
    │  │ direction     ││
    │  │ entry_price   ││
    │  │ shares        ││
    │  │ sl_price      ││  ←── Monitored every minute
    │  │ target_price  ││      Check P&L against these
    │  │ obi_at_signal ││
    │  └──────┬─────────┘│
    └─────────┼──────────┘
              │
    ┌─────────┼─────────────────────────────────────────┐
    │         │                                          │
    ▼         ▼                                          ▼
┌─────────┐ ┌─────────────────┐                 ┌─────────────────┐
│ SL HIT  │ │ TARGET HIT      │                 │    EOD EXIT     │
│         │ │                  │                 │ (15:25 sharp)  │
└────┬────┘ └─────────────────┘                 └─────────────────┘
     │                                              Exit all positions
     │ Place square-off order                       (LONG → SELL,
     │ Mark exit reason: "SL_HIT"                   SHORT → BUY)
     │
     ▼                                             
┌──────────────────┐
│   CLOSED POSITION │
│   Log to CSV     │
│   Clear state    │
└──────────────────┘
```

---

## Ghost Tracking (Counterfactuals)

```
┌─────────────────────────────────────────────────────────────────────┐
│                     GHOST TRACKING LOGIC                             │
│     "What would have happened if OBI had approved the trade?"       │
└─────────────────────────────────────────────────────────────────────┘

    ┌─────────────────────────┐
    │ OBI Gate REJECTED       │
    │ (blocked due to:        │
    │  - rolling_obi too weak │
    │  - VWMP condition fail  │
    │  - stale data           │
    │  - low depth levels)    │
    └──────────┬──────────────┘
               │
               ▼
    ┌─────────────────────────┐
    │  START GHOST            │
    │  ├─ Same entry logic    │
    │  ├─ Same SL/target      │
    │  ├─ Mark as "blocked"   │
    │  └─ Track price move    │
    └──────────┬──────────────┘
               │
    ┌──────────▼──────────────────────┐
    │  MONITOR GHOST STATE            │
    │  ┌────────────────────────────┐│
    │  │ GHOST EXIT CONDITIONS:     ││
    │  │ • Price hits SL (-0.4%)    ││  ←─ Apply same rules
    │  │ • Price hits target (+0.6%)││     as real trade
    │  │ • EOD (15:25)              ││
    │  └────────────────────────────┘│
    │                                │
    │  ┌────────────────────────────┐│
    │  │ If exited at SL:           ││
    │  │   ghost_pnl = NEGATIVE     ││  ←─ Shows OBI saved us!
    │  └────────────────────────────┘│
    │                                │
    │  ┌────────────────────────────┐│
    │  │ If exited at target:       ││
    │  │   ghost_pnl = POSITIVE     ││  ←─ Opportunity missed
    │  └────────────────────────────┘│
    └────────────────────────────────┘
               │
               ▼
    ┌─────────────────────────┐
    │  Log to ghosts.csv      │
    │  ├─ What was blocked    │
    │  ├─ Ghost exit reason   │
    │  └─ Counterfactual PnL  │
    └─────────────────────────┘
```

---

## Daily Timeline

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              TRADING DAY                                       │
└─────────────────────────────────────────────────────────────────────────────┘

    09:15 │ Market Open
          │
    09:25 │ ──── ENTRY WINDOW OPENS ────────
          │     • Bot subscribes to OBI WebSocket
          │     • State resets (positions cleared)
          │     • Warmup begins (50 ticks)
          │
    09:25-15:15 │ ──── LIVE ENTRY EVALUATION (every minute) ────
                    • Check conditions for RELIANCE & HDFCBANK
                    • Enter LONG if bullish signals
                    • Enter SHORT if bearish signals
                    • OBI gate passes → place order
                    • OBI gate fails → start ghost tracking
          │
    15:15 │ ──── ENTRY WINDOW CLOSES ────────
          │     • No new entries after this
          │     • Only position monitoring
          │
    15:15-15:25 │ ──── EXIT WINDOW ──────────
                    • P&L checks continue
                    • SL/Target exits trigger immediately
                    • No new entries
          │
    15:25 │ ──── EOD EXIT ──────────────────
          │     • Force-close all positions
          │     • Force-close all ghosts
          │     • Generate summary report
          │     • Send Telegram summary
          │     • Stop WebSocket subscription
    15:25 │ ──── DAY ENDS ───────────────────
          │
```

---

## State File Structure

```
equity_obi_state.json
├─ timestamp              # Last save time
├─ session_active         # True if bot is running
├─ paper_mode             # True (Stage 11)
├─ obi_ticks              # WebSocket tick count
└─ symbols                # Per-symbol state
    ├─ RELIANCE           │
    │  ├─ position        │  {symbol, direction, entry_price, shares, sl_price, target_price, signal_time, obi_at_signal}
    │  ├─ ghost           │  {same fields + ghost_exit_time, ghost_exit_price, exit_reason, ghost_pnl}
    │  └─ trade_done      # False = can enter, True = skipped
    └─ HDFCBANK           # Same structure
```

---

## CSV Log Files

```
signals.csv          # Every signal bar (regardless of OBI gate)
┌──────────────────────────────────────────────────────────────────┐
│ date │ signal_time │ symbol  │ direction │ ltp │ entry │ ema20 │ obi_rolling │ obi_gate_pass │
├──────┼───────────────┼─────────┼───────────┼─────┼───────┼───────┼─────────────┼────────────────┤
│ 2026-│ 09:55:00     │ RELIANCE│ LONG      │5025.│ 5025. │ 5022. │ +18.2       │ False 🚫        │
│ /05  │               │         │           │     │       │       │             │
│      │ 10:05:00     │ HDFCBANK│ SHORT     │478. │ 478. │ 478.5│ -22.1       │ True ✅         │
└──────┴──────────────┴─────────┴───────────┴─────┴───────┴───────┴─────────────┴────────────────┘

trades.csv           # Completed trades only
┌──────────────────────────────────────────────────────────────────┐
│ date │ signal_time │ symbol  │ direction │ entry │ shares │ sl_price │ target │ exit_reason │
├──────┼──────────────┼─────────┼───────────┼───────┼────────┼──────────┼────────┼─────────────┤
│ 2026-│ 10:05:00    │ HDFCBANK │ SHORT     │478.0  │  2083  │ 476.80   │ 480.80 │ TARGET_HIT   │
│ /05  │              │          │           │       │        │          │        │
└──────┴─────────────┴─────────┴───────────┴───────┴────────┴──────────┴────────┴─────────────┘

ghosts.csv           # OBI-blocked counterfactuals
┌──────────────────────────────────────────────────────────────────┐
│ date │ signal_time │ symbol  │ direction │ entry │ shares │ sl_price │ target │ exit_reason │
├──────┼──────────────┼─────────┼───────────┼───────┼────────┼──────────┼────────┼─────────────┤
│ 2026-│ 09:55:00     │ RELIANCE │ LONG      │5025.  │  1990  │ 5010.00  │ 5045.25│ SL_HIT       │
│ /05  │              │          │           │       │        │          │        │
└──────┴─────────────┴─────────┴───────────┴───────┴────────┴──────────┴────────┴─────────────┘
```
