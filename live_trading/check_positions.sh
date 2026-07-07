#!/bin/bash
# Quick Position Checker for Stateless Live Trading Bots
# Queries the OpenAlgo Platform Sandbox Database

SANDBOX_DB="db/sandbox.db"

echo "=================================================="
echo "📊 LIVE (ANALYZER) TRADING POSITIONS STATUS"
echo "=================================================="
echo ""

if [ ! -f "$SANDBOX_DB" ]; then
    echo "❌ Error: Sandbox database $SANDBOX_DB not found."
    exit 1
fi

echo "🟢 SUPERTREND 5S BOT"
echo "================================"
sqlite3 "$SANDBOX_DB" << EOF
.mode column
.headers on
.width 15 10 25 10 10 12 15
SELECT 
    symbol as "Symbol",
    exchange as "Exch",
    product as "Product",
    quantity as "Qty",
    printf('%.2f', average_price) as "Average",
    printf('%.2f', ltp) as "LTP",
    printf('%.2f', pnl) as "PnL"
FROM sandbox_positions 
WHERE strategy = 'SUPERTREND_5S_LIVE' AND quantity != 0;
EOF

echo ""
echo "🔵 BOLLINGER BANDS BOT"
echo "================================"
sqlite3 "$SANDBOX_DB" << EOF
.mode column
.headers on
.width 15 10 25 10 10 12 15
SELECT 
    symbol as "Symbol",
    exchange as "Exch",
    product as "Product",
    quantity as "Qty",
    printf('%.2f', average_price) as "Average",
    printf('%.2f', ltp) as "LTP",
    printf('%.2f', pnl) as "PnL"
FROM sandbox_positions 
WHERE strategy = 'BB_OPTIONS_LIVE' AND quantity != 0;
EOF

echo ""
echo "=================================================="
echo "📈 TODAY'S SUMMARY (Realized + Unrealized)"
echo "=================================================="

# Function to get total strategy PnL
get_strategy_pnl() {
    local strat=$1
    sqlite3 "$SANDBOX_DB" "SELECT printf('%.2f', COALESCE(SUM(today_realized_pnl + pnl), 0)) FROM sandbox_positions WHERE strategy = '$strat';"
}

get_strategy_open() {
    local strat=$1
    sqlite3 "$SANDBOX_DB" "SELECT COUNT(*) FROM sandbox_positions WHERE strategy = '$strat' AND quantity != 0;"
}

ST_PNL=$(get_strategy_pnl "SUPERTREND_5S_LIVE")
BB_PNL=$(get_strategy_pnl "BB_OPTIONS_LIVE")
ST_OPEN=$(get_strategy_open "SUPERTREND_5S_LIVE")
BB_OPEN=$(get_strategy_open "BB_OPTIONS_LIVE")

echo "Supertrend 5s:   ₹$ST_PNL ($ST_OPEN open)"
echo "Bollinger Bands: ₹$BB_PNL ($BB_OPEN open)"
echo "=================================================="
echo ""

echo "💓 BOT HEARTBEATS (Last Activity)"
echo "=================================================="
now=$(date +%s)

for bot_log in "supertrend_5s_bot.log" "bollinger_options_bot.log"; do
    log_file="live_trading/$bot_log"
    if [ -f "$log_file" ]; then
        last_mod=$(stat -f %m "$log_file")
        diff=$((now - last_mod))
        last_time=$(date -r $last_mod "+%H:%M:%S")
        
        if [ $diff -lt 65 ]; then
            status="✅ ACTIVE"
        else
            status="⚠️  STALE ($diff seconds ago)"
        fi
        
        # Get Start Time
        start_time=$(grep -i "Started\|Initializing" "$log_file" | tail -n 1 | cut -d' ' -f2 | cut -d',' -f1)
        if [ -z "$start_time" ]; then start_time="N/A"; fi
        
        printf "%-28s : %s | Start: %-8s | Last: %-8s\n" "$bot_log" "$status" "$start_time" "$last_time"
    else
        printf "%-28s : ❌ LOG MISSING\n" "$bot_log"
    fi
done
echo "=================================================="
