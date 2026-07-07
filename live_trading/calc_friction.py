"""
calc_friction.py - Helper script for the Trading Backtester Skill.
Calculates accurate Indian market friction (taxes, slippage, brokerage).
"""
import sys
from datetime import datetime

def calculate_friction(price, quantity, is_buy=True, timestamp=None):
    """
    Calculates the total friction for a single leg (entry or exit).
    """
    turnover = price * quantity
    
    # 1. Brokerage (Fixed at ₹20 per order)
    brokerage = 20.0
    
    # 2. Exchange Txn Charges (approx 0.00322% for NSE)
    exchange_charges = turnover * 0.0000322
    
    # 3. STT (0.0625% on Sell side for options, different for stocks)
    stt = 0.0
    if not is_buy:
        stt = turnover * 0.000625
        
    # 4. GST (18% on brokerage + exchange charges)
    gst = (brokerage + exchange_charges) * 0.18
    
    # 5. Dynamic Slippage
    slippage_rate = 0.001 # Default 0.1%
    if timestamp:
        hour = timestamp.hour
        minute = timestamp.minute
        # Early Morning (09:15 - 09:45)
        if (hour == 9 and minute >= 15) or (hour == 9 and minute <= 45):
            slippage_rate = 0.003
        # Close (15:00 - 15:30)
        elif (hour == 15 and minute <= 30):
            slippage_rate = 0.002
            
    slippage_cost = turnover * slippage_rate
    
    total_friction = brokerage + exchange_charges + stt + gst + slippage_cost
    
    return {
        "turnover": round(turnover, 2),
        "brokerage": round(brokerage, 2),
        "taxes": round(exchange_charges + stt + gst, 2),
        "slippage": round(slippage_cost, 2),
        "total": round(total_friction, 2),
        "net_price": round(price + slippage_rate if is_buy else price - slippage_rate, 4)
    }

if __name__ == "__main__":
    # Example usage: python calc_friction.py 150 500 True
    if len(sys.argv) > 3:
        p = float(sys.argv[1])
        q = int(sys.argv[2])
        b = sys.argv[3].lower() == 'true'
        res = calculate_friction(p, q, b)
        print(f"--- Friction Report ---")
        for k, v in res.items():
            print(f"{k.capitalize()}: {v}")
