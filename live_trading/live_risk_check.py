"""
live_risk_check.py - Real-time Risk & Solvency Engine for Live Trading.
Connects to OpenAlgo to verify funds, spreads, and margin availability.
"""
import os
import sys
import json
from openalgo import api
from dotenv import load_dotenv

# Load platform configuration
load_dotenv()

def check_live_vitals(symbol=None, exchange=None, requested_qty=None):
    """
    Performs a live safety check before order execution.
    Returns a status report and a boolean 'safe_to_trade'.
    """
    api_key = str(os.getenv("OPENALGO_API_KEY", ""))
    host = os.getenv("HOST_SERVER", "http://127.0.0.1:8080")
    
    try:
        client = api(api_key=api_key, host=host)
        report = {"status": "success", "checks": {}}
        safe = True

        # 1. Fund & Mode Check
        funds_resp = client.funds()
        if funds_resp and 'data' in funds_resp:
            # Report the mode (Analyze/Paper vs Live/Real)
            report["platform_mode"] = funds_resp.get("mode", "unknown").upper()
            
            fund_data = funds_resp['data']
            # Fyers uses 'availablecash' in sandbox/analyze mode
            available = float(fund_data.get('available_margin', 0) or 
                             fund_data.get('cash', 0) or 
                             fund_data.get('availablecash', 0) or 0)
            
            report["checks"]["funds"] = {
                "available": available, 
                "status": "healthy" if available > 0 else "warning",
                "mode_context": "Simulated Capital" if report["platform_mode"] == "ANALYZE" else "Broker Capital"
            }
            if available <= 0:
                safe = False
        else:
            report["checks"]["funds"] = {"status": "error", "message": "Could not fetch fund data"}
            safe = False

        # 2. Liquidity (Spread) Check
        if symbol and exchange:
            quotes_resp = client.quotes(symbol=symbol, exchange=exchange)
            if quotes_resp and 'data' in quotes_resp:
                q = quotes_resp['data']
                ltp = float(q.get('ltp', 0))
                bid = float(q.get('bid', 0))
                ask = float(q.get('ask', 0))
                
                if bid > 0 and ask > 0:
                    spread_pts = ask - bid
                    spread_pct = (spread_pts / ltp) * 100
                    
                    report["checks"]["liquidity"] = {
                        "ltp": ltp,
                        "spread_points": round(spread_pts, 2),
                        "spread_pct": round(spread_pct, 2),
                        "status": "healthy" if spread_pct < 0.5 else "caution"
                    }
                    # If spread is too wide (>1%), it's a hard stop for the guardian
                    if spread_pct > 1.0:
                        safe = False
                else:
                    report["checks"]["liquidity"] = {"status": "warning", "message": "Incomplete depth data"}
            else:
                report["checks"]["liquidity"] = {"status": "error", "message": "Quotes API unreachable"}
                safe = False

        report["safe_to_trade"] = safe
        return report

    except Exception as e:
        return {
            "status": "error",
            "message": f"Risk engine failure: {str(e)}",
            "safe_to_trade": False
        }

if __name__ == "__main__":
    # Example: python live_risk_check.py NIFTY-I NSE_INDEX
    sym = sys.argv[1] if len(sys.argv) > 1 else None
    exch = sys.argv[2] if len(sys.argv) > 2 else None
    
    print("--- 🛡️ Live Execution Guardian: Risk Scan ---")
    results = check_live_vitals(sym, exch)
    print(json.dumps(results, indent=2))
    
    if not results["safe_to_trade"]:
        print("\n🚨 RISK ALERT: Safety thresholds not met! Manual intervention suggested.")
        sys.exit(1)
    else:
        print("\n✅ SYSTEM HEALTHY: All thresholds passed.")
        sys.exit(0)
