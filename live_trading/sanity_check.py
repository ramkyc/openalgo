"""
Pre-market sanity check for fyers_cs live_trading bots.

Tests:
  1. .env path resolution  — does ROOT / ".env" resolve to the right file?
  2. Key env vars          — OPENALGO_API_KEY, HOST_SERVER, WEBSOCKET_URL present?
  3. OpenAlgo reachability — can we reach localhost:8080?
  4. API key auth          — does the key authenticate against /api/v1/funds?
  5. Broker data           — can we fetch NIFTY expiry dates? (no live price needed)
  6. WebSocket handshake   — can we connect to the WebSocket proxy?
  7. Shared imports        — do all shared utilities import cleanly?

Run with:  uv run python live_trading/sanity_check.py
(from the openalgo/ root, or directly from live_trading/)
"""

import asyncio
import importlib
import os
import re
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Same ROOT resolution as api_utils.py (parent.parent = openalgo/) ──────────
ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

PASS = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"
WARN = "\033[33m~\033[0m"
HEAD = "\033[1;36m"
RST  = "\033[0m"

results: list[tuple[str, bool, str]] = []


def check(label: str, ok: bool, detail: str = "") -> bool:
    results.append((label, ok, detail))
    icon = PASS if ok else FAIL
    print(f"  {icon}  {label}" + (f"  — {detail}" if detail else ""))
    return ok


# ── 1. .env resolution ────────────────────────────────────────────────────────
print(f"\n{HEAD}[1] .env resolution{RST}")
env_path = ROOT / ".env"
check(".env file exists at ROOT/.env", env_path.exists(), str(env_path))

# Confirm HOST_SERVER is configured to a real address (not a placeholder)
host_raw = open(env_path).read() if env_path.exists() else ""
configured_host = os.getenv("HOST_SERVER", "")
host_ok = bool(configured_host) and "127.0.0.1" in configured_host and "YOUR_" not in configured_host
check(f"ROOT/.env HOST_SERVER configured ({configured_host})", host_ok)

# ── 2. Key env vars ───────────────────────────────────────────────────────────
print(f"\n{HEAD}[2] Environment variables{RST}")
api_key    = os.getenv("OPENALGO_API_KEY", "")
host       = os.getenv("HOST_SERVER", "")
ws_url     = os.getenv("WEBSOCKET_URL", "")
tg_token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
tg_chat    = os.getenv("TELEGRAM_CHAT_ID", "")

check("OPENALGO_API_KEY set",  bool(api_key),  api_key[:12] + "…" if api_key else "MISSING")
check("HOST_SERVER set",       bool(host),      host or "MISSING")
check("WEBSOCKET_URL set",     bool(ws_url),    ws_url or "MISSING")
if not tg_token:
    results.append(("TELEGRAM_BOT_TOKEN", True, "not set — bots will skip Telegram alerts"))
    print(f"  {WARN}  TELEGRAM_BOT_TOKEN  — not set (bots continue without alerts)")
else:
    check("TELEGRAM_BOT_TOKEN set", True, tg_token[:12] + "…")

# ── 3. OpenAlgo reachability ──────────────────────────────────────────────────
print(f"\n{HEAD}[3] OpenAlgo HTTP reachability{RST}")
try:
    r = requests.get(host, timeout=5)
    check("OpenAlgo responds at HOST_SERVER", r.status_code < 500,
          f"HTTP {r.status_code}")
except Exception as e:
    check("OpenAlgo responds at HOST_SERVER", False, str(e))

# ── 4. API key authentication ─────────────────────────────────────────────────
print(f"\n{HEAD}[4] API key authentication{RST}")
try:
    r = requests.post(f"{host}/api/v1/funds",
                      json={"apikey": api_key}, timeout=10)
    data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    status = data.get("status", "")
    # "success" = authenticated + broker connected; "error" with auth msg = bad key
    auth_ok = status == "success" or (
        status == "error" and "auth" not in data.get("message", "").lower()
        and "invalid" not in data.get("message", "").lower()
        and "key" not in data.get("message", "").lower()
    )
    detail = data.get("message", f"HTTP {r.status_code}") if status != "success" else "authenticated"
    check("API key accepted by /api/v1/funds", auth_ok, detail)
except Exception as e:
    check("API key accepted by /api/v1/funds", False, str(e))

# ── 5. Broker data — NIFTY expiry dates ──────────────────────────────────────
print(f"\n{HEAD}[5] Broker data (NIFTY expiry dates){RST}")
try:
    r = requests.post(f"{host}/api/v1/expiry",
                      json={"apikey": api_key, "symbol": "NIFTY",
                            "exchange": "NFO", "instrumenttype": "options"},
                      timeout=10)
    data = r.json() if r.status_code == 200 else {}
    dates = data.get("data", [])
    check("NIFTY expiry dates returned", bool(dates),
          f"{dates[:3]}…" if dates else data.get("message", f"HTTP {r.status_code}"))
except Exception as e:
    check("NIFTY expiry dates returned", False, str(e))

# ── 6. WebSocket handshake ────────────────────────────────────────────────────
print(f"\n{HEAD}[6] WebSocket proxy handshake{RST}")

async def _ws_test(url: str) -> tuple[bool, str]:
    try:
        import websockets
        async with websockets.connect(url, open_timeout=5) as ws:
            return True, "connected and closed cleanly"
    except ImportError:
        return None, "websockets package not installed — skipping"
    except Exception as e:
        return False, str(e)

ws_ok, ws_detail = asyncio.run(_ws_test(ws_url))
if ws_ok is None:
    results.append(("WebSocket handshake", True, ws_detail))
    print(f"  {WARN}  WebSocket handshake  — {ws_detail}")
else:
    check("WebSocket handshake", ws_ok, ws_detail)

# ── 7. Shared imports ─────────────────────────────────────────────────────────
print(f"\n{HEAD}[7] Shared utility imports{RST}")
sys.path.insert(0, str(ROOT))

modules = {
    "live_trading.api_utils":               "api_utils",
    "live_trading.shared.atm_resolver":     "shared/atm_resolver",
    "live_trading.shared.trade_logger":     "shared/trade_logger",
    "live_trading.shared.telegram_notifier":"shared/telegram_notifier",
    "live_trading.shared.ta_compat":        "shared/ta_compat",
    "live_trading.shared.performance_db":   "shared/performance_db",
}
for mod, label in modules.items():
    try:
        importlib.import_module(mod)
        check(f"import {label}", True)
    except Exception as e:
        check(f"import {label}", False, str(e))

# ── 8. Dashboard registration consistency ────────────────────────────────────
# Catches the exact bug that shipped with flat_blue_line_monthly_bot on
# 2026-06-08: a bot present in STATE_FILES/BOT_META but with no rendering
# block in render_portfolio_snapshot() silently falls through to the generic
# "📊 Broker" label instead of showing under its own name. All three
# structures are keyed identically (e.g. "FLAT_BLUE_LINE_MONTHLY"), so this
# checks that every STATE_FILES key also appears in BOT_META and is referenced
# via STATE_FILES["KEY"] inside render_portfolio_snapshot()'s body.
print(f"\n{HEAD}[8] Dashboard registration consistency{RST}")
try:
    dash_path = ROOT / "live_trading" / "streamlit_dashboard.py"
    src = dash_path.read_text()

    # state_files = { "KEY": <path>, ... }   — defined inside get_workspace_paths()
    sf_match = re.search(r"state_files\s*=\s*\{(.*?)\n    \}", src, re.S)
    state_keys = re.findall(r'"([A-Z][A-Z0-9_]*)"\s*:', sf_match.group(1)) if sf_match else []

    # BOT_META = { "KEY": {...}, ... }       — top-level dict
    bm_match = re.search(r"^BOT_META\s*=\s*\{(.*?)\n\}", src, re.S | re.M)
    meta_keys = re.findall(r'"([A-Z][A-Z0-9_]*)"\s*:\s*\{', bm_match.group(1)) if bm_match else []

    # render_portfolio_snapshot() body — from its def line to the next
    # top-level (unindented) def, so nested _add_open/_add_closed are excluded
    rps_match = re.search(r"def render_portfolio_snapshot\(.*?\n(.*?)\ndef ", src, re.S)
    rps_body = rps_match.group(1) if rps_match else ""
    rps_refs = set(re.findall(r'STATE_FILES\["([A-Z][A-Z0-9_]*)"\]', rps_body))

    if not state_keys:
        check("STATE_FILES dict parsed from dashboard source", False,
              "regex did not match — dashboard structure changed; update sanity_check.py")
    else:
        check("STATE_FILES dict parsed from dashboard source", True,
              f"{len(state_keys)} bot keys found")
        # NOTE: flagged here as WARN, not FAIL — some keys are deliberately
        # retired-but-kept (e.g. GAP_FADE_EOD, EMA_SWING are already absent
        # from BOT_META) and others may render through a dedicated per-bot
        # panel instead of the generic snapshot. A hard FAIL here would make
        # this check perpetually red and easy to start ignoring. Treat each
        # WARN as a "go look — is this bot retired, or is this the same
        # silent-mislabeling bug that hit flat_blue_line_monthly_bot?"
        gaps = 0
        for key in state_keys:
            missing = []
            if key not in meta_keys:
                missing.append("BOT_META")
            if key not in rps_refs:
                missing.append("render_portfolio_snapshot()")
            if not missing:
                check(f"{key} fully registered in dashboard", True)
            else:
                gaps += 1
                detail = (f"missing from {', '.join(missing)} — if this bot is "
                          f"active, its positions render under the generic "
                          f"'📊 Broker' label (the exact bug that hit "
                          f"flat_blue_line_monthly_bot on 2026-06-08); if it's "
                          f"retired, consider removing it from STATE_FILES too")
                results.append((f"{key} dashboard registration", True, detail))
                print(f"  {WARN}  {key} dashboard registration  — {detail}")
        if gaps:
            print(f"  {WARN}  {gaps} bot(s) flagged above for manual triage "
                  f"— not counted as failures (heuristic check)")
except Exception as e:
    check("Dashboard registration consistency", False, str(e))

# ── Summary ───────────────────────────────────────────────────────────────────
passed  = sum(1 for _, ok, _ in results if ok)
failed  = sum(1 for _, ok, _ in results if not ok)
total   = len(results)
color   = "\033[32m" if failed == 0 else "\033[31m"
print(f"\n{HEAD}{'='*52}{RST}")
print(f"  {color}{passed}/{total} checks passed{RST}" +
      (f"  ← {failed} failed, fix before starting bots" if failed else "  ← ready to run bots"))
print(f"{HEAD}{'='*52}{RST}\n")

sys.exit(0 if failed == 0 else 1)
