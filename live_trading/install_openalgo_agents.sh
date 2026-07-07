#!/usr/bin/env bash
# install_openalgo_agents.sh — Install / uninstall OpenAlgo launchd agents
#
# Installs two LaunchAgents:
#   com.openalgo.launcher       — start_all_bots.py (all bots, holiday-aware)
#   com.openalgo.depth-recorder — depth_recorder.py (market-hours data capture)
#
# Requirements:
#   • /opt/homebrew/bin/uv must exist
#   • Mac timezone must be Asia/Kolkata
#     Check:  sudo systemsetup -gettimezone
#     Fix:    sudo systemsetup -settimezone Asia/Kolkata
#
# Fyers holiday check (optional but recommended):
#   Add to openalgo/.env:
#     FYERS_TOKEN_BROKER_KEY=<same value as TOKEN_BROKER_KEY in crk_fyers/.env>
#   Without this key the launcher falls back to OpenAlgo's own holiday API.
#
# Usage:
#   bash install_openalgo_agents.sh           # install / reinstall both agents
#   bash install_openalgo_agents.sh uninstall # remove both agents
#
set -euo pipefail

LIVE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCH_AGENTS="${HOME}/Library/LaunchAgents"

AGENTS=(
    "com.openalgo.launcher"
    "com.openalgo.depth-recorder"
)

# ── Uninstall ─────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "uninstall" ]]; then
    for label in "${AGENTS[@]}"; do
        plist="${LAUNCH_AGENTS}/${label}.plist"
        launchctl unload "${plist}" 2>/dev/null || true
        rm -f "${plist}"
        echo "✅  Uninstalled ${label}"
    done
    exit 0
fi

# ── Pre-flight checks ─────────────────────────────────────────────────────────

# 1. uv binary
if [[ ! -x /opt/homebrew/bin/uv ]]; then
    echo "❌  /opt/homebrew/bin/uv not found."
    echo "   Install uv with: brew install uv"
    exit 1
fi

# 2. Plist files exist
for label in "${AGENTS[@]}"; do
    src="${LIVE_DIR}/${label}.plist"
    if [[ ! -f "${src}" ]]; then
        echo "❌  ${src} not found — run from live_trading/ or the repo root"
        exit 1
    fi
done

# 3. Timezone
TZ=$(sudo systemsetup -gettimezone 2>/dev/null | awk '{print $NF}')
if [[ "${TZ}" != "Asia/Kolkata" ]]; then
    echo "⚠️  WARNING: Mac timezone is '${TZ}', not Asia/Kolkata"
    echo "   Bots will use local time, not IST"
    echo "   Fix with: sudo systemsetup -settimezone Asia/Kolkata"
    read -rp "   Continue anyway? [y/N] " yn
    [[ "${yn}" =~ ^[Yy]$ ]] || exit 1
fi

# 4. Check FYERS_TOKEN_BROKER_KEY in .env (advisory only)
ENV_FILE="${LIVE_DIR}/../.env"
if [[ -f "${ENV_FILE}" ]] && ! grep -q "^FYERS_TOKEN_BROKER_KEY=" "${ENV_FILE}"; then
    echo ""
    echo "ℹ️  FYERS_TOKEN_BROKER_KEY is not set in openalgo/.env"
    echo "   Holiday detection will fall back to OpenAlgo's internal API."
    echo "   For authoritative Fyers-sourced holiday data, add to .env:"
    echo "     FYERS_TOKEN_BROKER_KEY=<same value as TOKEN_BROKER_KEY in crk_fyers/.env>"
    echo ""
fi

# ── Install / reinstall ───────────────────────────────────────────────────────
mkdir -p "${LAUNCH_AGENTS}"

for label in "${AGENTS[@]}"; do
    src="${LIVE_DIR}/${label}.plist"
    dst="${LAUNCH_AGENTS}/${label}.plist"

    # Unload existing agent if loaded
    if launchctl list "${label}" &>/dev/null; then
        echo "→  Unloading existing ${label}..."
        launchctl unload "${dst}" 2>/dev/null || true
    fi

    cp "${src}" "${dst}"
    launchctl load "${dst}"
    echo "✅  Installed: ${label}"
done

echo ""
echo "── Installed LaunchAgents ───────────────────────────────────────────────"
echo ""
echo "  com.openalgo.launcher"
echo "    Purpose  : Starts all trading bots at login, holiday-aware"
echo "    Logs     : live_trading/launcher_launchd.log"
echo "    Commands :"
echo "      launchctl start com.openalgo.launcher    # trigger now (test)"
echo "      launchctl stop  com.openalgo.launcher    # stop launcher + bots"
echo "      launchctl list  com.openalgo.launcher    # check status"
echo ""
echo "  com.openalgo.depth-recorder"
echo "    Purpose  : Depth data capture during market hours"
echo "    Logs     : live_trading/depth_recorder_launchd.log"
echo "    Commands :"
echo "      launchctl start com.openalgo.depth-recorder"
echo "      launchctl stop  com.openalgo.depth-recorder"
echo ""
echo "  Uninstall all : bash install_openalgo_agents.sh uninstall"
echo ""
