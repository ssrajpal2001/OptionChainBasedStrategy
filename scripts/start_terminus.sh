#!/bin/bash
# Daily startup for the real, current live process (pm2 app name: terminus).
# Run this each morning before market open.
#
# Deliberately does a full delete + fresh `pm2 start` every time, never a
# plain `pm2 restart` -- `pm2 restart` reuses whatever args the process was
# LAST started with, silently dropping any new flag added since (this bit
# us for real on 2026-10-13: --futures-oi-underlyings was missing for a
# full session because of exactly this). A fresh start here guarantees the
# args below are always what's actually running.
#
# Does NOT wipe any data (positions/history/logs) -- unlike the old, stale
# fresh_start.sh -- carried-forward hedge-and-carry positions and today's
# booked P&L must survive a restart, never be wiped.
#
# Usage: bash scripts/start_terminus.sh             # pulls latest code via git first
#        bash scripts/start_terminus.sh --no-pull    # skips the git pull -- use when code
#                                                     # was already synced another way (e.g.
#                                                     # scripts/deploy_direct.sh's rsync step)

set -e
cd "$(dirname "$0")/.."

echo "=== terminus daily start $(date '+%Y-%m-%d %H:%M:%S') ==="

if [ "${1:-}" = "--no-pull" ]; then
    echo "[1/4] Skipping git pull (--no-pull)."
else
    echo "[1/4] Pulling latest code..."
    git pull origin nifty-cascade-v4-indicators
fi

echo "[2/4] Checking dependencies..."
pip install -q -r requirements.txt 2>/dev/null || true

echo "[3/4] Stopping old terminus process (if running)..."
pm2 delete terminus 2>/dev/null || true

echo "[4/4] Starting terminus..."
# 2026-10-15: briefly changed to the full 4-strategy lineup, same evening
# REVERTED per direct user instruction -- "I ONLY WANT TO RUN SELL STRADDLE
# TOMORROW, will let u know if I want to run any other strategy." The
# sell_straddle-only lineup was the correct, deliberate choice all along.
pm2 start run_system.py \
    --name terminus \
    --interpreter python3 \
    -- --mode live --ui --port 5000 --index NIFTY --futures-oi-underlyings NIFTY \
       --strategies sell_straddle

pm2 save

echo ""
echo "=== DONE ==="
pm2 describe terminus | grep "script args"
pm2 list
