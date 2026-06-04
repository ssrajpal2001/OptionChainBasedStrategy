#!/usr/bin/env bash
# fresh_start.sh — pull latest code + wipe ALL runtime state (positions, history, logs) and
# restart the bot clean. One command for "start fresh".
#
# KEEPS:  data/clients.db (broker creds) and data/strategy_config.json (your config — gitignored,
#         so git reset never touches it).
# WIPES:  data/positions/*.json, data/history/*.json, all logs (app + clients + trades + PM2),
#         and optionally data/recorded/* parquet ticks.
#
# Usage:  bash scripts/fresh_start.sh [INDEX]        # default NIFTY
#         bash scripts/fresh_start.sh CRUDEOIL
#         WIPE_CONFIG=1 bash scripts/fresh_start.sh NIFTY   # ALSO wipe strategy_config.json
#                                                            # (then re-clone: clone_index_config.py)
set -e
cd "$(dirname "$0")/.."
INDEX="${1:-NIFTY}"

echo "== fresh_start: index=$INDEX =="

# 1. stop the bot
pm2 stop algo 2>/dev/null || true

# 2. pull latest code (config + creds are gitignored -> untouched by reset)
git fetch origin && git reset --hard origin/master

# 3. wipe ALL runtime state + logs
rm -f  data/positions/*.json 2>/dev/null || true
rm -f  data/history/*.json   2>/dev/null || true
rm -rf logs/trades/*         2>/dev/null || true
rm -rf logs/clients/*        2>/dev/null || true
rm -f  logs/*.log            2>/dev/null || true
# rm -f data/recorded/*      2>/dev/null || true   # uncomment to also wipe recorded ticks
if [ "${WIPE_CONFIG:-0}" = "1" ]; then
    rm -f data/strategy_config.json 2>/dev/null || true
    echo "   strategy_config.json WIPED — run: python3 scripts/clone_index_config.py to rebuild"
fi
pm2 flush algo 2>/dev/null || true   # clear PM2 stdout/err logs

# 4. confirm clean
echo "   positions: $(ls -1 data/positions/ 2>/dev/null | wc -l) | history: $(ls -1 data/history/ 2>/dev/null | wc -l)"

# 5. restart clean (delete first so args/env are fresh)
pm2 delete algo 2>/dev/null || true
pm2 start run_system.py --name algo --interpreter python3 -- --mode live --ui --port 5000 --index "$INDEX" --strategies sell_straddle
pm2 save

echo "== fresh start complete on $INDEX. Watch: pm2 logs algo =="
