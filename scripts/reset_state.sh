#!/usr/bin/env bash
# reset_state.sh — wipe runtime state for a FRESH start.
# PRESERVES: data/clients.db (broker creds) and data/strategy_config.json (your config).
# Run with the app STOPPED.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Resetting runtime state (keeping clients.db + strategy_config.json)…"

# 1. Open-position state (sell_straddle / iron_condor / trap restore files)
rm -f data/positions/*.json 2>/dev/null || true

# 2. Closed-trade history (dashboard History view)
rm -f data/history/*.json 2>/dev/null || true

# 3. Per-client trade logs + app logs
rm -rf logs/trades/* 2>/dev/null || true
rm -f  logs/*.log    2>/dev/null || true

# 4. (Optional) recorded parquet ticks — uncomment to also clear
# rm -f data/recorded/* 2>/dev/null || true

echo "Done. Remaining in data/:"
ls -1 data/ 2>/dev/null || true
echo "positions:"; ls -1 data/positions/ 2>/dev/null || echo "  (empty)"
echo "history:";   ls -1 data/history/   2>/dev/null || echo "  (empty)"
