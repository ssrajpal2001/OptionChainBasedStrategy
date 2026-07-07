#!/usr/bin/env bash
# scripts/full_clean_restart.sh — wipe logs + runtime state and restart terminus fresh.
#
# Run on EC2 from inside the repo:
#   bash scripts/full_clean_restart.sh
#
# Keeps:
#   data/strategy_config.json
#   config/client_profiles.json
# (backed up to backups/YYYYMMDD_HHMMSS_full_clean/)
#
# Deletes:
#   logs/*, logs/trades/*.log
#   data/positions/*, data/live_records/*, data/recorded/*, data/history/*, data/nse_option_cache/*
#   data/state_snapshots.db, data/trade_history.db, data/client_trade_history.db
#
# Restarts terminus live with NIFTY sell_straddle by default.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP_DIR="${REPO_DIR}/backups/${TIMESTAMP}_full_clean"

INDEX="${1:-NIFTY}"
STRATEGIES="${2:-sell_straddle}"
PORT="${3:-5000}"
LOG_LEVEL="${4:-INFO}"

log() { echo "[$(date +%H:%M:%S)] $*"; }

cd "${REPO_DIR}"

log "=== Full clean restart (index=${INDEX}, strategies=${STRATEGIES}) ==="
log "Backup dir: ${BACKUP_DIR}"

# Backup config
mkdir -p "${BACKUP_DIR}"
cp -n data/strategy_config.json "${BACKUP_DIR}/" 2>/dev/null || log "strategy_config.json not found, skipping backup"
cp -n config/client_profiles.json "${BACKUP_DIR}/" 2>/dev/null || log "client_profiles.json not found, skipping backup"

# Stop terminus
log "Stopping PM2 terminus..."
pm2 stop terminus || true

# Wipe logs
log "Wiping logs..."
rm -rf logs/* logs/trades/*.log 2>/dev/null || true

# Wipe runtime state
log "Wiping runtime state files..."
rm -rf data/positions/* data/live_records/* data/recorded/* data/history/* data/nse_option_cache/* 2>/dev/null || true
rm -f data/state_snapshots.db data/trade_history.db data/client_trade_history.db 2>/dev/null || true

# Remove old PM2 process and start fresh
log "Starting terminus live..."
pm2 delete terminus || true
pm2 start run_system.py --name terminus -- \
    --mode live \
    --index "${INDEX}" \
    --strategies "${STRATEGIES}" \
    --ui --port "${PORT}" \
    --log-level "${LOG_LEVEL}"

pm2 save

log "=== Restart complete ==="
log "Monitor: pm2 logs terminus"
log "Backup:  ${BACKUP_DIR}"
