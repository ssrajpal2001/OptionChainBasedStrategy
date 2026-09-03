#!/usr/bin/env bash
# scripts/ec2_restart_crudeoil.sh — safe restart procedure for the refactored
# sell-straddle on CRUDEOIL (and NIFTY).
#
# Run on EC2 from inside the repo:
#   bash scripts/ec2_restart_crudeoil.sh
#
# What it does:
#   1. Backs up data/strategy_config.json and config/client_profiles.json
#   2. Stops the PM2 terminus process
#   3. Deletes CRUDEOIL position/session/state files
#   4. Runs a 30-second paper/ghost smoke test
#   5. Restarts terminus in live mode with NIFTY,CRUDEOIL
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKUP_DIR="${REPO_DIR}/backups/$(date +%Y%m%d_%H%M%S)"
PM2_INDEX="NIFTY,CRUDEOIL"

log() { echo "[$(date +%H:%M:%S)] $*"; }

cd "${REPO_DIR}"

log "=== EC2 CRUDEOIL restart ==="
log "Repo: ${REPO_DIR}"
log "Backup dir: ${BACKUP_DIR}"

# 1) Backup config files
mkdir -p "${BACKUP_DIR}"
for f in data/strategy_config.json config/client_profiles.json; do
    if [[ -f "$f" ]]; then
        cp -v "$f" "${BACKUP_DIR}/"
    else
        log "WARNING: $f not found, skipping backup"
    fi
done

# 2) Stop the running terminus process
if pm2 pid terminus >/dev/null 2>&1; then
    log "Stopping PM2 process 'terminus'..."
    pm2 stop terminus
else
    log "PM2 process 'terminus' not running, skipping stop"
fi

# 3) Clean CRUDEOIL state
log "Cleaning CRUDEOIL position/session/state files..."
python3 scripts/clean_instrument_state.py CRUDEOIL

# 4) Smoke test in paper/ghost mode
log "Running 30s paper smoke test for CRUDEOIL..."
SMOKE_SECONDS=30 python3 scripts/smoke_crudeoil.py
log "Smoke test PASSED"

# 5) Restart terminus live
log "Restarting terminus live with index=${PM2_INDEX}..."
pm2 restart terminus || \
    pm2 start run_system.py --name terminus -- \
        --mode live \
        --index "${PM2_INDEX}" \
        --strategies sell_straddle \
        --ui --port 5000 \
        --log-level INFO

pm2 save

log "=== Restart complete ==="
log "Monitor with: pm2 logs terminus"
log "Backup saved at: ${BACKUP_DIR}"
