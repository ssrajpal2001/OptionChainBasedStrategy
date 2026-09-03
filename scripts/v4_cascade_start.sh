#!/usr/bin/env bash
# scripts/v4_cascade_start.sh — (re)start the V4 Cascade strategy with the
# multi-zone pool engine validated in the July 2026 backtest (PF 2.07).
#
# Usage:
#   bash scripts/v4_cascade_start.sh              # NIFTY, preserve state
#   CLEAN=1 bash scripts/v4_cascade_start.sh      # NIFTY, wipe positions + logs first
#   bash scripts/v4_cascade_start.sh BANKNIFTY    # different index
#
# ENV OVERRIDES (all optional):
#   INDEX                  underlying to trade    (default: NIFTY)
#   PORT                   dashboard port         (default: 5000)
#   TRADING_OFFSETS        comma-separated ATM offsets for CE/PE candidate strikes
#                          (default: 100,200,300 → ATM-100/200/300 for CE, ATM+100/200/300 for PE)
#   CLEAN=1                wipe positions, history, logs before starting
#
# The 3 guards validated in backtest are enforced at the engine level (no env flag needed):
#   - Same-session zone lock (zone.lock_ts.date() must equal bar.timestamp.date())
#   - 15:00 IST new-entry cutoff
#   - max_zone_depth=120 pts (zones wider than 120 pts discarded at HTF discovery)
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
INDEX="${1:-${INDEX:-NIFTY}}"
PORT="${PORT:-5000}"
OFFSETS="${TRADING_OFFSETS:-100,200,300}"
CLEAN="${CLEAN:-0}"
PM2_APP="terminus"

log() { echo "[$(date +%H:%M:%S)] $*"; }

cd "${REPO_DIR}"

log "=== V4 Cascade start (index=${INDEX}, offsets=${OFFSETS}) ==="

# ── 1. Optional clean wipe ───────────────────────────────────────────────────
if [ "$CLEAN" = "1" ]; then
    log "CLEAN=1 — wiping positions, history, logs (data/clients.db kept)..."
    BACKUP_DIR="${REPO_DIR}/backups/$(date +%Y%m%d_%H%M%S)_v4_clean"
    mkdir -p "${BACKUP_DIR}"
    [ -f data/strategy_config.json ] && cp data/strategy_config.json "${BACKUP_DIR}/"
    rm -rf data/positions/* data/history/* 2>/dev/null || true
    rm -rf logs/trades/* logs/clients/* logs/*.log 2>/dev/null || true
    pm2 flush "${PM2_APP}" 2>/dev/null || true
    log "Clean done. Backup: ${BACKUP_DIR}"
fi

# ── 2. Pull latest code ───────────────────────────────────────────────────────
log "Pulling latest code..."
git fetch origin
git merge --ff-only origin/master || {
    log "ERROR: fast-forward failed (branch has diverged). Resolve manually, then re-run."
    exit 1
}

# ── 3. Stop existing PM2 process ─────────────────────────────────────────────
log "Stopping PM2 process '${PM2_APP}'..."
pm2 stop "${PM2_APP}" 2>/dev/null || true

# ── 4. Write ecosystem config with pool engine env vars ──────────────────────
log "Writing ecosystem.config.js (pool_engine=1, offsets=${OFFSETS})..."
cat > ecosystem.config.js <<PMEOF
module.exports = {
  apps: [{
    name: '${PM2_APP}',
    script: 'run_system.py',
    interpreter: 'python3',
    args: '--mode live --ui --index ${INDEX} --strategies v4_cascade --port ${PORT}',
    cwd: __dirname,
    env: {
      V4CASCADE_USE_POOL_ENGINE: '1',
      V4CASCADE_TRACKING_OFFSETS: '${OFFSETS}',
      LOG_LEVEL: 'INFO',
    },
    max_memory_restart: '700M',
    watch: false,
    autorestart: true,
    restart_delay: 5000,
    instances: 1,
    exec_mode: 'fork',
    log_date_format: 'YYYY-MM-DD HH:mm:ss',
    out_file: 'logs/pm2-out.log',
    error_file: 'logs/pm2-err.log',
    merge_logs: true,
  }]
};
PMEOF

# ── 5. Start ──────────────────────────────────────────────────────────────────
log "Starting '${PM2_APP}'..."
pm2 delete "${PM2_APP}" 2>/dev/null || true
pm2 startOrRestart ecosystem.config.js
pm2 save

# ── 6. Health check (wait up to 20s for the dashboard to respond) ─────────────
log "Waiting for dashboard on port ${PORT}..."
for i in $(seq 1 20); do
    if curl -sf "http://localhost:${PORT}/api/system/status" >/dev/null 2>&1; then
        log "Dashboard is up."
        break
    fi
    sleep 1
done

# ── 7. Summary ────────────────────────────────────────────────────────────────
IP="$(curl -s https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]' || echo 'localhost')"
log ""
log "=== Started V4 Cascade on ${INDEX} ==="
log " Dashboard:    http://${IP}:${PORT}"
log " Live logs:    pm2 logs ${PM2_APP}"
log " Status:       pm2 status"
log ""
log " IMPORTANT — after each (re)start:"
log "   Admin -> V4 Cascade -> enter deploy_id -> [Force Ingest Zones]"
log "   This rebuilds the HTF pool zone state from 3 weeks of history."
log "   Takes ~15s. Watch pm2 logs for 'history ingested' confirmation."
log ""
log " Paper vs Live trading_mode is set PER BROKER BINDING in the UI."
log "   paper = simulated fills, no real orders sent to Fyers."
log "   live  = real Fyers orders (ensure Fyers token is valid first)."
