#!/usr/bin/env bash
# deploy/setup_ec2.sh — first-time EC2 setup for the OptionChain AlgoTrader.
#
# Run once on a FRESH Amazon Linux 2023 / Ubuntu 22.04 instance:
#   curl -fsSL https://raw.githubusercontent.com/YOUR_USER/YOUR_REPO/master/deploy/setup_ec2.sh | bash
#   OR: git clone + bash deploy/setup_ec2.sh
#
# After this script:
#   1. Python 3.11, pip packages, Node.js 20, PM2 are installed.
#   2. The repo is at ~/OptionChainBasedStrategy.
#   3. PM2 starts the bot with V4 Cascade + pool engine enabled.
#   4. Run bash scripts/setup_https.sh to add HTTPS / broker OAuth callback support.
#
# Idempotent — safe to re-run.
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/ssrajpal2001/OptionChainBasedStrategy.git}"
REPO_DIR="${REPO_DIR:-$HOME/OptionChainBasedStrategy}"
INDEX="${INDEX:-NIFTY}"
PORT="${PORT:-5000}"
PM2_APP="${PM2_APP:-terminus}"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# ── 0. OS detection ─────────────────────────────────────────────────────────
if   command -v apt-get &>/dev/null; then PKG_MGR="apt"
elif command -v dnf      &>/dev/null; then PKG_MGR="dnf"
elif command -v yum      &>/dev/null; then PKG_MGR="yum"
else log "ERROR: cannot detect package manager"; exit 1; fi

log "=== EC2 setup begin (PKG_MGR=$PKG_MGR) ==="

# ── 1. System packages ───────────────────────────────────────────────────────
if [ "$PKG_MGR" = "apt" ]; then
    sudo apt-get update -q
    sudo apt-get install -y -q build-essential git curl wget unzip \
        python3.11 python3.11-dev python3.11-venv python3-pip \
        libssl-dev libffi-dev sqlite3
elif [ "$PKG_MGR" = "dnf" ]; then
    sudo dnf install -y git curl wget unzip \
        python3.11 python3.11-devel python3-pip \
        gcc gcc-c++ openssl-devel libffi-devel sqlite
else
    sudo yum install -y git curl wget unzip python3 python3-devel python3-pip \
        gcc openssl-devel libffi-devel sqlite
fi

# Ensure python3.11 is the default python3 on Amazon Linux
if command -v python3.11 &>/dev/null && ! python3 --version 2>&1 | grep -q "3.11"; then
    sudo update-alternatives --install /usr/bin/python3 python3 "$(command -v python3.11)" 10 || true
fi

log "Python: $(python3 --version)"

# ── 2. Node.js 20 + PM2 (process manager) ───────────────────────────────────
if ! command -v node &>/dev/null; then
    log "Installing Node.js 20..."
    curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash - 2>/dev/null || \
    curl -fsSL https://deb.nodesource.com/setup_20.x | sudo bash - 2>/dev/null || true
    if [ "$PKG_MGR" = "apt" ]; then sudo apt-get install -y nodejs
    else sudo dnf install -y nodejs || sudo yum install -y nodejs; fi
fi

if ! command -v pm2 &>/dev/null; then
    log "Installing PM2..."
    sudo npm install -g pm2
fi

log "Node: $(node --version)  PM2: $(pm2 --version)"

# ── 3. Clone / update the repo ──────────────────────────────────────────────
if [ -d "$REPO_DIR/.git" ]; then
    log "Repo exists — pulling latest..."
    cd "$REPO_DIR"
    git fetch origin
    git reset --hard origin/master
else
    log "Cloning repo to $REPO_DIR..."
    git clone "$REPO_URL" "$REPO_DIR"
    cd "$REPO_DIR"
fi

# ── 4. Python virtualenv + pip dependencies ──────────────────────────────────
log "Setting up Python virtualenv..."
python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip -q

# Core
pip install -q numpy pyarrow zstandard

# Dashboard + async
pip install -q "fastapi>=0.110" "uvicorn[standard]>=0.29" "websockets>=12"

# Broker auth
pip install -q pyotp curl_cffi

# Broker SDKs — comment out those not in use
pip install -q fyers-apiv3 upstox-client

# Optional extras
pip install -q httpx requests pandas pytz

log "Pip packages installed."
deactivate

# ── 5. Create required data directories (gitignored, must exist on disk) ─────
mkdir -p data/{positions,history,live_records,recorded,nse_option_cache} \
         logs/{trades,clients} \
         backups

log "Data dirs ready."

# ── 6. Ensure data/clients.db exists (empty, ready for first-run UI setup) ───
if [ ! -f data/clients.db ]; then
    log "Creating empty clients.db (configure brokers via the dashboard UI)..."
    python3 -c "
import sys; sys.path.insert(0, '.')
from data_layer.client_db import ClientDB
ClientDB('data/clients.db')
print('  clients.db initialised.')
"
fi

# ── 7. PM2 ecosystem file for V4 Cascade ────────────────────────────────────
log "Writing PM2 ecosystem file..."
cat > ecosystem.config.js <<'PMEOF'
module.exports = {
  apps: [{
    name: 'terminus',
    script: 'run_system.py',
    interpreter: '.venv/bin/python3',
    args: '--mode live --ui --index NIFTY --strategies v4_cascade --port 5000',
    cwd: __dirname,
    env: {
      // Enable the multi-zone pool engine validated in the July 2026 backtest
      V4CASCADE_USE_POOL_ENGINE: '1',
      // Three candidate offsets per side: ATM-100, ATM-200, ATM-300 (CE)
      //                                    ATM+100, ATM+200, ATM+300 (PE)
      V4CASCADE_TRACKING_OFFSETS: '100,200,300',
      // Log level (INFO is production default; DEBUG for troubleshooting)
      LOG_LEVEL: 'INFO',
    },
    max_memory_restart: '700M',
    watch: false,
    autorestart: true,
    restart_delay: 5000,
    // PM2 cluster mode: keep exactly 1 process (async single-thread, no benefit to forking)
    instances: 1,
    exec_mode: 'fork',
    // Rotate log file at 10 MB, keep 5 rotations
    log_date_format: 'YYYY-MM-DD HH:mm:ss',
    out_file: 'logs/pm2-out.log',
    error_file: 'logs/pm2-err.log',
    merge_logs: true,
  }]
};
PMEOF

# ── 8. Start the bot (or restart if already running) ─────────────────────────
log "Starting PM2 process '$PM2_APP'..."
pm2 startOrRestart ecosystem.config.js
pm2 save

# ── 9. PM2 startup on system reboot ──────────────────────────────────────────
log "Configuring PM2 to start on system boot..."
pm2 startup | grep "sudo" | bash || \
    log "WARN: pm2 startup command failed — run it manually from the output above."

# ── 10. Summary ───────────────────────────────────────────────────────────────
IP="$(curl -s https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]' || echo 'unknown')"

log ""
log "=========================================="
log " EC2 setup COMPLETE"
log "=========================================="
log " Bot:       pm2 status terminus"
log " Logs:      pm2 logs terminus"
log " Dashboard: http://$IP:$PORT"
log " HTTPS:     bash scripts/setup_https.sh"
log ""
log " NEXT STEPS:"
log "   1. Open http://$IP:$PORT in your browser."
log "   2. Admin -> Add Client -> configure Fyers broker binding."
log "      Set 'trading_mode' = paper for dry-run, live for real orders."
log "   3. Admin -> Deploy strategy -> V4 Cascade (Premium Trap) -> NIFTY."
log "   4. Admin -> V4 Cascade -> enter deploy_id -> Force Ingest Zones."
log "      (Fetches 3 weeks of history and rebuilds HTF pool state.)"
log "   5. Toggle 'Run' ON for the deployment to start trading."
log "=========================================="
