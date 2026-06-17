#!/bin/bash
# Morning startup script - run at 9:00 AM IST
# Starts SENSEX (live at 9:15) + CrudeOil (entry window 14:30-22:45)
# Usage: bash scripts/morning_start.sh

set -e

cd "$(dirname "$0")/.."
echo "=== AlgoTrader Morning Start $(date '+%Y-%m-%d %H:%M:%S') ==="

# Pull latest code
echo "[1/4] Pulling latest code..."
git pull origin feat/delta-crypto-integration

# Install any new deps (fast if nothing changed)
echo "[2/4] Checking dependencies..."
pip install -q -r requirements.txt 2>/dev/null || true

# Stop any existing pm2 processes
echo "[3/4] Stopping old processes..."
pm2 delete sensex 2>/dev/null || true
pm2 delete crudeoil 2>/dev/null || true
pm2 delete algo 2>/dev/null || true

# Start SENSEX process (port 5000)
echo "[4a/4] Starting SENSEX dashboard on port 5000..."
pm2 start run_system.py \
    --name sensex \
    --interpreter python \
    -- --mode live --ui --port 5000 --index SENSEX --strategies trap_scanner

# Start CrudeOil process (port 5001)
echo "[4b/4] Starting CrudeOil dashboard on port 5001..."
pm2 start run_system.py \
    --name crudeoil \
    --interpreter python \
    -- --mode live --ui --port 5001 --index CRUDEOIL --strategies trap_scanner

# Save pm2 process list
pm2 save

echo ""
echo "=== DONE ==="
echo ""
echo "Next steps:"
echo "  1. Open http://<your-ec2-ip>:5000  → SENSEX dashboard"
echo "  2. Open http://<your-ec2-ip>:5001  → CrudeOil dashboard"
echo ""
echo "  On EACH dashboard:"
echo "  3. Data Feeders panel → Enable Upstox + Fyers (for SENSEX dashboard)"
echo "     OR Upstox2 (for CrudeOil dashboard) → click Login if token expired"
echo "  4. Client Profiles → AngelOne → Login (get fresh token)"
echo "  5. Client Profiles → AngelOne → Start Terminal"
echo "  6. Deployments → Add deployment: select SENSEX + Trap Scanner strategy"
echo "     OR CrudeOil + Trap Scanner strategy → Enable Trade ON"
echo ""
echo "  SENSEX will start trading at 9:15 AM"
echo "  CrudeOil will idle until 14:30 then auto-trade"
echo ""
pm2 list
