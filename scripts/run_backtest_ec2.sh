#!/bin/bash
# Run on EC2 to get full trap scanner backtest report.
# Usage: bash scripts/run_backtest_ec2.sh <UPSTOX_TOKEN>
#
# Example:
#   bash scripts/run_backtest_ec2.sh "eyJ0eXAiOiJKV1Q..."
#
# The Upstox token is needed ONLY for the backtest (historical data fetch).
# It is NOT saved to DB — just used for this script run.

set -e
cd "$(dirname "$0")/.."

UPSTOX_TOKEN="${1:-}"
if [ -z "$UPSTOX_TOKEN" ]; then
    echo "Usage: bash scripts/run_backtest_ec2.sh <UPSTOX_TOKEN>"
    echo ""
    echo "Get your token from Upstox developer console or from the morning auth log:"
    echo "  grep 'access_token' logs/system-*.log | tail -1"
    exit 1
fi

echo "================================================================"
echo "  TRAP SCANNER BACKTEST — $(date '+%Y-%m-%d %H:%M')"
echo "================================================================"

# Patch the token into the backtest script and run
python -X utf8 - <<PYEOF
import sys, os
sys.path.insert(0, ".")

# Patch token
import scripts.correct_backtest as bt
bt.TOKEN = "${UPSTOX_TOKEN}"
bt.H = {"Authorization": f"Bearer ${UPSTOX_TOKEN}", "Accept": "application/json"}

# Run SENSEX backtest (last 5 trading days)
import pandas as pd
from datetime import date, timedelta

# Get last 5 weekdays
def last_n_trading_days(n=5):
    days = []
    d = date.today()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:  # Mon-Fri
            days.append(str(d))
    return list(reversed(days))

DAYS = last_n_trading_days(5)
print(f"Backtest days: {DAYS}")

s_total = bt.run_sensex_backtest(DAYS)

# CrudeOil with correct key
c_total = bt.run_crudeoil_backtest(DAYS, "NSE_COM|149475")

print(f"\n{'='*60}")
print(f"SENSEX  net: {s_total:.1f} pts  (lot=20)  Rs {s_total*20:.0f}")
print(f"CRUDEOIL net: {c_total:.1f} pts  (lot=100)  Rs {c_total*100:.0f}")
print(f"COMBINED: Rs {s_total*20 + c_total*100:.0f}")
PYEOF
