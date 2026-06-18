"""
EC2 Backtest Runner — SENSEX + CrudeOil trap scanner
Run: python scripts/run_backtest_ec2.py <UPSTOX_TOKEN>

Get token from:
  grep 'access_token' logs/system-*.log | tail -1
  OR from today's Upstox headless auth log

Results show:
  - Last 5 trading days
  - SENSEX: spot direction + option entry (2 lots, T1 + TSL)
  - CrudeOil: futures detect + trade (2 lots)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if len(sys.argv) < 2:
    print("Usage: python scripts/run_backtest_ec2.py <UPSTOX_TOKEN>")
    print()
    print("Get token from today's auth log:")
    print("  grep 'access_token' logs/system-$(date +%Y%m%d).log | tail -1")
    sys.exit(1)

TOKEN = sys.argv[1].strip()

# Patch token into backtest module before running
import importlib, types
import scripts.correct_backtest as bt
bt.TOKEN = TOKEN
bt.H = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

from datetime import date, timedelta

def last_n_trading_days(n=5):
    days = []
    d = date.today()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            days.append(str(d))
    return list(reversed(days))

DAYS = last_n_trading_days(5)
print(f"\nBacktest period: {DAYS[0]} to {DAYS[-1]}")

s_total = bt.run_sensex_backtest(DAYS)
# Jun contract (expired ~Jun 18): MCX_FO|499095  Jul contract (from Jun 19): MCX_FO|520702
c_total = bt.run_crudeoil_backtest(DAYS, "MCX_FO|499095")

print(f"\n{'='*60}")
print(f"SENSEX   net: {s_total:.1f} pts  (lot=20)  Rs {s_total*20:.0f}")
print(f"CRUDEOIL net: {c_total:.1f} pts  (lot=200)  Rs {c_total*200:.0f}")
print(f"COMBINED P&L: Rs {s_total*20 + c_total*200:.0f}")
