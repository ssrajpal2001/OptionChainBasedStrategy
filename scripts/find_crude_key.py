import requests, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from urllib.parse import quote

TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI0SkNIRDciLCJqdGkiOiI2YTMxOGFmYjMyNTdiYzE2ZTA1MTllNDciLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzgxNjMxNzM5LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODE2NDcyMDB9.C3eJij616XXpMbn9SWwiSknLGzg6j8jEmkxilTuN0R4"
H = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

# Try known MCX CrudeOil futures key formats
candidates = [
    "MCX_FO|CRUDEOIL26JUNFUT",
    "MCX_FO|CRUDEOIL26JULFUT",
    "MCX_FO|CRUDEOILM26JUNFUT",
    "MCX_FO|CRUDEOIL25JUNFUT",
    "MCX_FO|CRUDEOIL25JULFUT",
]
for key in candidates:
    enc = quote(key, safe="")
    r = requests.get(
        f"https://api.upstox.com/v2/historical-candle/{enc}/1minute/2026-06-16/2026-06-15",
        headers=H, timeout=10
    )
    candles = r.json().get("data", {}).get("candles", []) if r.status_code == 200 else []
    print(f"{key}: status={r.status_code} bars={len(candles)}")
    if candles:
        print(f"  First bar: {candles[-1]}")
        break

# Also check the NIFTY data more carefully
key = quote("NSE_INDEX|Nifty 50", safe="")
r = requests.get(f"https://api.upstox.com/v2/historical-candle/{key}/1minute/2026-06-17/2026-06-13", headers=H, timeout=15)
data = r.json().get("data", {}).get("candles", [])
dates = sorted(set(c[0][:10] for c in data))
print(f"\nNIFTY dates available: {dates}")
print(f"Total bars: {len(data)}")
