import requests, sys, os, io, csv, gzip, json
from urllib.parse import quote
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI0SkNIRDciLCJqdGkiOiI2YTMxOGFmYjMyNTdiYzE2ZTA1MTllNDciLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzgxNjMxNzM5LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODE2NDcyMDB9.C3eJij616XXpMbn9SWwiSknLGzg6j8jEmkxilTuN0R4"
H = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

# Try alternate master CSV URL
urls = [
    "https://assets.upstox.com/market-quote/instruments/exchange/MCX.csv.gz",
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz",
]
for url in urls:
    try:
        r = requests.get(url, timeout=30)
        print(f"URL={url} status={r.status_code} len={len(r.content)}")
        if r.status_code == 200 and len(r.content) > 100:
            raw = gzip.decompress(r.content).decode("utf-8")
            reader = csv.DictReader(io.StringIO(raw))
            for row in reader:
                sym = row.get("tradingsymbol","") or row.get("name","")
                itype = row.get("instrument_type","")
                if "CRUDE" in sym.upper() and "FUT" in itype.upper():
                    print(f"  KEY={row.get('instrument_key')}  SYM={sym}  TYPE={itype}  EXP={row.get('expiry')}")
            break
    except Exception as e:
        print(f"  {url} error: {e}")

# Also try the v3 instruments API
r2 = requests.get("https://api.upstox.com/v2/instruments/MCX", headers=H, timeout=20)
print("\nv2 instruments MCX:", r2.status_code, r2.text[:500])
