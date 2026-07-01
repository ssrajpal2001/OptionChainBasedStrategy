"""Find the correct CrudeOil near-month futures key from Upstox."""
import requests, sys, os
from urllib.parse import quote
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI0SkNIRDciLCJqdGkiOiI2YTMxOGFmYjMyNTdiYzE2ZTA1MTllNDciLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzgxNjMxNzM5LCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODE2NDcyMDB9.C3eJij616XXpMbn9SWwiSknLGzg6j8jEmkxilTuN0R4"
H = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

# Download MCX master CSV to find CrudeOil futures key
r = requests.get("https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz", timeout=30)
import gzip, json
data = json.loads(gzip.decompress(r.content))
crude_futures = [d for d in data if "CRUDEOIL" in d.get("tradingsymbol","") and d.get("instrument_type") in ("FUT","FUTCOM")]
for d in crude_futures[:10]:
    print(d.get("instrument_key"), d.get("tradingsymbol"), d.get("expiry"))
