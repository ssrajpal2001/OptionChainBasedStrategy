"""Delta Exchange (India) ORDER + INDICATOR test — RUN ON THE WHITELISTED EC2 BOX (13.200.171.160).

Places ONE small order on the nearest-expiry ~ATM BTC option and prints:
  order_id, strike, expiry, LTP, mark(VWAP), IV, greeks(theta/delta), + RSI/ROC from Delta candles.
You can then close the position manually in the Delta app.

Usage (on EC2):
    DELTA_KEY=... DELTA_SECRET=... python scripts/delta_order_test.py --size 1 --side buy
    # add --cancel to auto-cancel the order right after (no fill), or --dry to skip placing entirely.

SAFETY: defaults to a LIMIT order 1 tick BELOW best_bid (won't fill immediately) unless --market.
"""
import argparse
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timedelta, timezone

import requests

BASE = os.environ.get("DELTA_BASE", "https://api.india.delta.exchange")
KEY = os.environ.get("DELTA_KEY", "")
SEC = os.environ.get("DELTA_SECRET", "")


def _req(method, path, payload=None, auth=True):
    ts = str(int(time.time()))
    body = json.dumps(payload) if payload else ""
    headers = {"User-Agent": "rest-client", "Content-Type": "application/json"}
    if auth:
        sig = hmac.new(SEC.encode(), (method + ts + path + "" + body).encode(), hashlib.sha256).hexdigest()
        headers.update({"api-key": KEY, "timestamp": ts, "signature": sig})
    url = BASE + path
    r = (requests.post(url, headers=headers, data=body, timeout=12) if method == "POST"
         else requests.delete(url, headers=headers, data=body, timeout=12) if method == "DELETE"
         else requests.get(url, headers=headers, timeout=12))
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"raw": r.text}


def _rsi(closes, n=14):
    if len(closes) < n + 1:
        return None
    gains = losses = 0.0
    for i in range(-n, 0):
        ch = closes[i] - closes[i - 1]
        gains += max(ch, 0.0); losses += max(-ch, 0.0)
    if losses == 0:
        return 100.0
    rs = (gains / n) / (losses / n)
    return 100 - 100 / (1 + rs)


def _roc(closes, n=10):
    if len(closes) < n + 1 or closes[-n - 1] == 0:
        return None
    return (closes[-1] - closes[-n - 1]) / closes[-n - 1] * 100


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size", type=int, default=1)
    ap.add_argument("--side", choices=["buy", "sell"], default="buy")
    ap.add_argument("--market", action="store_true")
    ap.add_argument("--cancel", action="store_true")
    ap.add_argument("--dry", action="store_true")
    args = ap.parse_args()

    # 1. Pick nearest-expiry ~ATM BTC call from products.
    _, pr = _req("GET", "/v2/products", auth=False)
    btc = [p for p in pr.get("result", []) if p.get("contract_type") == "call_options"
           and (p.get("underlying_asset") or {}).get("symbol") == "BTC"]
    spot = float(requests.get(BASE + "/v2/tickers/BTCUSD", timeout=12).json()
                 .get("result", {}).get("spot_price") or 0) if btc else 0
    if not spot:  # fall back to any option's spot
        spot = float(requests.get(BASE + f"/v2/tickers/{btc[0]['symbol']}", timeout=12)
                     .json().get("result", {}).get("spot_price") or 0)
    nearest_exp = min(str(p["settlement_time"])[:10] for p in btc)
    cands = [p for p in btc if str(p["settlement_time"])[:10] == nearest_exp]
    atm = min(cands, key=lambda p: abs(float(p["strike_price"]) - spot))
    sym, pid = atm["symbol"], int(atm["id"])

    # 2. Snapshot: LTP / mark(VWAP) / IV / greeks.
    t = requests.get(BASE + f"/v2/tickers/{sym}", timeout=12).json().get("result", {})
    q, g = (t.get("quotes") or {}), (t.get("greeks") or {})
    ltp, mark = float(t.get("close") or 0), float(t.get("mark_price") or 0)
    print(f"INSTRUMENT : {sym}  (product_id={pid})")
    print(f"  strike   : {atm['strike_price']}   expiry: {nearest_exp} (17:30 IST)   spot: {spot}")
    print(f"  LTP      : {ltp}")
    print(f"  VWAP(mark): {mark}   IV: {q.get('mark_iv')}   theta: {g.get('theta')}   delta: {g.get('delta')}")
    print(f"  bid/ask  : {q.get('best_bid')} / {q.get('best_ask')}")

    # 3. Indicators from Delta 1-min candles (RSI/ROC). VWAP/SLOPE are live mark-price deltas.
    end = int(time.time()); start = end - 60 * 60
    _, c = _req("GET", f"/v2/history/candles?resolution=1m&symbol={sym}&start={start}&end={end}", auth=False)
    candles = (c.get("result") or [])
    closes = [float(x.get("close") or 0) for x in reversed(candles)] if candles else []
    print(f"  candles  : {len(closes)} 1m bars   RSI(14)={_rsi(closes)}   ROC(10)={_roc(closes)}")

    if args.dry:
        print("DRY RUN — no order placed."); return

    # 4. Place a small order; print order_id.
    body = {"product_id": pid, "size": args.size, "side": args.side,
            "order_type": "market_order" if args.market else "limit_order"}
    if not args.market:
        bid = float(q.get("best_bid") or ltp or 1)
        body["limit_price"] = str(round(max(bid - float(t.get("tick_size") or 0.1), 0.1), 2))
    sc, res = _req("POST", "/v2/orders", body)
    if sc == 200 and res.get("success"):
        oid = res["result"].get("id")
        print(f"ORDER PLACED ✓  order_id={oid}  state={res['result'].get('state')}  "
              f"size={res['result'].get('size')}  avg_fill={res['result'].get('average_fill_price')}")
        if args.cancel and oid:
            sc2, _ = _req("DELETE", "/v2/orders", {"id": oid, "product_id": pid})
            print(f"  cancel requested (HTTP {sc2}).")
        else:
            print("  → close it manually in the Delta app when done.")
    else:
        print(f"ORDER FAILED (HTTP {sc}): {res.get('error')}  "
              f"(if ip_not_whitelisted → run on the whitelisted box 13.200.171.160)")


if __name__ == "__main__":
    main()
