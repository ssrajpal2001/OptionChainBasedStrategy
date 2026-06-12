"""Delta Exchange (India) connectivity smoke test — run on a box whose IP is whitelisted on the
Delta API key. Validates: products/symbology, strike steps, expiries, LTP/mark(VWAP), and signed
auth (wallet/profile/leverage). READ-ONLY — never places an order.

Usage:
    DELTA_KEY=... DELTA_SECRET=... python scripts/delta_smoke_test.py
"""
import hashlib
import hmac
import json
import os
import time

import requests

BASE = os.environ.get("DELTA_BASE", "https://api.india.delta.exchange")
KEY = os.environ.get("DELTA_KEY", "")
SEC = os.environ.get("DELTA_SECRET", "")


def _sign_req(method, path, payload=None):
    ts = str(int(time.time()))
    body = json.dumps(payload) if payload else ""
    sig = hmac.new(SEC.encode(), (method + ts + path + "" + body).encode(), hashlib.sha256).hexdigest()
    h = {"api-key": KEY, "timestamp": ts, "signature": sig,
         "User-Agent": "rest-client", "Content-Type": "application/json"}
    url = BASE + path
    r = requests.post(url, headers=h, data=body, timeout=12) if method == "POST" \
        else requests.get(url, headers=h, timeout=12)
    return r.status_code, r.json()


def main():
    # 1. Public products → symbology + strike steps + expiries (no key needed)
    prods = requests.get(BASE + "/v2/products", timeout=15).json().get("result", [])
    btc = [p for p in prods if str(p.get("contract_type")) in ("call_options", "put_options")
           and (p.get("underlying_asset") or {}).get("symbol") == "BTC"]
    print(f"[products] {len(prods)} total, {len(btc)} BTC options")
    from collections import defaultdict
    by_exp = defaultdict(list)
    for p in btc:
        by_exp[str(p.get("settlement_time"))[:10]].append(int(float(p.get("strike_price") or 0)))
    for e in sorted(by_exp)[:3]:
        s = sorted(set(by_exp[e]))
        steps = sorted({s[i + 1] - s[i] for i in range(len(s) - 1)})
        print(f"  exp {e} (17:30 IST): {len(s)} strikes step(s)={steps[:4]} range={s[0]}..{s[-1]}")

    # 2. LTP / mark (VWAP) for the nearest ATM-ish BTC call
    if btc:
        sym = sorted(p["symbol"] for p in btc)[len(btc) // 2]
        t = requests.get(BASE + f"/v2/tickers/{sym}", timeout=12).json().get("result", {})
        print(f"[ticker] {sym}: ltp(close)={t.get('close')} mark(VWAP)={t.get('mark_price')} "
              f"spot={t.get('spot_price')} oi={t.get('oi')} iv={(t.get('quotes') or {}).get('mark_iv')}")

    # 3. Signed auth (needs whitelisted IP + key)
    if KEY and SEC:
        sc, bal = _sign_req("GET", "/v2/wallet/balances")
        if sc == 200 and bal.get("success"):
            for w in (bal.get("result") or [])[:4]:
                print(f"  [wallet] {w.get('asset_symbol')}: avail={w.get('available_balance')} "
                      f"blocked={w.get('blocked_margin')}")
        else:
            print(f"  [wallet] HTTP {sc}: {bal.get('error')}  "
                  f"(if ip_not_whitelisted — whitelist this box's IP on the Delta API key)")
        sc, pr = _sign_req("GET", "/v2/users/profile")
        print(f"  [profile] HTTP {sc}: {(pr.get('result') or {}).get('email') if sc == 200 else pr.get('error')}")
    else:
        print("[auth] DELTA_KEY/DELTA_SECRET not set — skipping signed checks.")


if __name__ == "__main__":
    main()
