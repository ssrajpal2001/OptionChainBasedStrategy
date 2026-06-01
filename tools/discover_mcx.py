"""
tools/discover_mcx.py — Discover the EXACT MCX Crude Oil futures + option
instrument keys / symbols for Upstox and Fyers.

Run this ON EC2 (where the SDKs + DB tokens live) BEFORE wiring MCX into the
system, so we use verified symbol formats instead of guessed ones.

    python3 tools/discover_mcx.py

It prints, for CRUDEOIL (and CRUDEOILM mini if present):
  • Upstox: the current-month FUTURES instrument_key (used as the ATM source)
            and a sample of option instrument_keys with their strikes/expiry.
  • Fyers : the derived futures symbol + sample option symbols, and verifies
            them against the Fyers quotes API if a token is available.

No writes, no orders — read-only discovery.
"""

from __future__ import annotations

import gzip
import json
import sys
from datetime import date
from pathlib import Path
from urllib.request import urlopen, Request

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

UNDERLYINGS = ["CRUDEOIL", "CRUDEOILM"]
MCX_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz"


def _load_tokens():
    """Pull Upstox + Fyers access tokens from the client DB (sync)."""
    upstox_tok = fyers_tok = ""
    try:
        from data_layer.client_db import ClientDB
        db = ClientDB()
        u = db.get_feeder_creds_sync("upstox") or {}
        f = db.get_feeder_creds_sync("fyers") or {}
        upstox_tok = u.get("access_token", "")
        fyers_tok = f.get("access_token", "")
    except Exception as exc:
        print(f"[warn] could not read tokens from DB: {exc}")
    return upstox_tok, fyers_tok


def discover_upstox_master():
    """Download the Upstox MCX master and dump CRUDEOIL futures + options."""
    print("\n" + "=" * 70)
    print("UPSTOX — MCX master JSON")
    print("=" * 70)
    try:
        import ssl
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = Request(MCX_MASTER_URL, headers={"Accept-Encoding": "gzip"})
        with urlopen(req, timeout=60, context=ctx) as r:
            raw = r.read()
        try:
            data = json.loads(gzip.decompress(raw))
        except Exception:
            data = json.loads(raw)
        print(f"MCX master loaded: {len(data)} instruments")
    except Exception as exc:
        print(f"[error] MCX master download failed: {exc}")
        return

    today = date.today()
    for u in UNDERLYINGS:
        futs, opts = [], []
        for inst in data:
            ts = (inst.get("trading_symbol") or inst.get("tradingsymbol") or "")
            ik = inst.get("instrument_key", "")
            itype = (inst.get("instrument_type") or "")
            if not ts.upper().startswith(u):
                continue
            entry = (ik, ts, inst.get("expiry", ""), inst.get("strike_price", 0), itype)
            if itype in ("CE", "PE"):
                opts.append(entry)
            elif "FUT" in str(itype).upper() or ts.upper().endswith("FUT"):
                futs.append(entry)
        print(f"\n--- {u} ---  futures={len(futs)}  options={len(opts)}")
        for ik, ts, exp, strike, itype in sorted(futs)[:4]:
            print(f"  FUT  ikey={ik!r}  ts={ts!r}  expiry={exp}")
        for ik, ts, exp, strike, itype in sorted(opts)[:6]:
            print(f"  OPT  ikey={ik!r}  ts={ts!r}  strike={strike} {itype} expiry={exp}")
        if not futs and not opts:
            print(f"  (no {u} instruments found — check underlying name)")


def discover_fyers(fyers_tok: str):
    """Derive candidate Fyers crude symbols and verify via quotes API."""
    print("\n" + "=" * 70)
    print("FYERS — derived crude symbols")
    print("=" * 70)
    # Fyers commodity futures format: MCX:CRUDEOIL{YY}{MON}FUT (current contract)
    months = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]
    today = date.today()
    yy = today.strftime("%y")
    mon = months[today.month - 1]
    candidates = [
        f"MCX:CRUDEOIL{yy}{mon}FUT",
        f"MCX:CRUDEOILM{yy}{mon}FUT",
    ]
    print("Candidate futures symbols:")
    for c in candidates:
        print(f"  {c}")

    if not fyers_tok:
        print("[info] no Fyers token in DB — cannot verify via quotes API. "
              "Verify the above manually in Fyers.")
        return
    try:
        from fyers_apiv3 import fyersModel
        # access_token for fyersModel is 'appid:token'; DB may store just token.
        fy = fyersModel.FyersModel(token=fyers_tok, is_async=False, log_path="")
        resp = fy.quotes({"symbols": ",".join(candidates)})
        print("\nFyers quotes() response:")
        print(json.dumps(resp, indent=2)[:1500])
    except Exception as exc:
        print(f"[warn] Fyers quotes verify failed: {exc}")


def main():
    upstox_tok, fyers_tok = _load_tokens()
    print(f"tokens: upstox={'yes' if upstox_tok else 'no'}  fyers={'yes' if fyers_tok else 'no'}")
    discover_upstox_master()
    discover_fyers(fyers_tok)
    print("\nDone. Paste this output back so the exact keys can be wired in.")


if __name__ == "__main__":
    main()
