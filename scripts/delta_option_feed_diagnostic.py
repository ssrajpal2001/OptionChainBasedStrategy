"""
Delta Exchange (India) option-feed diagnostic — run on EC2 to verify why option
ticks are not arriving in the live bot.

Checks, in order:
  1. REST /v2/products reachable and returns BTC option chain
  2. REST /v2/tickers/<symbol> returns LTP/mark/spot for an ATM option
  3. WebSocket v2/ticker connects and receives ticks for perp + options

Usage:
    python scripts/delta_option_feed_diagnostic.py

If step 1 fails with an auth/IP error, whitelist the EC2 IP on the Delta API key.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

try:
    import aiohttp
except ImportError as exc:  # pragma: no cover
    print("aiohttp not installed. Install with: pip install aiohttp")
    raise SystemExit(1)

import requests

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from data_layer.symbol_translator import InternalSymbol  # noqa: E402
from data_layer.universal_option_mapper import UniversalOptionMapper  # noqa: E402

BASE = "https://api.india.delta.exchange"
WS_URL = "wss://socket.india.delta.exchange"
UND = "BTC"
WINDOW = 6
TIMEOUT_SEC = 20


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def check_rest() -> tuple[list[str], str]:
    print(f"[{_now()}] 1. Fetching {BASE}/v2/products ...")
    try:
        r = requests.get(BASE + "/v2/products", timeout=15)
        r.raise_for_status()
    except Exception as exc:
        print(f"[{_now()}]    FAIL: {exc}")
        return [], ""

    rows = r.json().get("result", [])
    opts = [
        p for p in rows
        if str(p.get("contract_type")) in ("call_options", "put_options")
        and (p.get("underlying_asset") or {}).get("symbol") == UND
    ]
    print(f"[{_now()}]    OK: {len(rows)} products, {len(opts)} BTC options")

    by_exp = defaultdict(list)
    for p in opts:
        by_exp[str(p.get("settlement_time"))[:10]].append(p)
    for e in sorted(by_exp)[:3]:
        strikes = sorted({int(float(p.get("strike_price") or 0)) for p in by_exp[e]})
        print(f"         expiry {e}: {len(by_exp[e])} opts, strikes {len(strikes)} "
              f"range {strikes[0]}..{strikes[-1]}")

    active_exp = UniversalOptionMapper.active_daily_expiry()
    ddmmyy = active_exp.strftime("%d%m%y")
    active = [p for p in opts if str(p.get("symbol", "")).endswith(ddmmyy)]
    print(f"[{_now()}]    Active expiry {active_exp} ({ddmmyy}): {len(active)} opts")
    if not active:
        print(f"[{_now()}]    FAIL: no active-daily-expiry options found")
        return [], ""

    strikes = sorted({int(float(p.get("strike_price") or 0)) for p in active})
    return strikes, ddmmyy


def check_ticker(sym: str) -> bool:
    print(f"[{_now()}] 2. Fetching {BASE}/v2/tickers/{sym} ...")
    try:
        r = requests.get(BASE + f"/v2/tickers/{sym}", timeout=12)
        r.raise_for_status()
        t = r.json().get("result", {})
    except Exception as exc:
        print(f"[{_now()}]    FAIL: {exc}")
        return False

    print(f"[{_now()}]    OK: {sym} close={t.get('close')} mark={t.get('mark_price')} "
          f"spot={t.get('spot_price')} oi={t.get('oi')}")
    return True


async def check_ws(symbols: list[str]) -> dict[str, int]:
    print(f"[{_now()}] 3. Connecting WebSocket {WS_URL} ...")
    counts: dict[str, int] = {}
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(WS_URL, heartbeat=30) as ws:
            await ws.send_json({"type": "enable_heartbeat"})
            await ws.send_json({
                "type": "subscribe",
                "payload": {"channels": [{"name": "v2/ticker", "symbols": symbols}]},
            })
            print(f"[{_now()}]    Subscribed {len(symbols)} symbols; listening {TIMEOUT_SEC}s ...")

            t0 = time.time()
            while time.time() - t0 < TIMEOUT_SEC:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                if msg.type == aiohttp.WSMsgType.TEXT:
                    d = json.loads(msg.data)
                    typ = d.get("type")
                    if typ == "v2/ticker":
                        sym = d.get("symbol", "?")
                        counts[sym] = counts.get(sym, 0) + 1
                        if counts[sym] <= 3:
                            print(f"[{_now()}]    TICK {sym}: close={d.get('close')} "
                                  f"spot={d.get('spot_price')} mark={d.get('mark_price')}")
                    elif typ == "subscriptions":
                        print(f"[{_now()}]    WS ack: {d}")
                    elif typ == "error":
                        print(f"[{_now()}]    WS error: {d}")
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    print(f"[{_now()}]    WS closed/error: {msg}")
                    break

    return counts


async def main() -> int:
    strikes, ddmmyy = check_rest()
    if not strikes:
        print("\nDIAGNOSIS: /v2/products failed. Whitelist EC2 IP on Delta API key.")
        return 1

    spot = 0.0
    try:
        spot = float(requests.get(BASE + "/v2/tickers/BTCUSD", timeout=10)
                     .json().get("result", {}).get("spot_price") or 0)
    except Exception as exc:
        print(f"[{_now()}]    WARN: could not fetch BTCUSD spot: {exc}")

    atm = min(strikes, key=lambda k: abs(k - spot)) if spot > 0 else strikes[len(strikes) // 2]
    i = strikes.index(atm)
    sel = strikes[max(0, i - WINDOW): i + WINDOW + 1]
    symbols = [f"{UND}USD"]
    exp = UniversalOptionMapper.active_daily_expiry()
    for k in sel:
        symbols.append(UniversalOptionMapper.to_delta_symbol(
            InternalSymbol(UND, float(k), "CE", exp)
        ))
        symbols.append(UniversalOptionMapper.to_delta_symbol(
            InternalSymbol(UND, float(k), "PE", exp)
        ))

    # Use a known ATM pair for the REST ticker check too
    test_sym = f"C-{UND}-{atm}-{ddmmyy}"
    if not check_ticker(test_sym):
        print("\nDIAGNOSIS: /v2/tickers failed. Whitelist EC2 IP on Delta API key.")
        return 1

    counts = await check_ws(symbols)
    print(f"\n[{_now()}] WS tick counts ({len(counts)} symbols received ticks):")
    for sym, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"    {sym}: {n}")

    opt_counts = {s: c for s, c in counts.items() if s.startswith(("C-", "P-"))}
    if not opt_counts:
        print("\nDIAGNOSIS: REST works but NO option ticks arrived on WebSocket.")
        print("  -> Verify subscription symbols match Delta format C-BTC-STRIKE-DDMMYY")
        print("  -> Check Delta WS status / channel name v2/ticker")
        return 1

    print("\nDIAGNOSIS: REST + WS option feed are healthy. The issue is in the bot's subscription path.")
    print("  -> Confirm BTC is in monitored_indices and sell_straddle deployment is_running=1")
    print("  -> Confirm DeltaChainManager started and logged 'subscribed X strikes'")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        raise SystemExit(130)
