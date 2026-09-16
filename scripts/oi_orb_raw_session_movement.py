"""
scripts/oi_orb_raw_session_movement.py

Direct user spec: "check each stock individually as raw without the
threshold when does movement occur which session." Independent of the
2% price-trigger / OI-confirm / VWAP-retest mechanic entirely -- for
each of the 35 real (date,symbol) pairs already analyzed, walks that
day's REAL raw 1-min spot bars and reports, for three sessions:
  MORNING   09:15-11:30
  MIDDAY    11:30-13:30
  AFTERNOON 13:30-15:30 (also the window most relevant to the proposed
                          "no new entries after 13:30" cutoff)
the real high/low RANGE (as % of the day's open) and the real NET move
(close of session vs open of session, as %) -- so which session
actually carried the stock's big move is visible directly from raw
price action, with no threshold applied at all.

MUST run on EC2 (real Upstox account access tokens + real historical
range data).

Usage: python scripts/oi_orb_raw_session_movement.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, time as dtime

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.core.trap_zone_utils import Bar
from strategies.oi_orb_screener import stock_resolve

SESSIONS = [
    ("MORNING", dtime(9, 15), dtime(11, 30)),
    ("MIDDAY", dtime(11, 30), dtime(13, 30)),
    ("AFTERNOON", dtime(13, 30), dtime(15, 30)),
]

# All 35 real (date, symbol) pairs from the 7-day backtest that reached
# at least Step 2 (OI confirm) -- both the 19 traded and 16 no-retest.
STOCKS = [
    ("2026-09-01", "LTF"), ("2026-09-01", "POLYCAB"), ("2026-09-01", "ASHOKLEY"),
    ("2026-09-01", "HEROMOTOCO"), ("2026-09-01", "KALYANKJIL"), ("2026-09-01", "KEI"),
    ("2026-09-01", "MARUTI"),
    ("2026-09-02", "BSE"), ("2026-09-02", "EICHERMOT"), ("2026-09-02", "HEROMOTOCO"),
    ("2026-09-02", "SWIGGY"), ("2026-09-02", "VOLTAS"),
    ("2026-09-03", "APLAPOLLO"), ("2026-09-03", "GODREJCP"), ("2026-09-03", "SOLARINDS"),
    ("2026-09-03", "KAYNES"), ("2026-09-03", "RBLBANK"),
    ("2026-09-04", "ATHERENERG"), ("2026-09-04", "HAVELLS"), ("2026-09-04", "KEI"),
    ("2026-09-04", "POLYCAB"), ("2026-09-04", "MOTILALOFS"),
    ("2026-09-07", "MANAPPURAM"), ("2026-09-07", "WIPRO"), ("2026-09-07", "BOSCHLTD"),
    ("2026-09-07", "ICICIPRULI"), ("2026-09-07", "VMM"),
    ("2026-09-08", "GVT&D"), ("2026-09-08", "POWERINDIA"),
    ("2026-09-09", "COFORGE"), ("2026-09-09", "MUTHOOTFIN"), ("2026-09-09", "INFY"),
    ("2026-09-09", "PERSISTENT"), ("2026-09-09", "TCS"), ("2026-09-09", "TECHM"),
]


def _access_tokens():
    db = ClientDB()
    tokens = []
    for account in ("upstox2", "upstox"):
        creds = db.get_feeder_creds_sync(account)
        if creds and creds.get("access_token"):
            tokens.append(creds["access_token"])
    if not tokens:
        raise RuntimeError("No upstox/upstox2 feeder access_token found -- run this on EC2.")
    return tokens


def _to_bars(rows):
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                        low=float(r["low"]), close=float(r["close"])))
    return out


async def analyze(tokens, trade_date_str, symbol):
    trade_date = date.fromisoformat(trade_date_str)
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return {"symbol": symbol, "date": trade_date_str, "error": "NO_EQ_KEY"}
    rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, trade_date, trade_date)
    bars = _to_bars(rows)
    if not bars:
        return {"symbol": symbol, "date": trade_date_str, "error": "NO_BARS"}
    day_open = bars[0].open
    sessions_out = []
    biggest = (None, 0.0)
    for name, start_t, end_t in SESSIONS:
        sess_bars = [b for b in bars if start_t <= b.ts.time() < end_t]
        if not sess_bars:
            sessions_out.append((name, None))
            continue
        hi = max(b.high for b in sess_bars)
        lo = min(b.low for b in sess_bars)
        range_pct = (hi - lo) / day_open * 100.0
        net_pct = (sess_bars[-1].close - sess_bars[0].open) / day_open * 100.0
        sessions_out.append((name, {"hi": hi, "lo": lo, "range_pct": range_pct, "net_pct": net_pct,
                                     "start": sess_bars[0].ts.strftime("%H:%M"),
                                     "end": sess_bars[-1].ts.strftime("%H:%M")}))
        if range_pct > biggest[1]:
            biggest = (name, range_pct)
    return {"symbol": symbol, "date": trade_date_str, "day_open": day_open,
            "sessions": sessions_out, "biggest_session": biggest[0], "biggest_range_pct": biggest[1]}


async def main():
    tokens = _access_tokens()
    print("=" * 130)
    print("OI-ORB Screener -- RAW session movement, no threshold applied (35 real stocks)")
    print("Sessions: MORNING 09:15-11:30 | MIDDAY 11:30-13:30 | AFTERNOON 13:30-15:30")
    print("=" * 130)

    results = []
    for trade_date_str, symbol in STOCKS:
        r = await analyze(tokens, trade_date_str, symbol)
        results.append(r)
        print(f"\n{'-'*130}\n{r['symbol']} ({r['date']})\n{'-'*130}")
        if "error" in r:
            print(f"  {r['error']}")
            continue
        for name, s in r["sessions"]:
            if s is None:
                print(f"  {name:10s}  no real bars in this window")
                continue
            marker = "  <== biggest range" if name == r["biggest_session"] else ""
            print(f"  {name:10s}  [{s['start']}-{s['end']}]  range={s['range_pct']:+.2f}%  "
                  f"net={s['net_pct']:+.2f}%  (hi={s['hi']} lo={s['lo']}){marker}")

    print("\n" + "=" * 130)
    counts = {"MORNING": 0, "MIDDAY": 0, "AFTERNOON": 0}
    for r in results:
        if r.get("biggest_session"):
            counts[r["biggest_session"]] += 1
    print(f"SUMMARY -- which session carried the biggest real range, across all {len(results)} stocks:")
    for name, c in counts.items():
        print(f"  {name:10s}: {c} stocks")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
