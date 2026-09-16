"""
scripts/oi_orb_raw_session_movement.py

Direct user spec: "check each stock individually as raw without the
threshold when does movement occur which session."

2026-09-16 CORRECTION (real bug found by the user, via the COFORGE
direction-mismatch investigation): v1 of this script anchored `net%`
to TODAY'S OWN OPENING PRINT (day_open), not to yesterday's close --
the actual reference the mechanic's own price trigger uses. That let a
stock that gapped down hard and only partially recovered (COFORGE:
-8.13% at open, still -5.38% vs prev_close at day's end) read as "net
positive" purely because it climbed back part-way toward its OWN open,
while remaining solidly bearish the entire session relative to the
real trigger baseline. Fixed: every % figure below is now anchored to
the REAL previous trading day's close (prev_close), matching what
side_from_pchange/the price trigger itself actually measures. Also now
prints the CUMULATIVE %-vs-prev_close as of the end of each session
(the running level, not just that session's own local swing) --
that's the number that actually answers "was this stock still net
bullish/bearish, relative to the real baseline, at this point in the
day" -- and is what should be cross-referenced against each trade's
own real entry time and side, not the old (flawed) local net%.

MUST run on EC2 (real Upstox account access tokens + real historical
range data).

Usage: python scripts/oi_orb_raw_session_movement.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, time as dtime, timedelta

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

# (date, symbol, side, kind, entry_hhmm_or_None) -- side/entry now
# included so the cumulative-vs-prev_close figures can be directly
# cross-checked against what each trade actually did, right in this
# same run.
STOCKS = [
    ("2026-09-01", "LTF", "PUT", "TRADED", "14:33"),
    ("2026-09-01", "POLYCAB", "PUT", "TRADED", "11:29"),
    ("2026-09-01", "ASHOKLEY", "PUT", "NO_RETEST", None),
    ("2026-09-01", "HEROMOTOCO", "CALL", "NO_RETEST", None),
    ("2026-09-01", "KALYANKJIL", "CALL", "NO_RETEST", None),
    ("2026-09-01", "KEI", "PUT", "NO_RETEST", None),
    ("2026-09-01", "MARUTI", "PUT", "NO_RETEST", None),
    ("2026-09-02", "BSE", "PUT", "TRADED", "14:25"),
    ("2026-09-02", "EICHERMOT", "PUT", "TRADED", "13:27"),
    ("2026-09-02", "HEROMOTOCO", "PUT", "TRADED", "14:28"),
    ("2026-09-02", "SWIGGY", "PUT", "TRADED", "12:24"),
    ("2026-09-02", "VOLTAS", "PUT", "NO_RETEST", None),
    ("2026-09-03", "APLAPOLLO", "PUT", "TRADED", "14:24"),
    ("2026-09-03", "GODREJCP", "PUT", "TRADED", "09:17"),
    ("2026-09-03", "SOLARINDS", "CALL", "TRADED", "12:27"),
    ("2026-09-03", "KAYNES", "PUT", "NO_RETEST", None),
    ("2026-09-03", "RBLBANK", "CALL", "NO_RETEST", None),
    ("2026-09-04", "ATHERENERG", "PUT", "TRADED", "14:54"),
    ("2026-09-04", "HAVELLS", "PUT", "TRADED", "09:31"),
    ("2026-09-04", "KEI", "PUT", "TRADED", "09:27"),
    ("2026-09-04", "POLYCAB", "PUT", "TRADED", "09:17"),
    ("2026-09-04", "MOTILALOFS", "CALL", "NO_RETEST", None),
    ("2026-09-07", "MANAPPURAM", "PUT", "TRADED", "14:22"),
    ("2026-09-07", "WIPRO", "PUT", "TRADED", "15:11"),
    ("2026-09-07", "BOSCHLTD", "CALL", "NO_RETEST", None),
    ("2026-09-07", "ICICIPRULI", "PUT", "NO_RETEST", None),
    ("2026-09-07", "VMM", "PUT", "NO_RETEST", None),
    ("2026-09-08", "GVT&D", "CALL", "TRADED", "09:27"),
    ("2026-09-08", "POWERINDIA", "PUT", "TRADED", "10:13"),
    ("2026-09-09", "COFORGE", "PUT", "TRADED", "09:23"),
    ("2026-09-09", "MUTHOOTFIN", "PUT", "TRADED", "12:02"),
    ("2026-09-09", "INFY", "PUT", "NO_RETEST", None),
    ("2026-09-09", "PERSISTENT", "PUT", "NO_RETEST", None),
    ("2026-09-09", "TCS", "PUT", "NO_RETEST", None),
    ("2026-09-09", "TECHM", "PUT", "NO_RETEST", None),
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


async def _prev_close_asof(eq_key, tokens, ref_date, max_step_back=10):
    d = ref_date - timedelta(days=1)
    for _ in range(max_step_back):
        if d.weekday() < 5:
            rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, d, d)
            if rows:
                return float(rows[-1]["close"])
        d -= timedelta(days=1)
    return None


async def analyze(tokens, trade_date_str, symbol):
    trade_date = date.fromisoformat(trade_date_str)
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return {"symbol": symbol, "date": trade_date_str, "error": "NO_EQ_KEY"}
    prev_close = await _prev_close_asof(eq_key, tokens, trade_date)
    if not prev_close:
        return {"symbol": symbol, "date": trade_date_str, "error": "NO_PREV_CLOSE"}
    rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, trade_date, trade_date)
    bars = _to_bars(rows)
    if not bars:
        return {"symbol": symbol, "date": trade_date_str, "error": "NO_BARS"}
    sessions_out = []
    biggest = (None, 0.0)
    for name, start_t, end_t in SESSIONS:
        sess_bars = [b for b in bars if start_t <= b.ts.time() < end_t]
        if not sess_bars:
            sessions_out.append((name, None))
            continue
        hi = max(b.high for b in sess_bars)
        lo = min(b.low for b in sess_bars)
        range_pct = (hi - lo) / prev_close * 100.0
        cum_pct_end = (sess_bars[-1].close - prev_close) / prev_close * 100.0
        sessions_out.append((name, {"hi": hi, "lo": lo, "range_pct": range_pct,
                                     "cum_pct_end": cum_pct_end,
                                     "start": sess_bars[0].ts.strftime("%H:%M"),
                                     "end": sess_bars[-1].ts.strftime("%H:%M")}))
        if range_pct > biggest[1]:
            biggest = (name, range_pct)
    day_close = bars[-1].close
    day_cum_pct = (day_close - prev_close) / prev_close * 100.0
    return {"symbol": symbol, "date": trade_date_str, "prev_close": prev_close,
            "sessions": sessions_out, "biggest_session": biggest[0], "biggest_range_pct": biggest[1],
            "day_cum_pct": day_cum_pct}


def _cum_pct_asof(r, hhmm):
    """Best-effort: the cumulative %-vs-prev_close as of the END of the
    session that CONTAINS hhmm (close enough for a same-session sanity
    check without re-walking bar-by-bar)."""
    if hhmm is None:
        return None
    t = datetime.strptime(hhmm, "%H:%M").time()
    for name, start_t, end_t in SESSIONS:
        if start_t <= t < end_t:
            for sname, s in r["sessions"]:
                if sname == name and s is not None:
                    return s["cum_pct_end"]
    return r.get("day_cum_pct")


async def main():
    tokens = _access_tokens()
    print("=" * 130)
    print("OI-ORB Screener -- RAW session movement, CORRECTED (anchored to real prev_close, not today's own open)")
    print("Sessions: MORNING 09:15-11:30 | MIDDAY 11:30-13:30 | AFTERNOON 13:30-15:30")
    print("=" * 130)

    results = {}
    for trade_date_str, symbol, side, kind, entry_hhmm in STOCKS:
        r = await analyze(tokens, trade_date_str, symbol)
        results[(trade_date_str, symbol)] = r
        print(f"\n{'-'*130}\n{r['symbol']} ({r['date']}, side={side}, {kind}"
              f"{f', entry={entry_hhmm}' if entry_hhmm else ''})\n{'-'*130}")
        if "error" in r:
            print(f"  {r['error']}")
            continue
        print(f"  prev_close={r['prev_close']}")
        for name, s in r["sessions"]:
            if s is None:
                print(f"  {name:10s}  no real bars in this window")
                continue
            marker = "  <== biggest range" if name == r["biggest_session"] else ""
            print(f"  {name:10s}  [{s['start']}-{s['end']}]  range={s['range_pct']:+.2f}%  "
                  f"cum-vs-prevclose-at-end={s['cum_pct_end']:+.2f}%  (hi={s['hi']} lo={s['lo']}){marker}")
        print(f"  DAY CLOSE cum-vs-prevclose = {r['day_cum_pct']:+.2f}%")

    print("\n" + "=" * 130)
    print("DIRECTION CROSS-CHECK (corrected): trade side vs real cumulative %-vs-prev_close "
          "as of the session containing the actual entry time")
    print("-" * 130)
    matches = mismatches = 0
    for trade_date_str, symbol, side, kind, entry_hhmm in STOCKS:
        if kind != "TRADED":
            continue
        r = results[(trade_date_str, symbol)]
        if "error" in r:
            continue
        cum = _cum_pct_asof(r, entry_hhmm)
        if cum is None:
            continue
        expected_sign = "+" if side == "CALL" else "-"
        actual_sign = "+" if cum >= 0 else "-"
        ok = expected_sign == actual_sign
        matches += ok
        mismatches += not ok
        print(f"  {symbol:14s} ({trade_date_str}) side={side:4s} entry={entry_hhmm}  "
              f"real cum%={cum:+.2f}%  {'MATCH' if ok else '*** MISMATCH ***'}")
    print(f"\n{matches} match, {mismatches} mismatch (out of {matches+mismatches} traded stocks checked)")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
