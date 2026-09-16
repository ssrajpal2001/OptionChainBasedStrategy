"""
scripts/oi_orb_screener_20260916_vwap_timeline.py

Real-data VWAP arm/retest TIMELINE, 2026-09-16, direct user follow-up
(from a real TradingView PATANJALI chart showing price staying well above
VWAP all session while we assigned it PUT): "is it that it was entered
later and before that VWAP was touched -- so ur result should also show
what time all criteria fulfilled without VWAP touch."

The full-trace script only showed the FINAL fired/not-fired verdict for
STEP 4 (VWAP retest). This shows the complete real intraday timeline for
every stock that reached step 4 today: every ARM event (price first
crosses to the correct side of VWAP) and every FIRE event (price retests
back through VWAP within the 15-min window), or an explicit "armed then
EXPIRED without a retest" when 15 minutes passed with no touch -- exactly
the diagnostic gap the user flagged.

Reuses the REAL VwapState (session-anchored cumulative typical-price VWAP,
same hlc3-based formula TradingView's own "VWAP (hlc3, Session)" uses --
directly comparable to the pasted chart) and the REAL RollingVwapRetestTracker
(strategies/oi_orb_screener/screener.py) bar-by-bar over real Yahoo Finance
1-min data -- the SAME data source historical_rolling_retest_check() itself
uses (yfinance period="1d" always returns TODAY's data, which is why this
script -- like that function -- is only valid run on 2026-09-16 itself).
Never reimplements the arm/retest DECISION logic -- calls tracker.check()
for every real bar exactly like the live class does; only the event
LOGGING (which the real function discards) is new here.

MUST run on EC2 or anywhere with real internet access (yfinance, no broker
token needed for this one -- same as the historical retest check itself).

Usage: python scripts/oi_orb_screener_20260916_vwap_timeline.py
"""
from __future__ import annotations

import sys
from datetime import datetime

sys.path.insert(0, ".")

from config.global_config import IST
from strategies.oi_orb_screener.screener import VwapState, RollingVwapRetestTracker

TRADE_DATE = "2026-09-16"
ORB_START = "09:15"
WINDOW_MIN = 15.0

# (symbol, side) -- every stock that passed STEP 1+2+3 in today's full
# trace (oi_orb_screener_20260916_full_trace.py), i.e. actually reached
# the VWAP-retest step. Stocks that failed step 1/2/3 never reach this
# check at all in the live system, so they're intentionally excluded here.
CANDIDATES = [
    ("PATANJALI", "PUT"), ("POLICYBZR", "PUT"), ("PREMIERENE", "PUT"),
    ("PAYTM", "PUT"), ("OFSS", "PUT"), ("BSE", "PUT"), ("TCS", "PUT"),
    ("NYKAA", "PUT"), ("MFSL", "PUT"),
]


def _fmt(ts) -> str:
    return ts.strftime("%H:%M") if ts is not None else "?"


def main():
    import yfinance as yf

    symbols = [s for s, _ in CANDIDATES]
    sides = dict(CANDIDATES)
    tickers = [s + ".NS" for s in symbols]
    print("=" * 120)
    print(f"OI-ORB Screener -- {TRADE_DATE} VWAP ARM/RETEST TIMELINE (real intraday data, "
          f"hlc3 session VWAP -- matches TradingView's own VWAP(hlc3,Session))")
    print("=" * 120)
    print("\nFetching real 1-min data (yfinance)...")
    df = yf.download(tickers, period="1d", interval="1m", progress=False, group_by="ticker")

    for sym, ticker in zip(symbols, tickers):
        side = sides[sym]
        print(f"\n{'-' * 120}")
        print(f"{sym}  (side={side})")
        print(f"{'-' * 120}")
        try:
            sub = df[ticker]
        except Exception as exc:
            print(f"  NO DATA from yfinance: {exc}")
            continue

        tracker = RollingVwapRetestTracker(window_min=WINDOW_MIN)
        vwap_state = VwapState()
        events = []   # (ts, kind, price, vwap) kind in {"ARM","FIRE","EXPIRE"}
        was_armed = False
        last_ts = last_price = last_vwap = None

        for ts, row in sub.iterrows():
            if any(row.get(c) != row.get(c) for c in ("High", "Low", "Close", "Volume")):  # NaN check
                continue
            ts_ist = ts.tz_convert(IST) if ts.tzinfo else ts.tz_localize(IST)
            hhmm = ts_ist.strftime("%H:%M")
            if hhmm < ORB_START:
                continue
            high, low, close, vol = (float(row["High"]), float(row["Low"]),
                                      float(row["Close"]), float(row["Volume"]))
            typical = (high + low + close) / 3.0
            if vol > 0:
                vwap_state.update(sym, typical, vol)
            vwap = vwap_state.current(sym)
            if vwap is None:
                continue
            bar_ts = ts_ist.replace(second=0, microsecond=0)

            armed_before = tracker._armed and tracker._armed_side == side
            fired = tracker.check(side, bar_ts, close, vwap)
            armed_after = tracker._armed and tracker._armed_side == side

            if not armed_before and armed_after:
                events.append((bar_ts, "ARM", close, vwap))
            elif armed_before and not armed_after and not fired:
                # tracker.check() itself silently expires a stale arm at the
                # TOP of the next call once elapsed_min > window_min -- catch
                # that transition here so it shows up as an explicit event.
                events.append((bar_ts, "EXPIRE", close, vwap))
            if fired:
                events.append((bar_ts, "FIRE", close, vwap))

            last_ts, last_price, last_vwap = bar_ts, close, vwap

        if not events:
            print(f"  Price NEVER crossed to the {side}-side of VWAP all day -- "
                  f"never even armed. Last real reading: {_fmt(last_ts)} "
                  f"price={last_price} vwap={last_vwap:.2f}" if last_vwap else "  NO real bars found today.")
            continue

        for ts, kind, price, vwap in events:
            side_desc = "below" if side == "PUT" else "above"
            if kind == "ARM":
                print(f"  {_fmt(ts)}  ARMED    price={price:.2f} vwap={vwap:.2f} "
                      f"(price moved {side_desc} VWAP -- watching for a retest within {WINDOW_MIN:.0f}min)")
            elif kind == "FIRE":
                print(f"  {_fmt(ts)}  FIRED    price={price:.2f} vwap={vwap:.2f} "
                      f"(genuine retest -- this is a real, valid entry signal)")
            elif kind == "EXPIRE":
                print(f"  {_fmt(ts)}  EXPIRED  price={price:.2f} vwap={vwap:.2f} "
                      f"({WINDOW_MIN:.0f}min passed with no retest -- arm discarded, must re-arm)")

        any_fire = any(k == "FIRE" for _, k, _, _ in events)
        last_event_ts = events[-1][0]
        if any_fire:
            # Is the LAST fire within window_min of the last real bar (i.e.
            # still "live"/actionable), matching historical_rolling_retest_
            # check's own staleness rule?
            last_fire_ts = [t for t, k, _, _ in events if k == "FIRE"][-1]
            stale_min = (last_ts - last_fire_ts).total_seconds() / 60.0 if last_ts else None
            if stale_min is not None and stale_min <= WINDOW_MIN:
                print(f"  RESULT: last fire is still within {WINDOW_MIN:.0f}min of the most recent "
                      f"bar -- ACTIONABLE (this is why the trace script showed FIRED).")
            else:
                print(f"  RESULT: fired earlier today but that fire is now {stale_min:.0f}min old "
                      f"(>{WINDOW_MIN:.0f}min) -- STALE, correctly not treated as a live entry any more.")
        else:
            print(f"  RESULT: armed {sum(1 for _,k,_,_ in events if k=='ARM')} time(s) today but NEVER "
                  f"retested back through VWAP -- price moved to the {('below' if side=='PUT' else 'above')} "
                  f"side and just kept going, it never came back to touch VWAP. This is exactly why no "
                  f"trade fired -- not a bug, the setup genuinely never completed.")

    print("\n" + "=" * 120)


if __name__ == "__main__":
    main()
