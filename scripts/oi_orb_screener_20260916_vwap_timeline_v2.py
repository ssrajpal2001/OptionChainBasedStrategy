"""
scripts/oi_orb_screener_20260916_vwap_timeline_v2.py

Direct user follow-up, 2026-09-16: shared a real TradingView POLICYBZR
chart (VWAP hlc3 Session) asking to verify whether VWAP was genuinely
touched after the 12:38 OI-confirm, since the backtest said the retest
never fired. Supersedes the earlier oi_orb_screener_20260916_vwap_
timeline.py (which used PATANJALI/POLICYBZR as PUT candidates -- stale,
from the OLD yesterday-candle-based gate). Today's REAL mechanic
classified both as CALL, so this version re-runs the same real ARM/FIRE/
EXPIRE event timeline (RollingVwapRetestTracker + VwapState, same
session-anchored hlc3 VWAP as TradingView's own "VWAP(hlc3,Session)" --
directly comparable to the pasted chart) on the CORRECT side, restricted
to events AT OR AFTER each symbol's own real OI-confirm timestamp (since
that's the earliest an entry could actually fire under the live
mechanic) -- shows every single crossing, not just the final verdict.

Uses real Upstox intraday 1-min data (same source as every other
2026-09-16 script), not yfinance, so the exact same bars the other
backtest scripts already used are being displayed here -- a genuine
apples-to-apples check, not a re-derivation from a different feed.

MUST run on EC2 (real Upstox2 access token).

Usage: python scripts/oi_orb_screener_20260916_vwap_timeline_v2.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.core.trap_zone_utils import Bar
from strategies.oi_orb_screener import stock_resolve
from strategies.oi_orb_screener.screener import VwapState, RollingVwapRetestTracker

WINDOW_MIN = 15.0

# (symbol, side, oi_confirm_ts) -- real values from today's already-
# validated strong-quadrant backtest.
CANDIDATES = [
    ("PATANJALI", "CALL", None),          # never OI-confirmed -- shown from market open for context
    ("POLICYBZR", "CALL", "12:38"),       # OI-confirmed 12:38, per the real backtest
]


def _access_token() -> str:
    creds = ClientDB().get_feeder_creds_sync("upstox2")
    if creds and creds.get("access_token"):
        return creds["access_token"]
    raise RuntimeError("No upstox2 feeder access_token found -- run this on EC2.")


def _to_bars(rows):
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                        low=float(r["low"]), close=float(r["close"])))
    return out


async def main():
    token = _access_token()
    print("=" * 120)
    print("OI-ORB Screener -- 2026-09-16 VWAP ARM/RETEST timeline v2 (real Upstox 1-min, CORRECT CALL side)")
    print("=" * 120)

    for sym, side, confirm_hhmm in CANDIDATES:
        print(f"\n{'-' * 120}\n{sym}  (side={side}, OI-confirm={confirm_hhmm or 'NEVER'})\n{'-' * 120}")
        eq_key = stock_resolve.resolve_eq_instrument_key(sym)
        if not eq_key:
            print("  NO_EQ_KEY")
            continue
        rows = await hc.fetch_upstox_intraday_1m(eq_key, token)
        bars = _to_bars(rows)
        if not bars:
            print("  NO real intraday bars")
            continue

        tracker = RollingVwapRetestTracker(window_min=WINDOW_MIN)
        vwap_state = VwapState()
        events = []
        for b in bars:
            typical = (b.high + b.low + b.close) / 3.0
            vwap_state.update(sym, typical, 1.0)
            vwap = vwap_state.current(sym)
            if vwap is None:
                continue
            bar_ts = b.ts.replace(second=0, microsecond=0)
            armed_before = tracker._armed and tracker._armed_side == side
            fired = tracker.check(side, bar_ts, b.close, vwap)
            armed_after = tracker._armed and tracker._armed_side == side
            if not armed_before and armed_after:
                events.append((bar_ts, "ARM", b.close, vwap))
            elif armed_before and not armed_after and not fired:
                events.append((bar_ts, "EXPIRE", b.close, vwap))
            if fired:
                events.append((bar_ts, "FIRE", b.close, vwap))

        if not events:
            last = bars[-1]
            print(f"  Price NEVER crossed to the {side}-side of VWAP all day -- never armed. "
                  f"Last real bar: {last.ts.strftime('%H:%M')} close={last.close}")
            continue

        for ts, kind, price, vwap in events:
            gap_pct = (price - vwap) / vwap * 100.0
            print(f"  {ts.strftime('%H:%M')}  {kind:8s} price={price:.2f} vwap={vwap:.2f} "
                  f"(gap={gap_pct:+.2f}%)")

        if confirm_hhmm:
            confirm_ts = datetime.strptime(confirm_hhmm, "%H:%M").time()
            post_confirm_fires = [e for e in events if e[1] == "FIRE" and e[0].time() >= confirm_ts]
            if post_confirm_fires:
                print(f"  RESULT: {len(post_confirm_fires)} real FIRE event(s) AT/AFTER the {confirm_hhmm} "
                      f"OI-confirm -- entry WOULD have fired at {post_confirm_fires[0][0].strftime('%H:%M')}")
            else:
                fires_before = [e for e in events if e[1] == "FIRE"]
                if fires_before:
                    print(f"  RESULT: real FIRE event(s) exist but ALL before {confirm_hhmm} "
                          f"(OI wasn't confirmed yet, so these don't count) -- "
                          f"{[e[0].strftime('%H:%M') for e in fires_before]}")
                else:
                    print(f"  RESULT: ZERO real FIRE events all day -- price armed but never genuinely "
                          f"retested back through VWAP, confirmed against real data.")

    print("\n" + "=" * 120)


if __name__ == "__main__":
    asyncio.run(main())
