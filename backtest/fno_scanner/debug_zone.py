"""
Debug: show exactly which D1 bars + zones the scanner detected for a given stock.
Usage: python3 backtest/fno_scanner/debug_zone.py DMART RADICO
"""
from __future__ import annotations
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.fno_scanner.scan_live import _fetch_fno_universe, HARD_SL_BUF, MIN_RR, MAX_ZONE_AGE, APPROACH_PCT
from backtest.fno_scanner.backtest import load_or_fetch
from strategies.v4_cascade.rolling_base import find_all_bear_zones, find_all_bull_zones


def debug_stock(symbol: str, token: str) -> None:
    from datetime import datetime as _dt
    from config.global_config import IST
    _now_ist = _dt.now(IST)
    if _now_ist.hour > 15 or (_now_ist.hour == 15 and _now_ist.minute >= 31):
        end_date = date.today()
    else:
        end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=31)

    print(f"\n{'='*70}")
    print(f"  DEBUG: {symbol}  ({start_date} → {end_date})")
    print(f"{'='*70}")

    universe = _fetch_fno_universe(token)
    key = universe.stocks.get(symbol)
    if not key:
        print(f"  {symbol} not found in FnO universe")
        return

    bars = load_or_fetch(symbol, key, token, start_date, end_date)
    print(f"\n  {len(bars)} D1 bars loaded")
    print(f"\n  Last 10 D1 bars:")
    print(f"  {'Date':<12} {'Open':>8} {'High':>8} {'Low':>8} {'Close':>8}")
    print(f"  {'-'*12} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    for b in bars[-10:]:
        print(f"  {str(b.timestamp.date()):<12} {b.open:>8.1f} {b.high:>8.1f} {b.low:>8.1f} {b.close:>8.1f}")

    last_bar = bars[-1]
    today = last_bar.timestamp.date()
    print(f"\n  Last bar: {last_bar.timestamp.date()}  O={last_bar.open:.1f}  H={last_bar.high:.1f}  L={last_bar.low:.1f}  C={last_bar.close:.1f}")

    bear_zones = find_all_bear_zones(bars)
    bull_zones = find_all_bull_zones(bars)
    all_zones  = [(z, "CE") for z in bear_zones] + [(z, "PE") for z in bull_zones]

    print(f"\n  Zones found: {len(bear_zones)} bear (CE) + {len(bull_zones)} bull (PE)")

    for zone, direction in all_zones:
        if zone.entry_line is None or zone.sweep_low is None or zone.lock_ts is None:
            continue

        age_days = (today - zone.lock_ts.date()).days
        if age_days > MAX_ZONE_AGE:
            continue

        entry_line = zone.entry_line
        sweep_ref  = zone.sweep_low
        zone_lo    = min(entry_line, sweep_ref)
        zone_hi    = max(entry_line, sweep_ref)

        if direction == "CE":
            hard_sl  = zone_lo * (1 - HARD_SL_BUF / 100)
            day_t1   = last_bar.high
            risk     = entry_line - hard_sl
            reward   = day_t1 - entry_line
            retest   = last_bar.low <= entry_line and last_bar.close >= zone_lo
            dist_pct = (last_bar.close - entry_line) / entry_line * 100
            btst_reward = day_t1 - last_bar.close
            btst_risk   = last_bar.close - hard_sl
        else:
            hard_sl  = zone_hi * (1 + HARD_SL_BUF / 100)
            day_t1   = last_bar.low
            risk     = hard_sl - entry_line
            reward   = entry_line - day_t1
            retest   = last_bar.high >= entry_line and last_bar.close <= zone_hi
            dist_pct = (entry_line - last_bar.close) / entry_line * 100
            btst_reward = last_bar.close - day_t1
            btst_risk   = hard_sl - last_bar.close

        if risk <= 0 or reward < 0:
            continue
        rr = reward / risk
        btst_rr = (btst_reward / btst_risk) if btst_risk > 0 else 0.0
        if rr < MIN_RR:
            continue

        # T1 consumption: how much of entry→T1 headroom has close already used?
        if direction == "CE":
            t1_consumed = (last_bar.close - entry_line) / (day_t1 - entry_line) if (day_t1 - entry_line) > 0 else 1.0
        else:
            t1_consumed = (entry_line - last_bar.close) / (entry_line - day_t1) if (entry_line - day_t1) > 0 else 1.0

        status = "TRIGGERED" if retest else ("APPROACHING" if abs(dist_pct) <= APPROACH_PCT else "FAR")

        print(f"\n  [{direction}]  lock={zone.lock_ts.date()}  age={age_days}d  entry={entry_line:.1f}  "
              f"sweep={sweep_ref:.1f}  zone=[{zone_lo:.1f}–{zone_hi:.1f}]")
        print(f"         sl={hard_sl:.1f}  t1={day_t1:.1f}  risk={risk:.1f}  reward={reward:.1f}  zone_rr={rr:.2f}")
        print(f"         BTST (from close={last_bar.close:.1f}): reward={btst_reward:.1f}  risk={btst_risk:.1f}  btst_rr={btst_rr:.2f}")
        print(f"         t1_consumed={t1_consumed:.0%}  (>80% → would be filtered out)")
        print(f"         last.H={last_bar.high:.1f} >= entry({entry_line:.1f})? {last_bar.high >= entry_line}  "
              f"last.L={last_bar.low:.1f} <= entry({entry_line:.1f})? {last_bar.low <= entry_line}")
        print(f"         last.C={last_bar.close:.1f} <= zone_hi({zone_hi:.1f})? {last_bar.close <= zone_hi}  "
              f"last.C={last_bar.close:.1f} >= zone_lo({zone_lo:.1f})? {last_bar.close >= zone_lo}")
        print(f"         retest={retest}  dist%={dist_pct:.2f}%  → STATUS: {status}")


if __name__ == "__main__":
    import asyncio

    async def _get_token():
        from data_layer.client_db import ClientDB
        db = ClientDB(); await db.initialise()
        return (db.get_feeder_creds_sync("upstox") or {}).get("access_token", "")

    token = asyncio.run(_get_token())
    symbols = sys.argv[1:] if len(sys.argv) > 1 else ["DMART", "RADICO"]
    for sym in symbols:
        debug_stock(sym.upper(), token)
