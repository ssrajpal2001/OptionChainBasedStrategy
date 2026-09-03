"""
scripts/d1trap_banknifty_sr_trade_detail.py — full per-trade detail (entry_ts,
exit_ts, hold-time, initial SL distance) for ONE S&R (tf, exit_mode) variant,
requested 2026-08-08 to answer "how many SL hits, and how early did SL get
hit" for the winning BANKNIFTY candidate (tf=3, exit_mode="raw") that
scripts/d1trap_banknifty_sr_sweep.py's aggregate log doesn't capture (it only
saves date/side/pnl/exit_reason, not per-trade timestamps).

Drives the real D1TrapBearOnlyBook class exactly like
d1trap_banknifty_sr_sweep.py (same harness pieces, reused via import) --
T1/T2 entries suppressed, only the zone pool is used, fed to
SRPingPongTracker per side per day. Reuses the already-warmed
data/trap_zone_cache/ files from the earlier sweep (same htf/itm -> same
strikes/date range).

Usage:
    python3 scripts/d1trap_banknifty_sr_trade_detail.py --htf-minutes 15 \\
        --itm-offset 300 --tf-minutes 3 --exit-mode raw
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import tempfile
from datetime import date, datetime, timedelta
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import GlobalConfig, IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.instrument_registry import REGISTRY  # noqa: E402
import data_layer.historical_candles as historical_candles  # noqa: E402
import data_layer.position_store as position_store  # noqa: E402
import strategies.d1_trap_option.bear_only_book as bb  # noqa: E402
from strategies.d1_trap_option.support_resistance import SRPingPongTracker  # noqa: E402
from strategies.d1_trap_option.book import _fetch_1m_bars, _upstox_key_for  # noqa: E402
from scripts.d1trap_banknifty_sweep import (  # noqa: E402
    UNDERLYING, ATM_ROUND_STEP, _bars_to_df, load_option,
    _fake_fetch_range_1m, _fake_fetch_intraday_1m, _FrozenClock,
    _make_fixed_datetime, _TaskTracker, _FakeBus, _key_to_df,
)

LOT_SIZE = 30


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--htf-minutes", type=int, default=15)
    ap.add_argument("--itm-offset", type=int, default=300)
    ap.add_argument("--tf-minutes", type=int, default=3)
    ap.add_argument("--exit-mode", default="raw")
    ap.add_argument("--start-date", default="2026-07-01")
    args = ap.parse_args()
    start_date = date.fromisoformat(args.start_date)

    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1

    today = datetime.now(IST).date()
    await asyncio.to_thread(REGISTRY.load_sync, UNDERLYING, token)
    expiry = REGISTRY.get_active_expiry(UNDERLYING)
    if not expiry:
        print("FATAL: could not resolve an active BANKNIFTY expiry.")
        return 1

    spot_key = _upstox_key_for(UNDERLYING)
    fetch_start = start_date - timedelta(days=bb._HIST_WARMUP_DAYS)
    spot_bars = await asyncio.to_thread(_fetch_1m_bars, spot_key, fetch_start, today, token)
    if not spot_bars:
        print("FATAL: no real spot data returned.")
        return 1
    spot_df = _bars_to_df(spot_bars)
    _key_to_df[spot_key] = spot_df
    trading_days = sorted(d for d in spot_df["datetime"].dt.date.unique() if start_date <= d <= today)
    print(f"tf={args.tf_minutes}m mode={args.exit_mode} htf={args.htf_minutes}m itm={args.itm_offset} "
          f"| {len(trading_days)} day(s): {trading_days[0]} .. {trading_days[-1]}")

    cfg = GlobalConfig()
    fake_bus = _FakeBus()
    scratch_dir = tempfile.mkdtemp(prefix="d1trap_bnf_sr_detail_")
    clock = _FrozenClock()
    fixed_datetime = _make_fixed_datetime(clock)
    tracker_wrapper = _TaskTracker()

    book = bb.D1TrapBearOnlyBook(
        bus=fake_bus, cfg=cfg, underlying=UNDERLYING, client_id="SRDETAIL",
        binding_id="BNF_SR_DETAIL", lot_multiplier=1, feeder_token=token,
        itm_offset_pts=args.itm_offset, htf_minutes=args.htf_minutes,
    )
    fake_bus.book = book
    book._enter_leg = lambda *a, **k: None

    all_trades = []

    with patch.object(bb, "datetime", fixed_datetime), \
         patch.object(bb, "fetch_upstox_range_1m", _fake_fetch_range_1m), \
         patch.object(historical_candles, "fetch_upstox_intraday_1m", _fake_fetch_intraday_1m), \
         patch.object(position_store, "_DIR", scratch_dir), \
         patch("asyncio.create_task", tracker_wrapper.create_task):

        for day in trading_days:
            day_opens = spot_df[spot_df["datetime"].dt.date == day]
            if day_opens.empty:
                continue
            spot_open = float(day_opens.iloc[0]["open"])
            atm = round(spot_open / ATM_ROUND_STEP) * ATM_ROUND_STEP
            ce_strike, pe_strike = int(atm - args.itm_offset), int(atm + args.itm_offset)

            ce_data = await load_option(ce_strike, "CE", expiry, fetch_start, today, token)
            pe_data = await load_option(pe_strike, "PE", expiry, fetch_start, today, token)
            if ce_data is None or pe_data is None:
                continue

            ce_today_1m = ce_data["m1"][ce_data["m1"]["datetime"].dt.date == day]
            pe_today_1m = pe_data["m1"][pe_data["m1"]["datetime"].dt.date == day]
            from strategies.d1_trap_option.bear_only_book import _Bar
            ce_bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                       for r in ce_today_1m.to_dict("records")]
            pe_bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                       for r in pe_today_1m.to_dict("records")]

            clock.value = datetime(day.year, day.month, day.day, 9, 16, tzinfo=IST)
            book.reset_session()
            book._today = day
            await book._select_strikes_for_today(spot_open)
            await tracker_wrapper.drain()

            merged = sorted(
                [(b.timestamp, "CE", b) for b in ce_bars] + [(b.timestamp, "PE", b) for b in pe_bars],
                key=lambda row: row[0],
            )
            for ts, side, bar in merged:
                clock.value = ts
                series = book._series.get(side)
                if series is None:
                    continue
                series.bars_1m.append(bar)
                series.last_ltp = bar.close
                book._process_new_bar(side)
                await tracker_wrapper.drain()

            for side_label, zones, day_1m, strike in (
                (f"CE{ce_strike}", list(book._series["CE"].zones) if "CE" in book._series else [], ce_today_1m, ce_strike),
                (f"PE{pe_strike}", list(book._series["PE"].zones) if "PE" in book._series else [], pe_today_1m, pe_strike),
            ):
                bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                        for r in day_1m.to_dict("records")]
                if not bars:
                    continue
                sr_tracker = SRPingPongTracker(zones, args.tf_minutes, LOT_SIZE, exit_mode=args.exit_mode)
                entry_ev = None
                for bar in bars:
                    ev = sr_tracker.on_bar(bar)
                    if ev and ev["type"] == "entry":
                        entry_ev = ev
                    elif ev and ev["type"] == "exit":
                        hold_min = round((ev["exit_ts"] - ev["entry_ts"]).total_seconds() / 60)
                        risk_at_entry = ev["entry_premium"] - (entry_ev["initial_sl"] if entry_ev else float("nan"))
                        all_trades.append(dict(
                            date=day.isoformat(), side=side_label, entry_ts=ev["entry_ts"],
                            entry_premium=ev["entry_premium"], initial_sl=entry_ev["initial_sl"] if entry_ev else None,
                            risk_at_entry=risk_at_entry, exit_reason=ev["reason"], exit_price=ev["exit_price"],
                            exit_ts=ev["exit_ts"], hold_min=hold_min, pnl=ev["pnl"],
                        ))
                        break
            print(f"  {day}: done")

    all_trades.sort(key=lambda t: t["entry_ts"])
    print(f"\n{'='*150}\nFULL TRADE DETAIL -- tf={args.tf_minutes}m mode={args.exit_mode}  n={len(all_trades)}")
    print(f"{'Date':<12}{'Side':<9}{'EntryTS':<18}{'Entry':>8}{'InitSL':>8}{'Risk':>7}"
          f"{'ExitReason':<16}{'Exit':>8}{'Hold(m)':>8}{'PnL':>9}")
    for t in all_trades:
        print(f"{t['date']:<12}{t['side']:<9}{t['entry_ts'].strftime('%m-%d %H:%M'):<18}"
              f"{t['entry_premium']:>8.1f}{(t['initial_sl'] or 0):>8.1f}{t['risk_at_entry']:>7.1f}"
              f"{str(t['exit_reason'])[:15]:<16}{t['exit_price']:>8.1f}{t['hold_min']:>8}{t['pnl']:>9.0f}")

    sl_trades = [t for t in all_trades if str(t["exit_reason"]).startswith("sl_")]
    if sl_trades:
        holds = sorted(t["hold_min"] for t in sl_trades)
        print(f"\nSL-exit hold times (minutes): min={holds[0]} p25={holds[len(holds)//4]} "
              f"median={holds[len(holds)//2]} p75={holds[3*len(holds)//4]} max={holds[-1]}")
        quick = [t for t in sl_trades if t["hold_min"] <= 15]
        print(f"SL hits within 15 minutes of entry: {len(quick)}/{len(sl_trades)}")
        for t in quick:
            print(f"  {t['date']} {t['side']} entry@{t['entry_ts'].strftime('%H:%M')} "
                  f"stopped@{t['exit_ts'].strftime('%H:%M')} ({t['hold_min']}m) pnl={t['pnl']:.0f}")

    import json
    out = {"config": vars(args), "trades": [
        {**t, "entry_ts": str(t["entry_ts"]), "exit_ts": str(t["exit_ts"])} for t in all_trades
    ]}
    from pathlib import Path
    out_path = Path(__file__).resolve().parents[1] / "data" / "sweeps" / "banknifty" / \
        f"sr_trade_detail_tf{args.tf_minutes}_{args.exit_mode}.json"
    out_path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"\n-> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
