"""
scripts/d1trap_banknifty_sr_sweep.py — S&R ping-pong candidate for the
BANKNIFTY sweep (2026-08-08), evaluated over the SAME full month window and
the SAME real zones the winning HTF/ITM config's D1TrapBearOnlyBook builds
each day, using _run_sr_variant (the validated S&R state machine from
scripts/d1trap_sr_zone_backtest.py) instead of the T1/T2 tranche entries.

Does NOT touch data/d1trap_sr_exit_variant_log.jsonl (the shared production
log used by the live daily NIFTY/SENSEX/BANKNIFTY S&R tracking runs) --
writes its own tagged output file so it can't collide with anything else.

T1/T2 entries are suppressed entirely (book._enter_leg patched to a no-op)
so the book only does what we need from it: build+maintain the real zone
pool (_process_new_bar's zone state machine runs unmodified) -- exactly
mirroring how run_sr_day in d1trap_banknifty_month_backtest.py fed
_run_sr_variant the zones "the live T1/T2 class actually built and used
that day."

Usage:
    python3 scripts/d1trap_banknifty_sr_sweep.py --htf-minutes 15 --itm-offset 300
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from config.global_config import GlobalConfig, IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.instrument_registry import REGISTRY  # noqa: E402
import data_layer.historical_candles as historical_candles  # noqa: E402
import data_layer.position_store as position_store  # noqa: E402
import strategies.d1_trap_option.bear_only_book as bb  # noqa: E402
from strategies.d1_trap_option.book import _fetch_1m_bars, _upstox_key_for  # noqa: E402
from scripts.d1trap_sr_zone_backtest import _run_sr_variant, _EXIT_MODES  # noqa: E402
from scripts.d1trap_banknifty_sweep import (  # noqa: E402
    UNDERLYING, ATM_ROUND_STEP, _bars_to_df, load_option, _rows_for_key,
    _fake_fetch_range_1m, _fake_fetch_intraday_1m, _FrozenClock,
    _make_fixed_datetime, _TaskTracker, _FakeBus, _opt_cache, _key_to_df,
)

LOT_SIZE = 30
SR_TF_SWEEP = (1, 3, 5, 10, 15)   # 2026-08-08: widened from (1,3,5) per direct user request
                                   # to "optimise the SL trail tf" -- also now includes
                                   # exit_mode="hold_eod" automatically since that's part
                                   # of the shared _EXIT_MODES tuple.
_WIDE_BUFFER_PCTS = (0.02, 0.04, 0.06, 0.08, 0.10)   # exit_mode="buffered" only -- 2026-08-08,
                                   # tests whether a wider give-back allowance (vs the 2%
                                   # default) reduces the trailing-SL whipsaw pattern found
                                   # in the trade-log analysis (most losses were give-backs,
                                   # not wrong entries).
RESULTS_DIR = Path(__file__).resolve().parents[1] / "data" / "sweeps" / "banknifty"


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--htf-minutes", type=int, default=15)
    ap.add_argument("--itm-offset", type=int, default=300)
    ap.add_argument("--start-date", default="2026-07-01")
    args = ap.parse_args()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    start_date = date.fromisoformat(args.start_date)

    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1

    today = datetime.now(IST).date()
    await asyncio.to_thread(REGISTRY.load_sync, UNDERLYING, token)
    if not REGISTRY.is_loaded(UNDERLYING):
        print("FATAL: could not load BANKNIFTY instrument registry.")
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
    print(f"[S&R] {len(trading_days)} trading day(s): {trading_days[0]} .. {trading_days[-1]} "
          f"| htf={args.htf_minutes}m itm={args.itm_offset}")

    cfg = GlobalConfig()
    fake_bus = _FakeBus()
    scratch_dir = tempfile.mkdtemp(prefix="d1trap_bnf_sr_sweep_")
    clock = _FrozenClock()
    fixed_datetime = _make_fixed_datetime(clock)
    tracker = _TaskTracker()

    book = bb.D1TrapBearOnlyBook(
        bus=fake_bus, cfg=cfg, underlying=UNDERLYING, client_id="SRSWEEP",
        binding_id="BNF_SR", lot_multiplier=1, feeder_token=token,
        itm_offset_pts=args.itm_offset, htf_minutes=args.htf_minutes,
    )
    fake_bus.book = book
    book._enter_leg = lambda *a, **k: None   # T1/T2 entries suppressed -- zones only

    all_records: List[dict] = []

    with patch.object(bb, "datetime", fixed_datetime), \
         patch.object(bb, "fetch_upstox_range_1m", _fake_fetch_range_1m), \
         patch.object(historical_candles, "fetch_upstox_intraday_1m", _fake_fetch_intraday_1m), \
         patch.object(position_store, "_DIR", scratch_dir), \
         patch("asyncio.create_task", tracker.create_task):

        for day in trading_days:
            day_opens = spot_df[spot_df["datetime"].dt.date == day]
            if day_opens.empty:
                continue
            expiry = REGISTRY.get_active_expiry_strict(UNDERLYING, day)
            if expiry is None:
                print(f"  [S&R] {day}: SKIP -- true front-month contract for this "
                      f"historical date has already expired and is no longer resolvable.")
                continue
            spot_open = float(day_opens.iloc[0]["open"])
            atm = round(spot_open / ATM_ROUND_STEP) * ATM_ROUND_STEP
            ce_strike, pe_strike = int(atm - args.itm_offset), int(atm + args.itm_offset)

            ce_data = await load_option(ce_strike, "CE", expiry, fetch_start, today, token)
            pe_data = await load_option(pe_strike, "PE", expiry, fetch_start, today, token)
            if ce_data is None or pe_data is None:
                print(f"  [S&R] {day}: SKIP -- missing real premium data.")
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
            await tracker.drain()

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
                await tracker.drain()

            ce_zones = list(book._series["CE"].zones) if "CE" in book._series else []
            pe_zones = list(book._series["PE"].zones) if "PE" in book._series else []
            print(f"  [S&R] {day}: CE zones={len(ce_zones)} PE zones={len(pe_zones)}")

            for side_label, zones, day_1m in (
                (f"CE{ce_strike}", ce_zones, ce_today_1m), (f"PE{pe_strike}", pe_zones, pe_today_1m),
            ):
                bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                        for r in day_1m.to_dict("records")]
                if not bars:
                    continue
                for tf in SR_TF_SWEEP:
                    for mode in _EXIT_MODES:
                        result = _run_sr_variant(zones, bars, tf, LOT_SIZE, exit_mode=mode)
                        all_records.append({
                            "date": day.isoformat(), "side": side_label, "tf_minutes": tf, "exit_mode": mode,
                            "pnl": result.get("pnl"), "exit_reason": result.get("exit_reason"),
                            "no_entry": bool(result.get("no_entry")),
                        })
                    # Wider give-back buffer sweep -- "buffered" mode only, tagged with a
                    # distinct exit_mode label so it doesn't collide with the plain
                    # "buffered" (2%) row above.
                    for pct in _WIDE_BUFFER_PCTS:
                        result = _run_sr_variant(zones, bars, tf, LOT_SIZE, exit_mode="buffered",
                                                  sl_buffer_pct=pct)
                        all_records.append({
                            "date": day.isoformat(), "side": side_label, "tf_minutes": tf,
                            "exit_mode": f"buffered_{int(pct*100)}pct",
                            "pnl": result.get("pnl"), "exit_reason": result.get("exit_reason"),
                            "no_entry": bool(result.get("no_entry")),
                        })

    out_path = RESULTS_DIR / f"sr_htf{args.htf_minutes}_itm{args.itm_offset}.json"
    out_path.write_text(json.dumps(all_records, indent=2, default=str), encoding="utf-8")

    print(f"\n{'='*70}\nS&R RESULTS by (tf, exit_mode) -- htf={args.htf_minutes}m itm={args.itm_offset}")
    print(f"  {'TF':<5}{'Mode':<16}{'Trades':>8}{'Win%':>7}{'Net P&L':>14}{'PF':>10}")
    combos = sorted({(r["tf_minutes"], r["exit_mode"]) for r in all_records})
    best = None
    for tf, mode in combos:
        bucket = [r for r in all_records if r["tf_minutes"] == tf and r["exit_mode"] == mode and r["pnl"] is not None]
        n = len(bucket)
        wins = [r["pnl"] for r in bucket if r["pnl"] > 0]
        losses = [r["pnl"] for r in bucket if r["pnl"] <= 0]
        win_pct = (len(wins) / n * 100) if n else 0.0
        net = sum(r["pnl"] for r in bucket)
        gl = abs(sum(losses))
        pf = (sum(wins) / gl) if gl > 0 else (float("inf") if wins else 0.0)
        pf_str = "inf" if pf == float("inf") else f"{pf:.3f}"
        sign = "+" if net >= 0 else ""
        print(f"  {tf}m{'':<3}{mode:<16}{n:>8}{win_pct:>6.1f}%{sign}Rs{net:>10,.0f}{pf_str:>10}")
        if best is None or (pf if pf != float("inf") else 999) > best[0]:
            best = (pf if pf != float("inf") else 999, tf, mode, n, win_pct, net)
    if best:
        print(f"\nBest S&R variant: tf={best[1]}m mode={best[2]}  n={best[3]} win%={best[4]:.1f} "
              f"net=Rs{best[5]:+,.0f} PF={'inf' if best[0]==999 else best[0]:.3f}")
    print(f"-> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
