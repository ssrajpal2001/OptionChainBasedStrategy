"""
scripts/d1trap_banknifty_sr_gate_compare.py — 2026-08-09, direct answer to the
user's question: after HTF zone + LTP-touches-zone, should S&R start
immediately (gate_mode="touch", the validated config) or only after the
zone's own MTF ref-candle breach (gate_mode="breach", same trigger T1 uses)?

Builds the REAL zone pool ONCE per day (driving D1TrapBearOnlyBook's actual
T1/T2 state machine, which populates breach_ts as a side effect -- needed for
gate_mode="breach"), then runs BOTH gate modes' SRPingPongTracker against the
SAME zones/bars for a clean, single-source-of-truth A/B comparison at the
already-validated tf=3m/exit_mode=raw/htf=15m/itm=300 BANKNIFTY config.

Usage:
    python3 scripts/d1trap_banknifty_sr_gate_compare.py
"""
from __future__ import annotations

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

from config.global_config import GlobalConfig, IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.instrument_registry import REGISTRY  # noqa: E402
import data_layer.historical_candles as historical_candles  # noqa: E402
import data_layer.position_store as position_store  # noqa: E402
import strategies.d1_trap_option.bear_only_book as bb  # noqa: E402
from strategies.d1_trap_option.book import _fetch_1m_bars, _upstox_key_for  # noqa: E402
from scripts.d1trap_sr_zone_backtest import _run_sr_variant  # noqa: E402
from scripts.d1trap_banknifty_sweep import (  # noqa: E402
    UNDERLYING, ATM_ROUND_STEP, _bars_to_df, load_option,
    _fake_fetch_range_1m, _fake_fetch_intraday_1m, _FrozenClock,
    _make_fixed_datetime, _TaskTracker, _FakeBus, _key_to_df,
)

HTF_MINUTES = 15
ITM_OFFSET = 300
SR_TF = 3
EXIT_MODE = "raw"
LOT_SIZE = 30


async def main() -> int:
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

    start_date = date(2026, 7, 1)
    spot_key = _upstox_key_for(UNDERLYING)
    fetch_start = start_date - timedelta(days=bb._HIST_WARMUP_DAYS)
    spot_bars = await asyncio.to_thread(_fetch_1m_bars, spot_key, fetch_start, today, token)
    if not spot_bars:
        print("FATAL: no real spot data returned.")
        return 1
    spot_df = _bars_to_df(spot_bars)
    _key_to_df[spot_key] = spot_df
    trading_days = sorted(d for d in spot_df["datetime"].dt.date.unique() if start_date <= d <= today)
    print(f"{len(trading_days)} trading day(s): {trading_days[0]} .. {trading_days[-1]}\n")

    cfg = GlobalConfig()
    fake_bus = _FakeBus()
    scratch_dir = tempfile.mkdtemp(prefix="d1trap_bnf_gate_compare_")
    clock = _FrozenClock()
    fixed_datetime = _make_fixed_datetime(clock)
    tracker = _TaskTracker()

    book = bb.D1TrapBearOnlyBook(
        bus=fake_bus, cfg=cfg, underlying=UNDERLYING, client_id="GATECMP",
        binding_id="BNF_GATE_CMP", lot_multiplier=1, feeder_token=token,
        itm_offset_pts=ITM_OFFSET, htf_minutes=HTF_MINUTES,
    )
    fake_bus.book = book

    records_touch: List[dict] = []
    records_breach: List[dict] = []

    with patch.object(bb, "datetime", fixed_datetime), \
         patch.object(bb, "fetch_upstox_range_1m", _fake_fetch_range_1m), \
         patch.object(historical_candles, "fetch_upstox_intraday_1m", _fake_fetch_intraday_1m), \
         patch.object(position_store, "_DIR", scratch_dir), \
         patch("asyncio.create_task", tracker.create_task):

        for day in trading_days:
            day_opens = spot_df[spot_df["datetime"].dt.date == day]
            if day_opens.empty:
                continue
            spot_open = float(day_opens.iloc[0]["open"])
            atm = round(spot_open / ATM_ROUND_STEP) * ATM_ROUND_STEP
            ce_strike, pe_strike = int(atm - ITM_OFFSET), int(atm + ITM_OFFSET)

            ce_data = await load_option(ce_strike, "CE", expiry, fetch_start, today, token)
            pe_data = await load_option(pe_strike, "PE", expiry, fetch_start, today, token)
            if ce_data is None or pe_data is None:
                print(f"  {day}: SKIP -- missing real premium data.")
                continue

            ce_today_1m = ce_data["m1"][ce_data["m1"]["datetime"].dt.date == day]
            pe_today_1m = pe_data["m1"][pe_data["m1"]["datetime"].dt.date == day]
            from strategies.d1_trap_option.bear_only_book import _Bar
            ce_bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                       for r in ce_today_1m.to_dict("records")]
            pe_bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                       for r in pe_today_1m.to_dict("records")]

            # Drive the REAL T1/T2 state machine to build zones WITH breach_ts populated.
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
                book._check_exit(side, bar.close, ts)
                book._check_fast_flip_tranche1(side, bar.high, ts)
                book._process_new_bar(side)
                await tracker.drain()
            for _ in range(50):
                await tracker.drain()
                if not book._positions:
                    break
                await asyncio.sleep(0.01)

            ce_zones = list(book._series["CE"].zones) if "CE" in book._series else []
            pe_zones = list(book._series["PE"].zones) if "PE" in book._series else []
            n_breach = sum(1 for z in ce_zones + pe_zones if z.get("breach_ts") is not None)
            print(f"  {day}: CE zones={len(ce_zones)} PE zones={len(pe_zones)} (breached={n_breach})")

            for side_label, zones, day_1m in (
                (f"CE{ce_strike}", ce_zones, ce_today_1m), (f"PE{pe_strike}", pe_zones, pe_today_1m),
            ):
                bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                        for r in day_1m.to_dict("records")]
                if not bars:
                    continue
                r_touch = _run_sr_variant(zones, bars, SR_TF, LOT_SIZE, exit_mode=EXIT_MODE, gate_mode="touch")
                r_breach = _run_sr_variant(zones, bars, SR_TF, LOT_SIZE, exit_mode=EXIT_MODE, gate_mode="breach")
                for rec_list, result in ((records_touch, r_touch), (records_breach, r_breach)):
                    rec_list.append({
                        "date": day.isoformat(), "side": side_label,
                        "pnl": result.get("pnl"), "exit_reason": result.get("exit_reason"),
                        "no_entry": bool(result.get("no_entry")),
                    })

    def _summarize(records, label):
        trades = [r for r in records if r["pnl"] is not None]
        wins = [r["pnl"] for r in trades if r["pnl"] > 0]
        losses = [r["pnl"] for r in trades if r["pnl"] <= 0]
        gl = abs(sum(losses))
        pf = (sum(wins) / gl) if gl > 0 else (float("inf") if wins else 0.0)
        win_pct = (100 * len(wins) / len(trades)) if trades else 0.0
        net = sum(r["pnl"] for r in trades)
        print(f"\n{label}: n={len(trades)} win%={win_pct:.1f} net=Rs{net:+,.0f} "
              f"PF={'inf' if pf==float('inf') else round(pf,3)}")
        return dict(n=len(trades), win_pct=win_pct, net=net, pf=("inf" if pf==float("inf") else pf))

    print(f"\n{'='*70}\nGATE MODE COMPARISON -- BANKNIFTY htf={HTF_MINUTES}m itm={ITM_OFFSET} "
          f"tf={SR_TF}m exit_mode={EXIT_MODE}")
    s_touch = _summarize(records_touch, "Option A (gate_mode=touch, start S&R immediately)")
    s_breach = _summarize(records_breach, "Option B (gate_mode=breach, wait for MTF ref-candle breach)")

    out = {"touch": records_touch, "breach": records_breach,
           "summary": {"touch": s_touch, "breach": s_breach}}
    out_path = Path(__file__).resolve().parents[1] / "data" / "sweeps" / "banknifty" / "sr_gate_mode_compare.json"
    out_path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"\n-> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
