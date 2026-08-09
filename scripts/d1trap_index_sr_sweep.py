"""
scripts/d1trap_index_sr_sweep.py — generic (any index) S&R ping-pong sweep,
generalized from scripts/d1trap_banknifty_sr_sweep.py 2026-08-09 so NIFTY/
SENSEX can get the same tf x exit_mode validation BANKNIFTY got, reusing
each index's ALREADY-VALIDATED HTF/ITM zone config
(_HTF_MINUTES_DEFAULT_BY_UNDERLYING / _ITM_OFFSET_DEFAULT_BY_UNDERLYING in
bear_only_book.py) rather than re-sweeping those from scratch -- narrower
scope than the full BANKNIFTY validation (no HTF/ITM re-sweep, no entry-
mechanic-vs-T1/T2 comparison), by direct user request given time pressure
for a next-day go-live decision. Flag clearly to the user that this is a
faster, less thorough pass than BANKNIFTY's.

Drives the REAL D1TrapBearOnlyBook class for zone detection (same harness
pattern as d1trap_banknifty_sweep.py/d1trap_banknifty_sr_sweep.py), then
feeds each day's zones to SRPingPongTracker across the tf x exit_mode grid
via _run_sr_variant -- same single-source-of-truth discipline as every
other S&R backtest this session.

Usage:
    python3 scripts/d1trap_index_sr_sweep.py --underlying NIFTY --start-date 2026-07-01
    python3 scripts/d1trap_index_sr_sweep.py --underlying SENSEX --start-date 2026-07-01
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
from execution_bridge.d1_trap_bridge import D1TrapFillEvent  # noqa: E402
from scripts.d1trap_sr_zone_backtest import _run_sr_variant, _EXIT_MODES  # noqa: E402

ATM_ROUND_STEP = 100
SR_TF_SWEEP = (1, 3, 5, 10)
RESULTS_DIR = Path(__file__).resolve().parents[1] / "data" / "sweeps"

_opt_cache: Dict[tuple, Optional[dict]] = {}
_key_to_df: Dict[str, pd.DataFrame] = {}


def _bars_to_df(bars: list) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close"])
    return pd.DataFrame([
        {"datetime": b.timestamp, "open": b.open, "high": b.high, "low": b.low, "close": b.close}
        for b in bars
    ])


async def load_option(underlying: str, strike: int, side: str, expiry: date, fetch_start: date,
                       fetch_end: date, token: str) -> Optional[dict]:
    key = (underlying, expiry, strike, side)
    if key in _opt_cache:
        return _opt_cache[key]
    opt_key = REGISTRY.get_upstox_key(underlying, expiry, strike, side)
    if not opt_key:
        _opt_cache[key] = None
        return None
    bars = await asyncio.to_thread(_fetch_1m_bars, opt_key, fetch_start, fetch_end, token)
    if not bars:
        _opt_cache[key] = None
        return None
    df = _bars_to_df(bars)
    data = dict(m1=df, upstox_key=opt_key)
    _opt_cache[key] = data
    _key_to_df[opt_key] = df
    return data


def _rows_for_key(instrument_key: str, start: date, end: date) -> List[dict]:
    df = _key_to_df.get(instrument_key)
    if df is None or df.empty:
        return []
    mask = (df["datetime"].dt.date >= start) & (df["datetime"].dt.date <= end)
    sub = df[mask]
    return [
        {"ts": r["datetime"].isoformat(), "open": float(r["open"]), "high": float(r["high"]),
         "low": float(r["low"]), "close": float(r["close"])}
        for _, r in sub.iterrows()
    ]


async def _fake_fetch_range_1m(instrument_key: str, access_token: str, start: date, end: date) -> List[dict]:
    return _rows_for_key(instrument_key, start, end)


async def _fake_fetch_intraday_1m(instrument_key: str, access_token: str) -> List[dict]:
    return []


class _FrozenClock:
    value: Optional[datetime] = None


def _make_fixed_datetime(clock: _FrozenClock):
    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock.value
    return _FixedDatetime


class _TaskTracker:
    def __init__(self) -> None:
        self.pending: list = []
        self._real_create_task = asyncio.create_task

    def create_task(self, coro, **kwargs):
        t = self._real_create_task(coro, **kwargs)
        self.pending.append(t)
        return t

    async def drain(self) -> None:
        if not self.pending:
            return
        tasks, self.pending = self.pending, []
        await asyncio.gather(*tasks, return_exceptions=True)


class _FakeBus:
    def __init__(self) -> None:
        self.events: list = []
        self.book: Optional["bb.D1TrapBearOnlyBook"] = None

    async def publish(self, topic, event) -> None:
        self.events.append((topic, event))
        if getattr(event, "action", None) == "SELL" and self.book is not None:
            fill = D1TrapFillEvent(
                action="SELL", underlying=event.underlying, option_type=event.option_type,
                strike=event.strike, fill_price=event.exit_price, qty=event.quantity,
                client_id=event.client_id, binding_id=event.binding_id,
                event_id=event.event_id, paper_mode=True,
            )
            self.book._on_fill(fill)

    def subscribe(self, topic):
        return asyncio.Queue()

    def unsubscribe(self, topic, q) -> None:
        pass


async def run_t1t2_day(book, clock, tracker, day, ce_bars, pe_bars, spot_open) -> None:
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


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--underlying", required=True,
                     choices=["NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"])
    ap.add_argument("--start-date", default="2026-07-01")
    ap.add_argument("--htf-minutes", type=int, default=None)
    ap.add_argument("--itm-offset", type=int, default=None)
    args = ap.parse_args()
    underlying = args.underlying
    start_date = date.fromisoformat(args.start_date)
    htf_minutes = args.htf_minutes or bb._HTF_MINUTES_DEFAULT_BY_UNDERLYING.get(underlying, 60)
    itm_offset = args.itm_offset or bb._ITM_OFFSET_DEFAULT_BY_UNDERLYING.get(underlying, 200)

    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1

    cfg = GlobalConfig()
    lot_size = int(cfg.exchange.lot_sizes.get(underlying, 75))
    print(f"[{underlying}] htf={htf_minutes}m itm_offset={itm_offset}pt lot_size={lot_size}")

    today = datetime.now(IST).date()
    await asyncio.to_thread(REGISTRY.load_sync, underlying, token)
    if not REGISTRY.is_loaded(underlying):
        print(f"FATAL: could not load {underlying} instrument registry.")
        return 1

    spot_key = _upstox_key_for(underlying)
    fetch_start = start_date - timedelta(days=bb._HIST_WARMUP_DAYS)
    spot_bars = await asyncio.to_thread(_fetch_1m_bars, spot_key, fetch_start, today, token)
    if not spot_bars:
        print("FATAL: no real spot data returned.")
        return 1
    spot_df = _bars_to_df(spot_bars)
    _key_to_df[spot_key] = spot_df
    trading_days = sorted(d for d in spot_df["datetime"].dt.date.unique() if start_date <= d <= today)
    print(f"{len(trading_days)} trading day(s): {trading_days[0]} .. {trading_days[-1]}")

    fake_bus = _FakeBus()
    scratch_dir = tempfile.mkdtemp(prefix=f"d1trap_{underlying.lower()}_sr_sweep_")
    clock = _FrozenClock()
    fixed_datetime = _make_fixed_datetime(clock)
    tracker = _TaskTracker()

    book = bb.D1TrapBearOnlyBook(
        bus=fake_bus, cfg=cfg, underlying=underlying, client_id="SRSWEEP",
        binding_id=f"{underlying}_SR_SWEEP", lot_multiplier=1, feeder_token=token,
        itm_offset_pts=itm_offset, htf_minutes=htf_minutes,
    )
    fake_bus.book = book

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
            expiry = REGISTRY.get_active_expiry_strict(underlying, day)
            if expiry is None:
                print(f"  {day}: SKIP -- true front-month contract for this historical "
                      f"date has already expired and is no longer resolvable.")
                continue
            spot_open = float(day_opens.iloc[0]["open"])
            atm = round(spot_open / ATM_ROUND_STEP) * ATM_ROUND_STEP
            ce_strike, pe_strike = int(atm - itm_offset), int(atm + itm_offset)

            ce_data = await load_option(underlying, ce_strike, "CE", expiry, fetch_start, today, token)
            pe_data = await load_option(underlying, pe_strike, "PE", expiry, fetch_start, today, token)
            if ce_data is None or pe_data is None:
                print(f"  {day}: SKIP -- missing real premium data for one or both strikes.")
                continue

            ce_today_1m = ce_data["m1"][ce_data["m1"]["datetime"].dt.date == day]
            pe_today_1m = pe_data["m1"][pe_data["m1"]["datetime"].dt.date == day]
            from strategies.d1_trap_option.bear_only_book import _Bar
            ce_bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                       for r in ce_today_1m.to_dict("records")]
            pe_bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                       for r in pe_today_1m.to_dict("records")]

            await run_t1t2_day(book, clock, tracker, day, ce_bars, pe_bars, spot_open)

            ce_zones = list(book._series["CE"].zones) if "CE" in book._series else []
            pe_zones = list(book._series["PE"].zones) if "PE" in book._series else []
            print(f"  {day}: CE zones={len(ce_zones)} PE zones={len(pe_zones)}")

            for side_label, zones, day_1m in (
                (f"CE{ce_strike}", ce_zones, ce_today_1m), (f"PE{pe_strike}", pe_zones, pe_today_1m),
            ):
                bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                        for r in day_1m.to_dict("records")]
                if not bars:
                    continue
                for tf in SR_TF_SWEEP:
                    for mode in _EXIT_MODES:
                        result = _run_sr_variant(zones, bars, tf, lot_size, exit_mode=mode)
                        all_records.append({
                            "date": day.isoformat(), "side": side_label, "tf_minutes": tf, "exit_mode": mode,
                            "pnl": result.get("pnl"), "exit_reason": result.get("exit_reason"),
                            "no_entry": bool(result.get("no_entry")),
                        })

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RESULTS_DIR / f"{underlying.lower()}_sr_htf{htf_minutes}_itm{itm_offset}.json"
    out_path.write_text(json.dumps(all_records, indent=2, default=str), encoding="utf-8")

    print(f"\n{'='*70}\n{underlying} S&R RESULTS by (tf, exit_mode) -- htf={htf_minutes}m itm={itm_offset}")
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
        if n >= 5 and (best is None or (pf if pf != float("inf") else 999) > best[0]):
            best = (pf if pf != float("inf") else 999, tf, mode, n, win_pct, net)
    if best:
        print(f"\nBest {underlying} S&R variant (n>=5): tf={best[1]}m mode={best[2]}  n={best[3]} "
              f"win%={best[4]:.1f} net=Rs{best[5]:+,.0f} PF={'inf' if best[0]==999 else best[0]:.3f}")
    print(f"-> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
