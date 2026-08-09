"""
scripts/d1trap_index_full_sweep.py — generalized (any MONTHLY-expiry index)
version of scripts/d1trap_banknifty_sweep.py, 2026-08-09.

IMPORTANT: only valid for MONTHLY-expiry underlyings (BANKNIFTY, FINNIFTY,
MIDCPNIFTY) -- resolves ONE fixed expiry (REGISTRY.get_active_expiry, no
from_date) and uses it for the ENTIRE backtest window, exactly like the
original BANKNIFTY script. This is WRONG for weekly-expiry underlyings
(NIFTY, SENSEX) -- confirmed live 2026-08-09: both those runs silently used
a single ~5-week-out contract for the whole window, invalidating those
results. Do NOT point this script at NIFTY/SENSEX without first adding
proper rolling multi-contract expiry resolution.

Drives the REAL D1TrapBearOnlyBook class (same harness as
scripts/d1trap_banknifty_sweep.py) with the same on/off toggles: entry-mode
(cascade/breakout_only/ltf_only), flip, structure-sl, reentry-cap, risk-cap,
tsl-step, bias-filter. Used for the HTF sweep, ITM-depth sweep, and T1/T2
entry-mechanic comparison stages of the per-index validation pipeline (S&R
itself is validated separately via scripts/d1trap_index_sr_sweep.py).

Usage:
    python3 scripts/d1trap_index_full_sweep.py --underlying FINNIFTY \\
        --tag stage1_htf60 --htf-minutes 60 --itm-offset 150 \\
        --entry-mode cascade --flip on --structure-sl off --reentry-cap 0 \\
        --risk-cap flat --tsl-step 0.20 --tsl-step-lock 0.125 --bias-filter on
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

ATM_ROUND_STEP = 100
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


def _apply_entry_mode_wrapper(book, entry_mode: str) -> None:
    if entry_mode == "cascade":
        return
    orig = book._enter_leg

    def patched(side, tranche, entry_price, sl, zone_lock_ts, order_reason, use_tranche_tsl=False):
        if entry_mode == "breakout_only" and order_reason == "bear_trap_swing_breach_t2":
            return
        if entry_mode == "ltf_only" and order_reason == "bear_trap_ref_breach_t1":
            return
        return orig(side, tranche, entry_price, sl, zone_lock_ts, order_reason, use_tranche_tsl)

    book._enter_leg = patched


def _apply_flip_off(book) -> None:
    book._create_flip_candidate = lambda *a, **k: None
    book._process_flip_cancellation = lambda *a, **k: None
    book._process_flip_entry = lambda *a, **k: None
    book._check_fast_flip_tranche1 = lambda *a, **k: None


def _apply_bias_off(book) -> None:
    book._bias_allows = lambda side: True


async def run_t1t2_day(book, clock: _FrozenClock, tracker: _TaskTracker, day: date,
                        ce_bars: List, pe_bars: List, spot_open: float) -> None:
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


def _metrics(events: list) -> dict:
    trades = [ev for topic, ev in events if getattr(ev, "action", None) == "SELL"]
    trades.sort(key=lambda ev: ev.entry_ts or ev.trigger_ts)
    rows = []
    for ev in trades:
        pnl = (ev.exit_price - ev.entry_price) * ev.quantity
        rows.append(dict(
            date=str((ev.entry_ts or ev.trigger_ts).date()) if (ev.entry_ts or ev.trigger_ts) else None,
            side=ev.option_type, strike=ev.strike, entry_reason=ev.entry_reason,
            entry_price=ev.entry_price, exit_price=ev.exit_price, exit_reason=ev.reason,
            entry_ts=str(ev.entry_ts) if ev.entry_ts else None, exit_ts=str(ev.trigger_ts),
            pnl=pnl,
        ))
    wins = [r["pnl"] for r in rows if r["pnl"] > 0]
    losses = [r["pnl"] for r in rows if r["pnl"] <= 0]
    gw, gl = sum(wins), abs(sum(losses))
    pf = (gw / gl) if gl > 0 else (float("inf") if gw > 0 else 0.0)
    net = sum(r["pnl"] for r in rows)
    win_pct = (100 * len(wins) / len(rows)) if rows else 0.0
    return dict(n=len(rows), win_pct=round(win_pct, 1), net=round(net, 2),
                pf=(None if pf == float("inf") else round(pf, 3)), pf_inf=(pf == float("inf")),
                gross_win=round(gw, 2), gross_loss=round(gl, 2), trades=rows)


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--underlying", required=True, choices=["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"])
    ap.add_argument("--tag", required=True)
    ap.add_argument("--htf-minutes", type=int, default=60)
    ap.add_argument("--itm-offset", type=int, default=150)
    ap.add_argument("--entry-mode", choices=["cascade", "breakout_only", "ltf_only"], default="cascade")
    ap.add_argument("--flip", choices=["on", "off"], default="on")
    ap.add_argument("--structure-sl", choices=["on", "off"], default="off")
    ap.add_argument("--reentry-cap", type=int, default=0)
    ap.add_argument("--risk-cap", default="flat")
    ap.add_argument("--tsl-step", type=float, default=0.20)
    ap.add_argument("--tsl-step-lock", type=float, default=0.125)
    ap.add_argument("--bias-filter", choices=["on", "off"], default="on")
    ap.add_argument("--start-date", default="2026-07-01")
    args = ap.parse_args()
    underlying = args.underlying

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    start_date = date.fromisoformat(args.start_date)

    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1

    today = datetime.now(IST).date()
    await asyncio.to_thread(REGISTRY.load_sync, underlying, token)
    if not REGISTRY.is_loaded(underlying):
        print(f"FATAL: could not load {underlying} instrument registry.")
        return 1

    cfg = GlobalConfig()
    lot_size = int(cfg.exchange.lot_sizes.get(underlying, 75))

    spot_key = _upstox_key_for(underlying)
    fetch_start = start_date - timedelta(days=bb._HIST_WARMUP_DAYS)
    spot_bars = await asyncio.to_thread(_fetch_1m_bars, spot_key, fetch_start, today, token)
    if not spot_bars:
        print("FATAL: no real spot data returned.")
        return 1
    spot_df = _bars_to_df(spot_bars)
    _key_to_df[spot_key] = spot_df
    trading_days = sorted(d for d in spot_df["datetime"].dt.date.unique() if start_date <= d <= today)
    print(f"[{args.tag}] {len(trading_days)} trading day(s): {trading_days[0]} .. {trading_days[-1]} "
          f"| htf={args.htf_minutes}m itm={args.itm_offset} mode={args.entry_mode} flip={args.flip} "
          f"struct_sl={args.structure_sl} reentry={args.reentry_cap} risk_cap={args.risk_cap} "
          f"tsl_step={args.tsl_step}/{args.tsl_step_lock} bias={args.bias_filter}")

    fake_bus = _FakeBus()
    scratch_dir = tempfile.mkdtemp(prefix=f"d1trap_{underlying.lower()}_sweep_{args.tag}_")
    clock = _FrozenClock()
    fixed_datetime = _make_fixed_datetime(clock)
    tracker = _TaskTracker()

    book = bb.D1TrapBearOnlyBook(
        bus=fake_bus, cfg=cfg, underlying=underlying, client_id="SWEEP",
        binding_id=f"{underlying}_{args.tag}", lot_multiplier=1, feeder_token=token,
        itm_offset_pts=args.itm_offset, htf_minutes=args.htf_minutes,
    )
    fake_bus.book = book

    if args.entry_mode != "cascade":
        _apply_entry_mode_wrapper(book, args.entry_mode)
    if args.flip == "off":
        _apply_flip_off(book)
    if args.bias_filter == "off":
        _apply_bias_off(book)

    reentry_patch = {underlying: args.reentry_cap} if args.reentry_cap > 0 else {}
    risk_pct_patch = {underlying: float(args.risk_cap)} if args.risk_cap != "flat" else {}
    tsl_step_patch = {underlying: args.tsl_step}
    tsl_lock_patch = {underlying: args.tsl_step_lock}

    with patch.object(bb, "datetime", fixed_datetime), \
         patch.object(bb, "fetch_upstox_range_1m", _fake_fetch_range_1m), \
         patch.object(historical_candles, "fetch_upstox_intraday_1m", _fake_fetch_intraday_1m), \
         patch.object(position_store, "_DIR", scratch_dir), \
         patch.object(bb, "_STRUCTURE_GATED_SL_ENABLED", args.structure_sl == "on"), \
         patch.object(bb, "_MAX_ZONE_REENTRIES_BY_UNDERLYING", reentry_patch), \
         patch.object(bb, "_MAX_RISK_PCT_BY_UNDERLYING", risk_pct_patch), \
         patch.object(bb, "_TSL_TRANCHE_STEP_PCT_BY_UNDERLYING", tsl_step_patch), \
         patch.object(bb, "_TSL_TRANCHE_STEP_LOCK_PCT_BY_UNDERLYING", tsl_lock_patch), \
         patch("asyncio.create_task", tracker.create_task):

        for day in trading_days:
            day_opens = spot_df[spot_df["datetime"].dt.date == day]
            if day_opens.empty:
                continue
            expiry = REGISTRY.get_active_expiry_strict(underlying, day)
            if expiry is None:
                print(f"  [{args.tag}] {day}: SKIP -- true front-month contract for this "
                      f"historical date has already expired and is no longer resolvable.")
                continue
            spot_open = float(day_opens.iloc[0]["open"])
            atm = round(spot_open / ATM_ROUND_STEP) * ATM_ROUND_STEP
            ce_strike, pe_strike = int(atm - args.itm_offset), int(atm + args.itm_offset)

            ce_data = await load_option(underlying, ce_strike, "CE", expiry, fetch_start, today, token)
            pe_data = await load_option(underlying, pe_strike, "PE", expiry, fetch_start, today, token)
            if ce_data is None or pe_data is None:
                print(f"  [{args.tag}] {day}: SKIP -- missing real premium data for one or both strikes.")
                continue

            ce_today_1m = ce_data["m1"][ce_data["m1"]["datetime"].dt.date == day]
            pe_today_1m = pe_data["m1"][pe_data["m1"]["datetime"].dt.date == day]
            from strategies.d1_trap_option.bear_only_book import _Bar
            ce_bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                       for r in ce_today_1m.to_dict("records")]
            pe_bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
                       for r in pe_today_1m.to_dict("records")]

            await run_t1t2_day(book, clock, tracker, day, ce_bars, pe_bars, spot_open)
            print(f"  [{args.tag}] {day}: done, open_positions={len(book._positions)}")

    result = _metrics(fake_bus.events)
    result["config"] = vars(args)
    out_path = RESULTS_DIR / f"{underlying.lower()}_{args.tag}.json"
    out_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(f"\n[{args.tag}] n={result['n']} win%={result['win_pct']} net=Rs{result['net']:+,.0f} "
          f"PF={'inf' if result['pf_inf'] else result['pf']}  -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
