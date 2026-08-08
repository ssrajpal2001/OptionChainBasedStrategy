"""
scripts/d1trap_banknifty_month_backtest.py — real-data BANKNIFTY backtest,
2026-07-01 through today, against a SINGLE fixed contract throughout: the
August monthly expiry (per direct user request) -- avoids mid-window
expiry-rollover contamination, same discipline as every other multi-week
real-data backtest this session (see e.g. FVG's next-week-expiry test).
Resolved live via REGISTRY.get_active_expiry("BANKNIFTY") -- whatever
monthly contract is currently active is printed at the top of the run so
you can visually confirm it's the one you expect before reading results.

2026-08-08 REWRITE (T1/T2 mechanic): the first version of this script
reimplemented bear_only_book.py's zone/entry/exit logic as standalone
functions (mirroring scripts/d1trap_month_backtest_v2.py). That real run
surfaced two problems: (1) it was missing the 2026-08-07 live fix for a
duplicate-entry bug (~Rs59,733 of a reported -Rs43,286 net loss was pure
duplicate-churn), and (2) it turned out to replicate an OLDER
pre-2026-08-01 mechanic (single-shot raw_breakout/swing_breach), not the
CURRENT live T1/T2 tranche architecture (T1 fires on ref-candle breach,
optional T2 retrace add-on, tranche-specific staircase TSL, spot HTF bias
filter, zone re-arm, flip concept) -- a hand-maintained second copy of
this mechanic is exactly the kind of thing that goes stale as the real
one keeps evolving.

Fix: this version DRIVES THE REAL D1TrapBearOnlyBook CLASS directly
(strategies/d1_trap_option/bear_only_book.py) against real historical
data, instead of reimplementing its logic a second time. Concretely:
  - A minimal FakeBus stands in for the EventBus: records every
    D1TrapOrderEvent, and for a SELL (exit) synchronously answers with a
    fake D1TrapFillEvent so the class's own confirm-then-finalize
    _square_off_leg() resolves immediately (no real broker round trip).
  - bear_only_book.datetime is monkeypatched to a fixed-clock subclass
    whose now() returns the CURRENT simulated bar's own timestamp, updated
    as each day/bar is replayed -- everywhere the class calls
    datetime.now(IST) (entry_ts, trigger_ts, zone-age cutoffs, expiry
    resolution, EOD checks) sees the correct simulated instant.
  - fetch_upstox_range_1m/fetch_upstox_intraday_1m are monkeypatched to
    serve real pre-fetched data (see load_option() below) instead of
    hitting the network -- the class believes it's doing its own live
    REST warmup, but every row is real historical data already fetched
    once by this script. The "today" intraday-replay fetch is patched to
    return empty (that path exists live to catch a restart up to the
    current moment WITHOUT re-firing orders that already happened earlier
    today -- _enter_leg no-ops during that replay -- which is the OPPOSITE
    of what a backtest needs). Today's real bars are instead fed directly
    via the SAME method calls the live tick loop uses
    (_check_exit/_check_fast_flip_tranche1/_process_new_bar), so real
    entries actually fire.
  - data_layer.position_store._DIR is redirected to a scratch temp dir so
    this never touches the real data/ position-store files.

This is now BYTE-IDENTICAL to whatever the live class currently does --
including anything not documented here, since it's the real code running,
not a description of it. Verified via a targeted offline scenario before
handing off (two zone objects sharing one ref candle -> exactly one T1
entry, confirming both the dedup fix and the harness wiring are intact).

SUPPORT & RESISTANCE MECHANIC (unchanged from the first version) — the
2026-08-07 "ping-pong" S&R tracker (strategies/d1_trap_option/support_
resistance.py), reusing _run_sr_variant + all 4 exit-mode variants
(raw/bucket_close/buffered/profit_trigger, all risk-capped) directly from
scripts/d1trap_sr_zone_backtest.py -- run once per day per side per
(tf, exit_mode), fed the zones the live T1/T2 class actually built and
used that day (book._series[side].zones -- the real thing, not a
separately-recomputed copy). Results log into the SAME
data/d1trap_sr_exit_variant_log.jsonl used by NIFTY/SENSEX daily runs.

Per-day ATM CE/PE strike selection uses the SAME "3-ITM-step" pattern
CLAUDE.md documents for NIFTY (150pts=3x50) and SENSEX (300pts=3x100),
extrapolated to BANKNIFTY's own 100pt grid -> 300pts -- an assumption by
extrapolation, not a validated BANKNIFTY-specific value.

Run on the box with a real Upstox access_token (data/clients.db) --
expect this to take a while: BANKNIFTY can range widely over 5+ weeks, so
this may need to fetch a real month+ of 1-minute data for 15-30+ distinct
strikes:
    python3 scripts/d1trap_banknifty_month_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from datetime import date, datetime, timedelta
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
from scripts.d1trap_sr_zone_backtest import (  # noqa: E402
    _run_sr_variant,
    _EXIT_MODES,
    _append_variant_log,
    _print_variant_track_record,
)

UNDERLYING = "BANKNIFTY"
START_DATE = date(2026, 7, 1)
LOT_SIZE = 30
ITM_OFFSET_PTS = 300   # extrapolated 3-ITM-step on BANKNIFTY's 100pt grid -- see docstring
ATM_ROUND_STEP = 100
HIST_WARMUP_DAYS = bb._HIST_WARMUP_DAYS
SR_TF_SWEEP = (1, 3, 5)

_opt_cache: Dict[tuple, Optional[dict]] = {}
_key_to_df: Dict[str, pd.DataFrame] = {}   # instrument_key -> full-range 1m DataFrame,
                                            # used by the patched fetch functions below.


def _bars_to_df(bars: list) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close"])
    return pd.DataFrame([
        {"datetime": b.timestamp, "open": b.open, "high": b.high, "low": b.low, "close": b.close}
        for b in bars
    ])


async def load_option(strike: int, side: str, expiry: date, fetch_start: date, fetch_end: date,
                       token: str) -> Optional[dict]:
    """Fetch (or return cached) full-range 1m data for one (strike, side) against
    the fixed expiry, and register it into _key_to_df so the harness's patched
    fetch functions can serve it back to the live class as if it were real-time."""
    key = (strike, side)
    if key in _opt_cache:
        return _opt_cache[key]
    opt_key = REGISTRY.get_upstox_key(UNDERLYING, expiry, strike, side)
    if not opt_key:
        print(f"    [skip] {strike}{side}: no Upstox instrument key for expiry {expiry}.")
        _opt_cache[key] = None
        return None
    bars = await asyncio.to_thread(_fetch_1m_bars, opt_key, fetch_start, fetch_end, token)
    if not bars:
        print(f"    [skip] {strike}{side}: no real premium data returned.")
        _opt_cache[key] = None
        return None
    df = _bars_to_df(bars)
    data = dict(m1=df, upstox_key=opt_key)
    _opt_cache[key] = data
    _key_to_df[opt_key] = df
    print(f"    [fetched] {strike}{side}: {len(df)} real 1m bars ({fetch_start} .. {fetch_end}).")
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
    # Deliberately empty -- see module docstring: the live class's own "replay
    # today's already-elapsed bars" path exists to catch a live restart up to
    # now WITHOUT re-firing orders (_enter_leg no-ops during _warming_up). A
    # backtest wants the opposite -- today's bars fed via the real per-bar
    # method calls below, so real entries actually fire.
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
    """bear_only_book.py fires _square_off_leg/publish via bare
    asyncio.create_task(...) (fire-and-forget -- correct for live, where the
    event loop keeps running independently). A harness driving bars in a tight
    synchronous loop has no such background loop, so a bare `await
    asyncio.sleep(0)` is NOT enough to guarantee a created task's full await
    chain (publish -> _on_fill -> waiter.set() -> resume wait_for) actually
    completes before the next bar is processed -- confirmed live in this
    harness: real syn thetic-data run hung for 75s+ waiting on
    _square_off_leg's 15s fill-confirm timeout, which should have resolved
    instantly since the fake fill was already available. This wraps the REAL
    asyncio.create_task (patched globally for the harness's run -- safe, this
    is a standalone script, not a shared process) so every task it schedules
    can be explicitly awaited/drained after each bar."""

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
    """Records every published D1TrapOrderEvent; synchronously answers a SELL
    (exit) with a fake fill so _square_off_leg's confirm-then-finalize wait
    resolves immediately -- no real broker round trip needed."""

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


async def run_t1t2_day(book, clock: _FrozenClock, tracker: _TaskTracker, day: date,
                        ce_bars: List, pe_bars: List, spot_open: float) -> None:
    """Drives ONE simulated day through the real D1TrapBearOnlyBook, using the
    exact method-call sequence the live tick loop uses (_check_exit ->
    _check_fast_flip_tranche1 -> _process_new_bar), fed real historical
    1-minute bars directly (bypassing on_tick's raw-tick bucketing, which is
    unnecessary here -- these ARE already-closed real 1-minute bars, same
    shortcut the live class's own intraday-replay path takes)."""
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
        await tracker.drain()   # let every create_task-scheduled _square_off_leg/publish
                                 # actually run to completion before the next bar sees
                                 # book._positions -- a bare sleep(0) is NOT enough, see
                                 # _TaskTracker's docstring.

    # Drain any exits still in flight (EOD square-offs fired on the last bars).
    for _ in range(50):
        await tracker.drain()
        if not book._positions:
            break
        await asyncio.sleep(0.01)
    if book._positions:
        print(f"    WARNING: {len(book._positions)} position(s) still open after {day} "
              f"-- MIS EOD square-off did not complete as expected.")


def run_sr_day(day: date, zones: List[dict], side_label: str, day_1m: "pd.DataFrame", lot_size: int) -> None:
    from strategies.d1_trap_option.bear_only_book import _Bar
    bars = [_Bar(timestamp=r["datetime"], open=r["open"], high=r["high"], low=r["low"], close=r["close"])
            for r in day_1m.to_dict("records")]
    if not bars:
        return
    log_records = []
    for tf in SR_TF_SWEEP:
        for mode in _EXIT_MODES:
            result = _run_sr_variant(zones, bars, tf, lot_size, exit_mode=mode)
            log_records.append({
                "date": day.isoformat(), "underlying": UNDERLYING, "side": side_label,
                "tf_minutes": tf, "exit_mode": mode,
                "pnl": result.get("pnl"), "entry_ts": str(result.get("entry_ts", "")),
                "exit_reason": result.get("exit_reason"), "no_entry": bool(result.get("no_entry")),
            })
    _append_variant_log(log_records)


def _print_t1t2_trades(events: list) -> None:
    trades = [ev for topic, ev in events if getattr(ev, "action", None) == "SELL"]
    trades.sort(key=lambda ev: ev.entry_ts or ev.trigger_ts)

    def fmt(ts):
        return ts.strftime("%m-%d %H:%M") if ts else "-"

    print("\n" + "=" * 140)
    print("T1/T2 MECHANIC (real live class) -- FULL TRADE LIST (chronological)")
    print("=" * 140)
    print(f"{'Date':<8}{'Strike':>7}{'Side':>5}{'EntryReason':>22}{'Entry':>9}{'EntryTS':>13}"
          f"{'Exit':>9}{'ExitTS':>13}{'Reason':>18}{'PnL':>10}")
    print("-" * 140)
    total = 0.0
    for ev in trades:
        pnl = (ev.exit_price - ev.entry_price) * ev.quantity
        total += pnl
        print(f"{fmt(ev.entry_ts).split(' ')[0]:<8}{ev.strike:>7}{ev.option_type:>5}"
              f"{ev.entry_reason:>22}{ev.entry_price:>9.1f}{fmt(ev.entry_ts):>13}"
              f"{ev.exit_price:>9.1f}{fmt(ev.trigger_ts):>13}{ev.reason:>18}{pnl:>+10.0f}")
    wins = [ (ev.exit_price - ev.entry_price) * ev.quantity for ev in trades
             if (ev.exit_price - ev.entry_price) * ev.quantity > 0]
    losses = [(ev.exit_price - ev.entry_price) * ev.quantity for ev in trades
              if (ev.exit_price - ev.entry_price) * ev.quantity <= 0]
    gw, gl = sum(wins), abs(sum(losses))
    pf = gw / gl if gl > 0 else (99 if gw > 0 else 0)
    win_pct = 100 * len(wins) / len(trades) if trades else 0
    print(f"\nn={len(trades)}  win%={win_pct:.1f}  Rs{total:+,.0f}  PF={pf:.2f}")
    flips = [ev for ev in trades if "flip" in ev.entry_reason]
    print(f"flip-origin trades: {len(flips)}  PnL from flips: "
          f"Rs{sum((ev.exit_price - ev.entry_price) * ev.quantity for ev in flips):+,.0f}")


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
    print(f"Resolved expiry: {expiry}  (confirm this is the August monthly you expect)")

    spot_key = _upstox_key_for(UNDERLYING)
    fetch_start = START_DATE - timedelta(days=HIST_WARMUP_DAYS)
    print(f"Fetching real BANKNIFTY spot 1m bars {fetch_start} .. {today} ...")
    spot_bars = await asyncio.to_thread(_fetch_1m_bars, spot_key, fetch_start, today, token)
    if not spot_bars:
        print("FATAL: no real spot data returned.")
        return 1
    spot_df = _bars_to_df(spot_bars)
    _key_to_df[spot_key] = spot_df
    trading_days = sorted(d for d in spot_df["datetime"].dt.date.unique() if START_DATE <= d <= today)
    print(f"{len(trading_days)} real trading day(s) in range: {trading_days[0]} .. {trading_days[-1]}")

    cfg = GlobalConfig()
    fake_bus = _FakeBus()
    scratch_dir = tempfile.mkdtemp(prefix="d1trap_banknifty_backtest_")
    clock = _FrozenClock()
    fixed_datetime = _make_fixed_datetime(clock)
    tracker = _TaskTracker()

    book = bb.D1TrapBearOnlyBook(
        bus=fake_bus, cfg=cfg, underlying=UNDERLYING, client_id="BACKTEST",
        binding_id="BANKNIFTY_MONTH", lot_multiplier=1, feeder_token=token,
    )
    fake_bus.book = book

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
            ce_strike, pe_strike = int(atm - ITM_OFFSET_PTS), int(atm + ITM_OFFSET_PTS)
            print(f"\n{day}: real spot open={spot_open:.2f} ATM={atm} -> CE{ce_strike} / PE{pe_strike}")

            ce_data = await load_option(ce_strike, "CE", expiry, fetch_start, today, token)
            pe_data = await load_option(pe_strike, "PE", expiry, fetch_start, today, token)
            if ce_data is None or pe_data is None:
                print("  SKIP this day -- missing real premium data for one or both strikes.")
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
            print(f"  zones today: CE={len(ce_zones)} PE={len(pe_zones)}")
            run_sr_day(day, ce_zones, f"CE{ce_strike}", ce_today_1m, LOT_SIZE)
            run_sr_day(day, pe_zones, f"PE{pe_strike}", pe_today_1m, LOT_SIZE)

    _print_t1t2_trades(fake_bus.events)

    print("\n" + "=" * 70)
    print("S&R MECHANIC -- see aggregate track record below "
          "(now includes BANKNIFTY alongside any prior NIFTY/SENSEX days)")
    _print_variant_track_record()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
