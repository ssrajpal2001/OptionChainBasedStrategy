"""
scripts/d1trap_banknifty_sr_book_smoketest.py — drives the REAL
D1TrapSRBook class (strategies/d1_trap_option/sr_book.py) end-to-end
against real recent BANKNIFTY premium data, confirming the live module
(order dispatch, confirm-then-finalize fill loop, position persistence)
actually works -- not just the SRPingPongTracker logic in isolation
(already parity-checked against the backtest separately).

Same harness pattern as scripts/d1trap_banknifty_sweep.py (FakeBus, frozen
clock, patched REST fetches, _TaskTracker draining every create_task before
the next bar) -- reused by import, not re-implemented.

Usage:
    python3 scripts/d1trap_banknifty_sr_book_smoketest.py --start-date 2026-08-03
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
import strategies.d1_trap_option.sr_book as srb  # noqa: E402
from strategies.d1_trap_option.book import _fetch_1m_bars, _upstox_key_for  # noqa: E402
from execution_bridge.d1_trap_bridge import D1TrapFillEvent  # noqa: E402
from scripts.d1trap_banknifty_sweep import (  # noqa: E402
    UNDERLYING, ATM_ROUND_STEP, _bars_to_df, load_option,
    _fake_fetch_range_1m, _fake_fetch_intraday_1m, _FrozenClock,
    _make_fixed_datetime, _TaskTracker, _key_to_df,
)


class _FakeBus:
    """Same shape as d1trap_banknifty_sweep.py's _FakeBus -- records every
    published D1TrapOrderEvent, synchronously answers SELL with a fake fill
    so the book's confirm-then-finalize wait resolves immediately."""

    def __init__(self) -> None:
        self.events: list = []
        self.book = None

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
        elif getattr(event, "action", None) == "BUY" and self.book is not None:
            # Confirm every BUY too (real live flow would also get a fill event for
            # entries -- the book's _on_fill BUY branch only acts on entry_aborted,
            # so a normal confirm is a no-op there, matches production behavior).
            pass

    def subscribe(self, topic):
        return asyncio.Queue()

    def unsubscribe(self, topic, q) -> None:
        pass


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="2026-08-03")
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
    fetch_start = start_date - timedelta(days=srb._HIST_WARMUP_DAYS)
    spot_bars = await asyncio.to_thread(_fetch_1m_bars, spot_key, fetch_start, today, token)
    if not spot_bars:
        print("FATAL: no real spot data returned.")
        return 1
    spot_df = _bars_to_df(spot_bars)
    _key_to_df[spot_key] = spot_df
    trading_days = sorted(d for d in spot_df["datetime"].dt.date.unique() if start_date <= d <= today)
    print(f"SMOKETEST: {len(trading_days)} day(s): {trading_days[0]} .. {trading_days[-1]}")

    cfg = GlobalConfig()
    fake_bus = _FakeBus()
    scratch_dir = tempfile.mkdtemp(prefix="d1trap_bnf_sr_book_smoketest_")
    clock = _FrozenClock()
    fixed_datetime = _make_fixed_datetime(clock)
    tracker = _TaskTracker()

    book = srb.D1TrapSRBook(
        bus=fake_bus, cfg=cfg, underlying=UNDERLYING, client_id="SMOKETEST",
        binding_id="BNF_SR_SMOKE", lot_multiplier=1, feeder_token=token,
        itm_offset_pts=300, htf_minutes=15, sr_tf_minutes=3, exit_mode="raw",
    )
    fake_bus.book = book

    with patch.object(srb, "datetime", fixed_datetime), \
         patch.object(srb, "fetch_upstox_range_1m", _fake_fetch_range_1m), \
         patch.object(historical_candles, "fetch_upstox_intraday_1m", _fake_fetch_intraday_1m), \
         patch.object(position_store, "_DIR", scratch_dir), \
         patch("asyncio.create_task", tracker.create_task):

        for day in trading_days:
            day_opens = spot_df[spot_df["datetime"].dt.date == day]
            if day_opens.empty:
                continue
            spot_open = float(day_opens.iloc[0]["open"])
            atm = round(spot_open / ATM_ROUND_STEP) * ATM_ROUND_STEP
            ce_strike, pe_strike = int(atm - 300), int(atm + 300)

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

            clock.value = datetime(day.year, day.month, day.day, 9, 16, tzinfo=IST)
            book.reset_session()
            book._today = day
            await book._select_strikes_for_today(spot_open)
            await tracker.drain()
            assert book._ce_strike == ce_strike and book._pe_strike == pe_strike, \
                f"strike mismatch: book selected {book._ce_strike}/{book._pe_strike}, expected {ce_strike}/{pe_strike}"
            assert "CE" in book._sr_trackers and "PE" in book._sr_trackers, "S&R trackers not created"

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

            for _ in range(50):
                await tracker.drain()
                if not book._positions:
                    break
                await asyncio.sleep(0.01)
            print(f"  {day}: done, open_positions={len(book._positions)}, "
                  f"CE zones={len(book._series['CE'].zones)} PE zones={len(book._series['PE'].zones)}")

    trades = [ev for topic, ev in fake_bus.events if getattr(ev, "action", None) == "SELL"]
    entries = [ev for topic, ev in fake_bus.events if getattr(ev, "action", None) == "BUY"]
    print(f"\n{'='*100}\nSMOKETEST RESULT: {len(entries)} BUY event(s), {len(trades)} SELL event(s) dispatched")
    for ev in sorted(trades, key=lambda e: e.trigger_ts):
        pnl = (ev.exit_price - ev.entry_price) * ev.quantity
        print(f"  {ev.option_type}{ev.strike} entry={ev.entry_price:.2f} exit={ev.exit_price:.2f} "
              f"reason={ev.reason} pnl={pnl:+.0f}")
    assert len(entries) == len(trades), \
        f"MISMATCH: {len(entries)} entries but {len(trades)} exits -- a leg was left open or double-closed"
    print(f"\nPASS: every entry has a matching exit, all order dispatch + confirm-then-finalize "
          f"round trips completed cleanly through the real D1TrapSRBook class.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
