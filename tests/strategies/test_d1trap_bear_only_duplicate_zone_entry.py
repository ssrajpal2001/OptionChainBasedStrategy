"""
tests/strategies/test_d1trap_bear_only_duplicate_zone_entry.py — two zone
objects tracking the same real reference candle must never fire two real
entries, even when the pre-existing same-call `active_zone_locks` guard
doesn't cover the case.

2026-08-07 real incident: NIFTY, 10:48:00 IST -- two back-to-back log lines:
  "BearTrap[NIFTY]: CE ref-candle breach @ 2026-08-07 10:47:00+05:30 (high=290.40)"
appeared 1ms apart, each immediately followed by a real "ENTER BUY CE 24350 [T1]"
with byte-identical entry/SL. Both filled, both later hit the hard-cap SL,
booking 2x the intended loss on a single signal.

Investigation: strategies/d1_trap_option/bear_only_book.py's _process_new_bar
maintains a POOL of zone objects (series.zones), deduped at the 60m
zone-detection layer by ref_ts. Confirmed via direct tracing that if TWO
distinct zone objects (different ref_ts/lock_ts, so not deduped there) both
reach Stage 2 and get assigned the SAME 15m ref candle via
_find_latest_closed_ref_bar (which has no notion of "which zone is asking"),
the EXISTING `active_zone_locks` check DOES prevent both from firing within a
single _process_new_bar call (confirmed: it correctly blocks a second zone
once self._positions is non-empty). The real incident must therefore involve
two SEPARATE _process_new_bar calls where self._positions was still empty at
the moment each one's checks ran (most plausible: a mid-day restart's
_warmup_intraday replay -- which yields control via `await asyncio.sleep(0)`
between every replayed bar specifically to let other coroutines interleave --
racing against live ticks arriving on _option_tick_loop concurrently). That
exact interleaving is out of scope to reproduce deterministically in a unit
test, but the important thing is the guard added below does NOT depend on
self._positions/active_zone_locks at all, so it holds regardless of how two
separate calls come to see empty self._positions.

Fix: _process_new_bar now tracks (side, ref_open) combinations that have
already fired an entry this session (self._fired_ref_breach_refs) and refuses
to act on the same real reference candle twice, independent of position
state or which zone object is asking.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

from config.global_config import IST
from data_layer.base_feeder import EventBus
from strategies.d1_trap_option.bear_only_book import D1TrapBearOnlyBook, _Bar, _OptionSeries


def _make_book() -> D1TrapBearOnlyBook:
    return D1TrapBearOnlyBook(
        bus=EventBus(), cfg=None, underlying="NIFTY",
        client_id="C", binding_id="B",
    )


class _RecordingBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


def _flat_1m_bars(start: datetime, n: int, price: float) -> list:
    """n minutes of flat OHLC bars -- guaranteed to never satisfy
    find_all_bear_zones' sweep+reclaim pattern (all lows/highs equal), so
    _detect_bear_zones contributes nothing extra to series.zones and the
    manually-seeded test zone stays the only one in the pool."""
    return [
        _Bar(start + timedelta(minutes=i), price, price, price, price)
        for i in range(n)
    ]


def _seed_zone(ref_open: datetime, ref_close_time: datetime, ref_high: float,
               ref_low: float, lock_ts: datetime) -> dict:
    """A zone already past Stage 1/2 -- state=MONITORING, ref candle already
    assigned -- reproducing the exact real state right before the breach
    tick. zone_lo/zone_hi kept consistent with ref_high/ref_low so the
    "ongoing invalidation" check never trips against the test's warm-up
    price band."""
    return dict(
        zone_lo=ref_low - 5, zone_hi=ref_high - 5, entry_line=ref_low, lock_ts=lock_ts,
        ref_ts=ref_open - timedelta(minutes=1), ref_idx=0,
        state="MONITORING", ref_bar=None, done=False, invalid=False,
        contact_ts=ref_open, ref_open=ref_open, ref_close_time=ref_close_time,
        breach_ts=None, sub_lo=None, sub_hi=None,
        ref_high=ref_high, ref_low=ref_low,
    )


def _bars_for_breach(ref_close_time: datetime, ref_high: float, zone_lo: float) -> list:
    """30 contiguous flat warm-up bars (len(df_1m) >= 30 gate) ending exactly
    where the breach bar begins, then one breach bar whose high exceeds
    ref_high at ref_close_time -- the real trigger condition. Priced between
    zone_lo and ref_high so the warm-up bars never trip the "ongoing
    invalidation" check (latest15.close < zone_lo)."""
    warmup_start = ref_close_time - timedelta(minutes=30)
    price = zone_lo + 10
    bars = _flat_1m_bars(warmup_start, 30, price)
    bars.append(_Bar(ref_close_time, price, ref_high + 1.0, price, ref_high + 1.0))
    return bars


@pytest.mark.asyncio
async def test_two_zones_sharing_same_ref_candle_across_separate_calls_fire_once():
    """The scenario the pre-existing active_zone_locks guard does NOT cover:
    two separate _process_new_bar calls, self._positions manually reset to
    empty between them (simulating whatever real-world race let this happen
    live -- see module docstring). The new (side, ref_open) guard must still
    hold even though the older position-based guard can't see across this
    gap."""
    book = _make_book()
    book._bus = _RecordingBus()
    book._ce_strike = 24350
    book._pe_strike = 24700
    book._series["CE"] = _OptionSeries(strike=24350, side="CE")
    book._series["PE"] = _OptionSeries(strike=24700, side="PE")

    ref_open = datetime(2026, 8, 7, 10, 47, tzinfo=IST)
    ref_close_time = ref_open + timedelta(minutes=15)
    ref_high, ref_low = 290.40, 259.63
    zone_lo = ref_low - 5

    bars = _bars_for_breach(ref_close_time, ref_high, zone_lo)
    book._series["CE"].bars_1m = bars

    # Call 1: zone A (its own distinct zone object) breaches and fires for real.
    zone_a = _seed_zone(ref_open, ref_close_time, ref_high, ref_low,
                         lock_ts=datetime(2026, 8, 7, 10, 40, tzinfo=IST))
    book._series["CE"].zones = [zone_a]
    book._process_new_bar("CE")
    await asyncio.sleep(0)

    assert len(book._bus.published) == 1
    assert zone_a["breach_ts"] is not None

    # Simulate the race: self._positions is empty again when the SECOND,
    # distinct zone object (representing whatever upstream mechanism produced
    # a duplicate tracking the same real reference candle) gets its own
    # breach check -- the exact condition under which active_zone_locks
    # alone would NOT have blocked a second real order.
    book._positions = []
    zone_b = _seed_zone(ref_open, ref_close_time, ref_high, ref_low,
                         lock_ts=datetime(2026, 8, 7, 10, 41, tzinfo=IST))
    book._series["CE"].zones = [zone_b]
    book._process_new_bar("CE")
    await asyncio.sleep(0)

    assert len(book._bus.published) == 1, (
        "a second zone object sharing the same (side, ref_open) must NOT fire "
        "a second real order, even with self._positions empty at the time"
    )
    assert zone_b["breach_ts"] is not None   # zone_b still marks its own bookkeeping;
                                              # only the real order dispatch is suppressed


@pytest.mark.asyncio
async def test_different_ref_candle_is_a_genuinely_new_signal_and_fires():
    """Sanity check: the guard must key on the SPECIFIC reference candle, not
    just "side" -- a real, later, genuinely different breach on the same side
    must still fire normally."""
    book = _make_book()
    book._bus = _RecordingBus()
    book._ce_strike = 24350
    book._pe_strike = 24700
    book._series["CE"] = _OptionSeries(strike=24350, side="CE")
    book._series["PE"] = _OptionSeries(strike=24700, side="PE")

    ref_open_1 = datetime(2026, 8, 7, 10, 47, tzinfo=IST)
    ref_close_1 = ref_open_1 + timedelta(minutes=15)
    ref_high_1, ref_low_1 = 290.40, 259.63
    bars = _bars_for_breach(ref_close_1, ref_high_1, ref_low_1 - 5)
    book._series["CE"].bars_1m = bars
    zone_1 = _seed_zone(ref_open_1, ref_close_1, ref_high_1, ref_low_1,
                         lock_ts=datetime(2026, 8, 7, 10, 40, tzinfo=IST))
    book._series["CE"].zones = [zone_1]
    book._process_new_bar("CE")
    await asyncio.sleep(0)
    assert len(book._bus.published) == 1

    book._positions = []
    ref_open_2 = datetime(2026, 8, 7, 13, 0, tzinfo=IST)   # a genuinely different, later candle
    ref_close_2 = ref_open_2 + timedelta(minutes=15)
    ref_high_2, ref_low_2 = 310.00, 275.00
    more_bars = _bars_for_breach(ref_close_2, ref_high_2, ref_low_2 - 5)
    book._series["CE"].bars_1m = book._series["CE"].bars_1m + more_bars
    zone_2 = _seed_zone(ref_open_2, ref_close_2, ref_high_2, ref_low_2,
                         lock_ts=datetime(2026, 8, 7, 12, 55, tzinfo=IST))
    book._series["CE"].zones = [zone_2]
    book._process_new_bar("CE")
    await asyncio.sleep(0)

    assert len(book._bus.published) == 2, "a genuinely different reference candle must still fire"


@pytest.mark.asyncio
async def test_reset_session_clears_fired_ref_breach_tracking():
    book = _make_book()
    book._fired_ref_breach_refs.add(("CE", datetime(2026, 8, 7, 10, 47, tzinfo=IST)))
    book.reset_session()
    assert book._fired_ref_breach_refs == set()
