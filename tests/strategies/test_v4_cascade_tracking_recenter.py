"""V4CascadeBook._maybe_recenter_tracking_strikes -- 2026-07-21: re-centers
the tracking/scanner strikes when the underlying has drifted far enough from
the ATM the CURRENT tracking strikes were derived from, but ONLY while flat
(no open position) -- carrying an open position's zone/SL/target state
across a strike change has no valid conversion between two different
instruments' unrelated price scales, so re-centering never happens mid-trade.

2026-07-22: upgraded to `async def` -- a re-center now also re-warms the new
CE/PE tracking symbols' scanner state from real historical+intraday data
(the same fetch-and-replay sequence _ingest_history already performs at
boot), instead of leaving a re-centered book scanning cold from an empty
reset."""
from datetime import date, datetime
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook, _Bar
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


class _FakeFeeder:
    def __init__(self) -> None:
        self.subscribed = []
        self.unsubscribed = []

    async def subscribe_tokens(self, tokens) -> None:
        self.subscribed.extend(tokens)

    async def unsubscribe_tokens(self, tokens) -> None:
        self.unsubscribed.extend(tokens)


class _FakeRebalancer:
    def __init__(self, feeder) -> None:
        self._feeder = feeder


def _book(underlying="NIFTY"):
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying=underlying, client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    book._tracking_reference_atm = 24216.05
    book._ce_strike = 24000
    book._pe_strike = 24400
    return book


@pytest.mark.asyncio
async def test_recenters_when_flat_and_drift_exceeds_threshold_nifty():
    book = _book("NIFTY")
    book._engine.position = None
    assert book._v4cfg.tracking_recenter_pts == 100.0

    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)   # drift = 103.95 >= 100

    assert book._tracking_reference_atm == 24320.0
    assert book._ce_strike != 24000 or book._pe_strike != 24400


@pytest.mark.asyncio
async def test_does_not_recenter_when_drift_under_threshold():
    book = _book("NIFTY")
    book._engine.position = None

    await book._maybe_recenter_tracking_strikes(current_atm=24250.0)   # drift = 33.95 < 100

    assert book._tracking_reference_atm == 24216.05
    assert book._ce_strike == 24000
    assert book._pe_strike == 24400


@pytest.mark.asyncio
async def test_does_not_recenter_while_position_open_regardless_of_drift():
    book = _book("NIFTY")
    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65, entry_price=20.0, status="open")
    book._engine.position = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, open_time=datetime(2026, 7, 21, 12, 0, tzinfo=IST),
    )

    await book._maybe_recenter_tracking_strikes(current_atm=25000.0)   # huge drift, but position open

    assert book._tracking_reference_atm == 24216.05   # unchanged
    assert book._ce_strike == 24000
    assert book._pe_strike == 24400


@pytest.mark.asyncio
async def test_crudeoil_uses_200_point_threshold():
    book = _book("CRUDEOIL")
    assert book._v4cfg.tracking_recenter_pts == 200.0


@pytest.mark.asyncio
async def test_recenter_rewarms_scanners_from_real_history_not_bare_reset():
    """The regression this guards against: a re-center that resets scanners
    without re-warming them, leaving the new strikes cold for hours until
    enough live bars accumulate a fresh pattern from scratch.

    Timestamps are 5 minutes apart (not consecutive minutes) so each row
    lands in its OWN 5m bar after _to_5m_bars resamples -- find_all_bear_
    traps_2candle needs >=3 DISTINCT 5m bars to detect a zone (ref candle,
    a sweep candle whose low < ref.low, and a later reclaim candle whose
    high > ref.high); three 1-minute rows inside a single 5-minute bucket
    would collapse to just one bar and could never produce a zone."""
    book = _book("NIFTY")
    book._engine.position = None

    range_rows = [
        {"ts": "2026-07-21T09:15:00", "open": 100, "high": 110, "low": 100, "close": 105, "volume": 10},
        {"ts": "2026-07-21T09:20:00", "open": 98, "high": 105, "low": 95, "close": 100, "volume": 10},
        {"ts": "2026-07-21T09:25:00", "open": 110, "high": 115, "low": 105, "close": 112, "volume": 10},
    ]
    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=range_rows)), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)

    # A real zone from the fetched history must now be present -- not an
    # empty reset.
    assert len(book._engine._scanners["CE"].setups) >= 1


@pytest.mark.asyncio
async def test_recenter_leaves_position_and_spot_confirm_untouched():
    book = _book("NIFTY")
    book._engine.position = None
    original_spot_confirm = book._engine._spot_confirm

    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)

    assert book._engine._spot_confirm is original_spot_confirm   # untouched, same object
    assert book._engine.position is None


# ── 2026-07-22 review fixes: TOCTOU race + reentrancy guard ─────────────────

@pytest.mark.asyncio
async def test_position_opened_during_await_aborts_recenter_no_corruption():
    """Critical fix: the flatness gate is checked ONCE at function entry,
    before two real awaits (the access-token fetch, then the 4-way REST
    gather). Because _maybe_recenter_tracking_strikes is dispatched
    fire-and-forget from _close_5m_bucket, a genuine Gate-3 trigger on a
    SEPARATE _close_5m_bucket call can open a live position while this
    coroutine is suspended at either await -- simulated here via a side
    effect on the token fetch (standing in for "a concurrent call opened a
    position mid-await"). The atomic re-check immediately before the
    scanner-reset/replay mutations must catch this and abort, leaving the
    newly-opened position's T1/T2 status completely untouched (no phantom
    replay-driven close, no tracking-strike change)."""
    book = _book("NIFTY")
    book._engine.position = None

    t1 = TrancheLeg(tranche="T1", option_type="CE", strike=24200.0, qty=65,
                     entry_price=20.0, status="open")
    t2 = TrancheLeg(tranche="T2", option_type="CE", strike=24200.0, qty=65,
                     entry_price=20.0, status="open")
    opened_pos = CascadePosition(
        underlying="NIFTY", side="CE", tracking_strike=24000.0, execution_strike=24200.0,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, status="open",
        open_time=datetime(2026, 7, 21, 12, 0, tzinfo=IST),
    )

    def _open_position_during_await():
        # Stands in for a concurrent _close_5m_bucket call's Gate-3 trigger
        # opening a real position while this coroutine is suspended at the
        # asyncio.to_thread(self._access_token) await.
        book._engine.position = opened_pos
        return "tok"

    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", side_effect=_open_position_during_await), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)   # drift = 103.95 >= 100

    # Recenter must have aborted -- tracking strikes/reference ATM untouched.
    assert book._ce_strike == 24000
    assert book._pe_strike == 24400
    assert book._tracking_reference_atm == 24216.05

    # The genuinely-opened position must be completely uncorrupted -- no
    # phantom close from replaying foreign historical bars through it.
    assert book._engine.position is opened_pos
    assert book._engine.position.status == "open"
    assert book._engine.position.t1.status == "open"
    assert book._engine.position.t2.status == "open"


@pytest.mark.asyncio
async def test_recentering_guard_blocks_overlapping_calls():
    """Important fix: self._recentering is an in-flight guard so two
    overlapping recenter triggers (e.g. CE's and PE's 5m bucket closes
    landing close together) can't both run the fetch+reset+replay+subscribe
    sequence concurrently. A call that finds the guard already set must
    return immediately without touching the network or any book state."""
    book = _book("NIFTY")
    book._engine.position = None
    book._recentering = True   # simulate an already-in-flight recenter

    fetch_mock = AsyncMock(return_value=[])
    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=fetch_mock), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)

    fetch_mock.assert_not_called()
    assert book._ce_strike == 24000
    assert book._pe_strike == 24400
    assert book._tracking_reference_atm == 24216.05
    assert book._recentering is True   # untouched -- this call never entered the body


@pytest.mark.asyncio
async def test_recentering_flag_cleared_after_normal_completion():
    book = _book("NIFTY")
    book._engine.position = None
    assert book._recentering is False

    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)

    assert book._recentering is False


@pytest.mark.asyncio
async def test_recentering_flag_cleared_after_exception_mid_fetch():
    book = _book("NIFTY")
    book._engine.position = None

    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m",
               new=AsyncMock(side_effect=RuntimeError("boom"))), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        with pytest.raises(RuntimeError):
            await book._maybe_recenter_tracking_strikes(current_atm=24320.0)

    assert book._recentering is False   # cleared by the finally block even on failure


# ── 2026-07-22 final-review fix: stale _buckets[side] on the tracking side ──

@pytest.mark.asyncio
async def test_recenter_clears_stale_in_progress_tracking_bucket():
    """The bug this guards against: _maybe_recenter_tracking_strikes rebuilds
    self._bars_5m[side] from freshly-fetched history but, before this fix,
    never touched self._buckets[side] -- the in-progress LIVE bar for the
    OLD strike that was still accumulating right up until the re-center
    fired. Because this coroutine is fire-and-forget with two real awaits
    (token fetch, REST gather), by the time it flips the strikes/symbols the
    REST round-trip has almost certainly crossed a 5-minute boundary, so the
    very next tick on the (now new) strike would hit _on_option_tick's
    `cur.timestamp != bucket` branch and flush that stale OLD-strike bar
    (built from a totally different instrument's price scale) into the
    just-rebuilt self._bars_5m[side] and through the freshly re-warmed
    scanner -- corrupting the new strike's first Gate-2 zone.

    Steps: (a) seed an in-progress OLD-strike bucket bar with prices wildly
    foreign to the fetched NEW-strike history, (b) trigger a re-center,
    (c) confirm self._buckets["CE"] is cleared to None, and (d) feed a tick
    for the new strike and confirm it starts a genuinely fresh bucket at the
    tick's own price -- not a merge with the stale bar's OHLC."""
    book = _book("NIFTY")
    book._engine.position = None
    book._ce_symbol = "NSE_FO|old_ce"
    book._pe_symbol = "NSE_FO|old_pe"

    # (a) In-progress OLD-strike bucket bar mid-formation, at a price level
    # (~9000) wildly foreign to the fetched NEW-strike history (~100-115)
    # below -- if this leaks into the new strike's bars it would obviously
    # dominate/skew the high/low range fed to the scanner.
    stale_old_bar = _Bar(datetime(2026, 7, 21, 10, 5, tzinfo=IST), 9000, 9050, 8950, 9010, tf=5)
    book._buckets["CE"] = stale_old_bar
    book._buckets["PE"] = _Bar(datetime(2026, 7, 21, 10, 5, tzinfo=IST), 50, 55, 48, 52, tf=5)

    range_rows = [
        {"ts": "2026-07-21T09:15:00", "open": 100, "high": 110, "low": 100, "close": 105, "volume": 10},
        {"ts": "2026-07-21T09:20:00", "open": 98, "high": 105, "low": 95, "close": 100, "volume": 10},
        {"ts": "2026-07-21T09:25:00", "open": 110, "high": 115, "low": 105, "close": 112, "volume": 10},
    ]
    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=range_rows)), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new_ce"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)   # drift = 103.95 >= 100

    # (c) Both sides' in-progress buckets must be cleared, not carried over.
    assert book._buckets["CE"] is None
    assert book._buckets["PE"] is None

    # The rebuilt bars_5m must be the fresh history, not contaminated by the
    # stale bar's foreign OHLC (which was never appended anywhere).
    assert all(b.high < 200 for b in book._bars_5m["CE"])
    assert all(b is not stale_old_bar for b in book._bars_5m["CE"])

    # (d) A tick for the new strike immediately after must start a genuinely
    # fresh bucket at the tick's own price -- not a merge with the stale
    # bar's OHLC (which would show high=9050/low=8950 if it had leaked in).
    tick_ts = datetime(2026, 7, 21, 10, 6, tzinfo=IST)
    book._on_option_tick("CE", 120.0, tick_ts)

    fresh_bucket = book._buckets["CE"]
    assert fresh_bucket is not None
    assert fresh_bucket is not stale_old_bar
    assert fresh_bucket.open == 120.0
    assert fresh_bucket.high == 120.0
    assert fresh_bucket.low == 120.0
    assert fresh_bucket.close == 120.0


@pytest.mark.asyncio
async def test_recenter_unsubscribes_old_symbols_and_subscribes_new():
    book = _book("NIFTY")
    book._engine.position = None
    book._ce_symbol = "NSE_FO|old_ce"
    book._pe_symbol = "NSE_FO|old_pe"
    feeder = _FakeFeeder()
    book._rebalancer = _FakeRebalancer(feeder)

    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
        await book._maybe_recenter_tracking_strikes(current_atm=24320.0)

    assert feeder.subscribed == ["NSE_FO|new", "NSE_FO|new"]
    assert feeder.unsubscribed == ["NSE_FO|old_ce", "NSE_FO|old_pe"]


@pytest.mark.asyncio
async def test_recenter_diffs_multi_strike_window_only_touching_changed_strikes():
    """When ATM drifts, the pool-engine recenter must diff the OLD 5-strike
    window against the NEW one -- only strikes that fell out of range get
    unsubscribed+dropped, and only strikes newly in range get subscribed+
    fetched+reset; strikes that stay in-window (24000/23900/23800 for CE,
    24400/24500/24600 for PE in this scenario) are left completely alone."""
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15", use_pool_engine=True,
        tracking_offsets_pts=[100.0, 200.0, 300.0, 400.0, 500.0],
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    book._tracking_reference_atm = 24100.0
    # OLD window: ATM=24100 -> CE [24000,23900,23800,23700,23600], PE [24200,24300,24400,24500,24600]
    book._ce_strikes = [24000, 23900, 23800, 23700, 23600]
    book._pe_strikes = [24200, 24300, 24400, 24500, 24600]
    book._ce_symbols = [f"NSE_FO|CE{s}" for s in book._ce_strikes]
    book._pe_symbols = [f"NSE_FO|PE{s}" for s in book._pe_strikes]
    feeder = _FakeFeeder()
    book._rebalancer = _FakeRebalancer(feeder)

    def _fake_upstox_key(underlying, expiry, strike, opt_type):
        return f"NSE_FO|{opt_type}{int(strike)}"

    with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=[])), \
         patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
         patch.object(book, "_access_token", return_value="tok"), \
         patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", side_effect=_fake_upstox_key):
        # NEW ATM=24300 (drift=200 >= tracking_recenter_pts=100) -> CE
        # [24200,24100,24000,23900,23800], PE [24400,24500,24600,24700,24800] --
        # partially overlaps the old window (3 CE + 3 PE strikes unchanged),
        # so this genuinely tests the diff, not a full-window replacement.
        await book._maybe_recenter_tracking_strikes(current_atm=24300.0)

    assert book._ce_strikes == [24200, 24100, 24000, 23900, 23800]
    assert book._pe_strikes == [24400, 24500, 24600, 24700, 24800]
    assert sorted(feeder.subscribed) == sorted([
        "NSE_FO|CE24200", "NSE_FO|CE24100", "NSE_FO|PE24700", "NSE_FO|PE24800",
    ])
    assert sorted(feeder.unsubscribed) == sorted([
        "NSE_FO|CE23700", "NSE_FO|CE23600", "NSE_FO|PE24200", "NSE_FO|PE24300",
    ])
