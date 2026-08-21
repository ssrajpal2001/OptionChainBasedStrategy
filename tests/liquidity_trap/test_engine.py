"""
tests/liquidity_trap/test_engine.py — engine-level tests for strategies/
liquidity_trap/engine.py (LiquidityTrapStrategy). Constructed via __new__ +
manual attribute assignment (same pattern as tests/liquidity_sweep/
test_engine.py) -- exercises the book's own entry/exit/scale-in logic
directly and deterministically rather than driving the real async bus/loops
end-to-end.

Multi-ref (2026-08-21): self._setups replaces the old single self._bias/
_ref_idx/_lock_idx/_sl_hit_ts/_confirm_ts/_sweep_extreme fields -- any
number of _LiveSetup objects tracked in parallel. _try_enter() now takes
(entry_ts, entry_price, bias, sweep_extreme) as explicit arguments instead
of reading them off self.
"""
import asyncio
from collections import deque
from datetime import date, datetime, time as dtime, timedelta

from config.global_config import IST, Topic
from strategies.liquidity_trap.detector import Bar, BarAccumulator
from strategies.liquidity_trap.engine import LiquidityTrapStrategy, _LiveSetup
from strategies.liquidity_trap.events import LiquidityTrapOrderEvent, LiquidityTrapFillEvent

BASE = datetime(2026, 8, 21, 9, 15, tzinfo=IST)


class _CapturingBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


def _make_book(**overrides) -> LiquidityTrapStrategy:
    book = LiquidityTrapStrategy.__new__(LiquidityTrapStrategy)
    book._bus = _CapturingBus()
    book._cfg = None
    book._underlying = "SENSEX"
    book._client_id = "ssrajpal2001"
    book._binding_id = "SA5770"
    book._running = True
    book._tasks = []
    book._loop_queues = {}

    book._strategy_name = "liquidity_trap"
    book._lot_multiplier = 1
    book._lots_initial = 2
    book._rr = 2.0
    book._itm_offset_pts = 0.0
    book._scale_in_enabled = True
    book._hard_risk_rs_per_lot = 2000.0
    book._product_type = "MIS"
    book._squareoff_time = dtime(15, 15)
    book._lot_size = 20
    book._strike_step = 100.0
    book._persist_key = "test_liqtrap"
    book._clog = type("L", (), {"info": lambda *a, **k: None, "critical": lambda *a, **k: None,
                                  "warning": lambda *a, **k: None})()

    book._today = BASE.date()
    book._ref_tf_min = 20
    book._confirm_tf_min = 3
    book._trend_tf_min = 60
    book._trend_sma_len = 10
    book._trend_filter_enabled = False   # off by default -- most tests aren't about the trend filter
    book._acc_ref = BarAccumulator(timeframe_min=book._ref_tf_min)
    book._acc_confirm = BarAccumulator(timeframe_min=book._confirm_tf_min)
    book._acc_trend = BarAccumulator(timeframe_min=book._trend_tf_min)
    book._acc_1m = BarAccumulator(timeframe_min=1)
    book._live_ltp = {}

    book._setups = []
    book._day_done = False
    book._ref_watch_count = 0
    book._warming_up = False
    book._feeder_token = ""

    book._position = None
    book._cooldown_until = None
    book._event_counter = 0
    book._fill_waiters = {}
    book._fill_results = {}
    book._recent_remarks = deque(maxlen=30)

    for k, v in overrides.items():
        setattr(book, k, v)
    return book


# ── _try_enter ───────────────────────────────────────────────────────────────

def test_try_enter_publishes_order_and_sets_position(monkeypatch):
    import strategies.liquidity_trap.engine as eng_mod

    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 21, 10, 0, tzinfo=IST)   # well before squareoff (15:15)

    monkeypatch.setattr(eng_mod, "datetime", _FakeDT)

    async def run():
        book = _make_book()
        book._live_ltp[(79600.0, "CE")] = 150.0
        book._resolve_expiry = lambda: date(2026, 8, 26)

        book._try_enter(BASE + timedelta(minutes=60), entry_price=79613.0,
                        bias="BULL", sweep_extreme=79500.0)
        await asyncio.sleep(0.01)

        assert book._position is not None
        assert book._position["side"] == "CE"
        assert book._position["lots"] == 2
        assert book._position["sl_spot"] == 79500.0
        assert book._position["target_spot"] == 79613.0 + 2 * (79613.0 - 79500.0)
        assert book._day_done is False   # multi-ref: NOT set on a successful entry anymore
        assert len(book._bus.published) == 1
        topic, ev = book._bus.published[0]
        assert topic == Topic.LIQUIDITY_TRAP_ORDER_REQUEST
        assert isinstance(ev, LiquidityTrapOrderEvent)
        assert ev.action == "BUY"
        assert ev.quantity == 2 * 20 * 1
        assert ev.is_add_on is False
    asyncio.run(run())


def test_try_enter_skips_when_no_live_option_ltp():
    async def run():
        book = _make_book()
        # no live_ltp seeded for the resolved PE strike
        book._try_enter(BASE + timedelta(minutes=60), entry_price=79700.0,
                        bias="BEAR", sweep_extreme=79800.0)
        await asyncio.sleep(0.01)
        assert book._position is None
        assert book._bus.published == []
    asyncio.run(run())


def test_try_enter_noop_once_day_done():
    async def run():
        book = _make_book()
        book._day_done = True
        book._live_ltp[(79600.0, "CE")] = 150.0
        book._try_enter(BASE + timedelta(minutes=60), entry_price=79613.0,
                        bias="BULL", sweep_extreme=79500.0)
        await asyncio.sleep(0.01)
        assert book._position is None
        assert book._bus.published == []
    asyncio.run(run())


def test_try_enter_sets_day_done_past_squareoff_time(monkeypatch):
    import strategies.liquidity_trap.engine as eng_mod

    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 21, 15, 20, tzinfo=IST)   # past 15:15 squareoff

    monkeypatch.setattr(eng_mod, "datetime", _FakeDT)

    async def run():
        book = _make_book()
        book._live_ltp[(79600.0, "CE")] = 150.0
        book._try_enter(BASE, entry_price=79613.0, bias="BULL", sweep_extreme=79500.0)
        await asyncio.sleep(0.01)
        assert book._position is None
        assert book._day_done is True
        assert book._bus.published == []
    asyncio.run(run())


# ── _check_exit_and_scale_in / _on_fill (unaffected by the multi-ref rewrite,
#    these only ever operate on self._position, never self._setups) ─────────

def test_check_exit_fires_sl_for_bull():
    async def run():
        book = _make_book()
        book._position = dict(
            side="CE", direction=1, strike=79600.0, entry_price=150.0,
            entry_spot=79613.0, sl_spot=79500.0, target_spot=79839.0,
            lots=2, qty_unit=20, add_on_done=True,   # add_on_done=True so scale-in isn't attempted
            zone_bars_since_entry=[], entry_ts=BASE, expiry=date(2026, 8, 26),
            _entry_event_id="X",
        )
        book._live_ltp[(79600.0, "CE")] = 130.0
        closed = []
        async def _fake_square_off(pos, reason, exit_price):
            closed.append((reason, exit_price))
            book._position = None
        book._square_off = _fake_square_off

        book._check_exit_and_scale_in(spot_ltp=79480.0)   # below SL
        await asyncio.sleep(0.01)

        assert closed == [("sl_hit", 130.0)]
    asyncio.run(run())


def test_check_exit_fires_target_for_bear():
    async def run():
        book = _make_book()
        book._position = dict(
            side="PE", direction=-1, strike=79700.0, entry_price=140.0,
            entry_spot=79700.0, sl_spot=79800.0, target_spot=79500.0,
            lots=2, qty_unit=20, add_on_done=True,
            zone_bars_since_entry=[], entry_ts=BASE, expiry=date(2026, 8, 26),
            _entry_event_id="X",
        )
        book._live_ltp[(79700.0, "PE")] = 200.0
        closed = []
        async def _fake_square_off(pos, reason, exit_price):
            closed.append((reason, exit_price))
            book._position = None
        book._square_off = _fake_square_off

        book._check_exit_and_scale_in(spot_ltp=79490.0)   # below (<=) target for a BEAR trade
        await asyncio.sleep(0.01)

        assert closed == [("target_hit", 200.0)]
    asyncio.run(run())


def test_scale_in_triggers_add_on_order():
    async def run():
        book = _make_book()
        entry_ts = BASE + timedelta(minutes=10)
        book._position = dict(
            side="CE", direction=1, strike=79600.0, entry_price=150.0,
            entry_spot=79613.0, sl_spot=79500.0, target_spot=79839.0,
            lots=2, qty_unit=20, add_on_done=False,
            zone_bars_since_entry=[], entry_ts=entry_ts, expiry=date(2026, 8, 26),
            _entry_event_id="X",
        )
        book._live_ltp[(79600.0, "CE")] = 160.0
        # Seed a bear-trap 3-candle zone directly on the 1m accumulator's closed bars.
        book._acc_1m.bars = [
            Bar(ts=entry_ts + timedelta(minutes=1), open=79600, high=79601, low=79590, close=79595),
            Bar(ts=entry_ts + timedelta(minutes=2), open=79595, high=79596, low=79580, close=79582),
            Bar(ts=entry_ts + timedelta(minutes=3), open=79582, high=79603, low=79581, close=79600),
        ]
        # zone: entry_line=ref.low=79590, sweep_low=79580 -> lo=79580 hi=79590 size=10
        # add_on_level = 79580 + 10/3 = 79583.33
        book._check_exit_and_scale_in(spot_ltp=79583.0)   # inside the lowest third
        await asyncio.sleep(0.01)

        assert book._position["add_on_done"] is True
        assert book._position["lots"] == 4
        assert len(book._bus.published) == 1
        topic, ev = book._bus.published[0]
        assert topic == Topic.LIQUIDITY_TRAP_ORDER_REQUEST
        assert ev.is_add_on is True
        assert ev.quantity == 2 * 20 * 1
    asyncio.run(run())


def test_on_fill_entry_aborted_discards_position():
    book = _make_book()
    book._position = dict(
        side="CE", direction=1, strike=79600.0, entry_price=150.0,
        entry_spot=79613.0, sl_spot=79500.0, target_spot=79839.0,
        lots=2, qty_unit=20, add_on_done=False,
        zone_bars_since_entry=[], entry_ts=BASE, expiry=date(2026, 8, 26),
        _entry_event_id="EID1",
    )
    book._persist_position = lambda: None
    fill = LiquidityTrapFillEvent(
        action="BUY", underlying="SENSEX", option_type="CE", strike=79600,
        fill_price=0.0, qty=40, client_id="ssrajpal2001", binding_id="SA5770",
        event_id="EID1", entry_aborted=True,
    )
    book._on_fill(fill)
    assert book._position is None


def test_on_fill_add_on_failure_reverts_lots_without_discarding_position():
    book = _make_book()
    book._position = dict(
        side="CE", direction=1, strike=79600.0, entry_price=150.0,
        entry_spot=79613.0, sl_spot=79500.0, target_spot=79839.0,
        lots=4, qty_unit=20, add_on_done=True,
        zone_bars_since_entry=[], entry_ts=BASE, expiry=date(2026, 8, 26),
        _entry_event_id="EID1",
    )
    book._persist_position = lambda: None
    fill = LiquidityTrapFillEvent(
        action="BUY", underlying="SENSEX", option_type="CE", strike=79600,
        fill_price=0.0, qty=40, client_id="ssrajpal2001", binding_id="SA5770",
        event_id="EID2", is_add_on=True, entry_aborted=True,
    )
    book._on_fill(fill)
    assert book._position is not None
    assert book._position["lots"] == 2   # reverted to lots_initial, original entry untouched


# ── Multi-ref Stage 1: _on_bar_close finds + tracks setups ──────────────────

def test_on_bar_close_finds_new_setup_and_tracks_it():
    book = _make_book()
    book._acc_ref.bars = [
        Bar(ts=BASE, open=100, high=105, low=95, close=102),
        Bar(ts=BASE + timedelta(minutes=20), open=102, high=110, low=101, close=108),
    ]
    book._on_bar_close()
    assert len(book._setups) == 1
    s = book._setups[0]
    assert (s.ref_idx, s.direction, s.locked_idx) == (0, "BULL", 1)
    assert s.sl_hit_ts is None
    assert not s.dead


def test_on_bar_close_does_not_duplicate_already_tracked_setups():
    book = _make_book()
    existing = _LiveSetup(ref_idx=0, direction="BULL", locked_idx=1)
    book._setups = [existing]
    book._acc_ref.bars = [
        Bar(ts=BASE, open=100, high=105, low=95, close=102),
        Bar(ts=BASE + timedelta(minutes=20), open=102, high=110, low=101, close=108),
    ]
    book._on_bar_close()
    assert len(book._setups) == 1
    assert book._setups[0] is existing


def test_on_bar_close_advances_sl_hit_for_pending_setup():
    book = _make_book()
    s = _LiveSetup(ref_idx=0, direction="BULL", locked_idx=1)
    book._setups = [s]
    book._acc_ref.bars = [
        Bar(ts=BASE, open=100, high=105, low=95, close=102),                       # ref: L=95
        Bar(ts=BASE + timedelta(minutes=20), open=102, high=110, low=101, close=108),
        Bar(ts=BASE + timedelta(minutes=40), open=108, high=109, low=90, close=92),  # breaches ref's low
    ]
    book._on_bar_close()
    assert s.sl_hit_ts == BASE + timedelta(minutes=40)


# ── Trend filter (real-data-validated optimization) ─────────────────────────

def test_stage4_choch_filtered_out_against_trend():
    book = _make_book(trend_filter_enabled=True)
    s = _LiveSetup(ref_idx=0, direction="BEAR", locked_idx=1)
    s.sl_hit_ts = BASE
    s.confirm_ts = BASE
    s.sweep_extreme = 79800.0
    book._setups = [s]
    book._acc_ref.bars = [Bar(ts=BASE, open=100, high=105, low=95, close=102)]
    # DOWN-trending 60m history (closes falling) -> should block a BEAR... wait,
    # DOWN trend actually WANTS BEAR. Use an UP trend to filter out this BEAR setup.
    book._acc_trend.bars = [
        Bar(ts=BASE + timedelta(hours=i), open=100 + i, high=100 + i, low=100 + i, close=100 + i)
        for i in range(10)
    ]   # closes 100..109 rising -> latest(109) > sma(104.5) -> UP -> wants BULL, not BEAR
    book._acc_1m.bars = [
        # Verified BEAR CHoCH sequence: bar2 (L=95) confirms as a swing LOW
        # once bar4 exists (both before/after neighbors have higher lows),
        # bar5's close (90) breaks below it -> CHoCH fires BEAR @90.
        Bar(ts=BASE + timedelta(minutes=0), open=100, high=101, low=99, close=100),
        Bar(ts=BASE + timedelta(minutes=1), open=100, high=102, low=100, close=101),
        Bar(ts=BASE + timedelta(minutes=2), open=101, high=103, low=95, close=100),
        Bar(ts=BASE + timedelta(minutes=3), open=100, high=101, low=97, close=99),
        Bar(ts=BASE + timedelta(minutes=4), open=99, high=100, low=98, close=98.5),
        Bar(ts=BASE + timedelta(minutes=5), open=98.5, high=99, low=90, close=90),
    ]
    book._on_bar_close()
    assert book._position is None
    assert s.dead is True   # CHoCH moment consumed, but filtered out -- not entered


def test_stage4_choch_enters_when_trend_agrees(monkeypatch):
    import strategies.liquidity_trap.engine as eng_mod

    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 21, 10, 0, tzinfo=IST)   # well before squareoff (15:15)

    monkeypatch.setattr(eng_mod, "datetime", _FakeDT)

    async def run():
        book = _make_book(trend_filter_enabled=True)
        book._live_ltp[(100.0, "CE")] = 50.0
        book._resolve_expiry = lambda: date(2026, 8, 26)
        s = _LiveSetup(ref_idx=0, direction="BULL", locked_idx=1)
        s.sl_hit_ts = BASE
        s.confirm_ts = BASE
        s.sweep_extreme = 90.0
        book._setups = [s]
        book._acc_ref.bars = [Bar(ts=BASE, open=100, high=105, low=95, close=102)]
        book._acc_trend.bars = [
            Bar(ts=BASE + timedelta(hours=i), open=100 + i, high=100 + i, low=100 + i, close=100 + i)
            for i in range(10)
        ]   # UP trend -> agrees with this BULL setup
        book._acc_1m.bars = [
            Bar(ts=BASE + timedelta(minutes=0), open=100, high=101, low=99, close=100),
            Bar(ts=BASE + timedelta(minutes=1), open=100, high=102, low=100, close=101),
            Bar(ts=BASE + timedelta(minutes=2), open=101, high=105, low=101, close=104),
            Bar(ts=BASE + timedelta(minutes=3), open=104, high=104.5, low=102, close=103),
            Bar(ts=BASE + timedelta(minutes=4), open=103, high=103.5, low=101, close=102),
            Bar(ts=BASE + timedelta(minutes=5), open=102, high=106, low=101, close=106),  # breaks confirmed swing high
        ]
        book._on_bar_close()
        await asyncio.sleep(0.01)
        assert book._position is not None
        assert book._position["side"] == "CE"
        assert s.dead is True
    asyncio.run(run())


def test_stage4_choch_skipped_without_enough_trend_history():
    book = _make_book(trend_filter_enabled=True)
    s = _LiveSetup(ref_idx=0, direction="BULL", locked_idx=1)
    s.sl_hit_ts = BASE
    s.confirm_ts = BASE
    s.sweep_extreme = 90.0
    book._setups = [s]
    book._acc_ref.bars = [Bar(ts=BASE, open=100, high=105, low=95, close=102)]
    book._acc_trend.bars = []   # no trend history at all -- never guess
    book._acc_1m.bars = [
        Bar(ts=BASE + timedelta(minutes=0), open=100, high=101, low=99, close=100),
        Bar(ts=BASE + timedelta(minutes=1), open=100, high=102, low=100, close=101),
        Bar(ts=BASE + timedelta(minutes=2), open=101, high=105, low=101, close=104),
        Bar(ts=BASE + timedelta(minutes=3), open=104, high=104.5, low=102, close=103),
        Bar(ts=BASE + timedelta(minutes=4), open=103, high=103.5, low=101, close=102),
        Bar(ts=BASE + timedelta(minutes=5), open=102, high=106, low=101, close=106),
    ]
    book._on_bar_close()
    assert book._position is None
    assert s.dead is True


# ── Only one position at a time: same-direction ignored, opposite skipped ──

def test_stage4_choch_same_direction_as_open_position_is_ignored():
    book = _make_book()
    book._position = dict(side="CE", direction=1, strike=100.0, entry_price=50.0,
                          entry_spot=100.0, sl_spot=90.0, target_spot=120.0,
                          lots=2, qty_unit=20, add_on_done=True, zone_bars_since_entry=[],
                          entry_ts=BASE, expiry=date(2026, 8, 26), _entry_event_id="X")
    s = _LiveSetup(ref_idx=0, direction="BULL", locked_idx=1)   # same direction as open position
    s.sl_hit_ts = BASE
    s.confirm_ts = BASE
    s.sweep_extreme = 90.0
    book._setups = [s]
    book._acc_ref.bars = [Bar(ts=BASE, open=100, high=105, low=95, close=102)]
    book._acc_1m.bars = [
        Bar(ts=BASE + timedelta(minutes=0), open=100, high=101, low=99, close=100),
        Bar(ts=BASE + timedelta(minutes=1), open=100, high=102, low=100, close=101),
        Bar(ts=BASE + timedelta(minutes=2), open=101, high=105, low=101, close=104),
        Bar(ts=BASE + timedelta(minutes=3), open=104, high=104.5, low=102, close=103),
        Bar(ts=BASE + timedelta(minutes=4), open=103, high=103.5, low=101, close=102),
        Bar(ts=BASE + timedelta(minutes=5), open=102, high=106, low=101, close=106),
    ]
    book._on_bar_close()
    assert s.dead is True
    assert book._bus.published == []   # no second order -- still just the one open position


def test_stage4_choch_opposite_of_open_position_is_skipped_not_flipped():
    book = _make_book()
    book._position = dict(side="CE", direction=1, strike=100.0, entry_price=50.0,
                          entry_spot=100.0, sl_spot=90.0, target_spot=120.0,
                          lots=2, qty_unit=20, add_on_done=True, zone_bars_since_entry=[],
                          entry_ts=BASE, expiry=date(2026, 8, 26), _entry_event_id="X")
    s = _LiveSetup(ref_idx=0, direction="BEAR", locked_idx=1)   # opposite of the open BULL position
    s.sl_hit_ts = BASE
    s.confirm_ts = BASE
    s.sweep_extreme = 110.0
    book._setups = [s]
    book._acc_ref.bars = [Bar(ts=BASE, open=100, high=105, low=95, close=102)]
    book._acc_1m.bars = [
        # Same verified BEAR CHoCH sequence as test_stage4_choch_filtered_out_against_trend.
        Bar(ts=BASE + timedelta(minutes=0), open=100, high=101, low=99, close=100),
        Bar(ts=BASE + timedelta(minutes=1), open=100, high=102, low=100, close=101),
        Bar(ts=BASE + timedelta(minutes=2), open=101, high=103, low=95, close=100),
        Bar(ts=BASE + timedelta(minutes=3), open=100, high=101, low=97, close=99),
        Bar(ts=BASE + timedelta(minutes=4), open=99, high=100, low=98, close=98.5),
        Bar(ts=BASE + timedelta(minutes=5), open=98.5, high=99, low=90, close=90),
    ]
    book._on_bar_close()
    assert s.dead is True
    assert book._position["side"] == "CE"   # still the original position, never flipped
    assert book._bus.published == []        # no exit/flip order fired


# ── Mid-day intraday warmup (2026-08-21) ────────────────────────────────────

def test_on_bar_close_warming_up_marks_setup_dead_not_entire_day():
    # Reuses the exact CHoCH bar sequence from test_detector.py's own
    # test_choch_entry_fires_on_close_above_confirmed_swing_high -- Stage1-3
    # pre-seeded as already resolved (mirrors what a real warmup replay would
    # have caught up to), only Stage4 (CHoCH) evaluated on this call.
    book = _make_book()
    book._warming_up = True
    s = _LiveSetup(ref_idx=0, direction="BULL", locked_idx=1)
    s.sl_hit_ts = BASE
    s.confirm_ts = BASE
    other = _LiveSetup(ref_idx=0, direction="BEAR", locked_idx=0)   # a second, still-pending setup
    book._setups = [s, other]
    book._acc_ref.bars = [Bar(ts=BASE, open=100, high=105, low=95, close=102)]
    book._acc_1m.bars = [
        Bar(ts=BASE + timedelta(minutes=0), open=100, high=101, low=99, close=100),
        Bar(ts=BASE + timedelta(minutes=1), open=100, high=102, low=100, close=101),
        Bar(ts=BASE + timedelta(minutes=2), open=101, high=105, low=101, close=104),
        Bar(ts=BASE + timedelta(minutes=3), open=104, high=104.5, low=102, close=103),
        Bar(ts=BASE + timedelta(minutes=4), open=103, high=103.5, low=101, close=102),
        Bar(ts=BASE + timedelta(minutes=5), open=102, high=106, low=101, close=106),
    ]
    book._on_bar_close()
    assert s.dead is True                  # this setup's stale CHoCH consumed, not entered
    assert other.dead is False             # a DIFFERENT still-pending setup is unaffected
    assert book._day_done is False         # multi-ref: no longer a global day-kill
    assert book._position is None
    assert book._bus.published == []       # no stale-price order ever fired


def test_warmup_intraday_replays_history_and_locks_setup_without_entering(monkeypatch):
    import strategies.liquidity_trap.engine as eng_mod
    import data_layer.historical_candles as hc_mod

    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 21, 10, 0, tzinfo=IST)

    async def _fake_fetch(key, token):
        return [
            {"ts": "2026-08-21T09:15:00+05:30", "open": 100, "high": 105, "low": 95, "close": 102},
            {"ts": "2026-08-21T09:35:00+05:30", "open": 102, "high": 108, "low": 101, "close": 106},
            # a 3rd bar is required only so the 2nd ref-tf bucket's own first
            # tick closes it (a bucket closes on the NEXT bucket's first tick,
            # same as live) -- its own OHLC values are irrelevant to this test.
            {"ts": "2026-08-21T09:55:00+05:30", "open": 106, "high": 107, "low": 105, "close": 106},
        ]

    monkeypatch.setattr(eng_mod, "datetime", _FakeDT)
    monkeypatch.setattr(hc_mod, "fetch_upstox_intraday_1m", _fake_fetch)

    async def run():
        book = _make_book()
        book._feeder_token = "FAKE_TOKEN"
        await book._warmup_intraday()
        assert len(book._setups) == 1
        s = book._setups[0]
        assert s.direction == "BULL"          # candle2 breached candle1's high only -> locked
        assert not s.dead
        assert book._warming_up is False       # reset after replay finishes
        assert book._bus.published == []       # state catch-up only, never a live order
        # Regression: _today must be set to today's real date by warmup itself --
        # _index_tick_loop's own new-day check ("if self._today != today:
        # reset_session()") runs on the very first live tick right after this
        # returns; if _today were still None, that check would silently wipe
        # out everything just replayed the instant live ticks resume.
        assert book._today == date(2026, 8, 21)
    asyncio.run(run())


def test_warmup_intraday_skips_gracefully_without_feeder_token(monkeypatch):
    import strategies.liquidity_trap.engine as eng_mod

    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 21, 10, 0, tzinfo=IST)

    monkeypatch.setattr(eng_mod, "datetime", _FakeDT)

    async def run():
        book = _make_book()
        book._feeder_token = ""
        await book._warmup_intraday()          # must not raise
        assert book._setups == []
        assert book._warming_up is False
    asyncio.run(run())


# ── Trend history seed (2026-08-21) ──────────────────────────────────────────

def test_seed_trend_history_populates_acc_trend(monkeypatch):
    import strategies.liquidity_trap.engine as eng_mod
    import data_layer.historical_candles as hc_mod

    async def _fake_range_fetch(key, token, start, end):
        # 3 real days' worth of 1m bars, enough to build several 60m closed bars
        rows = []
        base_day = datetime(2026, 8, 18, 9, 15, tzinfo=IST)
        for day_offset in range(3):
            for hour_offset in range(6):
                ts = base_day + timedelta(days=day_offset, hours=hour_offset)
                rows.append({"ts": ts.isoformat(), "open": 100, "high": 101, "low": 99, "close": 100})
        return rows

    monkeypatch.setattr(hc_mod, "fetch_upstox_range_1m", _fake_range_fetch)

    async def run():
        book = _make_book()
        book._feeder_token = "FAKE_TOKEN"
        assert book._acc_trend.bars == []
        await book._seed_trend_history()
        assert len(book._acc_trend.bars) > 0
    asyncio.run(run())


def test_seed_trend_history_noop_if_already_seeded():
    async def run():
        book = _make_book()
        book._feeder_token = "FAKE_TOKEN"
        sentinel = [Bar(ts=BASE, open=1, high=1, low=1, close=1)]
        book._acc_trend.bars = sentinel
        await book._seed_trend_history()
        assert book._acc_trend.bars is sentinel   # untouched -- never re-seeds mid-process
    asyncio.run(run())


def test_seed_trend_history_noop_without_feeder_token():
    async def run():
        book = _make_book()
        book._feeder_token = ""
        await book._seed_trend_history()          # must not raise
        assert book._acc_trend.bars == []
    asyncio.run(run())
