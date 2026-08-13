"""
2026-08-12: integration-style tests for strategies/oi_flow/engine.py
(OIFlowStrategy). Constructed via __new__ + manual attribute assignment
(same pattern used for D1TrapSRBook tests earlier this session) rather
than driving the real async bus/loops end-to-end -- this exercises the
book's own decision logic (signal -> confirmation -> order, exit checks,
fill handling) directly and deterministically, without needing a live
feed (which doesn't exist for this strategy anyway -- no historical OI
data to replay).
"""
import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pytest

from config.global_config import IST, Topic
from data_layer.base_feeder import IndexTick, OptionTick
import strategies.oi_flow.engine as engine_module
from strategies.oi_flow.detector import Bar, BarAccumulator
from strategies.oi_flow.engine import OIFlowStrategy
from strategies.oi_flow.events import OIFlowFillEvent, OIFlowOrderEvent
from strategies.oi_flow.tracker import OIFlowTracker


@pytest.fixture(autouse=True)
def _no_real_telemetry_writes(monkeypatch):
    """_try_enter() now logs a telemetry row (Phase 5) on every evaluation
    -- these tests call it many times and shouldn't touch real disk (same
    reasoning as _persist_position being stubbed to a no-op below)."""
    monkeypatch.setattr(engine_module, "log_signal_evaluation", lambda row: None)


class _CapturingBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


@dataclass
class _FakeTick:
    strike: float
    option_type: str
    oi: int
    timestamp: datetime
    ltp: float = 0.0
    underlying: str = "BANKNIFTY"


class _FakeSnap:
    def __init__(self, max_call_oi_strike=57700.0, max_put_oi_strike=57200.0, pcr=1.3, underlying="BANKNIFTY"):
        self.underlying = underlying
        self.max_call_oi_strike = max_call_oi_strike
        self.max_put_oi_strike = max_put_oi_strike
        self._pcr = pcr

    def pcr_smooth(self, n: int = 5) -> float:
        return self._pcr


def _base():
    return datetime(2026, 8, 12, 9, 20, tzinfo=IST)


def _make_book() -> OIFlowStrategy:
    book = OIFlowStrategy.__new__(OIFlowStrategy)
    book._bus = _CapturingBus()
    book._cfg = None
    book._underlying = "BANKNIFTY"
    book._client_id = "ssrajpal2001"
    book._binding_id = "SA5770"
    book._running = True
    book._tasks = []
    book._loop_queues = {}

    book._strategy_name = "oi_flow"
    book._lot_multiplier = 1
    book._window_sec = 180
    book._max_opposing_roc_pct = -0.01
    book._min_supporting_roc_pct = 0.02
    book._min_pcr_bias = 1.2
    book._max_pcr_bias = 0.7
    book._proximity_pct = 0.005
    book._hard_risk_rs_per_lot = 2000.0
    book._trail_trigger_pct = 0.15
    book._first_lock_pct = 0.08
    book._step_pct = 0.10
    book._step_lock_pct = 0.05
    book._sl_cooldown_minutes = 15.0
    book._cooldown_until = None
    book._product_type = "MIS"
    from datetime import time as _time
    book._squareoff_time = _time(15, 15)
    book._lot_size = 30
    book._strike_step = 100.0
    book._persist_key = "test_oi_flow_persist_key"

    book._today = None
    book._oi_tracker = OIFlowTracker(max_history_sec=600)
    book._spot_acc = BarAccumulator(1)
    book._option_acc = {"CE": BarAccumulator(1), "PE": BarAccumulator(1)}
    book._latest_snap = None
    book._live_option_ltp = {}
    book._tracked_option_strike = {"CE": None, "PE": None}
    book._watched_strikes = {}
    book._position = None
    book._last_position_tick_ts = None
    book._staleness_alerted = False
    book._day_done = False
    book._event_counter = 0
    book._fill_waiters = {}
    book._fill_results = {}
    book._recent_remarks = deque(maxlen=30)
    book._clog = logging.getLogger("test_oi_flow_clog")   # avoid real log-file I/O in tests
    book._persist_position = lambda: None   # avoid real disk I/O in tests
    return book


def _flat_spot_bars(base, n=10, price=57690.0):
    return [Bar(timestamp=base + timedelta(minutes=i), open=price, high=price, low=price, close=price)
            for i in range(n)]


def _seed_oi_for_signal(book: OIFlowStrategy, base) -> None:
    """Feeds real ticks through the real OIFlowTracker so the opposing/
    supporting ROC conditions genuinely pass -- mirrors
    tests/oi_flow/test_detector.py's own fixture logic."""
    book._latest_snap = _FakeSnap(max_call_oi_strike=57700.0, max_put_oi_strike=57200.0, pcr=1.3)
    book._rewatch_oi_strikes(book._latest_snap)
    t0 = base
    t1 = base + timedelta(seconds=book._window_sec + 10)
    book._oi_tracker.on_option_tick(_FakeTick(57700.0, "CE", 100_000, t0))
    book._oi_tracker.on_option_tick(_FakeTick(57700.0, "CE", 95_000, t1))
    book._oi_tracker.on_option_tick(_FakeTick(57600.0, "PE", 50_000, t0))
    book._oi_tracker.on_option_tick(_FakeTick(57600.0, "PE", 53_000, t1))


def _seed_option_bars_ok(book: OIFlowStrategy, base) -> None:
    """CE-side option bars that pass confirm_option_price_action at the
    ENGINE's real default swing_pivot=2 -- needs >=5 bars for find_swing_
    points' range(pivot, n-pivot) to be non-empty at all (a 4-bar fixture,
    as used in test_detector.py's own hand-tuned pivot=1 tests, silently
    confirms nothing here and returns ok=False/no_swing_sl_anchor_yet --
    caught by actually running this against the real book instead of
    assuming the smaller fixture would transfer)."""
    bars = [
        Bar(base, 520, 522, 518, 521),
        Bar(base + timedelta(minutes=1), 519, 521, 515, 518),
        Bar(base + timedelta(minutes=2), 506, 512, 502, 508),   # confirmed swing low @ 502 (pivot=2)
        Bar(base + timedelta(minutes=3), 514, 518, 511, 515),
        Bar(base + timedelta(minutes=4), 519, 524, 516, 522),
        Bar(base + timedelta(minutes=5), 523, 528, 520, 526),   # last bar: closes near its high
    ]
    book._option_acc["CE"].bars = bars
    book._live_option_ltp["CE"] = 526.0


# ── dedicated per-underlying log file (2026-08-13) ───────────────────────────

def test_make_strategy_logger_gives_a_distinct_file_per_underlying(tmp_path, monkeypatch):
    """Explicit ask: OI-Flow needs its own dedicated log file per underlying,
    same as SellStraddle's ss_{UND}_{client}_{binding}_{date}.log -- running
    NIFTY and SENSEX for the same client/binding must never mix into one
    file. Uses the real utils.logging_utils.make_strategy_logger (the same
    platform utility SellStraddle/V4Cascade already use) redirected to a
    temp dir so this doesn't touch real logs/clients/."""
    import utils.logging_utils as logging_utils_module
    _real = logging_utils_module.make_strategy_logger
    monkeypatch.setattr(
        logging_utils_module, "make_strategy_logger",
        lambda stem, **kw: _real(stem, log_dir=str(tmp_path), propagate=kw.get("propagate", False)),
    )
    from strategies.oi_flow.engine import _make_strategy_logger
    lg_nifty = _make_strategy_logger("NIFTY", "ssrajpal2001", "SA5770")
    lg_sensex = _make_strategy_logger("SENSEX", "ssrajpal2001", "SA5770")
    assert lg_nifty is not lg_sensex
    assert lg_nifty.name != lg_sensex.name
    assert "NIFTY" in lg_nifty.name and "SENSEX" not in lg_nifty.name
    assert "SENSEX" in lg_sensex.name and "NIFTY" not in lg_sensex.name
    import os
    files = os.listdir(tmp_path)
    assert any("NIFTY" in f for f in files)
    assert any("SENSEX" in f for f in files)


# ── _rewatch_oi_strikes ───────────────────────────────────────────────────────

def test_rewatch_oi_strikes_derives_correct_watch_list():
    book = _make_book()
    snap = _FakeSnap(max_call_oi_strike=57700.0, max_put_oi_strike=57200.0)
    book._rewatch_oi_strikes(snap)
    assert (57700.0, "CE") in book._watched_strikes    # call wall itself
    assert (57600.0, "PE") in book._watched_strikes    # one step below -- CE's supporting side
    assert (57200.0, "PE") in book._watched_strikes    # put wall itself
    assert (57300.0, "CE") in book._watched_strikes    # one step above -- PE's supporting side


# ── _on_spot_bar_close: single-position-at-a-time (CE XOR PE) ────────────────

def test_on_spot_bar_close_never_evaluates_either_side_while_a_position_is_open():
    """Explicit requirement: only one side's trade may be active at a time --
    if CE is running, a PE entry must never even be ATTEMPTED (not just
    blocked after evaluation). _on_spot_bar_close() is the ONLY caller of
    _try_enter()/_enter() in the whole engine, and its very first check is
    `self._position is not None` -- this drives that guard directly (not
    _try_enter() in isolation, which every other test uses) to prove the
    real-world entry point actually skips evaluation entirely."""
    book = _make_book()
    base = _base()
    # An open CE position already exists...
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0)
    # ...and PE's own gates would trivially pass if ever evaluated (proves
    # the guard, not a coincidental PE-side rejection, is what's blocking it).
    book._spot_acc.bars = _flat_spot_bars(base, price=57190.0)
    book._latest_snap = _FakeSnap(max_call_oi_strike=57700.0, max_put_oi_strike=57200.0, pcr=0.5)
    book._rewatch_oi_strikes(book._latest_snap)
    _try_enter_calls = []
    book._try_enter = lambda side: _try_enter_calls.append(side)

    book._on_spot_bar_close()

    assert _try_enter_calls == []   # never even attempted, PE or otherwise
    assert book._position["side"] == "CE"   # the original CE position is untouched


def test_on_spot_bar_close_evaluates_when_flat():
    """Sanity check for the test above: with no open position, evaluation
    DOES proceed (proves the guard is position-gated, not permanently off)."""
    book = _make_book()
    base = _base()
    book._position = None
    book._spot_acc.bars = _flat_spot_bars(base, price=57690.0)
    book._latest_snap = _FakeSnap(max_call_oi_strike=57700.0, max_put_oi_strike=57200.0, pcr=1.3)
    _try_enter_calls = []
    book._try_enter = lambda side: _try_enter_calls.append(side)

    book._on_spot_bar_close()

    assert "CE" in _try_enter_calls


def test_on_spot_bar_close_logs_wait_when_no_snapshot_yet():
    """2026-08-13 fix: before this, a book with no MATRIX_SNAPSHOT yet just
    silently returned every bar close -- the log file looked completely
    dead during startup. Now it writes a WAIT line every bar close until
    the first snapshot arrives, same visibility SellStraddle's own _clog
    already gives."""
    book = _make_book()
    base = _base()
    book._position = None
    book._latest_snap = None
    book._spot_acc.bars = _flat_spot_bars(base, price=57690.0)
    logged = []
    book._clog = type("FakeLog", (), {"info": staticmethod(lambda *a, **kw: logged.append(a))})()

    book._on_spot_bar_close()

    assert len(logged) == 1
    assert "WAIT" in logged[0][0]


# ── re-entry cooldown ─────────────────────────────────────────────────────────

def test_on_spot_bar_close_skips_evaluation_while_cooldown_active():
    book = _make_book()
    base = _base()
    book._position = None
    book._latest_snap = _FakeSnap(max_call_oi_strike=57700.0, max_put_oi_strike=57200.0, pcr=1.3)
    book._spot_acc.bars = _flat_spot_bars(base, price=57690.0)
    book._cooldown_until = datetime.now(IST) + timedelta(minutes=10)
    _try_enter_calls = []
    book._try_enter = lambda side: _try_enter_calls.append(side)
    logged = []
    book._clog = type("FakeLog", (), {"info": staticmethod(lambda *a, **kw: logged.append(a))})()

    book._on_spot_bar_close()

    assert _try_enter_calls == []
    assert any("COOLDOWN" in l[0] for l in logged)
    assert book._cooldown_until is not None   # still active, not cleared early


def test_on_spot_bar_close_resumes_once_cooldown_expires():
    book = _make_book()
    base = _base()
    book._position = None
    book._latest_snap = _FakeSnap(max_call_oi_strike=57700.0, max_put_oi_strike=57200.0, pcr=1.3)
    book._spot_acc.bars = _flat_spot_bars(base, price=57690.0)
    book._cooldown_until = datetime.now(IST) - timedelta(seconds=1)   # already expired
    _try_enter_calls = []
    book._try_enter = lambda side: _try_enter_calls.append(side)

    book._on_spot_bar_close()

    assert "CE" in _try_enter_calls
    assert book._cooldown_until is None   # cleared once expired


@pytest.mark.asyncio
async def test_square_off_starts_cooldown_after_stopout_exit():
    book = _make_book()
    pos = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
               entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0, s1_floor=480.0)
    book._position = pos

    async def _feed_full_fill():
        await asyncio.sleep(0.01)
        eid = next(iter(book._fill_waiters))
        fill = OIFlowFillEvent(action="SELL", underlying="BANKNIFTY", option_type="CE", strike=57700,
                                fill_price=475.0, qty=30, client_id="ssrajpal2001", binding_id="SA5770",
                                event_id=eid)
        book._on_fill(fill)

    await asyncio.gather(
        book._square_off(pos, "sl_option_swing_low@480.00", 475.0), _feed_full_fill(),
    )

    assert book._cooldown_until is not None
    assert book._cooldown_until > datetime.now(IST)


@pytest.mark.asyncio
async def test_square_off_does_not_start_cooldown_after_eod_exit():
    book = _make_book()
    pos = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
               entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0, s1_floor=480.0)
    book._position = pos

    async def _feed_full_fill():
        await asyncio.sleep(0.01)
        eid = next(iter(book._fill_waiters))
        fill = OIFlowFillEvent(action="SELL", underlying="BANKNIFTY", option_type="CE", strike=57700,
                                fill_price=505.0, qty=30, client_id="ssrajpal2001", binding_id="SA5770",
                                event_id=eid)
        book._on_fill(fill)

    await asyncio.gather(book._square_off(pos, "eod", 505.0), _feed_full_fill())

    assert book._cooldown_until is None


# ── corrupt-tick date guard ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_index_tick_loop_rejects_implausible_tick_date():
    """A single malformed/corrupt tick reporting a wildly wrong date must
    never trigger reset_session() -- would wipe bars/tracked-strike/
    cooldown/remarks for a day that hasn't actually changed."""
    book = _make_book()
    book._today = date(2026, 8, 13)   # today, matching the real date
    reset_calls = []
    book.reset_session = lambda: reset_calls.append(True)

    q = asyncio.Queue()
    book._loop_queues[Topic.INDEX_TICK] = q
    bad_tick = IndexTick(symbol="BANKNIFTY", ltp=57700.0, open=57600.0, high=57800.0, low=57500.0,
                          close=57700.0, volume=0, timestamp=datetime(1970, 1, 1, tzinfo=IST))
    await q.put(bad_tick)

    try:
        await asyncio.wait_for(book._index_tick_loop(), timeout=0.2)
    except asyncio.TimeoutError:
        pass

    assert reset_calls == []
    assert book._today == date(2026, 8, 13)   # unchanged
    assert len(book._spot_acc.bars) == 0 and book._spot_acc._bucket is None   # tick never bucketed either


@pytest.mark.asyncio
async def test_index_tick_loop_accepts_plausible_tick_date():
    book = _make_book()
    book._today = None   # first tick of the "day"
    reset_calls = []
    book.reset_session = lambda: reset_calls.append(True)

    q = asyncio.Queue()
    book._loop_queues[Topic.INDEX_TICK] = q
    real_today = datetime.now(IST).date()
    good_tick = IndexTick(symbol="BANKNIFTY", ltp=57700.0, open=57600.0, high=57800.0, low=57500.0,
                           close=57700.0, volume=0, timestamp=datetime.now(IST))
    await q.put(good_tick)

    try:
        await asyncio.wait_for(book._index_tick_loop(), timeout=0.2)
    except asyncio.TimeoutError:
        pass

    assert reset_calls == [True]   # first tick of the day -- genuine reset expected
    assert book._today == real_today


# ── _try_enter ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_try_enter_places_order_when_both_gates_pass():
    # async: _enter() internally does asyncio.create_task(...) to publish
    # the order event, which needs a running event loop.
    book = _make_book()
    base = _base()
    book._spot_acc.bars = _flat_spot_bars(base, price=57690.0)
    _seed_oi_for_signal(book, base)
    _seed_option_bars_ok(book, base)

    book._try_enter("CE")
    await asyncio.sleep(0.01)

    assert book._position is not None
    assert book._position["side"] == "CE"
    assert book._position["strike"] == 57700.0
    assert book._position["entry_price"] == 526.0
    assert book._position["sl_price"] == 502   # option chart's own confirmed swing low


def test_try_enter_no_entry_when_option_confirmation_blocks():
    book = _make_book()
    base = _base()
    book._spot_acc.bars = _flat_spot_bars(base, price=57690.0)
    _seed_oi_for_signal(book, base)
    # Option bars show price BELOW its own VWAP -- confirmation must block.
    bars = [Bar(base + timedelta(minutes=i), 500, 502, 498, 500) for i in range(3)]
    bars.append(Bar(base + timedelta(minutes=3), 480, 482, 460, 462))
    book._option_acc["CE"].bars = bars
    book._live_option_ltp["CE"] = 462.0

    book._try_enter("CE")

    assert book._position is None


def test_try_enter_no_entry_when_spot_signal_never_fires():
    book = _make_book()
    base = _base()
    book._spot_acc.bars = _flat_spot_bars(base, price=50000.0)   # far from any wall
    _seed_oi_for_signal(book, base)
    _seed_option_bars_ok(book, base)

    book._try_enter("CE")

    assert book._position is None


def test_try_enter_no_entry_when_live_ltp_not_ready():
    book = _make_book()
    base = _base()
    book._spot_acc.bars = _flat_spot_bars(base, price=57690.0)
    _seed_oi_for_signal(book, base)
    _seed_option_bars_ok(book, base)
    book._live_option_ltp.pop("CE", None)   # no live tick arrived yet for the wall strike

    book._try_enter("CE")

    assert book._position is None


@pytest.mark.asyncio
async def test_enter_publishes_order_event_via_bus():
    book = _make_book()
    base = _base()
    book._spot_acc.bars = _flat_spot_bars(base, price=57690.0)
    _seed_oi_for_signal(book, base)
    _seed_option_bars_ok(book, base)

    book._try_enter("CE")
    await asyncio.sleep(0.01)   # let the create_task'd publish actually run

    orders = [e for t, e in book._bus.published if t == Topic.OI_FLOW_ORDER_REQUEST]
    assert len(orders) == 1
    order = orders[0]
    assert isinstance(order, OIFlowOrderEvent)
    assert order.action == "BUY"
    assert order.option_type == "CE"
    assert order.strike == 57700
    assert order.entry_price == 526.0


# ── _option_tick_loop ────────────────────────────────────────────────────────

def _real_option_tick(strike, side, ltp, volume, ts, underlying="BANKNIFTY"):
    """_option_tick_loop() does `isinstance(ev, OptionTick)` -- unlike
    _FakeTick (duck-typed, used only against OIFlowTracker directly), this
    loop needs the real, frozen dataclass."""
    return OptionTick(
        symbol=f"{underlying}{side}{int(strike)}", underlying=underlying, strike=strike,
        option_type=side, expiry=date(2026, 8, 27), ltp=ltp, bid=ltp, ask=ltp,
        oi=0, change_oi=0, volume=volume, iv=0.0, delta=0.0, timestamp=ts,
    )


@pytest.mark.asyncio
async def test_option_tick_loop_threads_cumulative_volume_into_option_bars():
    """Regression guard for the ev.volume -> BarAccumulator.on_tick()
    wiring (2026-08-13): without passing ev.volume through, an option
    bar's own volume would silently stay 0.0 forever and
    detect_volume_spike() could never fire for real ticks."""
    book = _make_book()
    base = _base()
    book._latest_snap = _FakeSnap(max_call_oi_strike=57700.0, max_put_oi_strike=57200.0, pcr=1.3)
    book._rewatch_oi_strikes(book._latest_snap)

    q = asyncio.Queue()
    book._loop_queues[Topic.OPTION_TICK] = q
    # First two ticks stay WITHIN the same 1-min bucket (volume accrues on
    # the second one); the third rolls into a new bucket, closing the first.
    await q.put(_real_option_tick(57700.0, "CE", 500.0, 10_000, base))
    await q.put(_real_option_tick(57700.0, "CE", 502.0, 10_600, base + timedelta(seconds=30)))
    await q.put(_real_option_tick(57700.0, "CE", 501.0, 10_900, base + timedelta(minutes=1)))

    try:
        await asyncio.wait_for(book._option_tick_loop(), timeout=0.2)
    except asyncio.TimeoutError:
        pass   # expected -- the loop only exits on self._running=False, never on its own

    assert len(book._option_acc["CE"].bars) == 1
    assert book._option_acc["CE"].bars[0].volume == 600   # 10_600 - 10_000
    assert book._live_option_ltp["CE"] == 501.0


# ── _check_exit ──────────────────────────────────────────────────────────────

def test_check_exit_triggers_on_sl_hit_for_ce():
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(475.0)   # below the 480 SL

    assert exited.get("exit_price") == 475.0
    assert "sl_option_swing_low" in exited.get("reason", "")


def test_check_exit_triggers_on_sl_hit_for_pe_same_direction_as_ce():
    """2026-08-13 regression guard: a bought PE is long its own premium,
    same as CE -- SL fires when the PREMIUM FALLS to/through its own
    swing-low floor, not when it rises. Before the fix, PE's condition was
    `ltp >= sl_price` (fires on a RISE) -- this proves the fixed version
    fires on a FALL, identically to CE."""
    book = _make_book()
    book._position = dict(side="PE", strike=57200.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(475.0)   # premium fell below the 480 SL floor

    assert exited.get("exit_price") == 475.0
    assert "sl_option_swing_low" in exited.get("reason", "")


def test_check_exit_no_trigger_when_above_sl():
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(495.0)   # still above SL

    assert exited == {}


# ── _check_exit: step-locked trailing profit-lock ("target" concept) ────────

def test_check_exit_tsl_activates_above_trigger_without_exiting():
    # entry=500, trigger=0.15 -> activation price 575; ltp=580 -> profit_pct=0.16
    # -> steps=0 -> lock=first_lock_pct=0.08 -> floor=500*1.08=540; 580>540, no exit.
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(580.0)

    assert exited == {}
    assert book._position["high_lock_pct"] == pytest.approx(0.08)


def test_check_exit_tsl_hit_after_activation_fires_on_pullback_to_locked_floor():
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(580.0)    # activates TSL, locks floor @ 540
    assert exited == {}
    book._check_exit(535.0)    # falls through the locked 540 floor (well below the original 480 SL too)

    assert exited.get("exit_price") == 535.0
    assert "tsl_hit@540.00" in exited.get("reason", "")


def test_check_exit_tsl_ratchets_up_with_further_gains():
    # profit_pct=0.26 -> steps=int((0.26-0.15)//0.10)=1 -> lock=0.08+1*0.05=0.13 -> floor=565.
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(630.0)   # profit_pct=0.26

    assert exited == {}
    assert book._position["high_lock_pct"] == pytest.approx(0.13)


def test_check_exit_tsl_never_unlocks_on_a_pullback_that_stays_above_floor():
    """The ratchet only ever tightens -- a pullback that still clears the
    ALREADY-locked floor must not loosen it back down, even though that
    pullback's OWN profit_pct would only justify a smaller lock on its own."""
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(630.0)   # locks 0.13 (floor 565)
    assert book._position["high_lock_pct"] == pytest.approx(0.13)
    book._check_exit(590.0)   # profit_pct=0.18 alone would only justify 0.08 -- must stay 0.13

    assert book._position["high_lock_pct"] == pytest.approx(0.13)
    assert exited == {}   # 590 still clears the 565 floor


def test_check_exit_tsl_symmetric_for_pe():
    """Same ratchet math for a bought PE -- long its own premium too."""
    book = _make_book()
    book._position = dict(side="PE", strike=57200.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(580.0)
    assert book._position["high_lock_pct"] == pytest.approx(0.08)
    book._check_exit(535.0)

    assert "tsl_hit@540.00" in exited.get("reason", "")


# ── S1 trailing stop ("S1 will act as TSL") ──────────────────────────────────

def test_maybe_promote_s1_promotes_to_a_new_higher_confirmed_swing_low():
    book = _make_book()
    base = _base()
    book._position = dict(side="CE", strike=57700.0, entry_price=520.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0, s1_floor=480.0)
    book._option_acc["CE"].bars = [
        Bar(base, 520, 522, 518, 521),
        Bar(base + timedelta(minutes=1), 519, 521, 515, 518),
        Bar(base + timedelta(minutes=2), 506, 512, 502, 508),   # confirmed swing low @ 502 (pivot=2)
        Bar(base + timedelta(minutes=3), 514, 518, 511, 515),
        Bar(base + timedelta(minutes=4), 519, 524, 516, 522),
        Bar(base + timedelta(minutes=5), 523, 528, 520, 526),
    ]

    book._maybe_promote_s1("CE")

    assert book._position["s1_floor"] == 502


def test_maybe_promote_s1_never_demotes():
    book = _make_book()
    base = _base()
    book._position = dict(side="CE", strike=57700.0, entry_price=520.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0, s1_floor=502.0)
    # This sequence's OWN confirmed swing low (482) is LOWER than the
    # already-promoted s1_floor (502) -- must not un-ratchet.
    book._option_acc["CE"].bars = [
        Bar(base, 500, 502, 498, 500),
        Bar(base + timedelta(minutes=1), 499, 501, 495, 497),
        Bar(base + timedelta(minutes=2), 486, 492, 482, 488),   # confirmed swing low @ 482
        Bar(base + timedelta(minutes=3), 494, 498, 491, 495),
        Bar(base + timedelta(minutes=4), 499, 504, 496, 502),
        Bar(base + timedelta(minutes=5), 503, 508, 500, 506),
    ]

    book._maybe_promote_s1("CE")

    assert book._position["s1_floor"] == 502.0   # unchanged


def test_maybe_promote_s1_noop_when_no_position():
    book = _make_book()
    book._position = None
    book._option_acc["CE"].bars = _flat_spot_bars(_base(), price=500.0)
    book._maybe_promote_s1("CE")   # must not raise
    assert book._position is None


def test_maybe_promote_s1_noop_for_the_non_position_side():
    book = _make_book()
    base = _base()
    book._position = dict(side="CE", strike=57700.0, entry_price=520.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0, s1_floor=480.0)
    book._option_acc["PE"].bars = [
        Bar(base, 520, 522, 518, 521),
        Bar(base + timedelta(minutes=1), 519, 521, 515, 518),
        Bar(base + timedelta(minutes=2), 506, 512, 502, 508),
        Bar(base + timedelta(minutes=3), 514, 518, 511, 515),
        Bar(base + timedelta(minutes=4), 519, 524, 516, 522),
        Bar(base + timedelta(minutes=5), 523, 528, 520, 526),
    ]

    book._maybe_promote_s1("PE")   # position is CE -- PE-side bars must never affect it

    assert book._position["s1_floor"] == 480.0


def test_check_exit_s1_hit_when_s1_floor_is_the_tighter_constraint():
    """S1 has promoted well above both the original SL and the (inactive)
    percentage floor -- it alone should bind, and the exit reason should
    credit S1, not the plain SL or the percentage TSL."""
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0, s1_floor=495.0)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(490.0)   # below S1's 495 floor; pct_floor is still just the original 480 SL

    assert exited.get("exit_price") == 490.0
    assert "s1_hit@495.00" in exited.get("reason", "")


def test_check_exit_plain_sl_hit_when_neither_tsl_nor_s1_ever_promoted():
    """Regression guard: a fresh position where NEITHER mechanism ever
    activated must still report the original 'sl_option_swing_low' label,
    not a misleading 's1_hit' just because s1_floor defaults to sl_price."""
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0, s1_floor=480.0)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(475.0)

    assert "sl_option_swing_low" in exited.get("reason", "")
    assert "s1_hit" not in exited.get("reason", "")


# ── option-strike lock (wall-drift-during-position fix) ──────────────────────

@pytest.mark.asyncio
async def test_option_tick_loop_locks_onto_position_strike_even_if_wall_drifts():
    """2026-08-13 fix: _option_acc/_live_option_ltp used to ALWAYS follow
    the CURRENT OI wall from the latest snapshot, even with an open
    position at a DIFFERENT strike -- if the wall drifted intraday, these
    would silently start tracking the NEW wall's premium instead of the
    position's actual held strike (corrupting S1/EOD-fallback/dashboard
    P&L). Drives the real _option_tick_loop with a position open at 57700
    while the snapshot's wall has already moved to 57800, and proves only
    57700 ticks are tracked."""
    book = _make_book()
    base = _base()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0, s1_floor=480.0)
    book._latest_snap = _FakeSnap(max_call_oi_strike=57800.0, max_put_oi_strike=57200.0, pcr=1.3)

    q = asyncio.Queue()
    book._loop_queues[Topic.OPTION_TICK] = q
    await q.put(_real_option_tick(57800.0, "CE", 999.0, 1000, base))                         # drifted wall -- ignore
    await q.put(_real_option_tick(57700.0, "CE", 505.0, 2000, base + timedelta(seconds=1)))  # the real held strike

    try:
        await asyncio.wait_for(book._option_tick_loop(), timeout=0.2)
    except asyncio.TimeoutError:
        pass

    assert book._live_option_ltp["CE"] == 505.0   # never picked up the drifted wall's 999.0
    assert len(book._option_acc["CE"].bars) == 0  # only one tick landed in the still-open bucket -- no bar closed yet


@pytest.mark.asyncio
async def test_option_tick_loop_check_exit_only_fires_for_the_position_strike():
    """Same drift scenario, proving _check_exit() itself is never called
    for the drifted wall's ticks -- only for the position's own strike."""
    book = _make_book()
    base = _base()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0, s1_floor=480.0)
    book._latest_snap = _FakeSnap(max_call_oi_strike=57800.0, max_put_oi_strike=57200.0, pcr=1.3)
    exit_calls = []
    book._check_exit = lambda ltp: exit_calls.append(ltp)

    q = asyncio.Queue()
    book._loop_queues[Topic.OPTION_TICK] = q
    await q.put(_real_option_tick(57800.0, "CE", 400.0, 1000, base))   # would be a "SL hit" on the WRONG strike
    await q.put(_real_option_tick(57700.0, "CE", 505.0, 2000, base + timedelta(seconds=1)))

    try:
        await asyncio.wait_for(book._option_tick_loop(), timeout=0.2)
    except asyncio.TimeoutError:
        pass

    assert exit_calls == [505.0]   # only the real strike's tick reached _check_exit


@pytest.mark.asyncio
async def test_option_tick_loop_resets_accumulator_when_wall_drifts_while_flat():
    """2026-08-13 fix, twin of the position-side lock above: while FLAT/
    scanning, _option_acc/_live_option_ltp always followed the CURRENT
    wall -- correct in principle, but the accumulator itself was never
    RESET when the wall moved to a different strike, so its .bars list
    would silently keep growing with a MIX of two different option
    contracts' OHLC once the wall drifted even once. This is the far more
    common case (happens any time the wall moves while flat, not just
    during an open trade). Feeds ticks for wall A, then a NEW snapshot
    moves the wall to strike B, then feeds ticks for B -- proves the
    accumulator reset and only reflects B afterward."""
    book = _make_book()
    base = _base()
    book._position = None
    book._latest_snap = _FakeSnap(max_call_oi_strike=57700.0, max_put_oi_strike=57200.0, pcr=1.3)

    q = asyncio.Queue()
    book._loop_queues[Topic.OPTION_TICK] = q
    # Two ticks on wall A (57700) within the same bucket -- would normally
    # start building a real bar.
    await q.put(_real_option_tick(57700.0, "CE", 500.0, 10_000, base))
    await q.put(_real_option_tick(57700.0, "CE", 502.0, 10_600, base + timedelta(seconds=30)))
    try:
        await asyncio.wait_for(book._option_tick_loop(), timeout=0.15)
    except asyncio.TimeoutError:
        pass
    assert book._live_option_ltp["CE"] == 502.0
    assert book._tracked_option_strike["CE"] == 57700.0

    # Wall drifts to a NEW strike (57800) -- a fresh snapshot arrives.
    book._latest_snap = _FakeSnap(max_call_oi_strike=57800.0, max_put_oi_strike=57200.0, pcr=1.3)
    await q.put(_real_option_tick(57800.0, "CE", 300.0, 500, base + timedelta(seconds=45)))
    try:
        await asyncio.wait_for(book._option_tick_loop(), timeout=0.15)
    except asyncio.TimeoutError:
        pass

    assert book._tracked_option_strike["CE"] == 57800.0
    assert book._live_option_ltp["CE"] == 300.0   # reflects ONLY the new wall, not a mix
    # The old wall's in-progress bucket must be gone, not carried forward
    # into a bar mixing both instruments' prices.
    assert book._option_acc["CE"]._bucket is None or book._option_acc["CE"]._bucket.open == 300.0


# ── feed-staleness watchdog ───────────────────────────────────────────────────

def test_check_tick_staleness_no_alert_when_recent():
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30)
    book._last_position_tick_ts = datetime.now(IST) - timedelta(seconds=10)
    logged = []
    book._clog = type("FakeLog", (), {"info": staticmethod(lambda *a, **kw: logged.append(a))})()

    book._check_tick_staleness()

    assert logged == []
    assert book._staleness_alerted is False


def test_check_tick_staleness_alerts_when_stale():
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30)
    book._last_position_tick_ts = datetime.now(IST) - timedelta(seconds=90)   # well past the 60s threshold
    logged = []
    book._clog = type("FakeLog", (), {"info": staticmethod(lambda *a, **kw: logged.append(a))})()

    book._check_tick_staleness()

    assert len(logged) == 1
    assert "ALERT" in logged[0][0]
    assert book._staleness_alerted is True


def test_check_tick_staleness_alerts_only_once_per_episode():
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30)
    book._last_position_tick_ts = datetime.now(IST) - timedelta(seconds=90)
    logged = []
    book._clog = type("FakeLog", (), {"info": staticmethod(lambda *a, **kw: logged.append(a))})()

    book._check_tick_staleness()
    book._check_tick_staleness()   # still stale, called again 5s later (as _eod_loop would)

    assert len(logged) == 1   # not re-alerted every cycle


def test_check_tick_staleness_noop_when_never_ticked():
    """No position tick recorded yet (e.g. no position open) -- must not
    crash or false-alert."""
    book = _make_book()
    book._position = None
    book._last_position_tick_ts = None
    book._check_tick_staleness()   # must not raise
    assert book._staleness_alerted is False


def test_check_exit_hard_risk_cap_backstop():
    book = _make_book()
    # entry=500, qty=30, hard_risk_rs_per_lot=2000 -> risk_floor = 500 - 2000/30 = 433.33
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=100.0,  # SL far away
                           entry_ts=datetime.now(IST), qty=30)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(430.0)   # below the hard risk cap floor, even though SL (100) wasn't hit

    assert exited.get("exit_price") == 430.0
    assert "hard_risk_cap" in exited.get("reason", "")


# ── _on_fill ─────────────────────────────────────────────────────────────────

def test_on_fill_entry_aborted_discards_optimistic_position():
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, _event_id="EV1")
    fill = OIFlowFillEvent(action="BUY", underlying="BANKNIFTY", option_type="CE", strike=57700,
                            fill_price=0.0, qty=30, client_id="ssrajpal2001", binding_id="SA5770",
                            event_id="EV1", entry_aborted=True)
    book._on_fill(fill)
    assert book._position is None


def test_on_fill_buy_partial_fill_reconciles_position_qty_down():
    """The position was booked optimistically at decision time before this
    confirmation arrives -- a partial fill (broker only filled 15 of the
    requested 30) must reconcile qty down, not silently keep the position
    at the full requested amount (would corrupt P&L/risk-cap sizing and
    the eventual exit order's own quantity)."""
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, _event_id="EV1")
    fill = OIFlowFillEvent(action="BUY", underlying="BANKNIFTY", option_type="CE", strike=57700,
                            fill_price=500.0, qty=30, client_id="ssrajpal2001", binding_id="SA5770",
                            event_id="EV1", filled_qty=15)

    book._on_fill(fill)

    assert book._position is not None   # NOT discarded -- a partial fill is a real, held position
    assert book._position["qty"] == 15


def test_on_fill_buy_full_fill_leaves_qty_unchanged():
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30, _event_id="EV1")
    fill = OIFlowFillEvent(action="BUY", underlying="BANKNIFTY", option_type="CE", strike=57700,
                            fill_price=500.0, qty=30, client_id="ssrajpal2001", binding_id="SA5770",
                            event_id="EV1")   # filled_qty defaults to qty (full fill) via __post_init__

    book._on_fill(fill)

    assert book._position["qty"] == 30


def test_on_fill_sell_sets_waiter_for_matching_event_id():
    book = _make_book()
    waiter = asyncio.Event()
    book._fill_waiters["EV2"] = waiter
    fill = OIFlowFillEvent(action="SELL", underlying="BANKNIFTY", option_type="CE", strike=57700,
                            fill_price=520.0, qty=30, client_id="ssrajpal2001", binding_id="SA5770",
                            event_id="EV2")
    book._on_fill(fill)
    assert waiter.is_set()
    assert book._fill_results["EV2"] is fill


@pytest.mark.asyncio
async def test_square_off_partial_exit_fill_reduces_qty_and_keeps_position_open():
    """A partial EXIT fill (broker only closed 15 of 30 held lots) must
    NOT clear the position -- that would make the engine believe it's
    flat while still actually holding the remainder naked, unprotected by
    any further SL/TSL/S1 check (which only run while self._position is
    not None). qty must reduce to what's genuinely still open, and the
    leg stays open (same as an unconfirmed/aborted exit) for a retry."""
    book = _make_book()
    pos = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
               entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0, s1_floor=480.0)
    book._position = pos

    async def _feed_partial_fill():
        await asyncio.sleep(0.01)
        eid = next(iter(book._fill_waiters))
        fill = OIFlowFillEvent(action="SELL", underlying="BANKNIFTY", option_type="CE", strike=57700,
                                fill_price=490.0, qty=30, client_id="ssrajpal2001", binding_id="SA5770",
                                event_id=eid, filled_qty=15)
        book._on_fill(fill)

    await asyncio.gather(book._square_off(pos, "sl_option_swing_low@480.00", 490.0), _feed_partial_fill())

    assert book._position is pos    # NOT cleared
    assert book._position["qty"] == 15


@pytest.mark.asyncio
async def test_square_off_full_exit_fill_clears_position():
    """Sanity check for the test above: a FULL exit fill still clears the
    position exactly as before (proves the partial-fill branch doesn't
    accidentally also catch the normal case)."""
    book = _make_book()
    pos = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
               entry_ts=datetime.now(IST), qty=30, high_lock_pct=0.0, s1_floor=480.0)
    book._position = pos

    async def _feed_full_fill():
        await asyncio.sleep(0.01)
        eid = next(iter(book._fill_waiters))
        fill = OIFlowFillEvent(action="SELL", underlying="BANKNIFTY", option_type="CE", strike=57700,
                                fill_price=490.0, qty=30, client_id="ssrajpal2001", binding_id="SA5770",
                                event_id=eid)   # filled_qty defaults to qty (full) via __post_init__
        book._on_fill(fill)

    await asyncio.gather(book._square_off(pos, "sl_option_swing_low@480.00", 490.0), _feed_full_fill())

    assert book._position is None


# ── monitoring_state() / remarks (dashboard) ─────────────────────────────────

def test_try_enter_appends_a_remark_on_every_evaluation():
    book = _make_book()
    base = _base()
    book._spot_acc.bars = _flat_spot_bars(base, price=50000.0)   # far from any wall -> rejects
    _seed_oi_for_signal(book, base)
    _seed_option_bars_ok(book, base)

    book._try_enter("CE")

    assert len(book._recent_remarks) == 1
    remark = book._recent_remarks[0]
    assert remark["side"] == "CE"
    assert "no spot signal" in remark["text"]


@pytest.mark.asyncio
async def test_try_enter_remark_on_entry_mentions_strike_and_sl():
    book = _make_book()
    base = _base()
    book._spot_acc.bars = _flat_spot_bars(base, price=57690.0)
    _seed_oi_for_signal(book, base)
    _seed_option_bars_ok(book, base)

    book._try_enter("CE")
    await asyncio.sleep(0.01)

    remark = book._recent_remarks[0]
    assert remark["level"] == "entry"
    assert "ENTERED" in remark["text"]
    assert "57700" in remark["text"]


def test_monitoring_state_shape_with_no_snapshot_or_position():
    book = _make_book()
    state = book.monitoring_state()
    assert state["underlying"] == "BANKNIFTY"
    assert state["client_id"] == "ssrajpal2001"
    assert state["binding_id"] == "SA5770"
    assert state["walls"] == []
    assert state["position"] is None
    assert state["remarks"] == []


def test_monitoring_state_shows_oi_walls_and_buildup():
    book = _make_book()
    base = _base()
    _seed_oi_for_signal(book, base)   # sets book._latest_snap + real tracker data

    state = book.monitoring_state()

    walls = {w["side"]: w for w in state["walls"]}
    assert walls["CE"]["wall_strike"] == 57700.0
    assert walls["CE"]["wall_oi"] == 95_000        # latest CE-wall OI reading
    assert walls["CE"]["wall_oi_roc"] == -5000      # opposing OI dropping -- writers fleeing
    assert walls["CE"]["supporting_strike"] == 57600.0
    assert walls["CE"]["supporting_oi_roc"] == 3000  # supporting side building


def test_monitoring_state_shows_open_position_with_live_pnl():
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30)
    book._live_option_ltp["CE"] = 520.0

    state = book.monitoring_state()

    assert state["position"]["side"] == "CE"
    assert state["position"]["entry_price"] == 500.0
    assert state["position"]["ltp"] == 520.0
    assert state["position"]["pnl"] == 600.0   # (520-500)*30


# ── telemetry (Phase 5): every evaluation logs a row, fired or not ──────────

@pytest.mark.asyncio
async def test_try_enter_logs_a_full_telemetry_row_on_entry(monkeypatch):
    captured = []
    monkeypatch.setattr(engine_module, "log_signal_evaluation", lambda row: captured.append(row))

    book = _make_book()
    base = _base()
    book._spot_acc.bars = _flat_spot_bars(base, price=57690.0)
    _seed_oi_for_signal(book, base)
    _seed_option_bars_ok(book, base)

    book._try_enter("CE")
    await asyncio.sleep(0.01)

    assert len(captured) == 1
    row = captured[0]
    assert row.entered is True
    assert row.skip_reason == ""
    assert row.spot_gate_fired is True
    assert row.wall_strike == 57700.0
    assert row.opposing_roc == -5000
    assert row.supporting_roc == 3000
    assert row.pcr == 1.3
    assert row.option_gate_ok is True
    assert row.option_sl_level == 502
    assert row.option_vwap is not None


def test_try_enter_logs_a_row_with_skip_reason_when_spot_gate_rejects(monkeypatch):
    captured = []
    monkeypatch.setattr(engine_module, "log_signal_evaluation", lambda row: captured.append(row))

    book = _make_book()
    base = _base()
    book._spot_acc.bars = _flat_spot_bars(base, price=50000.0)   # far from any wall -> spot gate rejects
    _seed_oi_for_signal(book, base)
    _seed_option_bars_ok(book, base)

    book._try_enter("CE")

    assert len(captured) == 1
    row = captured[0]
    assert row.entered is False
    assert row.skip_reason == "spot_gate_no_signal"
    assert row.spot_gate_fired is False
    # Raw OI/PCR diagnostics are still captured even though the gate rejected --
    # exactly the point: a rejection must be just as reviewable as a firing.
    assert row.opposing_roc == -5000
    assert row.pcr == 1.3
    # Option gate never ran (spot gate rejected first) -- its fields stay unset.
    assert row.option_gate_ok is None


def test_try_enter_logs_option_gate_rejection_reason(monkeypatch):
    captured = []
    monkeypatch.setattr(engine_module, "log_signal_evaluation", lambda row: captured.append(row))

    book = _make_book()
    base = _base()
    book._spot_acc.bars = _flat_spot_bars(base, price=57690.0)
    _seed_oi_for_signal(book, base)
    bars = [Bar(base + timedelta(minutes=i), 500, 502, 498, 500) for i in range(3)]
    bars.append(Bar(base + timedelta(minutes=3), 480, 482, 460, 462))
    book._option_acc["CE"].bars = bars
    book._live_option_ltp["CE"] = 462.0

    book._try_enter("CE")

    assert len(captured) == 1
    row = captured[0]
    assert row.entered is False
    assert row.skip_reason == "option_gate_blocked"
    assert row.spot_gate_fired is True
    assert row.option_gate_ok is False
    assert row.option_gate_reason == "below_vwap"
