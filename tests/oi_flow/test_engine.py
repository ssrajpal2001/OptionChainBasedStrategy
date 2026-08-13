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
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pytest

from config.global_config import IST, Topic
from data_layer.base_feeder import OptionTick
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
    book._watched_strikes = {}
    book._position = None
    book._day_done = False
    book._event_counter = 0
    book._fill_waiters = {}
    book._fill_results = {}
    book._recent_remarks = deque(maxlen=30)
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


# ── _rewatch_oi_strikes ───────────────────────────────────────────────────────

def test_rewatch_oi_strikes_derives_correct_watch_list():
    book = _make_book()
    snap = _FakeSnap(max_call_oi_strike=57700.0, max_put_oi_strike=57200.0)
    book._rewatch_oi_strikes(snap)
    assert (57700.0, "CE") in book._watched_strikes    # call wall itself
    assert (57600.0, "PE") in book._watched_strikes    # one step below -- CE's supporting side
    assert (57200.0, "PE") in book._watched_strikes    # put wall itself
    assert (57300.0, "CE") in book._watched_strikes    # one step above -- PE's supporting side


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


def test_check_exit_no_trigger_when_above_sl():
    book = _make_book()
    book._position = dict(side="CE", strike=57700.0, entry_price=500.0, sl_price=480.0,
                           entry_ts=datetime.now(IST), qty=30)
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)

    book._check_exit(495.0)   # still above SL

    assert exited == {}


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
