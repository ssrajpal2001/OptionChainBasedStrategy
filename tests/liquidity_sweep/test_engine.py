"""
Integration-style tests for strategies/liquidity_sweep/engine.py
(LiquiditySweepStrategy). Constructed via __new__ + manual attribute
assignment (same pattern as tests/oi_flow/test_engine.py) rather than
driving the real async bus/loops end-to-end -- exercises the book's own
decision logic (pipeline state machine, entry, exit) directly and
deterministically.
"""
import asyncio
from collections import deque
from datetime import datetime, time as dtime, timedelta

import pytest

from config.global_config import IST, Topic
from strategies.liquidity_sweep.detector import BarAccumulator
from strategies.liquidity_sweep.engine import LiquiditySweepStrategy
from strategies.liquidity_sweep.events import LiquiditySweepOrderEvent

BASE = datetime(2026, 8, 18, 9, 15, tzinfo=IST)


class _CapturingBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


def _make_book(**overrides) -> LiquiditySweepStrategy:
    book = LiquiditySweepStrategy.__new__(LiquiditySweepStrategy)
    book._bus = _CapturingBus()
    book._cfg = None
    book._underlying = "NIFTY"
    book._client_id = "ssrajpal2001"
    book._binding_id = "SA5770"
    book._running = True
    book._tasks = []
    book._loop_queues = {}

    book._strategy_name = "liquidity_sweep"
    book._lot_multiplier = 1
    book._ltf_min = 5
    book._htf_min = 75
    book._liq_source = "swing_pivots"
    book._pivot_left = 1
    book._pivot_right = 1
    book._pool_tol_pts = 5.0
    book._pool_min_touches = 2
    book._use_struct_bias = False
    book._atr_len = 4
    book._atr_mult = 0.7
    book._disp_window = 5
    book._swing_len = 2
    book._fvg_confirm_window = 5
    book._stale_bars = 5
    book._tgt1_rr = 1.5
    book._use_liquidity_target2 = True
    book._tgt2_rr = 3.0
    book._itm_offset_pts = 0.0
    book._hard_risk_rs_per_lot = 2000.0
    book._sl_cooldown_minutes = 15.0
    book._product_type = "MIS"
    book._squareoff_time = dtime(23, 59)   # avoid real-wall-clock EOD flakiness in tests
    book._lot_size = 75
    book._strike_step = 50.0
    book._persist_key = "test_liqsweep_persist_key"

    book._today = None
    book._feeder_token = ""
    book._warming_up = False
    book._ltf_acc = BarAccumulator(timeframe_min=book._ltf_min)
    book._htf_acc = BarAccumulator(timeframe_min=book._htf_min)
    book._live_ltp = {}

    book._pending_dir = 0
    book._pending_bars_left = 0
    book._sweep_extreme = None
    book._disp_c1 = None
    book._awaiting_fvg_dir = 0
    book._fvg_bars_left = 0
    book._armed_sweep_extreme = None
    book._fvg_dir = 0
    book._fvg_lo = None
    book._fvg_hi = None
    book._sl_anchor = None
    book._retest_bars_left = 0
    book._last_bias = 0
    book._last_level_high = None
    book._last_level_low = None

    book._position = None
    book._cooldown_until = None
    book._day_done = False
    book._event_counter = 0
    book._fill_waiters = {}
    book._fill_results = {}
    book._recent_remarks = deque(maxlen=30)

    import logging
    book._clog = logging.getLogger("test_liqsweep_clog")
    book._persist_position = lambda: None
    for k, v in overrides.items():
        setattr(book, k, v)
    return book


class _BarFeeder:
    """Feeds synthetic (o,h,l,c) bars one at a time into the book's real
    BarAccumulator via 4 in-bucket ticks each (open/high/low/close, all
    within the same 5-min window) -- BarAccumulator.on_tick() only actually
    CLOSES a bar's bucket on the FIRST tick of the NEXT bar (a real,
    non-obvious property of its bucket-by-timestamp design), so each bar's
    _on_ltf_bar_close() fires one bar "behind" the feed() call that
    introduced the FOLLOWING bar."""

    def __init__(self, book: LiquiditySweepStrategy) -> None:
        self.book = book
        self._i = 0

    def feed(self, o, h, l, c) -> None:
        ts = BASE + timedelta(minutes=5 * self._i)
        acc = self.book._ltf_acc
        for offset, val in ((0, o), (30, h), (60, l), (90, c)):
            closed = acc.on_tick(ts + timedelta(seconds=offset), val)
            if closed:
                self.book._on_ltf_bar_close()
        self._i += 1


@pytest.mark.asyncio
async def test_full_pipeline_sweep_to_retest_entry():
    """Drives one complete bearish (PE) sweep -> displacement -> FVG ->
    retest -> entry sequence through the real state machine, using
    liq_source='swing_pivots' (single-swing, no clustering needed) and
    use_struct_bias=False to isolate the pipeline wiring itself -- the
    underlying pure functions already have their own dedicated unit tests
    in test_detector.py.

    IMPORTANT timing note: BarAccumulator only closes a bar's bucket on the
    NEXT bar's first tick (see _BarFeeder's own docstring) -- so
    _on_ltf_bar_close() for "bar k" always fires one feed() call AFTER bar
    k itself was fed, and its effects are asserted after THAT next call.

    bar0: baseline. bar1: swing-HIGH candidate (112). bar2: baseline.
    bar3: sweep bar (pierces 112, closes back under the level's body).
    bar4: displacement (big bearish body breaking the micro-swing low,
    high kept above bar3's low so it doesn't ALSO gap on the same bar).
    bar5: FVG bar (gaps below bar3's low) -- note the retest check runs
    in the SAME closure right after FVG confirms (faithful to the
    validated Pine script's own same-bar cascading: the FVG's own lo
    boundary is defined as this bar's OWN high, so check_retest is
    satisfied by construction on the very bar that created the gap)."""
    book = _make_book()
    feeder = _BarFeeder(book)

    feeder.feed(100, 100, 99, 100)     # bar0
    feeder.feed(100, 112, 99, 101)     # bar1 -- closes bar0 (1 bar, no-op)
    feeder.feed(101, 101, 99, 100)     # bar2 -- closes bar1 (2 bars, no-op)
    feeder.feed(101, 113, 100, 100.8)  # bar3 -- closes bar2 (3 bars: swing HIGH@bar1 confirmed, cur=bar2, no sweep)
    assert book._pending_dir == 0

    feeder.feed(99, 100.5, 90, 91)     # bar4 -- closes bar3 (cur=bar3: SWEEP fires)
    assert book._pending_dir == -1
    assert book._sweep_extreme == 113

    feeder.feed(90, 91, 85, 86)        # bar5 -- closes bar4 (cur=bar4: DISPLACEMENT fires, no same-bar gap)
    assert book._pending_dir == 0
    assert book._awaiting_fvg_dir == -1
    assert book._armed_sweep_extreme == 113

    assert book._position is None
    feeder.feed(95, 96, 94, 95)        # bar6 -- closes bar5 (cur=bar5: FVG confirms AND self-retests same bar)
    assert book._awaiting_fvg_dir == 0
    assert book._fvg_dir == 0          # consumed same-bar (no live LTP yet -> entry skipped, not faked)
    assert book._sl_anchor == 113

    # No live option LTP was available at retest time -- entry must have
    # been SKIPPED, never faked.
    assert book._position is None
    assert book._bus.published == []

    # Seed a live option LTP for the strike this retest would trade, then
    # re-drive _try_enter directly using the REAL levels the pipeline just
    # computed and stored (book._last_level_high/_low) -- confirms
    # _try_enter's own guard/strike-selection/order-publish logic once the
    # gates are actually satisfiable, without re-consuming pipeline state.
    # entry_spot = bar5's own close (86.0) -- the bar whose own closure
    # triggered the self-retest above.
    entry_spot = 86.0
    atm = round(entry_spot / book._strike_step) * book._strike_step   # 100
    strike = atm + book._itm_offset_pts   # PE: atm + offset = 100
    book._live_ltp[(strike, "PE")] = 12.5
    level_high, level_low = book._last_level_high, book._last_level_low
    assert level_low is None   # bar5's own LOW pivot isn't confirmable yet (needs a bar after it)
    book._try_enter(-1, entry_spot, level_high, level_low)
    await asyncio.sleep(0.01)

    assert book._position is not None
    pos = book._position
    assert pos["side"] == "PE"
    assert pos["strike"] == strike
    assert pos["entry_price"] == 12.5
    assert pos["entry_spot"] == entry_spot
    assert pos["sl_spot"] == 113.0   # sl_anchor from the FVG stage
    # No valid opposing (support) liquidity was available -- must fall back
    # to the R-multiple target, never silently use level_high (wrong side).
    risk = abs(entry_spot - 113.0)
    assert pos["t2_spot"] == entry_spot - risk * book._tgt2_rr

    assert len(book._bus.published) == 1
    topic, ev = book._bus.published[0]
    assert topic == Topic.LIQUIDITY_SWEEP_ORDER_REQUEST
    assert isinstance(ev, LiquiditySweepOrderEvent)
    assert ev.action == "BUY" and ev.option_type == "PE" and ev.strike == int(strike)


def test_try_enter_skips_when_no_live_option_ltp():
    book = _make_book()
    book._sl_anchor = 113.0
    book._try_enter(-1, 95.0, None, None)
    assert book._position is None
    assert book._bus.published == []


@pytest.mark.asyncio
async def test_try_enter_uses_liquidity_target2_on_correct_side():
    """direction=1 (CE/bullish) must target the level ABOVE entry
    (level_high), never level_low -- this is the exact bug class found and
    fixed during construction (opp_level was originally picked backwards).
    async: _enter() internally does asyncio.create_task(...) to publish the
    order event, which needs a running event loop."""
    from strategies.liquidity_sweep.detector import SwingPoint
    book = _make_book()
    book._sl_anchor = 90.0
    book._live_ltp[(100.0, "CE")] = 25.0
    level_high = SwingPoint(index=1, timestamp=BASE, price=120.0, body_extreme=118.0, kind="HIGH")
    level_low = SwingPoint(index=0, timestamp=BASE, price=80.0, body_extreme=82.0, kind="LOW")
    book._try_enter(1, 100.0, level_high, level_low)
    await asyncio.sleep(0.01)
    assert book._position is not None
    assert book._position["t2_spot"] == 120.0   # the ABOVE level, not level_low


def test_try_enter_skips_when_already_in_position():
    book = _make_book()
    book._position = {"side": "CE", "strike": 100.0, "_closing": False}
    book._sl_anchor = 90.0
    book._live_ltp[(100.0, "PE")] = 10.0
    book._try_enter(-1, 100.0, None, None)
    assert book._bus.published == []


def test_try_enter_noop_while_warming_up():
    """A retest found during intraday warmup replay (2026-08-21 fix) must
    never fire a live order off an hours-stale price."""
    book = _make_book()
    book._warming_up = True
    book._sl_anchor = 90.0
    book._live_ltp[(100.0, "CE")] = 25.0
    book._try_enter(1, 100.0, None, None)
    assert book._position is None
    assert book._bus.published == []


# ── exit checks ───────────────────────────────────────────────────────────

def _open_position(book, direction=1, entry_price=20.0, entry_spot=100.0, sl_spot=90.0, t1_spot=115.0, t2_spot=130.0):
    book._position = dict(
        side="CE" if direction == 1 else "PE", direction=direction, strike=100.0,
        entry_price=entry_price, entry_spot=entry_spot, sl_spot=sl_spot, t1_spot=t1_spot,
        t2_spot=t2_spot, t1_hit=False, entry_ts=BASE, qty=75, _event_id="test_evt",
    )
    book._live_ltp[(100.0, book._position["side"])] = entry_price
    return book._position


def _stub_exit(book):
    """Same pattern tests/oi_flow/test_engine.py uses -- stubs book._exit
    with a plain synchronous callable so _check_exit_on_spot can be tested
    without needing a running event loop (the real _exit() schedules an
    asyncio.create_task for the actual order round-trip, which is its own
    concern tested separately via _try_enter/_square_off)."""
    exited = {}
    book._exit = lambda reason, exit_price: exited.update(reason=reason, exit_price=exit_price)
    return exited


def test_check_exit_sl_hit_bullish():
    book = _make_book()
    pos = _open_position(book, direction=1, entry_price=20.0, entry_spot=100.0, sl_spot=90.0, t1_spot=115.0)
    exited = _stub_exit(book)
    book._live_ltp[(100.0, "CE")] = 15.0
    book._check_exit_on_spot(89.0)   # spot fell through SL
    assert exited.get("reason") == "sl_hit"
    assert exited.get("exit_price") == 15.0


def test_check_exit_target1_arms_breakeven_not_exit():
    book = _make_book()
    pos = _open_position(book, direction=1, entry_price=20.0, entry_spot=100.0, sl_spot=90.0, t1_spot=115.0, t2_spot=140.0)
    exited = _stub_exit(book)
    book._live_ltp[(100.0, "CE")] = 28.0
    book._check_exit_on_spot(116.0)   # spot reached T1
    assert pos["t1_hit"] is True
    assert exited == {}   # T1 arms breakeven, does not exit


def test_check_exit_breakeven_stop_after_t1():
    book = _make_book()
    pos = _open_position(book, direction=1, entry_price=20.0, entry_spot=100.0, sl_spot=90.0, t1_spot=115.0, t2_spot=140.0)
    pos["t1_hit"] = True
    exited = _stub_exit(book)
    book._live_ltp[(100.0, "CE")] = 22.0
    book._check_exit_on_spot(99.0)   # spot fell back through entry (breakeven), after T1 already hit
    assert exited.get("reason") == "breakeven_stop"


def test_check_exit_target2_hit():
    book = _make_book()
    pos = _open_position(book, direction=1, entry_price=20.0, entry_spot=100.0, sl_spot=90.0, t1_spot=115.0, t2_spot=140.0)
    exited = _stub_exit(book)
    book._live_ltp[(100.0, "CE")] = 40.0
    book._check_exit_on_spot(141.0)
    assert exited.get("reason") == "target2_hit"


def test_check_exit_hard_risk_cap_backstop():
    """Independent option-premium safety net -- fires even if spot hasn't
    hit its own SL yet, e.g. an IV crush blowing out the premium."""
    book = _make_book()
    pos = _open_position(book, direction=1, entry_price=20.0, entry_spot=100.0, sl_spot=50.0, t1_spot=200.0, t2_spot=300.0)
    exited = _stub_exit(book)
    risk_floor = 20.0 - (book._hard_risk_rs_per_lot / (book._lot_size * book._lot_multiplier))
    book._live_ltp[(100.0, "CE")] = risk_floor - 1.0
    book._check_exit_on_spot(99.0)   # spot hasn't moved much -- only the premium backstop should fire
    assert "hard_risk_cap" in exited.get("reason", "")


def test_check_exit_bearish_mirrors_bullish():
    book = _make_book()
    pos = _open_position(book, direction=-1, entry_price=15.0, entry_spot=100.0, sl_spot=110.0, t1_spot=85.0, t2_spot=60.0)
    exited = _stub_exit(book)
    book._live_ltp[(100.0, "PE")] = 22.0
    book._check_exit_on_spot(84.0)   # spot fell to T1 for a bearish trade
    assert pos["t1_hit"] is True
    assert exited == {}

    pos["t1_hit"] = False
    book._check_exit_on_spot(111.0)   # spot rose through SL for a bearish trade
    assert exited.get("reason") == "sl_hit"


# ── dedicated per-underlying log file ────────────────────────────────────

def test_make_strategy_logger_gives_a_distinct_file_per_underlying(tmp_path, monkeypatch):
    import utils.logging_utils as logging_utils_module
    _real = logging_utils_module.make_strategy_logger
    monkeypatch.setattr(
        logging_utils_module, "make_strategy_logger",
        lambda stem, **kw: _real(stem, log_dir=str(tmp_path), propagate=kw.get("propagate", False)),
    )
    from strategies.liquidity_sweep.engine import _make_strategy_logger
    lg_nifty = _make_strategy_logger("NIFTY", "ssrajpal2001", "SA5770")
    lg_sensex = _make_strategy_logger("SENSEX", "ssrajpal2001", "SA5770")
    assert lg_nifty is not lg_sensex
    assert lg_nifty.name != lg_sensex.name
    import os
    files = os.listdir(tmp_path)
    assert any("NIFTY" in f for f in files)
    assert any("SENSEX" in f for f in files)


# ── reset_session ─────────────────────────────────────────────────────────

def test_reset_session_wipes_pipeline_state():
    book = _make_book()
    book._pending_dir = -1
    book._fvg_dir = 1
    book._fvg_lo = 10.0
    book._day_done = True
    book._cooldown_until = datetime.now(IST)
    book.reset_session()
    assert book._pending_dir == 0
    assert book._fvg_dir == 0
    assert book._fvg_lo is None
    assert book._day_done is False
    assert book._cooldown_until is None


# ── mid-day intraday warmup (2026-08-21) ─────────────────────────────────────
# Confirmed gap: unlike D1Trap/FVG/Liquidity Trap, this strategy had NO
# REST-based warmup at all -- a mid-day restart rebuilt its entire sweep/
# structure/FVG pipeline from live ticks alone, which could take hours.

def test_warmup_intraday_skips_gracefully_without_feeder_token(monkeypatch):
    import strategies.liquidity_sweep.engine as eng_mod

    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 21, 10, 0, tzinfo=IST)

    monkeypatch.setattr(eng_mod, "datetime", _FakeDT)

    async def run():
        book = _make_book()
        book._feeder_token = ""
        await book._warmup_intraday()          # must not raise
        assert book._pending_dir == 0
        assert book._warming_up is False
    asyncio.run(run())


def test_warmup_intraday_skips_before_market_open(monkeypatch):
    import strategies.liquidity_sweep.engine as eng_mod

    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 21, 8, 30, tzinfo=IST)   # pre-market

    monkeypatch.setattr(eng_mod, "datetime", _FakeDT)

    async def run():
        book = _make_book()
        book._feeder_token = "FAKE_TOKEN"
        await book._warmup_intraday()
        assert len(book._ltf_acc.bars) == 0   # nothing replayed
    asyncio.run(run())


def test_warmup_intraday_replays_history_and_sets_today(monkeypatch):
    import strategies.liquidity_sweep.engine as eng_mod
    import data_layer.historical_candles as hc_mod

    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 21, 10, 0, tzinfo=IST)

    async def _fake_fetch(key, token):
        return [
            {"ts": "2026-08-21T09:15:00+05:30", "open": 100, "high": 105, "low": 95, "close": 102},
            {"ts": "2026-08-21T09:20:00+05:30", "open": 102, "high": 108, "low": 101, "close": 106},
            {"ts": "2026-08-21T09:25:00+05:30", "open": 106, "high": 107, "low": 105, "close": 106},
            {"ts": "2026-08-21T09:30:00+05:30", "open": 106, "high": 109, "low": 104, "close": 108},
            {"ts": "2026-08-21T09:35:00+05:30", "open": 108, "high": 110, "low": 106, "close": 109},
            {"ts": "2026-08-21T09:40:00+05:30", "open": 109, "high": 111, "low": 107, "close": 110},
        ]

    monkeypatch.setattr(eng_mod, "datetime", _FakeDT)
    monkeypatch.setattr(hc_mod, "fetch_upstox_intraday_1m", _fake_fetch)

    async def run():
        book = _make_book(ltf_min=5)
        book._ltf_acc = BarAccumulator(timeframe_min=5)
        book._feeder_token = "FAKE_TOKEN"
        await book._warmup_intraday()
        # 6 raw 5-min-spaced source bars -> 6 closed ltf bars fed in as
        # synthetic o/h/l/c ticks; BarAccumulator only closes a bucket on the
        # NEXT bucket's first tick, so 6 source bars yield 5 CLOSED ltf bars.
        assert len(book._ltf_acc.bars) == 5
        assert book._warming_up is False        # reset after replay finishes
        assert book._bus.published == []        # state catch-up only, never a live order
        # Regression: _today must be set to today's real date by warmup itself,
        # same critical fix already applied to strategies/liquidity_trap/
        # engine.py -- otherwise the first live tick's own new-day check
        # would silently reset_session() and wipe everything just replayed.
        from datetime import date
        assert book._today == date(2026, 8, 21)
    asyncio.run(run())


def test_warmup_intraday_never_fires_a_live_order_even_if_a_retest_completes(monkeypatch):
    """If the full sweep->displacement->FVG->retest sequence completes
    entirely within the replayed history, _try_enter's own _warming_up guard
    must block it -- no stale-price live order."""
    import strategies.liquidity_sweep.engine as eng_mod
    import data_layer.historical_candles as hc_mod

    class _FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 21, 12, 0, tzinfo=IST)

    async def _fake_fetch(key, token):
        # Deliberately volatile synthetic sequence -- doesn't need to produce a
        # real signal, just proves that IF one fires during replay, no order
        # is published (the guard is the thing under test, not the detector).
        base = datetime(2026, 8, 21, 9, 15, tzinfo=IST)
        rows = []
        price = 100.0
        for i in range(40):
            ts = (base + timedelta(minutes=5 * i)).isoformat()
            o = price
            h = price + 3
            l = price - 6 if i % 7 == 0 else price - 1
            c = price + (2 if i % 2 == 0 else -1)
            rows.append({"ts": ts, "open": o, "high": h, "low": l, "close": c})
            price = c
        return rows

    monkeypatch.setattr(eng_mod, "datetime", _FakeDT)
    monkeypatch.setattr(hc_mod, "fetch_upstox_intraday_1m", _fake_fetch)

    async def run():
        book = _make_book(ltf_min=5, liq_source="liquidity_pool")
        book._ltf_acc = BarAccumulator(timeframe_min=5)
        book._feeder_token = "FAKE_TOKEN"
        await book._warmup_intraday()
        assert book._position is None
        assert book._bus.published == []
    asyncio.run(run())
