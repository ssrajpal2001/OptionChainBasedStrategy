"""
2026-08-24: integration-style smoke test for
strategies/oi_orb_screener/engine.py (OiOrbScreenerStrategy) -- drives the
real class (not a standalone reimplementation, per this repo's own
feedback_backtest_drive_real_class discipline) through signal -> contract
resolution -> live-LTP wait -> order emitted -> simulated fill -> position
tracked -> EOD close, using a fake bus with real asyncio.Queue-backed
subscribe/publish so the book's own running tasks (option-tick loop, fill
loop) do the actual work.

Confirms multiple concurrent stock positions are tracked independently
(per direct user instruction 2026-08-24: one position per shortlisted
stock allowed, not capped to 1).
"""
import asyncio
import time as _time
from datetime import date, datetime, time as dtime, timedelta

import pytest

from config.global_config import IST, Topic
from data_layer.base_feeder import IndexTick, OptionTick
from strategies.oi_orb_screener import screener, stock_resolve, store
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy
from strategies.oi_orb_screener.events import OiOrbFillEvent


@pytest.fixture(autouse=True)
def _isolated_store_db(tmp_path, monkeypatch):
    """Every test in this file that touches the book's real logic (fills,
    signal evaluation, the daily pipeline) now also writes to
    strategies/oi_orb_screener/store.py -- point it at an isolated tmp_path
    file so the whole suite never touches the real data/oi_orb_screener.db,
    matching the existing _TEST_CLIENT_ID/_TEST_BINDING_ID discipline this
    file already uses for logs (see the comment above those constants)."""
    monkeypatch.setattr(store, "_DB_PATH", str(tmp_path / "oi_orb_test.db"))
    monkeypatch.setattr(store, "_initialized", False)
    yield


class _FakeGlobalFeeder:
    def __init__(self) -> None:
        self.subscribed_tokens: list = []
        self.subscribed_equity: list = []   # [(fyers_sym, underlying), ...]

    async def subscribe_tokens(self, tokens):
        self.subscribed_tokens.extend(tokens)

    def subscribe_fno_equity(self, fyers_sym, underlying):
        self.subscribed_equity.append((fyers_sym, underlying))


class _FakeBus:
    """Real asyncio.Queue per topic so the book's own running loop tasks
    (started via book.start()) actually consume what tests publish."""

    def __init__(self) -> None:
        self._queues: dict = {}
        self.published: list = []
        self._global_feeder = _FakeGlobalFeeder()

    def subscribe(self, topic):
        q = self._queues.setdefault(topic, asyncio.Queue())
        return q

    def unsubscribe(self, topic, q):
        pass

    async def publish(self, topic, event):
        self.published.append((topic, event))
        q = self._queues.get(topic)
        if q is not None:
            await q.put(event)


# 2026-08-24 CRITICAL fix: this used to construct real OiOrbScreenerStrategy
# instances with client_id="ssrajpal2001"/binding_id="SA5770" -- the real
# production client/binding. Since the constructor opens a REAL rotating log
# file at logs/clients/oiorb_{client}_{binding}_{date}.log (same path a live
# run writes to), running this test suite wrote fabricated per-test fixture
# data (fake fills, fake EOD/kill_switch closes) straight into the real log
# the user was watching for genuine live signals -- confusing and easily
# mistaken for real activity. Never reuse a real client_id/binding_id in a
# test fixture that touches make_strategy_logger (or anything else that
# opens a real file keyed by those IDs).
_TEST_CLIENT_ID = "TESTCLIENT"
_TEST_BINDING_ID = "TESTBINDING"


def _make_book(bus) -> OiOrbScreenerStrategy:
    return OiOrbScreenerStrategy(
        bus, cfg=None, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID,
        lot_multiplier=1, product_type="MIS", squareoff_time="15:15",
    )


def _contract(symbol: str, strike: int, opt_type: str) -> "stock_resolve.ResolvedContract":
    return stock_resolve.ResolvedContract(
        underlying=symbol, expiry=date(2026, 8, 27), strike=strike, option_type=opt_type,
        upstox_key=f"NSE_FO|{symbol}{strike}{opt_type}",
        broker_symbols={"zerodha": f"{symbol}25AUG{strike}{opt_type}"},
    )


@pytest.mark.asyncio
async def test_handle_signal_resolves_contract_subscribes_feed_and_emits_buy(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    # Only the option-tick loop is needed for this test -- NOT book.start()'s
    # full task set (_daily_loop would try real NSE calls off the wall clock,
    # which this test must never risk triggering).
    book._subscribe(Topic.OPTION_TICK)
    opt_tick_task = asyncio.create_task(book._option_tick_loop())
    try:
        contract = _contract("MANAPPURAM", 365, "CE")
        monkeypatch.setattr(stock_resolve, "resolve_lot_async", _async_return(6900))
        monkeypatch.setattr(stock_resolve, "resolve_contract_async", _async_return(contract))

        sig = screener.Signal(symbol="MANAPPURAM", side="CALL", reason="orb_high_breakout",
                               trigger_price=365.25, orb_high=362.30, orb_low=358.95, ts="10:10:58")

        book._running = True
        task = asyncio.create_task(book._handle_signal(sig))
        await asyncio.sleep(0.05)   # let _ensure_option_feed's subscribe task run
        # Push a live option tick matching the resolved contract -- the running
        # _option_tick_loop picks this up and populates _live_option_ltp.
        await bus.publish(Topic.OPTION_TICK, OptionTick(
            symbol="MANAPPURAM365CE", underlying="MANAPPURAM", strike=365, option_type="CE",
            expiry=date(2026, 8, 27), ltp=8.65, bid=8.5, ask=8.8, oi=0, change_oi=0,
            volume=0, iv=0.0, delta=0.0, timestamp=datetime(2026, 8, 24, 10, 10, 58),
        ))
        await asyncio.wait_for(task, timeout=2.0)

        assert "NSE_FO|MANAPPURAM365CE" in bus._global_feeder.subscribed_tokens
        buy_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST]
        assert len(buy_events) == 1
        assert buy_events[0].action == "BUY"
        assert buy_events[0].underlying == "MANAPPURAM"
        assert buy_events[0].entry_price == 8.65
        assert buy_events[0].quantity == 6900
    finally:
        book._running = False
        opt_tick_task.cancel()
        try:
            await opt_tick_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_handle_signal_on_different_strike_does_not_reuse_stale_ltp(monkeypatch):
    """2026-09-10, real incident: a re-entry onto a DIFFERENT strike than the
    just-closed contract used to read the OLD contract's leftover LTP as its
    own entry price (_live_option_ltp is keyed by stock symbol only, and
    nothing cleared it between positions -- _await_first_ltp's "> 0" check
    can't tell a stale leftover from a genuine fresh tick). Real trade:
    ATHERENERG closed CE1640 @ 54.50, re-entered CE1620, and the recorded
    entry was 54.50 -- CE1640's price, not CE1620's real ~64.95."""
    bus = _FakeBus()
    book = _make_book(bus)
    # Simulate the just-closed contract's leftover LTP still sitting in the dict.
    book._live_option_ltp["ATHERENERG"] = 54.50
    book._live_option_atp["ATHERENERG"] = 54.50

    book._subscribe(Topic.OPTION_TICK)
    opt_tick_task = asyncio.create_task(book._option_tick_loop())
    try:
        new_contract = _contract("ATHERENERG", 1620, "CE")
        monkeypatch.setattr(stock_resolve, "resolve_lot_async", _async_return(375))
        monkeypatch.setattr(stock_resolve, "resolve_contract_async", _async_return(new_contract))

        sig = screener.Signal(symbol="ATHERENERG", side="CALL", reason="vwap_retest_historical_top20",
                               trigger_price=1630.60, orb_high=1638.10, orb_low=1629.00, ts="09:33:00")

        book._running = True
        task = asyncio.create_task(book._handle_signal(sig))
        await asyncio.sleep(0.05)   # let _ensure_option_feed's subscribe task run

        # The stale value must be gone the instant the new contract is set up --
        # BEFORE any genuine new tick has arrived.
        assert book._live_option_ltp.get("ATHERENERG") is None

        # Now the genuinely fresh tick for the NEW contract arrives.
        await bus.publish(Topic.OPTION_TICK, OptionTick(
            symbol="ATHERENERG1620CE", underlying="ATHERENERG", strike=1620, option_type="CE",
            expiry=date(2026, 8, 27), ltp=64.95, bid=64.8, ask=65.1, oi=0, change_oi=0,
            volume=0, iv=0.0, delta=0.0, timestamp=datetime(2026, 9, 10, 9, 33, 0),
        ))
        await asyncio.wait_for(task, timeout=2.0)

        buy_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST]
        assert len(buy_events) == 1
        assert buy_events[0].entry_price == 64.95   # NOT the stale 54.50 leftover
    finally:
        book._running = False
        opt_tick_task.cancel()
        try:
            await opt_tick_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_on_fill_confirms_entry_and_tracks_position():
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("SIEMENS", 4050, "CE")
    book._pending_contracts["SIEMENS"] = contract
    book._pending_fills["EVT1"] = {
        "symbol": "SIEMENS", "contract": contract, "qty": 300, "entry_price": 105.0, "reason": "orb_high_breakout",
    }

    await book._on_fill(OiOrbFillEvent(
        action="BUY", underlying="SIEMENS", option_type="CE", strike=4050, fill_price=106.2,
        qty=300, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id="EVT1", paper_mode=True,
    ))

    assert "SIEMENS" in book._positions
    assert book._positions["SIEMENS"]["entry_price"] == 106.2
    assert "SIEMENS" not in book._pending_contracts
    assert "EVT1" not in book._pending_fills


@pytest.mark.asyncio
async def test_on_fill_new_entry_tagged_trap_mechanic():
    """2026-08-31, direct user spec: every NEW entry (going forward) uses
    the trap+TSL mechanic, not the old VWAP-retest/VWAP-SL one."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("SIEMENS", 4050, "CE")
    book._pending_contracts["SIEMENS"] = contract
    book._pending_fills["EVT1"] = {
        "symbol": "SIEMENS", "contract": contract, "qty": 300, "entry_price": 105.0, "reason": "trap_retest",
    }
    await book._on_fill(OiOrbFillEvent(
        action="BUY", underlying="SIEMENS", option_type="CE", strike=4050, fill_price=106.2,
        qty=300, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id="EVT1", paper_mode=True,
    ))
    assert book._positions["SIEMENS"]["sl_mechanic"] == "trap"


@pytest.mark.asyncio
async def test_restored_position_tagged_vwap_mechanic_not_retroactively_switched(monkeypatch):
    """A position restored from the DB predates the trap+TSL mechanic --
    must stay on the old option-premium-VWAP SL it was actually entered
    under, never silently switched to the new risk model."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._today = date(2026, 8, 31)
    contract = _contract("PERSISTENT", 5500, "PE")
    monkeypatch.setattr(store, "load_open_positions", lambda cid, bid, td: [
        {"symbol": "PERSISTENT", "expiry": "2026-09-29", "strike": 5500, "option_type": "PE",
         "qty": 125, "entry_price": 114.90, "paper_mode": 1, "entry_ts": "2026-08-31T11:26:30"},
    ])
    monkeypatch.setattr(store, "load_already_fired", lambda cid, bid, trade_date=None: set())
    monkeypatch.setattr(store, "load_rejected", lambda cid, bid, trade_date=None: set())
    monkeypatch.setattr(stock_resolve, "resolve_contract_exact_async", _async_return(contract))
    monkeypatch.setattr(book, "_ensure_option_feed", lambda *a, **k: None)
    monkeypatch.setattr(book, "_ensure_spot_feed", lambda *a, **k: None)
    monkeypatch.setattr(book, "_seed_option_bars_from_history", _async_return(None))

    await book._restore_from_db()

    assert book._positions["PERSISTENT"]["sl_mechanic"] == "vwap"


@pytest.mark.asyncio
async def test_update_option_sl_target_skips_trap_tagged_positions():
    """The old option-premium-VWAP SL path must never run for a
    trap-tagged position -- that mechanic's exit is _trap_update_tsl_and_
    check_exit, driven by the poll loop, not option ticks."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._positions["SIEMENS"] = {
        "contract": _contract("SIEMENS", 4050, "CE"), "qty": 300, "entry_price": 106.2,
        "paper_mode": True, "opened_at": datetime.now(IST), "sl_mechanic": "trap",
    }
    await book._update_option_sl_target_and_check("SIEMENS", 110.0, datetime.now(IST))
    assert "SIEMENS" not in book._option_sl_bar_key   # old-mechanic bar tracking never touched


def test_trap_check_entry_returns_false_before_any_zone_forms():
    bus = _FakeBus()
    book = _make_book(bus)
    now = datetime(2026, 8, 31, 9, 15, tzinfo=IST)
    fired = book._trap_check_entry("SIEMENS", "CALL", 100.0, now)
    assert fired is False
    assert "SIEMENS" not in book._trap_entry_calc


@pytest.mark.asyncio
async def test_on_fill_entry_aborted_discards_pending():
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("SIEMENS", 4050, "CE")
    book._pending_contracts["SIEMENS"] = contract
    book._pending_fills["EVT1"] = {
        "symbol": "SIEMENS", "contract": contract, "qty": 300, "entry_price": 105.0, "reason": "orb_high_breakout",
    }

    await book._on_fill(OiOrbFillEvent(
        action="BUY", underlying="SIEMENS", option_type="CE", strike=4050, fill_price=0.0,
        qty=300, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id="EVT1",
        entry_aborted=True,
    ))

    assert "SIEMENS" not in book._positions
    assert "SIEMENS" not in book._pending_contracts


@pytest.mark.asyncio
async def test_multiple_concurrent_positions_tracked_independently():
    """Per direct user instruction 2026-08-24: one position PER shortlisted
    stock, not capped to 1 -- two different stocks filling must both end
    up tracked, independently, in the same book."""
    bus = _FakeBus()
    book = _make_book(bus)

    for i, (sym, strike) in enumerate([("MANAPPURAM", 365), ("SIEMENS", 4050)]):
        contract = _contract(sym, strike, "CE")
        eid = f"EVT{i}"
        book._pending_contracts[sym] = contract
        book._pending_fills[eid] = {"symbol": sym, "contract": contract, "qty": 100 * (i + 1),
                                     "entry_price": 10.0 + i, "reason": "orb_high_breakout"}
        await book._on_fill(OiOrbFillEvent(
            action="BUY", underlying=sym, option_type="CE", strike=strike, fill_price=10.0 + i,
            qty=100 * (i + 1), client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id=eid,
        ))

    assert set(book._positions.keys()) == {"MANAPPURAM", "SIEMENS"}
    assert book._positions["MANAPPURAM"]["qty"] == 100
    assert book._positions["SIEMENS"]["qty"] == 200


def test_monitoring_state_computes_pnl_and_pct_per_position():
    """2026-08-24, direct user request -- client-perspective UI review: the
    panel showed entry/LTP side by side with no P&L, forcing the client to
    do the math themselves. monitoring_state() now computes both ₹ P&L and
    %% P&L per position, plus opened_at for a "time held" display."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 24, 13, 0, 0),
    }
    book._live_option_ltp["MANAPPURAM"] = 12.5

    state = book.monitoring_state()
    pos = state["positions"]["MANAPPURAM"]
    assert pos["pnl"] == pytest.approx((12.5 - 10.0) * 100)
    assert pos["pnl_pct"] == pytest.approx((12.5 - 10.0) / 10.0 * 100.0)
    assert pos["opened_at"] == "2026-08-24T13:00:00"


def test_monitoring_state_pnl_is_none_without_a_live_ltp_yet():
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 24, 13, 0, 0),
    }
    # no self._live_option_ltp entry yet

    state = book.monitoring_state()
    pos = state["positions"]["MANAPPURAM"]
    assert pos["pnl"] is None
    assert pos["pnl_pct"] is None


@pytest.mark.asyncio
async def test_eod_loop_closes_open_positions_at_squareoff_time():
    bus = _FakeBus()
    book = _make_book(bus)
    book._squareoff_time = dtime(0, 0)   # always past squareoff -- fires immediately
    book._running = True
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 6900, "entry_price": 8.5, "paper_mode": True,
    }

    task = asyncio.create_task(book._eod_loop())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST]
    assert len(sell_events) == 1
    assert sell_events[0].action == "SELL"
    assert sell_events[0].underlying == "MANAPPURAM"
    assert sell_events[0].reason == "eod_squareoff"


@pytest.mark.asyncio
async def test_liquidate_closes_positions_immediately_for_kill_switch():
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 6900, "entry_price": 8.5, "paper_mode": True,
    }

    await book.liquidate("kill_switch")

    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST]
    assert len(sell_events) == 1
    assert sell_events[0].reason == "kill_switch"


def _async_return(value):
    async def _f(*a, **kw):
        return value
    return _f


class _FakeNSESession:
    """No-op stand-in for screener.NSESession -- real construction does
    real network warm-up GETs, which these tests must never trigger."""
    def __init__(self) -> None:
        pass


@pytest.mark.asyncio
async def test_run_today_pipeline_retries_build_shortlist_on_transient_failure(monkeypatch):
    """2026-08-24, confirmed live on EC2: a fresh NSESession's first request
    burst can hit a short-lived Akamai throttle that clears moments later.
    Without a retry, that single transient failure silently kills the whole
    trading day (the daily pipeline only runs once per calendar day).

    2026-09-01: build_shortlist's 3rd (successful) attempt returns an EMPTY
    shortlist -- since the empty-shortlist fix below now falls through into
    the real polling loop instead of returning, this test must stop that
    loop itself (real production paces it with a real asyncio.sleep between
    iterations; only the test's OWN asyncio.sleep mock made an unbounded
    loop unsafe here)."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._screener_cfg["IGNORE_TIME_WINDOWS"] = True
    book._running = True

    monkeypatch.setattr(screener, "NSESession", _FakeNSESession)
    monkeypatch.setattr(asyncio, "sleep", _async_return(None))

    calls = {"n": 0}
    monkeypatch.setattr(screener, "build_shortlist",
                         lambda nse, cfg: _flaky_build_shortlist_sync(nse, cfg, calls))

    def _stop_after_one_poll(nse):
        book._running = False   # exit the polling loop after this single iteration
        raise RuntimeError("stop the test here, no real network call")
    monkeypatch.setattr(screener, "fetch_fno_price_universe", _stop_after_one_poll)

    await book._run_today_pipeline()

    assert calls["n"] == 3   # 2 failures + 1 success, never hit the max-attempts cap
    assert book._shortlist_symbols == []   # the successful attempt's shortlist was empty


def _flaky_build_shortlist_sync(nse, cfg, calls):
    calls["n"] += 1
    if calls["n"] < 3:
        raise RuntimeError("transient NSE throttle")
    import pandas as pd
    return pd.DataFrame(), 0.0


@pytest.mark.asyncio
async def test_run_today_pipeline_gives_up_after_max_attempts(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._screener_cfg["IGNORE_TIME_WINDOWS"] = True
    book._running = True

    monkeypatch.setattr(screener, "NSESession", _FakeNSESession)
    monkeypatch.setattr(asyncio, "sleep", _async_return(None))

    calls = {"n": 0}
    def _always_fails(nse, cfg):
        calls["n"] += 1
        raise RuntimeError("persistent NSE block")
    monkeypatch.setattr(screener, "build_shortlist", _always_fails)

    await book._run_today_pipeline()

    from strategies.oi_orb_screener.engine import _BUILD_SHORTLIST_MAX_ATTEMPTS
    assert calls["n"] == _BUILD_SHORTLIST_MAX_ATTEMPTS
    assert book._shortlist_symbols == []


@pytest.mark.asyncio
async def test_empty_morning_shortlist_still_reaches_afternoon_scan(monkeypatch):
    """2026-09-01 CRITICAL FIX, real incident: an empty morning shortlist used
    to `return` out of _run_today_pipeline() entirely, so the polling loop
    below -- the ONLY place _maybe_run_afternoon_scan() is ever called -- was
    never reached. Since _daily_loop() only calls _run_today_pipeline() once
    per calendar day, two_session_scan_enabled could never actually rescue a
    day that started empty, which is exactly the scenario it exists for.

    Real incident: 2026-09-01, NIFTY flat (-0.18%) at 09:25 -> morning
    shortlist empty -> book went silent all day even though real candidates
    (HEROMOTOCO/BAJAJ-AUTO/ADANIENSOL/MARUTI/KALYANKJIL/POLYCAB/KEI) existed
    by 12:51 and two_session_scan_enabled was explicitly on.

    This test proves the fix: build_shortlist returns empty on the morning
    call, and the very next thing the polling loop does is call
    _maybe_run_afternoon_scan -- confirmed here by making THAT call itself
    the thing that stops the loop and recording that it happened.

    2026-09-02 fix: the polling loop's own hard-stop check (`datetime.now(IST)
    .time() < _hard_stop_time`, _hard_stop_time=15:20) used the REAL wall
    clock -- this test failed with zero calls whenever actually run after
    15:20 IST, nothing to do with any code regression. Pins engine.py's own
    `datetime.now()` to a fixed mid-day time so the test is deterministic
    regardless of when it's actually run."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._screener_cfg["IGNORE_TIME_WINDOWS"] = True
    book._running = True

    import strategies.oi_orb_screener.engine as _engine_mod
    from datetime import datetime as _dt, date as _date, time as _dtime
    _fixed_now = _dt(2026, 9, 2, 13, 0, 0, tzinfo=_engine_mod.IST)
    class _FixedDT(_dt):
        @classmethod
        def now(cls, tz=None):
            return _fixed_now
    monkeypatch.setattr(_engine_mod, "datetime", _FixedDT)

    monkeypatch.setattr(screener, "NSESession", _FakeNSESession)
    monkeypatch.setattr(asyncio, "sleep", _async_return(None))

    import pandas as pd
    monkeypatch.setattr(screener, "build_shortlist", lambda nse, cfg: (pd.DataFrame(), -0.18))

    afternoon_scan_calls = {"n": 0}
    _orig_maybe_run_afternoon_scan = book._maybe_run_afternoon_scan

    async def _spy_afternoon_scan(now, cfg):
        afternoon_scan_calls["n"] += 1
        book._running = False   # stop the loop right after the first real call
        return await _orig_maybe_run_afternoon_scan(now, cfg)
    book._maybe_run_afternoon_scan = _spy_afternoon_scan

    def _fetch_universe_noop(nse):
        return pd.DataFrame(columns=["symbol", "lastPrice", "pChange"])
    monkeypatch.setattr(screener, "fetch_fno_price_universe", _fetch_universe_noop)

    await book._run_today_pipeline()

    assert book._shortlist_symbols == []   # morning found nothing, as before
    assert afternoon_scan_calls["n"] >= 1, (
        "the empty-morning-shortlist path must still reach the polling loop "
        "so _maybe_run_afternoon_scan() gets a real chance to run"
    )


# ── Two-session scan (2026-08-27, direct user spec): session 1 is a single
# point-in-time scan at scan_start; session 2 re-scans between afternoon_
# scan_start/end, ADDING any newly-qualifying stock without dropping one
# already being watched. No scanning outside these two windows. ───────────

def _shortlist_df(rows):
    import pandas as pd
    return pd.DataFrame(rows)


def _ranked_df(rows):
    import pandas as pd
    return pd.DataFrame(rows)


@pytest.mark.asyncio
async def test_do_rank_poll_first_poll_only_records_no_drop_detection(monkeypatch):
    """First poll of the day has nothing to compare against -- must never
    drop anything, only seed self._rank_prev_top."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._nse = object()
    book._shortlist_symbols = ["AAA", "BBB"]
    book._shortlist_pchange = {"AAA": 3.0, "BBB": -2.5}
    monkeypatch.setattr(screener, "poll_oi_rank", lambda nse, cfg: _ranked_df([
        {"symbol": "AAA", "rank": 1, "oi_spurt_pct": 20.0, "pChange": 3.0},
        {"symbol": "BBB", "rank": 2, "oi_spurt_pct": 15.0, "pChange": -2.5},
    ]))

    now = datetime(2026, 8, 30, 9, 16, 0, tzinfo=IST)
    await book._do_rank_poll(now, book._screener_cfg)

    assert book._rank_prev_top == {"AAA", "BBB"}
    assert book._shortlist_symbols == ["AAA", "BBB"]   # untouched
    assert book._rejected == set()


@pytest.mark.asyncio
async def test_do_rank_poll_drops_not_yet_entered_symbol_that_falls_out_of_top_n(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._nse = object()
    book._shortlist_symbols = ["AAA", "BBB"]
    book._shortlist_pchange = {"AAA": 3.0, "BBB": -2.5}

    polls = [
        _ranked_df([
            {"symbol": "AAA", "rank": 1, "oi_spurt_pct": 20.0, "pChange": 3.0},
            {"symbol": "BBB", "rank": 2, "oi_spurt_pct": 15.0, "pChange": -2.5},
        ]),
        # BBB fell out of the ranked list entirely on this later poll.
        _ranked_df([
            {"symbol": "AAA", "rank": 1, "oi_spurt_pct": 22.0, "pChange": 3.2},
            {"symbol": "CCC", "rank": 2, "oi_spurt_pct": 14.0, "pChange": 2.1},
        ]),
    ]
    calls = {"n": 0}

    def _poll(nse, cfg):
        df = polls[calls["n"]]
        calls["n"] += 1
        return df
    monkeypatch.setattr(screener, "poll_oi_rank", _poll)

    now1 = datetime(2026, 8, 30, 9, 16, 0, tzinfo=IST)
    now2 = datetime(2026, 8, 30, 9, 18, 0, tzinfo=IST)
    await book._do_rank_poll(now1, book._screener_cfg)
    await book._do_rank_poll(now2, book._screener_cfg)

    side_bbb = screener.side_from_pchange(-2.5)
    assert "BBB" not in book._shortlist_symbols
    assert "AAA" in book._shortlist_symbols
    assert (side_bbb, ) != ()  # sanity -- side computed
    assert (("BBB", side_bbb) in book._rejected)
    assert "BBB" in book._rank_dropped


@pytest.mark.asyncio
async def test_do_rank_poll_never_drops_an_already_entered_symbol(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._nse = object()
    book._shortlist_symbols = ["AAA", "BBB"]
    book._shortlist_pchange = {"AAA": 3.0, "BBB": -2.5}
    book._positions["BBB"] = {"contract": None, "qty": 1, "entry_price": 10.0,
                               "paper_mode": True, "opened_at": datetime.now(IST)}

    polls = [
        _ranked_df([
            {"symbol": "AAA", "rank": 1, "oi_spurt_pct": 20.0, "pChange": 3.0},
            {"symbol": "BBB", "rank": 2, "oi_spurt_pct": 15.0, "pChange": -2.5},
        ]),
        _ranked_df([
            {"symbol": "AAA", "rank": 1, "oi_spurt_pct": 22.0, "pChange": 3.2},
            {"symbol": "CCC", "rank": 2, "oi_spurt_pct": 14.0, "pChange": 2.1},
        ]),
    ]
    calls = {"n": 0}

    def _poll(nse, cfg):
        df = polls[calls["n"]]
        calls["n"] += 1
        return df
    monkeypatch.setattr(screener, "poll_oi_rank", _poll)

    now1 = datetime(2026, 8, 30, 9, 16, 0, tzinfo=IST)
    now2 = datetime(2026, 8, 30, 9, 18, 0, tzinfo=IST)
    await book._do_rank_poll(now1, book._screener_cfg)
    await book._do_rank_poll(now2, book._screener_cfg)

    assert "BBB" in book._shortlist_symbols, "an already-open position must never be dropped"
    assert "BBB" not in book._rank_dropped


# ── Full-day OI-spurt history capture (2026-09-07, direct user spec):
# a SEPARATE, purely-observational poll -- must never touch shortlist/
# rejected/positions state, unlike _do_rank_poll above. ───────────────────

@pytest.mark.asyncio
async def test_do_oi_spurt_history_poll_records_top_n_and_never_touches_trading_state(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._nse = object()
    book._shortlist_symbols = ["AAA", "BBB"]
    book._shortlist_pchange = {"AAA": 3.0, "BBB": -2.5}

    captured = {}

    def _poll(nse, cfg, top_n):
        captured["top_n"] = top_n
        return _ranked_df([
            {"symbol": "AAA", "rank": 1, "oi_spurt_pct": 20.0, "pChange": 3.0},
            {"symbol": "ZZZ", "rank": 2, "oi_spurt_pct": 9.73, "pChange": -2.43},
        ])
    monkeypatch.setattr(screener, "poll_oi_rank", _poll)

    recorded = {}

    def _record(client_id, binding_id, poll_ts, rows):
        recorded["client_id"] = client_id
        recorded["binding_id"] = binding_id
        recorded["rows"] = rows
    monkeypatch.setattr(store, "record_oi_spurt_history", _record)

    now = datetime(2026, 9, 7, 11, 8, 50, tzinfo=IST)
    await book._do_oi_spurt_history_poll(now, book._screener_cfg)

    assert captured["top_n"] == book._screener_cfg.get("OI_SPURT_HISTORY_TOP_N", 20)
    assert recorded["rows"] == [
        {"symbol": "AAA", "rank": 1, "oi_spurt_pct": 20.0, "price_change_pct": 3.0},
        {"symbol": "ZZZ", "rank": 2, "oi_spurt_pct": 9.73, "price_change_pct": -2.43},
    ]
    # ZZZ was never shortlisted and never becomes a candidate -- this poll
    # only logs, it must never mutate shortlist/rejected/positions.
    assert book._shortlist_symbols == ["AAA", "BBB"]
    assert book._rejected == set()
    assert book._positions == {}


@pytest.mark.asyncio
async def test_do_oi_spurt_history_poll_empty_ranked_df_is_a_noop(monkeypatch):
    """An empty ranked frame (e.g. NSE returned nothing) must not call
    store.record_oi_spurt_history at all."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._nse = object()
    monkeypatch.setattr(screener, "poll_oi_rank", lambda nse, cfg, top_n: _ranked_df([]))

    def _record_should_not_be_called(*a, **kw):
        raise AssertionError("record_oi_spurt_history must not be called for an empty poll")
    monkeypatch.setattr(store, "record_oi_spurt_history", _record_should_not_be_called)

    now = datetime(2026, 9, 7, 11, 8, 50, tzinfo=IST)
    await book._do_oi_spurt_history_poll(now, book._screener_cfg)


@pytest.mark.asyncio
async def test_afternoon_scan_noop_outside_window(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr(screener, "build_shortlist", lambda nse, cfg: (_ for _ in ()).throw(
        AssertionError("must not scan outside the afternoon window")))
    now = datetime(2026, 8, 27, 11, 0, 0, tzinfo=IST)   # before 12:00
    await book._maybe_run_afternoon_scan(now, book._screener_cfg)
    assert book._shortlist_symbols == []


@pytest.mark.asyncio
async def test_afternoon_scan_noop_when_disabled(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._screener_cfg["TWO_SESSION_SCAN_ENABLED"] = False
    monkeypatch.setattr(screener, "build_shortlist", lambda nse, cfg: (_ for _ in ()).throw(
        AssertionError("must not scan when two_session_scan_enabled is off")))
    now = datetime(2026, 8, 27, 12, 30, 0, tzinfo=IST)
    await book._maybe_run_afternoon_scan(now, book._screener_cfg)
    assert book._shortlist_symbols == []


@pytest.mark.asyncio
async def test_afternoon_scan_adds_new_symbols_without_dropping_existing(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_symbols = ["EXISTING"]
    book._shortlist_pchange = {"EXISTING": 3.0}
    book._regime = "bullish"   # already frozen at 09:25 -- must stay untouched

    df = _shortlist_df([
        {"symbol": "EXISTING", "pChange": 3.0, "oi_spurt_pct": 8.0, "score": 0.5,
         "lastPrice": 100.0, "previousClose": 97.0},
        {"symbol": "NEWSTOCK", "pChange": -2.5, "oi_spurt_pct": 9.0, "score": 0.6,
         "lastPrice": 50.0, "previousClose": 51.3},
    ])
    monkeypatch.setattr(screener, "build_shortlist", lambda nse, cfg: (df, 0.4))
    monkeypatch.setattr(screener, "backfill_orb_from_yahoo", lambda *a, **k: None)
    monkeypatch.setattr(screener, "backfill_vwap_from_yahoo", lambda *a, **k: None)
    monkeypatch.setattr(asyncio, "to_thread", lambda fn, *a, **k: _async_return(fn(*a, **k))())

    now = datetime(2026, 8, 27, 12, 30, 0, tzinfo=IST)
    await book._maybe_run_afternoon_scan(now, book._screener_cfg)

    assert sorted(book._shortlist_symbols) == ["EXISTING", "NEWSTOCK"]
    assert book._shortlist_pchange["NEWSTOCK"] == -2.5
    assert book._regime == "bullish"   # untouched -- afternoon reuses the frozen regime


@pytest.mark.asyncio
async def test_afternoon_scan_sets_orb_frozen_for_the_new_symbol(monkeypatch):
    """2026-09-04 CRITICAL FIX regression: an afternoon-added symbol's ORB
    level was never being set on self._orb_frozen (nor persisted), which
    silently, permanently blocked _immediate_check_entry() for it forever
    (that function hard-requires self._orb_frozen.get(sym) to be non-None).
    Trap-retest mode was unaffected. This seeds real 09:15-09:25 bars into
    self._bars (mirroring what backfill_orb_from_yahoo does for real) and
    confirms the fix actually computes and stores the level."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._regime = "bullish"

    df = _shortlist_df([
        {"symbol": "NEWSTOCK", "pChange": -2.5, "oi_spurt_pct": 9.0, "score": 0.6,
         "lastPrice": 50.0, "previousClose": 51.3},
    ])
    monkeypatch.setattr(screener, "build_shortlist", lambda nse, cfg: (df, 0.4))

    def _fake_backfill_orb(bars, symbols, cfg):
        for sym in symbols:
            bars.bars[sym]["09:16"] = {"o": 48.0, "h": 49.5, "l": 47.5, "c": 48.5}
            bars.bars[sym]["09:20"] = {"o": 48.5, "h": 50.0, "l": 48.0, "c": 49.0}
    monkeypatch.setattr(screener, "backfill_orb_from_yahoo", _fake_backfill_orb)
    monkeypatch.setattr(screener, "backfill_vwap_from_yahoo", lambda *a, **k: None)
    monkeypatch.setattr(asyncio, "to_thread", lambda fn, *a, **k: _async_return(fn(*a, **k))())

    now = datetime(2026, 8, 27, 12, 30, 0, tzinfo=IST)
    await book._maybe_run_afternoon_scan(now, book._screener_cfg)

    assert book._orb_frozen.get("NEWSTOCK") == (50.0, 47.5)

    import sqlite3
    con = sqlite3.connect(store._DB_PATH)
    row = con.execute(
        "SELECT orb_high, orb_low FROM shortlist WHERE client_id=? AND binding_id=? AND symbol='NEWSTOCK'",
        (_TEST_CLIENT_ID, _TEST_BINDING_ID),
    ).fetchone()
    con.close()
    assert row == (50.0, 47.5)


@pytest.mark.asyncio
async def test_afternoon_scan_throttled_by_interval(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    calls = {"n": 0}

    def _build(nse, cfg):
        calls["n"] += 1
        return _shortlist_df([]), 0.0
    monkeypatch.setattr(screener, "build_shortlist", _build)
    monkeypatch.setattr(asyncio, "to_thread", lambda fn, *a, **k: _async_return(fn(*a, **k))())

    now = datetime(2026, 8, 27, 12, 30, 0, tzinfo=IST)
    await book._maybe_run_afternoon_scan(now, book._screener_cfg)
    # Same instant again -- must be throttled, not re-scanned.
    await book._maybe_run_afternoon_scan(now, book._screener_cfg)
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_afternoon_scan_never_refetches_regime(monkeypatch):
    """Direct user spec: reuse the SAME regime frozen at 09:25 all day --
    the afternoon scan must never call fetch_nifty_pchange itself."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._regime = "bearish"
    df = _shortlist_df([{"symbol": "NEWSTOCK", "pChange": -3.0, "oi_spurt_pct": 8.0,
                          "score": 0.5, "lastPrice": 50.0, "previousClose": 51.5}])
    monkeypatch.setattr(screener, "build_shortlist", lambda nse, cfg: (df, 0.0))
    monkeypatch.setattr(screener, "fetch_nifty_pchange", lambda nse: (_ for _ in ()).throw(
        AssertionError("afternoon scan must not re-fetch NIFTY regime")))
    monkeypatch.setattr(asyncio, "to_thread", lambda fn, *a, **k: _async_return(fn(*a, **k))())

    now = datetime(2026, 8, 27, 12, 30, 0, tzinfo=IST)
    await book._maybe_run_afternoon_scan(now, book._screener_cfg)
    assert book._regime == "bearish"


@pytest.mark.asyncio
async def test_shortlist_pchange_exposed_via_monitoring_state(monkeypatch):
    """2026-08-24: the dashboard panel used to show every shortlisted stock
    as just "ORB pending" with zero directional signal, even though the
    shortlist itself already knows bullish vs bearish (that's how it got
    split in the first place). monitoring_state() must expose pChange per
    symbol so the UI can show CALL-bias vs PUT-bias before any ORB level
    exists."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._screener_cfg["IGNORE_TIME_WINDOWS"] = True
    book._running = True

    monkeypatch.setattr(screener, "NSESession", _FakeNSESession)
    monkeypatch.setattr(asyncio, "sleep", _async_return(None))
    monkeypatch.setattr(screener, "backfill_orb_from_yahoo", lambda bars, syms, cfg: None)

    import pandas as pd
    fake_shortlist = pd.DataFrame([
        {"symbol": "VMM", "pChange": 9.28, "previousClose": 103.43},
        {"symbol": "DIXON", "pChange": -2.04, "previousClose": 14700.0},
    ])
    monkeypatch.setattr(screener, "build_shortlist", lambda nse, cfg: (fake_shortlist, 0.11))

    def _stop_after_shortlist(nse):
        # Only care about the shortlist-building portion of the pipeline --
        # stop the book so the monitor while-loop exits on its next check
        # instead of spinning (asyncio.sleep is mocked to return instantly).
        book._running = False
        raise RuntimeError("stop test here")
    monkeypatch.setattr(screener, "fetch_fno_price_universe", _stop_after_shortlist)

    await book._run_today_pipeline()

    assert book._shortlist_pchange == {"VMM": 9.28, "DIXON": -2.04}
    state = book.monitoring_state()
    assert state["shortlist_pchange"] == {"VMM": 9.28, "DIXON": -2.04}
    assert set(state["shortlist"]) == {"VMM", "DIXON"}


# ── restore-on-restart (2026-08-24 real incident: DIXON PE14500 entered,
# then a pm2 restart at ~14:40 silently lost all memory it existed) ────────

@pytest.mark.asyncio
async def test_restore_from_db_reopens_position_and_resubscribes_feed(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._today = date(2026, 8, 24)

    store.open_position(_TEST_CLIENT_ID, _TEST_BINDING_ID, "DIXON", "PE", 14500,
                         "2026-08-25", 50, 118.80, "orb_low_breakdown", True, "EVT1",
                         trade_date="2026-08-24")

    contract = _contract("DIXON", 14500, "PE")
    monkeypatch.setattr(stock_resolve, "resolve_contract_exact_async", _async_return(contract))

    await book._restore_from_db()

    assert "DIXON" in book._positions
    assert book._positions["DIXON"]["qty"] == 50
    assert book._positions["DIXON"]["entry_price"] == 118.80
    assert book._positions["DIXON"]["paper_mode"] is True
    assert contract.upstox_key in bus._global_feeder.subscribed_tokens


@pytest.mark.asyncio
async def test_restore_from_db_seeds_vwap_for_reopened_position(monkeypatch):
    """2026-09-10, real incident: 3 positions restored after a restart
    (TECHM/LODHA/PNB) all showed blank 'Spot LTP vs VWAP' -- _restore_from_db
    subscribed feeds and seeded option-premium SL/target history, but never
    seeded VWAP at all, meaning the real spot-based VWAP-close SL had no
    protection until enough live ticks happened to build one from zero."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._today = date(2026, 8, 24)

    store.open_position(_TEST_CLIENT_ID, _TEST_BINDING_ID, "DIXON", "PE", 14500,
                         "2026-08-25", 50, 118.80, "orb_low_breakdown", True, "EVT1",
                         trade_date="2026-08-24")

    contract = _contract("DIXON", 14500, "PE")
    monkeypatch.setattr(stock_resolve, "resolve_contract_exact_async", _async_return(contract))
    monkeypatch.setattr(stock_resolve, "resolve_eq_instrument_key", lambda sym: "NSE_EQ|INE879I01012")
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_intraday_1m", _async_return([
        {"ts": "2026-08-24T09:15:00", "high": 14520, "low": 14480, "close": 14500, "volume": 1000},
        {"ts": "2026-08-24T09:16:00", "high": 14540, "low": 14500, "close": 14530, "volume": 500},
    ]))

    await book._restore_from_db()

    assert book._vwap.current("DIXON") is not None
    # Volume-weighted HLC3 blend of both real bars.
    hlc3_a = (14520 + 14480 + 14500) / 3.0
    hlc3_b = (14540 + 14500 + 14530) / 3.0
    expected = (hlc3_a * 1000 + hlc3_b * 500) / 1500
    assert book._vwap.current("DIXON") == pytest.approx(expected, abs=0.01)


# ── option history backfill on mid-day restart (2026-08-27, direct user spec:
# "it should get historical intraday data for that option chart from time
# entry happened and then evaluate the sl and target in tf which we have
# applied") ──────────────────────────────────────────────────────────────

def _bar(hhmm: str, h: float, l: float, c: float, vol: float) -> dict:
    return {"ts": f"2026-08-24T{hhmm}:00", "high": h, "low": l, "close": c, "volume": vol}


# ── side must come from the position's own contract, never re-derived from
# the stock's current pChange sign (2026-09-10 real incident: TECHM, a
# genuine CE/CALL position, got mislabeled side="PUT" once real price decline
# flipped its pChange negative after entry, silently running the WRONG exit
# mechanic -- wrong zone type, wrong S&R level, wrong breach direction -- for
# the rest of the session) ───────────────────────────────────────────────────

def test_side_from_option_type_maps_ce_pe_correctly():
    bus = _FakeBus()
    book = _make_book(bus)
    assert book._side_from_option_type("CE") == "CALL"
    assert book._side_from_option_type("PE") == "PUT"


def test_monitoring_state_side_uses_contract_not_flipped_pchange():
    """The exact field that showed side="PUT" for TECHM (a real CE position)
    in the live dashboard API response."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("TECHM", 1540, "CE")
    book._positions["TECHM"] = {
        "contract": contract, "qty": 600, "entry_price": 40.80, "paper_mode": True,
        "opened_at": datetime.now(IST), "sl_mechanic": "trap",
    }
    # pChange has flipped negative since entry (real TECHM price declined all
    # session) -- side_from_pchange would now wrongly say "PUT".
    book._shortlist_pchange["TECHM"] = -1.2

    state = book.monitoring_state()

    assert state["positions"]["TECHM"]["side"] == "CALL"


@pytest.mark.asyncio
async def test_restore_from_db_seeds_trap_exit_state_with_contract_side_not_pchange(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._today = date(2026, 9, 10)

    store.open_position(_TEST_CLIENT_ID, _TEST_BINDING_ID, "TECHM", "CE", 1540,
                         "2026-09-29", 600, 40.80, "vwap_retest_historical_top20", True, "EVT1",
                         trade_date="2026-09-10")
    monkeypatch.setattr(stock_resolve, "resolve_contract_exact_async",
                         _async_return(_contract("TECHM", 1540, "CE")))
    # Same flip as the real incident -- pChange has gone negative since entry.
    book._shortlist_pchange["TECHM"] = -1.2

    captured = []
    async def _spy_seed(sym, side, entry_ts):
        captured.append((sym, side))
    book._seed_trap_exit_state = _spy_seed

    await book._restore_from_db()
    await asyncio.sleep(0.05)   # let the create_task'd seed actually run

    assert ("TECHM", "CALL") in captured


@pytest.mark.asyncio
async def test_on_fill_seeds_trap_exit_state_with_contract_side_not_pchange(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("TECHM", 1540, "CE")
    eid = "evt_techm"
    book._pending_fills[eid] = {
        "symbol": "TECHM", "contract": contract, "qty": 600,
        "entry_price": 40.80, "reason": "vwap_retest_historical_top20",
    }
    # If pChange were read at this instant it would (in the real incident)
    # still be positive at entry -- but assert the fix reads the CONTRACT,
    # not pChange, regardless, by deliberately mismatching them here too.
    book._shortlist_pchange["TECHM"] = -1.2

    captured = []
    async def _spy_seed(sym, side, entry_ts):
        captured.append((sym, side))
    book._seed_trap_exit_state = _spy_seed

    fill = OiOrbFillEvent(
        client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id=eid,
        action="BUY", underlying="TECHM", option_type="CE", strike=1540,
        qty=600, fill_price=40.80, paper_mode=True,
    )
    await book._on_fill(fill)
    await asyncio.sleep(0.05)

    assert ("TECHM", "CALL") in captured


@pytest.mark.asyncio
async def test_exit_check_loop_uses_contract_side_not_flipped_pchange(monkeypatch):
    """2026-09-10 real incident, the PRIMARY site: _run_today_pipeline's main
    polling loop re-derived `side` for every open position from screener.
    side_from_pchange(self._shortlist_pchange[sym]) on EVERY cycle -- once a
    stock's pChange flipped sign after entry (exactly what happened to
    TECHM: entered CALL while pChange was positive, real price declined all
    session until pChange went negative), every subsequent exit-check call
    silently ran the WRONG side's mechanic. Proven here by driving the real
    loop body and spying on _vwap_close_sl_check to capture the side it was
    actually called with."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._screener_cfg["IGNORE_TIME_WINDOWS"] = True
    book._running = True
    book._shortlist_symbols = []

    contract = _contract("TECHM", 1540, "CE")
    book._positions["TECHM"] = {
        "contract": contract, "qty": 600, "entry_price": 40.80, "paper_mode": True,
        "opened_at": datetime.now(IST), "sl_mechanic": "trap",
    }
    book._shortlist_pchange["TECHM"] = -1.2   # flipped negative since entry, like the real incident

    import strategies.oi_orb_screener.engine as _engine_mod
    _fixed_now = datetime(2026, 9, 10, 13, 0, 0, tzinfo=_engine_mod.IST)
    class _FixedDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return _fixed_now
    monkeypatch.setattr(_engine_mod, "datetime", _FixedDT)

    monkeypatch.setattr(screener, "NSESession", _FakeNSESession)
    monkeypatch.setattr(asyncio, "sleep", _async_return(None))

    import pandas as pd
    monkeypatch.setattr(screener, "build_shortlist", lambda nse, cfg: (pd.DataFrame(), 0.0))
    monkeypatch.setattr(screener, "fetch_fno_price_universe",
                         lambda nse: pd.DataFrame(columns=["symbol", "lastPrice", "pChange"]))
    book._maybe_run_afternoon_scan = _async_return(None)
    book._live_price = lambda sym, live_df, log_source=False: 1517.50

    captured = []
    async def _spy_vwap_sl_check(sym, side, ltp, ts):
        captured.append(side)
        book._running = False   # stop the loop right after the first real call
    book._vwap_close_sl_check = _spy_vwap_sl_check

    await book._run_today_pipeline()

    assert captured == ["CALL"]


# ── _replay_vwap_close_sl restart-safety replay (2026-09-10 real incident:
# FORCEMOT closed at 13:10:04 citing "close=17826.00 vs vwap=17862.01" while
# real live spot was 18077-18078 the entire time, ~1.3% ABOVE the real
# contemporaneous VWAP -- nowhere near a genuine breach) ────────────────────

def _row(hhmm: str, h: float, l: float, c: float, vol: float) -> dict:
    # Real Upstox candle timestamps carry a tz offset (e.g. "...+05:30") --
    # matches that shape, unlike the naive _bar() helper above.
    hh, mm = hhmm.split(":")
    ts = datetime(2026, 8, 24, int(hh), int(mm), 0, tzinfo=IST).isoformat()
    return {"ts": ts, "high": h, "low": l, "close": c, "volume": vol}


def _rows_flat(start_hh: int, start_mm: int, n: int, price: float, vol: float) -> list:
    out = []
    t = start_hh * 60 + start_mm
    for _ in range(n):
        hh, mm = divmod(t, 60)
        out.append(_row(f"{hh:02d}:{mm:02d}", price, price, price, vol))
        t += 1
    return out


def _bars_from_rows(rows: list):
    from strategies.core.trap_zone_utils import Bar as _Bar
    return [_Bar(ts=datetime.fromisoformat(r["ts"]), open=r["close"], high=r["high"],
                 low=r["low"], close=r["close"]) for r in rows]


@pytest.mark.asyncio
async def test_replay_vwap_close_sl_does_not_fire_on_a_stale_bucket_judged_against_todays_final_vwap(monkeypatch):
    """2026-09-10 real incident fix: the replay used to compare EVERY
    historical 20-min bucket's close against self._vwap.current(sym) --
    today's single, final/current VWAP snapshot -- instead of the VWAP as
    it genuinely stood when that bucket itself closed. VWAP only grows/
    drifts as the day accumulates more volume, so a bucket from hours ago
    judged against today's LATER (and here, much higher) VWAP produces a
    false breach. Real incident: FORCEMOT was closed this way while live
    spot sat comfortably ~1.3% ABOVE the real contemporaneous VWAP the
    whole time.

    Bucket A [09:15-09:35) price=100 vol=10/min -> vwap-as-of-then=100.
    Bucket B [09:35-09:55) price=101 vol=10/min -> vwap-as-of-then=100.5
      (close 101 > vwap 100.5 -- healthy, NOT adverse).
    Bucket C [09:55-10:15) price=115 vol=1000/min (heavy volume) drags the
      CUMULATIVE/current vwap up to ~114.7 by end of day.
    book._vwap (the live "current" snapshot) is seeded to an even higher
    120 to simulate exactly what the old buggy code would have compared
    every earlier bucket against -- under the OLD code this would have
    fired on bucket A itself ((120-100)/120=16.7% adverse). The FIX must
    fire on NONE of these three buckets (all genuinely non-adverse at
    their own contemporaneous vwap)."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("FORCEMOT", 17500, "CE")
    entry_ts = datetime(2026, 8, 24, 9, 0, 0, tzinfo=IST)
    book._positions["FORCEMOT"] = {
        "contract": contract, "qty": 25, "entry_price": 845.70, "paper_mode": True,
        "opened_at": entry_ts, "sl_mechanic": "vwap",
    }
    book._vwap.replace("FORCEMOT", 120.0 * 1000.0, 1000.0)   # simulates a much higher "current" vwap

    rows = _rows_flat(9, 15, 20, 100.0, 10.0) + _rows_flat(9, 35, 20, 101.0, 10.0) + _rows_flat(9, 55, 20, 115.0, 1000.0)
    today_bars = _bars_from_rows(rows)

    import strategies.oi_orb_screener.engine as _engine_mod
    _fixed_now = datetime(2026, 8, 24, 10, 20, 0, tzinfo=_engine_mod.IST)
    class _FixedDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return _fixed_now
    monkeypatch.setattr(_engine_mod, "datetime", _FixedDT)

    fired = []
    async def _spy_fire(sym, side, ltp, vwap, candle_bar=None):
        fired.append((sym, side, ltp, vwap))
    book._fire_vwap_close_sl = _spy_fire

    closed = await book._replay_vwap_close_sl("FORCEMOT", "CALL", today_bars, entry_ts, rows)

    assert closed is False
    assert fired == []


@pytest.mark.asyncio
async def test_replay_vwap_close_sl_still_fires_on_a_genuine_contemporaneous_breach(monkeypatch):
    """Continuing the same series past the heavy-volume bucket C: bucket D
    [10:15-10:35) drops hard to close=90 while the running vwap-as-of-then
    is still ~114.5 (dominated by bucket C's own heavy volume) -- a
    genuine, large contemporaneous breach that must still fire, proving
    the fix doesn't just blanket-suppress every historical bucket."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("FORCEMOT", 17500, "CE")
    entry_ts = datetime(2026, 8, 24, 9, 0, 0, tzinfo=IST)
    book._positions["FORCEMOT"] = {
        "contract": contract, "qty": 25, "entry_price": 845.70, "paper_mode": True,
        "opened_at": entry_ts, "sl_mechanic": "vwap",
    }
    book._vwap.replace("FORCEMOT", 120.0 * 1000.0, 1000.0)

    rows = (_rows_flat(9, 15, 20, 100.0, 10.0) + _rows_flat(9, 35, 20, 101.0, 10.0)
            + _rows_flat(9, 55, 20, 115.0, 1000.0) + _rows_flat(10, 15, 20, 90.0, 10.0))
    today_bars = _bars_from_rows(rows)

    import strategies.oi_orb_screener.engine as _engine_mod
    _fixed_now = datetime(2026, 8, 24, 10, 40, 0, tzinfo=_engine_mod.IST)
    class _FixedDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return _fixed_now
    monkeypatch.setattr(_engine_mod, "datetime", _FixedDT)

    fired = []
    async def _spy_fire(sym, side, ltp, vwap, candle_bar=None):
        fired.append((sym, side, ltp, vwap))
    book._fire_vwap_close_sl = _spy_fire

    closed = await book._replay_vwap_close_sl("FORCEMOT", "CALL", today_bars, entry_ts, rows)

    assert closed is True
    assert len(fired) == 1
    sym, side, ltp, vwap = fired[0]
    assert ltp == pytest.approx(90.0)                 # bucket D's own close, not an earlier bucket
    assert vwap == pytest.approx(114.476, abs=0.01)    # vwap AS OF bucket D's own close, not 120


@pytest.mark.asyncio
async def test_vwap_close_sl_check_skips_a_bucket_that_closed_before_this_entry(monkeypatch):
    """2026-09-11 real incident fix: every fresh entry (including a same-day
    re-entry) resets the SL accumulator then seeds it in the background,
    via _replay_vwap_close_sl, with TODAY'S FULL-DAY bars -- not just
    post-entry ones. The live per-tick check (_vwap_close_sl_check) had no
    guard against evaluating a bucket that closed BEFORE this position's own
    entry_ts, so a re-entry (or any entry landing mid-bucket) could have its
    very first live tick see the most-recently-closed bucket as "latest" --
    one that closed before the position even existed -- and fire an SL off
    it using TODAY'S CURRENT vwap, seconds after entry, with zero real
    post-entry price action involved. Real incidents this session: DIXON/
    ABCAPITAL/HDFCAMC/PRESTIGE/LODHA re-entries and a PIIND historical-fire
    entry all closed 3-20 seconds after opening.

    Bucket [09:15-09:35) closes at 09:35 with close=90 -- genuinely adverse
    for a CALL vs a seeded vwap of 120 ((120-90)/120=25%). Position enters
    AFTER that bucket already closed, at 09:36 -- the very next live tick
    (09:36:20) must NOT fire off that stale, pre-entry bucket."""
    from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc

    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("DIXON", 13250, "PE")
    # Same date _row()/_rows_flat() hardcode (2026-08-24) -- entry lands
    # AFTER the bucket below already closed.
    entry_ts = datetime(2026, 8, 24, 9, 36, 0, tzinfo=IST)
    book._positions["DIXON"] = {
        "contract": contract, "qty": 50, "entry_price": 459.05, "paper_mode": True,
        "opened_at": entry_ts, "sl_mechanic": "vwap",
    }
    book._vwap.seed("DIXON", 120.0 * 10.0, 10.0)   # today's CURRENT vwap snapshot = 120

    # Seed the accumulator exactly like _replay_vwap_close_sl does on a fresh
    # entry: the WHOLE day's bars, including one closed bucket that predates
    # this position's own entry_ts.
    rows = _rows_flat(9, 15, 20, 90.0, 10.0)   # [09:15-09:35) closes 09:35, close=90
    acc = _TrapAcc(timeframe_min=1)
    acc.bars = _bars_from_rows(rows)
    book._sl_vwap_1m_acc["DIXON"] = acc

    fired = []
    async def _spy_fire(sym, side, ltp, vwap, candle_bar=None):
        fired.append((sym, side, ltp, vwap))
    book._fire_vwap_close_sl = _spy_fire

    # First live tick after entry -- 20 seconds later, still inside the NEXT
    # (still-forming) bucket. Under the old code this reads "latest" as the
    # [09:15-09:35) bucket (already closed, pre-entry) and fires immediately.
    await book._vwap_close_sl_check("DIXON", "CALL", 91.0, datetime(2026, 8, 24, 9, 36, 20, tzinfo=IST))

    assert fired == []


@pytest.mark.asyncio
async def test_vwap_close_sl_check_still_fires_on_a_genuine_post_entry_breach(monkeypatch):
    """Sanity: the entry_ts guard above must not blanket-suppress a REAL
    post-entry breach -- same bucket as above, but entry happened BEFORE
    the bucket closed, so it's genuinely this position's own price action."""
    from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc

    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("DIXON", 13250, "PE")
    entry_ts = datetime(2026, 8, 24, 9, 16, 0, tzinfo=IST)   # BEFORE the bucket below closes
    book._positions["DIXON"] = {
        "contract": contract, "qty": 50, "entry_price": 459.05, "paper_mode": True,
        "opened_at": entry_ts, "sl_mechanic": "vwap",
    }
    book._vwap.seed("DIXON", 120.0 * 10.0, 10.0)

    rows = _rows_flat(9, 15, 20, 90.0, 10.0)   # [09:15-09:35) closes 09:35, close=90
    acc = _TrapAcc(timeframe_min=1)
    acc.bars = _bars_from_rows(rows)
    book._sl_vwap_1m_acc["DIXON"] = acc

    fired = []
    async def _spy_fire(sym, side, ltp, vwap, candle_bar=None):
        fired.append((sym, side, ltp, vwap))
    book._fire_vwap_close_sl = _spy_fire

    await book._vwap_close_sl_check("DIXON", "CALL", 91.0, datetime(2026, 8, 24, 9, 36, 20, tzinfo=IST))

    assert len(fired) == 1
    sym, side, ltp, vwap = fired[0]
    assert ltp == pytest.approx(91.0)
    assert vwap == pytest.approx(120.0)


# ── 2026-09-16, direct user spec: VWAP-close SL reverts to Heikin-Ashi,
# combined with (not replacing) the VWAP-gap test -- the SAME HA-shape rule
# already validated for ha_stoch_shape_exit_signal (HA_high==HA_open for
# CALL/no upper wick, HA_low==HA_open for PUT/no lower wick). BOTH the gap
# AND the shape must hold. ──────────────────────────────────────────────────

def test_ha_vwap_close_sl_adverse_requires_both_shape_and_gap():
    from strategies.core.trap_zone_utils import Bar
    book = _make_book(_FakeBus())
    vwap = 100.0

    # Clean bearish shape (no upper wick) AND a real gap below vwap -> fires.
    clean = Bar(ts=None, open=99.0, high=99.0, low=97.0, close=97.5)
    assert book._ha_vwap_close_sl_adverse(clean, vwap, "CALL") is True

    # Same close (same gap), but an upper wick (high > open) -> shape fails,
    # must NOT fire even though the price gap alone would qualify.
    wicked = Bar(ts=None, open=99.0, high=101.0, low=97.0, close=97.5)
    assert book._ha_vwap_close_sl_adverse(wicked, vwap, "CALL") is False

    # Clean shape but the gap is too small -> must NOT fire.
    shallow = Bar(ts=None, open=99.9, high=99.9, low=99.7, close=99.85)
    assert book._ha_vwap_close_sl_adverse(shallow, vwap, "CALL") is False


def test_ha_vwap_close_sl_adverse_put_side_mirrors_call():
    from strategies.core.trap_zone_utils import Bar
    book = _make_book(_FakeBus())
    vwap = 100.0

    clean = Bar(ts=None, open=101.0, high=103.0, low=101.0, close=102.5)
    assert book._ha_vwap_close_sl_adverse(clean, vwap, "PUT") is True

    # Lower wick (low < open) -> shape fails for PUT.
    wicked = Bar(ts=None, open=101.0, high=103.0, low=99.0, close=102.5)
    assert book._ha_vwap_close_sl_adverse(wicked, vwap, "PUT") is False


@pytest.mark.asyncio
async def test_vwap_close_sl_check_does_not_fire_on_gap_breach_without_clean_ha_shape(monkeypatch):
    """The critical proof: a real 20-min bucket that breaches the VWAP gap
    (would have fired under the pre-2026-09-16 plain-candle test) but whose
    HA shape shows an upper wick (a brief intrabar spike above the bucket's
    own open before falling) must NOT fire -- the shape gate is a genuine
    AND, not decoration."""
    from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc

    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("DIXON", 13250, "PE")
    entry_ts = datetime(2026, 8, 24, 9, 16, 0, tzinfo=IST)
    book._positions["DIXON"] = {
        "contract": contract, "qty": 50, "entry_price": 459.05, "paper_mode": True,
        "opened_at": entry_ts, "sl_mechanic": "vwap",
    }
    book._vwap.seed("DIXON", 120.0 * 10.0, 10.0)   # vwap = 120

    # [09:15-09:35) bucket: opens ~100, spikes to 130 mid-bucket (real upper
    # wick), then falls and closes well below vwap (a real, large gap).
    rows = [_row(f"09:{15+i:02d}", 100.0, 100.0, 100.0, 10.0) for i in range(3)]
    rows.append(_row("09:18", 132.0, 130.0, 131.0, 10.0))   # the spike
    rows += [_row(f"09:{19+i:02d}", 91.0, 89.0, 90.0, 10.0) for i in range(16)]
    acc = _TrapAcc(timeframe_min=1)
    acc.bars = _bars_from_rows(rows)
    book._sl_vwap_1m_acc["DIXON"] = acc

    fired = []
    async def _spy_fire(sym, side, ltp, vwap, candle_bar=None):
        fired.append((sym, side, ltp, vwap))
    book._fire_vwap_close_sl = _spy_fire

    await book._vwap_close_sl_check("DIXON", "CALL", 91.0, datetime(2026, 8, 24, 9, 36, 20, tzinfo=IST))

    assert fired == []   # gap alone would have fired pre-2026-09-16 -- shape blocks it now


@pytest.mark.asyncio
async def test_seed_vwap_from_upstox_intraday_replaces_state_with_hlc3_weighted_real_bars(monkeypatch):
    """2026-09-10, real incident fix: TECHM entered the shortlist via the
    streaming path mid-rally, with VWAP never seeded from any real history
    -- this proves the new seed genuinely computes a real HLC3-weighted VWAP
    from real bars and REPLACES (not merges with) whatever was there
    before."""
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr(stock_resolve, "resolve_eq_instrument_key", lambda sym: "NSE_EQ|INE669C01036")
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})
    bars = [
        _bar("09:15", 1500, 1497, 1499, 1000),   # hlc3=1498.67
        _bar("09:20", 1520, 1500, 1518, 2000),   # hlc3=1512.67
        _bar("09:39", 1540, 1530, 1538, 1500),   # hlc3=1536.00
    ]
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_intraday_1m", _async_return(bars))

    # Simulate stale poll-based state accumulated before the fix would have
    # run (must be wiped, not merged with).
    book._vwap.update("TECHM", 1000.0, 1.0)

    await book._seed_vwap_from_upstox_intraday("TECHM")

    expected_num = (1498.0 + 2.0/3) * 1000 + (1512.0 + 2.0/3) * 2000 + 1536.0 * 1500
    expected_den = 1000 + 2000 + 1500
    assert book._vwap.current("TECHM") == pytest.approx(expected_num / expected_den, abs=0.01)


@pytest.mark.asyncio
async def test_seed_option_bars_from_history_reconstructs_sl_from_real_bars(monkeypatch):
    """Baseline bars (09:55-09:59, flat @100) establish a running vwap ~100.
    Position entered at 10:00. The 10:00-10:04 bucket (vwap_sl_tf_minutes=5)
    drifts down to a close of 82 -- adverse, a lone touch, no arm yet. The
    10:05-10:09 bucket stays near that same level (low=81.70, within
    pool_sl_from_adverse_lows' tol_pct% of bucket1's 82.0) -- the SECOND
    touch confirms the cluster and arms the SL there. A later still-forming
    bucket (10:10, partial) must NOT be replayed -- it becomes the seeded
    "current" bar for the live loop to continue."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("DIXON", 14500, "PE")
    book._positions["DIXON"] = {
        "contract": contract, "qty": 50, "entry_price": 118.80, "paper_mode": True,
        "opened_at": datetime(2026, 8, 24, 10, 0, 0),
    }
    bars = (
        [_bar(f"09:{m:02d}", 100, 100, 100, 100) for m in range(55, 60)]
        + [
            _bar("10:00", 100, 95, 95, 100),
            _bar("10:01", 95, 90, 90, 100),
            _bar("10:02", 90, 88, 88, 100),
            _bar("10:03", 88, 85, 85, 100),
            _bar("10:04", 85, 82, 82, 100),
            _bar("10:05", 82.0, 81.8, 81.9, 100),
            _bar("10:06", 81.9, 81.8, 81.85, 100),
            _bar("10:07", 81.85, 81.75, 81.8, 100),
            _bar("10:08", 81.8, 81.75, 81.78, 100),
            _bar("10:09", 81.78, 81.70, 81.75, 100),
            _bar("10:10", 81.75, 80.0, 80.0, 50),   # still-forming bucket -- not replayed
        ]
    )
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_intraday_1m", _async_return(bars))
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})

    await book._seed_option_bars_from_history("DIXON", contract, book._positions["DIXON"]["opened_at"])

    assert book._option_adverse_lows["DIXON"] == [82.0, 81.70]
    assert book._live_sl["DIXON"] == 81.70
    # entry=118.80, sl=81.70 -> risk=37.10, default rr_multiple=2.0 -> target=193.00
    assert book._live_target["DIXON"] == pytest.approx(193.00, abs=0.01)
    # The still-forming 10:10 bucket is seeded as the CURRENT bar, not replayed.
    assert book._option_sl_bar_key["DIXON"] == "10:10"
    assert book._option_sl_bar_cur["DIXON"]["c"] == 80.0
    assert book._live_option_atp["DIXON"] is not None


@pytest.mark.asyncio
async def test_seed_option_bars_from_history_ignores_bars_before_entry(monkeypatch):
    """A bucket that closed adverse entirely BEFORE the position existed must
    never arm an SL -- only bars from entry_ts onward count."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("DIXON", 14500, "PE")
    book._positions["DIXON"] = {
        "contract": contract, "qty": 50, "entry_price": 118.80, "paper_mode": True,
        "opened_at": datetime(2026, 8, 24, 10, 30, 0),
    }
    bars = [
        _bar("09:55", 100, 60, 60, 100),   # adverse close, but BEFORE entry -- must be ignored
        _bar("10:30", 100, 100, 100, 100),
        _bar("10:35", 100, 100, 100, 50),   # still-forming
    ]
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_intraday_1m", _async_return(bars))
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})

    await book._seed_option_bars_from_history("DIXON", contract, book._positions["DIXON"]["opened_at"])

    assert "DIXON" not in book._live_sl


@pytest.mark.asyncio
async def test_seed_option_bars_from_history_noop_without_a_token(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("DIXON", 14500, "PE")
    book._positions["DIXON"] = {
        "contract": contract, "qty": 50, "entry_price": 118.80, "paper_mode": True,
        "opened_at": datetime(2026, 8, 24, 10, 0, 0),
    }
    async def _must_not_be_called(*a, **k):
        raise AssertionError("must never fetch without a token")
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_intraday_1m", _must_not_be_called)
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {})

    await book._seed_option_bars_from_history("DIXON", contract, book._positions["DIXON"]["opened_at"])

    assert "DIXON" not in book._live_sl
    assert "DIXON" not in book._live_option_atp


@pytest.mark.asyncio
async def test_seed_option_bars_from_history_noop_on_fetch_exception(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("DIXON", 14500, "PE")
    book._positions["DIXON"] = {
        "contract": contract, "qty": 50, "entry_price": 118.80, "paper_mode": True,
        "opened_at": datetime(2026, 8, 24, 10, 0, 0),
    }

    async def _raise(*a, **k):
        raise RuntimeError("network error")
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_intraday_1m", _raise)
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})

    await book._seed_option_bars_from_history("DIXON", contract, book._positions["DIXON"]["opened_at"])

    assert "DIXON" not in book._live_sl


@pytest.mark.asyncio
async def test_restore_from_db_restores_already_fired_and_rejected_sets(monkeypatch):
    """The other real half of the 2026-08-24 incident: DIXON PUT re-signaled
    a second time the same day after the restart, because the in-memory
    _already_fired/_rejected sets were wiped along with everything else.
    Only harmless that day because contract resolution happened to fail on
    the retry -- must not rely on that luck going forward."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._today = date(2026, 8, 24)

    store.log_signal_event(_TEST_CLIENT_ID, _TEST_BINDING_ID, "DIXON", "signal_fired",
                            side="PUT", trade_date="2026-08-24")
    store.log_signal_event(_TEST_CLIENT_ID, _TEST_BINDING_ID, "VMM", "rejection_rule_triggered",
                            side="CALL", trade_date="2026-08-24")

    await book._restore_from_db()

    assert ("DIXON", "PUT") in book._already_fired
    assert ("VMM", "CALL") in book._rejected


@pytest.mark.asyncio
async def test_restore_from_db_restores_shortlist_and_regime_regardless_of_time_of_day(monkeypatch):
    """2026-09-10 CRITICAL FIX, real incident: a restart happening AFTER
    ENTRY_WINDOW_END used to leave the whole dashboard panel blank for the
    rest of the day -- _run_today_pipeline's own actionable-window check
    bails out before its main polling loop (the only place regime/
    shortlist/ORB used to get reconstructed from the DB on a restart) is
    ever reached. _restore_from_db must now restore them unconditionally,
    regardless of what time it is when this restart happens."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._today = date(2026, 9, 10)
    book._top20_mode = True

    store.record_scan(_TEST_CLIENT_ID, _TEST_BINDING_ID, -0.8, "ok", trade_date="2026-09-10")
    store.update_scan_regime(_TEST_CLIENT_ID, _TEST_BINDING_ID, "bearish", trade_date="2026-09-10")
    store.record_oi_spurt_history(
        _TEST_CLIENT_ID, _TEST_BINDING_ID, "12:45:00",
        [{"symbol": "UNIONBANK", "rank": 1, "oi_spurt_pct": 12.0, "price_change_pct": -3.0}],
        trade_date="2026-09-10",
    )

    async def _no_seed(sym):
        return None
    monkeypatch.setattr(book, "_seed_vwap_from_upstox_intraday", _no_seed)
    monkeypatch.setattr(screener, "backfill_orb_from_yahoo", lambda bars, syms, cfg: None)
    monkeypatch.setattr(book, "_ensure_spot_feed", lambda sym: None)

    await book._restore_from_db()

    assert book._regime == "bearish"
    assert "UNIONBANK" in book._shortlist_symbols
    assert book._restart_db_reconcile_applied is True


@pytest.mark.asyncio
async def test_restore_from_db_with_nothing_stored_is_a_safe_noop():
    bus = _FakeBus()
    book = _make_book(bus)
    book._today = date(2026, 8, 24)

    await book._restore_from_db()

    assert book._positions == {}
    assert book._already_fired == set()
    assert book._rejected == set()


@pytest.mark.asyncio
async def test_restore_from_db_logs_critical_when_contract_cannot_be_re_resolved(monkeypatch):
    """A restored position whose contract can no longer be resolved (e.g.
    the registry can't produce an upstox_key any more) must not silently
    vanish -- it stays in the DB as still-open (so a human can reconcile
    against the broker) and the failure is logged at CRITICAL, not just
    dropped."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._today = date(2026, 8, 24)

    store.open_position(_TEST_CLIENT_ID, _TEST_BINDING_ID, "DIXON", "PE", 14500,
                         "2026-08-25", 50, 118.80, "orb_low_breakdown", True, "EVT1",
                         trade_date="2026-08-24")
    monkeypatch.setattr(stock_resolve, "resolve_contract_exact_async", _async_return(None))

    await book._restore_from_db()

    assert "DIXON" not in book._positions
    # the DB row itself is untouched (still open) -- restore failure never closes it
    still_open = store.load_open_positions(_TEST_CLIENT_ID, _TEST_BINDING_ID, trade_date="2026-08-24")
    assert len(still_open) == 1


@pytest.mark.asyncio
async def test_on_fill_persists_position_open_and_close_to_db():
    """End-to-end: a confirmed BUY fill writes an open row, and the
    matching SELL fill closes it with the real exit reason (threaded
    through _pending_closes, since OiOrbFillEvent itself carries no reason
    field) and computed P&L."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("DIXON", 14500, "PE")

    book._pending_contracts["DIXON"] = contract
    book._pending_fills["EVT1"] = {
        "symbol": "DIXON", "contract": contract, "qty": 50,
        "entry_price": 118.80, "reason": "orb_low_breakdown",
    }
    await book._on_fill(OiOrbFillEvent(
        action="BUY", underlying="DIXON", option_type="PE", strike=14500, fill_price=118.80,
        qty=50, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id="EVT1", paper_mode=True,
    ))

    open_rows = store.load_open_positions(_TEST_CLIENT_ID, _TEST_BINDING_ID)
    assert len(open_rows) == 1 and open_rows[0]["symbol"] == "DIXON"

    book._pending_closes["EVT2"] = "sma_exit"
    await book._on_fill(OiOrbFillEvent(
        action="SELL", underlying="DIXON", option_type="PE", strike=14500, fill_price=95.30,
        qty=50, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id="EVT2",
    ))

    assert store.load_open_positions(_TEST_CLIENT_ID, _TEST_BINDING_ID) == []
    import sqlite3
    con = sqlite3.connect(store._DB_PATH)
    con.row_factory = sqlite3.Row
    row = dict(con.execute("SELECT * FROM positions WHERE symbol='DIXON'").fetchone())
    con.close()
    assert row["exit_reason"] == "sma_exit"
    assert row["pnl"] == round((95.30 - 118.80) * 50, 2)


# ── S&R (R1/S1/R2/S2) SL tracking (2026-08-26, direct user spec, replaces
# the removed SMA-exit) ──────────────────────────────────────────────────

def test_ensure_spot_feed_subscribes_and_is_idempotent():
    bus = _FakeBus()
    book = _make_book(bus)
    book._ensure_spot_feed("MANAPPURAM")
    book._ensure_spot_feed("MANAPPURAM")   # second call must be a no-op
    assert bus._global_feeder.subscribed_equity == [("NSE:MANAPPURAM-EQ", "MANAPPURAM")]


# ── dedicated upstox2 feeder routing (2026-08-27) ────────────────────────

class _FakeOiOrbFeeder:
    """Stand-in for run_system.py's dedicated `bus._oiorb_feeder` -- the
    Upstox-native GlobalFeeder wrapper. Only the two methods OI-ORB actually
    calls on it are faked."""

    def __init__(self) -> None:
        self.subscribed_tokens: list = []
        self.registered_spot_keys: dict = {}

    async def subscribe_tokens(self, tokens):
        self.subscribed_tokens.extend(tokens)

    def register_extra_spot_keys(self, mapping):
        self.registered_spot_keys.update(mapping)


def test_ensure_spot_feed_prefers_dedicated_oiorb_feeder_when_available(monkeypatch):
    bus = _FakeBus()
    bus._oiorb_feeder = _FakeOiOrbFeeder()
    monkeypatch.setattr(stock_resolve, "resolve_eq_instrument_key", lambda sym: "NSE_EQ|INE123A01011")
    book = _make_book(bus)
    book._ensure_spot_feed("MANAPPURAM")
    assert bus._oiorb_feeder.registered_spot_keys == {"NSE_EQ|INE123A01011": "MANAPPURAM"}
    # The dedicated route succeeded -- the shared Fyers-only fallback must NOT fire too.
    assert bus._global_feeder.subscribed_equity == []


def test_ensure_spot_feed_falls_back_to_shared_feeder_when_no_eq_key_resolved(monkeypatch):
    bus = _FakeBus()
    bus._oiorb_feeder = _FakeOiOrbFeeder()
    monkeypatch.setattr(stock_resolve, "resolve_eq_instrument_key", lambda sym: "")
    book = _make_book(bus)
    book._ensure_spot_feed("MANAPPURAM")
    assert bus._oiorb_feeder.registered_spot_keys == {}
    assert bus._global_feeder.subscribed_equity == [("NSE:MANAPPURAM-EQ", "MANAPPURAM")]


def test_ensure_spot_feed_uses_shared_feeder_when_no_dedicated_feeder_configured():
    bus = _FakeBus()   # no _oiorb_feeder attribute at all
    book = _make_book(bus)
    book._ensure_spot_feed("MANAPPURAM")
    assert bus._global_feeder.subscribed_equity == [("NSE:MANAPPURAM-EQ", "MANAPPURAM")]


# ── _live_price carried-forward poll fallback (2026-09-15 real incident) ────
# INFY/TCS/LTM/PERSISTENT/WIPRO/HDFCBANK/TATAELXSI dropped out of live_df on
# every poll after the initial shortlist build for a whole real session --
# _live_price returned None every cycle, all day, with zero fallback.

def test_live_price_returns_none_when_missing_everywhere():
    import pandas as pd
    bus = _FakeBus()
    book = _make_book(bus)
    live = pd.DataFrame({"lastPrice": [100.0]}, index=["OTHERSTOCK"])
    assert book._live_price("INFY", live) is None


def test_live_price_carries_forward_last_poll_price_when_missing_from_live_df():
    import pandas as pd
    bus = _FakeBus()
    book = _make_book(bus)
    live1 = pd.DataFrame({"lastPrice": [1500.0]}, index=["INFY"])
    assert book._live_price("INFY", live1) == 1500.0   # seeds the carry-forward cache

    # Next poll cycle: INFY missing from live_df entirely (the real incident).
    live2 = pd.DataFrame({"lastPrice": [200.0]}, index=["OTHERSTOCK"])
    assert book._live_price("INFY", live2) == 1500.0   # carried forward, not None


def test_live_price_carried_forward_price_expires_after_stale_window(monkeypatch):
    import pandas as pd
    from strategies.oi_orb_screener import engine as engine_mod
    bus = _FakeBus()
    book = _make_book(bus)
    live1 = pd.DataFrame({"lastPrice": [1500.0]}, index=["INFY"])
    book._live_price("INFY", live1)

    live2 = pd.DataFrame({"lastPrice": [200.0]}, index=["OTHERSTOCK"])
    # Push the carried-forward timestamp far enough into the past to expire it.
    book._last_poll_price_ts["INFY"] = datetime.now(IST) - timedelta(
        seconds=engine_mod._POLL_PRICE_STALE_SEC + 5)
    assert book._live_price("INFY", live2) is None


def test_live_price_prefers_fresh_tick_over_carried_forward_poll_price():
    import pandas as pd
    bus = _FakeBus()
    book = _make_book(bus)
    live1 = pd.DataFrame({"lastPrice": [1500.0]}, index=["INFY"])
    book._live_price("INFY", live1)

    book._live_spot_ltp["INFY"] = 1555.0
    book._live_spot_ltp_ts["INFY"] = datetime.now(IST)
    live2 = pd.DataFrame({"lastPrice": [200.0]}, index=["OTHERSTOCK"])
    assert book._live_price("INFY", live2) == 1555.0


@pytest.mark.asyncio
async def test_ensure_option_feed_prefers_dedicated_oiorb_feeder_when_available():
    bus = _FakeBus()
    bus._oiorb_feeder = _FakeOiOrbFeeder()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._ensure_option_feed("MANAPPURAM", contract)
    await asyncio.sleep(0.05)
    assert contract.upstox_key in bus._oiorb_feeder.subscribed_tokens
    assert contract.upstox_key not in bus._global_feeder.subscribed_tokens


@pytest.mark.asyncio
async def test_emit_close_threads_entry_ts_from_position_opened_at():
    """2026-09-10, real finding: entry_ts was only ever set on the BUY
    event's own construction -- the SELL/close event never threaded the
    original entry time forward, so every closed trade's dashboard History
    row showed a blank entry TIME (confirmed live: entry_ts=null in every
    stored oi_orb_screener_top20 history record)."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    opened_at = datetime(2026, 9, 10, 9, 25, 12, tzinfo=IST)
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": opened_at,
    }
    book._live_option_ltp["MANAPPURAM"] = 12.5

    await book._emit_close("MANAPPURAM", book._positions["MANAPPURAM"], "vwap_close_sl")

    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert len(sell_events) == 1
    assert sell_events[0].entry_ts == opened_at


@pytest.mark.asyncio
async def test_spot_tick_loop_reacts_to_index_tick_not_just_equity_tick():
    """The dedicated upstox2 feeder's register_extra_spot_keys() publishes
    stock spot ticks as INDEX_TICK (Upstox-native mechanic), not EQUITY_TICK
    (the Fyers-only fallback route) -- _spot_tick_loop must react to both."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    # Only the spot-tick loop is needed -- NOT book.start()'s full task set
    # (_daily_loop would try real NSE calls off the wall clock).
    book._subscribe(Topic.EQUITY_TICK)
    book._subscribe(Topic.INDEX_TICK)
    book._running = True
    spot_task = asyncio.create_task(book._spot_tick_loop())
    try:
        await bus.publish(Topic.INDEX_TICK, IndexTick(
            symbol="MANAPPURAM", ltp=101.5, open=101.5, high=101.5, low=101.5, close=101.5,
            volume=0, timestamp=datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST),
        ))
        await asyncio.sleep(0.1)
        assert book._live_spot_ltp.get("MANAPPURAM") == 101.5
    finally:
        book._running = False
        spot_task.cancel()
        try:
            await spot_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_spot_tick_loop_accumulates_vwap_tick_by_tick_top20_mode():
    """2026-09-10, direct user spec: real tick-by-tick VWAP accumulation off
    IndexTick.volume deltas, replacing the old 20s poll-snapshot method --
    proves consecutive ticks with real cumulative-volume deltas produce a
    genuine volume-weighted VWAP, not just the latest LTP."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._top20_mode = True
    book._shortlist_symbols = ["TECHM"]
    book._subscribe(Topic.EQUITY_TICK)
    book._subscribe(Topic.INDEX_TICK)
    book._running = True
    spot_task = asyncio.create_task(book._spot_tick_loop())
    try:
        # First tick establishes the cumulative-volume baseline -- no VWAP
        # contribution yet (nothing to diff against).
        await bus.publish(Topic.INDEX_TICK, IndexTick(
            symbol="TECHM", ltp=1530.0, open=1530.0, high=1530.0, low=1530.0, close=1530.0,
            volume=100000, timestamp=datetime(2026, 9, 10, 9, 40, 0, tzinfo=IST),
        ))
        await asyncio.sleep(0.05)
        assert book._vwap.current("TECHM") is None

        # Second tick: 1000 real shares traded at 1540.
        await bus.publish(Topic.INDEX_TICK, IndexTick(
            symbol="TECHM", ltp=1540.0, open=1530.0, high=1540.0, low=1530.0, close=1540.0,
            volume=101000, timestamp=datetime(2026, 9, 10, 9, 40, 5, tzinfo=IST),
        ))
        await asyncio.sleep(0.05)
        assert book._vwap.current("TECHM") == pytest.approx(1540.0)

        # Third tick: 1000 more real shares at 1520 -- VWAP must be the real
        # volume-weighted blend of both deltas, not just the latest LTP.
        await bus.publish(Topic.INDEX_TICK, IndexTick(
            symbol="TECHM", ltp=1520.0, open=1530.0, high=1540.0, low=1520.0, close=1520.0,
            volume=102000, timestamp=datetime(2026, 9, 10, 9, 40, 10, tzinfo=IST),
        ))
        await asyncio.sleep(0.05)
        assert book._vwap.current("TECHM") == pytest.approx(1530.0)
    finally:
        book._running = False
        spot_task.cancel()
        try:
            await spot_task
        except asyncio.CancelledError:
            pass


def test_on_fill_buy_resets_option_sl_target_state_and_subscribes_spot_feed():
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._pending_fills["EVT1"] = {
        "symbol": "MANAPPURAM", "contract": contract, "qty": 100,
        "entry_price": 10.0, "reason": "signal",
    }
    # Stale state from an earlier (already-closed) run on this same symbol today --
    # must not leak into the freshly-opened position's own SL/target tracking.
    book._option_sl_bar_key["MANAPPURAM"] = "09:15"
    book._option_sl_bar_cur["MANAPPURAM"] = {"h": 1.0, "l": 1.0, "c": 1.0, "ts": datetime(2026, 8, 26, 9, 15)}
    book._live_sl["MANAPPURAM"] = 999.0
    book._live_target["MANAPPURAM"] = 999.0
    book._live_option_atp["MANAPPURAM"] = 5.0

    asyncio.run(book._on_fill(OiOrbFillEvent(
        action="BUY", underlying="MANAPPURAM", option_type="CE", strike=365, fill_price=10.0,
        qty=100, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id="EVT1",
        paper_mode=True,
    )))

    assert "MANAPPURAM" not in book._option_sl_bar_key
    assert "MANAPPURAM" not in book._option_sl_bar_cur
    assert "MANAPPURAM" not in book._live_sl
    assert "MANAPPURAM" not in book._live_target
    assert "MANAPPURAM" not in book._live_option_atp
    assert bus._global_feeder.subscribed_equity == [("NSE:MANAPPURAM-EQ", "MANAPPURAM")]


@pytest.mark.asyncio
async def test_spot_feed_retry_loop_resubscribes_when_no_ticks_after_grace_period(monkeypatch):
    """2026-09-10, real incident: _ensure_spot_feed's own subscribed flag is
    set the instant it CALLS the feeder, not once a real tick arrives -- if
    that call raced ahead of the feeder's WebSocket being ready, the symbol
    was marked done forever with zero live ticks ever arriving. This loop
    must detect "subscribed a while ago, still nothing" and force a genuine
    retry."""
    import strategies.oi_orb_screener.engine as engine_mod
    monkeypatch.setattr(engine_mod, "_SPOT_FEED_RETRY_GRACE_SEC", 0.01)
    monkeypatch.setattr(engine_mod, "_SPOT_FEED_RETRY_POLL_SEC", 0.05)

    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    book._spot_tick_subscribed["MANAPPURAM"] = True
    book._spot_feed_subscribed_at["MANAPPURAM"] = datetime.now(IST) - timedelta(seconds=100)
    # Deliberately no self._live_spot_ltp["MANAPPURAM"] -- simulates the
    # real incident: subscribed, but zero live ticks ever arrived.

    book._running = True
    task = asyncio.create_task(book._spot_feed_retry_loop())
    try:
        await asyncio.sleep(0.2)
        # Retried at least once (repeatedly, since the fixture never
        # populates a real tick -- matches real behavior, it keeps trying
        # until ticks actually start flowing): the stale flag was cleared
        # and re-subscribed via the Fyers fallback route (no dedicated
        # upstox2 feeder in this fixture).
        assert len(bus._global_feeder.subscribed_equity) >= 1
        assert all(c == ("NSE:MANAPPURAM-EQ", "MANAPPURAM") for c in bus._global_feeder.subscribed_equity)
        assert book._spot_tick_subscribed.get("MANAPPURAM") is True
    finally:
        book._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_spot_feed_retry_loop_does_not_retry_within_grace_period():
    """A symbol subscribed only moments ago must NOT be retried yet -- the
    grace period exists specifically so a genuinely-in-flight subscribe
    isn't churned."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    book._spot_tick_subscribed["MANAPPURAM"] = True
    book._spot_feed_subscribed_at["MANAPPURAM"] = datetime.now(IST)

    book._running = True
    task = asyncio.create_task(book._spot_feed_retry_loop())
    try:
        await asyncio.sleep(0.1)
        assert bus._global_feeder.subscribed_equity == []
    finally:
        book._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_spot_feed_retry_loop_does_not_retry_once_ticks_are_flowing():
    """A symbol with a real live spot_ltp already must never be touched,
    regardless of how long ago it was subscribed."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    book._spot_tick_subscribed["MANAPPURAM"] = True
    book._spot_feed_subscribed_at["MANAPPURAM"] = datetime.now(IST) - timedelta(seconds=100)
    book._live_spot_ltp["MANAPPURAM"] = 366.5

    book._running = True
    task = asyncio.create_task(book._spot_feed_retry_loop())
    try:
        await asyncio.sleep(0.1)
        assert bus._global_feeder.subscribed_equity == []
    finally:
        book._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_option_sl_does_not_arm_on_a_single_adverse_bar():
    """2026-08-28 real incident fix: a single adverse bar's own low is
    ordinary intraday noise, not a real defended level -- two real trades
    the same session (COFORGE CE2000, KPITTECH CE620) got stopped by the
    OLD single-bar anchor right before a genuine reversal (confirmed on
    real TradingView charts). See screener.pool_sl_from_adverse_lows' own
    docstring. One adverse bar close must NOT arm the SL by itself."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._vwap_sl_tf_minutes = 1
    book._live_option_atp["MANAPPURAM"] = 100.0
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }

    await book._update_option_sl_target_and_check("MANAPPURAM", 105.0, datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 108.0, datetime(2026, 8, 26, 9, 15, 40, tzinfo=IST))
    # bar1 (09:15, close=108 > vwap=100 -- FAVORABLE) closes on this next tick.
    await book._update_option_sl_target_and_check("MANAPPURAM", 102.0, datetime(2026, 8, 26, 9, 16, 10, tzinfo=IST))
    assert "MANAPPURAM" not in book._live_sl   # favorable close never arms

    await book._update_option_sl_target_and_check("MANAPPURAM", 90.0, datetime(2026, 8, 26, 9, 16, 40, tzinfo=IST))
    # bar2 (09:16, high=102/low=90/close=90 < vwap=100 -- ADVERSE) closes on this next tick.
    await book._update_option_sl_target_and_check("MANAPPURAM", 91.0, datetime(2026, 8, 26, 9, 17, 10, tzinfo=IST))

    # Only ONE adverse bar so far -- a lone touch, not a confirmed cluster.
    assert "MANAPPURAM" not in book._live_sl
    assert book._option_adverse_lows["MANAPPURAM"] == [90.0]


@pytest.mark.asyncio
async def test_option_sl_arms_once_a_second_adverse_bar_clusters_near_the_first():
    """A second adverse bar whose own low sits within pool_sl_from_adverse_
    lows' tol_pct% of the first confirms a real anchor -- the SL arms to
    that (most recent) clustered low (still computed and shown in the UI's
    "Option SL/Target" field for reference).

    2026-09-10, direct user spec: "the backtest also doesn't use the option
    sl and target, it used only spot sl and target, then why are we
    checking for option sl and target" -- this option-premium level no
    longer closes the position on its own; the universal spot-based exit
    (_vwap_close_sl_check et al, tested elsewhere) is the sole trigger now.
    A live tick breaching this armed level must NOT close anything."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._vwap_sl_tf_minutes = 1
    book._live_option_atp["MANAPPURAM"] = 100.0
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    # bar1 (09:15): low=90 -- adverse, lone touch, no arm yet.
    await book._update_option_sl_target_and_check("MANAPPURAM", 95.0, datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 90.0, datetime(2026, 8, 26, 9, 15, 40, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 92.0, datetime(2026, 8, 26, 9, 16, 10, tzinfo=IST))
    assert "MANAPPURAM" not in book._live_sl

    # bar2 (09:16): low=90.5 -- within 1% of 90 (tol=0.905) -- clusters, arms at 90.5.
    await book._update_option_sl_target_and_check("MANAPPURAM", 90.5, datetime(2026, 8, 26, 9, 16, 40, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 91.0, datetime(2026, 8, 26, 9, 17, 10, tzinfo=IST))
    assert book._live_sl["MANAPPURAM"] == 90.5
    # entry=100, sl=90.5 -> risk=9.5, default rr_multiple=2.0 -> target=119.0
    assert book._live_target["MANAPPURAM"] == 119.0
    assert "MANAPPURAM" in book._positions   # not breached yet (91 > 90.5)

    sell_events_before = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert sell_events_before == []

    # A live tick breaches the armed option-premium SL -- must NOT close.
    await book._update_option_sl_target_and_check("MANAPPURAM", 90.4, datetime(2026, 8, 26, 9, 17, 20, tzinfo=IST))

    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert sell_events == []
    assert "MANAPPURAM" not in book._eod_closing
    assert "MANAPPURAM" in book._positions


@pytest.mark.asyncio
async def test_option_sl_two_far_apart_adverse_lows_do_not_cluster():
    """Two adverse bars whose lows sit FAR apart (outside tol_pct%) are two
    separate one-off dips, not a real defended level -- neither should arm
    the SL alone."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._vwap_sl_tf_minutes = 1
    book._live_option_atp["MANAPPURAM"] = 100.0
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    await book._update_option_sl_target_and_check("MANAPPURAM", 95.0, datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 90.0, datetime(2026, 8, 26, 9, 15, 40, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 92.0, datetime(2026, 8, 26, 9, 16, 10, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 85.0, datetime(2026, 8, 26, 9, 16, 40, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 88.0, datetime(2026, 8, 26, 9, 17, 10, tzinfo=IST))

    assert "MANAPPURAM" not in book._live_sl
    assert book._option_adverse_lows["MANAPPURAM"] == [90.0, 85.0]


@pytest.mark.asyncio
async def test_option_target_hit_does_not_close_position():
    """2026-09-10, direct user spec (same as the SL test above): option-
    premium target hit must NOT close a position -- only the universal
    spot-based exit does. self._live_target is still tracked for display."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._vwap_sl_tf_minutes = 1
    book._live_option_atp["MANAPPURAM"] = 100.0
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    book._live_sl["MANAPPURAM"] = 90.0
    book._live_target["MANAPPURAM"] = 120.0

    await book._update_option_sl_target_and_check("MANAPPURAM", 121.0, datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST))

    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert sell_events == []
    assert "MANAPPURAM" in book._positions


@pytest.mark.asyncio
async def test_option_sl_rearms_once_a_new_cluster_forms_at_a_different_level():
    """2026-08-28: re-arm is no longer "every single adverse bar" -- it now
    requires its own fresh 2-touch cluster (screener.pool_sl_from_adverse_
    lows). A confirmed level holds steady while a new, uncorroborated dip
    comes in, then replaces it once THAT dip earns its own second touch."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._vwap_sl_tf_minutes = 1
    book._live_option_atp["MANAPPURAM"] = 100.0
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }

    # bar1 (09:15) low=90, bar2 (09:16) low=90.3 -- cluster confirms SL=90.3.
    await book._update_option_sl_target_and_check("MANAPPURAM", 95.0, datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 90.0, datetime(2026, 8, 26, 9, 15, 40, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 92.0, datetime(2026, 8, 26, 9, 16, 10, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 90.3, datetime(2026, 8, 26, 9, 16, 40, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 91.5, datetime(2026, 8, 26, 9, 17, 10, tzinfo=IST))
    assert book._live_sl["MANAPPURAM"] == 90.3
    assert book._live_target["MANAPPURAM"] == pytest.approx(119.4)   # entry=100, risk=9.7, rr=2 -> 119.4

    # bar3 (09:17): low=80 -- a fresh, uncorroborated dip -- does NOT re-arm yet.
    await book._update_option_sl_target_and_check("MANAPPURAM", 80.0, datetime(2026, 8, 26, 9, 17, 40, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 81.0, datetime(2026, 8, 26, 9, 18, 10, tzinfo=IST))
    assert book._live_sl["MANAPPURAM"] == 90.3   # unchanged -- only one touch at 80 so far

    # bar4 (09:18): low=80.2 -- clusters with bar3's 80 -- NOW re-arms down to 80.2.
    await book._update_option_sl_target_and_check("MANAPPURAM", 80.2, datetime(2026, 8, 26, 9, 18, 40, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 81.0, datetime(2026, 8, 26, 9, 19, 10, tzinfo=IST))
    assert book._live_sl["MANAPPURAM"] == 80.2
    assert book._live_target["MANAPPURAM"] == pytest.approx(139.6)   # entry=100, risk=19.8, rr=2 -> 139.6


@pytest.mark.asyncio
async def test_option_sl_no_close_while_sl_not_yet_established():
    """Only ONE bar is still forming so far -- it hasn't CLOSED yet, so no SL
    exists and a big drop must NOT trigger a close (the position runs on the
    hard risk cap alone during this window)."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    await book._update_option_sl_target_and_check("MANAPPURAM", 100.0, datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 1.0, datetime(2026, 8, 26, 9, 15, 40, tzinfo=IST))

    assert "MANAPPURAM" not in book._live_sl
    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert sell_events == []


def test_default_vwap_sl_tf_minutes_is_5():
    """2026-08-27, direct user spec default -- fresh, unvalidated (this
    strategy still can't be backtested), watch real forward telemetry."""
    bus = _FakeBus()
    book = _make_book(bus)
    assert book._vwap_sl_tf_minutes == 5


def test_default_rr_multiple_is_2():
    bus = _FakeBus()
    book = _make_book(bus)
    assert book._rr_multiple == 2.0


@pytest.mark.asyncio
async def test_option_sl_bars_bucket_by_the_configured_tf_not_always_1min():
    """09:15 and 09:16 must fall in the SAME bar at the 5-min default
    (floor(15/5)*5 == floor(16/5)*5 == 15) -- confirms the bucketing actually
    uses self._vwap_sl_tf_minutes, not a hardcoded 1-min key."""
    bus = _FakeBus()
    book = _make_book(bus)
    assert book._vwap_sl_tf_minutes == 5
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    await book._update_option_sl_target_and_check("MANAPPURAM", 100.0, datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST))
    await book._update_option_sl_target_and_check("MANAPPURAM", 95.0, datetime(2026, 8, 26, 9, 16, 40, tzinfo=IST))
    # Still the SAME 5-min bar (09:15-09:20) -- no bar close, no arm yet.
    assert book._option_sl_bar_key["MANAPPURAM"] == "09:15"
    assert "MANAPPURAM" not in book._live_sl
    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert sell_events == []


@pytest.mark.asyncio
async def test_option_tick_loop_feeds_atp_but_no_longer_arms_sl_target():
    """End-to-end via the real _option_tick_loop (not calling the SL/target
    method directly) -- confirms OptionTick.atp still feeds the option's own
    VWAP reference, but the SL/target ratchet is no longer armed by real
    ticks. 2026-09-07, direct user spec: "only exit is HA+StockRSI as we
    have done backtest with that only -- remove other exit condition from
    oi scanner" (confirmed to include hard_risk_cap too, not just this
    ratchet) -- _option_tick_loop no longer calls
    _update_option_sl_target_and_check/_check_hard_risk_cap at all, so
    self._live_sl/_live_target must stay empty regardless of how adverse the
    tick sequence is. This test used to assert the OLD behavior (SL arming
    at 89.5 via the pooled multi-touch anchor) -- inverted here to lock in
    the removal instead."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._vwap_sl_tf_minutes = 1
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    book._subscribe(Topic.OPTION_TICK)
    book._running = True
    task = asyncio.create_task(book._option_tick_loop())
    try:
        ticks = [
            (105.0, 100.0, datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST)),
            (108.0, 100.0, datetime(2026, 8, 26, 9, 15, 40, tzinfo=IST)),
            (102.0, 100.0, datetime(2026, 8, 26, 9, 16, 10, tzinfo=IST)),   # closes bar1 (favorable)
            (90.0, 100.0, datetime(2026, 8, 26, 9, 16, 40, tzinfo=IST)),
            (89.5, 100.0, datetime(2026, 8, 26, 9, 17, 10, tzinfo=IST)),    # closes bar2 (adverse, low=90.0) -- lone touch
            (91.0, 100.0, datetime(2026, 8, 26, 9, 18, 10, tzinfo=IST)),    # closes bar3 (adverse, low=89.5) -- clusters w/ 90.0 -> arms SL=89.5
        ]
        for ltp, atp, ts in ticks:
            await bus.publish(Topic.OPTION_TICK, OptionTick(
                symbol="MANAPPURAM365CE", underlying="MANAPPURAM", strike=365, option_type="CE",
                expiry=date(2026, 8, 27), ltp=ltp, bid=ltp, ask=ltp, oi=0, change_oi=0,
                volume=0, iv=0.0, delta=0.0, timestamp=ts, atp=atp,
            ))
            await asyncio.sleep(0.02)
        assert book._live_option_atp["MANAPPURAM"] == 100.0
        assert "MANAPPURAM" not in book._live_sl
        assert "MANAPPURAM" not in book._live_target
    finally:
        book._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_option_tick_loop_never_triggers_hard_risk_cap_even_on_severe_adverse_move():
    """2026-09-07, direct user spec (same as the SL/target removal above):
    hard_risk_cap must never fire either -- HA+StochRSI + EOD are the ONLY
    things that can close a position now. Feeds a tick sequence with a loss
    far exceeding the old Rs2000/lot cap and confirms no close is emitted."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 3000, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    book._subscribe(Topic.OPTION_TICK)
    book._running = True
    task = asyncio.create_task(book._option_tick_loop())
    try:
        # loss = (100 - 10) * 3000 = Rs270,000 -- far past any Rs2000/lot cap.
        await bus.publish(Topic.OPTION_TICK, OptionTick(
            symbol="MANAPPURAM365CE", underlying="MANAPPURAM", strike=365, option_type="CE",
            expiry=date(2026, 8, 27), ltp=10.0, bid=10.0, ask=10.0, oi=0, change_oi=0,
            volume=0, iv=0.0, delta=0.0, timestamp=datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST),
            atp=10.0,
        ))
        await asyncio.sleep(0.05)
        assert "MANAPPURAM" in book._positions, \
            "hard_risk_cap must not close the position -- only HA+StochRSI/EOD may"
        assert "MANAPPURAM" not in book._eod_closing
    finally:
        book._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_monitoring_state_includes_live_spot_ltp_and_sl():
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 24, 13, 0, 0),
    }
    book._live_spot_ltp["MANAPPURAM"] = 372.5
    book._live_sl["MANAPPURAM"] = 365.0
    book._live_target["MANAPPURAM"] = 400.0

    state = book.monitoring_state()
    pos = state["positions"]["MANAPPURAM"]
    assert pos["spot_ltp"] == 372.5
    assert pos["sl"] == 365.0
    assert pos["target"] == 400.0


def test_monitoring_state_sl_is_none_before_establishment():
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 24, 13, 0, 0),
    }
    state = book.monitoring_state()
    pos = state["positions"]["MANAPPURAM"]
    assert pos["spot_ltp"] is None
    assert pos["sl"] is None
    assert pos["target"] is None


# ── Hard Rs/lot risk cap backstop (2026-08-26) ───────────────────────────

@pytest.mark.asyncio
async def test_hard_risk_cap_closes_when_loss_meets_the_cap():
    bus = _FakeBus()
    book = _make_book(bus)
    book._lot_multiplier = 1
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 30.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    await book._check_hard_risk_cap("MANAPPURAM", 9.9)   # loss = (30-9.9)*100 = 2010 >= 2000

    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert len(sell_events) == 1
    assert sell_events[0].underlying == "MANAPPURAM"
    assert "MANAPPURAM" in book._eod_closing


@pytest.mark.asyncio
async def test_hard_risk_cap_no_close_within_the_cap():
    bus = _FakeBus()
    book = _make_book(bus)
    book._lot_multiplier = 1
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 30.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    await book._check_hard_risk_cap("MANAPPURAM", 25.0)   # loss = (30-25)*100 = 500 < 2000

    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert sell_events == []
    assert "MANAPPURAM" not in book._eod_closing


@pytest.mark.asyncio
async def test_hard_risk_cap_scales_with_lot_multiplier():
    bus = _FakeBus()
    book = _make_book(bus)
    book._lot_multiplier = 2   # cap doubles to Rs4000
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 30.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    # loss = (30-9.9)*100 = 2010 -- would have hit a Rs2000 cap, but not a Rs4000 one.
    await book._check_hard_risk_cap("MANAPPURAM", 9.9)

    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert sell_events == []


# ── Periodic per-stock heartbeat log (2026-08-27) ────────────────────────

def test_heartbeat_logs_price_vs_orb_levels():
    """_clog uses propagate=False (its own dedicated per-binding log file),
    so caplog can't observe it -- monkeypatch .info directly instead, same
    pattern the throttle test below already uses."""
    bus = _FakeBus()
    book = _make_book(bus)
    calls = []
    book._clog.info = lambda *a, **k: calls.append(a)
    book._maybe_log_heartbeat(["GVT&D=4550.00 [ORB 4433.20-4523.40] CALL@-0.59% PUT@2.57%"])
    assert len(calls) == 1
    assert "WATCH" in calls[0][0]
    assert "GVT&D" in calls[0][-1]


def test_heartbeat_is_throttled():
    bus = _FakeBus()
    book = _make_book(bus)
    calls = []
    book._clog.info = lambda *a, **k: calls.append(a)
    book._maybe_log_heartbeat(["A=1 [ORB pending]"])
    book._maybe_log_heartbeat(["A=2 [ORB pending]"])   # same cycle, must not double-log
    assert len(calls) == 1


def test_heartbeat_no_op_with_no_parts():
    bus = _FakeBus()
    book = _make_book(bus)
    calls = []
    book._clog.info = lambda *a, **k: calls.append(a)
    book._maybe_log_heartbeat([])
    assert calls == []


# ── trap TSL: unconfirmed levels must not trigger an exit (2026-09-01 fix) ──
# Real incident: ITC entered 13:33:11, TSL level {low:267.6,high:267.95}
# (is_established=False) hit at 13:37:30 on a 0.15% underlying pullback --
# same shape on ASHOKLEY, stopped under 2 minutes after entry. Both levels
# were the extreme of just 1-2 three-minute bars since entry, not real
# structure. _check_hard_risk_cap's independent Rs2000/lot backstop still
# protects the position the whole time this gate is waiting.

class _FakeSRCalc:
    """Stand-in for SupportResistanceCalculator -- returns a fixed,
    caller-controlled S1/R1 level regardless of what bars get fed in,
    so the test can isolate the is_established gate itself."""
    def __init__(self, level: dict, key: str):
        self._level = level
        self._key = key

    def process_straddle_candle(self, sym, bar):
        pass

    def get_calculated_sr_state(self, sym):
        return {"sr_levels": {self._key: self._level}}


def _rig_trap_tsl(book, sym, side, level, key, entry_price=100.0, qty=10):
    from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc
    book._positions[sym] = {
        "contract": type("C", (), {"option_type": "CE" if side == "CALL" else "PE", "strike": 100,
                                     "expiry": date(2026, 9, 29)})(),
        "qty": qty, "entry_price": entry_price, "paper_mode": True,
        "opened_at": datetime.now(IST), "sl_mechanic": "trap",
    }
    book._trap_tsl_calc[sym] = _FakeSRCalc(level, key)
    book._trap_tsl_acc[sym] = _TrapAcc(timeframe_min=3)
    book._trap_tsl_fed_bars[sym] = 0


@pytest.mark.asyncio
async def test_unestablished_level_does_not_trigger_exit(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    level = {"low": 267.6, "high": 267.95, "is_established": False}
    _rig_trap_tsl(book, "ITC", "CALL", level, "S1")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    # underlying breaches the level's low (267.50 < 267.6) -- same real numbers as the incident
    await book._trap_update_tsl_and_check_exit("ITC", "CALL", 267.50, datetime.now(IST))

    assert closed == [], "an unconfirmed (is_established=False) level must not trigger the TSL"
    assert "ITC" not in book._eod_closing


@pytest.mark.asyncio
async def test_established_level_still_triggers_exit(monkeypatch):
    """The gate must not block genuine, confirmed breaches -- only unconfirmed ones."""
    bus = _FakeBus()
    book = _make_book(bus)
    level = {"low": 267.6, "high": 267.95, "is_established": True}
    _rig_trap_tsl(book, "ITC", "CALL", level, "S1")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    await book._trap_update_tsl_and_check_exit("ITC", "CALL", 267.50, datetime.now(IST))

    assert closed == [("ITC", "trap_tsl")]
    assert "ITC" in book._eod_closing


@pytest.mark.asyncio
async def test_unestablished_level_no_breach_also_no_exit(monkeypatch):
    """Sanity: the gate itself isn't what's suppressing the exit above --
    confirm a price that doesn't even breach the level correctly no-ops too."""
    bus = _FakeBus()
    book = _make_book(bus)
    level = {"low": 267.6, "high": 267.95, "is_established": True}
    _rig_trap_tsl(book, "ITC", "CALL", level, "S1")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    await book._trap_update_tsl_and_check_exit("ITC", "CALL", 268.50, datetime.now(IST))  # above the low, no breach

    assert closed == []
    assert "ITC" not in book._eod_closing


@pytest.mark.asyncio
async def test_trap_tsl_populates_live_sl_for_dashboard(monkeypatch):
    """2026-09-03 fix, direct user report: same gap as the immediate_15m
    mechanic -- _live_sl was never written for trap-mechanic positions
    either, so the dashboard showed 'establishing...' despite the trap TSL
    genuinely protecting the position once its level is established."""
    bus = _FakeBus()
    book = _make_book(bus)
    level = {"low": 267.6, "high": 267.95, "is_established": True}
    _rig_trap_tsl(book, "ITC", "CALL", level, "S1")
    book._emit_close = lambda *a, **k: None

    await book._trap_update_tsl_and_check_exit("ITC", "CALL", 268.50, datetime.now(IST))  # no breach
    assert book._live_sl["ITC"] == 267.6


@pytest.mark.asyncio
async def test_trap_tsl_does_not_populate_live_sl_while_unestablished(monkeypatch):
    """The unestablished-level gate returns before the _live_sl write --
    confirms we don't surface a not-yet-real level to the dashboard either."""
    bus = _FakeBus()
    book = _make_book(bus)
    level = {"low": 267.6, "high": 267.95, "is_established": False}
    _rig_trap_tsl(book, "ITC", "CALL", level, "S1")
    book._emit_close = lambda *a, **k: None

    await book._trap_update_tsl_and_check_exit("ITC", "CALL", 267.50, datetime.now(IST))
    assert "ITC" not in book._live_sl


@pytest.mark.asyncio
async def test_put_side_uses_r1_and_same_established_gate(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    level = {"low": 169.32, "high": 169.61, "is_established": False}
    _rig_trap_tsl(book, "ASHOKLEY", "PUT", level, "R1")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    # PUT breaches on ltp >= level["high"] -- same real numbers as the ASHOKLEY incident
    await book._trap_update_tsl_and_check_exit("ASHOKLEY", "PUT", 169.61, datetime.now(IST))

    assert closed == [], "unconfirmed R1 must not trigger the PUT-side TSL either"


# ── 2026-09-02 opt-in immediate-entry alternate mode ────────────────────────
# Skips the zone/retest wait entirely, entering the instant ORB freezes, with
# a HYBRID stop: fixed ORB(09:15-09:25) extreme protects from entry, ratchets
# tighter to a 15-min S1/R1 ladder once that ladder establishes a genuine
# level closer than the ORB floor (never loosens past the floor either way).

def test_immediate_check_entry_fires_once_orb_frozen():
    bus = _FakeBus()
    book = _make_book(bus)
    book._orb_frozen["ITC"] = (267.25, 261.5)

    fired = book._immediate_check_entry("ITC", "CALL", datetime.now(IST))

    assert fired is True
    assert "ITC" in book._immediate_tsl_calc
    assert "ITC" in book._immediate_tsl_acc


def test_immediate_check_entry_false_before_orb_frozen():
    bus = _FakeBus()
    book = _make_book(bus)
    # ORB never frozen for this symbol
    fired = book._immediate_check_entry("ITC", "CALL", datetime.now(IST))
    assert fired is False
    assert "ITC" not in book._immediate_tsl_calc


def test_immediate_check_entry_does_not_refire():
    bus = _FakeBus()
    book = _make_book(bus)
    book._orb_frozen["ITC"] = (267.25, 261.5)

    first = book._immediate_check_entry("ITC", "CALL", datetime.now(IST))
    second = book._immediate_check_entry("ITC", "CALL", datetime.now(IST))

    assert first is True
    assert second is False


def _rig_immediate_tsl(book, sym, side, level, key, entry_price=100.0, qty=10):
    from strategies.core.trap_zone_utils import BarAccumulator as _TrapAcc
    book._positions[sym] = {
        "contract": type("C", (), {"option_type": "CE" if side == "CALL" else "PE", "strike": 100,
                                     "expiry": date(2026, 9, 29)})(),
        "qty": qty, "entry_price": entry_price, "paper_mode": True,
        "opened_at": datetime.now(IST), "sl_mechanic": "immediate_15m",
    }
    book._immediate_tsl_calc[sym] = _FakeSRCalc(level, key)
    book._immediate_tsl_acc[sym] = _TrapAcc(timeframe_min=15)
    book._immediate_tsl_fed_bars[sym] = 0


@pytest.mark.asyncio
async def test_immediate_hybrid_uses_orb_floor_when_ladder_unestablished():
    """An unconfirmed 15-min level must be ignored entirely -- the stop
    stays at the ORB floor, which itself still protects the position."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._orb_frozen["ITC"] = (267.25, 261.5)   # CALL -> floor is 261.5
    level = {"low": 267.6, "high": 267.95, "is_established": False}
    _rig_immediate_tsl(book, "ITC", "CALL", level, "S1")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    # Above the unestablished ladder level's own low (267.6) but nowhere
    # near the ORB floor (261.5) -- must NOT fire, since the ladder level
    # was never confirmed and the ORB floor alone is what's active.
    await book._immediate_update_tsl_and_check_exit("ITC", "CALL", 267.50, datetime.now(IST))
    assert closed == [], "an unconfirmed 15-min level must never itself trigger the stop"

    # The ORB floor itself still protects the position.
    await book._immediate_update_tsl_and_check_exit("ITC", "CALL", 261.50, datetime.now(IST))
    assert closed == [("ITC", "immediate_hybrid_sl")]
    assert "ITC" in book._eod_closing


@pytest.mark.asyncio
async def test_immediate_hybrid_ratchets_tighter_once_ladder_established():
    """A genuinely established 15-min level, tighter than the ORB floor,
    must tighten the effective stop -- firing at the ladder level, not
    waiting all the way down to the (looser) ORB floor."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._orb_frozen["ITC"] = (267.25, 261.5)   # CALL -> floor is 261.5
    level = {"low": 265.0, "high": 267.95, "is_established": True}   # tighter than 261.5
    _rig_immediate_tsl(book, "ITC", "CALL", level, "S1")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    # Below the established ladder level (265.0) but still above the ORB
    # floor (261.5) -- must fire, since the ladder has tightened the stop.
    await book._immediate_update_tsl_and_check_exit("ITC", "CALL", 264.80, datetime.now(IST))

    assert closed == [("ITC", "immediate_hybrid_sl")]
    assert "ITC" in book._eod_closing


@pytest.mark.asyncio
async def test_immediate_hybrid_never_loosens_past_orb_floor():
    """A defensive case: an established ladder level that is somehow LOOSER
    than the ORB floor must never widen the stop -- the ORB floor is the
    permanent minimum protection."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._orb_frozen["ITC"] = (267.25, 261.5)   # CALL -> floor is 261.5
    level = {"low": 258.0, "high": 267.95, "is_established": True}   # looser than 261.5
    _rig_immediate_tsl(book, "ITC", "CALL", level, "S1")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    # Between the (looser) ladder level and the (tighter) ORB floor -- must
    # fire at the ORB floor, since the stop never widens past it.
    await book._immediate_update_tsl_and_check_exit("ITC", "CALL", 260.00, datetime.now(IST))

    assert closed == [("ITC", "immediate_hybrid_sl")]


@pytest.mark.asyncio
async def test_immediate_hybrid_put_side_same_ratchet_logic():
    bus = _FakeBus()
    book = _make_book(bus)
    book._orb_frozen["ASHOKLEY"] = (169.61, 165.00)   # PUT -> floor is 169.61
    level = {"low": 160.0, "high": 168.0, "is_established": True}   # tighter than 169.61
    _rig_immediate_tsl(book, "ASHOKLEY", "PUT", level, "R1")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    await book._immediate_update_tsl_and_check_exit("ASHOKLEY", "PUT", 168.20, datetime.now(IST))

    assert closed == [("ASHOKLEY", "immediate_hybrid_sl")]


@pytest.mark.asyncio
async def test_immediate_hybrid_populates_live_sl_for_dashboard():
    """2026-09-03 fix, direct user report: monitoring_state()'s 'sl' field
    read straight from self._live_sl, which was only ever written by the
    legacy vwap mechanic -- a trap/immediate_15m position's dashboard panel
    showed 'establishing...' forever, even with real protection active.
    _live_sl must reflect the current effective stop (ORB floor or the
    tighter ratcheted ladder level) on every tick, not just at breach time."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._orb_frozen["ITC"] = (267.25, 261.5)   # CALL -> floor is 261.5
    level = {"low": 267.6, "high": 267.95, "is_established": False}
    _rig_immediate_tsl(book, "ITC", "CALL", level, "S1")
    book._emit_close = lambda *a, **k: None

    # Unestablished ladder -- effective stop is still the ORB floor.
    await book._immediate_update_tsl_and_check_exit("ITC", "CALL", 267.50, datetime.now(IST))
    assert book._live_sl["ITC"] == 261.5

    # Ladder establishes tighter than the floor -- _live_sl must ratchet with it.
    book._immediate_tsl_calc["ITC"] = _FakeSRCalc(
        {"low": 265.0, "high": 267.95, "is_established": True}, "S1")
    await book._immediate_update_tsl_and_check_exit("ITC", "CALL", 266.00, datetime.now(IST))
    assert book._live_sl["ITC"] == 265.0


@pytest.mark.asyncio
async def test_on_fill_tags_immediate_15m_when_reason_is_immediate_orb_entry(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("ITC", 270, "CE")
    eid = "evt1"
    book._pending_fills[eid] = {
        "symbol": "ITC", "contract": contract, "qty": 375,
        "entry_price": 10.0, "reason": "immediate_orb_entry",
    }
    fill = OiOrbFillEvent(
        client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id=eid,
        action="BUY", underlying="ITC", option_type="CE", strike=270,
        qty=375, fill_price=10.0, paper_mode=True,
    )
    await book._on_fill(fill)

    assert book._positions["ITC"]["sl_mechanic"] == "immediate_15m"


@pytest.mark.asyncio
async def test_on_fill_tags_trap_when_reason_is_trap_retest(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("ITC", 270, "CE")
    eid = "evt2"
    book._pending_fills[eid] = {
        "symbol": "ITC", "contract": contract, "qty": 375,
        "entry_price": 10.0, "reason": "trap_retest",
    }
    fill = OiOrbFillEvent(
        client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id=eid,
        action="BUY", underlying="ITC", option_type="CE", strike=270,
        qty=375, fill_price=10.0, paper_mode=True,
    )
    await book._on_fill(fill)

    assert book._positions["ITC"]["sl_mechanic"] == "trap"


# ── 2026-09-06: _ha_stoch_check_exit -- the confirmed HA-shape + StochRSI(9,9)
# exit ported from this week's real-data backtest series into the live
# engine. These tests isolate the NEW orchestration code (BarAccumulator
# feed, closed-bar-only guard, once-per-bar dedup, eod_closing claim,
# _emit_close call) from the already-validated pure math (to_heikin_ashi/
# compute_stoch_rsi/ha_stoch_shape_exit_signal) by monkeypatching the shape/
# cross check itself -- the math functions have their own coverage via the
# backtest cross-check against the unmodified original script this session.

def _rig_ha_stoch_position(book, sym, side, entry_price=100.0, qty=10):
    book._positions[sym] = {
        "contract": type("C", (), {"option_type": "CE" if side == "CALL" else "PE", "strike": 100,
                                     "expiry": date(2026, 9, 29)})(),
        "qty": qty, "entry_price": entry_price, "paper_mode": True,
        "opened_at": datetime.now(IST), "sl_mechanic": "vwap",
    }


async def _feed_one_closed_15m_bar(book, sym, side, start_ts, base_price=100.0):
    """Feeds exactly 16 one-minute ticks (start_ts, start_ts+1min, ...,
    start_ts+15min) -- the 16th tick is what closes the first 15-min bucket
    (start_ts's own minute floored to a 15-min boundary), matching
    BarAccumulator.on_tick's own "a new bucket started" semantics. Prices
    are flat/trivial -- the actual HA-shape/StochRSI condition is
    monkeypatched in these tests, not genuinely computed from these prices."""
    ts = start_ts
    for i in range(16):
        await book._ha_stoch_check_exit(sym, side, base_price + i * 0.01, ts)
        ts = ts + timedelta(minutes=1)
    return ts


@pytest.mark.asyncio
async def test_ha_stoch_exit_fires_when_condition_confirms(monkeypatch):
    monkeypatch.setattr(
        "strategies.core.candle_indicators.ha_stoch_shape_exit_signal",
        lambda *a, **k: True,
    )
    bus = _FakeBus()
    book = _make_book(bus)
    _rig_ha_stoch_position(book, "ITC", "CALL")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    start = datetime(2026, 9, 8, 9, 15, tzinfo=IST)
    await _feed_one_closed_15m_bar(book, "ITC", "CALL", start)

    assert closed == [("ITC", "ha_stoch_exit")]
    assert "ITC" in book._eod_closing


@pytest.mark.asyncio
async def test_ha_stoch_exit_does_not_fire_when_condition_false(monkeypatch):
    monkeypatch.setattr(
        "strategies.core.candle_indicators.ha_stoch_shape_exit_signal",
        lambda *a, **k: False,
    )
    bus = _FakeBus()
    book = _make_book(bus)
    _rig_ha_stoch_position(book, "ITC", "CALL")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    start = datetime(2026, 9, 8, 9, 15, tzinfo=IST)
    await _feed_one_closed_15m_bar(book, "ITC", "CALL", start)

    assert closed == []
    assert "ITC" not in book._eod_closing


@pytest.mark.asyncio
async def test_ha_stoch_exit_skips_still_forming_bar(monkeypatch):
    """Fewer than 16 ticks -- the first 15-min bucket hasn't genuinely
    closed yet (wall-clock hasn't reached its own +15min boundary), so the
    condition must never even be evaluated, regardless of what it would
    return."""
    monkeypatch.setattr(
        "strategies.core.candle_indicators.ha_stoch_shape_exit_signal",
        lambda *a, **k: True,
    )
    bus = _FakeBus()
    book = _make_book(bus)
    _rig_ha_stoch_position(book, "ITC", "CALL")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    start = datetime(2026, 9, 8, 9, 15, tzinfo=IST)
    ts = start
    for i in range(10):   # only 10 of the needed 16 ticks
        await book._ha_stoch_check_exit("ITC", "CALL", 100.0, ts)
        ts = ts + timedelta(minutes=1)

    assert closed == []
    assert "ITC" not in book._eod_closing


@pytest.mark.asyncio
async def test_ha_stoch_exit_no_op_when_no_position(monkeypatch):
    monkeypatch.setattr(
        "strategies.core.candle_indicators.ha_stoch_shape_exit_signal",
        lambda *a, **k: True,
    )
    bus = _FakeBus()
    book = _make_book(bus)   # no position rigged for "ITC" at all

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    start = datetime(2026, 9, 8, 9, 15, tzinfo=IST)
    await _feed_one_closed_15m_bar(book, "ITC", "CALL", start)

    assert closed == []


@pytest.mark.asyncio
async def test_ha_stoch_exit_respects_already_closing_guard(monkeypatch):
    """A position another exit check already claimed this cycle
    (self._eod_closing) must not also be closed a second time by this
    check."""
    monkeypatch.setattr(
        "strategies.core.candle_indicators.ha_stoch_shape_exit_signal",
        lambda *a, **k: True,
    )
    bus = _FakeBus()
    book = _make_book(bus)
    _rig_ha_stoch_position(book, "ITC", "CALL")
    book._eod_closing.add("ITC")

    closed = []
    async def _fake_emit_close(sym, pos, reason):
        closed.append((sym, reason))
    book._emit_close = _fake_emit_close

    start = datetime(2026, 9, 8, 9, 15, tzinfo=IST)
    await _feed_one_closed_15m_bar(book, "ITC", "CALL", start)

    assert closed == []


# ── 2026-09-06: VWAP-retest is the active entry mechanic again (matching
# this week's entire validated backtest series) -- _trap_check_entry (bear/
# bull-trap zone entry) was found to be the engine's real active entry
# mechanic while porting this week's exit work, a mismatch never covered by
# any backtest this session ran. These tests cover the new _vwap_check_entry
# wrapper and the _on_fill tagging branch for its "vwap_retest" reason.

def test_vwap_check_entry_arms_then_fires_on_retest():
    bus = _FakeBus()
    book = _make_book(bus)
    book._vwap.update("ITC", 100.0, 10.0)   # vwap = 100.0

    # CALL: not yet armed, ltp above vwap -- arms, no fire yet.
    fire = book._vwap_check_entry("ITC", "CALL", 105.0)
    assert fire is False
    assert book._vwap_armed["ITC"] is True

    # Price comes back down onto vwap from above -- fires.
    fire = book._vwap_check_entry("ITC", "CALL", 100.0)
    assert fire is True


def test_vwap_check_entry_put_side_mirrors_call():
    bus = _FakeBus()
    book = _make_book(bus)
    book._vwap.update("KEI", 200.0, 10.0)   # vwap = 200.0

    fire = book._vwap_check_entry("KEI", "PUT", 195.0)
    assert fire is False
    assert book._vwap_armed["KEI"] is True

    fire = book._vwap_check_entry("KEI", "PUT", 200.0)
    assert fire is True


def test_vwap_check_entry_no_fire_before_vwap_exists():
    bus = _FakeBus()
    book = _make_book(bus)   # self._vwap never updated for this symbol

    fire = book._vwap_check_entry("ITC", "CALL", 100.0)
    assert fire is False
    assert "ITC" not in book._vwap_armed


@pytest.mark.asyncio
async def test_on_fill_tags_vwap_when_reason_is_vwap_retest(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("ITC", 270, "CE")
    eid = "evt3"
    book._pending_fills[eid] = {
        "symbol": "ITC", "contract": contract, "qty": 375,
        "entry_price": 10.0, "reason": "vwap_retest",
    }
    fill = OiOrbFillEvent(
        client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id=eid,
        action="BUY", underlying="ITC", option_type="CE", strike=270,
        qty=375, fill_price=10.0, paper_mode=True,
    )
    await book._on_fill(fill)

    assert book._positions["ITC"]["sl_mechanic"] == "vwap"


@pytest.mark.asyncio
async def test_apply_historical_vwap_retest_fires_immediately_when_already_retested(monkeypatch):
    """2026-09-07 direct user spec: a stock added to the shortlist well after
    market open must check real intraday history for a VWAP-retest that
    already completed, and fire immediately if so -- not start its arm/
    retest state cold and wait for a brand new cross-and-retest cycle."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_pchange["MANAPPURAM"] = -3.49  # bearish -> PUT
    book._regime = "bearish"  # PUT is tradeable on both bullish and bearish days

    monkeypatch.setattr(screener, "historical_vwap_retest_check", lambda symbols_sides, cfg: {
        "MANAPPURAM": {"armed": True, "fired": True, "fire_ts": "09:47",
                        "fire_price": 327.30, "final_vwap": 327.79, "bars_replayed": 30},
    })

    handled = []
    async def _fake_handle_signal(sig):
        handled.append(sig)
    book._handle_signal = _fake_handle_signal
    book._evaluate_additive_filters = lambda sig: True  # isolate the historical-retest logic itself

    await book._apply_historical_vwap_retest(["MANAPPURAM"], book._screener_cfg)
    await asyncio.sleep(0.05)   # let the create_task'd _handle_signal actually run

    assert ("MANAPPURAM", "PUT") in book._already_fired
    assert len(handled) == 1
    assert handled[0].reason == "vwap_retest_historical"
    assert handled[0].trigger_price == 327.30


@pytest.mark.asyncio
async def test_apply_historical_vwap_retest_seeds_armed_state_without_firing(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_pchange["SOLARINDS"] = 2.19  # bullish -> CALL

    monkeypatch.setattr(screener, "historical_vwap_retest_check", lambda symbols_sides, cfg: {
        "SOLARINDS": {"armed": True, "fired": False, "fire_ts": None,
                       "fire_price": None, "final_vwap": 21824.27, "bars_replayed": 12},
    })

    handled = []
    async def _fake_handle_signal(sig):
        handled.append(sig)
    book._handle_signal = _fake_handle_signal

    await book._apply_historical_vwap_retest(["SOLARINDS"], book._screener_cfg)

    assert book._vwap_armed["SOLARINDS"] is True
    assert ("SOLARINDS", "CALL") not in book._already_fired
    assert handled == []


@pytest.mark.asyncio
async def test_apply_historical_vwap_retest_respects_regime_gate(monkeypatch):
    """2026-09-07 real incident: this path fired unconditionally, unlike the
    live tick loop (which always gates on side_allowed_by_regime) -- a
    SOLARINDS CALL fired here on a real BEARISH day, which should have
    blocked it (bearish day: CALL ignored, PUT tradeable)."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_pchange["SOLARINDS"] = 3.0  # bullish pchange -> CALL side
    book._regime = "bearish"  # CALL is NOT tradeable on a bearish day

    monkeypatch.setattr(screener, "historical_vwap_retest_check", lambda symbols_sides, cfg: {
        "SOLARINDS": {"armed": True, "fired": True, "fire_ts": "09:17",
                       "fire_price": 21530.00, "final_vwap": 21400.0, "bars_replayed": 20},
    })

    handled = []
    async def _fake_handle_signal(sig):
        handled.append(sig)
    book._handle_signal = _fake_handle_signal
    book._evaluate_additive_filters = lambda sig: True

    await book._apply_historical_vwap_retest(["SOLARINDS"], book._screener_cfg)
    await asyncio.sleep(0.05)

    assert ("SOLARINDS", "CALL") not in book._already_fired
    assert handled == []


@pytest.mark.asyncio
async def test_apply_historical_vwap_retest_missing_result_degrades_safely(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_pchange["GHOST"] = 1.0

    monkeypatch.setattr(screener, "historical_vwap_retest_check", lambda symbols_sides, cfg: {})

    await book._apply_historical_vwap_retest(["GHOST"], book._screener_cfg)

    assert "GHOST" not in book._vwap_armed
    assert ("GHOST", "CALL") not in book._already_fired


@pytest.mark.asyncio
async def test_apply_historical_vwap_retest_skips_symbol_already_checked_today(monkeypatch):
    """2026-09-10 real incident fix: this deterministic replay must run AT
    MOST ONCE per (symbol, side) per day -- a symbol already marked done in
    self._historical_check_done (restored from store.load_historical_check_done
    on a restart, or set earlier this same process lifetime) must never be
    re-evaluated, since it always finds/re-reports the SAME stale reference
    price (the GVT&D incident: "retested at 09:18, price=4638.90" re-logged
    across 10+ restarts while real spot had moved to ~4540-4547)."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_pchange["MANAPPURAM"] = -3.49  # bearish -> PUT
    book._regime = "bearish"
    book._historical_check_done.add(("MANAPPURAM", "PUT"))

    calls = []
    def _fake_check(symbols_sides, cfg):
        calls.append(dict(symbols_sides))
        return {"MANAPPURAM": {"armed": True, "fired": True, "fire_ts": "09:47",
                                "fire_price": 327.30, "final_vwap": 327.79, "bars_replayed": 30}}
    monkeypatch.setattr(screener, "historical_vwap_retest_check", _fake_check)

    await book._apply_historical_vwap_retest(["MANAPPURAM"], book._screener_cfg)

    assert calls == []  # the expensive replay must never even be called
    assert ("MANAPPURAM", "PUT") not in book._already_fired


@pytest.mark.asyncio
async def test_apply_historical_vwap_retest_marks_symbol_checked_after_processing(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_pchange["SOLARINDS"] = 2.19  # bullish -> CALL

    monkeypatch.setattr(screener, "historical_vwap_retest_check", lambda symbols_sides, cfg: {
        "SOLARINDS": {"armed": True, "fired": False, "fire_ts": None,
                       "fire_price": None, "final_vwap": 21824.27, "bars_replayed": 12},
    })

    await book._apply_historical_vwap_retest(["SOLARINDS"], book._screener_cfg)

    assert ("SOLARINDS", "CALL") in book._historical_check_done


@pytest.mark.asyncio
async def test_apply_historical_rolling_retest_skips_symbol_already_checked_today(monkeypatch):
    """Same restart-safety gate as the standard-variant test above, applied
    to the top20-mode rolling-retest counterpart."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._top20_mode = True
    book._shortlist_pchange["GVT&D"] = -2.0  # bearish -> PUT
    book._historical_check_done.add(("GVT&D", "PUT"))

    calls = []
    def _fake_check(symbols_sides, cfg, window_min):
        calls.append(dict(symbols_sides))
        return {}
    monkeypatch.setattr(screener, "historical_rolling_retest_check", _fake_check)

    await book._apply_historical_rolling_retest(["GVT&D"], book._screener_cfg)

    assert calls == []


@pytest.mark.asyncio
async def test_apply_historical_rolling_retest_marks_symbol_checked_after_processing(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._top20_mode = True
    book._shortlist_pchange["GVT&D"] = -2.0  # bearish -> PUT

    class _FakeTracker:
        pass

    monkeypatch.setattr(screener, "historical_rolling_retest_check",
                         lambda symbols_sides, cfg, window_min: {
                             "GVT&D": {"tracker": _FakeTracker(), "fired": False}})

    await book._apply_historical_rolling_retest(["GVT&D"], book._screener_cfg)

    assert ("GVT&D", "PUT") in book._historical_check_done


# ── _oi_spurt_history_loop must wait for _restore_from_db to finish before
# its first poll cycle (2026-09-10 real incident: LTM's historical retest
# check ran TWICE on the same restart -- 13:20:37 and again 13:28:57 -- because
# this loop and _daily_loop's own restore sequence are launched as separate,
# concurrent asyncio.create_task() calls with nothing synchronizing them; the
# OI-spurt poll fired at 13:28:51, before "restored 8 already-fired..." logged
# at 13:28:54) ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_oi_spurt_history_loop_waits_for_restore_from_db_ready(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._running = True
    book._restore_from_db_ready = False

    poll_calls = {"n": 0}
    async def _spy_poll(now, cfg):
        poll_calls["n"] += 1
        book._running = False
    book._do_oi_spurt_history_poll = _spy_poll

    sleeps = []
    real_sleep = asyncio.sleep
    async def _tracking_sleep(secs):
        sleeps.append(secs)
        if len(sleeps) > 20:
            book._running = False   # safety valve -- never spin forever in a test
        await real_sleep(0)
    monkeypatch.setattr(asyncio, "sleep", _tracking_sleep)

    await book._oi_spurt_history_loop()

    assert poll_calls["n"] == 0, "must never poll while restore-from-db hasn't finished yet"
    assert 1 in sleeps, "must be waiting via the 1s not-ready sleep"


@pytest.mark.asyncio
async def test_oi_spurt_history_loop_polls_once_restore_from_db_is_ready(monkeypatch):
    """IGNORE_TIME_WINDOWS is deliberately NOT set here -- this loop's own
    window check treats that flag as an ADDITIONAL always-skip condition
    (`if not (win_start<=now_key<win_end) or cfg.get("IGNORE_TIME_WINDOWS")`),
    unlike every other IGNORE_TIME_WINDOWS check in this file which bypasses
    a restriction. A fixed mid-session wall clock is used instead so the
    real window check passes normally."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._running = True
    book._restore_from_db_ready = True   # already finished, e.g. later in the same session

    import strategies.oi_orb_screener.engine as _engine_mod
    _fixed_now = datetime(2026, 9, 10, 13, 0, 0, tzinfo=_engine_mod.IST)
    class _FixedDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return _fixed_now
    monkeypatch.setattr(_engine_mod, "datetime", _FixedDT)

    poll_calls = {"n": 0}
    async def _spy_poll(now, cfg):
        poll_calls["n"] += 1
        book._running = False
    book._do_oi_spurt_history_poll = _spy_poll
    monkeypatch.setattr(asyncio, "sleep", _async_return(None))

    await book._oi_spurt_history_loop()

    assert poll_calls["n"] == 1


@pytest.mark.asyncio
async def test_daily_loop_sets_restore_from_db_ready_after_successful_restore(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._running = True
    book._today = None
    assert book._restore_from_db_ready is False

    book._restore_from_db = _async_return(None)
    async def _stop_pipeline():
        book._running = False
    book._run_today_pipeline = _stop_pipeline
    monkeypatch.setattr(asyncio, "sleep", _async_return(None))

    await book._daily_loop()

    assert book._restore_from_db_ready is True


@pytest.mark.asyncio
async def test_daily_loop_sets_restore_from_db_ready_even_if_restore_raises(monkeypatch):
    """The readiness flag must be set even on failure -- a restore exception
    must never leave _oi_spurt_history_loop blocked forever."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._running = True
    book._today = None

    async def _raise():
        raise RuntimeError("simulated restore failure")
    book._restore_from_db = _raise
    async def _stop_pipeline():
        book._running = False
    book._run_today_pipeline = _stop_pipeline
    monkeypatch.setattr(asyncio, "sleep", _async_return(None))

    await book._daily_loop()

    assert book._restore_from_db_ready is True


# ── 2026-09-16, direct user spec: pre-entry trap-target-already-touched
# gate. Before actually taking a genuine VWAP-retest signal, check whether
# today's own 180-min multi-day trap target has already been touched by
# real price -- if so, the target is spent, skip the trade and watch for a
# fresh day-high (CALL) / day-low (PUT) breach to re-trigger with a freshly
# recomputed zone. ────────────────────────────────────────────────────────

def _with_open(rows: list) -> list:
    """The shared _row/_rows_flat helpers above don't set "open" (existing
    callers build Bar objects with open=close explicitly) -- real Upstox
    rows always carry it (_parse_candles), and _check_trap_target_touched_
    today reads r["open"] directly to match _seed_trap_exit_state's own
    established convention. Adds it defaulting to close for these tests."""
    out = []
    for r in rows:
        r = dict(r)
        r.setdefault("open", r["close"])
        out.append(r)
    return out


@pytest.mark.asyncio
async def test_check_trap_target_touched_today_true_when_real_bar_inside_zone(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr(stock_resolve, "resolve_eq_instrument_key", lambda sym: "NSE_EQ|INE000A01001")
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_range_1m", _async_return([]))

    today_rows = _rows_flat(9, 15, 200, 90.0, 10.0)
    today_rows.append(_row("13:00", 102.0, 101.0, 102.0, 10.0))   # touches [100,105]
    today_rows += _rows_flat(13, 1, 200, 90.0, 10.0)
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_intraday_1m",
                         _async_return(_with_open(today_rows)))

    fixed_zone = {"zone_lo": 100.0, "zone_hi": 105.0,
                  "lock_ts": datetime(2026, 8, 24, 9, 0, 0, tzinfo=IST)}
    monkeypatch.setattr(screener, "bull_trap_zones", lambda htf: [fixed_zone])

    import strategies.oi_orb_screener.engine as _engine_mod
    _fixed_now = datetime(2026, 8, 24, 16, 0, 0, tzinfo=_engine_mod.IST)
    class _FixedDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return _fixed_now
    monkeypatch.setattr(_engine_mod, "datetime", _FixedDT)

    touched, zone = await book._check_trap_target_touched_today("TESTSTOCK", "CALL")
    assert touched is True
    assert zone == fixed_zone


@pytest.mark.asyncio
async def test_check_trap_target_touched_today_false_when_no_bar_inside_zone(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr(stock_resolve, "resolve_eq_instrument_key", lambda sym: "NSE_EQ|INE000A01001")
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_range_1m", _async_return([]))

    today_rows = _rows_flat(9, 15, 200, 90.0, 10.0) + _rows_flat(13, 0, 200, 90.0, 10.0)
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_intraday_1m",
                         _async_return(_with_open(today_rows)))

    fixed_zone = {"zone_lo": 100.0, "zone_hi": 105.0,
                  "lock_ts": datetime(2026, 8, 24, 9, 0, 0, tzinfo=IST)}
    monkeypatch.setattr(screener, "bull_trap_zones", lambda htf: [fixed_zone])

    import strategies.oi_orb_screener.engine as _engine_mod
    _fixed_now = datetime(2026, 8, 24, 16, 0, 0, tzinfo=_engine_mod.IST)
    class _FixedDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return _fixed_now
    monkeypatch.setattr(_engine_mod, "datetime", _FixedDT)

    touched, zone = await book._check_trap_target_touched_today("TESTSTOCK", "CALL")
    assert touched is False
    assert zone == fixed_zone


@pytest.mark.asyncio
async def test_check_trap_target_touched_today_best_effort_false_on_no_instrument_key(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr(stock_resolve, "resolve_eq_instrument_key", lambda sym: "")
    touched, zone = await book._check_trap_target_touched_today("TESTSTOCK", "CALL")
    assert touched is False
    assert zone is None


@pytest.mark.asyncio
async def test_check_trap_target_touched_today_best_effort_false_on_no_token(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr(stock_resolve, "resolve_eq_instrument_key", lambda sym: "NSE_EQ|INE000A01001")
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": ""})
    touched, zone = await book._check_trap_target_touched_today("TESTSTOCK", "CALL")
    assert touched is False
    assert zone is None


def test_reset_session_clears_trap_gate_state():
    bus = _FakeBus()
    book = _make_book(bus)
    book._trap_gate_skipped[("SYM", "CALL")] = {"extreme": 100.0, "last_check_ts": 0.0}
    book.reset_session()
    assert book._trap_gate_skipped == {}


# ── 2026-09-16, direct user spec: history must show the real candle/bucket
# time + values that fired an SL, not just the reason code. ────────────────

@pytest.mark.asyncio
async def test_fire_vwap_close_sl_detail_includes_real_candle_window_and_values():
    from strategies.core.trap_zone_utils import Bar as _Bar
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("TESTSTOCK", 100, "CE")
    book._positions["TESTSTOCK"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 9, 16, 9, 30, tzinfo=IST), "sl_mechanic": "vwap",
    }
    captured = {}
    async def _spy_emit_close(symbol, pos, reason, detail=""):
        captured["reason"] = reason
        captured["detail"] = detail
    book._emit_close = _spy_emit_close

    bar = _Bar(ts=datetime(2026, 9, 16, 10, 15, tzinfo=IST), open=553.0, high=555.0,
               low=550.0, close=553.0)
    await book._fire_vwap_close_sl("TESTSTOCK", "CALL", 553.20, 551.65, candle_bar=bar)

    assert captured["reason"] == "vwap_close_sl"
    assert "[10:15-10:35)" in captured["detail"]
    assert "close=553.00" in captured["detail"]
    assert "vwap=551.65" in captured["detail"]
    assert "live_ltp=553.20" in captured["detail"]


@pytest.mark.asyncio
async def test_fire_vwap_close_sl_detail_degrades_without_candle_bar():
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("TESTSTOCK", 100, "CE")
    book._positions["TESTSTOCK"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 9, 16, 9, 30, tzinfo=IST), "sl_mechanic": "vwap",
    }
    captured = {}
    async def _spy_emit_close(symbol, pos, reason, detail=""):
        captured["detail"] = detail
    book._emit_close = _spy_emit_close

    await book._fire_vwap_close_sl("TESTSTOCK", "CALL", 553.20, 551.65)

    assert "candle=" not in captured["detail"]
    assert "underlying_ltp=553.20" in captured["detail"]
    assert "vwap=551.65" in captured["detail"]


# ── entry-loop wiring: drives the real _run_today_pipeline loop body, same
# heavy-mock scaffold as test_exit_check_loop_uses_contract_side_not_flipped_
# pchange above, to prove the gate is actually wired into the live entry
# path, not just correct in isolation. ─────────────────────────────────────

def _drive_entry_loop_scaffold(book, monkeypatch, fixed_now: datetime):
    """Real shortlist DataFrame (not a manual self._shortlist_symbols
    pre-set) -- the pipeline's own morning-build step unconditionally
    overwrites self._shortlist_symbols/_shortlist_pchange from whatever
    build_shortlist returns, so a pre-set list would just get wiped."""
    import strategies.oi_orb_screener.engine as _engine_mod
    class _FixedDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now
    monkeypatch.setattr(_engine_mod, "datetime", _FixedDT)
    monkeypatch.setattr(screener, "NSESession", _FakeNSESession)
    import pandas as pd
    df = pd.DataFrame([{"symbol": "TESTSTOCK", "pChange": 3.0, "oi_spurt_pct": 8.0,
                         "score": 0.5, "lastPrice": 100.0, "previousClose": 97.0}])
    monkeypatch.setattr(screener, "build_shortlist", lambda nse, cfg: (df, 0.4))
    monkeypatch.setattr(screener, "backfill_orb_from_yahoo", lambda *a, **k: None)
    monkeypatch.setattr(screener, "backfill_vwap_from_yahoo", lambda *a, **k: None)
    monkeypatch.setattr(screener, "fetch_fno_price_universe",
                         lambda nse: pd.DataFrame(columns=["symbol", "lastPrice", "pChange"]))
    monkeypatch.setattr(screener, "side_allowed_by_regime", lambda side, regime, filt: True)
    monkeypatch.setattr(asyncio, "to_thread", lambda fn, *a, **k: _async_return(fn(*a, **k))())
    book._maybe_run_afternoon_scan = _async_return(None)
    book._ensure_spot_feed = lambda *a, **k: None
    book._screener_cfg["IGNORE_TIME_WINDOWS"] = True
    book._running = True
    book._regime = "BULLISH"


@pytest.mark.asyncio
async def test_entry_loop_skips_trade_when_trap_target_already_touched_today(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    _drive_entry_loop_scaffold(book, monkeypatch, datetime(2026, 9, 16, 10, 0, 0, tzinfo=IST))
    book._live_price = lambda sym, live_df, log_source=False: 100.0
    book._vwap_check_entry = lambda sym, side, ltp: True   # genuine retest fires

    emitted = []
    async def _spy_emit(*a, **kw):
        emitted.append((a, kw))
    book._emit_vwap_signal = _spy_emit

    checked = []
    async def _spy_touched(sym, side):
        checked.append((sym, side))
        return True, {"zone_lo": 95.0, "zone_hi": 105.0}   # already touched today
    book._check_trap_target_touched_today = _spy_touched

    # Run exactly one cycle of the pipeline's inner while-loop: patch
    # asyncio.sleep to flip _running off the first time the loop body itself
    # calls it (NOT to_thread's own internal sleeps, already bypassed above).
    async def _sleep_once(_secs):
        book._running = False
    monkeypatch.setattr(asyncio, "sleep", _sleep_once)

    await book._run_today_pipeline()

    assert checked == [("TESTSTOCK", "CALL")]
    assert emitted == []   # entry was skipped, not fired
    assert ("TESTSTOCK", "CALL") not in book._already_fired
    assert ("TESTSTOCK", "CALL") in book._trap_gate_skipped
    assert book._trap_gate_skipped[("TESTSTOCK", "CALL")]["extreme"] == 100.0


@pytest.mark.asyncio
async def test_entry_loop_retriggers_immediately_on_fresh_day_high_when_zone_no_longer_touched(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    _drive_entry_loop_scaffold(book, monkeypatch, datetime(2026, 9, 16, 11, 0, 0, tzinfo=IST))
    book._live_price = lambda sym, live_df, log_source=False: 101.0   # new high, breaches 100.0
    book._trap_gate_skipped[("TESTSTOCK", "CALL")] = {"extreme": 100.0, "last_check_ts": 0.0}  # old -> not throttled

    emitted = []
    async def _spy_emit(sym, side, ltp, reason, orb_lvl, ts_str, label):
        emitted.append((sym, side, ltp, reason, label))
    book._emit_vwap_signal = _spy_emit

    checked = []
    async def _spy_touched(sym, side):
        checked.append((sym, side))
        return False, None   # fresh zone -- no longer touched
    book._check_trap_target_touched_today = _spy_touched

    async def _sleep_once(_secs):
        book._running = False
    monkeypatch.setattr(asyncio, "sleep", _sleep_once)

    await book._run_today_pipeline()

    assert checked == [("TESTSTOCK", "CALL")]
    assert len(emitted) == 1
    assert emitted[0][0] == "TESTSTOCK"
    assert emitted[0][1] == "CALL"
    assert emitted[0][3] == "trap_gate_new_extreme_retrigger"
    assert ("TESTSTOCK", "CALL") in book._already_fired
    assert ("TESTSTOCK", "CALL") not in book._trap_gate_skipped


@pytest.mark.asyncio
async def test_entry_loop_gate_recheck_is_throttled(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    _drive_entry_loop_scaffold(book, monkeypatch, datetime(2026, 9, 16, 11, 0, 0, tzinfo=IST))
    book._live_price = lambda sym, live_df, log_source=False: 101.0
    recent_ts = _time.monotonic()
    book._trap_gate_skipped[("TESTSTOCK", "CALL")] = {"extreme": 100.0, "last_check_ts": recent_ts}

    checked = []
    async def _spy_touched(sym, side):
        checked.append((sym, side))
        return False, None
    book._check_trap_target_touched_today = _spy_touched
    emitted = []
    async def _spy_emit(*a, **kw):
        emitted.append((a, kw))
    book._emit_vwap_signal = _spy_emit

    async def _sleep_once(_secs):
        book._running = False
    monkeypatch.setattr(asyncio, "sleep", _sleep_once)

    await book._run_today_pipeline()

    assert checked == []   # throttled -- no recheck performed
    assert emitted == []
    assert book._trap_gate_skipped[("TESTSTOCK", "CALL")]["extreme"] == 101.0   # still updated


# ── 2026-09-16, direct user spec: futures-OI-regime directional gate.
# INCREASING (>+1%) -> yesterday's candle decides CALL/PUT. DECREASING
# (<-5%) -> today's pChange-sign trend decides. Otherwise -> no trade.
# Empirically verified live (2026-09-15/16) that Upstox V3's previous_oi
# equals the previous session's closing futures OI (matched
# fetch_upstox_daily's own 'oi' exactly on 4 real contracts). ─────────────

def _v3_quote(oi: float, previous_oi: float) -> dict:
    return {"oi": oi, "previous_oi": previous_oi}


@pytest.mark.asyncio
async def test_oi_regime_increasing_uses_previous_day_green_candle_for_call(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.load_futures_only_sync",
                         lambda sym, today=None: None)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.get_futures_upstox",
                         lambda sym: "NSE_FO|12345")
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})
    # +2% OI change -> INCREASING regime
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_v3_quote",
                         _async_return(_v3_quote(oi=102.0, previous_oi=100.0)))
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_daily",
                         _async_return([{"ts": "2026-09-15", "open": 100.0, "high": 110.0,
                                          "low": 99.0, "close": 108.0, "volume": 1000, "oi": 100}]))

    side = await book._compute_oi_regime_side("TESTSTOCK")
    assert side == "CALL"


@pytest.mark.asyncio
async def test_oi_regime_increasing_uses_previous_day_red_candle_for_put(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.load_futures_only_sync",
                         lambda sym, today=None: None)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.get_futures_upstox",
                         lambda sym: "NSE_FO|12345")
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_v3_quote",
                         _async_return(_v3_quote(oi=105.0, previous_oi=100.0)))
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_daily",
                         _async_return([{"ts": "2026-09-15", "open": 108.0, "high": 110.0,
                                          "low": 99.0, "close": 100.0, "volume": 1000, "oi": 100}]))

    side = await book._compute_oi_regime_side("TESTSTOCK")
    assert side == "PUT"


@pytest.mark.asyncio
async def test_oi_regime_decreasing_uses_todays_pchange_trend_not_yesterdays_candle(monkeypatch):
    """Direct spec: yesterday's candle must be DISCARDED entirely in the
    DECREASING regime -- seed it bearish (red) but today's pChange positive
    must still win, proving the two regimes never mix."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_pchange["TESTSTOCK"] = 3.5   # today bullish
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.load_futures_only_sync",
                         lambda sym, today=None: None)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.get_futures_upstox",
                         lambda sym: "NSE_FO|12345")
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})
    # -8% OI change -> DECREASING regime
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_v3_quote",
                         _async_return(_v3_quote(oi=92.0, previous_oi=100.0)))
    daily_calls = []
    async def _must_not_be_called(*a, **k):
        daily_calls.append(a)
        return []
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_daily", _must_not_be_called)

    side = await book._compute_oi_regime_side("TESTSTOCK")
    assert side == "CALL"          # today's bullish pChange won
    assert daily_calls == []       # yesterday's candle never even fetched


@pytest.mark.asyncio
async def test_oi_regime_decreasing_bearish_trend_gives_put(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_pchange["TESTSTOCK"] = -3.5
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.load_futures_only_sync",
                         lambda sym, today=None: None)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.get_futures_upstox",
                         lambda sym: "NSE_FO|12345")
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_v3_quote",
                         _async_return(_v3_quote(oi=90.0, previous_oi=100.0)))

    side = await book._compute_oi_regime_side("TESTSTOCK")
    assert side == "PUT"


@pytest.mark.asyncio
async def test_oi_regime_neutral_band_blocks_trade():
    bus = _FakeBus()
    book = _make_book(bus)
    import strategies.oi_orb_screener.engine as _engine_mod

    async def _fake_quote(key, token):
        return {"oi": 100.5, "previous_oi": 100.0}   # +0.5% -- inside -5%..+1%
    import data_layer.historical_candles as hc
    orig = hc.fetch_upstox_v3_quote
    hc.fetch_upstox_v3_quote = _fake_quote
    try:
        import strategies.oi_orb_screener.stock_resolve as sr
        from data_layer.instrument_registry import REGISTRY
        REGISTRY.load_futures_only_sync = lambda sym, today=None: None
        REGISTRY.get_futures_upstox = lambda sym: "NSE_FO|12345"
        from data_layer.client_db import ClientDB
        ClientDB.get_feeder_creds_sync = lambda self, provider: {"access_token": "tok"}
        side = await book._compute_oi_regime_side("TESTSTOCK")
        assert side is None
    finally:
        hc.fetch_upstox_v3_quote = orig


@pytest.mark.asyncio
async def test_oi_regime_best_effort_blocks_on_no_futures_key(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.load_futures_only_sync",
                         lambda sym, today=None: None)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.get_futures_upstox",
                         lambda sym: "")
    side = await book._compute_oi_regime_side("TESTSTOCK")
    assert side is None


@pytest.mark.asyncio
async def test_oi_regime_best_effort_blocks_on_fetch_exception(monkeypatch):
    """CRITICAL distinction from every other best-effort seed in this file:
    this gate is a hard entry prerequisite per direct spec ('Otherwise: No
    trade today'), so a failure must BLOCK the trade (return None), not
    silently let it proceed."""
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.load_futures_only_sync",
                         lambda sym, today=None: (_ for _ in ()).throw(RuntimeError("boom")))
    side = await book._compute_oi_regime_side("TESTSTOCK")
    assert side is None


def test_reset_session_clears_oi_regime_state():
    bus = _FakeBus()
    book = _make_book(bus)
    book._oi_regime_side["SYM"] = "CALL"
    book._oi_regime_computed.add("SYM")
    book.reset_session()
    assert book._oi_regime_side == {}
    assert book._oi_regime_computed == set()


@pytest.mark.asyncio
async def test_entry_loop_uses_oi_regime_side_when_gate_enabled(monkeypatch):
    """Drives the real _run_today_pipeline entry loop to prove the gate is
    actually wired in, not just correct in isolation -- and that it
    REPLACES the plain pChange-based side, matching direct spec (previous
    day's candle can override today's initial price action)."""
    bus = _FakeBus()
    book = _make_book(bus)
    _drive_entry_loop_scaffold(book, monkeypatch, datetime(2026, 9, 16, 9, 20, 0, tzinfo=IST))
    book._screener_cfg["OI_REGIME_GATE_ENABLED"] = True
    book._live_price = lambda sym, live_df, log_source=False: 100.0
    book._vwap_check_entry = lambda sym, side, ltp: True

    computed_calls = []
    async def _spy_compute(sym):
        computed_calls.append(sym)
        return "PUT"   # OI-regime says PUT even though shortlist pChange (+3.0) says CALL
    book._compute_oi_regime_side = _spy_compute

    emitted = []
    async def _spy_emit(sym, side, ltp, reason, orb_lvl, ts_str, label):
        emitted.append((sym, side))
    book._emit_vwap_signal = _spy_emit

    async def _sleep_once(_secs):
        book._running = False
    monkeypatch.setattr(asyncio, "sleep", _sleep_once)

    await book._run_today_pipeline()

    assert computed_calls == ["TESTSTOCK"]
    assert len(emitted) == 1
    assert emitted[0] == ("TESTSTOCK", "PUT")   # OI-regime side won, not pChange-derived CALL
    assert "TESTSTOCK" in book._oi_regime_computed
    assert book._oi_regime_side["TESTSTOCK"] == "PUT"


@pytest.mark.asyncio
async def test_entry_loop_skips_symbol_before_oi_regime_check_time(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    _drive_entry_loop_scaffold(book, monkeypatch, datetime(2026, 9, 16, 9, 10, 0, tzinfo=IST))
    book._screener_cfg["OI_REGIME_GATE_ENABLED"] = True
    book._screener_cfg["IGNORE_TIME_WINDOWS"] = False   # must respect the check-time gate itself
    book._live_price = lambda sym, live_df, log_source=False: 100.0

    computed_calls = []
    async def _spy_compute(sym):
        computed_calls.append(sym)
        return "CALL"
    book._compute_oi_regime_side = _spy_compute

    async def _sleep_once(_secs):
        book._running = False
    monkeypatch.setattr(asyncio, "sleep", _sleep_once)

    await book._run_today_pipeline()

    assert computed_calls == []   # too early -- 09:10 < 09:16 check time
    assert "TESTSTOCK" not in book._oi_regime_computed


@pytest.mark.asyncio
async def test_entry_loop_blocks_symbol_when_oi_regime_neutral(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    _drive_entry_loop_scaffold(book, monkeypatch, datetime(2026, 9, 16, 9, 20, 0, tzinfo=IST))
    book._screener_cfg["OI_REGIME_GATE_ENABLED"] = True
    book._live_price = lambda sym, live_df, log_source=False: 100.0
    book._vwap_check_entry = lambda sym, side, ltp: True

    async def _spy_compute(sym):
        return None   # neutral/blocked
    book._compute_oi_regime_side = _spy_compute

    emitted = []
    async def _spy_emit(*a, **kw):
        emitted.append((a, kw))
    book._emit_vwap_signal = _spy_emit

    async def _sleep_once(_secs):
        book._running = False
    monkeypatch.setattr(asyncio, "sleep", _sleep_once)

    await book._run_today_pipeline()

    assert emitted == []
    assert "TESTSTOCK" in book._oi_regime_computed
    assert book._oi_regime_side["TESTSTOCK"] is None


@pytest.mark.asyncio
async def test_entry_loop_computes_oi_regime_only_once_per_symbol_per_day(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    _drive_entry_loop_scaffold(book, monkeypatch, datetime(2026, 9, 16, 9, 20, 0, tzinfo=IST))
    book._screener_cfg["OI_REGIME_GATE_ENABLED"] = True
    book._live_price = lambda sym, live_df, log_source=False: 100.0
    book._vwap_check_entry = lambda sym, side, ltp: False   # never actually fires

    computed_calls = []
    async def _spy_compute(sym):
        computed_calls.append(sym)
        return "CALL"
    book._compute_oi_regime_side = _spy_compute

    call_count = {"n": 0}
    async def _sleep_twice(_secs):
        call_count["n"] += 1
        if call_count["n"] >= 2:
            book._running = False
    monkeypatch.setattr(asyncio, "sleep", _sleep_twice)

    await book._run_today_pipeline()

    assert computed_calls == ["TESTSTOCK"]   # computed once, not once per cycle


# ── 2026-09-16, direct user spec: real-time futures-OI histogram in the
# dashboard for every shortlisted stock, independent of whether
# OI_REGIME_GATE_ENABLED is on. Backed by store.futures_oi_history, written by
# _do_futures_oi_history_poll (a separate loop from _oi_spurt_history_loop --
# different data source entirely, Upstox futures V3 quote not NSE OI-spurt).
# ─────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_do_futures_oi_history_poll_records_every_shortlisted_symbol(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_symbols = ["TESTSTOCK", "OTHERSTOCK"]

    async def _fake_snapshot(sym):
        return {"TESTSTOCK": (102.0, 100.0, 2.0), "OTHERSTOCK": (95.0, 100.0, -5.0)}[sym]
    book._fetch_futures_oi_snapshot = _fake_snapshot

    recorded = []
    monkeypatch.setattr(store, "record_futures_oi_history",
                         lambda *a, **k: recorded.append((a, k)))

    await book._do_futures_oi_history_poll(datetime(2026, 9, 16, 9, 16, tzinfo=IST))

    assert len(recorded) == 2
    syms = {call[0][2] for call in recorded}   # (client_id, binding_id, symbol, poll_ts, ...)
    assert syms == {"TESTSTOCK", "OTHERSTOCK"}


@pytest.mark.asyncio
async def test_do_futures_oi_history_poll_skips_symbol_with_no_snapshot(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_symbols = ["TESTSTOCK"]
    book._fetch_futures_oi_snapshot = _async_return(None)

    recorded = []
    monkeypatch.setattr(store, "record_futures_oi_history",
                         lambda *a, **k: recorded.append((a, k)))

    await book._do_futures_oi_history_poll(datetime(2026, 9, 16, 9, 16, tzinfo=IST))

    assert recorded == []


@pytest.mark.asyncio
async def test_do_futures_oi_history_poll_one_symbol_failure_does_not_abort_the_rest(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._shortlist_symbols = ["BADSTOCK", "GOODSTOCK"]

    async def _fake_snapshot(sym):
        if sym == "BADSTOCK":
            raise RuntimeError("boom")
        return (102.0, 100.0, 2.0)
    book._fetch_futures_oi_snapshot = _fake_snapshot

    recorded = []
    monkeypatch.setattr(store, "record_futures_oi_history",
                         lambda *a, **k: recorded.append((a, k)))

    await book._do_futures_oi_history_poll(datetime(2026, 9, 16, 9, 16, tzinfo=IST))

    assert len(recorded) == 1
    assert recorded[0][0][2] == "GOODSTOCK"


@pytest.mark.asyncio
async def test_fetch_futures_oi_snapshot_returns_tuple_from_live_v3_quote(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.load_futures_only_sync",
                         lambda sym, today=None: None)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.get_futures_upstox",
                         lambda sym: "NSE_FO|12345")
    monkeypatch.setattr("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                         lambda self, provider: {"access_token": "tok"})
    monkeypatch.setattr("data_layer.historical_candles.fetch_upstox_v3_quote",
                         _async_return(_v3_quote(oi=102.0, previous_oi=100.0)))

    snap = await book._fetch_futures_oi_snapshot("TESTSTOCK")

    assert snap == (102.0, 100.0, 2.0)


@pytest.mark.asyncio
async def test_fetch_futures_oi_snapshot_returns_none_on_no_futures_key(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.load_futures_only_sync",
                         lambda sym, today=None: None)
    monkeypatch.setattr("data_layer.instrument_registry.REGISTRY.get_futures_upstox",
                         lambda sym: "")

    snap = await book._fetch_futures_oi_snapshot("TESTSTOCK")

    assert snap is None


@pytest.mark.asyncio
async def test_futures_oi_history_loop_polls_all_shortlisted_symbols_and_never_touches_trading_state(monkeypatch):
    """Same zero-trading-effect guarantee as _oi_spurt_history_loop -- this
    loop must never read/write _positions/_rejected/_already_fired."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._running = True
    book._restore_from_db_ready = True
    book._shortlist_symbols = ["TESTSTOCK"]
    book._screener_cfg = {"FUTURES_OI_HISTORY_POLL_SEC": 0.0}
    book._rejected = {("SHOULD", "NEVERTOUCH")}
    book._already_fired = {("SHOULD", "NEVERTOUCH")}

    polled = []
    async def _fake_poll(now):
        polled.append(now)
    book._do_futures_oi_history_poll = _fake_poll

    async def _sleep_once(_secs):
        book._running = False
    monkeypatch.setattr(asyncio, "sleep", _sleep_once)

    await book._futures_oi_history_loop()

    assert len(polled) == 1
    assert book._rejected == {("SHOULD", "NEVERTOUCH")}
    assert book._already_fired == {("SHOULD", "NEVERTOUCH")}


@pytest.mark.asyncio
async def test_futures_oi_history_loop_waits_for_restore_before_first_poll(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._running = True
    book._restore_from_db_ready = False
    book._shortlist_symbols = ["TESTSTOCK"]

    polled = []
    async def _fake_poll(now):
        polled.append(now)
    book._do_futures_oi_history_poll = _fake_poll

    calls = {"n": 0}
    async def _sleep_then_stop(_secs):
        calls["n"] += 1
        if calls["n"] >= 2:
            book._running = False
    monkeypatch.setattr(asyncio, "sleep", _sleep_then_stop)

    await book._futures_oi_history_loop()

    assert polled == []   # never polled -- restore never became ready


@pytest.mark.asyncio
async def test_futures_oi_history_loop_disabled_via_config_never_polls(monkeypatch):
    bus = _FakeBus()
    book = _make_book(bus)
    book._running = True
    book._restore_from_db_ready = True
    book._shortlist_symbols = ["TESTSTOCK"]
    book._screener_cfg = {"FUTURES_OI_HISTORY_ENABLED": False}

    polled = []
    async def _fake_poll(now):
        polled.append(now)
    book._do_futures_oi_history_poll = _fake_poll

    calls = {"n": 0}
    async def _sleep_then_stop(_secs):
        calls["n"] += 1
        if calls["n"] >= 2:
            book._running = False
    monkeypatch.setattr(asyncio, "sleep", _sleep_then_stop)

    await book._futures_oi_history_loop()

    assert polled == []


def test_reset_session_does_not_crash_futures_oi_hist_last_poll_ts():
    bus = _FakeBus()
    book = _make_book(bus)
    book._futures_oi_hist_last_poll_ts = 12345.0
    book.reset_session()
    assert book._futures_oi_hist_last_poll_ts == 0.0
