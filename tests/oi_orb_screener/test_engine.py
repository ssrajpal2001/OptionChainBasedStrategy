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
from datetime import date, datetime, time as dtime

import pytest

from config.global_config import Topic
from data_layer.base_feeder import OptionTick
from strategies.oi_orb_screener import screener, stock_resolve
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy
from strategies.oi_orb_screener.events import OiOrbFillEvent


class _FakeGlobalFeeder:
    def __init__(self) -> None:
        self.subscribed_tokens: list = []

    async def subscribe_tokens(self, tokens):
        self.subscribed_tokens.extend(tokens)


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
async def test_on_fill_confirms_entry_and_tracks_position():
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("SIEMENS", 4050, "CE")
    book._pending_contracts["SIEMENS"] = contract
    book._pending_fills["EVT1"] = {
        "symbol": "SIEMENS", "contract": contract, "qty": 300, "entry_price": 105.0, "reason": "orb_high_breakout",
    }

    book._on_fill(OiOrbFillEvent(
        action="BUY", underlying="SIEMENS", option_type="CE", strike=4050, fill_price=106.2,
        qty=300, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id="EVT1", paper_mode=True,
    ))

    assert "SIEMENS" in book._positions
    assert book._positions["SIEMENS"]["entry_price"] == 106.2
    assert "SIEMENS" not in book._pending_contracts
    assert "EVT1" not in book._pending_fills


def test_on_fill_entry_aborted_discards_pending():
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("SIEMENS", 4050, "CE")
    book._pending_contracts["SIEMENS"] = contract
    book._pending_fills["EVT1"] = {
        "symbol": "SIEMENS", "contract": contract, "qty": 300, "entry_price": 105.0, "reason": "orb_high_breakout",
    }

    book._on_fill(OiOrbFillEvent(
        action="BUY", underlying="SIEMENS", option_type="CE", strike=4050, fill_price=0.0,
        qty=300, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id="EVT1",
        entry_aborted=True,
    ))

    assert "SIEMENS" not in book._positions
    assert "SIEMENS" not in book._pending_contracts


def test_multiple_concurrent_positions_tracked_independently():
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
        book._on_fill(OiOrbFillEvent(
            action="BUY", underlying=sym, option_type="CE", strike=strike, fill_price=10.0 + i,
            qty=100 * (i + 1), client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id=eid,
        ))

    assert set(book._positions.keys()) == {"MANAPPURAM", "SIEMENS"}
    assert book._positions["MANAPPURAM"]["qty"] == 100
    assert book._positions["SIEMENS"]["qty"] == 200


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
    trading day (the daily pipeline only runs once per calendar day)."""
    bus = _FakeBus()
    book = _make_book(bus)
    book._screener_cfg["IGNORE_TIME_WINDOWS"] = True
    book._running = True

    monkeypatch.setattr(screener, "NSESession", _FakeNSESession)
    monkeypatch.setattr(asyncio, "sleep", _async_return(None))

    calls = {"n": 0}
    monkeypatch.setattr(screener, "build_shortlist",
                         lambda nse, cfg: _flaky_build_shortlist_sync(nse, cfg, calls))

    await book._run_today_pipeline()

    assert calls["n"] == 3   # 2 failures + 1 success, never hit the max-attempts cap


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
