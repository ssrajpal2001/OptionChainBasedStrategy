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


def test_on_fill_buy_resets_sr_state_and_subscribes_spot_feed():
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._pending_fills["EVT1"] = {
        "symbol": "MANAPPURAM", "contract": contract, "qty": 100,
        "entry_price": 10.0, "reason": "signal",
    }
    # Stale state from an earlier (already-closed) run on this same symbol today --
    # must not leak into the freshly-opened position's own S&R tracking.
    book._sr_calc.states["MANAPPURAM"] = {"stale": True}
    book._live_sl["MANAPPURAM"] = 999.0

    asyncio.run(book._on_fill(OiOrbFillEvent(
        action="BUY", underlying="MANAPPURAM", option_type="CE", strike=365, fill_price=10.0,
        qty=100, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id="EVT1",
        paper_mode=True,
    )))

    assert "MANAPPURAM" not in book._sr_calc.states
    assert "MANAPPURAM" not in book._live_sl
    assert bus._global_feeder.subscribed_equity == [("NSE:MANAPPURAM-EQ", "MANAPPURAM")]


@pytest.mark.asyncio
async def test_sr_sl_establishes_after_two_bars_and_breaches_on_the_next_tick():
    """Drives real 1-min bars through the actual SupportResistanceCalculator
    (not a stand-in) for a CALL position: bar1 (09:15) high=100/low=95, bar2
    (09:16) high=105/low=97 -- a clean breakout-high bounce -- establishes S1
    at bar1's low (95) the moment bar2 closes. A live tick at 94 (below 95)
    must then close the position immediately, on that tick, not waiting for
    bar3 to close."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }

    await book._update_sr_and_check_sl("MANAPPURAM", 100.0, datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST))
    await book._update_sr_and_check_sl("MANAPPURAM", 95.0, datetime(2026, 8, 26, 9, 15, 40, tzinfo=IST))
    # bar1 (09:15) closes on this next tick, which starts bar2 (09:16)
    await book._update_sr_and_check_sl("MANAPPURAM", 102.0, datetime(2026, 8, 26, 9, 16, 10, tzinfo=IST))
    await book._update_sr_and_check_sl("MANAPPURAM", 105.0, datetime(2026, 8, 26, 9, 16, 40, tzinfo=IST))
    await book._update_sr_and_check_sl("MANAPPURAM", 97.0, datetime(2026, 8, 26, 9, 16, 50, tzinfo=IST))
    # bar2 (09:16, high=105/low=97) closes on this next tick -> S1 established at 95
    await book._update_sr_and_check_sl("MANAPPURAM", 99.0, datetime(2026, 8, 26, 9, 17, 10, tzinfo=IST))

    assert book._live_sl["MANAPPURAM"] == 95.0
    assert "MANAPPURAM" in book._positions   # not breached yet (99 > 95)

    sell_events_before = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert sell_events_before == []

    # A live tick (same forming bar, no new bar close needed) breaches SL immediately.
    await book._update_sr_and_check_sl("MANAPPURAM", 94.0, datetime(2026, 8, 26, 9, 17, 20, tzinfo=IST))

    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert len(sell_events) == 1
    assert sell_events[0].underlying == "MANAPPURAM"
    assert "MANAPPURAM" in book._eod_closing


@pytest.mark.asyncio
async def test_sr_sl_no_close_while_sl_not_yet_established():
    """Only ONE candle has closed so far -- the S&R tracker hasn't confirmed
    any S1/R1 yet, so no SL exists and a big drop must NOT trigger a close
    (the position runs on the hard risk cap alone during this window)."""
    bus = _FakeBus()
    book = _make_book(bus)
    contract = _contract("MANAPPURAM", 365, "CE")
    book._positions["MANAPPURAM"] = {
        "contract": contract, "qty": 100, "entry_price": 10.0, "paper_mode": True,
        "opened_at": datetime(2026, 8, 26, 9, 15, 0),
    }
    await book._update_sr_and_check_sl("MANAPPURAM", 100.0, datetime(2026, 8, 26, 9, 15, 10, tzinfo=IST))
    await book._update_sr_and_check_sl("MANAPPURAM", 1.0, datetime(2026, 8, 26, 9, 15, 40, tzinfo=IST))

    assert "MANAPPURAM" not in book._live_sl
    sell_events = [e for t, e in bus.published if t == Topic.OI_ORB_ORDER_REQUEST and e.action == "SELL"]
    assert sell_events == []


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

    state = book.monitoring_state()
    pos = state["positions"]["MANAPPURAM"]
    assert pos["spot_ltp"] == 372.5
    assert pos["sl"] == 365.0


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
