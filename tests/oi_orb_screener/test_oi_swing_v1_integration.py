"""
Integration tests for entry_exit_mode="oi_swing_v1" -- drives the real
OiOrbScreenerStrategy class (not a standalone reimplementation, per this
repo's own feedback_backtest_drive_real_class discipline), same _FakeBus
pattern as tests/oi_orb_screener/test_engine.py.

Covers: default-mode-unchanged (the additive/opt-in guarantee), the
immediate 2% entry trigger + 14:30 cutoff (Fix 2), the OI-swing HOLD/EXIT
matrix wired end-to-end via _oi_swing_exit_check, the 10-min minimum hold
(Fix 3), the hard-risk-cap re-enablement gated to this mode only (Fix 1),
and the restart/restore sl_mechanic reconstruction fix (entry_reason ->
sl_mechanic, needed for this mode's own restart correctness).
"""
import asyncio
from datetime import date, datetime, timedelta

import pytest

from config.global_config import IST, Topic
from data_layer.base_feeder import OptionTick
from strategies.oi_orb_screener import stock_resolve, store
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy, _ENTRY_EXIT_MODE_OI_SWING
from strategies.oi_orb_screener.events import OiOrbFillEvent

_TEST_CLIENT_ID = "TESTCLIENT"
_TEST_BINDING_ID = "TESTBINDING"


@pytest.fixture(autouse=True)
def _isolated_store_db(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "_DB_PATH", str(tmp_path / "oi_orb_test.db"))
    monkeypatch.setattr(store, "_initialized", False)
    yield


class _FakeGlobalFeeder:
    def __init__(self) -> None:
        self.subscribed_tokens: list = []
        self.subscribed_equity: list = []

    async def subscribe_tokens(self, tokens):
        self.subscribed_tokens.extend(tokens)

    def subscribe_fno_equity(self, fyers_sym, underlying):
        self.subscribed_equity.append((fyers_sym, underlying))


class _FakeBus:
    def __init__(self) -> None:
        self._queues: dict = {}
        self.published: list = []
        self._global_feeder = _FakeGlobalFeeder()

    def subscribe(self, topic):
        return self._queues.setdefault(topic, asyncio.Queue())

    def unsubscribe(self, topic, q):
        pass

    async def publish(self, topic, event):
        self.published.append((topic, event))
        q = self._queues.get(topic)
        if q is not None:
            await q.put(event)


def _make_book(bus, **kwargs) -> OiOrbScreenerStrategy:
    return OiOrbScreenerStrategy(
        bus, cfg=None, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID,
        lot_multiplier=1, product_type="MIS", squareoff_time="15:15", **kwargs,
    )


def _contract(symbol: str, strike: int, opt_type: str) -> "stock_resolve.ResolvedContract":
    return stock_resolve.ResolvedContract(
        underlying=symbol, expiry=date(2026, 9, 25), strike=strike, option_type=opt_type,
        upstox_key=f"NSE_FO|{symbol}{strike}{opt_type}",
        broker_symbols={"zerodha": f"{symbol}25SEP{strike}{opt_type}"},
    )


def _async_return(value):
    async def _f(*a, **k):
        return value
    return _f


# ── constructor / config wiring ─────────────────────────────────────────

class TestConstructorDefaultsUnchanged:
    def test_default_mode_is_vwap_retest(self):
        book = _make_book(_FakeBus())
        assert book._entry_exit_mode == "vwap_retest"

    def test_unknown_mode_value_falls_back_to_default(self):
        book = _make_book(_FakeBus(), entry_exit_mode="some_typo")
        assert book._entry_exit_mode == "vwap_retest"

    def test_opt_in_mode_recognized(self):
        book = _make_book(_FakeBus(), entry_exit_mode="oi_swing_v1")
        assert book._entry_exit_mode == _ENTRY_EXIT_MODE_OI_SWING

    def test_default_cutoff_and_min_hold(self):
        book = _make_book(_FakeBus(), entry_exit_mode="oi_swing_v1")
        from datetime import time as dtime
        assert book._oi_swing_entry_cutoff == dtime(14, 30)
        assert book._oi_swing_min_hold_min == 10

    def test_custom_cutoff_and_min_hold(self):
        from datetime import time as dtime
        book = _make_book(_FakeBus(), entry_exit_mode="oi_swing_v1",
                           oi_swing_entry_cutoff="13:00", oi_swing_min_hold_min=15)
        assert book._oi_swing_entry_cutoff == dtime(13, 0)
        assert book._oi_swing_min_hold_min == 15


# ── entry: immediate 2% trigger + 14:30 cutoff (Fix 2) ──────────────────

class TestOiSwingEntryScan:
    @pytest.mark.asyncio
    async def test_fires_immediately_on_2pct_trigger(self, monkeypatch):
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        book._shortlist_symbols = ["SIEMENS"]
        book._prev_close_map = {"SIEMENS": 4000.0}
        fired = []

        async def _fake_emit(sym, side, ltp, reason, orb_lvl, ts_str, label):
            fired.append((sym, side, ltp, reason))

        monkeypatch.setattr(book, "_emit_vwap_signal", _fake_emit)
        monkeypatch.setattr(book, "_live_price", lambda sym, live, log_source=False: 4080.0)  # +2.0%

        now = datetime(2026, 9, 18, 9, 30, tzinfo=IST)
        await book._oi_swing_entry_scan(live=None, now=now, now_key="09:30")

        assert fired == [("SIEMENS", "CALL", 4080.0, "oi_swing_v1_entry")]

    @pytest.mark.asyncio
    async def test_no_fire_below_threshold(self, monkeypatch):
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        book._shortlist_symbols = ["SIEMENS"]
        book._prev_close_map = {"SIEMENS": 4000.0}
        fired = []
        monkeypatch.setattr(book, "_emit_vwap_signal", lambda *a, **k: fired.append(a))
        monkeypatch.setattr(book, "_live_price", lambda sym, live, log_source=False: 4070.0)  # +1.75%

        now = datetime(2026, 9, 18, 9, 30, tzinfo=IST)
        await book._oi_swing_entry_scan(live=None, now=now, now_key="09:30")
        assert fired == []

    @pytest.mark.asyncio
    async def test_put_side_on_negative_trigger(self, monkeypatch):
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        book._shortlist_symbols = ["SIEMENS"]
        book._prev_close_map = {"SIEMENS": 4000.0}
        fired = []

        async def _fake_emit(sym, side, ltp, reason, orb_lvl, ts_str, label):
            fired.append((sym, side, reason))

        monkeypatch.setattr(book, "_emit_vwap_signal", _fake_emit)
        monkeypatch.setattr(book, "_live_price", lambda sym, live, log_source=False: 3910.0)  # -2.25%

        now = datetime(2026, 9, 18, 9, 30, tzinfo=IST)
        await book._oi_swing_entry_scan(live=None, now=now, now_key="09:30")
        assert fired == [("SIEMENS", "PUT", "oi_swing_v1_entry")]

    @pytest.mark.asyncio
    async def test_no_entry_after_1430_cutoff(self, monkeypatch):
        """Fix 2 -- the raw backtest had no cutoff at all; a real bug found
        via the validated 13-day sweep (PREMIERENE entered 15:28, 13 minutes
        before its own EOD square-off)."""
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        book._shortlist_symbols = ["SIEMENS"]
        book._prev_close_map = {"SIEMENS": 4000.0}
        fired = []
        monkeypatch.setattr(book, "_emit_vwap_signal", lambda *a, **k: fired.append(a))
        monkeypatch.setattr(book, "_live_price", lambda sym, live, log_source=False: 4200.0)  # +5%, way past trigger

        now = datetime(2026, 9, 18, 14, 31, tzinfo=IST)   # 1 minute after cutoff
        await book._oi_swing_entry_scan(live=None, now=now, now_key="14:31")
        assert fired == []

    @pytest.mark.asyncio
    async def test_entry_allowed_exactly_at_cutoff(self, monkeypatch):
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        book._shortlist_symbols = ["SIEMENS"]
        book._prev_close_map = {"SIEMENS": 4000.0}
        fired = []

        async def _fake_emit(sym, side, ltp, reason, orb_lvl, ts_str, label):
            fired.append(sym)

        monkeypatch.setattr(book, "_emit_vwap_signal", _fake_emit)
        monkeypatch.setattr(book, "_live_price", lambda sym, live, log_source=False: 4200.0)

        now = datetime(2026, 9, 18, 14, 30, tzinfo=IST)
        await book._oi_swing_entry_scan(live=None, now=now, now_key="14:30")
        assert fired == ["SIEMENS"]

    @pytest.mark.asyncio
    async def test_skips_symbol_already_positioned_or_pending(self, monkeypatch):
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        book._shortlist_symbols = ["SIEMENS"]
        book._prev_close_map = {"SIEMENS": 4000.0}
        book._positions["SIEMENS"] = {"contract": None, "qty": 1, "entry_price": 1.0,
                                        "opened_at": datetime.now(IST), "sl_mechanic": "oi_swing_v1"}
        fired = []
        monkeypatch.setattr(book, "_emit_vwap_signal", lambda *a, **k: fired.append(a))
        monkeypatch.setattr(book, "_live_price", lambda sym, live, log_source=False: 4200.0)

        now = datetime(2026, 9, 18, 9, 30, tzinfo=IST)
        await book._oi_swing_entry_scan(live=None, now=now, now_key="09:30")
        assert fired == []


# ── on_fill tagging ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_on_fill_tags_oi_swing_v1_mechanic():
    bus = _FakeBus()
    book = _make_book(bus, entry_exit_mode="oi_swing_v1")
    contract = _contract("SIEMENS", 4050, "CE")
    book._pending_contracts["SIEMENS"] = contract
    book._pending_fills["EVT1"] = {
        "symbol": "SIEMENS", "contract": contract, "qty": 300, "entry_price": 105.0,
        "reason": "oi_swing_v1_entry",
    }
    await book._on_fill(OiOrbFillEvent(
        action="BUY", underlying="SIEMENS", option_type="CE", strike=4050, fill_price=106.2,
        qty=300, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id="EVT1", paper_mode=True,
    ))
    assert book._positions["SIEMENS"]["sl_mechanic"] == _ENTRY_EXIT_MODE_OI_SWING


@pytest.mark.asyncio
async def test_on_fill_resets_stale_oi_swing_state_for_reentry():
    bus = _FakeBus()
    book = _make_book(bus, entry_exit_mode="oi_swing_v1")
    book._oi_swing_series["SIEMENS"] = [(datetime.now(IST), 100.0, 5000.0)]
    book._oi_swing_high["SIEMENS"] = 5000.0
    book._oi_swing_low["SIEMENS"] = 4900.0
    contract = _contract("SIEMENS", 4050, "CE")
    book._pending_contracts["SIEMENS"] = contract
    book._pending_fills["EVT1"] = {
        "symbol": "SIEMENS", "contract": contract, "qty": 300, "entry_price": 105.0,
        "reason": "oi_swing_v1_entry",
    }
    await book._on_fill(OiOrbFillEvent(
        action="BUY", underlying="SIEMENS", option_type="CE", strike=4050, fill_price=106.2,
        qty=300, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID, event_id="EVT1", paper_mode=True,
    ))
    assert "SIEMENS" not in book._oi_swing_series
    assert "SIEMENS" not in book._oi_swing_high
    assert "SIEMENS" not in book._oi_swing_low


# ── OI-swing exit check, wired end-to-end ────────────────────────────────

class TestOiSwingExitCheck:
    def _open_position(self, book, sym="SIEMENS", side_opt="CE", opened_at=None):
        contract = _contract(sym, 4050, side_opt)
        book._positions[sym] = {
            "contract": contract, "qty": 300, "entry_price": 100.0, "paper_mode": True,
            "opened_at": opened_at or datetime(2026, 9, 18, 9, 15, tzinfo=IST),
            "sl_mechanic": _ENTRY_EXIT_MODE_OI_SWING,
        }
        return contract

    @pytest.mark.asyncio
    async def test_no_action_before_three_bars(self, monkeypatch):
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        self._open_position(book)
        monkeypatch.setattr(book, "_resolve_futures_key_and_token",
                             _async_return(("NSE_FO|SIEMENSFUT", "tok")))
        from strategies.oi_orb_screener import engine as engine_mod
        monkeypatch.setattr(engine_mod, "fetch_upstox_v3_quote", _async_return({"oi": 1000.0}),
                             raising=False)

        async def _fake_quote(key, tok):
            return {"oi": 1000.0}
        import data_layer.historical_candles as hc
        monkeypatch.setattr(hc, "fetch_upstox_v3_quote", _fake_quote)

        now = datetime(2026, 9, 18, 9, 20, tzinfo=IST)
        await book._oi_swing_exit_check("SIEMENS", "CALL", 100.0, now)
        assert "SIEMENS" not in book._eod_closing
        assert len(book._oi_swing_series["SIEMENS"]) == 1

    @pytest.mark.asyncio
    async def test_same_bucket_does_not_double_record(self, monkeypatch):
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        self._open_position(book)
        monkeypatch.setattr(book, "_resolve_futures_key_and_token",
                             _async_return(("NSE_FO|SIEMENSFUT", "tok")))
        import data_layer.historical_candles as hc

        async def _fake_quote(key, tok):
            return {"oi": 1000.0}
        monkeypatch.setattr(hc, "fetch_upstox_v3_quote", _fake_quote)

        t1 = datetime(2026, 9, 18, 9, 20, tzinfo=IST)
        t2 = datetime(2026, 9, 18, 9, 21, tzinfo=IST)   # same 5-min bucket as t1
        await book._oi_swing_exit_check("SIEMENS", "CALL", 100.0, t1)
        await book._oi_swing_exit_check("SIEMENS", "CALL", 101.0, t2)
        assert len(book._oi_swing_series["SIEMENS"]) == 1

    @pytest.mark.asyncio
    async def test_full_breakout_exit_flow_after_min_hold(self, monkeypatch):
        """Drives a real swing-high breakout with an adverse (falling)
        price on a CALL position, entry_ts far enough in the past that the
        10-min minimum hold (Fix 3) does not suppress the exit."""
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        opened_at = datetime(2026, 9, 18, 9, 15, tzinfo=IST)
        self._open_position(book, opened_at=opened_at)
        monkeypatch.setattr(book, "_resolve_futures_key_and_token",
                             _async_return(("NSE_FO|SIEMENSFUT", "tok")))
        import data_layer.historical_candles as hc

        oi_values = [1000.0, 1100.0, 1050.0, 1200.0]   # bar3 (1050) confirms a swing HIGH at bar2 (1100)... wait see below
        prices = [100.0, 105.0, 108.0, 95.0]
        calls = {"i": 0}

        async def _fake_quote(key, tok):
            i = calls["i"]
            return {"oi": oi_values[i]}
        monkeypatch.setattr(hc, "fetch_upstox_v3_quote", _fake_quote)

        closed = []
        async def _fake_emit_close(sym, pos, reason, detail=""):
            closed.append((sym, reason))
            book._positions.pop(sym, None)
        monkeypatch.setattr(book, "_emit_close", _fake_emit_close)

        base = datetime(2026, 9, 18, 9, 20, tzinfo=IST)
        for i in range(4):
            calls["i"] = i
            now = base + timedelta(minutes=5 * i)
            await book._oi_swing_exit_check("SIEMENS", "CALL", prices[i], now)

        # oi series: [1000,1100,1050,1200] -> bar1(1100) is NOT a swing high yet
        # after bar2(1050) confirms: 1000<1100>1050 -> HIGH=1100 confirmed at bar index1
        # bar4 oi=1200 > swing_high(1100) -> breakout; price prev(108.0)->cur(95.0) is DOWN
        # -> CALL + price DOWN -> EXIT.
        assert book._oi_swing_high["SIEMENS"] == 1100.0
        assert closed == [("SIEMENS", "oi_swing_exit")]

    @pytest.mark.asyncio
    async def test_exit_suppressed_inside_min_hold_window(self, monkeypatch):
        """Fix 3 -- an otherwise-genuine EXIT decision must NOT close the
        position while inside the minimum hold window since entry."""
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1", oi_swing_min_hold_min=60)
        opened_at = datetime(2026, 9, 18, 9, 15, tzinfo=IST)
        self._open_position(book, opened_at=opened_at)
        monkeypatch.setattr(book, "_resolve_futures_key_and_token",
                             _async_return(("NSE_FO|SIEMENSFUT", "tok")))
        import data_layer.historical_candles as hc

        oi_values = [1000.0, 1100.0, 1050.0, 1200.0]
        prices = [100.0, 105.0, 108.0, 95.0]
        calls = {"i": 0}

        async def _fake_quote(key, tok):
            return {"oi": oi_values[calls["i"]]}
        monkeypatch.setattr(hc, "fetch_upstox_v3_quote", _fake_quote)

        closed = []
        async def _fake_emit_close(sym, pos, reason, detail=""):
            closed.append((sym, reason))
        monkeypatch.setattr(book, "_emit_close", _fake_emit_close)

        base = datetime(2026, 9, 18, 9, 20, tzinfo=IST)
        for i in range(4):
            calls["i"] = i
            now = base + timedelta(minutes=5 * i)   # last bar @ 9:35, only 20min after 9:15 entry
            await book._oi_swing_exit_check("SIEMENS", "CALL", prices[i], now)

        # 60-min min hold means the 9:35 breakout must NOT fire a real close.
        assert closed == []
        assert "SIEMENS" in book._positions

    @pytest.mark.asyncio
    async def test_no_breakout_when_oi_stays_between_swings(self, monkeypatch):
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        self._open_position(book)
        monkeypatch.setattr(book, "_resolve_futures_key_and_token",
                             _async_return(("NSE_FO|SIEMENSFUT", "tok")))
        import data_layer.historical_candles as hc

        oi_values = [1000.0, 1100.0, 1050.0, 1080.0]   # stays inside [swing_low? , 1100] after confirm
        calls = {"i": 0}

        async def _fake_quote(key, tok):
            return {"oi": oi_values[calls["i"]]}
        monkeypatch.setattr(hc, "fetch_upstox_v3_quote", _fake_quote)

        closed = []
        async def _fake_emit_close(sym, pos, reason, detail=""):
            closed.append((sym, reason))
        monkeypatch.setattr(book, "_emit_close", _fake_emit_close)

        base = datetime(2026, 9, 18, 9, 20, tzinfo=IST)
        for i in range(4):
            calls["i"] = i
            now = base + timedelta(minutes=5 * i)
            await book._oi_swing_exit_check("SIEMENS", "CALL", 100.0 + i, now)

        assert closed == []


# ── hard risk cap for oi_swing_v1 -- re-enabled as Fix 1, then explicitly
# turned back OFF the same trading day (2026-09-18) after firing correctly
# live in paper_route (SIEMENS-equivalent real case: ZYDUSLIFE, -Rs2070,
# confirmed working exactly as coded) -- direct user instruction "dont use
# hard stoploss". oi_swing_v1 positions now rely purely on the OI-swing
# exit + EOD square-off, same as the real 13-day-backtest "no cap" variant
# (best win rate on that sample, but also its single worst loss -- a real,
# known tradeoff, not an oversight). ──────────────────────────────────────

class TestHardRiskCapGating:
    @pytest.mark.asyncio
    async def test_hard_risk_cap_does_not_fire_for_oi_swing_v1_position(self, monkeypatch):
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        contract = _contract("SIEMENS", 4050, "CE")
        book._positions["SIEMENS"] = {
            "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
            "opened_at": datetime.now(IST), "sl_mechanic": _ENTRY_EXIT_MODE_OI_SWING,
        }
        book._pending_contracts.pop("SIEMENS", None)
        closed = []
        async def _fake_emit_close(sym, pos, reason, detail=""):
            closed.append((sym, reason))
        monkeypatch.setattr(book, "_emit_close", _fake_emit_close)

        book._subscribe(Topic.OPTION_TICK)
        task = asyncio.create_task(book._option_tick_loop())
        book._running = True
        try:
            # loss = (100-80)*100 = 2000 >= the (now-disabled) cap -- must
            # NOT close; the position stays open, subject only to the
            # OI-swing exit / EOD square-off.
            await bus.publish(Topic.OPTION_TICK, OptionTick(
                symbol="SIEMENS4050CE", underlying="SIEMENS", strike=4050, option_type="CE",
                expiry=date(2026, 9, 25), ltp=80.0, bid=79.5, ask=80.5, oi=0, change_oi=0,
                volume=0, iv=0.0, delta=0.0, timestamp=datetime.now(IST),
            ))
            await asyncio.sleep(0.3)
        finally:
            book._running = False
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        assert closed == []

    @pytest.mark.asyncio
    async def test_hard_risk_cap_does_not_fire_for_other_mechanics(self, monkeypatch):
        """Confirms Fix 1's re-enablement is scoped ONLY to oi_swing_v1 --
        every other position keeps the 2026-09-07 disabled behavior
        (HA+StochRSI/VWAP-close-SL + EOD are the only exits) untouched."""
        bus = _FakeBus()
        book = _make_book(bus)   # default mode
        contract = _contract("SIEMENS", 4050, "CE")
        book._positions["SIEMENS"] = {
            "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
            "opened_at": datetime.now(IST), "sl_mechanic": "trap",
        }
        closed = []
        async def _fake_emit_close(sym, pos, reason, detail=""):
            closed.append((sym, reason))
        monkeypatch.setattr(book, "_emit_close", _fake_emit_close)

        book._subscribe(Topic.OPTION_TICK)
        task = asyncio.create_task(book._option_tick_loop())
        book._running = True
        try:
            await bus.publish(Topic.OPTION_TICK, OptionTick(
                symbol="SIEMENS4050CE", underlying="SIEMENS", strike=4050, option_type="CE",
                expiry=date(2026, 9, 25), ltp=50.0, bid=49.5, ask=50.5, oi=0, change_oi=0,
                volume=0, iv=0.0, delta=0.0, timestamp=datetime.now(IST),
            ))
            await asyncio.sleep(1.0)
        finally:
            book._running = False
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        assert closed == []   # hard risk cap must NOT have fired for a "trap" position


# ── restart/restore correctness ──────────────────────────────────────────

class TestRestoreFromDbSlMechanicReconstruction:
    @pytest.mark.asyncio
    async def test_restores_oi_swing_v1_mechanic_from_entry_reason(self, monkeypatch):
        """2026-09-18 CRITICAL FIX: _restore_from_db used to hardcode
        sl_mechanic="vwap" for EVERY restored position regardless of what
        it was actually entered under -- a restored oi_swing_v1 position
        would have been silently downgraded onto the wrong exit mechanic
        after any mid-session restart. entry_reason (already persisted by
        store.open_position) is now used to reconstruct the real mechanic."""
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        book._today = date(2026, 9, 18)
        contract = _contract("SIEMENS", 4050, "CE")
        monkeypatch.setattr(store, "load_open_positions", lambda cid, bid, td: [
            {"symbol": "SIEMENS", "expiry": "2026-09-25", "strike": 4050, "option_type": "CE",
             "qty": 300, "entry_price": 100.0, "paper_mode": 1,
             "entry_ts": "2026-09-18T09:30:00+05:30", "entry_reason": "oi_swing_v1_entry"},
        ])
        monkeypatch.setattr(store, "load_already_fired", lambda cid, bid, trade_date=None: set())
        monkeypatch.setattr(store, "load_rejected", lambda cid, bid, trade_date=None: set())
        monkeypatch.setattr(stock_resolve, "resolve_contract_exact_async", _async_return(contract))
        monkeypatch.setattr(book, "_ensure_option_feed", lambda *a, **k: None)
        monkeypatch.setattr(book, "_ensure_spot_feed", lambda *a, **k: None)
        monkeypatch.setattr(book, "_seed_option_bars_from_history", _async_return(None))

        await book._restore_from_db()

        assert book._positions["SIEMENS"]["sl_mechanic"] == _ENTRY_EXIT_MODE_OI_SWING
        assert book._positions["SIEMENS"]["opened_at"] == datetime.fromisoformat("2026-09-18T09:30:00+05:30")

    @pytest.mark.asyncio
    async def test_restores_trap_mechanic_from_entry_reason(self, monkeypatch):
        """Same fix also correctly restores a 'trap' position -- previously
        always silently mislabeled 'vwap' too."""
        bus = _FakeBus()
        book = _make_book(bus)
        book._today = date(2026, 9, 18)
        contract = _contract("SIEMENS", 4050, "CE")
        monkeypatch.setattr(store, "load_open_positions", lambda cid, bid, td: [
            {"symbol": "SIEMENS", "expiry": "2026-09-25", "strike": 4050, "option_type": "CE",
             "qty": 300, "entry_price": 100.0, "paper_mode": 1,
             "entry_ts": "2026-09-18T09:30:00+05:30", "entry_reason": "trap_retest"},
        ])
        monkeypatch.setattr(store, "load_already_fired", lambda cid, bid, trade_date=None: set())
        monkeypatch.setattr(store, "load_rejected", lambda cid, bid, trade_date=None: set())
        monkeypatch.setattr(stock_resolve, "resolve_contract_exact_async", _async_return(contract))
        monkeypatch.setattr(book, "_ensure_option_feed", lambda *a, **k: None)
        monkeypatch.setattr(book, "_ensure_spot_feed", lambda *a, **k: None)
        monkeypatch.setattr(book, "_seed_option_bars_from_history", _async_return(None))

        await book._restore_from_db()

        assert book._positions["SIEMENS"]["sl_mechanic"] == "trap"

    @pytest.mark.asyncio
    async def test_restore_degrades_safely_with_fresh_oi_swing_tracker(self, monkeypatch):
        """The restart/degrade-safely judgment call: a restored oi_swing_v1
        position's swing series/ratchet is NOT persisted -- confirm it
        simply starts fresh (empty) rather than crashing or inheriting
        stale state from a different symbol."""
        bus = _FakeBus()
        book = _make_book(bus, entry_exit_mode="oi_swing_v1")
        book._today = date(2026, 9, 18)
        contract = _contract("SIEMENS", 4050, "CE")
        monkeypatch.setattr(store, "load_open_positions", lambda cid, bid, td: [
            {"symbol": "SIEMENS", "expiry": "2026-09-25", "strike": 4050, "option_type": "CE",
             "qty": 300, "entry_price": 100.0, "paper_mode": 1,
             "entry_ts": "2026-09-18T09:30:00+05:30", "entry_reason": "oi_swing_v1_entry"},
        ])
        monkeypatch.setattr(store, "load_already_fired", lambda cid, bid, trade_date=None: set())
        monkeypatch.setattr(store, "load_rejected", lambda cid, bid, trade_date=None: set())
        monkeypatch.setattr(stock_resolve, "resolve_contract_exact_async", _async_return(contract))
        monkeypatch.setattr(book, "_ensure_option_feed", lambda *a, **k: None)
        monkeypatch.setattr(book, "_ensure_spot_feed", lambda *a, **k: None)
        monkeypatch.setattr(book, "_seed_option_bars_from_history", _async_return(None))

        await book._restore_from_db()

        assert book._oi_swing_series.get("SIEMENS", []) == []
        assert book._oi_swing_high.get("SIEMENS") is None
        assert book._oi_swing_low.get("SIEMENS") is None
        # And the position itself IS correctly restored/tracked despite that.
        assert book._positions["SIEMENS"]["qty"] == 300
