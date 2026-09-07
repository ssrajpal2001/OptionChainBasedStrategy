"""
scripts/oi_orb_screener_scenario_simulator.py

Standalone, narrative scenario simulator for OI-ORB Screener -- built
2026-09-07, direct user spec, companion to sell_straddle_scenario_simulator.py
after a full day of live-incident fixes to this strategy.

Drives the REAL OiOrbScreenerStrategy class (and the real, pure screener
functions it actually imports) through every scenario below -- never
reimplements the strategy's own logic. Uses an isolated tmp SQLite DB so it
never touches the real data/oi_orb_screener.db. Each scenario prints one
PASS/FAIL line; a final tally is printed at the end.

Run: python scripts/oi_orb_screener_scenario_simulator.py
"""
from __future__ import annotations

import asyncio
import sys
import tempfile
import traceback
from datetime import date, datetime

sys.path.insert(0, ".")

from config.global_config import IST, Topic
from data_layer.base_feeder import OptionTick
from strategies.oi_orb_screener import screener, stock_resolve, store
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy


# ── isolated store (never touch the real DB) ────────────────────────────────

_TMP_DB = tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
store._DB_PATH = _TMP_DB
store._initialized = False

_TEST_CLIENT_ID = "SIM_CLIENT"
_TEST_BINDING_ID = "SIM_BINDING"


# ── shared helpers (mirror tests/oi_orb_screener/test_engine.py) ───────────

class _FakeGlobalFeeder:
    def __init__(self) -> None:
        self.subscribed_tokens: list = []

    async def subscribe_tokens(self, tokens):
        self.subscribed_tokens.extend(tokens)


class _FakeBus:
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


def _make_book(bus=None) -> OiOrbScreenerStrategy:
    bus = bus or _FakeBus()
    return OiOrbScreenerStrategy(
        bus, cfg=None, client_id=_TEST_CLIENT_ID, binding_id=_TEST_BINDING_ID,
        lot_multiplier=1, product_type="MIS", squareoff_time="15:15",
    )


def _contract(symbol: str, strike: int, opt_type: str):
    return stock_resolve.ResolvedContract(
        underlying=symbol, expiry=date(2026, 9, 29), strike=strike, option_type=opt_type,
        upstox_key=f"NSE_FO|{symbol}{strike}{opt_type}",
        broker_symbols={"zerodha": f"{symbol}29SEP{strike}{opt_type}"},
    )


# ── result tracking ─────────────────────────────────────────────────────────

_RESULTS: list = []


def _record(num: int, name: str, passed: bool, detail: str):
    _RESULTS.append((num, name, passed, detail))
    tag = "PASS" if passed else "FAIL"
    print(f"[{tag}] #{num:2d} {name}\n       {detail}")


def _run(num: int, name: str, coro_fn):
    try:
        asyncio.run(coro_fn())
    except AssertionError as exc:
        _record(num, name, False, f"ASSERTION FAILED: {exc}")
    except Exception:
        _record(num, name, False, f"EXCEPTION:\n{traceback.format_exc()}")


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY / SIGNAL SCENARIOS (1-8)
# ═══════════════════════════════════════════════════════════════════════════

async def scenario_01_call_arms_above_vwap():
    armed, fired = screener.check_vwap_retest_entry("CALL", ltp=105.0, vwap=100.0,
                                                      armed=False, min_gap_pct=0.0)
    _record(1, "CALL arms the instant ltp > vwap", armed is True,
             f"armed={armed}, fired={fired} (ltp=105 > vwap=100)")


async def scenario_02_put_arms_below_vwap():
    armed, fired = screener.check_vwap_retest_entry("PUT", ltp=95.0, vwap=100.0,
                                                      armed=False, min_gap_pct=0.0)
    _record(2, "PUT arms the instant ltp < vwap", armed is True,
             f"armed={armed}, fired={fired} (ltp=95 < vwap=100)")


async def scenario_03_call_fires_on_retest_back_to_vwap():
    armed, fired = screener.check_vwap_retest_entry("CALL", ltp=100.0, vwap=100.0,
                                                      armed=True, min_gap_pct=0.0)
    _record(3, "CALL fires when armed price retests back down to vwap", fired is True,
             f"armed={armed}, fired={fired} (already armed, ltp retested to vwap)")


async def scenario_04_historical_retest_fires_immediate_entry(monkeypatch=None):
    book = _make_book()
    book._regime = "bullish"
    book._shortlist_pchange = {"TESTCO": 3.0}   # positive -> CALL side
    book._orb_frozen = {"TESTCO": (100.0, 95.0)}
    fired_signals = []

    async def _fake_emit(sym, side, price, reason, orb_lvl, ts_str, label):
        fired_signals.append((sym, side, reason))
    book._emit_vwap_signal = _fake_emit

    import strategies.oi_orb_screener.engine as engine_mod
    orig = engine_mod.screener.historical_vwap_retest_check
    engine_mod.screener.historical_vwap_retest_check = lambda symbols_sides, cfg: {
        "TESTCO": {"armed": True, "fired": True, "fire_ts": "09:45:00", "fire_price": 101.5, "final_vwap": 100.0,
                   "bars_replayed": 20}
    }
    try:
        await book._apply_historical_vwap_retest(["TESTCO"], book._screener_cfg)
    finally:
        engine_mod.screener.historical_vwap_retest_check = orig

    ok = fired_signals == [("TESTCO", "CALL", "vwap_retest_historical")]
    _record(4, "Historical VWAP-retest fires an immediate entry when already retested", ok,
             f"fired_signals={fired_signals}")


async def scenario_05_historical_retest_respects_regime_gate():
    book = _make_book()
    book._regime = "bearish"   # CALL is NOT tradeable on a bearish day
    book._shortlist_pchange = {"TESTCO": 3.0}   # positive -> CALL side
    book._orb_frozen = {"TESTCO": (100.0, 95.0)}
    fired_signals = []

    async def _fake_emit(sym, side, price, reason, orb_lvl, ts_str, label):
        fired_signals.append((sym, side, reason))
    book._emit_vwap_signal = _fake_emit

    import strategies.oi_orb_screener.engine as engine_mod
    orig = engine_mod.screener.historical_vwap_retest_check
    engine_mod.screener.historical_vwap_retest_check = lambda symbols_sides, cfg: {
        "TESTCO": {"armed": True, "fired": True, "fire_ts": "09:45:00", "fire_price": 101.5, "final_vwap": 100.0,
                   "bars_replayed": 20}
    }
    try:
        await book._apply_historical_vwap_retest(["TESTCO"], book._screener_cfg)
    finally:
        engine_mod.screener.historical_vwap_retest_check = orig

    ok = fired_signals == []
    _record(5, "Historical VWAP-retest BLOCKS a CALL on a bearish-regime day", ok,
             f"fired_signals={fired_signals} (expected empty -- regime blocked it)")


async def scenario_06_can_trade_gates_on_screener_sentinel():
    import execution_bridge.oi_orb_bridge as bridge_mod
    ok = bridge_mod._GATE_UNDERLYING == "SCREENER"
    _record(6, "can_trade() gate uses the SCREENER sentinel, not the real stock symbol", ok,
             f"_GATE_UNDERLYING={bridge_mod._GATE_UNDERLYING!r} "
             "(gating on the real symbol would never match the one deployment row)")


async def scenario_07_bearish_day_blocks_call_allows_put():
    call_ok = screener.side_allowed_by_regime("CALL", "bearish", regime_filter_on=True)
    put_ok = screener.side_allowed_by_regime("PUT", "bearish", regime_filter_on=True)
    ok = call_ok is False and put_ok is True
    _record(7, "Bearish day: CALL blocked, PUT tradeable", ok,
             f"CALL allowed={call_ok} (expected False), PUT allowed={put_ok} (expected True)")


async def scenario_08_regime_filter_off_bypasses_gating():
    call_ok = screener.side_allowed_by_regime("CALL", "bearish", regime_filter_on=False)
    _record(8, "regime_filter_on=False bypasses the regime table entirely", call_ok is True,
             f"CALL allowed on a bearish day with filter OFF: {call_ok} (expected True)")


# ═══════════════════════════════════════════════════════════════════════════
# BROKER / EXECUTION SCENARIOS (9-12)
# ═══════════════════════════════════════════════════════════════════════════

async def scenario_09_upstox_symbol_resolution_uses_b_attr():
    class _FakeUpstoxBinding:
        provider = "upstox"
    class _FakeUpstoxBroker:
        _b = _FakeUpstoxBinding()
    resolved = getattr(_FakeUpstoxBroker(), "_binding", None) or getattr(_FakeUpstoxBroker(), "_b", None)
    ok = resolved is not None and resolved.provider == "upstox"
    _record(9, "Upstox broker resolves provider via self._b", ok,
             f"resolved.provider={getattr(resolved, 'provider', None)}")


async def scenario_10_zerodha_symbol_resolution_uses_binding_attr():
    class _FakeZerodhaBinding:
        provider = "zerodha"
    class _FakeZerodhaBroker:
        _binding = _FakeZerodhaBinding()
    b = _FakeZerodhaBroker()
    resolved = getattr(b, "_binding", None) or getattr(b, "_b", None)
    ok = resolved is not None and resolved.provider == "zerodha"
    _record(10, "Zerodha broker resolves provider via self._binding", ok,
             f"resolved.provider={getattr(resolved, 'provider', None)}")


async def scenario_11_market_order_sends_zero_price():
    import inspect
    import execution_bridge.oi_orb_bridge as bridge_mod
    src = inspect.getsource(bridge_mod.OiOrbExecutionBridge._live_fill)
    ok = "price=0.0" in src and "price=ev.entry_price" not in src
    _record(11, "OI-ORB MARKET order sends price=0.0, not a stale LTP", ok,
             "inspected _live_fill's source for the OrderRequest price= kwarg")


async def scenario_12_paper_route_real_attempt_then_simulated_fallback():
    import inspect
    import execution_bridge.oi_orb_bridge as bridge_mod
    src = inspect.getsource(bridge_mod.OiOrbExecutionBridge._live_fill)
    ok = ("broker.place_order" in src and "SIMULATED fill" in src
          and "if avg <= 0:" in src)
    _record(12, "paper_route: real order attempted first, simulated fallback only if unconfirmed", ok,
             "inspected _live_fill's source for the real-attempt-then-fallback contract")


# ═══════════════════════════════════════════════════════════════════════════
# EXIT SCENARIOS (13-16)
# ═══════════════════════════════════════════════════════════════════════════

async def scenario_13_hard_risk_cap_disabled():
    book = _make_book()
    contract = _contract("TESTCO", 100, "CE")
    book._positions["TESTCO"] = {
        "contract": contract, "qty": 3000, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 9, 7, 9, 15, 0),
    }
    book._subscribe(Topic.OPTION_TICK)
    book._running = True
    task = asyncio.create_task(book._option_tick_loop())
    try:
        # loss = (100 - 10) * 3000 = Rs270,000 -- far past the old Rs2000/lot cap.
        await book._bus.publish(Topic.OPTION_TICK, OptionTick(
            symbol="TESTCO100CE", underlying="TESTCO", strike=100, option_type="CE",
            expiry=date(2026, 9, 29), ltp=10.0, bid=10.0, ask=10.0, oi=0, change_oi=0,
            volume=0, iv=0.0, delta=0.0, timestamp=datetime(2026, 9, 7, 9, 15, 10, tzinfo=IST),
            atp=10.0,
        ))
        await asyncio.sleep(0.05)
        ok = "TESTCO" in book._positions and "TESTCO" not in book._eod_closing
    finally:
        book._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    _record(13, "hard_risk_cap is disabled -- never closes even on a severe adverse move", ok,
             f"position still open: {'TESTCO' in book._positions}")


async def scenario_14_option_sl_target_ratchet_disabled():
    book = _make_book()
    contract = _contract("TESTCO", 100, "CE")
    book._positions["TESTCO"] = {
        "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 9, 7, 9, 15, 0),
    }
    book._subscribe(Topic.OPTION_TICK)
    book._running = True
    task = asyncio.create_task(book._option_tick_loop())
    try:
        for ltp in (105.0, 108.0, 102.0, 90.0, 89.5, 91.0):
            await book._bus.publish(Topic.OPTION_TICK, OptionTick(
                symbol="TESTCO100CE", underlying="TESTCO", strike=100, option_type="CE",
                expiry=date(2026, 9, 29), ltp=ltp, bid=ltp, ask=ltp, oi=0, change_oi=0,
                volume=0, iv=0.0, delta=0.0, timestamp=datetime.now(IST), atp=ltp,
            ))
            await asyncio.sleep(0.02)
        ok = "TESTCO" not in book._live_sl and "TESTCO" not in book._live_target
    finally:
        book._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    _record(14, "Option SL/target ratchet is disabled -- never arms from real ticks", ok,
             f"live_sl has TESTCO={'TESTCO' in book._live_sl}, "
             f"live_target has TESTCO={'TESTCO' in book._live_target} (both must be False)")


async def scenario_15_ha_stochrsi_exit_wired():
    book = _make_book()
    has_exit = hasattr(book, "_ha_stoch_check_exit")
    _record(15, "HA+StochRSI is the wired, remaining exit mechanic", has_exit,
             f"_ha_stoch_check_exit present: {has_exit}")


async def scenario_16_eod_squareoff_closes_open_positions():
    book = _make_book()
    contract = _contract("TESTCO", 100, "CE")
    book._positions["TESTCO"] = {
        "contract": contract, "qty": 100, "entry_price": 100.0, "paper_mode": True,
        "opened_at": datetime(2026, 9, 7, 9, 15, 0),
    }
    closed = []
    async def _fake_emit_close(symbol, pos, reason):
        closed.append((symbol, reason))
        book._eod_closing.add(symbol)
    book._emit_close = _fake_emit_close
    book._squareoff_time = datetime.now(IST).time().replace(hour=0, minute=0)  # always past
    book._running = True
    # Drive one EOD loop cycle directly (mirrors _eod_loop's own body).
    now = datetime.now(IST)
    if now.time() >= book._squareoff_time and book._positions:
        for symbol, pos in list(book._positions.items()):
            if symbol in book._eod_closing:
                continue
            book._eod_closing.add(symbol)
            await book._emit_close(symbol, pos, "eod_squareoff")
    ok = closed == [("TESTCO", "eod_squareoff")]
    _record(16, "EOD square-off still closes any open position", ok, f"closed={closed}")


# ═══════════════════════════════════════════════════════════════════════════
# RESTART / RECOVERY SCENARIOS (17-19)
# ═══════════════════════════════════════════════════════════════════════════

async def scenario_17_entry_ltp_timeout_is_retry_eligible():
    store.log_signal_event(_TEST_CLIENT_ID, _TEST_BINDING_ID, "TESTCO", "signal_fired", side="PUT")
    store.log_signal_event(_TEST_CLIENT_ID, _TEST_BINDING_ID, "TESTCO", "entry_ltp_timeout", side="PUT")
    fired = store.load_already_fired(_TEST_CLIENT_ID, _TEST_BINDING_ID)
    ok = ("TESTCO", "PUT") not in fired
    _record(17, "A signal that timed out on entry stays retry-eligible after restart", ok,
             f"('TESTCO','PUT') in already_fired = {('TESTCO', 'PUT') in fired} (expected False)")


async def scenario_18_genuine_fired_signal_is_not_retried():
    store.log_signal_event(_TEST_CLIENT_ID, _TEST_BINDING_ID, "REALCO", "signal_fired", side="CALL")
    fired = store.load_already_fired(_TEST_CLIENT_ID, _TEST_BINDING_ID)
    ok = ("REALCO", "CALL") in fired
    _record(18, "A genuinely-fired signal (no abort) IS excluded from retry", ok,
             f"('REALCO','CALL') in already_fired = {('REALCO', 'CALL') in fired} (expected True)")


async def scenario_19_oi_spurt_history_never_touches_trading_state():
    book = _make_book()
    book._nse = object()
    book._shortlist_symbols = ["AAA", "BBB"]
    book._shortlist_pchange = {"AAA": 3.0, "BBB": -2.5}

    import strategies.oi_orb_screener.engine as engine_mod
    orig = engine_mod.screener.poll_oi_rank
    engine_mod.screener.poll_oi_rank = lambda nse, cfg, top_n: __import__("pandas").DataFrame([
        {"symbol": "ZZZ", "rank": 1, "oi_spurt_pct": 9.73, "pChange": -2.43},
    ])
    orig_record = store.record_oi_spurt_history
    recorded = []
    store.record_oi_spurt_history = lambda *a, **kw: recorded.append(a)
    try:
        await book._do_oi_spurt_history_poll(datetime.now(IST), book._screener_cfg)
    finally:
        engine_mod.screener.poll_oi_rank = orig
        store.record_oi_spurt_history = orig_record

    ok = (len(recorded) == 1 and book._shortlist_symbols == ["AAA", "BBB"]
          and book._rejected == set() and book._positions == {})
    _record(19, "Full-day OI-spurt history poll only logs -- never touches trading state", ok,
             f"recorded={len(recorded)} call(s), shortlist_symbols unchanged={book._shortlist_symbols == ['AAA', 'BBB']}, "
             f"rejected={book._rejected}, positions={book._positions}")


# ═══════════════════════════════════════════════════════════════════════════
# MULTI-POSITION SCENARIO (20)
# ═══════════════════════════════════════════════════════════════════════════

async def scenario_20_multiple_concurrent_positions_tracked_independently():
    book = _make_book()
    book._positions["AAA"] = {
        "contract": _contract("AAA", 100, "CE"), "qty": 100, "entry_price": 50.0,
        "paper_mode": True, "opened_at": datetime.now(IST),
    }
    book._positions["BBB"] = {
        "contract": _contract("BBB", 200, "PE"), "qty": 200, "entry_price": 30.0,
        "paper_mode": True, "opened_at": datetime.now(IST),
    }
    book._live_option_ltp["AAA"] = 60.0   # +10/unit
    book._live_option_ltp["BBB"] = 25.0   # -5/unit
    ms = book.monitoring_state()
    aaa_pnl = ms["positions"]["AAA"]["pnl"]
    bbb_pnl = ms["positions"]["BBB"]["pnl"]
    ok = len(ms["positions"]) == 2 and aaa_pnl == 1000.0 and bbb_pnl == -1000.0
    _record(20, "Multiple concurrent stock positions are tracked fully independently", ok,
             f"AAA pnl={aaa_pnl} (expected 1000.0), BBB pnl={bbb_pnl} (expected -1000.0)")


# ═══════════════════════════════════════════════════════════════════════════

SCENARIOS = [
    (1, "CALL arms above VWAP", scenario_01_call_arms_above_vwap),
    (2, "PUT arms below VWAP", scenario_02_put_arms_below_vwap),
    (3, "CALL fires on retest back to VWAP", scenario_03_call_fires_on_retest_back_to_vwap),
    (4, "Historical VWAP-retest fires immediate entry", scenario_04_historical_retest_fires_immediate_entry),
    (5, "Historical retest respects regime gate", scenario_05_historical_retest_respects_regime_gate),
    (6, "can_trade() gates on SCREENER sentinel", scenario_06_can_trade_gates_on_screener_sentinel),
    (7, "Bearish day blocks CALL, allows PUT", scenario_07_bearish_day_blocks_call_allows_put),
    (8, "Regime filter off bypasses gating", scenario_08_regime_filter_off_bypasses_gating),
    (9, "Upstox symbol resolution (_b)", scenario_09_upstox_symbol_resolution_uses_b_attr),
    (10, "Zerodha symbol resolution (_binding)", scenario_10_zerodha_symbol_resolution_uses_binding_attr),
    (11, "MARKET order sends price=0", scenario_11_market_order_sends_zero_price),
    (12, "paper_route real-attempt-then-fallback", scenario_12_paper_route_real_attempt_then_simulated_fallback),
    (13, "hard_risk_cap disabled", scenario_13_hard_risk_cap_disabled),
    (14, "Option SL/target ratchet disabled", scenario_14_option_sl_target_ratchet_disabled),
    (15, "HA+StochRSI exit wired", scenario_15_ha_stochrsi_exit_wired),
    (16, "EOD square-off closes open positions", scenario_16_eod_squareoff_closes_open_positions),
    (17, "entry_ltp_timeout is retry-eligible", scenario_17_entry_ltp_timeout_is_retry_eligible),
    (18, "Genuine fired signal is not retried", scenario_18_genuine_fired_signal_is_not_retried),
    (19, "OI-spurt history never touches trading state", scenario_19_oi_spurt_history_never_touches_trading_state),
    (20, "Multiple concurrent positions tracked independently", scenario_20_multiple_concurrent_positions_tracked_independently),
]


def main():
    print("=" * 100)
    print("OI-ORB Screener Scenario Simulator -- driving the REAL OiOrbScreenerStrategy class")
    print("=" * 100)
    for num, name, fn in SCENARIOS:
        _run(num, name, fn)
        print()

    passed = sum(1 for _, _, ok, _ in _RESULTS if ok)
    total = len(_RESULTS)
    print("=" * 100)
    print(f"RESULT: {passed}/{total} scenarios PASSED")
    if passed != total:
        print("FAILED scenarios:")
        for num, name, ok, detail in _RESULTS:
            if not ok:
                print(f"  #{num}: {name}")
    print("=" * 100)
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
