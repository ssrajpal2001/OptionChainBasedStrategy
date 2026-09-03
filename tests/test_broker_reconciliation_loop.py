"""
tests/test_broker_reconciliation_loop.py -- tests for run_system.py's
broker-reconciliation orchestration (_reconcile_sell_straddle_book,
_reconcile_single_leg_book, _broker_reconciliation_loop).

Lives at the orchestration layer (not inside each strategy engine)
because it needs both the ExecutionRouter's real broker instances and
each strategy's book manager -- see run_system.py's own module comment
above these functions for the full rationale. The underlying
detection/alert logic itself is tested in
tests/strategies/test_broker_reconciliation.py; these tests cover the
per-strategy adapter logic (deriving ExpectedLeg from each strategy's own
position shape) and the loop's own defensiveness.
"""
from datetime import date
from types import SimpleNamespace

import pytest

from config.global_config import IST, SysEvent, Topic
from run_system import (
    _reconcile_sell_straddle_book, _reconcile_single_leg_book, _broker_reconciliation_pass,
    _binding_trading_mode,
)
from strategies.sell_straddle import StraddlePosition, StraddleLeg


class _FakePosition:
    def __init__(self, symbol: str, qty: int) -> None:
        self.symbol = symbol
        self.qty = qty
        self.avg_price = 100.0
        self.pnl = 0.0
        self.product = "MIS"


class _FakeBroker:
    def __init__(self, positions=None, binding_provider: str = "zerodha") -> None:
        self._positions = positions or []
        self._binding = SimpleNamespace(provider=binding_provider)

    async def get_positions(self):
        return self._positions


class _CapturingBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


def _fake_router(broker=None, client_id="ssrajpal2001", binding_id="SA5770"):
    brokers = {client_id: {binding_id: broker}} if broker is not None else {}
    return SimpleNamespace(_brokers=brokers)


class _FakeClientDB:
    def __init__(self, bindings: list) -> None:
        self._bindings = bindings

    def get_bindings_safe_sync(self, client_id: str) -> list:
        return self._bindings


def _fake_router_with_trading_mode(broker, client_id, binding_id, trading_mode):
    router = _fake_router(broker, client_id, binding_id)
    router._client_db = _FakeClientDB([{"binding_id": binding_id, "trading_mode": trading_mode}])
    return router


# ── _binding_trading_mode ────────────────────────────────────────────────────

def test_binding_trading_mode_returns_real_value():
    router = _fake_router_with_trading_mode(None, "c", "b", "paper_route")
    assert _binding_trading_mode(router, "c", "b") == "paper_route"


def test_binding_trading_mode_defaults_to_paper_when_binding_not_found():
    router = _fake_router_with_trading_mode(None, "c", "b", "live")
    assert _binding_trading_mode(router, "c", "other_binding") == "paper"


def test_binding_trading_mode_defaults_to_paper_when_no_client_db():
    router = _fake_router(None, "c", "b")   # no _client_db attribute at all
    assert _binding_trading_mode(router, "c", "b") == "paper"


def test_binding_trading_mode_defaults_to_paper_on_lookup_error():
    router = _fake_router(None, "c", "b")
    router._client_db = SimpleNamespace(get_bindings_safe_sync=lambda cid: (_ for _ in ()).throw(RuntimeError("db down")))
    assert _binding_trading_mode(router, "c", "b") == "paper"


# ── SellStraddle adapter ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_sell_straddle_reconcile_ok_when_flat():
    book = SimpleNamespace(_client_id="c", _binding_id="b", _underlying="NIFTY",
                            _position=None, _clog=None)
    broker = _FakeBroker(positions=[])
    router = _fake_router(broker, "c", "b")
    bus = _CapturingBus()
    await _reconcile_sell_straddle_book(book, router, bus)
    assert bus.published == []


@pytest.mark.asyncio
async def test_sell_straddle_reconcile_skips_when_symbols_not_yet_populated():
    """A position that just opened optimistically (fill not yet confirmed)
    has empty leg symbols -- must not be treated as a mismatch."""
    pos = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
        ce_leg=StraddleLeg("CE", 24500, 100.0, symbol=""),
        pe_leg=StraddleLeg("PE", 24500, 95.0, symbol=""),
        status="open",
    )
    book = SimpleNamespace(_client_id="c", _binding_id="b", _underlying="NIFTY",
                            _position=pos, _clog=None)
    broker = _FakeBroker(positions=[])
    router = _fake_router(broker, "c", "b")
    bus = _CapturingBus()
    await _reconcile_sell_straddle_book(book, router, bus)
    assert bus.published == []


@pytest.mark.asyncio
async def test_sell_straddle_reconcile_flags_a_missing_leg():
    pos = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
        ce_leg=StraddleLeg("CE", 24500, 100.0, symbol="NIFTY24AUG24500CE"),
        pe_leg=StraddleLeg("PE", 24500, 95.0, symbol="NIFTY24AUG24500PE"),
        status="open",
    )
    book = SimpleNamespace(_client_id="c", _binding_id="b", _underlying="NIFTY",
                            _position=pos, _clog=None)
    # Broker only confirms the CE leg -- PE is missing.
    broker = _FakeBroker(positions=[_FakePosition("NIFTY24AUG24500CE", -75)])
    router = _fake_router(broker, "c", "b")
    bus = _CapturingBus()
    await _reconcile_sell_straddle_book(book, router, bus)
    mismatches = [e for t, e in bus.published if t == Topic.SYSTEM_EVENT and e.code == SysEvent.POSITION_MISMATCH]
    assert len(mismatches) == 1
    assert "PE" in mismatches[0].message


@pytest.mark.asyncio
async def test_sell_straddle_reconcile_ok_when_both_legs_confirmed():
    pos = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
        ce_leg=StraddleLeg("CE", 24500, 100.0, symbol="NIFTY24AUG24500CE"),
        pe_leg=StraddleLeg("PE", 24500, 95.0, symbol="NIFTY24AUG24500PE"),
        status="open",
    )
    book = SimpleNamespace(_client_id="c", _binding_id="b", _underlying="NIFTY",
                            _position=pos, _clog=None)
    broker = _FakeBroker(positions=[
        _FakePosition("NIFTY24AUG24500CE", -75), _FakePosition("NIFTY24AUG24500PE", -75),
    ])
    router = _fake_router(broker, "c", "b")
    bus = _CapturingBus()
    await _reconcile_sell_straddle_book(book, router, bus)
    assert bus.published == []


@pytest.mark.asyncio
async def test_sell_straddle_reconcile_skips_entirely_for_paper_route():
    """2026-08-26 real incident: SA5770 (paper_route) fired a CRITICAL
    'MISSING AT BROKER' mismatch every 5 minutes for the entire life of a
    genuinely correct, working position -- paper_route deliberately has its
    real broker order rejected (routing/whitelist verification only, zero
    real funds) while the strategy books a local simulated fill regardless,
    so the broker will ALWAYS show nothing for it by design. Must not even
    attempt the check for a paper_route binding."""
    pos = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
        ce_leg=StraddleLeg("CE", 24500, 100.0, symbol="NIFTY24AUG24500CE"),
        pe_leg=StraddleLeg("PE", 24500, 95.0, symbol="NIFTY24AUG24500PE"),
        status="open",
    )
    book = SimpleNamespace(_client_id="ssrajpal2001", _binding_id="SA5770", _underlying="NIFTY",
                            _position=pos, _clog=None)
    broker = _FakeBroker(positions=[])   # broker genuinely shows nothing, as expected for paper_route
    router = _fake_router_with_trading_mode(broker, "ssrajpal2001", "SA5770", "paper_route")
    bus = _CapturingBus()
    await _reconcile_sell_straddle_book(book, router, bus)
    assert bus.published == []


@pytest.mark.asyncio
async def test_sell_straddle_reconcile_still_fires_for_live_binding():
    """Confirms the paper_route skip doesn't accidentally suppress a genuine
    live-mode mismatch."""
    pos = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
        ce_leg=StraddleLeg("CE", 24500, 100.0, symbol="NIFTY24AUG24500CE"),
        pe_leg=StraddleLeg("PE", 24500, 95.0, symbol="NIFTY24AUG24500PE"),
        status="open",
    )
    book = SimpleNamespace(_client_id="gurmeet", _binding_id="zerodha", _underlying="NIFTY",
                            _position=pos, _clog=None)
    broker = _FakeBroker(positions=[])
    router = _fake_router_with_trading_mode(broker, "gurmeet", "zerodha", "live")
    bus = _CapturingBus()
    await _reconcile_sell_straddle_book(book, router, bus)
    mismatches = [e for t, e in bus.published if t == Topic.SYSTEM_EVENT and e.code == SysEvent.POSITION_MISMATCH]
    assert len(mismatches) == 1


@pytest.mark.asyncio
async def test_sell_straddle_reconcile_no_broker_no_crash():
    """No broker resolved for this (client,binding) -- must skip gracefully,
    not raise."""
    pos = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
        ce_leg=StraddleLeg("CE", 24500, 100.0, symbol="X"), status="open",
    )
    book = SimpleNamespace(_client_id="c", _binding_id="b", _underlying="NIFTY",
                            _position=pos, _clog=None)
    router = _fake_router(None)
    bus = _CapturingBus()
    await _reconcile_sell_straddle_book(book, router, bus)   # must not raise
    assert bus.published == []


# ── single-leg (OI-Flow / Liquidity Trap) adapter ────────────────────────────

@pytest.mark.asyncio
async def test_single_leg_reconcile_ok_when_flat_and_no_untracked_broker_position():
    book = SimpleNamespace(_client_id="c", _binding_id="b", _underlying="NIFTY",
                            _position=None, _clog=None, _today=None)
    broker = _FakeBroker(positions=[])
    router = _fake_router(broker, "c", "b")
    bus = _CapturingBus()
    await _reconcile_single_leg_book(book, router, bus, "OI-Flow")
    assert bus.published == []


@pytest.mark.asyncio
async def test_single_leg_reconcile_flags_an_untracked_position_when_flat():
    book = SimpleNamespace(_client_id="c", _binding_id="b", _underlying="NIFTY",
                            _position=None, _clog=None, _today=None)
    broker = _FakeBroker(positions=[_FakePosition("NIFTY24AUG24500CE", 75)])
    router = _fake_router(broker, "c", "b")
    bus = _CapturingBus()
    await _reconcile_single_leg_book(book, router, bus, "OI-Flow")
    mismatches = [e for t, e in bus.published if t == Topic.SYSTEM_EVENT and e.code == SysEvent.POSITION_MISMATCH]
    assert len(mismatches) == 1


@pytest.mark.asyncio
async def test_single_leg_reconcile_flags_a_missing_position_when_open():
    book = SimpleNamespace(
        _client_id="c", _binding_id="b", _underlying="NIFTY", _today=date(2026, 8, 21),
        _position={"side": "CE", "strike": 24500, "expiry": date(2026, 8, 28)}, _clog=None,
    )
    broker = _FakeBroker(positions=[])   # broker shows nothing
    router = _fake_router(broker, "c", "b")
    bus = _CapturingBus()
    await _reconcile_single_leg_book(book, router, bus, "Liquidity Trap")
    mismatches = [e for t, e in bus.published if t == Topic.SYSTEM_EVENT and e.code == SysEvent.POSITION_MISMATCH]
    assert len(mismatches) == 1
    assert "CE" in mismatches[0].message


@pytest.mark.asyncio
async def test_single_leg_reconcile_ok_when_broker_confirms():
    expiry = date(2026, 8, 28)
    book = SimpleNamespace(
        _client_id="c", _binding_id="b", _underlying="NIFTY", _today=date(2026, 8, 21),
        _position={"side": "CE", "strike": 24500, "expiry": expiry}, _clog=None,
    )
    from data_layer.instrument_registry import REGISTRY
    expected_symbol = REGISTRY.get_broker_symbol("NIFTY", expiry, 24500, "CE", "zerodha")
    broker = _FakeBroker(positions=[_FakePosition(expected_symbol, 75)], binding_provider="zerodha")
    router = _fake_router(broker, "c", "b")
    bus = _CapturingBus()
    await _reconcile_single_leg_book(book, router, bus, "OI-Flow")
    assert bus.published == []


@pytest.mark.asyncio
async def test_single_leg_reconcile_skips_entirely_for_paper_route():
    """Same paper_route false-positive fix as the SellStraddle adapter."""
    book = SimpleNamespace(
        _client_id="ssrajpal2001", _binding_id="SA5770", _underlying="NIFTY",
        _today=date(2026, 8, 21),
        _position={"side": "CE", "strike": 24500, "expiry": date(2026, 8, 28)}, _clog=None,
    )
    broker = _FakeBroker(positions=[])   # broker genuinely shows nothing, as expected for paper_route
    router = _fake_router_with_trading_mode(broker, "ssrajpal2001", "SA5770", "paper_route")
    bus = _CapturingBus()
    await _reconcile_single_leg_book(book, router, bus, "OI-Flow")
    assert bus.published == []


@pytest.mark.asyncio
async def test_single_leg_reconcile_no_broker_no_crash():
    book = SimpleNamespace(
        _client_id="c", _binding_id="b", _underlying="NIFTY", _today=None,
        _position={"side": "CE", "strike": 24500, "expiry": date(2026, 8, 28)}, _clog=None,
    )
    router = _fake_router(None)
    bus = _CapturingBus()
    await _reconcile_single_leg_book(book, router, bus, "OI-Flow")   # must not raise
    assert bus.published == []


@pytest.mark.asyncio
async def test_single_leg_reconcile_malformed_position_skips_gracefully():
    """A position dict missing an expected key (e.g. "strike") must not
    crash the whole reconciliation cycle -- skip this book, keep going."""
    book = SimpleNamespace(
        _client_id="c", _binding_id="b", _underlying="NIFTY", _today=None,
        _position={"side": "CE"}, _clog=None,   # no "strike"
    )
    broker = _FakeBroker(positions=[])
    router = _fake_router(broker, "c", "b")
    bus = _CapturingBus()
    await _reconcile_single_leg_book(book, router, bus, "OI-Flow")   # must not raise
    assert bus.published == []


# ── loop-level defensiveness ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_pass_survives_a_manager_raising_and_still_checks_other_managers():
    """A manager whose .books property itself raises must not kill the
    whole reconciliation pass for the OTHER strategies' managers -- the
    "good" manager's book must still get reconciled (and flagged) in the
    SAME pass."""
    class _BoomManager:
        @property
        def books(self):
            raise RuntimeError("simulated manager failure")

    good_book = SimpleNamespace(_client_id="c", _binding_id="b", _underlying="NIFTY",
                                 _position=None, _clog=None, _today=None)

    class _GoodManager:
        books = [good_book]

    managers = {"sell_straddle": _BoomManager(), "oi_flow": _GoodManager(), "liquidity_trap": None}
    broker = _FakeBroker(positions=[_FakePosition("NIFTY24AUG24500CE", 75)])
    router = _fake_router(broker, "c", "b")
    bus = _CapturingBus()

    await _broker_reconciliation_pass(managers, router, bus)   # must not raise

    mismatches = [e for t, e in bus.published if t == Topic.SYSTEM_EVENT and e.code == SysEvent.POSITION_MISMATCH]
    assert len(mismatches) == 1, (
        "the OI-Flow ('good') manager's book must still be reconciled and its "
        "heuristic mismatch flagged, even though sell_straddle's manager raised"
    )


@pytest.mark.asyncio
async def test_pass_with_no_managers_is_a_noop():
    await _broker_reconciliation_pass({}, _fake_router(None), _CapturingBus())   # must not raise
