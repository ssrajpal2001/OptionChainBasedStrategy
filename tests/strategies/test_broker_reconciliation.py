"""
tests/strategies/test_broker_reconciliation.py -- tests for
strategies/core/broker_reconciliation.py, the shared cross-check between a
strategy book's own believed position and the broker's real position book.

Built 2026-08-23 after an overnight audit found: if a position's persisted
state is ever corrupted/lost, every strategy silently treats itself as
flat with ZERO cross-check against the broker's real positions. Detection
+ loud alerting only -- deliberately never auto-remediation (see the
module's own docstring for the full rationale).
"""
import pytest

from config.global_config import SysEvent, Topic
from strategies.core.broker_reconciliation import (
    ExpectedLeg, reconcile_book, reconcile_and_alert,
)


class _FakePosition:
    def __init__(self, symbol: str, qty: int) -> None:
        self.symbol = symbol
        self.qty = qty
        self.avg_price = 100.0
        self.pnl = 0.0
        self.product = "MIS"


class _FakeBroker:
    def __init__(self, positions=None, raise_on_fetch: bool = False) -> None:
        self._positions = positions or []
        self._raise = raise_on_fetch

    async def get_positions(self):
        if self._raise:
            raise RuntimeError("simulated broker API failure")
        return self._positions


class _CapturingBus:
    def __init__(self) -> None:
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


# ── precise check (book believes OPEN) ───────────────────────────────────────

@pytest.mark.asyncio
async def test_precise_check_ok_when_broker_confirms_both_legs():
    broker = _FakeBroker(positions=[
        _FakePosition("NIFTY24AUG24500CE", -75),
        _FakePosition("NIFTY24AUG24500PE", -75),
    ])
    legs = [ExpectedLeg("NIFTY24AUG24500CE", "CE 24500"), ExpectedLeg("NIFTY24AUG24500PE", "PE 24500")]
    result = await reconcile_book(broker, "NIFTY", legs)
    assert result.ok is True
    assert result.missing_legs == []


@pytest.mark.asyncio
async def test_precise_check_flags_a_missing_leg():
    """The exact scenario this feature exists for: the book believes a leg
    is open, but the broker's real position book shows nothing for it."""
    broker = _FakeBroker(positions=[
        _FakePosition("NIFTY24AUG24500CE", -75),
        # PE leg missing entirely -- e.g. closed at broker without the app knowing,
        # or the app's own tracked state is stale/corrupted.
    ])
    legs = [ExpectedLeg("NIFTY24AUG24500CE", "CE 24500"), ExpectedLeg("NIFTY24AUG24500PE", "PE 24500")]
    result = await reconcile_book(broker, "NIFTY", legs)
    assert result.ok is False
    assert result.missing_legs == ["PE 24500"]
    assert "MISSING AT BROKER" in result.detail


@pytest.mark.asyncio
async def test_precise_check_flags_a_zero_qty_position_as_missing():
    """A broker position record with qty=0 is not really "open" -- must be
    treated the same as no record at all, not as a match."""
    broker = _FakeBroker(positions=[_FakePosition("NIFTY24AUG24500CE", 0)])
    legs = [ExpectedLeg("NIFTY24AUG24500CE", "CE 24500")]
    result = await reconcile_book(broker, "NIFTY", legs)
    assert result.ok is False
    assert result.missing_legs == ["CE 24500"]


# ── heuristic check (book believes FLAT) ──────────────────────────────────────

@pytest.mark.asyncio
async def test_heuristic_check_ok_when_flat_and_no_broker_position():
    broker = _FakeBroker(positions=[])
    result = await reconcile_book(broker, "NIFTY", [])
    assert result.ok is True
    assert result.heuristic_flags == []


@pytest.mark.asyncio
async def test_heuristic_check_flags_an_untracked_position_on_the_same_underlying():
    """The core motivating scenario: app believes FLAT (e.g. a corrupted
    persistence file), but the broker shows a real NIFTY option position
    that nothing is tracking."""
    broker = _FakeBroker(positions=[_FakePosition("NIFTY24AUG24500CE", -75)])
    result = await reconcile_book(broker, "NIFTY", [])
    assert result.ok is False
    assert result.heuristic_flags == ["NIFTY24AUG24500CE"]
    assert "HEURISTIC" in result.detail


@pytest.mark.asyncio
async def test_heuristic_check_ignores_a_different_underlying():
    broker = _FakeBroker(positions=[_FakePosition("BANKNIFTY24AUG51000CE", -30)])
    result = await reconcile_book(broker, "NIFTY", [])
    assert result.ok is True
    assert result.heuristic_flags == []


@pytest.mark.asyncio
async def test_heuristic_check_ignores_zero_qty_positions():
    broker = _FakeBroker(positions=[_FakePosition("NIFTY24AUG24500CE", 0)])
    result = await reconcile_book(broker, "NIFTY", [])
    assert result.ok is True


# ── fail-safe behavior ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_skips_gracefully_when_broker_is_none():
    result = await reconcile_book(None, "NIFTY", [ExpectedLeg("X", "leg")])
    assert result.ok is True
    assert result.skipped is True


@pytest.mark.asyncio
async def test_skips_gracefully_when_get_positions_raises():
    """A transient broker API failure must never itself be reported as a
    position mismatch -- that would be a false alarm eroding trust in the
    real alerts."""
    broker = _FakeBroker(raise_on_fetch=True)
    result = await reconcile_book(broker, "NIFTY", [ExpectedLeg("X", "leg")])
    assert result.ok is True
    assert result.skipped is True


@pytest.mark.asyncio
async def test_skips_gracefully_when_broker_has_no_get_positions_method():
    class _NoPositionsBroker:
        pass
    result = await reconcile_book(_NoPositionsBroker(), "NIFTY", [ExpectedLeg("X", "leg")])
    assert result.ok is True
    assert result.skipped is True


# ── reconcile_and_alert wiring ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_reconcile_and_alert_publishes_position_mismatch_on_real_mismatch():
    bus = _CapturingBus()
    broker = _FakeBroker(positions=[])   # book believes CE open, broker shows nothing
    legs = [ExpectedLeg("NIFTY24AUG24500CE", "CE 24500")]
    result = await reconcile_and_alert(
        bus, broker, "NIFTY", legs, strategy_name="SellStraddle",
        client_id="ssrajpal2001", binding_id="SA5770",
    )
    assert result.ok is False
    mismatches = [e for t, e in bus.published if t == Topic.SYSTEM_EVENT and e.code == SysEvent.POSITION_MISMATCH]
    assert len(mismatches) == 1
    assert "CE 24500" in mismatches[0].message


@pytest.mark.asyncio
async def test_reconcile_and_alert_publishes_nothing_when_reconciled_ok():
    bus = _CapturingBus()
    broker = _FakeBroker(positions=[_FakePosition("NIFTY24AUG24500CE", -75)])
    legs = [ExpectedLeg("NIFTY24AUG24500CE", "CE 24500")]
    result = await reconcile_and_alert(
        bus, broker, "NIFTY", legs, strategy_name="SellStraddle",
        client_id="ssrajpal2001", binding_id="SA5770",
    )
    assert result.ok is True
    assert bus.published == []


@pytest.mark.asyncio
async def test_reconcile_and_alert_publishes_nothing_when_skipped():
    bus = _CapturingBus()
    result = await reconcile_and_alert(
        bus, None, "NIFTY", [ExpectedLeg("X", "leg")], strategy_name="SellStraddle",
        client_id="ssrajpal2001", binding_id="SA5770",
    )
    assert result.skipped is True
    assert bus.published == []
