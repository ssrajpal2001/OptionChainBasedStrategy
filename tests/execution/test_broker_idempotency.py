"""
tests/execution/test_broker_idempotency.py -- find_recent_order() on the two
broker adapters that carry retry-wrapped real capital today (Zerodha,
Upstox). See execution_bridge/base_broker.py's own BaseBroker.
find_recent_order docstring for the full rationale: SmartOrderExecutor's
retry loop can otherwise fire a genuine duplicate real order when
place_order() raises AFTER the broker already accepted it (a lost response,
not a lost request).

Both broker objects are constructed via __new__ (bypassing __init__/auth --
neither needs a real BrokerBinding for this) with just the attributes
find_recent_order() actually touches set directly, matching this test
suite's existing minimal-fake-SDK-object pattern (see
test_zerodha_source_ip.py).
"""
from datetime import datetime, timedelta

import pytest

from config.global_config import IST
from execution_bridge.base_broker import MockBroker, OrderRequest, OrderSide, OrderType
from execution_bridge.broker_zerodha import ZerodhaBroker
from execution_bridge.broker_upstox import UpstoxBroker


def _req(side=OrderSide.SELL, qty=75, tag="SS_NIFTY_UPSTOX") -> OrderRequest:
    return OrderRequest(
        broker_symbol="NIFTY24700CE", exchange="NFO", side=side, qty=qty,
        order_type=OrderType.MARKET, tag=tag,
    )


# ── BaseBroker default (every broker without a real override) ──────────────

@pytest.mark.asyncio
async def test_base_broker_find_recent_order_is_a_safe_noop():
    assert await MockBroker("b", "c").find_recent_order(_req()) is None


# ── ZerodhaBroker ────────────────────────────────────────────────────────

def _zerodha(kite):
    b = ZerodhaBroker.__new__(ZerodhaBroker)
    b._kite = kite
    b.client_id = "c1"
    return b


class _FakeKiteOrders:
    def __init__(self, orders):
        self._orders = orders

    def orders(self):
        return self._orders


@pytest.mark.asyncio
async def test_zerodha_finds_matching_recent_order():
    now = datetime.now(IST)
    kite = _FakeKiteOrders([
        {"order_id": "OID1", "tradingsymbol": "NIFTY24700CE", "transaction_type": "SELL",
         "quantity": 75, "tag": "SS_NIFTY_UPSTOX", "order_timestamp": now - timedelta(seconds=5)},
    ])
    oid = await _zerodha(kite).find_recent_order(_req())
    assert oid == "OID1"


@pytest.mark.asyncio
async def test_zerodha_ignores_a_match_outside_the_recency_window():
    now = datetime.now(IST)
    kite = _FakeKiteOrders([
        {"order_id": "OID_OLD", "tradingsymbol": "NIFTY24700CE", "transaction_type": "SELL",
         "quantity": 75, "tag": "SS_NIFTY_UPSTOX", "order_timestamp": now - timedelta(seconds=90)},
    ])
    oid = await _zerodha(kite).find_recent_order(_req(), within_sec=30.0)
    assert oid is None


@pytest.mark.asyncio
async def test_zerodha_ignores_a_different_symbol_side_or_qty():
    now = datetime.now(IST)
    kite = _FakeKiteOrders([
        {"order_id": "WRONG_SYM", "tradingsymbol": "NIFTY24700PE", "transaction_type": "SELL",
         "quantity": 75, "tag": "SS_NIFTY_UPSTOX", "order_timestamp": now},
        {"order_id": "WRONG_SIDE", "tradingsymbol": "NIFTY24700CE", "transaction_type": "BUY",
         "quantity": 75, "tag": "SS_NIFTY_UPSTOX", "order_timestamp": now},
        {"order_id": "WRONG_QTY", "tradingsymbol": "NIFTY24700CE", "transaction_type": "SELL",
         "quantity": 150, "tag": "SS_NIFTY_UPSTOX", "order_timestamp": now},
    ])
    oid = await _zerodha(kite).find_recent_order(_req())
    assert oid is None


@pytest.mark.asyncio
async def test_zerodha_ignores_a_different_tag():
    """A same-shape order from a DIFFERENT strategy/binding must not match --
    tag narrows the heuristic specifically to avoid this false positive."""
    now = datetime.now(IST)
    kite = _FakeKiteOrders([
        {"order_id": "OTHER_STRATEGY", "tradingsymbol": "NIFTY24700CE", "transaction_type": "SELL",
         "quantity": 75, "tag": "CAG_NIFTY_SA5770", "order_timestamp": now},
    ])
    oid = await _zerodha(kite).find_recent_order(_req(tag="SS_NIFTY_UPSTOX"))
    assert oid is None


@pytest.mark.asyncio
async def test_zerodha_picks_the_most_recent_among_multiple_matches():
    now = datetime.now(IST)
    kite = _FakeKiteOrders([
        {"order_id": "OLDER", "tradingsymbol": "NIFTY24700CE", "transaction_type": "SELL",
         "quantity": 75, "tag": "SS_NIFTY_UPSTOX", "order_timestamp": now - timedelta(seconds=10)},
        {"order_id": "NEWER", "tradingsymbol": "NIFTY24700CE", "transaction_type": "SELL",
         "quantity": 75, "tag": "SS_NIFTY_UPSTOX", "order_timestamp": now - timedelta(seconds=1)},
    ])
    oid = await _zerodha(kite).find_recent_order(_req())
    assert oid == "NEWER"


@pytest.mark.asyncio
async def test_zerodha_degrades_safely_when_orders_query_raises():
    class _BrokenKite:
        def orders(self):
            raise RuntimeError("Kite API unreachable")
    oid = await _zerodha(_BrokenKite()).find_recent_order(_req())
    assert oid is None


@pytest.mark.asyncio
async def test_zerodha_no_kite_instance_returns_none():
    b = ZerodhaBroker.__new__(ZerodhaBroker)
    b._kite = None
    b.client_id = "c1"
    assert await b.find_recent_order(_req()) is None


# ── UpstoxBroker ─────────────────────────────────────────────────────────

class _FakeAttr:
    """Mimics upstox_client's generated SDK objects, which expose fields as
    plain attributes (o.trading_symbol), not dict keys."""
    def __init__(self, **kw):
        self.__dict__.update(kw)


class _FakeOrderApi:
    def __init__(self, orders, status="success"):
        self._orders = orders
        self._status = status

    def get_order_book(self, api_version="2.0"):
        return _FakeAttr(status=self._status, data=self._orders)


def _upstox(order_api, instrument_map=None):
    b = UpstoxBroker.__new__(UpstoxBroker)
    b._order_api = order_api
    b.client_id = "c1"
    b._instrument_map = instrument_map or {}
    return b


@pytest.mark.asyncio
async def test_upstox_finds_matching_recent_order_by_trading_symbol():
    now = datetime.now(IST)
    api = _FakeOrderApi([
        _FakeAttr(order_id="OID1", trading_symbol="NIFTY24700CE", instrument_token=None,
                  transaction_type="SELL", quantity=75, tag="SS_NIFTY_UPSTOX",
                  order_timestamp=now - timedelta(seconds=5)),
    ])
    oid = await _upstox(api).find_recent_order(_req())
    assert oid == "OID1"


@pytest.mark.asyncio
async def test_upstox_finds_matching_recent_order_by_instrument_token():
    now = datetime.now(IST)
    api = _FakeOrderApi([
        _FakeAttr(order_id="OID2", trading_symbol="SOMETHING_ELSE",
                  instrument_token="NSE_FO|12345", transaction_type="SELL", quantity=75,
                  tag="SS_NIFTY_UPSTOX", order_timestamp=now - timedelta(seconds=5)),
    ])
    oid = await _upstox(api, instrument_map={"NIFTY24700CE": "NSE_FO|12345"}).find_recent_order(_req())
    assert oid == "OID2"


@pytest.mark.asyncio
async def test_upstox_ignores_a_match_outside_the_recency_window():
    now = datetime.now(IST)
    api = _FakeOrderApi([
        _FakeAttr(order_id="OLD", trading_symbol="NIFTY24700CE", instrument_token=None,
                  transaction_type="SELL", quantity=75, tag="SS_NIFTY_UPSTOX",
                  order_timestamp=now - timedelta(seconds=90)),
    ])
    oid = await _upstox(api).find_recent_order(_req(), within_sec=30.0)
    assert oid is None


@pytest.mark.asyncio
async def test_upstox_ignores_a_different_tag():
    now = datetime.now(IST)
    api = _FakeOrderApi([
        _FakeAttr(order_id="OTHER", trading_symbol="NIFTY24700CE", instrument_token=None,
                  transaction_type="SELL", quantity=75, tag="CAG_NIFTY_SA5770",
                  order_timestamp=now),
    ])
    oid = await _upstox(api).find_recent_order(_req(tag="SS_NIFTY_UPSTOX"))
    assert oid is None


@pytest.mark.asyncio
async def test_upstox_degrades_safely_on_non_success_status():
    api = _FakeOrderApi([], status="error")
    oid = await _upstox(api).find_recent_order(_req())
    assert oid is None


@pytest.mark.asyncio
async def test_upstox_degrades_safely_when_order_book_query_raises():
    class _BrokenOrderApi:
        def get_order_book(self, api_version="2.0"):
            raise RuntimeError("Upstox API unreachable")
    oid = await _upstox(_BrokenOrderApi()).find_recent_order(_req())
    assert oid is None


@pytest.mark.asyncio
async def test_upstox_no_order_api_returns_none():
    b = UpstoxBroker.__new__(UpstoxBroker)
    b._order_api = None
    b.client_id = "c1"
    b._instrument_map = {}
    assert await b.find_recent_order(_req()) is None
