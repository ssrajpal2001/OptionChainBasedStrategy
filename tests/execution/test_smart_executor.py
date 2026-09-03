"""SmartOrderExecutor state machine: market, limit-fills, chase-then-fill, chase-exhausted→market,
cancel-race (no double-fill), partial fill. Real-money edges covered."""
import asyncio

from execution_bridge.smart_executor import SmartOrderExecutor
from execution_bridge.base_broker import OrderFill, OrderSide, OrderStatus


class _MockBroker:
    """Scripted broker. `behavior[order_seq]` = ('fill'|'open'|'partial', qty, avg)."""
    def __init__(self, quote=(100.0, 104.0), script=None):
        self._quote = quote
        self._script = script or []      # per-placed-order outcome
        self._n = 0
        self.placed = []                 # (order_type, side, qty, price)
        self.cancelled = []
        self._status = {}                # order_id -> (status, qty, avg)

    async def get_quote(self, symbol):
        return self._quote

    async def place_order(self, req):
        oid = f"O{self._n}"
        outcome = self._script[self._n] if self._n < len(self._script) else ("fill", req.qty, 0.0)
        kind, q, avg = outcome
        # default avg: limit→its price, market→ask (worst)
        if avg == 0.0:
            avg = float(req.price) if str(req.order_type).endswith("LIMIT") else self._quote[1]
        self.placed.append((str(req.order_type), req.side, req.qty, float(req.price or 0)))
        if kind == "fill":
            self._status[oid] = (OrderStatus.COMPLETE, q if q else req.qty, avg)
        elif kind == "partial":
            self._status[oid] = (OrderStatus.OPEN, q, avg)   # partial shows OPEN until cancel
        else:  # open / never fills
            self._status[oid] = (OrderStatus.OPEN, 0, 0.0)
        self._n += 1
        return oid

    async def get_order_status(self, oid):
        st, q, avg = self._status.get(oid, (OrderStatus.UNKNOWN, 0, 0.0))
        return OrderFill(order_id=oid, broker_symbol="X", side=OrderSide.SELL, qty=q, avg_price=avg, status=st)

    async def cancel_order(self, oid):
        self.cancelled.append(oid)
        st, q, avg = self._status.get(oid, (OrderStatus.UNKNOWN, 0, 0.0))
        if st == OrderStatus.OPEN and q == 0:
            self._status[oid] = (OrderStatus.CANCELLED, 0, 0.0)
        return True


def _exec():
    return SmartOrderExecutor(fill_timeout_sec=0.3, chase_attempts=2, poll_interval=0.05)


def _run(coro):
    return asyncio.run(coro)


def _leg(broker, use_limit):
    return _run(_exec().execute_leg(
        broker, broker_symbol="C-BTC-60000-130626", exchange="DELTA", side=OrderSide.SELL,
        qty=10, product="NRML", tag="t", client_id="c", use_limit=use_limit, tick=0.1))


def test_market_path_fills_at_ask():
    b = _MockBroker(script=[("fill", 10, 0.0)])
    fill = _leg(b, use_limit=False)
    assert fill.completed and fill.filled_qty == 10 and fill.avg_price == 104.0   # ask
    assert b.placed[0][0].endswith("MARKET")


def test_limit_fills_at_mid_immediately():
    b = _MockBroker(quote=(100.0, 104.0), script=[("fill", 10, 0.0)])
    fill = _leg(b, use_limit=True)
    assert fill.completed and fill.filled_qty == 10
    assert fill.avg_price == 102.0                         # mid (100+104)/2
    assert b.placed[0][0].endswith("LIMIT")


# ── 2026-08-06 CONFIRM-MODEL REDESIGN: on_placed callback + placement retries ──────────────


def test_on_placed_fires_once_before_fill_confirms():
    """The core speed fix: on_placed must fire the INSTANT an order_id exists, not wait for
    the fill -- so a caller can react to 'reached the broker' without blocking on a full
    confirm cycle."""
    b = _MockBroker(script=[("fill", 10, 0.0)])
    calls: list = []

    async def _on_placed(oid):
        calls.append(oid)

    fill = _run(_exec().execute_leg(
        b, broker_symbol="NIFTY24700CE", exchange="NFO", side=OrderSide.SELL,
        qty=10, product="NRML", tag="t", client_id="c", use_limit=False,
        on_placed=_on_placed))

    assert calls == ["O0"]
    assert fill.completed and fill.filled_qty == 10


def test_on_placed_fires_exactly_once_even_with_chase_and_mopup():
    """A LIMIT chase can place several orders for the same leg (ladder rungs + market
    mop-up) -- on_placed must only fire for the FIRST one, not once per order."""
    b = _MockBroker(script=[("open", 0, 0.0), ("open", 0, 0.0), ("open", 0, 0.0), ("fill", 10, 0.0)])
    calls: list = []

    async def _on_placed(oid):
        calls.append(oid)

    fill = _run(_exec().execute_leg(
        b, broker_symbol="C-BTC-60000-130626", exchange="DELTA", side=OrderSide.SELL,
        qty=10, product="NRML", tag="t", client_id="c", use_limit=True, tick=0.1,
        on_placed=_on_placed))

    assert len(calls) == 1
    assert calls[0] == "O0"
    assert fill.completed


class _FlakyBroker(_MockBroker):
    """place_order raises for the first `fail_times` calls, then behaves normally."""
    def __init__(self, fail_times: int, **kw):
        super().__init__(**kw)
        self._fail_times = fail_times
        self._attempts = 0

    async def place_order(self, req):
        self._attempts += 1
        if self._attempts <= self._fail_times:
            raise ConnectionError("transient broker API error")
        return await super().place_order(req)


def test_placement_retries_on_transient_failure_then_succeeds():
    b = _FlakyBroker(fail_times=2, script=[("fill", 10, 0.0)])
    fill = _run(_exec().execute_leg(
        b, broker_symbol="NIFTY24700CE", exchange="NFO", side=OrderSide.SELL,
        qty=10, product="NRML", tag="t", client_id="c", use_limit=False))
    assert fill.completed and fill.filled_qty == 10
    assert b._attempts == 3  # 2 failures + 1 success, within the 3-attempt budget


def test_placement_gives_up_after_3_failed_attempts():
    import pytest
    from execution_bridge.smart_executor import OrderPlacementFailed
    b = _FlakyBroker(fail_times=10, script=[("fill", 10, 0.0)])
    with pytest.raises(OrderPlacementFailed, match="order placement failed after 3 attempts"):
        _run(_exec().execute_leg(
            b, broker_symbol="NIFTY24700CE", exchange="NFO", side=OrderSide.SELL,
            qty=10, product="NRML", tag="t", client_id="c", use_limit=False))
    assert b._attempts == 3  # never more than 3 -- no indefinite retrying at this layer


def test_ioc_chase_fills_on_second_rung_no_cancel():
    # IOC ladder: rung-0 (mid) doesn't cross → killed (0); rung-1 (more marketable) fills.
    # No resting order ⇒ NO cancel call, and no market fallback needed.
    b = _MockBroker(script=[("open", 0, 0.0), ("fill", 10, 0.0)])
    fill = _leg(b, use_limit=True)
    assert fill.completed and fill.filled_qty == 10
    assert b.cancelled == []                               # IOC self-cancels — we never call cancel
    assert not any(p[0].endswith("MARKET") for p in b.placed)   # filled via IOC limit, no market
    assert 100.0 <= fill.avg_price <= 104.0                # filled within the spread (a ladder rung)


def test_chase_exhausted_falls_back_to_market():
    b = _MockBroker(script=[("open", 0, 0.0), ("open", 0, 0.0), ("open", 0, 0.0), ("fill", 10, 0.0)])
    fill = _leg(b, use_limit=True)
    assert fill.completed and fill.filled_qty == 10
    assert b.placed[-1][0].endswith("MARKET")              # final leg is a market order
    assert fill.avg_price == 104.0                         # market filled at ask
    assert b.cancelled == []                               # IOC path never cancels


def test_ioc_books_authoritative_qty_no_double():
    # The IOC fill is booked ONCE from the broker's authoritative status — never doubled.
    b = _MockBroker(script=[("fill", 10, 0.0)])
    fill = _leg(b, use_limit=True)
    assert fill.filled_qty == 10                           # exactly 10, not 20
    assert len([p for p in b.placed if p[0].endswith("LIMIT")]) == 1   # filled on the first rung
    assert not any(p[0].endswith("MARKET") for p in b.placed)


def test_ioc_partial_then_market_remainder():
    # IOC rung-0 partially fills 4 (rest killed), rungs 1-2 cross nothing, MARKET mops up the 6.
    b = _MockBroker(script=[("partial", 4, 102.0), ("open", 0, 0.0), ("open", 0, 0.0), ("fill", 6, 104.0)])
    fill = _leg(b, use_limit=True)
    assert fill.filled_qty == 10 and fill.completed
    # VWAP: 4@102 + 6@104 = (408+624)/10 = 103.2
    assert abs(fill.avg_price - 103.2) < 1e-6
    assert b.placed[-1][0].endswith("MARKET")
    assert b.cancelled == []
