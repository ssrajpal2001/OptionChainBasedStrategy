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


def test_chase_then_fills_on_second_attempt():
    b = _MockBroker(script=[("open", 0, 0.0), ("fill", 10, 0.0)])   # 1st limit hangs, 2nd fills
    fill = _leg(b, use_limit=True)
    assert fill.completed and fill.filled_qty == 10 and fill.avg_price == 102.0
    assert b.cancelled == ["O0"]                           # first was cancelled, no market needed


def test_chase_exhausted_falls_back_to_market():
    b = _MockBroker(script=[("open", 0, 0.0), ("open", 0, 0.0), ("open", 0, 0.0), ("fill", 10, 0.0)])
    fill = _leg(b, use_limit=True)
    assert fill.completed and fill.filled_qty == 10
    assert b.placed[-1][0].endswith("MARKET")              # final leg is a market order
    assert fill.avg_price == 104.0                         # market filled at ask


def test_cancel_race_does_not_double_fill():
    # Limit shows OPEN during polling, but on cancel it reconciles as FILLED → must NOT also market.
    b = _MockBroker(script=[("open", 0, 0.0)])
    b._status["O0"] = (OrderStatus.OPEN, 0, 0.0)
    # monkeypatch cancel to "discover" it actually filled (the race):
    async def _cancel(oid):
        b.cancelled.append(oid)
        b._status[oid] = (OrderStatus.COMPLETE, 10, 102.0)
        return True
    b.cancel_order = _cancel
    fill = _leg(b, use_limit=True)
    assert fill.filled_qty == 10 and fill.avg_price == 102.0
    assert not any(p[0].endswith("MARKET") for p in b.placed)   # no extra market order → no double


def test_partial_then_market_remainder():
    # First limit fills 4 (then cancel books it), market completes the remaining 6 at ask.
    b = _MockBroker(script=[("open", 0, 0.0), ("open", 0, 0.0), ("open", 0, 0.0), ("fill", 6, 0.0)])
    async def _cancel(oid):
        b.cancelled.append(oid)
        if oid == "O0":
            b._status[oid] = (OrderStatus.COMPLETE, 4, 102.0)   # 4 filled on the race
        else:
            b._status[oid] = (OrderStatus.CANCELLED, 0, 0.0)
        return True
    b.cancel_order = _cancel
    fill = _leg(b, use_limit=True)
    assert fill.filled_qty == 10 and fill.completed
    # VWAP: 4@102 + 6@104 = (408+624)/10 = 103.2
    assert abs(fill.avg_price - 103.2) < 1e-6


def test_maker_fills_during_settle_no_double():
    """The PRODUCTION bug: a resting LIMIT (maker) still shows OPEN right after cancel, then fills a
    moment later. The executor must WAIT for the terminal state (settle), book that fill, and NOT
    place another order — otherwise it double-sells (12 instead of 6)."""
    b = _MockBroker(script=[("open", 0, 0.0)])
    polls = {"n": 0}
    base_status = b.get_order_status

    async def _status(oid):
        # O0: OPEN for the first 2 polls after cancel, then COMPLETE (maker fills late).
        if oid == "O0" and oid in b.cancelled:
            polls["n"] += 1
            if polls["n"] >= 2:
                return OrderFill(order_id=oid, broker_symbol="X", side=OrderSide.SELL,
                                 qty=10, avg_price=101.5, status=OrderStatus.COMPLETE)
            return OrderFill(order_id=oid, broker_symbol="X", side=OrderSide.SELL,
                             qty=0, avg_price=0.0, status=OrderStatus.OPEN)
        return await base_status(oid)
    b.get_order_status = _status

    fill = _leg(b, use_limit=True)
    assert fill.filled_qty == 10                          # booked the late maker fill
    assert not any(p[0].endswith("MARKET") for p in b.placed)   # NO extra order → no double-fill
    assert len([p for p in b.placed if p[0].endswith("LIMIT")]) == 1   # only the one limit
