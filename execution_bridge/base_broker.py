"""
execution_bridge/base_broker.py — Abstract execution broker interface.

Every concrete broker (Shoonya, Fyers, Angel One, Dhan) must subclass
BaseBroker and implement the abstract methods.  The ExecutionRouter only
ever calls BaseBroker methods — swapping a broker is zero logic change.

Adding a new broker:
  1. Create execution_bridge/broker_<name>.py
  2. Subclass BaseBroker, implement all @abstractmethods
  3. Register in BROKER_REGISTRY at the bottom of this file
  4. Done — the router picks it up automatically from client credentials
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Dict, List, Optional

from config.global_config import IST

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Order Domain Objects
# ─────────────────────────────────────────────────────────────────────────────

class OrderSide(Enum):
    BUY  = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT  = "LIMIT"
    SL_M   = "SL-M"       # Stop-loss market
    SL_L   = "SL-L"       # Stop-loss limit


class OrderStatus(Enum):
    PENDING   = auto()
    OPEN      = auto()
    COMPLETE  = auto()
    CANCELLED = auto()
    REJECTED  = auto()
    UNKNOWN   = auto()


@dataclass
class OrderRequest:
    broker_symbol: str           # Broker-specific symbol (from SymbolTranslator)
    exchange: str                # "NFO", "BSE", etc.
    side: OrderSide
    qty: int
    order_type: OrderType
    price: float = 0.0
    trigger_price: float = 0.0
    product: str = "INTRADAY"
    tag: str = ""                # For linking to position_id
    client_id: str = ""          # Which client account
    time_in_force: str = ""      # "", "ioc", "gtc", "fok" — IOC = fill-now-or-kill (no resting order)


@dataclass
class OrderFill:
    order_id: str
    broker_symbol: str
    side: OrderSide
    qty: int
    avg_price: float
    status: OrderStatus
    timestamp: datetime = field(default_factory=lambda: datetime.now(IST))
    client_id: str = ""
    tag: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PositionRecord:
    symbol: str
    qty: int
    avg_price: float
    pnl: float
    product: str


# ─────────────────────────────────────────────────────────────────────────────
# Abstract Base Broker
# ─────────────────────────────────────────────────────────────────────────────

class BaseBroker(ABC):
    """
    Execution-only broker interface.

    Concrete classes must NOT perform any strategy logic.  They are
    thin wrappers around one broker's REST/WS API.

    All methods are async to avoid blocking the event loop during
    network round-trips.  The ExecutionRouter calls them via
    asyncio.gather for concurrent multi-broker dispatch.
    """

    def __init__(self, binding_id: str, client_id: str) -> None:
        self.binding_id = binding_id
        self.client_id = client_id
        self._authenticated   = False
        self._trading_mode_raw = "paper"  # "paper" | "live" — set by each broker at auth

    @abstractmethod
    async def authenticate(self) -> bool:
        """Login / token refresh.  Returns True on success."""

    @abstractmethod
    async def logout(self) -> None:
        """Gracefully invalidate session."""

    @abstractmethod
    async def place_order(self, req: OrderRequest) -> str:
        """Submit an order.  Returns broker order_id string."""

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order.  Returns True if accepted."""

    @abstractmethod
    async def get_order_status(self, order_id: str) -> OrderFill:
        """Fetch current fill status for an order."""

    @abstractmethod
    async def get_positions(self) -> List[PositionRecord]:
        """Return all current intraday positions."""

    @abstractmethod
    async def get_funds(self) -> Dict[str, float]:
        """Return {'available': float, 'used': float}."""

    async def find_recent_order(self, req: "OrderRequest", within_sec: float = 30.0) -> Optional[str]:
        """2026-09-16, direct user spec: idempotency guard for
        SmartOrderExecutor._place_with_retries (execution_bridge/
        smart_executor.py) -- a retry there fires on ANY exception from
        place_order(), including one that happens AFTER the broker already
        accepted the order but the response never made it back (a network
        blip mid-response, not mid-request). Without this check, that retry
        places a genuine SECOND real order.

        Returns the order_id of a real order at the broker that matches
        this request's own identity (symbol/side/qty/tag) and was placed
        within the last `within_sec` seconds, or None if no such order is
        found (or this broker doesn't support the check at all).

        Default: always None (safe no-op) -- every broker that doesn't
        override this keeps today's exact retry behavior, zero regression
        risk. Only overridden for brokers that actually carry retry-wrapped
        real capital today (Zerodha, Upstox) -- see each adapter's own
        implementation. This is a best-effort HEURISTIC match (symbol+side+
        qty+tag+recency), not a cryptographic guarantee -- a genuine
        coincidental collision within the same few seconds is possible,
        though unlikely given tags are already strategy-and-underlying-
        specific. Any failure in an override (network error, broker API
        change) must be swallowed and return None -- never block or corrupt
        the caller's own retry loop."""
        return None

    @property
    def is_authenticated(self) -> bool:
        return self._authenticated

    async def __aenter__(self) -> "BaseBroker":
        await self.authenticate()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.logout()


# ─────────────────────────────────────────────────────────────────────────────
# Mock Broker — for paper trading and tests
# ─────────────────────────────────────────────────────────────────────────────

class MockBroker(BaseBroker):
    """Simulates order fills instantly with no network calls."""

    def __init__(self, binding_id: str, client_id: str, capital: float = 500_000.0) -> None:
        super().__init__(binding_id, client_id)
        self._counter = 0
        self._orders: Dict[str, OrderFill] = {}
        self._funds = {"available": capital, "used": 0.0}

    async def authenticate(self) -> bool:
        self._authenticated = True
        logger.info("MockBroker [%s/%s]: Authenticated.", self.client_id, self.binding_id)
        return True

    async def logout(self) -> None:
        self._authenticated = False

    async def place_order(self, req: OrderRequest) -> str:
        self._counter += 1
        oid = f"MOCK-{self.client_id}-{self._counter:05d}"
        price = req.price if req.order_type == OrderType.LIMIT else (req.price or 100.0)
        fill = OrderFill(
            order_id=oid, broker_symbol=req.broker_symbol,
            side=req.side, qty=req.qty, avg_price=price,
            status=OrderStatus.COMPLETE, client_id=self.client_id, tag=req.tag,
        )
        self._orders[oid] = fill
        cost = price * req.qty
        if req.side == OrderSide.BUY:
            self._funds["available"] -= cost
            self._funds["used"] += cost
        else:
            self._funds["available"] += cost
            self._funds["used"] = max(0.0, self._funds["used"] - cost)
        logger.info("MockBroker ORDER: %s %s %s qty=%d @ %.2f → %s",
                    self.client_id, req.side.value, req.broker_symbol, req.qty, price, oid)
        return oid

    async def cancel_order(self, order_id: str) -> bool:
        if order_id in self._orders:
            self._orders[order_id].status = OrderStatus.CANCELLED
            return True
        return False

    async def get_order_status(self, order_id: str) -> OrderFill:
        return self._orders.get(order_id, OrderFill(
            order_id=order_id, broker_symbol="",
            side=OrderSide.BUY, qty=0, avg_price=0,
            status=OrderStatus.UNKNOWN,
        ))

    async def get_positions(self) -> List[PositionRecord]:
        return []

    async def get_funds(self) -> Dict[str, float]:
        return dict(self._funds)


# ─────────────────────────────────────────────────────────────────────────────
# Broker Registry — maps provider string → factory function
# ─────────────────────────────────────────────────────────────────────────────

from config.client_profiles import BrokerBinding


def _mock_factory(b: BrokerBinding, client_id: str) -> BaseBroker:
    from config.client_profiles import ClientProfile
    # Attempt to find capital from caller context — default 500k
    return MockBroker(b.binding_id, client_id, capital=500_000.0)


BROKER_REGISTRY: Dict[str, Any] = {
    "mock": _mock_factory,
    # "shoonya":  lambda b, cid: ShoonyaBroker(b, cid),   ← added by broker module
    # "fyers":    lambda b, cid: FyersBroker(b, cid),
    # "angelone": lambda b, cid: AngelBroker(b, cid),
    # "dhan":     lambda b, cid: DhanBroker(b, cid),
}


def create_broker(binding: BrokerBinding, client_id: str) -> BaseBroker:
    # Paper bindings must never hit a real exchange. Use the mock broker so
    # that paper trading stays local and fast, and rejected real orders do
    # not spam the broker's OMS.
    if getattr(binding, "trading_mode", "paper").lower() == "paper":
        return _mock_factory(binding, client_id)
    factory = BROKER_REGISTRY.get(binding.provider.lower())
    if factory is None:
        raise ValueError(
            f"Unknown broker provider '{binding.provider}'. "
            f"Available: {list(BROKER_REGISTRY)}"
        )
    return factory(binding, client_id)
