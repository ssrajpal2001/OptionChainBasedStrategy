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
    factory = BROKER_REGISTRY.get(binding.provider.lower())
    if factory is None:
        raise ValueError(
            f"Unknown broker provider '{binding.provider}'. "
            f"Available: {list(BROKER_REGISTRY)}"
        )
    return factory(binding, client_id)
