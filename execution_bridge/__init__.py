"""
execution_bridge — Order dispatch layer.

Importing this package auto-registers all concrete broker implementations
into BROKER_REGISTRY via their module-level side effects.
"""

from execution_bridge.base_broker import (
    BaseBroker,
    MockBroker,
    OrderFill,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionRecord,
    BROKER_REGISTRY,
    create_broker,
)
from execution_bridge.execution_router import ExecutionRouter, CostCalc

# Trigger self-registration of all broker modules
import execution_bridge.broker_fyers    # noqa: F401
import execution_bridge.broker_angel    # noqa: F401
import execution_bridge.broker_dhan     # noqa: F401
import execution_bridge.broker_upstox   # noqa: F401
import execution_bridge.broker_zerodha  # noqa: F401

__all__ = [
    "BaseBroker", "MockBroker", "OrderFill", "OrderRequest",
    "OrderSide", "OrderStatus", "OrderType", "PositionRecord",
    "BROKER_REGISTRY", "create_broker",
    "ExecutionRouter", "CostCalc",
]
