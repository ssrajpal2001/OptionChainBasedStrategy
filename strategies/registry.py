"""
strategies/registry.py — central strategy registry.

New strategies are registered here so ``run_system.py`` can construct and wire them
without hard-coding imports or construction logic.
"""
from __future__ import annotations

from typing import Any, Dict, List

from strategies.sell_straddle import StraddleBookManager
from strategies.v4_cascade_book_manager import V4CascadeBookManager
from strategies.fno_positional import FnOPositionalBookManager
from strategies.hourly_breakout import HourlyBreakoutBookManager


STRATEGY_REGISTRY: Dict[str, Dict[str, Any]] = {
    "sell_straddle": {
        "manager_class": StraddleBookManager,
        "per_binding": True,
    },
    "v4_cascade": {
        "manager_class": V4CascadeBookManager,
        "per_binding": True,
    },
    "fno_positional": {
        "manager_class": FnOPositionalBookManager,
        "per_binding": True,
    },
    "hourly_breakout": {
        "manager_class": HourlyBreakoutBookManager,
        "per_binding": True,
    },
}


def create_strategy_manager(name: str, bus, cfg, client_db, monitored_indices):
    """
    Factory: build the manager for ``name``.

    Per-binding managers receive ``(bus, cfg, client_db, monitored_indices)``.
    """
    if name not in STRATEGY_REGISTRY:
        raise KeyError(f"Unknown strategy '{name}'. Registered: {list(STRATEGY_REGISTRY)}")

    entry = STRATEGY_REGISTRY[name]
    manager_class = entry["manager_class"]

    if entry.get("per_binding"):
        return manager_class(bus, cfg, client_db, monitored_indices)

    return manager_class(
        bus, cfg, monitored_indices,
        entry.get("strategy_class"),
    )


def get_strategy_names() -> List[str]:
    """Return the list of registered strategy keys."""
    return list(STRATEGY_REGISTRY.keys())
