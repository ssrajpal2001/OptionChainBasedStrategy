"""
strategies/registry.py — central strategy registry.

New strategies are registered here so ``run_system.py`` can construct and wire them
without hard-coding imports or construction logic.
"""
from __future__ import annotations

from typing import Any, Dict, List

from strategies.core.per_index_manager import PerIndexManager
from strategies.iron_condor import IronCondorStrategy
from strategies.sell_straddle import StraddleBookManager
from strategies.trap_scanner import TrapBookManager


STRATEGY_REGISTRY: Dict[str, Dict[str, Any]] = {
    "sell_straddle": {
        "manager_class": StraddleBookManager,
        "per_binding": True,
    },
    "trap_scanner": {
        "manager_class": TrapBookManager,
        "per_binding": True,
    },
    "iron_condor": {
        "manager_class": PerIndexManager,
        "per_binding": False,
        "strategy_class": IronCondorStrategy,
    },
}


def create_strategy_manager(name: str, bus, cfg, client_db, monitored_indices):
    """
    Factory: build the manager for ``name``.

    Per-binding managers receive ``(bus, cfg, client_db, monitored_indices)``.
    Per-index managers receive ``(bus, cfg, monitored_indices, strategy_class)``.
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
