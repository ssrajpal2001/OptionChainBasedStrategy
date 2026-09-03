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
from strategies.d1_trap_option import D1TrapOptionBookManager
from strategies.fvg import FVGBookManager
from strategies.oi_flow import OIFlowBookManager
from strategies.liquidity_sweep import LiquiditySweepBookManager
from strategies.liquidity_trap import LiquidityTrapBookManager
from strategies.oi_orb_screener import OiOrbScreenerBookManager
from strategies.cag_straddle import CagStraddleBookManager


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
    # d1_trap_option is the canonical launch key for ALL trap scanner variants.
    # d1_trap_index and d1_trap_fno are deployment-level names stored in the DB;
    # the single D1TrapOptionBookManager._wanted() scans all three names and
    # spawns one book per (client, binding, underlying) regardless of which
    # variant the deployment was saved as.
    "d1_trap_option": {
        "manager_class": D1TrapOptionBookManager,
        "per_binding": True,
    },
    "fvg": {
        "manager_class": FVGBookManager,
        "per_binding": True,
    },
    # 2026-08-12: fully standalone -- shares no runtime infra (Topics,
    # events, bridge, book manager) with any other strategy above. See
    # strategies/oi_flow/__init__.py.
    "oi_flow": {
        "manager_class": OIFlowBookManager,
        "per_binding": True,
    },
    # 2026-08-19: fully standalone -- shares no runtime infra (Topics,
    # events, bridge, book manager) with any other strategy above, same
    # mandate as oi_flow. See strategies/liquidity_sweep/__init__.py.
    "liquidity_sweep": {
        "manager_class": LiquiditySweepBookManager,
        "per_binding": True,
    },
    # 2026-08-21: fully standalone -- shares no runtime infra (Topics,
    # events, bridge, book manager) with any other strategy above, same
    # mandate as oi_flow/liquidity_sweep. See strategies/liquidity_trap/
    # __init__.py. Built and validated as a real-data (1yr SENSEX spot)
    # backtest first -- scripts/liquidity_trap_backtest.py.
    "liquidity_trap": {
        "manager_class": LiquidityTrapBookManager,
        "per_binding": True,
    },
    # 2026-08-24: fully standalone -- shares no runtime infra (Topics,
    # events, bridge, book manager) with any other strategy above, same
    # mandate as oi_flow/liquidity_sweep/liquidity_trap. See
    # strategies/oi_orb_screener/__init__.py. Ported from the standalone
    # Colab screener (colab/oi_orb_screener/) as a connectivity/plumbing
    # proof (real paper_route order placement + live LTP subscription) --
    # NO SL/target/risk-cap logic this pass, EOD square-off only. First
    # strategy in this codebase's live pipeline to trade individual F&O
    # STOCKS (chosen dynamically each day) rather than a fixed underlying.
    "oi_orb_screener": {
        "manager_class": OiOrbScreenerBookManager,
        "per_binding": True,
    },
    # 2026-08-27: 8th standalone strategy -- explicit exception to the prior
    # 7-strategy cap (see CLAUDE.md's own CAG Straddle section), same
    # zero-shared-runtime mandate as oi_flow/liquidity_sweep/liquidity_trap/
    # oi_orb_screener. See strategies/cag_straddle/__init__.py. Built from a
    # real-data-validated backtest --
    # scripts/nifty_1500_sr_breakout_backtest.py.
    "cag_straddle": {
        "manager_class": CagStraddleBookManager,
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
