"""
strategies/registry.py — central strategy registry.

New strategies are registered here so ``run_system.py`` can construct and wire them
without hard-coding imports or construction logic.
"""
from __future__ import annotations

from typing import Any, Dict, List

from strategies.sell_straddle import StraddleBookManager
from strategies.oi_orb_screener import OiOrbScreenerBookManager
from strategies.cag_straddle import CagStraddleBookManager


STRATEGY_REGISTRY: Dict[str, Dict[str, Any]] = {
    "sell_straddle": {
        "manager_class": StraddleBookManager,
        "per_binding": True,
    },
    # 2026-09-06 (2nd pass): v4_cascade, fno_positional, and hourly_breakout
    # removed entirely -- same scope-clarity decision as the removal below
    # (not disk space; user confirmed only sell_straddle/oi_orb_screener/
    # cag_straddle are the actively-used 3). Recoverable via git history.
    # 2026-09-06: d1_trap_option, fvg, oi_flow, liquidity_sweep, and
    # liquidity_trap were removed entirely (code, bridges, tests, backtest
    # scripts) -- direct user decision, all fully stopped and not needed
    # going forward. Current focus is sell_straddle, oi_orb_screener, and
    # cag_straddle only. Recoverable via git history if ever needed again.
    # NOTE: oi_orb_screener's own trap-zone detection (screener.py's
    # bull_trap_zones/sharp_bear_zones) and its S1/R1 ratchet TSL still
    # depend on primitives that were RESCUED (not deleted) into
    # strategies/core/support_resistance.py and
    # strategies/core/trap_zone_utils.py before this removal.
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
