"""
strategies/core/gate.py — reusable per-binding trade gate.

Mirrors the gating logic from:
  - strategies/sell_straddle.py::_any_active_terminal (per-binding path)
  - strategies/trap_scanner_engine.py::_can_trade

Fail-open when no ClientDB is wired so unit tests / headless runs are
unaffected. Cached for 5 seconds to keep the per-tick hot path cheap.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 5.0

# (client_id, binding_id, strategy_name, underlying, db_id) -> (monotonic_ts, result)
_cache: Dict[Tuple[str, str, str, str, int], Tuple[float, bool]] = {}


def _now() -> float:
    return time.monotonic()


def _cache_key(client_id: str, binding_id: str, client_db: Any, strategy_name: str, underlying: str) -> Tuple[str, str, str, str, int]:
    return (
        client_id, binding_id, strategy_name.lower(),
        (underlying or "").upper(), id(client_db),
    )


def _evaluate(client_id: str, binding_id: str, client_db: Any, strategy_name: str, underlying: str) -> bool:
    """Uncached gate evaluation."""
    try:
        bindings = {b.get("binding_id"): b for b in client_db.get_bindings_safe_sync(client_id)}
        binding = bindings.get(binding_id)
        if not binding:
            return False
        if not binding.get("terminal_connected"):
            return False

        strategy = strategy_name.lower()
        if strategy == "trap_scanner":
            return bool(binding.get("is_trade_enabled"))

        if strategy == "sell_straddle":
            # Per-binding path: the deployment's per-strategy Run toggle
            # (is_running) is the authority.
            try:
                deployments = client_db.get_deployments_sync(client_id)
            except Exception:
                deployments = []
            return any(
                d.get("binding_id") == binding_id
                and str(d.get("strategy_name", "")).lower() == "sell_straddle"
                and str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper()
                == (underlying or "").upper()
                and int(d.get("is_running", 0) or 0) == 1
                for d in deployments
            )

        # Unknown strategy: terminal_connected is the only generic check we can apply.
        return True
    except Exception as exc:
        logger.debug("Gate evaluation error for %s/%s/%s: %s", client_id, binding_id, strategy_name, exc)
        return False


def can_trade(
    client_id: str,
    binding_id: str,
    client_db: Optional[Any],
    strategy_name: str,
    underlying: str,
) -> bool:
    """
    Return True if the binding may trade for the given strategy.

    Trap scanner: terminal_connected AND is_trade_enabled.
    Sell straddle: terminal_connected AND a running sell_straddle deployment
    for this underlying on this binding.

    Fail-open when ``client_db`` is None. Result is cached for 5 seconds.
    """
    if client_db is None:
        return True

    key = _cache_key(client_id, binding_id, client_db, strategy_name, underlying)
    now = _now()
    cached = _cache.get(key)
    if cached is not None and (now - cached[0]) < _CACHE_TTL_SECONDS:
        return cached[1]

    active = _evaluate(client_id, binding_id, client_db, strategy_name, underlying)
    _cache[key] = (now, active)
    return active
