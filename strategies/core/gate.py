"""
strategies/core/gate.py — reusable per-binding trade gate.

Mirrors the gating logic from strategies/sell_straddle.py::_any_active_terminal
(per-binding path).

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
    """Uncached gate evaluation.

    Generalized (2026-07-19) — no more per-strategy-name hardcoding. EVERY
    strategy (sell_straddle, oi_orb_screener, and any future one) is gated
    the same way: terminal_connected AND is_trade_enabled AND a running
    deployment of THIS strategy_name for THIS underlying on THIS binding.
    Previously only sell_straddle got the full check and every other
    strategy silently fell through to a terminal-only check — the exact
    class of routing bug documented in project memory (the BTC --mode paper
    routing bug), now closed for good by removing the special case.

    2026-08-05: briefly added an `engine_active` check alongside
    `is_trade_enabled`, then REVERTED it the same day after code review
    found it was a critical regression. `engine_active` is set to True in
    exactly two dashboard endpoints (`/api/client/set_trade/{id}` and
    `/api/client/broker/{id}/engine-start`), but NEITHER is wired to a
    reachable UI control any more — `git show da5161c` (2026-06-11) removed
    the per-broker Trade toggle that used to drive it and replaced the live
    control surface with per-STRATEGY Run toggles
    (`POST /api/client/deployment/{id}/run`, which only ever touches
    `strategy_deployments.is_running`). Confirmed against a real production
    DB snapshot (`data/clients.db.bak_20260701_085629`): `engine_active=0`
    on every binding, including ones actively trading with
    `is_trade_enabled=1`. Requiring `engine_active` here would have
    silently blocked every ENTRY for every strategy the moment this
    shipped. CLAUDE.md's "Gated on Terminal ON + Trade ON" describes the
    pre-da5161c UI and is stale on this point. The live bridges
    (straddle_bridge.py etc.) do reference `engine_active` in one legacy/
    effectively-dead broadcast code path, not in the per-binding path every
    real book actually uses — so "the live bridges have always gated on
    engine_active" is NOT a safe generalization; do not re-add this check
    without first confirming a currently-reachable write path sets
    `engine_active=True` for real trading bindings."""
    try:
        bindings = {b.get("binding_id"): b for b in client_db.get_bindings_safe_sync(client_id)}
        binding = bindings.get(binding_id)
        if not binding:
            return False
        if not binding.get("terminal_connected"):
            return False
        if not binding.get("is_trade_enabled"):
            return False

        strategy = strategy_name.lower()
        try:
            deployments = client_db.get_deployments_sync(client_id)
        except Exception:
            deployments = []
        return any(
            d.get("binding_id") == binding_id
            and str(d.get("strategy_name", "")).lower() == strategy
            and str(d.get("underlying", "") or d.get("assigned_instrument", "")).upper()
            == (underlying or "").upper()
            and int(d.get("is_running", 0) or 0) == 1
            for d in deployments
        )
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

    Requires terminal_connected AND is_trade_enabled AND a running
    deployment of that strategy for this underlying on this binding.

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
