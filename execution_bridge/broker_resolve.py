"""
Shared "resolve a live broker instance, never fake success" helper used by
every order bridge (straddle_bridge, cascade_bridge, d1_trap_bridge,
fvg_bridge, fno_bridge).

Why this exists: every bridge independently did
    broker = router._brokers.get(client_id, {}).get(binding_id)
    if broker is None:
        await self._paper_fill(...)   # silent -- fabricates a fill even in live mode
which on 2026-08-04 caused two real, unmanaged live-order failures for a real
client (gurmeet, SellStraddle) -- the bridge told the strategy an EXIT
succeeded when the order never reached Zerodha. This module centralizes the
lookup with a short retry (covers a transient gap during a broker hot-swap,
see execution_bridge/parallel_worker_pool.py add_broker_to_worker) and, if
still unresolved, logs CRITICAL and publishes a SYSTEM_EVENT instead of
letting the caller silently proceed as if nothing happened. The caller is
responsible for NOT faking a fill when this returns None.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from config.global_config import Topic, SysEvent

logger = logging.getLogger(__name__)


async def resolve_broker_or_alert(
    bus,
    router,
    client_id: str,
    binding_id: str,
    strategy: str,
    context: str = "",
    attempts: int = 3,
    delay_sec: float = 1.0,
    client_db=None,
) -> Optional[object]:
    for attempt in range(attempts):
        broker = (router._brokers or {}).get(client_id, {}).get(binding_id)
        if broker is not None:
            return broker
        if attempt < attempts - 1:
            await asyncio.sleep(delay_sec)

    message = (
        f"{strategy}: broker unavailable for {client_id}/{binding_id} — "
        f"live order NOT sent ({context})"
    )
    logger.critical(
        "%s: broker unavailable for %s/%s after %d attempt(s) — refusing to fake a live "
        "fill (%s). No order was sent to the broker.",
        strategy, client_id, binding_id, attempts, context,
    )
    if bus is not None:
        try:
            await bus.publish(Topic.SYSTEM_EVENT, {
                "event": SysEvent.BROKER_UNAVAILABLE,
                "message": message,
                "client_id": client_id,
                "binding_id": binding_id,
                "strategy": strategy,
            })
        except Exception:
            logger.exception("%s: failed to publish BROKER_UNAVAILABLE system event", strategy)
    db = client_db or getattr(router, "_client_db", None) or getattr(router, "_db", None)
    if db is not None:
        try:
            await db.record_client_event(
                client_id=client_id, binding_id=binding_id, strategy_name=strategy,
                severity="CRITICAL", source="broker", message=message,
            )
        except Exception:
            logger.exception("%s: failed to persist client_event for broker-unavailable", strategy)
    return None
