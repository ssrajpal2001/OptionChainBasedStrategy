"""
ui_layer/ws_bridge.py — EventBus-to-WebSocket transmission bridge.

Subscribes to INDEX_TICK, MATRIX_SNAPSHOT, ORDER_FILL, SYSTEM_EVENT on the
production EventBus and broadcasts serialised JSON frames to every connected
browser WebSocket in real time.

A 2-second periodic heartbeat pushes worker stats and client summaries via
registered provider callbacks so the dashboard stays current even between
market-event bursts.

Guaranteed properties:
  • No time.sleep — all yielding via asyncio.wait_for / asyncio.sleep
  • No direct calls into execution workers or market data feeds
  • Dead WebSocket connections are silently pruned on each broadcast
  • Zero overhead when no browsers are connected (broadcast() short-circuits)

No time.sleep.  All async.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Set

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus, IndexTick, OptionTick

logger = logging.getLogger(__name__)

_HEARTBEAT_INTERVAL = 2.0   # seconds
_AUTH_PUSH_TYPES = {"terminal_connected", "feeder_token_updated"}


def _round_to_step(price: float, step: float) -> float:
    return round(round(price / step) * step, 2)


class WsBridge:
    """
    Sits between the EventBus and every connected browser WebSocket client.

    Lifecycle:
      1. Instantiate with an EventBus reference.
      2. Register stats providers for the periodic heartbeat.
      3. Call await run() (blocks until stopped).
      4. FastAPI WebSocket endpoint calls add_connection() on accept and
         remove_connection() on disconnect.
    """

    def __init__(
        self,
        bus: EventBus,
        cfg=None,        # GlobalConfig — for ATM strike-step computation
        rebalancer=None, # StrikeRebalancer — optional; not used directly here
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._connections: Set[Any] = set()
        self._running = False
        self._stats_providers: Dict[str, Callable[[], Any]] = {}

        # Subscribe once — queues are drained by independent sub-loops
        self._tick_q   = bus.subscribe(Topic.INDEX_TICK)
        self._snap_q   = bus.subscribe(Topic.MATRIX_SNAPSHOT)
        self._fill_q   = bus.subscribe(Topic.ORDER_FILL)
        self._sys_q    = bus.subscribe(Topic.SYSTEM_EVENT)
        self._option_q = bus.subscribe(Topic.OPTION_TICK)
        self._audit_q  = bus.subscribe(Topic.EXIT_AUDIT)

        # Per-underlying spot cache (updated by _tick_loop) — used to flag ATM strikes
        self._spot_cache: Dict[str, float] = {}
        # Option chain cache: key = "{underlying}_{strike}", value = row dict for IV matrix
        self._option_cache: Dict[str, dict] = {}

    # ── Connection management ─────────────────────────────────────────────────

    def add_connection(self, ws: Any) -> None:
        self._connections.add(ws)
        logger.debug("WsBridge: client connected (%d total).", len(self._connections))

    def remove_connection(self, ws: Any) -> None:
        self._connections.discard(ws)
        logger.debug("WsBridge: client disconnected (%d total).", len(self._connections))

    def register_stats_provider(self, name: str, fn: Callable[[], Any]) -> None:
        """
        Register a zero-arg callable that returns JSON-serialisable data.
        Called every HEARTBEAT_INTERVAL seconds; result is broadcast as:
          {"type": "stats", "name": name, "data": <result>}
        """
        self._stats_providers[name] = fn

    # ── Broadcast ────────────────────────────────────────────────────────────

    async def broadcast(self, payload: dict) -> None:
        """Send JSON payload to all connected browsers; prune dead connections."""
        if not self._connections:
            return
        text = json.dumps(payload, default=str)
        dead: Set[Any] = set()
        for ws in list(self._connections):
            try:
                await ws.send_text(text)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self.remove_connection(ws)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("WsBridge: started.")
        try:
            await asyncio.gather(
                self._tick_loop(),
                self._snapshot_loop(),
                self._fill_loop(),
                self._sys_loop(),
                self._heartbeat_loop(),
                self._option_loop(),
                self._exit_audit_loop(),
            )
        except asyncio.CancelledError:
            pass

    def stop(self) -> None:
        self._running = False

    # ── Event loops ───────────────────────────────────────────────────────────

    async def _tick_loop(self) -> None:
        while self._running:
            try:
                tick: IndexTick = await asyncio.wait_for(self._tick_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                self._spot_cache[tick.symbol] = tick.ltp
                atm = self._compute_atm(tick.symbol, tick.ltp)
                await self.broadcast({
                    "type": "tick",
                    "sym":  tick.symbol,
                    "ltp":  round(tick.ltp, 2),
                    "atm":  atm,
                    "ts":   datetime.now(IST).strftime("%H:%M:%S IST"),
                })
            except Exception as exc:
                logger.debug("WsBridge._tick_loop: %s", exc)

    async def _snapshot_loop(self) -> None:
        while self._running:
            try:
                snap = await asyncio.wait_for(self._snap_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self.broadcast({
                    "type":     "snapshot",
                    "sym":      getattr(snap, "symbol", ""),
                    "tf":       getattr(snap, "timeframe", 0),
                    "rsi":      round(float(getattr(snap, "rsi",      0) or 0), 2),
                    "vwap":     round(float(getattr(snap, "vwap_val", 0) or 0), 2),
                    "adx":      round(float(getattr(snap, "adx_val",  0) or 0), 2),
                    "plus_di":  round(float(getattr(snap, "plus_di",  0) or 0), 2),
                    "minus_di": round(float(getattr(snap, "minus_di", 0) or 0), 2),
                    "ema_fast": round(float(getattr(snap, "ema_fast", 0) or 0), 2),
                    "ema_slow": round(float(getattr(snap, "ema_slow", 0) or 0), 2),
                    "ltp":      round(float(getattr(snap, "ltp",      0) or 0), 2),
                    "ts":       datetime.now(IST).strftime("%H:%M:%S IST"),
                })
            except Exception as exc:
                logger.debug("WsBridge._snapshot_loop: %s", exc)

    async def _fill_loop(self) -> None:
        while self._running:
            try:
                fill = await asyncio.wait_for(self._fill_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self.broadcast({
                    "type":      "fill",
                    "client_id": getattr(fill, "client_id", ""),
                    "sym":       getattr(fill, "broker_symbol", ""),
                    "side":      str(getattr(fill, "side", "")),
                    "qty":       int(getattr(fill, "qty", 0) or 0),
                    "avg_price": round(float(getattr(fill, "avg_price", 0) or 0), 2),
                    "ts":        datetime.now(IST).strftime("%H:%M:%S IST"),
                })
            except Exception as exc:
                logger.debug("WsBridge._fill_loop: %s", exc)

    async def _sys_loop(self) -> None:
        while self._running:
            try:
                evt = await asyncio.wait_for(self._sys_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                # Auth events forwarded verbatim so frontend auto-flips toggles instantly
                if isinstance(evt, dict) and evt.get("type") in _AUTH_PUSH_TYPES:
                    await self.broadcast({
                        **evt,
                        "ts": datetime.now(IST).strftime("%H:%M:%S IST"),
                    })
                else:
                    code = (
                        getattr(evt, "code", None)
                        or (evt.get("event") if isinstance(evt, dict) else None)
                        or ""
                    )
                    msg = (
                        getattr(evt, "message", "")
                        or (evt.get("message", "") if isinstance(evt, dict) else "")
                    )
                    await self.broadcast({
                        "type": "sys",
                        "code": str(code),
                        "msg":  str(msg),
                        "ts":   datetime.now(IST).strftime("%H:%M:%S IST"),
                    })
            except Exception as exc:
                logger.debug("WsBridge._sys_loop: %s", exc)

    async def _option_loop(self) -> None:
        """Cache incoming OptionTick events for the IV matrix endpoint."""
        while self._running:
            try:
                tick: OptionTick = await asyncio.wait_for(self._option_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                key = f"{tick.underlying}_{int(tick.strike)}"
                row = self._option_cache.get(key) or {
                    "strike":   tick.strike,
                    "is_atm":   False,
                    "call_oi":  0, "call_iv":  0.0, "call_bid": 0.0, "call_ask": 0.0,
                    "put_oi":   0, "put_iv":   0.0, "put_bid":  0.0, "put_ask":  0.0,
                    "spread":   0.0,
                }
                # iv stored as decimal (0.18 = 18%) — convert to percentage for frontend
                iv_pct = round(tick.iv * 100, 2) if tick.iv < 2.0 else round(tick.iv, 2)
                if tick.option_type.upper() == "CE":
                    row["call_oi"]  = int(tick.oi)
                    row["call_iv"]  = iv_pct
                    row["call_bid"] = round(float(tick.bid), 2)
                    row["call_ask"] = round(float(tick.ask), 2)
                else:
                    row["put_oi"]   = int(tick.oi)
                    row["put_iv"]   = iv_pct
                    row["put_bid"]  = round(float(tick.bid), 2)
                    row["put_ask"]  = round(float(tick.ask), 2)
                # Bid-ask spread: average of call and put half-spreads
                c_spread = row["call_ask"] - row["call_bid"]
                p_spread = row["put_ask"]  - row["put_bid"]
                row["spread"] = round((c_spread + p_spread) / 2, 2) if (c_spread + p_spread) > 0 else 0.0
                # Mark ATM based on current spot
                spot = self._spot_cache.get(tick.underlying, 0.0)
                if spot > 0.0:
                    step = (
                        self._cfg.exchange.strike_steps.get(tick.underlying, 50.0)
                        if self._cfg else 50.0
                    )
                    row["is_atm"] = (int(tick.strike) == int(_round_to_step(spot, step)))
                self._option_cache[key] = row
            except Exception as exc:
                logger.debug("WsBridge._option_loop: %s", exc)

    async def _exit_audit_loop(self) -> None:
        """Forward per-tick exit-criteria audit payloads (granular UI) verbatim. The
        strategy only publishes these when an admin has enabled show_granular_ticks for a
        client, so this loop is idle in the common case. The frontend filters by client_id."""
        while self._running:
            try:
                ev = await asyncio.wait_for(self._audit_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                if isinstance(ev, dict):
                    await self.broadcast(ev)
            except Exception as exc:
                logger.debug("WsBridge._exit_audit_loop: %s", exc)

    async def _heartbeat_loop(self) -> None:
        """Broadcast worker stats and client summaries every HEARTBEAT_INTERVAL seconds."""
        while self._running:
            try:
                await asyncio.sleep(_HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                return
            for name, fn in list(self._stats_providers.items()):
                try:
                    data = fn()
                    await self.broadcast({"type": "stats", "name": name, "data": data})
                except Exception as exc:
                    logger.debug("WsBridge.heartbeat[%s]: %s", name, exc)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _compute_atm(self, symbol: str, ltp: float) -> float:
        if self._cfg is not None:
            step = self._cfg.exchange.strike_steps.get(symbol, 50.0)
            return _round_to_step(ltp, step)
        return _round_to_step(ltp, 50.0)
