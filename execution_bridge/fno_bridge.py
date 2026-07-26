"""
execution_bridge/fno_bridge.py — FnO Positional order router.

Subscribes to Topic.FNO_ORDER_REQUEST for FnOOrderEvent objects published by
FnOPositionalBook.  Routes to the exact (client_id, binding_id) broker — no
broadcast, same as V4CascadeExecutionBridge.

Paper mode: places the real Zerodha order (verifies routing / source-IP whitelist)
then immediately books a local sim-fill at price_hint.

Live mode: places the order and polls for fill via broker.get_order_status().

Publishes FnOFillEvent to Topic.FNO_ORDER_FILL so the book can confirm the
position entry/exit price.

Exchange: NSE FO stock options → exchange="NFO".
Product: NRML (positional/carry-forward, not intraday MIS).
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus
from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType

logger = logging.getLogger(__name__)


# ── Events ────────────────────────────────────────────────────────────────────

@dataclass
class FnOOrderEvent:
    """Published by FnOPositionalBook to Topic.FNO_ORDER_REQUEST."""
    action:        str           # "ENTRY" | "EXIT"
    symbol:        str
    direction:     str           # "CE" | "PE"
    strike:        int
    expiry_str:    str           # "28 AUG 26"
    broker_symbol: str           # Zerodha NFO symbol e.g. "RELIANCE26AUG1280CE"
    qty:           int
    price_hint:    float         # paper fill price / limit price fallback
    client_id:     str
    binding_id:    str
    event_id:      str
    mode:          str = "paper" # "paper" | "live"


@dataclass
class FnOFillEvent:
    """Published by FnOExecutionBridge to Topic.FNO_ORDER_FILL on completion."""
    event_id:     str
    action:       str
    symbol:       str
    fill_price:   float
    qty:          int
    client_id:    str
    binding_id:   str
    order_id:     str    = ""
    order_failed: bool   = False
    paper_mode:   bool   = True
    timestamp:    datetime = field(default_factory=lambda: datetime.now(IST))


# ── Trade logger ──────────────────────────────────────────────────────────────

class _FnOTradeLogger:
    def __init__(self, log_dir: str = "logs/trades") -> None:
        os.makedirs(log_dir, exist_ok=True)
        self._log_dir = log_dir
        self._handles: dict = {}

    def _handle(self, client_id: str, binding_id: str):
        today = datetime.now(IST).strftime("%Y%m%d")
        key   = f"{client_id}-{binding_id}-fno_positional-{today}"
        if key not in self._handles:
            path = os.path.join(self._log_dir, f"{key}.log")
            self._handles[key] = open(path, "a", encoding="utf-8", buffering=1)
        return self._handles[key]

    def log(self, cid: str, bid: str, msg: str) -> None:
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self._handle(cid, bid).write(f"{ts}  FNO_POS  {msg}\n")

    def close_all(self) -> None:
        for h in self._handles.values():
            try: h.close()
            except Exception: pass
        self._handles.clear()


# ── Bridge ────────────────────────────────────────────────────────────────────

class FnOExecutionBridge:
    """Routes FnOOrderEvent to the owning (client_id, binding_id) broker."""

    def __init__(self, bus: EventBus, router, log_dir: str = "logs/trades") -> None:
        self._bus       = bus
        self._router    = router
        self._tlog      = _FnOTradeLogger(log_dir)
        self._running   = False
        self._q         = bus.subscribe(Topic.FNO_ORDER_REQUEST)

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                ev: FnOOrderEvent = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            asyncio.create_task(self._handle(ev))

    async def stop(self) -> None:
        self._running = False

    async def _handle(self, ev: FnOOrderEvent) -> None:
        broker = (self._router._brokers.get(ev.client_id, {}) or {}).get(ev.binding_id)
        if not broker:
            logger.error("FnOBridge: no broker for %s/%s — cannot route order",
                         ev.client_id, ev.binding_id)
            self._bus.publish(Topic.FNO_ORDER_FILL, FnOFillEvent(
                event_id=ev.event_id, action=ev.action, symbol=ev.symbol,
                fill_price=0.0, qty=ev.qty,
                client_id=ev.client_id, binding_id=ev.binding_id,
                order_failed=True,
            ))
            return

        req = OrderRequest(
            broker_symbol=ev.broker_symbol,
            exchange="NFO",
            side=OrderSide.BUY if ev.action == "ENTRY" else OrderSide.SELL,
            qty=ev.qty,
            order_type=OrderType.MARKET,
            price=ev.price_hint,
            product="NRML",   # positional carry-forward
            tag=f"fno_{ev.symbol[:8]}",
            client_id=ev.client_id,
        )

        is_paper = (ev.mode == "paper")
        fill_price = ev.price_hint
        order_id   = ""
        failed     = False

        try:
            if is_paper:
                # Paper: route for verification; local sim-fill at price_hint
                order_id = await broker.place_order(req)
                logger.info("FnOBridge[%s/%s]: PAPER order_id=%s %s %s x %d @ %.2f",
                            ev.client_id, ev.binding_id, order_id,
                            ev.action, ev.broker_symbol, ev.qty, fill_price)
            else:
                # Live: real order + poll for fill
                order_id = await broker.place_order(req)
                fill_obj = await broker.get_order_status(order_id)
                if fill_obj and getattr(fill_obj, "avg_price", 0) > 0:
                    fill_price = fill_obj.avg_price
                logger.info("FnOBridge[%s/%s]: LIVE order_id=%s %s %s x %d fill=%.2f",
                            ev.client_id, ev.binding_id, order_id,
                            ev.action, ev.broker_symbol, ev.qty, fill_price)
        except Exception as exc:
            logger.error("FnOBridge[%s/%s]: order FAILED %s %s: %s",
                         ev.client_id, ev.binding_id, ev.action, ev.broker_symbol, exc)
            failed = True

        self._tlog.log(
            ev.client_id, ev.binding_id,
            f"{ev.action:5s}  {ev.broker_symbol}  qty={ev.qty}  "
            f"fill={fill_price:.2f}  id={order_id}  paper={is_paper}  failed={failed}",
        )

        self._bus.publish(Topic.FNO_ORDER_FILL, FnOFillEvent(
            event_id=ev.event_id, action=ev.action, symbol=ev.symbol,
            fill_price=fill_price, qty=ev.qty,
            client_id=ev.client_id, binding_id=ev.binding_id,
            order_id=order_id, order_failed=failed, paper_mode=is_paper,
        ))
