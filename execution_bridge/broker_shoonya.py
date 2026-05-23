"""
execution_bridge/broker_shoonya.py — Shoonya / Finvasia NorenAPI broker.

Requires: pip install NorenRestApiPy
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List

from config.client_profiles import BrokerBinding
from execution_bridge.base_broker import (
    BaseBroker, OrderFill, OrderRequest, OrderSide, OrderStatus,
    OrderType, PositionRecord, BROKER_REGISTRY,
)

logger = logging.getLogger(__name__)


class ShoonyaBroker(BaseBroker):

    _BASE_URL = "https://api.shoonya.com/NorenWClientTP"
    _WS_URL   = "wss://api.shoonya.com/NorenWSTP/"

    def __init__(self, binding: BrokerBinding, client_id: str) -> None:
        super().__init__(binding.binding_id, client_id)
        self._b = binding
        self._api: Any = None

    async def authenticate(self) -> bool:
        try:
            from NorenRestApiPy.NorenApi import NorenApi  # type: ignore[import]

            _base, _ws = self._BASE_URL, self._WS_URL

            class _API(NorenApi):
                def __init__(self_inner) -> None:
                    super().__init__(host=_base, websocket=_ws)

            self._api = _API()
            ret = await asyncio.to_thread(
                self._api.login,
                userid=self._b.user_id,
                password=self._b.password,
                twoFA=self._b.totp_secret,
                vendor_code=self._b.vendor_code,
                api_secret=self._b.api_secret,
                imei=self._b.imei,
            )
            if ret and ret.get("stat") == "Ok":
                self._authenticated = True
                logger.info("ShoonyaBroker [%s]: Authenticated.", self.client_id)
                return True
            logger.error("ShoonyaBroker [%s]: Auth failed: %s", self.client_id, ret)
            return False
        except ImportError:
            logger.error("NorenRestApiPy not installed. pip install NorenRestApiPy")
            return False

    async def logout(self) -> None:
        if self._api:
            await asyncio.to_thread(self._api.logout)
        self._authenticated = False

    async def place_order(self, req: OrderRequest) -> str:
        if not self._api:
            raise RuntimeError("Not authenticated.")
        _type_map = {
            OrderType.MARKET: "MKT", OrderType.LIMIT: "LMT",
            OrderType.SL_M: "SL-MKT", OrderType.SL_L: "SL-LMT",
        }
        ret = await asyncio.to_thread(
            self._api.place_order,
            buy_or_sell="B" if req.side == OrderSide.BUY else "S",
            product_type="I",          # Intraday
            exchange=req.exchange,
            tradingsymbol=req.broker_symbol,
            quantity=req.qty,
            discloseqty=0,
            price_type=_type_map[req.order_type],
            price=req.price,
            trigger_price=req.trigger_price or None,
            retention="DAY",
            remarks=req.tag,
        )
        if ret and ret.get("stat") == "Ok":
            return ret["norenordno"]
        raise RuntimeError(f"Shoonya place_order failed: {ret}")

    async def cancel_order(self, order_id: str) -> bool:
        ret = await asyncio.to_thread(self._api.cancel_order, orderno=order_id)
        return bool(ret and ret.get("stat") == "Ok")

    async def get_order_status(self, order_id: str) -> OrderFill:
        orders = await asyncio.to_thread(self._api.get_orderbook)
        for o in (orders or []):
            if o.get("norenordno") == order_id:
                status_map = {
                    "COMPLETE": OrderStatus.COMPLETE, "OPEN": OrderStatus.OPEN,
                    "CANCELLED": OrderStatus.CANCELLED, "REJECTED": OrderStatus.REJECTED,
                }
                return OrderFill(
                    order_id=order_id,
                    broker_symbol=o.get("tsym", ""),
                    side=OrderSide.BUY if o.get("trantype") == "B" else OrderSide.SELL,
                    qty=int(o.get("qty", 0) or 0),
                    avg_price=float(o.get("avgprc", 0) or 0),
                    status=status_map.get(o.get("status", ""), OrderStatus.UNKNOWN),
                    client_id=self.client_id, raw=o,
                )
        return OrderFill(order_id=order_id, broker_symbol="", side=OrderSide.BUY,
                         qty=0, avg_price=0, status=OrderStatus.UNKNOWN)

    async def get_positions(self) -> List[PositionRecord]:
        raw = await asyncio.to_thread(self._api.get_positions)
        result = []
        for p in (raw or []):
            result.append(PositionRecord(
                symbol=p.get("tsym", ""),
                qty=int(p.get("netqty", 0) or 0),
                avg_price=float(p.get("netavgprc", 0) or 0),
                pnl=float(p.get("rpnl", 0) or 0),
                product=p.get("prd", "I"),
            ))
        return result

    async def get_funds(self) -> Dict[str, float]:
        raw = await asyncio.to_thread(self._api.get_limits)
        if raw:
            return {
                "available": float(raw.get("cash", 0) or 0),
                "used": float(raw.get("marginused", 0) or 0),
            }
        return {"available": 0.0, "used": 0.0}


# Self-register
BROKER_REGISTRY["shoonya"] = lambda b, cid: ShoonyaBroker(b, cid)
