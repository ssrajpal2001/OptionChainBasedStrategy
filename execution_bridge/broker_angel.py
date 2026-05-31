"""
execution_bridge/broker_angel.py — Angel One SmartAPI broker.

Requires: pip install smartapi-python pyotp
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


class AngelBroker(BaseBroker):

    def __init__(self, binding: BrokerBinding, client_id: str) -> None:
        super().__init__(binding.binding_id, client_id)
        self._b = binding
        self._smartapi: Any = None
        self._is_amo  = False
        self._product = "INTRADAY"   # overridden from binding at auth time

    async def authenticate(self) -> bool:
        try:
            from SmartApi import SmartConnect  # type: ignore[import]
        except ImportError:
            logger.error("smartapi-python not installed. pip install smartapi-python pyotp")
            return False

        try:
            self._smartapi = SmartConnect(api_key=self._b.api_key)

            # Path 1 — OAuth access_token already stored (from /callback/angelone)
            if self._b.access_token:
                # Strip "Bearer " prefix if present — SmartAPI expects the raw JWT
                raw_token = self._b.access_token
                if raw_token.startswith("Bearer "):
                    raw_token = raw_token[7:]
                try:
                    self._smartapi.setAccessToken(raw_token)
                except AttributeError:
                    self._smartapi.access_token = raw_token

            # Path 2 — headless login with client_code (or user_id) + password + TOTP
            elif (self._b.client_code or self._b.user_id) and self._b.password:
                import pyotp
                totp = pyotp.TOTP(self._b.totp_secret).now() if self._b.totp_secret else ""
                angel_client = self._b.client_code or self._b.user_id
                data = await asyncio.to_thread(
                    self._smartapi.generateSession,
                    angel_client, self._b.password, totp,
                )
                if not (data and data.get("status")):
                    logger.error("AngelBroker [%s]: Headless auth failed: %s", self.client_id, data)
                    return False

            else:
                logger.error(
                    "AngelBroker [%s]: No access_token (OAuth) and no client_code+password "
                    "(headless). Complete the AngelOne OAuth login from the client portal.",
                    self.client_id,
                )
                return False

            self._authenticated = True
            pt   = getattr(self._b, "product_type", "").strip().upper()
            mode = getattr(self._b, "trading_mode", "intraday").lower()
            if pt in ("MIS", "INTRADAY"):
                self._product = "INTRADAY"
            elif pt in ("NRML", "NORMAL"):
                self._product = "DELIVERY"
            else:
                self._product = "INTRADAY" if mode not in ("carryforward", "normal", "nrml") else "DELIVERY"
            logger.info("AngelBroker [%s]: Authenticated. product=%s", self.client_id, self._product)
            return True

        except Exception as exc:
            logger.error("AngelBroker [%s]: authenticate() error: %s", self.client_id, exc)
            return False

    async def logout(self) -> None:
        if self._smartapi:
            await asyncio.to_thread(self._smartapi.terminateSession, self._b.client_code)
        self._authenticated = False

    async def place_order(self, req: OrderRequest) -> str:
        if not self._smartapi:
            raise RuntimeError("Not authenticated.")
        _type_map = {
            OrderType.MARKET: "MARKET", OrderType.LIMIT: "LIMIT",
            OrderType.SL_M: "STOPLOSS_MARKET", OrderType.SL_L: "STOPLOSS_LIMIT",
        }
        order_data = {
            "variety": "AMO" if self._is_amo else "NORMAL",
            "tradingsymbol": req.broker_symbol,
            "symboltoken": "",      # Requires pre-lookup from instrument master
            "transactiontype": req.side.value,
            "exchange": req.exchange,
            "ordertype": _type_map[req.order_type],
            "producttype": self._product,
            "duration": "DAY",
            "price": str(req.price),
            "squareoff": "0",
            "stoploss": "0",
            "quantity": str(req.qty),
            "ordertag": req.tag[:20] if req.tag else "",
        }
        if req.trigger_price > 0:
            order_data["triggerprice"] = str(req.trigger_price)
        ret = await asyncio.to_thread(self._smartapi.placeOrder, order_data)
        if ret and ret.get("status"):
            return ret.get("data", {}).get("orderid", "")
        raise RuntimeError(f"Angel One place_order failed: {ret}")

    async def cancel_order(self, order_id: str) -> bool:
        ret = await asyncio.to_thread(
            self._smartapi.cancelOrder, order_id, "NORMAL"
        )
        return bool(ret and ret.get("status"))

    async def get_order_status(self, order_id: str) -> OrderFill:
        raw = await asyncio.to_thread(self._smartapi.orderBook)
        for o in (raw.get("data") or []):
            if str(o.get("orderid")) == str(order_id):
                status_map = {
                    "complete": OrderStatus.COMPLETE, "cancelled": OrderStatus.CANCELLED,
                    "rejected": OrderStatus.REJECTED, "open": OrderStatus.OPEN,
                }
                return OrderFill(
                    order_id=order_id,
                    broker_symbol=o.get("tradingsymbol", ""),
                    side=OrderSide.BUY if o.get("transactiontype") == "BUY" else OrderSide.SELL,
                    qty=int(o.get("quantity", 0) or 0),
                    avg_price=float(o.get("averageprice", 0) or 0),
                    status=status_map.get(o.get("status", "").lower(), OrderStatus.UNKNOWN),
                    client_id=self.client_id, raw=o,
                )
        return OrderFill(order_id=order_id, broker_symbol="", side=OrderSide.BUY,
                         qty=0, avg_price=0, status=OrderStatus.UNKNOWN)

    async def get_positions(self) -> List[PositionRecord]:
        raw = await asyncio.to_thread(self._smartapi.position)
        result = []
        for p in (raw.get("data") or []):
            result.append(PositionRecord(
                symbol=p.get("tradingsymbol", ""),
                qty=int(p.get("netqty", 0) or 0),
                avg_price=float(p.get("netprice", 0) or 0),
                pnl=float(p.get("unrealised", 0) or 0),
                product=p.get("producttype", "INTRADAY"),
            ))
        return result

    async def get_funds(self) -> Dict[str, float]:
        raw = await asyncio.to_thread(self._smartapi.rmsLimit)
        data = (raw or {}).get("data", {})
        return {
            "available": float(data.get("availablecash", 0) or 0),
            "used": float(data.get("utiliseddebits", 0) or 0),
        }


# Self-register
BROKER_REGISTRY["angelone"] = lambda b, cid: AngelBroker(b, cid)
