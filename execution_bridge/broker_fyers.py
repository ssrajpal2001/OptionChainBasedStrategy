"""
execution_bridge/broker_fyers.py — Fyers API v3 broker.

Requires: pip install fyers-apiv3
Auth flow: generate access token via TOTP → store as binding.access_token
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


class FyersBroker(BaseBroker):

    def __init__(self, binding: BrokerBinding, client_id: str) -> None:
        super().__init__(binding.binding_id, client_id)
        self._b = binding
        self._fyers: Any = None
        self._is_amo  = False
        self._product = "INTRADAY"   # overridden from binding at auth time

    async def authenticate(self) -> bool:
        try:
            from fyers_apiv3 import fyersModel  # type: ignore[import]

            if not self._b.access_token:
                logger.error("FyersBroker: access_token not set for %s. "
                             "Generate via Fyers auth flow and store in binding.", self.client_id)
                return False

            self._fyers = fyersModel.FyersModel(
                client_id=self._b.api_key,
                token=self._b.access_token,
                log_path="logs/",
            )
            profile = await asyncio.to_thread(self._fyers.get_profile)
            if profile and profile.get("s") == "ok":
                self._authenticated = True
                pt   = getattr(self._b, "product_type", "").strip().upper()
                mode = getattr(self._b, "trading_mode", "intraday").lower()
                if pt in ("MIS", "INTRADAY"):
                    self._product = "INTRADAY"
                elif pt in ("NRML", "NORMAL"):
                    self._product = "MARGIN"
                else:
                    self._product = "INTRADAY" if mode not in ("carryforward", "normal", "nrml") else "MARGIN"
                logger.info("FyersBroker [%s]: Authenticated. product=%s", self.client_id, self._product)
                return True
            logger.error("FyersBroker [%s]: Auth check failed: %s", self.client_id, profile)
            return False
        except ImportError:
            logger.error("fyers-apiv3 not installed. pip install fyers-apiv3")
            return False

    async def logout(self) -> None:
        self._authenticated = False

    async def place_order(self, req: OrderRequest) -> str:
        if not self._fyers:
            raise RuntimeError("Not authenticated.")
        _type_map = {
            OrderType.MARKET: 2, OrderType.LIMIT: 1,
            OrderType.SL_M: 4, OrderType.SL_L: 3,
        }
        data = {
            "symbol":       req.broker_symbol,
            "qty":          req.qty,
            "type":         _type_map[req.order_type],
            "side":         1 if req.side == OrderSide.BUY else -1,
            "productType":  self._product,
            "limitPrice":   req.price,
            "stopPrice":    req.trigger_price,
            "validity":     "DAY",
            "disclosedQty": 0,
            "offlineOrder": self._is_amo,   # True = AMO
            "stopLoss":     0,
            "takeProfit":   0,
            "orderTag":     req.tag[:20] if req.tag else "",
        }
        ret = await asyncio.to_thread(self._fyers.place_order, data=data)
        if ret and ret.get("s") == "ok":
            return ret.get("id", "")
        raise RuntimeError(f"Fyers place_order failed: {ret}")

    async def cancel_order(self, order_id: str) -> bool:
        ret = await asyncio.to_thread(self._fyers.cancel_order, data={"id": order_id})
        return bool(ret and ret.get("s") == "ok")

    async def get_order_status(self, order_id: str) -> OrderFill:
        raw = await asyncio.to_thread(self._fyers.orderbook)
        for o in (raw.get("orderBook") or []):
            if str(o.get("id")) == str(order_id):
                status_map = {2: OrderStatus.COMPLETE, 6: OrderStatus.CANCELLED, 5: OrderStatus.REJECTED}
                return OrderFill(
                    order_id=order_id,
                    broker_symbol=o.get("symbol", ""),
                    side=OrderSide.BUY if o.get("side") == 1 else OrderSide.SELL,
                    qty=int(o.get("qty", 0) or 0),
                    avg_price=float(o.get("tradedPrice", 0) or 0),
                    status=status_map.get(o.get("status", 0), OrderStatus.UNKNOWN),
                    client_id=self.client_id, raw=o,
                )
        return OrderFill(order_id=order_id, broker_symbol="", side=OrderSide.BUY,
                         qty=0, avg_price=0, status=OrderStatus.UNKNOWN)

    async def get_positions(self) -> List[PositionRecord]:
        raw = await asyncio.to_thread(self._fyers.positions)
        result = []
        for p in (raw.get("netPositions") or []):
            result.append(PositionRecord(
                symbol=p.get("symbol", ""),
                qty=int(p.get("netQty", 0) or 0),
                avg_price=float(p.get("netAvg", 0) or 0),
                pnl=float(p.get("pl", 0) or 0),
                product=p.get("productType", "INTRADAY"),
            ))
        return result

    async def get_funds(self) -> Dict[str, float]:
        raw = await asyncio.to_thread(self._fyers.funds)
        fund_list = raw.get("fund_limit", [])
        available = next((f["equityAmount"] for f in fund_list if f.get("title") == "Available Balance"), 0.0)
        return {"available": float(available), "used": 0.0}


# Self-register
BROKER_REGISTRY["fyers"] = lambda b, cid: FyersBroker(b, cid)
