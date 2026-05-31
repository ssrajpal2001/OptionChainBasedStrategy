"""
execution_bridge/broker_dhan.py — Dhan HQ broker.

Requires: pip install dhanhq
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


class DhanBroker(BaseBroker):
    """
    Dhan HQ broker via dhanhq library.

    Symbol lookup for options requires a pre-fetched instrument master CSV.
    Store the security_id in BrokerBinding.symbol_map or supply via inject_symbol_map().
    """

    def __init__(self, binding: BrokerBinding, client_id: str) -> None:
        super().__init__(binding.binding_id, client_id)
        self._b = binding
        self._dhan: Any = None
        self._is_amo  = False
        self._product = "INTRADAY"   # overridden from binding at auth time
        # symbol_map: {internal_key: security_id}
        self._symbol_map: Dict[str, str] = {}

    def inject_symbol_map(self, mapping: Dict[str, str]) -> None:
        """
        Inject pre-fetched {lookup_key: security_id} mapping.
        Call this after downloading the Dhan instrument master CSV.
        """
        self._symbol_map.update(mapping)

    async def authenticate(self) -> bool:
        try:
            from dhanhq import dhanhq  # type: ignore[import]

            if not self._b.access_token or not self._b.client_code:
                logger.error(
                    "DhanBroker [%s]: access_token and client_code are required.",
                    self.client_id,
                )
                return False

            self._dhan = dhanhq(self._b.client_code, self._b.access_token)
            # Verify by fetching fund limits
            funds = await asyncio.to_thread(self._dhan.get_fund_limits)
            if funds and funds.get("status") == "success":
                self._authenticated = True
                pt   = getattr(self._b, "product_type", "").strip().upper()
                mode = getattr(self._b, "trading_mode", "intraday").lower()
                if pt in ("MIS", "INTRADAY"):
                    self._product = "INTRADAY"
                elif pt in ("NRML", "NORMAL"):
                    self._product = "MARGIN"
                else:
                    self._product = "INTRADAY" if mode not in ("carryforward", "normal", "nrml") else "MARGIN"
                logger.info("DhanBroker [%s]: Authenticated. product=%s", self.client_id, self._product)
                return True
            logger.error("DhanBroker [%s]: Auth check failed: %s", self.client_id, funds)
            return False
        except ImportError:
            logger.error("dhanhq not installed. pip install dhanhq")
            return False

    async def logout(self) -> None:
        self._authenticated = False

    async def place_order(self, req: OrderRequest) -> str:
        if not self._dhan:
            raise RuntimeError("Not authenticated.")

        # Dhan requires security_id; broker_symbol here is the lookup key
        security_id = self._symbol_map.get(req.broker_symbol, req.broker_symbol)

        _type_map = {
            OrderType.MARKET: "MARKET",
            OrderType.LIMIT:  "LIMIT",
            OrderType.SL_M:   "STOP_LOSS_MARKET",
            OrderType.SL_L:   "STOP_LOSS",
        }
        order_params = {
            "security_id":        security_id,
            "exchange_segment":   "NSE_FNO" if req.exchange == "NFO" else "BSE_FNO",
            "transaction_type":   req.side.value,
            "quantity":           req.qty,
            "order_type":         _type_map[req.order_type],
            "product_type":       self._product,
            "price":              req.price,
            "trigger_price":      req.trigger_price if req.trigger_price > 0 else 0,
            "validity":           "DAY",
            "after_market_order": self._is_amo,
            "tag":                req.tag[:20] if req.tag else "",
        }
        ret = await asyncio.to_thread(self._dhan.place_order, **order_params)
        if ret and ret.get("status") == "success":
            return str(ret.get("data", {}).get("orderId", ""))
        raise RuntimeError(f"Dhan place_order failed: {ret}")

    async def cancel_order(self, order_id: str) -> bool:
        ret = await asyncio.to_thread(self._dhan.cancel_order, order_id)
        return bool(ret and ret.get("status") == "success")

    async def get_order_status(self, order_id: str) -> OrderFill:
        raw = await asyncio.to_thread(self._dhan.get_order_by_id, order_id)
        if raw and raw.get("status") == "success":
            o = raw.get("data", {})
            status_map = {
                "TRADED":    OrderStatus.COMPLETE,
                "CANCELLED": OrderStatus.CANCELLED,
                "REJECTED":  OrderStatus.REJECTED,
                "PENDING":   OrderStatus.OPEN,
                "TRANSIT":   OrderStatus.OPEN,
            }
            return OrderFill(
                order_id=order_id,
                broker_symbol=o.get("tradingSymbol", ""),
                side=OrderSide.BUY if o.get("transactionType") == "BUY" else OrderSide.SELL,
                qty=int(o.get("quantity", 0) or 0),
                avg_price=float(o.get("averageTradedPrice", 0) or 0),
                status=status_map.get(o.get("orderStatus", ""), OrderStatus.UNKNOWN),
                client_id=self.client_id,
                raw=o,
            )
        return OrderFill(
            order_id=order_id, broker_symbol="", side=OrderSide.BUY,
            qty=0, avg_price=0, status=OrderStatus.UNKNOWN,
        )

    async def get_positions(self) -> List[PositionRecord]:
        raw = await asyncio.to_thread(self._dhan.get_positions)
        result = []
        for p in (raw.get("data") or []):
            result.append(PositionRecord(
                symbol=p.get("tradingSymbol", ""),
                qty=int(p.get("netQty", 0) or 0),
                avg_price=float(p.get("costPrice", 0) or 0),
                pnl=float(p.get("unrealizedProfit", 0) or 0),
                product=p.get("productType", "INTRADAY"),
            ))
        return result

    async def get_funds(self) -> Dict[str, float]:
        raw = await asyncio.to_thread(self._dhan.get_fund_limits)
        data = (raw or {}).get("data", {})
        return {
            "available": float(data.get("availabelBalance", 0) or 0),
            "used": float(data.get("utilizedAmount", 0) or 0),
        }


# Self-register
BROKER_REGISTRY["dhan"] = lambda b, cid: DhanBroker(b, cid)
