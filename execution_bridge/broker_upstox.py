"""
execution_bridge/broker_upstox.py — Upstox API v2 broker.

Requires: pip install upstox-python-sdk

Auth flow:
  1. Generate authorization code via Upstox OAuth2 redirect URL
  2. Exchange for access_token (valid until next trading day)
  3. Store access_token in BrokerBinding.access_token before starting

Upstox option symbol format (ISIN / instrument_key):
  NSE_FO|{instrument_key}
  e.g. NSE_FO|NIFTY2562522000CE
  SymbolTranslator.to_upstox() produces the instrument_key portion.
  A full instrument master fetch is required to resolve to the API key.

For order placement Upstox requires:
  - quantity, price, trigger_price
  - instrument_token: resolved from instrument master at startup
  - transaction_type: "BUY" / "SELL"
  - order_type: "MARKET" / "LIMIT" / "SL" / "SL-M"
  - product: "I" (intraday) / "D" (delivery)
  - validity: "DAY" / "IOC"
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from config.client_profiles import BrokerBinding
from execution_bridge.base_broker import (
    BaseBroker, OrderFill, OrderRequest, OrderSide, OrderStatus,
    OrderType, PositionRecord, BROKER_REGISTRY,
)

logger = logging.getLogger(__name__)

# Upstox order-type mapping
_TYPE_MAP = {
    OrderType.MARKET: "MARKET",
    OrderType.LIMIT:  "LIMIT",
    OrderType.SL_M:   "SL-M",
    OrderType.SL_L:   "SL",
}

# Upstox order-status mapping
_STATUS_MAP = {
    "complete":           OrderStatus.COMPLETE,
    "cancelled":          OrderStatus.CANCELLED,
    "rejected":           OrderStatus.REJECTED,
    "open":               OrderStatus.OPEN,
    "open pending":       OrderStatus.OPEN,
    "trigger pending":    OrderStatus.OPEN,
    "validation pending": OrderStatus.OPEN,
    "put order req received": OrderStatus.OPEN,
    "modify validation pending": OrderStatus.OPEN,
    "modify pending":     OrderStatus.OPEN,
    "after market order req received": OrderStatus.OPEN,
    "modified":           OrderStatus.OPEN,
    "not modified":       OrderStatus.OPEN,
}


class UpstoxBroker(BaseBroker):
    """
    Upstox API v2 broker using the official upstox-python-sdk.

    Requires BrokerBinding fields:
      api_key         -- Client API key from Upstox developer portal
      api_secret      -- API secret from Upstox developer portal
      access_token    -- Daily access token (refresh before 09:00 IST)
      client_code     -- Upstox client ID / User ID

    Symbol resolution:
      Upstox uses instrument_key strings like "NSE_FO|NIFTY2562522000CE".
      Call inject_instrument_map() with a {lookup_key: instrument_key} dict
      built from the instrument master CSV downloaded at startup.
    """

    SANDBOX_HOST = "https://api-hft.upstox.com"   # high-frequency trading endpoint
    PROD_HOST    = "https://api.upstox.com"

    def __init__(self, binding: BrokerBinding, client_id: str) -> None:
        super().__init__(binding.binding_id, client_id)
        self._b = binding
        self._api_client: Any = None
        self._order_api: Any = None
        self._portfolio_api: Any = None
        self._user_api: Any = None
        self._is_amo  = False
        self._product = "I"   # I=intraday, D=delivery — overridden at auth
        # {lookup_key (InternalSymbol canonical str): instrument_key}
        self._instrument_map: Dict[str, str] = {}

    def inject_instrument_map(self, mapping: Dict[str, str]) -> None:
        """
        Inject pre-fetched {canonical_symbol_str: instrument_key} mapping.
        Build this by downloading:
          https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz
        and filtering for NFO options.
        """
        self._instrument_map.update(mapping)
        logger.debug("UpstoxBroker [%s]: Loaded %d instrument keys.", self.client_id, len(mapping))

    async def authenticate(self) -> bool:
        try:
            import upstox_client  # type: ignore[import]
        except ImportError:
            logger.error("upstox-python-sdk not installed. pip install upstox-python-sdk")
            return False

        if not self._b.access_token:
            logger.error(
                "UpstoxBroker [%s]: access_token not set. "
                "Generate via OAuth2 flow and store in binding.",
                self.client_id,
            )
            return False

        try:
            config = upstox_client.Configuration()
            config.access_token = self._b.access_token
            self._api_client = upstox_client.ApiClient(configuration=config)

            self._order_api     = upstox_client.OrderApi(self._api_client)
            self._portfolio_api = upstox_client.PortfolioApi(self._api_client)
            self._user_api      = upstox_client.UserApi(self._api_client)

            # Verify token by fetching profile
            profile = await asyncio.to_thread(
                self._user_api.get_profile, api_version="2.0"
            )
            if profile and profile.status == "success":
                self._authenticated = True
                pt   = getattr(self._b, "product_type", "").strip().upper()
                mode = getattr(self._b, "trading_mode", "intraday").lower()
                self._trading_mode_raw = "live" if mode == "live" else "paper"
                if pt in ("MIS", "INTRADAY", "I"):
                    self._product = "I"
                elif pt in ("NRML", "NORMAL", "DELIVERY", "D"):
                    self._product = "D"
                else:
                    self._product = "I" if mode not in ("carryforward", "normal", "nrml") else "D"
                logger.info("UpstoxBroker [%s]: Authenticated (user=%s) product=%s.",
                            self.client_id, profile.data.user_name if profile.data else "?",
                            self._product)
                return True

            logger.error("UpstoxBroker [%s]: Auth check failed: %s", self.client_id, profile)
            return False

        except Exception as exc:
            logger.error("UpstoxBroker [%s]: authenticate() failed: %s", self.client_id, exc)
            return False

    async def logout(self) -> None:
        self._authenticated = False
        if self._api_client:
            try:
                await asyncio.to_thread(self._api_client.close)
            except Exception:
                pass

    async def place_order(self, req: OrderRequest) -> str:
        if not self._order_api:
            raise RuntimeError("Not authenticated.")

        import upstox_client  # type: ignore[import]

        instrument_key = self._instrument_map.get(req.broker_symbol, req.broker_symbol)

        body = upstox_client.PlaceOrderRequest(
            quantity=req.qty,
            product=self._product,
            validity="DAY",
            price=req.price,
            tag=req.tag[:20] if req.tag else "",
            instrument_token=instrument_key,
            order_type=_TYPE_MAP[req.order_type],
            transaction_type=req.side.value,
            disclosed_quantity=0,
            trigger_price=req.trigger_price if req.trigger_price > 0 else 0,
            is_amo=self._is_amo,
        )

        ret = await asyncio.to_thread(
            self._order_api.place_order, body, api_version="2.0"
        )

        if ret and ret.status == "success":
            return str(ret.data.order_id if ret.data else "")
        raise RuntimeError(f"Upstox place_order failed: {ret}")

    async def cancel_order(self, order_id: str) -> bool:
        try:
            ret = await asyncio.to_thread(
                self._order_api.cancel_order, order_id, api_version="2.0"
            )
            return bool(ret and ret.status == "success")
        except Exception as exc:
            logger.error("UpstoxBroker [%s]: cancel_order failed: %s", self.client_id, exc)
            return False

    async def get_order_status(self, order_id: str) -> OrderFill:
        try:
            ret = await asyncio.to_thread(
                self._order_api.get_order_details,
                api_version="2.0",
                order_id=order_id,
            )
            if ret and ret.status == "success" and ret.data:
                o = ret.data[0] if isinstance(ret.data, list) else ret.data
                status = _STATUS_MAP.get(
                    str(getattr(o, "status", "")).lower(), OrderStatus.UNKNOWN
                )
                return OrderFill(
                    order_id=order_id,
                    broker_symbol=str(getattr(o, "trading_symbol", "")),
                    side=OrderSide.BUY if str(getattr(o, "transaction_type", "")) == "BUY"
                         else OrderSide.SELL,
                    qty=int(getattr(o, "quantity", 0) or 0),
                    avg_price=float(getattr(o, "average_price", 0) or 0),
                    status=status,
                    client_id=self.client_id,
                    raw={"order_id": order_id, "status": str(status)},
                )
        except Exception as exc:
            logger.error("UpstoxBroker [%s]: get_order_status failed: %s", self.client_id, exc)

        return OrderFill(
            order_id=order_id, broker_symbol="",
            side=OrderSide.BUY, qty=0, avg_price=0,
            status=OrderStatus.UNKNOWN,
        )

    async def get_positions(self) -> List[PositionRecord]:
        try:
            ret = await asyncio.to_thread(
                self._portfolio_api.get_positions, api_version="2.0"
            )
            result: List[PositionRecord] = []
            for p in (ret.data if ret and ret.data else []):
                result.append(PositionRecord(
                    symbol=str(getattr(p, "trading_symbol", "")),
                    qty=int(getattr(p, "quantity", 0) or 0),
                    avg_price=float(getattr(p, "average_price", 0) or 0),
                    pnl=float(getattr(p, "unrealised_profit", 0) or 0),
                    product=str(getattr(p, "product", "I")),
                ))
            return result
        except Exception as exc:
            logger.error("UpstoxBroker [%s]: get_positions failed: %s", self.client_id, exc)
            return []

    async def get_funds(self) -> Dict[str, float]:
        try:
            import upstox_client  # type: ignore[import]
            user_fund_api = upstox_client.UserApi(self._api_client)
            ret = await asyncio.to_thread(
                user_fund_api.get_user_fund_margin, api_version="2.0", segment="SEC"
            )
            if ret and ret.status == "success" and ret.data:
                equity = ret.data.equity if hasattr(ret.data, "equity") else None
                if equity:
                    return {
                        "available": float(getattr(equity, "available_margin", 0) or 0),
                        "used": float(getattr(equity, "used_margin", 0) or 0),
                    }
        except Exception as exc:
            logger.error("UpstoxBroker [%s]: get_funds failed: %s", self.client_id, exc)
        return {"available": 0.0, "used": 0.0}


# Self-register
BROKER_REGISTRY["upstox"] = lambda b, cid: UpstoxBroker(b, cid)
