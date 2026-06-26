"""
execution_bridge/broker_zerodha.py — Zerodha Kite Connect broker.

Requires: pip install kiteconnect

ORDER TYPE BEHAVIOUR (Zerodha F&O):
  Zerodha does NOT support pure MARKET orders for options in the same way
  as cash equity. The OMS requires either:
    a) ORDER_TYPE_MARKET with market_protection % (newer kiteconnect builds)
    b) ORDER_TYPE_LIMIT at LTP ± 1 tick  ← "market-limit" fallback

  This broker detects which path is available at runtime via inspect:
    - kiteconnect >= 4.x  → path (a): MARKET + market_protection
    - kiteconnect < 4.x   → path (b): LIMIT at LTP ± 1 (fills immediately)

PRODUCT TYPE:
  Each client's broker binding carries a `trading_mode` field:
    "intraday" → PRODUCT_MIS  (auto square-off at 3:20 PM)
    "carryforward" or "normal" → PRODUCT_NRML  (carry forward)
  Default is MIS (intraday) since sell_straddle is an intraday strategy.

Symbol format (NSE weekly options):
  Weekly: NIFTY{YY}{M_single}{DD}{STRIKE}{CE|PE}   e.g. NIFTY2561923300CE
  Monthly: NIFTY{YY}{MON}{STRIKE}{CE|PE}            e.g. NIFTY25JAN23300CE
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from config.client_profiles import BrokerBinding
from config.global_config import IST
from execution_bridge.base_broker import (
    BaseBroker, OrderFill, OrderRequest, OrderSide, OrderStatus,
    OrderType, PositionRecord, BROKER_REGISTRY,
)

logger = logging.getLogger(__name__)

# Zerodha exchange constants (matched to kiteconnect strings)
_EXCHANGE_NFO = "NFO"
_EXCHANGE_BFO = "BFO"
_EXCHANGE_MCX = "MCX"


def _bind_session_source_ip(kite, source_ip: str) -> None:
    """Bind a KiteConnect instance's HTTP session so all API calls egress from `source_ip`
    (a LOCAL/private interface address). Used for multi-client Zerodha static-IP whitelisting:
    each client's orders leave from the EIP mapped to their bound private IP."""
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.poolmanager import PoolManager

    class _SourceIPAdapter(HTTPAdapter):
        def init_poolmanager(self, connections, maxsize, block=False, **kw):
            self.poolmanager = PoolManager(
                num_pools=connections, maxsize=maxsize, block=block,
                source_address=(source_ip, 0), **kw)

    adapter = _SourceIPAdapter()
    # pykiteconnect keeps its requests.Session on `reqsession`; fall back to creating one.
    sess = getattr(kite, "reqsession", None)
    if sess is None or not isinstance(sess, requests.Session):
        sess = requests.Session()
        try:
            kite.reqsession = sess
        except Exception:
            pass
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)


def _resolve_exchange(req_exchange: str) -> str:
    """Map an OrderRequest exchange string to a Zerodha exchange constant."""
    e = (req_exchange or "").upper()
    if e == "BFO":
        return _EXCHANGE_BFO
    if e == "MCX":
        return _EXCHANGE_MCX
    return _EXCHANGE_NFO

# Month abbreviations for monthly expiry symbol construction
_MONTH_ABBR = {1:"JAN",2:"FEB",3:"MAR",4:"APR",5:"MAY",6:"JUN",
               7:"JUL",8:"AUG",9:"SEP",10:"OCT",11:"NOV",12:"DEC"}

# Single-digit month codes used in Zerodha's weekly option symbol format
_MONTH_CODE = {1:"1",2:"2",3:"3",4:"4",5:"5",6:"6",
               7:"7",8:"8",9:"9",10:"O",11:"N",12:"D"}

_STATUS_MAP = {
    "complete":              OrderStatus.COMPLETE,
    "cancelled":             OrderStatus.CANCELLED,
    "rejected":              OrderStatus.REJECTED,
    "open":                  OrderStatus.OPEN,
    "trigger pending":       OrderStatus.OPEN,
    "validation pending":    OrderStatus.OPEN,
    "put order req received":OrderStatus.OPEN,
    "modify pending":        OrderStatus.OPEN,
    "after market order req received": OrderStatus.OPEN,
}


class ZerodhaBroker(BaseBroker):
    """
    Zerodha Kite Connect broker adapter.

    Reads access_token from BrokerBinding.access_token (populated by
    HeadlessAuthEngine or OAuth callback before start).
    """

    def __init__(self, binding: BrokerBinding, client_id: str) -> None:
        super().__init__(binding.binding_id, client_id)
        self._binding   = binding
        self._kite: Any = None          # KiteConnect instance
        self._api_key   = ""
        self._token     = ""
        self._product   = "MIS"         # default intraday; overridden per binding
        self._is_amo    = False         # set True for after-market orders

    # ── Auth ─────────────────────────────────────────────────────────────────

    async def authenticate(self) -> bool:
        try:
            from kiteconnect import KiteConnect  # type: ignore
        except ImportError:
            logger.error("ZerodhaBroker: kiteconnect not installed — pip install kiteconnect")
            return False

        creds = self._binding
        self._api_key = creds.api_key or ""
        self._token   = creds.access_token or ""

        if not self._api_key or not self._token:
            logger.error("ZerodhaBroker[%s]: api_key or access_token missing.", self.client_id)
            return False

        self._kite = KiteConnect(api_key=self._api_key)
        self._kite.set_access_token(self._token)

        # Per-binding source-IP binding: Zerodha Kite whitelists a GLOBALLY-UNIQUE static IP
        # per app, so multiple clients on one server must each egress from their own IP. When
        # source_ip is set (the LOCAL/private IP whose EIP is whitelisted for this app), bind
        # KiteConnect's HTTP session to it so orders leave from the right public IP.
        _src = (getattr(creds, "source_ip", "") or "").strip()
        self.egress_ip = _src
        self.egress_bound = False
        if _src:
            try:
                _bind_session_source_ip(self._kite, _src)
                self.egress_bound = True
                logger.info("ZerodhaBroker[%s]: bound API egress to source IP %s",
                            self.client_id, _src)
            except Exception as exc:
                logger.error("ZerodhaBroker[%s]: source-IP bind to %s FAILED: %s "
                             "(orders will use the default IP).", self.client_id, _src, exc)

        # Determine product type:
        # 1. Explicit product_type field on binding ("MIS" or "NRML") — highest priority
        # 2. Infer from trading_mode ("carryforward"/"normal" → NRML, else MIS)
        mode = getattr(creds, "trading_mode", "intraday").lower()
        self._trading_mode_raw = "live" if mode == "live" else "paper"
        pt   = getattr(creds, "product_type", "").strip().upper()
        # Did the CLIENT explicitly choose a product on this binding (the deployment screen)?
        # If so it is AUTHORITATIVE — it wins over any strategy-section default (req.product).
        self._product_explicit = pt in ("MIS", "NRML", "INTRADAY", "NORMAL", "DELIVERY")
        if pt in ("MIS", "NRML"):
            self._product = pt
        elif pt == "INTRADAY":
            self._product = "MIS"
        elif pt in ("NORMAL", "DELIVERY"):
            self._product = "NRML"
        else:
            self._product = "NRML" if mode in ("carryforward", "normal", "nrml") else "MIS"
        logger.info(
            "ZerodhaBroker[%s]: authenticated — product=%s mode=%s",
            self.client_id, self._product, mode,
        )
        self._authenticated = True
        return True

    async def logout(self) -> None:
        self._authenticated = False
        self._kite = None

    # ── Order placement ───────────────────────────────────────────────────────

    async def place_order(self, req: OrderRequest) -> str:
        if not self._kite:
            raise RuntimeError("ZerodhaBroker: not authenticated")

        kite = self._kite
        exchange = _resolve_exchange(req.exchange)
        transaction = (kite.TRANSACTION_TYPE_BUY
                       if req.side == OrderSide.BUY
                       else kite.TRANSACTION_TYPE_SELL)
        # Product precedence (per client requirement — deployment-driven):
        #   1. MCX exchange → always MIS (NRML/carry invalid for MCX intraday; fails on expiry day).
        #   2. The CLIENT's explicit choice on THIS binding (deployment screen) — AUTHORITATIVE.
        #   3. Else the ORDER's product (per-strategy default from the bridge), if valid MIS/NRML.
        #   4. Else the binding's inferred default.
        # So the client picks MIS/NRML/carry per deployment, and that drives the order.
        _req_prod = (getattr(req, "product", "") or "").strip().upper()
        if req.exchange.upper() == "MCX":
            _prod_str = "MIS"  # MCX intraday always — NRML rejected on expiry day
        elif getattr(self, "_product_explicit", False):
            _prod_str = self._product
        elif _req_prod in ("MIS", "NRML"):
            _prod_str = _req_prod
        else:
            _prod_str = self._product
        product = kite.PRODUCT_NRML if _prod_str == "NRML" else kite.PRODUCT_MIS

        order_type, price = self._resolve_order_type(req, kite)

        variety = kite.VARIETY_AMO if self._is_amo else kite.VARIETY_REGULAR
        params: Dict[str, Any] = {
            "variety":          variety,
            "exchange":         exchange,
            "tradingsymbol":    req.broker_symbol,
            "transaction_type": transaction,
            "quantity":         req.qty,
            "product":          product,
            "order_type":       order_type,
            "price":            0.0 if price == -1.0 else price,
            "validity":         "DAY",
            "tag":              req.tag[:20] if req.tag else "",
        }
        # Zerodha requires market_protection % for MARKET orders on F&O
        if price == -1.0:
            params["market_protection"] = 1   # 1% protection — accepted by NSE OMS

        logger.info(
            "ZerodhaBroker[%s]: placing %s %s x %s | type=%s price=%.2f product=%s",
            self.client_id, req.side.value, req.qty, req.broker_symbol,
            order_type, price, _prod_str,
        )

        order_id = await asyncio.to_thread(kite.place_order, **params)
        logger.info("ZerodhaBroker[%s]: order_id=%s", self.client_id, order_id)
        return str(order_id)

    def _resolve_order_type(self, req: OrderRequest, kite: Any):
        """
        Zerodha market order resolution:

        1. If kiteconnect library supports market_protection parameter
           → use MARKET order (clean, exchange-native)
        2. Otherwise ("market-limit" fallback)
           → use LIMIT order at LTP ± 1 tick (fills immediately like a market order)
           → BUY:  LTP + 1.0 (willing to pay slightly above)
           → SELL: LTP - 1.0 (willing to accept slightly below)
           Price is rounded to nearest 0.05 (NSE F&O tick size)

        This fallback is needed because older kiteconnect builds do not
        support market_protection and some Zerodha OMS versions reject
        plain MARKET orders on illiquid option strikes.
        """
        if req.order_type == OrderType.MARKET:
            # Check if the installed kiteconnect supports market_protection
            try:
                sig = inspect.signature(kite.place_order)
                supports_mprot = "market_protection" in sig.parameters
            except Exception:
                supports_mprot = False

            if supports_mprot:
                # Zerodha does not support pure MARKET for F&O — use market_protection %
                # We inject this into params after returning; signal via special sentinel price=-1
                return kite.ORDER_TYPE_MARKET, -1.0   # sentinel: caller adds market_protection
            else:
                # Legacy kiteconnect fallback — LIMIT at LTP ± 1 tick (fills immediately)
                ltp = self._get_ltp_safe(req.broker_symbol, req.exchange)
                if ltp <= 0:
                    ltp = 100.0
                if req.side == OrderSide.BUY:
                    price = round((ltp + 1.0) / 0.05) * 0.05
                else:
                    price = round(max(ltp - 1.0, 0.05) / 0.05) * 0.05
                logger.info(
                    "ZerodhaBroker[%s]: market-limit fallback — LTP=%.2f → LIMIT %.2f (%s)",
                    self.client_id, ltp, price, req.side.value,
                )
                return kite.ORDER_TYPE_LIMIT, price

        if req.order_type == OrderType.LIMIT:
            return kite.ORDER_TYPE_LIMIT, req.price

        if req.order_type == OrderType.SL_M:
            return kite.ORDER_TYPE_SLM, req.trigger_price

        if req.order_type == OrderType.SL_L:
            return kite.ORDER_TYPE_SL, req.price

        return kite.ORDER_TYPE_MARKET, 0.0

    def _get_ltp_safe(self, symbol: str, exchange: str) -> float:
        """Fetch LTP from Kite; return 0 on any failure."""
        try:
            full = f"{_resolve_exchange(exchange)}:{symbol}"
            data = self._kite.ltp([full])
            return float(data[full]["last_price"])
        except Exception:
            return 0.0

    # ── Order management ─────────────────────────────────────────────────────

    async def cancel_order(self, order_id: str) -> bool:
        try:
            await asyncio.to_thread(
                self._kite.cancel_order,
                variety=self._kite.VARIETY_REGULAR,
                order_id=order_id,
            )
            return True
        except Exception as exc:
            logger.warning("ZerodhaBroker[%s]: cancel_order %s failed: %s",
                           self.client_id, order_id, exc)
            return False

    async def get_order_status(self, order_id: str) -> OrderFill:
        try:
            history = await asyncio.to_thread(self._kite.order_history, order_id)
            latest = history[-1] if history else {}
            status_str = (latest.get("status") or "unknown").lower()
            status = _STATUS_MAP.get(status_str, OrderStatus.UNKNOWN)
            return OrderFill(
                order_id     = order_id,
                broker_symbol= latest.get("tradingsymbol", ""),
                side         = (OrderSide.BUY if latest.get("transaction_type") == "BUY"
                                else OrderSide.SELL),
                qty          = int(latest.get("filled_quantity", 0)),
                avg_price    = float(latest.get("average_price", 0.0)),
                status       = status,
                client_id    = self.client_id,
                raw          = latest,
            )
        except Exception as exc:
            logger.warning("ZerodhaBroker[%s]: get_order_status %s: %s",
                           self.client_id, order_id, exc)
            return OrderFill(
                order_id=order_id, broker_symbol="", side=OrderSide.BUY,
                qty=0, avg_price=0.0, status=OrderStatus.UNKNOWN,
                client_id=self.client_id,
            )

    async def get_positions(self) -> List[PositionRecord]:
        try:
            raw = await asyncio.to_thread(self._kite.positions)
            records = []
            for p in raw.get("day", []):
                qty = int(p.get("quantity", 0))
                if qty == 0:
                    continue
                records.append(PositionRecord(
                    symbol    = p.get("tradingsymbol", ""),
                    qty       = qty,
                    avg_price = float(p.get("average_price", 0.0)),
                    pnl       = float(p.get("pnl", 0.0)),
                    product   = p.get("product", self._product),
                ))
            return records
        except Exception as exc:
            logger.warning("ZerodhaBroker[%s]: get_positions: %s", self.client_id, exc)
            return []

    async def get_funds(self) -> Dict[str, float]:
        try:
            margins = await asyncio.to_thread(self._kite.margins, "equity")
            avail = margins.get("available", {})
            return {
                "available": float(avail.get("cash", 0.0)),
                "used":      float(margins.get("utilised", {}).get("span", 0.0)),
            }
        except Exception as exc:
            logger.warning("ZerodhaBroker[%s]: get_funds: %s", self.client_id, exc)
            return {"available": 0.0, "used": 0.0}


# ── Register in BROKER_REGISTRY ───────────────────────────────────────────────

def _zerodha_factory(binding: BrokerBinding, client_id: str) -> ZerodhaBroker:
    return ZerodhaBroker(binding, client_id)


BROKER_REGISTRY["zerodha"] = _zerodha_factory
BROKER_REGISTRY["kite"]    = _zerodha_factory   # alias
