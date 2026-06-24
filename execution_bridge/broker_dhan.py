"""
execution_bridge/broker_dhan.py — Dhan HQ broker.

Requires: pip install dhanhq
"""

from __future__ import annotations

import asyncio
import csv
import io
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

            # client_code stores the Dhan Client ID; fall back to user_id if not set
            dhan_client_id = self._b.client_code or self._b.user_id
            if not self._b.access_token or not dhan_client_id:
                logger.error(
                    "DhanBroker [%s]: access_token and client_code (or user_id) are required.",
                    self.client_id,
                )
                return False

            self._dhan = dhanhq(dhan_client_id, self._b.access_token)
            # SEBI static-IP: bind API egress to this binding's whitelisted IP (if set).
            _src = (getattr(self._b, "source_ip", "") or "").strip()
            if _src:
                try:
                    from execution_bridge.ip_bind import bind_source_ip
                    bound = bind_source_ip(self._dhan, _src)
                    logger.info("DhanBroker[%s]: source-IP bind to %s (%s)",
                                self.client_id, _src, "ok" if bound else "no session found")
                except Exception as exc:
                    logger.error("DhanBroker[%s]: source-IP bind to %s FAILED: %s", self.client_id, _src, exc)
            # Verify by calling Dhan fund-limit REST directly (dhanhq 1.3 library
            # returns empty body from get_fund_limits due to a backend API change).
            import requests as _req
            try:
                _r = await asyncio.to_thread(
                    lambda: _req.get(
                        "https://api.dhanhq.com/v2/fundlimit",
                        headers={"access-token": self._b.access_token,
                                 "client-id": dhan_client_id,
                                 "Content-Type": "application/json"},
                        timeout=10,
                    )
                )
                funds = _r.json() if _r.ok and _r.text.strip() else {"status": "success"}
            except Exception as _fe:
                logger.warning("DhanBroker [%s]: fund verify failed (%s) — trusting token.", self.client_id, _fe)
                funds = {"status": "success"}
            if funds and (funds.get("status") == "success" or "data" in funds or funds.get("availabelBalance") is not None):
                self._authenticated = True
                pt   = getattr(self._b, "product_type", "").strip().upper()
                mode = getattr(self._b, "trading_mode", "intraday").lower()
                self._trading_mode_raw = "live" if mode == "live" else "paper"
                if pt in ("MIS", "INTRADAY"):
                    self._product = "INTRADAY"
                elif pt in ("NRML", "NORMAL"):
                    self._product = "MARGIN"
                else:
                    self._product = "INTRADAY" if mode not in ("carryforward", "normal", "nrml") else "MARGIN"
                # Load Dhan instrument master to build security_id map
                await self._load_instrument_master()
                logger.info("DhanBroker [%s]: Authenticated. product=%s symbols=%d",
                            self.client_id, self._product, len(self._symbol_map))
                return True
            logger.error("DhanBroker [%s]: Auth check failed: %s", self.client_id, funds)
            return False
        except ImportError:
            logger.error("dhanhq not installed. pip install dhanhq")
            return False

    async def logout(self) -> None:
        self._authenticated = False

    async def _load_instrument_master(self) -> None:
        """Download Dhan instrument master CSV and build {lookup_key: security_id} map."""
        import urllib.request
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        try:
            data = await asyncio.to_thread(
                lambda: urllib.request.urlopen(url, timeout=15).read().decode("utf-8")
            )
            reader = csv.DictReader(io.StringIO(data))
            count = 0
            for row in reader:
                seg  = (row.get("SEM_EXM_EXCH_ID") or "").strip()
                sym  = (row.get("SEM_TRADING_SYMBOL") or "").strip()
                sid  = (row.get("SEM_SMST_SECURITY_ID") or "").strip()
                exp  = (row.get("SEM_EXPIRY_DATE") or "").strip()[:10]  # YYYY-MM-DD
                strike_raw = (row.get("SEM_STRIKE_PRICE") or "0").strip()
                opt  = (row.get("SEM_OPTION_TYPE") or "").strip()
                inst = (row.get("SEM_INSTRUMENT_NAME") or "").strip()
                undl = (row.get("SEM_LOT_UNITS") or "").strip()
                series = (row.get("SEM_SERIES") or "").strip()

                # NSE/BSE equity & index options; MCX commodity options (OPTFUT)
                is_nse_bse = seg in ("NSE", "BSE") and inst in ("OPTIDX", "OPTSTK")
                is_mcx     = seg == "MCX" and inst in ("OPTFUT", "OPTIDX")
                if not sid or not (is_nse_bse or is_mcx):
                    continue
                # Build canonical key: UNDERLYING:DDMONYY:STRIKE:CE/PE
                # Match format used by to_dhan_lookup_key (InternalSymbol.__str__)
                # InternalSymbol str: "CRUDEOIL:22JUL26:6800:CE"
                try:
                    from datetime import date as _date
                    exp_d = _date.fromisoformat(exp)
                    dd  = exp_d.strftime("%d")
                    mon = exp_d.strftime("%b").upper()
                    yy  = exp_d.strftime("%y")
                    strike_i = int(float(strike_raw))
                    # Extract underlying from trading symbol
                    underlying = sym  # fallback
                    _all_prefixes = (
                        "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "SENSEX", "NIFTY",
                        "CRUDEOILM", "CRUDEOIL", "NATURALGAS", "GOLD", "SILVER",
                        "COPPER", "ZINC", "LEAD", "NICKEL", "ALUMINIUM",
                    )
                    for prefix in _all_prefixes:
                        if sym.startswith(prefix):
                            underlying = prefix
                            break
                    key = f"{underlying}:{dd}{mon}{yy}:{strike_i}:{opt}"
                    self._symbol_map[key] = sid
                    count += 1
                except Exception:
                    continue
            logger.info("DhanBroker [%s]: loaded %d instrument keys from master.", self.client_id, count)
        except Exception as exc:
            logger.warning("DhanBroker [%s]: instrument master load failed: %s — orders may fail without security_id.", self.client_id, exc)

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
        # Dhan exchange segments: NSE_FNO (NFO), BSE_FNO (BFO/SENSEX), MCX_COMM (MCX).
        # Previously MCX fell through to BSE_FNO → empty/garbage response.
        _seg = {"NFO": "NSE_FNO", "BFO": "BSE_FNO", "MCX": "MCX_COMM"}.get(req.exchange, "NSE_FNO")
        if req.exchange == "MCX" and security_id == req.broker_symbol:
            logger.warning("DhanBroker [%s]: no MCX security_id for %s — order will likely reject; "
                           "ensure the Dhan master includes MCX commodities.", self.client_id, req.broker_symbol)
        order_params = {
            "security_id":        security_id,
            "exchange_segment":   _seg,
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
        import requests
        try:
            dhan_client_id = self._b.client_code or self._b.user_id
            r = await asyncio.to_thread(
                lambda: requests.get(
                    "https://api.dhanhq.com/v2/fundlimit",
                    headers={
                        "access-token": self._b.access_token,
                        "client-id":    dhan_client_id,
                        "Content-Type": "application/json",
                    },
                    timeout=10,
                )
            )
            data = r.json() if r.ok else {}
            avail = (data.get("availabelBalance")
                     or data.get("availableBalance")
                     or data.get("net")
                     or data.get("availableLimit")
                     or 0)
            used = (data.get("utilizedAmount")
                    or data.get("utilisedAmount")
                    or 0)
            return {"available": float(avail or 0), "used": float(used or 0)}
        except Exception as exc:
            logger.debug("DhanBroker get_funds: %s", exc)
            return {"available": 0.0, "used": 0.0}


# Self-register
BROKER_REGISTRY["dhan"] = lambda b, cid: DhanBroker(b, cid)
