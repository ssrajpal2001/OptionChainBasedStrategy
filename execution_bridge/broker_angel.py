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
        # Symboltoken resolution caches (avoid per-order searchScrip → rate limit).
        self._tok_cache: dict = {}     # (exchange, our_symbol) -> (token, angel_symbol)
        self._scrip_cache: dict = {}   # (exchange, underlying) -> [scrip dicts]
        self._bfo_master: dict = {}    # tradingsymbol -> {"token":..., "tradingsymbol":...}

    async def authenticate(self) -> bool:
        try:
            from SmartApi import SmartConnect  # type: ignore[import]
        except ImportError:
            logger.error("smartapi-python not installed. pip install smartapi-python pyotp")
            return False

        try:
            self._smartapi = SmartConnect(api_key=self._b.api_key)

            # SEBI static-IP: bind this binding's API egress to its whitelisted IP (if set).
            _src = (getattr(self._b, "source_ip", "") or "").strip()
            if _src:
                try:
                    from execution_bridge.ip_bind import bind_source_ip
                    bound = bind_source_ip(self._smartapi, _src)
                    logger.info("AngelBroker[%s]: source-IP bind to %s (%s)",
                                self.client_id, _src, "ok" if bound else "no session found")
                except Exception as exc:
                    logger.error("AngelBroker[%s]: source-IP bind to %s FAILED: %s", self.client_id, _src, exc)

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
                # Save JWT token back to binding so UI shows "Token OK"
                jwt = (data.get("data") or {}).get("jwtToken", "")
                if jwt:
                    self._b.access_token = jwt

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
            self._trading_mode_raw = "live" if mode == "live" else "paper"
            if pt in ("MIS", "INTRADAY"):
                self._product = "INTRADAY"
            elif pt in ("NRML", "NORMAL"):
                self._product = "DELIVERY"
            else:
                self._product = "INTRADAY" if mode not in ("carryforward", "normal", "nrml") else "DELIVERY"
            logger.info("AngelBroker [%s]: Authenticated. product=%s", self.client_id, self._product)
            # Warm BFO scrip master so SENSEX symbol tokens resolve without searchScrip.
            await self._warm_bfo_master()
            return True

        except Exception as exc:
            logger.error("AngelBroker [%s]: authenticate() error: %s", self.client_id, exc)
            return False

    async def logout(self) -> None:
        # AngelOne terminateSession requires the refresh token; skip silently if not available
        self._authenticated = False
        self._smartapi = None

    async def _warm_bfo_master(self) -> None:
        """Download AngelOne scrip master JSON, filter to BFO options, cache daily.
        Avoids searchScrip("BFO",...) which returns empty for most SENSEX strikes."""
        import json, os, urllib.request
        from datetime import date as _date
        cache_path = os.path.join("data", "cache", "angel_bfo_master.json")
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        today = str(_date.today())
        # Load from daily cache if available
        if os.path.exists(cache_path):
            try:
                with open(cache_path) as f:
                    cached = json.load(f)
                if cached.get("date") == today and cached.get("instruments"):
                    self._bfo_master = cached["instruments"]
                    logger.info("AngelBroker[%s]: BFO master loaded from cache (%d instruments)",
                                self.client_id, len(self._bfo_master))
                    return
            except Exception:
                pass
        # Download full scrip master from AngelOne public URL
        url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        try:
            logger.info("AngelBroker[%s]: downloading scrip master for BFO cache...", self.client_id)
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = json.loads(resp.read())
            bfo: dict = {}
            for it in raw:
                if it.get("exch_seg") == "BFO" and it.get("instrumenttype") == "OPTIDX":
                    ts  = str(it.get("symbol") or "").upper()
                    tok = str(it.get("token") or "")
                    if ts and tok:
                        bfo[ts] = {"token": tok, "tradingsymbol": ts}
            with open(cache_path, "w") as f:
                json.dump({"date": today, "instruments": bfo}, f)
            self._bfo_master = bfo
            logger.info("AngelBroker[%s]: BFO master cached (%d SENSEX option instruments)",
                        self.client_id, len(bfo))
        except Exception as exc:
            logger.warning("AngelBroker[%s]: BFO master download failed — will use searchScrip fallback: %s",
                           self.client_id, exc)
            self._bfo_master = {}

    def _lookup_symbol(self, exchange: str, tradingsymbol: str):
        """Resolve (symboltoken, angel_tradingsymbol). Cached so we never re-hit
        searchScrip for the same contract (the rate-limit cause). Strategy:
          1. exact match on our symbol (works for NSE),
          2. else search by UNDERLYING NAME (cached once per underlying) and match
             the contract by strike + option type + expiry month/year. Needed for
             MCX where Angel's tradingsymbol format differs from ours
             (e.g. our CRUDEOIL26JUN8800CE ≠ Angel's master symbol)."""
        import re
        key = (exchange, tradingsymbol)
        if key in self._tok_cache:
            return self._tok_cache[key]
        resolved = ("", tradingsymbol)
        # BFO (BSE F&O / SENSEX): use pre-downloaded master — searchScrip("BFO",...) returns empty.
        if exchange == "BFO" and self._bfo_master:
            hit = self._bfo_master.get(tradingsymbol.upper())
            if hit:
                resolved = (hit["token"], hit["tradingsymbol"])
                self._tok_cache[key] = resolved
                return resolved
            logger.warning("AngelBroker[%s]: %s not in BFO master (%d entries) — trying searchScrip",
                           self.client_id, tradingsymbol, len(self._bfo_master))
        try:
            res = self._smartapi.searchScrip(exchange, tradingsymbol)
            if res and res.get("status") and res.get("data"):
                for it in res["data"]:
                    if it.get("tradingsymbol") == tradingsymbol:
                        resolved = (str(it.get("symboltoken", "")), tradingsymbol)
                        break

            if not resolved[0]:
                # Format: UNDERLYING + DD + MON + YY + strike + CE/PE
                # e.g. SENSEX25JUN2681000CE or NIFTY02JUN2624500CE
                # We extract strike by stripping the trailing CE/PE, then
                # stripping the 2-digit YY that precedes it: the remaining
                # digits after DD+MON are YY(2d)+strike, so skip first 2 digits.
                m = re.match(r'^([A-Z]+?)(\d{2})([A-Z]{3})(\d{2})(\d+)(CE|PE)$', tradingsymbol)
                if m:
                    name, dd, mon, yy, strike, opt = m.groups()
                    lst = self._scrip_cache.get((exchange, name))
                    if lst is None:
                        r2 = self._smartapi.searchScrip(exchange, name)
                        lst = (r2.get("data") or []) if r2 and r2.get("status") else []
                        self._scrip_cache[(exchange, name)] = lst
                    for it in lst:
                        ts = str(it.get("tradingsymbol", "")).upper()
                        if (ts.endswith(opt) and mon in ts and dd in ts
                                and re.search(rf'(?<!\d){int(strike)}(?={opt}$)', ts)):
                            resolved = (str(it.get("symboltoken", "")), it.get("tradingsymbol", tradingsymbol))
                            break
        except Exception as exc:
            logger.warning("AngelBroker [%s]: symbol resolve failed for %s: %s",
                           self.client_id, tradingsymbol, exc)
        self._tok_cache[key] = resolved
        return resolved

    async def place_order(self, req: OrderRequest) -> str:
        if not self._smartapi:
            raise RuntimeError("Not authenticated.")
        _type_map = {
            OrderType.MARKET: "MARKET", OrderType.LIMIT: "LIMIT",
            OrderType.SL_M: "STOPLOSS_MARKET", OrderType.SL_L: "STOPLOSS_LIMIT",
        }
        # Resolve symboltoken + Angel's own tradingsymbol (cached; handles MCX).
        symbol_token, resolved_symbol = await asyncio.to_thread(
            self._lookup_symbol, req.exchange, req.broker_symbol
        )
        if not symbol_token:
            raise RuntimeError(
                f"Angel One: could not resolve symboltoken for {req.broker_symbol} "
                f"on {req.exchange} (not in scrip master)")
        # AngelOne does not support variety='AMO' — valid values: NORMAL, STOPLOSS, ROBO.
        # Orders placed outside market hours with NORMAL are queued as AMO automatically.
        order_data = {
            "variety": "NORMAL",
            "tradingsymbol": resolved_symbol,
            "symboltoken": symbol_token,
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
        # SmartAPI v1.5.5 returns orderid string directly on success
        if isinstance(ret, str) and ret:
            return ret
        # Older builds return full response dict
        if isinstance(ret, dict) and ret.get("status"):
            data = ret.get("data") or {}
            return str(data.get("orderid", ""))
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
