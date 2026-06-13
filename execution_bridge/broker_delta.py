"""
execution_bridge/broker_delta.py — Delta Exchange (India) crypto-options broker.

STAGE 2a of the Delta plug-and-play integration. Subclasses the EXISTING BaseBroker and registers
in BROKER_REGISTRY, so the ExecutionRouter routes to it with zero strategy/logic change — the strategy
stays market-agnostic.

Auth (per Delta India spec):
  signature = hex( HMAC_SHA256(api_secret, method + timestamp + path + query_string + payload) )
  headers: api-key, timestamp (unix seconds, 5s window), signature, Content-Type: application/json
Base URL:
  production India : https://api.india.delta.exchange
  testnet          : https://cdn-ind.testnet.deltaex.org   (binding.source_ip == "testnet" → testnet)

Symbology: WS/market string  O-{UND}-{STRIKE}-{DDMMYY}-{C|P}  (UniversalOptionMapper), but REST order
entry needs the integer `product_id` resolved from GET /v2/products. Crypto has LEVERAGE
(POST /v2/products/change_leverage) and wallet balances (GET /v2/wallet/balances).

Requires only `requests` (already a dependency). All network I/O via asyncio.to_thread.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time as _time
from typing import Any, Dict, List, Optional

from config.client_profiles import BrokerBinding
from execution_bridge.base_broker import (
    BaseBroker, OrderFill, OrderRequest, OrderSide, OrderStatus,
    OrderType, PositionRecord, BROKER_REGISTRY,
)

logger = logging.getLogger(__name__)

PROD_BASE = "https://api.india.delta.exchange"
TESTNET_BASE = "https://cdn-ind.testnet.deltaex.org"

_ORDER_TYPE = {
    OrderType.MARKET: "market_order",
    OrderType.LIMIT:  "limit_order",
    OrderType.SL_M:   "market_order",
    OrderType.SL_L:   "limit_order",
}
_STATUS = {
    "open": OrderStatus.OPEN, "pending": OrderStatus.OPEN,
    "closed": OrderStatus.COMPLETE, "filled": OrderStatus.COMPLETE,
    "cancelled": OrderStatus.CANCELLED, "rejected": OrderStatus.REJECTED,
}


def delta_signature(api_secret: str, method: str, path: str,
                    timestamp: str, query_string: str = "", payload: str = "") -> str:
    """Pure HMAC-SHA256 hex signature over method+timestamp+path+query+payload (Delta spec)."""
    data = f"{method.upper()}{timestamp}{path}{query_string}{payload}"
    return hmac.new(api_secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256).hexdigest()


class DeltaBroker(BaseBroker):
    def __init__(self, binding: BrokerBinding, client_id: str) -> None:
        super().__init__(binding.binding_id, client_id)
        self._b = binding
        self._key = getattr(binding, "api_key", "") or ""
        self._secret = getattr(binding, "api_secret", "") or ""
        # Reuse the source_ip field as an optional "testnet" flag for crypto (no SEBI IP on Delta).
        self._base = TESTNET_BASE if str(getattr(binding, "source_ip", "")).lower() == "testnet" else PROD_BASE
        self._symbol_to_pid: Dict[str, int] = {}     # "C-BTC-60000-310726" -> product_id
        self._order_pid: Dict[str, int] = {}          # order_id -> product_id (Delta cancel needs BOTH)
        self._products: Dict[str, dict] = {}          # symbol -> full product meta (tick/contract/settle)
        self._leverage: float = 1.0

    # ── signed REST ───────────────────────────────────────────────────────────
    def _request_sync(self, method: str, path: str, payload: Optional[dict] = None,
                      auth: bool = True) -> dict:
        import requests
        ts = str(int(_time.time()))
        body = json.dumps(payload) if payload else ""
        headers = {"Content-Type": "application/json", "User-Agent": "rest-client"}
        if auth:
            headers.update({
                "api-key": self._key, "timestamp": ts,
                "signature": delta_signature(self._secret, method, path, ts, "", body),
            })
        url = f"{self._base}{path}"
        try:
            if method.upper() == "POST":
                r = requests.post(url, headers=headers, data=body, timeout=10)
            else:
                r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 429:
                logger.warning("DeltaBroker[%s]: rate-limited (429) on %s", self.client_id, path)
                return {"success": False, "error": "rate_limited"}
            return r.json()
        except Exception as exc:
            logger.error("DeltaBroker[%s]: request %s %s failed: %s", self.client_id, method, path, exc)
            return {"success": False, "error": str(exc)}

    async def _request(self, method: str, path: str, payload: Optional[dict] = None, auth: bool = True) -> dict:
        import asyncio
        return await asyncio.to_thread(self._request_sync, method, path, payload, auth)

    # ── lifecycle ─────────────────────────────────────────────────────────────
    async def authenticate(self) -> bool:
        if not self._key or not self._secret:
            logger.error("DeltaBroker[%s]: api_key/api_secret required.", self.client_id)
            return False
        await self._load_products()
        bal = await self._request("GET", "/v2/wallet/balances")
        if bal and (bal.get("success") is True or "result" in bal):
            self._authenticated = True
            mode = str(getattr(self._b, "trading_mode", "paper")).lower()
            self._trading_mode_raw = "live" if mode == "live" else "paper"
            logger.info("DeltaBroker[%s]: authenticated (base=%s, %d products).",
                        self.client_id, self._base, len(self._symbol_to_pid))
            return True
        logger.error("DeltaBroker[%s]: auth check failed: %s", self.client_id, bal)
        return False

    async def logout(self) -> None:
        self._authenticated = False

    async def _load_products(self) -> None:
        """GET /v2/products (public) → index option contracts. Real Delta India shape:
        contract_type in {call_options, put_options}, symbol like 'C-BTC-60000-310726',
        strike_price, settlement_time (…T12:00:00Z == 17:30 IST), tick_size, contract_value."""
        res = await self._request("GET", "/v2/products", auth=False)
        rows = (res or {}).get("result", []) or []
        n = 0
        for p in rows:
            if str(p.get("contract_type", "")) not in ("call_options", "put_options"):
                continue
            sym = str(p.get("symbol", "")).upper()
            pid = p.get("id")
            if not sym or pid is None:
                continue
            self._symbol_to_pid[sym] = int(pid)
            self._products[sym] = p
            n += 1
        logger.info("DeltaBroker[%s]: loaded %d option products.", self.client_id, n)

    # ── exchange-derived chain (strikes / steps / expiries are NOT hardcoded) ──
    def discover_chain(self, underlying: str) -> Dict[str, Any]:
        """From the loaded products, return the REAL option chain metadata for `underlying`:
        per-expiry sorted strikes + the observed strike step(s) (Delta steps are non-uniform —
        e.g. BTC 200 near ATM, 400/600 in the wings), plus tick_size/contract_value. The strategy
        must read strike spacing from HERE, never assume a fixed step."""
        und = str(underlying).upper()
        by_exp: Dict[str, list] = {}
        tick = contract_val = None
        for sym, p in self._products.items():
            if (p.get("underlying_asset") or {}).get("symbol") != und:
                continue
            exp = str(p.get("settlement_time", ""))[:10]          # YYYY-MM-DD (17:30 IST)
            try:
                by_exp.setdefault(exp, []).append(int(float(p.get("strike_price") or 0)))
            except Exception:
                continue
            tick = tick or p.get("tick_size")
            contract_val = contract_val or p.get("contract_value")
        out: Dict[str, Any] = {"underlying": und, "tick_size": tick,
                               "contract_value": contract_val, "expiries": {}}
        for exp, strikes in by_exp.items():
            s = sorted(set(strikes))
            steps = sorted({s[i + 1] - s[i] for i in range(len(s) - 1)}) if len(s) > 1 else []
            out["expiries"][exp] = {"strikes": s, "steps": steps,
                                    "min_step": steps[0] if steps else None,
                                    "count": len(s)}
        return out

    def product_id_for(self, market_symbol: str) -> Optional[int]:
        return self._symbol_to_pid.get(str(market_symbol).upper())

    async def get_quote(self, market_symbol: str):
        """(best_bid, best_ask) for LIMIT mid-pricing. Public ticker endpoint."""
        res = await self._request("GET", f"/v2/tickers/{market_symbol}", auth=False)
        q = ((res or {}).get("result") or {}).get("quotes") or {}
        try:
            return float(q.get("best_bid") or 0.0), float(q.get("best_ask") or 0.0)
        except Exception:
            return 0.0, 0.0

    # ── orders ────────────────────────────────────────────────────────────────
    async def place_order(self, req: OrderRequest) -> str:
        pid = self._symbol_to_pid.get(req.broker_symbol.upper())
        if pid is None:
            raise RuntimeError(f"Delta: no product_id for {req.broker_symbol} (not in /v2/products).")
        body = {
            "product_id": pid,
            "size": int(req.qty),
            "side": "buy" if req.side == OrderSide.BUY else "sell",
            "order_type": _ORDER_TYPE[req.order_type],
        }
        if req.order_type in (OrderType.LIMIT, OrderType.SL_L) and req.price > 0:
            body["limit_price"] = str(req.price)
        res = await self._request("POST", "/v2/orders", body)
        if res and res.get("success") and res.get("result"):
            oid = str(res["result"].get("id", ""))
            if oid:
                self._order_pid[oid] = pid           # remember pid so we can CANCEL it later
            return oid
        # Surface the EXCHANGE rejection reason verbatim (e.g. insufficient_margin) for analysis.
        _err = (res or {}).get("error") if res else None
        _code = (_err or {}).get("code") if isinstance(_err, dict) else _err
        _ctx = (_err or {}).get("context") if isinstance(_err, dict) else None
        raise RuntimeError(f"Delta REJECTED {req.side} {req.qty} {req.broker_symbol}: "
                           f"reason={_code} context={_ctx}")

    async def cancel_order(self, order_id: str) -> bool:
        # Delta's DELETE /v2/orders requires BOTH id AND product_id. Sending only id silently
        # fails → the order stays live and fills later as a maker (the cancel-race that left the
        # smart executor thinking a leg was dead while Delta kept filling it). Always include pid.
        body: Dict[str, Any] = {"id": order_id}
        pid = self._order_pid.get(str(order_id))
        if pid is None:
            # Fall back: read the order to recover its product_id, then cancel.
            try:
                _o = ((await self._request("GET", f"/v2/orders/{order_id}")) or {}).get("result") or {}
                pid = _o.get("product_id")
            except Exception:
                pid = None
        if pid is not None:
            body["product_id"] = int(pid)
        res = await self._request("DELETE", "/v2/orders", body)
        ok = bool(res and res.get("success"))
        if not ok:
            logger.warning("Delta cancel_order %s failed (pid=%s): %s",
                           order_id, pid, (res or {}).get("error") if res else res)
        return ok

    async def get_order_status(self, order_id: str) -> OrderFill:
        res = await self._request("GET", f"/v2/orders/{order_id}")
        o = (res or {}).get("result") or {}
        avg = float(o.get("average_fill_price") or 0.0) if o else 0.0
        # FILLED qty = size − unfilled_size (NOT the order size). Returning the order size made a
        # working/cancelled order look fully filled → the smart executor double-counted on a chase.
        _size = int(float(o.get("size", 0) or 0))
        _unfilled = int(float(o.get("unfilled_size", _size) or 0)) if o else _size
        _filled = max(0, _size - _unfilled)
        _state = str(o.get("state", "")).lower()
        status = _STATUS.get(_state, OrderStatus.UNKNOWN)
        # Robust terminal detection — _cancel_and_settle waits for a TERMINAL state, and an unmapped
        # Delta state string (e.g. 'pending_cancel') used to read UNKNOWN forever → the executor
        # bailed ('did not reach terminal state') while the order kept filling. Derive terminality
        # from the fill counters when the state string isn't decisive:
        if status == OrderStatus.UNKNOWN:
            if _size > 0 and _unfilled == 0:
                status = OrderStatus.COMPLETE          # fully filled regardless of state label
            elif not o:
                status = OrderStatus.CANCELLED         # order no longer exists → treat as terminal
        return OrderFill(
            order_id=str(order_id), broker_symbol=str(o.get("product_symbol", "")),
            side=OrderSide.BUY if str(o.get("side", "")) == "buy" else OrderSide.SELL,
            qty=_filled, avg_price=avg,
            status=status, client_id=self.client_id, raw=o,
        )

    async def get_positions(self) -> List[PositionRecord]:
        res = await self._request("GET", "/v2/positions/margined")
        out: List[PositionRecord] = []
        for p in ((res or {}).get("result") or []):
            out.append(PositionRecord(
                symbol=str(p.get("product_symbol", "")),
                qty=int(p.get("size", 0) or 0),
                avg_price=float(p.get("entry_price", 0) or 0),
                pnl=float(p.get("unrealized_pnl", 0) or 0),
                product="MARGIN",
            ))
        return out

    # ── funds / profile / leverage (surfaced to the UI) ────────────────────────
    async def get_funds(self) -> Dict[str, float]:
        res = await self._request("GET", "/v2/wallet/balances")
        rows = (res or {}).get("result") or []
        avail = sum(float(w.get("available_balance", 0) or 0) for w in rows)
        used = sum(float(w.get("blocked_margin", 0) or 0) for w in rows)
        return {"available": avail, "used": used}

    async def get_profile(self) -> Dict[str, Any]:
        res = await self._request("GET", "/v2/users/profile")
        return (res or {}).get("result") or {}

    async def get_leverage(self, product_id: int) -> float:
        res = await self._request("GET", f"/v2/products/{product_id}/orders/leverage")
        try:
            return float(((res or {}).get("result") or {}).get("leverage", 1.0))
        except Exception:
            return 1.0

    async def set_leverage(self, product_id: int, leverage: float) -> bool:
        res = await self._request("POST", "/v2/products/change_leverage",
                                  {"product_id": int(product_id), "leverage": str(leverage)})
        ok = bool(res and res.get("success"))
        if ok:
            self._leverage = float(leverage)
            logger.info("DeltaBroker[%s]: leverage set to %sx on product %s.",
                        self.client_id, leverage, product_id)
        return ok


# Self-register so the ExecutionRouter / factory picks it up from client credentials.
BROKER_REGISTRY["delta"] = lambda b, cid: DeltaBroker(b, cid)
