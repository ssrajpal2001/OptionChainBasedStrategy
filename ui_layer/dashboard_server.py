"""
ui_layer/dashboard_server.py — FastAPI async web dashboard server with RBAC.

Endpoints:
  PUBLIC
    POST /api/auth/login              — issue JWT (admin or client role)

  ADMIN-ONLY  (Bearer admin JWT required)
    GET  /api/status                  — all client status summary
    GET  /api/workers                 — execution worker stats
    GET  /api/broker_status           — auth status for all broker bindings
    POST /api/halt/{client_id}        — halt a single client
    POST /api/resume/{client_id}      — resume a halted client
    POST /api/halt_all                — halt every client
    POST /api/kill_switch             — halt all + KILL_SWITCH event
    POST /api/rebalance/{index}       — force ATM rebalance for an underlying
    POST /api/set_lots/{cid}/{bid}/{m}— override lot multiplier
    POST /api/client/update_token     — inject new broker credentials + re-auth
    GET  /api/admin/feeder/status     — GlobalFeeder connection info
    POST /api/admin/feeder/connect    — switch / reconnect data feeder live

  CLIENT-ONLY  (Bearer client JWT required)
    GET  /api/client/me               — own portfolio snapshot
    GET  /api/client/brokers          — own broker binding status
    POST /api/client/register_broker  — provision + authenticate a new broker on-the-fly

  COMMON (any valid JWT)
    WS   /ws?token=<jwt>              — live event stream

No time.sleep.  All async.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
from datetime import datetime
from typing import List, Optional

from config.global_config import IST, Topic, SysEvent
from data_layer.base_feeder import EventBus
from ui_layer.auth import create_token, verify_token
from ui_layer.ws_bridge import WsBridge

logger = logging.getLogger(__name__)

# FastAPI imports at module level so 'from __future__ import annotations' doesn't
# prevent FastAPI from resolving type hints on route functions (lazy-string annotations
# are resolved against module globals, so locally-imported types are invisible).
try:
    from fastapi import (
        FastAPI, WebSocket, WebSocketDisconnect,
        HTTPException, Depends, Query, Body, Request,
    )
    from fastapi.responses import FileResponse, HTMLResponse
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    from pydantic import BaseModel as _PydanticBase
    _HAS_FASTAPI = True

    # Pydantic schemas at module level so FastAPI can resolve type hints even
    # when 'from __future__ import annotations' makes all annotations lazy strings.
    class _LoginSchema(_PydanticBase):
        role:     str = ""
        username: str = ""
        password: str = ""

    class _TokenUpdateSchema(_PydanticBase):
        client_id:    str
        binding_id:   str
        access_token: Optional[str] = None
        api_key:      Optional[str] = None
        api_secret:   Optional[str] = None
        user_id:      Optional[str] = None
        password:     Optional[str] = None
        totp_secret:  Optional[str] = None

    class _FeederConnectSchema(_PydanticBase):
        provider:     str
        user_id:      Optional[str] = None
        password:     Optional[str] = None
        api_key:      Optional[str] = None
        api_secret:   Optional[str] = None
        access_token: Optional[str] = None

    class _BrokerProvisionSchema(_PydanticBase):
        binding_id:     str
        provider:       str
        label:          str = ""
        user_id:        str = ""
        password:       str = ""
        api_key:        str = ""
        api_secret:     str = ""
        totp_secret:    str = ""
        vendor_code:    str = ""
        imei:           str = ""
        client_code:    str = ""
        access_token:   str = ""
        lot_multiplier: float = 1.0

    class _DualFeederSchema(_PydanticBase):
        upstox_client_id:    str = ""
        upstox_api_key:      str = ""
        upstox_secret:       str = ""
        upstox_access_token: str = ""
        upstox_totp:         str = ""
        fyers_client_id:     str = ""
        fyers_app_key:       str = ""
        fyers_access_token:  str = ""
        fyers_totp:          str = ""

    class _RmsConfigSchema(_PydanticBase):
        max_drawdown_pct:       float = 5.0
        order_throttle_per_sec: int   = 5
        squareoff_time:         str   = "15:15"
        distance_filter_pct:    float = 5.0

    class _ClientRegisterSchema(_PydanticBase):
        client_id:          str
        name:               str   = ""
        capital:            float = 500_000.0
        provider:           str   = "mock"
        binding_id:         str   = ""
        lot_multiplier:     float = 1.0
        max_risk_pct:       float = 1.0
        max_daily_loss_pct: float = 3.0
        strategies:         List[str] = ["A", "B", "C"]

except ImportError:
    _HAS_FASTAPI = False

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
_MONITOR_HTML = os.path.join(_TEMPLATE_DIR, "monitor.html")


class DashboardServer:
    """
    FastAPI-based async admin/client dashboard with JWT RBAC.

    Two roles:
      admin  — full system view + all management actions
      client — context-locked to own sub-portfolio; can register new brokers live

    Usage:
        dashboard = DashboardServer(bus, cfg, registry,
                                    router=router, rebalancer=rebalancer,
                                    feeder=feeder)
        task = asyncio.create_task(dashboard.serve(host="0.0.0.0", port=8080))
        ...
        dashboard.stop()
        await task
    """

    def __init__(
        self,
        bus: EventBus,
        cfg,              # GlobalConfig
        registry,         # ClientRegistry
        router=None,      # ExecutionRouter
        rebalancer=None,  # StrikeRebalancer
        feeder=None,      # GlobalFeeder — optional, for admin feeder management
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._registry = registry
        self._router = router
        self._rebalancer = rebalancer
        self._feeder = feeder
        self._ws_bridge = WsBridge(bus, cfg=cfg)
        self._uvicorn_server = None

        # Register heartbeat providers
        self._ws_bridge.register_stats_provider("clients", self._client_summary)
        if router is not None:
            self._ws_bridge.register_stats_provider(
                "workers",
                lambda: self._router.worker_stats() if self._router else [],
            )
            self._ws_bridge.register_stats_provider("brokers", self._broker_summary)

        self._app = self._build_app()

        if not hasattr(cfg, "rms"):
            cfg.rms = {
                "max_drawdown_pct":       5.0,
                "order_throttle_per_sec": 5,
                "squareoff_time":         "15:15",
                "distance_filter_pct":    5.0,
            }

    @property
    def ws_bridge(self) -> WsBridge:
        return self._ws_bridge

    # ── FastAPI application ───────────────────────────────────────────────────

    def _build_app(self):
        if not _HAS_FASTAPI:
            raise ImportError(
                "FastAPI not installed. Run: pip install fastapi uvicorn[standard]"
            )

        app = FastAPI(
            title="TERMINUS — OptionChain AlgoTrader",
            description="Role-based real-time multi-tenant options trading monitor",
            version="2.0.0",
            docs_url="/api/docs",
        )
        _srv   = self
        bridge = self._ws_bridge

        # ── Auth helpers ──────────────────────────────────────────────────────

        _bearer = HTTPBearer(auto_error=False)

        async def _current_user(
            creds: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
        ) -> dict:
            if not creds:
                raise HTTPException(status_code=401, detail="Authorization required.")
            try:
                return verify_token(creds.credentials)
            except ValueError as exc:
                raise HTTPException(status_code=401, detail=str(exc))

        async def _require_admin(user: dict = Depends(_current_user)) -> dict:
            if user.get("role") != "admin":
                raise HTTPException(status_code=403, detail="Admin access required.")
            return user

        async def _require_client(user: dict = Depends(_current_user)) -> dict:
            if user.get("role") != "client":
                raise HTTPException(status_code=403, detail="Client portal access required.")
            return user

        # Use module-level schemas (defined at module scope so annotations resolve)
        TokenUpdateSchema    = _TokenUpdateSchema
        FeederConnectSchema  = _FeederConnectSchema
        BrokerProvisionSchema = _BrokerProvisionSchema

        # ── Serve dashboard HTML ──────────────────────────────────────────────

        @app.get("/", include_in_schema=False)
        async def index():
            if not os.path.exists(_MONITOR_HTML):
                return HTMLResponse(
                    "<h1>monitor.html not found</h1>"
                    "<p>Expected at: " + _MONITOR_HTML + "</p>",
                    status_code=503,
                )
            return FileResponse(_MONITOR_HTML, media_type="text/html")

        # ── PUBLIC — Authentication ───────────────────────────────────────────

        @app.post("/api/auth/login", tags=["Auth"])
        async def login(request: Request):
            try:
                raw = await request.json()
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid JSON body.")
            auth_cfg = _srv._cfg.auth
            role     = str(raw.get("role")     or "").strip()
            username = str(raw.get("username") or "").strip()
            password = str(raw.get("password") or "")

            if not role or not username or not password:
                raise HTTPException(
                    status_code=400,
                    detail="username and password are required.",
                )

            if role == "admin":
                if not (
                    hmac.compare_digest(username, auth_cfg.admin_username)
                    and hmac.compare_digest(password, auth_cfg.admin_password)
                ):
                    raise HTTPException(status_code=401, detail="Invalid admin credentials.")
                token = create_token(username, "admin")
                return {"access_token": token, "token_type": "bearer", "role": "admin"}

            elif role == "client":
                client = _srv._registry.get(username) if _srv._registry else None
                if client is None or not client.active:
                    raise HTTPException(status_code=401, detail="Client not found or inactive.")
                expected = auth_cfg.client_pin(username)
                if not hmac.compare_digest(password, expected):
                    raise HTTPException(status_code=401, detail="Invalid client PIN.")
                token = create_token(username, "client", username)
                return {
                    "access_token": token,
                    "token_type": "bearer",
                    "role": "client",
                    "client_id": username,
                    "client_name": getattr(client, "name", username),
                }

            raise HTTPException(status_code=400, detail="role must be 'admin' or 'client'.")

        # ── ADMIN — read-only ─────────────────────────────────────────────────

        @app.get("/api/status", tags=["Admin"])
        async def api_status(_: dict = Depends(_require_admin)):
            return {"ts": datetime.now(IST).isoformat(), "clients": _srv._client_summary()}

        @app.get("/api/workers", tags=["Admin"])
        async def api_workers(_: dict = Depends(_require_admin)):
            stats = _srv._router.worker_stats() if _srv._router else []
            return {"workers": stats}

        @app.get("/api/broker_status", tags=["Admin"])
        async def api_broker_status(_: dict = Depends(_require_admin)):
            return {"ts": datetime.now(IST).isoformat(), "brokers": _srv._broker_summary()}

        # ── ADMIN — client lifecycle management ───────────────────────────────

        @app.post("/api/halt/{client_id}", tags=["Admin"])
        async def api_halt(client_id: str, _: dict = Depends(_require_admin)):
            client = _srv._registry.get(client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {client_id!r} not found.")
            client.halt()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_HALTED", "client_id": client_id, "reason": "web_dashboard",
            })
            logger.info("Dashboard: halted client %s.", client_id)
            return {"ok": True, "message": f"Client {client_id} halted."}

        @app.post("/api/resume/{client_id}", tags=["Admin"])
        async def api_resume(client_id: str, _: dict = Depends(_require_admin)):
            client = _srv._registry.get(client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {client_id!r} not found.")
            client.resume()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_RESUMED", "client_id": client_id,
            })
            return {"ok": True, "message": f"Client {client_id} resumed."}

        @app.post("/api/halt_all", tags=["Admin"])
        async def api_halt_all(_: dict = Depends(_require_admin)):
            if _srv._registry:
                _srv._registry.halt_all()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {"event": "ALL_HALTED"})
            return {"ok": True, "message": "All clients halted."}

        @app.post("/api/kill_switch", tags=["Admin"])
        async def api_kill_switch(_: dict = Depends(_require_admin)):
            if _srv._registry:
                _srv._registry.halt_all()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event":   SysEvent.KILL_SWITCH,
                "message": "Emergency kill switch activated via web dashboard.",
                "ts":      datetime.now(IST).isoformat(),
            })
            logger.critical("Dashboard: KILL SWITCH activated.")
            return {"ok": True, "message": "KILL SWITCH activated — all clients halted immediately."}

        @app.post("/api/rebalance/{underlying}", tags=["Admin"])
        async def api_rebalance(underlying: str, _: dict = Depends(_require_admin)):
            underlying = underlying.upper()
            if _srv._rebalancer is None:
                raise HTTPException(503, "StrikeRebalancer not wired to dashboard.")
            state = _srv._rebalancer._state.get(underlying)
            if state is None:
                raise HTTPException(404, f"Unknown underlying: {underlying!r}")
            state.current_atm = None
            state.open_atm = None
            logger.info("Dashboard: rebalance triggered for %s.", underlying)
            return {"ok": True, "message": f"Rebalance triggered for {underlying}."}

        @app.post("/api/set_lots/{client_id}/{binding_id}/{multiplier}", tags=["Admin"])
        async def api_set_lots(
            client_id: str, binding_id: str, multiplier: float,
            _: dict = Depends(_require_admin),
        ):
            client = _srv._registry.get(client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {client_id!r} not found.")
            for b in client.broker_bindings:
                if b.binding_id == binding_id:
                    b.lot_multiplier = multiplier
                    return {"ok": True, "message": f"{client_id}/{binding_id} lot_multiplier → {multiplier}"}
            raise HTTPException(404, f"Binding {binding_id!r} not found for {client_id!r}.")

        @app.post("/api/client/update_token", tags=["Admin"])
        async def api_update_token(
            body: TokenUpdateSchema, _: dict = Depends(_require_admin),
        ):
            client = _srv._registry.get(body.client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {body.client_id!r} not found.")
            binding = next(
                (b for b in client.broker_bindings if b.binding_id == body.binding_id), None
            )
            if binding is None:
                raise HTTPException(404, f"Binding {body.binding_id!r} not found.")
            creds = {
                k: v for k, v in body.model_dump().items()
                if k not in ("client_id", "binding_id") and v is not None
            }
            if creds and _srv._registry:
                _srv._registry.inject_credentials(body.client_id, body.binding_id, **creds)
            broker = None
            if _srv._router:
                broker = (_srv._router._brokers.get(body.client_id) or {}).get(body.binding_id)
            if broker is None:
                raise HTTPException(503, "Broker worker not found — router not started.")
            ok = await broker.authenticate()
            event = "AUTH_SUCCESS" if ok else "AUTH_FAILED"
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": event, "client_id": body.client_id, "binding_id": body.binding_id,
            })
            if ok:
                logger.info("Dashboard: re-authenticated %s/%s.", body.client_id, body.binding_id)
                return {"ok": True, "message": f"Authenticated {body.client_id}/{body.binding_id}."}
            logger.warning("Dashboard: auth FAILED for %s/%s.", body.client_id, body.binding_id)
            raise HTTPException(502, f"Authentication failed for {body.client_id}/{body.binding_id}.")

        # ── ADMIN — feeder management ─────────────────────────────────────────

        @app.get("/api/admin/feeder/status", tags=["Admin"])
        async def api_feeder_status(_: dict = Depends(_require_admin)):
            feeder   = _srv._feeder
            provider = (
                feeder.active_provider
                if feeder and hasattr(feeder, "active_provider")
                else _srv._cfg.primary_feeder_provider
            )
            connected = feeder.is_running if feeder else False
            dual_lat  = feeder.dual_latency if feeder and hasattr(feeder, "dual_latency") else {}
            return {
                "ts":                datetime.now(IST).isoformat(),
                "provider":          provider,
                "connected":         connected,
                "upstox_latency_ms": round(dual_lat.get("upstox", 0.0), 3),
                "fyers_latency_ms":  round(dual_lat.get("fyers",  0.0), 3),
            }

        @app.post("/api/admin/feeder/connect", tags=["Admin"])
        async def api_feeder_connect(
            body: FeederConnectSchema, _: dict = Depends(_require_admin),
        ):
            feeder = _srv._feeder
            if feeder is None:
                raise HTTPException(503, "GlobalFeeder not wired to dashboard.")

            # Validate provider is known
            from data_layer.global_feeder import _FEEDER_REGISTRY
            if body.provider not in _FEEDER_REGISTRY:
                raise HTTPException(
                    400,
                    f"Unknown provider '{body.provider}'. Available: {list(_FEEDER_REGISTRY)}",
                )

            logger.info("Dashboard: feeder reconnect requested → provider=%s.", body.provider)
            old_provider = _srv._cfg.primary_feeder_provider

            # Stop current feeder, update provider, restart
            try:
                await feeder.stop()
            except Exception as exc:
                logger.warning("Dashboard: feeder stop raised: %s", exc)

            _srv._cfg.primary_feeder_provider = body.provider  # type: ignore[misc]

            # Inject optional credentials into the feeder registry or cfg before start
            # (Real broker feeders read these from their BrokerBinding or cfg at connect time)
            if body.access_token:
                _srv._cfg.__dict__.setdefault("_feeder_creds", {})
                _srv._cfg.__dict__["_feeder_creds"]["access_token"] = body.access_token
            if body.user_id:
                _srv._cfg.__dict__.setdefault("_feeder_creds", {})
                _srv._cfg.__dict__["_feeder_creds"]["user_id"] = body.user_id
            if body.api_key:
                _srv._cfg.__dict__.setdefault("_feeder_creds", {})
                _srv._cfg.__dict__["_feeder_creds"]["api_key"] = body.api_key

            try:
                await feeder.start()
                await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                    "event":   SysEvent.FEEDER_RESTORED,
                    "message": f"Feeder switched to '{body.provider}' via dashboard.",
                })
                logger.info("Dashboard: feeder started with provider=%s.", body.provider)
                return {"ok": True, "message": f"Feeder connected via '{body.provider}'."}
            except Exception as exc:
                logger.error("Dashboard: feeder connect failed: %s", exc)
                _srv._cfg.primary_feeder_provider = old_provider  # type: ignore[misc]
                raise HTTPException(502, f"Feeder connect failed: {exc}")

        # ── CLIENT — own portfolio ────────────────────────────────────────────

        @app.get("/api/client/me", tags=["Client"])
        async def api_client_me(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            client = _srv._registry.get(cid) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {cid!r} not found.")
            brokers_status = [
                b for b in _srv._broker_summary() if b["client_id"] == cid
            ]
            return {
                "ts":        datetime.now(IST).isoformat(),
                "client":    _build_client_dict(client),
                "brokers":   brokers_status,
            }

        @app.get("/api/client/brokers", tags=["Client"])
        async def api_client_brokers(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            return {
                "ts":      datetime.now(IST).isoformat(),
                "brokers": [b for b in _srv._broker_summary() if b["client_id"] == cid],
            }

        # ── CLIENT — live broker provisioning ────────────────────────────────

        @app.post("/api/client/register_broker", tags=["Client"])
        async def client_register_broker(
            payload: BrokerProvisionSchema,
            user: dict = Depends(_require_client),
        ):
            from config.client_profiles import BrokerBinding
            from execution_bridge.base_broker import create_broker

            cid = user.get("client_id", "")
            client = _srv._registry.get(cid) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {cid!r} not found.")

            # 1. Build the binding
            binding = BrokerBinding(
                binding_id=payload.binding_id,
                provider=payload.provider,      # type: ignore[arg-type]
                label=payload.label,
                user_id=payload.user_id,
                password=payload.password,
                api_key=payload.api_key,
                api_secret=payload.api_secret,
                totp_secret=payload.totp_secret,
                vendor_code=payload.vendor_code,
                imei=payload.imei,
                client_code=payload.client_code,
                access_token=payload.access_token,
                lot_multiplier=payload.lot_multiplier,
            )

            # 2. Register binding in registry (raises ValueError if duplicate)
            try:
                _srv._registry.add_broker_binding(cid, binding)
            except ValueError as exc:
                raise HTTPException(409, str(exc))

            # 3. Create broker instance and authenticate
            try:
                broker = create_broker(binding, cid)
            except ValueError as exc:
                raise HTTPException(400, str(exc))

            ok = await broker.authenticate()
            if not ok:
                # Remove the binding if auth fails so state stays consistent
                client.broker_bindings = [
                    b for b in client.broker_bindings if b.binding_id != payload.binding_id
                ]
                raise HTTPException(
                    502,
                    f"Authentication failed for provider '{payload.provider}'. "
                    "Check credentials and try again.",
                )

            # 4. Inject into router broker map
            if _srv._router:
                _srv._router._brokers.setdefault(cid, {})[payload.binding_id] = broker

            # 5. Inject into existing worker OR spawn a new isolated worker
            if _srv._router:
                updated = _srv._router._pool.add_broker_to_worker(
                    cid, payload.binding_id, broker, payload.provider
                )
                if not updated:
                    # First broker for this client — create a dedicated worker task
                    from execution_bridge.parallel_worker_pool import ClientExecutionWorker
                    worker = ClientExecutionWorker(
                        client=client,
                        brokers=_srv._router._brokers[cid],
                        bus=_srv._bus,
                        cfg=_srv._cfg,
                    )
                    await _srv._router._pool.add_worker(worker)
                    logger.info("Dashboard: spawned new execution worker for client %s.", cid)

            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event":      "BROKER_PROVISIONED",
                "client_id":  cid,
                "binding_id": payload.binding_id,
                "provider":   payload.provider,
            })
            logger.info("Dashboard: provisioned broker %s/%s (%s).",
                        cid, payload.binding_id, payload.provider)
            return {
                "ok":      True,
                "message": (
                    f"Broker '{payload.binding_id}' ({payload.provider}) "
                    f"authenticated and registered for client {cid}."
                ),
            }

        # ── ADMIN — single-provider feeder connects ──────────────────────────

        @app.post("/api/admin/feeder/connect/upstox", tags=["Admin"])
        async def api_feeder_connect_upstox(
            request: Request, _: dict = Depends(_require_admin),
        ):
            feeder = _srv._feeder
            if feeder is None:
                raise HTTPException(503, "GlobalFeeder not wired to dashboard.")
            try:
                raw = await request.json()
            except Exception:
                raise HTTPException(400, "Invalid JSON body.")
            creds = {
                "client_id":    raw.get("client_id", ""),
                "api_key":      raw.get("api_key", ""),
                "secret":       raw.get("secret", ""),
                "access_token": raw.get("access_token", ""),
                "totp":         raw.get("totp", ""),
            }
            try:
                await feeder.start_single("upstox", creds)
            except Exception as exc:
                logger.error("Dashboard: upstox connect failed: %s", exc)
                raise HTTPException(502, f"Upstox connect failed: {exc}")
            return {"ok": True, "message": "Upstox feed stream initialized.", "provider": "upstox"}

        @app.post("/api/admin/feeder/connect/fyers", tags=["Admin"])
        async def api_feeder_connect_fyers(
            request: Request, _: dict = Depends(_require_admin),
        ):
            feeder = _srv._feeder
            if feeder is None:
                raise HTTPException(503, "GlobalFeeder not wired to dashboard.")
            try:
                raw = await request.json()
            except Exception:
                raise HTTPException(400, "Invalid JSON body.")
            creds = {
                "client_id":    raw.get("client_id", ""),
                "app_key":      raw.get("app_key", ""),
                "access_token": raw.get("access_token", ""),
                "totp":         raw.get("totp", ""),
            }
            try:
                await feeder.start_single("fyers", creds)
            except Exception as exc:
                logger.error("Dashboard: fyers connect failed: %s", exc)
                raise HTTPException(502, f"Fyers connect failed: {exc}")
            return {"ok": True, "message": "Fyers feed stream initialized.", "provider": "fyers"}

        # ── ADMIN — dual active-active feeder ────────────────────────────────

        @app.post("/api/admin/feeder/connect_dual", tags=["Admin"])
        async def api_feeder_connect_dual(
            request: Request, _: dict = Depends(_require_admin),
        ):
            feeder = _srv._feeder
            if feeder is None:
                raise HTTPException(503, "GlobalFeeder not wired to dashboard.")
            try:
                raw = await request.json()
            except Exception:
                raise HTTPException(400, "Invalid JSON body.")
            upstox_creds = {
                "client_id":    raw.get("upstox_client_id", ""),
                "api_key":      raw.get("upstox_api_key", ""),
                "secret":       raw.get("upstox_secret", ""),
                "access_token": raw.get("upstox_access_token", ""),
                "totp":         raw.get("upstox_totp", ""),
            }
            fyers_creds = {
                "client_id":    raw.get("fyers_client_id", ""),
                "app_key":      raw.get("fyers_app_key", ""),
                "access_token": raw.get("fyers_access_token", ""),
                "totp":         raw.get("fyers_totp", ""),
            }
            try:
                await feeder.start_dual(upstox_creds, fyers_creds)
            except Exception as exc:
                logger.error("Dashboard: start_dual failed: %s", exc)
                raise HTTPException(502, f"Dual feeder connect failed: {exc}")
            return {"ok": True, "message": "Dual active-active feed established.", "provider": "dual"}

        # ── ADMIN — RMS config ────────────────────────────────────────────────

        @app.get("/api/admin/rms", tags=["Admin"])
        async def api_rms_get(_: dict = Depends(_require_admin)):
            defaults = {
                "max_drawdown_pct":       5.0,
                "order_throttle_per_sec": 5,
                "squareoff_time":         "15:15",
                "distance_filter_pct":    5.0,
            }
            rms = getattr(_srv._cfg, "rms", {})
            return {**defaults, **rms}

        @app.post("/api/admin/rms", tags=["Admin"])
        async def api_rms_post(request: Request, _: dict = Depends(_require_admin)):
            try:
                raw = await request.json()
            except Exception:
                raise HTTPException(400, "Invalid JSON body.")
            if not hasattr(_srv._cfg, "rms") or not isinstance(_srv._cfg.rms, dict):
                _srv._cfg.rms = {}
            _srv._cfg.rms.update(raw)
            if "squareoff_time" in raw and hasattr(_srv._cfg, "squareoff_time"):
                _srv._cfg.squareoff_time = raw["squareoff_time"]
            logger.info("Dashboard: RMS config updated: %s", raw)
            return {"ok": True}

        # ── ADMIN — force liquidate all positions ─────────────────────────────

        @app.post("/api/admin/force_liquidate", tags=["Admin"])
        async def api_force_liquidate(_: dict = Depends(_require_admin)):
            n = 0
            if _srv._registry:
                for client in _srv._registry.all_active():
                    try:
                        client.halt()
                        n += 1
                    except Exception as exc:
                        logger.warning(
                            "Dashboard: force_liquidate: halt(%s) raised: %s",
                            getattr(client, "client_id", "?"), exc,
                        )
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event":   SysEvent.KILL_SWITCH,
                "message": "Force liquidation triggered via dashboard.",
                "ts":      datetime.now(IST).isoformat(),
            })
            logger.critical("Dashboard: FORCE LIQUIDATE — halted %d clients.", n)
            return {"ok": True, "halted": n}

        # ── ADMIN — checkpoint ────────────────────────────────────────────────

        @app.post("/api/admin/checkpoint", tags=["Admin"])
        async def api_checkpoint(_: dict = Depends(_require_admin)):
            now_ist = datetime.now(IST).isoformat()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CHECKPOINT_REQUESTED",
                "ts":    now_ist,
            })
            logger.info("Dashboard: checkpoint requested at %s.", now_ist)
            return {"ok": True, "ts": now_ist}

        # ── ADMIN — warm boot ─────────────────────────────────────────────────

        @app.post("/api/admin/warm_boot", tags=["Admin"])
        async def api_warm_boot(_: dict = Depends(_require_admin)):
            now_ist = datetime.now(IST).isoformat()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "WARM_BOOT_REQUESTED",
                "ts":    now_ist,
            })
            logger.info("Dashboard: warm boot requested at %s.", now_ist)
            return {"ok": True, "ts": now_ist}

        # ── ADMIN — client profile management ────────────────────────────────

        @app.get("/api/admin/clients", tags=["Admin"])
        async def api_clients_list(_: dict = Depends(_require_admin)):
            if _srv._registry is None:
                return {"clients": [], "ts": datetime.now(IST).isoformat()}
            clients = []
            for c in _srv._registry._clients.values():
                d = _build_client_dict(c)
                d["halted"]         = bool(getattr(c, "_halted", False))
                d["lot_multiplier"] = float(c.risk.size_multiplier)
                d["broker_bindings"] = [
                    {"binding_id": b.binding_id, "provider": b.provider, "enabled": b.enabled}
                    for b in c.broker_bindings
                ]
                clients.append(d)
            return {"clients": clients, "ts": datetime.now(IST).isoformat()}

        @app.post("/api/admin/clients/register", tags=["Admin"])
        async def api_clients_register(
            body: _ClientRegisterSchema, _: dict = Depends(_require_admin),
        ):
            from config.client_profiles import ClientProfile, RiskProfile, BrokerBinding

            if _srv._registry is None:
                raise HTTPException(503, "ClientRegistry not available.")
            risk = RiskProfile(
                capital=body.capital,
                max_risk_per_trade_pct=body.max_risk_pct,
                max_daily_loss_pct=body.max_daily_loss_pct,
                size_multiplier=body.lot_multiplier,
            )
            try:
                risk.validate()
            except AssertionError as exc:
                raise HTTPException(400, str(exc))

            profile = ClientProfile(
                client_id=body.client_id,
                name=body.name,
                risk=risk,
                enabled_strategies=body.strategies or ["A", "B", "C"],
            )
            try:
                _srv._registry.register(profile)
            except ValueError as exc:
                raise HTTPException(409, str(exc))

            if body.binding_id:
                binding = BrokerBinding(
                    binding_id=body.binding_id,
                    provider=body.provider or "mock",
                    lot_multiplier=body.lot_multiplier,
                )
                profile.broker_bindings.append(binding)
                if _srv._router:
                    try:
                        from execution_bridge.base_broker import create_broker
                        from execution_bridge.parallel_worker_pool import ClientExecutionWorker
                        broker = create_broker(binding, body.client_id)
                        _srv._router._brokers.setdefault(body.client_id, {})[body.binding_id] = broker
                        worker = ClientExecutionWorker(
                            client=profile,
                            brokers=_srv._router._brokers[body.client_id],
                            bus=_srv._bus,
                            cfg=_srv._cfg,
                        )
                        await _srv._router._pool.add_worker(worker)
                        logger.info("Dashboard: spawned worker for new client %s.", body.client_id)
                    except Exception as exc:
                        logger.warning("Dashboard: worker spawn for %s failed: %s", body.client_id, exc)

            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event":     "CLIENT_REGISTERED",
                "client_id": body.client_id,
            })
            logger.info("Dashboard: registered client %s.", body.client_id)
            return {"ok": True, "message": f"Client '{body.client_id}' registered and live."}

        @app.post("/api/admin/clients/{client_id}/reauth", tags=["Admin"])
        async def api_clients_reauth(
            client_id: str, _: dict = Depends(_require_admin),
        ):
            client = _srv._registry.get(client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {client_id!r} not found.")
            if _srv._router is None:
                raise HTTPException(503, "ExecutionRouter not wired to dashboard.")
            client_brokers = _srv._router._brokers.get(client_id) or {}
            if not client_brokers:
                raise HTTPException(404, f"No broker workers found for client {client_id!r}.")
            results = []
            for binding_id, broker in client_brokers.items():
                ok = await broker.authenticate()
                results.append({"binding_id": binding_id, "ok": ok})
                event = "AUTH_SUCCESS" if ok else "AUTH_FAILED"
                await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                    "event": event, "client_id": client_id, "binding_id": binding_id,
                })
            all_ok = all(r["ok"] for r in results)
            logger.info("Dashboard: reauth %s — %s.", client_id, "OK" if all_ok else "PARTIAL_FAIL")
            return {"ok": all_ok, "results": results}

        # ── ADMIN — IV matrix ─────────────────────────────────────────────────

        @app.get("/api/admin/iv_matrix", tags=["Admin"])
        async def api_iv_matrix(_: dict = Depends(_require_admin)):
            now_ist = datetime.now(IST).isoformat()
            option_cache = (
                getattr(_srv._ws_bridge, "_option_cache", None)
                or getattr(_srv._ws_bridge, "option_snapshot", None)
            )
            if option_cache:
                return {"strikes": list(option_cache.values()), "ts": now_ist}
            return {"strikes": [], "ts": now_ist}

        # ── ADMIN — audit log (JSON) ──────────────────────────────────────────

        @app.get("/api/admin/audit_log", tags=["Admin"])
        async def api_audit_log(_: dict = Depends(_require_admin)):
            import glob as _glob
            now_ist = datetime.now(IST).isoformat()
            log_dir = getattr(getattr(_srv._cfg, "storage", None), "log_dir", None) or "logs"
            try:
                files = sorted(_glob.glob(os.path.join(log_dir, "*.log")), key=os.path.getmtime)
                if not files:
                    files = sorted(_glob.glob(os.path.join(log_dir, "*.txt")), key=os.path.getmtime)
                if not files:
                    return {"lines": [], "ts": now_ist}
                with open(files[-1], "r", encoding="utf-8", errors="replace") as fh:
                    lines = [ln.rstrip("\n") for ln in fh.readlines()[-200:]]
                return {"lines": lines, "ts": now_ist}
            except Exception as exc:
                logger.warning("Dashboard: audit_log read failed: %s", exc)
                return {"lines": [], "ts": now_ist, "error": str(exc)}

        # ── ADMIN — audit log (CSV download) ─────────────────────────────────

        @app.get("/api/admin/audit_log/csv", tags=["Admin"])
        async def api_audit_log_csv(
            token: str = Query(default=""),
            _: dict = Depends(_require_admin),
        ):
            import glob as _glob
            log_dir = getattr(getattr(_srv._cfg, "storage", None), "log_dir", None) or "logs"
            try:
                files = sorted(_glob.glob(os.path.join(log_dir, "*.log")), key=os.path.getmtime)
                if not files:
                    files = sorted(_glob.glob(os.path.join(log_dir, "*.txt")), key=os.path.getmtime)
                if not files:
                    raise HTTPException(404, "No log files found.")
                return FileResponse(files[-1], media_type="text/csv", filename="audit_log.csv")
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(500, f"Log file read failed: {exc}")

        # ── ANY AUTH — telemetry ──────────────────────────────────────────────

        @app.get("/api/admin/telemetry", tags=["Admin"])
        async def api_telemetry(_: dict = Depends(_current_user)):
            feeder   = _srv._feeder
            dual_lat = feeder.dual_latency if feeder and hasattr(feeder, "dual_latency") else {}
            return {
                "upstox_latency_ms": round(dual_lat.get("upstox", 0.0), 3),
                "fyers_latency_ms":  round(dual_lat.get("fyers",  0.0), 3),
                "ws_clients":        len(bridge._connections),
            }

        # ── WebSocket endpoint ────────────────────────────────────────────────

        @app.websocket("/ws")
        async def websocket_endpoint(
            websocket: WebSocket,
            token: str = Query(default=""),
        ):
            if not token:
                await websocket.close(code=1008, reason="auth required")
                return
            try:
                verify_token(token)
            except ValueError as exc:
                await websocket.close(code=1008, reason=str(exc))
                return

            await websocket.accept()
            bridge.add_connection(websocket)
            try:
                while True:
                    await websocket.receive_text()
            except (WebSocketDisconnect, Exception):
                pass
            finally:
                bridge.remove_connection(websocket)

        return app

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def serve(self, host: str = "0.0.0.0", port: int = 8080) -> None:
        try:
            import uvicorn
        except ImportError:
            logger.error(
                "uvicorn not installed — dashboard will not start. "
                "Install with: pip install uvicorn[standard]"
            )
            return

        # Pre-flight port check — avoids confusing raw uvicorn error on Windows
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _s:
            _s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                _s.bind((host if host != "0.0.0.0" else "127.0.0.1", port))
            except OSError:
                logger.error(
                    "Dashboard: port %d is already in use — "
                    "stop the previous instance or choose a different --port.  "
                    "System continues without dashboard.",
                    port,
                )
                return

        config = uvicorn.Config(
            app=self._app,
            host=host,
            port=port,
            log_level="warning",
            loop="none",
            lifespan="off",
        )
        self._uvicorn_server = uvicorn.Server(config)
        self._uvicorn_server.install_signal_handlers = lambda: None

        logger.info("Dashboard: http://%s:%d  (WebSocket: ws://%s:%d/ws)", host, port, host, port)
        try:
            await asyncio.gather(
                self._uvicorn_server.serve(),
                self._ws_bridge.run(),
            )
        except asyncio.CancelledError:
            pass

    def stop(self) -> None:
        self._ws_bridge.stop()
        if self._uvicorn_server is not None:
            self._uvicorn_server.should_exit = True

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _client_summary(self) -> List[dict]:
        if self._registry is None:
            return []
        result = []
        for c in self._registry.all_active():
            result.append(_build_client_dict(c))
        return result

    def _broker_summary(self) -> List[dict]:
        if self._registry is None:
            return []
        result = []
        for c in self._registry.all_active():
            cid = c.client_id
            for binding in c.broker_bindings:
                broker = (
                    (self._router._brokers.get(cid) or {}).get(binding.binding_id)
                    if self._router else None
                )
                result.append({
                    "client_id":        cid,
                    "client_name":      getattr(c, "name", cid),
                    "binding_id":       binding.binding_id,
                    "provider":         binding.provider,
                    "label":            binding.label,
                    "api_key_set":      bool(binding.api_key),
                    "is_authenticated": broker.is_authenticated if broker else False,
                    "enabled":          binding.enabled,
                })
        return result


# ── Module-level helper (used inside route closures) ─────────────────────────

def _build_client_dict(c) -> dict:
    return {
        "client_id":      c.client_id,
        "name":           getattr(c, "name", c.client_id),
        "tradeable":      c.is_tradeable(),
        "daily_pnl":      round(float(getattr(c, "_daily_pnl", 0.0)), 2),
        "capital":        float(c.risk.capital),
        "max_risk_pct":   float(c.risk.max_risk_per_trade_pct),
        "daily_loss_pct": float(c.risk.max_daily_loss_pct),
        "strategies":     list(c.enabled_strategies),
        "brokers":        [b.provider for b in c.broker_bindings if b.enabled],
    }
