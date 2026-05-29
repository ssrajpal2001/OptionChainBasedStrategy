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
    GET  /api/admin/feeder/auth-url?provider={p} — generate feeder OAuth login URL
    POST /api/admin/feeder/save-creds           — save feeder api_key + secret (no password/totp)

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
from typing import Dict, List, Optional

from config.global_config import IST, Topic, SysEvent
from data_layer.base_feeder import EventBus
from ui_layer.auth import create_token, verify_token
from ui_layer.ws_bridge import WsBridge

logger = logging.getLogger(__name__)

from broker_auth.headless_auth import _ist_eod


def _base_url(request) -> str:
    """Return the public base URL, respecting X-Forwarded-Proto from nginx."""
    scheme = request.headers.get("x-forwarded-proto") or request.url.scheme
    host   = request.headers.get("x-forwarded-host") or request.url.netloc
    host   = host.split(":")[0] if request.headers.get("x-forwarded-proto") else host
    return f"{scheme}://{host}"


def _callback_page(status: str, provider: str, message: str) -> str:
    """Return a minimal HTML page shown after broker OAuth redirect."""
    color   = "#22c55e" if status == "success" else "#ef4444"
    icon    = "✓" if status == "success" else "✗"
    title   = "Connected!" if status == "success" else "Authentication Failed"
    script  = "setTimeout(()=>window.close(),3000);" if status == "success" else ""
    return f"""<!DOCTYPE html><html><head><title>TERMINUS — {provider.upper()}</title>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{{font-family:monospace;background:#0a0a0f;color:#e2e8f0;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
.box{{text-align:center;padding:40px;border:1px solid {color}33;border-radius:12px;
background:{color}0a;max-width:400px}}
.icon{{font-size:48px;color:{color};margin-bottom:16px}}
h2{{color:{color};margin:0 0 12px}}
p{{color:#94a3b8;margin:8px 0}}
small{{color:#475569}}
</style></head><body>
<div class="box">
<div class="icon">{icon}</div>
<h2>{title}</h2>
<p>{provider.upper()} broker</p>
<p style="color:{color}">{message}</p>
<small>{"This tab will close automatically in 3 seconds." if status == "success" else "Please close this tab and try again."}</small>
</div>
<script>{script}</script>
</body></html>"""


def _upstox_translate_error(raw: str) -> str:
    """Map known Upstox error codes to actionable UI messages."""
    if "UDAPI100060" in raw:
        return (
            "Upstox Resource Not Found (UDAPI100060). Please check your settings: "
            "(1) Confirm your API Key is correct and has no hidden spaces or dashes mixed up, "
            "(2) Ensure the Redirect URI in your Upstox Developer Console is set EXACTLY to https://www.google.com, "
            "(3) Confirm the Mobile number field contains your 10-digit registered number (NOT your Client ID)."
        )
    return raw

# FastAPI imports at module level so 'from __future__ import annotations' doesn't
# prevent FastAPI from resolving type hints on route functions (lazy-string annotations
# are resolved against module globals, so locally-imported types are invisible).
try:
    from fastapi import (
        FastAPI, WebSocket, WebSocketDisconnect,
        HTTPException, Depends, Query, Body, Request,
    )
    from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
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

    class _FeederConnectSchema(_PydanticBase):
        provider: str

    class _SaveFeederCredsSchema(_PydanticBase):
        provider:  str
        client_id: str = ""
        api_key:   str = ""
        secret:    str = ""

    class _BrokerProvisionSchema(_PydanticBase):
        binding_id:     str
        provider:       str
        label:          str = ""
        user_id:        str = ""
        api_key:        str = ""
        api_secret:     str = ""
        access_token:   str = ""
        lot_multiplier: float = 1.0

    # _DualFeederSchema removed — replaced by _SaveFeederCredsSchema per-provider

    class _RmsConfigSchema(_PydanticBase):
        max_drawdown_pct:       float = 5.0
        order_throttle_per_sec: int   = 5
        squareoff_time:         str   = "15:15"
        distance_filter_pct:    float = 5.0

    class _StrategyConfigSchema(_PydanticBase):
        # Global RMS
        rms_max_drawdown_pct:       float = 5.0
        rms_order_throttle:         int   = 5
        rms_squareoff_time:         str   = "15:15"
        rms_distance_filter_pct:    float = 5.0
        # Indicator periods
        ind_rsi_period:    int   = 14
        ind_vwap_window:   int   = 500
        ind_adx_period:    int   = 20
        ind_ema_fast:      int   = 9
        ind_ema_slow:      int   = 21
        ind_htf_minutes:   int   = 75
        ind_ltf_minutes:   int   = 5
        # Iron Condor
        ic_squareoff_time: str   = "15:15"
        ic_rsi_min:        float = 40.0
        ic_rsi_max:        float = 60.0
        ic_adx_max:        float = 25.0
        ic_profit_pct:     float = 50.0
        ic_sl_pct:         float = 200.0
        ic_nifty_otm:      float = 200.0
        ic_nifty_wing:     float = 200.0
        ic_banknifty_otm:  float = 400.0
        ic_banknifty_wing: float = 500.0
        ic_finnifty_otm:   float = 200.0
        ic_finnifty_wing:  float = 200.0
        ic_sensex_otm:     float = 500.0
        ic_sensex_wing:    float = 500.0
        ic_midcp_otm:      float = 150.0
        ic_midcp_wing:     float = 200.0
        # Sell Straddle
        ss_entry_start:     str   = "09:20"
        ss_entry_end:       str   = "12:00"
        ss_squareoff_time:  str   = "15:15"
        ss_rsi_min:         float = 35.0
        ss_rsi_max:         float = 65.0
        ss_adx_max:         float = 30.0
        ss_profit_pct:      float = 30.0
        ss_sl_pct:          float = 200.0
        ss_trail_lock_pct:  float = 20.0
        ss_trail_floor_pct: float = 10.0
        ss_max_trades:      int   = 1
        ss_roc_limit_pct:   float = 1.5
        # Trap Trading
        tt_htf_minutes:          int   = 75
        tt_ltf_minutes:          int   = 5
        tt_adx_threshold:        float = 20.0
        tt_volume_spike_mult:    float = 1.5
        tt_swing_lookback:       int   = 5
        tt_zone_tol_pct:         float = 0.5
        tt_void_atr_mult:        float = 2.0

    class _KillAllConfirmSchema(_PydanticBase):
        confirm: bool = False   # must be True to proceed

    class _SaveUpstoxCredsSchema(_PydanticBase):
        client_id: str = ""
        api_key:   str = ""
        secret:    str = ""

    class _SaveFyersCredsSchema(_PydanticBase):
        client_id: str = ""
        app_key:   str = ""
        secret:    str = ""

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

    # Phase 7 — multi-tenant lifecycle schemas
    class _ClientSelfRegisterSchema(_PydanticBase):
        client_id:          str
        name:               str   = ""
        email:              str   = ""
        password:           str
        capital:            float = 500_000.0
        max_risk_pct:       float = 1.0
        max_daily_loss_pct: float = 3.0

    class _ApproveClientSchema(_PydanticBase):
        strategy_assignments: Dict[str, str] = {}  # binding_id -> "A"/"B"/"C"

    class _AddPortalBrokerSchema(_PydanticBase):
        binding_id:          str
        provider:            str
        label:               str   = ""
        user_id:             str   = ""
        api_key:             str   = ""
        api_secret:          str   = ""
        lot_multiplier:      float = 1.0
        trading_mode:        str   = "paper"
        assigned_strategy:   str   = ""
        assigned_instrument: str   = "NIFTY"

    class _BrokerModeSchema(_PydanticBase):
        mode: str  # "paper" | "live"

    class _FyersAuthCodeSchema(_PydanticBase):
        auth_code: str

    class _SetIndexSchema(_PydanticBase):
        index: str  # NIFTY / BANKNIFTY / FINNIFTY

    class _StrategySelectionItem(_PydanticBase):
        strategy: str   # sell_straddle | iron_condor | trap_trading
        instrument: str # NIFTY | BANKNIFTY | FINNIFTY | SENSEX | MIDCPNIFTY

    class _StrategySelectionsSchema(_PydanticBase):
        selections: list  # List[_StrategySelectionItem]

    class _DeploymentSchema(_PydanticBase):
        binding_id:     str
        strategy_name:  str
        underlying:     str   = "NIFTY"
        lot_multiplier: float = 1.0
        max_profit_rs:  float = 0.0
        max_sl_rs:      float = 0.0
        squareoff_time: str   = "15:15"

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
        task = asyncio.create_task(dashboard.serve(host="0.0.0.0", port=5000))
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
        trap_engine=None, # TrapTradingEngine — optional, for strategy telemetry
        risk_manager=None, # RiskManager — optional, for firm risk summary + kill-all
        iron_condors=None,  # List[IronCondorStrategy]
        sell_straddles=None, # List[SellStraddleStrategy]
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._registry = registry
        self._router = router
        self._rebalancer = rebalancer
        self._feeder = feeder
        self._trap_engine = trap_engine
        self._risk_manager = risk_manager
        self._iron_condors: list = iron_condors or []
        self._sell_straddles: list = sell_straddles or []
        self._ws_bridge = WsBridge(bus, cfg=cfg)
        self._uvicorn_server = None

        from data_layer.client_db import ClientDB
        self._client_db = ClientDB()
        self._auth_alerts: dict = {"feeder": ""}

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
                # Try registry first (approved, active clients)
                client = _srv._registry.get(username) if _srv._registry else None
                if client is not None and client.active:
                    # Legacy clients use PIN; DB clients use password
                    pw_ok = _srv._client_db.verify_client_password(username, password)
                    if not pw_ok:
                        # Fall back to legacy PIN
                        expected = auth_cfg.client_pin(username)
                        if not hmac.compare_digest(password, expected):
                            raise HTTPException(status_code=401, detail="Invalid client credentials.")
                    token = create_token(username, "client", username)
                    return {
                        "access_token": token,
                        "token_type":   "bearer",
                        "role":         "client",
                        "client_id":    username,
                        "client_name":  getattr(client, "name", username),
                    }
                # Not in registry — check ClientDB (pending / newly registered)
                db_client = _srv._client_db.get_client_sync(username)
                if db_client is None:
                    raise HTTPException(status_code=401, detail="Client not found.")
                if not _srv._client_db.verify_client_password(username, password):
                    raise HTTPException(status_code=401, detail="Invalid client credentials.")
                token = create_token(username, "client", username)
                return {
                    "access_token": token,
                    "token_type":   "bearer",
                    "role":         "client",
                    "client_id":    username,
                    "client_name":  db_client.get("name", username),
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
            from broker_auth.headless_auth import _token_is_fresh
            feeder   = _srv._feeder
            provider = (
                feeder.active_provider
                if feeder and hasattr(feeder, "active_provider")
                else _srv._cfg.primary_feeder_provider
            )
            connected = feeder.is_running if feeder else False
            dual_lat  = feeder.dual_latency if feeder and hasattr(feeder, "dual_latency") else {}

            _ALL_PROVIDERS = ["upstox", "fyers", "zerodha", "dhan", "angelone", "aliceblue"]
            providers_status: dict = {}
            for p in _ALL_PROVIDERS:
                row = _srv._client_db.get_feeder_creds_sync(p) or {}
                creds_present = bool(row.get("client_id") or row.get("api_key") or row.get("access_token"))
                token_fresh   = _token_is_fresh(
                    row.get("token_generated_at", ""), row.get("token_expiry_at", "")
                ) if creds_present else False
                providers_status[p] = {
                    "creds_present": creds_present,
                    "token_fresh":   token_fresh,
                    "latency_ms":    round(dual_lat.get(p, 0.0), 3),
                }

            return {
                "ts":        datetime.now(IST).isoformat(),
                "provider":  provider,
                "connected": connected,
                # flat legacy keys for Upstox/Fyers (UI still reads these)
                "upstox_latency_ms":    providers_status["upstox"]["latency_ms"],
                "fyers_latency_ms":     providers_status["fyers"]["latency_ms"],
                "upstox_creds_present": providers_status["upstox"]["creds_present"],
                "fyers_creds_present":  providers_status["fyers"]["creds_present"],
                "upstox_token_fresh":   providers_status["upstox"]["token_fresh"],
                "fyers_token_fresh":    providers_status["fyers"]["token_fresh"],
                # full per-provider map
                "providers": providers_status,
            }

        @app.get("/api/admin/feeder/auth-url", tags=["Admin"])
        async def api_feeder_auth_url(
            provider: str,
            request:  Request,
            _: dict = Depends(_require_admin),
        ):
            """
            Generate the broker OAuth login URL for the admin data feeder.
            Admin clicks this, browser opens broker login page.
            After login, broker redirects to /callback/{provider} and token is stored automatically.
            Requires feeder credentials (api_key + secret) to be saved first via /save-creds.
            """
            import time as _time
            from broker_auth.oauth_manager import generate_auth_url, build_state
            t0 = _time.monotonic()

            db_row   = _srv._client_db.get_feeder_creds_sync(provider) or {}
            api_key  = db_row.get("api_key", "")
            secret   = db_row.get("secret", "")
            user_id  = db_row.get("client_id", "")

            if not api_key:
                return {
                    "ok": False,
                    "error": (
                        f"No API key saved for '{provider}' feeder. "
                        "Save credentials via /api/admin/feeder/save-creds first."
                    ),
                }

            base_url     = _base_url(request)
            callback_url = f"{base_url}/callback/{provider}"
            state        = build_state("admin", "feeder", provider)

            auth_ok, auth_url = await asyncio.to_thread(
                generate_auth_url, provider, api_key, secret, callback_url, state, user_id
            )
            elapsed = (_time.monotonic() - t0) * 1000

            if not auth_ok:
                logger.error("[Feeder] Auth URL generation failed for %s in %.1fms: %s",
                             provider, elapsed, auth_url)
                return {"ok": False, "error": auth_url}

            logger.info("[Feeder] %s auth URL ready in %.1fms", provider.upper(), elapsed)
            return {"ok": True, "provider": provider, "auth_url": auth_url}

        @app.post("/api/admin/feeder/save-creds", tags=["Admin"])
        async def api_feeder_save_creds(
            body: _SaveFeederCredsSchema, _: dict = Depends(_require_admin),
        ):
            """
            Save admin feeder credentials (client_id, api_key, secret only).
            No passwords, PINs, or TOTP secrets — authentication happens via broker portal.
            """
            import time as _time
            t0 = _time.monotonic()
            if not body.provider:
                raise HTTPException(400, "provider is required.")
            await _srv._client_db.upsert_feeder_creds(
                provider=body.provider,
                client_id=body.client_id,
                api_key=body.api_key,
                secret=body.secret,
            )
            elapsed = (_time.monotonic() - t0) * 1000
            logger.info("[Feeder] Credentials saved for %s in %.1fms", body.provider.upper(), elapsed)
            return {
                "ok": True,
                "message": (
                    f"Credentials saved for {body.provider.upper()} feeder. "
                    "Use the auth-url endpoint to complete OAuth login."
                ),
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

        # ── PUBLIC — self-registration ───────────────────────────────────────

        @app.post("/api/auth/register", tags=["Auth"])
        async def api_self_register(body: _ClientSelfRegisterSchema):
            existing = _srv._client_db.get_client_sync(body.client_id)
            if existing is not None:
                raise HTTPException(409, f"Client ID '{body.client_id}' already exists.")
            if len(body.password) < 6:
                raise HTTPException(400, "Password must be at least 6 characters.")
            await _srv._client_db.register_client(
                client_id=body.client_id,
                name=body.name,
                email=body.email,
                password=body.password,
                capital=body.capital,
                max_risk_pct=body.max_risk_pct,
                max_daily_loss_pct=body.max_daily_loss_pct,
            )
            logger.info("Dashboard: self-registered client %s (pending approval).", body.client_id)
            return {"ok": True, "message": "Registration submitted. Awaiting admin approval."}

        # ── CLIENT — lifecycle status ─────────────────────────────────────────

        @app.get("/api/client/status", tags=["Client"])
        async def api_client_status(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            # Check registry first
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client is not None:
                bindings = _srv._client_db.get_bindings_safe_sync(cid)
                return {
                    "phase":               "active",
                    "client_id":           cid,
                    "name":                getattr(reg_client, "name", cid),
                    "is_admin_approved":   True,
                    "is_client_bot_active": getattr(reg_client, "is_client_bot_active", False),
                    "target_index":        getattr(reg_client, "target_index", "NIFTY"),
                    "capital":             float(reg_client.risk.capital),
                    "daily_pnl":           round(float(getattr(reg_client, "_daily_pnl", 0.0)), 2),
                    "tradeable":           reg_client.is_tradeable(),
                    "bindings":            bindings,
                }
            # Not in registry — check DB
            db_client = _srv._client_db.get_client_sync(cid)
            if db_client is None:
                raise HTTPException(404, "Client not found.")
            bindings = _srv._client_db.get_bindings_safe_sync(cid)
            phase = "pending" if bindings else "onboarding"
            if db_client.get("is_admin_approved"):
                phase = "active"
            return {
                "phase":               phase,
                "client_id":           cid,
                "name":                db_client.get("name", cid),
                "is_admin_approved":   bool(db_client.get("is_admin_approved", 0)),
                "is_client_bot_active": bool(db_client.get("is_client_bot_active", 0)),
                "target_index":        db_client.get("target_index", "NIFTY"),
                "capital":             float(db_client.get("capital", 500_000.0)),
                "daily_pnl":           0.0,
                "tradeable":           False,
                "bindings":            bindings,
            }

        # ── CLIENT — add broker binding (portal onboarding) ───────────────────

        @app.post("/api/client/add_broker", tags=["Client"])
        async def api_client_add_broker(
            body: _AddPortalBrokerSchema, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            if not body.binding_id.strip():
                raise HTTPException(400, "binding_id is required.")

            # Guard: binding_id already taken by a *different* provider → reject
            existing_bindings = await asyncio.to_thread(
                _srv._client_db.get_bindings_safe_sync, cid
            )
            clash = next(
                (b for b in existing_bindings
                 if b["binding_id"] == body.binding_id
                 and b["provider"] != body.provider),
                None,
            )
            if clash:
                raise HTTPException(
                    400,
                    f"Binding ID '{body.binding_id}' is already used by "
                    f"{clash['provider'].upper()}. Choose a different Binding ID "
                    f"(e.g. '{body.provider}_main').",
                )

            try:
                await _srv._client_db.upsert_binding(
                    client_id=cid,
                    binding_id=body.binding_id,
                    provider=body.provider,
                    label=body.label,
                    user_id=body.user_id,
                    api_key=body.api_key,
                    api_secret=body.api_secret,
                    access_token="",
                    lot_multiplier=body.lot_multiplier,
                    trading_mode=body.trading_mode,
                    assigned_strategy=body.assigned_strategy,
                    assigned_instrument=body.assigned_instrument,
                )
            except HTTPException:
                raise
            except Exception as exc:
                logger.error("Dashboard: add_broker DB error for %s: %s", cid, exc)
                raise HTTPException(500, f"Failed to save broker credentials: {exc}")

            logger.info("Dashboard: client %s added broker binding %s (%s).", cid, body.binding_id, body.provider)
            return {
                "ok": True,
                "message": (
                    f"Broker '{body.binding_id}' saved. "
                    "Click the Terminal toggle to authenticate via your broker's login page."
                ),
            }

        # ── CLIENT — start bot (pre-flight validation) ────────────────────────

        @app.post("/api/client/start_bot", tags=["Client"])
        async def api_client_start_bot(user: dict = Depends(_require_client)):
            from broker_auth.headless_auth import headless_engine as _he

            cid = user.get("client_id", "")
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client is None:
                raise HTTPException(403, "Client not yet approved or not active.")
            if not reg_client.is_admin_approved:
                raise HTTPException(403, "Account not yet approved by admin.")

            # Pre-flight: headless authenticate all trade-enabled bindings
            bindings_db = _srv._client_db.get_bindings_sync(cid)
            failed_ids: List[str] = []
            for b in bindings_db:
                if not b.get("is_trade_enabled", 1):
                    continue
                try:
                    ok, msg, token = await _he.authenticate_binding(b, cid, _srv._client_db)
                    if ok and token:
                        # Mirror fresh token into in-memory binding
                        bb = next(
                            (x for x in reg_client.broker_bindings if x.binding_id == b["binding_id"]),
                            None,
                        )
                        if bb is not None:
                            bb.access_token = token
                    if not ok:
                        logger.warning(
                            "Dashboard: pre-flight auth failed [%s/%s]: %s",
                            cid, b["binding_id"], msg,
                        )
                        failed_ids.append(b["binding_id"])
                except Exception as exc:
                    logger.warning("Dashboard: pre-flight auth error for %s/%s: %s", cid, b["binding_id"], exc)
                    failed_ids.append(b["binding_id"])

            if failed_ids:
                raise HTTPException(
                    401,
                    f"Authentication failed for: {', '.join(failed_ids)}. "
                    "Token expired or invalid. Please update your access token and try again.",
                )

            # Activate bot flag
            reg_client.is_client_bot_active = True
            await _srv._client_db.upsert_client(cid, is_client_bot_active=1)

            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_BOT_STARTED", "client_id": cid,
            })
            logger.info("Dashboard: client %s started bot.", cid)
            return {"ok": True, "message": "Bot activated. Pre-flight authentication passed."}

        # ── CLIENT — stop bot ─────────────────────────────────────────────────

        @app.post("/api/client/stop_bot", tags=["Client"])
        async def api_client_stop_bot(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client is not None:
                reg_client.is_client_bot_active = False
                reg_client.halt("CLIENT_STOPPED_BOT")
            await _srv._client_db.upsert_client(cid, is_client_bot_active=0)
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_BOT_STOPPED", "client_id": cid,
            })
            logger.info("Dashboard: client %s stopped bot.", cid)
            return {"ok": True, "message": "Bot deactivated."}

        # ── CLIENT — per-broker trade toggle ──────────────────────────────────

        @app.post("/api/client/set_trade/{binding_id}", tags=["Client"])
        async def api_client_set_trade(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            # Read current state and toggle
            bindings = _srv._client_db.get_bindings_safe_sync(cid)
            b = next((x for x in bindings if x["binding_id"] == binding_id), None)
            if b is None:
                raise HTTPException(404, f"Binding '{binding_id}' not found.")
            new_state = not bool(b.get("is_trade_enabled", 1))
            await _srv._client_db.set_trade_enabled(cid, binding_id, new_state)
            # Mirror in in-memory profile
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client:
                for rb in reg_client.broker_bindings:
                    if rb.binding_id == binding_id:
                        rb.is_trade_enabled = new_state
            return {"ok": True, "binding_id": binding_id, "is_trade_enabled": new_state}

        # ── CLIENT — set target index ─────────────────────────────────────────

        @app.post("/api/client/set_index", tags=["Client"])
        async def api_client_set_index(
            body: _SetIndexSchema, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            allowed = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}
            idx = body.index.upper()
            if idx not in allowed:
                raise HTTPException(400, f"Invalid index. Allowed: {sorted(allowed)}")
            await _srv._client_db.upsert_client(cid, target_index=idx)
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client is not None:
                reg_client.target_index = idx
            return {"ok": True, "target_index": idx}

        # ── CLIENT — strategy selections ─────────────────────────────────────

        @app.get("/api/client/strategy_selections", tags=["Client"])
        async def api_client_get_selections(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            db_client = _srv._client_db.get_client_sync(cid)
            if db_client is None:
                raise HTTPException(404, "Client not found.")
            import json as _json
            raw = db_client.get("strategy_selections", "[]") or "[]"
            try:
                selections = _json.loads(raw)
            except Exception:
                selections = []
            return {"ok": True, "selections": selections}

        @app.post("/api/client/strategy_selections", tags=["Client"])
        async def api_client_save_selections(
            body: _StrategySelectionsSchema, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            allowed_strategies = {"sell_straddle", "iron_condor", "trap_trading"}
            allowed_instruments = {"NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY"}
            import json as _json
            validated = []
            for item in (body.selections or []):
                s = item.get("strategy", "") if isinstance(item, dict) else getattr(item, "strategy", "")
                ins = item.get("instrument", "") if isinstance(item, dict) else getattr(item, "instrument", "")
                if s in allowed_strategies and ins.upper() in allowed_instruments:
                    validated.append({"strategy": s, "instrument": ins.upper()})
            await _srv._client_db.upsert_client(cid, strategy_selections=_json.dumps(validated))
            return {"ok": True, "saved": len(validated)}

        # ── CLIENT — positions (live open legs) ───────────────────────────────

        @app.get("/api/client/positions", tags=["Client"])
        async def api_client_positions(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            by_broker: dict = {}
            if reg_client is not None:
                for binding_id, worker in getattr(reg_client, "workers", {}).items():
                    by_broker[binding_id] = {}
                    for strat_name, strat in getattr(worker, "strategies", {}).items():
                        legs = []
                        pos = getattr(strat, "_position", None) or getattr(strat, "position", None)
                        if pos:
                            for attr in ("ce_symbol", "pe_symbol"):
                                sym = getattr(pos, attr, None)
                                if sym:
                                    qty  = getattr(pos, attr.replace("symbol","qty"), 0)
                                    ep   = getattr(pos, attr.replace("symbol","entry_price"), 0)
                                    ltp  = getattr(pos, attr.replace("symbol","ltp"), ep)
                                    pnl  = round((ep - ltp) * abs(qty or 0), 2)
                                    legs.append({"symbol": sym, "qty": qty, "entry_price": round(ep,2), "ltp": round(ltp,2), "pnl": pnl})
                        pnl_total = sum(l["pnl"] for l in legs)
                        by_broker[binding_id][strat_name] = {"legs": legs, "pnl": pnl_total}
            return {"ok": True, "by_broker": by_broker}

        # ── CLIENT — history ──────────────────────────────────────────────────

        @app.get("/api/client/history", tags=["Client"])
        async def api_client_history(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            # Pull from event bus log — filter fills + exits for this client
            trades = []
            for evt in list(reversed(_srv._event_log[-200:] if hasattr(_srv, "_event_log") else [])):
                if evt.get("client_id") == cid and evt.get("type") in ("fill", "exit", "trade"):
                    trades.append({
                        "date":        evt.get("ts", ""),
                        "strategy":    evt.get("strategy", "—"),
                        "instrument":  evt.get("instrument", evt.get("index", "—")),
                        "entry_price": evt.get("entry_price", "—"),
                        "exit_price":  evt.get("exit_price", evt.get("avg_price", "—")),
                        "exit_reason": evt.get("reason", evt.get("code", "—")),
                        "pnl":         float(evt.get("pnl", 0)),
                    })
            return {"ok": True, "trades": trades}

        # ── CLIENT — kill broker ──────────────────────────────────────────────

        @app.post("/api/client/kill_broker/{binding_id}", tags=["Client"])
        async def api_client_kill_broker(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            reg_client = _srv._registry.get(cid) if _srv._registry else None
            if reg_client is None:
                raise HTTPException(404, "Client worker not running.")
            worker = getattr(reg_client, "workers", {}).get(binding_id)
            if worker is None:
                raise HTTPException(404, f"Binding '{binding_id}' not found.")
            if hasattr(worker, "halt"):
                await worker.halt()
            elif hasattr(worker, "stop"):
                await worker.stop()
            await _srv._client_db.upsert_client(cid, **{f"trade_enabled_{binding_id}": False})
            return {"ok": True, "message": f"Broker '{binding_id}' halted."}

        # ── CLIENT — delete broker binding ───────────────────────────────────

        @app.delete("/api/client/broker/{binding_id}", tags=["Client"])
        async def api_client_delete_broker(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            bindings = await asyncio.to_thread(
                _srv._client_db.get_bindings_safe_sync, cid
            )
            match = next((b for b in bindings if b["binding_id"] == binding_id), None)
            if match is None:
                raise HTTPException(404, f"Binding '{binding_id}' not found.")
            if match.get("is_trade_enabled"):
                raise HTTPException(
                    400,
                    f"Cannot delete '{binding_id}' while trading is ON. "
                    "Disable trading first.",
                )
            await _srv._client_db.delete_binding(cid, binding_id)
            logger.info("Dashboard: deleted binding %s for client %s", binding_id, cid)
            return {"ok": True, "message": f"Broker '{binding_id}' deleted."}

        # Fyers manual auth endpoints removed — use unified /connect → /callback/fyers flow

        # ── CLIENT — per-broker start / stop / mode ──────────────────────────

        @app.post("/api/client/broker/{binding_id}/start", tags=["Client"])
        async def api_client_broker_start(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            """
            Enable trading for a specific broker binding.
            Automatically checks token validity; runs headless auth if token
            is missing or expired before enabling trading.
            """
            from broker_auth.headless_auth import headless_engine as _he
            cid = user.get("client_id", "")
            bindings_db = _srv._client_db.get_bindings_sync(cid)
            b_row = next((b for b in bindings_db if b["binding_id"] == binding_id), None)
            if b_row is None:
                raise HTTPException(404, f"Binding '{binding_id}' not found.")

            provider = (b_row.get("provider") or "mock").lower()
            try:
                ok, auth_msg, token = await _he.authenticate_binding(b_row, cid, _srv._client_db)
            except Exception as exc:
                return {"ok": False, "message": f"Auth error: {exc}", "token_ok": False, "trading_enabled": False}

            if not ok:
                # Auth failed — do NOT enable trading
                return {
                    "ok": False,
                    "message": f"Could not connect {provider.title()}: {auth_msg}",
                    "token_ok": False,
                    "trading_enabled": False,
                }

            # Auth succeeded — enable trading for this binding
            await _srv._client_db.set_trade_enabled(cid, binding_id, True)
            logger.info("Dashboard: broker %s/%s started (token_ok=%s).", cid, binding_id, bool(token))
            return {
                "ok": True,
                "message": f"Connected & trading enabled. {auth_msg}",
                "token_ok": True,
                "trading_enabled": True,
            }

        @app.post("/api/client/broker/{binding_id}/stop", tags=["Client"])
        async def api_client_broker_stop(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            await _srv._client_db.set_trade_enabled(cid, binding_id, False)
            logger.info("Dashboard: broker %s/%s stopped.", cid, binding_id)
            return {"ok": True, "message": f"Broker '{binding_id}' stopped — trading disabled."}

        @app.post("/api/client/broker/{binding_id}/mode", tags=["Client"])
        async def api_client_broker_mode(
            binding_id: str, body: _BrokerModeSchema, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            if body.mode not in ("paper", "live"):
                raise HTTPException(400, "mode must be 'paper' or 'live'.")
            await _srv._client_db.set_trading_mode(cid, binding_id, body.mode)
            return {"ok": True, "mode": body.mode, "message": f"Mode set to {body.mode.upper()}."}

        # ── CLIENT — Terminal toggle: Interactive OAuth Flow ─────────────────────

        @app.post("/api/client/broker/{binding_id}/connect", tags=["Client"])
        async def api_client_broker_connect(
            binding_id: str,
            request:    Request,
            user:       dict = Depends(_require_client),
        ):
            """
            Terminal toggle ON — Interactive OAuth handshake for all 6 providers:
              1. Check DB for cached access_token generated today → API ping
              2a. Token valid  → connect instantly (terminal_connected=1)
              2b. Token missing/expired → return broker OAuth login URL
                  Fyers/Upstox/Zerodha : standard OAuth2 redirect
                  Dhan                 : 3-step consent (server pre-generates consentAppId)
                  AngelOne             : implicit redirect (state in callback path)
                  AliceBlue            : OAuth2 + SHA256 exchange
            """
            import time as _time
            from broker_auth.headless_auth import headless_engine as _he
            from broker_auth.oauth_manager import generate_auth_url, build_state

            t0 = _time.monotonic()
            cid = user.get("client_id", "")
            bindings = await asyncio.to_thread(_srv._client_db.get_bindings_sync, cid)
            b = next((x for x in bindings if x["binding_id"] == binding_id), None)
            if b is None:
                return {"ok": False, "error": f"Broker '{binding_id}' not found."}

            provider   = (b.get("provider") or "mock").lower()
            api_key    = b.get("api_key", "")
            api_secret = b.get("api_secret", "")
            user_id    = b.get("user_id", "")

            logger.info(
                "[Terminal] [%s/%s] %s — Step 1: checking cached token (%.1fms)",
                cid, binding_id, provider.upper(), (_time.monotonic()-t0)*1000,
            )

            ok, msg, token = await _he.authenticate_binding(b, cid, _srv._client_db)

            if ok:
                await _srv._client_db.set_terminal_connected(cid, binding_id, True)
                logger.info(
                    "[Terminal] [%s/%s] CONNECTED instantly — cached token valid (%.1fms)",
                    cid, binding_id, (_time.monotonic()-t0)*1000,
                )
                return {"ok": True, "connected": True, "message": msg, "flow": "cached"}

            # Token missing/expired — generate broker OAuth login URL
            base_url     = _base_url(request)
            callback_url = f"{base_url}/callback/{provider}"
            state        = build_state("client", cid, binding_id)

            # generate_auth_url may make an HTTP call (Dhan), so run in thread
            auth_ok, auth_url = await asyncio.to_thread(
                generate_auth_url, provider, api_key, api_secret, callback_url, state, user_id
            )
            elapsed = (_time.monotonic() - t0) * 1000

            if not auth_ok:
                logger.warning(
                    "[Terminal] [%s/%s] auth URL generation failed (%.1fms): %s",
                    cid, binding_id, elapsed, auth_url,
                )
                return {"ok": False, "connected": False, "flow": "error", "error": auth_url}

            logger.info(
                "[Terminal] [%s/%s] OAuth URL generated — awaiting user login (%.1fms)",
                cid, binding_id, elapsed,
            )
            return {
                "ok":       False,
                "connected": False,
                "flow":     "oauth",
                "auth_url": auth_url,
                "message":  f"Open the broker login page to authenticate {provider.upper()}.",
            }

        @app.post("/api/client/broker/{binding_id}/disconnect", tags=["Client"])
        async def api_client_broker_disconnect(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            """Terminal toggle OFF — also stops engine if running."""
            cid = user.get("client_id", "")
            await _srv._client_db.set_terminal_connected(cid, binding_id, False)
            await _srv._client_db.set_engine_active(cid, binding_id, False)
            logger.info("Terminal disconnect: [%s/%s] disconnected.", cid, binding_id)
            return {"ok": True, "message": "Terminal disconnected. Engine stopped."}

        # ── CLIENT — Engine toggle (Step 2: hot-reload strategy execution) ─────

        @app.post("/api/client/broker/{binding_id}/engine-start", tags=["Client"])
        async def api_client_engine_start(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            """
            Trading Engine toggle ON.
            Requires terminal_connected=1 first.
            Scans strategy assignments and hot-attaches broker to the running engine.
            """
            cid = user.get("client_id", "")
            bindings = await asyncio.to_thread(_srv._client_db.get_bindings_safe_sync, cid)
            b = next((x for x in bindings if x["binding_id"] == binding_id), None)
            if b is None:
                return {"ok": False, "error": f"Broker '{binding_id}' not found."}
            if not b.get("terminal_connected"):
                return {
                    "ok": False,
                    "error": "Terminal not connected. Switch Terminal ON first.",
                }

            strategy  = b.get("assigned_strategy", "") or ""
            underlying = b.get("assigned_instrument", "NIFTY") or "NIFTY"

            await _srv._client_db.set_engine_active(cid, binding_id, True)

            # Apply any saved deployment config to RuntimeConfig immediately
            deploy_id = f"{cid}_{binding_id}_{strategy}"
            try:
                from data_layer.deployment_store import load_deployment_json, apply_deployment_to_runtime_config
                deploy = load_deployment_json(deploy_id)
                if deploy:
                    apply_deployment_to_runtime_config(deploy)
                    logger.info(
                        "Engine start: [%s/%s] applied deployment config %s",
                        cid, binding_id, deploy_id,
                    )
            except Exception as exc:
                logger.warning("Engine start: deployment config apply failed: %s", exc)

            logger.info(
                "Engine start: [%s/%s] ACTIVE — strategy=%s underlying=%s mode=%s",
                cid, binding_id, strategy or "(none)", underlying,
                b.get("trading_mode", "paper"),
            )
            return {
                "ok": True,
                "engine_active": True,
                "strategy": strategy,
                "underlying": underlying,
                "message": (
                    f"Trading Engine active — {strategy or 'no strategy'} "
                    f"on {underlying} [{b.get('trading_mode','paper').upper()}]"
                ),
            }

        @app.post("/api/client/broker/{binding_id}/engine-stop", tags=["Client"])
        async def api_client_engine_stop(
            binding_id: str, user: dict = Depends(_require_client),
        ):
            """Trading Engine toggle OFF — stops execution routing for this broker."""
            cid = user.get("client_id", "")
            await _srv._client_db.set_engine_active(cid, binding_id, False)
            logger.info("Engine stop: [%s/%s] engine deactivated.", cid, binding_id)
            return {"ok": True, "engine_active": False, "message": "Trading Engine stopped."}

        # ── CLIENT — Strategy deployment ──────────────────────────────────────

        @app.post("/api/client/strategy/deploy", tags=["Client"])
        async def api_client_strategy_deploy(
            body: _DeploymentSchema, user: dict = Depends(_require_client),
        ):
            """
            Save a strategy deployment config.
            Persists to SQLite + JSON file at data/deployments/{deploy_id}.json
            Immediately applies to RuntimeConfig if engine is active.
            """
            cid = user.get("client_id", "")

            # Validate squareoff time
            sq = (body.squareoff_time or "15:15").strip()
            try:
                h, m = sq.split(":")
                assert 0 <= int(h) <= 23 and 0 <= int(m) <= 59
            except Exception:
                return {"ok": False, "error": f"Invalid squareoff_time '{sq}'. Use HH:MM format."}

            allowed_strategies = {"sell_straddle", "iron_condor", "trap_trading"}
            if body.strategy_name not in allowed_strategies:
                return {"ok": False, "error": f"Unknown strategy '{body.strategy_name}'."}

            deploy_id = await _srv._client_db.save_deployment(
                client_id      = cid,
                binding_id     = body.binding_id,
                strategy_name  = body.strategy_name,
                underlying     = body.underlying,
                lot_multiplier = body.lot_multiplier,
                max_profit_rs  = body.max_profit_rs,
                max_sl_rs      = body.max_sl_rs,
                squareoff_time = sq,
            )

            from data_layer.deployment_store import save_deployment_json, apply_deployment_to_runtime_config
            save_deployment_json(
                deploy_id      = deploy_id,
                client_id      = cid,
                binding_id     = body.binding_id,
                strategy_name  = body.strategy_name,
                underlying     = body.underlying,
                lot_multiplier = body.lot_multiplier,
                max_profit_rs  = body.max_profit_rs,
                max_sl_rs      = body.max_sl_rs,
                squareoff_time = sq,
            )

            # If engine is already active for this broker, hot-apply immediately
            bindings = await asyncio.to_thread(_srv._client_db.get_bindings_safe_sync, cid)
            b = next((x for x in bindings if x["binding_id"] == body.binding_id), None)
            hot_applied = False
            if b and b.get("engine_active"):
                try:
                    apply_deployment_to_runtime_config({
                        "strategy_name": body.strategy_name, "underlying": body.underlying,
                        "lot_multiplier": body.lot_multiplier, "max_profit_rs": body.max_profit_rs,
                        "max_sl_rs": body.max_sl_rs, "squareoff_time": sq,
                    })
                    hot_applied = True
                except Exception as exc:
                    logger.warning("Deploy hot-apply failed: %s", exc)

            logger.info(
                "Deploy saved: %s [lots=%.1f profit=%.0f sl=%.0f sq=%s hot=%s]",
                deploy_id, body.lot_multiplier, body.max_profit_rs,
                body.max_sl_rs, sq, hot_applied,
            )
            return {
                "ok": True,
                "deploy_id": deploy_id,
                "hot_applied": hot_applied,
                "message": f"Deployment saved{' and hot-applied' if hot_applied else ''}.",
            }

        @app.get("/api/client/strategy/deployments", tags=["Client"])
        async def api_client_get_deployments(user: dict = Depends(_require_client)):
            cid = user.get("client_id", "")
            rows = await asyncio.to_thread(_srv._client_db.get_deployments_sync, cid)
            return {"ok": True, "deployments": rows}

        @app.delete("/api/client/strategy/deploy/{deploy_id}", tags=["Client"])
        async def api_client_delete_deployment(
            deploy_id: str, user: dict = Depends(_require_client),
        ):
            cid = user.get("client_id", "")
            await _srv._client_db.delete_deployment(deploy_id, cid)
            from data_layer.deployment_store import delete_deployment_json
            delete_deployment_json(deploy_id)
            return {"ok": True, "message": "Deployment removed."}

        # ── UNIFIED OAuth Callback Handler ────────────────────────────────────
        # Receives redirects from ALL 6 broker login portals.
        # Routing strategy:
        #   Fyers/Upstox/Zerodha/AliceBlue: state in ?state= query param
        #   AngelOne: state in URL path (/callback/angelone/{state})
        #   Dhan: no state; consume-consent returns dhanClientId → DB lookup

        async def _handle_oauth_callback(
            broker_name: str,
            request:     Request,
            path_state:  str = "",   # AngelOne: state from URL path
        ):
            import time as _t
            from broker_auth.oauth_manager import parse_state, exchange_code, consume_dhan_consent
            t0       = _t.monotonic()
            provider = broker_name.lower()

            error = request.query_params.get("error", "")
            if error:
                return HTMLResponse(_callback_page("error", provider, f"Login failed: {error}"))

            # ── Extract the auth code from broker-specific query param ────────
            actual_code = ""
            extra: dict = {}

            if provider == "dhan":
                actual_code = request.query_params.get("tokenId", "")
            elif provider == "angelone":
                actual_code = request.query_params.get("auth_token", "") or request.query_params.get("authToken", "")
            elif provider == "aliceblue":
                actual_code = request.query_params.get("authCode", "")
                extra["user_id"] = request.query_params.get("userId", "")
            elif provider == "fyers":
                actual_code = request.query_params.get("auth_code", "") or request.query_params.get("code", "")
            elif provider == "zerodha":
                actual_code = request.query_params.get("request_token", "") or request.query_params.get("code", "")
            else:
                actual_code = request.query_params.get("code", "") or request.query_params.get("auth_code", "")

            logger.info(
                "[Callback] %s received — code=%s path_state=%s error=%s",
                provider.upper(), bool(actual_code), bool(path_state), error or "none",
            )

            if not actual_code:
                return HTMLResponse(_callback_page("error", provider, "No auth code received from broker."))

            # ── Identify admin vs client (routing) ────────────────────────────

            if provider == "dhan":
                # Dhan: call consume-consent → get dhanClientId → DB lookup
                platform_creds = await asyncio.to_thread(
                    _srv._client_db.get_platform_credentials_sync, "dhan"
                )
                if not platform_creds:
                    return HTMLResponse(_callback_page("error", provider, "Dhan app credentials not configured."))

                dhan_ok, dhan_msg, access_token, dhan_client_id = await asyncio.to_thread(
                    consume_dhan_consent,
                    platform_creds["api_key"],
                    platform_creds["api_secret"],
                    actual_code,
                )
                elapsed = (_t.monotonic() - t0) * 1000
                if not dhan_ok:
                    logger.error("[Callback] Dhan consume-consent failed in %.1fms: %s", elapsed, dhan_msg)
                    return HTMLResponse(_callback_page("error", provider, dhan_msg))

                # Look up which binding/feeder this dhanClientId belongs to
                match = await asyncio.to_thread(
                    _srv._client_db.find_by_broker_user_id_sync, "dhan", dhan_client_id
                )
                if not match:
                    return HTMLResponse(_callback_page("error", provider,
                        f"No binding found for Dhan client {dhan_client_id}. "
                        "Ensure the Client ID is saved in your broker binding."))

                if match["scope"] == "feeder":
                    await _srv._client_db.update_feeder_token(
                        provider, access_token,
                        generated_at=datetime.now(IST).isoformat(), expiry_at=_ist_eod(),
                    )
                    _srv._bus.publish("system_event", {"type": "feeder_token_updated", "provider": provider, "ok": True})
                    logger.info("[Callback] Dhan feeder token stored in %.1fms", elapsed)
                    return HTMLResponse(_callback_page("success", provider, "Dhan data feeder connected!"))
                else:
                    cid = match["client_id"]
                    bid = match["binding_id"]
                    await _srv._client_db.update_access_token(cid, bid, access_token, datetime.now(IST).isoformat(), _ist_eod())
                    await _srv._client_db.set_terminal_connected(cid, bid, True)
                    _srv._bus.publish("system_event", {"type": "terminal_connected", "client_id": cid, "binding_id": bid, "provider": provider, "ok": True})
                    logger.info("[Callback] Dhan [%s/%s] token stored, terminal=ON in %.1fms", cid, bid, elapsed)
                    return HTMLResponse(_callback_page("success", provider, f"Dhan broker connected!"))

            elif provider == "aliceblue":
                # AliceBlue: userId in callback → DB lookup, then exchange
                alice_user_id = extra.get("user_id", "")
                if not alice_user_id:
                    return HTMLResponse(_callback_page("error", provider, "AliceBlue userId missing from callback."))

                match = await asyncio.to_thread(
                    _srv._client_db.find_by_broker_user_id_sync, "aliceblue", alice_user_id
                )
                if not match:
                    return HTMLResponse(_callback_page("error", provider,
                        f"No binding found for AliceBlue userId {alice_user_id}. "
                        "Ensure the Client ID (userId) is saved in your broker binding."))

                api_key    = match["api_key"]
                api_secret = match["api_secret"]
                base_url   = _base_url(request)
                callback_url = f"{base_url}/callback/{provider}"

                ok, msg, token = await asyncio.to_thread(
                    exchange_code, provider, api_key, api_secret, actual_code, callback_url, extra
                )
                elapsed = (_t.monotonic() - t0) * 1000

                if not ok:
                    return HTMLResponse(_callback_page("error", provider, msg))

                if match["scope"] == "feeder":
                    await _srv._client_db.update_feeder_token(provider, token, datetime.now(IST).isoformat(), _ist_eod())
                    _srv._bus.publish("system_event", {"type": "feeder_token_updated", "provider": provider, "ok": True})
                    return HTMLResponse(_callback_page("success", provider, "AliceBlue feeder connected!"))
                else:
                    cid, bid = match["client_id"], match["binding_id"]
                    await _srv._client_db.update_access_token(cid, bid, token, datetime.now(IST).isoformat(), _ist_eod())
                    await _srv._client_db.set_terminal_connected(cid, bid, True)
                    _srv._bus.publish("system_event", {"type": "terminal_connected", "client_id": cid, "binding_id": bid, "provider": provider, "ok": True})
                    logger.info("[Callback] AliceBlue [%s/%s] token stored in %.1fms", cid, bid, elapsed)
                    return HTMLResponse(_callback_page("success", provider, "AliceBlue broker connected!"))

            else:
                # Standard state-based routing: Fyers, Upstox, Zerodha, AngelOne
                state_str = path_state or request.query_params.get("state", "")
                parsed    = parse_state(state_str) if state_str else {}
                role       = parsed.get("role", "")
                client_id  = parsed.get("client_id", "")
                binding_id = parsed.get("binding_id", "")
                base_url   = _base_url(request)
                callback_url = f"{base_url}/callback/{provider}"

                if role == "admin":
                    db_row     = _srv._client_db.get_feeder_creds_sync(provider) or {}
                    api_key    = db_row.get("api_key", "")
                    api_secret = db_row.get("secret", "")
                    ok, msg, token = await asyncio.to_thread(
                        exchange_code, provider, api_key, api_secret, actual_code, callback_url, extra
                    )
                    elapsed = (_t.monotonic() - t0) * 1000
                    if ok and token:
                        await _srv._client_db.update_feeder_token(provider, token, datetime.now(IST).isoformat(), _ist_eod())
                        _srv._bus.publish("system_event", {"type": "feeder_token_updated", "provider": provider, "ok": True})
                        logger.info("[Callback] Admin %s token stored in %.1fms", provider.upper(), elapsed)
                        return HTMLResponse(_callback_page("success", provider, "Data feeder connected!"))
                    logger.error("[Callback] Admin %s exchange failed: %s", provider, msg)
                    return HTMLResponse(_callback_page("error", provider, msg))

                elif role == "client" and client_id and binding_id:
                    bindings = await asyncio.to_thread(_srv._client_db.get_bindings_sync, client_id)
                    b = next((x for x in bindings if x["binding_id"] == binding_id), None)
                    if b is None:
                        return HTMLResponse(_callback_page("error", provider, "Binding not found."))
                    api_key    = b.get("api_key", "")
                    api_secret = b.get("api_secret", "")
                    ok, msg, token = await asyncio.to_thread(
                        exchange_code, provider, api_key, api_secret, actual_code, callback_url, extra
                    )
                    elapsed = (_t.monotonic() - t0) * 1000
                    if ok and token:
                        now = datetime.now(IST).isoformat()
                        await _srv._client_db.update_access_token(client_id, binding_id, token, now, _ist_eod())
                        await _srv._client_db.set_terminal_connected(client_id, binding_id, True)
                        _srv._bus.publish("system_event", {"type": "terminal_connected", "client_id": client_id, "binding_id": binding_id, "provider": provider, "ok": True})
                        logger.info("[Callback] Client [%s/%s] %s token stored, terminal=ON in %.1fms",
                                    client_id, binding_id, provider.upper(), elapsed)
                        return HTMLResponse(_callback_page("success", provider,
                            f"Broker {provider.upper()} connected! You can close this tab."))
                    logger.error("[Callback] Client [%s/%s] %s exchange failed: %s",
                                 client_id, binding_id, provider, msg)
                    return HTMLResponse(_callback_page("error", provider, msg))
                else:
                    return HTMLResponse(_callback_page("error", provider, "Invalid or missing state parameter."))

        @app.get("/callback/{broker_name}", tags=["Auth"], include_in_schema=False)
        async def oauth_callback(broker_name: str, request: Request):
            return await _handle_oauth_callback(broker_name, request, path_state="")

        @app.get("/callback/{broker_name}/{path_state}", tags=["Auth"], include_in_schema=False)
        async def oauth_callback_with_state(broker_name: str, path_state: str, request: Request):
            """AngelOne embeds our state token in the redirect_url path."""
            return await _handle_oauth_callback(broker_name, request, path_state=path_state)

        # ── ADMIN — pending clients ───────────────────────────────────────────

        @app.get("/api/admin/clients/pending", tags=["Admin"])
        async def api_pending_clients(_: dict = Depends(_require_admin)):
            rows = _srv._client_db.get_pending_clients_sync()
            return {"pending": rows, "ts": datetime.now(IST).isoformat()}

        @app.get("/api/admin/auth_alerts", tags=["Admin"])
        async def api_auth_alerts(_: dict = Depends(_require_admin)):
            return {
                "feeder": _srv._auth_alerts.get("feeder", ""),
                "ts":     datetime.now(IST).isoformat(),
            }

        # ── ADMIN — strategy telemetry (TrapTradingEngine live state) ────────

        @app.get("/api/admin/strategy/telemetry", tags=["Admin"])
        async def api_strategy_telemetry(_: dict = Depends(_require_admin)):
            now_ist = datetime.now(IST).isoformat()
            eng = _srv._trap_engine
            if eng is None:
                return {"ts": now_ist, "trap_engine": {}, "active": False}
            try:
                snapshot = eng.telemetry_snapshot()
                return {"ts": now_ist, "trap_engine": snapshot, "active": True}
            except Exception as exc:
                logger.warning("Dashboard: strategy telemetry read failed: %s", exc)
                return {"ts": now_ist, "trap_engine": {}, "active": False, "error": str(exc)}

        # ── ADMIN — premium-selling strategy registry ────────────────────

        @app.get("/api/admin/strategies", tags=["Admin"])
        async def api_strategies(_: dict = Depends(_require_admin)):
            now_ist = datetime.now(IST).isoformat()
            out = []
            for ic in _srv._iron_condors:
                pos = ic.position
                entry = None
                if pos:
                    entry = {
                        "short_ce": pos.short_ce.strike,
                        "short_pe": pos.short_pe.strike,
                        "long_ce":  pos.long_ce.strike,
                        "long_pe":  pos.long_pe.strike,
                        "net_credit":    round(pos.net_credit, 2),
                        "profit_target": round(pos.profit_target, 2),
                        "stop_loss":     round(pos.stop_loss, 2),
                        "open_time": pos.open_time.isoformat() if pos.open_time else None,
                    }
                out.append({
                    "type":         "iron_condor",
                    "underlying":   ic._underlying,
                    "running":      ic._running,
                    "has_position": ic.has_open_position,
                    "spot":         round(ic._spot, 2),
                    "entry_allowed": ic.entry_allowed,
                    "position":     entry,
                })
            for ss in _srv._sell_straddles:
                pos = ss.position
                entry = None
                if pos:
                    entry = {
                        "atm":       pos.atm_at_entry,
                        "ce_strike": pos.ce_leg.strike,
                        "pe_strike": pos.pe_leg.strike,
                        "net_credit":    round(pos.net_credit, 2),
                        "unrealized_pnl": round(pos.unrealized_pnl, 2),
                        "profit_target": round(pos.profit_target, 2),
                        "stop_loss":     round(pos.stop_loss_limit, 2),
                        "trailing_active": pos.trailing_active,
                        "open_time": pos.open_time.isoformat() if pos.open_time else None,
                    }
                out.append({
                    "type":         "sell_straddle",
                    "underlying":   ss._underlying,
                    "running":      ss._running,
                    "has_position": ss.has_open_position,
                    "trades_today": ss.trades_today,
                    "spot":         round(ss._spot, 2),
                    "rsi":          round(ss._rsi, 1),   # live reference — may be used in entry rules
                    "adx":          round(ss._adx, 1),   # live reference — may be used in entry rules
                    "entry_allowed": ss.entry_allowed,
                    "position":     entry,
                })
            return {"strategies": out, "ts": now_ist}

        # ── ADMIN — runtime strategy configuration ───────────────────────

        @app.get("/api/admin/strategy/config", tags=["Admin"])
        async def api_strategy_config_get(_: dict = Depends(_require_admin)):
            from data_layer.runtime_config import RuntimeConfig
            cfg = RuntimeConfig.get()
            rms = cfg.get("rms", {})
            ind = cfg.get("indicators", {})
            ic  = cfg.get("iron_condor", {})
            ss  = cfg.get("sell_straddle", {})
            tt  = cfg.get("trap_trading", {})
            ic_idx = ic.get("per_index", {})
            return {
                "rms_max_drawdown_pct":       rms.get("max_drawdown_pct",       5.0),
                "rms_order_throttle":         rms.get("order_throttle_per_sec", 5),
                "rms_squareoff_time":         rms.get("squareoff_time",         "15:15"),
                "rms_distance_filter_pct":    rms.get("distance_filter_pct",    5.0),
                "ind_rsi_period":             ind.get("rsi_period",   14),
                "ind_vwap_window":            ind.get("vwap_window",  500),
                "ind_adx_period":             ind.get("adx_period",   20),
                "ind_ema_fast":               ind.get("ema_fast",     9),
                "ind_ema_slow":               ind.get("ema_slow",     21),
                "ind_htf_minutes":            ind.get("htf_minutes",  75),
                "ind_ltf_minutes":            ind.get("ltf_minutes",  5),
                "ic_squareoff_time":          ic.get("squareoff_time", "15:15"),
                "ic_rsi_min":                 ic.get("rsi_min",  40.0),
                "ic_rsi_max":                 ic.get("rsi_max",  60.0),
                "ic_adx_max":                 ic.get("adx_max",  25.0),
                "ic_profit_pct":              ic.get("profit_pct", 50.0),
                "ic_sl_pct":                  ic.get("sl_pct",   200.0),
                "ic_nifty_otm":              ic_idx.get("NIFTY",      {}).get("short_otm_pts", 200.0),
                "ic_nifty_wing":             ic_idx.get("NIFTY",      {}).get("wing_width_pts",200.0),
                "ic_banknifty_otm":          ic_idx.get("BANKNIFTY",  {}).get("short_otm_pts", 400.0),
                "ic_banknifty_wing":         ic_idx.get("BANKNIFTY",  {}).get("wing_width_pts",500.0),
                "ic_finnifty_otm":           ic_idx.get("FINNIFTY",   {}).get("short_otm_pts", 200.0),
                "ic_finnifty_wing":          ic_idx.get("FINNIFTY",   {}).get("wing_width_pts",200.0),
                "ic_sensex_otm":             ic_idx.get("SENSEX",     {}).get("short_otm_pts", 500.0),
                "ic_sensex_wing":            ic_idx.get("SENSEX",     {}).get("wing_width_pts",500.0),
                "ic_midcp_otm":              ic_idx.get("MIDCPNIFTY", {}).get("short_otm_pts", 150.0),
                "ic_midcp_wing":             ic_idx.get("MIDCPNIFTY", {}).get("wing_width_pts",200.0),
                "ss_entry_start":            ss.get("entry_start",     "09:20"),
                "ss_entry_end":              ss.get("entry_end",       "12:00"),
                "ss_squareoff_time":         ss.get("squareoff_time",  "15:15"),
                "ss_rsi_min":                ss.get("rsi_min",         35.0),
                "ss_rsi_max":                ss.get("rsi_max",         65.0),
                "ss_adx_max":                ss.get("adx_max",         30.0),
                "ss_profit_pct":             ss.get("profit_pct",      30.0),
                "ss_sl_pct":                 ss.get("sl_pct",         200.0),
                "ss_trail_lock_pct":         ss.get("trail_lock_pct",  20.0),
                "ss_trail_floor_pct":        ss.get("trail_floor_pct", 10.0),
                "ss_max_trades":             ss.get("max_trades",       1),
                "ss_roc_limit_pct":          ss.get("roc_limit_pct",   1.5),
                "tt_htf_minutes":            tt.get("htf_minutes",          75),
                "tt_ltf_minutes":            tt.get("ltf_minutes",           5),
                "tt_adx_threshold":          tt.get("adx_threshold",        20.0),
                "tt_volume_spike_mult":      tt.get("volume_spike_multiplier",1.5),
                "tt_swing_lookback":         tt.get("swing_lookback",        5),
                "tt_zone_tol_pct":           tt.get("zone_tolerance_pct",    0.5),
                "tt_void_atr_mult":          tt.get("void_atr_mult",         2.0),
            }

        @app.post("/api/admin/strategy/config/update", tags=["Admin"])
        async def api_strategy_config_update(
            body: _StrategyConfigSchema,
            _: dict = Depends(_require_admin),
        ):
            from data_layer.runtime_config import RuntimeConfig
            patch = {
                "rms": {
                    "max_drawdown_pct":       body.rms_max_drawdown_pct,
                    "order_throttle_per_sec": body.rms_order_throttle,
                    "squareoff_time":         body.rms_squareoff_time,
                    "distance_filter_pct":    body.rms_distance_filter_pct,
                },
                "indicators": {
                    "rsi_period":   body.ind_rsi_period,
                    "vwap_window":  body.ind_vwap_window,
                    "adx_period":   body.ind_adx_period,
                    "ema_fast":     body.ind_ema_fast,
                    "ema_slow":     body.ind_ema_slow,
                    "htf_minutes":  body.ind_htf_minutes,
                    "ltf_minutes":  body.ind_ltf_minutes,
                },
                "iron_condor": {
                    "squareoff_time": body.ic_squareoff_time,
                    "rsi_min":  body.ic_rsi_min,
                    "rsi_max":  body.ic_rsi_max,
                    "adx_max":  body.ic_adx_max,
                    "profit_pct": body.ic_profit_pct,
                    "sl_pct":     body.ic_sl_pct,
                    "per_index": {
                        "NIFTY":      {"short_otm_pts": body.ic_nifty_otm,     "wing_width_pts": body.ic_nifty_wing},
                        "BANKNIFTY":  {"short_otm_pts": body.ic_banknifty_otm, "wing_width_pts": body.ic_banknifty_wing},
                        "FINNIFTY":   {"short_otm_pts": body.ic_finnifty_otm,  "wing_width_pts": body.ic_finnifty_wing},
                        "SENSEX":     {"short_otm_pts": body.ic_sensex_otm,    "wing_width_pts": body.ic_sensex_wing},
                        "MIDCPNIFTY": {"short_otm_pts": body.ic_midcp_otm,     "wing_width_pts": body.ic_midcp_wing},
                    },
                },
                "sell_straddle": {
                    "entry_start":     body.ss_entry_start,
                    "entry_end":       body.ss_entry_end,
                    "squareoff_time":  body.ss_squareoff_time,
                    "rsi_min":         body.ss_rsi_min,
                    "rsi_max":         body.ss_rsi_max,
                    "adx_max":         body.ss_adx_max,
                    "profit_pct":      body.ss_profit_pct,
                    "sl_pct":          body.ss_sl_pct,
                    "trail_lock_pct":  body.ss_trail_lock_pct,
                    "trail_floor_pct": body.ss_trail_floor_pct,
                    "max_trades":      body.ss_max_trades,
                    "roc_limit_pct":   body.ss_roc_limit_pct,
                },
                "trap_trading": {
                    "htf_minutes":             body.tt_htf_minutes,
                    "ltf_minutes":             body.tt_ltf_minutes,
                    "adx_threshold":           body.tt_adx_threshold,
                    "volume_spike_multiplier": body.tt_volume_spike_mult,
                    "swing_lookback":          body.tt_swing_lookback,
                    "zone_tolerance_pct":      body.tt_zone_tol_pct,
                    "void_atr_mult":           body.tt_void_atr_mult,
                },
            }
            RuntimeConfig.update(patch)

            # Live-inject into running strategy instances
            reconfigure_errors = []
            for ic in (_srv._iron_condors or []):
                try:
                    ic.reconfigure()
                except Exception as e:
                    reconfigure_errors.append(f"iron_condor[{ic._underlying}]: {e}")
            for ss in (_srv._sell_straddles or []):
                try:
                    ss.reconfigure()
                except Exception as e:
                    reconfigure_errors.append(f"sell_straddle[{ss._underlying}]: {e}")
            if _srv._trap_engine is not None:
                try:
                    _srv._trap_engine.reconfigure()
                except Exception as e:
                    reconfigure_errors.append(f"trap_engine: {e}")

            # Sync RMS into GlobalConfig so existing RMS endpoint stays consistent
            if hasattr(_srv._cfg, "rms") and isinstance(_srv._cfg.rms, dict):
                _srv._cfg.rms.update(patch["rms"])

            if reconfigure_errors:
                logger.warning("strategy/config/update: partial reconfigure errors: %s", reconfigure_errors)
                return {"ok": True, "message": "Config saved. Some live-inject errors.", "errors": reconfigure_errors}

            logger.info("strategy/config/update: all strategies reconfigured live.")
            return {"ok": True, "message": "Runtime configuration live-deployed to all strategies."}

        # ── ADMIN — per-index strategy config (rule builder) ─────────────────

        @app.get("/api/admin/strategy/config/{index}", tags=["Admin"])
        async def api_index_config_get(index: str, _: dict = Depends(_require_admin)):
            from data_layer.runtime_config import RuntimeConfig
            idx = index.upper()
            allowed = {"NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY"}
            if idx not in allowed:
                raise HTTPException(400, f"Unknown index '{idx}'. Allowed: {sorted(allowed)}")
            return {
                "index": idx,
                "sell_straddle": RuntimeConfig.index_section(idx, "sell_straddle"),
                "iron_condor":   RuntimeConfig.index_section(idx, "iron_condor"),
            }

        @app.post("/api/admin/strategy/config/{index}", tags=["Admin"])
        async def api_index_config_save(
            index: str, request: Request, _: dict = Depends(_require_admin),
        ):
            from data_layer.runtime_config import RuntimeConfig
            idx = index.upper()
            allowed = {"NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY"}
            if idx not in allowed:
                raise HTTPException(400, f"Unknown index '{idx}'. Allowed: {sorted(allowed)}")
            try:
                body = await request.json()
            except Exception:
                raise HTTPException(400, "Invalid JSON body.")

            # Validate and persist each strategy section if present
            if "sell_straddle" in body:
                RuntimeConfig.set_index_section(idx, "sell_straddle", body["sell_straddle"])
                for ss in (_srv._sell_straddles or []):
                    if getattr(ss, "_underlying", None) == idx:
                        try:
                            ss.reconfigure()
                        except Exception as exc:
                            logger.warning("reconfigure SS[%s]: %s", idx, exc)

            if "iron_condor" in body:
                RuntimeConfig.set_index_section(idx, "iron_condor", body["iron_condor"])
                for ic in (_srv._iron_condors or []):
                    if getattr(ic, "_underlying", None) == idx:
                        try:
                            ic.reconfigure()
                        except Exception as exc:
                            logger.warning("reconfigure IC[%s]: %s", idx, exc)

            logger.info("Dashboard: per-index config saved for %s.", idx)
            return {"ok": True, "message": f"Config for {idx} saved and injected into running strategies."}

        @app.get("/api/admin/strategy/config/all/indices", tags=["Admin"])
        async def api_all_indices_config(_: dict = Depends(_require_admin)):
            from data_layer.runtime_config import RuntimeConfig
            return RuntimeConfig.get_all_indices()

        @app.get("/api/admin/strategy/trap-config", tags=["Admin"])
        async def api_trap_config_get(_: dict = Depends(_require_admin)):
            from data_layer.runtime_config import RuntimeConfig
            cfg = RuntimeConfig.get("trap_trading") or {}
            return {"ok": True, "config": cfg}

        @app.post("/api/admin/strategy/trap-config", tags=["Admin"])
        async def api_trap_config_save(
            request: Request, _: dict = Depends(_require_admin),
        ):
            from data_layer.runtime_config import RuntimeConfig
            try:
                body = await request.json()
            except Exception:
                return {"ok": False, "error": "Invalid JSON body."}
            allowed = {"htf_minutes", "ltf_minutes", "adx_threshold",
                       "volume_spike_multiplier", "swing_lookback",
                       "zone_tolerance_pct", "void_atr_mult"}
            patch = {k: v for k, v in body.items() if k in allowed}
            if not patch:
                return {"ok": False, "error": "No valid fields provided."}
            RuntimeConfig.update({"trap_trading": patch})
            # Live-inject into running TrapTradingEngine instances
            te = getattr(_srv, "_trap_engine", None)
            if te and hasattr(te, "reconfigure"):
                try:
                    te.reconfigure(RuntimeConfig.get("trap_trading"))
                except Exception as exc:
                    logger.warning("Trap config live-inject failed: %s", exc)
            logger.info("Trap Trading config saved: %s", patch)
            return {"ok": True, "message": "Trap Trading config saved."}

        # ── ADMIN — portfolio risk command center ────────────────────────────

        @app.get("/api/admin/risk/summary", tags=["Admin"])
        async def api_risk_summary(_: dict = Depends(_require_admin)):
            rm = _srv._risk_manager
            if rm is None:
                # Fallback: build summary purely from registry state
                clients = _srv._registry.all_active() if _srv._registry else []
                total_capital  = sum(c.risk.capital    for c in clients)
                total_net_mtm  = sum(getattr(c, "_daily_pnl", 0.0) for c in clients)
                total_open_lots = 0
                return {
                    "ts":               datetime.now(IST).isoformat(),
                    "total_capital":    round(total_capital,  2),
                    "total_net_mtm":    round(total_net_mtm,  2),
                    "total_open_lots":  total_open_lots,
                    "avg_slippage_pts": 0.0,
                    "client_count":     len(clients),
                    "clients":          [_build_client_dict(c) for c in clients],
                    "clients_at_risk":  [],
                    "risk_manager_active": False,
                }
            try:
                summary = rm.risk_summary()
                summary["risk_manager_active"] = True
                return summary
            except Exception as exc:
                logger.warning("Dashboard: risk summary read failed: %s", exc)
                return {
                    "ts":               datetime.now(IST).isoformat(),
                    "risk_manager_active": False,
                    "error":            str(exc),
                }

        @app.post("/api/admin/risk/kill_all", tags=["Admin"])
        async def api_risk_kill_all(
            body: _KillAllConfirmSchema,
            _: dict = Depends(_require_admin),
        ):
            if not body.confirm:
                raise HTTPException(400, "confirm must be true to execute firm-wide kill-all.")
            rm = _srv._risk_manager
            if rm is not None:
                result = await rm.kill_all()
            else:
                # Fallback: halt all via registry
                if _srv._registry:
                    _srv._registry.halt_all()
                actioned = [c.client_id for c in (_srv._registry.all_active() if _srv._registry else [])]
                await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                    "event":     "KILL_SWITCH",
                    "scope":     "FIRM_WIDE",
                    "clients":   actioned,
                    "timestamp": datetime.now(IST).isoformat(),
                })
                result = {"status": "ok", "halted": actioned, "count": len(actioned),
                          "ts": datetime.now(IST).isoformat()}
            logger.warning("Dashboard: FIRM-WIDE KILL-ALL executed by admin. Result: %s", result)
            return result

        # ── ADMIN — approve client + assign strategies ────────────────────────

        @app.post("/api/admin/clients/{client_id}/approve", tags=["Admin"])
        async def api_approve_client(
            client_id: str,
            body: _ApproveClientSchema,
            _: dict = Depends(_require_admin),
        ):
            from config.client_profiles import ClientProfile, RiskProfile, BrokerBinding

            db_client = _srv._client_db.get_client_sync(client_id)
            if db_client is None:
                raise HTTPException(404, f"Client '{client_id}' not found in DB.")

            # Write strategy assignments and set approved flag
            for binding_id, strategy in body.strategy_assignments.items():
                await _srv._client_db.set_assigned_strategy(client_id, binding_id, strategy)
            await _srv._client_db.upsert_client(client_id, is_admin_approved=1)

            # Build ClientProfile and register in registry
            if _srv._registry is not None and _srv._registry.get(client_id) is None:
                strategies_raw = db_client.get("enabled_strategies") or "A,B,C"
                strategies = [s.strip() for s in strategies_raw.split(",") if s.strip()]
                risk = RiskProfile(
                    capital=float(db_client.get("capital", 500_000)),
                    max_risk_per_trade_pct=float(db_client.get("max_risk_pct", 1.0)),
                    max_daily_loss_pct=float(db_client.get("max_daily_loss_pct", 3.0)),
                )
                profile = ClientProfile(
                    client_id=client_id,
                    name=db_client.get("name", ""),
                    email=db_client.get("email", ""),
                    risk=risk,
                    enabled_strategies=strategies,
                    is_admin_approved=True,
                    target_index=db_client.get("target_index", "NIFTY"),
                )
                for b in _srv._client_db.get_bindings_sync(client_id):
                    strategy_for_b = body.strategy_assignments.get(b["binding_id"], b.get("assigned_strategy", ""))
                    profile.broker_bindings.append(BrokerBinding(
                        binding_id=b["binding_id"],
                        provider=b["provider"],      # type: ignore[arg-type]
                        label=b.get("label", ""),
                        user_id=b.get("user_id", ""),
                        api_key=b.get("api_key", ""),
                        api_secret=b.get("api_secret", ""),
                        totp_secret=b.get("totp_secret", ""),
                        access_token=b.get("access_token", ""),
                        lot_multiplier=float(b.get("lot_multiplier", 1.0)),
                        enabled=bool(b.get("enabled", 1)),
                        assigned_strategy=strategy_for_b,
                        is_trade_enabled=bool(b.get("is_trade_enabled", 1)),
                    ))
                try:
                    _srv._registry.register(profile)
                except ValueError:
                    pass  # already registered from a previous call

            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_APPROVED", "client_id": client_id,
            })
            logger.info("Dashboard: approved client %s with strategies %s.", client_id, body.strategy_assignments)
            return {"ok": True, "message": f"Client '{client_id}' approved and activated."}

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
                raw = {}
            # Strip all whitespace — hidden spaces in copy-pasted keys cause UDAPI100060.
            import re as _re
            _mobile_raw = str(raw.get("client_id", "") or "").strip()
            # Keep only digits; if 12-digit with leading 91 (country code) drop the prefix
            _mobile_digits = _re.sub(r"\D", "", _mobile_raw)
            if len(_mobile_digits) == 12 and _mobile_digits.startswith("91"):
                _mobile_digits = _mobile_digits[2:]
            form = {
                "client_id":   _mobile_digits,
                "api_key":     str(raw.get("api_key",     "") or "").strip(),
                "secret":      str(raw.get("secret",      "") or "").strip(),
                "password":    str(raw.get("password",    "") or "").strip(),
                "totp_secret": str(raw.get("totp_secret", "") or raw.get("totp", "") or "").strip(),
            }
            # Merge from DB if form fields are incomplete
            db_row = _srv._client_db.get_feeder_creds_sync("upstox") or {}
            creds = {
                "client_id":          form["client_id"]   or str(db_row.get("client_id",   "") or "").strip(),
                "api_key":            form["api_key"]     or str(db_row.get("api_key",     "") or "").strip(),
                "secret":             form["secret"]      or str(db_row.get("secret",      "") or "").strip(),
                "password":           form["password"]    or str(db_row.get("password",    "") or "").strip(),
                "totp_secret":        form["totp_secret"] or str(db_row.get("totp_secret", "") or "").strip(),
                "access_token":       db_row.get("access_token",       ""),
                "token_generated_at": db_row.get("token_generated_at", ""),
                "token_expiry_at":    db_row.get("token_expiry_at",    ""),
            }
            if not creds["client_id"] and not creds["api_key"]:
                raise HTTPException(400, "Upstox credentials are required. Enter them in the form or they must be cached in the database.")
            # Persist any newly-entered credentials to DB
            if form["client_id"] or form["api_key"]:
                await _srv._client_db.upsert_feeder_creds(
                    "upstox",
                    client_id=form["client_id"],
                    api_key=form["api_key"],
                    secret=form["secret"],
                    password=form["password"],
                    totp_secret=form["totp_secret"],
                )
            # Auto-generate access token via headless TOTP auth
            try:
                from broker_auth.headless_auth import headless_engine as _he
                ok, msg, token = await _he.validate_feeder_creds("upstox", creds)
                if ok and token:
                    creds["access_token"] = token
                    _srv._auth_alerts["feeder"] = ""
                    from broker_auth.headless_auth import _ist_eod
                    await _srv._client_db.update_feeder_token(
                        "upstox", token,
                        generated_at=datetime.now(IST).isoformat(),
                        expiry_at=_ist_eod(),
                    )
                else:
                    _srv._auth_alerts["feeder"] = f"Upstox auth failed: {msg}"
                    logger.error("Dashboard: Upstox headless auth failed: %s", msg)
                    await _srv._client_db.update_feeder_token(
                        "upstox", "", generated_at="", expiry_at="",
                    )
                    clean_error = _upstox_translate_error(msg)
                    return JSONResponse(status_code=502, content={"ok": False, "error": clean_error})
            except Exception as exc:
                _srv._auth_alerts["feeder"] = f"Upstox auth error: {exc}"
                logger.error("Dashboard: Upstox auth error: %s", exc)
                await _srv._client_db.update_feeder_token(
                    "upstox", "", generated_at="", expiry_at="",
                )
                clean_error = _upstox_translate_error(str(exc))
                return JSONResponse(status_code=502, content={"ok": False, "error": clean_error})
            try:
                await feeder.start_single("upstox", creds)
            except Exception as exc:
                logger.error("Dashboard: upstox connect failed: %s", exc)
                return JSONResponse(
                    status_code=502,
                    content={"ok": False, "error": f"Upstox connect failed: {exc}"},
                )
            return {"ok": True, "message": "Upstox feed stream initialized.", "provider": "upstox", "token_fresh": True}

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
                raw = {}
            # Fyers uses app_key / pin but we store as api_key / password internally
            form = {
                "client_id":   raw.get("client_id", "") or raw.get("fy_id", ""),
                "api_key":     raw.get("app_key", ""),
                "secret":      raw.get("secret", ""),
                "password":    raw.get("pin", ""),
                "totp_secret": raw.get("totp_secret", "") or raw.get("totp", ""),
            }
            # Merge from DB if form fields are incomplete
            db_row = _srv._client_db.get_feeder_creds_sync("fyers") or {}
            creds = {
                "client_id":         form["client_id"]   or db_row.get("client_id",   ""),
                "app_key":           form["api_key"]     or db_row.get("api_key",     ""),
                "secret":            form["secret"]      or db_row.get("secret",      ""),
                "pin":               form["password"]    or db_row.get("password",    ""),
                "totp_secret":       form["totp_secret"] or db_row.get("totp_secret", ""),
                "access_token":      db_row.get("access_token",      ""),
                "token_generated_at": db_row.get("token_generated_at", ""),
                "token_expiry_at":   db_row.get("token_expiry_at",   ""),
            }
            if not creds["client_id"] and not creds["app_key"]:
                raise HTTPException(400, "Fyers credentials are required. Enter them in the form or they must be cached in the database.")
            # Persist any newly-entered credentials to DB (api_key = app_key, password = pin)
            if form["client_id"] or form["api_key"]:
                await _srv._client_db.upsert_feeder_creds(
                    "fyers",
                    client_id=form["client_id"],
                    api_key=form["api_key"],
                    secret=form["secret"],
                    password=form["password"],
                    totp_secret=form["totp_secret"],
                )
            # Auto-generate access token via headless TOTP auth
            try:
                from broker_auth.headless_auth import headless_engine as _he
                ok, msg, token = await _he.validate_feeder_creds("fyers", creds)
                if ok and token:
                    creds["access_token"] = token
                    _srv._auth_alerts["feeder"] = ""
                    from broker_auth.headless_auth import _ist_eod
                    await _srv._client_db.update_feeder_token(
                        "fyers", token,
                        generated_at=datetime.now(IST).isoformat(),
                        expiry_at=_ist_eod(),
                    )
                else:
                    _srv._auth_alerts["feeder"] = f"Fyers auth failed: {msg}"
                    logger.error("Dashboard: Fyers headless auth failed: %s", msg)
                    # Invalidate cached token without touching credential fields
                    await _srv._client_db.update_feeder_token(
                        "fyers", "", generated_at="", expiry_at="",
                    )
                    return JSONResponse(
                        status_code=502,
                        content={"ok": False, "error": f"Fyers auth failed: {msg}"},
                    )
            except Exception as exc:
                _srv._auth_alerts["feeder"] = f"Fyers auth error: {exc}"
                logger.error("Dashboard: Fyers auth error: %s", exc)
                await _srv._client_db.update_feeder_token(
                    "fyers", "", generated_at="", expiry_at="",
                )
                return JSONResponse(
                    status_code=502,
                    content={"ok": False, "error": f"Fyers auth error: {exc}"},
                )
            try:
                await feeder.start_single("fyers", creds)
            except Exception as exc:
                logger.error("Dashboard: fyers connect failed: %s", exc)
                return JSONResponse(
                    status_code=502,
                    content={"ok": False, "error": f"Fyers connect failed: {exc}"},
                )
            return {"ok": True, "message": "Fyers feed stream initialized.", "provider": "fyers", "token_fresh": True}

        # ── ADMIN — Universal feeder OAuth flow (all providers) ──────────────

        @app.get("/api/admin/feeder/{provider_name}/auth-url", tags=["Admin"])
        async def api_admin_feeder_auth_url(
            provider_name: str,
            request: Request,
            _: dict = Depends(_require_admin),
        ):
            """Generate OAuth authorization URL for the admin data feeder."""
            from broker_auth.oauth_manager import generate_auth_url, build_state
            provider = provider_name.lower()
            db_row = _srv._client_db.get_feeder_creds_sync(provider) or {}
            api_key    = db_row.get("api_key", "")
            api_secret = db_row.get("secret", "")
            if not api_key:
                return {"ok": False, "error": f"{provider.upper()} App ID/Key not saved. Configure credentials first."}

            base_url     = _base_url(request)
            callback_url = f"{base_url}/callback/{provider}"
            state        = build_state("admin", "admin", provider)

            ok, url = generate_auth_url(provider, api_key, api_secret, callback_url, state)
            if ok:
                logger.info("[Admin] %s feeder OAuth URL generated → %s", provider.upper(), callback_url)
                return {"ok": True, "url": url, "callback_url": callback_url}
            return {"ok": False, "error": url}  # url contains instructions for manual providers

        # Keep legacy Fyers-specific URL for backward compat
        @app.get("/api/admin/feeder/fyers/auth-url", tags=["Admin"])
        async def api_fyers_auth_url(request: Request, _: dict = Depends(_require_admin)):
            from broker_auth.oauth_manager import generate_auth_url, build_state
            db_row = _srv._client_db.get_feeder_creds_sync("fyers") or {}
            api_key    = db_row.get("api_key", "")
            api_secret = db_row.get("secret", "")
            if not api_key:
                return {"ok": False, "error": "Fyers App ID not saved."}
            base_url     = _base_url(request)
            callback_url = f"{base_url}/callback/fyers"
            state        = build_state("admin", "admin", "fyers")
            ok, url = generate_auth_url("fyers", api_key, api_secret, callback_url, state)
            return {"ok": ok, "url": url}

        @app.post("/api/admin/feeder/fyers/exchange-code", tags=["Admin"])
        async def api_fyers_exchange_code(
            request: Request, _: dict = Depends(_require_admin),
        ):
            """Step 2 of the Fyers OAuth flow: exchange auth_code for access token."""
            feeder = _srv._feeder
            if feeder is None:
                raise HTTPException(503, "GlobalFeeder not wired to dashboard.")
            try:
                raw = await request.json()
            except Exception:
                raw = {}

            # Accept either a full redirect URL or just the raw auth_code
            redirect_url = raw.get("redirect_url", "")
            auth_code    = raw.get("auth_code", "")
            if redirect_url and not auth_code:
                from urllib.parse import urlparse, parse_qs
                qs = parse_qs(urlparse(redirect_url).query)
                auth_code = (qs.get("auth_code") or qs.get("code") or [""])[0]
            if not auth_code:
                return JSONResponse(
                    status_code=400,
                    content={"ok": False, "error": "auth_code not found. Paste the full redirect URL or just the auth_code value."},
                )

            db_row = _srv._client_db.get_feeder_creds_sync("fyers") or {}
            app_id     = db_row.get("api_key", "")
            secret_key = db_row.get("secret",  "")
            if not app_id or not secret_key:
                return JSONResponse(
                    status_code=400,
                    content={"ok": False, "error": "Fyers App ID and Secret not in DB. Save credentials first."},
                )

            try:
                import hashlib, requests as _rq
                app_id_hash = hashlib.sha256(f"{app_id}:{secret_key}".encode()).hexdigest()
                resp = await asyncio.to_thread(
                    lambda: _rq.post(
                        "https://api-t1.fyers.in/api/v3/validate-authcode",
                        json={
                            "grant_type": "authorization_code",
                            "appIdHash":  app_id_hash,
                            "code":       auth_code,
                        },
                        headers={"Content-Type": "application/json", "Accept": "application/json"},
                        timeout=15,
                    ).json()
                )

                access_token = resp.get("access_token") or (resp.get("data") or {}).get("access_token")
                if not access_token:
                    msg = resp.get("message") or str(resp)
                    _srv._auth_alerts["feeder"] = f"Fyers OAuth failed: {msg}"
                    return JSONResponse(status_code=502, content={"ok": False, "error": f"Fyers: {msg}"})

                from broker_auth.headless_auth import _ist_eod
                await _srv._client_db.update_feeder_token(
                    "fyers", access_token,
                    generated_at=datetime.now(IST).isoformat(),
                    expiry_at=_ist_eod(),
                )
                _srv._auth_alerts["feeder"] = ""
                logger.info("Dashboard: Fyers OAuth token saved successfully.")

                # Rebuild creds dict for feeder start
                creds = {
                    "client_id":          db_row.get("client_id", ""),
                    "app_key":            app_id,
                    "secret":             secret_key,
                    "pin":                db_row.get("password", ""),
                    "totp_secret":        db_row.get("totp_secret", ""),
                    "access_token":       access_token,
                    "token_generated_at": datetime.now(IST).isoformat(),
                    "token_expiry_at":    _ist_eod(),
                }
                try:
                    await feeder.start_single("fyers", creds)
                except Exception as exc:
                    logger.warning("Dashboard: Fyers feeder start after OAuth: %s", exc)
                    return {"ok": True, "message": "Token saved. Feeder start failed — restart manually.", "token_fresh": True}

                return {"ok": True, "message": "Fyers connected via OAuth — token refreshed.", "provider": "fyers", "token_fresh": True}

            except Exception as exc:
                logger.error("Dashboard: Fyers exchange-code error: %s", exc)
                return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})

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
                raw = {}

            # Merge form fields with DB fallback for each provider
            from broker_auth.headless_auth import headless_engine as _he, _ist_eod
            u_db = _srv._client_db.get_feeder_creds_sync("upstox") or {}
            f_db = _srv._client_db.get_feeder_creds_sync("fyers")  or {}

            u_form = {
                "client_id":   raw.get("upstox_client_id", ""),
                "api_key":     raw.get("upstox_api_key",   ""),
                "secret":      raw.get("upstox_secret",    ""),
                "password":    raw.get("upstox_password",  ""),
                "totp_secret": raw.get("upstox_totp",      ""),
            }
            f_form = {
                "client_id":   raw.get("fyers_client_id", ""),
                "api_key":     raw.get("fyers_app_key",   ""),
                "secret":      raw.get("fyers_secret",    ""),
                "password":    raw.get("fyers_pin",       ""),
                "totp_secret": raw.get("fyers_totp",      ""),
            }
            upstox_creds = {
                "client_id":   u_form["client_id"]   or u_db.get("client_id",   ""),
                "api_key":     u_form["api_key"]     or u_db.get("api_key",     ""),
                "secret":      u_form["secret"]      or u_db.get("secret",      ""),
                "password":    u_form["password"]    or u_db.get("password",    ""),
                "totp_secret": u_form["totp_secret"] or u_db.get("totp_secret", ""),
                "access_token":       u_db.get("access_token",      ""),
                "token_generated_at": u_db.get("token_generated_at",""),
                "token_expiry_at":    u_db.get("token_expiry_at",   ""),
            }
            fyers_creds = {
                "client_id":   f_form["client_id"]   or f_db.get("client_id",   ""),
                "app_key":     f_form["api_key"]     or f_db.get("api_key",     ""),
                "secret":      f_form["secret"]      or f_db.get("secret",      ""),
                "pin":         f_form["password"]    or f_db.get("password",    ""),
                "totp_secret": f_form["totp_secret"] or f_db.get("totp_secret", ""),
                "access_token":       f_db.get("access_token",      ""),
                "token_generated_at": f_db.get("token_generated_at",""),
                "token_expiry_at":    f_db.get("token_expiry_at",   ""),
            }
            # Persist any newly-entered form credentials to DB
            if u_form["client_id"] or u_form["api_key"]:
                await _srv._client_db.upsert_feeder_creds("upstox", **u_form)
            if f_form["client_id"] or f_form["api_key"]:
                await _srv._client_db.upsert_feeder_creds("fyers",  **f_form)

            # Auto-generate tokens for both providers via headless auth
            now_ist = datetime.now(IST).isoformat()
            eod     = _ist_eod()
            failures = []

            ok_u, msg_u, tok_u = await _he.validate_feeder_creds("upstox", upstox_creds)
            if ok_u and tok_u:
                upstox_creds["access_token"] = tok_u
                await _srv._client_db.update_feeder_token("upstox", tok_u, now_ist, eod)
            else:
                failures.append(f"Upstox: {msg_u}")
                upstox_creds = {}   # don't try to connect with a bad/missing token

            ok_f, msg_f, tok_f = await _he.validate_feeder_creds("fyers", fyers_creds)
            if ok_f and tok_f:
                fyers_creds["access_token"] = tok_f
                await _srv._client_db.update_feeder_token("fyers", tok_f, now_ist, eod)
            else:
                failures.append(f"Fyers: {msg_f}")
                fyers_creds = {}    # don't try to connect with a bad/missing token

            # Abort only if BOTH providers failed — partial success still starts the feeder
            if not ok_u and not ok_f:
                alert = "CRITICAL: " + " | ".join(failures)
                _srv._auth_alerts["feeder"] = alert
                logger.error("Dashboard: dual feeder auth failed (both providers): %s", alert)
                raise HTTPException(502, f"Both providers failed: {'; '.join(failures)}")

            if failures:
                _srv._auth_alerts["feeder"] = "WARNING: " + " | ".join(failures)
                logger.warning("Dashboard: dual feeder partial auth — %s", failures)
            else:
                _srv._auth_alerts["feeder"] = ""

            try:
                await feeder.start_dual(upstox_creds, fyers_creds)
            except Exception as exc:
                logger.error("Dashboard: start_dual failed: %s", exc)
                raise HTTPException(502, f"Dual feeder connect failed: {exc}")

            active = [p for p, ok in [("upstox", ok_u), ("fyers", ok_f)] if ok]
            warn   = f" | WARNING: {'; '.join(failures)}" if failures else ""
            return {
                "ok": True,
                "message": f"Dual feed active: {', '.join(active)}.{warn}",
                "provider": "dual",
                "active_providers": active,
            }

        # ── ADMIN — save-only credential endpoints ───────────────────────────

        @app.post("/api/admin/feeder/creds/upstox", tags=["Admin"])
        async def api_save_upstox_creds(
            body: _SaveUpstoxCredsSchema, _: dict = Depends(_require_admin),
        ):
            db = _srv._client_db
            if db is None:
                return JSONResponse(status_code=503, content={"ok": False, "error": "Database not available."})
            # Sanitise — Pydantic guarantees str but strip whitespace and guard None
            client_id   = str(body.client_id   or "").strip()
            api_key     = str(body.api_key     or "").strip()
            secret      = str(body.secret      or "").strip()
            password    = str(body.password    or "").strip()
            totp_secret = str(body.totp_secret or "").strip()
            if not any([client_id, api_key, secret, password, totp_secret]):
                return JSONResponse(status_code=400, content={"ok": False, "error": "At least one credential field must be provided."})
            try:
                await db.upsert_feeder_creds(
                    provider="upstox",
                    client_id=client_id,
                    api_key=api_key,
                    secret=secret,
                    password=password,
                    totp_secret=totp_secret,
                )
            except Exception as exc:
                logger.error("Dashboard: Upstox credential save failed: %s", exc, exc_info=exc)
                return JSONResponse(status_code=500, content={"ok": False, "error": f"Database write failed: {exc}"})
            logger.info("Dashboard: Upstox feeder credentials saved to DB.")
            return {"ok": True, "message": "Upstox credentials saved."}

        @app.post("/api/admin/feeder/creds/fyers", tags=["Admin"])
        async def api_save_fyers_creds(
            body: _SaveFyersCredsSchema, _: dict = Depends(_require_admin),
        ):
            db = _srv._client_db
            if db is None:
                return JSONResponse(status_code=503, content={"ok": False, "error": "Database not available."})
            client_id   = str(body.client_id   or "").strip()
            api_key     = str(body.app_key     or "").strip()
            secret      = str(body.secret      or "").strip()
            password    = str(body.pin         or "").strip()
            totp_secret = str(body.totp_secret or "").strip()
            if not any([client_id, api_key, secret, password, totp_secret]):
                return JSONResponse(status_code=400, content={"ok": False, "error": "At least one credential field must be provided."})
            try:
                await db.upsert_feeder_creds(
                    provider="fyers",
                    client_id=client_id,
                    api_key=api_key,
                    secret=secret,
                    password=password,
                    totp_secret=totp_secret,
                )
            except Exception as exc:
                logger.error("Dashboard: Fyers credential save failed: %s", exc, exc_info=exc)
                return JSONResponse(status_code=500, content={"ok": False, "error": f"Database write failed: {exc}"})
            logger.info("Dashboard: Fyers feeder credentials saved to DB.")
            return {"ok": True, "message": "Fyers credentials saved."}

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

        # ── ADMIN — live broker positions ────────────────────────────────────

        @app.get("/api/admin/positions", tags=["Admin"])
        async def api_positions(_: dict = Depends(_require_admin)):
            """Fetch open positions from all authenticated broker bindings."""
            router = _srv._router
            if router is None:
                return {"ok": True, "positions": []}
            results = []
            for client_id, bindings in router._brokers.items():
                for binding_id, broker in bindings.items():
                    try:
                        positions = await broker.get_positions()
                        for pos in positions:
                            if pos.qty == 0:
                                continue
                            results.append({
                                "client_id":  client_id,
                                "binding_id": binding_id,
                                "symbol":     pos.symbol,
                                "qty":        pos.qty,
                                "avg_price":  round(pos.avg_price, 2),
                                "pnl":        round(pos.pnl, 2),
                                "product":    pos.product,
                            })
                    except Exception as exc:
                        logger.warning("Dashboard: positions fetch failed %s/%s: %s",
                                       client_id, binding_id, exc)
            return {"ok": True, "positions": results}

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
            import json as _json
            clients = []
            for c in _srv._registry._clients.values():
                d = _build_client_dict(c)
                d["halted"]         = bool(getattr(c, "_halted", False))
                d["lot_multiplier"] = float(c.risk.size_multiplier)
                d["broker_bindings"] = [
                    {"binding_id": b.binding_id, "provider": b.provider, "enabled": b.enabled}
                    for b in c.broker_bindings
                ]
                # Include client's own strategy selections (set from client side)
                db_row = _srv._client_db.get_client_sync(c.client_id) or {}
                raw_sel = db_row.get("strategy_selections", "[]") or "[]"
                try:
                    d["strategy_selections"] = _json.loads(raw_sel)
                except Exception:
                    d["strategy_selections"] = []
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

    async def serve(self, host: str = "0.0.0.0", port: int = 5000) -> None:
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

        await self._client_db.initialise()

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

        # Kick off background boot-time feeder auto-connect after a short settle delay
        boot_task = asyncio.create_task(
            self._boot_feeder_auto_connect(), name="boot_feeder_auto_connect"
        )

        try:
            await asyncio.gather(
                self._uvicorn_server.serve(),
                self._ws_bridge.run(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            if not boot_task.done():
                boot_task.cancel()

    async def _boot_feeder_auto_connect(self) -> None:
        """
        On server startup, check DB for cached feeder credentials and
        auto-reconnect if found.  Runs after a 4-second settle delay so the
        feeder object is fully initialised before we touch it.
        """
        await asyncio.sleep(4.0)
        try:
            feeder = self._feeder
            if feeder is None:
                return

            from broker_auth.headless_auth import headless_engine as _he, _token_is_fresh, _ist_eod
            u_row = self._client_db.get_feeder_creds_sync("upstox") or {}
            f_row = self._client_db.get_feeder_creds_sync("fyers")  or {}

            has_upstox = bool(u_row.get("client_id") or u_row.get("api_key"))
            has_fyers  = bool(f_row.get("client_id") or f_row.get("api_key"))

            if not has_upstox and not has_fyers:
                logger.info("DashboardServer: No cached feeder credentials — skipping auto-connect.")
                return

            logger.info(
                "DashboardServer: Boot-time auto-connect (upstox=%s, fyers=%s).",
                has_upstox, has_fyers,
            )
            now_ist = datetime.now(IST).isoformat()
            eod     = _ist_eod()

            upstox_creds: dict = {}
            fyers_creds:  dict = {}

            if has_upstox:
                upstox_creds = {
                    "client_id":   u_row["client_id"],
                    "api_key":     u_row["api_key"],
                    "secret":      u_row["secret"],
                    "password":    u_row["password"],
                    "totp_secret": u_row["totp_secret"],
                    "access_token":       u_row.get("access_token", ""),
                    "token_generated_at": u_row.get("token_generated_at", ""),
                    "token_expiry_at":    u_row.get("token_expiry_at", ""),
                }
                # Use cached token if fresh, otherwise re-auth
                if not _token_is_fresh(u_row.get("token_generated_at",""), u_row.get("token_expiry_at","")):
                    ok, msg, tok = await _he.validate_feeder_creds("upstox", upstox_creds)
                    if ok and tok:
                        upstox_creds["access_token"] = tok
                        await self._client_db.update_feeder_token("upstox", tok, now_ist, eod)
                        self._auth_alerts["feeder"] = ""
                        logger.info("DashboardServer: Upstox boot-auth succeeded.")
                    else:
                        self._auth_alerts["feeder"] = f"Upstox boot-auth failed: {msg}"
                        logger.warning("DashboardServer: Upstox boot-auth failed: %s", msg)
                        has_upstox = False  # Don't try to connect with a bad token

            if has_fyers:
                fyers_creds = {
                    "client_id":   f_row["client_id"],
                    "app_key":     f_row["api_key"],
                    "secret":      f_row["secret"],
                    "pin":         f_row["password"],
                    "totp_secret": f_row["totp_secret"],
                    "access_token":       f_row.get("access_token", ""),
                    "token_generated_at": f_row.get("token_generated_at", ""),
                    "token_expiry_at":    f_row.get("token_expiry_at", ""),
                }
                if not _token_is_fresh(f_row.get("token_generated_at",""), f_row.get("token_expiry_at","")):
                    ok, msg, tok = await _he.validate_feeder_creds("fyers", fyers_creds)
                    if ok and tok:
                        fyers_creds["access_token"] = tok
                        await self._client_db.update_feeder_token("fyers", tok, now_ist, eod)
                        logger.info("DashboardServer: Fyers boot-auth succeeded.")
                    else:
                        logger.warning("DashboardServer: Fyers boot-auth failed: %s", msg)
                        has_fyers = False

            # Connect the feeder with whatever credentials are ready
            try:
                if has_upstox and has_fyers:
                    await feeder.start_dual(upstox_creds, fyers_creds)
                    logger.info("DashboardServer: Boot auto-connect — dual feed active.")
                elif has_upstox:
                    await feeder.start_single("upstox", upstox_creds)
                    logger.info("DashboardServer: Boot auto-connect — Upstox feed active.")
                elif has_fyers:
                    await feeder.start_single("fyers", fyers_creds)
                    logger.info("DashboardServer: Boot auto-connect — Fyers feed active.")
            except Exception as exc:
                logger.error("DashboardServer: Boot auto-connect feeder start failed: %s", exc)
        except Exception as exc:
            logger.warning("DashboardServer: _boot_feeder_auto_connect error: %s", exc)

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
