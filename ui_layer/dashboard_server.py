"""
ui_layer/dashboard_server.py — FastAPI async web dashboard server.

Creates a FastAPI application with:
  • GET  /                          — serves monitor.html
  • GET  /api/status                — all client status summary
  • GET  /api/workers               — execution worker stats
  • POST /api/halt/{client_id}      — halt a single client
  • POST /api/resume/{client_id}    — resume a halted client
  • POST /api/halt_all              — halt every client
  • POST /api/kill_switch           — halt all + KILL_SWITCH event
  • POST /api/rebalance/{index}     — force ATM rebalance for an underlying
  • POST /api/set_lots/{cid}/{bid}/{m} — override lot multiplier
  • WS   /ws                        — live event stream to browser

The server runs inside the existing asyncio event loop via uvicorn's
programmatic API (uvicorn.Server.serve()) so it is fully non-blocking and
starts/stops alongside the trading engine tasks.

AdminConsole integration (spec requirement):
    dashboard = DashboardServer(bus, cfg, registry, router=router,
                                rebalancer=rebalancer)
    # AdminConsole creates the background task automatically when wired:
    admin = AdminConsole(..., dashboard_server=dashboard, dashboard_port=8080)

No time.sleep.  All async.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import List, Optional

from config.global_config import IST, Topic, SysEvent
from data_layer.base_feeder import EventBus
from ui_layer.ws_bridge import WsBridge

logger = logging.getLogger(__name__)

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
_MONITOR_HTML = os.path.join(_TEMPLATE_DIR, "monitor.html")


class DashboardServer:
    """
    FastAPI-based async admin dashboard.

    Owns a WsBridge that relays EventBus events to connected browsers.
    Exposes REST endpoints for all admin actions exposed by AdminConsole.

    Usage (standalone):
        dashboard = DashboardServer(bus, cfg, registry, router=router,
                                    rebalancer=rebalancer)
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
        router=None,      # ExecutionRouter — worker stats + broker access
        rebalancer=None,  # StrikeRebalancer — ATM + manual rebalance
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._registry = registry
        self._router = router
        self._rebalancer = rebalancer
        self._ws_bridge = WsBridge(bus, cfg=cfg)
        self._uvicorn_server = None   # set in serve()

        # Register heartbeat providers
        self._ws_bridge.register_stats_provider("clients", self._client_summary)
        if router is not None:
            self._ws_bridge.register_stats_provider(
                "workers",
                lambda: self._router.worker_stats() if self._router else [],
            )

        self._app = self._build_app()

    @property
    def ws_bridge(self) -> WsBridge:
        return self._ws_bridge

    # ── FastAPI application ───────────────────────────────────────────────────

    def _build_app(self):
        try:
            from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
            from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
        except ImportError as exc:
            raise ImportError(
                "FastAPI not installed. Run: pip install fastapi uvicorn[standard]"
            ) from exc

        app = FastAPI(
            title="OptionChain AlgoTrader",
            description="Real-time multi-tenant options trading monitor",
            version="1.0.0",
            docs_url="/api/docs",
        )
        # Store self reference for use inside route closures
        _srv = self
        bridge = self._ws_bridge

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

        # ── REST — read-only ──────────────────────────────────────────────────

        @app.get("/api/status")
        async def api_status():
            return {
                "ts":      datetime.now(IST).isoformat(),
                "clients": _srv._client_summary(),
            }

        @app.get("/api/workers")
        async def api_workers():
            stats = _srv._router.worker_stats() if _srv._router else []
            return {"workers": stats}

        # ── REST — admin mutations ────────────────────────────────────────────

        @app.post("/api/halt/{client_id}")
        async def api_halt(client_id: str):
            client = _srv._registry.get(client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {client_id!r} not found.")
            client.halt()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_HALTED", "client_id": client_id, "reason": "web_dashboard",
            })
            logger.info("Dashboard: halted client %s.", client_id)
            return {"ok": True, "message": f"Client {client_id} halted."}

        @app.post("/api/resume/{client_id}")
        async def api_resume(client_id: str):
            client = _srv._registry.get(client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {client_id!r} not found.")
            client.resume()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event": "CLIENT_RESUMED", "client_id": client_id,
            })
            return {"ok": True, "message": f"Client {client_id} resumed."}

        @app.post("/api/halt_all")
        async def api_halt_all():
            if _srv._registry:
                _srv._registry.halt_all()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {"event": "ALL_HALTED"})
            return {"ok": True, "message": "All clients halted."}

        @app.post("/api/kill_switch")
        async def api_kill_switch():
            """Emergency kill: halt all clients and broadcast KILL_SWITCH event."""
            if _srv._registry:
                _srv._registry.halt_all()
            await _srv._bus.publish(Topic.SYSTEM_EVENT, {
                "event":   SysEvent.KILL_SWITCH,
                "message": "Emergency kill switch activated via web dashboard.",
                "ts":      datetime.now(IST).isoformat(),
            })
            logger.critical("Dashboard: KILL SWITCH activated.")
            return {"ok": True, "message": "KILL SWITCH activated — all clients halted immediately."}

        @app.post("/api/rebalance/{underlying}")
        async def api_rebalance(underlying: str):
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

        @app.post("/api/set_lots/{client_id}/{binding_id}/{multiplier}")
        async def api_set_lots(client_id: str, binding_id: str, multiplier: float):
            client = _srv._registry.get(client_id) if _srv._registry else None
            if client is None:
                raise HTTPException(404, f"Client {client_id!r} not found.")
            for b in client.broker_bindings:
                if b.binding_id == binding_id:
                    b.lot_multiplier = multiplier
                    return {"ok": True, "message": f"{client_id}/{binding_id} lot_multiplier → {multiplier}"}
            raise HTTPException(404, f"Binding {binding_id!r} not found for {client_id!r}.")

        # ── WebSocket endpoint ────────────────────────────────────────────────

        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            bridge.add_connection(websocket)
            try:
                # Keep connection alive; we ignore client-to-server messages
                while True:
                    await websocket.receive_text()
            except (WebSocketDisconnect, Exception):
                pass
            finally:
                bridge.remove_connection(websocket)

        return app

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def serve(self, host: str = "0.0.0.0", port: int = 8080) -> None:
        """
        Start uvicorn + WsBridge in the current asyncio event loop.

        Designed to run as an asyncio.create_task() so it does not block
        the admin console, trading engine, or other system tasks.
        """
        try:
            import uvicorn
        except ImportError:
            logger.error(
                "uvicorn not installed — dashboard will not start. "
                "Install with: pip install uvicorn[standard]"
            )
            return

        config = uvicorn.Config(
            app=self._app,
            host=host,
            port=port,
            log_level="warning",
            loop="none",     # Use the already-running event loop, never spawn a new one
            lifespan="off",
        )
        self._uvicorn_server = uvicorn.Server(config)
        # Prevent uvicorn from installing its own SIGINT/SIGTERM handlers
        # so the main process's shutdown logic stays in control.
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
        """Signal both uvicorn and the WsBridge to stop cleanly."""
        self._ws_bridge.stop()
        if self._uvicorn_server is not None:
            self._uvicorn_server.should_exit = True

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _client_summary(self) -> List[dict]:
        if self._registry is None:
            return []
        result = []
        for c in self._registry.all_active():
            result.append({
                "client_id":      c.client_id,
                "name":           getattr(c, "name", c.client_id),
                "tradeable":      c.is_tradeable(),
                "daily_pnl":      round(float(getattr(c, "_daily_pnl", 0.0)), 2),
                "capital":        float(c.risk.capital),
                "max_risk_pct":   float(c.risk.max_risk_per_trade_pct),
                "daily_loss_pct": float(c.risk.max_daily_loss_pct),
                "strategies":     list(c.enabled_strategies),
                "brokers":        [b.provider for b in c.broker_bindings if b.enabled],
            })
        return result
