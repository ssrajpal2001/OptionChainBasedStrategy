"""
ui_layer — Async web dashboard package.

Exports:
  DashboardServer  — FastAPI + Uvicorn server wired to the EventBus
  WsBridge         — EventBus-to-WebSocket broadcast loop
"""

from ui_layer.ws_bridge import WsBridge
from ui_layer.dashboard_server import DashboardServer

__all__ = ["DashboardServer", "WsBridge"]
