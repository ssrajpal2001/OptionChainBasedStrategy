"""
management/admin_console.py — Async REPL for runtime admin operations.

Provides a non-blocking command interface that runs inside the main asyncio
event loop. Commands are read from stdin via asyncio.to_thread so they never
block the trading event loop.

If a DashboardServer is wired in via the dashboard_server parameter, the
console starts it as a background asyncio.create_task() on first run — the
web UI server runs completely independently of the REPL loop.

Available commands:
  status                        — Show all clients, status, P&L, broker bindings
  halt <client_id>              — Immediately halt a client (no new orders)
  resume <client_id>            — Re-enable a halted client
  halt_all                      — Halt all clients
  reset_daily                   — Reset daily P&L counters (normally auto at 09:15)
  funds <client_id>             — Fetch live fund balances from broker
  positions <client_id>         — Fetch live positions from broker
  add_client <json>             — Register a new client profile at runtime
  set_lots <id> <binding> <n>   — Override lot_multiplier for a binding
  drop_counts                   — Show EventBus message drop counts per topic
  worker_stats                  — Show per-client execution worker queue depths
  state_status                  — Show SQLite snapshot stats (flush count, last save)
  state_restore                 — Reload indicator + strategy state from last snapshot
  rebalance <underlying>        — Force immediate ATM strike rebalance for underlying
  dashboard                     — Show web dashboard URL and connection count
  quit                          — Graceful shutdown
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Awaitable, Optional

from config.client_profiles import ClientRegistry, ClientProfile, BrokerBinding, RiskProfile
from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus

if TYPE_CHECKING:
    from execution_bridge.execution_router import ExecutionRouter
    from matrix_engine.state_persistence import StatePersistence
    from data_layer.strike_rebalancer import StrikeRebalancer
    from ui_layer.dashboard_server import DashboardServer

logger = logging.getLogger(__name__)

_HELP = __doc__


class AdminConsole:
    """
    Async REPL wired into the main event loop.

    shutdown_callback is called when the operator types 'quit'.
    """

    def __init__(
        self,
        bus: EventBus,
        registry: ClientRegistry,
        router: Optional["ExecutionRouter"] = None,
        state_persistence: Optional["StatePersistence"] = None,
        strike_rebalancer: Optional["StrikeRebalancer"] = None,
        shutdown_callback: Optional[Callable[[], Awaitable[None]]] = None,
        dashboard_server: Optional["DashboardServer"] = None,
        dashboard_port: int = 8080,
        dashboard_host: str = "0.0.0.0",
    ) -> None:
        self._bus = bus
        self._registry = registry
        self._router = router
        self._state = state_persistence
        self._rebalancer = strike_rebalancer
        self._shutdown = shutdown_callback
        self._dashboard = dashboard_server
        self._dashboard_port = dashboard_port
        self._dashboard_host = dashboard_host
        self._dashboard_task: Optional[asyncio.Task] = None
        self._running = False

    async def run(self) -> None:
        self._running = True

        # Start web dashboard as a fully-independent background task
        if self._dashboard is not None and self._dashboard_task is None:
            self._dashboard_task = asyncio.create_task(
                self._dashboard.serve(
                    host=self._dashboard_host,
                    port=self._dashboard_port,
                ),
                name="dashboard_server",
            )
            print(
                f"\n[Dashboard] http://{self._dashboard_host}:{self._dashboard_port}  "
                f"(WebSocket: ws://{self._dashboard_host}:{self._dashboard_port}/ws)\n",
                flush=True,
            )

        print("\n[AdminConsole] Ready.  Type 'help' for commands.\n", flush=True)
        while self._running:
            try:
                line: str = await asyncio.to_thread(self._readline)
            except (EOFError, KeyboardInterrupt):
                break
            line = line.strip()
            if not line:
                continue
            await self._dispatch(line)

    async def stop(self) -> None:
        self._running = False
        if self._dashboard is not None:
            self._dashboard.stop()
        if self._dashboard_task is not None and not self._dashboard_task.done():
            self._dashboard_task.cancel()
            try:
                await self._dashboard_task
            except (asyncio.CancelledError, Exception):
                pass

    # ── Command Dispatcher ────────────────────────────────────────────────────

    async def _dispatch(self, line: str) -> None:
        parts = line.split()
        cmd = parts[0].lower()
        args = parts[1:]

        handlers = {
            "help":          self._cmd_help,
            "status":        self._cmd_status,
            "halt":          self._cmd_halt,
            "resume":        self._cmd_resume,
            "halt_all":      self._cmd_halt_all,
            "reset_daily":   self._cmd_reset_daily,
            "funds":         self._cmd_funds,
            "positions":     self._cmd_positions,
            "add_client":    self._cmd_add_client,
            "set_lots":      self._cmd_set_lots,
            "drop_counts":   self._cmd_drop_counts,
            "worker_stats":  self._cmd_worker_stats,
            "state_status":  self._cmd_state_status,
            "state_restore": self._cmd_state_restore,
            "rebalance":     self._cmd_rebalance,
            "dashboard":     self._cmd_dashboard,
            "quit":          self._cmd_quit,
            "exit":          self._cmd_quit,
        }
        handler = handlers.get(cmd)
        if handler:
            await handler(args)
        else:
            print(f"Unknown command '{cmd}'. Type 'help'.", flush=True)

    # ── Commands ──────────────────────────────────────────────────────────────

    async def _cmd_help(self, _: list) -> None:
        print(_HELP, flush=True)

    async def _cmd_status(self, _: list) -> None:
        clients = self._registry.all_active()
        if not clients:
            print("No clients registered.", flush=True)
            return
        print(f"\n{'ID':<15} {'Tradeable':<10} {'Daily P&L':>12} {'Strategies':<20} {'Brokers'}", flush=True)
        print("-" * 80, flush=True)
        for c in clients:
            brokers = ", ".join(b.provider for b in c.broker_bindings if b.enabled)
            strats = ", ".join(c.enabled_strategies)
            tradeable = "YES" if c.is_tradeable() else "HALTED"
            print(
                f"{c.client_id:<15} {tradeable:<10} {c._daily_pnl:>12.2f} "
                f"{strats:<20} {brokers}",
                flush=True,
            )
        print(flush=True)

    async def _cmd_halt(self, args: list) -> None:
        if not args:
            print("Usage: halt <client_id>", flush=True)
            return
        client = self._registry.get(args[0])
        if client is None:
            print(f"Client '{args[0]}' not found.", flush=True)
            return
        client.halt()
        await self._bus.publish(Topic.SYSTEM_EVENT, {
            "event": "CLIENT_HALTED", "client_id": args[0], "reason": "admin_console",
        })
        print(f"Client {args[0]} HALTED.", flush=True)

    async def _cmd_resume(self, args: list) -> None:
        if not args:
            print("Usage: resume <client_id>", flush=True)
            return
        client = self._registry.get(args[0])
        if client is None:
            print(f"Client '{args[0]}' not found.", flush=True)
            return
        client.resume()
        await self._bus.publish(Topic.SYSTEM_EVENT, {
            "event": "CLIENT_RESUMED", "client_id": args[0],
        })
        print(f"Client {args[0]} RESUMED.", flush=True)

    async def _cmd_halt_all(self, _: list) -> None:
        self._registry.halt_all()
        await self._bus.publish(Topic.SYSTEM_EVENT, {"event": "ALL_HALTED"})
        print("All clients halted.", flush=True)

    async def _cmd_reset_daily(self, _: list) -> None:
        self._registry.reset_all_daily()
        print("Daily P&L counters reset.", flush=True)

    async def _cmd_funds(self, args: list) -> None:
        if not self._router or not args:
            print("Usage: funds <client_id>  (requires live router)", flush=True)
            return
        client_id = args[0]
        brokers = self._router._brokers.get(client_id, {})
        if not brokers:
            print(f"No authenticated brokers for {client_id}.", flush=True)
            return
        for binding_id, broker in brokers.items():
            funds = await broker.get_funds()
            print(
                f"  {client_id}/{binding_id}: available={funds['available']:.2f}  used={funds['used']:.2f}",
                flush=True,
            )

    async def _cmd_positions(self, args: list) -> None:
        if not self._router or not args:
            print("Usage: positions <client_id>  (requires live router)", flush=True)
            return
        client_id = args[0]
        brokers = self._router._brokers.get(client_id, {})
        if not brokers:
            print(f"No authenticated brokers for {client_id}.", flush=True)
            return
        for binding_id, broker in brokers.items():
            positions = await broker.get_positions()
            if not positions:
                print(f"  {client_id}/{binding_id}: No open positions.", flush=True)
                continue
            for p in positions:
                print(
                    f"  {client_id}/{binding_id}: {p.symbol}  qty={p.qty}  "
                    f"avg={p.avg_price:.2f}  pnl={p.pnl:.2f}",
                    flush=True,
                )

    async def _cmd_add_client(self, args: list) -> None:
        """
        Usage: add_client <json_string>

        Minimal JSON:
          {"client_id":"C99","capital":500000,"max_risk_pct":1.0,
           "strategies":["A","B"],"expiry":"CURRENT_WEEK","broker":"mock"}
        """
        if not args:
            print("Usage: add_client <json_string>", flush=True)
            return
        try:
            data = json.loads(" ".join(args))
            risk = RiskProfile(
                capital=float(data.get("capital", 500_000)),
                max_risk_per_trade_pct=float(data.get("max_risk_pct", 1.0)),
                max_daily_loss_pct=float(data.get("max_daily_loss_pct", 3.0)),
            )
            binding = BrokerBinding(
                binding_id=f"{data['client_id']}_default",
                provider=data.get("broker", "mock"),
            )
            profile = ClientProfile(
                client_id=data["client_id"],
                risk=risk,
                broker_bindings=[binding],
                enabled_strategies=data.get("strategies", ["A"]),
                expiry_preference=data.get("expiry", "CURRENT_WEEK"),
            )
            self._registry.register(profile)
            print(f"Client {data['client_id']} registered.", flush=True)
        except Exception as exc:
            print(f"add_client error: {exc}", flush=True)

    async def _cmd_set_lots(self, args: list) -> None:
        if len(args) < 3:
            print("Usage: set_lots <client_id> <binding_id> <multiplier>", flush=True)
            return
        client = self._registry.get(args[0])
        if client is None:
            print(f"Client '{args[0]}' not found.", flush=True)
            return
        for b in client.broker_bindings:
            if b.binding_id == args[1]:
                b.lot_multiplier = float(args[2])
                print(f"Set {args[0]}/{args[1]} lot_multiplier={args[2]}", flush=True)
                return
        print(f"Binding '{args[1]}' not found for client '{args[0]}'.", flush=True)

    async def _cmd_drop_counts(self, _: list) -> None:
        counts = self._bus.drop_counts()
        if not counts:
            print("No drops recorded.", flush=True)
            return
        print("\nEventBus drop counts:", flush=True)
        for topic, count in sorted(counts.items()):
            print(f"  {topic}: {count}", flush=True)
        print(flush=True)

    async def _cmd_worker_stats(self, _: list) -> None:
        """Show per-client execution worker queue depths and throughput."""
        if not self._router:
            print("No router attached.", flush=True)
            return
        stats = self._router.worker_stats()
        if not stats:
            print("No active workers.", flush=True)
            return
        print(f"\n{'Client':<15} {'Q Depth':>8} {'Processed':>10} {'Dropped':>8}", flush=True)
        print("-" * 45, flush=True)
        for s in stats:
            print(
                f"{s['client_id']:<15} {s['queue_depth']:>8} "
                f"{s['processed']:>10} {s['dropped']:>8}",
                flush=True,
            )
        print(flush=True)

    async def _cmd_state_status(self, _: list) -> None:
        """Show state persistence stats: flush count, DB path, file size."""
        if self._state is None:
            print("StatePersistence not attached (pass state_persistence= to AdminConsole).", flush=True)
            return
        db_path = self._state._db_path
        flush_count = self._state.flush_count
        size_kb = 0
        try:
            size_kb = os.path.getsize(db_path) // 1024
        except OSError:
            pass
        print(f"\nState Persistence:", flush=True)
        print(f"  DB path:     {db_path}", flush=True)
        print(f"  DB size:     {size_kb} KB", flush=True)
        print(f"  Flush count: {flush_count}", flush=True)
        print(f"  Timestamp:   {datetime.now(IST).strftime('%H:%M:%S IST')}", flush=True)
        print(flush=True)

    async def _cmd_state_restore(self, _: list) -> None:
        """
        Reload indicator and strategy state from the most recent SQLite snapshot.
        Prints a summary of what was restored.
        """
        if self._state is None:
            print("StatePersistence not attached.", flush=True)
            return
        restored = await asyncio.to_thread(self._state.restore_state)
        n_snap  = len(restored.get("snapshots", {}))
        n_strat = len(restored.get("strategy_b", {}))
        n_orders = len(restored.get("order_tickets", []))
        n_risk   = len(restored.get("risk_params", {}))
        print(f"\nState restored from {self._state._db_path}:", flush=True)
        print(f"  Tech snapshots:    {n_snap}", flush=True)
        print(f"  Strategy B states: {n_strat}", flush=True)
        print(f"  Open order tickets: {n_orders}", flush=True)
        print(f"  Risk param sets:   {n_risk}", flush=True)
        if n_strat:
            print("\n  Strategy B state:", flush=True)
            for und, st in restored["strategy_b"].items():
                print(
                    f"    {und}: phase={st.get('phase')}  "
                    f"rolling_base={st.get('rolling_base', 0):.0f}  "
                    f"htf_level={st.get('htf_entry_level', 0):.0f}",
                    flush=True,
                )
        print(flush=True)

    async def _cmd_rebalance(self, args: list) -> None:
        """
        Force an immediate ATM strike rebalance for an underlying.
        Usage: rebalance <underlying>   e.g.  rebalance NIFTY
        """
        if not args:
            print("Usage: rebalance <underlying>   e.g.  rebalance NIFTY", flush=True)
            return
        underlying = args[0].upper()
        if self._rebalancer is None:
            print("StrikeRebalancer not attached (pass strike_rebalancer= to AdminConsole).", flush=True)
            return
        state = self._rebalancer._state.get(underlying)
        if state is None:
            print(f"Unknown underlying '{underlying}'.", flush=True)
            return
        print(f"Forcing rebalance for {underlying} ...", flush=True)
        # Reset current_atm to force rebalance on next tick
        state.current_atm = None
        state.open_atm = None
        stats = self._rebalancer.rebalance_stats()
        print(
            f"  {underlying}: ATM baseline cleared. Rebalance will trigger on next tick.\n"
            f"  Total rebalances so far: {stats.get(underlying, 0)}",
            flush=True,
        )

    async def _cmd_dashboard(self, _: list) -> None:
        """Show web dashboard URL and current WebSocket connection count."""
        if self._dashboard is None:
            print("Dashboard not configured (pass dashboard_server= to AdminConsole).", flush=True)
            return
        bridge = self._dashboard.ws_bridge
        n = len(bridge._connections)
        host = self._dashboard_host if self._dashboard_host != "0.0.0.0" else "localhost"
        print(
            f"\n  Dashboard URL:   http://{host}:{self._dashboard_port}\n"
            f"  WebSocket:       ws://{host}:{self._dashboard_port}/ws\n"
            f"  Active clients:  {n}\n",
            flush=True,
        )

    async def _cmd_quit(self, _: list) -> None:
        print("Shutting down...", flush=True)
        self._running = False
        if self._shutdown:
            await self._shutdown()

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _readline() -> str:
        try:
            return input("admin> ")
        except EOFError:
            return "quit"
