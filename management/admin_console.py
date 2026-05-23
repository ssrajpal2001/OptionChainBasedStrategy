"""
management/admin_console.py — Async REPL for runtime admin operations.

Provides a non-blocking command interface that runs inside the main asyncio
event loop. Commands are read from stdin via asyncio.to_thread so they never
block the trading event loop.

Available commands:
  status                  — Show all clients, status, P&L, broker bindings
  halt <client_id>        — Immediately halt a client (no new orders)
  resume <client_id>      — Re-enable a halted client
  halt_all                — Halt all clients
  reset_daily             — Reset daily P&L counters (normally auto at 09:15)
  funds <client_id>       — Fetch live fund balances from broker
  positions <client_id>   — Fetch live positions from broker
  add_client <json>       — Register a new client profile at runtime
  set_lots <id> <binding> <n> — Override lot_multiplier for a binding
  drop_counts             — Show EventBus message drop counts per topic
  quit                    — Graceful shutdown
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import TYPE_CHECKING, Callable, Awaitable, Optional

from config.client_profiles import ClientRegistry, ClientProfile, BrokerBinding, RiskProfile
from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus

if TYPE_CHECKING:
    from execution_bridge.execution_router import ExecutionRouter

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
        shutdown_callback: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        self._bus = bus
        self._registry = registry
        self._router = router
        self._shutdown = shutdown_callback
        self._running = False

    async def run(self) -> None:
        self._running = True
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

    # ── Command Dispatcher ────────────────────────────────────────────────────

    async def _dispatch(self, line: str) -> None:
        parts = line.split()
        cmd = parts[0].lower()
        args = parts[1:]

        handlers = {
            "help":        self._cmd_help,
            "status":      self._cmd_status,
            "halt":        self._cmd_halt,
            "resume":      self._cmd_resume,
            "halt_all":    self._cmd_halt_all,
            "reset_daily": self._cmd_reset_daily,
            "funds":       self._cmd_funds,
            "positions":   self._cmd_positions,
            "add_client":  self._cmd_add_client,
            "set_lots":    self._cmd_set_lots,
            "drop_counts": self._cmd_drop_counts,
            "quit":        self._cmd_quit,
            "exit":        self._cmd_quit,
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
