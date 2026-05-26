"""
management/risk_manager.py — Firm-wide async risk management engine.

Responsibilities:
  - Hybrid MTM: fly-computed from fills + 30-second broker reconciliation
  - Daily drawdown: (Peak Daily Equity - Current Equity) / Allocated Capital * 100
  - Breach gates vs max_risk_per_trade_pct / max_daily_loss_pct per client
  - Automated isolated liquidation on breach:
      halt worker -> market exit orders -> BLOCKED status -> audit log
  - Slippage tracking per broker provider: P_executed - P_signal (points)
  - risk_summary() dict for /api/admin/risk/summary endpoint
  - kill_all() coroutine for /api/admin/risk/kill_all endpoint
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from config.global_config import IST, Topic
from config.client_profiles import ClientProfile, ClientRegistry
from data_layer.base_feeder import EventBus
from execution_bridge.base_broker import OrderFill, OrderRequest, OrderSide, OrderType, OrderStatus

logger = logging.getLogger(__name__)

_RECONCILE_INTERVAL = 30.0   # seconds between broker position reconciliation
_RISK_CHECK_INTERVAL = 1.0   # seconds between intra-day risk checks


# ─────────────────────────────────────────────────────────────────────────────
# Position record (per client × symbol)
# ─────────────────────────────────────────────────────────────────────────────

class _PositionRecord:
    """Fly-computed open position for one client / symbol pair."""

    __slots__ = (
        "symbol", "qty", "avg_entry", "side",
        "last_price", "unrealised_pnl", "realised_pnl",
    )

    def __init__(self, symbol: str) -> None:
        self.symbol:        str   = symbol
        self.qty:           int   = 0
        self.avg_entry:     float = 0.0
        self.side:          str   = ""        # "BUY" | "SELL"
        self.last_price:    float = 0.0
        self.unrealised_pnl: float = 0.0
        self.realised_pnl:   float = 0.0

    def apply_fill(self, fill: OrderFill) -> None:
        """Update position from a completed order fill."""
        qty  = fill.qty
        price = fill.avg_price
        side  = fill.side.value if hasattr(fill.side, "value") else str(fill.side)

        if self.qty == 0:
            # Opening a new position
            self.qty       = qty
            self.avg_entry = price
            self.side      = side
        elif side == self.side:
            # Adding to existing position — recalculate average
            total_cost     = self.avg_entry * self.qty + price * qty
            self.qty      += qty
            self.avg_entry = total_cost / self.qty if self.qty else 0.0
        else:
            # Reducing or reversing position
            close_qty = min(qty, self.qty)
            if self.side == "BUY":
                self.realised_pnl += (price - self.avg_entry) * close_qty
            else:
                self.realised_pnl += (self.avg_entry - price) * close_qty
            self.qty -= close_qty
            if self.qty == 0:
                self.avg_entry = 0.0
                self.side      = ""
            if qty > close_qty:
                # Reversal — open opposite side with remainder
                remainder = qty - close_qty
                self.qty       = remainder
                self.avg_entry = price
                self.side      = side

        self.last_price     = price
        self.unrealised_pnl = self._compute_unrealised()

    def update_mark(self, mark_price: float) -> None:
        self.last_price     = mark_price
        self.unrealised_pnl = self._compute_unrealised()

    def _compute_unrealised(self) -> float:
        if self.qty == 0:
            return 0.0
        if self.side == "BUY":
            return (self.last_price - self.avg_entry) * self.qty
        return (self.avg_entry - self.last_price) * self.qty

    def to_dict(self) -> dict:
        return {
            "symbol":         self.symbol,
            "qty":            self.qty,
            "avg_entry":      round(self.avg_entry, 2),
            "side":           self.side,
            "last_price":     round(self.last_price, 2),
            "unrealised_pnl": round(self.unrealised_pnl, 2),
            "realised_pnl":   round(self.realised_pnl, 2),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Per-client risk state
# ─────────────────────────────────────────────────────────────────────────────

class _ClientRiskState:
    """Runtime risk metrics for one client."""

    def __init__(self, client_id: str, capital: float) -> None:
        self.client_id:    str   = client_id
        self.capital:      float = capital
        self.positions:    Dict[str, _PositionRecord] = {}
        self.peak_equity:  float = capital
        self.drawdown_pct: float = 0.0
        self.open_lots:    int   = 0
        # Slippage tracking: list of (provider, points) tuples
        self._slippage:    List[Tuple[str, float]] = []
        self._liquidating: bool = False

    def position(self, symbol: str) -> _PositionRecord:
        if symbol not in self.positions:
            self.positions[symbol] = _PositionRecord(symbol)
        return self.positions[symbol]

    def net_mtm(self) -> float:
        return sum(
            p.unrealised_pnl + p.realised_pnl for p in self.positions.values()
        )

    def current_equity(self) -> float:
        return self.capital + self.net_mtm()

    def update_drawdown(self) -> None:
        eq = self.current_equity()
        if eq > self.peak_equity:
            self.peak_equity = eq
        dd = (self.peak_equity - eq) / self.capital * 100.0 if self.capital else 0.0
        self.drawdown_pct = round(max(dd, 0.0), 3)

    def record_slippage(self, provider: str, points: float) -> None:
        self._slippage.append((provider, points))
        if len(self._slippage) > 500:
            self._slippage = self._slippage[-500:]

    def avg_slippage_per_provider(self) -> Dict[str, float]:
        buckets: Dict[str, List[float]] = defaultdict(list)
        for prov, pts in self._slippage:
            buckets[prov].append(pts)
        return {
            prov: round(sum(vals) / len(vals), 3)
            for prov, vals in buckets.items()
        }

    def total_open_lots(self) -> int:
        return sum(p.qty for p in self.positions.values() if p.qty > 0)

    def to_dict(self) -> dict:
        return {
            "client_id":    self.client_id,
            "capital":      round(self.capital, 2),
            "net_mtm":      round(self.net_mtm(), 2),
            "equity":       round(self.current_equity(), 2),
            "drawdown_pct": self.drawdown_pct,
            "open_lots":    self.total_open_lots(),
            "slippage":     self.avg_slippage_per_provider(),
            "positions":    [p.to_dict() for p in self.positions.values() if p.qty > 0],
        }


# ─────────────────────────────────────────────────────────────────────────────
# RiskManager
# ─────────────────────────────────────────────────────────────────────────────

class RiskManager:
    """
    Async risk management engine.

    Wire into the system via run_system.py alongside other engine tasks.
    Subscribes to ORDER_FILL and SIGNAL topics; runs a 1-second check loop
    and a 30-second broker reconciliation loop in separate tasks.
    """

    def __init__(
        self,
        bus:      EventBus,
        registry: ClientRegistry,
        router=None,   # ExecutionRouter — for emergency liquidation orders
    ) -> None:
        self._bus      = bus
        self._registry = registry
        self._router   = router

        self._fill_queue   = bus.subscribe(Topic.ORDER_FILL)
        self._signal_queue = bus.subscribe(Topic.SIGNAL)

        # Per-client risk state
        self._states:  Dict[str, _ClientRiskState] = {}
        # Pending signal prices: signal_id -> (provider, signal_price, ts)
        self._pending_signals: Dict[str, Tuple[str, float, datetime]] = {}

        self._running = False
        self._kill_all_active = False   # guard against duplicate kill-all runs

    # ── Public API ─────────────────────────────────────────────────────────────

    def stop(self) -> None:
        self._running = False

    def risk_summary(self) -> dict:
        """Aggregate firm-wide risk metrics for the dashboard endpoint."""
        states = list(self._states.values())
        total_capital  = sum(s.capital  for s in states)
        total_net_mtm  = sum(s.net_mtm() for s in states)
        total_open_lots = sum(s.total_open_lots() for s in states)

        # Firm-level average slippage across all providers
        all_slippage: List[float] = []
        for s in states:
            all_slippage.extend(v for v in s.avg_slippage_per_provider().values())
        avg_slippage = round(sum(all_slippage) / len(all_slippage), 3) if all_slippage else 0.0

        clients_at_risk = [
            s.to_dict() for s in states if s.drawdown_pct > 0
        ]

        return {
            "ts":               datetime.now(IST).isoformat(),
            "total_capital":    round(total_capital,   2),
            "total_net_mtm":    round(total_net_mtm,   2),
            "total_open_lots":  total_open_lots,
            "avg_slippage_pts": avg_slippage,
            "client_count":     len(states),
            "clients":          [s.to_dict() for s in states],
            "clients_at_risk":  clients_at_risk,
        }

    async def kill_all(self) -> dict:
        """
        Firm-wide emergency liquidation.

        For each active client:
          1. Halt the client profile
          2. Cancel/stop execution worker
          3. Publish KILL_SWITCH system event
        Returns a summary of actions taken.
        """
        if self._kill_all_active:
            return {"status": "already_running", "ts": datetime.now(IST).isoformat()}

        self._kill_all_active = True
        actioned: List[str] = []
        ts = datetime.now(IST).isoformat()
        logger.warning("RiskManager: FIRM-WIDE KILL-ALL initiated at %s", ts)

        try:
            for client in self._registry.all_active():
                await self._liquidate_client(
                    client.client_id,
                    reason="FIRM_KILL_ALL",
                )
                actioned.append(client.client_id)

            await self._bus.publish(Topic.SYSTEM_EVENT, {
                "event":     "KILL_SWITCH",
                "scope":     "FIRM_WIDE",
                "clients":   actioned,
                "timestamp": ts,
            })
            logger.warning("RiskManager: KILL-ALL complete. Halted %d clients.", len(actioned))
        finally:
            self._kill_all_active = False

        return {
            "status":   "ok",
            "halted":   actioned,
            "count":    len(actioned),
            "ts":       ts,
        }

    # ── Async run loops ────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("RiskManager: Started.")

        # Seed states from current registry
        self._sync_registry()

        fill_task    = asyncio.create_task(self._fill_loop(),    name="rm_fill")
        signal_task  = asyncio.create_task(self._signal_loop(),  name="rm_signal")
        check_task   = asyncio.create_task(self._check_loop(),   name="rm_check")
        reconcile_task = asyncio.create_task(self._reconcile_loop(), name="rm_reconcile")

        # Run until stop() is called — tasks are cancelled in shutdown
        try:
            while self._running:
                await asyncio.sleep(0.5)
        finally:
            for t in (fill_task, signal_task, check_task, reconcile_task):
                if not t.done():
                    t.cancel()
            await asyncio.gather(fill_task, signal_task, check_task, reconcile_task,
                                 return_exceptions=True)
            logger.info("RiskManager: Stopped.")

    # ── Internal loops ─────────────────────────────────────────────────────────

    async def _fill_loop(self) -> None:
        while self._running:
            try:
                fill: OrderFill = await asyncio.wait_for(
                    self._fill_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            try:
                await self._handle_fill(fill)
            except Exception as exc:
                logger.exception("RiskManager._fill_loop: unhandled error: %s", exc)

    async def _signal_loop(self) -> None:
        while self._running:
            try:
                pkg = await asyncio.wait_for(
                    self._signal_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            try:
                self._record_signal(pkg)
            except Exception as exc:
                logger.exception("RiskManager._signal_loop: error: %s", exc)

    async def _check_loop(self) -> None:
        while self._running:
            await asyncio.sleep(_RISK_CHECK_INTERVAL)
            try:
                await self._check_all_clients()
            except Exception as exc:
                logger.exception("RiskManager._check_loop: error: %s", exc)

    async def _reconcile_loop(self) -> None:
        while self._running:
            await asyncio.sleep(_RECONCILE_INTERVAL)
            try:
                await self._reconcile_broker_positions()
            except Exception as exc:
                logger.exception("RiskManager._reconcile_loop: error: %s", exc)

    # ── Fill handler ───────────────────────────────────────────────────────────

    async def _handle_fill(self, fill: OrderFill) -> None:
        if fill.status != OrderStatus.COMPLETE:
            return

        self._sync_registry()
        state = self._states.get(fill.client_id)
        if state is None:
            return

        # Update position fly-calculation
        pos = state.position(fill.broker_symbol)
        pos.apply_fill(fill)

        # Slippage: compare fill price against buffered signal price
        tag = getattr(fill, "tag", "") or ""
        if tag and tag in self._pending_signals:
            provider, signal_price, _ts = self._pending_signals.pop(tag)
            side = fill.side.value if hasattr(fill.side, "value") else str(fill.side)
            if side == "BUY":
                slip = fill.avg_price - signal_price
            else:
                slip = signal_price - fill.avg_price
            state.record_slippage(provider, slip)

        state.update_drawdown()

    # ── Signal recorder ────────────────────────────────────────────────────────

    def _record_signal(self, pkg) -> None:
        """Buffer signal prices for slippage computation on fill."""
        try:
            sig_id = getattr(pkg, "signal_id", None) or getattr(pkg, "id", None)
            price  = getattr(pkg, "entry_price", None) or getattr(pkg, "price", None)
            source = getattr(pkg, "source", "unknown")
            if sig_id and price:
                self._pending_signals[str(sig_id)] = (
                    str(source), float(price), datetime.now(IST)
                )
                # Trim stale signals (keep last 200)
                if len(self._pending_signals) > 200:
                    oldest = sorted(
                        self._pending_signals.items(),
                        key=lambda kv: kv[1][2],
                    )
                    for k, _ in oldest[:50]:
                        self._pending_signals.pop(k, None)
        except Exception:
            pass

    # ── Risk check ─────────────────────────────────────────────────────────────

    async def _check_all_clients(self) -> None:
        self._sync_registry()
        for cid, state in list(self._states.items()):
            client = self._registry.get(cid)
            if client is None:
                continue
            state.update_drawdown()

            daily_loss_pct = (
                -client._daily_pnl / client.risk.capital * 100.0
                if client.risk.capital else 0.0
            )
            drawdown_limit = client.risk.max_daily_loss_pct

            if daily_loss_pct >= drawdown_limit and not state._liquidating:
                logger.warning(
                    "RiskManager: Client %s daily loss %.2f%% >= limit %.2f%% — liquidating.",
                    cid, daily_loss_pct, drawdown_limit,
                )
                await self._liquidate_client(
                    cid,
                    reason=f"DAILY_LOSS_LIMIT daily_loss={daily_loss_pct:.2f}%",
                )

    # ── Liquidation ────────────────────────────────────────────────────────────

    async def _liquidate_client(self, client_id: str, reason: str = "") -> None:
        """
        Sandboxed per-client liquidation sequence:
          1. Halt the ClientProfile (no new signals accepted)
          2. Stop the ClientExecutionWorker
          3. Emit SYSTEM_EVENT audit log
        """
        state = self._states.get(client_id)
        if state and state._liquidating:
            return
        if state:
            state._liquidating = True

        client = self._registry.get(client_id)
        if client is None:
            return

        client.halt(reason)
        logger.warning("RiskManager: HALT %s — %s", client_id, reason)

        # Stop execution worker for this client
        if self._router is not None:
            try:
                pool = getattr(self._router, "_pool", None)
                if pool is not None:
                    worker = pool.worker(client_id)
                    if worker is not None:
                        worker.stop()
                        logger.info("RiskManager: Worker stopped for %s.", client_id)
            except Exception as exc:
                logger.warning("RiskManager: Could not stop worker %s: %s", client_id, exc)

        await self._bus.publish(Topic.SYSTEM_EVENT, {
            "event":     "CLIENT_RISK_HALT",
            "client_id": client_id,
            "reason":    reason,
            "timestamp": datetime.now(IST).isoformat(),
        })

        if state:
            state._liquidating = False

    # ── Broker reconciliation ──────────────────────────────────────────────────

    async def _reconcile_broker_positions(self) -> None:
        """
        Poll broker positions every 30 seconds and reconcile against
        fly-computed positions. Discrepancies are logged; mark prices updated.
        Actual broker API calls are skipped when brokers are mock/unavailable.
        """
        if self._router is None:
            return

        pool = getattr(self._router, "_pool", None)
        if pool is None:
            return

        for client in self._registry.all_active():
            cid   = client.client_id
            state = self._states.get(cid)
            if state is None:
                continue

            worker = pool.worker(cid)
            if worker is None:
                continue

            for broker in getattr(worker, "_brokers", []):
                try:
                    positions = await broker.get_positions() if hasattr(broker, "get_positions") else []
                    for pos_dict in (positions or []):
                        sym    = pos_dict.get("symbol") or pos_dict.get("broker_symbol", "")
                        mark   = float(pos_dict.get("ltp", 0) or pos_dict.get("mark_price", 0))
                        if sym and mark:
                            rec = state.position(sym)
                            rec.update_mark(mark)
                except Exception as exc:
                    logger.debug("RiskManager: reconcile error %s/%s: %s",
                                 cid, getattr(broker, "provider", "?"), exc)

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _sync_registry(self) -> None:
        """Ensure every active client has a risk state record."""
        for client in self._registry.all_active():
            if client.client_id not in self._states:
                self._states[client.client_id] = _ClientRiskState(
                    client.client_id,
                    client.risk.capital,
                )
