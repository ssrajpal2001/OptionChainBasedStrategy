"""
execution_bridge/cascade_bridge.py — V4 Cascade order router.

Subscribes to Topic.CASCADE_ORDER_REQUEST for CascadeOrderEvent objects,
published by strategies/v4_cascade/book.py (ONE independent book per
(client, binding, underlying) deployment — see StraddleBookManager's
per-binding refactor, mirrored here from day one).

Unlike execution_bridge/straddle_bridge.py's legacy "loop every client,
mirror-to-all-unless-stamped" routing (a holdover from before the per-binding
refactor), every CascadeOrderEvent is ALWAYS stamped with the exact
(client_id, binding_id) that owns it — so routing here is a direct lookup,
never a broadcast.

Paper mode  — pure local simulated fill at the engine's own price_hint
              (no real order sent — same convention sell_straddle's plain
              "paper" mode uses).
Live mode   — SmartOrderExecutor: crypto (Delta, wide spreads) uses
              LIMIT-at-mid with chase→market; NIFTY uses MARKET. Position is
              booked from the REAL broker fill, never the price_hint.

Single-leg, single-side (V4Cascade never holds CE+PE simultaneously — T1/T2
are two tranches of the SAME side). ENTRY places one combined order for
T1+T2's total qty; EXIT places one order per tranche as each closes
independently (T1 fixed target/SL, T2 trailing stop).

Log files: logs/trades/{client_id}-{binding_id}-{YYYYMMDD}.log (same
directory/convention as straddle_bridge.py's TradeLogger, separate lines).
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, Optional

from config.global_config import IST, Topic, order_exchange
from data_layer.base_feeder import EventBus
from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
from execution_bridge.straddle_bridge import _resolve_option_symbol

logger = logging.getLogger(__name__)

_CRYPTO_CONTRACT_VALUE = {"BTC": 0.001, "ETH": 0.01}


# ── Events ────────────────────────────────────────────────────────────────────

@dataclass
class CascadeOrderEvent:
    """Published by V4CascadeBook to Topic.CASCADE_ORDER_REQUEST. Always
    stamped with the owning (client_id, binding_id) — routing is a direct
    lookup, never a mirror-to-all-clients broadcast."""
    action:       str             # "ENTRY" | "EXIT"
    underlying:   str
    side:         str             # "CE" | "PE" — engine's bear-scan/bull-scan side label,
                                   # NOT a real option side for crypto (spot/perpetual only)
    strike:       float           # execution strike (0.0 for crypto — perpetual, no strike)
    qty:          int             # ENTRY: T1+T2 combined; EXIT: the closing tranche's qty
    price_hint:   float           # engine's own computed price at decision time (paper/fallback fill)
    tranche:      str = "BOTH"    # "T1" | "T2" | "BOTH" (BOTH = the combined ENTRY order)
    sl_price:     float = 0.0
    target_price: Optional[float] = None
    close_reason: str = ""        # populated on EXIT
    entry_price:  float = 0.0     # populated on EXIT: the tranche's real entry (for P&L)
    is_crypto:    bool = False
    expiry:       Optional[date] = None
    client_id:    str = ""
    binding_id:   str = ""
    event_id:     str = ""
    timestamp:    Optional[datetime] = None


@dataclass
class CascadeFillEvent:
    """Published by V4CascadeExecutionBridge to Topic.ORDER_FILL after order
    execution. Filtered by isinstance() alongside StraddleFillEvent/
    ICFillEvent, which share the same topic."""
    action:      str    # "ENTRY" | "EXIT"
    underlying:  str
    side:        str
    tranche:     str    # "T1" | "T2" | "BOTH"
    fill_price:  float
    qty:         int
    client_id:   str
    binding_id:  str
    event_id:    str
    paper_mode:  bool = True
    symbol:      str = ""
    timestamp:   datetime = field(default_factory=lambda: datetime.now(IST))
    # True when a LIVE ENTRY failed to route or the order was rejected — the
    # book must discard its optimistic position rather than manage a phantom.
    entry_aborted:  bool = False
    routing_failed: bool = False


# ── Per-client-broker trade logger (reuses the same log dir/convention) ──────

class _CascadeTradeLogger:
    def __init__(self, log_dir: str = "logs/trades") -> None:
        self._log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self._handles: Dict[str, object] = {}

    def _handle(self, client_id: str, binding_id: str) -> object:
        today = datetime.now(IST).strftime("%Y%m%d")
        key = f"{client_id}-{binding_id}-{today}"
        if key not in self._handles:
            path = os.path.join(self._log_dir, f"{key}.log")
            self._handles[key] = open(path, "a", encoding="utf-8", buffering=1)
        return self._handles[key]

    def log(self, client_id: str, binding_id: str, message: str) -> None:
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self._handle(client_id, binding_id).write(f"{ts}  V4CASCADE  {message}\n")

    def close_all(self) -> None:
        for h in self._handles.values():
            try:
                h.close()
            except Exception:
                pass
        self._handles.clear()


# ── Bridge ────────────────────────────────────────────────────────────────────

class V4CascadeExecutionBridge:
    """Listens for CascadeOrderEvent on Topic.CASCADE_ORDER_REQUEST. Routes
    STRICTLY to the event's own (client_id, binding_id) — never broadcasts.
    Publishes CascadeFillEvent to Topic.ORDER_FILL on completion."""

    def __init__(self, bus: EventBus, router, log_dir: str = "logs/trades") -> None:
        self._bus = bus
        self._router = router
        self._trade_log = _CascadeTradeLogger(log_dir)
        self._running = False
        self._q = bus.subscribe(Topic.CASCADE_ORDER_REQUEST)
        # order_ids per (client, binding, underlying, side, tranche) — best-effort
        # bookkeeping, mirrors straddle_bridge's per-leg order tracking.
        self._order_ids: Dict[tuple, str] = {}
        from execution_bridge.smart_executor import SmartOrderExecutor
        self._executor = SmartOrderExecutor(fill_timeout_sec=4.0, chase_attempts=2)
        self._exit_executor = SmartOrderExecutor(fill_timeout_sec=2.0, chase_attempts=1)

    async def run(self) -> None:
        self._running = True
        logger.info("V4CascadeExecutionBridge: started.")
        while self._running:
            try:
                ev = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, CascadeOrderEvent):
                continue
            try:
                await self._handle(ev)
            except Exception as exc:
                logger.exception(
                    "V4CascadeExecutionBridge: _handle error for %s %s %s: %s",
                    ev.action, ev.underlying, ev.side, exc,
                )

    def stop(self) -> None:
        self._running = False
        self._trade_log.close_all()
        logger.info("V4CascadeExecutionBridge: stopped.")

    # ── order handling ────────────────────────────────────────────────────────

    async def _handle(self, ev: CascadeOrderEvent) -> None:
        if not ev.client_id or not ev.binding_id:
            logger.error("V4CascadeExecutionBridge: %s %s event missing client_id/binding_id — dropped.",
                         ev.action, ev.underlying)
            return

        db = getattr(self._router, "_client_db", None) or getattr(self._router, "_db", None)
        live_binding = None
        if db is not None and hasattr(db, "get_bindings_safe_sync"):
            try:
                for b in db.get_bindings_safe_sync(ev.client_id):
                    if b.get("binding_id") == ev.binding_id:
                        live_binding = b
                        break
            except Exception:
                live_binding = None

        if live_binding is None or not live_binding.get("terminal_connected"):
            logger.warning("V4CascadeExecutionBridge: %s %s — [%s/%s] terminal not connected, no route.",
                           ev.action, ev.underlying, ev.client_id, ev.binding_id)
            if ev.action == "ENTRY":
                await self._abort(ev, routing_failed=True)
            return

        # ENTRY is gated on a RUNNING v4_cascade deployment for this exact
        # binding+underlying (mirrors straddle's is_running gate). EXIT must
        # ALWAYS be allowed to route — a square-off/stop sets is_running=0
        # the instant after the EXIT publishes; gating the close would strand
        # the open leg on the exchange.
        if ev.action == "ENTRY" and db is not None and hasattr(db, "get_deployments_sync"):
            try:
                deployments = db.get_deployments_sync(ev.client_id)
            except Exception:
                deployments = []
            matching = [
                d for d in deployments
                if d.get("binding_id") == ev.binding_id
                and d.get("strategy_name") == "v4_cascade"
                and str(d.get("underlying", "")).upper() == ev.underlying.upper()
                and int(d.get("is_running", 0) or 0) == 1
            ]
            if not matching:
                logger.warning("V4CascadeExecutionBridge: %s %s — [%s/%s] no running v4_cascade "
                               "deployment, no route.", ev.action, ev.underlying, ev.client_id, ev.binding_id)
                await self._abort(ev, routing_failed=True)
                return

        broker = (self._router._brokers or {}).get(ev.client_id, {}).get(ev.binding_id)
        mode = live_binding.get("trading_mode", "paper") or "paper"

        logger.info("V4CascadeExecutionBridge: routing %s %s %s tranche=%s → [%s/%s] mode=%s",
                    ev.action, ev.underlying, ev.side, ev.tranche, ev.client_id, ev.binding_id, mode)

        if broker is None or mode == "paper":
            await self._paper_fill(ev)
        else:
            await self._live_fill(ev, broker)

    async def _abort(self, ev: CascadeOrderEvent, routing_failed: bool = False) -> None:
        await self._bus.publish(Topic.ORDER_FILL, CascadeFillEvent(
            action=ev.action, underlying=ev.underlying, side=ev.side, tranche=ev.tranche,
            fill_price=0.0, qty=ev.qty, client_id=ev.client_id, binding_id=ev.binding_id,
            event_id=ev.event_id, entry_aborted=(ev.action == "ENTRY"), routing_failed=routing_failed,
        ))

    async def _paper_fill(self, ev: CascadeOrderEvent) -> None:
        """Pure local simulated fill at the engine's own price_hint — no
        real order sent (same convention as sell_straddle's plain 'paper'
        mode). Still produces a verifiable per-binding log line + (on EXIT)
        a trade_history record, unlike the old broadcast SignalPackage path
        this replaces, which silently did nothing in paper mode."""
        fill = CascadeFillEvent(
            action=ev.action, underlying=ev.underlying, side=ev.side, tranche=ev.tranche,
            fill_price=ev.price_hint, qty=ev.qty, client_id=ev.client_id, binding_id=ev.binding_id,
            event_id=ev.event_id, paper_mode=True,
        )
        logger.info("[PAPER] V4Cascade %s %s %s tranche=%s qty=%d @ %.4f | client=%s/%s",
                    ev.action, ev.underlying, ev.side, ev.tranche, ev.qty, ev.price_hint,
                    ev.client_id, ev.binding_id)
        self._trade_log.log(ev.client_id, ev.binding_id,
            f"[PAPER] {ev.action} {ev.underlying} {ev.side} tranche={ev.tranche} "
            f"qty={ev.qty} @ {ev.price_hint:.4f}"
            + (f" reason={ev.close_reason}" if ev.action == "EXIT" else ""))
        if ev.action == "EXIT":
            self._record_history(ev, fill.fill_price)
        await self._bus.publish(Topic.ORDER_FILL, fill)

    async def _live_fill(self, ev: CascadeOrderEvent, broker) -> None:
        """Place a real single-leg order via the broker. Crypto trades the
        underlying's own perpetual (BTCUSD/ETHUSD, no strike); NIFTY trades
        the real ATM+-50 execution strike resolved by book.py at trigger
        time. Booked from the REAL fill, never price_hint."""
        symbol = self._resolve_symbol(ev, broker)
        if not symbol:
            logger.error("V4CascadeExecutionBridge: no tradable symbol for %s %s %s — falling back to LTP.",
                        ev.underlying, ev.side, ev.strike)
            await self._paper_fill(ev)
            return

        # Crypto's PE side is a SHORT (bull-trap -> bearish signal), so its
        # ENTRY sells to open / EXIT buys to close -- mirrored from every
        # other case (NIFTY CE/PE always buy options; crypto CE always
        # longs the perpetual), which stays BUY-to-open/SELL-to-close.
        is_short = ev.is_crypto and ev.side == "PE"
        if is_short:
            side = OrderSide.SELL if ev.action == "ENTRY" else OrderSide.BUY
        else:
            side = OrderSide.BUY if ev.action == "ENTRY" else OrderSide.SELL
        exchange = order_exchange(ev.underlying)
        use_limit = (exchange == "DELTA")
        executor = self._exit_executor if ev.action == "EXIT" else self._executor
        tag = f"V4C_{ev.underlying}_{ev.action}"[:20]

        try:
            legfill = await executor.execute_leg(
                broker, broker_symbol=symbol, exchange=exchange, side=side, qty=ev.qty,
                product="MIS", tag=tag, client_id=ev.client_id, use_limit=use_limit, tick=0.0,
            )
            avg = float(getattr(legfill, "avg_price", 0.0) or 0.0)
            px = avg if avg > 0 else ev.price_hint
            fq = int(getattr(legfill, "filled_qty", 0) or 0)
            oids = getattr(legfill, "order_ids", []) or []
            if oids:
                self._order_ids[(ev.client_id, ev.binding_id, ev.underlying, ev.side, ev.tranche)] = str(oids[-1])
            logger.info("[LIVE] V4Cascade %s %s %s tranche=%s — filled %d/%d @ %.4f via %s (orders=%s) | client=%s",
                        ev.action, ev.underlying, ev.side, ev.tranche, fq, ev.qty, px,
                        "LIMIT-chase" if use_limit else "MARKET", oids, ev.client_id)
            self._trade_log.log(ev.client_id, ev.binding_id,
                f"[LIVE] {ev.action} {ev.underlying} {ev.side} tranche={ev.tranche} "
                f"filled {fq}/{ev.qty} @ {px:.4f} ({'LIMIT-chase' if use_limit else 'MARKET'}; orders={oids})"
                + (f" reason={ev.close_reason}" if ev.action == "EXIT" else ""))
        except Exception as exc:
            logger.error("[LIVE] V4Cascade %s %s %s order FAILED: %s — falling back to LTP.",
                        ev.action, ev.underlying, ev.side, exc)
            self._trade_log.log(ev.client_id, ev.binding_id,
                f"LIVE {ev.action} {ev.underlying} {ev.side} ORDER FAILED: {exc}")
            px, fq = ev.price_hint, 0

        # ENTRY that didn't fill at all is aborted, not booked at a phantom
        # price — a naked/fake position must never be assumed established.
        if ev.action == "ENTRY" and fq <= 0:
            logger.error("[LIVE] V4Cascade %s ENTRY got NO fill — ABORTING (no phantom position). client=%s/%s",
                        ev.underlying, ev.client_id, ev.binding_id)
            self._trade_log.log(ev.client_id, ev.binding_id,
                f"ENTRY ABORT {ev.underlying} {ev.side} — zero fill")
            await self._bus.publish(Topic.ORDER_FILL, CascadeFillEvent(
                action="ENTRY", underlying=ev.underlying, side=ev.side, tranche=ev.tranche,
                fill_price=0.0, qty=ev.qty, client_id=ev.client_id, binding_id=ev.binding_id,
                event_id=ev.event_id, paper_mode=False, entry_aborted=True,
            ))
            return

        fill = CascadeFillEvent(
            action=ev.action, underlying=ev.underlying, side=ev.side, tranche=ev.tranche,
            fill_price=px, qty=ev.qty, client_id=ev.client_id, binding_id=ev.binding_id,
            event_id=ev.event_id, paper_mode=False, symbol=symbol,
        )
        if ev.action == "EXIT":
            self._record_history(ev, px)
        await self._bus.publish(Topic.ORDER_FILL, fill)

    def _resolve_symbol(self, ev: CascadeOrderEvent, broker) -> str:
        if ev.is_crypto:
            return f"{ev.underlying.upper()}USD"
        if not ev.strike:
            return ""
        _b = getattr(broker, "_binding", None)
        provider = (_b.provider if _b else getattr(broker, "provider", "mock"))
        return _resolve_option_symbol(ev.underlying, ev.expiry, int(ev.strike), ev.side, provider)

    def _record_history(self, ev: CascadeOrderEvent, exit_price: float) -> None:
        """Persist a closed tranche to the client trade-history (History tab)."""
        try:
            from data_layer import trade_history as _th
            cv = _CRYPTO_CONTRACT_VALUE.get(ev.underlying.upper(), 1.0)
            is_short = ev.is_crypto and ev.side == "PE"
            # SHORT: profit = entry - exit (price falling is the win).
            # LONG (everything else): profit = exit - entry.
            pnl = ((ev.entry_price - exit_price) if is_short
                   else (exit_price - ev.entry_price)) * ev.qty * cv
            _th.record(
                ev.client_id, "v4_cascade", ev.underlying,
                ev.entry_price, exit_price, ev.close_reason, pnl,
                binding_id=ev.binding_id,
                legs=[{"side": ev.side, "strike": ev.strike, "entry": ev.entry_price,
                       "exit": exit_price, "pnl": pnl, "entry_reason": ev.tranche}],
            )
        except Exception:
            logger.exception("V4CascadeExecutionBridge: trade_history record failed for %s/%s",
                             ev.client_id, ev.binding_id)
