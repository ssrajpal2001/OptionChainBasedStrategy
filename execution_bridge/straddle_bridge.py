"""
execution_bridge/straddle_bridge.py — Straddle order router.

Subscribes to Topic.ORDER_REQUEST for StraddleOrderEvent objects.
Routes SELL/BUY CE+PE orders to every registered client broker.

Paper mode  — fills immediately at the sent LTP, zero latency.
Live mode   — calls broker.place_order() for each leg, waits for fill.

After fill publishes Topic.ORDER_FILL → StraddleFillEvent so
SellStraddleStrategy can confirm position entry/exit prices.

Log files:
  logs/trades/{client_id}-{binding_id}-{YYYYMMDD}.log
  One file per client-broker per day.  Every ENTRY and EXIT line
  is written here so you can audit the whole session at a glance.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus

logger = logging.getLogger(__name__)


# ── Events ────────────────────────────────────────────────────────────────────

@dataclass
class StraddleOrderEvent:
    """Published by SellStraddleStrategy to Topic.ORDER_REQUEST."""
    action:         str        # "ENTRY" | "EXIT"
    underlying:     str        # "NIFTY", "BANKNIFTY" …
    atm:            float      # ATM strike used
    ce_strike:      float
    pe_strike:      float
    ce_ltp:         float      # Price at signal time (paper fill price)
    pe_ltp:         float
    lot_multiplier: int  = 1
    lot_size:       int  = 50
    spot:           float = 0.0
    indicators:     dict = field(default_factory=dict)
    close_reason:   str  = ""  # populated on EXIT
    realized_pnl:   float = 0.0  # populated on EXIT
    event_id:       str  = ""    # filled by bridge for correlation


@dataclass
class StraddleFillEvent:
    """Published by bridge to Topic.ORDER_FILL after order execution."""
    action:     str    # "ENTRY" | "EXIT"
    underlying: str
    atm:        float
    ce_strike:  float
    pe_strike:  float
    ce_fill:    float  # actual fill price
    pe_fill:    float
    client_id:  str
    binding_id: str
    event_id:   str
    timestamp:  datetime = field(default_factory=lambda: datetime.now(IST))
    paper_mode: bool = True


# ── Iron Condor order events ──────────────────────────────────────────────────

@dataclass
class ICOrderEvent:
    """Published by IronCondorStrategy to Topic.IC_ORDER_REQUEST."""
    action:          str    # "ENTRY" | "EXIT" | "ADJUST" (close + reopen one side)
    underlying:      str
    atm:             float
    # Short legs (sell to open, buy to close)
    short_ce_strike: float
    short_pe_strike: float
    short_ce_ltp:    float
    short_pe_ltp:    float
    # Long legs / hedges (buy to open, sell to close)
    long_ce_strike:  float
    long_pe_strike:  float
    long_ce_ltp:     float
    long_pe_ltp:     float
    lot_size:        int   = 65
    lot_multiplier:  int   = 1
    close_reason:    str   = ""
    cumulative_pnl:  float = 0.0   # running P&L across all rolls for this IC cycle
    event_id:        str   = ""


@dataclass
class ICFillEvent:
    """Published by ICExecutionBridge after order execution."""
    action:          str
    underlying:      str
    short_ce_fill:   float
    short_pe_fill:   float
    long_ce_fill:    float
    long_pe_fill:    float
    client_id:       str
    binding_id:      str
    event_id:        str
    paper_mode:      bool     = True
    timestamp:       datetime = field(default_factory=lambda: datetime.now(IST))


# ── Per-client-broker trade logger ────────────────────────────────────────────

class TradeLogger:
    """
    Writes human-readable trade records to per-client-broker daily log files.

    File path:  logs/trades/{client_id}-{binding_id}-{YYYYMMDD}.log
    Each line:  ISO_TS | ACTION | UNDERLYING | ATM | CE@price | PE@price | ...
    """

    def __init__(self, log_dir: str = "logs/trades") -> None:
        self._log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self._handles: Dict[str, object] = {}   # key → open file handle

    def _handle(self, client_id: str, binding_id: str) -> object:
        today = datetime.now(IST).strftime("%Y%m%d")
        key   = f"{client_id}-{binding_id}-{today}"
        if key not in self._handles:
            path = os.path.join(self._log_dir, f"{key}.log")
            self._handles[key] = open(path, "a", encoding="utf-8", buffering=1)
        return self._handles[key]

    def log_entry(
        self,
        client_id:  str,
        binding_id: str,
        ev:         StraddleOrderEvent,
        fill:       StraddleFillEvent,
    ) -> None:
        ts    = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        ind   = ev.indicators
        qty   = ev.lot_size * ev.lot_multiplier
        credit = fill.ce_fill + fill.pe_fill
        line = (
            f"{ts} | ENTRY | {ev.underlying} | ATM={ev.atm:.0f} | "
            f"CE={ev.ce_strike:.0f}@{fill.ce_fill:.2f} | "
            f"PE={ev.pe_strike:.0f}@{fill.pe_fill:.2f} | "
            f"Credit={credit:.2f} | Qty={qty} | Spot={ev.spot:.0f} | "
            f"RSI={ind.get('rsi', 0):.1f} ADX={ind.get('adx', 0):.1f} "
            f"VWAP={ind.get('vwap', 0):.2f} | "
            f"{'[PAPER]' if fill.paper_mode else '[LIVE]'}\n"
        )
        self._handle(client_id, binding_id).write(line)

    def log_exit(
        self,
        client_id:  str,
        binding_id: str,
        ev:         StraddleOrderEvent,
        fill:       StraddleFillEvent,
        entry_ce:   float,
        entry_pe:   float,
    ) -> None:
        ts       = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        qty      = ev.lot_size * ev.lot_multiplier
        pnl_pts  = ev.realized_pnl
        pnl_rs   = pnl_pts * qty
        line = (
            f"{ts} | EXIT  | {ev.underlying} | ATM={ev.atm:.0f} | "
            f"CE={ev.ce_strike:.0f} {entry_ce:.2f}→{fill.ce_fill:.2f} | "
            f"PE={ev.pe_strike:.0f} {entry_pe:.2f}→{fill.pe_fill:.2f} | "
            f"PnL={pnl_pts:+.2f}pts {pnl_rs:+.0f}Rs | "
            f"Reason={ev.close_reason} | "
            f"{'[PAPER]' if fill.paper_mode else '[LIVE]'}\n"
        )
        self._handle(client_id, binding_id).write(line)

    def close_all(self) -> None:
        for h in self._handles.values():
            try:
                h.close()
            except Exception:
                pass
        self._handles.clear()


# ── Bridge ────────────────────────────────────────────────────────────────────

class StraddleExecutionBridge:
    """
    Listens for StraddleOrderEvent on Topic.ORDER_REQUEST.
    Routes to all registered client brokers.
    Paper mode  → immediate simulated fill at sent LTP.
    Live mode   → calls broker.place_order() for CE + PE legs.
    Publishes StraddleFillEvent to Topic.ORDER_FILL on success.
    """

    def __init__(
        self,
        bus:      EventBus,
        registry,                  # ClientRegistry
        router,                    # ExecutionRouter (for broker map)
        log_dir:  str = "logs/trades",
    ) -> None:
        self._bus      = bus
        self._registry = registry
        self._router   = router
        self._trade_log = TradeLogger(log_dir)
        self._running   = False
        self._q         = bus.subscribe(Topic.ORDER_REQUEST)
        # Track last ENTRY event per underlying for exit price correlation
        self._last_entry: Dict[str, StraddleOrderEvent] = {}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("StraddleExecutionBridge: started.")
        while self._running:
            try:
                ev = await asyncio.wait_for(self._q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, StraddleOrderEvent):
                continue
            await self._handle(ev)

    def stop(self) -> None:
        self._running = False
        self._trade_log.close_all()
        logger.info("StraddleExecutionBridge: stopped.")

    # ── Order handling ────────────────────────────────────────────────────────

    async def _handle(self, ev: StraddleOrderEvent) -> None:
        clients = self._registry.all_active()
        if not clients:
            logger.warning("StraddleExecutionBridge: no active clients for %s %s", ev.action, ev.underlying)
            return

        routed = 0
        for client in clients:
            # Fetch live DB state for this client's bindings (checks engine_active)
            db = getattr(self._router, "_client_db", None) or getattr(self._router, "_db", None)
            live_bindings: list = []
            if db and hasattr(db, "get_bindings_safe_sync"):
                try:
                    live_bindings = db.get_bindings_safe_sync(client.client_id)
                except Exception:
                    live_bindings = []

            for live_b in live_bindings:
                binding_id = live_b.get("binding_id", "")

                # Gate 1: engine must be active for this broker
                if not live_b.get("engine_active"):
                    continue

                # Gate 2: terminal must be connected
                if not live_b.get("terminal_connected"):
                    continue

                # Gate 3: strategy assignment must match
                assigned = live_b.get("assigned_strategy", "") or ""
                if assigned and assigned != "sell_straddle":
                    continue

                # Gate 4: instrument must match
                assigned_idx = live_b.get("assigned_instrument", "") or ""
                if assigned_idx and assigned_idx != ev.underlying:
                    continue

                broker = (self._router._brokers or {}).get(client.client_id, {}).get(binding_id)
                mode = live_b.get("trading_mode", "paper") or "paper"

                logger.info(
                    "StraddleExecutionBridge: routing %s %s → [%s/%s] mode=%s",
                    ev.action, ev.underlying, client.client_id, binding_id, mode,
                )

                if mode == "paper" or broker is None:
                    await self._paper_fill(ev, client.client_id, binding_id, broker)
                else:
                    await self._live_fill(ev, client.client_id, binding_id, broker)
                routed += 1

        if routed == 0:
            logger.warning(
                "StraddleExecutionBridge: %s %s — no engine-active brokers found. "
                "Ensure Terminal is ON and Engine is ON for at least one broker.",
                ev.action, ev.underlying,
            )

    async def _paper_fill(
        self,
        ev:         StraddleOrderEvent,
        client_id:  str,
        binding_id: str,
        broker,
    ) -> None:
        """Simulate immediate fill at the LTP sent in the event."""
        fill = StraddleFillEvent(
            action     = ev.action,
            underlying = ev.underlying,
            atm        = ev.atm,
            ce_strike  = ev.ce_strike,
            pe_strike  = ev.pe_strike,
            ce_fill    = ev.ce_ltp,
            pe_fill    = ev.pe_ltp,
            client_id  = client_id,
            binding_id = binding_id,
            event_id   = ev.event_id,
            paper_mode = True,
        )

        if ev.action == "ENTRY":
            self._last_entry[ev.underlying] = ev
            logger.info(
                "[PAPER] %s %s ENTRY | CE=%s@%.2f PE=%s@%.2f credit=%.2f | client=%s broker=%s",
                ev.underlying, ev.atm,
                ev.ce_strike, fill.ce_fill,
                ev.pe_strike, fill.pe_fill,
                fill.ce_fill + fill.pe_fill,
                client_id, binding_id,
            )
            self._trade_log.log_entry(client_id, binding_id, ev, fill)
        else:
            entry_ev = self._last_entry.get(ev.underlying)
            entry_ce = entry_ev.ce_ltp if entry_ev else 0.0
            entry_pe = entry_ev.pe_ltp if entry_ev else 0.0
            logger.info(
                "[PAPER] %s %s EXIT | CE@%.2f PE@%.2f PnL=%.2fpts ₹%.0f | reason=%s | client=%s broker=%s",
                ev.underlying, ev.atm,
                fill.ce_fill, fill.pe_fill,
                ev.realized_pnl, ev.realized_pnl * ev.lot_size * ev.lot_multiplier,
                ev.close_reason, client_id, binding_id,
            )
            self._trade_log.log_exit(client_id, binding_id, ev, fill, entry_ce, entry_pe)

        # Publish fill so SellStraddleStrategy can confirm
        self._bus.publish(Topic.ORDER_FILL, fill)

    async def _live_fill(
        self,
        ev:         StraddleOrderEvent,
        client_id:  str,
        binding_id: str,
        broker,
    ) -> None:
        """Place actual SELL/BUY orders via broker API."""
        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
        from data_layer.symbol_translator import SymbolTranslator

        translator = SymbolTranslator()
        qty = ev.lot_size * ev.lot_multiplier
        side = OrderSide.SELL if ev.action == "ENTRY" else OrderSide.BUY

        fills = {}
        for opt_type, strike in [("CE", ev.ce_strike), ("PE", ev.pe_strike)]:
            symbol = translator.to_broker_symbol(
                underlying=ev.underlying,
                strike=strike,
                option_type=opt_type,
                provider=getattr(broker, "provider", "mock"),
            )
            req = OrderRequest(
                broker_symbol=symbol,
                exchange="NFO",
                side=side,
                qty=qty,
                order_type=OrderType.MARKET,
                product="INTRADAY",
                tag=f"SS_{ev.underlying}_{ev.action}",
                client_id=client_id,
            )
            try:
                fill = await broker.place_order(req)
                fills[opt_type] = fill.avg_price if fill else (ev.ce_ltp if opt_type == "CE" else ev.pe_ltp)
                logger.info(
                    "[LIVE] %s %s %s order placed — %s@%.2f | client=%s",
                    ev.action, ev.underlying, opt_type, symbol, fills[opt_type], client_id,
                )
            except Exception as exc:
                logger.error(
                    "[LIVE] %s %s %s order FAILED: %s — falling back to LTP",
                    ev.action, ev.underlying, opt_type, exc,
                )
                fills[opt_type] = ev.ce_ltp if opt_type == "CE" else ev.pe_ltp

        fill_ev = StraddleFillEvent(
            action     = ev.action,
            underlying = ev.underlying,
            atm        = ev.atm,
            ce_strike  = ev.ce_strike,
            pe_strike  = ev.pe_strike,
            ce_fill    = fills.get("CE", ev.ce_ltp),
            pe_fill    = fills.get("PE", ev.pe_ltp),
            client_id  = client_id,
            binding_id = binding_id,
            event_id   = ev.event_id,
            paper_mode = False,
        )

        if ev.action == "ENTRY":
            self._last_entry[ev.underlying] = ev
            self._trade_log.log_entry(client_id, binding_id, ev, fill_ev)
        else:
            entry_ev = self._last_entry.get(ev.underlying)
            entry_ce = entry_ev.ce_ltp if entry_ev else 0.0
            entry_pe = entry_ev.pe_ltp if entry_ev else 0.0
            self._trade_log.log_exit(client_id, binding_id, ev, fill_ev, entry_ce, entry_pe)

        self._bus.publish(Topic.ORDER_FILL, fill_ev)
