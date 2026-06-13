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
from datetime import datetime, date
from typing import Dict, List, Optional

from config.global_config import IST, Topic, order_exchange
from data_layer.base_feeder import EventBus
from data_layer.instrument_registry import REGISTRY as _REG
from data_layer.runtime_config import RuntimeConfig as _RC
from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType

logger = logging.getLogger(__name__)


def _resolve_option_symbol(underlying, expiry, strike, opt_type, provider):
    """Broker option symbol. Crypto (Delta) → 'C-BTC-60000-130626' via UniversalOptionMapper using
    the ACTIVE daily expiry (ignores the NSE registry expiry); else the NSE instrument registry."""
    if order_exchange(underlying) == "DELTA" or str(provider).lower() == "delta":
        from data_layer.universal_option_mapper import UniversalOptionMapper as _M
        from data_layer.symbol_translator import InternalSymbol
        return _M.to_delta_symbol(InternalSymbol(
            underlying=str(underlying).upper(), strike=float(strike),
            option_type="CE" if str(opt_type).upper().startswith("C") else "PE",
            expiry=_M.active_daily_expiry(),
        ))
    return _REG.get_broker_symbol(underlying, expiry, int(strike), opt_type, provider)


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
    # True per-leg ENTRY (sold) prices, carried on EXIT events. The bridge used to read these
    # from its in-memory `_last_entry`, which is EMPTY after a restart → history recorded the
    # sold rate as 0.00 and a garbage P&L when EOD squared off a restored position. The strategy
    # knows the real entry prices (on the restored position) and passes them here.
    ce_entry:       float = 0.0
    pe_entry:       float = 0.0
    event_id:       str  = ""    # filled by bridge for correlation
    legs:           list = field(default_factory=lambda: ["CE", "PE"])  # legs to act on
    leg_open_times: dict = field(default_factory=dict)  # "CE"/"PE" -> ISO open_time (for history)
    leg_open_reasons: dict = field(default_factory=dict)  # "CE"/"PE" -> open reason code (for history)
    # Per-binding refactor: when a per-(client,binding) book emits an order it stamps its OWN
    # identity here, so the bridge routes to EXACTLY that broker (no mirror-to-all). Empty =
    # legacy per-index engine → bridge keeps the old behaviour (route to all eligible brokers).
    client_id:      str  = ""
    binding_id:     str  = ""


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
    legs:       list = field(default_factory=lambda: ["CE", "PE"])
    # True when a LIVE ENTRY filled asymmetrically (one leg only) and the bridge flattened the filled
    # leg and ABORTED — the strategy must discard its optimistic position, never manage a naked leg.
    entry_aborted: bool = False


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
    expiry:          Optional[date] = None   # chosen expiry (min-LTP shift); None → bridge resolves current


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
        # Persist to the client trade-history (powers the dashboard History view).
        try:
            from data_layer import trade_history as _th
            # Record ONLY the legs actually in this event. A single-side roll/cleanup publishes
            # legs=[ "CE" ] or [ "PE" ]; recording both legs every time produced duplicate history
            # rows (the same pair logged once per leg-close, and twice for a physical roll).
            _sides = set(getattr(ev, "legs", None) or ["CE", "PE"])
            _open_ts = getattr(ev, "leg_open_times", None) or {}
            _open_rs = getattr(ev, "leg_open_reasons", None) or {}
            _exit_ts = datetime.now(IST).isoformat(timespec="seconds")
            _all = [
                {"side": "CE", "strike": ev.ce_strike, "entry": entry_ce,
                 "exit": fill.ce_fill, "pnl": (entry_ce - fill.ce_fill) * qty,
                 "entry_ts": _open_ts.get("CE"), "exit_ts": _exit_ts,
                 "entry_reason": _open_rs.get("CE", "")},
                {"side": "PE", "strike": ev.pe_strike, "entry": entry_pe,
                 "exit": fill.pe_fill, "pnl": (entry_pe - fill.pe_fill) * qty,
                 "entry_ts": _open_ts.get("PE"), "exit_ts": _exit_ts,
                 "entry_reason": _open_rs.get("PE", "")},
            ]
            _legs = [l for l in _all if l["side"] in _sides]
            if _legs:
                _th.record(
                    client_id, "sell_straddle", ev.underlying,
                    sum(l["entry"] for l in _legs), sum(l["exit"] for l in _legs),
                    ev.close_reason, sum(l["pnl"] for l in _legs),
                    binding_id=binding_id, legs=_legs,
                )
        except Exception:
            pass

    def log_event(self, client_id: str, binding_id: str, message: str) -> None:
        """Generic per-client-broker line writer (square-offs, order placements/rejections)."""
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        self._handle(client_id, binding_id).write(f"{ts}  {message}\n")

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
        # Broker order_ids per (client, binding, underlying) → {"CE": id, "PE": id} so a
        # later close can reference the exact orders the app opened (close-own-legs only,
        # and for cancel/modify of the exact exchange order).
        self._order_ids: Dict[tuple, Dict[str, str]] = {}
        # Slippage-aware executor: crypto LIMIT-at-mid (chase→market); books from the REAL fill.
        from execution_bridge.smart_executor import SmartOrderExecutor
        # ENTRY: no rush — try the mid harder (2 chases × 4s) to save the spread.
        self._executor = SmartOrderExecutor(fill_timeout_sec=4.0, chase_attempts=2)
        # EXIT/square-off: get flat PROMPTLY — try the mid ONCE (2s) then market the remainder, so a
        # kill/EOD/manual square-off doesn't dawdle ~12s on a wide Delta book. Still anti-slippage
        # (one mid attempt) but guarantees a fast flat via the market fallback.
        self._exit_executor = SmartOrderExecutor(fill_timeout_sec=2.0, chase_attempts=1)

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
            try:
                await self._handle(ev)
            except Exception as exc:
                # One bad order must NOT kill the bridge (which would silently stop ALL
                # future routing). Log and keep serving.
                logger.exception(
                    "StraddleExecutionBridge: _handle error for %s %s: %s",
                    ev.action, ev.underlying, exc,
                )

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

        # Per-binding TARGETED routing: if the event is stamped with a client+binding (emitted by
        # a per-binding book), route to ONLY that broker — never mirror to others.
        _target = (ev.client_id, ev.binding_id) if (ev.client_id and ev.binding_id) else None

        routed = 0
        for client in clients:
            if _target and client.client_id != _target[0]:
                continue
            # Fetch live DB state for this client's bindings (checks engine_active)
            db = getattr(self._router, "_client_db", None) or getattr(self._router, "_db", None)
            live_bindings: list = []
            if db and hasattr(db, "get_bindings_safe_sync"):
                try:
                    live_bindings = db.get_bindings_safe_sync(client.client_id)
                except Exception:
                    live_bindings = []

            # Deployments for this client — the real source of "which strategy on
            # which broker for which instrument". Gating on these (not the empty
            # binding.assigned_strategy field) is what stops every strategy routing
            # to every broker.
            deployments: list = []
            if db and hasattr(db, "get_deployments_sync"):
                try:
                    deployments = db.get_deployments_sync(client.client_id)
                except Exception:
                    deployments = []

            for live_b in live_bindings:
                binding_id = live_b.get("binding_id", "")

                # Targeted routing: skip every binding except the stamped one.
                if _target and binding_id != _target[1]:
                    continue

                # Gate: terminal must be connected (broker authenticated).
                if not live_b.get("terminal_connected"):
                    continue

                # Gate: this binding must have a RUNNING sell_straddle deployment on THIS
                # underlying. The per-strategy Run toggle (is_running) is the authority now —
                # not the binding-level engine_active (legacy events without tags still honour
                # engine_active for back-compat).
                _matching = [
                    d for d in deployments
                    if d.get("binding_id") == binding_id
                    and d.get("strategy_name") == "sell_straddle"
                    and str(d.get("underlying", "")).upper() == ev.underlying.upper()
                ]
                # An EXIT (buy-to-close) must ALWAYS be allowed to route — a square-off / kill /
                # stop sets is_running=False the instant after the EXIT is published, so gating the
                # close on is_running would strand the open legs on the exchange (the exact bug:
                # "squared in the UI but still open on Delta"). Only ENTRIES are gated on a RUNNING
                # deployment. The EXIT still needs terminal_connected (checked above) to place.
                _is_exit = (ev.action == "EXIT")
                if not _is_exit:
                    if not _matching:
                        continue
                    if _target:
                        if not any(int(d.get("is_running", 0) or 0) == 1 for d in _matching):
                            continue
                    elif not live_b.get("engine_active"):
                        continue

                broker = (self._router._brokers or {}).get(client.client_id, {}).get(binding_id)
                mode = live_b.get("trading_mode", "paper") or "paper"

                logger.info(
                    "StraddleExecutionBridge: routing %s %s → [%s/%s] mode=%s",
                    ev.action, ev.underlying, client.client_id, binding_id, mode,
                )

                if broker is None or mode == "paper":
                    # PAPER = PURE LOCAL SIMULATION — never send a real order. On a FUNDED exchange
                    # (e.g. Delta) a "paper" order can partially fill (the cheap leg) and a later
                    # paper-close BUY fills too, leaving a real phantom position. Paper must stay
                    # entirely in-app; use LIVE mode for real order placement.
                    await self._paper_fill(ev, client.client_id, binding_id, broker)
                else:
                    # LIVE: real broker order + real fill, order_id tracked for close-via-order-id.
                    await self._live_fill(ev, client.client_id, binding_id, broker, paper=False)
                routed += 1

        if routed == 0:
            logger.warning(
                "StraddleExecutionBridge: %s %s — no engine-active brokers found. "
                "Ensure Terminal is ON and Engine is ON for at least one broker.",
                ev.action, ev.underlying,
            )

    def _other_active_broker_for(self, underlying: str, excl_client: str, excl_binding: str) -> bool:
        """True if some OTHER client-broker (not excl_client/excl_binding) is still engine-active
        + terminal-connected AND deployed to sell_straddle on this underlying. Used to decide
        whether squaring off this binding leaves the strategy with no broker → safe to discard the
        logical position so a restart doesn't restore a ghost."""
        db = getattr(self._router, "_client_db", None) or getattr(self._router, "_db", None)
        if db is None:
            return False
        try:
            for _client in db.get_all_clients_sync():
                _cid = _client.get("client_id", "")
                if not _cid:
                    continue
                _binds = {b.get("binding_id"): b for b in db.get_bindings_safe_sync(_cid)}
                for _dep in db.get_deployments_sync(_cid):
                    if str(_dep.get("strategy_name", "")).lower() != "sell_straddle":
                        continue
                    _ul = str(_dep.get("underlying", "") or _dep.get("assigned_instrument", "")).upper()
                    if _ul != underlying.upper():
                        continue
                    _bid = _dep.get("binding_id")
                    if _cid == excl_client and _bid == excl_binding:
                        continue
                    _b = _binds.get(_bid)
                    if _b and _b.get("engine_active") and _b.get("terminal_connected"):
                        return True
        except Exception as _exc:
            logger.debug("StraddleBridge._other_active_broker_for(%s): %s", underlying, _exc)
        return False

    async def _reconcile_flat(self, broker, symbols, product, exchange,
                              client_id, binding_id, underlying, settle_sec: float = 3.0) -> None:
        """Make the broker ACTUALLY flat in `symbols` — the only reliable guard when an order the
        executor reported as unfilled is still live and fills late (Delta cancel/status is unreliable).
        Steps: cancel this entry's known orders for these symbols, wait `settle_sec` for any pending
        fill to land, then read the broker's REAL positions and market-flatten any residual qty."""
        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
        _syms = {str(s).upper() for s in (symbols or []) if s}
        if not _syms:
            return
        # 1) Cancel every order_id we placed for this (client,binding,underlying) — best-effort.
        try:
            _oids = (self._order_ids.get((client_id, binding_id, underlying)) or {})
            for _oid in list(_oids.values()):
                try:
                    await broker.cancel_order(str(_oid))
                except Exception:
                    pass
        except Exception:
            pass
        # 2) Let any in-flight fill settle, then reconcile against REAL positions.
        await asyncio.sleep(settle_sec)
        if not hasattr(broker, "get_positions"):
            return
        try:
            positions = await broker.get_positions()
        except Exception as exc:
            logger.error("StraddleBridge: reconcile get_positions failed for %s/%s: %s — RECONCILE MANUALLY.",
                         client_id, binding_id, exc)
            return
        for p in (positions or []):
            sym = str(getattr(p, "symbol", "")).upper()
            qty = int(getattr(p, "qty", 0) or 0)
            if sym not in _syms or qty == 0:
                continue
            _side = OrderSide.SELL if qty > 0 else OrderSide.BUY      # opposite of the open position
            try:
                _req = OrderRequest(broker_symbol=sym, exchange=exchange, side=_side, qty=abs(qty),
                                    order_type=OrderType.MARKET, product=product,
                                    tag=f"SS_{underlying}_RECON"[:20], client_id=client_id)
                _oid = await broker.place_order(_req)
                logger.error("StraddleBridge: RECONCILE-FLATTEN residual %s qty=%d (%s) → order %s | %s/%s",
                             sym, qty, _side, _oid, client_id, binding_id)
                self._trade_log.log_event(client_id, binding_id,
                    f"RECONCILE-FLATTEN residual {sym} qty={qty} → {_oid}")
            except Exception as exc:
                logger.error("StraddleBridge: RECONCILE-FLATTEN %s FAILED: %s — RECONCILE MANUALLY.", sym, exc)
                self._trade_log.log_event(client_id, binding_id,
                    f"RECONCILE-FLATTEN {sym} FAILED: {exc} — RECONCILE MANUALLY")

    async def square_off_binding(self, client_id: str, binding_id: str, strategies,
                                 underlying: str = "") -> int:
        """Square off the open sell-straddle legs for ONE binding's broker by driving the strategy's
        OWN exit path (`_close_position`) — the SAME pipeline a normal/EOD exit uses. That guarantees
        the legs are bought-to-close ON THE EXCHANGE (via SmartOrderExecutor, paper→sim-fill) AND the
        exit is written to trade history. The old path here fired raw place_order()s and discarded the
        position, doing NEITHER → "squared in the UI but still open on the exchange" + empty history.
        Returns the number of legs squared off (2 per closed straddle)."""
        legs_closed = 0
        for ss in (strategies or []):
            # STRICT per-binding identity: square off ONLY the book that belongs to exactly THIS
            # (client, binding). Every book now carries identity; a book without it is never a
            # per-binding trading book and must not be flattened by another binding's square-off.
            if (getattr(ss, "_client_id", "") != client_id
                    or getattr(ss, "_binding_id", "") != binding_id):
                continue
            # Per-strategy square-off: restrict to one underlying when given.
            if underlying and str(getattr(ss, "_underlying", "")).upper() != underlying.upper():
                continue
            pos = getattr(ss, "_position", None)
            if not pos or getattr(pos, "status", "") != "open":
                continue
            und = ss._underlying
            try:
                # Block re-entry while we tear the book down, then route through the real exit.
                ss._stop_for_day = True
                await ss._close_position(f"manual_squareoff_{client_id}_{binding_id}"[:40])
                legs_closed += 2
                logger.info("StraddleBridge: SQUARE-OFF %s for %s/%s — routed via _close_position "
                            "(real buy-to-close + history).", und, client_id, binding_id)
                self._trade_log.log_event(client_id, binding_id,
                    f"SQUARE-OFF (manual) {und} — closed via exit pipeline (real close + history)")
            except Exception as exc:
                logger.error("StraddleBridge: SQUARE-OFF FAILED %s for %s/%s: %s",
                             und, client_id, binding_id, exc)
                self._trade_log.log_event(client_id, binding_id,
                    f"SQUARE-OFF FAILED {und}: {exc}")
        return legs_closed

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
            ce_fill    = ev.ce_ltp if "CE" in ev.legs else 0.0,
            pe_fill    = ev.pe_ltp if "PE" in ev.legs else 0.0,
            client_id  = client_id,
            binding_id = binding_id,
            event_id   = ev.event_id,
            paper_mode = True,
            legs       = ev.legs,
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
            # Prefer the real entry prices carried on the EXIT event (survive restarts);
            # fall back to the in-memory last-entry only if the event didn't carry them.
            entry_ev = self._last_entry.get(ev.underlying)
            entry_ce = ev.ce_entry if getattr(ev, "ce_entry", 0.0) else (entry_ev.ce_ltp if entry_ev else 0.0)
            entry_pe = ev.pe_entry if getattr(ev, "pe_entry", 0.0) else (entry_ev.pe_ltp if entry_ev else 0.0)
            logger.info(
                "[PAPER] %s %s EXIT | CE@%.2f PE@%.2f PnL=%.2fpts ₹%.0f | reason=%s | client=%s broker=%s",
                ev.underlying, ev.atm,
                fill.ce_fill, fill.pe_fill,
                ev.realized_pnl, ev.realized_pnl * ev.lot_size * ev.lot_multiplier,
                ev.close_reason, client_id, binding_id,
            )
            self._trade_log.log_exit(client_id, binding_id, ev, fill, entry_ce, entry_pe)

        # Publish fill so SellStraddleStrategy can confirm
        await self._bus.publish(Topic.ORDER_FILL, fill)

    async def _live_fill(
        self,
        ev:         StraddleOrderEvent,
        client_id:  str,
        binding_id: str,
        broker,
        paper:      bool = False,
    ) -> None:
        """Place actual SELL/BUY orders via broker API.

        paper=True → STILL sends the real order (so the client can verify the order routes to
        their broker from the whitelisted IP), but books a LOCAL simulated fill at the strategy
        LTP regardless of the broker's response (the order is expected to reject for no-fund).
        paper=False → books the real broker average fill and keeps the order_id."""
        from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
        from data_layer.instrument_registry import REGISTRY as _REG
        from config.global_config import IST as _IST
        from datetime import datetime as _dt

        # Resolve the execution broker's provider + active expiry, then the broker-specific
        # symbol via the registry (mirrors ic_bridge). SymbolTranslator has no
        # 'to_broker_symbol' — that call was crashing the whole bridge.
        _b = getattr(broker, "_binding", None)
        provider = (_b.provider if _b else getattr(broker, "provider", "mock"))
        _today = _dt.now(_IST).date()
        expiry = getattr(ev, "expiry", None)
        if not expiry:
            _exps = _REG.all_expiries(ev.underlying)
            expiry = next((e for e in _exps if e >= _today), _today)

        qty = ev.lot_size * ev.lot_multiplier
        side = OrderSide.SELL if ev.action == "ENTRY" else OrderSide.BUY
        # Strategy-wise product (MIS/NRML) from the sell_straddle config — was hardcoded
        # INTRADAY (ignored by the broker, which used the binding default). Now per-strategy.
        try:
            from data_layer.runtime_config import RuntimeConfig as _RC
            _ss_product = str(_RC.index_section(ev.underlying, "sell_straddle").get("product_type", "MIS")).upper()
        except Exception:
            _ss_product = "MIS"
        if _ss_product not in ("MIS", "NRML"):
            _ss_product = "MIS"

        # Crypto (Delta, wide spreads) → LIMIT-at-mid with chase→market via SmartOrderExecutor; NSE/MCX
        # → MARKET. Both legs execute CONCURRENTLY so neither sits half-on while the other is worked
        # (minimises naked-leg risk during a chase). Position is booked from the REAL fill, not LTP.
        _use_limit = (order_exchange(ev.underlying) == "DELTA")

        # An EXIT must get flat promptly → faster mid-then-market executor; ENTRY tries the mid harder.
        _ex = self._exit_executor if ev.action == "EXIT" else self._executor

        async def _do_leg(opt_type, strike):
            symbol = _resolve_option_symbol(ev.underlying, expiry, int(strike), opt_type, provider)
            _fallback_ltp = ev.ce_ltp if opt_type == "CE" else ev.pe_ltp
            if not symbol:
                logger.warning("StraddleBridge: no %s symbol for %s %d%s — skipping leg",
                               provider, ev.underlying, int(strike), opt_type)
                return opt_type, _fallback_ltp, 0, symbol
            try:
                legfill = await _ex.execute_leg(
                    broker, broker_symbol=symbol, exchange=order_exchange(ev.underlying),
                    side=side, qty=qty, product=_ss_product,
                    tag=f"SS_{ev.underlying}_{ev.action}", client_id=client_id,
                    use_limit=_use_limit, tick=0.0,
                )
                _avg = float(getattr(legfill, "avg_price", 0.0) or 0.0)
                _px = _avg if _avg > 0 else _fallback_ltp
                _fq = int(getattr(legfill, "filled_qty", 0) or 0)
                _oids = getattr(legfill, "order_ids", []) or []
                if _oids:
                    self._order_ids.setdefault((client_id, binding_id, ev.underlying), {})[opt_type] = str(_oids[-1])
                logger.info("[LIVE] %s %s %s — filled %d@%.4f via %s (orders=%s) | client=%s",
                            ev.action, ev.underlying, opt_type, _fq,
                            _px, "LIMIT-chase" if _use_limit else "MARKET", _oids, client_id)
                self._trade_log.log_event(client_id, binding_id,
                    f"{ev.action} {ev.underlying} {opt_type}{int(strike)} filled "
                    f"{_fq}@{_px:.4f} ({'LIMIT-chase' if _use_limit else 'MARKET'}; orders={_oids})")
                return opt_type, _px, _fq, symbol
            except Exception as exc:
                logger.error("[LIVE] %s %s %s order FAILED: %s — falling back to LTP",
                             ev.action, ev.underlying, opt_type, exc)
                self._trade_log.log_event(client_id, binding_id,
                    f"LIVE {ev.action} {ev.underlying} {opt_type}{int(strike)} ORDER FAILED: {exc}")
                return opt_type, _fallback_ltp, 0, symbol

        _legs = [(ot, st) for ot, st in (("CE", ev.ce_strike), ("PE", ev.pe_strike)) if ot in ev.legs]
        _results = await asyncio.gather(*[_do_leg(ot, st) for ot, st in _legs])
        fills = {ot: px for ot, px, _fq, _sym in _results}
        filled_qty_by_leg = {ot: _fq for ot, _px, _fq, _sym in _results}
        symbol_by_leg = {ot: _sym for ot, _px, _fq, _sym in _results}

        # ── ATOMICITY GUARD (live ENTRY) — never keep a one-sided straddle ──────────────────────
        # If an ENTRY filled asymmetrically (a leg got 0 / partial fills — the Delta cancel-race),
        # FLATTEN whatever DID fill and ABORT, rather than manage a naked leg. (Policy: flatten+abort.)
        if ev.action == "ENTRY" and not paper:
            _full = [ot for ot in fills if filled_qty_by_leg.get(ot, 0) >= qty]
            _partial = [ot for ot in fills if 0 < filled_qty_by_leg.get(ot, 0) < qty]
            _any_filled = [ot for ot in fills if filled_qty_by_leg.get(ot, 0) > 0]
            if _any_filled and len(_full) < len(_legs):
                logger.error("[LIVE] %s ENTRY ASYMMETRIC — filled %s; FLATTENING + ABORTING (no naked leg). client=%s",
                             ev.underlying, filled_qty_by_leg, client_id)
                self._trade_log.log_event(client_id, binding_id,
                    f"ENTRY ABORT {ev.underlying} asymmetric fill {filled_qty_by_leg} — flattening filled legs")
                _close_side = OrderSide.BUY if side == OrderSide.SELL else OrderSide.SELL
                for ot in _any_filled:
                    _sym = symbol_by_leg.get(ot)
                    _fq = filled_qty_by_leg.get(ot, 0)
                    if not _sym or _fq <= 0:
                        continue
                    try:
                        _req = OrderRequest(broker_symbol=_sym, exchange=order_exchange(ev.underlying),
                                            side=_close_side, qty=int(_fq), order_type=OrderType.MARKET,
                                            product=_ss_product, tag=f"SS_{ev.underlying}_ABORT"[:20],
                                            client_id=client_id)
                        _oid = await broker.place_order(_req)
                        logger.info("[LIVE] %s ENTRY-ABORT flatten %s %d → order %s | client=%s",
                                    ev.underlying, ot, int(_fq), _oid, client_id)
                        self._trade_log.log_event(client_id, binding_id,
                            f"ENTRY ABORT flatten {ev.underlying} {ot} {int(_fq)} → {_oid}")
                    except Exception as exc:
                        logger.error("[LIVE] %s ENTRY-ABORT flatten %s FAILED: %s — RECONCILE MANUALLY. client=%s",
                                     ev.underlying, ot, exc, client_id)
                        self._trade_log.log_event(client_id, binding_id,
                            f"ENTRY ABORT flatten {ev.underlying} {ot} FAILED: {exc} — RECONCILE MANUALLY")
                # The UNFILLED leg may have a resting/pending order that fills AFTER this abort (Delta's
                # flaky cancel/status — the exact recurrence: PE reported 0 but its order was still
                # live and filled late → naked short). Cancel its known orders, then RECONCILE against
                # the broker's ACTUAL positions (ground truth) and flatten any residual qty.
                _leg_syms = [s for s in symbol_by_leg.values() if s]
                await self._reconcile_flat(broker, _leg_syms, _ss_product,
                                           order_exchange(ev.underlying), client_id, binding_id,
                                           ev.underlying)
                # Tell the strategy to discard its optimistic position — entry never established.
                abort_ev = StraddleFillEvent(
                    action="ENTRY", underlying=ev.underlying, atm=ev.atm,
                    ce_strike=ev.ce_strike, pe_strike=ev.pe_strike, ce_fill=0.0, pe_fill=0.0,
                    client_id=client_id, binding_id=binding_id, event_id=ev.event_id,
                    paper_mode=paper, legs=ev.legs, entry_aborted=True)
                await self._bus.publish(Topic.ORDER_FILL, abort_ev)
                return

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
            paper_mode = paper,
            legs       = ev.legs,
        )

        if ev.action == "ENTRY":
            self._last_entry[ev.underlying] = ev
            self._trade_log.log_entry(client_id, binding_id, ev, fill_ev)
        else:
            # Prefer real entry prices on the EXIT event (survive restarts); fall back to
            # in-memory last-entry only if absent.
            entry_ev = self._last_entry.get(ev.underlying)
            entry_ce = ev.ce_entry if getattr(ev, "ce_entry", 0.0) else (entry_ev.ce_ltp if entry_ev else 0.0)
            entry_pe = ev.pe_entry if getattr(ev, "pe_entry", 0.0) else (entry_ev.pe_ltp if entry_ev else 0.0)
            self._trade_log.log_exit(client_id, binding_id, ev, fill_ev, entry_ce, entry_pe)

        await self._bus.publish(Topic.ORDER_FILL, fill_ev)
