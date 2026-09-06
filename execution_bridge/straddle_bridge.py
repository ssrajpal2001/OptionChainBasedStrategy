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
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

from config.global_config import IST, Topic, order_exchange
from data_layer.base_feeder import EventBus
from data_layer.instrument_registry import REGISTRY as _REG
from data_layer.runtime_config import RuntimeConfig as _RC
from execution_bridge.base_broker import OrderRequest, OrderSide, OrderType
from strategies.core.gate import can_trade

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
    close_reason:   str  = ""  # populated on EXIT (machine code; used by tests/throttling)
    close_remark:   str  = ""  # populated on EXIT (human-readable context)
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
    # Chosen expiry for this order. None → bridge resolves the nearest active expiry.
    expiry:         Optional[date] = None
    # Strategy decision timestamp for exits. Used so EOD square-off records at the configured time.
    close_time:     Optional[datetime] = None
    # Per-binding refactor: when a per-(client,binding) book emits an order it stamps its OWN
    # identity here, so the bridge routes to EXACTLY that broker (no mirror-to-all). Empty =
    # legacy per-index engine → bridge keeps the old behaviour (route to all eligible brokers).
    client_id:      str  = ""
    binding_id:     str  = ""
    # 2026-09-07: the book's own strategy_name ("sell_straddle" or
    # "sell_straddle_calc_vwap") -- stamped by SellStraddleStrategy._emit_order so
    # trade_history.record() can attribute a row to the real book that placed it,
    # not a hardcoded "sell_straddle" literal. Two books can now share the same
    # (client,binding,underlying) for a deliberate VWAP-source A/B comparison.
    strategy_name:  str  = "sell_straddle"
    # Set by the STRATEGY (never by the bridge) after the fact, on the order_ev it already
    # returned to a caller — True when this EXIT's fill was never confirmed (bridge couldn't
    # route it, or the wait timed out). Callers (e.g. single-side roll code in rolling.py) must
    # check this before treating the leg as actually closed / proceeding to open a new partner.
    close_aborted:  bool = False


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
    paper_mode:  bool = True
    legs:        list = field(default_factory=lambda: ["CE", "PE"])
    # Full broker symbols e.g. C-BTC-64000-140626 / NIFTY24600CE — empty string if unavailable.
    ce_symbol:   str  = ""
    pe_symbol:   str  = ""
    # True when a LIVE ENTRY filled asymmetrically (one leg only) and the bridge flattened the filled
    # leg and ABORTED — the strategy must discard its optimistic position, never manage a naked leg.
    entry_aborted: bool = False
    # True when the bridge could not route the order to any eligible broker (terminal off / no running
    # deployment). The strategy must treat this like an aborted entry and clear its pending flag.
    routing_failed: bool = False
    # True when a LIVE EXIT could not be routed to a broker (resolve_broker_or_alert exhausted its
    # retries) or timed out unconfirmed. The strategy must NOT treat this as a real close -- the
    # position stays open, in memory and on disk, so a later tick can retry the exit. Never fake a
    # close on this path (2026-08-04 incident: bridge told the strategy an EXIT succeeded when the
    # order never reached the broker).
    exit_aborted: bool = False
    # 2026-08-06 CONFIRM-MODEL REDESIGN: split "reached the broker" from "filled". `accepted=True`
    # fires the INSTANT an order_id exists (placement retried up to 3x internally) -- fast, no
    # price yet, ce_fill/pe_fill are 0.0 on this event. The strategy uses this ONLY to know a close/
    # entry is genuinely in flight (stop worrying about duplicate dispatch); it must keep waiting
    # (no timeout) for the LATER, real fill event to actually finalize P&L/position state.
    accepted: bool = False
    # True when placement itself failed after 3 retries (order_id never obtained -- broker
    # unreachable / persistent API error). Distinct from exit_aborted/routing_failed (which mean
    # "we didn't even try" or "gave up waiting for confirmation") -- this means "we tried to place
    # the order and the broker never accepted it". ENTRY: strategy stops for the day. EXIT: the
    # position stays exactly as it was (open, not "closing") so the very next tick retries -- an
    # open real position must never stop being retried for close.
    placement_failed: bool = False


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
        _sides = set(getattr(ev, "legs", None) or ["CE", "PE"])
        credit = (fill.ce_fill if "CE" in _sides else 0.0) + (fill.pe_fill if "PE" in _sides else 0.0)
        _legtag = "+".join(sorted(_sides)) if _sides != {"CE", "PE"} else "CE+PE"
        _ce_str = f"{ev.ce_strike:.0f}@{fill.ce_fill:.2f}" if "CE" in _sides else f"{ev.ce_strike:.0f}@-"
        _pe_str = f"{ev.pe_strike:.0f}@{fill.pe_fill:.2f}" if "PE" in _sides else f"{ev.pe_strike:.0f}@-"
        line = (
            f"{ts} | ENTRY | {ev.underlying} | ATM={ev.atm:.0f} | legs={_legtag} | "
            f"CE={_ce_str} | "
            f"PE={_pe_str} | "
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
        _exit_dt = ev.close_time if getattr(ev, "close_time", None) else datetime.now(IST)
        ts       = _exit_dt.strftime("%Y-%m-%d %H:%M:%S")
        qty      = ev.lot_size * ev.lot_multiplier
        # P&L from the REAL fills (short: entry − buyback), scoped to the legs ACTUALLY closed in this
        # event. NOT ev.realized_pnl — the strategy set that from its cached signal-time LTP, which on a
        # fast-moving leg can diverge from the fill (it even showed a PROFIT on a ratio roll that really
        # filled at a LOSS, because the leg bounced between the trigger and the IOC fill). This makes the
        # log line agree with the dashboard History (which already books off the real fill).
        _sides   = set(getattr(ev, "legs", None) or ["CE", "PE"])
        pnl_pts  = 0.0
        if "CE" in _sides:
            pnl_pts += (entry_ce - fill.ce_fill)
        if "PE" in _sides:
            pnl_pts += (entry_pe - fill.pe_fill)
        _und = str(ev.underlying).upper()
        _is_crypto = _und in ("BTC", "ETH")
        _cv  = 0.001 if _und == "BTC" else (0.01 if _und == "ETH" else 1.0)  # reverted 2026-07-19, same day, later
        _ccy = "$" if _is_crypto else "₹"
        pnl_rs   = pnl_pts * qty * _cv
        _legtag  = "+".join(sorted(_sides)) if _sides != {"CE", "PE"} else "CE+PE"
        _rs_fmt  = f"{pnl_rs:+.2f}" if _is_crypto else f"{pnl_rs:+.0f}"
        line = (
            f"{ts} | EXIT  | {ev.underlying} | ATM={ev.atm:.0f} | legs={_legtag} | "
            f"CE={ev.ce_strike:.0f} {entry_ce:.2f}→{fill.ce_fill:.2f} | "
            f"PE={ev.pe_strike:.0f} {entry_pe:.2f}→{fill.pe_fill:.2f} | "
            f"PnL={pnl_pts:+.2f}pts {_ccy}{_rs_fmt} | "
            f"Reason={ev.close_reason} | "
            f"Remark={getattr(ev, 'close_remark', '') or '-'} | "
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
            _exit_ts = _exit_dt.isoformat(timespec="seconds")
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
                    client_id, getattr(ev, "strategy_name", None) or "sell_straddle", ev.underlying,
                    sum(l["entry"] for l in _legs), sum(l["exit"] for l in _legs),
                    ev.close_reason, sum(l["pnl"] for l in _legs),
                    binding_id=binding_id, legs=_legs,
                    exit_remark=getattr(ev, "close_remark", "") or "",
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
        # Track last ENTRY event per (client_id, binding_id, underlying) for exit price
        # correlation -- 2026-09-06 (stale-value audit F16): previously keyed by
        # underlying alone, which collided across two bindings trading the same
        # underlying concurrently (a real deployment shape here).
        self._last_entry: Dict[Tuple[str, str, str], StraddleOrderEvent] = {}
        # Broker order_ids per (client, binding, underlying) → {"CE": id, "PE": id} so a
        # later close can reference the exact orders the app opened (close-own-legs only,
        # and for cancel/modify of the exact exchange order).
        self._order_ids: Dict[tuple, Dict[str, str]] = {}
        # Slippage-aware executor: crypto LIMIT-at-mid (chase→market); books from the REAL fill.
        from execution_bridge.smart_executor import SmartOrderExecutor
        # ENTRY: no rush — try the mid harder (2 chases × 4s) to save the spread.
        # market_fill_timeout_sec=15.0 (2026-08-06): plain NSE MARKET orders confirmed via a
        # failed cancel_order ("Order cannot be cancelled as it is being processed") taking
        # longer than the old 4s to settle on Zerodha's side under real conditions -- the
        # atomicity guard was aborting real fills that simply hadn't been confirmed yet. Kept
        # separate from fill_timeout_sec so Delta's LIMIT-chase cadence is untouched.
        self._executor = SmartOrderExecutor(fill_timeout_sec=4.0, chase_attempts=2,
                                             market_fill_timeout_sec=15.0)
        # EXIT/square-off: get flat PROMPTLY — try the mid ONCE (2s) then market the remainder, so a
        # kill/EOD/manual square-off doesn't dawdle ~12s on a wide Delta book. Still anti-slippage
        # (one mid attempt) but guarantees a fast flat via the market fallback. Market-side timeout
        # also extended (same root cause as ENTRY) but stays shorter than ENTRY's -- an EXIT
        # genuinely needs to get flat fast, and the position-cross-check safety net doesn't apply
        # here (EXIT was deliberately left out of that fix, see straddle_bridge.py under-fill path).
        self._exit_executor = SmartOrderExecutor(fill_timeout_sec=2.0, chase_attempts=1,
                                                  market_fill_timeout_sec=8.0)
        # 2026-08-06 CRITICAL FIX: per-(client,binding) task chain. run() used to `await
        # self._handle(ev)` directly in its single consumer loop -- meaning EVERY client's
        # orders funneled through one queue processed strictly one-at-a-time, globally. Real
        # incident: gurmeet and ssrajpal2001 both hit their 15:20 EOD force-exit in the same
        # tick (order-request timestamps 13ms apart); gurmeet's order sat queued behind
        # ssrajpal2001's slower one and didn't even reach the broker until ~16s later --
        # blowing past gurmeet's own strategy-side confirm wait even though the individual
        # order, once actually picked up, placed and filled in under a second. With N clients
        # this compounds directly (client #50 could wait for 49 others' orders to clear first).
        # Fix: different (client,binding) keys now run FULLY CONCURRENTLY (one asyncio task
        # each) so one client can never block another's order from even starting. A single
        # client's OWN events still process strictly in the order they were queued (each new
        # task for a key awaits the prior task for that SAME key first) -- entries/rolls/exits
        # for one client's one binding are never reordered or raced against each other.
        self._key_tasks: Dict[Tuple[str, str], asyncio.Task] = {}

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
            key = (ev.client_id or "", ev.binding_id or "")
            prev_task = self._key_tasks.get(key)
            task = asyncio.create_task(self._handle_chained(ev, prev_task, key))
            self._key_tasks[key] = task

    async def _handle_chained(self, ev: StraddleOrderEvent, prev_task: Optional[asyncio.Task],
                               key: Tuple[str, str]) -> None:
        """Run `_handle(ev)` after any prior in-flight order for this SAME (client,binding)
        has finished (preserves per-client ordering), while different keys' tasks run
        concurrently with no wait on each other at all."""
        if prev_task is not None and not prev_task.done():
            try:
                await prev_task
            except Exception:
                pass  # the prior order's own failure was already logged where it happened
        try:
            await self._handle(ev)
        except Exception as exc:
            # One bad order must NOT kill the bridge (which would silently stop ALL
            # future routing). Log and keep serving.
            logger.exception(
                "StraddleExecutionBridge: _handle error for %s %s: %s",
                ev.action, ev.underlying, exc,
            )
        finally:
            # Only clear the slot if we're still the latest task registered for this key
            # (avoid a late-finishing older task wiping a newer one's entry).
            if self._key_tasks.get(key) is asyncio.current_task():
                self._key_tasks.pop(key, None)

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
        _abort_published = False  # a per-binding broker-unavailable abort was already published
        for client in clients:
            if _target and client.client_id != _target[0]:
                continue
            # Fetch live DB state for this client's bindings (checks engine_active)
            db = getattr(self._router, "_client_db", None) or getattr(self._router, "_db", None)
            live_bindings: list = []
            if db and hasattr(db, "get_bindings_safe_sync"):
                try:
                    # 2026-08-23 fix: get_bindings_safe_sync() is a SYNC sqlite3
                    # call that internally retries with a blocking time.sleep()
                    # on "database is locked" contention (client_db.py's own
                    # docstring anticipates this) -- called directly (not via
                    # asyncio.to_thread) here, it freezes the ENTIRE event loop
                    # for every client's tick processing and order routing, not
                    # just this one, for the duration of the retry+DB latency.
                    # Gets worse as more concurrent clients/bindings mean more
                    # DB write contention.
                    live_bindings = await asyncio.to_thread(db.get_bindings_safe_sync, client.client_id)
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
                # underlying, AND terminal_connected/is_trade_enabled — the shared
                # can_trade() gate (strategies/core/gate.py). Replaces the old inline
                # predicate, which diverged between the _target (per-binding) path
                # (checked is_running but not is_trade_enabled) and the legacy broadcast
                # path (checked engine_active but not is_running). NOTE: `engine_active`
                # is deliberately NOT part of can_trade() — no currently-reachable UI
                # control sets it True (the per-broker Trade toggle that used to drive it
                # was removed 2026-06-11 in favor of per-strategy Run toggles; see
                # gate.py's _evaluate() docstring), so it's effectively always 0 in
                # production and would silently block every ENTRY if required here.
                # can_trade() now requires terminal_connected + is_trade_enabled +
                # a running deployment on both paths.
                #
                # An EXIT (buy-to-close) must ALWAYS be allowed to route — a square-off / kill /
                # stop sets is_running=False the instant after the EXIT is published, so gating the
                # close on is_running would strand the open legs on the exchange (the exact bug:
                # "squared in the UI but still open on Delta"). Only ENTRIES are gated on a RUNNING
                # deployment. The EXIT still needs terminal_connected (checked above) to place.
                _is_exit = (ev.action == "EXIT")
                if not _is_exit:
                    # Preserve pre-existing fail-closed behavior when no ClientDB is wired
                    # (can_trade() itself fails OPEN with client_db=None — correct for unit
                    # tests/headless callers, but here `db` missing means we could not read
                    # deployments at all, so the old code always blocked ENTRY in that case).
                    if db is None:
                        continue
                    if not can_trade(client.client_id, binding_id, db, "sell_straddle", ev.underlying):
                        continue

                mode = live_b.get("trading_mode", "paper") or "paper"

                if mode == "paper":
                    # PAPER = PURE LOCAL SIMULATION — never send a real order, never touch the
                    # broker resolver. Use this when you want to backtest / replay without any
                    # broker interaction.
                    logger.info(
                        "StraddleExecutionBridge: routing %s %s → [%s/%s] mode=paper",
                        ev.action, ev.underlying, client.client_id, binding_id,
                    )
                    await self._paper_fill(ev, client.client_id, binding_id, None)
                    routed += 1
                    continue

                # mode in {"paper_route", <live>}: needs a REAL broker instance. Never fall back
                # to _paper_fill on a missing broker here — that fabricates a fill the strategy
                # would treat as a real exchange confirmation (the 2026-08-04 incident). Retry
                # then alert loudly instead.
                from execution_bridge.broker_resolve import resolve_broker_or_alert
                broker = await resolve_broker_or_alert(
                    self._bus, self._router, client.client_id, binding_id, "SellStraddle",
                    context=f"{ev.action} {ev.underlying}",
                )

                logger.info(
                    "StraddleExecutionBridge: routing %s %s → [%s/%s] mode=%s broker=%s",
                    ev.action, ev.underlying, client.client_id, binding_id, mode,
                    "resolved" if broker is not None else "UNAVAILABLE",
                )

                if broker is None:
                    # resolve_broker_or_alert already logged CRITICAL + published SYSTEM_EVENT.
                    # Do NOT increment `routed` -- this attempt did not actually route anywhere,
                    # and letting it count would mask the "no engine-active brokers found"
                    # warning below when every binding in this pass was really an abort.
                    await self._bus.publish(
                        Topic.ORDER_FILL,
                        StraddleFillEvent(
                            action=ev.action,
                            underlying=ev.underlying,
                            atm=ev.atm,
                            ce_strike=ev.ce_strike,
                            pe_strike=ev.pe_strike,
                            ce_fill=0.0,
                            pe_fill=0.0,
                            client_id=client.client_id,
                            binding_id=binding_id,
                            event_id=ev.event_id,
                            paper_mode=False,
                            legs=ev.legs,
                            entry_aborted=(ev.action == "ENTRY"),
                            exit_aborted=(ev.action == "EXIT"),
                            routing_failed=True,
                        ),
                    )
                    _abort_published = True
                    continue
                elif mode == "paper_route":
                    # PAPER_ROUTE = send the real order to the broker for connectivity verification,
                    # but book a LOCAL simulated fill at strategy LTP. Intended for no-fund accounts
                    # where the broker is expected to REJECT the order; the strategy state stays
                    # consistent with the simulation.
                    await self._live_fill(ev, client.client_id, binding_id, broker, paper=True)
                else:
                    # 2026-08-12: opt-in per deployment (strategy_params {"shadow_on_reject": true})
                    # -- looked up from the SAME `deployments` list already fetched above, no extra
                    # DB query. Only ever affects the OrderPlacementFailed fallback inside
                    # _live_fill (see its docstring) -- a genuinely-successful live order is
                    # completely unaffected by this flag.
                    _shadow = False
                    for _d in deployments:
                        if (str(_d.get("binding_id", "")) == binding_id
                                and str(_d.get("strategy_name", "")).lower() == "sell_straddle"
                                and str(_d.get("underlying", "") or _d.get("assigned_instrument", "")).upper()
                                    == ev.underlying.upper()):
                            try:
                                _shadow = bool(json.loads(_d.get("strategy_params") or "{}").get("shadow_on_reject", False))
                            except Exception:
                                _shadow = False
                            break
                    # LIVE: real broker order + real fill, order_id tracked for close-via-order-id.
                    await self._live_fill(ev, client.client_id, binding_id, broker, paper=False,
                                           shadow_on_reject=_shadow)
                routed += 1

        if routed == 0:
            logger.warning(
                "StraddleExecutionBridge: %s %s — no engine-active brokers found. "
                "Ensure Terminal is ON and Engine is ON for at least one broker.",
                ev.action, ev.underlying,
            )
            # If a per-binding broker-unavailable abort was already published inside the loop
            # above, don't publish a second (generic) abort fill for the same event_id — the
            # strategy's waiter only needs one signal.
            if not _abort_published and ev.action == "ENTRY":
                # Publish an aborted fill so the strategy clears its optimistic position and
                # pending flag instead of blocking future entries forever.
                await self._bus.publish(
                    Topic.ORDER_FILL,
                    StraddleFillEvent(
                        action="ENTRY",
                        underlying=ev.underlying,
                        atm=ev.atm,
                        ce_strike=ev.ce_strike,
                        pe_strike=ev.pe_strike,
                        ce_fill=0.0,
                        pe_fill=0.0,
                        client_id=ev.client_id or "",
                        binding_id=ev.binding_id or "",
                        event_id=ev.event_id,
                        legs=ev.legs,
                        entry_aborted=True,
                        routing_failed=True,
                    ),
                )
            elif not _abort_published and ev.action == "EXIT":
                # Same idea for EXIT: no eligible binding was found to route the close to (e.g.
                # terminal not connected). Publish an aborted EXIT fill immediately instead of
                # making the strategy's waiter sit out the full confirmation timeout — the
                # strategy must NOT treat this as a real close (position stays open, retries later).
                await self._bus.publish(
                    Topic.ORDER_FILL,
                    StraddleFillEvent(
                        action="EXIT",
                        underlying=ev.underlying,
                        atm=ev.atm,
                        ce_strike=ev.ce_strike,
                        pe_strike=ev.pe_strike,
                        ce_fill=0.0,
                        pe_fill=0.0,
                        client_id=ev.client_id or "",
                        binding_id=ev.binding_id or "",
                        event_id=ev.event_id,
                        legs=ev.legs,
                        exit_aborted=True,
                        routing_failed=True,
                    ),
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
            # 2026-08-27, real incident: a manual Trade/Terminal-OFF square-off
            # tried to buy-to-close the SOLD legs of a position that had already
            # converted to a deliberate EOD hedge-and-carry (is_hedged_positional
            # =True) -- exactly the position the hedge exists to protect from
            # being flattened by routine controls. Skip it here instead (same
            # spirit as _eod_close_or_hedge's own "if pos.is_hedged_positional:
            # return" guard) -- it only ever closes via the hedge-cumulative-
            # profit check or T-1-from-expiry, never a manual toggle.
            if getattr(pos, "is_hedged_positional", False):
                logger.warning(
                    "StraddleBridge: SQUARE-OFF SKIPPED %s for %s/%s — position is a deliberate "
                    "EOD hedge-and-carry (is_hedged_positional=True); it closes only via the "
                    "hedge-cumulative-profit check or T-1-from-expiry, never a manual toggle.",
                    und, client_id, binding_id,
                )
                self._trade_log.log_event(client_id, binding_id,
                    f"SQUARE-OFF SKIPPED {und} — hedged positional carry, left untouched")
                continue
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
            # 2026-09-06, direct user follow-up (stale-value audit F16): keyed
            # by underlying ALONE, this silently collided the instant two
            # DIFFERENT bindings traded the same underlying concurrently (the
            # confirmed-real deployment shape here -- e.g. ssrajpal2001/SA5770
            # and gurmeet/zerodha both on NIFTY) -- one binding's ENTRY would
            # overwrite the other's cached entry, and the fallback read below
            # could pick up the WRONG binding's prices. Scoping the key by
            # (client_id, binding_id, underlying) makes each binding's own
            # cache genuinely its own; the real fix that already made this
            # safe in practice (ev.ce_entry/pe_entry carried directly on the
            # EXIT event, preferred below) stays unchanged -- this closes the
            # remaining fallback-only exposure.
            self._last_entry[(client_id, binding_id, ev.underlying)] = ev
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
            entry_ev = self._last_entry.get((client_id, binding_id, ev.underlying))
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
        shadow_on_reject: bool = False,
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

        # Crypto (Delta, wide spreads) → LIMIT-at-mid with chase→market via SmartOrderExecutor; NSE/BSE
        # → MARKET. Both legs execute CONCURRENTLY so neither sits half-on while the other is worked
        # (minimises naked-leg risk during a chase). Position is booked from the REAL fill, not LTP.
        _use_limit = (order_exchange(ev.underlying) == "DELTA")
        # 2026-08-25: a real gurmeet/Zerodha order was observed on the broker's own order book as
        # LIMIT instead of the expected MARKET, but the app-side log needed to correlate it had
        # already been flushed by the time this was investigated -- no code path was found that
        # should produce LIMIT for NIFTY (order_exchange("NIFTY")=="NFO" != "DELTA"), so the root
        # cause is still open. This one line makes the actual per-order decision unambiguous and
        # permanent in the log going forward, so a recurrence can be confirmed in seconds instead
        # of lost to a routine log flush/rotation.
        logger.info(
            "StraddleBridge: %s %s exchange=%s -> use_limit=%s (order_type will be %s)",
            ev.action, ev.underlying, order_exchange(ev.underlying), _use_limit,
            "LIMIT-chase" if _use_limit else "MARKET",
        )

        # An EXIT must get flat promptly → faster mid-then-market executor; ENTRY tries the mid harder.
        _ex = self._exit_executor if ev.action == "EXIT" else self._executor

        # 2026-08-06 CONFIRM-MODEL REDESIGN: publish a fast "accepted" signal the instant EVERY
        # expected leg has a real order_id at the broker -- BEFORE waiting for any fill. This is
        # what lets the strategy stop blocking on a slow/uncertain confirm wait; it only needs to
        # know the order genuinely reached the broker, not that it filled yet.
        _expected_legs = len(ev.legs)
        _placed_count = 0
        _accepted_published = False
        _placement_failed = False

        async def _mark_placed(opt_type: str, oid: str) -> None:
            nonlocal _placed_count, _accepted_published
            _placed_count += 1
            if _placed_count >= _expected_legs and not _accepted_published:
                _accepted_published = True
                await self._bus.publish(
                    Topic.ORDER_FILL,
                    StraddleFillEvent(
                        action=ev.action, underlying=ev.underlying, atm=ev.atm,
                        ce_strike=ev.ce_strike, pe_strike=ev.pe_strike,
                        ce_fill=0.0, pe_fill=0.0,
                        client_id=client_id, binding_id=binding_id,
                        event_id=ev.event_id, paper_mode=paper, legs=ev.legs, accepted=True,
                    ),
                )

        async def _do_leg(opt_type, strike):
            from execution_bridge.smart_executor import OrderPlacementFailed
            symbol = _resolve_option_symbol(ev.underlying, expiry, int(strike), opt_type, provider)
            _fallback_ltp = ev.ce_ltp if opt_type == "CE" else ev.pe_ltp
            if not symbol:
                logger.warning("StraddleBridge: no %s symbol for %s %d%s — skipping leg",
                               provider, ev.underlying, int(strike), opt_type)
                return opt_type, _fallback_ltp, 0, symbol
            try:
                legfill = await _ex.execute_leg(
                    broker, broker_symbol=symbol, exchange=order_exchange(ev.underlying),
                    on_placed=lambda oid, _ot=opt_type: _mark_placed(_ot, oid),
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
                # 2026-08-19: a MARKET order that looks fully filled by QUANTITY on the
                # very first poll used to be trusted immediately, with zero further
                # reconciliation -- but the broker's own avg_price field can still be
                # settling for a short moment after the quantity itself looks complete
                # (the exchange-side trade confirmation and the broker API's own
                # average-price reconciliation don't always land in the same instant).
                # Confirmed live: a real gurmeet NIFTY entry recorded CE=102.35/PE=108.90
                # from this exact fast path while the broker's own terminal settled to
                # CE=101.45/PE=108.30 moments later -- both reads we took were genuine
                # broker-reported averages, just captured before the broker had finished
                # updating them. ENTRY only (the case under discussion; EXIT/P&L timing
                # has different risk characteristics and isn't touched here). The
                # _fq < qty branch below already does its own, more thorough
                # reconciliation loop for a genuine under-fill -- this covers the
                # complementary "looked fully filled immediately" case that branch never
                # reaches.
                if ev.action == "ENTRY" and _avg > 0 and _fq >= qty:
                    _settled_px = _px
                    try:
                        if _oids and hasattr(broker, "get_order_status"):
                            await asyncio.sleep(1.5)
                            _settled = await broker.get_order_status(str(_oids[-1]))
                            _settled_avg = float(getattr(_settled, "avg_price", 0.0) or 0.0)
                            if _settled_avg > 0:
                                _settled_px = _settled_avg
                        if hasattr(broker, "get_positions"):
                            _all_pos = await broker.get_positions()
                            for _pos in _all_pos:
                                if (_pos.symbol == symbol and _pos.avg_price > 0
                                        and abs(_pos.qty) >= qty):
                                    _settled_px = _pos.avg_price   # ground truth wins over the order-status re-poll
                                    break
                        if abs(_settled_px - _px) > 1e-6:
                            logger.info(
                                "[LIVE] %s %s %s — avg price SETTLED after re-check: %.4f -> %.4f "
                                "(first read was still updating on the broker's side)",
                                ev.action, ev.underlying, opt_type, _px, _settled_px,
                            )
                            self._trade_log.log_event(client_id, binding_id,
                                f"{ev.action} {ev.underlying} {opt_type}{int(strike)} avg SETTLED "
                                f"{_px:.4f} -> {_settled_px:.4f}")
                            _px = _settled_px
                            _avg = _settled_px
                    except Exception:
                        logger.debug(
                            "[LIVE] %s %s %s settle re-check failed (keeping first-read avg=%.4f).",
                            ev.action, ev.underlying, opt_type, _px,
                        )
                # Under-fill with NO exception (order was accepted but didn't fully fill) — pull the
                # EXCHANGE's final order state so the reason is exchange-sourced, not inferred
                # (distinguishes 'rested unfilled / cancelled' from a margin/contract rejection).
                # If filled_qty=0, poll order status until broker confirms real fill. 2026-08-06:
                # extended 5s->15s -- confirmed via a failed cancel_order ("Order cannot be
                # cancelled as it is being processed") that Zerodha needed longer than 5s to settle
                # a real, genuinely-filling MARKET order; giving up early made the atomicity guard
                # abort fills that would have confirmed moments later.
                #
                # 2026-08-06 SPEED FIX: this loop used to blindly run all 15 attempts (15s)
                # regardless of the broker's actual reported status -- including for an order
                # that was REJECTED on the very first poll (e.g. paper_route's expected
                # no-funds rejection, or any real margin/contract rejection), which can never
                # later show a fill. SmartOrderExecutor._await_fill already short-circuits on
                # REJECTED/CANCELLED (smart_executor.py); this loop is the SEPARATE retry pass
                # straddle_bridge.py runs on top of that, and it was blind to the same signal --
                # so a known-dead order still cost a further ~15s here on every single close/entry
                # for a no-funds R&D client, compounding across every roll in a session. Only a
                # genuinely still-PENDING/OPEN order should keep polling; a terminal
                # REJECTED/CANCELLED status ends the wait immediately.
                from execution_bridge.base_broker import OrderStatus as _OrderStatus
                if _fq < qty and _oids and hasattr(broker, "get_order_status"):
                    try:
                        _f = None
                        for _attempt in range(15):
                            await asyncio.sleep(1)
                            _f = await broker.get_order_status(str(_oids[-1]))
                            _real_avg = float(getattr(_f, "avg_price", 0.0) or 0.0)
                            # OrderFill's field is `qty`, not `filled_qty` -- the old
                            # getattr(_f, "filled_qty", 0) always silently returned the
                            # 0 default regardless of the real value.
                            _real_qty = int(getattr(_f, "qty", 0) or 0)
                            if _real_avg > 0:
                                _px = _real_avg
                                _fq = _real_qty if _real_qty > 0 else qty
                                logger.info("[LIVE] %s %s %s — broker fill confirmed after %ds: qty=%d avg=%.4f",
                                            ev.action, ev.underlying, opt_type, _attempt+1, _fq, _px)
                                break
                            if getattr(_f, "status", None) in (_OrderStatus.REJECTED, _OrderStatus.CANCELLED):
                                logger.info(
                                    "[LIVE] %s %s %s — broker reports terminal %s after %ds, "
                                    "stopping poll early (was going to wait up to 15s).",
                                    ev.action, ev.underlying, opt_type, _f.status, _attempt + 1,
                                )
                                break
                        if _f is not None and _px == _fallback_ltp:
                            # 2026-08-06: the order-status endpoint (e.g. Kite order_history)
                            # has been observed returning an empty/inconclusive response for
                            # orders that DID fill at the exchange -- twice today a real,
                            # confirmed-in-broker fill was discarded as "unfilled" this way,
                            # leaving a genuinely open position untracked. Before giving up,
                            # cross-check the broker's ACTUAL positions -- ground truth for
                            # what's really held, independent of whether the status endpoint
                            # kept up. ENTRY only (the observed failure mode); EXIT keeps the
                            # existing conservative behavior since "did the qty reduce" is a
                            # harder match to make safely without the pre-exit baseline.
                            # NSE/BSE only -- Delta (crypto) has its own dedicated, battle-tested
                            # late-residual-fill reconcile further down this handler (2026-06-13
                            # cancel-race fix); this earlier check must not preempt it.
                            _confirmed_via_position = False
                            if ev.action == "ENTRY" and not _use_limit and hasattr(broker, "get_positions"):
                                try:
                                    _all_pos = await broker.get_positions()
                                    for _pos in _all_pos:
                                        if (_pos.symbol == symbol and _pos.avg_price > 0
                                                and abs(_pos.qty) >= qty):
                                            _px = _pos.avg_price
                                            _fq = qty
                                            _confirmed_via_position = True
                                            logger.info(
                                                "[LIVE] %s %s %s — order-status inconclusive but "
                                                "broker POSITION confirms fill: qty=%d avg=%.4f "
                                                "(status endpoint was stale/empty, not the broker)",
                                                ev.action, ev.underlying, opt_type, _fq, _px,
                                            )
                                            self._trade_log.log_event(client_id, binding_id,
                                                f"{ev.action} {ev.underlying} {opt_type}{int(strike)} "
                                                f"CONFIRMED via broker position (order-status was "
                                                f"inconclusive) {_fq}@{_px:.4f}")
                                            break
                                    else:
                                        # No match -- log what we searched for vs what the broker
                                        # actually returned, so a symbol-format mismatch is visible
                                        # immediately instead of looking identical to "genuinely
                                        # no position" (the previous silent gap).
                                        logger.warning(
                                            "[LIVE] %s %s %s position cross-check: NO MATCH for "
                                            "symbol=%r qty>=%d among %d broker position(s): %s",
                                            ev.action, ev.underlying, opt_type, symbol, qty,
                                            len(_all_pos),
                                            [(p.symbol, p.qty, p.avg_price) for p in _all_pos],
                                        )
                                except Exception as exc:
                                    logger.warning(
                                        "[LIVE] %s %s %s position cross-check failed: %s",
                                        ev.action, ev.underlying, opt_type, exc,
                                    )
                            if not _confirmed_via_position:
                                _raw = getattr(_f, "raw", {}) or {}
                                # 2026-08-06: the old `state=%r` read a field that does not exist in
                                # Kite's response at all (Zerodha calls it `status`) -- this log has
                                # NEVER actually shown the real order status or rejection reason in
                                # months of use. Fixed to read the real parsed OrderFill.status plus
                                # Kite's actual raw fields (status/status_message/pending_quantity),
                                # so a genuine REJECTED-with-reason is finally distinguishable from a
                                # real pending/still-processing order.
                                _reason = (f"parsed_status={getattr(_f,'status',None)} "
                                           f"raw_status={_raw.get('status')} "
                                           f"status_message={_raw.get('status_message')} "
                                           f"pending_qty={_raw.get('pending_quantity')} "
                                           f"filled_qty={_raw.get('filled_quantity')} "
                                           f"avg={getattr(_f,'avg_price',0)}")
                                logger.warning("[LIVE] %s %s %s UNDER-FILL %d/%d — exchange: %s",
                                               ev.action, ev.underlying, opt_type, _fq, qty, _reason)
                                self._trade_log.log_event(client_id, binding_id,
                                    f"{ev.action} {ev.underlying} {opt_type}{int(strike)} UNDER-FILL {_fq}/{qty} — exchange: {_reason}")
                    except Exception:
                        pass
                return opt_type, _px, _fq, symbol
            except OrderPlacementFailed as exc:
                # The order never reached the broker at all -- distinct from a REJECTED/
                # under-filled order (which DID reach the broker). Never fake acceptance here.
                nonlocal _placement_failed
                _placement_failed = True
                logger.error("[LIVE] %s %s %s PLACEMENT FAILED (order never reached broker): %s",
                             ev.action, ev.underlying, opt_type, exc)
                self._trade_log.log_event(client_id, binding_id,
                    f"LIVE {ev.action} {ev.underlying} {opt_type}{int(strike)} PLACEMENT FAILED: {exc}")
                # 2026-08-12 direct request: surface the REAL broker rejection text in the
                # dashboard, not just server-side logs the client can't see. Published on
                # Topic.POSITION_UPDATE (not SYSTEM_EVENT) deliberately -- ws_bridge.py's
                # _sys_loop strips SYSTEM_EVENT payloads down to {code,msg}, dropping
                # client_id/binding_id; POSITION_UPDATE is forwarded verbatim, so this
                # dict reaches the browser with its client_id intact for the frontend to
                # filter on (never shown to a different client's session).
                if self._bus is not None:
                    asyncio.create_task(self._bus.publish(Topic.POSITION_UPDATE, {
                        "type": "strategy_alert", "severity": "critical",
                        "client_id": client_id, "binding_id": binding_id,
                        "underlying": ev.underlying,
                        "msg": f"{ev.underlying} {opt_type}{int(strike)} order REJECTED by broker: {exc}",
                        "ts": datetime.now(IST).isoformat(),
                    }))
                return opt_type, _fallback_ltp, 0, symbol
            except Exception as exc:
                logger.error("[LIVE] %s %s %s order FAILED: %s — falling back to LTP",
                             ev.action, ev.underlying, opt_type, exc)
                self._trade_log.log_event(client_id, binding_id,
                    f"LIVE {ev.action} {ev.underlying} {opt_type}{int(strike)} ORDER FAILED: {exc}")
                return opt_type, _fallback_ltp, 0, symbol

        _legs = [(ot, st) for ot, st in (("CE", ev.ce_strike), ("PE", ev.pe_strike)) if ot in ev.legs]
        _results = await asyncio.gather(*[_do_leg(ot, st) for ot, st in _legs])

        if _placement_failed:
            # At least one leg's order never reached the broker. ENTRY: tell the strategy to
            # stop-for-day (nothing was risked, no naked leg -- whatever DID place, if anything,
            # still needs the existing atomicity-guard flatten below, so fall through for ENTRY
            # rather than return early). EXIT: the position must stay exactly "open" (never
            # "closing") so the very next tick retries -- an open real position must never stop
            # being retried for close, per explicit requirement.
            logger.critical(
                "[LIVE] %s %s — PLACEMENT FAILED for at least one leg after 3 retries each. "
                "%s", ev.action, ev.underlying,
                "Stopping entries for today." if ev.action == "ENTRY"
                else "Position stays OPEN; will retry next tick.",
            )
            self._trade_log.log_event(client_id, binding_id,
                f"{ev.action} {ev.underlying} PLACEMENT FAILED for at least one leg after retries")
            if ev.action == "EXIT" and shadow_on_reject and not any(_fq > 0 for _ot, _px, _fq, _sym in _results):
                # Mirror of the ENTRY shadow branch below -- a shadow position (entered via
                # _paper_fill after a rejection) still routes through this SAME live path on
                # exit, since the binding's own trading_mode is genuinely "live" and has no
                # memory of any individual position's paper_mode. Without this, a shadow
                # position could never close -- it would sit "open" forever, retrying an exit
                # against a broker that will never accept it. Same all-or-nothing safety
                # check as ENTRY: only when no leg got any real fill.
                logger.warning(
                    "[LIVE] %s EXIT — shadow_on_reject fallback: broker rejected, no real fill "
                    "on either leg, closing the SIMULATED position at strategy LTP. client=%s",
                    ev.underlying, client_id,
                )
                await self._paper_fill(ev, client_id, binding_id, broker)
                return
            elif ev.action == "EXIT":
                abort_ev = StraddleFillEvent(
                    action="EXIT", underlying=ev.underlying, atm=ev.atm,
                    ce_strike=ev.ce_strike, pe_strike=ev.pe_strike, ce_fill=0.0, pe_fill=0.0,
                    client_id=client_id, binding_id=binding_id, event_id=ev.event_id,
                    paper_mode=paper, legs=ev.legs, placement_failed=True)
                await self._bus.publish(Topic.ORDER_FILL, abort_ev)
                return
            elif paper:
                # ENTRY + paper mode: the live-only atomicity guard below never runs for
                # paper, so handle the abort here directly rather than falling through to a
                # fabricated successful fill_ev at the bottom of this function.
                abort_ev = StraddleFillEvent(
                    action="ENTRY", underlying=ev.underlying, atm=ev.atm,
                    ce_strike=ev.ce_strike, pe_strike=ev.pe_strike, ce_fill=0.0, pe_fill=0.0,
                    client_id=client_id, binding_id=binding_id, event_id=ev.event_id,
                    paper_mode=paper, legs=ev.legs, entry_aborted=True, placement_failed=True)
                await self._bus.publish(Topic.ORDER_FILL, abort_ev)
                return
            elif shadow_on_reject and not any(_fq > 0 for _ot, _px, _fq, _sym in _results):
                # 2026-08-12, direct request, opt-in per deployment: order never reached the
                # broker AND -- critically -- NO leg got any real fill at all (a clean, total
                # failure, e.g. today's real SENSEX BFO-segment restriction, which blocks the
                # whole exchange segment so both legs fail identically). Fall back to the SAME
                # _paper_fill() a genuinely paper-mode deployment already uses -- not a
                # hand-rolled event -- so the position runs full real exit logic (SL/TSL/Day%/
                # etc.) against a fill that never touched the broker, tagged paper_mode=True,
                # never confusable with a real fill. If ANY leg DID get a real fill
                # (asymmetric), this branch is skipped entirely and control falls through to
                # the existing atomicity guard below, which flattens the real leg(s) and
                # aborts -- never silently paired with a fake one.
                logger.warning(
                    "[LIVE] %s ENTRY — shadow_on_reject fallback: broker rejected, no real fill "
                    "on either leg, booking a SIMULATED position at strategy LTP. client=%s",
                    ev.underlying, client_id,
                )
                await self._paper_fill(ev, client_id, binding_id, broker)
                return
            # ENTRY + live: fall through to the atomicity guard below, which will see
            # filled_qty_by_leg short of `qty` for the failed leg(s) and flatten+abort with
            # placement_failed=_placement_failed threaded onto its own abort_ev.

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
            # 2026-08-05: was `if _any_filled and len(_full) < len(_legs)` -- only guarded
            # ASYMMETRIC fills (one leg succeeded, one failed). A SYMMETRIC total failure
            # (confirmed live: Fyers rejecting BOTH legs identically with "Algo orders are
            # not allowed from this app") fell through this guard entirely (_any_filled was
            # empty) and was reported to the strategy as a normal successful entry at the
            # fallback LTP price -- a fully fabricated position, no real order ever reached
            # the broker. Now triggers whenever not ALL legs achieved a full fill, regardless
            # of whether some, none, or all legs filled.
            if len(_full) < len(_legs):
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
                    paper_mode=paper, legs=ev.legs, entry_aborted=True,
                    # Only true when a leg's order never reached the broker at all (placement
                    # exhausted its 3 retries) -- distinguishes "stop entries for today" from a
                    # recoverable asymmetric-fill abort, which should NOT stop future entries.
                    placement_failed=_placement_failed)
                await self._bus.publish(Topic.ORDER_FILL, abort_ev)
                return

        # ── EXIT INCOMPLETE-FILL GUARD (live) ────────────────────────────────────────
        # 2026-08-05: mirrors the ENTRY guard above -- a live EXIT whose leg(s) got
        # rejected/failed at the broker (confirmed live: Fyers "Algo orders are not
        # allowed") used to fall straight through to the normal fill_ev construction
        # below, which reports the fallback LTP price as if the close genuinely
        # happened. exit_aborted already exists on StraddleFillEvent specifically for
        # "never fake a close" (2026-08-04), but was only ever set for the
        # couldn't-route-to-any-broker case, not for a broker that WAS reached but
        # REJECTED the order. The strategy's own confirm-then-finalize wait
        # (_close_position/_close_leg) already correctly leaves the position open and
        # retries on close_aborted/exit_aborted -- this guard is what actually tells it
        # to do so instead of believing a fabricated close.
        if ev.action == "EXIT" and not paper:
            _full_exit = [ot for ot in fills if filled_qty_by_leg.get(ot, 0) >= qty]
            if len(_full_exit) < len(_legs):
                logger.error(
                    "[LIVE] %s EXIT INCOMPLETE — filled %s of legs %s; NOT reporting a fake "
                    "close. Position stays open in the strategy's state, will retry. client=%s",
                    ev.underlying, filled_qty_by_leg, [ot for ot, _ in _legs], client_id,
                )
                self._trade_log.log_event(client_id, binding_id,
                    f"EXIT ABORT {ev.underlying} incomplete fill {filled_qty_by_leg} — "
                    f"position stays open, will retry")
                abort_ev = StraddleFillEvent(
                    action="EXIT", underlying=ev.underlying, atm=ev.atm,
                    ce_strike=ev.ce_strike, pe_strike=ev.pe_strike, ce_fill=0.0, pe_fill=0.0,
                    client_id=client_id, binding_id=binding_id, event_id=ev.event_id,
                    paper_mode=paper, legs=ev.legs, exit_aborted=True)
                await self._bus.publish(Topic.ORDER_FILL, abort_ev)
                return

        # Single-leg EXIT/ENTRY events must not carry a fake fill price for the untouched leg;
        # otherwise the strategy logs/trade-history look like the whole straddle was closed/re-opened.
        _ce_in = "CE" in ev.legs
        _pe_in = "PE" in ev.legs
        fill_ev = StraddleFillEvent(
            action     = ev.action,
            underlying = ev.underlying,
            atm        = ev.atm,
            ce_strike  = ev.ce_strike,
            pe_strike  = ev.pe_strike,
            ce_fill    = fills.get("CE", ev.ce_ltp) if _ce_in else 0.0,
            pe_fill    = fills.get("PE", ev.pe_ltp) if _pe_in else 0.0,
            client_id  = client_id,
            binding_id = binding_id,
            event_id   = ev.event_id,
            paper_mode = paper,
            legs       = ev.legs,
            ce_symbol  = symbol_by_leg.get("CE", "") if _ce_in else "",
            pe_symbol  = symbol_by_leg.get("PE", "") if _pe_in else "",
        )

        if ev.action == "ENTRY":
            # 2026-09-06 (stale-value audit F16): scoped per-binding, same fix as
            # _paper_fill's own _last_entry write above -- see that comment.
            self._last_entry[(client_id, binding_id, ev.underlying)] = ev
            self._trade_log.log_entry(client_id, binding_id, ev, fill_ev)
        else:
            # Prefer real entry prices on the EXIT event (survive restarts); fall back to
            # in-memory last-entry only if absent.
            entry_ev = self._last_entry.get((client_id, binding_id, ev.underlying))
            entry_ce = ev.ce_entry if getattr(ev, "ce_entry", 0.0) else (entry_ev.ce_ltp if entry_ev else 0.0)
            entry_pe = ev.pe_entry if getattr(ev, "pe_entry", 0.0) else (entry_ev.pe_ltp if entry_ev else 0.0)
            self._trade_log.log_exit(client_id, binding_id, ev, fill_ev, entry_ce, entry_pe)

        await self._bus.publish(Topic.ORDER_FILL, fill_ev)
