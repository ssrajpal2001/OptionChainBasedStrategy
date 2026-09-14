"""strategies/iron_fly/engine.py -- IronFlyEngine, the stateful driver of
strategies/iron_fly/detector.py's pure functions.

Deliberately synchronous and framework-agnostic for Phase 1 (backtest only,
per the approved plan) -- no EventBus/asyncio/execution-bridge dependency.
`on_spot_tick(spot, get_premium)` is the single entry point: `get_premium`
is a `(strike, side) -> Optional[float]` callable the caller supplies (a
historical-bar lookup in the backtest, a live-tick cache in a future Phase 2
async wrapper around this same class). Every fill in this phase is an
instant paper-style fill at whatever `get_premium` returns -- there is no
broker interaction here at all; that is explicitly out of scope until
Phase 2 (paper_route wiring).

Priority ladder per tick (approved plan, Client Guide Section 11):
  1. profit >= 65% of the cycle's frozen expected max profit -> close all,
     immediately search for a fresh Iron Condor.
  2. a short strike has become the live ATM -> convert that side to Iron
     Fly (permanent for the rest of the cycle -- no reversion exists in the
     source doc).
  3. a pending gap adjustment fires (price retraced to the original trigger).
  4. +/-100pt move on a side not yet fly-converted -> roll that side, or
     arm a pending adjustment if the move was a gap (overshot by a full
     extra increment).
  5. otherwise hold.

Phase 2 (2026-09-14, direct user spec: paper_route live tomorrow) adds
`IronFlyStrategy` below -- the async per-(client,binding) live book wrapping
`IronFlyEngine`. Deliberately a thin wrapper, not a rewrite: the engine's
own decide-and-immediately-mutate model (the same one the backtest already
validated) stays authoritative. Every tick, the wrapper snapshots the 4 leg
slots before calling `engine.on_spot_tick()`, diffs against the post-tick
state via `diff_legs_for_orders()`, and fires one `IronFlyOrderEvent` per
changed leg to `execution_bridge/iron_fly_bridge.py` for real (paper_route)
broker routing + trade-history recording -- fire-and-forget, not blocking
the next tick on a fill confirmation. This matches paper_route's own
established contract (the bridge always finalizes a fill, simulated if the
real broker doesn't confirm one) and is an explicit, honestly-flagged
simplification versus a full accept-then-mutate live model: correct for
paper_route where nothing here risks real capital, but would need a real
confirm-before-mutate redesign before this could carry live money.
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Callable, Deque, Dict, List, Optional, Tuple

from data_layer import position_store
from strategies.iron_fly.detector import (
    Leg,
    PendingAdjustment,
    check_atm_conversion,
    check_pending_fire,
    classify_move,
    cycle_pnl,
    expected_max_profit,
    find_long_strike,
    find_short_strike,
    leg_pnl,
    profit_target_hit,
    reconcile_protective_leg,
    round_to_atm,
)

logger = logging.getLogger(__name__)

PremiumFn = Callable[[int, str], Optional[float]]

_LEG_SLOTS = ("short_ce", "long_ce", "short_pe", "long_pe")


def snapshot_legs(engine: "IronFlyEngine") -> Dict[str, Optional[Leg]]:
    return {slot: getattr(engine, slot) for slot in _LEG_SLOTS}


def diff_legs_for_orders(
    prev_snapshot: Dict[str, Optional[Leg]],
    engine: "IronFlyEngine",
    new_log_events: List[dict],
) -> Tuple[List[dict], List[dict]]:
    """Compares a snapshot taken BEFORE an `on_spot_tick()` call against the
    engine's CURRENT legs (taken right after), and returns (closed, opened)
    -- everything a live async wrapper needs to translate one tick's worth
    of engine decisions into real broker orders, regardless of which event
    type caused them (ENTRY, ROLL_CALL, ROLL_PUT, FLY_CONVERSION, or a
    profit-target close+reinit all touch different combinations of the 4
    leg slots, but the diff logic is the same for all of them).

    Each `closed` entry's `close_price` is pulled from the matching CLOSE
    line in `new_log_events` (the engine's own trade_log slice added during
    this tick) -- the actual price the engine used to realize that leg's
    P&L, not an approximation. Each `opened` entry carries the new Leg
    object directly (its `entry_price` is already the engine's own live-LTP
    decision for this tick)."""
    closed: List[dict] = []
    opened: List[dict] = []
    curr = snapshot_legs(engine)
    for slot in _LEG_SLOTS:
        side = "CE" if slot.endswith("ce") else "PE"
        old_leg = prev_snapshot.get(slot)
        new_leg = curr.get(slot)
        old_strike = old_leg.strike if old_leg else None
        new_strike = new_leg.strike if new_leg else None
        if old_strike == new_strike:
            continue
        if old_leg is not None:
            match = next(
                (e for e in new_log_events
                 if e.get("event") == "CLOSE" and e.get("strike") == old_leg.strike and e.get("side") == side),
                None,
            )
            close_price = match["price"] if match is not None else old_leg.entry_price
            closed.append({"slot": slot, "side": side, "leg": old_leg, "close_price": close_price})
        if new_leg is not None:
            opened.append({"slot": slot, "side": side, "leg": new_leg})
    return closed, opened


class IronFlyEngine:
    def __init__(
        self,
        qty: int,
        strike_step: float = 50.0,
        otm1: int = 50,
        adjustment_distance: float = 100.0,
        short_threshold: float = 20.0,
        long_threshold: float = 20.0,
        profit_target_pct: float = 0.65,
        max_search_steps: int = 60,
    ) -> None:
        self.qty = qty
        self.strike_step = strike_step
        self.otm1 = otm1
        self.adjustment_distance = adjustment_distance
        self.short_threshold = short_threshold
        self.long_threshold = long_threshold
        self.profit_target_pct = profit_target_pct
        self.max_search_steps = max_search_steps

        self.short_ce: Optional[Leg] = None
        self.long_ce: Optional[Leg] = None
        self.short_pe: Optional[Leg] = None
        self.long_pe: Optional[Leg] = None
        self.is_flied = False  # True once ANY conversion has fired -- the whole
                                # position is a fixed Iron Fly for the rest of the
                                # cycle, per the approved plan; not per-side.
        self.reference_price: Optional[float] = None
        self.cycle_expected_max_profit: Optional[float] = None
        self.cycle_realized_pnl: float = 0.0
        self.pending_adjustment: Optional[PendingAdjustment] = None
        self.cycle_number = 0
        self.trade_log: List[dict] = []
        self._current_ts = None
        self.order_count = 0  # every leg opened OR closed, +1 each -- a
                               # LIFETIME counter across cycles (real
                               # brokerage/charges don't reset at a cycle
                               # boundary), used by the backtest script to
                               # report total transaction costs.
        self.lifetime_realized_pnl = 0.0  # like cycle_realized_pnl but never
                                           # resets at a cycle boundary --
                                           # the running total for a cost-
                                           # adjusted P&L ledger across the
                                           # whole backtest, not just the
                                           # current cycle's 65%-target math.

    # ── state helpers ────────────────────────────────────────────────────

    def is_flat(self) -> bool:
        return self.short_ce is None and self.short_pe is None

    def _leg_sides(self) -> List[Tuple[Optional[Leg], str]]:
        return [
            (self.short_ce, "CE"), (self.long_ce, "CE"),
            (self.short_pe, "PE"), (self.long_pe, "PE"),
        ]

    def _legs_dict(self) -> dict:
        return {
            "short_ce": self.short_ce.strike if self.short_ce else None,
            "long_ce": self.long_ce.strike if self.long_ce else None,
            "short_pe": self.short_pe.strike if self.short_pe else None,
            "long_pe": self.long_pe.strike if self.long_pe else None,
        }

    def _log(self, event: str, **fields) -> None:
        fields.setdefault("ts", self._current_ts)
        fields.setdefault("order_count", self.order_count)
        fields.setdefault("lifetime_realized_pnl", self.lifetime_realized_pnl)
        self.trade_log.append({"event": event, **fields})

    def _candidate_strikes(self, atm: int, direction: int) -> List[int]:
        return [int(atm + direction * k * self.strike_step) for k in range(0, self.max_search_steps + 1)]

    def _search_side(self, spot: float, side: str, direction: int, get_premium: PremiumFn) -> Optional[Tuple[Leg, Leg]]:
        atm = round_to_atm(spot, self.strike_step)
        strikes = self._candidate_strikes(atm, direction)
        premiums = {s: get_premium(s, side) for s in strikes}
        short_strike = find_short_strike(strikes, premiums, self.short_threshold)
        if short_strike is None:
            return None
        idx = strikes.index(short_strike)
        long_strike = find_long_strike(strikes[idx + 1:], premiums, self.long_threshold)
        if long_strike is None:
            return None
        short_leg = Leg(strike=short_strike, entry_price=premiums[short_strike], qty=self.qty, is_short=True, side=side)
        long_leg = Leg(strike=long_strike, entry_price=premiums[long_strike], qty=self.qty, is_short=False, side=side)
        return short_leg, long_leg

    def _close_leg(self, leg: Leg, side: str, get_premium: PremiumFn) -> None:
        price = get_premium(leg.strike, side)
        if price is None:
            price = leg.entry_price
        pnl = leg_pnl(leg, price)
        self.cycle_realized_pnl += pnl
        self.lifetime_realized_pnl += pnl
        self.order_count += 1
        self._log("CLOSE", strike=leg.strike, side=side, is_short=leg.is_short, price=price, pnl=pnl)

    def _close_side(self, side: str, get_premium: PremiumFn) -> None:
        if side == "CE":
            for leg in (self.short_ce, self.long_ce):
                if leg is not None:
                    self._close_leg(leg, "CE", get_premium)
            self.short_ce = self.long_ce = None
        else:
            for leg in (self.short_pe, self.long_pe):
                if leg is not None:
                    self._close_leg(leg, "PE", get_premium)
            self.short_pe = self.long_pe = None

    def _close_all(self, get_premium: PremiumFn) -> None:
        self._close_side("CE", get_premium)
        self._close_side("PE", get_premium)

    # ── entry / rolls ────────────────────────────────────────────────────

    def try_enter(self, spot: float, get_premium: PremiumFn) -> bool:
        if not self.is_flat():
            return False
        call_result = self._search_side(spot, "CE", 1, get_premium)
        put_result = self._search_side(spot, "PE", -1, get_premium)
        if call_result is None or put_result is None:
            return False
        self.short_ce, self.long_ce = call_result
        self.short_pe, self.long_pe = put_result
        self.order_count += 4
        self.reference_price = spot
        self.cycle_expected_max_profit = expected_max_profit(
            self.short_ce.entry_price, self.long_ce.entry_price,
            self.short_pe.entry_price, self.long_pe.entry_price, self.qty,
        )
        self.cycle_realized_pnl = 0.0
        self.is_flied = False  # True once ANY conversion has fired -- the whole
                                # position is a fixed Iron Fly for the rest of the
                                # cycle, per the approved plan; not per-side.
        self.pending_adjustment = None
        self.cycle_number += 1
        self._log("ENTRY", spot=spot, legs=self._legs_dict(), expected_max_profit=self.cycle_expected_max_profit)
        return True

    def roll_call_side(self, spot: float, get_premium: PremiumFn) -> bool:
        result = self._search_side(spot, "CE", 1, get_premium)
        if result is None:
            return False
        self._close_side("CE", get_premium)
        self.short_ce, self.long_ce = result
        self.order_count += 2
        self.reference_price = spot
        self._log("ROLL_CALL", spot=spot, legs=self._legs_dict())
        return True

    def roll_put_side(self, spot: float, get_premium: PremiumFn) -> bool:
        result = self._search_side(spot, "PE", -1, get_premium)
        if result is None:
            return False
        self._close_side("PE", get_premium)
        self.short_pe, self.long_pe = result
        self.order_count += 2
        self.reference_price = spot
        self._log("ROLL_PUT", spot=spot, legs=self._legs_dict())
        return True

    # ── Iron Fly conversion ──────────────────────────────────────────────

    def _convert_put_becomes_atm(self, get_premium: PremiumFn) -> bool:
        atm = self.short_pe.strike
        new_short_strike = atm
        new_long_strike = atm + self.otm1
        short_price = get_premium(new_short_strike, "CE")
        long_price = get_premium(new_long_strike, "CE")
        if short_price is None or long_price is None:
            return False
        needs_replace, correct_pe_long = reconcile_protective_leg(atm, self.long_pe.strike, self.otm1, direction=-1)
        new_pe_long_price = None
        if needs_replace:
            new_pe_long_price = get_premium(correct_pe_long, "PE")
            if new_pe_long_price is None:
                return False
        if self.short_ce is not None or self.long_ce is not None:
            self._close_side("CE", get_premium)
        self.short_ce = Leg(strike=new_short_strike, entry_price=short_price, qty=self.qty, is_short=True, side="CE")
        self.long_ce = Leg(strike=new_long_strike, entry_price=long_price, qty=self.qty, is_short=False, side="CE")
        self.order_count += 2
        if needs_replace:
            self._close_leg(self.long_pe, "PE", get_premium)
            self.long_pe = Leg(strike=correct_pe_long, entry_price=new_pe_long_price, qty=self.qty, is_short=False, side="PE")
            self.order_count += 1
        self.is_flied = True
        self._log("FLY_CONVERSION", trigger="PUT_BECOMES_ATM", legs=self._legs_dict())
        return True

    def _convert_call_becomes_atm(self, get_premium: PremiumFn) -> bool:
        atm = self.short_ce.strike
        new_short_strike = atm
        new_long_strike = atm - self.otm1
        short_price = get_premium(new_short_strike, "PE")
        long_price = get_premium(new_long_strike, "PE")
        if short_price is None or long_price is None:
            return False
        needs_replace, correct_ce_long = reconcile_protective_leg(atm, self.long_ce.strike, self.otm1, direction=1)
        new_ce_long_price = None
        if needs_replace:
            new_ce_long_price = get_premium(correct_ce_long, "CE")
            if new_ce_long_price is None:
                return False
        if self.short_pe is not None or self.long_pe is not None:
            self._close_side("PE", get_premium)
        self.short_pe = Leg(strike=new_short_strike, entry_price=short_price, qty=self.qty, is_short=True, side="PE")
        self.long_pe = Leg(strike=new_long_strike, entry_price=long_price, qty=self.qty, is_short=False, side="PE")
        self.order_count += 2
        if needs_replace:
            self._close_leg(self.long_ce, "CE", get_premium)
            self.long_ce = Leg(strike=correct_ce_long, entry_price=new_ce_long_price, qty=self.qty, is_short=False, side="CE")
            self.order_count += 1
        self.is_flied = True
        self._log("FLY_CONVERSION", trigger="CALL_BECOMES_ATM", legs=self._legs_dict())
        return True

    # ── main tick handler ────────────────────────────────────────────────

    def on_spot_tick(self, spot: float, get_premium: PremiumFn, ts=None) -> None:
        self._current_ts = ts
        if self.is_flat():
            self.try_enter(spot, get_premium)
            return

        # 1. Profit target -- highest priority.
        # Keyed by (strike, side), NOT strike alone -- a CE leg and a PE leg
        # legitimately share the same strike once a side has Iron-Fly-
        # converted, and a strike-only key would silently collide them.
        live_premiums: Dict[Tuple[int, str], float] = {}
        have_all_data = True
        open_legs: List[Leg] = []
        for leg, side in self._leg_sides():
            if leg is None:
                continue
            open_legs.append(leg)
            price = get_premium(leg.strike, side)
            if price is None:
                have_all_data = False
                break
            live_premiums[(leg.strike, side)] = price
        if have_all_data:
            pnl = cycle_pnl(self.cycle_realized_pnl, open_legs, live_premiums)
            if profit_target_hit(pnl, self.cycle_expected_max_profit, self.profit_target_pct):
                self._close_all(get_premium)
                self._log(
                    "CYCLE_EXIT_PROFIT_TARGET", spot=spot, pnl=pnl,
                    target=self.profit_target_pct * self.cycle_expected_max_profit,
                )
                self.try_enter(spot, get_premium)
                return

        # 2. Iron Fly conversion -- including a gap that skips straight past
        # a sold strike (direct user spec, 2026-09-14: "gap-through" must
        # convert immediately, no waiting for a retrace or another +/-100
        # move). Detection alone outranks every routine adjustment below,
        # even on a tick where the conversion itself can't complete yet
        # (missing premium data) -- always `return` here rather than
        # falling through to the ordinary roll/gap-pending logic, so a
        # stalled conversion never gets masked by a routine adjustment.
        conv = check_atm_conversion(
            spot,
            self.short_ce.strike if self.short_ce else None,
            self.short_pe.strike if self.short_pe else None,
        )
        if conv == "PUT_BECOMES_ATM" and not self.is_flied:
            self._convert_put_becomes_atm(get_premium)
            return
        elif conv == "CALL_BECOMES_ATM" and not self.is_flied:
            self._convert_call_becomes_atm(get_premium)
            return

        # 3. Pending gap adjustment.
        if self.pending_adjustment is not None:
            if check_pending_fire(self.pending_adjustment, spot):
                side = self.pending_adjustment.side
                self.pending_adjustment = None
                if side == "CALL" and not self.is_flied:
                    self.roll_call_side(spot, get_premium)
                elif side == "PUT" and not self.is_flied:
                    self.roll_put_side(spot, get_premium)
            return

        # 4. +/-100pt roll / gap arm.
        side, is_gap = classify_move(spot, self.reference_price, self.adjustment_distance)
        if side == "CALL" and not self.is_flied:
            if is_gap:
                self.pending_adjustment = PendingAdjustment("CALL", self.reference_price - self.adjustment_distance)
                self._log("GAP_PENDING_ARMED", side="CALL", trigger=self.pending_adjustment.trigger_price)
            else:
                self.roll_call_side(spot, get_premium)
        elif side == "PUT" and not self.is_flied:
            if is_gap:
                self.pending_adjustment = PendingAdjustment("PUT", self.reference_price + self.adjustment_distance)
                self._log("GAP_PENDING_ARMED", side="PUT", trigger=self.pending_adjustment.trigger_price)
            else:
                self.roll_put_side(spot, get_premium)


# ── Phase 2: live async book (2026-09-14) ─────────────────────────────────

def _make_strategy_logger(underlying: str, client_id: str = "", binding_id: str = "") -> logging.Logger:
    from utils.logging_utils import make_strategy_logger
    from config.global_config import IST
    tag = f"{underlying}" + (f"_{client_id}_{binding_id}" if client_id and binding_id else "")
    date_str = datetime.now(IST).strftime("%Y%m%d")
    return make_strategy_logger(f"ironfly_{tag}_{date_str}", propagate=False)


def _serialize_leg(leg: Optional[Leg]) -> Optional[dict]:
    if leg is None:
        return None
    return {"strike": leg.strike, "entry_price": leg.entry_price, "qty": leg.qty,
            "is_short": leg.is_short, "side": leg.side}


def _deserialize_leg(d: Optional[dict]) -> Optional[Leg]:
    if not d:
        return None
    return Leg(strike=int(d["strike"]), entry_price=float(d["entry_price"]), qty=int(d["qty"]),
               is_short=bool(d["is_short"]), side=d["side"])


class IronFlyStrategy:
    """One instance per (client, binding) -- underlying is fixed to NIFTY by
    the approved plan's own scope decision, but this class doesn't hardcode
    that, matching every other strategy's own (client, binding, underlying)
    shape. See this module's own docstring above for the wrapper's design.
    """

    def __init__(
        self,
        bus,
        cfg,
        underlying: str,
        client_id: str,
        binding_id: str,
        *,
        lot_multiplier: int = 1,
        otm1: int = 50,
        adjustment_distance: float = 100.0,
        short_threshold: float = 20.0,
        long_threshold: float = 20.0,
        profit_target_pct: float = 0.65,
        chain_depth_strikes: int = 20,
        product_type: str = "NRML",
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._underlying = underlying
        self._client_id = client_id
        self._binding_id = binding_id
        self._lot_multiplier = max(1, int(lot_multiplier))
        self._otm1 = int(otm1)
        self._adjustment_distance = float(adjustment_distance)
        self._short_threshold = float(short_threshold)
        self._long_threshold = float(long_threshold)
        self._profit_target_pct = float(profit_target_pct)
        self._chain_depth_strikes = max(4, int(chain_depth_strikes))
        self._product_type = product_type

        lot_size = (cfg.exchange.lot_sizes.get(underlying, 75) if cfg else 75)
        strike_step = float(cfg.exchange.strike_steps.get(underlying, 50) if cfg else 50)
        qty = lot_size * self._lot_multiplier
        self._engine = IronFlyEngine(
            qty=qty, strike_step=strike_step, otm1=self._otm1,
            adjustment_distance=self._adjustment_distance,
            short_threshold=self._short_threshold, long_threshold=self._long_threshold,
            profit_target_pct=self._profit_target_pct,
        )

        self._live_premium: Dict[Tuple[int, str], float] = {}
        self._day_expiry: Optional[date] = None
        self._persist_key = f"{client_id}_{binding_id}_{underlying}_iron_fly"
        self._clog = _make_strategy_logger(underlying, client_id, binding_id)
        self._event_counter = 0
        self._recent_remarks: Deque[dict] = deque(maxlen=30)

        self._running = False
        self._tasks: list = []
        self._loop_queues: Dict[str, "asyncio.Queue"] = {}

    # ── lifecycle ────────────────────────────────────────────────────────

    def reset_session(self) -> None:
        """No daily reset by design -- this strategy carries positions
        across days (no EOD square-off, per the approved plan). Present
        only to satisfy strategy-manager conventions elsewhere in this
        codebase that assume a reset hook exists."""
        pass

    def start(self) -> None:
        from config.global_config import Topic
        self._running = True
        self._subscribe(Topic.INDEX_TICK)
        self._subscribe(Topic.OPTION_TICK)
        self._subscribe(Topic.IRON_FLY_ORDER_FILL)
        self._restore_position()
        self._tasks.append(asyncio.create_task(
            self._index_tick_loop(), name=f"ironfly_idx_{self._underlying}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._option_tick_loop(), name=f"ironfly_opt_{self._underlying}_{self._binding_id}"))
        self._tasks.append(asyncio.create_task(
            self._fill_loop(), name=f"ironfly_fill_{self._underlying}_{self._binding_id}"))
        logger.info("IronFlyStrategy[%s/%s/%s]: started (qty=%d).",
                    self._client_id, self._binding_id, self._underlying, self._engine.qty)

    def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()

    async def stop_async(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._unsubscribe_all()

    def _subscribe(self, topic) -> None:
        q = self._bus.subscribe(topic)
        self._loop_queues[topic] = q
        return q

    def _unsubscribe_all(self) -> None:
        for topic, q in list(self._loop_queues.items()):
            try:
                self._bus.unsubscribe(topic, q)
            except Exception:
                pass
        self._loop_queues.clear()

    def is_flat(self) -> bool:
        return self._engine.is_flat()

    def liquidate(self) -> None:
        """Called by StrategyBookManager when this deployment is removed.
        Deliberately a no-op beyond stopping the book -- per the approved
        plan's "no EOD square-off" decision, an un-deployed binding does
        NOT auto-close a real position; the position (if any) stays open
        and this book will simply stop managing it until re-deployed. This
        matches the multi-day-carry design: undeploying is not the same as
        wanting a forced exit."""
        pass

    def _is_own_underlying_tick(self, symbol: str) -> bool:
        u = self._underlying.upper()
        if symbol == self._underlying:
            return True
        aliases = {
            "NIFTY": ("NSE_INDEX|Nifty 50", "NIFTY", "NSE:NIFTY50-INDEX"),
            "SENSEX": ("BSE_INDEX|SENSEX", "SENSEX"),
            "BANKNIFTY": ("NSE_INDEX|Nifty Bank", "BANKNIFTY"),
        }
        return symbol in aliases.get(u, ())

    def _resolve_expiry(self):
        from data_layer.instrument_registry import REGISTRY
        from config.global_config import IST
        return REGISTRY.get_active_expiry_strict(self._underlying, datetime.now(IST).date())

    def _get_premium(self, strike: int, side: str) -> Optional[float]:
        return self._live_premium.get((int(strike), side))

    # ── spot ticks -> engine.on_spot_tick -> diff -> orders ───────────────

    async def _index_tick_loop(self) -> None:
        from data_layer.base_feeder import IndexTick
        from config.global_config import Topic
        q = self._loop_queues.get(Topic.INDEX_TICK)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                if not isinstance(ev, IndexTick) or not self._is_own_underlying_tick(ev.symbol):
                    continue
                if getattr(ev, "source", "spot") != "spot":
                    continue
                if self._day_expiry is None:
                    self._day_expiry = self._resolve_expiry()
                    if self._day_expiry is None:
                        continue  # can't trade without a resolvable expiry -- retry next tick
                prev = snapshot_legs(self._engine)
                old_len = len(self._engine.trade_log)
                self._engine.on_spot_tick(ev.ltp, self._get_premium, ts=ev.timestamp)
                new_events = self._engine.trade_log[old_len:]
                if new_events:
                    closed, opened = diff_legs_for_orders(prev, self._engine, new_events)
                    if closed or opened:
                        self._fire_orders(closed, opened, new_events)
                        self._persist_position()
                        self._log_remark(new_events)
            except Exception:
                logger.exception("IronFlyStrategy[%s/%s]: _index_tick_loop iteration error (recovered).",
                                  self._binding_id, self._underlying)

    async def _option_tick_loop(self) -> None:
        from data_layer.base_feeder import OptionTick
        from config.global_config import Topic
        q = self._loop_queues.get(Topic.OPTION_TICK)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                if not isinstance(ev, OptionTick) or ev.underlying != self._underlying or not ev.ltp:
                    continue
                if self._day_expiry is not None and ev.expiry != self._day_expiry:
                    continue  # never mix a different expiry's ticks into this book's premium cache
                self._live_premium[(int(ev.strike), str(ev.option_type).upper())] = ev.ltp
            except Exception:
                logger.exception("IronFlyStrategy[%s/%s]: _option_tick_loop iteration error (recovered).",
                                  self._binding_id, self._underlying)

    def _reason_for(self, new_events: List[dict]) -> str:
        for ev in new_events:
            if ev["event"] != "CLOSE":
                return ev["event"].lower()
        return "close"

    def _fire_orders(self, closed: List[dict], opened: List[dict], new_events: List[dict]) -> None:
        from config.global_config import Topic
        from strategies.iron_fly.events import IronFlyOrderEvent
        reason = self._reason_for(new_events)
        expiry = self._day_expiry or self._resolve_expiry()
        for c in closed:
            leg: Leg = c["leg"]
            self._event_counter += 1
            eid = f"{self._underlying}_{c['side']}{leg.strike}_CLOSE_{self._event_counter}"
            order_side = "BUY" if leg.is_short else "SELL"
            order_ev = IronFlyOrderEvent(
                client_id=self._client_id, binding_id=self._binding_id,
                underlying=self._underlying, option_type=c["side"], strike=int(leg.strike),
                expiry=expiry, quantity=leg.qty, order_side=order_side, is_open=False,
                is_short=leg.is_short, price=c["close_price"], reason=reason, event_id=eid,
                entry_price=leg.entry_price, product_type=self._product_type,
            )
            self._clog.info("CLOSE %s%d is_short=%s @ %.2f reason=%s event_id=%s",
                             c["side"], leg.strike, leg.is_short, c["close_price"], reason, eid)
            if self._bus is not None:
                asyncio.create_task(self._bus.publish(Topic.IRON_FLY_ORDER_REQUEST, order_ev))
        for o in opened:
            leg: Leg = o["leg"]
            self._event_counter += 1
            eid = f"{self._underlying}_{o['side']}{leg.strike}_OPEN_{self._event_counter}"
            order_side = "SELL" if leg.is_short else "BUY"
            order_ev = IronFlyOrderEvent(
                client_id=self._client_id, binding_id=self._binding_id,
                underlying=self._underlying, option_type=o["side"], strike=int(leg.strike),
                expiry=expiry, quantity=leg.qty, order_side=order_side, is_open=True,
                is_short=leg.is_short, price=leg.entry_price, reason=reason, event_id=eid,
                product_type=self._product_type,
            )
            self._clog.info("OPEN %s%d is_short=%s @ %.2f reason=%s event_id=%s",
                             o["side"], leg.strike, leg.is_short, leg.entry_price, reason, eid)
            if self._bus is not None:
                asyncio.create_task(self._bus.publish(Topic.IRON_FLY_ORDER_REQUEST, order_ev))

    def _log_remark(self, new_events: List[dict]) -> None:
        from config.global_config import IST
        for ev in new_events:
            if ev["event"] == "CLOSE":
                text = f"CLOSE {ev.get('side')}{ev.get('strike')} @{ev.get('price'):.2f} pnl={ev.get('pnl', 0):+.2f}"
            elif ev["event"] == "FLY_CONVERSION":
                text = f"IRON FLY CONVERSION ({ev.get('trigger')}) legs={ev.get('legs')}"
            elif ev["event"] == "CYCLE_EXIT_PROFIT_TARGET":
                text = f"65% PROFIT TARGET HIT (pnl={ev.get('pnl'):.2f} >= target={ev.get('target'):.2f}) -- fresh cycle starting"
            elif ev["event"] in ("ROLL_CALL", "ROLL_PUT"):
                text = f"{ev['event']} legs={ev.get('legs')}"
            elif ev["event"] == "ENTRY":
                text = f"ENTRY legs={ev.get('legs')} expected_max_profit={ev.get('expected_max_profit'):.2f}"
            else:
                text = str(ev)
            self._clog.info(text)
            self._recent_remarks.appendleft({"ts": datetime.now(IST).isoformat(), "text": text})

    # ── fills ────────────────────────────────────────────────────────────

    async def _fill_loop(self) -> None:
        from config.global_config import Topic
        from strategies.iron_fly.events import IronFlyFillEvent
        q = self._loop_queues.get(Topic.IRON_FLY_ORDER_FILL)
        if q is None:
            return
        while self._running:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            if not isinstance(ev, IronFlyFillEvent):
                continue
            if ev.client_id != self._client_id or ev.binding_id != self._binding_id \
                    or ev.underlying != self._underlying:
                continue
            try:
                if ev.aborted:
                    # Known Phase-2 limitation (see module docstring): the
                    # engine already committed this leg change synchronously
                    # before the order was even placed. An abort here means
                    # the REAL broker-routing attempt failed (e.g. gate
                    # closed, terminal disconnected) -- logged loudly since
                    # there's no automatic state rollback in this pass.
                    logger.critical(
                        "IronFlyStrategy[%s/%s]: order ABORTED (event_id=%s, routing_failed=%s) -- "
                        "engine's own position state is UNCHANGED by this (it already committed "
                        "optimistically) -- verify against the real broker manually.",
                        self._binding_id, self._underlying, ev.event_id, ev.routing_failed,
                    )
                    self._clog.critical("ORDER ABORTED event_id=%s routing_failed=%s",
                                         ev.event_id, ev.routing_failed)
            except Exception:
                logger.exception("IronFlyStrategy[%s/%s]: _fill_loop error (recovered).",
                                  self._binding_id, self._underlying)

    # ── persistence ──────────────────────────────────────────────────────

    def _persist_position(self) -> None:
        eng = self._engine
        data = {
            "short_ce": _serialize_leg(eng.short_ce), "long_ce": _serialize_leg(eng.long_ce),
            "short_pe": _serialize_leg(eng.short_pe), "long_pe": _serialize_leg(eng.long_pe),
            "is_flied": eng.is_flied, "reference_price": eng.reference_price,
            "cycle_expected_max_profit": eng.cycle_expected_max_profit,
            "cycle_realized_pnl": eng.cycle_realized_pnl,
            "lifetime_realized_pnl": eng.lifetime_realized_pnl,
            "cycle_number": eng.cycle_number, "order_count": eng.order_count,
            "pending_adjustment": (
                None if eng.pending_adjustment is None
                else {"side": eng.pending_adjustment.side, "trigger_price": eng.pending_adjustment.trigger_price}
            ),
            "day_expiry": self._day_expiry.isoformat() if self._day_expiry else None,
        }
        position_store.save(self._persist_key, data, product_type=self._product_type)

    def _restore_position(self) -> None:
        data = position_store.load(self._persist_key)
        if not data:
            return
        eng = self._engine
        eng.short_ce = _deserialize_leg(data.get("short_ce"))
        eng.long_ce = _deserialize_leg(data.get("long_ce"))
        eng.short_pe = _deserialize_leg(data.get("short_pe"))
        eng.long_pe = _deserialize_leg(data.get("long_pe"))
        eng.is_flied = bool(data.get("is_flied", False))
        eng.reference_price = data.get("reference_price")
        eng.cycle_expected_max_profit = data.get("cycle_expected_max_profit")
        eng.cycle_realized_pnl = float(data.get("cycle_realized_pnl", 0.0))
        eng.lifetime_realized_pnl = float(data.get("lifetime_realized_pnl", 0.0))
        eng.cycle_number = int(data.get("cycle_number", 0))
        eng.order_count = int(data.get("order_count", 0))
        pa = data.get("pending_adjustment")
        eng.pending_adjustment = PendingAdjustment(pa["side"], pa["trigger_price"]) if pa else None
        if data.get("day_expiry"):
            self._day_expiry = date.fromisoformat(data["day_expiry"])
        logger.info(
            "IronFlyStrategy[%s/%s]: restored from store (flat=%s, is_flied=%s, cycle=%d, "
            "lifetime_realized=%.2f).",
            self._binding_id, self._underlying, eng.is_flat(), eng.is_flied,
            eng.cycle_number, eng.lifetime_realized_pnl,
        )
        self._clog.info("RESTORED flat=%s is_flied=%s cycle=%d lifetime_realized=%.2f",
                         eng.is_flat(), eng.is_flied, eng.cycle_number, eng.lifetime_realized_pnl)

    # ── monitoring / UI ──────────────────────────────────────────────────

    def monitoring_state(self) -> dict:
        eng = self._engine

        def _leg_view(leg: Optional[Leg], side: str) -> Optional[dict]:
            if leg is None:
                return None
            live = self._live_premium.get((leg.strike, side))
            unrealized = None
            if live is not None:
                unrealized = leg_pnl(leg, live)
            return dict(strike=leg.strike, entry_price=leg.entry_price, is_short=leg.is_short,
                        live_price=live, unrealized_pnl=unrealized)

        return dict(
            underlying=self._underlying, client_id=self._client_id, binding_id=self._binding_id,
            is_flat=eng.is_flat(), is_flied=eng.is_flied, reference_price=eng.reference_price,
            cycle_number=eng.cycle_number, cycle_expected_max_profit=eng.cycle_expected_max_profit,
            cycle_realized_pnl=eng.cycle_realized_pnl, lifetime_realized_pnl=eng.lifetime_realized_pnl,
            order_count=eng.order_count,
            legs=dict(
                short_ce=_leg_view(eng.short_ce, "CE"), long_ce=_leg_view(eng.long_ce, "CE"),
                short_pe=_leg_view(eng.short_pe, "PE"), long_pe=_leg_view(eng.long_pe, "PE"),
            ),
            recent_remarks=list(self._recent_remarks),
        )
