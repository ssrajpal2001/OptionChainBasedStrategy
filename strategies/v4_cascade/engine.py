"""
strategies/v4_cascade/engine.py — V4CascadeEngine: pure orchestration of the
2026-07-19 3-gate pure-premium funnel.

**NIFTY Spot vs. Option Premium separation (explicit, per spec):**
  - NIFTY Spot (75m) is scanned for BOTH bear AND bull traps
    (spot_confirm.py's SpotConfirmTracker, unchanged) purely to set a
    directional BIAS: a confirmed spot BEAR trap (bullish read) arms the CE
    funnel; a confirmed spot BULL trap (bearish read) arms the PE funnel.
    Spot itself is never traded and never re-checked at trigger time — bias
    is applied once, at the moment a side would start a fresh Gate 1 scan.
  - Once armed, each side's OWN option premium chart (CE or PE tracking
    contract) is scanned EXCLUSIVELY for BEAR TRAPS at every gate
    (PremiumGateScanner + rolling_base.find_bear_trap_2candle) — option
    short-sellers trapped as premium spikes back above their structural
    reference high. Bull traps are never scanned on premium charts.

Gate sequence (per side, via zone_state.PremiumGateScanner):
  ARMED_WAIT -> HTF_SCANNING -> HTF_LOCKED -> WAITING_FOR_HTF_ZONE_ENTRY
  -> MTF_SCANNING_5M -> MTF_LOCKED (or MTF_SCANNING_15M fallback)
  -> WAITING_FOR_MTF_ZONE_ENTRY -> LIMIT_ARMED -> TRIGGERED

``update(spot_bar=None, ce_bar=None, pe_bar=None) -> List[CascadeEvent]``
dispatches internally on each bar's ``.timeframe`` (75 -> spot bias + Gate 1;
5 -> Gate 2/Gate 3 +, once a position is open, T1/T2 exit checks via
exits.py, unchanged). Tracking (ATM-200/+200) vs execution (ATM+-50)
contract split is preserved — this pure engine emits tracking-contract price
hints only; book.py (a later phase, not yet built) maps to the real
execution-contract fill.

No bus/broker/DB/asyncio dependency — pure, fed CandleEvent-shaped bars.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.dataclasses import (
    CascadeEvent, CascadeEventType, CascadePosition, GateState, TrancheLeg,
)
from strategies.v4_cascade.entries import check_limit_pierce, compute_risk_mapping
from strategies.v4_cascade.exits import TrailingBaseTracker, check_t1, map_trailing_stop_to_execution
from strategies.v4_cascade.spot_confirm import SpotConfirmTracker
from strategies.v4_cascade.zone_state import PremiumGateScanner

_SIDES = ("CE", "PE")


class V4CascadeEngine:
    """One instance per (client, binding, underlying) deployment — mirrors
    sell_straddle's per-binding book pattern (see project memory), just not
    yet wired to a book.py adapter (that's a later phase)."""

    def __init__(self, cfg: Optional[V4CascadeConfig] = None, pe_scans_bull: bool = False) -> None:
        """``pe_scans_bull``: 2026-07-19, crypto-spot-only path ONLY (see
        strategies/v4_cascade/book.py's _is_crypto branch) — when True, the
        PE scanner looks for BULL traps (real option premium has no
        inversion on raw spot, so PE must scan for genuine bearish patterns
        directly rather than reusing bear-trap logic). Defaults to False,
        the exact validated NIFTY behavior (both CE and PE bear-trap-only)."""
        self._cfg = cfg or V4CascadeConfig()
        self._spot_confirm = SpotConfirmTracker()
        self._scanners: Dict[str, PremiumGateScanner] = {
            "CE": PremiumGateScanner(bear=True),
            "PE": PremiumGateScanner(bear=not pe_scans_bull),
        }
        self._trackers: Dict[str, TrailingBaseTracker] = {}
        # tracking-contract entry price at trigger time, per side — needed to
        # proportionally rescale the T2 trailing stop onto the execution
        # contract the same way entries.compute_risk_mapping does for SL/target.
        self._tracking_entry_price: Dict[str, float] = {}

        self.position: Optional[CascadePosition] = None

    # ── ingest ───────────────────────────────────────────────────────────────
    def update(self, spot_bar=None, ce_bar=None, pe_bar=None) -> List[CascadeEvent]:
        events: List[CascadeEvent] = []

        if spot_bar is not None and getattr(spot_bar, "timeframe", 75) == 75:
            self._spot_confirm.on_75m_bar(spot_bar)
            self._apply_spot_bias()

        if ce_bar is not None:
            events += self._update_side("CE", ce_bar)
        if pe_bar is not None:
            events += self._update_side("PE", pe_bar)
        return events

    def _apply_spot_bias(self) -> None:
        """Arm CE on a spot bear-trap close (bullish read), PE on a spot
        bull-trap close (bearish read). Only takes effect for a side
        currently ARMED_WAIT (decision: does not abort an in-flight
        HTF/MTF/limit sequence already underway on either side)."""
        for side in _SIDES:
            self._scanners[side].set_armed(self._spot_confirm.confirms(side))

    def _update_side(self, side: str, bar) -> List[CascadeEvent]:
        events: List[CascadeEvent] = []
        scanner = self._scanners[side]
        tf = getattr(bar, "timeframe", 5)

        if tf == 75:
            scanner.on_75m_bar(bar)
            return events

        if tf != 5:
            return events

        # Don't re-arm a side that already has a live position.
        if self.position is not None and self.position.is_open:
            if self.position.side == side:
                events += self._check_exits(side, bar)
            return events

        scanner.on_5m_bar(bar)
        trigger = check_limit_pierce(scanner, bar)
        if trigger.fired:
            events.append(self._open_position(side, scanner, trigger.setup, bar))
        return events

    # ── entry ────────────────────────────────────────────────────────────────
    def _open_position(self, side: str, scanner: PremiumGateScanner, setup, bar) -> CascadeEvent:
        zone = setup.mtf_zone
        entry_price = setup.limit_entry_price or bar.low
        # is_short: True only for a bull-geometry scanner (crypto's PE side
        # scanning bull traps -> bearish signal -> short). NIFTY's CE and PE
        # both always use bear=True scanners, so is_short is always False
        # there -- this branch never fires for NIFTY.
        is_short = not scanner._bear
        # Pure engine has no separate execution-contract feed yet (book.py,
        # a later phase, resolves the real ATM+-50 fill) — tracking price is
        # used as both tracking_entry_price and exec_entry_price here, per
        # CascadeEvent.price_hint's existing "tracking-contract price at
        # decision time, not the real fill" contract.
        sl_price, target_price = compute_risk_mapping(
            zone, tracking_entry_price=entry_price, exec_entry_price=entry_price,
            target_r=self._cfg.t1_target_r, sl_buffer=self._cfg.sl_buffer, is_short=is_short,
        )
        qty = self._cfg.tranche_qty
        # Why this trade fired: a bear trap (sellers trapped, reclaim up) is
        # a bullish signal -> long; a bull trap (buyers trapped, reclaim
        # down) is bearish -> short (crypto PE only). Gate 3 = the 1/3-depth
        # limit-price pierce into the locked Inner (MTF) zone.
        entry_reason = "gate3_bull_trap_reclaim" if is_short else "gate3_bear_trap_reclaim"
        t1 = TrancheLeg(tranche="T1", option_type=side, strike=0.0, qty=qty,
                         entry_price=entry_price, entry_time=bar.timestamp, entry_reason=entry_reason,
                         sl_price=sl_price, target_price=target_price)
        t2 = TrancheLeg(tranche="T2", option_type=side, strike=0.0, qty=qty,
                         entry_price=entry_price, entry_time=bar.timestamp, entry_reason=entry_reason,
                         sl_price=sl_price, target_price=None)
        self.position = CascadePosition(
            underlying=self._cfg.underlying, side=side,
            tracking_strike=0.0, execution_strike=0.0,
            atm_at_trigger=0.0, entry_spot=0.0,
            t1=t1, t2=t2, open_time=bar.timestamp,
        )
        self._tracking_entry_price[side] = entry_price
        # TrailingBaseTracker's own `bear` flag mirrors the scanner's -- a
        # long (bear-zone) position trails a RISING floor, a short
        # (bull-zone, crypto PE) position trails a FALLING ceiling. This was
        # previously hardcoded bear=True regardless of side (latent bug,
        # never surfaced before crypto's PE-short path existed).
        self._trackers[side] = TrailingBaseTracker(bear=scanner._bear)

        scanner.pop_setup(setup, bar.timestamp)
        event_type = CascadeEventType.OPEN_LONG_CE if side == "CE" else CascadeEventType.OPEN_LONG_PE
        return CascadeEvent(
            event_type=event_type, side=side, price_hint=entry_price, reason=entry_reason,
            sl_price=sl_price, target_price=target_price, timestamp=bar.timestamp,
        )

    # ── exits (T1/T2 — exits.py logic unchanged, just re-fed the Inner
    #    Zone's risk instead of the old HTF zone's risk) ───────────────────────
    def _check_exits(self, side: str, bar) -> List[CascadeEvent]:
        events: List[CascadeEvent] = []
        pos = self.position
        if pos is None:
            return events
        t1, t2 = pos.t1, pos.t2
        tracking_entry = self._tracking_entry_price.get(side, 0.0)
        is_short = not self._scanners[side]._bear

        if t1 is not None and t1.status == "open":
            r = check_t1(t1, bar, is_short=is_short)
            if r.hit:
                t1.status = "closed"
                t1.close_price = r.price
                t1.close_reason = r.reason
                t1.close_time = bar.timestamp
                events.append(self._close_event(side, "T1", r.reason, r.price, bar.timestamp))

        trail = self._trackers.get(side)
        if t2 is not None and t2.status == "open" and trail is not None:
            moved = trail.on_5m_bar(bar)
            if moved and trail.current_stop is not None:
                t2.trail_stop_price = map_trailing_stop_to_execution(
                    trail.current_stop, tracking_entry, t2.entry_price,
                )
            r = trail.check_hit(bar)
            if r.hit:
                t2.status = "closed"
                t2.close_price = r.price
                t2.close_reason = r.reason
                t2.close_time = bar.timestamp
                events.append(self._close_event(side, "T2", r.reason, r.price, bar.timestamp))

        if (t1 is None or t1.status == "closed") and (t2 is None or t2.status == "closed"):
            pos.status = "closed"
            pos.close_time = bar.timestamp
            self._trackers.pop(side, None)
            self._tracking_entry_price.pop(side, None)
        return events

    @staticmethod
    def _close_event(side: str, tranche: str, reason: str, price: float, ts) -> CascadeEvent:
        event_type = CascadeEventType.CLOSE_LONG_CE if side == "CE" else CascadeEventType.CLOSE_LONG_PE
        return CascadeEvent(event_type=event_type, side=side, tranche=tranche,
                             reason=reason, price_hint=price, timestamp=ts)

    # ── lifecycle ────────────────────────────────────────────────────────────
    def reset_session(self) -> None:
        self._spot_confirm.reset()
        for scanner in self._scanners.values():
            scanner.reset()
        for trail in self._trackers.values():
            trail.reset()
        self._trackers.clear()
        self._tracking_entry_price.clear()
        self.position = None
