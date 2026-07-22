"""
strategies/v4_cascade/engine.py — V4CascadeEngine: pure orchestration of the
3-gate funnel.

**2026-07-20 Index/Premium decoupling (NIFTY/CRUDEOIL real-options path):**
  - Gate 1 (structural sweep+reclaim) now lives entirely on the Index/Futures
    chart (75m, spot_confirm.py's SpotConfirmTracker, unchanged mechanism):
    a confirmed Index BEAR trap (bullish read) arms the CE funnel; a
    confirmed Index BULL trap (bearish read) arms the PE funnel. This is now
    a HARD gate on Gate 2 discovery (zone_state.IndexGatedPremiumScanner),
    not a late trigger-time bias check — but per the user's explicit
    decision, an already-discovered premium setup is NEVER aborted by a
    later Index flip; only whether a NEW scan may START is gated.
  - Once armed, that side's OWN option premium chart (CE or PE tracking
    contract) is scanned EXCLUSIVELY for BEAR TRAPS on 5m (15m fallback) —
    option short-sellers trapped as premium spikes back above their
    structural reference high. Gate 3 (1/3-depth limit + pierce) is
    unchanged.

**Crypto (BTC/ETH) path — completely unchanged, kept on the legacy
zone_state.PremiumGateScanner**, selected via the existing ``pe_scans_bull``
flag (already exactly the crypto indicator — no new parameter): NIFTY Spot
is scanned for BOTH bear AND bull traps purely to set bias; each side's own
option premium chart still runs the full legacy two-stage HTF(75m)+MTF(5m/15m)
funnel, and the trigger-time bias check (``scanner.armed``) is still
consulted there exactly as before.

Gate sequence, NIFTY/CRUDEOIL (via zone_state.IndexGatedPremiumScanner):
  ARMED_WAIT (Index not confirmed) -> [Index confirms] -> PREMIUM_SCANNING
  -> PREMIUM_LOCKED -> WAITING_FOR_ZONE_ENTRY -> LIMIT_ARMED -> TRIGGERED

Gate sequence, crypto (via zone_state.PremiumGateScanner, unchanged):
  ARMED_WAIT -> HTF_SCANNING -> HTF_LOCKED -> WAITING_FOR_HTF_ZONE_ENTRY
  -> MTF_SCANNING_5M -> MTF_LOCKED (or MTF_SCANNING_15M fallback)
  -> WAITING_FOR_MTF_ZONE_ENTRY -> LIMIT_ARMED -> TRIGGERED

``update(spot_bar=None, ce_bar=None, pe_bar=None) -> List[CascadeEvent]``
dispatches internally on each bar's ``.timeframe`` (75 -> Index gate +
[crypto only] Gate 1; 5 -> Gate 2/Gate 3 +, once a position is open, T1/T2
exit checks via exits.py, unchanged). Tracking (ATM-200/+200) vs execution
(ATM+-50) contract split is preserved — this pure engine emits
tracking-contract price hints only; book.py maps to the real
execution-contract fill.

No bus/broker/DB/asyncio dependency — pure, fed CandleEvent-shaped bars.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple, Union

from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.dataclasses import (
    CascadeEvent, CascadeEventType, CascadePosition, GateState, TrancheLeg,
)
from strategies.v4_cascade.entries import check_limit_pierce, compute_risk_mapping
from strategies.v4_cascade.exits import TrailingBaseTracker, check_t1, map_trailing_stop_to_execution
from strategies.v4_cascade.spot_confirm import SpotConfirmTracker
from strategies.v4_cascade.zone_state import PremiumGateScanner, IndexGatedPremiumScanner

_SIDES = ("CE", "PE")
_Scanner = Union[PremiumGateScanner, IndexGatedPremiumScanner]


class V4CascadeEngine:
    """One instance per (client, binding, underlying) deployment — mirrors
    sell_straddle's per-binding book pattern (see project memory), just not
    yet wired to a book.py adapter (that's a later phase)."""

    def __init__(
        self, cfg: Optional[V4CascadeConfig] = None, pe_scans_bull: bool = False,
        session_open: Tuple[int, int] = (9, 15),
        entry_cutoff_hour_min: Optional[Tuple[int, int]] = None,
    ) -> None:
        """``pe_scans_bull``: 2026-07-19, crypto-spot-only path ONLY (see
        strategies/v4_cascade/book.py's _is_crypto branch) — when True, the
        PE scanner looks for BULL traps (real option premium has no
        inversion on raw spot, so PE must scan for genuine bearish patterns
        directly rather than reusing bear-trap logic). Defaults to False,
        the exact validated NIFTY behavior (both CE and PE bear-trap-only).
        ``session_open``: forwarded to both scanners' Gate-2 15m fallback
        resample — NIFTY/NSE default (9,15), MCX underlyings (CRUDEOIL) pass
        (9,0). ``entry_cutoff_hour_min``: 2026-07-21 fix -- (hour, minute)
        past which NO new entry may fire (book.py passes its own squareoff
        time). Confirmed live bug without this: sell_straddle has a distinct
        EntryEnd separate from SquareOff, but V4 Cascade had no equivalent at
        all -- Gate 3 kept firing brand-new positions after square-off,
        each immediately force-closed again on the very next bar, in a
        repeating loop that only stopped when a human manually stopped the
        deployment (observed on 0DTE NIFTY expiry: 3 spurious re-entries
        between 15:25 and 15:44, well past the 15:20 configured square-off,
        because near-zero decayed premium noise kept re-triggering Gate 3)."""
        self._cfg = cfg or V4CascadeConfig()
        self._entry_cutoff_hour_min = entry_cutoff_hour_min
        self._spot_confirm = SpotConfirmTracker()
        # pe_scans_bull is already exactly the crypto indicator (see
        # book.py's _is_crypto branch) -- reused here as the router between
        # the legacy two-stage scanner (crypto, untouched) and the new
        # Index-gated single-stage scanner (NIFTY/CRUDEOIL real options).
        self._legacy_scanners = pe_scans_bull
        self._scanners: Dict[str, _Scanner]
        if self._legacy_scanners:
            self._scanners = {
                "CE": PremiumGateScanner(bear=True, session_open=session_open),
                "PE": PremiumGateScanner(bear=not pe_scans_bull, session_open=session_open),
            }
        else:
            self._scanners = {
                "CE": IndexGatedPremiumScanner(session_open=session_open),
                "PE": IndexGatedPremiumScanner(session_open=session_open),
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
            self._apply_index_gate()

        if ce_bar is not None:
            events += self._update_side("CE", ce_bar)
        if pe_bar is not None:
            events += self._update_side("PE", pe_bar)
        return events

    def _apply_index_gate(self) -> None:
        """Arm CE on an Index bear-trap confirmed close (bullish read), PE on
        an Index bull-trap confirmed close (bearish read) — spot_confirm.py's
        single mutually-exclusive classification, recency-tiebroken,
        unchanged mechanism. For NIFTY/CRUDEOIL (IndexGatedPremiumScanner)
        this is now a HARD gate on new Gate-2 premium-scan discovery; for
        crypto (legacy PremiumGateScanner) it remains a late trigger-time-only
        check. Either way, never aborts an in-flight setup already underway."""
        for side in _SIDES:
            self._scanners[side].set_armed(
                self._spot_confirm.confirms(side),
                confirmed_ts=self._spot_confirm.confirmation_ts(side),
            )

    def _update_side(self, side: str, bar) -> List[CascadeEvent]:
        events: List[CascadeEvent] = []
        scanner = self._scanners[side]
        tf = getattr(bar, "timeframe", 5)

        if tf == 75:
            scanner.on_75m_bar(bar)
            return events

        if tf != 5:
            return events

        # Don't re-arm a side that already has a live position -- but the
        # OPPOSITE side keeps scanning (Gate 1/2/3 don't freeze just
        # because the other side is holding a trade). If the opposite
        # side independently reaches its own Gate-3 trigger, that's a
        # fresh, contradicting signal: close the current position
        # (structural flip) and, if this side's own conditions still hold
        # at this instant, open it immediately in the same cycle.
        if self.position is not None and self.position.is_open:
            if self.position.side == side:
                if self.position.risk_basis != "execution_native":
                    events += self._check_exits(side, bar)
                return events
            scanner.on_5m_bar(bar)
            trigger = check_limit_pierce(scanner, bar)
            if trigger.fired and self._may_fire(scanner, bar):
                events += self._close_for_structural_flip(bar.timestamp)
                events.append(self._open_position(side, scanner, trigger.setup, bar))
            return events

        scanner.on_5m_bar(bar)
        trigger = check_limit_pierce(scanner, bar)
        if trigger.fired and self._may_fire(scanner, bar):
            events.append(self._open_position(side, scanner, trigger.setup, bar))
        return events

    def _may_fire(self, scanner: _Scanner, bar) -> bool:
        """2026-07-21: discovery is unconditional on BOTH scanner types now
        (crypto's legacy PremiumGateScanner always was; IndexGatedPremiumScanner
        reverted to match, per explicit user direction — PE must be allowed to
        scan even while CE holds Index bias, otherwise it could never
        independently reach its own trigger and cause a structural flip when
        the Index bias itself crosses through PE's zone). `armed` is checked
        ONLY here, at trigger time, uniformly for both: a pierce on an
        unarmed side is left alone (not popped, not opened) so it can still
        fire later once that side becomes armed. Also gates on
        entry_cutoff_hour_min (2026-07-21) -- a pierce at/after the cutoff
        is likewise left alone, never opened; there is no reason to ever
        fire again that day once past square-off, so unlike the armed-gate
        this is effectively terminal for the session, but leaving the setup
        un-popped (rather than special-casing a hard stop) reuses the exact
        same safe, already-tested pending-setup mechanics."""
        if (self._entry_cutoff_hour_min is not None
                and (bar.timestamp.hour, bar.timestamp.minute) >= self._entry_cutoff_hour_min):
            return False
        return scanner.armed

    def _close_for_structural_flip(self, ts) -> List[CascadeEvent]:
        """Closes every still-open leg of the CURRENT position because the
        OPPOSITE side just independently reached its own valid Gate-3
        trigger -- a fresh, contradicting signal while a position is live.
        Uses each leg's own entry_price as the theoretical close price,
        same placeholder convention book.py already uses for EOD/manual
        closes -- book.py's _on_fill reconciles the REAL fill same as
        every other close, this is never treated as a real price."""
        events: List[CascadeEvent] = []
        pos = self.position
        if pos is None:
            return events
        close_side = pos.side
        for tranche, leg in (("T1", pos.t1), ("T2", pos.t2)):
            if leg is None or leg.status != "open":
                continue
            leg.status = "closed"
            leg.close_price = leg.entry_price
            leg.close_reason = "structural_flip"
            leg.close_time = ts
            events.append(self._close_event(close_side, tranche, "structural_flip", leg.close_price, ts))
        pos.status = "closed"
        pos.close_time = ts
        self._trackers.pop(close_side, None)
        self._tracking_entry_price.pop(close_side, None)
        return events

    # ── entry ────────────────────────────────────────────────────────────────
    def _open_position(self, side: str, scanner: _Scanner, setup, bar) -> CascadeEvent:
        # setup.zone (new IndexGatedPremiumScanner._PremiumSetup) or
        # setup.mtf_zone (legacy PremiumGateScanner._HTFSetup, crypto only) --
        # resolved here rather than adding a `.zone` alias to the untouched
        # legacy class.
        zone = getattr(setup, "zone", None)
        if zone is None:
            zone = getattr(setup, "mtf_zone", None)
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
            sl_buffer=self._cfg.sl_buffer, is_short=is_short,
            target_floor_multiple=self._cfg.target_floor_multiple,
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
                         sl_price=sl_price, target_price=None, tracking_current_stop=sl_price)
        self.position = CascadePosition(
            underlying=self._cfg.underlying, side=side,
            tracking_strike=0.0, execution_strike=0.0,
            atm_at_trigger=0.0, entry_spot=0.0,
            t1=t1, t2=t2, open_time=bar.timestamp,
            tracking_entry_price=entry_price,
        )
        self._tracking_entry_price[side] = entry_price
        # TrailingBaseTracker's own `bear` flag mirrors the scanner's -- a
        # long (bear-zone) position trails a RISING floor, a short
        # (bull-zone, crypto PE) position trails a FALLING ceiling. This was
        # previously hardcoded bear=True regardless of side (latent bug,
        # never surfaced before crypto's PE-short path existed).
        # initial_stop=sl_price (2026-07-21): T2 shares T1's structural SL as
        # its own floor from minute one, instead of running with no stop at
        # all until the tracker's first new base happens to lock.
        self._trackers[side] = TrailingBaseTracker(
            bear=scanner._bear, initial_stop=sl_price,
            lookback_bases=self._cfg.t2_trail_lookback_bases,
        )

        audit = self._build_entry_audit(scanner, setup, zone, bar, sl_price, target_price)
        scanner.pop_setup(setup, bar.timestamp)
        event_type = CascadeEventType.OPEN_LONG_CE if side == "CE" else CascadeEventType.OPEN_LONG_PE
        return CascadeEvent(
            event_type=event_type, side=side, price_hint=entry_price, reason=entry_reason,
            sl_price=sl_price, target_price=target_price, timestamp=bar.timestamp, audit=audit,
        )

    def _build_entry_audit(self, scanner: _Scanner, setup, zone, bar,
                            sl_price: float, target_price: float) -> dict:
        """Full gate-by-gate rationale for why this trade fired -- Gate 1
        (Index) state + which confirmation anchored the scan window, Gate 2
        (Demand Block) zone geometry + which timeframe it locked on, Gate 3's
        computed limit price and the actual pierce price/time. book.py logs
        this verbatim so a full day's trades can be audited after the fact
        without guessing whether every gate genuinely fired.

        ``demand_block_sl_level`` is the zone's OWN structural attribute
        (ref.high for a bear zone) -- NOT the trade's real stop-loss, and
        naturally sits on the opposite side of ``demand_block_entry_line``
        from where the trade's SL actually is (a repeated source of user
        confusion). ``computed_sl_price``/``computed_target_price`` (tracking-
        contract scale, same as every other field here) are the ACTUAL levels
        this trade manages risk against -- SL = zone_low - buffer, target =
        zone.sl_level itself (the trap-confirmation candle's opposite
        extreme) -- included explicitly so the two are never conflated
        again."""
        is_short = not scanner._bear
        pierce_price = bar.high if is_short else bar.low
        anchor_ts = getattr(scanner, "_scan_window_start_ts", None)
        audit = {
            "index_kind": getattr(self._spot_confirm.current_kind, "value",
                                   str(self._spot_confirm.current_kind)),
            "index_window_anchor_ts": anchor_ts.isoformat() if anchor_ts else None,
            "demand_block_ref_ts": (zone.reference_low_ts.isoformat()
                                     if zone and zone.reference_low_ts else None),
            "demand_block_lock_ts": zone.lock_ts.isoformat() if zone and zone.lock_ts else None,
            "demand_block_entry_line": zone.entry_line if zone else None,
            "demand_block_sl_level": zone.sl_level if zone else None,
            "demand_block_sweep_low": zone.sweep_low if zone else None,
            "demand_block_timeframe": getattr(setup, "timeframe", None) or getattr(setup, "mtf_timeframe", None),
            "limit_entry_price": setup.limit_entry_price,
            "pierce_price": pierce_price,
            "pierce_bar_ts": bar.timestamp.isoformat() if bar.timestamp else None,
            "computed_sl_price": sl_price,
            "computed_target_price": target_price,
        }
        # Legacy (crypto) scanner also has an outer HTF (75m) zone distinct
        # from the Inner/MTF zone captured above -- surface it too.
        htf_zone = getattr(setup, "htf_zone", None)
        if htf_zone is not None:
            audit["htf_zone_ref_ts"] = htf_zone.reference_low_ts.isoformat() if htf_zone.reference_low_ts else None
            audit["htf_zone_lock_ts"] = htf_zone.lock_ts.isoformat() if htf_zone.lock_ts else None
            audit["htf_zone_entry_line"] = htf_zone.entry_line
            audit["htf_zone_sl_level"] = htf_zone.sl_level
        return audit

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
        trail = self._trackers.get(side)

        if t1 is not None and t1.status == "open":
            r = check_t1(t1, bar, is_short=is_short)
            if r.hit:
                t1.status = "closed"
                t1.close_price = r.price
                t1.close_reason = r.reason
                t1.close_time = bar.timestamp
                events.append(self._close_event(side, "T1", r.reason, r.price, bar.timestamp))
                # 2026-07-21, per explicit user direction: once T1's target
                # (not its SL) is hit, ratchet T2's trailing stop up to
                # breakeven immediately -- guarantees the combined position
                # can no longer net a loss after T1 has already booked
                # profit. move_to_breakeven never regresses an already
                # more-favorable trail (e.g. a base already locked above
                # entry), it only raises a stop still at/below entry.
                if r.reason == "t1_target_2r" and t2 is not None and t2.status == "open" and trail is not None:
                    # BUG (2026-07-21, same-day fix): must ratchet using
                    # tracking_entry, NOT t2.entry_price -- current_stop is
                    # always a TRACKING-contract price (check_hit/on_5m_bar
                    # compare it against tracking bars). t2.entry_price starts
                    # as the tracking-scale price at _open_position time but
                    # book.py's _on_fill OVERWRITES it with the real EXECUTION
                    # fill the moment the broker confirms (which happens
                    # almost immediately after entry, well before T1 could
                    # ever hit target 5+ minutes later) -- so by the time this
                    # ever fires, t2.entry_price is already execution-scale.
                    # Passing it here silently corrupted current_stop onto
                    # the wrong scale (confirmed live: displayed as a
                    # nonsensical trail_stop_price until a later, correctly-
                    # scaled base lock happened to overwrite it).
                    # buffer=self._cfg.sl_buffer (2026-07-21): a small
                    # cushion past raw cost, same tracking-point units already
                    # tuned per-underlying for SL -- protects a sliver of
                    # real profit and absorbs slippage instead of scratching
                    # T2 at exactly zero.
                    trail.move_to_breakeven(tracking_entry, buffer=self._cfg.sl_buffer)
                    if trail.current_stop is not None:
                        t2.trail_stop_price = map_trailing_stop_to_execution(
                            trail.current_stop, tracking_entry, t2.entry_price,
                        )
                        # Mirror the raw tracking-scale value too (2026-07-21)
                        # -- persisted so a restart can rebuild the tracker
                        # with this exact protection level instead of losing
                        # it (see TrancheLeg.tracking_current_stop docstring).
                        t2.tracking_current_stop = trail.current_stop

        if t2 is not None and t2.status == "open" and trail is not None:
            moved = trail.on_5m_bar(bar)
            if moved and trail.current_stop is not None:
                t2.trail_stop_price = map_trailing_stop_to_execution(
                    trail.current_stop, tracking_entry, t2.entry_price,
                )
                t2.tracking_current_stop = trail.current_stop
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

    def check_exits_execution_native(self, side: str, exec_bar) -> List[CascadeEvent]:
        """2026-07-21: T1/T2 exit-checks for a risk_basis=="execution_native"
        position, fed EXECUTION-contract bars directly by book.py's parallel
        execution-bar clock (see book.py's execution-contract 5m bar-builder,
        Task 7). No scale mapping anywhere here -- t1.sl_price/target_price,
        t2's tracker current_stop, and exec_bar are all already on the SAME
        (execution) scale, unlike _check_exits which bridges tracking-scale
        levels onto execution-scale bars via map_trailing_stop_to_execution.
        Public (unlike _check_exits) because book.py's bar-builder, not
        engine.py's own update(), is what drives this clock."""
        events: List[CascadeEvent] = []
        pos = self.position
        if pos is None:
            return events
        t1, t2 = pos.t1, pos.t2
        is_short = not self._scanners[side]._bear
        trail = self._trackers.get(side)

        if t1 is not None and t1.status == "open":
            r = check_t1(t1, exec_bar, is_short=is_short)
            if r.hit:
                t1.status = "closed"
                t1.close_price = r.price
                t1.close_reason = r.reason
                t1.close_time = exec_bar.timestamp
                events.append(self._close_event(side, "T1", r.reason, r.price, exec_bar.timestamp))
                if r.reason == "t1_target_2r" and t2 is not None and t2.status == "open" and trail is not None:
                    trail.move_to_breakeven(t1.entry_price, buffer=self._cfg.sl_buffer)
                    if trail.current_stop is not None:
                        t2.trail_stop_price = trail.current_stop   # already execution-scale, no mapping
                        t2.tracking_current_stop = trail.current_stop

        if t2 is not None and t2.status == "open" and trail is not None:
            moved = trail.on_5m_bar(exec_bar)
            if moved and trail.current_stop is not None:
                t2.trail_stop_price = trail.current_stop
                t2.tracking_current_stop = trail.current_stop
            r = trail.check_hit(exec_bar)
            if r.hit:
                t2.status = "closed"
                t2.close_price = r.price
                t2.close_reason = r.reason
                t2.close_time = exec_bar.timestamp
                events.append(self._close_event(side, "T2", r.reason, r.price, exec_bar.timestamp))

        if (t1 is None or t1.status == "closed") and (t2 is None or t2.status == "closed"):
            pos.status = "closed"
            pos.close_time = exec_bar.timestamp
            self._trackers.pop(side, None)
            self._tracking_entry_price.pop(side, None)
        return events

    def check_exits_tick(self, side: str, ts, tracking_ltp: Optional[float] = None,
                          execution_ltp: Optional[float] = None) -> List[CascadeEvent]:
        """2026-07-22: tick-driven SL/target/trailing-stop enforcement --
        fires the INSTANT a live tick crosses t1.sl_price/target_price or
        t2.trail_stop_price, instead of waiting for the next 5m bar close
        (_check_exits / check_exits_execution_native). Per explicit user
        direction: a stop only checked once every 5 minutes leaves real,
        observed extra bleed between the tick that breaches it and the bar
        close that finally acts on it.

        SCALE IS NOT UNIFORM -- this is the one thing a caller must get
        right, and the reason this takes two separate optional LTPs instead
        of one:
          - t1.sl_price/target_price are TRACKING-contract scale for a
            risk_basis=="tracking" position (engine._open_position calls
            compute_risk_mapping with tracking_entry_price==exec_entry_price,
            since the pure engine has no real execution fill yet -- scale
            never gets remapped onto the execution contract afterward
            either; book.py only ever overwrites these fields for the
            execution_native case). They are EXECUTION-contract scale for a
            risk_basis=="execution_native" position (compute_execution_native_risk
            collapses to the real execution entry price, scale=1). So T1 is
            only ever checked against the LTP whose scale actually matches
            the position's risk_basis -- passing the wrong one silently
            does nothing (the None guard), rather than firing on a bogus
            cross-scale comparison.
          - t2.trail_stop_price is ALWAYS execution-contract scale
            regardless of risk_basis (map_trailing_stop_to_execution for
            the tracking path, set directly for the execution-native path
            -- see _check_exits/check_exits_execution_native), so T2 is
            only ever checked against execution_ltp.

        Deliberately does NOT touch trail.current_stop or move/discover any
        base -- that stays exclusively on the 5m bar clock (it needs real
        bar structure). This only asks "has the live tick already crossed
        the CURRENT threshold." Closes at the live LTP, not the static
        threshold price -- a real market order fired on breach fills near
        LTP, which by definition is already at-or-past the threshold, not
        exactly on it (unlike the bar-close checks, which record
        price=sl_price/target_price on the assumption the whole bar's
        low/high touched exactly that level)."""
        events: List[CascadeEvent] = []
        pos = self.position
        if pos is None or pos.side != side or not pos.is_open:
            return events
        t1, t2 = pos.t1, pos.t2
        tracking_entry = self._tracking_entry_price.get(side, 0.0)
        is_short = not self._scanners[side]._bear
        trail = self._trackers.get(side)

        t1_ltp = execution_ltp if pos.risk_basis == "execution_native" else tracking_ltp
        if t1_ltp is not None and t1_ltp > 0 and t1 is not None and t1.status == "open":
            hit_sl = bool(t1.sl_price) and (t1_ltp >= t1.sl_price if is_short else t1_ltp <= t1.sl_price)
            hit_target = bool(t1.target_price) and (
                t1_ltp <= t1.target_price if is_short else t1_ltp >= t1.target_price
            )
            if hit_sl or hit_target:
                reason = "t1_sl_structural_floor" if hit_sl else "t1_target_2r"
                t1.status = "closed"
                t1.close_price = t1_ltp
                t1.close_reason = reason
                t1.close_time = ts
                events.append(self._close_event(side, "T1", reason, t1_ltp, ts))
                if reason == "t1_target_2r" and t2 is not None and t2.status == "open" and trail is not None:
                    if pos.risk_basis == "execution_native":
                        trail.move_to_breakeven(t1.entry_price, buffer=self._cfg.sl_buffer)
                        if trail.current_stop is not None:
                            t2.trail_stop_price = trail.current_stop
                            t2.tracking_current_stop = trail.current_stop
                    else:
                        trail.move_to_breakeven(tracking_entry, buffer=self._cfg.sl_buffer)
                        if trail.current_stop is not None:
                            t2.trail_stop_price = map_trailing_stop_to_execution(
                                trail.current_stop, tracking_entry, t2.entry_price,
                            )
                            t2.tracking_current_stop = trail.current_stop

        if (execution_ltp is not None and execution_ltp > 0
                and t2 is not None and t2.status == "open" and t2.trail_stop_price is not None):
            hit = execution_ltp >= t2.trail_stop_price if is_short else execution_ltp <= t2.trail_stop_price
            if hit:
                t2.status = "closed"
                t2.close_price = execution_ltp
                t2.close_reason = "t2_trailing_base_stop"
                t2.close_time = ts
                events.append(self._close_event(side, "T2", "t2_trailing_base_stop", execution_ltp, ts))

        if (t1 is None or t1.status == "closed") and (t2 is None or t2.status == "closed"):
            pos.status = "closed"
            pos.close_time = ts
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
