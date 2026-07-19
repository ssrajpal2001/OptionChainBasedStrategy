"""
strategies/v4_cascade/zone_state.py — per-tracking-contract multiplier-ladder
zone scanner.

The tracking-contract premium chart is scanned for a 3-candle sweep+reclaim
(rolling_base.py) at 75m first; if no confirmed pattern exists yet, the same
3-candle check is re-run at 150m, then 225m, 300m, ... (the "multiplier
ladder") until either a confirmed pattern is found (first match wins — the
ladder stops climbing) or the ladder is exhausted (capped once its lookback
would exceed the previous+current trading week window — see
``ladder_for_lookback``).

The scan is re-run every time a new 5m bar closes (5m is the finest granularity
fed to this module — the ladder resamples 5m bars UP to each multiplier
in-process, rather than requiring the data layer to pre-build every timeframe).
Once a multiplier confirms, the resulting zone LOCKS and the module transitions
to RETEST_PENDING; the mechanical pierce-check (5m candle low <= entry_line)
is then evaluated on every subsequent 5m bar until the setup triggers, is
invalidated (stale / no spot confirmation), or the position closes and a new
scan cycle begins.

No bus/broker/DB dependency — pure, fed CandleEvent-shaped 5m bars directly.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import Deque, List, Optional

from strategies.v4_cascade.dataclasses import RollingBaseZone, ZoneState, GateState
from strategies.v4_cascade.rolling_base import (
    scan_ladder, build_ladder, LadderMatch, find_bear_trap_2candle,
    find_all_bear_traps_2candle, resample_bars,
)

# NSE trading day ~ 6h15m = 375 minutes. Cap the ladder once a multiplier's
# single 3-candle window (3 * multiplier minutes) would exceed 2 trading
# weeks (10 sessions) of context — "till we reach the prev week and current
# week" per spec.
_TRADING_MINUTES_PER_DAY = 375
_LOOKBACK_SESSIONS = 10   # previous week (5) + current week (5)
_MAX_LADDER_MINUTES = _TRADING_MINUTES_PER_DAY * _LOOKBACK_SESSIONS
_LADDER_STEP = 75
_BARS_PER_DAY_5M = _TRADING_MINUTES_PER_DAY // 5   # 75
# Buffer size is a whole number of extra trading DAYS beyond the lookback
# window, not just "+N bars" — resample_bars is now clock-anchored per
# calendar day (see rolling_base.py) so eviction no longer breaks alignment
# regardless, but keeping this a multiple of a day's bar count is still
# good hygiene (avoids ever evicting a day's bars mid-day).
_MAX_5M_BARS = (_MAX_LADDER_MINUTES // 5) + (2 * _BARS_PER_DAY_5M)

DEFAULT_LADDER: List[int] = build_ladder(_LADDER_STEP, _MAX_LADDER_MINUTES)


class TrackingZoneScanner:
    """One instance per tracking contract (CE or PE side). ``bear=True`` for
    the CE-tracking leg (bear trap on its own premium = bullish market read);
    ``bear=False`` for the PE-tracking leg (bull trap = bearish market read)."""

    def __init__(self, bear: bool, ladder: Optional[List[int]] = None) -> None:
        self._bear = bear
        self._ladder = ladder if ladder is not None else DEFAULT_LADDER
        self._bars_5m: Deque = deque(maxlen=_MAX_5M_BARS)
        self._consumed_before_ts: Optional[datetime] = None  # skip already-acted-on reclaims

        self.state: ZoneState = ZoneState.IDLE
        self.active_zone: Optional[RollingBaseZone] = None
        self.active_multiplier: Optional[int] = None

    def on_5m_bar(self, bar) -> None:
        """Feed one closed 5m bar (CandleEvent-shaped: .high/.low/.close/.timestamp).
        Re-runs the ladder scan and advances IDLE -> RETEST_PENDING as needed."""
        self._bars_5m.append(bar)

        if self.state in (ZoneState.IDLE, ZoneState.INVALIDATED):
            match: Optional[LadderMatch] = scan_ladder(
                list(self._bars_5m), self._ladder, bear=self._bear,
                skip_before_ts=self._consumed_before_ts,
            )
            if match is not None:
                self.active_zone = match.zone
                self.active_multiplier = match.multiplier
                self.state = ZoneState.RETEST_PENDING

    def check_pierce(self, bar) -> bool:
        """True if this 5m bar's low pierces the frozen entry line while
        RETEST_PENDING. Does not itself consume/transition state — caller
        (entries.py) decides fire vs. discard based on the spot-confirmation
        gate, then calls ``consume()`` or ``invalidate()`` accordingly."""
        if self.state != ZoneState.RETEST_PENDING or self.active_zone is None:
            return False
        entry_line = self.active_zone.entry_line
        if entry_line is None:
            return False
        if self._bear:
            return bar.low <= entry_line
        return bar.high >= entry_line

    def consume(self, ts: datetime) -> None:
        """A trigger fired and was acted on (entry taken). Mark this setup
        instance consumed and return to IDLE so a fresh scan can begin."""
        self._consumed_before_ts = ts
        self.active_zone = None
        self.active_multiplier = None
        self.state = ZoneState.IDLE

    def invalidate(self, ts: datetime) -> None:
        """A pierce fired but spot did not confirm — permanently discard this
        setup instance (per spec: 'discard the setup'), do not re-arm on the
        very next low-touch of the same frozen line."""
        self._consumed_before_ts = ts
        self.active_zone = None
        self.active_multiplier = None
        self.state = ZoneState.INVALIDATED

    def reset(self) -> None:
        self._bars_5m.clear()
        self._consumed_before_ts = None
        self.active_zone = None
        self.active_multiplier = None
        self.state = ZoneState.IDLE


# ─────────────────────────────────────────────────────────────────────────────
# 2026-07-19 3-gate pure-premium funnel (supersedes the dual tracking-contract
# + spot-confirmation-at-pierce model for entries). NIFTY spot is now scanned
# for BOTH bear and bull traps only to set a directional BIAS (spot_confirm.py,
# unchanged) that decides which side's funnel below is armed — a spot bear
# trap (bullish read) arms CE, a spot bull trap (bearish read) arms PE. Once
# armed, the CE/PE tracking contract's OWN premium chart is scanned
# exclusively for BEAR TRAPS (option sellers trapped as premium spikes above
# their structural high) at every gate — never bull traps; ``find_bull_zone``
# is not used anywhere in this class.
#
# MULTI-ZONE (2026-07-19, same-day follow-up fix): a real market has MANY
# concurrent HTF/MTF trap structures open at once at different price levels —
# a single-"active zone"-per-side scanner (the original design here, and the
# still-untouched TrackingZoneScanner above) silently misses almost all of
# them, exactly as found earlier this session on the spot-side validation.
# PremiumGateScanner therefore tracks a LIST of independent in-flight
# ``_HTFSetup`` instances per side (each with its OWN HTF zone, MTF zone,
# limit price, and gate state), re-scanning for brand-new HTF refs on every
# 75m bar via ``find_all_bear_traps_2candle`` (price-level-aware dedup,
# validated against real chart data this session) rather than stopping after
# the first lock.
# ─────────────────────────────────────────────────────────────────────────────

_MAX_75M_BARS_GATE = 200  # ~2 trading weeks headroom, matches spot_confirm.py's buffer


class _HTFSetup:
    """One independently-tracked HTF ref -> MTF Inner Zone -> limit funnel.
    Mutable (state advances in place) — not a frozen dataclass."""

    __slots__ = ("htf_zone", "htf_ref_ts", "state", "mtf_zone", "mtf_timeframe",
                 "limit_entry_price", "mtf_consumed_before_ts")

    def __init__(self, htf_zone: RollingBaseZone) -> None:
        self.htf_zone = htf_zone
        self.htf_ref_ts = htf_zone.reference_low_ts
        self.state: GateState = GateState.HTF_LOCKED
        self.mtf_zone: Optional[RollingBaseZone] = None
        self.mtf_timeframe: Optional[int] = None
        self.limit_entry_price: Optional[float] = None
        self.mtf_consumed_before_ts: Optional[datetime] = None


class PremiumGateScanner:
    """One instance per side (CE or PE tracking contract). Owns Gate 1 (75m
    HTF lock + zone-entry) and Gate 2 (5m Inner Zone lock, 15m fallback +
    zone-entry) for EVERY concurrently-forming/-locked HTF structure
    (``self.setups``), advancing each independently through to
    ``LIMIT_ARMED`` with its own computed ``limit_entry_price``. Gate 3's
    pierce CHECK (does a live 5m bar's low reach a setup's
    ``limit_entry_price``) is evaluated externally by
    ``entries.check_limit_pierce``, which pops any setups that fired via
    ``pop_triggered()``."""

    def __init__(self) -> None:
        self.armed: bool = False

        self._bars_75m: Deque = deque(maxlen=_MAX_75M_BARS_GATE)
        self._bars_5m: Deque = deque(maxlen=_MAX_5M_BARS)
        self._htf_consumed_before_ts: Optional[datetime] = None
        self._known_ref_ts: set = set()

        self.setups: List[_HTFSetup] = []

    @property
    def state(self) -> GateState:
        """Informational summary only (used by callers/logging that expect a
        single top-level state) — the most-advanced state across all
        in-flight setups, or ARMED_WAIT/HTF_SCANNING if none are locked yet."""
        if not self.setups:
            return GateState.HTF_SCANNING if self.armed else GateState.ARMED_WAIT
        order = [GateState.HTF_LOCKED, GateState.MTF_SCANNING_5M, GateState.MTF_SCANNING_15M,
                 GateState.MTF_LOCKED, GateState.LIMIT_ARMED]
        return max(self.setups, key=lambda s: order.index(s.state) if s.state in order else -1).state

    # ── engine-driven bias control ──────────────────────────────────────────
    def set_armed(self, armed: bool) -> None:
        """Called by the engine each time spot bias is re-evaluated. Gates
        whether NEW HTF refs are searched for — never aborts an in-flight
        setup already underway."""
        self.armed = armed

    # ── Gate 1 (75m) ─────────────────────────────────────────────────────────
    def on_75m_bar(self, bar) -> None:
        self._bars_75m.append(bar)
        if self.armed:
            self._scan_for_new_htf_setups()
        for setup in self.setups:
            if setup.state == GateState.HTF_LOCKED:
                self._check_htf_zone_entry(setup, bar)

    def _scan_for_new_htf_setups(self) -> None:
        zones = find_all_bear_traps_2candle(list(self._bars_75m), skip_before_ts=self._htf_consumed_before_ts)
        for zone in zones:
            if zone.reference_low_ts in self._known_ref_ts:
                continue
            self._known_ref_ts.add(zone.reference_low_ts)
            self.setups.append(_HTFSetup(zone))

    def _check_htf_zone_entry(self, setup: _HTFSetup, bar) -> None:
        z = setup.htf_zone
        if z is None or z.entry_line is None or z.sweep_low is None:
            return
        if bar.low <= z.entry_line and bar.high >= z.sweep_low:
            setup.state = GateState.WAITING_FOR_HTF_ZONE_ENTRY  # transient marker
            setup.state = GateState.MTF_SCANNING_5M

    # ── Gate 2 (5m, fallback 15m) — also drives Gate-1 zone-entry at finer
    #    granularity and buffers the 5m history Gate 2 scans over ──────────
    def on_5m_bar(self, bar) -> None:
        self._bars_5m.append(bar)
        for setup in self.setups:
            if setup.state == GateState.HTF_LOCKED:
                self._check_htf_zone_entry(setup, bar)
            if setup.state in (GateState.MTF_SCANNING_5M, GateState.MTF_SCANNING_15M):
                self._attempt_mtf_lock(setup)
            elif setup.state == GateState.MTF_LOCKED:
                self._check_mtf_zone_entry(setup, bar)

    def _mtf_window(self, setup: _HTFSetup) -> List:
        if setup.htf_ref_ts is None:
            return []
        return [b for b in self._bars_5m if b.timestamp >= setup.htf_ref_ts]

    def _attempt_mtf_lock(self, setup: _HTFSetup) -> None:
        window = self._mtf_window(setup)
        if len(window) < 3:
            return
        zone = find_bear_trap_2candle(window, skip_before_ts=setup.mtf_consumed_before_ts)
        if zone is not None:
            setup.mtf_zone = zone
            setup.mtf_timeframe = 5
            setup.state = GateState.MTF_LOCKED
            return
        # 5m found nothing yet -- fall back to a 15m resample of the SAME window.
        setup.state = GateState.MTF_SCANNING_15M
        resampled = resample_bars(window, 15)
        if len(resampled) < 3:
            setup.state = GateState.MTF_SCANNING_5M  # keep retrying as the window grows
            return
        zone15 = find_bear_trap_2candle(resampled, skip_before_ts=setup.mtf_consumed_before_ts)
        if zone15 is not None:
            setup.mtf_zone = zone15
            setup.mtf_timeframe = 15
            setup.state = GateState.MTF_LOCKED
        else:
            setup.state = GateState.MTF_SCANNING_5M  # keep retrying as the window grows

    def _check_mtf_zone_entry(self, setup: _HTFSetup, bar) -> None:
        z = setup.mtf_zone
        if z is None or z.entry_line is None or z.sweep_low is None:
            return
        if bar.low <= z.entry_line and bar.high >= z.sweep_low:
            setup.state = GateState.WAITING_FOR_MTF_ZONE_ENTRY  # transient marker
            inner_high, inner_low = z.entry_line, z.sweep_low
            setup.limit_entry_price = inner_high - (inner_high - inner_low) / 3.0
            setup.state = GateState.LIMIT_ARMED

    # ── Gate 3 support ───────────────────────────────────────────────────────
    def limit_armed_setups(self) -> List[_HTFSetup]:
        return [s for s in self.setups if s.state == GateState.LIMIT_ARMED]

    def pop_setup(self, setup: _HTFSetup, ts: datetime) -> None:
        """A Gate 3 trigger fired on ``setup`` and was acted on — remove just
        that setup; every OTHER in-flight setup keeps advancing untouched.
        ``_known_ref_ts`` is NOT cleared for this ref, so a fresh scan can
        never re-add the exact same already-traded HTF structure."""
        if setup in self.setups:
            self.setups.remove(setup)

    def invalidate_setup(self, setup: _HTFSetup) -> None:
        if setup in self.setups:
            self.setups.remove(setup)

    def reset(self) -> None:
        self._bars_75m.clear()
        self._bars_5m.clear()
        self._htf_consumed_before_ts = None
        self._known_ref_ts.clear()
        self.setups.clear()
        self.armed = False
