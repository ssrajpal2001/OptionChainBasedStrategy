"""
strategies/v4_cascade/dataclasses.py — pure data containers for the V4 Premium
Trap Cascade Engine.

Kept intentionally free of strategy/feed/broker/DB dependencies so they can be
imported and tested in isolation (mirrors strategies/sell_straddle/dataclasses.py).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from typing import Dict, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Rolling Base zone state (shared by entry-zone and trailing-stop usage)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RollingBaseZone:
    """Dynamic Rolling Base structural floor for one tracking contract (or spot).

    Ratchets down on successive lower closes while unlocked. Locks (freezes
    ``entry_line``) once a genuine sweep (price trades below ``reference_low``)
    is followed by a reclaim (a later close back above it). ``entry_line`` is
    the pre-sweep floor — the level a later candle's low must pierce to fire
    the mechanical retest trigger. ``sweep_low`` is the tighter extreme reached
    during the sweep, used only as the stop-loss reference.
    """
    reference_low: Optional[float] = None
    reference_low_ts: Optional[datetime] = None
    prev_close: Optional[float] = None

    swept: bool = False
    sweep_low: Optional[float] = None
    sweep_started_ts: Optional[datetime] = None
    bars_since_sweep: int = 0

    locked: bool = False
    lock_ts: Optional[datetime] = None
    entry_line: Optional[float] = None
    sl_level: Optional[float] = None   # the ref candle's opposite extreme (bears' SL / buyers' SL) —
                                        # trap confirms only once a later bar's high/low clears THIS level

    def to_dict(self) -> dict:
        return {
            "reference_low": self.reference_low,
            "reference_low_ts": self.reference_low_ts.isoformat() if self.reference_low_ts else None,
            "prev_close": self.prev_close,
            "swept": self.swept,
            "sweep_low": self.sweep_low,
            "sweep_started_ts": self.sweep_started_ts.isoformat() if self.sweep_started_ts else None,
            "bars_since_sweep": self.bars_since_sweep,
            "locked": self.locked,
            "lock_ts": self.lock_ts.isoformat() if self.lock_ts else None,
            "entry_line": self.entry_line,
            "sl_level": self.sl_level,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "RollingBaseZone":
        def _dt(x):
            return datetime.fromisoformat(x) if x else None
        return cls(
            reference_low=d.get("reference_low"),
            reference_low_ts=_dt(d.get("reference_low_ts")),
            prev_close=d.get("prev_close"),
            swept=d.get("swept", False),
            sweep_low=d.get("sweep_low"),
            sweep_started_ts=_dt(d.get("sweep_started_ts")),
            bars_since_sweep=d.get("bars_since_sweep", 0),
            locked=d.get("locked", False),
            lock_ts=_dt(d.get("lock_ts")),
            entry_line=d.get("entry_line"),
            sl_level=d.get("sl_level"),
        )


class ZoneState(str, Enum):
    IDLE = "idle"                     # unlocked, ratcheting (or nothing seen yet)
    SWEPT = "swept"                   # price traded below reference_low, awaiting reclaim
    RETEST_PENDING = "retest_pending"  # reclaimed & locked; watching for a low-touch pierce
    INVALIDATED = "invalidated"        # a setup instance was discarded (stale sweep, or
                                        # a pierce fired without spot confirmation)


class GateState(str, Enum):
    """2026-07-19 3-gate pure-premium funnel (PremiumGateScanner, zone_state.py).
    Separate from ZoneState/TrackingZoneScanner, which stays untouched — T2's
    4x5m trailing stop (exits.py TrailingBaseTracker) still reuses
    TrackingZoneScanner as-is and must not be disturbed by this refactor."""
    ARMED_WAIT = "armed_wait"                              # not armed by spot bias yet
    HTF_SCANNING = "htf_scanning"                           # armed; scanning 75m for a ref+next-candle+TRAPPED
    HTF_LOCKED = "htf_locked"                               # HTF zone frozen; watching for price to re-enter it
    WAITING_FOR_HTF_ZONE_ENTRY = "waiting_for_htf_zone_entry"  # transient: the instant HTF zone-entry fires
    MTF_SCANNING_5M = "mtf_scanning_5m"                     # scanning the 5m window from HTF ref_ts onward
    MTF_SCANNING_15M = "mtf_scanning_15m"                   # transient fallback retry when 5m finds nothing
    MTF_LOCKED = "mtf_locked"                               # Inner Zone frozen; watching for price to re-enter it
    WAITING_FOR_MTF_ZONE_ENTRY = "waiting_for_mtf_zone_entry"  # transient: the instant Inner Zone entry fires
    LIMIT_ARMED = "limit_armed"                             # pending 1/3-depth limit, watching for a 5m low pierce
    TRIGGERED = "triggered"                                 # fired — engine opens the position


class SpotTrapKind(str, Enum):
    NONE = "none"
    BEAR_TRAP_CLOSE = "bear_trap_close"   # sweep of spot demand zone + reclaim close (CE confirm)
    BULL_TRAP_CLOSE = "bull_trap_close"   # sweep of spot structural highs + reclaim close (PE confirm)


# ─────────────────────────────────────────────────────────────────────────────
# Position / tranche state
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TrancheLeg:
    tranche: str                 # "T1" | "T2"
    option_type: str             # "CE" | "PE"
    strike: float
    qty: int
    entry_price: float = 0.0     # execution-contract entry premium (tracking price_hint until filled)
    entry_time: Optional[datetime] = None
    sl_price: float = 0.0        # premium-mapped SL (proportionally scaled from tracking contract)
    target_price: Optional[float] = None       # T1 only: fixed 2R target; None for T2 (trailing-managed)
    trail_stop_price: Optional[float] = None    # T2 only: current trailing stop level
    status: str = "open"         # "open" | "closed"
    close_price: float = 0.0
    close_time: Optional[datetime] = None
    close_reason: str = ""
    realized_pnl: float = 0.0

    def to_dict(self) -> dict:
        return {
            "tranche": self.tranche, "option_type": self.option_type,
            "strike": self.strike, "qty": self.qty,
            "entry_price": self.entry_price,
            "entry_time": self.entry_time.isoformat() if self.entry_time else None,
            "sl_price": self.sl_price, "target_price": self.target_price,
            "trail_stop_price": self.trail_stop_price, "status": self.status,
            "close_price": self.close_price,
            "close_time": self.close_time.isoformat() if self.close_time else None,
            "close_reason": self.close_reason, "realized_pnl": self.realized_pnl,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TrancheLeg":
        def _dt(x):
            return datetime.fromisoformat(x) if x else None
        return cls(
            tranche=d["tranche"], option_type=d["option_type"],
            strike=d["strike"], qty=d["qty"],
            entry_price=d.get("entry_price", 0.0), entry_time=_dt(d.get("entry_time")),
            sl_price=d.get("sl_price", 0.0), target_price=d.get("target_price"),
            trail_stop_price=d.get("trail_stop_price"), status=d.get("status", "open"),
            close_price=d.get("close_price", 0.0), close_time=_dt(d.get("close_time")),
            close_reason=d.get("close_reason", ""), realized_pnl=d.get("realized_pnl", 0.0),
        )


@dataclass
class CascadePosition:
    underlying: str
    side: str                    # "CE" | "PE" — which side is currently live
    tracking_strike: float       # ATM∓200 tracking contract used to derive structure/SL/TP
    execution_strike: float      # ATM±50 execution contract actually traded
    atm_at_trigger: float        # live spot ATM at trigger time (NOT the 09:15 session-open ATM)
    entry_spot: float
    expiry_date: Optional[date] = None
    t1: Optional[TrancheLeg] = None
    t2: Optional[TrancheLeg] = None
    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    status: str = "open"         # "open" | "closed"
    entry_indicators: Dict[str, float] = field(default_factory=dict)

    @property
    def is_open(self) -> bool:
        return self.status == "open"

    def to_dict(self) -> dict:
        return {
            "underlying": self.underlying, "side": self.side,
            "tracking_strike": self.tracking_strike, "execution_strike": self.execution_strike,
            "atm_at_trigger": self.atm_at_trigger, "entry_spot": self.entry_spot,
            "expiry_date": self.expiry_date.isoformat() if self.expiry_date else None,
            "t1": self.t1.to_dict() if self.t1 else None,
            "t2": self.t2.to_dict() if self.t2 else None,
            "open_time": self.open_time.isoformat() if self.open_time else None,
            "close_time": self.close_time.isoformat() if self.close_time else None,
            "status": self.status,
            "entry_indicators": dict(self.entry_indicators),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CascadePosition":
        def _dt(x):
            return datetime.fromisoformat(x) if x else None
        return cls(
            underlying=d["underlying"], side=d["side"],
            tracking_strike=d.get("tracking_strike", 0.0), execution_strike=d.get("execution_strike", 0.0),
            atm_at_trigger=d.get("atm_at_trigger", 0.0), entry_spot=d.get("entry_spot", 0.0),
            expiry_date=_dt(d.get("expiry_date")).date() if d.get("expiry_date") else None,
            t1=TrancheLeg.from_dict(d["t1"]) if d.get("t1") else None,
            t2=TrancheLeg.from_dict(d["t2"]) if d.get("t2") else None,
            open_time=_dt(d.get("open_time")), close_time=_dt(d.get("close_time")),
            status=d.get("status", "open"),
            entry_indicators=dict(d.get("entry_indicators", {})),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Emitted events
# ─────────────────────────────────────────────────────────────────────────────

class CascadeEventType(str, Enum):
    OPEN_LONG_CE = "open_long_ce"
    OPEN_LONG_PE = "open_long_pe"
    CLOSE_LONG_CE = "close_long_ce"
    CLOSE_LONG_PE = "close_long_pe"
    TRAIL_UPDATE = "trail_update"
    STRUCTURAL_FLIP = "structural_flip"


@dataclass(frozen=True)
class CascadeEvent:
    event_type: CascadeEventType
    side: str                              # "CE" | "PE" — side this event pertains to
    tranche: Optional[str] = None          # "T1" | "T2" | None (position-level events)
    execution_strike: Optional[float] = None
    qty: Optional[int] = None
    reason: str = ""
    price_hint: Optional[float] = None     # tracking-contract price at decision time
    sl_price: Optional[float] = None
    target_price: Optional[float] = None
    trail_stop_price: Optional[float] = None
    timestamp: Optional[datetime] = None
