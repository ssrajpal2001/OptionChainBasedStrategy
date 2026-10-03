"""Pure data classes for the Bear Trap OI Confirmation strategy.

See docs/superpowers/specs/2026-10-03-bear-trap-oi-design.md for the full
mechanic. No behavior lives here -- only shapes.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Literal, Optional

Side = Literal["CE", "PE"]
Trend = Literal["RISING", "FALLING", "FLAT"]
PositionStatus = Literal["open", "closed"]
MtmTrigger = Literal["concurrent_entry", "periodic"]


@dataclass(frozen=True)
class Bar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float


class TrapZoneState(str, Enum):
    WAITING = "WAITING"
    BREAKDOWN_WATCH = "BREAKDOWN_WATCH"
    TRAP_WATCH = "TRAP_WATCH"
    ARMED_WAIT_REENTRY = "ARMED_WAIT_REENTRY"
    IN_POSITION = "IN_POSITION"


@dataclass
class TrapZone:
    state: TrapZoneState
    c1: Optional[Bar]
    c2: Optional[Bar]
    zone_lo: Optional[float]
    zone_hi: Optional[float]
    confirmed_ts: Optional[datetime]


@dataclass(frozen=True)
class OiSnapshot:
    strike: int
    side: Side
    oi: int
    ts: datetime


@dataclass(frozen=True)
class OiFilterResult:
    call_oi_total: int
    put_oi_total_same_band: int
    call_oi_trend: Trend
    put_oi_trend: Trend
    passed: bool
    detail: str


@dataclass
class Position:
    side: Side
    strike: int
    qty: int
    entry_price: float
    entry_ts: datetime
    status: PositionStatus


@dataclass(frozen=True)
class MtmSnapshot:
    side: Side
    observed_entry_price: float
    observed_qty: int
    observed_strike: int
    current_ltp: float
    running_mtm: float
    elapsed_sec: int
    trigger: MtmTrigger
    ts: datetime
    other_side_event: Optional[str]
