"""
strategies/d1_trap_option/index_intraday_cross.py — IndexIntradayCrossTracker
(2026-08-11).

Single-timeframe (15-min) intraday strategy for NIFTY/SENSEX, on the
option's own weekly-expiry premium chart, per the user's own spec:

  1. ATM fixed at day open (CE/PE strikes chosen once from opening spot,
     not re-selected intraday).
  2. Bear-trap zone (find_all_bear_zones, same sweep+reclaim algorithm used
     everywhere else in this codebase) detected on the CE's OWN 15-min
     premium bars.
  3. Live CE premium touches that zone -> the next 15-min candle becomes
     the ref candle -> the candle AFTER THAT breaking above the ref
     candle's high is the CE-side breakout confirmation.
  4. PE cross-check: at the moment CE's breakout would confirm, has PE's
     own reference candle (the PE bar at the SAME time-window as CE's ref
     candle) had its LOW breached by any PE candle since then? This is a
     state check ("has it already happened, whenever/whyever"), not a
     synchronized-event check -- PE can break down earlier for unrelated
     reasons (theta, liquidity, IV) and that still counts.
  5. Entry (CE) fires only when both conditions hold.
  6. Optional no_entry_after cutoff (e.g. 14:30) -- no NEW entries once
     the day is this close to the intraday square-off, added 2026-08-11
     after several backtest signals fired with no real time left to
     develop before EOD.

Mirror (PE/SHORT) not yet specified by the user -- LONG/CE only for now.

No exit rule specified yet -- this only detects and records ENTRY signals.

Every zone's full lifecycle is tracked and exposed via zone_log(), not just
successful entries -- a zone that was touched but never broke, or broke
without PE confirming, is a real, reportable outcome, not something to
silently drop.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time as _time
from typing import Dict, List, Optional

from strategies.v4_cascade.rolling_base import find_all_bear_zones


@dataclass
class _Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float


def resample_bars(bars: List[_Bar], minutes: int) -> List[_Bar]:
    if not bars:
        return []
    buckets: Dict[tuple, list] = {}
    order: List[tuple] = []
    for b in bars:
        day = b.timestamp.date()
        minute_of_day = b.timestamp.hour * 60 + b.timestamp.minute
        bucket_start_min = (minute_of_day // minutes) * minutes
        key = (day, bucket_start_min)
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(b)
    out = []
    for key in order:
        group = buckets[key]
        day, bucket_start_min = key
        ts = group[0].timestamp.replace(hour=bucket_start_min // 60, minute=bucket_start_min % 60,
                                         second=0, microsecond=0)
        out.append(_Bar(timestamp=ts, open=group[0].open, high=max(g.high for g in group),
                         low=min(g.low for g in group), close=group[-1].close))
    return out


@dataclass
class _ZoneState:
    zone_lo: float
    zone_hi: float
    lock_ts: datetime
    phase: str = "WAITING_TOUCH"   # -> WAITING_BREAK -> DONE
    touch_ts: Optional[datetime] = None
    ref_candle_ce: Optional[_Bar] = None
    ref_candle_pe: Optional[_Bar] = None
    pe_ref_broken: bool = False
    pe_ref_broken_ts: Optional[datetime] = None
    ce_break_ts: Optional[datetime] = None
    outcome: str = "NEVER_TOUCHED"   # NEVER_TOUCHED | TOUCHED_NO_BREAK | CE_BROKE_NO_PE_CONFIRM |
                                       # SIGNAL_FIRED | SKIPPED_TIME_CUTOFF


class IndexIntradayCrossTracker:
    """One instance per (underlying, day). Reset fresh each trading day --
    ATM/zones are day-scoped by design (per the user's own spec: ATM fixed
    at open, no carry across days)."""

    def __init__(self, no_entry_after: Optional[_time] = None, max_zone_age_days: Optional[int] = None) -> None:
        self.zones: List[_ZoneState] = []
        self.known_ref_ts: set = set()
        self.signals: List[dict] = []
        self.no_entry_after = no_entry_after
        # 2026-08-11: added per direct request -- was resetting the zone pool
        # fresh every day (same-day only), which meant a trending day with no
        # sweep+reclaim of its own had literally nothing to trade until very
        # late. None = old behavior (same-day only, via the caller only ever
        # seeding today's bars); a real int lets zones from up to N days back
        # stay tradeable today too.
        self.max_zone_age_days = max_zone_age_days

    def seed_zones(self, ce_bars_15m: List[_Bar]) -> None:
        for z in find_all_bear_zones(ce_bars_15m, known_ref_ts=self.known_ref_ts):
            self.known_ref_ts.add(z.reference_low_ts)
            self.zones.append(_ZoneState(
                zone_lo=min(z.entry_line, z.sweep_low), zone_hi=max(z.entry_line, z.sweep_low),
                lock_ts=z.lock_ts,
            ))

    def on_ce_15m_bar(self, bar: _Bar, pe_bar_same_window: Optional[_Bar]) -> Optional[dict]:
        """Feed one new CE 15-min bar (and the PE bar for that exact same
        time window, if available) in chronological order."""
        cutoff_hit = self.no_entry_after is not None and bar.timestamp.time() >= self.no_entry_after
        for zs in self.zones:
            if bar.timestamp <= zs.lock_ts:
                continue   # zone not locked yet as of this bar -- no lookahead
            if zs.phase == "DONE":
                continue
            if (self.max_zone_age_days is not None and zs.phase == "WAITING_TOUCH"
                    and (bar.timestamp.date() - zs.lock_ts.date()).days > self.max_zone_age_days):
                continue   # too old to still be tradeable -- once WAITING_BREAK has started
                           # (already touched), let it run its course regardless of the zone's age
            if zs.phase == "WAITING_TOUCH":
                if bar.low <= zs.zone_hi:
                    zs.phase = "WAITING_BREAK"
                    zs.touch_ts = bar.timestamp
                    zs.outcome = "TOUCHED_NO_BREAK"   # provisional -- upgraded below if it progresses
                continue
            if zs.phase == "WAITING_BREAK":
                if zs.ref_candle_ce is None:
                    zs.ref_candle_ce = bar
                    zs.ref_candle_pe = pe_bar_same_window
                    continue
                if pe_bar_same_window is not None and zs.ref_candle_pe is not None:
                    if pe_bar_same_window.low < zs.ref_candle_pe.low:
                        if not zs.pe_ref_broken:
                            zs.pe_ref_broken_ts = pe_bar_same_window.timestamp
                        zs.pe_ref_broken = True
                ce_broke = bar.high > zs.ref_candle_ce.high
                if ce_broke:
                    zs.ce_break_ts = bar.timestamp
                    if cutoff_hit:
                        zs.phase = "DONE"
                        zs.outcome = "SKIPPED_TIME_CUTOFF"
                        continue
                    if zs.pe_ref_broken:
                        sig = dict(
                            zone_lo=zs.zone_lo, zone_hi=zs.zone_hi, zone_lock_ts=zs.lock_ts,
                            touch_ts=zs.touch_ts,
                            ref_candle_ts=zs.ref_candle_ce.timestamp, ref_candle_high=zs.ref_candle_ce.high,
                            entry_ts=bar.timestamp, entry_price=bar.close,
                            pe_ref_low=zs.ref_candle_pe.low if zs.ref_candle_pe else None,
                            pe_ref_broken_ts=zs.pe_ref_broken_ts,
                        )
                        self.signals.append(sig)
                        zs.phase = "DONE"
                        zs.outcome = "SIGNAL_FIRED"
                        return sig
                    else:
                        zs.phase = "DONE"
                        zs.outcome = "CE_BROKE_NO_PE_CONFIRM"
        return None

    def zone_log(self) -> List[dict]:
        """Full lifecycle of every zone this tracker ever saw, not just the
        ones that fired -- touched-but-never-broke and broke-but-no-PE-
        confirm are both real, reportable outcomes."""
        out = []
        for zs in self.zones:
            out.append(dict(
                zone_lo=zs.zone_lo, zone_hi=zs.zone_hi, zone_lock_ts=zs.lock_ts,
                touch_ts=zs.touch_ts, ref_candle_ts=(zs.ref_candle_ce.timestamp if zs.ref_candle_ce else None),
                ce_break_ts=zs.ce_break_ts, pe_ref_broken_ts=zs.pe_ref_broken_ts,
                outcome=zs.outcome,
            ))
        return out
