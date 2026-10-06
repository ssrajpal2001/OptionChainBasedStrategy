"""NSE/BSE trading-day calendar -- weekends + published exchange holidays.

2026-10-06, direct user spec: the EOD hedge-and-carry T-1 check
(strategies/sell_straddle/exits.py:_is_t1_from_expiry) used to be a plain
calendar-date subtraction, which silently breaks whenever a real NSE
holiday falls between "now" and the position's expiry date -- e.g. a
Tuesday-expiry week with a Monday holiday: the TRUE last trading day
before expiry is Friday, but `(expiry_date - now.date()).days` only reads
<=1 on the Monday holiday itself (no trading happens, so the check never
even runs) or on expiry day itself (too late -- the whole point of T-1 is
to act the day BEFORE expiry, not on it).

NSE_HOLIDAYS starts EMPTY. This is a deliberate, safe default: with no
holidays configured, is_trading_day()/previous_trading_day() degrade to
pure weekend-skipping, which is exactly the old calendar-subtraction
behavior for every week that has no holiday in it -- zero behavior change
until real dates are added. Holiday dates are externally-verifiable facts
(NSE's own published circular), not something to guess at -- populate
this set from the real official NSE trading-holiday list for each year,
never fabricated.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Set

# Populate with real NSE/BSE equity-derivatives trading holidays (not bank
# holidays, not partial-session days) as date(YYYY, M, D) entries. Verify
# against NSE's own official circular each year before adding.
NSE_HOLIDAYS: Set[date] = set()


def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in NSE_HOLIDAYS


def previous_trading_day(d: date, max_step_back: int = 14) -> date:
    """The most recent trading day strictly BEFORE d. Steps back day-by-day
    (not just -1) so a holiday adjacent to a weekend (e.g. a Monday
    holiday right after a weekend) correctly lands on the Friday before,
    not the Sunday. max_step_back is a sanity ceiling against a
    misconfigured/huge NSE_HOLIDAYS set looping indefinitely."""
    cur = d
    for _ in range(max_step_back):
        cur = cur - timedelta(days=1)
        if is_trading_day(cur):
            return cur
    return cur  # pragma: no cover -- only reachable with a broken holiday list
