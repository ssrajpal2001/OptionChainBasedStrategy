"""
strategies/relative_strength/detector.py -- pure-function Python port of the
"Relative Strength" Pine Script v6 indicator by bharatTrader (direct user
request, 2026-09-24). No backtest/validation pass -- ported directly per
spec and unit-tested against hand-computed values.

Pine core formula (unchanged, this is the whole indicator's signal):
    res = baseSymbol / baseSymbol[length] / (comparativeSymbol / comparativeSymbol[length]) - 1

i.e. the base symbol's % return over `length` bars, divided by the
comparative symbol's % return over the SAME `length` bars, minus 1 --
positive means the base outperformed the comparative over that window,
negative means it underperformed.

Two secondary Pine signals are also ported, since the user's own spec asks
for "whichever is best performing" -- a single point-in-time RS value alone
doesn't capture whether strength is improving or fading:
- "RS Trend" (`angle0`/`zeroLineColor` in Pine): sign of `res - res[base bars
  ago]` -- is the RS value itself climbing or dropping.
- "RS Mean" (`sma_res`/`ma_rising`/`ma_falling` in Pine): a smoothed
  (SMA'd) version of the RS series, classified rising/falling the same way
  Pine's `ta.rising`/`ta.falling` do (strictly monotonic over the lookback).

The Pine script's "Price Confirmation" bubbles (pos_div/neg_div -- base
symbol's own price vs its own SMA) are deliberately NOT ported here: that
signal confirms the BASE symbol's own price trend in isolation, unrelated to
relative strength against a comparative symbol, and the user's spec (rank
sectors vs NIFTY, then stocks vs their sector) never asks for it.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence


def align_closes(base_candles: Sequence[dict], comp_candles: Sequence[dict]
                  ) -> tuple[List[float], List[float]]:
    """Two real candle lists (each a list of {'ts':..., 'close':...} dicts,
    any timestamp format usable as a dict key -- ISO string or datetime,
    as long as both lists use the SAME format) are rarely bar-for-bar
    aligned in practice (a holiday on one exchange calendar, a newly-listed
    stock's shorter history, a late/missing candle) -- Pine's own
    request.security implicitly aligns two series onto the same chart's bar
    index, which this has no equivalent for outside a real charting engine.
    Aligns onto the SORTED INTERSECTION of timestamps present in both,
    keeping only bars where both a base and a comparative close exist, so
    compute_rs_series's own bar-for-bar `length`-back lookup is always
    comparing genuinely same-instant closes on both sides."""
    base_by_ts = {c["ts"]: float(c["close"]) for c in base_candles if c.get("close") is not None}
    comp_by_ts = {c["ts"]: float(c["close"]) for c in comp_candles if c.get("close") is not None}
    common = sorted(set(base_by_ts) & set(comp_by_ts))
    return [base_by_ts[ts] for ts in common], [comp_by_ts[ts] for ts in common]


def align_closes_with_ts(base_candles: Sequence[dict], comp_candles: Sequence[dict]
                          ) -> tuple[List, List[float], List[float]]:
    """Same alignment as align_closes, but also returns the common timestamp
    list -- needed by strategies/relative_strength/backtest.py to date-stamp
    entry/exit trades (align_closes itself stays 2-tuple for backward
    compat with its existing scan.py callers, which never needed
    timestamps)."""
    base_by_ts = {c["ts"]: float(c["close"]) for c in base_candles if c.get("close") is not None}
    comp_by_ts = {c["ts"]: float(c["close"]) for c in comp_candles if c.get("close") is not None}
    common = sorted(set(base_by_ts) & set(comp_by_ts))
    return common, [base_by_ts[ts] for ts in common], [comp_by_ts[ts] for ts in common]


def sma(values: Sequence[float], length: int) -> List[Optional[float]]:
    """Simple moving average, index-aligned with `values` -- None for any
    index without `length` full values behind it (matches Pine's own
    ta.sma, which only starts printing once enough history exists)."""
    out: List[Optional[float]] = [None] * len(values)
    if length <= 0:
        return out
    running = 0.0
    for i, v in enumerate(values):
        running += v
        if i >= length:
            running -= values[i - length]
        if i >= length - 1:
            out[i] = running / length
    return out


def compute_rs_series(base_closes: Sequence[float], comp_closes: Sequence[float],
                       length: int) -> List[Optional[float]]:
    """Full RS series, index-aligned with base_closes/comp_closes (both must
    be the SAME length, same bar-for-bar alignment -- caller's
    responsibility, same as Pine's own request.security alignment). None for
    any index without `length` bars of history behind it, or where either
    denominator would be zero/missing (a real data gap -- never silently
    treated as 0, matching this codebase's established degrade-safely
    convention elsewhere)."""
    n = len(base_closes)
    out: List[Optional[float]] = [None] * n
    if length <= 0 or len(comp_closes) != n:
        return out
    for i in range(length, n):
        b_now, b_then = base_closes[i], base_closes[i - length]
        c_now, c_then = comp_closes[i], comp_closes[i - length]
        if not b_then or not c_now or not c_then:
            continue
        out[i] = (b_now / b_then) / (c_now / c_then) - 1
    return out


def _rising(series: Sequence[Optional[float]], length: int) -> Optional[bool]:
    """Port of Pine's ta.rising(source, length): true iff the series has been
    strictly increasing for the last `length` steps. None if there isn't
    enough history or any value in the window is missing."""
    if len(series) < length + 1:
        return None
    window = series[-(length + 1):]
    if any(v is None for v in window):
        return None
    return all(window[i] > window[i - 1] for i in range(1, len(window)))


def _falling(series: Sequence[Optional[float]], length: int) -> Optional[bool]:
    if len(series) < length + 1:
        return None
    window = series[-(length + 1):]
    if any(v is None for v in window):
        return None
    return all(window[i] < window[i - 1] for i in range(1, len(window)))


@dataclass
class RSReading:
    symbol: str
    rs: float                       # current RS value (Pine's `res`, latest bar)
    rs_trend: Optional[str]         # "rising" | "falling" | "flat" | None (not enough history)
    rs_ma: Optional[float]          # SMA(res, ma_length) at the latest bar, if computable
    ma_trend: Optional[str]         # "rising" | "falling" | "flat" | None


def evaluate_relative_strength(
    symbol: str, base_closes: Sequence[float], comp_closes: Sequence[float],
    length: int = 123, trend_base: int = 5, ma_length: int = 50,
) -> Optional[RSReading]:
    """One symbol's full RS reading vs a comparative series, mirroring the
    Pine indicator's own default inputs (length=123, RS-trend base=5,
    RS-mean period=50). Returns None if there isn't enough history for even
    the core `res` value at the latest bar."""
    rs_series = compute_rs_series(base_closes, comp_closes, length)
    if not rs_series or rs_series[-1] is None:
        return None
    rs_now = rs_series[-1]

    rs_trend: Optional[str] = None
    if len(rs_series) > trend_base and rs_series[-1 - trend_base] is not None:
        y0 = rs_now - rs_series[-1 - trend_base]
        rs_trend = "rising" if y0 > 0 else ("falling" if y0 < 0 else "flat")

    # sma() needs a plain float list; None entries (pre-warm-up bars, before
    # `length` bars of history exist) would corrupt the running sum if
    # passed through as 0.0, so build it from only the bars where rs_series
    # actually has a value (Pine's ta.sma on `res` behaves the same way --
    # `res` itself is `na` pre-warm-up and ta.sma skips na inputs).
    _valid_rs = [v for v in rs_series if v is not None]
    ma_series = sma(_valid_rs, ma_length)
    rs_ma = ma_series[-1] if ma_series else None

    ma_trend: Optional[str] = None
    if ma_series and len(ma_series) >= 4 and all(v is not None for v in ma_series[-4:]):
        rising = _rising(ma_series, 3)
        falling = _falling(ma_series, 3)
        if rising:
            ma_trend = "rising"
        elif falling:
            ma_trend = "falling"
        else:
            ma_trend = "flat"

    return RSReading(
        symbol=symbol, rs=rs_now, rs_trend=rs_trend, rs_ma=rs_ma, ma_trend=ma_trend,
    )


def rank_by_relative_strength(readings: Sequence[RSReading]) -> List[RSReading]:
    """Best-performing first: primary sort key is the raw RS value (matches
    the Pine indicator's own fundamental green/red > 0 distinction), with
    ties (or near-ties) broken by whichever has a "rising" rs_trend over a
    "flat"/"falling" one -- a symbol that is CURRENTLY less strong but
    actively strengthening is preferred over one that is currently stronger
    but fading, when the raw RS gap between them is negligible."""
    _trend_rank = {"rising": 0, "flat": 1, "falling": 2, None: 1}
    return sorted(
        readings,
        key=lambda r: (-round(r.rs, 6), _trend_rank.get(r.rs_trend, 1)),
    )
