"""
strategies/core/candle_indicators.py -- shared candle-transform / oscillator
primitives: Heikin-Ashi and StochRSI.

2026-09-06: moved here (not duplicated) from scripts/oi_orb_30_3_1_target_
backtest.py (`to_heikin_ashi`) and scripts/oi_orb_stoch_rsi_backtest.py
(`compute_rsi_series`, `compute_stoch_rsi`) while porting OI-ORB Screener's
own validated exit mechanic (15-min HA-shape + StochRSI(9,9,3) inclusive
cross) from backtest-only research into the live engine
(strategies/oi_orb_screener/engine.py) -- same feedback_backtest_drive_
real_class discipline this codebase always follows, just applied in the
less-common direction (the backtest found it first; the live code now
imports the exact same functions rather than re-deriving them). The
backtest scripts themselves are updated to import from here too, so
neither copy can drift from the other.

Both functions are pure, whole-series batch computations (not incremental/
stateful) -- deliberately, matching the same "re-scan the whole growing
bar list on every new bar" pattern already used elsewhere in this codebase
(e.g. strategies/liquidity_trap/engine.py) to guarantee the live class can
never behaviorally drift from what was actually backtested. A trading day
only ever produces ~25 15-minute bars, so recomputing from scratch on
every new bar is cheap -- no incremental optimization needed.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from strategies.core.trap_zone_utils import Bar


def to_n_min_bars_dateaware(bars_1m: List[Bar], n: int) -> List[Bar]:
    """Date-aware sibling of to_n_min_bars, for multi-day bar series --
    to_n_min_bars buckets purely by (hour, floored-minute), which silently
    MERGES same-hour bars from different calendar days into one bucket for
    any series spanning more than a single session (confirmed real bug hit
    2026-09-08 while building the OI-ORB same-side trap's multi-day HTF
    zone detection, which genuinely needs several real trading days of
    history for coarse timeframes like Daily/4H to form real zones on).
    Buckets by (calendar date, minutes-since-midnight // n) instead, so
    every bucket stays within one real trading day."""
    buckets: Dict[tuple, list] = {}
    for b in bars_1m:
        mins = b.ts.hour * 60 + b.ts.minute
        key = (b.ts.date(), mins // n)
        buckets.setdefault(key, []).append(b)
    out: List[Bar] = []
    for key in sorted(buckets.keys()):
        g = sorted(buckets[key], key=lambda x: x.ts)
        out.append(Bar(ts=g[0].ts, open=g[0].open, high=max(x.high for x in g),
                        low=min(x.low for x in g), close=g[-1].close))
    return out


def to_daily_bars(bars_1m: List[Bar]) -> List[Bar]:
    """One real bar per real calendar/trading day -- for a genuine 'Daily'
    HTF option, distinct from to_n_min_bars_dateaware(bars, 24*60) which
    would still slice by minutes-since-midnight and not represent a true
    daily OHLC."""
    from typing import Dict as _Dict
    from datetime import date as _date
    by_day: _Dict[_date, list] = {}
    for b in bars_1m:
        by_day.setdefault(b.ts.date(), []).append(b)
    out: List[Bar] = []
    for day in sorted(by_day):
        g = sorted(by_day[day], key=lambda x: x.ts)
        out.append(Bar(ts=g[0].ts, open=g[0].open, high=max(x.high for x in g),
                        low=min(x.low for x in g), close=g[-1].close))
    return out


def to_n_min_bars(bars_1m: List[Bar], n: int) -> List[Bar]:
    """Non-date-aware (hour, floored-minute) bucketing -- correct for a
    single intraday session (every strategy this codebase resamples this
    way operates within one trading day), matching the exact bucketing the
    validated backtest itself used. Moved here 2026-09-06 from
    scripts/oi_orb_entry_mode_backtest.py alongside to_heikin_ashi/
    compute_stoch_rsi for the same no-drift-from-backtest reason."""
    buckets: Dict[tuple, list] = {}
    for b in bars_1m:
        floored = (b.ts.minute // n) * n
        key = (b.ts.hour, floored)
        buckets.setdefault(key, []).append(b)
    out: List[Bar] = []
    for key in sorted(buckets.keys()):
        g = sorted(buckets[key], key=lambda x: x.ts)
        out.append(Bar(ts=g[0].ts, open=g[0].open, high=max(x.high for x in g),
                        low=min(x.low for x in g), close=g[-1].close))
    return out


def to_heikin_ashi(bars_1m: List[Bar]) -> List[Bar]:
    """Classic Heikin-Ashi transform, computed on the FINEST available
    series (1-min) and resampled up from there -- converting an
    already-aggregated N-min bar would give a different (wrong) result
    than aggregating a continuous 1-min HA series. HA_close = avg OHLC;
    HA_open = avg(prev HA_open, prev HA_close), seeded from the real
    open/close on the very first bar; HA_high/low widen to include the
    real bar's own high/low."""
    out: List[Bar] = []
    prev_open = prev_close = None
    for b in bars_1m:
        ha_close = (b.open + b.high + b.low + b.close) / 4.0
        ha_open = (b.open + b.close) / 2.0 if prev_open is None else (prev_open + prev_close) / 2.0
        ha_high = max(b.high, ha_open, ha_close)
        ha_low = min(b.low, ha_open, ha_close)
        out.append(Bar(ts=b.ts, open=ha_open, high=ha_high, low=ha_low, close=ha_close))
        prev_open, prev_close = ha_open, ha_close
    return out


def compute_rsi_series(closes: List[float], period: int) -> List[Optional[float]]:
    """Expanding-window Wilder-style RSI -- None until `period` real
    gain/loss samples exist."""
    out: List[Optional[float]] = [None] * len(closes)
    gains, losses = [], []
    for i in range(1, len(closes)):
        chg = closes[i] - closes[i - 1]
        gains.append(max(chg, 0.0))
        losses.append(max(-chg, 0.0))
        if i < period:
            continue
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss == 0:
            out[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i] = 100.0 - 100.0 / (1.0 + rs)
    return out


def compute_stoch_rsi(closes: List[float], rsi_period: int, stoch_period: int, smooth: int):
    """Returns (k_series, d_series), both list[Optional[float]] aligned to
    `closes`. None wherever warm-up hasn't completed yet."""
    rsi = compute_rsi_series(closes, rsi_period)
    k: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        window = [r for r in rsi[max(0, i - stoch_period + 1):i + 1] if r is not None]
        if len(window) < stoch_period or rsi[i] is None:
            continue
        lo, hi = min(window), max(window)
        k[i] = 100.0 if hi == lo else (rsi[i] - lo) / (hi - lo) * 100.0
    d: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        window = [k[j] for j in range(max(0, i - smooth + 1), i + 1) if k[j] is not None]
        if len(window) < smooth:
            continue
        d[i] = sum(window) / len(window)
    return k, d


def ha_stoch_shape_exit_signal(
    ha_bar: Bar, k: Optional[float], d: Optional[float], side: str, inclusive: bool = True,
) -> bool:
    """The confirmed OI-ORB Screener exit condition (2026-09-05/06 backtest
    series, scripts/oi_orb_ha_stochrsi_exit_backtest.py):
    CALL: HA candle is "bearish type" (HA_high == HA_open, no upper wick at
    all) AND %D >= %K (inclusive) / %D > %K (strict).
    PUT, mirrored: HA_low == HA_open (no lower wick) AND %K >= %D / %K > %D.

    The HA_high==HA_open / HA_low==HA_open checks are EXACT float equality,
    not a fragile rounding coincidence -- by construction, to_heikin_ashi's
    `ha_high = max(b.high, ha_open, ha_close)` returns the ha_open value
    completely unchanged (not computed via subtraction) whenever ha_open is
    the winning branch, so `==` is a correct, safe check here."""
    if k is None or d is None:
        return False
    if side == "CALL":
        cross = (d >= k) if inclusive else (d > k)
        return (ha_bar.high == ha_bar.open) and cross
    cross = (k >= d) if inclusive else (k > d)
    return (ha_bar.low == ha_bar.open) and cross
