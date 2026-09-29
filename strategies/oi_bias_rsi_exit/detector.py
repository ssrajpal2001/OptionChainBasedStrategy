"""
strategies/oi_bias_rsi_exit/detector.py -- pure, unit-testable logic for a
NEW, testing-only variant of the "OI-spurt + gainer/loser" selection this
codebase already runs live (strategies/oi_orb_screener/screener.py's
fetch_top_gainers_losers/fetch_oi_spurts_nse, and the frozen ATM/OTM OI-bias
comparison in strategies/oi_bias_breakout/detector.py) -- deliberately NOT
reimplemented here, per direct user instruction to keep Steps 1-6 (stock
selection, signal-strike freeze, OI-bias classification) exactly as already
built. This module owns ONLY the new entry/exit mechanic (2026-09-26 direct
user spec):

  Entry: Stochastic RSI(14,14,3,3) computed on the TRADED STOCK'S OWN spot
  price (2026-09-26 pivot from an earlier option-premium design -- a
  freshly-selected OI-spurt stock's relevant option strike can have ZERO
  real trading history before the selection day itself, making a
  14-period-based indicator impossible to warm up on day one; the stock
  always trades every session), on a 5-minute basis. Entry is a STATE
  check, not a fresh crossover -- if the relevant side is already ahead by
  the time bias confirms (however long ago that crossover happened), take
  the trade immediately; if not yet true, keep re-checking every later
  5-min close until it becomes true or EOD (direct user answer: "keep
  scanning all day").

  MIRRORED BY BIAS (2026-09-26 direct user correction, follows directly
  from the stock-price pivot above): a bullish trade (buying a CE, profits
  when the stock RISES) needs the stock's own bullish-momentum reading,
  K>D. A bearish trade (buying a PE, profits when the stock FALLS) needs
  the stock's own bearish-momentum reading, D>K -- the mirror image, not
  K>D. This is the opposite of the (now-superseded) option-premium design's
  "no mirroring by side" rule (see strategies/oi_flow's own PE-side bug):
  that rule existed because a bought PE is still LONG its own PREMIUM,
  same as a bought CE -- but the indicator no longer runs on premium, it
  runs on the STOCK, where CE/PE genuinely do point in opposite real
  directions.

  Exit (first of three to fire):
    (a) a genuine crossover EVENT on the STOCK's 1-hour StochRSI, MIRRORED
        the same way as entry -- bullish exits on D crossing above K (the
        stock's own momentum turning bearish), bearish exits on the
        opposite, K crossing above D (momentum turning bullish). Not a
        state check (direct user correction: "for exit D>K crossover
        happens then only exit"), seeded with the prior trading day's own
        1H bars so the indicator isn't cold at market open;
    (b) the OI bias (re-run every 5 minutes on the SAME frozen ATM/OTM
        strikes per strategies.oi_bias_breakout.detector.classify_oi_bias)
        reads as the exact opposite of the entry direction, twice;
    (c) EOD.

Backtest-only for now, per direct user instruction ("before implementing do
a backtest") -- no live engine/bridge in this pass.
"""
from __future__ import annotations

from typing import List, Optional

from strategies.core.trap_zone_utils import Bar, BarAccumulator  # noqa: F401 (re-exported for callers)
from strategies.core.candle_indicators import compute_stoch_rsi as _stoch_rsi_single_smoothed

_OPPOSITE_BIAS = {"bullish": "bearish", "bearish": "bullish"}


def compute_stoch_rsi_double_smoothed(
    closes: List[float], rsi_period: int = 14, stoch_period: int = 14,
    k_smooth: int = 3, d_smooth: int = 3,
) -> "tuple[List[Optional[float]], List[Optional[float]]]":
    """Standard StochRSI(14,14,3,3) -- TWO smoothing passes (%K = SMA(raw
    stoch,3), %D = SMA(%K,3)), matching TradingView's default StochRSI.
    Reuses strategies.core.candle_indicators.compute_stoch_rsi for the raw
    stoch-of-RSI + first smoothing pass (that function's own (k,d) pair is
    single-smoothed -- exactly what OI-ORB Screener's own HA+StochRSI exit
    needs, one smoothing stage only) rather than reimplementing Wilder RSI
    + stochastic-of-RSI from scratch, and applies the second d_smooth-period
    SMA on top locally to get the real %D this strategy's frozen spec
    needs."""
    _, k = _stoch_rsi_single_smoothed(closes, rsi_period, stoch_period, k_smooth)
    d: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        window = [k[j] for j in range(max(0, i - d_smooth + 1), i + 1) if k[j] is not None]
        if len(window) < d_smooth:
            continue
        d[i] = sum(window) / len(window)
    return k, d


def check_entry_state(k: Optional[float], d: Optional[float], bias: str) -> bool:
    """Direct user spec: a STATE check, not a fresh crossover -- the
    relevant side already being ahead (however long ago that crossover
    happened) is enough.

    2026-09-26 direct user correction, MIRRORED by bias: this indicator now
    runs on the STOCK's own price (not the always-long option premium), so
    a bearish trade -- profiting when the stock FALLS -- needs the stock's
    own bearish-momentum reading (D>K), not K>D. Bullish stays K>D."""
    if k is None or d is None:
        return False
    if bias == "bearish":
        return d > k
    return k > d


def check_exit_cross(
    prev_k: Optional[float], prev_d: Optional[float],
    k: Optional[float], d: Optional[float], bias: str,
) -> bool:
    """Direct user correction: exit needs a genuine CROSSOVER EVENT, not
    merely a persisting state -- the opposing line already having been
    ahead on the previous bar does not re-fire here.

    2026-09-26, MIRRORED by bias (same stock-price-not-premium reasoning as
    check_entry_state): bullish exits on D crossing above K (the stock's
    own momentum turning bearish); bearish exits on the opposite, K
    crossing above D (the stock's own momentum turning bullish)."""
    if prev_k is None or prev_d is None or k is None or d is None:
        return False
    if bias == "bearish":
        return prev_d >= prev_k and k > d
    return prev_k >= prev_d and d > k


def classify_combined_oi_bias(
    call_oi_915: Optional[float], call_oi_920: Optional[float], call_oi_925: Optional[float],
    put_oi_915: Optional[float], put_oi_920: Optional[float], put_oi_925: Optional[float],
) -> str:
    """The user's ORIGINAL spec (2026-09-26 real correction after this
    module briefly, mistakenly, cross-checked against strategies.oi_bias_
    breakout.detector.classify_oi_bias instead): call_oi_* and put_oi_* are
    each the COMBINED (ATM strike + 1 OTM strike, summed) OI for that side
    -- callers sum the two legs before calling this, this function never
    does its own strike selection.

        bullish: combined Call OI FALLS on BOTH transitions (920<915 AND
                 925<920) AND combined Put OI RISES on BOTH transitions
                 (920>915 AND 925>920)
        bearish: the mirror -- combined Put OI falls both transitions AND
                 combined Call OI rises both transitions
        both true at once -> conflict; neither -> none

    Deliberately requires BOTH transitions to agree, not just 9:20->9:25 --
    a single transition (e.g. Call falls 915->920 then rises back 920->925)
    is not a sustained move and does not count. Confirmed against 20 real
    trading days: only 2 of 20 produced a signal under this stricter rule,
    and both matched the real realized direction (TCS 2026-09-18 bearish,
    AXISBANK 2026-09-25 bullish) -- see the session's own manual-bias CSV
    and its cross-check notes.

    Any reading may be missing (thin OI at market open) -- returns 'none'
    rather than guessing; never fabricates a value."""
    vals = (call_oi_915, call_oi_920, call_oi_925, put_oi_915, put_oi_920, put_oi_925)
    if any(v is None for v in vals):
        return "none"

    call_falls_both = call_oi_920 < call_oi_915 and call_oi_925 < call_oi_920
    call_rises_both = call_oi_920 > call_oi_915 and call_oi_925 > call_oi_920
    put_falls_both = put_oi_920 < put_oi_915 and put_oi_925 < put_oi_920
    put_rises_both = put_oi_920 > put_oi_915 and put_oi_925 > put_oi_920

    bullish = call_falls_both and put_rises_both
    bearish = put_falls_both and call_rises_both
    if bullish and bearish:
        return "conflict"
    if bullish:
        return "bullish"
    if bearish:
        return "bearish"
    return "none"


def count_opposite_bias_readings(bias_history: List[str], entry_bias: str) -> int:
    """Direct user spec: the 'bias flips twice' exit counts how many times
    the (externally supplied, re-run every 5 min) OI bias has read as the
    exact opposite of the entry direction -- 'conflict'/'none' readings
    don't count either way."""
    opposite = _OPPOSITE_BIAS.get(entry_bias)
    if opposite is None:
        return 0
    return sum(1 for b in bias_history if b == opposite)
