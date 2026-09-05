"""
scripts/oi_orb_atr_chandelier_backtest.py -- 2026-09-05, direct user spec:
after the flat-% TSL grid and the multi-day wall-cascade both showed real
limits (flat-% clips every trade equally regardless of the stock's own
volatility; the wall-cascade only has ONE evolving rung per side and misses
stocks like BOSCHLTD with no relevant established level), researched actual
industry-standard SL/TSL practice (see conversation for sources) and picked
the two most commonly cited, complementary techniques:

  Initial SL = ATR(14)-multiple from entry (side-aware), replacing the
  fixed ORB-extreme SL. Scales the stop to the STOCK'S OWN volatility --
  directly fixes both real failures found earlier: FORCEMOT/BSE's ORB SL
  was too tight for their normal opening noise (whipsawed in minutes);
  BOSCHLTD's ORB SL was too wide (2.6% away) to ever catch its reversal.

  Trailing SL = Chandelier Exit (Charles Le Beau's classic formula):
    CALL: HighestHigh(since entry) - ATR(14)*tsl_mult
    PUT:  LowestLow(since entry)  + ATR(14)*tsl_mult
  Recalculated every bar off the CURRENT ATR (not fixed at entry) -- widens
  automatically in high volatility, tightens in low volatility, which is
  exactly the property the flat single-% grid was missing (a fixed 0.25%
  clips a calm Rs45 stock instantly while doing nothing for an Rs18,000
  stock like FORCEMOT).

ATR computed on 3-minute bars (period=14) -- matches the commonly-cited
intraday-scalping ATR timeframe from the research pass, not daily ATR
(that's for swing trades). Warm-up: expanding-window ATR (uses however many
TR samples exist so far) until 14 bars have accumulated, same warm-up
idiom this codebase already uses elsewhere (PoolIndicatorEngine, RSI/ROC
seeding) rather than blocking entries with no ATR yet.

Entry logic (variant 3, frozen, unchanged): historical-immediate at 09:25
if the VWAP arm+touch-back already resolved in the 09:15-09:25 window,
else live scan with no-re-entry + breach-cancel -- see
oi_orb_entry_mode_backtest.py's own docstrings for the full spec.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_atr_chandelier_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date
from functools import partial

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from scripts.oi_orb_entry_mode_backtest import (
    ROWS, SIDE, Trade, compute_orb, resolve_eq_key, simulate_fixed_sl_exit,
    run_vwap_retest_immediate_if_historically_fulfilled,
    to_bars, to_n_min_bars, volume_by_ts,
)

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
ATR_PERIOD = 14
ATR_TF_MIN = 3


def compute_atr_series(bars_tf, period=ATR_PERIOD):
    """Expanding-window True Range / ATR -- atrs[i] is the ATR AS OF
    bars_tf[i]'s own close (i.e. known the instant that bar closes, safe
    for walk-forward use). First bar's TR = high-low (no prior close)."""
    trs, atrs = [], []
    prev_close = None
    for b in bars_tf:
        tr = (b.high - b.low) if prev_close is None else \
             max(b.high - b.low, abs(b.high - prev_close), abs(b.low - prev_close))
        trs.append(tr)
        window = trs[-period:]
        atrs.append(sum(window) / len(window))
        prev_close = b.close
    return atrs


def _atr_as_of(bars_tf, atrs, ts):
    """Most recent ATR value known at or before `ts` (walk-forward safe)."""
    val = None
    for b, a in zip(bars_tf, atrs):
        if b.ts <= ts:
            val = a
        else:
            break
    return val


def _median(vals):
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def compute_atr_series_median(bars_tf, period=ATR_PERIOD):
    """2026-09-06, direct user follow-up (BSE regression): a plain-mean ATR
    lets ONE abnormally wide true-range bar (BSE's real ~10:30-10:40 spike)
    blow out the whole rolling average right at the moment the stop is
    tested -- widening the SL exactly when it should stay tight. Median of
    the last `period` true-range samples is far more resistant to that one
    outlier bar than a mean."""
    trs, atrs = [], []
    prev_close = None
    for b in bars_tf:
        tr = (b.high - b.low) if prev_close is None else \
             max(b.high - b.low, abs(b.high - prev_close), abs(b.low - prev_close))
        trs.append(tr)
        atrs.append(_median(trs[-period:]))
        prev_close = b.close
    return atrs


def compute_atr_series_capped(bars_tf, period=ATR_PERIOD, cap_mult=1.3):
    """2026-09-06: alternative de-spike approach -- keep the normal MEAN
    ATR shape (still reacts to genuine broad volatility increases, unlike
    the median version which is slower to widen for a real regime change),
    but winsorize each individual true-range sample at cap_mult x the
    running median BEFORE it enters the mean -- one freak bar can only
    contribute up to the cap, not its full raw magnitude."""
    trs, capped_trs, atrs = [], [], []
    prev_close = None
    for b in bars_tf:
        tr = (b.high - b.low) if prev_close is None else \
             max(b.high - b.low, abs(b.high - prev_close), abs(b.low - prev_close))
        trs.append(tr)
        window = trs[-period:]
        med = _median(window)
        capped_tr = min(tr, cap_mult * med) if med > 0 else tr
        capped_trs.append(capped_tr)
        cwindow = capped_trs[-period:]
        atrs.append(sum(cwindow) / len(cwindow))
        prev_close = b.close
    return atrs


def simulate_atr_chandelier_exit(entry_ts, entry_price, side, ref_h, ref_l, bars_1m,
                                  sl_mult=1.5, tsl_mult=2.5, atr_tf_min=ATR_TF_MIN):
    bars_tf = to_n_min_bars(bars_1m, atr_tf_min)
    atrs = compute_atr_series(bars_tf, ATR_PERIOD)

    entry_atr = _atr_as_of(bars_tf, atrs, entry_ts)
    if not entry_atr or entry_atr <= 0:
        entry_atr = max(ref_h - ref_l, 0.01)   # degenerate warm-up fallback: ORB range as a rough proxy

    sl_orig = entry_price - sl_mult * entry_atr if side == "CALL" else entry_price + sl_mult * entry_atr
    stop_level = sl_orig
    extreme = entry_price
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]

    for b in post_entry:
        cur_atr = _atr_as_of(bars_tf, atrs, b.ts) or entry_atr
        if side == "CALL":
            extreme = max(extreme, b.high)
            trail = extreme - tsl_mult * cur_atr
            stop_level = max(stop_level, trail)
            if b.low <= stop_level:
                reason = "chandelier_tsl_hit" if stop_level > sl_orig else "atr_sl_hit"
                return b.ts, stop_level, reason
        else:
            extreme = min(extreme, b.low)
            trail = extreme + tsl_mult * cur_atr
            stop_level = min(stop_level, trail)
            if b.high >= stop_level:
                reason = "chandelier_tsl_hit" if stop_level < sl_orig else "atr_sl_hit"
                return b.ts, stop_level, reason

    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def compute_supertrend_bands(bars_tf, atr_period=10, mult=3.0):
    """Classic Supertrend final_upper/final_lower ratchet (Olivier Seban's
    original formula, the version every Indian charting platform ships).
    Used here ONLY as a directional trailing stop (final_lower for a CALL,
    final_upper for a PUT) -- our position direction is already fixed by
    the VWAP-retest entry, so the flip/trend-switch half of the indicator
    (which exists to signal reversals) is irrelevant; the ratchet-only
    band is exactly a Chandelier Exit with a different band-center (mid
    HL/2 vs pure High/Low) and a slightly different ratchet condition."""
    atrs = compute_atr_series(bars_tf, atr_period)
    final_upper, final_lower = [], []
    for i, b in enumerate(bars_tf):
        atr = atrs[i]
        mid = (b.high + b.low) / 2.0
        basic_upper = mid + mult * atr
        basic_lower = mid - mult * atr
        if i == 0:
            final_upper.append(basic_upper)
            final_lower.append(basic_lower)
            continue
        prev_close = bars_tf[i - 1].close
        fu = basic_upper if (basic_upper < final_upper[-1] or prev_close > final_upper[-1]) else final_upper[-1]
        fl = basic_lower if (basic_lower > final_lower[-1] or prev_close < final_lower[-1]) else final_lower[-1]
        final_upper.append(fu)
        final_lower.append(fl)
    return final_upper, final_lower


def simulate_supertrend_exit(entry_ts, entry_price, side, ref_h, ref_l, bars_1m,
                              atr_period=10, mult=3.0, tf_min=ATR_TF_MIN):
    bars_tf = to_n_min_bars(bars_1m, tf_min)
    final_upper, final_lower = compute_supertrend_bands(bars_tf, atr_period, mult)
    band_by_ts = {b.ts: (fu, fl) for b, fu, fl in zip(bars_tf, final_upper, final_lower)}

    def band_as_of(ts):
        val = None
        for b in bars_tf:
            if b.ts <= ts:
                val = band_by_ts[b.ts]
            else:
                break
        return val

    entry_band = band_as_of(entry_ts)
    atrs0 = compute_atr_series(bars_tf, atr_period)
    entry_atr = _atr_as_of(bars_tf, atrs0, entry_ts) or max(ref_h - ref_l, 0.01)
    sl_orig = entry_price - 1.5 * entry_atr if side == "CALL" else entry_price + 1.5 * entry_atr
    stop_level = sl_orig
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]

    for b in post_entry:
        band = band_as_of(b.ts) or entry_band
        if band is None:
            breach = (b.low <= stop_level) if side == "CALL" else (b.high >= stop_level)
            if breach:
                return b.ts, stop_level, "atr_sl_hit"
            continue
        fu, fl = band
        if side == "CALL":
            stop_level = max(stop_level, fl)
            if b.low <= stop_level:
                reason = "supertrend_tsl_hit" if stop_level > sl_orig else "atr_sl_hit"
                return b.ts, stop_level, reason
        else:
            stop_level = min(stop_level, fu)
            if b.high >= stop_level:
                reason = "supertrend_tsl_hit" if stop_level < sl_orig else "atr_sl_hit"
                return b.ts, stop_level, reason

    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def simulate_rr_target_exit(entry_ts, entry_price, side, ref_h, ref_l, bars_1m,
                             sl_mult=1.5, rr_multiple=2.0, atr_tf_min=ATR_TF_MIN):
    """Fixed R:R -- ATR-based initial SL (same sizing as the ATR/Chandelier
    variant, NEVER trails here -- this variant isolates the R:R idea on its
    own), fixed target at rr_multiple x the entry-to-SL distance. Day
    trading industry convention is 1:1.5-1:2 (vs swing trading's 1:2-1:3),
    per the research pass. Whichever level is touched first wins; on the
    rare same-bar double-touch, SL is checked first (conservative,
    standard backtest convention)."""
    bars_tf = to_n_min_bars(bars_1m, atr_tf_min)
    atrs = compute_atr_series(bars_tf, ATR_PERIOD)
    entry_atr = _atr_as_of(bars_tf, atrs, entry_ts)
    if not entry_atr or entry_atr <= 0:
        entry_atr = max(ref_h - ref_l, 0.01)

    risk = sl_mult * entry_atr
    sl_level = entry_price - risk if side == "CALL" else entry_price + risk
    target = entry_price + rr_multiple * risk if side == "CALL" else entry_price - rr_multiple * risk
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]

    for b in post_entry:
        if side == "CALL":
            if b.low <= sl_level:
                return b.ts, sl_level, "atr_sl_hit"
            if b.high >= target:
                return b.ts, target, "rr_target_hit"
        else:
            if b.high >= sl_level:
                return b.ts, sl_level, "atr_sl_hit"
            if b.low <= target:
                return b.ts, target, "rr_target_hit"

    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def simulate_rr_ladder_exit(entry_ts, entry_price, side, ref_h, ref_l, bars_1m,
                             sl_mult=1.5, step_r=2.0, atr_tf_min=ATR_TF_MIN, atr_fn=compute_atr_series):
    """2026-09-05, direct user follow-up to the fixed R:R result: "if 1:2
    reaches we jump sl to cost and from then 1:2 position again" -- an
    R-multiple staircase instead of one fixed target. Risk = sl_mult x
    ATR(14), fixed at entry (same sizing as simulate_rr_target_exit).
    Targets sit at step_r, 2*step_r, 3*step_r, ... R away from entry. Each
    time a target is touched, the stop locks to the PREVIOUS target level
    (the first touch locks to breakeven = entry price, "jump sl to cost"),
    and the next target further out becomes active -- lets a big winner
    like MARUTI/POLYCAB keep running leg after leg instead of being capped
    at the first 2R, while a reversal after any leg still exits at the
    level that leg already banked (this is what protects BOSCHLTD-style
    reversals AFTER the first target, not just before it).

    atr_fn: 2026-09-06 -- pluggable ATR calculator (compute_atr_series /
    compute_atr_series_median / compute_atr_series_capped), so the SAME
    ladder logic can be tested against different de-spike strategies
    without duplicating the whole function (BSE regression follow-up:
    plain-mean ATR let one wide spike bar blow out the stop right when it
    should've stayed tight)."""
    bars_tf = to_n_min_bars(bars_1m, atr_tf_min)
    atrs = atr_fn(bars_tf, ATR_PERIOD)
    entry_atr = _atr_as_of(bars_tf, atrs, entry_ts)
    if not entry_atr or entry_atr <= 0:
        entry_atr = max(ref_h - ref_l, 0.01)

    risk = sl_mult * entry_atr
    stop_level = entry_price - risk if side == "CALL" else entry_price + risk
    rung = 1   # next target multiple to reach
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]

    def target_for(n):
        return entry_price + n * step_r * risk if side == "CALL" else entry_price - n * step_r * risk

    def locked_level_for(n):
        # level banked once rung n is reached: n=1 -> breakeven, n=2 -> target_for(1), etc.
        return entry_price if n == 1 else target_for(n - 1)

    for b in post_entry:
        favorable_extreme = b.high if side == "CALL" else b.low
        advanced = False
        while (favorable_extreme >= target_for(rung)) if side == "CALL" else (favorable_extreme <= target_for(rung)):
            stop_level = locked_level_for(rung)
            rung += 1
            advanced = True
        breach = (b.low <= stop_level) if side == "CALL" else (b.high >= stop_level)
        if breach:
            reason = "rr_ladder_lock" if rung > 1 else "atr_sl_hit"
            return b.ts, stop_level, reason
        if advanced:
            continue

    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def simulate_rr_ladder_exit_v2(entry_ts, entry_price, side, ref_h, ref_l, bars_1m,
                                sl_mult=2.0, step_r=1.5, atr_tf_min=ATR_TF_MIN,
                                atr_fn=None, close_confirm=False, close_confirm_tf=None,
                                risk_pct_floor=None, risk_pct_cap=None):
    """2026-09-06, final round: same R-multiple staircase as
    simulate_rr_ladder_exit, plus two independently-tested refinements from
    the expert-panel discussion:

    close_confirm: two independent experts (risk-sizing quant + price-action
    trader) converged unprompted on the same fix for "stopped out by a wick
    right before the real move" -- the ATR distance is calibrated on 3-min
    bar CLOSES, but the stop was being checked against every intrabar wick
    (bar low/high), a timeframe mismatch. When True, the stop only fires on
    a bar CLOSE beyond the level, not a touch -- catches ADANIENT-style
    whipsaws (a brief wick breach that closes back on-side) without
    widening risk at all.

    risk_pct_floor/risk_pct_cap: direct user follow-up -- "depending on
    price of the stock we can make changes to the ATR and other TSL logics,
    we cannot have generic for all stocks, it should depend on what is
    stock current price." Raw ATR points already scale with price to a
    degree, but the RATIO of ATR to price varies by stock/price-tier in a
    way a flat point-multiplier doesn't correct for (a sub-point ATR on a
    ~Rs46 stock vs a 50+ point ATR on a ~Rs17,600 stock aren't necessarily
    equivalent risk). Clamping the resulting risk distance to a MINIMUM and
    MAXIMUM % of entry price is a continuous, no-extra-parameters-per-stock
    normalization (not per-price-tier buckets, which would overfit this
    already-small 42-trade sample per the quant-skeptic's explicit
    warning) -- it only engages for stocks whose raw ATR-implied risk falls
    outside a sane %-of-price band, leaving everything else untouched."""
    if atr_fn is None:
        atr_fn = partial(compute_atr_series_capped, cap_mult=1.5)
    bars_tf = to_n_min_bars(bars_1m, atr_tf_min)
    atrs = atr_fn(bars_tf, ATR_PERIOD)
    entry_atr = _atr_as_of(bars_tf, atrs, entry_ts)
    if not entry_atr or entry_atr <= 0:
        entry_atr = max(ref_h - ref_l, 0.01)

    risk = sl_mult * entry_atr
    if risk_pct_floor is not None:
        risk = max(risk, risk_pct_floor * entry_price)
    if risk_pct_cap is not None:
        risk = min(risk, risk_pct_cap * entry_price)

    stop_level = entry_price - risk if side == "CALL" else entry_price + risk
    rung = 1
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]

    def target_for(n):
        return entry_price + n * step_r * risk if side == "CALL" else entry_price - n * step_r * risk

    def locked_level_for(n):
        return entry_price if n == 1 else target_for(n - 1)

    # 2026-09-06, real BSE/ATHERENERG follow-up: close_confirm was checking
    # every 1-MIN bar's own close -- a timeframe mismatch against the ATR's
    # own 3-min calibration, and much easier to trigger than a genuine 3-min
    # close-confirm (both BSE and ATHERENERG's actual breaching bar's 1-min
    # close confirmed beyond the SL even though the CONTAINING 3-min bar's
    # close did not). close_confirm_tf, when set, only evaluates the breach
    # at the close of each close_confirm_tf-minute bucket, using that
    # bucket's own close -- intervening 1-min bars still update the
    # favorable extreme (so a same-bucket target touch is never missed) but
    # never trigger a stop on their own.
    conf_close_by_end_min = None
    if close_confirm and close_confirm_tf:
        bars_conf = to_n_min_bars(post_entry, close_confirm_tf)
        conf_close_by_end_min = {}
        for cb in bars_conf:
            bucket_start_min = cb.ts.hour * 60 + cb.ts.minute
            bucket_end_min = (bucket_start_min // close_confirm_tf) * close_confirm_tf + close_confirm_tf - 1
            conf_close_by_end_min[bucket_end_min] = cb.close

    for b in post_entry:
        favorable_extreme = b.high if side == "CALL" else b.low
        advanced = False
        while (favorable_extreme >= target_for(rung)) if side == "CALL" else (favorable_extreme <= target_for(rung)):
            stop_level = locked_level_for(rung)
            rung += 1
            advanced = True

        if close_confirm and close_confirm_tf:
            b_min = b.ts.hour * 60 + b.ts.minute
            if b_min not in conf_close_by_end_min:
                continue   # not yet at this 3-min bucket's own close
            check_val = conf_close_by_end_min[b_min]
        elif close_confirm:
            check_val = b.close
        else:
            check_val = b.low if side == "CALL" else b.high
        breach = (check_val <= stop_level) if side == "CALL" else (check_val >= stop_level)
        if breach:
            reason = "rr_ladder_lock" if rung > 1 else "atr_sl_hit"
            return b.ts, stop_level, reason
        if advanced:
            continue

    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def simulate_rr_then_chandelier_exit(entry_ts, entry_price, side, ref_h, ref_l, bars_1m,
                                      sl_mult=1.5, first_r=2.0, tsl_mult=3.0, atr_tf_min=ATR_TF_MIN):
    """Hybrid, 2026-09-05: combines the two best-performing mechanics found
    so far. Stage 1 = fixed R:R (same sizing as simulate_rr_target_exit):
    ATR-based initial SL, first target at first_r x risk. Stage 2 (once
    that first target is touched): stop locks to breakeven, and PROTECTION
    SWITCHES from further fixed R rungs to a live Chandelier Exit
    (HighestHigh/LowestLow-since-entry -/+ ATR(14)*tsl_mult) -- lets a
    strong continuation trail adaptively (tightens in calm moves, widens in
    volatile ones) rather than waiting for another fixed-distance rung,
    while a reversal right after the first target still can't lose money
    (floor = breakeven, never loosens below/above it)."""
    bars_tf = to_n_min_bars(bars_1m, atr_tf_min)
    atrs = compute_atr_series(bars_tf, ATR_PERIOD)
    entry_atr = _atr_as_of(bars_tf, atrs, entry_ts)
    if not entry_atr or entry_atr <= 0:
        entry_atr = max(ref_h - ref_l, 0.01)

    risk = sl_mult * entry_atr
    sl_orig = entry_price - risk if side == "CALL" else entry_price + risk
    target1 = entry_price + first_r * risk if side == "CALL" else entry_price - first_r * risk
    stop_level = sl_orig
    stage2 = False
    extreme = entry_price
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]

    for b in post_entry:
        cur_atr = _atr_as_of(bars_tf, atrs, b.ts) or entry_atr
        if side == "CALL":
            extreme = max(extreme, b.high)
            if not stage2 and b.high >= target1:
                stage2 = True
                stop_level = entry_price
            if stage2:
                trail = extreme - tsl_mult * cur_atr
                stop_level = max(stop_level, trail)
            if b.low <= stop_level:
                reason = "chandelier_after_1r" if stage2 else "atr_sl_hit"
                return b.ts, stop_level, reason
        else:
            extreme = min(extreme, b.low)
            if not stage2 and b.low <= target1:
                stage2 = True
                stop_level = entry_price
            if stage2:
                trail = extreme + tsl_mult * cur_atr
                stop_level = min(stop_level, trail)
            if b.high >= stop_level:
                reason = "chandelier_after_1r" if stage2 else "atr_sl_hit"
                return b.ts, stop_level, reason

    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


async def fetch_all():
    cache = {}
    for trade_date, symbol, side_bias in ROWS:
        key = (trade_date, symbol)
        if key in cache:
            continue
        eq_key = resolve_eq_key(symbol)
        if eq_key is None:
            cache[key] = None
            continue
        d = date.fromisoformat(trade_date)
        rows = await fetch_upstox_range_1m(eq_key, TOKEN, d, d)
        if not rows:
            cache[key] = None
            continue
        bars_1m = to_bars(rows)
        vol_by_ts = volume_by_ts(rows)
        orb = compute_orb(bars_1m)
        if orb is None:
            cache[key] = None
            continue
        orb_h, orb_l = orb
        cache[key] = (bars_1m, vol_by_ts, orb_h, orb_l)
    print(f"Fetched {sum(1 for v in cache.values() if v)} usable rows.")
    return cache


def run_one(cache, exit_fn):
    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        vwap_trades = run_vwap_retest_immediate_if_historically_fulfilled(
            bars_1m, side, orb_h, orb_l, vol_by_ts, exit_fn=exit_fn)
        for (entry_ts, entry_price, exit_ts, exit_price, reason) in vwap_trades:
            trades.append(Trade(trade_date, symbol, side, "vwap_retest", entry_ts, entry_price,
                                 exit_ts, exit_price, reason))
    return trades


def summarize(label, trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    avg = (total / len(entered)) if entered else 0.0
    print(f"{label:>28}  entered={len(entered):2d}  win%={win_pct:5.1f}  PF={pf:6.2f}  "
          f"total={total:+9.2f}  avg/trade={avg:+7.2f}")
    return {"label": label, "trades": trades, "total": total, "pf": pf, "win_pct": win_pct, "entered": len(entered)}


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows once (cached for the whole grid)...")
    cache = await fetch_all()

    print("\n" + "=" * 110)
    print("ATR-SL + CHANDELIER / SUPERTREND / FIXED-R:R -- baseline vs all candidates")
    print("=" * 110)
    results = {}
    results["baseline"] = summarize("baseline(fixed SL/EOD)", run_one(cache, simulate_fixed_sl_exit))
    GRID = [(1.5, 2.0), (1.5, 2.5), (1.5, 3.0), (2.0, 2.5), (2.0, 3.0)]
    for sl_mult, tsl_mult in GRID:
        key = f"atr_sl={sl_mult}_tsl={tsl_mult}"
        exit_fn = partial(simulate_atr_chandelier_exit, sl_mult=sl_mult, tsl_mult=tsl_mult)
        results[key] = summarize(f"ATR SL={sl_mult}x / Chand={tsl_mult}x", run_one(cache, exit_fn))

    for label, atr_period, mult in [("classic(10/3)", 10, 3.0), ("intraday(7/2)", 7, 2.0)]:
        key = f"supertrend_{label}"
        exit_fn = partial(simulate_supertrend_exit, atr_period=atr_period, mult=mult)
        results[key] = summarize(f"Supertrend {label}", run_one(cache, exit_fn))

    for rr in [1.5, 2.0, 3.0]:
        key = f"rr_{rr}"
        exit_fn = partial(simulate_rr_target_exit, sl_mult=1.5, rr_multiple=rr)
        results[key] = summarize(f"Fixed R:R 1:{rr} (ATR SL=1.5x)", run_one(cache, exit_fn))

    print("\n" + "=" * 110)
    print("TIMEFRAME SWEEP -- Fixed R:R (SL=1.5x ATR) across ATR timeframe x R-multiple")
    print("=" * 110)
    tf_results = {}
    for tf in (1, 3, 5, 15):
        for rr in (1.5, 2.0, 2.5, 3.0):
            key = f"rr_tf{tf}_rr{rr}"
            exit_fn = partial(simulate_rr_target_exit, sl_mult=1.5, rr_multiple=rr, atr_tf_min=tf)
            tf_results[key] = summarize(f"R:R tf={tf}m rr=1:{rr}", run_one(cache, exit_fn))

    print("\n" + "=" * 110)
    print("TIMEFRAME SWEEP -- ATR-SL(1.5x)/Chandelier(3.0x) across ATR timeframe")
    print("=" * 110)
    for tf in (1, 5, 15):
        key = f"chand_tf{tf}"
        exit_fn = partial(simulate_atr_chandelier_exit, sl_mult=1.5, tsl_mult=3.0, atr_tf_min=tf)
        tf_results[key] = summarize(f"Chandelier tf={tf}m SL=1.5x/TSL=3.0x", run_one(cache, exit_fn))

    results.update(tf_results)
    best = max((v for k, v in results.items() if k != "baseline"), key=lambda r: r["total"])
    print(f"\nBest total-points among ALL candidates (incl. timeframe sweep): {best['label']} (total={best['total']:+.2f}, PF={best['pf']:.2f})")

    print("\n" + "=" * 110)
    print(f"FULL PER-TRADE DETAIL WITH TIMESTAMPS -- {best['label']}")
    print("=" * 110)
    print(f"{'Date':<12}{'Symbol':<13}{'Side':<5}{'Entry TS':<9}{'Entry Px':>10}  {'Exit TS':<9}{'Exit Px':>10}  {'Reason':<20}{'Points':>10}")
    for t in best["trades"]:
        if t.entry_price is None:
            print(f"{t.date:<12}{t.symbol:<13}{t.side:<5} NO ENTRY")
        else:
            print(f"{t.date:<12}{t.symbol:<13}{t.side:<5}{t.entry_ts.strftime('%H:%M'):<9}{t.entry_price:>10.2f}  "
                  f"{t.exit_ts.strftime('%H:%M'):<9}{t.exit_price:>10.2f}  {t.reason:<20}{t.points:>+10.2f}")

    print("\n" + "=" * 110)
    print("BOSCHLTD / FORCEMOT / BSE / EICHERMOT DETAIL ACROSS BASELINE VS BEST")
    print("=" * 110)
    watch = {"BOSCHLTD", "FORCEMOT", "BSE", "EICHERMOT", "HEROMOTOCO", "ATHERENERG"}
    for key in ("baseline",) + (next(k for k, v in results.items() if v is best),):
        print(f"\n-- {results[key]['label']} --")
        for t in results[key]["trades"]:
            if t.symbol in watch and t.entry_price is not None:
                print(f"  {t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
                      f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")


if __name__ == "__main__":
    asyncio.run(main())
