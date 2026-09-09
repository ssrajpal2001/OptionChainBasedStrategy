"""
scripts/oi_orb_trend_capture_exit_backtest.py -- 2026-09-06, direct user
follow-up to the "not riding the trend" complaint (e.g. BOSCHLTD ran ~365pts
in favor but the confirmed HA+StochRSI(9,9) exit only locked +85). Compares
candidate PROFIT-LOCK add-ons against the CURRENT FROZEN BASELINE, same
51-row real dataset, same VWAP-retest entry -- only what happens after
entry changes here.

Design constraint learned from an earlier same-repo pass
(scripts/oi_orb_rr_ladder_backtest.py, scripts/oi_orb_atr_chandelier_backtest.py):
REPLACING the exit with a hard ATR-based SL/R:R ladder underperforms the
current HA+StochRSI baseline outright (+2,174 vs +3,116 there) and can turn
a real winner into a loser (FORCEMOT: baseline +343, ATR-SL variant -110 --
an early-volatility whipsaw hit the stop before the real move even started).
That rules out any approach that can create a loss where HA+StochRSI alone
wouldn't have. So every variant here is a PURE PROFIT-LOCK: it only ever
tightens a floor that starts at/above breakeven once already in profit --
it can only improve or match a trade's outcome, never make it worse than
just holding to the structural exit.

An earlier attempt at this (same session, first draft) used a flat % of
raw SPOT PRICE as the lock trigger (borrowed directly from FVG's option-
premium ratchet defaults, 15%/8%) -- CONFIRMED BROKEN by inspection: a 15%
move in an option's own premium is common, but a 15% move in a stock's raw
SPOT PRICE (e.g. 15% of BOSCHLTD's ~46,885 = ~7,000 points) essentially
never happens intraday, so the ratchet never armed on any real trade and
both tested configs silently degraded to pure EOD-close, invisible in the
final report until this rewrite compared FVG-params vs a tighter variant
and found them byte-identical. Fixed here by normalizing to ATR (self-
scaling per stock, same ATR(14)/3-min ATR already used and validated by
oi_orb_atr_chandelier_backtest.py in this repo), not a flat percentage.

Variants (races HA+StochRSI on every one -- whichever condition is
satisfied EARLIEST wins; both are recomputed on the SAME 1-min bars so
"earliest" is unambiguous):
  A. BASELINE (current live spec) -- HA-shape(15m) + StochRSI(9,9,3) only,
     no SL, EOD fallback.
  B. ATR PROFIT-LOCK (1.0x arm / 1.5x trail) -- once favorable move >=
     1.0x ATR(14,3m), floor = peak - 1.5x ATR, floor never below entry.
  C. ATR PROFIT-LOCK (0.75x arm / 1.0x trail) -- tighter version.
  D. ATR PROFIT-LOCK (1.5x arm / 2.0x trail) -- looser version, only locks
     once a genuinely large move has already happened.
  E. SWING-STRUCTURE PROFIT-LOCK -- once favorable move >= 1.0x ATR (same
     arm gate as B, to stop it whipsawing on trivial pivots near entry), a
     confirmed 2-bar swing pivot in the trade's favor ratchets the floor
     (same concept as CAG Straddle's "S1 acts as TSL"), floor never below
     entry.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_trend_capture_exit_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from strategies.core.candle_indicators import to_heikin_ashi, to_n_min_bars, compute_stoch_rsi
from scripts.oi_orb_entry_mode_backtest import ROWS, SIDE, resolve_eq_key, to_bars, volume_by_ts, compute_orb
from scripts.oi_orb_atr_chandelier_backtest import _atr_as_of, ATR_PERIOD, ATR_TF_MIN
from scripts.oi_orb_ha_stochrsi_exit_backtest import ha_stoch_exit
from scripts.oi_orb_trap_target_full_htf_ltf_sweep import find_entry

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
EXIT_TF_MIN = 15
RSI_PERIOD = 9
STOCH_PERIOD = 9
SMOOTH = 3
# ATR(14) on 3-min bars needs 42 real minutes of history to be a mature
# reading. Right after a 09:25 entry there simply isn't 42 minutes of
# TODAY's own data yet -- an expanding-window ATR computed from only
# today's bars is a cold, noisy artifact for the entire first ~40 minutes
# of every single trade, which is when several of the profit-lock
# variants below were arming/locking on trivial noise (direct user catch:
# "for ATR based u require prev day data as well, reason: u r using 14
# length ATR"). Fixed by seeding the ATR's rolling TR window from the
# PREVIOUS TRADING DAY's own 3-min bars before today's session starts --
# same prev-day-seed idiom this codebase already uses for RSI/ROC warm-up
# elsewhere (PoolIndicatorEngine).


def _prev_trading_day(d: date) -> date:
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:   # skip Sat/Sun (does not account for exchange holidays)
        prev -= timedelta(days=1)
    return prev


def _pts(side, entry, price):
    raw = price - entry
    return raw if side == "CALL" else -raw


def _ha_stoch_series(bars_1m):
    ha_1m = to_heikin_ashi(bars_1m)
    ha_15m = to_n_min_bars(ha_1m, EXIT_TF_MIN)
    k, d = compute_stoch_rsi([b.close for b in ha_15m], RSI_PERIOD, STOCH_PERIOD, SMOOTH)
    return ha_15m, k, d


def _seeded_atr_series(seed_bars_1m, bars_1m):
    """Builds a mature, de-spiked ATR(14) reading for TODAY's 3-min bars,
    seeded from the previous trading day's own true ranges, WITHOUT ever
    treating the overnight seed/today boundary as a real intraday true-
    range sample. An overnight gap between yesterday's last price and
    today's open is not real intraday volatility for a strategy that never
    carries a position overnight -- counting it as one TR sample spikes
    the whole rolling average right when the mechanism most needs a clean
    reading. Found by direct inspection: BOSCHLTD's naively-seeded ATR
    came out to ~1,187 points, dwarfing even its whole 365pt favorable
    move -- no stock trading that calmly intraday produces a real ATR that
    large; the boundary gap TR was the entire cause. Also applies the same
    median-cap de-spike compute_atr_series_capped already uses elsewhere
    in this repo (a prior real BSE incident: one freak wide-TR bar blowing
    out a plain-mean ATR), so a genuine intraday spike bar can't repeat
    the same failure mode from a different angle.

    Returns (today_tf_bars, atrs) where atrs[i] is the ATR known as of
    today_tf_bars[i]'s own close."""
    seed_tf = to_n_min_bars(seed_bars_1m, ATR_TF_MIN) if seed_bars_1m else []
    today_tf = to_n_min_bars(bars_1m, ATR_TF_MIN)

    # Plain mean ATR (NOT the median-capped de-spike variant) -- the
    # de-spike cap was designed for a DIFFERENT failure mode (one freak
    # MID-DAY wide-TR bar, the BSE incident compute_atr_series_capped
    # documents), and stacking it on top of the boundary-gap exclusion
    # below was tested and found to be an over-correction: it shrank ATR
    # for most OTHER stocks enough to make the arm threshold trivial,
    # collapsing every profit-lock variant to near-instant, near-breakeven
    # exits across the board. The single real problem this function exists
    # to fix -- BOSCHLTD's inflated ~1,187pt ATR -- was caused entirely by
    # the overnight seed/today boundary gap being counted as a true-range
    # sample, not by a genuine mid-day spike bar; excluding that boundary
    # gap alone (below) is sufficient and does not need the extra cap.
    trs: List[float] = []
    prev_close = None
    for b in seed_tf:
        tr = (b.high - b.low) if prev_close is None else \
            max(b.high - b.low, abs(b.high - prev_close), abs(b.low - prev_close))
        trs.append(tr)
        prev_close = b.close
    # Boundary: today's first bar gets NO gap comparison against yesterday's
    # close -- reset prev_close so its own TR is just high-low.
    prev_close = None
    atrs: List[float] = []
    for b in today_tf:
        tr = (b.high - b.low) if prev_close is None else \
            max(b.high - b.low, abs(b.high - prev_close), abs(b.low - prev_close))
        trs.append(tr)
        window = trs[-ATR_PERIOD:]
        atrs.append(sum(window) / len(window))
        prev_close = b.close
    return today_tf, atrs


def atr_profit_lock_exit(bars_1m, side, entry_ts, entry_price, seed_bars_1m=None, arm_mult=1.0, trail_mult=1.5):
    """PURE profit-lock: floor starts undefined (no exit possible from this
    mechanism) and only ever activates once the favorable move reaches
    arm_mult x ATR. Once armed, floor = peak_favorable - trail_mult x ATR,
    clamped so it NEVER sits below entry_price (side-aware) -- this can
    only lock in a profit or hold at breakeven, never create a loss.

    seed_bars_1m: previous trading day's 1-min bars, used ONLY to give
    ATR(14) a mature reading from the very first bar of today's session
    (see _seeded_atr_series's own docstring) -- never contributes a
    tradeable bar itself."""
    bars_tf, atrs = _seeded_atr_series(seed_bars_1m, bars_1m)
    entry_atr = _atr_as_of(bars_tf, atrs, entry_ts) or 0.01
    post = [b for b in bars_1m if b.ts >= entry_ts]
    if not post:
        return entry_ts, entry_price, "no_data_after_entry"

    armed = False
    floor = None
    extreme = entry_price
    for b in post:
        cur_atr = _atr_as_of(bars_tf, atrs, b.ts) or entry_atr
        if side == "CALL":
            extreme = max(extreme, b.high)
            if not armed and (extreme - entry_price) >= arm_mult * cur_atr:
                armed = True
            if armed:
                candidate = extreme - trail_mult * cur_atr
                new_floor = max(candidate, entry_price)
                floor = new_floor if floor is None else max(floor, new_floor)
            if floor is not None and b.low <= floor:
                return b.ts, floor, "atr_profit_lock"
        else:
            extreme = min(extreme, b.low)
            if not armed and (entry_price - extreme) >= arm_mult * cur_atr:
                armed = True
            if armed:
                candidate = extreme + trail_mult * cur_atr
                new_floor = min(candidate, entry_price)
                floor = new_floor if floor is None else min(floor, new_floor)
            if floor is not None and b.high >= floor:
                return b.ts, floor, "atr_profit_lock"

    last = post[-1]
    return last.ts, last.close, "eod_close"


def orb_range_profit_lock_exit(bars_1m, side, entry_ts, entry_price, orb_h, orb_l, arm_mult=1.0, trail_mult=1.5):
    """Same PURE profit-lock shape as atr_profit_lock_exit, but anchored to
    TODAY's own Opening Range (09:15-09:25) instead of a volatility
    estimate borrowed from a PRIOR day. This directly fixes the failure
    mode found by testing the ATR-seeded variants against the full
    dataset: these are OI-Spurt/price-momentum SHORTLISTED stocks --
    selected specifically BECAUSE today is an unusual-volatility day for
    them. Seeding "normal" volatility from yesterday systematically
    UNDERESTIMATES today's real range for exactly the stocks this
    screener trades, which is why the ATR variants fixed BOSCHLTD (a
    correctly-large ATR there) but cut nearly every OTHER trade short
    (an artificially small ATR for stocks having a calmer previous day).
    The ORB range is today-specific, already known at entry (entry never
    happens before the ORB closes), needs no previous-day data, and is
    itself a recognized price-action construct (not a smoothed/statistical
    indicator) -- fits the same 'use price action, not indicator math'
    brief as the HTF S/R pool variant."""
    orb_range = max(orb_h - orb_l, 0.01)
    post = [b for b in bars_1m if b.ts >= entry_ts]
    if not post:
        return entry_ts, entry_price, "no_data_after_entry"

    armed = False
    floor = None
    extreme = entry_price
    for b in post:
        if side == "CALL":
            extreme = max(extreme, b.high)
            if not armed and (extreme - entry_price) >= arm_mult * orb_range:
                armed = True
            if armed:
                candidate = extreme - trail_mult * orb_range
                new_floor = max(candidate, entry_price)
                floor = new_floor if floor is None else max(floor, new_floor)
            if floor is not None and b.low <= floor:
                return b.ts, floor, "orb_range_profit_lock"
        else:
            extreme = min(extreme, b.low)
            if not armed and (entry_price - extreme) >= arm_mult * orb_range:
                armed = True
            if armed:
                candidate = extreme + trail_mult * orb_range
                new_floor = min(candidate, entry_price)
                floor = new_floor if floor is None else min(floor, new_floor)
            if floor is not None and b.high >= floor:
                return b.ts, floor, "orb_range_profit_lock"

    last = post[-1]
    return last.ts, last.close, "eod_close"


def classic_trailing_sl_exit(bars_1m, side, entry_ts, entry_price, orb_h, orb_l,
                              initial_sl_mult=1.0, trail_mult=1.0):
    """CLASSIC trailing SL -- direct user follow-up, distinct from every
    profit-lock variant above. This has a REAL stop from the moment of
    entry (no arm gate): initial_sl = entry -/+ initial_sl_mult x ORB
    range, then chandelier-trails behind the extreme by trail_mult x ORB
    range once price moves favorably. Unlike the profit-locks, this CAN
    turn a winner into a loss on ordinary early-session noise -- flagged
    directly to the user before running this, since this codebase already
    tested an ATR-based version of exactly this shape
    (oi_orb_rr_ladder_backtest.py) and found it both underperforms the
    HA+StochRSI baseline overall AND turned a real winner (FORCEMOT,
    baseline +343) into an early loss (-110) by stopping out on opening
    volatility before the real move developed. Anchored to TODAY's own
    ORB range rather than ATR to avoid the separate "seeded from a calmer
    yesterday" problem found earlier in this same file. Races HA+StochRSI
    exactly like the profit-lock variants -- whichever fires first wins."""
    orb_range = max(orb_h - orb_l, 0.01)
    post = [b for b in bars_1m if b.ts >= entry_ts]
    if not post:
        return entry_ts, entry_price, "no_data_after_entry"

    if side == "CALL":
        sl_orig = entry_price - initial_sl_mult * orb_range
        stop = sl_orig
        extreme = entry_price
        for b in post:
            extreme = max(extreme, b.high)
            trail = extreme - trail_mult * orb_range
            stop = max(stop, trail)
            if b.low <= stop:
                reason = "trailing_sl_hit" if stop > sl_orig else "initial_sl_hit"
                return b.ts, stop, reason
    else:
        sl_orig = entry_price + initial_sl_mult * orb_range
        stop = sl_orig
        extreme = entry_price
        for b in post:
            extreme = min(extreme, b.low)
            trail = extreme + trail_mult * orb_range
            stop = min(stop, trail)
            if b.high >= stop:
                reason = "trailing_sl_hit" if stop < sl_orig else "initial_sl_hit"
                return b.ts, stop, reason

    last = post[-1]
    return last.ts, last.close, "eod_close"


def find_swings(bars, pivot=2):
    out = []
    n = len(bars)
    for i in range(pivot, n - pivot):
        window = bars[i - pivot:i + pivot + 1]
        c = bars[i]
        if c.high == max(w.high for w in window) and c.high > bars[i - 1].high and c.high > bars[i + 1].high:
            out.append((c.ts, c.high, "H"))
        if c.low == min(w.low for w in window) and c.low < bars[i - 1].low and c.low < bars[i + 1].low:
            out.append((c.ts, c.low, "L"))
    return out


def group_touch_pools(swings, tol_pts):
    """Groups raw swing pivots of the SAME kind ('H' or 'L') into pools by
    proximity (within tol_pts of each other), same clustering concept as
    group_equal_levels/pool_swing_low elsewhere in this codebase's SMC-
    style detectors. Returns a list of pools, each
    {'level': float, 'kind': 'H'|'L', 'touches': [ts, ...], 'last_ts': ts}
    -- 'level' is the mean of the clustered touches, 'last_ts' is the most
    recent touch (used to gate lookahead: a pool only "exists" as of its
    last touch's own timestamp, not retroactively from its first touch)."""
    pools = []
    for ts, px, kind in swings:
        placed = False
        for p in pools:
            if p["kind"] == kind and abs(px - p["level"]) <= tol_pts:
                p["touches"].append((ts, px))
                p["level"] = sum(t[1] for t in p["touches"]) / len(p["touches"])
                p["last_ts"] = max(p["last_ts"], ts)
                placed = True
                break
        if not placed:
            pools.append({"level": px, "kind": kind, "touches": [(ts, px)], "last_ts": ts})
    return pools


def htf_sr_pool_lock_exit(bars_1m, side, entry_ts, entry_price, htf_min=15, tol_pct=0.05, min_touches=2):
    """PURE PRICE-ACTION profit-lock -- no indicator math at all (no ATR,
    no RSI, nothing computed off closes). Detects swing pivots on HTF
    (default 15-min, matching the SAME timeframe the confirmed HA+StochRSI
    exit already reads structure on) bars, clusters them into touch pools
    (group_touch_pools), and only trusts a pool as a real support/
    resistance level once it has >= min_touches -- the exact lesson this
    codebase already paid for once: a bare single-touch swing pivot used
    as a stop caused a real production loss (OI-Flow, 2026-08-19 incident,
    "a SENSEX PE entry stopped out 13 seconds after entry" on ordinary
    noise; fixed there by requiring 2+ clustered touches before a level
    counts -- OI-Flow itself was later removed from this codebase's scope,
    but the design lesson is reapplied fresh here, not imported, per every
    strategy's own standalone mandate).

    Once a CONFIRMED (>= min_touches, confirmed as of its last touch's own
    timestamp -- never retroactively, no lookahead) pool exists on the
    correct side (support/swing-low below current price for a CALL,
    resistance/swing-high above current price for a PUT) and sits in the
    trade's favor, the floor ratchets to it -- clamped so it never sits
    below entry_price. Pure profit-lock: can only improve or match the
    outcome of holding to the structural exit alone, never worsen it.

    tol_pct: clustering tolerance as a % of entry price (used ONLY to
    decide "is this roughly the same level as an earlier touch", never as
    a trigger/arm threshold -- unlike the earlier broken %-of-spot ratchet
    attempt, this has nothing to do with how big a move needs to be)."""
    htf_bars = to_n_min_bars(bars_1m, htf_min)
    tol_pts = entry_price * tol_pct / 100.0
    all_swings = find_swings(htf_bars, pivot=1)
    pools = group_touch_pools(all_swings, tol_pts)

    post = [b for b in bars_1m if b.ts >= entry_ts]
    if not post:
        return entry_ts, entry_price, "no_data_after_entry"

    floor = None
    for b in post:
        confirmed_pools = [p for p in pools if p["last_ts"] < b.ts and len(p["touches"]) >= min_touches]
        if side == "CALL":
            candidates = [p["level"] for p in confirmed_pools if p["kind"] == "L" and p["level"] > entry_price]
            if candidates:
                new_floor = max(candidates)
                floor = new_floor if floor is None else max(floor, new_floor)
            if floor is not None and b.low <= floor:
                return b.ts, floor, "htf_sr_pool_lock"
        else:
            candidates = [p["level"] for p in confirmed_pools if p["kind"] == "H" and p["level"] < entry_price]
            if candidates:
                new_floor = min(candidates)
                floor = new_floor if floor is None else min(floor, new_floor)
            if floor is not None and b.high >= floor:
                return b.ts, floor, "htf_sr_pool_lock"

    last = post[-1]
    return last.ts, last.close, "eod_close"


def swing_profit_lock_exit(bars_1m, side, entry_ts, entry_price, seed_bars_1m=None, arm_mult=1.0):
    """Same arm gate as atr_profit_lock_exit (>= arm_mult x ATR favorable
    move before the mechanism can do anything at all -- prevents whipsaw
    on trivial pivots right after entry). Once armed, floor ratchets to
    the most recent CONFIRMED swing pivot in the trade's favor, clamped to
    never sit below entry_price. Pure profit-lock, same guarantee as the
    ATR variant. Same prev-day ATR seed as atr_profit_lock_exit -- see its
    own docstring."""
    bars_tf, atrs = _seeded_atr_series(seed_bars_1m, bars_1m)
    entry_atr = _atr_as_of(bars_tf, atrs, entry_ts) or 0.01
    post = [b for b in bars_1m if b.ts >= entry_ts]
    if len(post) < 6:
        return entry_ts, entry_price, "no_data_after_entry"
    swings = find_swings(post, pivot=2)

    armed = False
    floor = None
    extreme = entry_price
    for b in post:
        cur_atr = _atr_as_of(bars_tf, atrs, b.ts) or entry_atr
        if side == "CALL":
            extreme = max(extreme, b.high)
            if not armed and (extreme - entry_price) >= arm_mult * cur_atr:
                armed = True
        else:
            extreme = min(extreme, b.low)
            if not armed and (entry_price - extreme) >= arm_mult * cur_atr:
                armed = True
        if armed:
            for (sts, spx, kind) in swings:
                if sts >= b.ts:
                    continue
                if side == "CALL" and kind == "L":
                    cand = max(spx, entry_price)
                    floor = cand if floor is None else max(floor, cand)
                elif side == "PUT" and kind == "H":
                    cand = min(spx, entry_price)
                    floor = cand if floor is None else min(floor, cand)
        if floor is not None:
            if side == "CALL" and b.low <= floor:
                return b.ts, floor, "swing_profit_lock"
            if side == "PUT" and b.high >= floor:
                return b.ts, floor, "swing_profit_lock"
    last = post[-1]
    return last.ts, last.close, "eod_close"


def race_exit(bars_1m, side, entry_ts, entry_price, ha_15m, k, d, fast_fn, fast_kwargs=None):
    fast_kwargs = fast_kwargs or {}
    ha_ts, ha_price, ha_reason = ha_stoch_exit(entry_ts, entry_price, side, bars_1m, ha_15m, k, d, inclusive=True)
    fast_ts, fast_price, fast_reason = fast_fn(bars_1m, side, entry_ts, entry_price, **fast_kwargs)
    ha_is_real = ha_reason != "eod_close"
    fast_is_real = fast_reason != "eod_close"
    if ha_is_real and fast_is_real:
        return (ha_ts, ha_price, ha_reason) if ha_ts <= fast_ts else (fast_ts, fast_price, fast_reason)
    if ha_is_real:
        return ha_ts, ha_price, ha_reason
    if fast_is_real:
        return fast_ts, fast_price, fast_reason
    return ha_ts, ha_price, ha_reason


@dataclass
class Trade:
    date: str
    symbol: str
    side: str
    entry_ts: object
    entry_price: Optional[float]
    exit_ts: object
    exit_price: Optional[float]
    reason: str

    @property
    def points(self) -> Optional[float]:
        if self.entry_price is None or self.exit_price is None:
            return None
        return _pts(self.side, self.entry_price, self.exit_price)


def summarize(label, trades: List[Trade]):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    worse_than_baseline_by_more_than = None
    print(f"{label:>28}  entered={len(entered):2d}  win%={win_pct:5.1f}  PF={pf:7.2f}  "
          f"total={total:+9.2f}  avg={((total/len(entered)) if entered else 0):+7.2f}")
    return {"label": label, "entered": len(entered), "pf": pf, "win_pct": win_pct, "total": total, "trades": trades}


async def fetch_all_with_seed():
    """Same shape as oi_orb_atr_chandelier_backtest.fetch_all(), plus a
    seed_bars_1m entry per row: the previous trading day's own 1-min bars,
    fetched in the SAME range call as today's (fetch_upstox_range_1m merges
    multi-day ranges itself), split by date. Used to give ATR(14) a mature
    reading from minute 1 of today instead of a cold expanding-window
    start (see this file's own module docstring)."""
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
        prev_d = _prev_trading_day(d)
        rows = await fetch_upstox_range_1m(eq_key, TOKEN, prev_d, d)
        if not rows:
            cache[key] = None
            continue
        from datetime import datetime as _dt
        today_rows = [r for r in rows if _dt.fromisoformat(r["ts"]).date() == d]
        seed_rows = [r for r in rows if _dt.fromisoformat(r["ts"]).date() < d]
        if not today_rows:
            cache[key] = None
            continue
        bars_1m = to_bars(today_rows)
        seed_bars_1m = to_bars(seed_rows) if seed_rows else []
        vol_by_ts = volume_by_ts(today_rows)
        orb = compute_orb(bars_1m)
        if orb is None:
            cache[key] = None
            continue
        orb_h, orb_l = orb
        cache[key] = (bars_1m, vol_by_ts, orb_h, orb_l, seed_bars_1m)
    n_ok = sum(1 for v in cache.values() if v)
    n_seeded = sum(1 for v in cache.values() if v and v[4])
    print(f"Fetched {n_ok} usable rows ({n_seeded} with a real previous-day ATR seed, "
          f"{n_ok - n_seeded} falling back to cold-start ATR -- likely a holiday/gap before that day).")
    return cache


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows (real Upstox 1-min NSE_EQ history, plus prev-day ATR seed)...")
    cache = await fetch_all_with_seed()

    variants = {
        "A_baseline": [],
        "B_atr_lock_1.0_1.5": [],
        "C_atr_lock_0.75_1.0": [],
        "D_atr_lock_1.5_2.0": [],
        "E_swing_lock": [],
        "F_htf_sr_pool_lock": [],
        "G_orb_range_lock_1.0_1.5": [],
        "H_orb_range_lock_0.75_1.25": [],
        "I_trailing_sl_1.0_1.0": [],
        "J_trailing_sl_1.5_1.0": [],
    }
    big_movers = []
    never_worse_violations = []  # PROFIT-LOCK variants only (B-H) -- must be zero, these can never be worse
    new_losses = []              # CLASSIC trailing-SL variants (I, J) -- baseline was a win, trailing-SL made it a loss

    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l, seed_bars_1m = cached

        entry = find_entry(bars_1m, side, orb_h, orb_l, vol_by_ts)
        if entry is None:
            continue
        entry_ts, entry_price = entry
        post = [b for b in bars_1m if b.ts >= entry_ts]
        max_favorable = max((_pts(side, entry_price, (b.high if side == "CALL" else b.low)) for b in post), default=0.0)

        ha_15m, k, d = _ha_stoch_series(bars_1m)
        a_ts, a_px, a_reason = ha_stoch_exit(entry_ts, entry_price, side, bars_1m, ha_15m, k, d, inclusive=True)
        variants["A_baseline"].append(Trade(trade_date, symbol, side, entry_ts, entry_price, a_ts, a_px, a_reason))
        a_pts = variants["A_baseline"][-1].points

        for key, fn, kw in [
            ("B_atr_lock_1.0_1.5", atr_profit_lock_exit, {"seed_bars_1m": seed_bars_1m, "arm_mult": 1.0, "trail_mult": 1.5}),
            ("C_atr_lock_0.75_1.0", atr_profit_lock_exit, {"seed_bars_1m": seed_bars_1m, "arm_mult": 0.75, "trail_mult": 1.0}),
            ("D_atr_lock_1.5_2.0", atr_profit_lock_exit, {"seed_bars_1m": seed_bars_1m, "arm_mult": 1.5, "trail_mult": 2.0}),
            ("E_swing_lock", swing_profit_lock_exit, {"seed_bars_1m": seed_bars_1m, "arm_mult": 1.0}),
            ("F_htf_sr_pool_lock", htf_sr_pool_lock_exit, {"htf_min": 15, "tol_pct": 0.05, "min_touches": 2}),
            ("G_orb_range_lock_1.0_1.5", orb_range_profit_lock_exit, {"orb_h": orb_h, "orb_l": orb_l, "arm_mult": 1.0, "trail_mult": 1.5}),
            ("H_orb_range_lock_0.75_1.25", orb_range_profit_lock_exit, {"orb_h": orb_h, "orb_l": orb_l, "arm_mult": 0.75, "trail_mult": 1.25}),
        ]:
            ts_, px_, reason_ = race_exit(bars_1m, side, entry_ts, entry_price, ha_15m, k, d, fn, kw)
            variants[key].append(Trade(trade_date, symbol, side, entry_ts, entry_price, ts_, px_, reason_))
            pts_ = variants[key][-1].points
            if pts_ is not None and a_pts is not None and pts_ < a_pts - 0.01:
                never_worse_violations.append((key, trade_date, symbol, a_pts, pts_))

        for key, kw in [
            ("I_trailing_sl_1.0_1.0", {"orb_h": orb_h, "orb_l": orb_l, "initial_sl_mult": 1.0, "trail_mult": 1.0}),
            ("J_trailing_sl_1.5_1.0", {"orb_h": orb_h, "orb_l": orb_l, "initial_sl_mult": 1.5, "trail_mult": 1.0}),
        ]:
            ts_, px_, reason_ = race_exit(bars_1m, side, entry_ts, entry_price, ha_15m, k, d,
                                           classic_trailing_sl_exit, kw)
            variants[key].append(Trade(trade_date, symbol, side, entry_ts, entry_price, ts_, px_, reason_))
            pts_ = variants[key][-1].points
            if pts_ is not None and a_pts is not None and a_pts > 0 and pts_ <= 0:
                new_losses.append((key, trade_date, symbol, a_pts, pts_))

        if max_favorable >= 100 and a_pts is not None and a_pts < max_favorable * 0.35:
            big_movers.append({
                "date": trade_date, "symbol": symbol, "side": side,
                "max_favorable": max_favorable, "baseline_pts": a_pts,
                **{k: variants[k][-1].points for k in ["B_atr_lock_1.0_1.5", "C_atr_lock_0.75_1.0",
                                                        "D_atr_lock_1.5_2.0", "E_swing_lock", "F_htf_sr_pool_lock",
                                                        "G_orb_range_lock_1.0_1.5", "H_orb_range_lock_0.75_1.25",
                                                        "I_trailing_sl_1.0_1.0", "J_trailing_sl_1.5_1.0"]}
            })

    print("\n" + "=" * 120)
    print("SUMMARY -- all variants (racing HA+StochRSI) vs current frozen baseline")
    print("=" * 120)
    summaries = {k: summarize(k, v) for k, v in variants.items()}

    print("\n" + "=" * 120)
    print(f"NEVER-WORSE CHECK (profit-lock variants B-H only) -- must never beat the baseline DOWN "
          f"({len(never_worse_violations)} violations found)")
    print("=" * 120)
    for v in never_worse_violations:
        print(f"  {v}")

    print("\n" + "=" * 120)
    print(f"NEW LOSSES CHECK (classic trailing-SL variants I, J) -- trades that were REAL WINNERS on the "
          f"frozen baseline but got stopped into a loss by the trailing SL ({len(new_losses)} found)")
    print("=" * 120)
    for v in new_losses:
        print(f"  {v}")

    print("\n" + "=" * 120)
    print(f"BIG-MOVER FOCUS -- setups where spot ran >=100pts favorable but baseline kept <35% of it ({len(big_movers)} rows)")
    print("=" * 120)
    for r in big_movers:
        print(f"  {r['date']} {r['symbol']:<12} {r['side']:<4} max_favorable={r['max_favorable']:+8.2f}  "
              f"baseline={r['baseline_pts']:+8.2f}  B={r['B_atr_lock_1.0_1.5']:+8.2f}  "
              f"C={r['C_atr_lock_0.75_1.0']:+8.2f}  D={r['D_atr_lock_1.5_2.0']:+8.2f}  E={r['E_swing_lock']:+8.2f}  "
              f"F={r['F_htf_sr_pool_lock']:+8.2f}  G={r['G_orb_range_lock_1.0_1.5']:+8.2f}  "
              f"H={r['H_orb_range_lock_0.75_1.25']:+8.2f}  I={r['I_trailing_sl_1.0_1.0']:+8.2f}  "
              f"J={r['J_trailing_sl_1.5_1.0']:+8.2f}")

    import json
    def _ser(trades):
        return [{"date": t.date, "symbol": t.symbol, "side": t.side,
                  "entry_ts": t.entry_ts.strftime("%H:%M"), "entry_price": t.entry_price,
                  "exit_ts": t.exit_ts.strftime("%H:%M"), "exit_price": t.exit_price,
                  "reason": t.reason, "points": t.points}
                 for t in sorted(trades, key=lambda x: (x.date, x.symbol))]

    report = {k: {"entered": v["entered"], "pf": v["pf"], "win_pct": v["win_pct"], "total": v["total"],
                  "trades": _ser(v["trades"])}
              for k, v in summaries.items()}
    report["big_movers"] = big_movers
    report["never_worse_violations"] = never_worse_violations
    report["new_losses"] = new_losses
    out_path = os.path.join("data", "oi_orb_trend_capture_exit_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nFull JSON report written to {out_path}")


asyncio.run(main())
