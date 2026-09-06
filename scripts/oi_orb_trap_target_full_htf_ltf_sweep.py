"""
scripts/oi_orb_trap_target_full_htf_ltf_sweep.py -- 2026-09-05/06, direct
user follow-up on scripts/oi_orb_trap_target_htf_sweep.py: compare the
CURRENT confirmed baseline exit (scripts/oi_orb_ha_stochrsi_exit_backtest.py
-- normal-candle entry, 15-min Heikin-Ashi shape + StochRSI(9,9) inclusive
cross, no SL, EOD fallback) against a fresh "trap-target" exit, swept
across an HTF x LTF grid, on the SAME entries (identical VWAP-retest entry
timing, same ROWS dataset) for an apples-to-apples comparison.

Mechanic (direct user spec, confirmed against the already-built
scripts/oi_orb_trap_target_htf_sweep.py -- same opposite-direction-trap
convention):
  - LONG (CALL): watch for a BULL TRAP (screener.bull_trap_zones -- a
    failed rally that reverses down, i.e. a resistance/reversal zone
    AHEAD of our own long) forming at the swept HTF.
  - SHORT (PUT): watch for a BEAR TRAP (screener.sharp_bear_zones) at the
    swept HTF.
  - The moment price touches an ALREADY-LOCKED trap zone of that opposite
    type (no lookahead -- the zone must have locked strictly before the
    touching bar), jump to the swept LTF and start a FRESH
    SupportResistanceCalculator from that instant.
  - Once that ladder's S1 (CALL) / R1 (PUT) becomes established, exit the
    moment it's breached. Because the calculator keeps accumulating LTF
    bars as time passes, S1/R1 can re-establish to a fresh (tighter, in
    the trade's favor) level more than once -- so this already behaves as
    a ratchet toward the newest established level, not a single frozen
    value from the first touch.
  - No fixed SL at all -- matches the baseline's own "No SL. Falls back to
    EOD" design exactly, so the comparison isolates the EXIT MECHANIC only.
  - If no zone ever forms/locks/gets touched, or the ladder never
    establishes+breaches, the trade runs to EOD (unchanged fallback, same
    as the baseline).
  - Entry timing, single-trade-per-day, and no-reentry are all UNCHANGED
    from the baseline (scripts/oi_orb_ha_stochrsi_exit_backtest.py's own
    run_one) -- entries are computed ONCE per (date, symbol) and reused
    for the baseline AND every trap-target variant, guaranteeing identical
    entry sets across the whole comparison.

2026-09-06 correction (direct user instruction): the first version of this
script fetched an 18-day multi-day lookback per symbol so 1D/4h/2h HTF
zones would have more than one bar per day to form on. That added real
network-flakiness risk (a symbol's multi-day fetch silently coming back
short/failing on one run and not another) for zero benefit once the HTF
list is confined to intraday timeframes -- 1h/30min/15min all produce
several real candles from a SINGLE trading day's own ~6.25-hour session,
so this version fetches ONLY today's single-day bars (the exact same
fetch_all() the baseline itself uses, not a reimplementation) for BOTH the
baseline and every trap-target variant. No multi-day fetch, no extra
lookback, no daily/4h/2h HTF options.

HTF options swept: 1h (60min), 30min, 15min. LTF options swept: 5min,
3min, 1min. 3 x 3 = 9 trap-target variants, each reported against the
same baseline.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_trap_target_full_htf_ltf_sweep.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

sys.path.insert(0, ".")

from strategies.core.support_resistance import SupportResistanceCalculator
from strategies.core.trap_zone_utils import Bar
from scripts.oi_orb_entry_mode_backtest import (
    ROWS, SIDE, ORB_START, ORB_END, ENTRY_WINDOW_END, _key_range, to_n_min_bars,
)
from scripts.oi_orb_atr_chandelier_backtest import fetch_all
from scripts.oi_orb_30_3_1_target_backtest import to_heikin_ashi
from scripts.oi_orb_stoch_rsi_backtest import compute_stoch_rsi
from scripts.oi_orb_ha_stochrsi_exit_backtest import ha_stoch_exit
from strategies.oi_orb_screener import screener

EXIT_TF_MIN = 15     # baseline's own HA+StochRSI timeframe, unchanged
RSI_PERIOD = 9
STOCH_PERIOD = 9
SMOOTH = 3

HTF_OPTIONS = [("1h", 60), ("30min", 30), ("15min", 15)]
LTF_OPTIONS = [5, 3, 1]


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
        raw = self.exit_price - self.entry_price
        return raw if self.side == "CALL" else -raw


def find_entry(bars_1m, side, orb_h, orb_l, vol_by_ts):
    """IDENTICAL entry logic to oi_orb_ha_stochrsi_exit_backtest.run_one's
    own entry half -- re-extracted (not reimplemented from scratch) so
    entries are byte-for-byte reproducible between the baseline and every
    trap-target variant. Returns (entry_ts, entry_price) or None."""
    orb_bars = _key_range(bars_1m, ORB_START, ORB_END)
    vwap_state = screener.VwapState()
    armed = False
    historically_fulfilled = False
    for b in orb_bars:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if fire:
            historically_fulfilled = True
            break

    entry_window = _key_range(bars_1m, ORB_END, ENTRY_WINDOW_END)
    if not entry_window:
        return None

    if historically_fulfilled:
        b0 = entry_window[0]
        return (b0.ts, b0.close)

    breached = False
    for b in entry_window:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        if side == "CALL" and b.low <= orb_l:
            breached = True
        elif side == "PUT" and b.high >= orb_h:
            breached = True
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if not fire:
            continue
        armed = False
        if breached:
            continue
        return (b.ts, b.close)
    return None


def trap_target_exit(entry_ts, entry_price, side, bars_1m, htf_bars, ltf_bars, ltf_min):
    """No SL (matches the baseline's own design) -- pure trap-zone-touch
    then LTF-S&R-breach exit, falling back to EOD if it never fires."""
    zones_fn = screener.bull_trap_zones if side == "CALL" else screener.sharp_bear_zones
    zones = zones_fn(htf_bars)

    post_entry_1m = [b for b in bars_1m if b.ts >= entry_ts]

    zone_touched_ts = None
    calc = None
    ltf_fed = 0

    for b in post_entry_1m:
        if zone_touched_ts is None:
            for z in zones:
                if z["lock_ts"] is None or z["lock_ts"] > b.ts:
                    continue   # not locked yet as of this bar -- no lookahead
                touched = (b.low <= z["zone_hi"]) and (b.high >= z["zone_lo"])
                if touched:
                    zone_touched_ts = b.ts
                    calc = SupportResistanceCalculator()
                    ltf_fed = 0
                    break

        if calc is not None:
            avail_ltf = [x for x in ltf_bars if entry_ts <= x.ts <= b.ts]
            for nb in avail_ltf[ltf_fed:]:
                calc.process_straddle_candle("SYM", {"timestamp": nb.ts, "high": nb.high,
                                                       "low": nb.low, "duration": ltf_min})
            ltf_fed = len(avail_ltf)
            sr = calc.get_calculated_sr_state("SYM").get("sr_levels", {})
            level = sr.get("S1") if side == "CALL" else sr.get("R1")
            if level is not None and level.get("is_established"):
                lvl = level["low"] if side == "CALL" else level["high"]
                breach = (b.low <= lvl) if side == "CALL" else (b.high >= lvl)
                if breach:
                    return b.ts, lvl, "trap_target_hit"

    if post_entry_1m:
        last = post_entry_1m[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def summarize(label, trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    target_hits = sum(1 for t in entered if t.reason == "trap_target_hit")
    print(f"{label:>16}  entered={len(entered):2d}  target_hits={target_hits:2d}  "
          f"win%={win_pct:5.1f}  PF={pf:6.2f}  total={total:+9.2f}  "
          f"avg={((total/len(entered)) if entered else 0):+7.2f}")
    return {"label": label, "trades": trades, "total": total, "pf": pf,
            "win_pct": win_pct, "entered": len(entered), "target_hits": target_hits}


def _trade_row(t):
    return {
        "date": t.date, "symbol": t.symbol, "side": t.side,
        "entry_ts": t.entry_ts.strftime("%H:%M") if t.entry_ts else None,
        "entry_price": t.entry_price,
        "exit_ts": t.exit_ts.strftime("%H:%M") if t.exit_ts else None,
        "exit_price": t.exit_price,
        "reason": t.reason, "points": t.points,
    }


async def main():
    if not os.environ.get("UPSTOX_TOKEN"):
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows (real Upstox 1-min NSE_EQ history, single-day only)...")
    cache = await fetch_all()
    json_report = {"baseline": None, "variants": {}}

    # Compute entries ONCE per (date, symbol) -- shared by baseline + every trap-target variant.
    entries: Dict[tuple, Optional[tuple]] = {}
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        if cached is None:
            entries[(trade_date, symbol)] = None
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        entries[(trade_date, symbol)] = find_entry(bars_1m, side, orb_h, orb_l, vol_by_ts)

    # ---- Baseline: HA-shape + StochRSI(9,9) inclusive cross, normal-candle entry ----
    baseline_trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = cache.get((trade_date, symbol))
        entry = entries.get((trade_date, symbol))
        if cached is None or entry is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l = cached
        entry_ts, entry_price = entry
        ha_1m = to_heikin_ashi(bars_1m)
        ha_15m = to_n_min_bars(ha_1m, EXIT_TF_MIN)
        k, d = compute_stoch_rsi([b.close for b in ha_15m], RSI_PERIOD, STOCH_PERIOD, SMOOTH)
        exit_ts, exit_price, reason = ha_stoch_exit(
            entry_ts, entry_price, side, bars_1m, ha_15m, k, d, inclusive=True)
        baseline_trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, exit_ts, exit_price, reason))

    print("\n" + "=" * 110)
    print("BASELINE (confirmed): normal-candle entry, 15m HA-shape + StochRSI(9,9) inclusive cross, no SL")
    print("=" * 110)
    baseline_summary = summarize("baseline", baseline_trades)
    for t in sorted(baseline_trades, key=lambda x: (x.date, x.symbol)):
        print(f"  {t.date} {t.symbol:<12} {t.side:<4} entry={t.entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
              f"exit={t.exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.reason})  pts={t.points:+8.2f}")
    json_report["baseline"] = {
        "pf": baseline_summary["pf"], "win_pct": baseline_summary["win_pct"],
        "total": baseline_summary["total"], "entered": baseline_summary["entered"],
        "trades": [_trade_row(t) for t in sorted(baseline_trades, key=lambda x: (x.date, x.symbol))],
    }

    # ---- Trap-target variants: HTF x LTF grid (single-day bars only) ----
    print("\n" + "=" * 110)
    print("TRAP-TARGET EXIT: HTF x LTF GRID (same entries as baseline, opposite-direction trap zone -> "
          "LTF S&R S1/R1 breach, no SL, single-day bars only)")
    print("=" * 110)
    grid_results = {}
    for htf_label, bucket_min in HTF_OPTIONS:
        for ltf_min in LTF_OPTIONS:
            trades = []
            for trade_date, symbol, side_bias in ROWS:
                side = SIDE[side_bias]
                cached = cache.get((trade_date, symbol))
                entry = entries.get((trade_date, symbol))
                if cached is None or entry is None:
                    continue
                bars_1m, vol_by_ts, orb_h, orb_l = cached
                entry_ts, entry_price = entry
                htf_bars = to_n_min_bars(bars_1m, bucket_min)
                ltf_bars = to_n_min_bars(bars_1m, ltf_min)
                exit_ts, exit_price, reason = trap_target_exit(
                    entry_ts, entry_price, side, bars_1m, htf_bars, ltf_bars, ltf_min)
                trades.append(Trade(trade_date, symbol, side, entry_ts, entry_price, exit_ts, exit_price, reason))
            label = f"HTF={htf_label} LTF={ltf_min}m"
            grid_results[(htf_label, ltf_min)] = summarize(label, trades)
            json_report["variants"][f"{htf_label}_{ltf_min}m"] = {
                "htf": htf_label, "ltf": ltf_min,
                "pf": grid_results[(htf_label, ltf_min)]["pf"],
                "win_pct": grid_results[(htf_label, ltf_min)]["win_pct"],
                "total": grid_results[(htf_label, ltf_min)]["total"],
                "entered": grid_results[(htf_label, ltf_min)]["entered"],
                "target_hits": grid_results[(htf_label, ltf_min)]["target_hits"],
                "trades": [_trade_row(t) for t in sorted(trades, key=lambda x: (x.date, x.symbol))],
            }

    print("\n" + "=" * 110)
    print("SUMMARY vs BASELINE")
    print("=" * 110)
    print(f"{'baseline':>16}  PF={baseline_summary['pf']:6.2f}  win%={baseline_summary['win_pct']:5.1f}  "
          f"total={baseline_summary['total']:+9.2f}")
    best_key = max(grid_results, key=lambda kk: grid_results[kk]["pf"])
    for key, r in sorted(grid_results.items()):
        tag = "  <-- BEST PF" if key == best_key else ""
        print(f"  HTF={key[0]:<6} LTF={key[1]}m  PF={r['pf']:6.2f}  win%={r['win_pct']:5.1f}  "
              f"total={r['total']:+9.2f}  target_hits={r['target_hits']:2d}{tag}")

    import json as _json
    report_path = os.path.join("data", "oi_orb_trap_target_sweep_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        _json.dump(json_report, f, indent=2)
    print(f"\nFull JSON report written to {report_path}")


asyncio.run(main())
