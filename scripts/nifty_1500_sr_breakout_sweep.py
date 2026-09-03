"""
scripts/nifty_1500_sr_breakout_sweep.py — parameter sweep for the R1/S1
breakout backtest (scripts/nifty_1500_sr_breakout_backtest.py), direct
user request (2026-08-27): try several TARGET_PREMIUM_RS and
STRIKE_SEARCH_STEPS combinations in ONE run and report which performs
best on real data, instead of manually re-running the base script with
different hardcoded constants each time.

Reuses every mechanic function from the validated base script directly
(run_day, fetch_option_day, _token_is_valid, etc. -- imported as `bt.*`)
-- never reimplements the S&R/entry/exit logic, per this repo's own
"backtest must drive the real class" discipline (see the base script's
own module docstring for the full mechanic + correction history).

To avoid multiplying REST-call volume on top of the rate-limit exhaustion
already hit on a plain 7-day run of the base script (see its own tenth/
eleventh fixes), every candidate strike is fetched ONCE per day, at the
WIDEST steps value anywhere in the sweep (fetch_all_candidates); every
(target_premium, steps) combination then just re-picks from that
already-fetched in-memory data -- ZERO extra API calls per combo. Net
REST volume for this sweep is therefore the SAME as a single base-script
run at the widest steps value, regardless of how many target/steps
combinations are being compared.

Usage:
    python scripts/nifty_1500_sr_breakout_sweep.py <upstox_token> [--days N]
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, ".")

from data_layer.instrument_registry import REGISTRY
import scripts.nifty_1500_sr_breakout_backtest as bt

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""

TARGET_PREMIUMS = [50.0, 75.0, 100.0, 125.0, 150.0]
STEPS_OPTIONS = [4, 6, 8, 10]
MAX_STEPS = max(STEPS_OPTIONS)


async def fetch_all_candidates(atm: int, side: str, expiry, day: date, token: str,
                                max_steps: int) -> Dict[int, tuple]:
    """Fetch every candidate strike ATM +/- max_steps*STRIKE_STEP ONCE (bounded
    concurrency, same semaphore as the base script). Returns {strike: (bars,
    premium_at_15:00)} for every candidate that returned real data."""
    candidates = [atm + k * bt.STRIKE_STEP for k in range(-max_steps, max_steps + 1)]
    sem = asyncio.Semaphore(bt._STRIKE_SEARCH_CONCURRENCY)

    async def _probe(strike: int):
        async with sem:
            bars = await bt.fetch_option_day(strike, side, expiry, day, token)
        bar_1500 = next((b for b in bars if b.ts.time() >= bt.ENTRY_CHECK_START), None)
        return strike, bars, (bar_1500.close if bar_1500 else None)

    results = await asyncio.gather(*[_probe(s) for s in candidates])
    return {s: (bars, p) for s, bars, p in results if p is not None and bars}


def pick_strike(cache: Dict[int, tuple], atm: int, steps: int,
                 target: float) -> Tuple[Optional[int], List]:
    """Pick whichever cached candidate within +/- steps*STRIKE_STEP of ATM has
    a 15:00 premium closest to `target` -- purely in-memory, no fetch."""
    subset = {s: v for s, v in cache.items() if abs(s - atm) <= steps * bt.STRIKE_STEP}
    if not subset:
        return None, []
    best_strike = min(subset, key=lambda s: abs(subset[s][1] - target))
    return best_strike, subset[best_strike][0]


async def main() -> None:
    if not TOKEN:
        print("Usage: python scripts/nifty_1500_sr_breakout_sweep.py <upstox_token> [--days N]")
        return
    if not await bt._token_is_valid(TOKEN):
        print("ERROR: Upstox token appears INVALID or EXPIRED (checked via /v2/user/profile).\n"
              "       Generate a fresh token and re-run.")
        return

    days_back = 7
    if "--days" in sys.argv:
        days_back = int(sys.argv[sys.argv.index("--days") + 1])

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=days_back * 2 + 5)
    print(f"Fetching NIFTY spot 1-min candles {start} .. {end} (historical) ...")
    spot_bars = bt._rows_to_bars(await bt.fetch_upstox_range_1m(bt.SPOT_KEY, TOKEN, start, end))
    spot_by_day = bt.by_day(spot_bars)

    today = date.today()
    today_rows = await bt.fetch_upstox_intraday_1m(bt.SPOT_KEY, TOKEN)
    if not today_rows:
        today_rows = await bt.fetch_upstox_range_1m(bt.SPOT_KEY, TOKEN, today, today)
    if today_rows:
        spot_by_day[today] = bt._rows_to_bars(today_rows)
        print(f"{today}: {len(spot_by_day[today])} spot bars fetched")

    if not spot_by_day:
        print("No spot data returned -- check token / date range.")
        return

    REGISTRY.load_sync("NIFTY", TOKEN)

    past_days = sorted(d for d in spot_by_day if d < today)[-days_back:]
    trading_days = past_days + ([today] if today in spot_by_day else [])

    results: Dict[Tuple[float, int], List] = {
        (t, s): [] for t in TARGET_PREMIUMS for s in STEPS_OPTIONS
    }

    for day_idx, day in enumerate(trading_days):
        if day_idx > 0:
            await asyncio.sleep(2.0)   # spread request rate across days -- see base script's own fix
        day_spot = spot_by_day[day]
        bar_1500 = next((b for b in day_spot if b.ts.time() >= bt.ENTRY_CHECK_START), None)
        if bar_1500 is None:
            print(f"{day}: no 15:00 spot bar -- skip")
            continue
        atm = round(bar_1500.close / bt.STRIKE_STEP) * bt.STRIKE_STEP

        expiry = REGISTRY.get_active_expiry_strict("NIFTY", from_date=day)
        if expiry is None:
            print(f"{day}: ATM={atm} -- no active expiry resolvable -- skip")
            continue

        ce_cache = await fetch_all_candidates(atm, "CE", expiry, day, TOKEN, MAX_STEPS)
        pe_cache = await fetch_all_candidates(atm, "PE", expiry, day, TOKEN, MAX_STEPS)
        if not ce_cache or not pe_cache:
            print(f"{day}: ATM={atm} -- no CE/PE candidate data at all -- skip")
            continue
        print(f"{day}: ATM={atm} -- fetched {len(ce_cache)} CE / {len(pe_cache)} PE candidates "
              f"(reused across all {len(TARGET_PREMIUMS)}x{len(STEPS_OPTIONS)} combos, no extra fetches)")

        for target in TARGET_PREMIUMS:
            for steps in STEPS_OPTIONS:
                ce_strike, ce_bars = pick_strike(ce_cache, atm, steps, target)
                pe_strike, pe_bars = pick_strike(pe_cache, atm, steps, target)
                if ce_strike is None or pe_strike is None:
                    continue
                day_trades, _diag = bt.run_day(day, ce_strike, pe_strike, ce_bars, pe_bars)
                results[(target, steps)].extend(day_trades)

    print("\n=== SWEEP RESULTS ===")
    print(f"{'target_Rs':>10} {'steps':>6} {'n':>4} {'win%':>7} {'PF':>7} {'net_pts':>9} {'net_Rs':>9}")
    best: Optional[Tuple[Tuple[float, int], float]] = None
    for (target, steps), trades in sorted(results.items()):
        if not trades:
            print(f"{target:>10.0f} {steps:>6}    0       -       -         -         -")
            continue
        n = len(trades)
        wins = [t for t in trades if t.pnl_pts > 0]
        losses = [t for t in trades if t.pnl_pts <= 0]
        gross_win = sum(t.pnl_pts for t in wins)
        gross_loss = -sum(t.pnl_pts for t in losses)
        pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
        net_pts = sum(t.pnl_pts for t in trades)
        net_rs = net_pts * bt.LOT_SIZE
        print(f"{target:>10.0f} {steps:>6} {n:>4} {100.0*len(wins)/n:>6.1f}% {pf:>7.2f} "
              f"{net_pts:>9.2f} {net_rs:>9.0f}")
        if best is None or net_rs > best[1]:
            best = ((target, steps), net_rs)

    if best:
        (best_target, best_steps), best_rs = best
        print(f"\nBest by net Rs: target_Rs={best_target:.0f} steps={best_steps} "
              f"-> net=Rs{best_rs:.0f}")
    else:
        print("\nNo combo produced any trades over this window.")


if __name__ == "__main__":
    asyncio.run(main())
