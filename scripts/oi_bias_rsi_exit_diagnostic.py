"""
scripts/oi_bias_rsi_exit_diagnostic.py -- real-data verification dump for the
2 trades produced by scripts/oi_bias_rsi_exit_backtest.py, per direct user
request (2026-09-26): "provide ... so i will see the chart and check that
all your values which you have for backtest are working correctly or not".

Prints, per (symbol, trade_date) row in the manual-bias CSV:
  1. REAL ATM/OTM Call+Put open interest at 09:15/09:20/09:25, fetched fresh
     from Upstox's own 1-min historical candle endpoint for each of the 4
     relevant contracts (reuses strategies.oi_bias_breakout.detector.
     freeze_signal_strikes for the ATM/OTM strikes, resolve_contract for
     each real listed contract) -- then runs the REAL, already-tested
     strategies.oi_bias_breakout.detector.classify_oi_bias on those real
     numbers and compares it against the bias this backtest assumed.
     NOTE 2026-09-26: this codebase's own older notes (OI-Flow, Liquidity
     Trap sections of CLAUDE.md) claim Upstox's historical-candle API
     hardcodes OI to 0 -- confirmed, empirically, against POLICYBZR PE1160's
     real 2026-09-25 data, that this is NO LONGER true for F&O option
     contracts (334,250 -> 880,600 rising through the morning, genuinely
     varying minute to minute) -- that claim predates data_layer/
     historical_candles.py's 2026-09-16 fix (_parse_candles now reads
     Upstox's real 7th OI column) built for fetch_upstox_prev_day_last_
     tick_oi's futures-OI-regime gate. Stale documentation, not stale data.
  2. The 5-min StochRSI K/D value on every real bar from market open up to
     and including the entry bar (to verify the state was genuinely K>D
     going into entry) and the EOD bar.
  3. The 1-hour StochRSI K/D value on every real bar from entry to EOD (to
     verify why/whether the D-cross-above-K exit ever came close to firing).
  4. The real post-entry high/low the option's own premium reached, vs. the
     actual EOD exit price ("how far did it go").

Reuses scripts.oi_bias_rsi_exit_backtest's own _run_one (same real fetch +
same entry/exit mechanic already validated there) rather than re-deriving
any of it -- this script only ADDS the OI-side verification and prints the
diagnostic tables _run_one already collects onto its Trade object but never
displayed.

Usage: python scripts/oi_bias_rsi_exit_diagnostic.py <upstox_token>
       [--csv path/to/manual_bias.csv]
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, time as dtime, timedelta
from typing import Optional

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
from strategies.oi_bias_breakout.detector import freeze_signal_strikes, classify_oi_bias
from strategies.oi_orb_screener import stock_resolve
from scripts.oi_bias_rsi_exit_backtest import (
    load_manual_bias_rows, DEFAULT_CSV, _token_is_valid, _run_one, Trade, check_exit_cross,
)

_SNAPSHOT_TIMES = (dtime(9, 15), dtime(9, 20), dtime(9, 25))


async def _fetch_oi_snapshots(symbol: str, strike: int, option_type: str,
                               day: date, token: str) -> "dict[dtime, Optional[float]]":
    """Real per-minute OI for one contract at 09:15/09:20/09:25 -- {} values
    are None if that contract couldn't be resolved or has no data that
    minute (never fabricated)."""
    out: "dict[dtime, Optional[float]]" = {t: None for t in _SNAPSHOT_TIMES}
    contract = await asyncio.to_thread(stock_resolve.resolve_contract, symbol, strike, option_type, ("upstox",))
    if contract is None:
        return out
    rows = await fetch_upstox_range_1m(contract.upstox_key, token, day, day)
    by_time = {}
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            from datetime import datetime as _dt
            ts = _dt.fromisoformat(ts)
        by_time[ts.time()] = r.get("oi")
    for t in _SNAPSHOT_TIMES:
        out[t] = by_time.get(t)
    if out[dtime(9, 15)] is None:
        # Direct user instruction: an illiquid contract can have no real
        # print in the exact 09:15 minute at all -- fall back to 09:16
        # rather than reporting a bare "n/a" for the whole leg. Only ever
        # applied to 9:15 (9:20/9:25 are never missing in the same way in
        # practice, and the frozen spec's own 9:20->9:25 gate doesn't need
        # this fallback anyway).
        out[dtime(9, 15)] = by_time.get(dtime(9, 16))
    return out, contract.strike


async def print_oi_table(symbol: str, day: date, token: str) -> None:
    stock_key = stock_resolve.resolve_eq_instrument_key(symbol)
    stock_rows = await fetch_upstox_range_1m(stock_key, token, day, day)
    from datetime import datetime as _dt
    bar_915 = next((r for r in stock_rows if (_dt.fromisoformat(r["ts"]) if isinstance(r["ts"], str) else r["ts"]).time() == dtime(9, 15)), None)
    if bar_915 is None:
        print(f"  [OI table] no real 09:15 stock bar for {symbol} -- cannot resolve ATM/OTM strikes.")
        return
    open_915 = bar_915["open"]
    strike_step = stock_resolve.resolve_strike_step_for_price(symbol, open_915)
    strikes = freeze_signal_strikes(open_915_price=open_915, strike_step=strike_step)
    REGISTRY.load_sync(symbol, token)

    (atm_call, atm_call_strike) = await _fetch_oi_snapshots(symbol, strikes.atm, "CE", day, token)
    (atm_put, atm_put_strike) = await _fetch_oi_snapshots(symbol, strikes.atm, "PE", day, token)
    (otm_call, otm_call_strike) = await _fetch_oi_snapshots(symbol, strikes.otm_call, "CE", day, token)
    (otm_put, otm_put_strike) = await _fetch_oi_snapshots(symbol, strikes.otm_put, "PE", day, token)

    def _fmt(v):
        return f"{v:,.0f}" if v is not None else "n/a"

    print(f"  [OI table] {symbol} {day} -- real 09:15 open={open_915:.2f}, strike_step={strike_step:g}, "
          f"ATM={strikes.atm:g} (resolved real strike {atm_call_strike})")
    print(f"  {'Contract':<22}{'OI@09:15':>14}{'OI@09:20':>14}{'OI@09:25':>14}")
    print(f"  {'ATM Call ' + str(atm_call_strike):<22}"
          f"{_fmt(atm_call[dtime(9,15)]):>14}{_fmt(atm_call[dtime(9,20)]):>14}{_fmt(atm_call[dtime(9,25)]):>14}")
    print(f"  {'ATM Put ' + str(atm_put_strike):<22}"
          f"{_fmt(atm_put[dtime(9,15)]):>14}{_fmt(atm_put[dtime(9,20)]):>14}{_fmt(atm_put[dtime(9,25)]):>14}")
    print(f"  {'OTM Call ' + str(otm_call_strike):<22}"
          f"{_fmt(otm_call[dtime(9,15)]):>14}{_fmt(otm_call[dtime(9,20)]):>14}{_fmt(otm_call[dtime(9,25)]):>14}")
    print(f"  {'OTM Put ' + str(otm_put_strike):<22}"
          f"{_fmt(otm_put[dtime(9,15)]):>14}{_fmt(otm_put[dtime(9,20)]):>14}{_fmt(otm_put[dtime(9,25)]):>14}")

    real_bias = classify_oi_bias(
        otm_call_oi_920=otm_call[dtime(9, 20)], otm_call_oi_925=otm_call[dtime(9, 25)],
        atm_put_oi_920=atm_put[dtime(9, 20)], atm_put_oi_925=atm_put[dtime(9, 25)],
        otm_put_oi_920=otm_put[dtime(9, 20)], otm_put_oi_925=otm_put[dtime(9, 25)],
        atm_call_oi_920=atm_call[dtime(9, 20)], atm_call_oi_925=atm_call[dtime(9, 25)],
    )
    print(f"  classify_oi_bias() on these REAL numbers -> {real_bias}")


def print_kd_table(t: Trade) -> None:
    """t.bars_5m/bars_1h/k5/d5/k60/d60 are all STOCK PRICE series (2026-09-26
    pivot) -- 'Close' below is the stock's own close, verifiable directly
    against a real stock chart. Only entry_price/exit_price are the option's
    own premium.

    2026-09-26 direct user correction: entry/exit are MIRRORED by bias on
    the stock's own price -- bullish wants K>D (bullish momentum), bearish
    wants D>K (bearish momentum, since a bought PE profits when the stock
    FALLS). This table's own state/cross columns mirror the same way so
    they show the condition that actually governed this trade, not always
    the bullish one."""
    entry_label = "K>D?" if t.bias == "bullish" else "D>K?"
    print(f"\n  [5-min K/D on STOCK price, {t.symbol}, bias={t.bias}, entry at {t.entry_ts.strftime('%H:%M')}]")
    print(f"  {'Time':<8}{'StockClose':>12}{'K':>10}{'D':>10}{entry_label:>8}")
    day_idx = [i for i, b in enumerate(t.bars_5m) if b.ts.date() == t.trade_date]
    show = [i for i in day_idx if i <= t.entry_idx_5m] + [i for i in day_idx if i == day_idx[-1]]
    seen = set()
    for i in show:
        if i in seen:
            continue
        seen.add(i)
        b = t.bars_5m[i]
        k, d = t.k5[i], t.d5[i]
        marker = " <- ENTRY" if i == t.entry_idx_5m else (" <- EOD" if i == day_idx[-1] else "")
        if k is None or d is None:
            state = "-"
        else:
            held = (d > k) if t.bias == "bearish" else (k > d)
            state = "YES" if held else "no"
        print(f"  {b.ts.strftime('%H:%M'):<8}{b.close:>12.2f}"
              f"{('%.2f' % k) if k is not None else 'n/a':>10}"
              f"{('%.2f' % d) if d is not None else 'n/a':>10}{state:>8}{marker}")

    exit_state_label = "D>K state?" if t.bias == "bullish" else "K>D state?"
    print(f"\n  [1-hour K/D on STOCK price, {t.symbol}, bias={t.bias}, from entry to EOD]")
    print(f"  {'Date/Time':<17}{'StockClose':>12}{'K':>10}{'D':>10}{exit_state_label:>12}{'Cross?':>8}")
    # A bar whose bucket STARTS before entry but CLOSES (start+60min) at/after
    # entry is still a legitimate, no-look-ahead exit check point the instant
    # it closes -- matches the corrected exit loop in _run_one (an earlier
    # version of both this table and that loop excluded it via `b.ts >=
    # entry_ts`, silently skipping the very first real exit-check chance).
    day_idx = [i for i, b in enumerate(t.bars_1h)
               if b.ts.date() == t.trade_date and (b.ts + timedelta(minutes=60)) > t.entry_ts]

    def exit_state(k, d):
        if k is None or d is None:
            return "-"
        held = (k > d) if t.bias == "bearish" else (d > k)
        return "YES" if held else "no"

    if day_idx and day_idx[0] > 0:
        pb = t.bars_1h[day_idx[0] - 1]
        pk, pd = t.k60[day_idx[0] - 1], t.d60[day_idx[0] - 1]
        print(f"  {pb.ts.strftime('%Y-%m-%d %H:%M'):<17}{pb.close:>12.2f}"
              f"{('%.2f' % pk) if pk is not None else 'n/a':>10}"
              f"{('%.2f' % pd) if pd is not None else 'n/a':>10}{exit_state(pk, pd):>12}{'':>8}  <- prior bar (context)")
    for i in day_idx:
        b = t.bars_1h[i]
        k, d = t.k60[i], t.d60[i]
        cross = "CROSS!" if i > 0 and check_exit_cross(t.k60[i - 1], t.d60[i - 1], k, d, t.bias) else ""
        marker = " <- EOD" if i == day_idx[-1] else ""
        print(f"  {b.ts.strftime('%Y-%m-%d %H:%M'):<17}{b.close:>12.2f}"
              f"{('%.2f' % k) if k is not None else 'n/a':>10}"
              f"{('%.2f' % d) if d is not None else 'n/a':>10}{exit_state(k, d):>12}{cross:>8}{marker}")

    hi, hi_ts, lo, lo_ts = t.post_entry_extreme()
    print(f"\n  [Post-entry extreme STOCK PRICE, {t.symbol}] "
          f"HIGH={hi:.2f} @ {hi_ts.strftime('%H:%M')}   LOW={lo:.2f} @ {lo_ts.strftime('%H:%M')}   "
          f"(actual OPTION EXIT PREMIUM={t.exit_price:.2f} @ {t.exit_ts.strftime('%H:%M')}, "
          f"reason={t.exit_reason})")


async def main() -> None:
    token = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else ""
    if not token:
        print("Usage: python scripts/oi_bias_rsi_exit_diagnostic.py <upstox_token> [--csv path]")
        return
    if not await _token_is_valid(token):
        print("ERROR: Upstox token appears INVALID or EXPIRED.")
        return

    csv_path = DEFAULT_CSV
    if "--csv" in sys.argv:
        csv_path = sys.argv[sys.argv.index("--csv") + 1]
    rows = load_manual_bias_rows(csv_path)

    for row in rows:
        print(f"\n{'=' * 70}\n{row.symbol} {row.trade_date} (assumed bias={row.bias})\n{'=' * 70}")
        await print_oi_table(row.symbol, row.trade_date, token)
        t = await _run_one(row, token)
        if t is None:
            print("  (no trade produced -- see backtest script's own skip reason above)")
            continue
        print_kd_table(t)


if __name__ == "__main__":
    asyncio.run(main())
