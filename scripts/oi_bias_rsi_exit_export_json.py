"""
scripts/oi_bias_rsi_exit_export_json.py -- builds the full per-stock detail
JSON (strike, option type, OI table, stock-price StochRSI walk at the
OPTIMIZED entry/exit timeframe+lengths, and the real OPTION PREMIUM
bar-by-bar walk from entry to exit) for the OI-RSI Trace artifact, entirely
from the local cache (data/oi_bias_rsi_exit_cache/, zero network calls) plus
the previously-fetched OI-table numbers (data/oi_bias_rsi_exit_oi_table.json,
independent of the entry/exit optimization -- OI is read at 9:15/9:20/9:25
regardless of what StochRSI timeframe/lengths are in use).

Usage: python scripts/oi_bias_rsi_exit_export_json.py > out.json
"""
from __future__ import annotations

import json
import sys
from datetime import timedelta

sys.path.insert(0, ".")

from strategies.core.candle_indicators import to_n_min_bars_market_anchored
from strategies.oi_bias_rsi_exit.detector import (
    compute_stoch_rsi_double_smoothed, check_entry_state, check_exit_cross,
)
from scripts.oi_bias_rsi_exit_backtest import (
    ENTRY_SCAN_START, ENTRY_TIMEFRAME_MIN, ENTRY_STOCH_RSI_LENGTHS,
    EXIT_TIMEFRAME_MIN, EXIT_STOCH_RSI_LENGTHS,
)
from scripts.oi_bias_rsi_exit_optimize import load_cache, _price_near

OI_TABLE_PATH = "data/oi_bias_rsi_exit_oi_table.json"


def main() -> None:
    rows = load_cache()
    oi_by_key = {}
    try:
        with open(OI_TABLE_PATH) as f:
            for r in json.load(f):
                oi_by_key[(r["symbol"], r["date"])] = r
    except FileNotFoundError:
        pass

    out = []
    for row in rows:
        bars_e = to_n_min_bars_market_anchored(row.stock_bars, ENTRY_TIMEFRAME_MIN)
        bars_x = to_n_min_bars_market_anchored(row.stock_bars, EXIT_TIMEFRAME_MIN)
        closes_e = [b.close for b in bars_e]
        closes_x = [b.close for b in bars_x]
        k_e, d_e = compute_stoch_rsi_double_smoothed(closes_e, *ENTRY_STOCH_RSI_LENGTHS)
        k_x, d_x = compute_stoch_rsi_double_smoothed(closes_x, *EXIT_STOCH_RSI_LENGTHS)

        entry_idx = next(
            (i for i, b in enumerate(bars_e)
             if b.ts.date() == row.trade_date and b.ts.time() >= ENTRY_SCAN_START
             and check_entry_state(k_e[i], d_e[i], row.bias)),
            None)
        if entry_idx is None:
            continue
        entry_ts = bars_e[entry_idx].ts

        exit_ts, exit_reason = None, "eod"
        for i in range(1, len(bars_x)):
            bar = bars_x[i]
            if bar.ts.date() != row.trade_date:
                continue
            bucket_close = bar.ts + timedelta(minutes=EXIT_TIMEFRAME_MIN)
            if bucket_close <= entry_ts:
                continue
            if check_exit_cross(k_x[i - 1], d_x[i - 1], k_x[i], d_x[i], row.bias):
                exit_ts, exit_reason = bucket_close, "stoch_d_cross_exit"
                break
        if exit_ts is None:
            day_bars = [b for b in bars_e if b.ts.date() == row.trade_date]
            exit_ts = day_bars[-1].ts

        entry_price = _price_near(row.option_bars, entry_ts)
        exit_price = _price_near(row.option_bars, exit_ts)
        if entry_price is None or exit_price is None:
            continue
        pnl_pts = exit_price - entry_price

        # Real option premium bar-by-bar walk, entry->exit, at the entry tf.
        prem_bars = to_n_min_bars_market_anchored(row.option_bars, ENTRY_TIMEFRAME_MIN)
        held_bars = [b for b in prem_bars if entry_ts <= b.ts <= exit_ts]
        premium_walk = [
            {"time": b.ts.strftime("%H:%M"), "open": b.open, "high": b.high, "low": b.low, "close": b.close}
            for b in held_bars
        ]
        peak_real = max((b.high for b in held_bars), default=None)
        low_real = min((b.low for b in held_bars), default=None)

        def rows_for(bars, k, d, bias, mode):
            out_rows = []
            day_idx = [i for i, b in enumerate(bars) if b.ts.date() == row.trade_date]
            if mode == "entry":
                show = [i for i in day_idx if i <= entry_idx] + ([day_idx[-1]] if day_idx else [])
            else:
                show = [i for i, b in enumerate(bars)
                        if (b.ts.date() == row.trade_date and (b.ts + timedelta(minutes=EXIT_TIMEFRAME_MIN)) > entry_ts)
                        or (day_idx and i == day_idx[0] - 1)]
            seen = set()
            for i in sorted(set(show)):
                if i in seen or i < 0 or i >= len(bars):
                    continue
                seen.add(i)
                b = bars[i]
                kk, dd = k[i], d[i]
                held = None
                if kk is not None and dd is not None:
                    held = (dd > kk) if bias == "bearish" else (kk > dd)
                marker = ""
                if mode == "entry" and i == entry_idx:
                    marker = "ENTRY"
                elif day_idx and i == day_idx[-1]:
                    marker = "EOD"
                cross = False
                if mode == "exit" and i > 0:
                    cross = check_exit_cross(k[i - 1], d[i - 1], kk, dd, bias)
                out_rows.append({
                    "time": b.ts.strftime("%Y-%m-%d %H:%M"), "close": b.close,
                    "k": kk, "d": dd, "held": ("YES" if held else ("no" if held is not None else "-")),
                    "marker": marker, "cross": cross,
                })
            return out_rows

        oi = oi_by_key.get((row.symbol, row.trade_date.isoformat()), {})

        out.append({
            "symbol": row.symbol, "date": row.trade_date.isoformat(), "bias": row.bias,
            "option_type": row.option_type, "strike": row.strike,
            "entry_tf": ENTRY_TIMEFRAME_MIN, "entry_lengths": list(ENTRY_STOCH_RSI_LENGTHS),
            "exit_tf": EXIT_TIMEFRAME_MIN, "exit_lengths": list(EXIT_STOCH_RSI_LENGTHS),
            "atm_call": oi.get("atm_call"), "atm_put": oi.get("atm_put"),
            "otm_call": oi.get("otm_call"), "otm_put": oi.get("otm_put"),
            "open_915": oi.get("open_915"), "strike_step": oi.get("strike_step"),
            "entry_ts": entry_ts.strftime("%H:%M"), "exit_ts": exit_ts.strftime("%H:%M"),
            "entry_price": entry_price, "exit_price": exit_price, "exit_reason": exit_reason,
            "pnl_pts": pnl_pts, "pnl_rs": pnl_pts * row.lot, "lot": row.lot,
            "peak_real": peak_real, "low_real": low_real,
            "premium_walk": premium_walk,
            "entry_rows": rows_for(bars_e, k_e, d_e, row.bias, "entry"),
            "exit_rows": rows_for(bars_x, k_x, d_x, row.bias, "exit"),
        })

    print(json.dumps(out))


if __name__ == "__main__":
    main()
