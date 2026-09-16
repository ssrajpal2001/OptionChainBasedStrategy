"""
scripts/oi_orb_sl_tf_candletype_sweep.py

Direct user spec, 2026-09-16: rejected re-enabling the disabled Rs2000/lot
hard risk cap ("NO NO NO") -- wants the SL mechanic itself optimized
instead: "we can try other price action concept or optimise the 20min
logic to HTF with normal or HA candle structure." Prompted by two real
worst-case losses in the 7-day frozen-mechanic backtest: POWERINDIA
(-84.80, SL never fired all session) and SOLARINDS 09-07 (-55.95, SL
fired but not until 2+ hours after entry, ~7.5% already lost by then).

Sweeps the SAME real adverse-close SL concept (candle CLOSE >=gap% on
the wrong side of session VWAP + matching shape -- no opposing wick,
reusing OiOrbScreenerStrategy._ha_vwap_close_sl_adverse directly, not
reimplemented) across two axes:
  CANDLE TYPE: HA (Heikin-Ashi, current live default) vs NORMAL (plain
  OHLC candles, no HA smoothing)
  TIMEFRAME:   10 / 15 / 20 (current live default) / 30 / 45 / 60 min

against the 26 REAL trades already found+validated by the 7-day frozen-
mechanic backtest (their real symbol/side/date/entry_ts/entry_price
taken as given -- only the exit side is re-swept here). Real per-day
equity bars + real option premium history fetched once per trade and
cached, then all 12 combos evaluated in-memory for speed.

MUST run on EC2 (real Upstox2 access token + real historical data).

Usage: python scripts/oi_orb_sl_tf_candletype_sweep.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import to_heikin_ashi, to_n_min_bars_market_anchored
from strategies.oi_orb_screener import stock_resolve
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy
from strategies.oi_orb_screener.screener import VwapState

EOD_TIME = "15:15"
TF_GRID = [10, 15, 20, 30, 45, 60]
CANDLE_TYPES = ["HA", "NORMAL"]

# The 26 real trades already found by the 7-day frozen-mechanic backtest:
# (trade_date, symbol, side, entry_ts "%H:%M", entry_option_price)
KNOWN_TRADES = [
    ("2026-09-01", "LTF", "PUT", "14:33", 11.70),
    ("2026-09-01", "MPHASIS", "CALL", "14:54", 87.30),
    ("2026-09-01", "POLYCAB", "PUT", "11:29", 242.45),
    ("2026-09-02", "BSE", "PUT", "14:25", 117.40),
    ("2026-09-02", "EICHERMOT", "PUT", "13:27", 148.00),
    ("2026-09-02", "HEROMOTOCO", "PUT", "14:28", 107.25),
    ("2026-09-02", "SWIGGY", "PUT", "12:24", 9.35),
    ("2026-09-03", "APLAPOLLO", "PUT", "14:24", 53.40),
    ("2026-09-03", "GODREJCP", "PUT", "09:17", 18.80),
    ("2026-09-03", "MAHABANK", "CALL", "12:28", 3.29),
    ("2026-09-03", "SBICARD", "CALL", "10:33", 21.80),
    ("2026-09-03", "SOLARINDS", "CALL", "12:27", 800.05),
    ("2026-09-04", "ANGELONE", "CALL", "09:47", 11.35),
    ("2026-09-04", "ATHERENERG", "PUT", "14:54", 61.30),
    ("2026-09-04", "HAVELLS", "PUT", "09:31", 28.00),
    ("2026-09-04", "KEI", "PUT", "09:27", 192.55),
    ("2026-09-04", "POLYCAB", "PUT", "09:17", 218.70),
    ("2026-09-07", "MANAPPURAM", "PUT", "14:22", 8.30),
    ("2026-09-07", "SOLARINDS", "CALL", "11:17", 742.95),
    ("2026-09-07", "WIPRO", "PUT", "15:11", 5.58),
    ("2026-09-08", "BLUESTARCO", "PUT", "11:13", 50.00),
    ("2026-09-08", "GVT&D", "CALL", "09:27", 190.85),
    ("2026-09-08", "POWERINDIA", "PUT", "10:13", 989.80),
    ("2026-09-09", "BIOCON", "CALL", "09:25", 10.35),
    ("2026-09-09", "COFORGE", "PUT", "09:23", 52.10),
    ("2026-09-09", "MUTHOOTFIN", "PUT", "12:02", 57.05),
]
# Real EOD baseline P&L (entry+SL(20min,HA) only, already validated) for comparison.
BASELINE_PNL = {
    ("2026-09-01", "LTF"): -0.05, ("2026-09-01", "MPHASIS"): 6.70, ("2026-09-01", "POLYCAB"): 134.55,
    ("2026-09-02", "BSE"): -2.15, ("2026-09-02", "EICHERMOT"): -26.50, ("2026-09-02", "HEROMOTOCO"): -4.25,
    ("2026-09-02", "SWIGGY"): -1.30, ("2026-09-03", "APLAPOLLO"): 3.85, ("2026-09-03", "GODREJCP"): -1.40,
    ("2026-09-03", "MAHABANK"): -0.12, ("2026-09-03", "SBICARD"): -1.50, ("2026-09-03", "SOLARINDS"): 70.30,
    ("2026-09-04", "ANGELONE"): -1.30, ("2026-09-04", "ATHERENERG"): 6.45, ("2026-09-04", "HAVELLS"): 6.50,
    ("2026-09-04", "KEI"): 52.00, ("2026-09-04", "POLYCAB"): 7.05, ("2026-09-07", "MANAPPURAM"): -0.15,
    ("2026-09-07", "SOLARINDS"): -55.95, ("2026-09-07", "WIPRO"): 0.29, ("2026-09-08", "BLUESTARCO"): 7.00,
    ("2026-09-08", "GVT&D"): 61.55, ("2026-09-08", "POWERINDIA"): -84.80, ("2026-09-09", "BIOCON"): -2.20,
    ("2026-09-09", "COFORGE"): -13.70, ("2026-09-09", "MUTHOOTFIN"): -0.05,
}


def _access_token():
    creds = ClientDB().get_feeder_creds_sync("upstox2")
    if creds and creds.get("access_token"):
        return creds["access_token"]
    raise RuntimeError("No upstox2 feeder access_token found -- run this on EC2.")


def _to_bars(rows):
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                        low=float(r["low"]), close=float(r["close"])))
    return out


async def _fetch_trade_cache(token, trade_date_str, symbol, side, entry_hhmm, entry_opt_price):
    trade_date = date.fromisoformat(trade_date_str)
    entry_ts = datetime.combine(trade_date, datetime.strptime(entry_hhmm, "%H:%M").time(), tzinfo=IST)
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return None
    day_rows = await hc.fetch_upstox_range_1m(eq_key, token, trade_date, trade_date)
    day_bars = _to_bars(day_rows)
    if not day_bars:
        return None
    entry_spot_candidates = [b for b in day_bars if b.ts <= entry_ts]
    if not entry_spot_candidates:
        return None
    entry_spot = entry_spot_candidates[-1].close

    opt_type = "CE" if side == "CALL" else "PE"
    contract = await stock_resolve.resolve_contract_async(symbol, entry_spot, opt_type)
    if contract is None:
        return None
    opt_rows = await hc.fetch_upstox_range_1m(contract.upstox_key, token, trade_date, trade_date)
    opt_bars = _to_bars(opt_rows)
    if not opt_bars:
        return None

    eod_ts = datetime.combine(trade_date, datetime.strptime(EOD_TIME, "%H:%M").time(), tzinfo=IST)
    return {"trade_date": trade_date, "symbol": symbol, "side": side, "entry_ts": entry_ts,
            "entry_opt_price": entry_opt_price, "day_bars": day_bars, "opt_bars": opt_bars, "eod_ts": eod_ts}


def _sl_exit(cache, candle_type: str, tf_min: int):
    symbol, side = cache["symbol"], cache["side"]
    day_bars, entry_ts, eod_ts = cache["day_bars"], cache["entry_ts"], cache["eod_ts"]
    src_bars = to_heikin_ashi(day_bars) if candle_type == "HA" else day_bars
    tf_bars = to_n_min_bars_market_anchored(src_bars, tf_min)

    vwap_state = VwapState()
    vwap_at_minute = {}
    for b in day_bars:
        typical = (b.high + b.low + b.close) / 3.0
        vwap_state.update(symbol, typical, 1.0)
        v = vwap_state.current(symbol)
        if v is not None:
            vwap_at_minute[b.ts.replace(second=0, microsecond=0)] = v
    sorted_minutes = sorted(vwap_at_minute.keys())

    def _vwap_as_of(bucket_end):
        eligible = [ts for ts in sorted_minutes if ts < bucket_end]
        return vwap_at_minute[eligible[-1]] if eligible else None

    entry_floor = entry_ts.replace(second=0, microsecond=0)
    for tb in tf_bars:
        bucket_end = tb.ts + timedelta(minutes=tf_min)
        if bucket_end <= entry_floor:
            continue
        if bucket_end > eod_ts:
            break
        vwap_now = _vwap_as_of(bucket_end)
        if vwap_now is None or vwap_now <= 0:
            continue
        if OiOrbScreenerStrategy._ha_vwap_close_sl_adverse(tb, vwap_now, side):
            return bucket_end
    return eod_ts


def _pnl_at(cache, exit_ts):
    exit_candidates = [b for b in cache["opt_bars"] if b.ts <= exit_ts]
    if not exit_candidates:
        return None
    exit_price = exit_candidates[-1].close
    return round(exit_price - cache["entry_opt_price"], 2)


async def main():
    token = _access_token()
    print("=" * 130)
    print("OI-ORB Screener -- SL candle-type x timeframe sweep, real 26-trade sample")
    print(f"Grid: candle_type in {CANDLE_TYPES}  x  tf_min in {TF_GRID}  ({len(CANDLE_TYPES) * len(TF_GRID)} combos)")
    print("Baseline (live default, HA/20min) already known: +160.82 pts total, POWERINDIA -84.80, SOLARINDS(09-07) -55.95")
    print("=" * 130)

    print("\nFetching + caching real data per trade (26 trades, once each)...")
    caches = []
    for trade_date_str, symbol, side, entry_hhmm, entry_opt_price in KNOWN_TRADES:
        c = await _fetch_trade_cache(token, trade_date_str, symbol, side, entry_hhmm, entry_opt_price)
        if c is not None:
            caches.append(c)
        else:
            print(f"  {trade_date_str} {symbol}: FAILED to cache real data")
    print(f"Cached {len(caches)}/{len(KNOWN_TRADES)} trades.\n")

    grid = {}
    for candle_type in CANDLE_TYPES:
        for tf_min in TF_GRID:
            total, worst = 0.0, (None, 0.0)
            for c in caches:
                exit_ts = _sl_exit(c, candle_type, tf_min)
                pnl = _pnl_at(c, exit_ts)
                if pnl is None:
                    continue
                total += pnl
                if pnl < worst[1]:
                    worst = (f"{c['trade_date'].isoformat()} {c['symbol']}", pnl)
            grid[(candle_type, tf_min)] = (total, worst)

    print(f"{'candle':>8} {'tf_min':>7} {'total pnl':>12} {'worst single trade':>28}")
    print("-" * 62)
    for (candle_type, tf_min), (total, worst) in sorted(grid.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        marker = "  <- current live default" if (candle_type, tf_min) == ("HA", 20) else ""
        print(f"{candle_type:>8} {tf_min:7d} {total:12.2f}   {worst[0]} ({worst[1]:+.2f}){marker}")

    best = max(grid.items(), key=lambda kv: kv[1][0])
    print("\n" + "=" * 130)
    (bct, btf), (btotal, bworst) = best
    print(f"BEST BY TOTAL P&L: candle_type={bct} tf_min={btf} -> {btotal:+.2f} pts, worst single trade: "
          f"{bworst[0]} ({bworst[1]:+.2f})")
    print("CAVEAT: n=26 real trades, entry side unchanged from the already-validated mechanic -- only the "
          "exit/SL side is swept here. A combo that wins on this specific sample isn't automatically the "
          "right long-term choice; look at whether it's winning broadly or just fixing these 2 known outliers.")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
