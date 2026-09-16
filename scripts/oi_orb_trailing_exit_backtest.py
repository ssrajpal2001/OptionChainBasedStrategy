"""
scripts/oi_orb_trailing_exit_backtest.py

Direct user follow-up, after the reversal-level check found NONE of
POWERINDIA/EICHERMOT/SOLARINDS's reversals cleanly aligned with any
real prior-day/week/month/Fib level: "yes go ahead and chk need
optimisation. also there is my S&R code we have have TSL as S1 for
long and R1 for short in diff tframes which need to be optimised."

Backtests TWO independent candidate exit mechanics against the same 19
real traded stocks, real data throughout:

  A) GIVE-BACK TRAILING STOP (on the OPTION's own premium): once the
     position is in real profit, track the running peak premium; exit
     the instant price gives back giveback_pct of the gain from that
     peak. Sweeps giveback_pct in [20%, 30%, 40%, 50%].

  B) REAL S&R STRUCTURAL TSL (on the UNDERLYING SPOT, per direct user
     spec: "S1 for long and R1 for short"): reuses
     strategies.core.support_resistance.SupportResistanceCalculator --
     the SAME real class strategies/cag_straddle already drives live,
     not reimplemented. Feeds real N-min spot bars from market open;
     once S1 is established (CALL) or R1 is established (PUT), a
     spot close breaching it triggers the exit. Sweeps timeframe in
     [3, 5, 15] minutes.

Both race the already-known real 20-min VWAP-close hard SL and EOD
(15:15) -- whichever fires first, chronologically, wins, exactly like
the already-validated baseline.

MUST run on EC2 (real Upstox account access tokens + real historical
range data).

Usage: python scripts/oi_orb_trailing_exit_backtest.py
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
from strategies.core.candle_indicators import to_heikin_ashi, to_n_min_bars_market_anchored, to_n_min_bars_dateaware
from strategies.core.support_resistance import SupportResistanceCalculator
from strategies.oi_orb_screener import stock_resolve
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy, _VWAP_SL_TF_MIN
from strategies.oi_orb_screener.screener import VwapState

EOD_TIME = "15:15"
GIVEBACK_GRID = [0.20, 0.30, 0.40, 0.50]
SR_TF_GRID = [3, 5, 15]

KNOWN_TRADES = [
    ("2026-09-01", "LTF", "PUT", "14:33", 11.70),
    ("2026-09-01", "POLYCAB", "PUT", "11:29", 242.45),
    ("2026-09-02", "BSE", "PUT", "14:25", 117.40),
    ("2026-09-02", "EICHERMOT", "PUT", "13:27", 148.00),
    ("2026-09-02", "HEROMOTOCO", "PUT", "14:28", 107.25),
    ("2026-09-02", "SWIGGY", "PUT", "12:24", 9.35),
    ("2026-09-03", "APLAPOLLO", "PUT", "14:24", 53.40),
    ("2026-09-03", "GODREJCP", "PUT", "09:17", 18.80),
    ("2026-09-03", "SOLARINDS", "CALL", "12:27", 800.05),
    ("2026-09-04", "ATHERENERG", "PUT", "14:54", 61.30),
    ("2026-09-04", "HAVELLS", "PUT", "09:31", 28.00),
    ("2026-09-04", "KEI", "PUT", "09:27", 192.55),
    ("2026-09-04", "POLYCAB", "PUT", "09:17", 218.70),
    ("2026-09-07", "MANAPPURAM", "PUT", "14:22", 8.30),
    ("2026-09-07", "WIPRO", "PUT", "15:11", 5.58),
    ("2026-09-08", "GVT&D", "CALL", "09:27", 190.85),
    ("2026-09-08", "POWERINDIA", "PUT", "10:13", 989.80),
    ("2026-09-09", "COFORGE", "PUT", "09:23", 52.10),
    ("2026-09-09", "MUTHOOTFIN", "PUT", "12:02", 57.05),
]
BASELINE_PNL = {
    "LTF": -0.05, "POLYCAB_1": 134.55, "BSE": -2.15, "EICHERMOT": -26.50, "HEROMOTOCO": -4.25,
    "SWIGGY": -1.30, "APLAPOLLO": 3.85, "GODREJCP": -1.40, "SOLARINDS": 70.30, "ATHERENERG": 6.45,
    "HAVELLS": 6.50, "KEI": 52.00, "POLYCAB_4": 7.05, "MANAPPURAM": -0.15, "WIPRO": 0.29,
    "GVT&D": 61.55, "POWERINDIA": -84.80, "COFORGE": -13.70, "MUTHOOTFIN": -0.05,
}


def _access_tokens():
    db = ClientDB()
    tokens = []
    for account in ("upstox2", "upstox"):
        creds = db.get_feeder_creds_sync(account)
        if creds and creds.get("access_token"):
            tokens.append(creds["access_token"])
    if not tokens:
        raise RuntimeError("No upstox/upstox2 feeder access_token found -- run this on EC2.")
    return tokens


def _to_bars(rows):
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                        low=float(r["low"]), close=float(r["close"])))
    return out


async def _fetch_trade_cache(token_list, trade_date_str, symbol, side, entry_hhmm, entry_opt_price):
    trade_date = date.fromisoformat(trade_date_str)
    entry_ts = datetime.combine(trade_date, datetime.strptime(entry_hhmm, "%H:%M").time(), tzinfo=IST)
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return None
    spot_rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, token_list, trade_date, trade_date)
    spot_bars = _to_bars(spot_rows)
    if not spot_bars:
        return None
    entry_spot_candidates = [b for b in spot_bars if b.ts <= entry_ts]
    if not entry_spot_candidates:
        return None
    entry_spot = entry_spot_candidates[-1].close

    opt_type = "CE" if side == "CALL" else "PE"
    contract = await stock_resolve.resolve_contract_async(symbol, entry_spot, opt_type)
    if contract is None:
        return None
    opt_rows = await hc.fetch_upstox_range_1m_multi_account(contract.upstox_key, token_list, trade_date, trade_date)
    opt_bars = _to_bars(opt_rows)
    if not opt_bars:
        return None

    eod_ts = datetime.combine(trade_date, datetime.strptime(EOD_TIME, "%H:%M").time(), tzinfo=IST)
    return {"trade_date": trade_date, "symbol": symbol, "side": side, "entry_ts": entry_ts,
            "entry_opt_price": entry_opt_price, "spot_bars": spot_bars, "opt_bars": opt_bars, "eod_ts": eod_ts}


def _real_sl_exit(cache):
    """Same real 20-min VWAP-close hard SL as the already-validated baseline."""
    symbol, side = cache["symbol"], cache["side"]
    spot_bars, entry_ts, eod_ts = cache["spot_bars"], cache["entry_ts"], cache["eod_ts"]
    ha_1m = to_heikin_ashi(spot_bars)
    ha_tf = to_n_min_bars_market_anchored(ha_1m, _VWAP_SL_TF_MIN)
    vwap_state = VwapState()
    vwap_at_minute = {}
    for b in spot_bars:
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
    for hb in ha_tf:
        bucket_end = hb.ts + timedelta(minutes=_VWAP_SL_TF_MIN)
        if bucket_end <= entry_floor:
            continue
        if bucket_end > eod_ts:
            break
        vwap_now = _vwap_as_of(bucket_end)
        if vwap_now is None or vwap_now <= 0:
            continue
        if OiOrbScreenerStrategy._ha_vwap_close_sl_adverse(hb, vwap_now, side):
            return bucket_end
    return None


def _pnl_at(cache, exit_ts):
    exit_candidates = [b for b in cache["opt_bars"] if b.ts <= exit_ts]
    if not exit_candidates:
        return None
    return round(exit_candidates[-1].close - cache["entry_opt_price"], 2)


def _giveback_exit(cache, giveback_pct):
    """On the OPTION's own premium path. Returns exit_ts or None."""
    entry_ts, eod_ts = cache["entry_ts"], cache["eod_ts"]
    entry_price = cache["entry_opt_price"]
    entry_floor = entry_ts.replace(second=0, microsecond=0)
    peak = entry_price
    for b in cache["opt_bars"]:
        if b.ts < entry_floor or b.ts > eod_ts:
            continue
        peak = max(peak, b.high)
        if peak > entry_price:
            trail_stop = peak - giveback_pct * (peak - entry_price)
            if b.low <= trail_stop:
                return b.ts
    return None


def _sr_exit(cache, tf_min):
    """Real SupportResistanceCalculator, fed real N-min SPOT bars from
    market open (so phases are genuinely established before entry, same
    discipline as every other real intraday-warmup in this codebase).
    CALL: exit on a spot close below S1 (once established).
    PUT: exit on a spot close above R1 (once established)."""
    symbol, side = cache["symbol"], cache["side"]
    entry_ts, eod_ts = cache["entry_ts"], cache["eod_ts"]
    tf_bars = to_n_min_bars_dateaware(cache["spot_bars"], tf_min)
    calc = SupportResistanceCalculator()
    inst_key = f"{symbol}_SR_{tf_min}"
    for b in tf_bars:
        candle = {"timestamp": b.ts, "high": b.high, "low": b.low, "duration": tf_min}
        calc.process_straddle_candle(inst_key, candle, silent=True)
        if b.ts < entry_ts:
            continue
        state = calc.get_calculated_sr_state(inst_key)
        levels = state.get("sr_levels") or {}
        s1 = levels.get("S1") or {}
        r1 = levels.get("R1") or {}
        if side == "CALL" and s1.get("is_established") and b.close < s1.get("low", float("-inf")):
            return b.ts
        if side == "PUT" and r1.get("is_established") and b.close > r1.get("high", float("inf")):
            return b.ts
        if b.ts > eod_ts:
            break
    return None


def _combine(cache, candidate_exit_ts):
    """Race the candidate exit against the real SL; earliest (or EOD) wins."""
    sl_ts = _real_sl_exit(cache)
    candidates = [t for t in [candidate_exit_ts, sl_ts] if t is not None]
    exit_ts = min(candidates) if candidates else cache["eod_ts"]
    return exit_ts


async def main():
    tokens = _access_tokens()
    print("=" * 130)
    print("OI-ORB Screener -- TRAILING EXIT backtest, real 19-trade sample")
    print(f"A) Give-back trailing stop (option premium): giveback_pct in {GIVEBACK_GRID}")
    print(f"B) Real S&R S1(CALL)/R1(PUT) structural TSL (underlying spot, SupportResistanceCalculator): "
          f"tf_min in {SR_TF_GRID}")
    print("Both race the real 20-min VWAP-close hard SL -- whichever fires first wins, else EOD.")
    print("=" * 130)

    print("\nFetching + caching real data per trade (19 trades)...")
    caches = []
    for trade_date_str, symbol, side, entry_hhmm, entry_opt_price in KNOWN_TRADES:
        c = await _fetch_trade_cache(tokens, trade_date_str, symbol, side, entry_hhmm, entry_opt_price)
        if c is not None:
            caches.append(c)
        else:
            print(f"  {trade_date_str} {symbol}: FAILED to cache real data")
    print(f"Cached {len(caches)}/{len(KNOWN_TRADES)} trades.\n")

    baseline_total = sum(BASELINE_PNL.values())
    print(f"BASELINE (already validated, EOD/SL only, no target): {baseline_total:+.2f} pts\n")

    print("=" * 130)
    print("A) GIVE-BACK TRAILING STOP SWEEP")
    print("=" * 130)
    for gb in GIVEBACK_GRID:
        total = 0.0
        for c in caches:
            gb_exit = _giveback_exit(c, gb)
            exit_ts = _combine(c, gb_exit)
            pnl = _pnl_at(c, exit_ts)
            if pnl is not None:
                total += pnl
        print(f"  giveback={gb*100:.0f}%  ->  total={total:+.2f} pts  (delta vs baseline: {total-baseline_total:+.2f})")

    print("\n" + "=" * 130)
    print("B) REAL S&R S1(CALL)/R1(PUT) STRUCTURAL TSL SWEEP")
    print("=" * 130)
    for tf in SR_TF_GRID:
        total = 0.0
        per_trade = []
        for c in caches:
            sr_exit = _sr_exit(c, tf)
            exit_ts = _combine(c, sr_exit)
            pnl = _pnl_at(c, exit_ts)
            if pnl is not None:
                total += pnl
                per_trade.append((c["symbol"], c["trade_date"].isoformat(), pnl))
        print(f"  tf={tf}min  ->  total={total:+.2f} pts  (delta vs baseline: {total-baseline_total:+.2f})")

    print("\n" + "=" * 130)
    print("CAVEAT: n=19 real trades, single 7-day sample. Both mechanics race the SAME real SL, only the "
          "'take profit early' side changes per candidate. A combo that wins here needs a larger forward "
          "sample before being trusted as the final design.")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
