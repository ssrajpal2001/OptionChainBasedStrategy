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

A and B both race the already-known real 20-min VWAP-close hard SL and
EOD (15:15) -- whichever fires first, chronologically, wins, exactly
like the already-validated baseline.

  C) PRIOR-CANDLE-EXTREME SL (on the UNDERLYING SPOT), direct user
     spec 2026-09-16: "if v change the sl from 20 min to 75 min -- if
     market closes below prev 75 min then sl of long is hit and it
     closes above 75 high then short sl is hit -- in spot worth
     checking." A genuinely different concept from both the existing
     live VWAP-distance SL and the S&R S1/R1 TSL above -- no VWAP
     involved at all, just the prior same-timeframe candle's own
     high/low. This REPLACES the 20-min VWAP-close SL entirely (does
     NOT race it) -- exactly matches "change the SL from 20min to
     75min". Sweeps tf_min in [45, 60, 75, 90] for context around the
     requested 75-min value.

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
PREV_CANDLE_SL_TF_GRID = [45, 60, 75, 90]

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
    """On the OPTION's own premium path. Returns exit_ts or None.

    2026-09-16 bug fix: the first version updated `peak` from THIS bar's
    own high, then immediately checked THIS SAME bar's low against the
    trail stop derived from that just-updated peak -- a same-bar
    lookahead bug (assumes the high happened before the low within a
    single 1-min bar, which a bar's own OHLC can never actually prove).
    Confirmed live: gave an identical +0.30 total across all 4 giveback
    thresholds -- the classic signature of stopping out within the
    first 1-2 bars on nearly every trade regardless of the threshold
    value. Fixed: the trail stop used to check THIS bar is always
    derived from the peak as of the PRIOR bar's close -- only after
    checking does this bar's own high get folded into the peak for the
    NEXT bar's check."""
    entry_ts, eod_ts = cache["entry_ts"], cache["eod_ts"]
    entry_price = cache["entry_opt_price"]
    entry_floor = entry_ts.replace(second=0, microsecond=0)
    peak = entry_price
    for b in cache["opt_bars"]:
        if b.ts < entry_floor or b.ts > eod_ts:
            continue
        if peak > entry_price:
            trail_stop = peak - giveback_pct * (peak - entry_price)
            if b.low <= trail_stop:
                return b.ts
        peak = max(peak, b.high)
    return None


def _prev_candle_sl_exit(cache, tf_min):
    """Direct user spec: replace the 20-min VWAP-close SL with a 75-min
    (or other tf) prior-CANDLE-extreme SL, checked on the underlying
    SPOT, not VWAP-distance at all:
      LONG (CALL): SL hit when the CURRENT tf_min candle CLOSES below
      the PREVIOUS tf_min candle's own LOW.
      SHORT (PUT): SL hit when the CURRENT tf_min candle CLOSES above
      the PREVIOUS tf_min candle's own HIGH.
    Market-anchored buckets (09:15 start), same as the existing live
    20-min VWAP-close SL's own bucketing -- not reimplemented, same
    real to_n_min_bars_market_anchored function. Returns the exit
    timestamp (bucket close) or None."""
    symbol, side = cache["symbol"], cache["side"]
    entry_ts, eod_ts = cache["entry_ts"], cache["eod_ts"]
    tf_bars = to_n_min_bars_market_anchored(cache["spot_bars"], tf_min)
    entry_floor = entry_ts.replace(second=0, microsecond=0)
    prev_bar = None
    for b in tf_bars:
        bucket_end = b.ts + timedelta(minutes=tf_min)
        if bucket_end <= entry_floor:
            prev_bar = b
            continue
        if bucket_end > eod_ts:
            break
        if prev_bar is not None:
            if side == "CALL" and b.close < prev_bar.low:
                return bucket_end
            if side == "PUT" and b.close > prev_bar.high:
                return bucket_end
        prev_bar = b
    return None


def _sr_exit(cache, tf_min, diag=None):
    """Real SupportResistanceCalculator, fed real N-min SPOT bars from
    market open. CALL: exit on a spot close below the CURRENT active
    floor. PUT: exit on a spot close above the CURRENT active ceiling.

    2026-09-16, direct user decision after the first version fired on
    ZERO of 19 real trades across all 3 timeframes: trail to whichever
    level is MOST RECENTLY relevant, not pinned to the original
    established S1/R1 forever. The calculator already promotes a
    confirmed S2/R2 into S1/R1 on full confirmation (S1 =
    S2.copy()) -- but a NEWER S2/R2 candidate that's still forming
    (is_established=False, real low/high value already present, not
    None) represents a tighter, more current pullback level than the
    older established S1/R1 it hasn't replaced yet. Uses S2's/R2's own
    raw low/high the moment one exists, falling back to established
    S1/R1 otherwise -- this is the "trail to the current active level"
    interpretation, not a fixed pin to S1/R1 specifically.

    `diag`, if passed a dict, is filled with real diagnostic info
    (final phase, when S1/R1 established, and the live gap between the
    active floor/ceiling and price at EOD) so a zero-impact result can
    be told apart from a real bug."""
    symbol, side = cache["symbol"], cache["side"]
    entry_ts, eod_ts = cache["entry_ts"], cache["eod_ts"]
    tf_bars = to_n_min_bars_dateaware(cache["spot_bars"], tf_min)
    calc = SupportResistanceCalculator()
    inst_key = f"{symbol}_SR_{tf_min}"
    s1_established_ts = r1_established_ts = None
    final_phase = "UNKNOWN"
    last_active_level = last_close = None
    for b in tf_bars:
        candle = {"timestamp": b.ts, "high": b.high, "low": b.low, "duration": tf_min}
        calc.process_straddle_candle(inst_key, candle, silent=True)
        state = calc.get_calculated_sr_state(inst_key)
        final_phase = state.get("current_phase", "UNKNOWN")
        levels = state.get("sr_levels") or {}
        s1 = levels.get("S1") or {}
        r1 = levels.get("R1") or {}
        s2 = levels.get("S2")
        r2 = levels.get("R2")
        if s1.get("is_established") and s1_established_ts is None:
            s1_established_ts = b.ts
        if r1.get("is_established") and r1_established_ts is None:
            r1_established_ts = b.ts

        active_level = None
        if side == "CALL":
            if s2 is not None and s2.get("low") is not None:
                active_level = s2["low"]
            elif s1.get("is_established"):
                active_level = s1.get("low")
        else:
            if r2 is not None and r2.get("high") is not None:
                active_level = r2["high"]
            elif r1.get("is_established"):
                active_level = r1.get("high")
        last_active_level, last_close = active_level, b.close

        if b.ts < entry_ts:
            continue
        if active_level is not None:
            if side == "CALL" and b.close < active_level:
                if diag is not None:
                    diag.update(final_phase=final_phase, s1_established_ts=s1_established_ts,
                                 r1_established_ts=r1_established_ts, exit_level=active_level)
                return b.ts
            if side == "PUT" and b.close > active_level:
                if diag is not None:
                    diag.update(final_phase=final_phase, s1_established_ts=s1_established_ts,
                                 r1_established_ts=r1_established_ts, exit_level=active_level)
                return b.ts
        if b.ts > eod_ts:
            break
    if diag is not None:
        gap_pct = (round((last_close - last_active_level) / last_active_level * 100.0, 3)
                   if last_active_level and last_close else None)
        diag.update(final_phase=final_phase, s1_established_ts=s1_established_ts,
                     r1_established_ts=r1_established_ts, exit_level=None,
                     last_active_level=last_active_level, last_close=last_close, live_gap_pct=gap_pct)
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
    print("A and B race the real 20-min VWAP-close hard SL -- whichever fires first wins, else EOD.")
    print(f"C) Prior-candle-extreme SL (underlying spot, REPLACES the 20-min VWAP SL entirely): "
          f"tf_min in {PREV_CANDLE_SL_TF_GRID}")
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
        show_diag = (tf == SR_TF_GRID[len(SR_TF_GRID) // 2])
        if show_diag:
            print(f"\n  -- per-trade diagnostic for tf={tf}min (final phase / S1,R1 established?) --")
        for c in caches:
            diag = {} if show_diag else None
            sr_exit = _sr_exit(c, tf, diag=diag)
            exit_ts = _combine(c, sr_exit)
            pnl = _pnl_at(c, exit_ts)
            if pnl is not None:
                total += pnl
            if show_diag and diag:
                fired = f"FIRED@{sr_exit.strftime('%H:%M')} level={diag.get('exit_level')}" if sr_exit else "never fired"
                s1e = diag['s1_established_ts'].strftime('%H:%M') if diag.get('s1_established_ts') else "never"
                r1e = diag['r1_established_ts'].strftime('%H:%M') if diag.get('r1_established_ts') else "never"
                gap = f"live_gap={diag['live_gap_pct']:+.2f}%" if diag.get('live_gap_pct') is not None else "no active level ever"
                print(f"    {c['symbol']:14s} side={c['side']:4s} final_phase={diag['final_phase']:22s} "
                      f"S1_est={s1e:6s} R1_est={r1e:6s} {fired}  ({gap})")
        print(f"  tf={tf}min  ->  total={total:+.2f} pts  (delta vs baseline: {total-baseline_total:+.2f})")

    print("\n" + "=" * 130)
    print("C) PRIOR-CANDLE-EXTREME SL SWEEP (replaces the 20-min VWAP SL entirely, spot-based)")
    print("=" * 130)
    for tf in PREV_CANDLE_SL_TF_GRID:
        total = 0.0
        fired_count = 0
        for c in caches:
            exit_ts = _prev_candle_sl_exit(c, tf)
            if exit_ts is None:
                exit_ts = c["eod_ts"]
            else:
                fired_count += 1
            pnl = _pnl_at(c, exit_ts)
            if pnl is not None:
                total += pnl
        marker = "  <== requested value" if tf == 75 else ""
        print(f"  tf={tf}min  ->  total={total:+.2f} pts  ({fired_count}/{len(caches)} real SL fires)  "
              f"(delta vs baseline: {total-baseline_total:+.2f}){marker}")

    print("\n" + "=" * 130)
    print("CAVEAT: n=19 real trades, single 7-day sample. A and B race the SAME real 20-min VWAP SL; C "
          "REPLACES it entirely (no VWAP involved). A combo that wins here needs a larger forward sample "
          "before being trusted as the final design.")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
