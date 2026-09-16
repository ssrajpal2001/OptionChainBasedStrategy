"""
scripts/oi_orb_screener_20260916_4scenario_backtest.py

Real-data backtest, 2026-09-16, direct user follow-up: a pasted external
plan ("anthropic/claude-sonnet-4.6") proposing a 4-scenario gap+OI
classification (Scenario A/B/C/D below), asking "please chk" -- checked
against today's real data first (see the conversation), then this script
builds and runs the plan CORRECTLY, fixing two real gaps found during
that check:

  1. Scenario B (gap UP + OI DECREASED = short covering) trades the
     OPPOSITE side of the original price trigger (fade with PUT, not
     continue with CALL) -- our earlier weak-quadrant backtest never
     actually tested this flip (PATANJALI, today's one real Scenario-B
     candidate, was only ever checked on its CALL side, which never
     fired).
  2. Scenario D (gap DOWN + OI DECREASED = long unwinding) is
     conditional, not blind continuation, per the plan: only trade PUT-
     continuation if price is already BELOW the previous day's real low
     at trigger time; otherwise skip (undefined risk). Tested here via a
     real prev-day-low fetch, not assumed.

Scenario table (gap direction x futures-OI direction at trigger time,
generalized from the plan's fixed-9:15 read to whatever minute today's
real price trigger actually fired, since most of today's real triggers
were intraday, not at the open):
  A: price UP   + OI UP   -> fresh longs,  BUY CALL (trend, continue)
  B: price UP   + OI DOWN -> short cover,  BUY PUT  (fade, FLIP side)
  C: price DOWN + OI UP   -> fresh shorts, BUY PUT  (trend, continue)
  D: price DOWN + OI DOWN -> long unwind,  conditional (see above)

Entry: VWAP retest (real RollingVwapRetestTracker+VwapState, same as
every other backtest today) if it fires by 09:45; else a 15-min Opening
Range breakout fallback (09:15-09:29 real high/low, first bar at/after
09:45 that closes through it in the trade side's direction) -- this is
the plan's own "Trigger A / Trigger B" pair, approximated as: whichever
fires first is used, VWAP preferred if it beats 09:45.

The "no entry after 11:00" hard rule is applied as a REPORTED comparison
(with-cutoff vs without-cutoff totals), not silently baked in, so its
real impact on today's actual trades is visible rather than assumed.

Exit stays the ALREADY-VALIDATED live HA+StochRSI(9,9,3) 15-min
shape+cross mechanic (51-row real sample, PF 29.40) -- the plan's own
proposed 1:1.5RR-partial + VWAP-trail + hard-14:45 exit is a genuinely
different, untested mechanic and is deliberately NOT swapped in here;
that needs its own dedicated real-data pass if pursued later.

MUST run on EC2 (real Upstox2 access token + real intraday history).

Usage: python scripts/oi_orb_screener_20260916_4scenario_backtest.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, time as dt_time, timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import (
    to_heikin_ashi, to_n_min_bars, compute_stoch_rsi, ha_stoch_shape_exit_signal,
)
from strategies.oi_orb_screener import stock_resolve
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy
from strategies.oi_orb_screener.screener import VwapState, RollingVwapRetestTracker

TRADE_DATE = "2026-09-16"
TODAY = date.fromisoformat(TRADE_DATE)
EOD_TIME = "15:15"
RSI_PERIOD, STOCH_PERIOD, SMOOTH = 9, 9, 3

PRICE_TRIGGER_PCT = 2.0
VWAP_WINDOW_MIN = 15.0
ORB_END = dt_time(9, 30)
BREAKOUT_START = dt_time(9, 45)
NO_ENTRY_AFTER = dt_time(11, 0)

CLIENT_ID = "ssrajpal2001"
BINDING_ID = "UPSTOX"

UNIVERSE = [
    "PATANJALI", "ASTRAL", "POLICYBZR", "PREMIERENE", "PAYTM", "BLUESTARCO",
    "OFSS", "SOLARINDS", "BAJAJHLDNG", "WAAREEENER", "YESBANK", "LAURUSLABS",
    "NESTLEIND", "BSE", "TCS", "NYKAA", "MFSL", "BOSCHLTD", "SHRIRAMFIN", "COFORGE",
]


class _NullBus:
    def subscribe(self, topic):
        return None

    def unsubscribe(self, topic, q):
        pass

    async def publish(self, topic, event):
        pass


def _access_token() -> str:
    creds = ClientDB().get_feeder_creds_sync("upstox2")
    if creds and creds.get("access_token"):
        return creds["access_token"]
    raise RuntimeError("No upstox2 feeder access_token found -- run this on EC2.")


def _to_bars(rows: List[dict]) -> List[Bar]:
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                        low=float(r["low"]), close=float(r["close"])))
    return out


async def _find_price_trigger_and_prev_low(sym: str, token: str):
    """Same real per-minute pChange walk as the other 2026-09-16 scripts,
    plus the real previous trading day's LOW (for the Scenario-D
    conditional filter). Returns (side, ts, pchange, eq_bars, prev_close,
    prev_low, kind) or (None, None, None, eq_bars_or_None, err, None, None)."""
    eq_key = stock_resolve.resolve_eq_instrument_key(sym)
    if not eq_key:
        return None, None, None, None, "NO_EQ_KEY", None, None
    daily = await hc.fetch_upstox_daily(eq_key, token, lookback_days=5)
    if not daily:
        return None, None, None, None, "NO_DAILY_DATA_FOR_PREV_CLOSE", None, None
    prev_close = prev_low = None
    for row in reversed(daily):
        row_date = row["ts"][:10] if isinstance(row["ts"], str) else row["ts"].date().isoformat()
        if row_date < TRADE_DATE:
            prev_close = float(row["close"])
            prev_low = float(row["low"])
            break
    if not prev_close:
        return None, None, None, None, "NO_PREV_CLOSE_FOUND", None, None

    rows = await hc.fetch_upstox_intraday_1m(eq_key, token)
    bars = _to_bars(rows)
    if not bars:
        return None, None, None, None, "NO_INTRADAY_BARS", None, None

    for i, b in enumerate(bars):
        pchange = (b.close - prev_close) / prev_close * 100.0
        if pchange >= PRICE_TRIGGER_PCT or pchange <= -PRICE_TRIGGER_PCT:
            side = "CALL" if pchange > 0 else "PUT"
            kind = "GAP-OPEN" if i == 0 else "INTRADAY"
            return side, b.ts, pchange, bars, prev_close, prev_low, kind
    return None, None, None, bars, prev_close, prev_low, None


async def _oi_change_at_trigger(book: OiOrbScreenerStrategy, sym: str, trigger_ts: datetime,
                                 yday_oi: float, token: str):
    """Real futures OI % change at (the first real reading at/after) the
    price-trigger minute -- the plan's '9:15 OI vs yesterday' calc,
    generalized to trigger time since most real triggers today weren't
    exactly at 09:15. Returns (ts, oi, change_pct) or (None, None, None)."""
    resolved = await book._resolve_futures_key_and_token(sym)
    if resolved is None:
        return None, None, None
    fut_key, _tok = resolved
    rows = await hc.fetch_upstox_intraday_1m(fut_key, token)
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        ts = ts.astimezone(IST)
        if ts < trigger_ts.replace(second=0, microsecond=0):
            continue
        oi = r.get("oi")
        if not oi:
            continue
        change_pct = (float(oi) - yday_oi) / yday_oi * 100.0
        return ts, float(oi), change_pct
    return None, None, None


def _classify_scenario(side: str, oi_change_pct: float, trigger_price: float, prev_low: Optional[float]):
    """Returns (scenario_letter, trade_side_or_None, conviction_label)."""
    oi_up = oi_change_pct > 0
    if side == "CALL" and oi_up:
        return "A", "CALL", "trend, high conviction (fresh longs)"
    if side == "CALL" and not oi_up:
        return "B", "PUT", "fade, medium conviction (short covering -- FLIPPED side)"
    if side == "PUT" and oi_up:
        return "C", "PUT", "trend, high conviction (fresh shorts)"
    # side == "PUT" and not oi_up -> Scenario D, conditional
    if prev_low is not None and trigger_price < prev_low:
        return "D", "PUT", "D-continuation (price already below prev-day low -- capitulation)"
    return "D", None, "D-skip (price not below prev-day low -- undefined risk per plan, SKIPPED)"


async def _find_orb_breakout(eq_bars: List[Bar], trade_side: str, after_ts: datetime):
    """09:15-09:29 real opening range high/low; first bar at/after
    BREAKOUT_START (and at/after after_ts) whose close breaks it in
    trade_side's direction."""
    or_bars = [b for b in eq_bars if dt_time(9, 15) <= b.ts.time() < ORB_END]
    if not or_bars:
        return None, None, None
    or_high = max(b.high for b in or_bars)
    or_low = min(b.low for b in or_bars)
    floor_ts = max(after_ts, datetime.combine(TODAY, BREAKOUT_START, tzinfo=IST))
    for b in eq_bars:
        if b.ts < floor_ts:
            continue
        if trade_side == "CALL" and b.close > or_high:
            return b.ts, b.close, or_high
        if trade_side == "PUT" and b.close < or_low:
            return b.ts, b.close, or_low
    return None, None, None


async def _find_vwap_retest(sym: str, side: str, after_ts: datetime, eq_bars: List[Bar]):
    tracker = RollingVwapRetestTracker(window_min=VWAP_WINDOW_MIN)
    vwap_state = VwapState()
    fire_ts = fire_price = fire_vwap = None
    for b in eq_bars:
        typical = (b.high + b.low + b.close) / 3.0
        vwap_state.update(sym, typical, 1.0)
        vwap = vwap_state.current(sym)
        if vwap is None:
            continue
        bar_ts = b.ts.replace(second=0, microsecond=0)
        fired = tracker.check(side, bar_ts, b.close, vwap)
        if fired and bar_ts >= after_ts.replace(second=0, microsecond=0):
            fire_ts, fire_price, fire_vwap = bar_ts, b.close, vwap
            break
    return fire_ts, fire_price, fire_vwap


async def _simulate_exit(symbol: str, side: str, entry_ts: datetime, entry_price_spot: float, token: str):
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return {"reason": "NO_EQ_KEY"}
    warm_rows = await hc.fetch_upstox_warm_1m(eq_key, token, min_bars=400)
    all_bars = _to_bars(warm_rows)
    entry_floor = entry_ts.replace(second=0, microsecond=0)
    post_entry_bars = [b for b in all_bars if b.ts >= entry_floor]
    if len(all_bars) < 15 or not post_entry_bars:
        return {"reason": f"INSUFFICIENT_DATA ({len(all_bars)} bars)"}

    exit_signal_ts = None
    last_evaluated = None
    for i in range(15, len(all_bars) + 1):
        window = all_bars[:i]
        cur_ts = window[-1].ts
        ha_1m = to_heikin_ashi(window)
        ha_15m = to_n_min_bars(ha_1m, 15)
        if not ha_15m:
            continue
        last_bar = ha_15m[-1]
        if cur_ts < last_bar.ts + timedelta(minutes=15):
            ha_15m = ha_15m[:-1]
        if not ha_15m:
            continue
        latest = ha_15m[-1]
        if latest.ts < entry_floor:
            continue
        if last_evaluated == latest.ts:
            continue
        last_evaluated = latest.ts
        closes = [b.close for b in ha_15m]
        k, d = compute_stoch_rsi(closes, RSI_PERIOD, STOCH_PERIOD, SMOOTH)
        if ha_stoch_shape_exit_signal(latest, k[-1], d[-1], side, inclusive=True):
            exit_signal_ts = cur_ts
            break

    if exit_signal_ts is None:
        exit_signal_ts = datetime.combine(TODAY, datetime.strptime(EOD_TIME, "%H:%M").time(), tzinfo=IST)
        exit_reason = "eod_squareoff (HA+StochRSI never fired)"
    else:
        exit_reason = "ha_stoch_exit"

    entry_price = exit_price = None
    try:
        opt_type = "CE" if side == "CALL" else "PE"
        contract = await stock_resolve.resolve_contract_async(symbol, entry_price_spot, opt_type)
        if contract is None:
            return {"reason": "could not resolve a real tradable contract", "exit_ts": exit_signal_ts}
        opt_rows = await hc.fetch_upstox_intraday_1m(contract.upstox_key, token)
        opt_bars = _to_bars(opt_rows)
        entry_candidates = [b for b in opt_bars if b.ts <= entry_ts]
        if entry_candidates:
            entry_price = entry_candidates[-1].close
        exit_candidates = [b for b in opt_bars if b.ts <= exit_signal_ts]
        if exit_candidates:
            exit_price = exit_candidates[-1].close
    except Exception as exc:
        return {"reason": f"option resolution failed: {exc}", "exit_ts": exit_signal_ts}

    pnl = None
    if entry_price is not None and exit_price is not None:
        # 2026-09-16 CRITICAL bug fix -- see oi_orb_screener_20260916_
        # live_oi_confirm_backtest.py's own _simulate_exit for the full
        # reasoning: entry/exit are the OPTION'S OWN premium, a long
        # option (CE or PE) profits purely on premium direction, no
        # side-based sign flip -- confirmed against the real live
        # engine's own formula (strategies/oi_orb_screener/engine.py:
        # pnl = (fill_price - entry_price) * qty, no CALL/PUT branch).
        pnl = round(exit_price - entry_price, 2)
    return {"entry_price": entry_price, "exit_price": exit_price, "exit_ts": exit_signal_ts,
            "exit_reason": exit_reason, "pnl": pnl}


async def main():
    token = _access_token()
    print("=" * 130)
    print(f"OI-ORB Screener -- {TRADE_DATE} 4-SCENARIO backtest (external plan check): "
          f"gap x futures-OI-direction classification -> scenario-correct side -> VWAP retest / "
          f"15min-ORB breakout -> validated HA+StochRSI exit")
    print(f"Universe ({len(UNIVERSE)}): {UNIVERSE}")
    print("Scenario table: A=price UP+OI UP=CALL trend | B=price UP+OI DOWN=PUT fade (FLIPPED) | "
          "C=price DOWN+OI UP=PUT trend | D=price DOWN+OI DOWN=conditional (prev-day-low filter)")
    print("=" * 130)

    bus = _NullBus()
    book = OiOrbScreenerStrategy(
        bus, cfg=None, client_id=CLIENT_ID, binding_id=BINDING_ID,
        lot_multiplier=1, product_type="MIS", squareoff_time="15:15",
        strategy_name="oi_orb_screener_top20",
    )

    total_pnl = 0.0
    trades = 0
    total_pnl_with_cutoff = 0.0
    trades_with_cutoff = 0

    for sym in UNIVERSE:
        print(f"\n{'-' * 130}")
        print(f"{sym}")
        print(f"{'-' * 130}")

        side, trigger_ts, pchange, eq_bars, prev_close_or_err, prev_low, kind = \
            await _find_price_trigger_and_prev_low(sym, token)
        if side is None:
            if isinstance(prev_close_or_err, str) and prev_close_or_err.startswith("NO_"):
                print(f"  STEP 1 -- price trigger: FAILED ({prev_close_or_err})")
            else:
                print(f"  STEP 1 -- price trigger: pChange never crossed +-{PRICE_TRIGGER_PCT}% today")
            continue
        trigger_price = next((b.close for b in eq_bars if b.ts == trigger_ts), None)
        print(f"  STEP 1 -- price trigger [{kind}]: {trigger_ts.strftime('%H:%M')} pChange={pchange:+.2f}% "
              f"-> original side={side} (prev_close={prev_close_or_err:.2f}, prev_day_low={prev_low})")

        if sym not in book._today_0915_oi:
            await book._compute_oi_regime_side(sym)
        yday_oi = book._prev_day_last_tick_oi.get(sym)
        if not yday_oi:
            print(f"  STEP 2 -- futures OI: NO YESTERDAY OI DATA -- cannot classify")
            continue

        oi_ts, oi_val, oi_change = await _oi_change_at_trigger(book, sym, trigger_ts, yday_oi, token)
        if oi_ts is None:
            print(f"  STEP 2 -- futures OI: NO real OI reading found at/after trigger -- cannot classify")
            continue
        print(f"  STEP 2 -- futures OI @ {oi_ts.strftime('%H:%M')}: OI={oi_val:.0f} change={oi_change:+.2f}% "
              f"vs yesterday 15:39 baseline")

        scenario, trade_side, conviction = _classify_scenario(side, oi_change, trigger_price, prev_low)
        print(f"  STEP 3 -- SCENARIO {scenario}: {conviction}")
        if trade_side is None:
            print(f"  RESULT: SKIPPED per plan's Scenario-D rule -- no trade")
            continue
        if trade_side != side:
            print(f"    NOTE: trade side FLIPPED from the original {side} trigger to {trade_side} "
                  f"per Scenario {scenario}'s fade logic")

        vwap_ts, vwap_price, vwap_vwap = await _find_vwap_retest(sym, trade_side, trigger_ts, eq_bars)
        entry_ts = entry_price = entry_kind = None
        if vwap_ts is not None and vwap_ts.time() <= BREAKOUT_START:
            entry_ts, entry_price, entry_kind = vwap_ts, vwap_price, f"VWAP retest (vwap={vwap_vwap:.2f})"
        else:
            brk_ts, brk_price, brk_level = await _find_orb_breakout(eq_bars, trade_side, trigger_ts)
            if brk_ts is not None:
                entry_ts, entry_price, entry_kind = brk_ts, brk_price, f"15min-ORB breakout (level={brk_level:.2f})"
            elif vwap_ts is not None:
                entry_ts, entry_price, entry_kind = vwap_ts, vwap_price, f"VWAP retest, late (vwap={vwap_vwap:.2f})"

        if entry_ts is None:
            print(f"  STEP 4 -- entry trigger: neither VWAP retest nor 15min-ORB breakout ever fired -- no trade")
            continue
        print(f"  STEP 4 -- entry trigger FIRED at {entry_ts.strftime('%H:%M')} via {entry_kind}, "
              f"price={entry_price:.2f}")
        late = entry_ts.time() > NO_ENTRY_AFTER
        if late:
            print(f"    NOTE: this entry is AFTER {NO_ENTRY_AFTER.strftime('%H:%M')} -- would be EXCLUDED "
                  f"under the plan's 'no entry after 11:00' hard rule")

        sim = await _simulate_exit(sym, trade_side, entry_ts, entry_price, token)
        if sim.get("pnl") is not None:
            print(f"  STEP 5 -- trade outcome: entry@{entry_ts.strftime('%H:%M:%S')} opt={sim['entry_price']} "
                  f"-> exit@{sim['exit_ts'].strftime('%H:%M:%S')} opt={sim['exit_price']} "
                  f"reason={sim['exit_reason']}")
            print(f"  RESULT: REAL TRADE -- {trade_side} {sym} (scenario {scenario}), PNL={sim['pnl']:+.2f} pts")
            total_pnl += sim["pnl"]
            trades += 1
            if not late:
                total_pnl_with_cutoff += sim["pnl"]
                trades_with_cutoff += 1
        else:
            print(f"  STEP 5 -- trade outcome: entry fired but P&L not reconstructed ({sim.get('reason')})")

    print("\n" + "=" * 130)
    print(f"TOTAL, no 11:00 cutoff (all entries counted): {trades} trade(s), {total_pnl:+.2f} pts")
    print(f"TOTAL, WITH 11:00 cutoff (plan's hard rule applied): {trades_with_cutoff} trade(s), "
          f"{total_pnl_with_cutoff:+.2f} pts")
    print("CAVEAT: single real day (n=1). Scenario classification uses the OI reading at trigger time "
          "(not a fixed 09:15 read, since most real triggers today were intraday). The NSE OI-spurt-page "
          "ge7% pre-filter from the plan's own Step 1 cannot be retroactively verified for today (that "
          "endpoint is live-snapshot-only, no historical query). Exit stays the already-validated live "
          "HA+StochRSI mechanic, not the plan's own untested RR/VWAP-trail proposal.")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
