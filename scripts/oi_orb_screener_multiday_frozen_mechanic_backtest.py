"""
scripts/oi_orb_screener_multiday_frozen_mechanic_backtest.py

Direct user spec, 2026-09-16: "u just fetch the stocks and do backtest
with ur logic which we are planning to freeze." Runs the FROZEN mechanic
(today's own pChange price trigger -> continuous futures-OI confirm ->
VWAP-retest entry -> 20-min VWAP-close hard SL + EOD, NO trap-zone
target -- per the latest "remove the target part, focus on entry and SL"
decision) against the REAL per-day shortlist universe recovered from the
archived pre-wipe DB (archive/20260910_085314/oi_orb_screener.db) for
every real trading day it has: 2026-09-01, 02, 03, 04, 07, 08, 09.

Universe per day = the real `shortlist` table rows for that trade_date
(genuine NSE OI-spurt + price-move shortlisted stocks that day, not an
assumption). Contract resolution (option strike/expiry) reuses the SAME
live resolve_contract_async/_resolve_futures_key_and_token used for
today's own backtests -- safe to reuse as-is here because NSE stock F&O
(options AND futures) only have MONTHLY expiries, and all 7 of these
historical dates + today (2026-09-16) fall within the same September
monthly cycle (expires ~09-25) -- no weekly-rollover date-leakage risk,
confirmed before writing this script, not assumed.

The one thing that CANNOT reuse a live/wall-clock-oriented helper: the
previous-trading-day OI baseline. The book's own _compute_oi_regime_side/
_prev_day_last_tick_oi are hardcoded to real wall-clock "today" --
_prev_day_oi_asof() below is a fresh, date-parameterized rewrite of the
exact same real logic (fetch_upstox_range_1m, step back day-by-day
skipping weekends, take the last real tick's own OI) for an arbitrary
historical `ref_date`.

2026-09-16 real incident: adding the trap-target-touched pre-entry gate
(see below) roughly tripled the real API calls this script makes,
confirmed live to trip Upstox's rate limit mid-run ("status=429 after 3
retries, giving up" -- retry/backoff alone wasn't enough for a sustained
limit). Direct user follow-up: "if primary data feeder is rate limit
jump to secondary data feeder -- we have upstox, upstox2, fyers,
angelone." Checked what's actually usable: Fyers only has an INTRADAY
(today-only) fetch function in this codebase (fetch_fyers_intraday_1m,
no past-date range fetch exists), and AngelOne has no historical fetch
function here at all -- neither can serve this script's need for PAST
trading days. The two real, already-configured Upstox accounts
("upstox2" and "upstox" in ClientDB's feeder_creds) ARE independently
rate-limited and ARE usable here -- every real fetch now goes through
hc.fetch_upstox_range_1m_multi_account() with both tokens, falling back
to the second account only when the first's whole-range fetch comes
back completely empty.

MUST run on EC2 (real Upstox account access tokens + real historical
range data + the archived DB file).

Usage: python scripts/oi_orb_screener_multiday_frozen_mechanic_backtest.py
"""
from __future__ import annotations

import asyncio
import sqlite3
import sys
from datetime import date, datetime, time as dtime, timedelta

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import (
    to_heikin_ashi, to_n_min_bars_market_anchored, to_n_min_bars_dateaware,
)
from strategies.oi_orb_screener import stock_resolve, screener
from strategies.oi_orb_screener.engine import (
    OiOrbScreenerStrategy, _VWAP_SL_TF_MIN, _VWAP_SL_MIN_GAP_PCT,
    _TRAP_EXIT_HTF_MULTIDAY_MIN, _TRAP_EXIT_LOOKBACK_CALENDAR_DAYS,
)
from strategies.oi_orb_screener.screener import VwapState, RollingVwapRetestTracker

ARCHIVE_DB = "archive/20260910_085314/oi_orb_screener.db"
EOD_TIME = "15:15"
PRICE_TRIGGER_PCT = 2.0
OI_CONFIRM_THRESHOLD_PCT = 3.0
VWAP_WINDOW_MIN = 15.0
# 2026-09-16, direct user spec + real evidence (see run_symbol's own
# comment at the cutoff check): no entry counted after this time.
ENTRY_CUTOFF_TIME = dtime(13, 30)

CLIENT_ID = "ssrajpal2001"
BINDING_ID = "UPSTOX"


class _NullBus:
    def subscribe(self, topic):
        return None

    def unsubscribe(self, topic, q):
        pass

    async def publish(self, topic, event):
        pass


def _access_tokens():
    """Both real Upstox accounts this system has, primary first -- see
    the module docstring's 2026-09-16 rate-limit incident for why this
    is a list, not a single token."""
    db = ClientDB()
    tokens = []
    for account in ("upstox2", "upstox"):
        creds = db.get_feeder_creds_sync(account)
        if creds and creds.get("access_token"):
            tokens.append(creds["access_token"])
    if not tokens:
        raise RuntimeError("No upstox/upstox2 feeder access_token found -- run this on EC2.")
    return tokens


def _real_day_universe():
    con = sqlite3.connect(ARCHIVE_DB)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT DISTINCT trade_date, symbol FROM shortlist WHERE trade_date >= '2026-09-01' "
        "ORDER BY trade_date, symbol"
    ).fetchall()
    con.close()
    out = {}
    for r in rows:
        out.setdefault(r["trade_date"], []).append(r["symbol"])
    return out


def _to_bars(rows):
    out = []
    for r in rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        out.append(Bar(ts=ts.astimezone(IST), open=float(r["open"]), high=float(r["high"]),
                        low=float(r["low"]), close=float(r["close"])))
    return out


async def _prev_day_oi_asof(fut_key, tokens, ref_date, max_step_back=7):
    """Date-parameterized rewrite of fetch_upstox_prev_day_last_tick_oi's
    exact real logic (step back real trading days, take the last real
    tick's own OI) for an arbitrary historical ref_date instead of
    wall-clock today."""
    d = ref_date - timedelta(days=1)
    for _ in range(max_step_back):
        if d.weekday() < 5:
            rows = await hc.fetch_upstox_range_1m_multi_account(fut_key, tokens, d, d)
            if rows:
                oi = rows[-1].get("oi")
                if oi:
                    return float(oi)
        d -= timedelta(days=1)
    return None


async def _trap_target_touched_today(eq_key, tokens, trade_date, side, entry_ts):
    """Real, currently-live pre-entry gate (OiOrbScreenerStrategy.
    _check_trap_target_touched_today, added 2026-09-16), faithfully
    date-parameterized for a historical trade_date/entry_ts instead of
    wall-clock now(). Confirmed real via a real SOLARINDS 09-07 check:
    the trap-target zone was touched 7 times at market open, over 2
    hours before the 11:17 VWAP-retest fired -- under the real live
    gate, that entry would have been skipped entirely. Returns
    (touched: bool, zone: dict|None). Same best-effort degrade as the
    real gate -- any failure returns (False, None), never blocks."""
    try:
        start = trade_date - timedelta(days=_TRAP_EXIT_LOOKBACK_CALENDAR_DAYS)
        rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, start, trade_date)
        seen, all_rows = set(), []
        for r in sorted(rows or [], key=lambda r: r["ts"]):
            if r["ts"] in seen:
                continue
            seen.add(r["ts"])
            all_rows.append(r)
        bars = _to_bars(all_rows)
        if not bars:
            return False, None
        htf = to_n_min_bars_dateaware(bars, _TRAP_EXIT_HTF_MULTIDAY_MIN)
        if len(htf) < 3:
            return False, None
        zones_fn = screener.bull_trap_zones if side == "CALL" else screener.sharp_bear_zones
        zones_all = zones_fn(htf)
        zone = OiOrbScreenerStrategy._latest_locked_zone(zones_all, entry_ts)
        if zone is None:
            return False, None
        today_start = datetime.combine(trade_date, dtime.min, tzinfo=IST)
        touched = any(
            zone["lock_ts"] <= b.ts and today_start <= b.ts <= entry_ts
            and zone["zone_lo"] <= b.close <= zone["zone_hi"]
            for b in bars
        )
        return touched, zone
    except Exception:
        return False, None


async def _prev_close_asof(eq_key, tokens, ref_date, max_step_back=10):
    """2026-09-16 bug fix: fetch_upstox_daily() is hardcoded to
    date.today()-1 (real wall-clock today), not an arbitrary historical
    ref_date -- for a past trade_date like 2026-09-01, that window never
    reaches far enough back, silently returning no usable prev_close and
    making every single symbol fail identically (confirmed live: this is
    exactly what happened for 5 of 7 real days before this fix). Same
    date-parameterized step-back pattern as _prev_day_oi_asof below --
    real 1-min bars for the actual previous trading day, last close."""
    d = ref_date - timedelta(days=1)
    for _ in range(max_step_back):
        if d.weekday() < 5:
            rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, d, d)
            if rows:
                return float(rows[-1]["close"])
        d -= timedelta(days=1)
    return None


async def run_symbol(book, tokens, trade_date: date, symbol: str):
    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        return None
    prev_close = await _prev_close_asof(eq_key, tokens, trade_date)
    if not prev_close:
        return None

    day_rows = await hc.fetch_upstox_range_1m_multi_account(eq_key, tokens, trade_date, trade_date)
    day_bars = _to_bars(day_rows)
    if not day_bars:
        return None

    side = trig_ts = None
    for b in day_bars:
        pchange = (b.close - prev_close) / prev_close * 100.0
        if pchange >= PRICE_TRIGGER_PCT or pchange <= -PRICE_TRIGGER_PCT:
            side = "CALL" if pchange > 0 else "PUT"
            trig_ts = b.ts
            break
    if side is None:
        return {"symbol": symbol, "stage": "no_price_trigger"}

    resolved = await book._resolve_futures_key_and_token(symbol)
    if resolved is None:
        return {"symbol": symbol, "stage": "no_futures_key", "side": side, "trig_ts": trig_ts}
    fut_key, _tok = resolved
    yday_oi = await _prev_day_oi_asof(fut_key, tokens, trade_date)
    if not yday_oi:
        return {"symbol": symbol, "stage": "no_yday_oi", "side": side, "trig_ts": trig_ts}

    oi_day_rows = await hc.fetch_upstox_range_1m_multi_account(fut_key, tokens, trade_date, trade_date)
    confirm_ts = None
    readings = []
    trig_floor = trig_ts.replace(second=0, microsecond=0)
    for r in oi_day_rows:
        ts = r["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        ts = ts.astimezone(IST)
        if ts < trig_floor:
            continue
        oi = r.get("oi")
        if not oi:
            continue
        change_pct = (float(oi) - yday_oi) / yday_oi * 100.0
        readings.append(change_pct)
        if change_pct > OI_CONFIRM_THRESHOLD_PCT:
            confirm_ts = ts
            break
    if confirm_ts is None:
        detail = (f"yday_oi={yday_oi:.0f}, {len(readings)} real readings from {trig_ts.strftime('%H:%M')}, "
                   f"first={readings[0]:+.2f}% last={readings[-1]:+.2f}% peak={max(readings):+.2f}%"
                   if readings else f"yday_oi={yday_oi:.0f}, NO real OI readings at/after trigger")
        return {"symbol": symbol, "stage": "no_oi_confirm", "side": side, "trig_ts": trig_ts, "detail": detail}

    tracker = RollingVwapRetestTracker(window_min=VWAP_WINDOW_MIN)
    vwap_state = VwapState()
    fire_ts = fire_price = None
    for b in day_bars:
        typical = (b.high + b.low + b.close) / 3.0
        vwap_state.update(symbol, typical, 1.0)
        vwap = vwap_state.current(symbol)
        if vwap is None:
            continue
        bar_ts = b.ts.replace(second=0, microsecond=0)
        fired = tracker.check(side, bar_ts, b.close, vwap)
        if fired and bar_ts >= confirm_ts.replace(second=0, microsecond=0):
            fire_ts, fire_price = bar_ts, b.close
            break
    if fire_ts is None:
        detail = (f"OI-confirmed at {confirm_ts.strftime('%H:%M')} but price never retested back "
                   f"through VWAP within {VWAP_WINDOW_MIN:.0f}min of arming, for the rest of the day")
        return {"symbol": symbol, "stage": "no_vwap_retest", "side": side, "trig_ts": trig_ts,
                "confirm_ts": confirm_ts, "detail": detail}

    # 2026-09-16, direct user spec, backed by real evidence checked this
    # session: entries after 13:30 netted +3.99 pts across 7 real trades
    # vs +204.20 across the 12 real trades at/before 13:30 -- late
    # entries add near-zero value once the day's real move (91% of the
    # 35 real stocks checked had their biggest range in the 09:15-11:30
    # morning session, none in the afternoon) has typically already
    # played out. New hard cutoff: no entry counted after 13:30.
    if fire_ts.time() > ENTRY_CUTOFF_TIME:
        detail = f"VWAP retest fired at {fire_ts.strftime('%H:%M')}, after the {ENTRY_CUTOFF_TIME.strftime('%H:%M')} entry cutoff"
        return {"symbol": symbol, "stage": "entry_cutoff_skipped", "side": side, "trig_ts": trig_ts,
                "confirm_ts": confirm_ts, "fire_ts": fire_ts, "detail": detail}

    eod_ts = datetime.combine(trade_date, datetime.strptime(EOD_TIME, "%H:%M").time(), tzinfo=IST)

    # 2026-09-16, real live pre-entry gate: if today's own 180-min
    # multi-day trap-target zone was already touched by real price
    # (market open -> entry) before this VWAP-retest fired, the real
    # live system skips the entry entirely (see _check_trap_target_
    # touched_today's own docstring -- confirmed real via SOLARINDS
    # 09-07, touched 7x at market open, over 2hrs before its 11:17
    # entry). Checked here so this multi-day backtest can't count a
    # trade the real live system would never have taken.
    touched, zone = await _trap_target_touched_today(eq_key, tokens, trade_date, side, fire_ts)
    if touched:
        detail = (f"zone=[{zone['zone_lo']:.2f},{zone['zone_hi']:.2f}] locked={zone['lock_ts']} -- "
                   f"already touched by real price before this entry, real live gate would skip it")
        return {"symbol": symbol, "stage": "trap_gate_skipped", "side": side, "trig_ts": trig_ts,
                "confirm_ts": confirm_ts, "fire_ts": fire_ts, "detail": detail}

    # -- real 20-min VWAP-close hard SL, same static method as today's scripts --
    ha_1m = to_heikin_ashi(day_bars)
    ha_tf = to_n_min_bars_market_anchored(ha_1m, _VWAP_SL_TF_MIN)
    vwap_state2 = VwapState()
    vwap_at_minute = {}
    for b in day_bars:
        typical = (b.high + b.low + b.close) / 3.0
        vwap_state2.update(symbol, typical, 1.0)
        v = vwap_state2.current(symbol)
        if v is not None:
            vwap_at_minute[b.ts.replace(second=0, microsecond=0)] = v
    sorted_minutes = sorted(vwap_at_minute.keys())

    def _vwap_as_of(bucket_end):
        eligible = [ts for ts in sorted_minutes if ts < bucket_end]
        return vwap_at_minute[eligible[-1]] if eligible else None

    # 2026-09-16 bug fix: an SL bucket whose own close falls AFTER the real
    # EOD square-off time can never be a genuine live exit -- the position
    # would already have been flattened by EOD square-off in reality before
    # that bucket ever finished forming. Cap the scan at eod_ts so a
    # same-day "last bucket of the trading day" (e.g. 15:15-15:35) can't be
    # mistaken for a real SL that fired after the market already closed.
    sl_ts = None
    entry_floor = fire_ts.replace(second=0, microsecond=0)
    for hb in ha_tf:
        bucket_end = hb.ts + timedelta(minutes=_VWAP_SL_TF_MIN)
        if bucket_end <= entry_floor:
            continue
        if bucket_end > eod_ts:
            break   # no bucket closing after EOD can be a real live SL
        vwap_now = _vwap_as_of(bucket_end)
        if vwap_now is None or vwap_now <= 0:
            continue
        if OiOrbScreenerStrategy._ha_vwap_close_sl_adverse(hb, vwap_now, side):
            sl_ts = bucket_end
            break

    exit_ts = sl_ts if sl_ts is not None else eod_ts
    exit_reason = "vwap_close_sl" if sl_ts is not None else "eod_squareoff"

    opt_type = "CE" if side == "CALL" else "PE"
    contract = await stock_resolve.resolve_contract_async(symbol, fire_price, opt_type)
    if contract is None:
        return {"symbol": symbol, "stage": "no_contract", "side": side, "trig_ts": trig_ts,
                "confirm_ts": confirm_ts, "fire_ts": fire_ts}
    opt_rows = await hc.fetch_upstox_range_1m_multi_account(contract.upstox_key, tokens, trade_date, trade_date)
    opt_bars = _to_bars(opt_rows)
    entry_candidates = [b for b in opt_bars if b.ts <= fire_ts]
    exit_candidates = [b for b in opt_bars if b.ts <= exit_ts]
    if not entry_candidates or not exit_candidates:
        return {"symbol": symbol, "stage": "no_option_premium", "side": side, "trig_ts": trig_ts,
                "confirm_ts": confirm_ts, "fire_ts": fire_ts}
    entry_price, exit_price = entry_candidates[-1].close, exit_candidates[-1].close
    pnl = round(exit_price - entry_price, 2)

    return {"symbol": symbol, "stage": "TRADED", "side": side, "trig_ts": trig_ts,
            "confirm_ts": confirm_ts, "fire_ts": fire_ts, "fire_price": fire_price,
            "exit_ts": exit_ts, "exit_reason": exit_reason,
            "entry_price": entry_price, "exit_price": exit_price, "pnl": pnl}


async def main():
    tokens = _access_tokens()
    universe = _real_day_universe()
    print("=" * 130)
    print("OI-ORB Screener -- MULTI-DAY backtest of the FROZEN mechanic (price trigger -> OI confirm -> "
          "VWAP retest -> 20min VWAP-close hard SL + EOD, NO target)")
    print(f"Real per-day universe recovered from {ARCHIVE_DB}'s shortlist table: "
          f"{sum(len(v) for v in universe.values())} (date,symbol) rows across {len(universe)} real days")
    print(f"Using {len(tokens)} real Upstox account(s) with automatic fallback on rate limit")
    print("=" * 130)

    bus = _NullBus()
    book = OiOrbScreenerStrategy(
        bus, cfg=None, client_id=CLIENT_ID, binding_id=BINDING_ID,
        lot_multiplier=1, product_type="MIS", squareoff_time="15:15",
        strategy_name="oi_orb_screener_top20",
    )

    grand_total = 0.0
    grand_trades = 0
    for trade_date_str in sorted(universe.keys()):
        trade_date = date.fromisoformat(trade_date_str)
        symbols = universe[trade_date_str]
        print(f"\n{'=' * 130}\n{trade_date_str}  ({len(symbols)} real shortlisted symbols)\n{'=' * 130}")
        day_total, day_trades = 0.0, 0
        for sym in symbols:
            try:
                r = await run_symbol(book, tokens, trade_date, sym)
            except Exception as exc:
                print(f"  {sym:14s} EXCEPTION: {exc}")
                continue
            if r is None:
                print(f"  {sym:14s} no real data available")
                continue
            if r["stage"] != "TRADED":
                side_part = f" side={r['side']}" if r.get("side") else ""
                detail_part = f" -- {r['detail']}" if r.get("detail") else ""
                print(f"  {sym:14s} stopped at: {r['stage']}{side_part}{detail_part}")
                continue
            print(f"  {sym:14s} {r['side']:4s} trig={r['trig_ts'].strftime('%H:%M')} "
                  f"oi_confirm={r['confirm_ts'].strftime('%H:%M')} "
                  f"entry={r['fire_ts'].strftime('%H:%M')}@{r['entry_price']} "
                  f"exit={r['exit_ts'].strftime('%H:%M')}@{r['exit_price']} [{r['exit_reason']}] "
                  f"PNL={r['pnl']:+.2f}")
            day_total += r["pnl"]
            day_trades += 1
        print(f"  --- {trade_date_str} TOTAL: {day_trades} trade(s), {day_total:+.2f} pts ---")
        grand_total += day_total
        grand_trades += day_trades

    print("\n" + "=" * 130)
    print(f"GRAND TOTAL, frozen mechanic (entry+SL only, no target), real 7-day universe: "
          f"{grand_trades} trade(s), {grand_total:+.2f} pts")
    print("CAVEAT: 7 real trading days is still a small sample. Contract resolution reuses today's live "
          "expiry logic, safe here only because all 7 dates + today share the same September monthly F&O "
          "cycle (stock options/futures are monthly-only in NSE, no weekly rollover risk in this window).")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
