"""
scripts/oi_orb_screener_20260916_live_oi_confirm_backtest.py

Real-data backtest, 2026-09-16, direct user spec for a NEW proposed entry
mechanic: "we check pday future oi and check every min if future oi -- if
at start we see that pchange>2% we only check for call side and when
future oi threshold % threshold is found for call side then we trigger to
next step of vwap checking and then vwap is done we take trade."

This is a genuinely different mechanic from the live gate (which decides
side from yesterday's candle / a fixed 09:15-vs-15:39 OI comparison). Here:

  STEP 1 (price trigger): walk today's REAL 1-min equity bars from market
  open. The instant intraday pChange (vs yesterday's real close) first
  crosses +2% -> CALL candidate locked; -2% -> PUT candidate locked
  (whichever happens first). No trigger -> stock never enters this flow.

  STEP 2 (continuous OI confirm): from that trigger minute onward, walk
  REAL per-minute FUTURES OI (fetch_upstox_range_1m, confirmed carrying
  genuine OI, verified against NSE's own Bhavcopy earlier today) and
  compute OI_Change% = (that minute's OI - yesterday's real 15:39 OI) /
  yesterday's 15:39 OI x 100 at EVERY minute. The instant this crosses
  above +3.0% (OI_CONFIRM_THRESHOLD_PCT -- a first-pass default, NOT yet
  tuned, flagged clearly below) -> confirmed, move to step 3. Direct user
  spec, adopting the standard OI-interpretation framework since the user
  left the exact direction to this session's own judgement: RISING OI is
  the confirmation signal for BOTH sides (price+OI both rising = fresh
  long buildup = real bullish conviction; price+OI both falling in the
  PUT case = fresh short buildup = real bearish conviction) -- falling OI
  (short-covering / long-unwinding) is deliberately NOT treated as
  confirmation for either side.

  STEP 3 (VWAP retest): from the OI-confirm minute onward, replay the
  REAL RollingVwapRetestTracker + VwapState (strategies/oi_orb_screener/
  screener.py, same session-anchored hlc3 VWAP as the live engine and as
  TradingView's own VWAP(hlc3,Session)) over real 1-min bars from market
  open (so VWAP itself is correctly session-anchored), watching for the
  first genuine arm-then-retest AT OR AFTER the OI-confirm minute.

  STEP 4 (trade outcome): if a retest fires, reconstruct the real HA+
  StochRSI(9,9,3) exit + P&L exactly like every other backtest script
  today.

Prints EVERY timestamp and value at each step for every one of today's
real 20-stock universe, so the OI_CONFIRM_THRESHOLD_PCT default (and the
whole mechanic) can be judged against real data before any decision to
build it into the live engine.

MUST run on EC2 (real Upstox2 access token + real intraday history needed).

Usage: python scripts/oi_orb_screener_20260916_live_oi_confirm_backtest.py
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer import historical_candles as hc
from data_layer.client_db import ClientDB
from strategies.core.trap_zone_utils import Bar
from strategies.core.candle_indicators import (
    to_heikin_ashi, to_n_min_bars, compute_stoch_rsi, ha_stoch_shape_exit_signal,
)
from strategies.oi_orb_screener import stock_resolve, screener
from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy
from strategies.oi_orb_screener.screener import VwapState, RollingVwapRetestTracker

TRADE_DATE = "2026-09-16"
TODAY = date.fromisoformat(TRADE_DATE)
EOD_TIME = "15:15"
RSI_PERIOD, STOCH_PERIOD, SMOOTH = 9, 9, 3

PRICE_TRIGGER_PCT = 2.0
OI_CONFIRM_THRESHOLD_PCT = 3.0   # first-pass default, NOT yet tuned -- see module docstring
VWAP_WINDOW_MIN = 15.0

CLIENT_ID = "ssrajpal2001"
BINDING_ID = "UPSTOX"

# Real 20-stock universe (same source as today's other backtest scripts --
# see oi_orb_screener_20260916_threshold_sweep.py's own docstring for why
# this is the real, available data for today).
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


async def _find_price_trigger(sym: str, token: str):
    """Real per-minute pChange walk. Returns (side, ts, pchange, eq_bars,
    prev_close, kind) where kind is "GAP-OPEN" (already +-2% on the very
    first real bar of the day -- market already repriced before/at open)
    or "INTRADAY" (only crossed later, a slower/weaker signal with less
    runway left before EOD). Returns (None, None, None, eq_bars,
    prev_close_or_err, None) if neither +-2% was ever crossed today."""
    eq_key = stock_resolve.resolve_eq_instrument_key(sym)
    if not eq_key:
        return None, None, None, None, "NO_EQ_KEY", None
    daily = await hc.fetch_upstox_daily(eq_key, token, lookback_days=5)
    if not daily:
        return None, None, None, None, "NO_DAILY_DATA_FOR_PREV_CLOSE", None
    prev_close = None
    for row in reversed(daily):
        row_date = row["ts"][:10] if isinstance(row["ts"], str) else row["ts"].date().isoformat()
        if row_date < TRADE_DATE:
            prev_close = float(row["close"])
            break
    if not prev_close:
        return None, None, None, None, "NO_PREV_CLOSE_FOUND", None

    rows = await hc.fetch_upstox_intraday_1m(eq_key, token)
    bars = _to_bars(rows)
    if not bars:
        return None, None, None, None, "NO_INTRADAY_BARS", None

    for i, b in enumerate(bars):
        pchange = (b.close - prev_close) / prev_close * 100.0
        if pchange >= PRICE_TRIGGER_PCT or pchange <= -PRICE_TRIGGER_PCT:
            side = "CALL" if pchange > 0 else "PUT"
            kind = "GAP-OPEN" if i == 0 else "INTRADAY"
            return side, b.ts, pchange, bars, prev_close, kind
    return None, None, None, bars, prev_close, None


async def _find_oi_confirm(book: OiOrbScreenerStrategy, sym: str, trigger_ts: datetime,
                            yday_oi: float, token: str):
    """Real per-minute futures-OI walk from trigger_ts onward. Returns
    (confirm_ts, confirm_oi, confirm_change_pct, all_readings) or
    (None, None, None, all_readings) if OI never crossed the threshold today."""
    resolved = await book._resolve_futures_key_and_token(sym)
    if resolved is None:
        return None, None, None, []
    fut_key, _tok = resolved
    # 2026-09-16 bug fix: fetch_upstox_range_1m hits Upstox's
    # historical-candle endpoint, which does NOT serve the CURRENT trading
    # day (confirmed live -- every single stock came back with zero rows
    # for today's date). fetch_upstox_today_0915_oi's own already-working
    # 09:15 read uses fetch_upstox_intraday_1m (the correct endpoint for
    # today's live-building data, same parser, same real OI column) --
    # reused here for a full continuous per-minute walk instead of a
    # single fixed point.
    rows = await hc.fetch_upstox_intraday_1m(fut_key, token)
    readings = []
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
        readings.append((ts, float(oi), change_pct))
        if change_pct > OI_CONFIRM_THRESHOLD_PCT:
            return ts, float(oi), change_pct, readings
    return None, None, None, readings


def _classify_oi_quadrant(side: str, confirmed: bool, last_change_pct: Optional[float]) -> str:
    """Label today's price/OI combination against the standard framework
    (2026-09-16, user-confirmed via the real PATANJALI case -- price up +
    OI falling was correctly rejected as short covering, not a bug):
      Price up   + OI rising  = fresh long buildup   (strong, CALL confirms)
      Price up   + OI falling = short covering        (weak, correctly no trade)
      Price down + OI rising  = fresh short buildup   (strong, PUT confirms)
      Price down + OI falling = long unwinding         (weak, correctly no trade)
    Diagnostic-only label -- does not change the pass/fail decision, which
    is already driven solely by whether OI actually crossed the rising
    threshold (see _find_oi_confirm)."""
    if confirmed:
        return "fresh long buildup (strong bullish)" if side == "CALL" else "fresh short buildup (strong bearish)"
    if last_change_pct is None:
        return "no OI data to classify"
    if last_change_pct < 0:
        return "short covering (weak -- correctly no trade)" if side == "CALL" else "long unwinding (weak -- correctly no trade)"
    return "OI rising but stayed under threshold (weak-to-moderate -- correctly no trade)"


async def _find_vwap_retest(sym: str, side: str, confirm_ts: datetime, eq_bars: List[Bar]):
    """Real VWAP arm-then-retest replay from market open (for correct
    session VWAP), only counting a fire AT OR AFTER confirm_ts."""
    tracker = RollingVwapRetestTracker(window_min=VWAP_WINDOW_MIN)
    vwap_state = VwapState()
    fire_ts = fire_price = fire_vwap = None
    for b in eq_bars:
        typical = (b.high + b.low + b.close) / 3.0
        # Real bar volume isn't in this Bar object (trap_zone_utils.Bar has
        # no volume field) -- use a constant weight of 1 per bar, which
        # still produces the correct RUNNING AVERAGE typical price over
        # time (same shape as VwapState, just unweighted by real volume
        # since intraday_1m's own volume isn't threaded through here).
        # This is a KNOWN approximation, flagged in the printed output.
        vwap_state.update(sym, typical, 1.0)
        vwap = vwap_state.current(sym)
        if vwap is None:
            continue
        bar_ts = b.ts.replace(second=0, microsecond=0)
        fired = tracker.check(side, bar_ts, b.close, vwap)
        if fired and bar_ts >= confirm_ts.replace(second=0, microsecond=0):
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
        raw = exit_price - entry_price
        pnl = round(raw if side == "CALL" else -raw, 2)
    return {"entry_price": entry_price, "exit_price": exit_price, "exit_ts": exit_signal_ts,
            "exit_reason": exit_reason, "pnl": pnl}


async def main():
    token = _access_token()
    print("=" * 130)
    print(f"OI-ORB Screener -- {TRADE_DATE} NEW MECHANIC backtest: today's pChange({PRICE_TRIGGER_PCT}%) "
          f"trigger -> continuous futures-OI confirm(>{OI_CONFIRM_THRESHOLD_PCT}%) -> VWAP retest -> trade")
    print(f"Universe ({len(UNIVERSE)}): {UNIVERSE}")
    print("NOTE: OI_CONFIRM_THRESHOLD_PCT is a first-pass default, NOT yet tuned -- judge it against "
          "the real OI values printed below.")
    print("=" * 130)

    bus = _NullBus()
    book = OiOrbScreenerStrategy(
        bus, cfg=None, client_id=CLIENT_ID, binding_id=BINDING_ID,
        lot_multiplier=1, product_type="MIS", squareoff_time="15:15",
        strategy_name="oi_orb_screener_top20",
    )

    total_pnl = 0.0
    trades = 0
    for sym in UNIVERSE:
        print(f"\n{'-' * 130}")
        print(f"{sym}")
        print(f"{'-' * 130}")

        side, trigger_ts, pchange, eq_bars, prev_close_or_err, kind = await _find_price_trigger(sym, token)
        if side is None:
            if isinstance(prev_close_or_err, str) and prev_close_or_err.startswith("NO_"):
                print(f"  STEP 1 -- price trigger: FAILED ({prev_close_or_err})")
            else:
                print(f"  STEP 1 -- price trigger: pChange never crossed +-{PRICE_TRIGGER_PCT}% today "
                      f"(prev_close={prev_close_or_err})")
            continue
        print(f"  STEP 1 -- price trigger [{kind}]: {trigger_ts.strftime('%H:%M')} pChange={pchange:+.2f}% "
              f"crossed {PRICE_TRIGGER_PCT}% -> candidate side={side} "
              f"(prev_close={prev_close_or_err:.2f})")
        if kind == "INTRADAY":
            print(f"    NOTE: this only crossed {PRICE_TRIGGER_PCT}% intraday, not at the open -- "
                  f"weaker/later signal, less runway left before EOD for OI-confirm+VWAP-retest to complete.")

        if sym not in book._today_0915_oi:
            await book._compute_oi_regime_side(sym)
        yday_oi = book._prev_day_last_tick_oi.get(sym)
        if not yday_oi:
            print(f"  STEP 2 -- futures-OI confirm: NO YESTERDAY OI DATA -- cannot confirm")
            continue
        print(f"  (yesterday's real 15:39 baseline OI = {yday_oi:.0f})")

        confirm_ts, confirm_oi, confirm_change, readings = await _find_oi_confirm(
            book, sym, trigger_ts, yday_oi, token)
        if readings:
            print(f"  STEP 2 -- futures-OI confirm: {len(readings)} real per-minute readings checked "
                  f"from {readings[0][0].strftime('%H:%M')} to {readings[-1][0].strftime('%H:%M')}")
            print(f"    first reading: {readings[0][1]:.0f} ({readings[0][2]:+.2f}%)   "
                  f"last reading: {readings[-1][1]:.0f} ({readings[-1][2]:+.2f}%)   "
                  f"peak: {max(r[2] for r in readings):+.2f}%")
        else:
            print(f"  STEP 2 -- futures-OI confirm: NO real per-minute futures OI data available")
        last_change = readings[-1][2] if readings else None
        if confirm_ts is None:
            label = _classify_oi_quadrant(side, confirmed=False, last_change_pct=last_change)
            print(f"  STEP 2 -- OI regime classification: {label}")
            print(f"  RESULT: OI never crossed {OI_CONFIRM_THRESHOLD_PCT}% after the price trigger -- no trade")
            continue
        print(f"  STEP 2 -- CONFIRMED at {confirm_ts.strftime('%H:%M')}: OI={confirm_oi:.0f} "
              f"change={confirm_change:+.2f}% (crossed {OI_CONFIRM_THRESHOLD_PCT}%)")
        label = _classify_oi_quadrant(side, confirmed=True, last_change_pct=last_change)
        print(f"  STEP 2 -- OI regime classification: {label}")

        fire_ts, fire_price, fire_vwap = await _find_vwap_retest(sym, side, confirm_ts, eq_bars)
        if fire_ts is None:
            print(f"  STEP 3 -- VWAP retest: never fired after OI-confirm -- no trade")
            continue
        print(f"  STEP 3 -- VWAP retest FIRED at {fire_ts.strftime('%H:%M')}: "
              f"price={fire_price:.2f} vwap={fire_vwap:.2f}")

        sim = await _simulate_exit(sym, side, fire_ts, fire_price, token)
        if sim.get("pnl") is not None:
            print(f"  STEP 4 -- trade outcome: entry@{fire_ts.strftime('%H:%M:%S')} opt={sim['entry_price']} "
                  f"-> exit@{sim['exit_ts'].strftime('%H:%M:%S')} opt={sim['exit_price']} "
                  f"reason={sim['exit_reason']}")
            print(f"  RESULT: REAL TRADE -- {side} {sym}, PNL={sim['pnl']:+.2f} pts")
            total_pnl += sim["pnl"]
            trades += 1
        else:
            print(f"  STEP 4 -- trade outcome: entry fired but P&L not reconstructed ({sim.get('reason')})")

    print("\n" + "=" * 130)
    print(f"TOTAL under this NEW mechanic today: {trades} trade(s), {total_pnl:+.2f} pts")
    print("CAVEAT: single real day (n=1), OI_CONFIRM_THRESHOLD_PCT=3.0% is an untuned first guess, "
          "and the VWAP replay here uses unweighted (not real-volume-weighted) typical price -- "
          "directionally informative only, not a validated mechanic.")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
