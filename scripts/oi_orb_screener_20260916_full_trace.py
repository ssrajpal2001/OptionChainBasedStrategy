"""
scripts/oi_orb_screener_20260916_full_trace.py

Real-data FULL DECISION TRACE, 2026-09-16, direct user spec: "why trade
where not taken ur output dosent show tha... i thought it should have
showz th 2 per logic with oi future threshold and if both pass thdn the
cande concept and then trade what exactly happened with all th stocks."

Shows, for EVERY one of today's real 20-stock top20 OI-spurt universe,
under TODAY'S LIVE config (PRICE_MOVE_MIN_PCT=2.0%, OI_REGIME_DECREASE_
MAX_PCT=-5.0%), the full step-by-step decision chain:

  1. STEP 1 (price filter): today's real |pChange| vs the 2% cutoff --
     pass/fail. A fail stops here (never even reaches the OI gate).
  2. STEP 2 (futures-OI regime): real today_0915_oi vs yday_1539_oi ->
     change% -> DECREASING (<=-5%) or INCREASING (>-5%).
  3. STEP 3 (the "candle concept" -- only for DECREASING): yesterday's
     real daily candle direction (bullish/bearish/doji) vs today's own
     pChange sign -- a trade only fires if today REVERSES yesterday's
     move; a continuation (or doji) blocks here. INCREASING skips this
     check entirely -- side comes straight from yesterday's candle.
  4. STEP 4 (VWAP retest): did a genuine real historical VWAP-retest
     for the resulting side actually complete today, via the SAME
     historical_rolling_retest_check() the live engine itself uses.
  5. STEP 5 (trade outcome): if step 4 fired, the real HA+StochRSI exit
     replay and reconstructed P&L (same simulation as the whatif/sweep
     scripts).

Never reimplements the live decision logic for the AUTHORITATIVE result
-- calls the real OiOrbScreenerStrategy._compute_oi_regime_side() for
the actual side, and separately re-derives the human-readable
intermediate steps (yesterday's candle direction, today's side-from-
pchange) purely for display, the same "re-derive for display without
touching the real decision function" pattern already used in
strategies/oi_orb_screener/engine.py's own _record_oi_regime_blocked().

MUST run on EC2 (real Upstox2 access token + real intraday history needed).

Usage: python scripts/oi_orb_screener_20260916_full_trace.py
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

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

_MIN_WARM_1M_BARS = 400
CLIENT_ID = "ssrajpal2001"
BINDING_ID = "UPSTOX"
TRADE_DATE = "2026-09-16"
EOD_TIME = "15:15"
RSI_PERIOD, STOCH_PERIOD, SMOOTH = 9, 9, 3

# Today's LIVE config -- same values currently deployed.
PRICE_MOVE_MIN_PCT = 2.0
OI_REGIME_DECREASE_MAX_PCT = -5.0

# Real 20-stock universe, last real poll of today (2026-09-16T15:29:23+05:30,
# the only ~10min window this session's own DB wipes left intact -- see
# oi_orb_screener_20260916_threshold_sweep.py's own module docstring).
UNIVERSE = [
    ("PATANJALI", 7.43), ("ASTRAL", 0.28), ("POLICYBZR", 5.47),
    ("PREMIERENE", -4.90), ("PAYTM", 3.64), ("BLUESTARCO", 1.56),
    ("OFSS", -2.27), ("SOLARINDS", -1.82), ("BAJAJHLDNG", -1.01),
    ("WAAREEENER", -1.99), ("YESBANK", 1.21), ("LAURUSLABS", -1.31),
    ("NESTLEIND", 1.63), ("BSE", -2.18), ("TCS", -3.02), ("NYKAA", -2.14),
    ("MFSL", 3.83), ("BOSCHLTD", 1.25), ("SHRIRAMFIN", -0.08), ("COFORGE", -0.37),
]


class _NullBus:
    def subscribe(self, topic):
        return None

    def unsubscribe(self, topic, q):
        pass

    async def publish(self, topic, event):
        pass


def _access_token() -> str:
    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox2")
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


async def _simulate_one(symbol: str, side: str, token: str) -> dict:
    try:
        results = await asyncio.to_thread(
            screener.historical_rolling_retest_check, {symbol: side}, screener.CONFIG, 15.0)
    except Exception as exc:
        return {"fired": False, "reason": f"retest check raised: {exc}"}
    r = results.get(symbol)
    if not r or not r.get("fired"):
        return {"fired": False, "reason": "no genuine VWAP-retest completed today"}

    entry_ts = datetime.combine(date.fromisoformat(TRADE_DATE),
                                 datetime.strptime(r["fire_ts"], "%H:%M").time(), tzinfo=IST)
    entry_price_spot = r["fire_price"]
    out = {"fired": True, "entry_ts": r["fire_ts"], "entry_spot": entry_price_spot}

    eq_key = stock_resolve.resolve_eq_instrument_key(symbol)
    if not eq_key:
        out["reason"] = "NO_EQ_KEY"
        return out
    warm_rows = await hc.fetch_upstox_warm_1m(eq_key, token, min_bars=_MIN_WARM_1M_BARS)
    all_bars = _to_bars(warm_rows)
    entry_floor = entry_ts.replace(second=0, microsecond=0)
    post_entry_bars = [b for b in all_bars if b.ts >= entry_floor]
    if len(all_bars) < 15 or not post_entry_bars:
        out["reason"] = f"INSUFFICIENT_DATA ({len(all_bars)} bars)"
        return out

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
        exit_signal_ts = datetime.combine(date.fromisoformat(TRADE_DATE),
                                           datetime.strptime(EOD_TIME, "%H:%M").time(), tzinfo=IST)
        out["exit_reason"] = "eod_squareoff (HA+StochRSI never fired)"
    else:
        out["exit_reason"] = "ha_stoch_exit"
    out["exit_ts"] = exit_signal_ts.strftime("%H:%M:%S")

    entry_price = None
    exit_price = None
    try:
        opt_type = "CE" if side == "CALL" else "PE"
        contract = await stock_resolve.resolve_contract_async(symbol, entry_price_spot, opt_type)
        if contract is None:
            out["reason"] = "could not resolve a real tradable contract"
            return out
        opt_rows = await hc.fetch_upstox_intraday_1m(contract.upstox_key, token)
        opt_bars = _to_bars(opt_rows)
        entry_candidates = [b for b in opt_bars if b.ts <= entry_ts]
        if entry_candidates:
            entry_price = entry_candidates[-1].close
        exit_candidates = [b for b in opt_bars if b.ts <= exit_signal_ts]
        if exit_candidates:
            exit_price = exit_candidates[-1].close
    except Exception as exc:
        out["reason"] = f"option resolution failed: {exc}"
        return out

    out["entry_price"] = entry_price
    out["exit_price"] = exit_price
    if entry_price is not None and exit_price is not None:
        # 2026-09-16 CRITICAL bug fix: a long option (CE or PE) profits
        # purely on its own premium direction, no side-based sign flip --
        # see oi_orb_screener_20260916_live_oi_confirm_backtest.py's
        # _simulate_exit for the full reasoning (confirmed against the
        # real live engine's own P&L formula, no CALL/PUT branch there).
        out["pnl"] = round(exit_price - entry_price, 2)
    else:
        out["reason"] = "option premium history unavailable"
    return out


async def main():
    token = _access_token()
    print("=" * 130)
    print(f"OI-ORB Screener (top20) -- {TRADE_DATE} FULL DECISION TRACE, today's LIVE config "
          f"(PRICE_MOVE_MIN_PCT={PRICE_MOVE_MIN_PCT}%, OI_REGIME_DECREASE_MAX_PCT={OI_REGIME_DECREASE_MAX_PCT}%)")
    print("=" * 130)

    bus = _NullBus()
    book = OiOrbScreenerStrategy(
        bus, cfg=None, client_id=CLIENT_ID, binding_id=BINDING_ID,
        lot_multiplier=1, product_type="MIS", squareoff_time="15:15",
        strategy_name="oi_orb_screener_top20",
    )
    book._screener_cfg["OI_REGIME_GATE_ENABLED"] = True
    book._screener_cfg["IGNORE_TIME_WINDOWS"] = True
    book._screener_cfg["OI_REGIME_DECREASE_MAX_PCT"] = OI_REGIME_DECREASE_MAX_PCT
    for sym, pch in UNIVERSE:
        book._shortlist_pchange[sym] = pch

    for sym, pch in UNIVERSE:
        print(f"\n{'-' * 130}")
        print(f"{sym}")
        print(f"{'-' * 130}")

        # STEP 1: price-move filter (this is the REAL check top20's own
        # build_top20_shortlist/_stream_new_top20_symbols apply).
        passes_price = abs(pch) >= PRICE_MOVE_MIN_PCT
        print(f"  STEP 1 -- price move filter: pChange={pch:+.2f}% "
              f"vs {PRICE_MOVE_MIN_PCT}% cutoff -> "
              f"{'PASS' if passes_price else 'FAIL -- stock never even enters the shortlist today'}")
        if not passes_price:
            continue

        # STEP 2: real futures-OI regime (the REAL, authoritative decision).
        side = await book._compute_oi_regime_side(sym)
        today_oi = book._today_0915_oi.get(sym)
        yday_oi = book._prev_day_last_tick_oi.get(sym)
        if today_oi is not None and yday_oi:
            oi_change = round((today_oi - yday_oi) / yday_oi * 100.0, 2)
            regime = "DECREASING" if oi_change <= OI_REGIME_DECREASE_MAX_PCT else "INCREASING"
            print(f"  STEP 2 -- futures-OI regime: today_0915_oi={today_oi:.0f} "
                  f"yday_1539_oi={yday_oi:.0f} change={oi_change:+.2f}% "
                  f"(cutoff {OI_REGIME_DECREASE_MAX_PCT}%) -> regime={regime}")
        else:
            print(f"  STEP 2 -- futures-OI regime: DATA UNAVAILABLE "
                  f"(today_0915_oi={today_oi} yday_1539_oi={yday_oi})")
            print(f"  RESULT: BLOCKED (no futures-OI data) -- no trade today")
            continue

        # STEP 3: the "candle concept" -- yesterday's real daily candle vs
        # today's own pChange sign. Only meaningful for DECREASING; for
        # INCREASING the real _compute_oi_regime_side already used it
        # internally to pick the side, so just show what it resolved to.
        resolved = await book._resolve_futures_key_and_token(sym)
        yday_dir = None
        if resolved is not None:
            fut_key, tok = resolved
            yday_dir = await book._yesterday_candle_direction(fut_key, tok)
        if regime == "DECREASING":
            today_side = "CALL" if pch > 0 else ("PUT" if pch < 0 else None)
            print(f"  STEP 3 -- candle concept (DECREASING): today's pChange sign -> "
                  f"today_side={today_side or 'FLAT (blocks immediately)'}; "
                  f"yesterday's real daily candle -> {yday_dir or 'doji/no data (blocks)'}")
            if today_side and yday_dir:
                is_reversal = (yday_dir == "bullish" and today_side == "PUT") or \
                              (yday_dir == "bearish" and today_side == "CALL")
                print(f"    -> {'REVERSAL of yesterday -- fires ' + today_side if is_reversal else 'CONTINUATION of yesterday (not a reversal) -- BLOCKED'}")
        else:
            print(f"  STEP 3 -- candle concept (INCREASING): side comes ONLY from yesterday's "
                  f"real daily candle -> {yday_dir or 'doji/no data (blocks)'} "
                  f"-> {'CALL' if yday_dir == 'bullish' else ('PUT' if yday_dir == 'bearish' else 'BLOCKED')}")

        if side is None:
            print(f"  RESULT: BLOCKED -- removed from pool for today, no trade")
            continue
        print(f"  RESOLVED SIDE: {side}")

        # STEP 4 + 5: real VWAP retest + HA/StochRSI exit simulation.
        sim = await _simulate_one(sym, side, token)
        if not sim.get("fired"):
            print(f"  STEP 4 -- VWAP retest: NOT FIRED ({sim.get('reason')})")
            print(f"  RESULT: gate passed, side={side}, but no real trade today")
            continue
        print(f"  STEP 4 -- VWAP retest: FIRED at {sim['entry_ts']} (spot={sim['entry_spot']:.2f})")
        if "pnl" in sim:
            print(f"  STEP 5 -- trade outcome: entry@{sim['entry_ts']} opt={sim.get('entry_price')} "
                  f"-> exit@{sim['exit_ts']} opt={sim.get('exit_price')} "
                  f"reason={sim['exit_reason']} PNL={sim['pnl']:+.2f} pts")
            print(f"  RESULT: REAL TRADE -- {side} {sym}, {sim['pnl']:+.2f} pts")
        else:
            print(f"  STEP 5 -- trade outcome: entry fired but could not fully reconstruct "
                  f"({sim.get('reason')})")
            print(f"  RESULT: entry fired, P&L not reconstructed")

    print("\n" + "=" * 130)
    print("Done -- every stock above shows the exact step it stopped at (or the full trade if it went all the way).")
    print("=" * 130)


if __name__ == "__main__":
    asyncio.run(main())
