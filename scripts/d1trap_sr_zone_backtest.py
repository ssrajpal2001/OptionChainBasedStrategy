"""
scripts/d1trap_sr_zone_backtest.py — BearTrap's existing HTF sweep+reclaim
zone (UNCHANGED, detected on the option's own premium chart -- exactly how
live D1TrapBearOnlyBook already works), but T1/T2 temporarily dropped and
replaced with the "ping-pong" Support & Resistance tracker
(strategies/d1_trap_option/support_resistance.py) once price re-touches the
zone.

Mechanic (confirmed with user, 2026-08-07):
  1. Outer zone: identical to live BearTrap -- sweep+reclaim detected on
     each strike's own real premium history, at the underlying's existing
     default HTF timeframe (NIFTY 60m / SENSEX 15m,
     _HTF_MINUTES_DEFAULT_BY_UNDERLYING).
  2. Inner: on the FIRST 1-minute premium touch of a zone, seed a *fresh*
     SupportResistanceCalculator and start feeding it TF-minute premium
     candles from that point forward (TF swept below).
  3. Entry: the first time the S&R state machine confirms a genuine
     breakout (phase INITIAL_TREND_ESTABLISHMENT -> R1_TRACKING, i.e. a
     real higher-high-AND-higher-low candle, not any single tick above the
     prior high).
  4. SL: trails the S&R tracker's own live S1 level (NOT frozen at entry --
     confirmed with user this should ratchet as the tracker's support
     level moves, same "structural trailing stop" idea as BearTrap's
     existing staircase TSL, just derived from S&R structure instead of a
     fixed premium %).

S&R candle timeframe is SWEPT across 1m / 3m / 5m per direct user spec --
this reports which granularity actually performs better on real data for
today, rather than assuming one value, same discipline as
scripts/d1trap_strike_ladder_backtest.py's ITM-depth sweep.

Strike selection: ATM +/- the underlying's existing default ITM offset
(_ITM_OFFSET_DEFAULT_BY_UNDERLYING) off the real day-open spot -- BearTrap's
own fixed-offset fallback path (live OI-wall selection can't be replicated
retroactively, same documented limitation as every other backtest this
session that touches strike selection).

SCOPE: standalone evaluation only. Does NOT change D1TrapBearOnlyBook.
T1/T2 are not run at all in this script (per user: "temp drop t1/t2") --
this is testing the S&R replacement in isolation, not stacked with the
existing tranches.

2026-08-07: added a persistent daily comparison log
(data/d1trap_sr_exit_variant_log.jsonl) so the TF x exit_mode comparison
accumulates a real track record across multiple days instead of being
re-read by hand from console output each run -- same pattern as
scripts/fno_positional_today_check.py's OI-vs-outcome log. Deduped per
(date, underlying, side, tf_minutes, exit_mode), safe to re-run same-day.

Run on the box with a real Upstox access_token (data/clients.db):
    python3 scripts/d1trap_sr_zone_backtest.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import GlobalConfig, IST  # noqa: E402
from data_layer.client_db import ClientDB  # noqa: E402
from data_layer.historical_candles import fetch_upstox_intraday_1m  # noqa: E402
from data_layer.instrument_registry import REGISTRY  # noqa: E402
from strategies.d1_trap_option.bear_only_book import (  # noqa: E402
    _Bar,
    _HTF_MINUTES_DEFAULT_BY_UNDERLYING,
    _ITM_OFFSET_DEFAULT_BY_UNDERLYING,
    _resample,
    _to_bars,
)
from strategies.d1_trap_option.book import _fetch_1m_bars, _upstox_key_for  # noqa: E402
from strategies.d1_trap_option.support_resistance import SRPingPongTracker, _EXIT_MODES  # noqa: E402
from scripts.d1trap_nested_15m_5m_backtest import _detect_zones  # noqa: E402

_HIST_WARMUP_DAYS = 14
_SESSION_OPEN = time(9, 15)
_ATM_ROUND_STEP = 100
_SR_TF_SWEEP = (1, 3, 5)
_LOG_PATH = Path(__file__).resolve().parents[1] / "data" / "d1trap_sr_exit_variant_log.jsonl"


def _append_variant_log(records: List[dict]) -> None:
    """Append/update today's (underlying, side, tf_minutes, exit_mode) rows in the running
    exit-variant track record. Dedupes by (date, underlying, side, tf_minutes, exit_mode) so
    re-running the script the same day updates rows in place instead of piling up
    duplicates -- same pattern as fno_positional_today_check.py's signal log."""
    if not records:
        return
    existing = []
    if _LOG_PATH.exists():
        for line in _LOG_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            existing.append(row)
    new_keys = {(r["date"], r["underlying"], r["side"], r["tf_minutes"], r["exit_mode"]) for r in records}
    kept = [r for r in existing if (r.get("date"), r.get("underlying"), r.get("side"),
                                     r.get("tf_minutes"), r.get("exit_mode")) not in new_keys]
    kept.extend(records)
    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _LOG_PATH.write_text("\n".join(json.dumps(r) for r in kept) + "\n", encoding="utf-8")


def _print_variant_track_record() -> None:
    if not _LOG_PATH.exists():
        return
    rows = []
    for line in _LOG_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    dated = [r for r in rows if r.get("pnl") is not None]
    if not dated:
        return
    days = sorted(set(r["date"] for r in rows))
    print(f"\n{'='*70}\nRUNNING EXIT-VARIANT TRACK RECORD  ({len(days)} day(s) logged: "
          f"{', '.join(days)})")
    print(f"  {'TF':<5}{'Mode':<16}{'Trades':>8}{'Win%':>7}{'Net P&L':>14}{'Profit Factor':>16}")
    combos = sorted({(r["tf_minutes"], r["exit_mode"]) for r in dated})
    for tf, mode in combos:
        bucket = [r for r in dated if r["tf_minutes"] == tf and r["exit_mode"] == mode]
        n = len(bucket)
        wins = [r["pnl"] for r in bucket if r["pnl"] > 0]
        losses = [r["pnl"] for r in bucket if r["pnl"] <= 0]
        win_pct = (len(wins) / n * 100) if n else 0.0
        net = sum(r["pnl"] for r in bucket)
        gross_loss = abs(sum(losses))
        pf = (sum(wins) / gross_loss) if gross_loss > 0 else float("inf") if wins else 0.0
        pf_str = "inf" if pf == float("inf") else f"{pf:.2f}"
        sign = "+" if net >= 0 else ""
        print(f"  {tf}m{'':<3}{mode:<16}{n:>8}{win_pct:>6.1f}%{sign}Rs{net:>10.2f}{pf_str:>16}")
    print("  (Still early -- read this as a running signal, not a verdict, until it covers "
          "at least 1-2 real weeks.)")


def _candles_to_bars(candles: list) -> List[_Bar]:
    """Matches strategies/d1_trap_option/book.py's own _fetch_bars parsing
    exactly (.astimezone(IST)) -- without it, these "today" bars end up with
    a different tz representation than the historical bars from
    _fetch_1m_bars, and pandas silently falls back to object dtype when the
    two are combined into one DataFrame column, breaking the .dt accessor
    _resample() depends on (confirmed live: AttributeError "Can only use
    .dt accessor with datetimelike values")."""
    out = []
    for c in candles:
        ts = datetime.fromisoformat(c["ts"]) if isinstance(c["ts"], str) else c["ts"]
        ts = ts.astimezone(IST)
        out.append(_Bar(timestamp=ts, open=float(c["open"]), high=float(c["high"]),
                         low=float(c["low"]), close=float(c["close"])))
    return out


def _bars_to_df(bars: List[_Bar]):
    import pandas as pd
    return pd.DataFrame([
        {"datetime": b.timestamp, "open": b.open, "high": b.high, "low": b.low, "close": b.close}
        for b in bars
    ])


def _run_sr_variant(zones: List[dict], today_1m: List[_Bar], tf_minutes: int, lot_size: int,
                     exit_mode: str = "raw", sl_buffer_pct: float = None, gate_mode: str = "touch") -> dict:
    """One S&R-timeframe variant for one (side, day) -- thin wrapper driving the shared
    strategies.d1_trap_option.support_resistance.SRPingPongTracker bar-by-bar, so this
    backtest and the live D1TrapSRBook (strategies/d1_trap_option/sr_book.py) can never
    silently diverge (2026-08-08 refactor -- see SRPingPongTracker's docstring for the
    full mechanic writeup, previously inlined here).

    sl_buffer_pct only affects exit_mode="buffered" (None = SRPingPongTracker's own
    default, currently 2%) -- added 2026-08-08 to sweep wider give-back buffers.

    gate_mode="touch" (default, validated BANKNIFTY config) starts S&R the instant
    price touches the zone; gate_mode="breach" (2026-08-09) waits for the zone's own
    MTF ref-candle breach_ts first -- requires `zones` to come from the real
    D1TrapBearOnlyBook state machine (book._series[side].zones), not a bare
    zone_lo/zone_hi/lock_ts dict, since only that populates breach_ts.

    Returns {"no_entry": True, "voided": [...]} if nothing fired, otherwise
    entry/exit/pnl/"voided" (the "trace" field this used to carry was internal
    per-bucket debug state, dropped when the loop moved into the tracker -- use
    the tracker directly if per-candle trace output is needed again)."""
    kwargs = {} if sl_buffer_pct is None else {"sl_buffer_pct": sl_buffer_pct}
    tracker = SRPingPongTracker(zones, tf_minutes, lot_size, exit_mode=exit_mode, gate_mode=gate_mode, **kwargs)
    for bar in today_1m:
        ev = tracker.on_bar(bar)
        if ev and ev["type"] == "exit":
            trace = tracker.active_sr.get(ev["zone_ts"], {}).get("trace", [])
            return {"entry_ts": ev["entry_ts"], "entry_premium": ev["entry_premium"],
                     "exit_reason": ev["reason"], "exit_price": ev["exit_price"],
                     "exit_ts": ev["exit_ts"], "pnl": ev["pnl"], "trace": trace, "voided": tracker.voided}
        if tracker.day_done and tracker.position is None:
            break

    # A position opened near the end of the available bars (no later bar ever hit
    # EOD_TIME or the trailing SL) -- close it at the last available price rather than
    # silently discarding a real fired entry (matches the pre-refactor behavior).
    if tracker.position is not None:
        last_bar = today_1m[-1]
        pos = tracker.position
        pnl = (last_bar.close - pos["entry_premium"]) * lot_size
        trace = tracker.active_sr.get(pos["zone_ts"], {}).get("trace", [])
        return {"entry_ts": pos["entry_ts"], "entry_premium": pos["entry_premium"],
                "exit_reason": "still running (no exit before available data ended)",
                "exit_price": last_bar.close, "exit_ts": last_bar.timestamp, "pnl": pnl,
                "trace": trace, "voided": tracker.voided}

    return {"no_entry": True, "voided": tracker.voided}


async def check_underlying(underlying: str, token: str, cfg: GlobalConfig) -> None:
    print(f"\n{'='*70}\n{underlying}  (HTF zone unchanged, S&R replaces T1/T2, "
          f"SL trails S&R's own S1)")

    lot_size = int(cfg.exchange.lot_sizes.get(underlying, 75))
    itm_offset = _ITM_OFFSET_DEFAULT_BY_UNDERLYING.get(underlying, 200)
    htf_minutes = _HTF_MINUTES_DEFAULT_BY_UNDERLYING.get(underlying, 60)
    today = datetime.now(IST).date()

    spot_key = _upstox_key_for(underlying)
    spot_today = await fetch_upstox_intraday_1m(spot_key, token)
    if not spot_today:
        print("  SKIP: no real spot data for today.")
        return
    spot_open = float(spot_today[0]["open"])
    atm = round(spot_open / _ATM_ROUND_STEP) * _ATM_ROUND_STEP
    ce_strike, pe_strike = int(atm - itm_offset), int(atm + itm_offset)
    print(f"  Real day-open spot={spot_open:.2f} ATM={atm} -> CE strike={ce_strike} PE strike={pe_strike} "
          f"(fixed-offset fallback, HTF={htf_minutes}m)")

    try:
        await asyncio.to_thread(REGISTRY.load_sync, underlying, token)
    except Exception:
        pass
    expiry = REGISTRY.get_active_expiry(underlying, today)
    if not expiry:
        print("  SKIP: no active expiry resolved.")
        return

    for direction, strike in (("CE", ce_strike), ("PE", pe_strike)):
        opt_key = REGISTRY.get_upstox_key(underlying, expiry, strike, direction)
        if not opt_key:
            print(f"  {direction}{strike}: SKIP -- no Upstox instrument key.")
            continue

        hist_start = today - timedelta(days=_HIST_WARMUP_DAYS)
        hist_end = today - timedelta(days=1)
        hist_1m = await asyncio.to_thread(_fetch_1m_bars, opt_key, hist_start, hist_end, token)
        today_candles = await fetch_upstox_intraday_1m(opt_key, token)
        if not today_candles:
            print(f"  {direction}{strike}: SKIP -- no real premium data for today.")
            continue
        today_1m = _candles_to_bars(today_candles)
        today_1m = [b for b in today_1m if b.timestamp.time() >= _SESSION_OPEN]

        all_1m = hist_1m + today_1m
        bars_htf = _to_bars(_resample(_bars_to_df(all_1m), htf_minutes))
        zones = _detect_zones(bars_htf, direction)
        print(f"\n  {direction}{strike}: {len(hist_1m)} historical + {len(today_1m)} today 1m bars -> "
              f"{len(zones)} HTF({htf_minutes}m) zones")

        summary_rows = []   # (tf, exit_mode, result) -- for the comparison table below
        for tf in _SR_TF_SWEEP:
            for mode in _EXIT_MODES:
                result = _run_sr_variant(zones, today_1m, tf, lot_size, exit_mode=mode)
                summary_rows.append((tf, mode, result))
                if mode != "raw":
                    continue   # only the validated baseline gets the full per-candle trace below
                void_note = f" ({len(result['voided'])} zone(s) voided -- moved below before entry)" \
                    if result["voided"] else ""
                if result.get("no_entry"):
                    print(f"    S&R TF={tf}m [raw]: NO ENTRY today{void_note}")
                    continue
                sign = "+" if result["pnl"] >= 0 else ""
                print(f"    S&R TF={tf}m [raw]: ENTRY @ {result['entry_ts']} premium={result['entry_premium']:.2f} -> "
                      f"exit={result['exit_reason']} @ {result['exit_price']:.2f} ({result['exit_ts']}) "
                      f"P&L/lot={sign}Rs{result['pnl']:.2f}{void_note}")
                print(f"      {'Bucket':<22}{'High':>9}{'Low':>9}{'Close':>9}{'Phase':>26}{'S1(SL)':>9}{'R1':>9}")
                for t in result["trace"]:
                    marker = ""
                    if "breach_ts" in t:
                        marker = f" <-- BREAKOUT CONFIRMED (real fill @ {t['breach_price']:.2f} " \
                                 f"on the {t['breach_ts']} 1m bar, not this bucket's close)"
                    elif t["ts"] == result["exit_ts"]:
                        marker = " <-- EXIT"
                    print(f"      {str(t['ts']):<22}{t['high']:>9.2f}{t['low']:>9.2f}{t['close']:>9.2f}"
                          f"{t['phase_before']+'->'+t['phase_after']:>26}{t['s1_low']:>9.2f}{t['r1_high']:>9.2f}{marker}")

        print(f"\n    {'--- EXIT-VARIANT COMPARISON (same entries, different SL rule) ---':<80}")
        print(f"    {'TF':<5}{'Mode':<16}{'Entry':>10}{'Exit':>10}{'Exit reason':<28}{'Hold':>8}{'P&L/lot':>12}")
        log_records = []
        side = f"{direction}{strike}"
        for tf, mode, result in summary_rows:
            log_records.append({
                "date": today.isoformat(), "underlying": underlying, "side": side,
                "tf_minutes": tf, "exit_mode": mode,
                "pnl": result.get("pnl"), "entry_ts": str(result.get("entry_ts", "")),
                "exit_reason": result.get("exit_reason"), "no_entry": bool(result.get("no_entry")),
            })
            if result.get("no_entry"):
                print(f"    {tf}m{'':<3}{mode:<16}{'NO ENTRY':>10}")
                continue
            hold_min = int((result["exit_ts"] - result["entry_ts"]).total_seconds() // 60)
            sign = "+" if result["pnl"] >= 0 else ""
            print(f"    {tf}m{'':<3}{mode:<16}{result['entry_premium']:>10.2f}{result['exit_price']:>10.2f}"
                  f"{result['exit_reason']:<28}{hold_min:>6}m{sign}Rs{result['pnl']:>9.2f}")
        _append_variant_log(log_records)


async def main() -> int:
    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    token = (creds or {}).get("access_token", "")
    if not token:
        print("FATAL: no Upstox access_token in data/clients.db.")
        return 1
    cfg = GlobalConfig()
    try:
        cfg.exchange.apply_db_overrides(db)
    except Exception:
        pass

    for underlying in ("NIFTY", "SENSEX"):
        await check_underlying(underlying, token, cfg)
    _print_variant_track_record()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
