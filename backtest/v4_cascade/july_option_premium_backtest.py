"""backtest/v4_cascade/july_option_premium_backtest.py -- real option
PREMIUM backtest for the CURRENT active NIFTY weekly expiry, replaying
PoolCascadeEngine day-by-day exactly as book.py would run it live: each
day, resolve that day's session-open ATM -> CE/PE tracking strikes
(ATM-200/ATM+200, matching strategies/v4_cascade/book.py's _resolve_symbols
exactly, 100-pt grid) -> fetch that day's CE/PE 1m premium -> feed into ONE
persistent PoolCascadeEngine per side, resetting a side's pool (mirroring
book.py's tracking-strike recenter) whenever the resolved strike changes
from the previous trading day.

DATA CONSTRAINT: Upstox's InstrumentRegistry only resolves instrument_keys
for CURRENTLY ACTIVE (unexpired) contracts, so this can only backtest
whatever real trading history exists for the CURRENT weekly expiry, not
arbitrary past weeks whose contracts have already expired. The actual
available window is printed at the top of the run, not assumed.

Usage:
    # Parameter sweep (default) — runs all 6 min_rr values, prints matrix:
    UPSTOX_TOKEN=<token> python backtest/v4_cascade/july_option_premium_backtest.py

    # Single detailed run at one min_rr value:
    UPSTOX_TOKEN=<token> python backtest/v4_cascade/july_option_premium_backtest.py --single --min-rr 1.0
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.historical_candles import fetch_upstox_intraday_1m, fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
from strategies.v4_cascade.book import _Bar, _bucket_end, _merge_rows, _to_5m_bars
from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.pool_engine import PoolCascadeEngine
from strategies.v4_cascade.rolling_base import resample_bars

_TRACKING_STRIKE_STEP = 100.0
_TRACKING_OFFSETS = [100, 200, 300]   # scan ATM-100/200/300 CE and ATM+100/200/300 PE simultaneously
_SESSION_OPEN = (9, 15)
_ENTRY_OFFSET = 5.0       # entry trigger fallback (degenerate zero-depth zones only)
_SL_BUFFER = 20.0         # SL: zone_low - sl_buffer  (structural; no mid-zone cap)
_MIN_RR = 0.0             # R:R gate disabled; width filtered at zone-discovery instead
_MAX_ZONE_DEPTH = 120.0   # zones deeper than this are discarded at HTF discovery time
_ENTRY_RSI_MAX = float("inf")  # 5m RSI gate disabled (38.0 tested, backfired — see run log)
_RSI_PERIOD = 14

# Specific trade dates tracked in the sweep matrix for regression visibility.
_TRACK_DATE_CE = {
    "Jul-14 CE": date(2026, 7, 14),
    "Jul-17 CE": date(2026, 7, 17),
}


def _compute_rsi(bars: List[_Bar], period: int = 14) -> List[float]:
    """Wilder RSI on close prices of the given bar list. Returns per-bar RSI
    values (NaN for the first `period` bars where the series isn't warm yet)."""
    closes = np.array([b.close for b in bars], dtype=float)
    if len(closes) < period + 1:
        return [float("nan")] * len(closes)
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    rsi_vals = [float("nan")] * (period + 1)
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi_vals.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi_vals.append(100.0 - 100.0 / (1.0 + rs))
    return rsi_vals


def _fetch_spot_1m(token: str, start: date, end: date) -> List[dict]:
    key = REGISTRY.get_upstox_index_key("NIFTY")
    range_rows = asyncio.run(fetch_upstox_range_1m(key, token, start, end - timedelta(days=1)))
    today_rows = asyncio.run(fetch_upstox_intraday_1m(key, token)) if end >= date.today() else []
    return _merge_rows(range_rows, today_rows)


def _daily_session_opens(spot_rows: List[dict]) -> Dict[date, float]:
    """First real tick at/after 09:15 IST for each trading day present in
    the fetched spot data."""
    by_day: Dict[date, List] = {}
    for r in spot_rows:
        ts = datetime.fromisoformat(r["ts"])
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=IST)
        by_day.setdefault(ts.date(), []).append((ts, r["open"]))
    opens: Dict[date, float] = {}
    for day, rows in by_day.items():
        rows.sort(key=lambda x: x[0])
        opens[day] = rows[0][1]
    return opens


def _resolve_multi_strikes(atm_open: float) -> Tuple[List[int], List[int]]:
    atm = round(atm_open / _TRACKING_STRIKE_STEP) * _TRACKING_STRIKE_STEP
    ce_strikes = [int(atm - off) for off in _TRACKING_OFFSETS]
    pe_strikes = [int(atm + off) for off in _TRACKING_OFFSETS]
    return ce_strikes, pe_strikes


def _fetch_option_1m(token: str, key: str, start: date, end: date) -> List[dict]:
    if not key:
        return []
    range_rows = asyncio.run(fetch_upstox_range_1m(key, token, start, end - timedelta(days=1)))
    today_rows = asyncio.run(fetch_upstox_intraday_1m(key, token)) if end >= date.today() else []
    return _merge_rows(range_rows, today_rows)


# ── Data bundle (fetched once, reused across all sweep runs) ──────────────────

def _fetch_all_data(token: str, start: date, end: date) -> dict:
    """Fetch all required market data once and return as a dict. Expensive
    network calls happen here only; _run_once() is pure computation on the
    returned bundle."""
    REGISTRY.load_sync("NIFTY", token)
    expiry = REGISTRY.get_active_expiry("NIFTY")
    print(f"Active expiry: {expiry}  (all loaded: {REGISTRY.all_expiries('NIFTY')})")
    print(f"Multi-strike offsets: {_TRACKING_OFFSETS} pts  (CE=ATM-offset, PE=ATM+offset)")

    print(f"\nFetching NIFTY spot {start}..{end} to derive daily session-open ATM...")
    spot_rows = _fetch_spot_1m(token, start, end)
    opens = _daily_session_opens(spot_rows)
    trading_days = sorted(d for d in opens if start <= d <= end)
    print(f"Found {len(trading_days)} trading days: "
          f"{trading_days[0] if trading_days else None} .. {trading_days[-1] if trading_days else None}")

    day_ce_strikes: Dict[date, List[int]] = {}
    day_pe_strikes: Dict[date, List[int]] = {}
    for d in trading_days:
        ce_s, pe_s = _resolve_multi_strikes(opens[d])
        day_ce_strikes[d] = ce_s
        day_pe_strikes[d] = pe_s
        print(f"  {d}  spot_open={opens[d]:.2f}  CE={ce_s}  PE={pe_s}")

    all_ce = sorted({s for ce in day_ce_strikes.values() for s in ce})
    all_pe = sorted({s for pe in day_pe_strikes.values() for s in pe})
    print(f"\nAll unique CE strikes: {all_ce}")
    print(f"All unique PE strikes: {all_pe}")

    fetched_cache: Dict[Tuple[str, int], List[_Bar]] = {}
    print("\nFetching option bars (one API call per unique strike)...")
    for strike in all_ce:
        key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, "CE")
        rows = _fetch_option_1m(token, key, start, end)
        b5 = _to_5m_bars(rows, filter_zero_volume=True)
        fetched_cache[("CE", strike)] = b5
        real_start = b5[0].timestamp.date() if b5 else None
        real_end = b5[-1].timestamp.date() if b5 else None
        print(f"  CE {strike} (key={key or 'UNRESOLVED'}): {len(b5)} 5m bars  {real_start}..{real_end}")
    for strike in all_pe:
        key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, "PE")
        rows = _fetch_option_1m(token, key, start, end)
        b5 = _to_5m_bars(rows, filter_zero_volume=True)
        fetched_cache[("PE", strike)] = b5
        real_start = b5[0].timestamp.date() if b5 else None
        real_end = b5[-1].timestamp.date() if b5 else None
        print(f"  PE {strike} (key={key or 'UNRESOLVED'}): {len(b5)} 5m bars  {real_start}..{real_end}")

    return {
        "expiry": expiry,
        "opens": opens,
        "trading_days": trading_days,
        "day_ce_strikes": day_ce_strikes,
        "day_pe_strikes": day_pe_strikes,
        "fetched_cache": fetched_cache,
    }


# ── Single engine replay ──────────────────────────────────────────────────────

def _run_once(data: dict, min_rr: float, max_sl_distance: float,
              quiet: bool = True) -> dict:
    """Replay bars through a fresh PoolCascadeEngine with the given parameters.
    Returns a stats dict; when quiet=False also prints the full event log and
    P&L summary (same output as the original run() function)."""
    trading_days = data["trading_days"]
    day_ce_strikes = data["day_ce_strikes"]
    day_pe_strikes = data["day_pe_strikes"]
    fetched_cache = data["fetched_cache"]

    eng = PoolCascadeEngine(
        V4CascadeConfig(underlying="NIFTY", lot_multiplier=2, lot_size=75),
        entry_offset=_ENTRY_OFFSET, session_open=_SESSION_OPEN,
        sl_buffer=_SL_BUFFER, min_rr=min_rr, max_zone_depth=_MAX_ZONE_DEPTH,
        entry_rsi_max=_ENTRY_RSI_MAX, rsi_period=_RSI_PERIOD,
    )
    all_events: List[dict] = []
    bars_5m: Dict[Tuple[str, int], List[_Bar]] = {}
    zones: Dict[Tuple[str, int, str], dict] = {}

    def _snapshot_zones(side: str, strike: int, at_ts: datetime) -> None:
        for slot in eng._pool.get((side, strike), []):
            key = (side, strike, slot.zone.reference_low_ts.isoformat())
            if key not in zones:
                zones[key] = {
                    "side": side, "strike": strike,
                    "ref_ts": slot.zone.reference_low_ts.isoformat(),
                    "discovered_at": at_ts.isoformat(),
                    "ref_low": slot.zone.entry_line, "ref_high": slot.zone.sl_level,
                    "sweep_low": slot.zone.sweep_low,
                    "lock_ts": slot.zone.lock_ts.isoformat() if slot.zone.lock_ts else None,
                    "zone_low": round(slot.zone_low, 2), "zone_high": round(slot.zone_high, 2),
                    "reentry_ts": None, "pending_entry": False, "trigger_ts": None,
                    "removed_at": None, "removed_reason": None,
                }
            z = zones[key]
            if slot.reentry_ts and not z["reentry_ts"]:
                z["reentry_ts"] = slot.reentry_ts.isoformat()
            if slot.pending_entry and not z["pending_entry"]:
                z["pending_entry"] = True
                z["trigger_ts"] = slot.trigger_ts.isoformat() if slot.trigger_ts else None

    if not quiet:
        print("\nReplaying option bars through pool engine (NO pool resets on ATM shift)...")

    for d in trading_days:
        active_pairs = (
            [("CE", s) for s in day_ce_strikes[d]] +
            [("PE", s) for s in day_pe_strikes[d]]
        )
        for side, strike in active_pairs:
            sk = (side, strike)
            if sk not in bars_5m:
                bars_5m[sk] = []
            all_bars = fetched_cache.get(sk, [])
            day_bars = [b for b in all_bars if b.timestamp.date() == d]
            for bar in day_bars:
                bars_5m[sk].append(bar)
                prior_refs = {s.zone.reference_low_ts for s in eng._pool.get(sk, [])}
                events = eng.on_5m_bar(side, strike, bar)
                for ev in events:
                    rsi_at_entry = float("nan")
                    if ev.event_type.value.startswith("open_long"):
                        rsi_series = _compute_rsi(bars_5m[sk], _RSI_PERIOD)
                        rsi_at_entry = rsi_series[-1] if rsi_series else float("nan")
                    all_events.append({
                        "ts": bar.timestamp, "side": side, "strike": strike,
                        "event": ev.event_type.value,
                        "tranche": ev.tranche, "reason": ev.reason,
                        "price": ev.price_hint,
                        "sl": ev.sl_price, "target": ev.target_price,
                        "audit": ev.audit, "rsi": rsi_at_entry,
                    })
                after_refs = {s.zone.reference_low_ts for s in eng._pool.get(sk, [])}
                fired_here = any(
                    ev.event_type.value == f"open_long_{side.lower()}" for ev in events
                )
                for ref in prior_refs - after_refs:
                    zk = (side, strike, ref.isoformat())
                    if zk in zones and zones[zk]["removed_at"] is None:
                        zones[zk]["removed_at"] = bar.timestamp.isoformat()
                        zones[zk]["removed_reason"] = (
                            "pool_cleared_on_fire" if fired_here else "broken_or_aged"
                        )
                if _bucket_end(bar.timestamp, 15, _SESSION_OPEN):
                    r15 = resample_bars(bars_5m[sk], 15, _SESSION_OPEN)
                    if r15:
                        last15 = r15[-1]
                        eng.on_15m_bar(side, strike, _Bar(
                            last15.timestamp, last15.close, last15.high,
                            last15.low, last15.close, tf=15))
                if _bucket_end(bar.timestamp, 75, _SESSION_OPEN):
                    r75 = resample_bars(bars_5m[sk], 75, _SESSION_OPEN)
                    if r75:
                        last75 = r75[-1]
                        eng.on_75m_bar(side, strike, _Bar(
                            last75.timestamp, last75.close, last75.high,
                            last75.low, last75.close, tf=75))
                        _snapshot_zones(side, strike, bar.timestamp)

    # ── Build leg-level trade records ─────────────────────────────────────────
    lot_size = 75
    lot_mult = 2
    tranche_qty = (lot_size * lot_mult) // 2

    legs: List[dict] = []
    current_entry: Optional[dict] = None
    open_count = 0
    for e in all_events:
        if e["event"].startswith("open_long"):
            current_entry = e
            open_count = 2
        elif e["event"].startswith("close_long") and current_entry:
            entry_p = current_entry["price"] or 0.0
            exit_p = e["price"] or 0.0
            pts = exit_p - entry_p
            pnl = pts * tranche_qty
            legs.append({
                "date": current_entry["ts"].date(),
                "side": current_entry["side"],
                "strike": current_entry.get("strike", 0),
                "tranche": e["tranche"] or "?",
                "entry": entry_p, "exit": exit_p,
                "reason": e["reason"],
                "pts": round(pts, 2),
                "pnl": round(pnl, 2),
                "rsi": current_entry.get("rsi", float("nan")),
                "entry_ts": current_entry["ts"],
            })
            open_count -= 1
            if open_count <= 0:
                current_entry = None

    # ── Aggregate stats ───────────────────────────────────────────────────────
    gross_win = sum(t["pnl"] for t in legs if t["pnl"] > 0)
    gross_loss = abs(sum(t["pnl"] for t in legs if t["pnl"] < 0))
    net_pnl = sum(t["pnl"] for t in legs)
    wins_leg = sum(1 for t in legs if t["pnl"] > 0)
    losses_leg = sum(1 for t in legs if t["pnl"] < 0)
    n_legs = len(legs)
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")

    # Trade-level (group T1+T2 by their shared entry: date+side+entry_price)
    trade_groups: Dict[tuple, List[dict]] = {}
    for t in legs:
        key = (t["date"], t["side"], round(t["entry"], 2))
        trade_groups.setdefault(key, []).append(t)
    trade_nets = {k: sum(t["pnl"] for t in v) for k, v in trade_groups.items()}
    n_trades = len(trade_groups)
    wins_trade = sum(1 for v in trade_nets.values() if v > 0)
    losses_trade = sum(1 for v in trade_nets.values() if v <= 0)

    # ── Track specific dates for regression visibility ────────────────────────
    def _trade_summary(d: date, side: str) -> str:
        matches = [(k, v) for k, v in trade_nets.items() if k[0] == d and k[1] == side]
        if not matches:
            return "not fired"
        # There may be multiple entries on the same date/side (unlikely but possible)
        parts = []
        for k, net in matches:
            entry_p = k[2]
            result = "WIN" if net > 0 else "LOSS"
            parts.append(f"{result} {net:+,.0f} @{entry_p:.0f}")
        return "  /  ".join(parts)

    tracked = {label: _trade_summary(d, "CE") for label, d in _TRACK_DATE_CE.items()}

    # ── Verbose output (quiet=False only) ─────────────────────────────────────
    if not quiet:
        print(f"\n{'='*100}\nTRADE / EVENT LOG ({len(all_events)} events)\n{'='*100}")
        header = (f"{'ts':<20} {'side':<5} {'event':<16} {'tranche':<4} "
                  f"{'reason':<28} {'price':>8} {'sl':>8} {'target':>8}")
        print(header)
        print("-" * len(header))
        for e in all_events:
            print(
                f"{e['ts'].strftime('%Y-%m-%d %H:%M'):<20} {e['side']:<5} "
                f"{e['event']:<16} {e['tranche'] or '-':<4} {e['reason']:<28} "
                f"{e['price'] or 0:>8.2f} {e['sl'] or 0:>8.2f} {e['target'] or 0:>8.2f}"
            )

        print(f"\n{'='*100}\nOPEN EVENTS WITH RSI (sl_buffer={_SL_BUFFER}pts)\n{'='*100}")
        for e in all_events:
            if not e["event"].startswith("open_long"):
                continue
            a = e["audit"] or {}
            rsi = e.get("rsi", float("nan"))
            rsi_flag = "  <<< RSI OVERBOUGHT" if not (rsi != rsi) and rsi > 65 else ""
            print(
                f"\n{e['side']} {e.get('strike','')} entry at "
                f"{e['ts'].strftime('%Y-%m-%d %H:%M')}  "
                f"price={e['price']:.2f} sl={e['sl']:.2f} target={e['target']:.2f}  "
                f"RSI-14={rsi:.1f}{rsi_flag}"
            )
            print(
                f"  zone:[{a.get('zone_low')}, {a.get('zone_high')}]  "
                f"75m lock:{a.get('htf_lock_ts')}  "
                f"re-entry:{a.get('reentry_ts')}  5m trigger:{a.get('trigger_ts')}"
            )

        rsi_tag = f"  entry_rsi_max={_ENTRY_RSI_MAX}" if _ENTRY_RSI_MAX < float("inf") else ""
        print(f"\n{'='*110}\nP&L SUMMARY  (tranche_qty={tranche_qty}/leg  "
              f"sl_buffer={_SL_BUFFER}pts  max_zone_depth={_MAX_ZONE_DEPTH}pts{rsi_tag})\n{'='*110}")
        print(f"{'Date':<12} {'Side':<5} {'Strike':<8} {'Tr':<4} "
              f"{'Entry':>8} {'Exit':>8} {'Pts':>7} {'P&L(Rs)':>9} {'RSI':>6}  Reason")
        print("-" * 90)
        for t in legs:
            rsi = t.get("rsi", float("nan"))
            rsi_str = f"{rsi:5.1f}" if rsi == rsi else "  n/a"
            flag = " <<OB" if rsi == rsi and rsi > 65 else ""
            print(
                f"{str(t['date']):<12} {t['side']:<5} {t['strike']:<8} {t['tranche']:<4} "
                f"{t['entry']:>8.2f} {t['exit']:>8.2f} {t['pts']:>+7.2f} "
                f"{t['pnl']:>9.0f} {rsi_str}{flag}  {t['reason']}"
            )
        print("-" * 80)
        print(
            f"Trades: {n_trades}  Tranche legs: {n_legs}  "
            f"Wins: {wins_leg}  Losses: {losses_leg}  "
            f"Win%: {100*wins_leg/max(n_legs,1):.0f}%  Net P&L: Rs {net_pnl:+,.0f}"
        )
        print(
            f"Profit Factor: {pf:.2f}  "
            f"Gross Win: Rs {gross_win:,.0f}  Gross Loss: Rs {gross_loss:,.0f}"
        )

    return {
        "min_rr": min_rr,
        "max_sl_distance": max_sl_distance,
        "n_trades": n_trades,
        "n_legs": n_legs,
        "wins_leg": wins_leg,
        "losses_leg": losses_leg,
        "win_pct_leg": 100.0 * wins_leg / max(n_legs, 1),
        "wins_trade": wins_trade,
        "losses_trade": losses_trade,
        "win_pct_trade": 100.0 * wins_trade / max(n_trades, 1),
        "gross_win": gross_win,
        "gross_loss": gross_loss,
        "profit_factor": pf,
        "net_pnl": net_pnl,
        "max_sl_pts": max_sl_distance,
        "max_sl_rs": max_sl_distance * tranche_qty,
        "tracked": tracked,
        "_legs": legs,
        "_zones": zones,
        "_all_events": all_events,
    }


# ── Parameter sweep ───────────────────────────────────────────────────────────

def sweep(data: dict) -> None:
    """Run all min_rr thresholds against the same pre-fetched data and print
    the performance matrix."""
    min_rr_values = [0.0, 0.5, 0.7, 0.8, 1.0, 1.5]
    results = []
    for mrr in min_rr_values:
        print(f"  Replaying: min_rr={mrr:.1f} ...", end="", flush=True)
        r = _run_once(data, min_rr=mrr, max_sl_distance=float("inf"), quiet=True)
        results.append(r)
        print(f"  {r['n_trades']} trades  Net {r['net_pnl']:+,.0f}  PF={r['profit_factor']:.2f}")

    # ── Print matrix ─────────────────────────────────────────────────────────
    track_labels = list(_TRACK_DATE_CE.keys())
    col_w = 28

    sep = "=" * (78 + col_w * len(track_labels))
    print(f"\n{sep}")
    print(f"PARAMETER SWEEP — Adaptive Depth Clamp + Max SL = {_MAX_SL_DISTANCE:.0f} pts "
          f"({_MAX_SL_DISTANCE:.0f} pts x 75 qty = Rs {_MAX_SL_DISTANCE*75:,.0f}/leg max risk)")
    print(sep)

    hdr = (f"{'min_rr':>6} | {'Trades':>6} | {'Win%(leg)':>9} | {'Win%(trd)':>9} | "
           f"{'MaxRisk(Rs)':>11} | {'GrossWin':>9} | {'GrossLoss':>9} | "
           f"{'PF':>5} | {'Net P&L':>10}")
    for lbl in track_labels:
        hdr += f" | {lbl:^{col_w}}"
    print(hdr)
    print("-" * len(hdr))

    for r in results:
        mrr_str = f"{r['min_rr']:.1f}"
        pf_str = f"{r['profit_factor']:.2f}" if r["profit_factor"] != float("inf") else "inf"
        row = (
            f"{mrr_str:>6} | {r['n_trades']:>6} | "
            f"{r['win_pct_leg']:>8.0f}% | {r['win_pct_trade']:>8.0f}% | "
            f"{r['max_sl_rs']:>11,.0f} | "
            f"{r['gross_win']:>9,.0f} | {r['gross_loss']:>9,.0f} | "
            f"{pf_str:>5} | {r['net_pnl']:>+10,.0f}"
        )
        for lbl in track_labels:
            cell = r["tracked"].get(lbl, "n/a")
            row += f" | {cell:^{col_w}}"
        print(row)

    print(sep)
    print("\nNote: Win%(leg) = winning tranche legs / total legs.  "
          "Win%(trd) = winning trades (T1+T2 net) / total trades.")


# ── Legacy single-run entry point (kept for dump-json / detailed inspection) ──

def run(start: date, end: date, dump_json: Optional[str] = None,
        min_rr: float = _MIN_RR) -> None:
    token = os.environ["UPSTOX_TOKEN"]
    data = _fetch_all_data(token, start, end)
    r = _run_once(data, min_rr=min_rr, max_sl_distance=float("inf"), quiet=False)

    if dump_json:
        expiry = data["expiry"]
        all_events = r["_all_events"]
        zones = r["_zones"]
        out = {
            "start": str(start), "end": str(end), "expiry": str(expiry),
            "offsets": _TRACKING_OFFSETS,
            "zones": [dict(v, ref_ts=v["ref_ts"]) for v in zones.values()],
            "events": [
                {"ts": e["ts"].isoformat(), "side": e["side"],
                 "strike": e.get("strike", 0),
                 "event": e["event"], "tranche": e["tranche"],
                 "reason": e["reason"], "price": e["price"],
                 "sl": e["sl"], "target": e["target"]}
                for e in all_events
            ],
        }
        with open(dump_json, "w") as f:
            json.dump(out, f, indent=2)
        print(f"\nWrote full zone/event ledger to {dump_json} "
              f"({len(zones)} zones, {len(all_events)} events)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, default="2026-07-01")
    ap.add_argument("--end", type=str, default="2026-07-23")
    ap.add_argument("--single", action="store_true",
                    help="Detailed single-run output instead of sweep matrix")
    ap.add_argument("--min-rr", type=float, default=_MIN_RR,
                    help="min_rr for --single mode (default %(default)s)")
    ap.add_argument("--dump-json", type=str, default=None,
                    help="(--single only) write zone/event ledger to JSON file")
    args = ap.parse_args()

    start_d = date.fromisoformat(args.start)
    end_d = date.fromisoformat(args.end)
    token = os.environ["UPSTOX_TOKEN"]

    if args.single:
        # Single detailed run (legacy sweep mode)
        data = _fetch_all_data(token, start_d, end_d)
        print(f"\nRunning parameter sweep (max_zone_depth={_MAX_ZONE_DEPTH} pts fixed)...")
        sweep(data)
    else:
        # Default: full detailed single run with configured params
        run(start_d, end_d, dump_json=args.dump_json, min_rr=args.min_rr)
