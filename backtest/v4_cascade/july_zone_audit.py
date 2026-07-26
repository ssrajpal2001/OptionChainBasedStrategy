"""backtest/v4_cascade/july_zone_audit.py — full 3-gate timestamp audit and
near-miss diagnostic for the July 2026 option-premium replay.

Answers two questions:
  1. EXECUTED TRADES: exact Gate-1/2/3a/3b timestamps for every filled entry
     (read directly from CascadeEvent.audit — no ambiguous cross-reference).
  2. NEAR-MISS TABLE: zones where Gate-3a (trigger: close>prev.high) armed
     but Gate-3b (fill: low<=zone_low+5) NEVER fired:
       • distance from closest 5m bar.low to the fill level after arming
       • whether price reached sl_level after the zone was removed
       • whether an "entry_line - depth/3" (3-gate-style) trigger would have
         fired during the armed window

Usage:
    UPSTOX_TOKEN=<token> python backtest/v4_cascade/july_zone_audit.py \
        --start 2026-07-01 --end 2026-07-24
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.historical_candles import fetch_upstox_intraday_1m, fetch_upstox_range_1m
from data_layer.instrument_registry import REGISTRY
from strategies.v4_cascade.book import _Bar, _bucket_end, _merge_rows, _to_5m_bars
from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.pool_engine import PoolCascadeEngine
from strategies.v4_cascade.rolling_base import resample_bars

_TRACKING_STRIKE_STEP = 100.0
_TRACKING_OFFSETS = [100, 200, 300]
_SESSION_OPEN = (9, 15)
_ENTRY_OFFSET = 5.0
_SL_BUFFER = 20.0


# ── data helpers ─────────────────────────────────────────────────────────────

def _fetch_spot_1m(token: str, start: date, end: date) -> List[dict]:
    key = REGISTRY.get_upstox_index_key("NIFTY")
    range_rows = asyncio.run(fetch_upstox_range_1m(key, token, start, end - timedelta(days=1)))
    today_rows = asyncio.run(fetch_upstox_intraday_1m(key, token)) if end >= date.today() else []
    return _merge_rows(range_rows, today_rows)


def _daily_session_opens(spot_rows: List[dict]) -> Dict[date, float]:
    by_day: Dict[date, list] = {}
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


# ── near-miss slot tracker ───────────────────────────────────────────────────

class _NearMissRecord:
    """Tracks a zone that armed (pending_entry=True) but never filled."""
    __slots__ = (
        "side", "strike",
        "zone_low", "zone_high", "entry_line", "sweep_low", "sl_level",
        "gate1_ts", "gate2_ts", "gate3a_ts",
        "min_low_armed",   # closest bar.low to limit price while armed
        "max_high_armed",  # highest bar.high while armed
        "armed_bar_count",
        "removal_ts", "removal_reason",
        "post_reached_sl", "post_bars_to_sl",
    )

    def __init__(self, slot, gate3a_ts: datetime) -> None:
        self.side = ""
        self.strike = slot.strike
        self.zone_low = slot.zone_low
        self.zone_high = slot.zone_high
        self.entry_line = slot.zone.entry_line
        self.sweep_low = slot.zone.sweep_low
        self.sl_level = slot.zone.sl_level
        self.gate1_ts = slot.zone.lock_ts
        self.gate2_ts = slot.reentry_ts
        self.gate3a_ts = gate3a_ts
        self.min_low_armed: float = float("inf")
        self.max_high_armed: float = 0.0
        self.armed_bar_count = 0
        self.removal_ts: Optional[datetime] = None
        self.removal_reason: Optional[str] = None
        self.post_reached_sl: Optional[bool] = None
        self.post_bars_to_sl: Optional[int] = None


# ── main run ─────────────────────────────────────────────────────────────────

def run(start: date, end: date) -> None:
    token = os.environ["UPSTOX_TOKEN"]
    REGISTRY.load_sync("NIFTY", token)
    expiry = REGISTRY.get_active_expiry("NIFTY")
    print(f"Expiry: {expiry}  Offsets: {_TRACKING_OFFSETS}  "
          f"entry_offset={_ENTRY_OFFSET}  sl_buffer={_SL_BUFFER}")

    spot_rows = _fetch_spot_1m(token, start, end)
    opens = _daily_session_opens(spot_rows)
    trading_days = sorted(d for d in opens if start <= d <= end)
    print(f"\n{len(trading_days)} trading days: {trading_days[0]} .. {trading_days[-1]}")

    day_ce: Dict[date, List[int]] = {}
    day_pe: Dict[date, List[int]] = {}
    for d in trading_days:
        ce_s, pe_s = _resolve_multi_strikes(opens[d])
        day_ce[d] = ce_s
        day_pe[d] = pe_s

    all_ce = sorted({s for ce in day_ce.values() for s in ce})
    all_pe = sorted({s for pe in day_pe.values() for s in pe})

    fetched: Dict[Tuple[str, int], List[_Bar]] = {}
    print("\nFetching option bars...")
    for strike in all_ce:
        key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, "CE")
        rows = _fetch_option_1m(token, key, start, end)
        fetched[("CE", strike)] = _to_5m_bars(rows, filter_zero_volume=True)
        print(f"  CE {strike}: {len(fetched[('CE', strike)])} bars")
    for strike in all_pe:
        key = REGISTRY.get_upstox_key("NIFTY", expiry, strike, "PE")
        rows = _fetch_option_1m(token, key, start, end)
        fetched[("PE", strike)] = _to_5m_bars(rows, filter_zero_volume=True)
        print(f"  PE {strike}: {len(fetched[('PE', strike)])} bars")

    eng = PoolCascadeEngine(
        V4CascadeConfig(underlying="NIFTY", lot_multiplier=2, lot_size=75),
        entry_offset=_ENTRY_OFFSET, session_open=_SESSION_OPEN, sl_buffer=_SL_BUFFER,
    )

    # ── collectors ──────────────────────────────────────────────────────────
    executed: List[dict] = []     # from ev.audit directly — no reconstruction
    close_evs: List[dict] = []

    # near_miss keyed by (side, int(strike), gate1_lock_ts_iso)
    # so that two different zones on the same strike don't collide.
    near_miss: Dict[Tuple[str, int, str], _NearMissRecord] = {}
    # Track which near_miss keys were "fired" (so we skip them in Table 2).
    fired_keys: set = set()

    bars_5m: Dict[Tuple[str, int], List[_Bar]] = {}

    def _nm_key(slot) -> Tuple[str, int, str]:
        lock_ts = slot.zone.lock_ts
        return (
            "",  # side filled in later
            int(slot.strike),
            lock_ts.isoformat() if lock_ts else "nolock",
        )

    def _slot_ref_ts(slot) -> str:
        return slot.zone.reference_low_ts.isoformat() if slot.zone.reference_low_ts else ""

    print("\nReplaying through pool engine...")
    for d in trading_days:
        active_pairs = (
            [("CE", s) for s in day_ce[d]] +
            [("PE", s) for s in day_pe[d]]
        )
        for side, strike in active_pairs:
            sk = (side, strike)
            if sk not in bars_5m:
                bars_5m[sk] = []
            day_bars = [b for b in fetched.get(sk, []) if b.timestamp.date() == d]

            for bar in day_bars:
                bars_5m[sk].append(bar)

                # ── Snapshot BEFORE on_5m_bar so we see slots that might fire ──
                pool_before = {_slot_ref_ts(s): s for s in eng._pool.get(sk, [])}

                events = eng.on_5m_bar(side, strike, bar)

                pool_after = {_slot_ref_ts(s): s for s in eng._pool.get(sk, [])}

                # ── Collect close events ──
                for ev in events:
                    if ev.event_type.value.startswith("close_long"):
                        close_evs.append({
                            "ts": bar.timestamp, "side": side, "strike": strike,
                            "tranche": ev.tranche, "reason": ev.reason, "price": ev.price_hint,
                        })

                # ── Collect executed trade from ev.audit (no reconstruction needed) ──
                for ev in events:
                    if ev.event_type.value.startswith("open_long"):
                        a = ev.audit or {}
                        executed.append({
                            "ts": bar.timestamp, "side": side, "strike": strike,
                            "fill": ev.price_hint,
                            "sl": ev.sl_price,
                            "t1_target": ev.target_price,
                            "t2_target": a.get("t2_target"),  # = htf.sl_level (full target)
                            "gate1_ts": a.get("htf_lock_ts"),
                            "gate2_ts": a.get("reentry_ts"),
                            "gate3a_ts": a.get("trigger_ts"),
                            "gate3b_ts": bar.timestamp.isoformat(),
                            "zone_low": a.get("zone_low"),
                            "zone_high": a.get("zone_high"),
                        })
                        # Mark this lock_ts as "fired" in near_miss tracking.
                        lock_iso = a.get("htf_lock_ts") or ""
                        fired_keys.add((side, int(strike), lock_iso))

                # ── Near-miss tracking: detect newly armed slots ──
                for ref_ts, slot in pool_before.items():
                    if not slot.pending_entry:
                        continue
                    # Was it already armed before this bar? Check if it was in pre-armed set.
                    # A slot just became armed if pool_before shows pending_entry=True but
                    # on the PREVIOUS bar it was False (we can't tell easily without storing).
                    # Instead: create the record on first sighting of pending_entry=True.
                    lock_iso = slot.zone.lock_ts.isoformat() if slot.zone.lock_ts else "nolock"
                    km = (side, int(strike), lock_iso)
                    if km not in near_miss and km not in fired_keys:
                        rec = _NearMissRecord(slot, slot.trigger_ts or bar.timestamp)
                        rec.side = side
                        near_miss[km] = rec

                # ── Update armed slot stats (for still-present armed slots) ──
                for ref_ts, slot in pool_after.items():
                    if not slot.pending_entry:
                        continue
                    lock_iso = slot.zone.lock_ts.isoformat() if slot.zone.lock_ts else "nolock"
                    km = (side, int(strike), lock_iso)
                    rec = near_miss.get(km)
                    if rec:
                        rec.min_low_armed = min(rec.min_low_armed, bar.low)
                        rec.max_high_armed = max(rec.max_high_armed, bar.high)
                        rec.armed_bar_count += 1

                # ── Near-miss removal detection ──
                removed_refs = set(pool_before) - set(pool_after)
                for ref_ts in removed_refs:
                    slot_was = pool_before[ref_ts]
                    lock_iso = slot_was.zone.lock_ts.isoformat() if slot_was.zone.lock_ts else "nolock"
                    km = (side, int(strike), lock_iso)
                    rec = near_miss.get(km)
                    if rec and rec.removal_ts is None and km not in fired_keys:
                        rec.removal_ts = bar.timestamp
                        if bar.close < rec.zone_low:
                            rec.removal_reason = "broken"
                        else:
                            rec.removal_reason = "aged"

                # ── 15m and 75m bar updates ──
                if _bucket_end(bar.timestamp, 15, _SESSION_OPEN):
                    r15 = resample_bars(bars_5m[sk], 15, _SESSION_OPEN)
                    if r15:
                        last15 = r15[-1]
                        eng.on_15m_bar(
                            side, strike,
                            _Bar(last15.timestamp, last15.close, last15.high,
                                 last15.low, last15.close, tf=15),
                        )

                if _bucket_end(bar.timestamp, 75, _SESSION_OPEN):
                    r75 = resample_bars(bars_5m[sk], 75, _SESSION_OPEN)
                    if r75:
                        last75 = r75[-1]
                        eng.on_75m_bar(
                            side, strike,
                            _Bar(last75.timestamp, last75.close, last75.high,
                                 last75.low, last75.close, tf=75),
                        )

    # ── Post-removal: did sl_level get reached after near-miss zone died? ──
    for km, rec in near_miss.items():
        if km in fired_keys:
            continue
        sk = (rec.side, int(rec.strike))
        remaining = [
            b for b in bars_5m.get(sk, [])
            if rec.removal_ts and b.timestamp > rec.removal_ts
        ]
        rec.post_reached_sl = False
        for i, b in enumerate(remaining):
            if b.high >= rec.sl_level:
                rec.post_reached_sl = True
                rec.post_bars_to_sl = i + 1
                break

    # ═══════════════════════════════════════════════════════════════════════
    # TABLE 1 — EXECUTED TRADES: gate timestamp chain
    # ═══════════════════════════════════════════════════════════════════════
    def _fmt(ts_str: Optional[str]) -> str:
        if not ts_str:
            return "---"
        try:
            dt = datetime.fromisoformat(ts_str)
            return dt.strftime("%m-%d %H:%M")
        except Exception:
            return ts_str[:16]

    print(f"\n{'='*130}")
    print(f"TABLE 1 — EXECUTED TRADES: 4-gate timestamp chain (read directly from CascadeEvent.audit)")
    print(f"{'='*130}")
    hdr1 = (
        f"{'Side':<5} {'Strike':<7} {'Zone [low,high]':<18} "
        f"{'Gate1 HTF-lock':<18} {'Gate2 reentry':<18} "
        f"{'Gate3a trigger':<18} {'Gate3b fill':<16} "
        f"{'Fill':>7} {'T1-tgt':>7} {'T2-tgt(HTF)':>11}  depth  3gate-entry  3gate>T2?"
    )
    print(hdr1)
    print("-" * len(hdr1))
    for e in sorted(executed, key=lambda x: x["ts"]):
        zl = e.get("zone_low") or 0.0
        zh = e.get("zone_high") or 0.0
        depth = zh - zl
        g3_limit = zh - depth / 3.0 if depth > 0 else zh
        t2 = e.get("t2_target") or 0.0
        # 3-gate profitable = 3-gate entry < T2 target (would profit on full move)
        g3_wins = "YES" if t2 > 0 and g3_limit < t2 else "no"
        zone_str = f"[{zl:.0f},{zh:.0f}]"
        print(
            f"{e['side']:<5} {e['strike']:<7} {zone_str:<18} "
            f"{_fmt(e['gate1_ts']):<18} {_fmt(e['gate2_ts']):<18} "
            f"{_fmt(e['gate3a_ts']):<18} {_fmt(e['gate3b_ts']):<16} "
            f"{e['fill'] or 0:>7.2f} {e.get('t1_target') or 0:>7.2f} {t2:>11.2f}  "
            f"{depth:>5.1f}  {g3_limit:>11.2f}  {g3_wins}"
        )

    # ═══════════════════════════════════════════════════════════════════════
    # TABLE 2 — NEAR-MISS ZONES
    # ═══════════════════════════════════════════════════════════════════════
    near_misses = [rec for km, rec in near_miss.items() if km not in fired_keys]
    near_misses.sort(key=lambda r: r.gate3a_ts or datetime.max.replace(tzinfo=IST))

    def _3gate(rec: _NearMissRecord) -> float:
        depth = rec.zone_high - rec.zone_low
        return rec.zone_high - depth / 3.0 if depth > 0 else rec.zone_high

    print(f"\n{'='*160}")
    print(f"TABLE 2 — NEAR-MISS ZONES: Gate3a ARMED but Gate3b fill (zone_low+{_ENTRY_OFFSET}) NEVER reached")
    print(f"  '3gate?' = would entry_line-depth/3 limit have been reached while armed?")
    print(f"{'='*160}")
    hdr2 = (
        f"{'Sd':<3} {'Str':<6} {'Zone[lo,hi]':<16} {'SL-tgt':>7}  "
        f"{'Gate2 reentry':<18} {'Gate3a trigger':<18} {'Removed':<18} {'Removal':<8}  "
        f"{'MinLow@armed':>13} {'FillLvl':>8} {'Miss':>5}  "
        f"{'3gateLvl':>9} {'3gate?':>7}  "
        f"{'MaxHi@armed':>12} {'SL@armed':>9} {'SL(post)':>9}"
    )
    print(hdr2)
    print("-" * len(hdr2))

    for rec in near_misses:
        fill_lvl = rec.zone_low + _ENTRY_OFFSET
        g3_limit = _3gate(rec)
        min_low = rec.min_low_armed
        min_low_str = f"{min_low:.1f}" if min_low < float("inf") else "---"
        miss = min_low - fill_lvl if min_low < float("inf") else float("nan")
        miss_str = f"+{miss:.1f}" if miss == miss else "---"
        g3_reached = min_low <= g3_limit if min_low < float("inf") else False
        g3_str = f"YES({g3_limit:.0f})" if g3_reached else f"no({g3_limit:.0f})"
        max_hi_str = f"{rec.max_high_armed:.1f}" if rec.max_high_armed > 0 else "---"
        # sl reached WHILE still armed (before removal):
        sl_while_armed = rec.max_high_armed >= rec.sl_level if rec.max_high_armed > 0 else False
        sl_armed_str = "YES" if sl_while_armed else "no"
        sl_post_str = ("YES" if rec.post_reached_sl
                       else ("no" if rec.post_reached_sl is False else "---"))

        def _dfmt(dt: Optional[datetime]) -> str:
            return dt.strftime("%m-%d %H:%M") if dt else "---"

        zone_str = f"[{rec.zone_low:.0f},{rec.zone_high:.0f}]"
        print(
            f"{rec.side:<3} {rec.strike:<6} {zone_str:<16} {rec.sl_level:>7.1f}  "
            f"{_dfmt(rec.gate2_ts):<18} {_dfmt(rec.gate3a_ts):<18} "
            f"{_dfmt(rec.removal_ts):<18} {rec.removal_reason or '?':<8}  "
            f"{min_low_str:>13} {fill_lvl:>8.1f} {miss_str:>5}  "
            f"{g3_str:>9} {('YES' if g3_reached else 'no'):>7}  "
            f"{max_hi_str:>12} {sl_armed_str:>9} {sl_post_str:>9}"
        )

    # ═══════════════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════════════
    n_exec = len(executed)
    n_nm = len(near_misses)
    n_nm_sl_reached = sum(1 for r in near_misses if r.post_reached_sl)
    n_nm_3gate = sum(1 for r in near_misses
                     if r.min_low_armed < float("inf") and r.min_low_armed <= _3gate(r))
    n_nm_broken = sum(1 for r in near_misses if r.removal_reason == "broken")
    n_nm_aged = sum(1 for r in near_misses if r.removal_reason == "aged")
    n_nm_active = sum(1 for r in near_misses if r.removal_ts is None)

    n_nm_sl_while_armed = sum(
        1 for r in near_misses if r.max_high_armed > 0 and r.max_high_armed >= r.sl_level
    )
    n_nm_3gate_and_sl = sum(
        1 for r in near_misses
        if r.min_low_armed < float("inf") and r.min_low_armed <= _3gate(r)
        and (r.max_high_armed >= r.sl_level or r.post_reached_sl)
    )
    would_win_3gate = sum(
        1 for r in near_misses
        if r.min_low_armed < float("inf") and r.min_low_armed <= _3gate(r)
        and (r.max_high_armed >= r.sl_level or r.post_reached_sl)
    )
    would_lose_3gate = n_nm_3gate - would_win_3gate

    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"  Executed trades (open events):               {n_exec}")
    print(f"  Near-miss zones (armed, no fill):            {n_nm}")
    print(f"    - sl reached WHILE armed (pre-removal):    {n_nm_sl_while_armed}  <- clear missed winners")
    print(f"    - sl reached post-removal:                 {n_nm_sl_reached}")
    print(f"    - 3-gate limit reached while armed:        {n_nm_3gate}")
    print(f"    - 3-gate limit hit AND sl_level reached:   {n_nm_3gate_and_sl}  <- additional winners possible")
    print(f"    - removal: broken={n_nm_broken}  aged={n_nm_aged}  still-active={n_nm_active}")
    print(f"")
    print(f"  === ENTRY TRIGGER COMPARISON ===")
    print(f"  Current:  low <= zone_low + {_ENTRY_OFFSET}  (near sweep_low)")
    print(f"  3-gate:   low <= entry_line - depth/3  (1/3 from zone TOP)")
    print(f"")
    print(f"  Switching to 3-gate trigger: +{n_nm_3gate} additional trades")
    print(f"    of those: {would_win_3gate} with sl reached = likely winners, "
          f"{would_lose_3gate} sl not reached = uncertain")
    print(f"")
    print(f"  NOTE: For large zones (depth > 50), 3-gate entry may be ABOVE T1-target (LTF sl).")
    print(f"  Always check T2-target (HTF sl_level) in Table 1 for true profit potential.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-07-01")
    ap.add_argument("--end", default="2026-07-24")
    args = ap.parse_args()
    run(date.fromisoformat(args.start), date.fromisoformat(args.end))
