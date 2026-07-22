"""backtest/v4_cascade/run_backtest.py -- replays NIFTY spot 5m/75m bars
through the real, unmodified V4CascadeEngine. CE bear-scans the raw spot
series (Gate 2 looks for a bear-trap Demand Block); PE bull-scans the SAME
series (zone_state.IndexGatedPremiumScanner's 2026-07-22 bear=False addition)
-- since there's no real option chain here, PE needs a genuinely different
pattern shape on the same feed, not the mirror-image trick a real inverse-
correlated PE premium chart gives you for free.

Captures every OPEN/CLOSE CascadeEvent into a flat per-leg trade table, and
replicates book.py's EOD force-square-off + daily Gate-2/3 reset (both are
book.py-level concerns, not part of the pure engine, since this script never
runs book.py itself)."""
from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

from strategies.v4_cascade.book import _Bar, _bucket_key, _to_5m_bars
from strategies.v4_cascade.config import V4CascadeConfig
from strategies.v4_cascade.dataclasses import CascadeEventType
from strategies.v4_cascade.engine import V4CascadeEngine
from strategies.v4_cascade.rolling_base import resample_bars

SESSION_OPEN: Tuple[int, int] = (9, 15)
EOD_HOUR_MIN: Tuple[int, int] = (15, 15)
GATE23_HOUR_MIN: Tuple[int, int] = (15, 30)


def build_5m_bars(rows_1m: List[dict]) -> List["_Bar"]:
    """Index/spot data has no meaningful traded volume -- never filter on it
    (unlike option-premium bars, which _to_5m_bars normally filters)."""
    return _to_5m_bars(rows_1m, filter_zero_volume=False)


def run_backtest(cfg: V4CascadeConfig, bars_5m: List["_Bar"]) -> List[dict]:
    """Returns a flat list of closed-leg dicts (see _finalize_leg for the
    exact keys), oldest-first."""
    engine = V4CascadeEngine(cfg, pe_scans_bull=False, session_open=SESSION_OPEN)
    engine._scanners["PE"]._bear = False

    bars_75m_by_key = {
        _bucket_key(b.timestamp, 75, SESSION_OPEN): b
        for b in resample_bars(bars_5m, 75, SESSION_OPEN)
    }

    last_audit: Dict[str, Optional[dict]] = {"CE": None, "PE": None}
    legs: List[dict] = []

    def handle(events) -> None:
        for ev in events:
            if ev.event_type in (CascadeEventType.OPEN_LONG_CE, CascadeEventType.OPEN_LONG_PE):
                last_audit[ev.side] = ev.audit
            elif ev.event_type in (CascadeEventType.CLOSE_LONG_CE, CascadeEventType.CLOSE_LONG_PE):
                pos = engine.position
                leg = None
                if pos is not None and pos.side == ev.side:
                    leg = pos.t1 if ev.tranche == "T1" else (pos.t2 if ev.tranche == "T2" else None)
                _finalize_leg(legs, ev.side, ev.tranche, leg, ev.reason, ev.timestamp,
                              last_audit.get(ev.side) or {})

    for idx, bar in enumerate(bars_5m):
        handle(engine.update(ce_bar=bar, pe_bar=bar))

        cur_key = _bucket_key(bar.timestamp, 75, SESSION_OPEN)
        bucket_closing = (idx + 1 < len(bars_5m)
                           and _bucket_key(bars_5m[idx + 1].timestamp, 75, SESSION_OPEN) != cur_key)
        if bucket_closing:
            src = bars_75m_by_key.get(cur_key)
            if src is not None:
                b75 = _Bar(src.timestamp, src.close, src.high, src.low, src.close, tf=75)
                handle(engine.update(spot_bar=b75))

        if (bar.timestamp.hour, bar.timestamp.minute) == EOD_HOUR_MIN:
            pos = engine.position
            if pos is not None and pos.is_open:
                for tranche, leg in (("T1", pos.t1), ("T2", pos.t2)):
                    if leg is None or leg.status != "open":
                        continue
                    leg.status = "closed"
                    leg.close_price = bar.close  # real bar close -- no async fill reconciliation here
                    leg.close_reason = "eod_force_close"
                    leg.close_time = bar.timestamp
                    _finalize_leg(legs, pos.side, tranche, leg, "eod_force_close", bar.timestamp,
                                  last_audit.get(pos.side) or {})
                pos.status = "closed"
                pos.close_time = bar.timestamp

        if (bar.timestamp.hour, bar.timestamp.minute) == GATE23_HOUR_MIN:
            for side in ("CE", "PE"):
                engine._scanners[side].setups.clear()

    return legs


def _finalize_leg(legs: List[dict], side: str, tranche: str, leg, reason: str,
                   close_ts: Optional[datetime], audit: dict) -> None:
    """One row per closed T1/T2 leg. PE is a SHORT (bull-trap-confirmed ->
    bearish -> short the spot series), so its P&L direction is inverted vs
    CE's long. index_kind in the audit tells us which; falls back to side
    (PE == short) if audit is missing (e.g. an EOD close with no OPEN audit
    captured this run -- should not happen but never crash the backtest)."""
    entry_price = getattr(leg, "entry_price", None)
    close_price = getattr(leg, "close_price", None)
    qty = getattr(leg, "qty", None)
    is_short = audit.get("index_kind") == "bull_trap_confirmed" if audit else side == "PE"
    pnl_points = None
    if entry_price is not None and close_price is not None:
        pnl_points = (entry_price - close_price) if is_short else (close_price - entry_price)
    legs.append({
        "side": side,
        "tranche": tranche,
        "is_short": is_short,
        "entry_ts": getattr(leg, "entry_time", None),
        "entry_price": entry_price,
        "sl_price": getattr(leg, "sl_price", None),
        "target_price": getattr(leg, "target_price", None),
        "close_ts": close_ts,
        "close_price": close_price,
        "close_reason": reason,
        "qty": qty,
        "pnl_points": pnl_points,
        "ref_ts": audit.get("demand_block_ref_ts"),
        "lock_ts": audit.get("demand_block_lock_ts"),
        "index_kind": audit.get("index_kind"),
        "index_window_anchor_ts": audit.get("index_window_anchor_ts"),
    })
