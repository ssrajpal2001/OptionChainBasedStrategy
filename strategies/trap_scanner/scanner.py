"""
strategies/trap_scanner/scanner.py — Trap detection engine.

Copied verbatim from NiftyTrapScanner (phase2/ltf-entry-engine branch).
Only change: removed 'from config import ...' and replaced with local constants
so this module has zero external dependencies — pure Python, testable standalone.

DO NOT modify the detection logic. Only data connectivity wiring belongs outside.
"""

from __future__ import annotations
from typing import Optional
import pandas as pd

# ── Local constants (replaces NiftyTrapScanner's config.py import) ─────────────
DEFAULT_SL_BUFFER = 2.0
DEFAULT_QTY       = 1
DEFAULT_LOT_SIZE  = 65


# ── HTF (75-min) scan ─────────────────────────────────────────────────────────

def scan_htf(df: pd.DataFrame) -> tuple:
    """
    Scan any-timeframe OHLCV bars for BEARISH TRAPS.

    Returns
    -------
    events : pd.DataFrame   — one row per trapped event (OPEN or CLOSED)
    entries : list[dict]    — raw entry state dicts (includes ACTIVE entries)

    Zone is defined the moment the bearish entry is detected:
        Zone HIGH = Ref Bar LOW          (where bears shorted)
        Zone LOW  = Next Bar LOW         (bar immediately after Ref Bar)
        Zone Trigger = Zone LOW + (Zone HIGH - Zone LOW) / 3   ← your entry
        Target = Ref Bar HIGH            (bears' SL = your target)
    CLOSED when: after TRAPPED, any future bar LOW <= Zone Trigger
    """
    entries = []
    events  = []

    def make(ref_ts, entry, sl, next_low):
        zone_high    = entry
        zone_low     = next_low
        zone_trigger = zone_low + (zone_high - zone_low) / 3
        return {
            "ref_ts"      : ref_ts,
            "entry"       : entry,
            "sl"          : sl,
            "zone_high"   : zone_high,
            "zone_low"    : zone_low,
            "zone_trigger": zone_trigger,
            "status"      : "ACTIVE",
            "trapped_on"  : None,
            "closed_on"   : None,
            "event_idx"   : None,
        }

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]
        ts   = curr["datetime"]

        for e in entries:
            if e["status"] == "CLOSED":
                continue

            # ── TRAP fires ────────────────────────────────────────────────────
            if e["status"] == "ACTIVE" and curr["high"] > e["sl"]:
                e["status"]     = "TRAPPED"
                e["trapped_on"] = ts
                e["event_idx"]  = len(events)
                events.append({
                    "Trap Bar"   : ts,
                    "Ref Bar"    : e["ref_ts"],
                    "Bear Entry" : round(e["entry"],        2),
                    "SL Level"   : round(e["sl"],           2),
                    "Zone High"  : round(e["zone_high"],    2),
                    "Zone Low"   : round(e["zone_low"],     2),
                    "Your Entry" : round(e["zone_trigger"], 2),
                    "Status"     : "OPEN",
                    "Close Bar"  : pd.NaT,
                })

            # ── CLOSE fires when price returns to bear entry — but NOT on same bar as trap.
            if (e["status"] == "TRAPPED"
                    and curr["low"] <= e["entry"]
                    and ts != e["trapped_on"]):
                e["status"]    = "CLOSED"
                e["closed_on"] = ts
                events[e["event_idx"]]["Status"]    = "CLOSED"
                events[e["event_idx"]]["Close Bar"] = ts

        # New bearish setup detected on this bar
        if curr["low"] < prev["low"]:
            entries.append(make(prev["datetime"], prev["low"], prev["high"], curr["low"]))

    df_events = pd.DataFrame(events) if events else pd.DataFrame()
    return df_events, entries


# ── LTF (5-min) scan inside an HTF zone ───────────────────────────────────────

def scan_ltf(df: pd.DataFrame,
             htf_zone_high: float,
             htf_zone_low: float,
             htf_ref_bar: str = "",
             htf_trap_bar: str = "",
             htf_target: float = 0.0) -> tuple:
    """
    Scan 5-min bars for a bearish trap INSIDE the HTF zone band.
    Tags every LTF entry dict with its parent HTF trap for full traceability.
    """
    zone_df = df[
        (df["low"]  <= htf_zone_high) &
        (df["close"] >= htf_zone_low * 0.95)
    ].copy()

    if len(zone_df) < 2:
        return pd.DataFrame(), []

    zone_df = zone_df.reset_index(drop=True)
    df_events, entries = scan_htf(zone_df)

    for e in entries:
        e["htf_ref_bar"]   = htf_ref_bar
        e["htf_trap_bar"]  = htf_trap_bar
        e["htf_zone_high"] = htf_zone_high
        e["htf_zone_low"]  = htf_zone_low
        e["htf_target"]    = htf_target

    return df_events, entries


def scan_htf_spot(df: pd.DataFrame) -> tuple:
    """
    Scan any-timeframe Nifty SPOT bars for BOTH bearish AND bullish traps.

    Bearish trap → spot going UP → CE bias
    Bullish trap → spot going DOWN → PE bias
    """
    bear_entries = []
    bull_entries = []
    events       = []

    def make_bear(ref_ts, entry, sl, next_low):
        return {
            "kind": "BEAR", "direction": "BULLISH",
            "ref_ts": ref_ts, "entry": entry, "sl": sl,
            "zone_high": entry, "zone_low": next_low,
            "status": "ACTIVE", "trapped_on": None, "closed_on": None, "event_idx": None,
        }

    def make_bull(ref_ts, entry, sl, next_high):
        return {
            "kind": "BULL", "direction": "BEARISH",
            "ref_ts": ref_ts, "entry": entry, "sl": sl,
            "zone_high": next_high, "zone_low": entry,
            "status": "ACTIVE", "trapped_on": None, "closed_on": None, "event_idx": None,
        }

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]
        ts   = curr["datetime"]

        for e in bear_entries:
            if e["status"] == "CLOSED":
                continue
            if e["status"] == "ACTIVE" and curr["high"] > e["sl"]:
                e["status"] = "TRAPPED"; e["trapped_on"] = ts; e["event_idx"] = len(events)
                events.append({"Trap Bar": ts, "Ref Bar": e["ref_ts"], "Kind": "BEAR TRAP",
                                "Direction": e["direction"], "Entry": round(e["entry"], 2),
                                "SL Level": round(e["sl"], 2), "Zone High": round(e["zone_high"], 2),
                                "Zone Low": round(e["zone_low"], 2), "Status": "OPEN", "Close Bar": pd.NaT})
            if (e["status"] == "TRAPPED" and curr["low"] <= e["entry"] and ts != e["trapped_on"]):
                e["status"] = "CLOSED"; e["closed_on"] = ts
                events[e["event_idx"]]["Status"] = "CLOSED"; events[e["event_idx"]]["Close Bar"] = ts

        for e in bull_entries:
            if e["status"] == "CLOSED":
                continue
            if e["status"] == "ACTIVE" and curr["low"] < e["sl"]:
                e["status"] = "TRAPPED"; e["trapped_on"] = ts; e["event_idx"] = len(events)
                events.append({"Trap Bar": ts, "Ref Bar": e["ref_ts"], "Kind": "BULL TRAP",
                                "Direction": e["direction"], "Entry": round(e["entry"], 2),
                                "SL Level": round(e["sl"], 2), "Zone High": round(e["zone_high"], 2),
                                "Zone Low": round(e["zone_low"], 2), "Status": "OPEN", "Close Bar": pd.NaT})
            if (e["status"] == "TRAPPED" and curr["high"] >= e["entry"] and ts != e["trapped_on"]):
                e["status"] = "CLOSED"; e["closed_on"] = ts
                events[e["event_idx"]]["Status"] = "CLOSED"; events[e["event_idx"]]["Close Bar"] = ts

        if curr["low"] < prev["low"]:
            bear_entries.append(make_bear(prev["datetime"], prev["low"], prev["high"], curr["low"]))
        if curr["high"] > prev["high"]:
            bull_entries.append(make_bull(prev["datetime"], prev["high"], prev["low"], curr["high"]))

    return pd.DataFrame(events) if events else pd.DataFrame(), bear_entries + bull_entries


def spot_bias(df_events: pd.DataFrame) -> str:
    if df_events.empty:
        return "NEUTRAL"
    open_traps = df_events[df_events["Status"] == "OPEN"]
    if open_traps.empty:
        return "NEUTRAL"
    return open_traps.iloc[-1]["Direction"]


def scan_ltf_bull(df: pd.DataFrame,
                  htf_zone_high: float,
                  htf_zone_low: float,
                  htf_ref_bar: str = "",
                  htf_trap_bar: str = "",
                  htf_target: float = 0.0) -> tuple:
    """Scan 5-min SPOT bars for a BULLISH trap INSIDE an HTF bullish zone."""
    zone_df = df[
        (df["high"] >= htf_zone_low) &
        (df["close"] <= htf_zone_high * 1.05)
    ].copy()

    if len(zone_df) < 2:
        return pd.DataFrame(), []

    zone_df = zone_df.reset_index(drop=True)
    entries = []
    events  = []

    for i in range(1, len(zone_df)):
        prev = zone_df.iloc[i - 1]
        curr = zone_df.iloc[i]
        ts   = curr["datetime"]

        for e in entries:
            if e["status"] == "CLOSED":
                continue
            if e["status"] == "ACTIVE" and curr["low"] < e["sl"]:
                e["status"] = "TRAPPED"; e["trapped_on"] = ts; e["event_idx"] = len(events)
                events.append({"Trap Bar": ts, "Ref Bar": e["ref_ts"],
                                "Bull Entry": round(e["entry"], 2), "SL Level": round(e["sl"], 2),
                                "Zone High": round(e["zone_high"], 2), "Zone Low": round(e["zone_low"], 2),
                                "Your Entry": round(e["zone_trigger"], 2), "Status": "OPEN", "Close Bar": pd.NaT})
            if e["status"] == "TRAPPED" and curr["high"] >= e["entry"]:
                e["status"] = "CLOSED"; e["closed_on"] = ts
                events[e["event_idx"]]["Status"] = "CLOSED"; events[e["event_idx"]]["Close Bar"] = ts

        if curr["high"] > prev["high"]:
            zhi = curr["high"]; zlo = prev["high"]
            entries.append({
                "ref_ts": prev["datetime"], "entry": prev["high"], "sl": prev["low"],
                "zone_high": zhi, "zone_low": zlo,
                "zone_trigger": zhi - (zhi - zlo) / 3,
                "status": "ACTIVE", "trapped_on": None, "closed_on": None, "event_idx": None,
                "htf_ref_bar": htf_ref_bar, "htf_trap_bar": htf_trap_bar,
                "htf_zone_high": htf_zone_high, "htf_zone_low": htf_zone_low, "htf_target": htf_target,
            })

    return pd.DataFrame(events) if events else pd.DataFrame(), entries


def select_best_ltf_entry(ltf_entries: list) -> Optional[dict]:
    """Return the CLOSED LTF entry with lowest zone_low (most conservative stop)."""
    closed = [e for e in ltf_entries if e["status"] == "CLOSED"]
    return min(closed, key=lambda e: e["zone_low"]) if closed else None


def select_fresh_ltf_entry(ltf_entries: list, opt_type: str = "CE") -> Optional[dict]:
    """
    "Clean old zones first" logic.

    TRAPPED = sellers/buyers just got trapped (price broke their zone) = entry signal.
    CLOSED  = zone resolved (price came back past entry = old traders escaped).

    Flow:
    1. Find the last CLOSED zone (most recent resolved structure).
    2. If no CLOSED zones exist → enter on the deepest fresh TRAPPED zone.
    3. If CLOSED zones exist → only enter on TRAPPED zones formed AFTER last CLOSED.
    4. If no fresh TRAPPED zone after last CLOSED → wait.

    Tie-breaking (multiple zones trapped in the same candle — fast open sweep):
      CE: pick the zone with the HIGHEST zone_low  (deepest = last bears to enter,
          weakest hands, tightest SL, maximum squeeze).
      PE: pick the zone with the LOWEST zone_high  (deepest = last bulls to enter).
      This is the correct priority because deeper = more recent = more bears squeezed.

    opt_type: "CE" uses bear zones, "PE" uses bull zones.
    """
    if not ltf_entries:
        return None

    def _ts(e, key):
        v = e.get(key)
        return str(v) if v else ""

    def _best_among(zones: list) -> Optional[dict]:
        """Among a set of TRAPPED zones, pick the deepest (most bears squeezed)."""
        if not zones:
            return None
        # Sort by trapped_on descending (most recent trap first).
        # Break ties by zone_low descending for CE (deepest = highest zone_low),
        # or zone_high ascending for PE (deepest = lowest zone_high).
        if opt_type == "CE":
            return sorted(zones, key=lambda e: (_ts(e, "trapped_on"), e["zone_low"]))[-1]
        else:
            return sorted(zones, key=lambda e: (_ts(e, "trapped_on"), -e["zone_high"]))[-1]

    closed_zones  = [e for e in ltf_entries if e["status"] == "CLOSED"]
    trapped_zones = [e for e in ltf_entries if e["status"] == "TRAPPED"]

    if not closed_zones:
        # No old resolved structure → enter on the deepest fresh TRAPPED zone.
        return _best_among(trapped_zones)

    # Find most recently closed zone.
    last_closed    = sorted(closed_zones, key=lambda e: _ts(e, "closed_on"))[-1]
    last_closed_ts = _ts(last_closed, "closed_on")

    # Fresh TRAPPED zones: formed (ref_ts) AFTER the last CLOSED zone resolved.
    fresh_trapped = [
        e for e in trapped_zones
        if _ts(e, "ref_ts") > last_closed_ts
    ]

    if not fresh_trapped:
        # Old structure cleared but no new trap yet — wait.
        return None

    return _best_among(fresh_trapped)


def simulate_today_trades(all_entries: list, df1: pd.DataFrame,
                          sl_buffer: float = 3.0,
                          lot_size: int = 20, qty: int = 2) -> list:
    """Replay today's HTF zones against 1-min bars. T1=50%, remainder trails."""
    from datetime import date
    today  = date.today()
    units  = qty * lot_size
    t1_units  = units // 2
    rem_units = units - t1_units
    trades = []

    if df1 is None or df1.empty:
        return trades

    df1_work = df1.copy()
    if df1_work.index.tz is not None:
        df1_work.index = df1_work.index.tz_localize(None)

    eligible = [
        e for e in all_entries
        if e["status"] == "CLOSED" and e.get("closed_on")
        and pd.Timestamp(e["closed_on"]).date() == today
    ]

    for e in eligible:
        entry_price = round(e["zone_trigger"], 2)
        sl_price    = round(e["zone_low"] - sl_buffer, 2)
        t1_price    = round(e["sl"], 2)
        closed_ts   = pd.Timestamp(e["closed_on"])
        if closed_ts.tzinfo is not None:
            closed_ts = closed_ts.tz_localize(None)

        df_fwd = df1_work[df1_work.index > closed_ts]
        if df_fwd.empty:
            trades.append({"entry_price": entry_price, "entry_time": closed_ts,
                           "sl": sl_price, "t1": t1_price, "result": "OPEN",
                           "exit_price": None, "exit_time": None, "pnl": 0, "units": units})
            continue

        t1_hit = False; t1_exit_price = t1_price; result = "OPEN"
        exit_price = None; exit_time = None; pnl = 0

        for bar_ts, bar in df_fwd.iterrows():
            if bar["low"] <= sl_price:
                if not t1_hit:
                    result = "SL_HIT"; exit_price = sl_price; exit_time = bar_ts
                    pnl = round((sl_price - entry_price) * units, 2)
                else:
                    result = "T1+SL"; exit_price = sl_price; exit_time = bar_ts
                    pnl = round((t1_exit_price - entry_price) * t1_units
                                + (sl_price - entry_price) * rem_units, 2)
                break
            if not t1_hit and bar["high"] >= t1_price:
                t1_hit = True; t1_exit_price = t1_price
        else:
            last_bar = df_fwd.iloc[-1]
            if t1_hit:
                result = "T1+RUNNING"
                pnl = round((t1_exit_price - entry_price) * t1_units
                            + (last_bar["close"] - entry_price) * rem_units, 2)
                exit_price = last_bar["close"]; exit_time = df_fwd.index[-1]
            else:
                result = "OPEN/SQ_OFF"; exit_price = last_bar["close"]
                exit_time = df_fwd.index[-1]
                pnl = round((exit_price - entry_price) * units, 2)

        trades.append({"entry_price": entry_price, "entry_time": closed_ts,
                       "sl": sl_price, "t1": t1_price, "result": result,
                       "exit_price": exit_price, "exit_time": exit_time,
                       "pnl": pnl, "units": units})

    return sorted(trades, key=lambda t: t["entry_time"])


def backtest(df, all_entries, htf_target_map=None, buffer=DEFAULT_SL_BUFFER,
             qty=DEFAULT_QTY, lot_size=DEFAULT_LOT_SIZE,
             max_entry_time=None, min_rr=0.0, min_zone_width=0.0, df_map=None):
    """Simulate intraday trades from scan results. (Full backtest — unchanged from NiftyTrapScanner.)"""
    units  = qty * lot_size
    trades = []
    eligible = sorted(
        [e for e in all_entries if e["status"] in ("TRAPPED", "CLOSED") and e.get("closed_on")],
        key=lambda e: pd.Timestamp(e["closed_on"])
    )
    open_until = None

    for e in eligible:
        if e.get("trapped_on") is None:
            continue
        entry_price = round(e["entry"], 2)
        target      = round(e.get("htf_target") or e["sl"], 2)
        sl_price    = round(e["zone_low"] - buffer, 2)
        entry_ts    = pd.Timestamp(e["closed_on"])
        entry_date  = entry_ts.date()

        if open_until is not None and entry_ts <= open_until:
            continue
        if max_entry_time:
            ch, cm = map(int, max_entry_time.split(":"));
            if (entry_ts.hour, entry_ts.minute) >= (ch, cm):
                continue
        if min_zone_width > 0 and (e["zone_high"] - e["zone_low"]) < min_zone_width:
            continue
        if min_rr > 0:
            risk = max(entry_price - sl_price, 0.01)
            if (target - entry_price) / risk < min_rr:
                continue

        entry_df = df_map.get(e.get("_df_key", ""), df) if df_map else df
        future = entry_df[(entry_df["datetime"] > entry_ts) & (entry_df["datetime"].dt.date == entry_date)]

        exit_price = exit_reason = exit_ts = None
        for _, bar in future.iterrows():
            if bar["low"] <= sl_price:
                exit_price, exit_reason, exit_ts = sl_price, "SL", bar["datetime"]; break
            if bar["high"] >= target:
                exit_price, exit_reason, exit_ts = target, "TARGET", bar["datetime"]; break

        if exit_price is None:
            ref = future.iloc[-1] if len(future) > 0 else None
            if ref is None:
                rows = entry_df[entry_df["datetime"] == entry_ts]
                ref = rows.iloc[0] if len(rows) > 0 else None
            if ref is None:
                continue
            exit_price = round(ref["close"], 2); exit_reason = "SQUAREOFF"; exit_ts = ref["datetime"]

        pnl = round((exit_price - entry_price) * units, 2)
        open_until = pd.Timestamp(exit_ts) if hasattr(exit_ts, "strftime") else pd.Timestamp(str(exit_ts))
        trades.append({
            "HTF Ref Bar": e.get("htf_ref_bar", "—"), "HTF Trap Bar": e.get("htf_trap_bar", "—"),
            "HTF Zone High": e.get("htf_zone_high", "—"), "HTF Zone Low": e.get("htf_zone_low", "—"),
            "HTF Target": e.get("htf_target", target), "Contract": e.get("_contract", "—"),
            "LTF Date": entry_date.strftime("%d %b %y"),
            "LTF Entry Time": entry_ts.strftime("%H:%M"),
            "LTF Exit Time": exit_ts.strftime("%H:%M") if hasattr(exit_ts, "strftime") else str(exit_ts),
            "LTF Entry": entry_price, "LTF Zone Low": round(e["zone_low"], 2),
            "LTF Target": target, "LTF SL": sl_price, "LTF Exit Price": exit_price,
            "Exit": exit_reason, "P&L (Rs)": pnl,
        })

    df_trades = pd.DataFrame(trades)
    if not df_trades.empty:
        df_trades["Cumulative P&L"] = df_trades["P&L (Rs)"].cumsum().round(2)
    return df_trades


def trade_summary(df_trades: pd.DataFrame) -> dict:
    if df_trades.empty:
        return {}
    total = len(df_trades); wins = int((df_trades["P&L (Rs)"] > 0).sum())
    losses = int((df_trades["P&L (Rs)"] <= 0).sum())
    return {
        "total": total, "wins": wins, "losses": losses,
        "net_pnl": round(df_trades["P&L (Rs)"].sum(), 2),
        "win_rate": round(wins / total * 100, 1) if total else 0,
        "best": round(df_trades["P&L (Rs)"].max(), 2),
        "worst": round(df_trades["P&L (Rs)"].min(), 2),
        "avg_win":  round(df_trades[df_trades["P&L (Rs)"] > 0]["P&L (Rs)"].mean(), 2) if wins else 0,
        "avg_loss": round(df_trades[df_trades["P&L (Rs)"] <= 0]["P&L (Rs)"].mean(), 2) if losses else 0,
    }
