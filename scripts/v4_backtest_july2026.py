"""
scripts/v4_backtest_july2026.py — comprehensive backtest of the 3-gate V4
Cascade premium funnel over the July 2026 monthly expiry (CE 23900 / PE
24300, 21-JUL-2026), under the following explicit rules (2026-07-19):

1. MACRO ZONE PERSISTENCE: HTF structural zones (Gate 1) are fed and locked
   continuously across the WHOLE backtest window — the real, unmodified
   PremiumGateScanner (multi-zone, 2026-07-19) drives Gate 1/Gate 2 exactly
   as in production, so an HTF zone found on 07-08 stays alive and
   unmitigated into 07-14 etc., same as already validated this session.
2. PURE INTRADAY EXECUTION: an open POSITION cannot carry overnight — force
   square-off (market order) if still open at 15:15. At 15:30 each day,
   Gate 2 / Gate 3 state is rolled back to HTF_LOCKED for every in-flight
   setup (MTF zone / limit price / sweep progress discarded) while the
   underlying HTF zone (Gate 1) is left completely untouched — so tomorrow's
   09:15 Gate 2 re-scan starts fresh, anchored to the SAME original HTF ref.
3. Risk model TESTED (this backtest only — does NOT touch the production
   entries.compute_risk_mapping / T1+T2 tranche model in exits.py):
     SL           = Inner_Zone_Low - 10 points (the MTF/Inner zone's own
                     Zone_Low — i.e. the next-candle low after the MTF ref —
                     NOT the original HTF ref; guaranteed < entry_price by
                     construction, per 2026-07-19 clarification) (5m CANDLE
                     CLOSE below -> stop out)
     Target A     = HTF Reference candle's exact HIGH
     Target B 1:2 = entry + 2 x (entry - SL)
     Target B 1:3 = entry + 3 x (entry - SL)
   All three exits are simulated in PARALLEL off the same single entry
   signal (one real position, three hypothetical exit styles compared).
4. Gate 3 liquidity-sweep filter + invalidation (this backtest only, applied
   on top of the real MTF_LOCKED Inner Zone): entry requires a 5m bar's low
   to pierce below the Inner Zone's LOW (deeper than the 1/3-depth limit
   price). Invalidated (no entry) if EITHER (a) that sweep bar's CLOSE is
   below Inner_Zone_Low, or (b) the very next 5m bar prints a lower high AND
   a lower low than the sweep bar. Entry fills at the 1/3-depth limit price
   on the bar after the sweep bar, once neither invalidation fired.

Only one position open at a time across CE+PE (mirrors the production
engine's single-CascadePosition constraint) — the A/B1/B2 target comparison
is a pure post-hoc forward simulation off that one real entry.

Reuses the real PremiumGateScanner (Gate 1 + Gate 2, completely unmodified)
and SpotConfirmTracker, fed real Upstox 1-minute history for every available
July trading day.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import quote as _q

import pandas as pd

sys.path.insert(0, ".")

from config.global_config import IST
from data_layer.client_db import ClientDB
from data_layer.historical_candles import _http_get_json, _parse_candles
from data_layer.instrument_registry import REGISTRY
from strategies.v4_cascade.dataclasses import GateState
from strategies.v4_cascade.rolling_base import resample_bars
from strategies.v4_cascade.spot_confirm import SpotConfirmTracker
from strategies.v4_cascade.zone_state import PremiumGateScanner

EXPIRY = date(2026, 7, 21)
CE_STRIKE = 23900
PE_STRIKE = 24300
# Full July monthly expiry cycle, start of month through the latest
# completed trading day available ("today" in this environment is 07-19 and
# 07-18 was a Saturday, so 07-17 is the latest -- cannot backtest beyond
# "now"). Strikes are fixed at the CE/PE selected from 17-JUL-2026's own
# 09:15 ATM (ATM-200/ATM+200, 100-pt rounding), per the tracking-contract
# convention already validated this session.
START = date(2026, 7, 1)
END = date(2026, 7, 17)

EOD_SQUARE_OFF = (15, 15)
GATE23_RESET_TIME = (15, 30)
SL_OFFSET = 10.0


@dataclass
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    timeframe: int
    volume: int = 0


# ── data fetch ────────────────────────────────────────────────────────────────

def fetch_1m(instrument_key: str, token: str, start: date, end: date) -> pd.DataFrame:
    rows: List[dict] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            url = f"https://api.upstox.com/v2/historical-candle/{_q(instrument_key, safe='')}/1minute/{d.isoformat()}/{d.isoformat()}"
            try:
                rows.extend(_parse_candles(_http_get_json(url, token)))
            except Exception:
                pass
        d += timedelta(days=1)
    if not rows:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"])
    return df.sort_values("ts").reset_index(drop=True)


def to_5m_bars(df: pd.DataFrame, filter_zero_volume: bool) -> List[Bar]:
    if df.empty:
        return []
    if filter_zero_volume:
        df = df[df["volume"] > 0]
        if df.empty:
            return []
    df = df.set_index("ts")
    ohlc = df.resample("5min", label="left", closed="left").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna()
    bars: List[Bar] = []
    for ts, row in ohlc.iterrows():
        py = ts.to_pydatetime()
        if py.tzinfo is None:
            py = py.replace(tzinfo=IST)
        bars.append(Bar(py, float(row["open"]), float(row["high"]), float(row["low"]),
                         float(row["close"]), timeframe=5, volume=int(row["volume"])))
    return bars


def to_75m_bars(bars_5m: List[Bar]) -> List[Bar]:
    rb = resample_bars(bars_5m, 75)
    return [Bar(b.timestamp, b.close, b.high, b.low, b.close, timeframe=75) for b in rb]


def bucket_start(ts: datetime, multiplier: int) -> datetime:
    open_dt = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    minutes_since_open = int((ts - open_dt).total_seconds() // 60)
    return open_dt + timedelta(minutes=(minutes_since_open // multiplier) * multiplier)


def bucket_end_minute(ts: datetime, multiplier: int) -> bool:
    open_dt = ts.replace(hour=9, minute=15, second=0, microsecond=0)
    minutes_since_open = int((ts - open_dt).total_seconds() // 60)
    return (minutes_since_open + 5) % multiplier == 0


# ── per-trade risk model (this backtest's own, NOT exits.py's) ──────────────

@dataclass
class Trade:
    side: str
    day: date
    entry_ts: datetime
    entry_price: float
    sl_price: float
    target_a: float
    target_b1: float  # 1:2
    target_b2: float  # 1:3
    htf_ref_ts: datetime
    htf_ref_low: float
    htf_ref_high: float
    htf_lock_ts: datetime
    mtf_timeframe: int
    mtf_ref_ts: datetime
    mtf_ref_low: float
    mtf_ref_high: float
    mtf_lock_ts: datetime
    inner_zone_low: float
    exit_a_ts: Optional[datetime] = None
    exit_a_price: Optional[float] = None
    exit_a_reason: str = ""
    exit_b1_ts: Optional[datetime] = None
    exit_b1_price: Optional[float] = None
    exit_b1_reason: str = ""
    exit_b2_ts: Optional[datetime] = None
    exit_b2_price: Optional[float] = None
    exit_b2_reason: str = ""


def simulate_exit(entry_price: float, sl: float, target: float, bars_after: List[Bar]) -> tuple:
    """SL = 5m CANDLE CLOSE below the level (per spec). Target = intrabar HIGH
    reaching the level. SL checked first on a shared bar (conservative). The
    last bar in ``bars_after`` is expected to already be the 15:15 EOD bar
    (forced square-off at its close) if no SL/target fired earlier."""
    for idx, b in enumerate(bars_after):
        if b.close < sl:
            return b.timestamp, sl, "SL"
        if b.high >= target:
            return b.timestamp, target, "TARGET"
        if (b.timestamp.hour, b.timestamp.minute) == EOD_SQUARE_OFF:
            return b.timestamp, b.close, "EOD"
    if bars_after:
        last = bars_after[-1]
        return last.timestamp, last.close, "EOD"
    return None, None, "NO_EXIT"


# ── Gate 3 (this backtest's sweep+invalidation variant) ─────────────────────

def check_sweep_gate3(setup, ts: datetime, bars_by_ts: Dict[datetime, Bar],
                       ts_index: Dict[datetime, int], ordered_ts: List[datetime]) -> Optional[tuple]:
    """Evaluate the liquidity-sweep entry rule at the CURRENT bar ``ts``
    (same-day only -- a sweep bar with no next bar available before 15:30
    today is left unresolved, not carried into tomorrow). Returns
    (entry_ts, entry_price, invalidated: bool)."""
    z = setup.mtf_zone
    if z is None or z.entry_line is None or z.sweep_low is None:
        return None
    inner_high, inner_low = z.entry_line, z.sweep_low
    limit_price = inner_high - (inner_high - inner_low) / 3.0

    b = bars_by_ts.get(ts)
    if b is None or b.low > inner_low:
        return None  # no sweep at this bar
    if b.close < inner_low:
        return "INVALIDATE", None, True  # (a) closed below the swept level
    i = ts_index[ts]
    if i + 1 >= len(ordered_ts):
        return None  # no next bar today yet -- unresolved, re-check will happen tomorrow post-reset anyway
    nxt_ts = ordered_ts[i + 1]
    if nxt_ts.date() != ts.date():
        return None  # next bar is a new day -- Gate2/3 already reset by then, treat as unresolved/expired
    nxt = bars_by_ts[nxt_ts]
    if nxt.high < b.high and nxt.low < b.low:
        return "INVALIDATE", None, True  # (b) next bar lower high AND lower low
    return nxt_ts, limit_price, False


def reset_gate23_to_htf_locked(scanner: PremiumGateScanner) -> None:
    """15:30 daily rule: roll every in-flight setup back to HTF_LOCKED,
    discarding Gate 2/3 progress (MTF zone, limit price, sweep state) while
    leaving the underlying HTF zone (Gate 1) completely intact."""
    for setup in scanner.setups:
        if setup.state != GateState.HTF_LOCKED:
            setup.state = GateState.HTF_LOCKED
            setup.mtf_zone = None
            setup.mtf_timeframe = None
            setup.limit_entry_price = None
            setup.mtf_consumed_before_ts = None


# ── main replay ───────────────────────────────────────────────────────────────

def run_backtest(spot_5m: List[Bar], ce_5m: List[Bar], pe_5m: List[Bar]) -> List[Trade]:
    spot_75m_by_ts = {b.timestamp: b for b in to_75m_bars(spot_5m)}
    ce_75m_by_ts = {b.timestamp: b for b in to_75m_bars(ce_5m)}
    pe_75m_by_ts = {b.timestamp: b for b in to_75m_bars(pe_5m)}
    ce_by_ts = {b.timestamp: b for b in ce_5m}
    pe_by_ts = {b.timestamp: b for b in pe_5m}
    bars_by_ts_side = {"CE": ce_by_ts, "PE": pe_by_ts}
    ordered_ts_side = {"CE": sorted(ce_by_ts), "PE": sorted(pe_by_ts)}
    index_side = {side: {t: i for i, t in enumerate(ordered_ts_side[side])} for side in ("CE", "PE")}

    all_ts = sorted(set(ce_by_ts) | set(pe_by_ts))

    spot_confirm = SpotConfirmTracker()
    scanners = {"CE": PremiumGateScanner(), "PE": PremiumGateScanner()}

    trades: List[Trade] = []
    position_blocked_until: Optional[datetime] = None
    last_day: Optional[date] = None

    for ts in all_ts:
        day = ts.date()
        if last_day is not None and day != last_day:
            # crossed into a new day -- nothing special needed here; the
            # 15:30 reset already fired on the PREVIOUS day's last bars below.
            pass
        last_day = day

        ce_bar = ce_by_ts.get(ts)
        pe_bar = pe_by_ts.get(ts)
        if ce_bar is not None:
            scanners["CE"].on_5m_bar(ce_bar)
        if pe_bar is not None:
            scanners["PE"].on_5m_bar(pe_bar)

        if bucket_end_minute(ts, 75):
            bstart = bucket_start(ts, 75)
            sbar = spot_75m_by_ts.get(bstart)
            if sbar is not None:
                spot_confirm.on_75m_bar(sbar)
                scanners["CE"].set_armed(spot_confirm.confirms("CE"))
                scanners["PE"].set_armed(spot_confirm.confirms("PE"))
            ce75 = ce_75m_by_ts.get(bstart)
            if ce75 is not None:
                scanners["CE"].on_75m_bar(ce75)
            pe75 = pe_75m_by_ts.get(bstart)
            if pe75 is not None:
                scanners["PE"].on_75m_bar(pe75)

        # -- Gate 3 sweep+invalidation check (only while no position blocking) --
        fired_this_bar = False
        if position_blocked_until is None or ts > position_blocked_until:
            for side in ("CE", "PE"):
                if fired_this_bar:
                    break
                scanner = scanners[side]
                for setup in list(scanner.setups):
                    if fired_this_bar:
                        break
                    if setup.state != GateState.MTF_LOCKED:
                        continue
                    result = check_sweep_gate3(setup, ts, bars_by_ts_side[side],
                                                index_side[side], ordered_ts_side[side])
                    if result is None:
                        continue
                    if result[2]:  # invalidated
                        scanner.invalidate_setup(setup)
                        continue
                    entry_ts, entry_price, _ = result
                    fired_this_bar = True
                    htf_ref_low, htf_ref_high = setup.htf_zone.entry_line, setup.htf_zone.sl_level
                    # SL is anchored to the INNER/MTF zone that actually produced
                    # the entry (Zone_Low = the MTF ref's next-candle low), NOT
                    # the original (possibly many-days-old) HTF ref -- per user
                    # clarification: "that entry point is our Zone_High and the
                    # next candle after the ref candle low is our Zone_Low. This
                    # Zone_Low - 10 is the SL. That can never be above the entry
                    # price." Guaranteed sl_price < entry_price by construction
                    # (entry sits strictly between inner_high and inner_low).
                    sl_price = setup.mtf_zone.sweep_low - SL_OFFSET
                    risk = entry_price - sl_price
                    target_a = htf_ref_high
                    target_b1 = entry_price + 2 * risk
                    target_b2 = entry_price + 3 * risk
                    bars_after = [b for t2, b in sorted(bars_by_ts_side[side].items()) if t2 > entry_ts]
                    a_ts, a_px, a_r = simulate_exit(entry_price, sl_price, target_a, bars_after)
                    b1_ts, b1_px, b1_r = simulate_exit(entry_price, sl_price, target_b1, bars_after)
                    b2_ts, b2_px, b2_r = simulate_exit(entry_price, sl_price, target_b2, bars_after)
                    mz = setup.mtf_zone
                    trades.append(Trade(
                        side=side, day=entry_ts.date(), entry_ts=entry_ts, entry_price=entry_price,
                        sl_price=sl_price, target_a=target_a, target_b1=target_b1, target_b2=target_b2,
                        htf_ref_ts=setup.htf_ref_ts, htf_ref_low=htf_ref_low, htf_ref_high=htf_ref_high,
                        htf_lock_ts=setup.htf_zone.lock_ts,
                        mtf_timeframe=setup.mtf_timeframe, mtf_ref_ts=mz.reference_low_ts,
                        mtf_ref_low=mz.entry_line, mtf_ref_high=mz.sl_level, mtf_lock_ts=mz.lock_ts,
                        inner_zone_low=mz.sweep_low,
                        exit_a_ts=a_ts, exit_a_price=a_px, exit_a_reason=a_r,
                        exit_b1_ts=b1_ts, exit_b1_price=b1_px, exit_b1_reason=b1_r,
                        exit_b2_ts=b2_ts, exit_b2_price=b2_px, exit_b2_reason=b2_r,
                    ))
                    scanner.pop_setup(setup, entry_ts)
                    position_blocked_until = max(
                        [t for t in (a_ts, b1_ts, b2_ts) if t is not None], default=entry_ts)

        # -- 15:30 daily Gate 2/3 reset (HTF zones untouched) --
        if (ts.hour, ts.minute) == GATE23_RESET_TIME:
            reset_gate23_to_htf_locked(scanners["CE"])
            reset_gate23_to_htf_locked(scanners["PE"])

    return trades


def main() -> None:
    db = ClientDB()
    creds = db.get_feeder_creds_sync("upstox")
    if not creds or not creds.get("access_token"):
        print("FATAL: no Upstox access_token in data/clients.db.")
        sys.exit(1)
    token = creds["access_token"]
    REGISTRY.load_sync("NIFTY", token)
    spot_key = REGISTRY.get_upstox_index_key("NIFTY")
    ce_key = REGISTRY.get_upstox_key("NIFTY", EXPIRY, CE_STRIKE, "CE")
    pe_key = REGISTRY.get_upstox_key("NIFTY", EXPIRY, PE_STRIKE, "PE")
    print(f"spot={spot_key}  CE({CE_STRIKE})={ce_key}  PE({PE_STRIKE})={pe_key}")
    print(f"Backtest window: {START} -> {END} (HTF zones persist across days; positions/Gate2-3 reset daily)\n")

    spot_5m = to_5m_bars(fetch_1m(spot_key, token, START, END), filter_zero_volume=False)
    ce_5m = to_5m_bars(fetch_1m(ce_key, token, START, END), filter_zero_volume=True)
    pe_5m = to_5m_bars(fetch_1m(pe_key, token, START, END), filter_zero_volume=True)
    print(f"spot 5m bars={len(spot_5m)}  CE 5m bars={len(ce_5m)}  PE 5m bars={len(pe_5m)}\n")

    trades = run_backtest(spot_5m, ce_5m, pe_5m)

    print(f"=== TRADE LOG ({len(trades)} trades) — full HTF/MTF detail ===")
    for i, t in enumerate(trades, 1):
        print(f"\n--- Trade {i}: {t.side} on {t.day} ---")
        print(f"  HTF (Gate 1, 75m):  ref_ts={t.htf_ref_ts}  entry(Zone_High)={t.htf_ref_low:.2f}  "
              f"SL_level={t.htf_ref_high:.2f}  TRAPPED(lock)_ts={t.htf_lock_ts}")
        print(f"  MTF (Gate 2, {t.mtf_timeframe}m): ref_ts={t.mtf_ref_ts}  entry(Zone_High)={t.mtf_ref_low:.2f}  "
              f"SL_level={t.mtf_ref_high:.2f}  Inner_Zone_Low={t.inner_zone_low:.2f}  TRAPPED(lock)_ts={t.mtf_lock_ts}")
        print(f"  Gate 3 (sweep):     entry_ts={t.entry_ts}  entry_price={t.entry_price:.2f}  "
              f"SL(Inner_Zone_Low-10)={t.sl_price:.2f}")
        print(f"  Target A (HTF ref high={t.target_a:.2f}):  exit={t.exit_a_price:.2f} @ {t.exit_a_ts}  [{t.exit_a_reason}]")
        print(f"  Target B 1:2 ({t.target_b1:.2f}):          exit={t.exit_b1_price:.2f} @ {t.exit_b1_ts}  [{t.exit_b1_reason}]")
        print(f"  Target B 1:3 ({t.target_b2:.2f}):          exit={t.exit_b2_price:.2f} @ {t.exit_b2_ts}  [{t.exit_b2_reason}]")

    print(f"\n=== TRADE LOG (condensed table) ===")
    hdr = (f"{'Day':10} {'Side':4} {'HTF Ref TS':20} {'HTF Lock':20} | {'MTF Ref TS':20} {'MTF Lock':20} tf | "
           f"{'EntryTS':20} {'Entry':>8} {'SL':>8} | {'A@':7} {'Arsn':5} | {'B1@':7} {'B1rsn':5} | {'B2@':7} {'B2rsn':5}")
    print(hdr)
    for t in trades:
        print(f"{str(t.day):10} {t.side:4} {str(t.htf_ref_ts):20} {str(t.htf_lock_ts):20} | "
              f"{str(t.mtf_ref_ts):20} {str(t.mtf_lock_ts):20} {t.mtf_timeframe:2} | "
              f"{str(t.entry_ts):20} {t.entry_price:8.2f} {t.sl_price:8.2f} | "
              f"{(t.exit_a_price or 0):7.2f} {t.exit_a_reason:5} | "
              f"{(t.exit_b1_price or 0):7.2f} {t.exit_b1_reason:5} | "
              f"{(t.exit_b2_price or 0):7.2f} {t.exit_b2_reason:5}")

    def stats(label: str, exit_price_attr: str, reason_attr: str) -> None:
        pts, wins = [], 0
        for t in trades:
            ep = getattr(t, exit_price_attr)
            reason = getattr(t, reason_attr)
            if ep is None:
                continue
            pnl = ep - t.entry_price
            pts.append(pnl)
            if pnl > 0:
                wins += 1
        n = len(pts)
        gross_win = sum(p for p in pts if p > 0)
        gross_loss = -sum(p for p in pts if p < 0)
        win_rate = (wins / n * 100.0) if n else 0.0
        pf = (gross_win / gross_loss) if gross_loss > 0 else (float("inf") if gross_win > 0 else 0.0)
        print(f"{label:14}: trades={n:3}  win_rate={win_rate:6.2f}%  "
              f"gross_win={gross_win:9.2f}  gross_loss={gross_loss:9.2f}  "
              f"net_pts={sum(pts):9.2f}  profit_factor={pf:6.3f}")

    print(f"\n=== SUMMARY (premium points, {CE_STRIKE}/{PE_STRIKE} tracking contracts) ===")
    stats("Target A", "exit_a_price", "exit_a_reason")
    stats("Target B 1:2", "exit_b1_price", "exit_b1_reason")
    stats("Target B 1:3", "exit_b2_price", "exit_b2_reason")


if __name__ == "__main__":
    main()
