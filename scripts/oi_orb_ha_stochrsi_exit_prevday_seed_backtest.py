"""
scripts/oi_orb_ha_stochrsi_exit_prevday_seed_backtest.py -- 2026-09-08, direct
user follow-up to oi_orb_ha_stochrsi_exit_backtest.py: "not saying to carry
Heikin-Ashi itself from the previous day, only the RSI/StochRSI/smooth
INDICATOR WARMUP."

Re-runs the exact same validated exit mechanic (HA_high==HA_open / HA_low==
HA_open + StochRSI(9,9,3) inclusive cross, entry unchanged: VWAP-retest /
historical-immediate), with ONE change: the RSI/Stoch/smooth rolling window
is primed with the PREVIOUS trading day's own (freshly, independently
computed -- day-reset, no cross-day HA recursion) 15-min HA closes, so
today's very first 15-min bar can already have a real %K/%D instead of
needing ~19-21 bars (~5 hours) of today's own history to warm up.

Baseline (cold-start, no priming) is oi_orb_ha_stochrsi_exit_backtest.py
itself -- this script reproduces that same baseline inline (via
warmup_days=0) so both numbers come from one apples-to-apples run against
the same ROWS/entry mechanic/exit shape check, and renders both side by side
in an HTML report.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_ha_stochrsi_exit_prevday_seed_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional

sys.path.insert(0, ".")

from scripts.oi_orb_entry_mode_backtest import (
    ROWS, ORB_START, ORB_END, ENTRY_WINDOW_END, _key_range, to_n_min_bars,
    compute_orb, resolve_eq_key, to_bars, volume_by_ts,
)
from scripts.oi_orb_30_3_1_target_backtest import to_heikin_ashi
from scripts.oi_orb_stoch_rsi_backtest import compute_stoch_rsi
from strategies.core.candle_indicators import ha_stoch_shape_exit_signal
from strategies.oi_orb_screener import screener
from data_layer.historical_candles import fetch_upstox_range_1m

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
EXIT_TF_MIN = 15
RSI_PERIOD = 9
STOCH_PERIOD = 9
SMOOTH = 3


@dataclass
class Trade:
    date: str
    symbol: str
    side: str
    variant: str
    entry_ts: object
    entry_price: Optional[float]
    exit_ts: object
    exit_price: Optional[float]
    reason: str

    @property
    def points(self) -> Optional[float]:
        if self.entry_price is None or self.exit_price is None:
            return None
        raw = self.exit_price - self.entry_price
        return raw if self.side == "CALL" else -raw


def _prev_weekday(d: date) -> date:
    """Simple Mon-Fri step-back (no NSE holiday calendar) -- same caveat as
    every other quick backtest script in this codebase that doesn't pull a
    real holiday list. A holiday will just mean that "previous day" fetch
    returns empty and this symbol/date falls back to the cold-start
    (unprimed) behavior, same as any other missing-data case."""
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


async def fetch_all_with_prevday():
    """cache[(trade_date, symbol)] = (bars_1m, vol_by_ts, orb_h, orb_l,
    prev_day_ha_15m_closes | None)"""
    cache = {}
    prevday_cache = {}
    for trade_date, symbol, _side_bias in ROWS:
        key = (trade_date, symbol)
        if key in cache:
            continue
        eq_key = resolve_eq_key(symbol)
        if eq_key is None:
            cache[key] = None
            continue
        d = date.fromisoformat(trade_date)
        rows = await fetch_upstox_range_1m(eq_key, TOKEN, d, d)
        if not rows:
            cache[key] = None
            continue
        bars_1m = to_bars(rows)
        vol_by_ts = volume_by_ts(rows)
        orb = compute_orb(bars_1m)
        if orb is None:
            cache[key] = None
            continue
        orb_h, orb_l = orb

        pkey = (symbol, _prev_weekday(d))
        if pkey not in prevday_cache:
            prev_rows = await fetch_upstox_range_1m(eq_key, TOKEN, pkey[1], pkey[1])
            if prev_rows:
                prev_bars_1m = to_bars(prev_rows)
                prev_ha_1m = to_heikin_ashi(prev_bars_1m)
                prev_ha_15m = to_n_min_bars(prev_ha_1m, EXIT_TF_MIN)
                prevday_cache[pkey] = [b.close for b in prev_ha_15m]
            else:
                prevday_cache[pkey] = None
        prev_closes = prevday_cache[pkey]

        cache[key] = (bars_1m, vol_by_ts, orb_h, orb_l, prev_closes)
    usable = sum(1 for v in cache.values() if v)
    with_prev = sum(1 for v in cache.values() if v and v[4])
    print(f"Fetched {usable} usable rows ({with_prev} with a usable previous-day seed).")
    return cache


def ha_stoch_exit(entry_ts, entry_price, side, bars_1m, ha_15m_today, k, d,
                   seed_len: int, inclusive=True):
    """Same shape as the baseline script's ha_stoch_exit, except k/d were
    computed over (seed_closes + today_closes) -- seed_len tells us the
    offset into k/d where TODAY's bars actually start, so indexing into
    ha_15m_today (today-only) stays aligned to k[seed_len + i]/d[seed_len + i]."""
    post_entry = [b for b in bars_1m if b.ts >= entry_ts]
    for i, hb in enumerate(ha_15m_today):
        if hb.ts < entry_ts:
            continue
        ki = k[seed_len + i]
        di = d[seed_len + i]
        if ha_stoch_shape_exit_signal(hb, ki, di, side, inclusive=inclusive):
            candidates = [b for b in post_entry if b.ts >= hb.ts]
            if candidates:
                exit_bar = candidates[0]
                return exit_bar.ts, exit_bar.close, "ha_stoch_exit"
    if post_entry:
        last = post_entry[-1]
        return last.ts, last.close, "eod_close"
    return entry_ts, entry_price, "no_data_after_entry"


def run_one(bars_1m, side, orb_h, orb_l, vol_by_ts, ha_15m_today, k, d, seed_len, inclusive=True):
    orb_bars = _key_range(bars_1m, ORB_START, ORB_END)
    vwap_state = screener.VwapState()
    armed = False
    historically_fulfilled = False
    for b in orb_bars:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if fire:
            historically_fulfilled = True
            break

    entry_window = _key_range(bars_1m, ORB_END, ENTRY_WINDOW_END)
    if not entry_window:
        return None

    if historically_fulfilled:
        b0 = entry_window[0]
        exit_ts, exit_price, reason = ha_stoch_exit(b0.ts, b0.close, side, bars_1m, ha_15m_today, k, d, seed_len, inclusive)
        return (b0.ts, b0.close, exit_ts, exit_price, "immediate_historical:" + reason)

    breached = False
    for b in entry_window:
        vol = vol_by_ts.get(b.ts, 0.0)
        typical = (b.high + b.low + b.close) / 3.0
        if vol > 0:
            vwap_state.update("SYM", typical, vol)
        if side == "CALL" and b.low <= orb_l:
            breached = True
        elif side == "PUT" and b.high >= orb_h:
            breached = True
        vwap = vwap_state.current("SYM")
        if vwap is None:
            continue
        armed, fire = screener.check_vwap_retest_entry(side, b.close, vwap, armed, 0.15)
        if not fire:
            continue
        armed = False
        if breached:
            continue
        exit_ts, exit_price, reason = ha_stoch_exit(b.ts, b.close, side, bars_1m, ha_15m_today, k, d, seed_len, inclusive)
        return (b.ts, b.close, exit_ts, exit_price, reason)
    return None


def run_all(cache, variant: str, use_seed: bool, inclusive=True):
    trades = []
    for trade_date, symbol, side_bias in ROWS:
        side = "CALL" if side_bias == "bullish" else "PUT"
        cached = cache.get((trade_date, symbol))
        if cached is None:
            continue
        bars_1m, vol_by_ts, orb_h, orb_l, prev_closes = cached

        ha_1m_today = to_heikin_ashi(bars_1m)
        ha_15m_today = to_n_min_bars(ha_1m_today, EXIT_TF_MIN)
        today_closes = [b.close for b in ha_15m_today]

        if use_seed and prev_closes:
            seed_len = len(prev_closes)
            all_closes = prev_closes + today_closes
        else:
            seed_len = 0
            all_closes = today_closes

        k, d = compute_stoch_rsi(all_closes, RSI_PERIOD, STOCH_PERIOD, SMOOTH)

        result = run_one(bars_1m, side, orb_h, orb_l, vol_by_ts, ha_15m_today, k, d, seed_len, inclusive)
        if result is None:
            continue
        entry_ts, entry_price, exit_ts, exit_price, reason = result
        trades.append(Trade(trade_date, symbol, side, variant, entry_ts, entry_price, exit_ts, exit_price, reason))
    return trades


def summarize(trades):
    entered = [t for t in trades if t.entry_price is not None]
    wins = [t for t in entered if t.points > 0]
    losses = [t for t in entered if t.points <= 0]
    total = sum(t.points for t in entered)
    loss_sum = sum(t.points for t in losses)
    pf = (sum(t.points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    exit_hits = sum(1 for t in entered if t.reason.endswith("ha_stoch_exit"))
    eod_hits = sum(1 for t in entered if t.reason.endswith("eod_close"))
    avg_hold_min = None
    holds = [(t.exit_ts - t.entry_ts).total_seconds() / 60.0 for t in entered
             if t.exit_ts is not None and t.entry_ts is not None]
    if holds:
        avg_hold_min = sum(holds) / len(holds)
    return {"trades": trades, "entered": len(entered), "wins": len(wins), "losses": len(losses),
            "exit_hits": exit_hits, "eod_hits": eod_hits, "win_pct": win_pct, "pf": pf,
            "total": total, "avg": (total / len(entered)) if entered else 0.0,
            "avg_hold_min": avg_hold_min}


def _fmt_pf(pf):
    return "inf" if pf == float("inf") else f"{pf:.2f}"


def render_html(baseline_all, primed_all, baseline_by_symbol, primed_by_symbol, fetch_note):
    def row(label, s):
        hold = f"{s['avg_hold_min']:.0f} min" if s["avg_hold_min"] is not None else "-"
        return (f"<tr><td>{label}</td><td>{s['entered']}</td><td>{s['wins']}</td>"
                f"<td>{s['losses']}</td><td>{s['exit_hits']}</td><td>{s['eod_hits']}</td>"
                f"<td>{s['win_pct']:.1f}%</td><td>{_fmt_pf(s['pf'])}</td>"
                f"<td>{s['total']:+.2f}</td><td>{s['avg']:+.2f}</td><td>{hold}</td></tr>")

    def trade_rows(trades):
        out = []
        for t in sorted(trades, key=lambda x: (x.date, x.symbol)):
            cls = "win" if (t.points is not None and t.points > 0) else "loss"
            pts = f"{t.points:+.2f}" if t.points is not None else "-"
            out.append(
                f"<tr class='{cls}'><td>{t.date}</td><td>{t.symbol}</td><td>{t.side}</td>"
                f"<td>{t.entry_price:.2f}@{t.entry_ts.strftime('%H:%M')}</td>"
                f"<td>{t.exit_price:.2f}@{t.exit_ts.strftime('%H:%M')}</td>"
                f"<td>{t.reason}</td><td>{pts}</td></tr>")
        return "\n".join(out)

    html = f"""<!doctype html>
<html><head><meta charset="utf-8">
<title>OI-ORB Screener: HA+StochRSI Exit -- Prev-Day Warmup vs Cold-Start</title>
<style>
:root {{ color-scheme: light dark; }}
body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; padding: 32px;
        background: #0b0e14; color: #e6e6e6; }}
@media (prefers-color-scheme: light) {{ body {{ background: #f7f7f9; color: #16181d; }} }}
h1 {{ font-size: 22px; margin-bottom: 4px; }}
h2 {{ font-size: 16px; margin-top: 36px; color: #9aa4b2; }}
.note {{ color: #9aa4b2; font-size: 13px; max-width: 900px; line-height: 1.5; }}
table {{ border-collapse: collapse; width: 100%; margin-top: 10px; font-size: 13px; }}
th, td {{ padding: 6px 10px; text-align: right; border-bottom: 1px solid rgba(128,128,128,0.25); }}
th:first-child, td:first-child {{ text-align: left; }}
th {{ color: #9aa4b2; font-weight: 600; }}
tr.win {{ background: rgba(46, 204, 113, 0.08); }}
tr.loss {{ background: rgba(231, 76, 60, 0.08); }}
.summary-card {{ display: flex; gap: 16px; margin-top: 12px; flex-wrap: wrap; }}
.card {{ border: 1px solid rgba(128,128,128,0.3); border-radius: 8px; padding: 14px 18px; min-width: 160px; }}
.card .k {{ font-size: 12px; color: #9aa4b2; }}
.card .v {{ font-size: 20px; font-weight: 700; }}
.pos {{ color: #2ecc71; }} .neg {{ color: #e74c3c; }}
.scroll {{ overflow-x: auto; }}
</style></head>
<body>
<h1>OI-ORB Screener &mdash; HA(15m)+StochRSI(9,9,3) Exit: Prev-Day Indicator Warmup vs Cold-Start</h1>
<p class="note">{fetch_note}<br>
Entry mechanic unchanged in both runs (VWAP-retest / historical-immediate, ENTRY_WINDOW = {ORB_END}&ndash;{ENTRY_WINDOW_END}).
Exit shape (HA_high==HA_open / HA_low==HA_open + StochRSI inclusive cross) unchanged. The ONLY difference:
"primed" seeds the RSI/Stoch(9,9,3)/smooth(3) rolling window with the previous trading day's own
independently-computed (day-reset) 15-min HA closes, so today's first bar already has a real %K/%D.
Heikin-Ashi itself is still computed fresh each day &mdash; no cross-day HA recursion.</p>

<h2>Overall (ALL trades, both CALL &amp; PUT)</h2>
<div class="scroll">
<table>
<tr><th>Variant</th><th>Entered</th><th>Wins</th><th>Losses</th><th>ha_stoch_exit</th><th>eod_close</th>
<th>Win%</th><th>PF</th><th>Total pts</th><th>Avg pts</th><th>Avg hold</th></tr>
{row("Cold-start (baseline, validated)", baseline_all)}
{row("Prev-day primed", primed_all)}
</table>
</div>

<h2>By side</h2>
<div class="scroll">
<table>
<tr><th>Variant</th><th>Entered</th><th>Wins</th><th>Losses</th><th>ha_stoch_exit</th><th>eod_close</th>
<th>Win%</th><th>PF</th><th>Total pts</th><th>Avg pts</th><th>Avg hold</th></tr>
{row("Cold-start CALL", baseline_by_symbol["CALL"])}
{row("Primed CALL", primed_by_symbol["CALL"])}
{row("Cold-start PUT", baseline_by_symbol["PUT"])}
{row("Primed PUT", primed_by_symbol["PUT"])}
</table>
</div>

<h2>Cold-start &mdash; individual trades</h2>
<div class="scroll">
<table>
<tr><th>Date</th><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>Reason</th><th>Points</th></tr>
{trade_rows(baseline_all["trades"])}
</table>
</div>

<h2>Prev-day primed &mdash; individual trades</h2>
<div class="scroll">
<table>
<tr><th>Date</th><th>Symbol</th><th>Side</th><th>Entry</th><th>Exit</th><th>Reason</th><th>Points</th></tr>
{trade_rows(primed_all["trades"])}
</table>
</div>

</body></html>"""
    return html


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    print("Fetching all rows + previous trading day (real Upstox 1-min NSE_EQ history)...")
    cache = await fetch_all_with_prevday()
    usable = sum(1 for v in cache.values() if v)
    with_prev = sum(1 for v in cache.values() if v and v[4])
    fetch_note = (f"Dataset: {len(ROWS)} shortlisted (date, symbol) rows from the real forward paper-trading "
                  f"shortlist, {usable} with usable same-day 1-min history, {with_prev} of those also with a "
                  f"usable previous-trading-day seed (plain Mon-Fri step-back, no NSE holiday calendar -- a "
                  f"holiday just falls back to cold-start for that one row).")

    baseline_trades = run_all(cache, "cold", use_seed=False, inclusive=True)
    primed_trades = run_all(cache, "primed", use_seed=True, inclusive=True)

    baseline_all = summarize(baseline_trades)
    primed_all = summarize(primed_trades)
    baseline_by_symbol = {
        "CALL": summarize([t for t in baseline_trades if t.side == "CALL"]),
        "PUT": summarize([t for t in baseline_trades if t.side == "PUT"]),
    }
    primed_by_symbol = {
        "CALL": summarize([t for t in primed_trades if t.side == "CALL"]),
        "PUT": summarize([t for t in primed_trades if t.side == "PUT"]),
    }

    print(f"\nCold-start   entered={baseline_all['entered']:2d}  win%={baseline_all['win_pct']:5.1f}  "
          f"PF={_fmt_pf(baseline_all['pf'])}  total={baseline_all['total']:+9.2f}  "
          f"avg_hold={baseline_all['avg_hold_min']}")
    print(f"Primed       entered={primed_all['entered']:2d}  win%={primed_all['win_pct']:5.1f}  "
          f"PF={_fmt_pf(primed_all['pf'])}  total={primed_all['total']:+9.2f}  "
          f"avg_hold={primed_all['avg_hold_min']}")

    html = render_html(baseline_all, primed_all, baseline_by_symbol, primed_by_symbol, fetch_note)
    out_path = "scratch_oi_orb_ha_stochrsi_prevday_seed_report.html"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
