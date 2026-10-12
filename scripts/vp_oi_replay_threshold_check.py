"""Replay today's REAL recorded OI history (data/vp_oi_regime.db) through the
NEW classification rules (10% threshold, no hold-over -- 2026-10-12 direct
user spec) and compare against what was ACTUALLY decided live (3% threshold,
old sticky hold-over). Run this ON the server where data/vp_oi_regime.db
lives (it's never been copied locally).

Usage: python scripts/vp_oi_replay_threshold_check.py NIFTY 2026-10-12
"""
import sqlite3
import sys

sys.path.insert(0, ".")
from strategies.vp_oi_regime.sticky_latch import StickyTrendLatch
from strategies.vp_oi_regime.decision_matrix import decide

underlying = sys.argv[1] if len(sys.argv) > 1 else "NIFTY"
trade_date = sys.argv[2] if len(sys.argv) > 2 else "2026-10-12"

conn = sqlite3.connect("data/vp_oi_regime.db")
rows = conn.execute(
    "SELECT ts, future_oi_now, call_oi_now, put_oi_now, regime "
    "FROM snapshots WHERE underlying=? AND trade_date=? "
    "AND future_oi_now IS NOT NULL ORDER BY ts",
    (underlying, trade_date),
).fetchall()
conn.close()

if not rows:
    print(f"No rows for {underlying} {trade_date}")
    sys.exit(1)

future_latch = StickyTrendLatch(trend_pct=10.0, eval_window_min=5)
call_latch = StickyTrendLatch(trend_pct=10.0, eval_window_min=5)
put_latch = StickyTrendLatch(trend_pct=10.0, eval_window_min=5)

print(f"{'ts':<30} {'old_regime':<16} {'new_future':<10} {'new_call':<10} {'new_put':<10} {'new_regime':<16} DIFF?")
prev_new_regime = None
for ts, fut_now, call_now, put_now, old_regime in rows:
    # ts is ISO with +05:30 offset; convert to a monotonic seconds value for
    # the latch's own eval_window_min gating (any consistent increasing
    # float works -- use a running index scaled to seconds since this is
    # just for gating the 5-min cadence, not real time math).
    from datetime import datetime
    t_epoch = datetime.fromisoformat(ts).timestamp()

    f_trend, f_pct, f_anchor = future_latch.update(fut_now, t_epoch)
    c_trend, c_pct, c_anchor = call_latch.update(call_now, t_epoch)
    p_trend, p_pct, p_anchor = put_latch.update(put_now, t_epoch)
    new_regime = decide(f_trend, p_trend, c_trend).regime

    if new_regime != prev_new_regime:
        diff = " <<<< DIFFERS FROM LIVE" if new_regime != old_regime else ""
        print(f"{ts:<30} {old_regime or '':<16} {f_trend:<10} {c_trend:<10} {p_trend:<10} {new_regime:<16}{diff}")
        prev_new_regime = new_regime
