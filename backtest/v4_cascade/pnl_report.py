"""backtest/v4_cascade/pnl_report.py -- computes per-trade P&L from a
multi_strike_option_premium_backtest.py JSON dump. Pairs each OPEN event
with its following T1/T2 CLOSE events (safe to do sequentially since the
engine only ever holds one position at a time, engine-wide) and prints a
full tabular P&L report in points and rupees (qty=75/tranche at
lot_size=75, lot_multiplier=2, matching the backtest's own config).

Usage:
    python backtest/v4_cascade/pnl_report.py backtest/v4_cascade/results/multi_strike_july.json
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Dict, List, Optional

QTY_PER_TRANCHE = 75  # lot_size=75, lot_multiplier=2 -> (75*2)//2 = 75, matches the backtest config


def load_events(path: str) -> List[dict]:
    with open(path) as f:
        data = json.load(f)
    return data["events"]


def pair_trades(events: List[dict]) -> List[dict]:
    trades: List[dict] = []
    open_pos: Optional[dict] = None
    for e in events:
        if e["event"].startswith("open_long"):
            side = e["event"].split("_")[-1].upper()  # "open_long_ce" -> "CE"
            open_pos = {
                "side": side, "strike": e["strike"], "entry_ts": e["ts"], "entry_price": e["price"],
                "sl": e["sl"], "target": e["target"], "t1_close": None, "t2_close": None,
            }
            trades.append(open_pos)
        elif e["event"].startswith("close_long"):
            if open_pos is None:
                continue  # shouldn't happen, defensive
            tranche = e["tranche"]
            if tranche == "T1":
                open_pos["t1_close"] = {"ts": e["ts"], "price": e["price"], "reason": e["reason"]}
            elif tranche == "T2":
                open_pos["t2_close"] = {"ts": e["ts"], "price": e["price"], "reason": e["reason"]}
    return trades


def compute_pnl(trade: dict) -> dict:
    entry = trade["entry_price"]
    t1, t2 = trade["t1_close"], trade["t2_close"]
    t1_pts = (t1["price"] - entry) if t1 else None
    t2_pts = (t2["price"] - entry) if t2 else None
    t1_rs = t1_pts * QTY_PER_TRANCHE if t1_pts is not None else None
    t2_rs = t2_pts * QTY_PER_TRANCHE if t2_pts is not None else None
    total_rs = (t1_rs or 0) + (t2_rs or 0)
    return {**trade, "t1_pts": t1_pts, "t2_pts": t2_pts, "t1_rs": t1_rs, "t2_rs": t2_rs, "total_rs": total_rs}


def main(path: str) -> None:
    events = load_events(path)
    trades = pair_trades(events)
    priced = [compute_pnl(t) for t in trades]

    header = (f"{'#':>3} {'entry_ts':<17} {'side':<5} {'strike':>7} {'entry':>8} "
              f"{'T1_exit':>8} {'T1_reason':<24} {'T1_pts':>8} {'T1_Rs':>10} "
              f"{'T2_exit':>8} {'T2_reason':<24} {'T2_pts':>8} {'T2_Rs':>10} {'TOTAL_Rs':>10}")
    print(header)
    print("-" * len(header))
    running_total = 0.0
    for i, t in enumerate(priced, 1):
        entry_ts = t["entry_ts"][:16].replace("T", " ")
        t1 = t["t1_close"]
        t2 = t["t2_close"]
        t1_exit = f"{t1['price']:.2f}" if t1 else "-"
        t1_reason = t1["reason"] if t1 else "-"
        t1_pts = f"{t['t1_pts']:.2f}" if t["t1_pts"] is not None else "-"
        t1_rs = f"{t['t1_rs']:.0f}" if t["t1_rs"] is not None else "-"
        t2_exit = f"{t2['price']:.2f}" if t2 else "-"
        t2_reason = t2["reason"] if t2 else "-"
        t2_pts = f"{t['t2_pts']:.2f}" if t["t2_pts"] is not None else "-"
        t2_rs = f"{t['t2_rs']:.0f}" if t["t2_rs"] is not None else "-"
        running_total += t["total_rs"]
        print(f"{i:>3} {entry_ts:<17} {t['side']:<5} {t['strike']:>7} {t['entry_price']:>8.2f} "
              f"{t1_exit:>8} {t1_reason:<24} {t1_pts:>8} {t1_rs:>10} "
              f"{t2_exit:>8} {t2_reason:<24} {t2_pts:>8} {t2_rs:>10} {t['total_rs']:>10.0f}")

    print("-" * len(header))
    wins = [t for t in priced if t["total_rs"] > 0]
    losses = [t for t in priced if t["total_rs"] < 0]
    flat = [t for t in priced if t["total_rs"] == 0]
    print(f"\nTotal trades: {len(priced)}   Wins: {len(wins)}   Losses: {len(losses)}   Flat: {len(flat)}")
    print(f"Win rate: {len(wins)/len(priced)*100:.1f}%" if priced else "Win rate: n/a")
    print(f"TOTAL P&L: Rs {running_total:,.0f}")
    if priced:
        print(f"Avg P&L/trade: Rs {running_total/len(priced):,.0f}")
    if wins:
        print(f"Avg win: Rs {sum(t['total_rs'] for t in wins)/len(wins):,.0f}")
    if losses:
        print(f"Avg loss: Rs {sum(t['total_rs'] for t in losses)/len(losses):,.0f}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "backtest/v4_cascade/results/multi_strike_july.json")
