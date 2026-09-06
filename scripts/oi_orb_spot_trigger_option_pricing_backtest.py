"""
scripts/oi_orb_spot_trigger_option_pricing_backtest.py -- 2026-09-06, direct
user correction on scripts/oi_orb_option_entry_spot_exit_backtest.py: that
script ran an INDEPENDENT VWAP-retest signal on the option's own chart to
decide the entry timestamp. That is NOT what was asked for. Direct user
correction: "all trigger in spot but entry exit in option" -- BOTH the
entry trigger AND the exit trigger (SL/target) stay exactly the confirmed
baseline mechanic, 100% computed on the SPOT chart, completely unchanged
from scripts/oi_orb_ha_stochrsi_exit_backtest.py (reused, not
reimplemented). The ONLY new thing this script adds is looking up what the
OPTION's own premium was at those two spot-triggered timestamps, so the
resulting P&L can be reported in real option-premium terms as well as spot
points -- no independent option-chart signal generation anywhere.

Strike/expiry selection matches the live engine exactly (2% OTM off the
ORB extreme, real strike-snapping, expiry resolved as of the historical
trade date) -- same as the previous (superseded) script.

Usage: UPSTOX_TOKEN=<token> python scripts/oi_orb_spot_trigger_option_pricing_backtest.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional

sys.path.insert(0, ".")

from data_layer.historical_candles import fetch_upstox_range_1m
from scripts.oi_orb_entry_mode_backtest import ROWS, SIDE, to_n_min_bars, to_bars, volume_by_ts
from scripts.oi_orb_atr_chandelier_backtest import fetch_all
from scripts.oi_orb_30_3_1_target_backtest import to_heikin_ashi
from scripts.oi_orb_stoch_rsi_backtest import compute_stoch_rsi
from scripts.oi_orb_ha_stochrsi_exit_backtest import ha_stoch_exit
from scripts.oi_orb_trap_target_full_htf_ltf_sweep import find_entry   # the UNCHANGED spot-chart entry trigger
from scripts.oi_orb_option_entry_spot_exit_backtest import resolve_leg  # unchanged strike/expiry resolution

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
EXIT_TF_MIN = 15
RSI_PERIOD = 9
STOCH_PERIOD = 9
SMOOTH = 3


def price_at_or_before(bars_1m, ts):
    candidates = [b for b in bars_1m if b.ts <= ts]
    return candidates[-1].close if candidates else None


def price_at_or_after(bars_1m, ts):
    candidates = [b for b in bars_1m if b.ts >= ts]
    return candidates[0].close if candidates else None


@dataclass
class Trade:
    date: str
    symbol: str
    side: str
    strike: int
    opt_type: str
    lot: int
    entry_ts: object
    exit_ts: object
    spot_entry_price: Optional[float]
    spot_exit_price: Optional[float]
    exit_reason: str
    opt_entry_price: Optional[float]
    opt_exit_price: Optional[float]

    @property
    def spot_points(self) -> Optional[float]:
        if self.spot_entry_price is None or self.spot_exit_price is None:
            return None
        raw = self.spot_exit_price - self.spot_entry_price
        return raw if self.side == "CALL" else -raw

    @property
    def option_points(self) -> Optional[float]:
        if self.opt_entry_price is None or self.opt_exit_price is None:
            return None
        return self.opt_exit_price - self.opt_entry_price   # long option: premium up = profit, regardless of side

    @property
    def option_pnl_rs(self) -> Optional[float]:
        pts = self.option_points
        return pts * self.lot if pts is not None else None

    @property
    def capital_invested(self) -> Optional[float]:
        """Real Rs outlay to open this position: entry premium x lot size --
        the actual cash required to buy this option, same convention every
        real options-buyer P&L calc in this codebase uses."""
        if self.opt_entry_price is None:
            return None
        return self.opt_entry_price * self.lot

    @property
    def return_pct(self) -> Optional[float]:
        """Return on the capital actually invested for THIS trade -- the
        metric that matters for an option BUYER (a Rs8,000 profit on
        Rs2,000 invested is a very different trade than the same profit on
        Rs20,000 invested), not just raw Rs P&L."""
        cap = self.capital_invested
        pnl = self.option_pnl_rs
        if cap is None or pnl is None or cap == 0:
            return None
        return (pnl / cap) * 100.0


async def fetch_option_bars(upstox_key: str, d: date):
    rows = await fetch_upstox_range_1m(upstox_key, TOKEN, d, d)
    if not rows:
        return []
    return to_bars(rows)


async def run_all():
    print("Fetching spot data (real Upstox 1-min NSE_EQ history)...")
    spot_cache = await fetch_all()

    trades: List[Trade] = []
    skipped: List[str] = []

    for trade_date, symbol, side_bias in ROWS:
        side = SIDE[side_bias]
        cached = spot_cache.get((trade_date, symbol))
        if cached is None:
            skipped.append(f"{trade_date} {symbol}: no spot data")
            continue
        spot_bars_1m, vol_by_ts, orb_h, orb_l = cached
        d = date.fromisoformat(trade_date)

        # ---- ENTRY TRIGGER: unchanged spot-chart VWAP-retest (same as baseline) ----
        entry = find_entry(spot_bars_1m, side, orb_h, orb_l, vol_by_ts)
        if entry is None:
            skipped.append(f"{trade_date} {symbol}: no spot-chart entry fired")
            continue
        entry_ts, spot_entry_price = entry

        # ---- EXIT TRIGGER: unchanged spot-chart HA-shape + StochRSI(9,9) inclusive, no SL, EOD fallback ----
        ha_1m = to_heikin_ashi(spot_bars_1m)
        ha_15m = to_n_min_bars(ha_1m, EXIT_TF_MIN)
        k, dd = compute_stoch_rsi([b.close for b in ha_15m], RSI_PERIOD, STOCH_PERIOD, SMOOTH)
        exit_ts, spot_exit_price, reason = ha_stoch_exit(
            entry_ts, spot_entry_price, side, spot_bars_1m, ha_15m, k, dd, inclusive=True)

        # ---- OPTION PRICING ONLY: resolve contract, look up premium at the SAME two spot-triggered timestamps ----
        leg = resolve_leg(symbol, side, orb_h, orb_l, d)
        if leg is None:
            skipped.append(f"{trade_date} {symbol}: could not resolve option contract")
            continue
        opt_bars_1m = await fetch_option_bars(leg.upstox_key, d)
        if not opt_bars_1m:
            skipped.append(f"{trade_date} {symbol}: no option data for {leg.option_type}{leg.strike}")
            continue

        opt_entry_price = price_at_or_after(opt_bars_1m, entry_ts)
        opt_exit_price = price_at_or_before(opt_bars_1m, exit_ts)

        trades.append(Trade(
            date=trade_date, symbol=symbol, side=side, strike=leg.strike, opt_type=leg.option_type, lot=leg.lot,
            entry_ts=entry_ts, exit_ts=exit_ts,
            spot_entry_price=spot_entry_price, spot_exit_price=spot_exit_price, exit_reason=reason,
            opt_entry_price=opt_entry_price, opt_exit_price=opt_exit_price,
        ))

    return trades, skipped


def summarize_spot(trades):
    entered = [t for t in trades if t.spot_entry_price is not None]
    wins = [t for t in entered if t.spot_points > 0]
    losses = [t for t in entered if t.spot_points <= 0]
    total = sum(t.spot_points for t in entered)
    loss_sum = sum(t.spot_points for t in losses)
    pf = (sum(t.spot_points for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    print(f"SPOT VIEW:   entered={len(entered):2d}  win%={win_pct:5.1f}  PF={pf:6.2f}  "
          f"total={total:+9.2f} pts  avg={((total/len(entered)) if entered else 0):+7.2f}")


def summarize_option(trades):
    entered = [t for t in trades if t.option_pnl_rs is not None]
    wins = [t for t in entered if t.option_pnl_rs > 0]
    losses = [t for t in entered if t.option_pnl_rs <= 0]
    total = sum(t.option_pnl_rs for t in entered)
    loss_sum = sum(t.option_pnl_rs for t in losses)
    pf = (sum(t.option_pnl_rs for t in wins) / abs(loss_sum)) if loss_sum != 0 else (float("inf") if wins else 0.0)
    win_pct = (len(wins) / len(entered) * 100) if entered else 0.0
    print(f"OPTION VIEW: entered={len(entered):2d}  win%={win_pct:5.1f}  PF={pf:6.2f}  "
          f"total=Rs{total:+11,.2f}  avg=Rs{((total/len(entered)) if entered else 0):+9.2f}")


def daywise_summary(trades):
    """Per-day rollup: capital deployed (sum of entry premium x lot across
    every trade entered that day -- the most-capital view, i.e. what you'd
    need on hand if every signal that day were taken), P&L, and day return%."""
    by_day: Dict[str, list] = {}
    for t in trades:
        by_day.setdefault(t.date, []).append(t)
    rows = []
    for d in sorted(by_day.keys()):
        day_trades = by_day[d]
        priced = [t for t in day_trades if t.capital_invested is not None and t.option_pnl_rs is not None]
        capital = sum(t.capital_invested for t in priced)
        pnl = sum(t.option_pnl_rs for t in priced)
        wins = [t for t in priced if t.option_pnl_rs > 0]
        losses = [t for t in priced if t.option_pnl_rs <= 0]
        day_return_pct = (pnl / capital * 100.0) if capital else 0.0
        rows.append({
            "date": d, "trades": len(day_trades), "priced": len(priced),
            "capital": capital, "pnl": pnl, "return_pct": day_return_pct,
            "wins": len(wins), "losses": len(losses),
        })
    return rows


async def main():
    if not TOKEN:
        print("Set UPSTOX_TOKEN env var first.")
        return
    trades, skipped = await run_all()

    print("\n" + "=" * 150)
    print("SPOT-TRIGGERED ENTRY/EXIT, PRICED IN BOTH SPOT AND OPTION TERMS -- trade log")
    print("=" * 150)
    for t in sorted(trades, key=lambda x: (x.date, x.symbol)):
        sp = f"{t.spot_points:+8.2f}" if t.spot_points is not None else "    n/a "
        op = f"{t.option_points:+8.2f}" if t.option_points is not None else "    n/a "
        opnl = f"Rs{t.option_pnl_rs:+10,.2f}" if t.option_pnl_rs is not None else "n/a"
        oep = f"{t.opt_entry_price:8.2f}" if t.opt_entry_price is not None else "     n/a"
        oxp = f"{t.opt_exit_price:8.2f}" if t.opt_exit_price is not None else "     n/a"
        cap = f"Rs{t.capital_invested:10,.2f}" if t.capital_invested is not None else "n/a"
        ret = f"{t.return_pct:+7.2f}%" if t.return_pct is not None else "n/a"
        print(f"  {t.date} {t.symbol:<12} {t.side:<4} {t.opt_type}{t.strike:<7} lot={t.lot:<5} "
              f"SPOT entry={t.spot_entry_price:9.2f}@{t.entry_ts.strftime('%H:%M')} "
              f"exit={t.spot_exit_price:9.2f}@{t.exit_ts.strftime('%H:%M')} ({t.exit_reason}) pts={sp}  |  "
              f"OPT entry={oep} exit={oxp} pts={op} pnl={opnl} capital={cap} ret={ret}")

    if skipped:
        print("\n" + "=" * 150)
        print(f"SKIPPED ({len(skipped)}):")
        print("=" * 150)
        for s in skipped:
            print(f"  {s}")

    print("\n" + "=" * 150)
    print("PER-DAY CAPITAL DEPLOYED / P&L / RETURN%")
    print("=" * 150)
    day_rows = daywise_summary(trades)
    total_capital = sum(r["capital"] for r in day_rows)
    total_pnl = sum(r["pnl"] for r in day_rows)
    for r in day_rows:
        print(f"  {r['date']}  trades={r['trades']:2d} (priced={r['priced']:2d}, "
              f"win={r['wins']:2d}/loss={r['losses']:2d})  "
              f"capital=Rs{r['capital']:12,.2f}  pnl=Rs{r['pnl']:+11,.2f}  return={r['return_pct']:+7.2f}%")
    overall_return_pct = (total_pnl / total_capital * 100.0) if total_capital else 0.0
    print(f"\n  TOTAL capital deployed across {len(day_rows)} days: Rs{total_capital:,.2f}  "
          f"total pnl: Rs{total_pnl:+,.2f}  overall return: {overall_return_pct:+.2f}%")

    priced = [t for t in trades if t.return_pct is not None]
    win_returns = [t.return_pct for t in priced if t.option_pnl_rs > 0]
    loss_returns = [t.return_pct for t in priced if t.option_pnl_rs <= 0]
    avg_win_pct = (sum(win_returns) / len(win_returns)) if win_returns else 0.0
    avg_loss_pct = (sum(loss_returns) / len(loss_returns)) if loss_returns else 0.0
    print(f"  Average return on WINNING trades: {avg_win_pct:+.2f}%  ({len(win_returns)} trades)")
    print(f"  Average return on LOSING trades:  {avg_loss_pct:+.2f}%  ({len(loss_returns)} trades)")

    print("\n" + "=" * 150)
    print("SUMMARY")
    print("=" * 150)
    summarize_spot(trades)
    summarize_option(trades)
    print(f"\n{len(trades)} entered / {len(ROWS)} total rows / {len(skipped)} skipped")

    def _tr(t):
        return {
            "date": t.date, "symbol": t.symbol, "side": t.side, "opt_type": t.opt_type,
            "strike": t.strike, "lot": t.lot,
            "entry_ts": t.entry_ts.strftime("%H:%M") if t.entry_ts else None,
            "exit_ts": t.exit_ts.strftime("%H:%M") if t.exit_ts else None,
            "spot_entry_price": t.spot_entry_price, "spot_exit_price": t.spot_exit_price,
            "exit_reason": t.exit_reason, "spot_points": t.spot_points,
            "opt_entry_price": t.opt_entry_price, "opt_exit_price": t.opt_exit_price,
            "option_points": t.option_points, "option_pnl_rs": t.option_pnl_rs,
            "capital_invested": t.capital_invested, "return_pct": t.return_pct,
        }

    report = {
        "trades": [_tr(t) for t in sorted(trades, key=lambda x: (x.date, x.symbol))],
        "skipped": skipped,
        "daywise": day_rows,
        "totals": {
            "total_capital": total_capital, "total_pnl": total_pnl,
            "overall_return_pct": overall_return_pct,
            "avg_win_pct": avg_win_pct, "avg_loss_pct": avg_loss_pct,
            "n_wins": len(win_returns), "n_losses": len(loss_returns),
        },
    }
    import json as _json
    report_path = os.path.join("data", "oi_orb_spot_trigger_option_pricing_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        _json.dump(report, f, indent=2)
    print(f"\nFull JSON report written to {report_path}")


asyncio.run(main())
