"""
Batch scale-in backtest runner — all F&O stocks + indices.

Usage:
    python scripts/batch_scalein_backtest.py --token YOUR_UPSTOX_TOKEN [--days 21] [--sl-buf 20]

Output:
    - Prints a sorted summary table to stdout
    - Saves detailed results to data/batch_backtest_YYYYMMDD.json
    - Saves summary CSV to data/batch_backtest_YYYYMMDD.csv
"""
import argparse
import json
import csv
import sys
import os
import time
from datetime import date

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── All symbols to run ──────────────────────────────────────────────────────
# Indices (weekly expiry)
_INDICES = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]

# F&O stocks — from nifty_scalein_backtest._STOCK_CONFIG (monthly expiry)
_STOCKS = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "WIPRO",
    "BHARTIARTL", "HINDUNILVR", "BAJFINANCE", "KOTAKBANK", "AXISBANK",
    "MARUTI", "TITAN", "SUNPHARMA", "HCLTECH", "ITC", "M&M", "TATASTEEL",
    "TATAMOTORS", "ULTRACEMCO", "NTPC", "TECHM", "ADANIPORTS", "POWERGRID",
    "ONGC", "COALINDIA", "JSWSTEEL", "GRASIM", "ASIANPAINT", "LT",
    "BAJAJ-AUTO", "HEROMOTOCO", "DIVISLAB", "BPCL", "CIPLA", "INDUSINDBANK",
    "DRREDDY", "BRITANNIA", "APOLLOHOSP", "PIDILITIND", "TATACONSUM",
    "NESTLEIND", "EICHERMOT", "TRENT", "ZOMATO", "ADANIENT", "VEDL",
    "SAIL", "PNB", "BANKBARODA", "CANBK", "HAL", "BEL", "BHEL", "NMDC",
    "IDFCFIRSTB", "IRFC", "IRCTC", "DMART", "NYKAA", "PAYTM", "POLICYBZR",
    "DELHIVERY", "MCDOWELL-N", "GODREJCP", "MARICO", "BERGEPAINT", "COLPAL",
    "BIOCON", "GLENMARK", "LUPIN", "AUROPHARMA", "TORNTPHARM", "LICHSGFIN",
    "RECLTD", "PFC", "HINDPETRO", "IOC", "GAIL", "CONCOR", "IGL", "MGL",
    "PETRONET", "APOLLOTYRE", "MRF", "BALKRISIND", "TIINDIA", "EXIDEIND",
    "DLF", "GODREJPROP", "OBEROIRLTY", "PRESTIGE", "ABBOTINDIA", "ALKEM",
    "PERSISTENT", "LTIM", "COFORGE", "MPHASIS", "OFSS", "KPITTECH", "LTTS",
]


def _run_one(sym: str, token: str, days: int, sl_buf: float) -> dict:
    """Run backtest for one symbol. Returns summary dict."""
    from scripts.nifty_scalein_backtest import run_scalein_backtest
    try:
        result = run_scalein_backtest(
            access_token=token,
            days=days,
            start="",
            end="",
            expiry_str="",
            index=sym,
            sl_buf_override=sl_buf,
            csv_path="",
        )
        return result
    except Exception as exc:
        return {"ok": False, "error": str(exc), "symbol": sym}


def _fmt_pnl(v) -> str:
    if v is None:
        return "  -"
    return f"{v:+,.0f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token", required=True)
    ap.add_argument("--days", type=int, default=21)
    ap.add_argument("--sl-buf", type=float, default=20.0)
    ap.add_argument("--only-stocks", action="store_true", help="Skip indices")
    ap.add_argument("--only-indices", action="store_true", help="Skip stocks")
    ap.add_argument("--symbol", default="", help="Run a single symbol only")
    args = ap.parse_args()

    if args.symbol:
        symbols = [args.symbol.upper()]
    elif args.only_stocks:
        symbols = _STOCKS
    elif args.only_indices:
        symbols = _INDICES
    else:
        symbols = _INDICES + _STOCKS

    today_str = date.today().strftime("%Y%m%d")
    out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    os.makedirs(out_dir, exist_ok=True)
    json_path = os.path.join(out_dir, f"batch_backtest_{today_str}.json")
    csv_path  = os.path.join(out_dir, f"batch_backtest_{today_str}.csv")

    all_results = []
    errors = []
    total = len(symbols)

    print(f"\nBatch Scale-In Backtest  |  {total} symbols  |  days={args.days}  |  sl_buf={args.sl_buf}")
    print("=" * 70)

    for i, sym in enumerate(symbols, 1):
        print(f"\n[{i}/{total}] {sym} ...", flush=True)
        t0 = time.time()
        res = _run_one(sym, args.token, args.days, args.sl_buf)
        elapsed = time.time() - t0

        if not res.get("ok"):
            err = res.get("error", "unknown error")
            print(f"  ERROR: {err}")
            errors.append({"symbol": sym, "error": err})
            all_results.append({
                "symbol": sym, "type": "INDEX" if sym in _INDICES else "STOCK",
                "ok": False, "error": err,
            })
            continue

        summary = res.get("summary", {})
        trades  = res.get("trades", [])
        n       = summary.get("total_trades", len(trades))
        wins    = summary.get("wins", 0)
        pnl     = summary.get("total_pnl_rs", 0)
        pf      = summary.get("profit_factor", 0)
        wr      = round(wins / n * 100, 1) if n > 0 else 0

        print(f"  trades={n}  wins={wins}  P&L=Rs {pnl:+,.0f}  WR={wr}%  PF={pf}  ({elapsed:.1f}s)")

        all_results.append({
            "symbol":       sym,
            "type":         "INDEX" if sym in _INDICES else "STOCK",
            "ok":           True,
            "trades":       n,
            "wins":         wins,
            "losses":       n - wins,
            "win_rate_pct": wr,
            "total_pnl_rs": pnl,
            "profit_factor": pf,
            "avg_win":      summary.get("avg_win", 0),
            "avg_loss":     summary.get("avg_loss", 0),
            "lot_size":     res.get("lot_size", "-"),
            "days_run":     res.get("days_run", args.days),
        })

        # Brief pause to avoid hitting Upstox rate limits
        time.sleep(0.5)

    # ── Save JSON ──────────────────────────────────────────────────────────
    with open(json_path, "w") as f:
        json.dump({"date": today_str, "days": args.days, "sl_buf": args.sl_buf,
                   "results": all_results, "errors": errors}, f, indent=2)
    print(f"\nDetailed results saved: {json_path}")

    # ── Save CSV ──────────────────────────────────────────────────────────
    ok_results = [r for r in all_results if r["ok"]]
    if ok_results:
        fields = ["symbol","type","trades","wins","losses","win_rate_pct",
                  "total_pnl_rs","profit_factor","avg_win","avg_loss","lot_size"]
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(sorted(ok_results, key=lambda x: x["total_pnl_rs"], reverse=True))
        print(f"Summary CSV saved:      {csv_path}")

    # ── Print summary table ────────────────────────────────────────────────
    print("\n\n" + "=" * 90)
    print(f"{'Symbol':<15} {'Type':<7} {'Trades':>6} {'WR%':>6} {'P&L (Rs)':>12} {'PF':>6} {'Lot':>6}")
    print("-" * 90)

    sorted_res = sorted(ok_results, key=lambda x: x["total_pnl_rs"], reverse=True)
    for r in sorted_res:
        sym   = r["symbol"]
        stype = r["type"]
        n     = r["trades"]
        wr    = r["win_rate_pct"]
        pnl   = r["total_pnl_rs"]
        pf    = r["profit_factor"]
        lot   = r["lot_size"]
        marker = " <<" if pnl > 5000 else (" >>" if pnl < -2000 else "")
        print(f"{sym:<15} {stype:<7} {n:>6} {wr:>5.1f}% {pnl:>+12,.0f} {pf:>6.2f} {str(lot):>6}{marker}")

    print("-" * 90)
    total_pnl = sum(r["total_pnl_rs"] for r in ok_results)
    total_trades = sum(r["trades"] for r in ok_results)
    total_wins   = sum(r["wins"] for r in ok_results)
    overall_wr   = round(total_wins / total_trades * 100, 1) if total_trades > 0 else 0
    print(f"{'TOTAL':<15} {'':7} {total_trades:>6} {overall_wr:>5.1f}% {total_pnl:>+12,.0f}")
    print("=" * 90)

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for e in errors:
            print(f"  {e['symbol']}: {e['error']}")

    print(f"\nDone. Results: {csv_path}")


if __name__ == "__main__":
    main()
