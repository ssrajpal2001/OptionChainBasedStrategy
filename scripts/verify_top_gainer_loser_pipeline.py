"""
scripts/verify_top_gainer_loser_pipeline.py

Standalone verification -- runs the new standalone "top gainer/loser"
data pipeline (strategies/oi_orb_screener/screener.py's poll_top_gainers_
losers) ONCE against real, live NSE data and prints the full result so it
can be independently cross-checked by hand against the real NSE website
(e.g. https://www.nseindia.com/market-data/top-gainers-stocks or the
equity-market-watch F&O universe page) and the OI-Spurts page.

Verify-only -- does NOT touch any entry/exit/shortlist/position state,
does NOT write anything except (optionally) this script's own printed
output. No DB writes here by default (pass --record to also exercise
store.record_top_gainer_loser_poll against a throwaway trade_date, if you
want to see the persistence path exercised against real data too).

MUST run with live network access to NSE (nseindia.com) -- this uses
strategies.oi_orb_screener.screener.NSESession directly, the same real
HTTP client build_shortlist/poll_oi_rank already use. No token/credential
needed (NSE's public endpoints, unlike Upstox, don't require one) -- but
NSE does rate-limit/Akamai-throttle aggressively, so a single run is
deliberately all this script does.

Usage: python scripts/verify_top_gainer_loser_pipeline.py [--record]
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

sys.path.insert(0, ".")

from config.global_config import IST
from strategies.oi_orb_screener import screener


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=10,
                         help="gainers/losers per side (default 10, matches TOP_GAINER_LOSER_N)")
    parser.add_argument("--oi-spurt-min", type=float, default=7.0)
    parser.add_argument("--pchange-max", type=float, default=4.0)
    parser.add_argument("--record", action="store_true",
                         help="also write this real poll to store.py, trade_date=VERIFY-<today>")
    args = parser.parse_args()

    print("=" * 110)
    print("TOP GAINER/LOSER PIPELINE -- real NSE data, single verification poll")
    print(f"Run at: {datetime.now(IST).isoformat()}")
    print(f"Params: top_n={args.top_n}/side  oi_spurt_min={args.oi_spurt_min}%  "
          f"pchange_max={args.pchange_max}% (upper bound)")
    print("=" * 110)

    print("\nConnecting to NSE (cookie warm-up)...")
    nse = screener.NSESession()

    cfg = dict(screener.CONFIG)
    cfg["TOP_GAINER_LOSER_N"] = args.top_n
    cfg["TOP_GAINER_LOSER_OI_SPURT_MIN_PCT"] = args.oi_spurt_min
    cfg["TOP_GAINER_LOSER_PCHANGE_MAX_PCT"] = args.pchange_max

    print("Fetching real F&O price universe (Step 1 source) + real NSE OI-Spurts (Step 2)...")
    candidates, qualifying = screener.poll_top_gainers_losers(nse, cfg)

    if candidates.empty:
        print("\nNo candidates returned -- NSE fetch may have failed/been throttled. See warnings above.")
        return

    print(f"\n{'=' * 110}\nSTEP 1 -- top {args.top_n} real GAINERS by pChange\n{'=' * 110}")
    print(f"{'symbol':16s} {'pChange%':>10s} {'rank':>5s} {'OI-spurt%':>10s} {'qualifies?':>12s}")
    gainers = candidates[candidates["rank_type"] == "gainer"].sort_values("rank")
    qualifying_symbols = set(qualifying["symbol"]) if not qualifying.empty else set()
    for _, r in gainers.iterrows():
        oi = r.get("oi_spurt_pct")
        oi_s = f"{oi:.2f}" if oi == oi else "n/a"   # NaN check
        print(f"{r['symbol']:16s} {r['pChange']:>10.2f} {int(r['rank']):>5d} {oi_s:>10s} "
              f"{'YES' if r['symbol'] in qualifying_symbols else 'no':>12s}")

    print(f"\n{'=' * 110}\nSTEP 1 -- top {args.top_n} real LOSERS by pChange\n{'=' * 110}")
    print(f"{'symbol':16s} {'pChange%':>10s} {'rank':>5s} {'OI-spurt%':>10s} {'qualifies?':>12s}")
    losers = candidates[candidates["rank_type"] == "loser"].sort_values("rank")
    for _, r in losers.iterrows():
        oi = r.get("oi_spurt_pct")
        oi_s = f"{oi:.2f}" if oi == oi else "n/a"
        print(f"{r['symbol']:16s} {r['pChange']:>10.2f} {int(r['rank']):>5d} {oi_s:>10s} "
              f"{'YES' if r['symbol'] in qualifying_symbols else 'no':>12s}")

    print(f"\n{'=' * 110}\nFINAL QUALIFYING LIST (Steps 3+4: OI-spurt > {args.oi_spurt_min}% AND "
          f"|pChange| < {args.pchange_max}%)\n{'=' * 110}")
    if qualifying.empty:
        print("(none qualify this poll)")
    else:
        for _, r in qualifying.sort_values("oi_spurt_pct", ascending=False).iterrows():
            print(f"  {r['symbol']:16s} {r['rank_type']:8s} pChange={r['pChange']:+.2f}%  "
                  f"oi_spurt={r['oi_spurt_pct']:.2f}%")

    print(f"\nTotal candidates: {len(candidates)}  |  Qualifying: {len(qualifying)}")

    if args.record:
        from strategies.oi_orb_screener import store
        trade_date = f"VERIFY-{datetime.now(IST).date().isoformat()}"
        poll_ts = datetime.now(IST).isoformat(timespec="seconds")
        rows = []
        for _, r in candidates.iterrows():
            oi = r.get("oi_spurt_pct")
            rows.append({
                "symbol": r["symbol"], "rank_type": r["rank_type"], "rank": int(r["rank"]),
                "price_change_pct": float(r["pChange"]), "oi_spurt_pct": float(oi) if oi == oi else None,
                "qualified": r["symbol"] in qualifying_symbols,
            })
        store.record_top_gainer_loser_poll("VERIFY", "VERIFY", poll_ts, rows, trade_date=trade_date)
        print(f"\nRecorded {len(rows)} rows to data/oi_orb_screener.db "
              f"(client_id=binding_id='VERIFY', trade_date='{trade_date}') for inspection.")

    print("\n" + "=" * 110)
    print("Cross-check by hand: NSE's own top-gainers/top-losers pages + OI-Spurts page, same moment.")
    print("=" * 110)


if __name__ == "__main__":
    main()
