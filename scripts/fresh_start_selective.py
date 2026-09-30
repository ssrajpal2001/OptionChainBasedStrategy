"""
scripts/fresh_start_selective.py — wipe ONLY sell_straddle + the OI scanner
strategies (oi_orb_screener, oi_orb_screener_top20, oi_bias_rsi_exit) clean for
today, leaving Iron Fly and CAG Straddle completely untouched.

Direct user spec (2026-09-30): "I want OI scanner and Sell Straddle start
fresh with no prev position and prev log. Iron Fly don't touch it is working
perfectly." All strategies currently run inside the SAME pm2 process
(`terminus`), so the process itself has to restart either way -- but Iron
Fly's and CAG Straddle's own state is never deleted, so they resume exactly
where they left off, same as any ordinary restart.

What this wipes:
  - data/positions/*sell_straddle*.json   (open position + pool/session state)
  - logs/clients/ss_*.log                 (sell_straddle's own per-binding logs)
  - data/oi_orb_screener.db               (whole DB -- fully isolated, own file)
  - data/oi_bias_rsi_exit.db              (whole DB -- fully isolated, own file)
  - logs/clients/oiorb_*.log, logs/clients/oibiasrsi_*.log
  - data/history/<client>.json trade-history entries whose "strategy" starts
    with sell_straddle/oi_orb_screener/oi_bias_rsi_exit -- entries for any
    OTHER strategy (iron_fly, cag_straddle) in that same per-client file are
    kept untouched (the file is genuinely shared across strategies for one
    client, so this can't be a blunt rm).

What this NEVER touches:
  - data/positions/*iron_fly*.json, *cag_straddle*.json
  - logs/clients/ironfly_*.log (or whatever its own prefix is), cagstraddle_*.log
  - data/clients.db, data/strategy_config.json, config/client_profiles.json
  - Any trade_history.py entry NOT belonging to the wiped strategies.

Usage (run from the repo root, on EC2, BEFORE restarting pm2):
    python3 scripts/fresh_start_selective.py
    python3 scripts/fresh_start_selective.py --dry-run   # preview only, deletes nothing
"""
from __future__ import annotations

import argparse
import glob
import json
import os

_WIPED_STRATEGY_PREFIXES = ("sell_straddle", "oi_orb_screener", "oi_bias_rsi_exit")


def _wipe_glob(pattern: str, dry_run: bool) -> int:
    n = 0
    for path in glob.glob(pattern):
        n += 1
        print(f"  {'[dry-run] would delete' if dry_run else 'deleting'}: {path}")
        if not dry_run:
            os.remove(path)
    return n


def _filter_trade_history(dry_run: bool) -> None:
    hist_dir = os.path.join("data", "history")
    if not os.path.isdir(hist_dir):
        return
    for fname in os.listdir(hist_dir):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(hist_dir, fname)
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as exc:
            print(f"  skip {path}: unreadable ({exc})")
            continue
        trades = data.get("trades", [])
        kept = [t for t in trades
                if not str(t.get("strategy", "")).startswith(_WIPED_STRATEGY_PREFIXES)]
        removed = len(trades) - len(kept)
        if removed == 0:
            continue
        print(f"  {path}: removing {removed} sell_straddle/oi_* records, "
              f"keeping {len(kept)} other-strategy records (e.g. iron_fly/cag_straddle)")
        if not dry_run:
            data["trades"] = kept
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, path)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    print("== fresh_start_selective: sell_straddle + OI scanner only (Iron Fly/CAG untouched) ==")

    print("\n[1/4] sell_straddle position/session/pool state:")
    _wipe_glob(os.path.join("data", "positions", "*sell_straddle*.json"), args.dry_run)

    print("\n[2/4] sell_straddle logs:")
    _wipe_glob(os.path.join("logs", "clients", "ss_*.log*"), args.dry_run)

    print("\n[3/4] OI scanner databases + logs:")
    for db in ("data/oi_orb_screener.db", "data/oi_bias_rsi_exit.db"):
        if os.path.exists(db):
            print(f"  {'[dry-run] would delete' if args.dry_run else 'deleting'}: {db}")
            if not args.dry_run:
                os.remove(db)
    _wipe_glob(os.path.join("logs", "clients", "oiorb_*.log*"), args.dry_run)
    _wipe_glob(os.path.join("logs", "clients", "oibiasrsi_*.log*"), args.dry_run)

    print("\n[4/4] trade_history.py per-client history (filtered, not deleted):")
    _filter_trade_history(args.dry_run)

    print("\nDone." if not args.dry_run else "\nDry-run complete -- nothing was deleted.")
    print("Iron Fly and CAG Straddle files were never touched.")
    print("\nNext: pm2 restart terminus   (all strategies share one process --")
    print("      Iron Fly/CAG Straddle will reload their own untouched state normally)")


if __name__ == "__main__":
    main()
