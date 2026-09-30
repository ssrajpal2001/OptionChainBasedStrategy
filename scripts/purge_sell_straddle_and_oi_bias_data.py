"""scripts/purge_sell_straddle_and_oi_bias_data.py

Direct user request (2026-09-30, after-market-close review): delete all logs,
open-position state, and trade history for SellStraddle and the OI-Bias-RSI-Exit
scanner ("oi scanner") so both strategies start completely fresh tomorrow --
while leaving Iron Fly (currently holding a real open position) untouched.

Touches, and ONLY touches, data belonging to these two strategies:

  1. Per-binding rotating log files under logs/clients/:
       ss_*.log*          (SellStraddle -- ss_{UND}_{client}_{binding}_{strategy}_{date}.log[.N])
       oibiasrsi_*.log*   (OI-Bias-RSI-Exit)

  2. Open-position JSON files under data/positions/ whose key ends with the
     sell_straddle family (base position, _session, _session_carry, _pool,
     _pool_carry -- see strategies/sell_straddle/engine.py's _persist_key
     property for the exact suffix set). OI-Bias-RSI-Exit does not use
     position_store at all (it tracks open positions purely in its own SQLite
     DB, deleted in full in step 4), so nothing to remove here for it.

  3. Closed-trade history: data/history/<client_id>.json is a PER-CLIENT file
     shared across every strategy that client runs (confirmed via
     data_layer/trade_history.py -- one file, a flat "trades" list, each
     record carrying its own "strategy" field). A client running Iron Fly
     AND SellStraddle/OI-Bias-RSI-Exit would have all three mixed in the same
     file, so this does NOT delete the file -- it filters out only the
     records whose strategy is sell_straddle / sell_straddle_calc_vwap /
     oi_bias_rsi_exit, and rewrites the file with everything else (iron_fly
     and any other strategy) kept byte-identical.

  4. OI-Bias-RSI-Exit's own dedicated SQLite DB: data/oi_bias_rsi_exit.db.
     This file belongs exclusively to this strategy (confirmed via
     strategies/oi_bias_rsi_exit/store.py's _DB_PATH -- Iron Fly never reads
     or writes it), so it is deleted outright rather than filtered.

Never touches: anything named/keyed "iron_fly", data/clients.db (broker
credentials + strategy_deployments -- deployment config is not "logs/
position/trade" data and must survive so both strategies redeploy correctly
tomorrow), or any other strategy's data.

Usage:
    python scripts/purge_sell_straddle_and_oi_bias_data.py --dry-run   # preview only
    python scripts/purge_sell_straddle_and_oi_bias_data.py             # actually delete
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LOGS_DIR = os.path.join(REPO_ROOT, "logs", "clients")
POSITIONS_DIR = os.path.join(REPO_ROOT, "data", "positions")
HISTORY_DIR = os.path.join(REPO_ROOT, "data", "history")
OI_BIAS_DB = os.path.join(REPO_ROOT, "data", "oi_bias_rsi_exit.db")

LOG_PREFIXES = ("ss_", "oibiasrsi_")

# Matches: <anything>_sell_straddle.json, <anything>_sell_straddle_calc_vwap.json,
# and either with a _session / _session_carry / _pool / _pool_carry suffix.
POSITION_KEY_RE = re.compile(
    r".*_sell_straddle(_calc_vwap)?(_session|_session_carry|_pool|_pool_carry)?\.json$"
)

HISTORY_STRATEGIES_TO_PURGE = {"sell_straddle", "sell_straddle_calc_vwap", "oi_bias_rsi_exit"}


def find_log_files() -> list:
    if not os.path.isdir(LOGS_DIR):
        return []
    out = []
    for name in os.listdir(LOGS_DIR):
        if name.startswith(LOG_PREFIXES):
            out.append(os.path.join(LOGS_DIR, name))
    return sorted(out)


def find_position_files() -> list:
    if not os.path.isdir(POSITIONS_DIR):
        return []
    out = []
    for name in os.listdir(POSITIONS_DIR):
        if "iron_fly" in name:
            continue  # never touch Iron Fly, even if a future rename made it match
        if POSITION_KEY_RE.match(name):
            out.append(os.path.join(POSITIONS_DIR, name))
    return sorted(out)


def find_history_files() -> list:
    if not os.path.isdir(HISTORY_DIR):
        return []
    return sorted(
        os.path.join(HISTORY_DIR, name)
        for name in os.listdir(HISTORY_DIR)
        if name.endswith(".json")
    )


def filter_history_file(path: str, dry_run: bool) -> int:
    """Rewrites one per-client history file, dropping only records whose
    "strategy" is in HISTORY_STRATEGIES_TO_PURGE. Returns how many records
    were removed. Iron Fly (and any other strategy's) records in the same
    file are preserved untouched."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"  [SKIP] {path}: could not read/parse ({exc})")
        return 0

    trades = data.get("trades", [])
    if not isinstance(trades, list):
        print(f"  [SKIP] {path}: unexpected shape (no 'trades' list)")
        return 0

    kept = [t for t in trades if t.get("strategy") not in HISTORY_STRATEGIES_TO_PURGE]
    removed = len(trades) - len(kept)
    if removed == 0:
        return 0

    print(f"  {os.path.basename(path)}: removing {removed} record(s), keeping {len(kept)} "
          f"(other strategies, e.g. iron_fly, untouched)")
    if not dry_run:
        data["trades"] = kept
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    return removed


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                     help="Preview what would be deleted/modified without touching anything.")
    args = ap.parse_args()

    print(f"{'DRY RUN — ' if args.dry_run else ''}Purging SellStraddle + OI-Bias-RSI-Exit "
          f"logs/positions/trade-history. Iron Fly is never touched.\n")

    # 1. Logs
    log_files = find_log_files()
    print(f"[1/4] Log files ({len(log_files)} matched under {LOGS_DIR}):")
    for p in log_files:
        print(f"  DELETE {p}")
        if not args.dry_run:
            os.remove(p)

    # 2. Position-store JSON files
    pos_files = find_position_files()
    print(f"\n[2/4] Position-store files ({len(pos_files)} matched under {POSITIONS_DIR}):")
    for p in pos_files:
        print(f"  DELETE {p}")
        if not args.dry_run:
            os.remove(p)

    # 3. Trade history (per-client, filtered in place)
    hist_files = find_history_files()
    print(f"\n[3/4] Trade history ({len(hist_files)} per-client file(s) under {HISTORY_DIR}, "
          f"filtered in place -- iron_fly records kept):")
    total_removed = 0
    for p in hist_files:
        total_removed += filter_history_file(p, args.dry_run)
    if total_removed == 0:
        print("  (no matching sell_straddle/oi_bias_rsi_exit records found)")

    # 4. OI-Bias-RSI-Exit's own dedicated DB
    print(f"\n[4/4] OI-Bias-RSI-Exit dedicated DB:")
    if os.path.exists(OI_BIAS_DB):
        print(f"  DELETE {OI_BIAS_DB}")
        if not args.dry_run:
            os.remove(OI_BIAS_DB)
    else:
        print(f"  (not found: {OI_BIAS_DB})")

    print("\nDone." if not args.dry_run else "\nDry run complete -- nothing was actually deleted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
