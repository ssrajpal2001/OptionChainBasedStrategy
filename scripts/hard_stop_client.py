"""
One-off: HARD STOP a single client's strategy deployments -- run-toggle OFF
only, NEVER a square-off/liquidate call.

Why this exists: every existing "stop" path in this codebase (Terminal OFF,
Kill Broker, /api/client/stop_squareoff) squares off open legs before/while
stopping. gurmeet explicitly asked for the opposite on 2026-08-04: stop the
application from touching his account (no fresh trades, no further exit
attempts either -- exits are currently the broken part), but do NOT let the
app attempt to close anything at the broker. He will square off manually
himself once he's confirmed his real Zerodha position.

The risk this script guards against: strategy_deployments.is_running is read
by each StrategyBookManager's reconcile loop (strategies/core/book_manager.py
_reconcile, ~line 190). When a deployment's key drops out of `_wanted()`
(is_running -> 0) AND the manager's in-memory book still believes it holds an
open position (`not self._is_flat(book)`), the manager itself schedules
`_liquidate_book()` -- a real (attempted) broker close -- within the next
~5s reconcile tick. Just flipping is_running=0 blindly is therefore NOT a
safe "no square-off" hard stop by itself.

This script only flips is_running=0 for a deployment when it can positively
confirm -- from the on-disk position_store file (data/positions/<key>.json,
data_layer/position_store.py) -- that the strategy currently believes it is
FLAT (no open position). Position-store keys follow
"<client_id>_<binding_id>_<underlying>_<strategy_name>" for every strategy
that persists to file (sell_straddle, d1_trap_bear_only, v4_cascade /
d1_trap_fno / d1_trap_index). Strategies that do NOT persist to a position
file (fvg, fno_positional as of 2026-08) cannot be verified this way -- those
rows are always SKIPPED and reported, never auto-toggled, so this script
never risks tripping the liquidate path for strategies it can't verify.

Usage (on EC2, repo root):
    python3 scripts/hard_stop_client.py gurmeet          # dry-run, shows what it WOULD do
    python3 scripts/hard_stop_client.py gurmeet --apply  # actually flips is_running=0

Only deployments confirmed flat are ever touched. Anything skipped is
printed with the reason -- review those manually.
"""
import os
import sqlite3
import sys

DB_PATH = "data/clients.db"
POSITIONS_DIR = "data/positions"

# Strategies known to persist an open position to data/positions/<key>.json.
# Only these can be verified flat/non-flat from disk.
FILE_PERSISTED_STRATEGIES = {"sell_straddle", "d1_trap_bear_only", "v4_cascade",
                              "d1_trap_fno", "d1_trap_index"}


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/hard_stop_client.py <client_id> [--apply]")
        sys.exit(1)
    client_id = sys.argv[1]
    apply = "--apply" in sys.argv[2:]

    if not os.path.exists(DB_PATH):
        print(f"ERROR: {DB_PATH} not found -- run this from the repo root on EC2.")
        sys.exit(1)

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute(
        "SELECT deploy_id, client_id, binding_id, strategy_name, underlying, is_running "
        "FROM strategy_deployments WHERE client_id=? AND is_active=1",
        (client_id,),
    ).fetchall()]

    if not rows:
        print(f"No active deployments found for client_id='{client_id}'.")
        con.close()
        return

    to_stop = []
    skipped = []

    for d in rows:
        if not d["is_running"]:
            skipped.append((d, "already stopped (is_running=0)"))
            continue
        strat = d["strategy_name"]
        if strat not in FILE_PERSISTED_STRATEGIES:
            skipped.append((d, f"strategy '{strat}' has no position-store file -- "
                                f"cannot verify flat from here, confirm manually"))
            continue
        key = f"{d['client_id']}_{d['binding_id']}_{d['underlying']}_{strat}"
        pos_file = os.path.join(POSITIONS_DIR, f"{key}.json")
        if os.path.exists(pos_file):
            skipped.append((d, f"OPEN position file exists ({pos_file}) -- "
                                f"toggling is_running=0 now would let the manager's "
                                f"reconcile loop attempt a real liquidate/square-off "
                                f"within ~5s. NOT touched."))
            continue
        to_stop.append(d)

    print(f"=== client_id={client_id} ===")
    print(f"\n{len(to_stop)} deployment(s) confirmed FLAT -- safe hard stop:")
    for d in to_stop:
        print(f"  - {d['strategy_name']} / {d['underlying']} / binding={d['binding_id']} "
              f"(deploy_id={d['deploy_id']})")

    print(f"\n{len(skipped)} deployment(s) SKIPPED (not touched):")
    for d, reason in skipped:
        print(f"  - {d['strategy_name']} / {d['underlying']} / binding={d['binding_id']}: {reason}")

    if not to_stop:
        print("\nNothing to apply.")
        con.close()
        return

    if not apply:
        print("\nDry run only -- no changes made. Re-run with --apply to write these.")
        con.close()
        return

    for d in to_stop:
        con.execute(
            "UPDATE strategy_deployments SET is_running=0 WHERE deploy_id=? AND client_id=?",
            (d["deploy_id"], client_id),
        )
    con.commit()
    con.close()
    print(f"\nApplied: {len(to_stop)} deployment(s) set is_running=0 for '{client_id}'. "
          f"Effective within ~5s (next reconcile tick) -- no restart needed. "
          f"'{client_id}' will receive NO fresh trades and books already stopped won't be "
          f"re-managed until you turn them back on. ssrajpal2001 untouched.")


if __name__ == "__main__":
    main()
