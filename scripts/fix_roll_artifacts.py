#!/usr/bin/env python3
"""One-time cleanup: remove the pre-c2eae5d "both-legs" recording artifact from trade history.

Old records logged BOTH legs on every exit, even on a single-side roll. So a KEPT leg shows a
phantom close (in the roll record) and a phantom reopen (in the next record). This reconstructs the
true per-leg lots:

  For each pair of consecutive records A (earlier) and B (later) that form a CONTINUOUS roll
  (A.ts == B's leg entry_ts), any leg with the SAME (side, strike) in both was KEPT, not closed —
  so we DROP it from A and propagate its real open time onto B's matching leg. Repeated down a
  chain, a leg kept across N rolls ends up recorded ONCE: opened at its first entry, closed at its
  real exit. Records left with no legs are removed; remaining record totals are recomputed.

Physical rolls (both strikes change) and genuine full closes keep both legs (nothing matches).

SAFE: writes <file>.bak first. Idempotent (re-running finds nothing more to merge).

Usage:
    python3 scripts/fix_roll_artifacts.py                 # all data/history/*.json
    python3 scripts/fix_roll_artifacts.py data/history/ssrajpal2001.json
"""
import glob
import json
import shutil
import sys


def fix(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    trades = sorted(data.get("trades", []), key=lambda t: t.get("ts", ""))

    removed = 0
    for i in range(len(trades) - 1):
        A, B = trades[i], trades[i + 1]
        a_exit = A.get("ts")
        b_entries = [l.get("entry_ts") for l in (B.get("legs") or []) if l.get("entry_ts")]
        b_entry = min(b_entries) if b_entries else None
        if not (a_exit and b_entry and a_exit == b_entry):
            continue   # not a continuous roll → both A legs really closed here
        b_by_key = {(l.get("side"), l.get("strike")): l for l in (B.get("legs") or [])}
        keep = []
        for legA in A.get("legs") or []:
            kb = b_by_key.get((legA.get("side"), legA.get("strike")))
            if kb is not None:
                # KEPT leg — propagate A's true open onto B, drop the phantom close from A.
                if legA.get("entry_ts"):
                    kb["entry_ts"] = legA["entry_ts"]
                removed += 1
            else:
                keep.append(legA)   # this leg actually rolled out → real close
        A["legs"] = keep

    out = []
    for t in trades:
        legs = t.get("legs") or []
        if not legs:
            continue   # record was purely a kept-leg phantom → drop
        t["entry_price"] = round(sum(float(l.get("entry", 0) or 0) for l in legs), 2)
        t["exit_price"] = round(sum(float(l.get("exit", 0) or 0) for l in legs), 2)
        t["pnl"] = round(sum(float(l.get("pnl", 0) or 0) for l in legs), 2)
        out.append(t)

    if removed:
        shutil.copy(path, path + ".bak")
        data["trades"] = out
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    return len(trades), len(out), removed


def main():
    files = [a for a in sys.argv[1:] if not a.endswith(".bak")] or \
            [p for p in glob.glob("data/history/*.json") if not p.endswith(".bak")]
    if not files:
        print("No history files found.")
        return
    for p in files:
        before, after, removed = fix(p)
        if removed:
            print(f"{p}: removed {removed} phantom kept-leg rows; {before} -> {after} records "
                  f"(backup {p}.bak)")
        else:
            print(f"{p}: clean — no artifacts (unchanged)")


if __name__ == "__main__":
    main()
