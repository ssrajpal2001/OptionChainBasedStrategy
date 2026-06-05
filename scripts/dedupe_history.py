#!/usr/bin/env python3
"""One-time cleanup for sell-straddle trade history.

Before the fix in c2eae5d, every leg-close recorded the WHOLE pair, so a roll (and its cleanup,
and a physical roll's two leg-closes) wrote the SAME (ts, legs) record two+ times under different
reasons/pnl. This collapses those exact duplicates — keeping ONE record per (ts, instrument,
leg-signature) — and recomputes each kept record's totals from its legs so the numbers are
self-consistent. Every GENUINE, distinct trade is preserved.

SAFE: writes a `<file>.bak` backup before touching anything. Idempotent (re-running is a no-op).

Usage:
    python3 scripts/dedupe_history.py                      # all data/history/*.json
    python3 scripts/dedupe_history.py data/history/ssrajpal2001.json
"""
import glob
import json
import shutil
import sys


def _sig(rec):
    """Identity of a trade event: timestamp + instrument + the exact legs (side/strike/entry/exit).
    Two records with the same signature are the same event recorded twice."""
    legs = rec.get("legs") or []
    return (
        rec.get("ts"),
        rec.get("instrument"),
        tuple((l.get("side"), l.get("strike"), l.get("entry"), l.get("exit")) for l in legs),
    )


def dedupe(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    trades = data.get("trades", [])

    seen = set()
    out = []
    for r in trades:
        s = _sig(r)
        if s in seen:
            continue          # duplicate of an already-kept event → drop
        seen.add(s)
        # Recompute record totals from the legs so the row is self-consistent.
        legs = r.get("legs") or []
        if legs:
            r["pnl"] = round(sum(float(l.get("pnl", 0) or 0) for l in legs), 2)
            r["entry_price"] = round(sum(float(l.get("entry", 0) or 0) for l in legs), 2)
            r["exit_price"] = round(sum(float(l.get("exit", 0) or 0) for l in legs), 2)
        out.append(r)

    if len(out) != len(trades):
        shutil.copy(path, path + ".bak")     # backup ONLY when we actually change something
        data["trades"] = out
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    return len(trades), len(out)


def main():
    files = [a for a in sys.argv[1:] if not a.endswith(".bak")] or glob.glob("data/history/*.json")
    files = [f for f in files if not f.endswith(".bak")]
    if not files:
        print("No history files found under data/history/.")
        return
    for p in files:
        before, after = dedupe(p)
        if after != before:
            print(f"{p}: {before} -> {after} records  ({before - after} duplicates removed; backup at {p}.bak)")
        else:
            print(f"{p}: {before} records — no duplicates (unchanged).")


if __name__ == "__main__":
    main()
