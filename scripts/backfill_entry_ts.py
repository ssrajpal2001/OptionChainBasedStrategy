#!/usr/bin/env python3
"""Backfill per-leg entry_ts / exit_ts into trade history from the per-client-broker trade logs.

History recorded before the timestamp fix has no leg open/close time, so the UI shows '—'. The
times DO exist in logs/trades/{client}-{binding}-{date}.log:
    2026-06-05 17:08:06 | ENTRY | CRUDEOIL | ATM=8800 | CE=8800@381.40 | PE=8800@347.20 | ...
    2026-06-05 17:10:02 | EXIT  | CRUDEOIL | ATM=8800 | CE=8800 381.40->383.10 | PE=8800 347.20->343.70 | ...
This matches each history leg by (instrument, side, strike, price) and fills its entry_ts/exit_ts.

SAFE: writes a `<file>.bak` backup before changing anything. Only fills MISSING timestamps.

Usage:
    python3 scripts/backfill_entry_ts.py                 # all data/history/*.json
    python3 scripts/backfill_entry_ts.py ssrajpal2001    # one client id
"""
import glob
import json
import os
import re
import shutil
import sys

_ENTRY = re.compile(
    r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)\s*\|\s*ENTRY\s*\|\s*(\w+)\s*\|\s*ATM=[\d.]+\s*\|\s*"
    r"CE=(\d+)@([\d.]+)\s*\|\s*PE=(\d+)@([\d.]+)")
_EXIT = re.compile(
    r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)\s*\|\s*EXIT\s*\|\s*(\w+)\s*\|\s*ATM=[\d.]+\s*\|\s*"
    r"CE=(\d+)\s+[\d.]+\D+([\d.]+)\s*\|\s*PE=(\d+)\s+[\d.]+\D+([\d.]+)")


def _iso(ts: str) -> str:
    return ts.replace(" ", "T")          # "2026-06-05 17:08:06" -> "2026-06-05T17:08:06"


def _key(inst, side, strike, price):
    return (str(inst), str(side), int(strike), round(float(price), 2))


def build_maps(client_id):
    """entry_map / exit_map: (instrument, side, strike, price) -> first ISO timestamp seen."""
    entry_map, exit_map = {}, {}
    for path in sorted(glob.glob(os.path.join("logs", "trades", f"{client_id}-*.log"))):
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for ln in f:
                m = _ENTRY.match(ln)
                if m:
                    ts, inst, ce_s, ce_p, pe_s, pe_p = m.groups()
                    entry_map.setdefault(_key(inst, "CE", ce_s, ce_p), _iso(ts))
                    entry_map.setdefault(_key(inst, "PE", pe_s, pe_p), _iso(ts))
                    continue
                m = _EXIT.match(ln)
                if m:
                    ts, inst, ce_s, ce_p, pe_s, pe_p = m.groups()
                    exit_map.setdefault(_key(inst, "CE", ce_s, ce_p), _iso(ts))
                    exit_map.setdefault(_key(inst, "PE", pe_s, pe_p), _iso(ts))
    return entry_map, exit_map


def backfill(hist_path, entry_map, exit_map):
    with open(hist_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    filled = 0
    for tr in data.get("trades", []):
        inst = tr.get("instrument")
        for lg in tr.get("legs") or []:
            k = _key(inst, lg.get("side"), lg.get("strike", 0), lg.get("entry", 0))
            if not lg.get("entry_ts") and k in entry_map:
                lg["entry_ts"] = entry_map[k]; filled += 1
            xk = _key(inst, lg.get("side"), lg.get("strike", 0), lg.get("exit", 0))
            if not lg.get("exit_ts") and xk in exit_map:
                lg["exit_ts"] = exit_map[xk]; filled += 1
    if filled:
        shutil.copy(hist_path, hist_path + ".bak")
        with open(hist_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    return filled


def main():
    args = sys.argv[1:]
    if args:
        clients = args
    else:
        clients = [os.path.splitext(os.path.basename(p))[0]
                   for p in glob.glob(os.path.join("data", "history", "*.json"))
                   if not p.endswith(".bak")]
    if not clients:
        print("No client history found under data/history/.")
        return
    for cid in clients:
        hist = os.path.join("data", "history", f"{cid}.json")
        if not os.path.exists(hist):
            print(f"{cid}: no history file ({hist})"); continue
        em, xm = build_maps(cid)
        n = backfill(hist, em, xm)
        if n:
            print(f"{cid}: filled {n} timestamps from trade logs (backup {hist}.bak)")
        else:
            print(f"{cid}: nothing to fill (already has timestamps, or no matching log lines)")


if __name__ == "__main__":
    main()
