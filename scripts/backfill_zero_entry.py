"""Backfill history records whose legs were written with entry=0 (the restart+EOD bug, fixed
forward by carrying ce_entry/pe_entry on the EXIT event). Recovers the real sold rate from the
per-client straddle trade log's ENTRY lines and recomputes per-leg + record P&L.

Run on the box where data/history/<cid>.json and logs/trades/*.log live (e.g. EC2):

    python scripts/backfill_zero_entry.py --client ssrajpal2001
    python scripts/backfill_zero_entry.py --client ssrajpal2001 --dry-run   # preview only

Matching: a broken record is paired to the ENTRY line on the SAME DATE whose CE+PE strikes match
the record's legs. Idempotent — only legs with entry<=0 are touched.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from glob import glob

# ts | ENTRY | NIFTY | ATM=23400 | CE=23400@152.30 | PE=23350@171.20 | Credit=...
_ENTRY_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\|\s*ENTRY\s*\|\s*(?P<und>\w+)\s*\|"
    r".*?CE=(?P<ce>\d+)@(?P<cep>[\d.]+)\s*\|\s*PE=(?P<pe>\d+)@(?P<pep>[\d.]+)"
)


def parse_entries(log_glob: str) -> list[dict]:
    """All ENTRY (full straddle open) lines across the matched log files."""
    out: list[dict] = []
    for path in glob(log_glob):
        try:
            with open(path, errors="ignore") as f:
                for line in f:
                    m = _ENTRY_RE.search(line)
                    if not m:
                        continue
                    out.append({
                        "date": m.group("ts")[:10], "und": m.group("und"),
                        "ce": int(m.group("ce")), "ce_price": float(m.group("cep")),
                        "pe": int(m.group("pe")), "pe_price": float(m.group("pep")),
                    })
        except OSError:
            continue
    return out


def _qty_from_old(old_pnl: float, exit_price: float) -> int:
    # old leg pnl = (0 - exit) * qty  →  qty = -old_pnl / exit
    if exit_price and exit_price > 0:
        return max(1, round(abs(old_pnl) / exit_price))
    return 0


def backfill(client_id: str, log_dir: str, dry_run: bool) -> int:
    hist_path = os.path.join("data", "history", f"{client_id}.json")
    if not os.path.exists(hist_path):
        print(f"[!] no history file: {hist_path}")
        return 1
    with open(hist_path) as f:
        data = json.load(f)
    trades = data.get("trades", [])

    entries = parse_entries(os.path.join(log_dir, "*.log"))
    print(f"[i] parsed {len(entries)} ENTRY lines from {log_dir}/*.log")

    patched = 0
    for tr in trades:
        legs = tr.get("legs") or []
        if not any((l.get("entry", 0) or 0) <= 0 for l in legs):
            continue  # nothing broken in this record
        date = str(tr.get("ts", ""))[:10]
        ce_leg = next((l for l in legs if l.get("side") == "CE"), None)
        pe_leg = next((l for l in legs if l.get("side") == "PE"), None)
        if not ce_leg or not pe_leg:
            continue
        match = next((e for e in entries if e["date"] == date
                      and e["ce"] == int(ce_leg.get("strike", -1))
                      and e["pe"] == int(pe_leg.get("strike", -2))), None)
        if not match:
            print(f"[-] no ENTRY match for {date} CE{ce_leg.get('strike')}/PE{pe_leg.get('strike')}")
            continue
        for leg, price in ((ce_leg, match["ce_price"]), (pe_leg, match["pe_price"])):
            if (leg.get("entry", 0) or 0) > 0:
                continue
            qty = _qty_from_old(float(leg.get("pnl", 0) or 0), float(leg.get("exit", 0) or 0))
            leg["entry"] = round(price, 2)
            leg["pnl"] = round((price - float(leg.get("exit", 0) or 0)) * qty, 2)
        # Refresh record-level aggregates
        tr["entry_price"] = round(sum(float(l.get("entry", 0) or 0) for l in legs), 2)
        tr["pnl"] = round(sum(float(l.get("pnl", 0) or 0) for l in legs), 2)
        patched += 1
        print(f"[+] patched {date} CE{ce_leg['strike']}@{ce_leg['entry']} "
              f"PE{pe_leg['strike']}@{pe_leg['entry']}  ->  record P&L {tr['pnl']}")

    if patched and not dry_run:
        tmp = hist_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, hist_path)
        print(f"[OK] wrote {patched} repaired record(s) to {hist_path}")
    elif patched:
        print(f"[dry-run] would repair {patched} record(s) (no file written)")
    else:
        print("[i] nothing to repair (no entry<=0 records matched a log ENTRY).")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", required=True, help="client_id (history file stem)")
    ap.add_argument("--log-dir", default=os.path.join("logs", "trades"),
                    help="dir with per-client straddle trade logs (default logs/trades)")
    ap.add_argument("--dry-run", action="store_true", help="preview without writing")
    args = ap.parse_args()
    sys.exit(backfill(args.client, args.log_dir, args.dry_run))
