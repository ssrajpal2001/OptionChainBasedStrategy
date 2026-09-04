"""
Unattended daily evening-stop script, run once at ~15:58 IST via a systemd
timer (see ops/systemd/auto-evening-stop.service), a few minutes before the
EventBridge-triggered EC2 stop. Trusts each strategy's own existing
force-exit time (all well before 16:00) -- no square-off/position-check
logic here, per the design spec's explicit non-goal.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import subprocess
import sys
from datetime import datetime
from typing import List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import IST
from utils.email_alert import send_summary_email

logger = logging.getLogger(__name__)


async def _stop_pm2() -> Tuple[bool, str]:
    try:
        result = subprocess.run(["pm2", "stop", "terminus"], capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return False, result.stderr.strip()[:300]
        return True, ""
    except Exception as exc:
        return False, str(exc)


async def run_evening_sequence(dry_run: bool = False) -> List[Tuple[str, bool, str]]:
    steps: List[Tuple[str, bool, str]] = []
    if dry_run:
        steps.append(("pm2 stop", True, "dry-run — not actually stopped"))
        return steps
    ok, detail = await _stop_pm2()
    steps.append(("pm2 stop", ok, detail))
    return steps


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--alert-to", default="ssrajpal2001@gmail.com")
    args = p.parse_args()

    steps = asyncio.run(run_evening_sequence(dry_run=args.dry_run))
    ok_count = sum(1 for _, ok, _ in steps if ok)
    subject = f"[AutoStop{'(dry-run)' if args.dry_run else ''}] {datetime.now(IST).date().isoformat()} — {ok_count}/{len(steps)} OK"
    send_summary_email(args.alert_to, subject, steps)
    for name, ok, detail in steps:
        logger.info("%s: %s %s", name, "OK" if ok else "FAILED", detail)


if __name__ == "__main__":
    main()
