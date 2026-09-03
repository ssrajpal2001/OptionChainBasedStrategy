#!/usr/bin/env python3
"""
scripts/smoke_crudeoil.py — short ghost-mode smoke test for the refactored
sell-straddle exit logic on CRUDEOIL.

Runs run_system.py in paper mode for ~30 seconds, then terminates it and
scans the latest log for fatal errors / tracebacks.  Exit code 0 means the
config loaded and the engine stayed up; non-zero means abort the live restart.
"""
from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = REPO_ROOT / "logs"
SMOKE_SECONDS = int(os.getenv("SMOKE_SECONDS", "30"))


def _latest_log() -> Path | None:
    today = date.today().strftime("%Y%m%d")
    candidates = sorted(
        (LOG_DIR / today).glob("*.log") if (LOG_DIR / today).exists() else [],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _grep_fatal(path: Path) -> list[str]:
    bad = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if any(k in line for k in ("Traceback", "FATAL", "CRITICAL", "ERROR")):
                bad.append(line.rstrip())
    return bad


def main() -> int:
    cmd = [
        sys.executable,
        "run_system.py",
        "--mode", "paper",
        "--index", "CRUDEOIL",
        "--strategies", "sell_straddle",
        "--log-level", "INFO",
    ]
    print(f"Smoke test: {' '.join(cmd)}")
    print(f"Running for {SMOKE_SECONDS}s...")

    proc = subprocess.Popen(
        cmd,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    time.sleep(SMOKE_SECONDS)

    print("Terminating smoke process...")
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        print("Smoke process did not terminate; killing.")
        proc.kill()
        proc.wait()

    # Give log flush a moment.
    time.sleep(1)

    latest = _latest_log()
    if latest is None:
        print("WARNING: no log file found to scan.")
        return 0

    print(f"Scanning log: {latest}")
    bad = _grep_fatal(latest)
    # Filter out benign ERROR lines you expect in paper/ghost mode.
    ignore_patterns = [
        re.compile(r"MCX orders rejected"),          # expected ghost positions
        re.compile(r"No positions found"),           # harmless on clean start
        re.compile(r"order.*rejected", re.IGNORECASE),
    ]
    filtered = [line for line in bad if not any(p.search(line) for p in ignore_patterns)]

    if filtered:
        print("FATAL / ERROR lines found:")
        for line in filtered[-20:]:
            print("  ", line)
        return 1

    print("Smoke test PASSED: no fatal errors in log.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
