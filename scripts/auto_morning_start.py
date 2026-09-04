"""
Unattended daily morning-start orchestrator, run once at server boot via a
systemd oneshot unit (see ops/systemd/auto-morning-start.service). Starts
the app under pm2, headless-logs-in Upstox + Zerodha (best-effort each),
attempts Fyers via Playwright (best-effort), and emails one summary
regardless of outcome. Does NOT explicitly start any strategy -- each
strategy's own book manager reconciles off terminal_connected/trade_enabled/
is_running on its existing 5s loop.

Usage:
    python scripts/auto_morning_start.py [--dry-run] [--alert-to you@example.com]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from typing import List, Tuple

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker_auth.headless_totp_auth import (
    HeadlessTotpAuthError,
    upstox_totp_login,
    zerodha_totp_login,
)
from config.global_config import IST
from data_layer.client_db import ClientDB
from utils.email_alert import send_summary_email

logger = logging.getLogger(__name__)

# 2026-09-06, confirmed against the real `pm2 start` command run on the server
# (production pins NIFTY+SENSEX, sell_straddle+oi_orb_screener+cag_straddle, and
# futures-atm-underlyings -- an earlier placeholder here only had --index NIFTY
# with no --strategies/--futures-atm-underlyings at all, which would have booted
# the wrong process on a real unattended morning). Rather than duplicate these
# args here (guaranteed to drift out of sync again the next time production's
# launch flags change), _start_pm2() now prefers `pm2 restart terminus` --
# pm2's OWN remembered definition from the last `pm2 save`, always accurate by
# construction. This full command is kept only as a rebuild-from-scratch
# fallback for the case where the process was fully `pm2 delete`d and pm2's
# saved dump was lost (e.g. a from-scratch EC2 instance before first deploy).
_PM2_START_CMD = [
    "pm2", "start", "run_system.py", "--name", "terminus", "--interpreter", "python3",
    "--", "--mode", "live", "--ui", "--port", "5000",
    "--index", "NIFTY,SENSEX",
    "--strategies", "sell_straddle,oi_orb_screener,cag_straddle",
    "--futures-atm-underlyings", "NIFTY,SENSEX",
]
_DASHBOARD_HEALTH_URL = "http://localhost:5000/"
_HEALTH_TIMEOUT_SEC = 90
_HEALTH_POLL_INTERVAL_SEC = 3


async def _start_pm2() -> Tuple[bool, str]:
    try:
        restart = subprocess.run(["pm2", "restart", "terminus"], capture_output=True, text=True, timeout=30)
        if restart.returncode == 0:
            return True, "via pm2 restart (existing saved process)"
        # Process not found (e.g. pm2 delete'd, or a fresh box) -- rebuild from scratch.
        result = subprocess.run(_PM2_START_CMD, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return False, result.stderr.strip()[:300]
        return True, "via pm2 start (fresh process, restart target not found)"
    except Exception as exc:
        return False, str(exc)


async def _wait_for_dashboard_health() -> Tuple[bool, str]:
    deadline = time.monotonic() + _HEALTH_TIMEOUT_SEC
    last_err = ""
    while time.monotonic() < deadline:
        try:
            r = requests.get(_DASHBOARD_HEALTH_URL, timeout=5)
            if r.status_code == 200:
                return True, ""
            last_err = f"HTTP {r.status_code}"
        except Exception as exc:
            last_err = str(exc)
        await asyncio.sleep(_HEALTH_POLL_INTERVAL_SEC)
    return False, f"dashboard did not become healthy within {_HEALTH_TIMEOUT_SEC}s ({last_err})"


async def run_morning_sequence(dry_run: bool = False, zerodha_client_id: str = "") -> List[Tuple[str, bool, str]]:
    steps: List[Tuple[str, bool, str]] = []

    if not dry_run:
        ok, detail = await _start_pm2()
        steps.append(("pm2 start", ok, detail))
        if not ok:
            return steps

        ok, detail = await _wait_for_dashboard_health()
        steps.append(("dashboard health", ok, detail))
        if not ok:
            return steps

    db = ClientDB()

    # -- Upstox --
    try:
        creds = db.get_feeder_creds_sync("upstox") or {}
        token = await asyncio.to_thread(
            upstox_totp_login,
            api_key=creds.get("api_key", ""), api_secret=creds.get("secret", ""),
            user_id=creds.get("client_id", ""), password=creds.get("password", ""),
            totp_secret=creds.get("totp_secret", ""),
        )
        if not dry_run:
            now = datetime.now(IST).isoformat()
            await db.update_feeder_token("upstox", token, generated_at=now)
        steps.append(("Upstox login", True, f"token generated {datetime.now(IST).strftime('%H:%M:%S')} IST"))
    except HeadlessTotpAuthError as exc:
        steps.append(("Upstox login", False, str(exc)))
    except Exception as exc:
        steps.append(("Upstox login", False, f"unexpected error: {exc}"))

    # -- Zerodha (SA5770-style binding) --
    try:
        bindings = db.get_bindings_sync(zerodha_client_id) if zerodha_client_id else []
        zb = next((b for b in bindings if b.get("provider") == "zerodha"), None)
        if zb is None:
            steps.append(("Zerodha login", False, "no zerodha binding found for this client_id"))
        else:
            binding_id = zb["binding_id"]
            token = await asyncio.to_thread(
                zerodha_totp_login,
                api_key=zb.get("api_key", ""), api_secret=zb.get("api_secret", ""),
                user_id=zb.get("user_id", ""), password=zb.get("password", ""),
                totp_secret=zb.get("totp_secret", ""),
            )
            if not dry_run:
                now = datetime.now(IST).isoformat()
                await db.update_access_token(zerodha_client_id, binding_id, token, generated_at=now)
                await db.set_terminal_connected(zerodha_client_id, binding_id, True)
                await db.set_trade_enabled(zerodha_client_id, binding_id, True)
            steps.append((f"Zerodha login ({binding_id})", True,
                          f"terminal+trade enabled {datetime.now(IST).strftime('%H:%M:%S')} IST"))
    except HeadlessTotpAuthError as exc:
        steps.append(("Zerodha login", False, str(exc)))
    except Exception as exc:
        steps.append(("Zerodha login", False, f"unexpected error: {exc}"))

    # -- Fyers (best-effort -- see broker_auth/headless_totp_auth_fyers.py) --
    try:
        from broker_auth.headless_totp_auth_fyers import fyers_totp_login
        creds = db.get_feeder_creds_sync("fyers") or {}
        token = await asyncio.to_thread(
            fyers_totp_login,
            client_id=creds.get("client_id", ""), app_id=creds.get("api_key", ""),
            password=creds.get("password", ""), totp_secret=creds.get("totp_secret", ""),
            pin=creds.get("password", ""),
        )
        if not dry_run:
            now = datetime.now(IST).isoformat()
            await db.update_feeder_token("fyers", token, generated_at=now)
        steps.append(("Fyers login (best-effort)", True, ""))
    except Exception as exc:
        steps.append(("Fyers login (best-effort)", False, str(exc)))

    steps.append((
        "Strategies auto-resume",
        True,
        "no explicit action -- driven by each book manager's own reconcile loop "
        "off terminal_connected/trade_enabled/is_running",
    ))
    return steps


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--alert-to", default="ssrajpal2001@gmail.com")
    p.add_argument("--zerodha-client-id", default="ssrajpal2001")
    args = p.parse_args()

    steps = asyncio.run(run_morning_sequence(dry_run=args.dry_run, zerodha_client_id=args.zerodha_client_id))

    ok_count = sum(1 for _, ok, _ in steps if ok)
    subject = f"[AutoStart{'(dry-run)' if args.dry_run else ''}] {datetime.now(IST).date().isoformat()} — {ok_count}/{len(steps)} OK"
    send_summary_email(args.alert_to, subject, steps)

    for name, ok, detail in steps:
        logger.info("%s: %s %s", name, "OK" if ok else "FAILED", detail)


if __name__ == "__main__":
    main()
