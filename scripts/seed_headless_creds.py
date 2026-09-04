"""
One-time interactive script to seed the passwords/TOTP secrets needed by
scripts/auto_morning_start.py's headless login. Run once by hand (on the
server, or locally against a copy of data/clients.db that's then deployed)
-- never part of the daily automation. Prompts are masked (getpass);
nothing typed here is ever printed or logged.

Usage: python scripts/seed_headless_creds.py [--db-path data/clients.db]
"""
from __future__ import annotations

import argparse
import asyncio
import getpass
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_layer.client_db import ClientDB


def seed_from_answers(db: ClientDB, answers: dict) -> None:
    """Pure, testable core -- no interactive I/O. answers shape:
    {"upstox": {...}, "fyers": {...}, "zerodha_binding": {client_id, binding_id, ...}}
    Any top-level key may be omitted to skip that provider/binding.
    """
    if "upstox" in answers:
        a = answers["upstox"]
        asyncio.run(db.upsert_feeder_creds(
            provider="upstox", client_id=a.get("client_id", ""),
            api_key=a.get("api_key", ""), secret=a.get("secret", ""),
            password=a.get("password", ""), totp_secret=a.get("totp_secret", ""),
        ))
    if "fyers" in answers:
        a = answers["fyers"]
        asyncio.run(db.upsert_feeder_creds(
            provider="fyers", client_id=a.get("client_id", ""),
            api_key=a.get("api_key", ""), secret=a.get("secret", ""),
            password=a.get("password", ""), totp_secret=a.get("totp_secret", ""),
        ))
    if "zerodha_binding" in answers:
        a = answers["zerodha_binding"]
        asyncio.run(db.set_binding_password_totp(
            client_id=a["client_id"], binding_id=a["binding_id"],
            password=a.get("password", ""), totp_secret=a.get("totp_secret", ""),
        ))
    if "gmail" in answers:
        a = answers["gmail"]
        from utils.email_alert import seed_gmail_credentials
        asyncio.run(seed_gmail_credentials(db, a.get("user", ""), a.get("app_password", "")))


def _prompt_provider(name: str) -> dict:
    print(f"\n--- {name} ---")
    return {
        "client_id":   input(f"{name} client_id (broker user ID, blank to skip field): "),
        "api_key":     input(f"{name} api_key: "),
        "secret":      getpass.getpass(f"{name} api_secret: "),
        "password":    getpass.getpass(f"{name} password/PIN: "),
        "totp_secret": getpass.getpass(f"{name} TOTP base32 secret: "),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", default="data/clients.db")
    args = p.parse_args()

    db = ClientDB(args.db_path)
    asyncio.run(db.initialise())
    answers: dict = {}

    if input("Seed Upstox? [y/N]: ").strip().lower() == "y":
        answers["upstox"] = _prompt_provider("Upstox")
    if input("Seed Fyers? [y/N]: ").strip().lower() == "y":
        answers["fyers"] = _prompt_provider("Fyers")
    if input("Seed a Zerodha broker binding? [y/N]: ").strip().lower() == "y":
        print("\n--- Zerodha binding ---")
        answers["zerodha_binding"] = {
            "client_id":   input("client_id: "),
            "binding_id":  input("binding_id: "),
            "password":    getpass.getpass("password: "),
            "totp_secret": getpass.getpass("TOTP base32 secret: "),
        }
    if input("Seed Gmail alert credentials? [y/N]: ").strip().lower() == "y":
        answers["gmail"] = {
            "user":         input("Gmail address to send FROM: "),
            "app_password": getpass.getpass("Gmail app-specific password: "),
        }

    seed_from_answers(db, answers)
    print("\nDone. Nothing typed above was logged or echoed back.")


if __name__ == "__main__":
    main()
