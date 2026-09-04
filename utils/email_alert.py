"""
Gmail-SMTP email alerting for the unattended morning/evening automation
scripts. Built from scratch -- no prior alerting mechanism existed in this
codebase. The Gmail account + app-specific password are stored the same
way as every other secret in this system: `system_settings` key-value
table, but XOR+PBKDF2 obfuscated via ClientDB's own `_encode_cred`/
`_decode_cred` (the table itself stores plain values for non-secret
settings, so encoding/decoding is done here, at the call site, rather
than by `set_setting`/`get_setting_sync` themselves) -- never in a
script, config file, or env var.

Sending failures are swallowed (logged, never raised) -- the alert
mechanism itself going down must never be the reason an automation script
aborts partway through its own sequence.
"""
from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from typing import List, Tuple

logger = logging.getLogger(__name__)

_SETTING_GMAIL_USER = "auto_alert_gmail_user"
_SETTING_GMAIL_APP_PASSWORD = "auto_alert_gmail_app_password"


def _get_gmail_credentials() -> Tuple[str, str]:
    from data_layer.client_db import ClientDB, _decode_cred
    db = ClientDB()
    user_enc = db.get_setting_sync(_SETTING_GMAIL_USER, "")
    app_pw_enc = db.get_setting_sync(_SETTING_GMAIL_APP_PASSWORD, "")
    return _decode_cred(user_enc), _decode_cred(app_pw_enc)


async def seed_gmail_credentials(db, user: str, app_password: str) -> None:
    """Called only from scripts/seed_headless_creds.py (interactive, one-time)."""
    from data_layer.client_db import _encode_cred
    await db.set_setting(_SETTING_GMAIL_USER, _encode_cred(user))
    await db.set_setting(_SETTING_GMAIL_APP_PASSWORD, _encode_cred(app_password))


def format_summary_body(steps: List[Tuple[str, bool, str]]) -> str:
    lines = []
    for name, ok, detail in steps:
        status = "OK" if ok else "FAILED"
        line = f"{name:.<30} {status}"
        if detail:
            line += f"  ({detail})"
        lines.append(line)
    return "\n".join(lines)


def send_summary_email(to_addr: str, subject: str, steps: List[Tuple[str, bool, str]]) -> bool:
    try:
        user, app_pw = _get_gmail_credentials()
        if not user or not app_pw:
            logger.error("email_alert: Gmail credentials not seeded — cannot send alert.")
            return False

        msg = EmailMessage()
        msg["From"] = user
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg.set_content(format_summary_body(steps))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(user, app_pw)
            smtp.send_message(msg)
        return True
    except Exception as exc:
        logger.error("email_alert: failed to send summary email: %s", exc)
        return False
