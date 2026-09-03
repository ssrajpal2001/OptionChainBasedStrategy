"""
broker_auth/headless_totp_auth.py -- unattended headless TOTP login for the
automated daily morning-start sequence (scripts/auto_morning_start.py).

Deliberately SEPARATE from broker_auth/headless_auth.py, which stays
OAuth-only and is used by every interactive dashboard flow -- this module
is used ONLY by the unattended morning script. Revived and hardened from
this repo's own pre-refactor implementation (git history at commit
84e2237, removed in 8b03adf) after direct user decision to accept the
same risk profile any unofficial headless broker-login script carries
(see docs/superpowers/specs/2026-09-03-fully-automated-daily-lifecycle-design.md).

Each provider function is synchronous/blocking (real network I/O) --
callers run it via asyncio.to_thread(). Each raises HeadlessTotpAuthError
with a message naming the specific step that failed, never raises a bare
exception type a caller can't act on.
"""
from __future__ import annotations

import base64
import hashlib
import random
import string
import time
from urllib.parse import parse_qs, urlparse


class HeadlessTotpAuthError(Exception):
    """A headless TOTP login flow failed at a specific, named step."""


def _mask(s: str) -> str:
    return (s[:4] + "****") if s and len(s) > 4 else "****"


def _upstox_session():
    """Isolated so tests can monkeypatch it without touching curl_cffi."""
    from curl_cffi import requests as cffi_requests
    headers = {
        "accept": "*/*",
        "accept-language": "en-GB,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://login.upstox.com",
        "referer": "https://login.upstox.com",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
        ),
        "x-request-id": "WPRO-" + "".join(
            random.choices(string.ascii_letters + string.digits, k=10)
        ),
    }
    return cffi_requests.Session(impersonate="chrome131", headers=headers)


def _upstox_parse(resp):
    try:
        body = resp.json()
    except Exception:
        raise HeadlessTotpAuthError(
            f"Upstox: non-JSON response (HTTP {resp.status_code}): {resp.text[:300]}"
        )
    if not isinstance(body, dict):
        raise HeadlessTotpAuthError(f"Upstox: unexpected response shape: {body!r}")
    if "success" not in body:
        return body
    if not body.get("success", True):
        err = body.get("error") or {}
        if isinstance(err, dict):
            code = err.get("errorCode") or err.get("code") or ""
            msg = err.get("message") or err.get("msg") or str(err)
            raise HeadlessTotpAuthError(f"Upstox {code}: {msg}".strip(": "))
        raise HeadlessTotpAuthError(f"Upstox login failed: {body}")
    return body.get("data")


def upstox_totp_login(
    api_key: str, api_secret: str, user_id: str, password: str, totp_secret: str,
) -> str:
    """
    6-step Upstox headless TOTP login (service.upstox.com internal API),
    curl_cffi chrome131 TLS fingerprint. Raises HeadlessTotpAuthError on any
    failure, naming the step. Returns the access_token on success.
    """
    import pyotp

    if not api_key:
        raise HeadlessTotpAuthError("Upstox: api_key is required.")
    if not totp_secret:
        raise HeadlessTotpAuthError("Upstox: totp_secret is required for headless auto-authentication.")
    if not password:
        raise HeadlessTotpAuthError("Upstox: password (6-digit PIN) is required.")

    totp_secret_clean = totp_secret.upper().replace(" ", "").replace("-", "")
    try:
        pyotp.TOTP(totp_secret_clean).now()
    except Exception as exc:
        raise HeadlessTotpAuthError(f"Upstox: invalid TOTP secret — {exc}")

    _API = "https://api.upstox.com"
    _SVC = "https://service.upstox.com"
    _INT_RDR = "https://api-v2.upstox.com/login/authorization/redirect"
    redirect_uri = "https://www.google.com"

    session = _upstox_session()

    # Step 1: dialog -> session user_id
    r1 = session.get(
        f"{_API}/v2/login/authorization/dialog",
        params={"response_type": "code", "client_id": api_key, "redirect_uri": redirect_uri},
        allow_redirects=True,
    )
    qs1 = parse_qs(urlparse(r1.url).query)
    sess_user_id = (qs1.get("user_id") or [""])[0]
    sess_client_id = (qs1.get("client_id") or [api_key])[0]
    if not sess_user_id:
        raise HeadlessTotpAuthError(f"Upstox: Step 1 failed — session user_id missing. final_url={r1.url!r}")
    time.sleep(1)

    # Step 2: generate OTP
    r2 = session.post(f"{_SVC}/login/open/v6/auth/1fa/otp/generate",
                       json={"data": {"mobileNumber": user_id, "userId": sess_user_id}})
    d2 = _upstox_parse(r2)
    validate_otp_token = (d2 or {}).get("validateOTPToken") or (d2 or {}).get("validateOtpToken")
    if not validate_otp_token:
        raise HeadlessTotpAuthError(f"Upstox: Step 2 failed — validateOTPToken missing. data={d2}")
    time.sleep(1)

    # Step 3: verify TOTP
    live_totp = pyotp.TOTP(totp_secret_clean).now()
    r3 = session.post(f"{_SVC}/login/open/v4/auth/1fa/otp-totp/verify",
                       json={"data": {"otp": live_totp, "validateOtpToken": validate_otp_token}})
    _upstox_parse(r3)
    time.sleep(1)

    # Step 4: submit PIN
    pin_b64 = base64.b64encode(password.encode()).decode()
    r4 = session.post(
        f"{_SVC}/login/open/v3/auth/2fa",
        params={"client_id": sess_client_id, "redirect_uri": _INT_RDR},
        json={"data": {"twoFAMethod": "SECRET_PIN", "inputText": pin_b64}},
        allow_redirects=True,
    )
    _upstox_parse(r4)
    time.sleep(1)

    # Step 5: OAuth approve -> auth code
    request_id = "WPRO-" + "".join(random.choices(string.ascii_letters + string.digits, k=10))
    r5 = session.post(
        f"{_SVC}/login/v2/oauth/authorize",
        params={"client_id": sess_client_id, "redirect_uri": _INT_RDR,
                "requestId": request_id, "response_type": "code"},
        json={"data": {"userOAuthApproval": True}},
        allow_redirects=True,
    )
    d5 = _upstox_parse(r5)
    oauth_redirect = (d5 or {}).get("redirectUri", "")
    qs5 = parse_qs(urlparse(oauth_redirect).query)
    auth_code = (qs5.get("code") or [""])[0]
    if not auth_code:
        raise HeadlessTotpAuthError(f"Upstox: Step 5 failed — auth code missing. redirectUri={oauth_redirect!r}")
    time.sleep(1)

    # Step 6: token exchange
    tok_sess = _upstox_session()
    r6 = tok_sess.post(
        f"{_API}/v2/login/authorization/token",
        data=(f"code={auth_code}&client_id={api_key}&client_secret={api_secret}"
              f"&redirect_uri={redirect_uri}&grant_type=authorization_code"),
        headers={"accept": "application/json", "content-type": "application/x-www-form-urlencoded"},
    )
    d6 = _upstox_parse(r6)
    access_token = (d6 or {}).get("access_token", "")
    if not access_token:
        raise HeadlessTotpAuthError(f"Upstox: Step 6 failed — access_token missing. data={d6}")
    return access_token
