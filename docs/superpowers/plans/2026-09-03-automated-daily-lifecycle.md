# Fully Automated Daily EC2 + Trading Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fully automate the daily trading lifecycle — EC2 boots at 09:00 IST, the app/feeders/broker log in unattended, strategies resume on their own, EC2 stops at 16:00 IST — with best-effort execution (one failed step never blocks the rest) and an email summary every run.

**Architecture:** Three independent pieces. (1) AWS EventBridge + Lambda starts/stops the EC2 instance on a cron schedule — zero code in this repo. (2) A `systemd`-triggered `scripts/auto_morning_start.py` starts the app under pm2, then headless-logs-in Upstox and Zerodha (revived from this repo's own pre-refactor git history, commit `84e2237`) plus a best-effort Fyers login via Playwright, then relies on each strategy's existing 5s reconcile loop to auto-resume anything left `is_running=1`. (3) A `systemd`-triggered `scripts/auto_evening_stop.py` stops pm2, trusting each strategy's own existing force-exit times.

**Tech Stack:** Python 3 (asyncio, `curl_cffi`, `requests`, `pyotp`, `playwright`), SQLite (existing `data/clients.db`), `smtplib` (Gmail SMTP), `systemd` (EC2 OS), AWS EventBridge Scheduler + Lambda + IAM (CloudFormation).

**Spec:** `docs/superpowers/specs/2026-09-03-fully-automated-daily-lifecycle-design.md`

## Global Constraints

- All secrets (broker passwords, TOTP base32 secrets, the Gmail app password) are stored ONLY in `data/clients.db`, obfuscated via the existing `_encode_cred`/`_decode_cred` (XOR+PBKDF2) helpers in `data_layer/client_db.py` — never in scripts, env vars, or `strategy_config.json`.
- Every headless-login/network step is wrapped so its failure never raises out to abort a sibling step — the morning script's philosophy is best-effort + report, not all-or-nothing.
- No log line may print a raw password, TOTP secret, or full access token — mask per the existing `_mask()` convention (`s[:4] + "****"`) used in the pre-refactor code being revived.
- `broker_auth/headless_auth.py` (the current OAuth-only engine used by every interactive dashboard flow) is NOT modified by this plan — all new headless-TOTP code lives in new, separate modules so the live dashboard's existing OAuth flow is never put at risk.
- No square-off / position-close logic is added anywhere in this plan — shutdown trusts each strategy's own existing force-exit time, per the spec's explicit non-goal.

---

### Task 1: DB migration — re-add password/TOTP columns to `system_feeder_creds`

**Files:**
- Modify: `data_layer/client_db.py:1354-1422` (additive migrations list + the old drop-migration block), `data_layer/client_db.py:~410-495` (`upsert_feeder_creds`, `get_feeder_creds_sync`)
- Test: `tests/data_layer/test_feeder_creds_totp_migration.py`

**Interfaces:**
- Produces: `ClientDB.upsert_feeder_creds(provider, client_id="", api_key="", secret="", password="", totp_secret="")` (two new optional kwargs); `ClientDB.get_feeder_creds_sync(provider)` return dict gains `"password"` and `"totp_secret"` keys (decoded).

- [ ] **Step 1: Write the failing test for the migration + round-trip**

```python
# tests/data_layer/test_feeder_creds_totp_migration.py
import asyncio
import sqlite3
import tempfile
import os
import pytest
from data_layer.client_db import ClientDB


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_clients.db")


def test_system_feeder_creds_has_password_and_totp_columns(db_path):
    db = ClientDB(db_path)
    con = sqlite3.connect(db_path)
    cols = {row[1] for row in con.execute("PRAGMA table_info(system_feeder_creds)").fetchall()}
    con.close()
    assert "password_enc" in cols
    assert "totp_secret_enc" in cols


def test_old_drop_migration_no_longer_wipes_the_columns_on_reopen(db_path):
    # Simulates a second process boot (e.g. a restart) re-running __init__'s
    # migration logic against an already-migrated DB -- must NOT re-drop
    # the columns that were just re-added.
    ClientDB(db_path)
    ClientDB(db_path)
    con = sqlite3.connect(db_path)
    cols = {row[1] for row in con.execute("PRAGMA table_info(system_feeder_creds)").fetchall()}
    con.close()
    assert "password_enc" in cols
    assert "totp_secret_enc" in cols


def test_upsert_and_get_feeder_creds_round_trips_password_and_totp(db_path):
    db = ClientDB(db_path)
    asyncio.run(db.upsert_feeder_creds(
        provider="upstox", client_id="UP123", api_key="key1", secret="sec1",
        password="123456", totp_secret="JBSWY3DPEHPK3PXP",
    ))
    creds = db.get_feeder_creds_sync("upstox")
    assert creds["password"] == "123456"
    assert creds["totp_secret"] == "JBSWY3DPEHPK3PXP"
    assert creds["api_key"] == "key1"


def test_upsert_feeder_creds_preserves_existing_password_when_omitted(db_path):
    db = ClientDB(db_path)
    asyncio.run(db.upsert_feeder_creds(
        provider="upstox", client_id="UP123", api_key="key1", secret="sec1",
        password="123456", totp_secret="JBSWY3DPEHPK3PXP",
    ))
    # Re-upsert without password/totp (e.g. admin UI updating just api_key)
    asyncio.run(db.upsert_feeder_creds(provider="upstox", api_key="key2"))
    creds = db.get_feeder_creds_sync("upstox")
    assert creds["api_key"] == "key2"
    assert creds["password"] == "123456"
    assert creds["totp_secret"] == "JBSWY3DPEHPK3PXP"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/data_layer/test_feeder_creds_totp_migration.py -v`
Expected: FAIL — columns don't exist yet, `upsert_feeder_creds` doesn't accept `password`/`totp_secret`.

- [ ] **Step 3: Remove the old drop-migration block and add the re-add migration**

In `data_layer/client_db.py`, delete the entire block from the comment `# Security migration: drop password_enc / totp_secret_enc from system_feeder_creds` through the matching `except Exception as exc: logger.error(...)` (currently lines ~1391-1421 — re-locate by searching for that comment text, since earlier edits in this task may shift line numbers). Replace it with nothing (delete outright) — the additive-migration loop below now owns these two columns permanently.

Add two entries to the additive-migrations tuple (the `for migration in (...)` block, alongside the existing `ALTER TABLE broker_bindings ADD COLUMN password_enc ...` pair):

```python
            "ALTER TABLE system_feeder_creds ADD COLUMN password_enc TEXT DEFAULT ''",
            "ALTER TABLE system_feeder_creds ADD COLUMN totp_secret_enc TEXT DEFAULT ''",
```

Also add the same two columns to the `CREATE TABLE IF NOT EXISTS system_feeder_creds` DDL near line 163, so a brand-new DB (tests, a fresh EC2 box) gets them immediately without relying on the migration path:

```python
CREATE TABLE IF NOT EXISTS system_feeder_creds (
    provider           TEXT PRIMARY KEY,
    client_id_enc      TEXT DEFAULT '',
    api_key_enc        TEXT DEFAULT '',
    secret_enc         TEXT DEFAULT '',
    password_enc       TEXT DEFAULT '',
    totp_secret_enc    TEXT DEFAULT '',
    access_token       TEXT DEFAULT '',
    token_generated_at TEXT DEFAULT '',
    token_expiry_at    TEXT DEFAULT '',
    updated_at         TEXT NOT NULL
);
```

- [ ] **Step 4: Extend `upsert_feeder_creds` and `get_feeder_creds_sync`**

Replace the existing `upsert_feeder_creds` method body with:

```python
    async def upsert_feeder_creds(
        self,
        provider:    str,
        client_id:   str = "",
        api_key:     str = "",
        secret:      str = "",
        password:    str = "",
        totp_secret: str = "",
    ) -> None:
        """
        Persist admin feeder credentials (XOR-obfuscated).
        password/totp_secret exist ONLY to support the unattended headless
        TOTP login used by scripts/auto_morning_start.py (see
        broker_auth/headless_totp_auth.py) -- never used by the interactive
        OAuth dashboard flow.
        """
        logger.info(
            "[DB] upsert_feeder_creds provider=%s client_id_present=%s api_key_present=%s "
            "secret_present=%s password_present=%s totp_present=%s",
            provider, bool(client_id), bool(api_key), bool(secret),
            bool(password), bool(totp_secret),
        )
        now = datetime.now(IST).isoformat()
        await asyncio.to_thread(
            self._exec,
            """INSERT INTO system_feeder_creds
               (provider, client_id_enc, api_key_enc, secret_enc, password_enc,
                totp_secret_enc, updated_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(provider) DO UPDATE SET
                 client_id_enc   = CASE WHEN excluded.client_id_enc   != '' THEN excluded.client_id_enc   ELSE client_id_enc   END,
                 api_key_enc     = CASE WHEN excluded.api_key_enc     != '' THEN excluded.api_key_enc     ELSE api_key_enc     END,
                 secret_enc      = CASE WHEN excluded.secret_enc      != '' THEN excluded.secret_enc      ELSE secret_enc      END,
                 password_enc    = CASE WHEN excluded.password_enc    != '' THEN excluded.password_enc    ELSE password_enc    END,
                 totp_secret_enc = CASE WHEN excluded.totp_secret_enc != '' THEN excluded.totp_secret_enc ELSE totp_secret_enc END,
                 updated_at      = excluded.updated_at""",
            (
                provider,
                _encode_cred(client_id),
                _encode_cred(api_key),
                _encode_cred(secret),
                _encode_cred(password),
                _encode_cred(totp_secret),
                now,
            ),
        )
```

Replace the existing `get_feeder_creds_sync` return dict to add two keys (insert after the `"secret"` line):

```python
                "password":           _decode_cred(r.get("password_enc", "")),
                "totp_secret":        _decode_cred(r.get("totp_secret_enc", "")),
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/data_layer/test_feeder_creds_totp_migration.py -v`
Expected: PASS (all 4 tests)

- [ ] **Step 6: Run the full existing client_db test suite to confirm no regression**

Run: `pytest tests/data_layer/ -v -k client_db or feeder`
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add data_layer/client_db.py tests/data_layer/test_feeder_creds_totp_migration.py
git commit -m "feat(db): re-add password/totp columns to system_feeder_creds for headless auto-login"
```

---

### Task 2: Revive Upstox headless TOTP login

**Files:**
- Create: `broker_auth/headless_totp_auth.py`
- Test: `tests/broker_auth/test_headless_totp_auth_upstox.py`

**Interfaces:**
- Consumes: nothing from earlier tasks (pure module).
- Produces: `class HeadlessTotpAuthError(Exception)`; `def upstox_totp_login(api_key: str, api_secret: str, user_id: str, password: str, totp_secret: str) -> str` — returns `access_token` or raises `HeadlessTotpAuthError(str)` with a human-readable step-labeled message. Synchronous (blocking) — callers wrap in `asyncio.to_thread`.

- [ ] **Step 1: Write the module skeleton + `_mask` helper**

```python
# broker_auth/headless_totp_auth.py
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
```

- [ ] **Step 2: Write the failing tests for the pure/testable pieces**

```python
# tests/broker_auth/test_headless_totp_auth_upstox.py
import pytest
from broker_auth.headless_totp_auth import _mask, HeadlessTotpAuthError, upstox_totp_login


def test_mask_short_string():
    assert _mask("ab") == "****"


def test_mask_long_string():
    assert _mask("ABCDEFGH") == "ABCD****"


def test_upstox_missing_api_key_raises():
    with pytest.raises(HeadlessTotpAuthError, match="api_key"):
        upstox_totp_login(api_key="", api_secret="s", user_id="u",
                           password="123456", totp_secret="JBSWY3DPEHPK3PXP")


def test_upstox_missing_totp_secret_raises():
    with pytest.raises(HeadlessTotpAuthError, match="totp_secret"):
        upstox_totp_login(api_key="k", api_secret="s", user_id="u",
                           password="123456", totp_secret="")


def test_upstox_invalid_totp_secret_raises(monkeypatch):
    class FakeSession:
        def __init__(self, *a, **k):
            pass

        def get(self, *a, **k):
            class R:
                url = "https://x/?user_id=SESSUSER&client_id=SESSCLIENT"
            return R()

        def post(self, *a, **k):
            class R:
                status_code = 200

                def json(self_r):
                    return {"success": True, "data": {"validateOTPToken": "tok123"}}
            return R()

    import broker_auth.headless_totp_auth as mod
    monkeypatch.setattr(mod, "_upstox_session", lambda: FakeSession())
    monkeypatch.setattr("time.sleep", lambda *_: None)
    with pytest.raises(HeadlessTotpAuthError, match="invalid TOTP secret"):
        upstox_totp_login(api_key="k", api_secret="s", user_id="9999999999",
                           password="123456", totp_secret="NOT-VALID-BASE32!!")


def test_upstox_full_success_path(monkeypatch):
    calls = {"n": 0}

    class R:
        def __init__(self, url="", status_code=200, body=None):
            self.url = url
            self.status_code = status_code
            self._body = body or {}
            self.text = str(self._body)

        def json(self):
            return self._body

    class FakeSession:
        def __init__(self, *a, **k):
            pass

        def get(self, url, **k):
            # Step 1: dialog redirect
            return R(url="https://x/?user_id=SESSUSER&client_id=SESSCLIENT")

        def post(self, url, **k):
            calls["n"] += 1
            if "otp/generate" in url:
                return R(body={"success": True, "data": {"validateOTPToken": "tok123"}})
            if "otp-totp/verify" in url:
                return R(body={"success": True, "data": {}})
            if "2fa" in url:
                return R(body={"success": True, "data": {}})
            if "oauth/authorize" in url:
                return R(body={"success": True, "data": {
                    "redirectUri": "https://cb/?code=AUTHCODE123"}})
            raise AssertionError(f"unexpected POST {url}")

    class FakeTokenSession:
        def __init__(self, *a, **k):
            pass

        def post(self, url, **k):
            return R(body={"access_token": "FINAL_TOKEN_XYZ"})

    import broker_auth.headless_totp_auth as mod
    sessions = iter([FakeSession(), FakeTokenSession()])
    monkeypatch.setattr(mod, "_upstox_session", lambda: next(sessions))
    monkeypatch.setattr("time.sleep", lambda *_: None)

    token = upstox_totp_login(api_key="k", api_secret="s", user_id="9999999999",
                               password="123456", totp_secret="JBSWY3DPEHPK3PXP")
    assert token == "FINAL_TOKEN_XYZ"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/broker_auth/test_headless_totp_auth_upstox.py -v`
Expected: FAIL — `upstox_totp_login`/`_upstox_session` don't exist yet.

- [ ] **Step 4: Implement `_upstox_session` seam + `upstox_totp_login`**

Append to `broker_auth/headless_totp_auth.py`:

```python
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
        pyotp.TOTP(totp_secret_clean)
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/broker_auth/test_headless_totp_auth_upstox.py -v`
Expected: PASS (5 tests)

- [ ] **Step 6: Commit**

```bash
git add broker_auth/headless_totp_auth.py tests/broker_auth/test_headless_totp_auth_upstox.py
git commit -m "feat(auth): revive Upstox headless TOTP login for unattended morning start"
```

---

### Task 3: Revive Zerodha headless TOTP login

**Files:**
- Modify: `broker_auth/headless_totp_auth.py` (append)
- Test: `tests/broker_auth/test_headless_totp_auth_zerodha.py`

**Interfaces:**
- Produces: `def zerodha_totp_login(api_key: str, api_secret: str, user_id: str, password: str, totp_secret: str) -> str` — same contract as `upstox_totp_login`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/broker_auth/test_headless_totp_auth_zerodha.py
import pytest
from broker_auth.headless_totp_auth import HeadlessTotpAuthError, zerodha_totp_login


def test_zerodha_missing_required_field_raises():
    with pytest.raises(HeadlessTotpAuthError, match="required"):
        zerodha_totp_login(api_key="", api_secret="s", user_id="u",
                            password="p", totp_secret="JBSWY3DPEHPK3PXP")


def test_zerodha_missing_totp_secret_raises():
    with pytest.raises(HeadlessTotpAuthError, match="totp_secret"):
        zerodha_totp_login(api_key="k", api_secret="s", user_id="u", password="p", totp_secret="")


def test_zerodha_wrong_password_raises(monkeypatch):
    class FakeResp:
        def __init__(self, body, status_code=200, url=""):
            self._body = body
            self.status_code = status_code
            self.url = url

        def json(self):
            return self._body

    class FakeSession:
        def __init__(self):
            self.headers = {}

        def get(self, url, **k):
            return FakeResp({}, url=url)

        def post(self, url, **k):
            if "api/login" in url:
                return FakeResp({"status": "error", "message": "Invalid password"})
            raise AssertionError(f"unexpected POST {url}")

    import broker_auth.headless_totp_auth as mod
    monkeypatch.setattr(mod, "_zerodha_session", lambda: FakeSession())
    monkeypatch.setattr("time.sleep", lambda *_: None)
    with pytest.raises(HeadlessTotpAuthError, match="Invalid password"):
        zerodha_totp_login(api_key="k", api_secret="s", user_id="u",
                            password="wrong", totp_secret="JBSWY3DPEHPK3PXP")


def test_zerodha_full_success_path(monkeypatch):
    class FakeResp:
        def __init__(self, body, status_code=200, url=""):
            self._body = body
            self.status_code = status_code
            self.url = url

        def json(self):
            return self._body

    class FakeSession:
        def __init__(self):
            self.headers = {}

        def get(self, url, **k):
            if "connect/login" in url:
                # second call (post-2FA) redirects with request_token
                if getattr(self, "_authed", False):
                    return FakeResp({}, url="https://cb/?request_token=REQTOK123&action=login&status=success")
                self._authed = False
                return FakeResp({}, url="https://kite.zerodha.com/connect/login")
            raise AssertionError(f"unexpected GET {url}")

        def post(self, url, **k):
            if "api/login" in url:
                return FakeResp({"status": "success", "data": {"request_id": "REQ123"}})
            if "api/twofa" in url:
                self._authed = True
                return FakeResp({"status": "success", "data": {}})
            if "session/token" in url:
                return FakeResp({"status": "success", "data": {"access_token": "ZTOKEN123"}})
            raise AssertionError(f"unexpected POST {url}")

    import broker_auth.headless_totp_auth as mod
    monkeypatch.setattr(mod, "_zerodha_session", lambda: FakeSession())
    monkeypatch.setattr("time.sleep", lambda *_: None)

    token = zerodha_totp_login(api_key="k", api_secret="s", user_id="AB1234",
                                password="pw", totp_secret="JBSWY3DPEHPK3PXP")
    assert token == "ZTOKEN123"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/broker_auth/test_headless_totp_auth_zerodha.py -v`
Expected: FAIL — `zerodha_totp_login`/`_zerodha_session` don't exist yet.

- [ ] **Step 3: Implement `_zerodha_session` seam + `zerodha_totp_login`**

Append to `broker_auth/headless_totp_auth.py`:

```python
def _zerodha_session():
    """Isolated so tests can monkeypatch it without touching requests."""
    import requests as _req
    s = _req.Session()
    s.headers.update({
        "X-Kite-Version": "3",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    })
    return s


def zerodha_totp_login(
    api_key: str, api_secret: str, user_id: str, password: str, totp_secret: str,
) -> str:
    """
    Zerodha Kite Connect headless TOTP login: password -> TOTP 2FA ->
    request_token via redirect -> checksum-signed token exchange.
    Raises HeadlessTotpAuthError on any failure, naming the step.
    """
    import pyotp

    if not all([api_key, api_secret, user_id, password]):
        raise HeadlessTotpAuthError("Zerodha: api_key, api_secret, user_id, and password are required.")
    if not totp_secret:
        raise HeadlessTotpAuthError("Zerodha: totp_secret is required for headless authentication.")

    s = _zerodha_session()

    # Step 0: init OAuth session context
    s.get("https://kite.zerodha.com/connect/login", params={"v": "3", "api_key": api_key},
          allow_redirects=True, timeout=15)
    time.sleep(0.5)

    # Step 1: password login
    r1 = s.post("https://kite.zerodha.com/api/login",
                data={"user_id": user_id, "password": password}, timeout=15)
    d1 = r1.json()
    if d1.get("status") != "success":
        raise HeadlessTotpAuthError(f"Zerodha Step 1 (login): {d1.get('message', 'Login failed.')}")
    request_id = d1["data"]["request_id"]
    time.sleep(0.5)

    # Step 2: TOTP 2FA
    totp_clean = totp_secret.upper().replace(" ", "").replace("-", "")
    try:
        totp_code = pyotp.TOTP(totp_clean).now()
    except Exception as exc:
        raise HeadlessTotpAuthError(f"Zerodha: invalid TOTP secret — {exc}")

    r2 = s.post("https://kite.zerodha.com/api/twofa",
                data={"user_id": user_id, "request_id": request_id,
                      "twofa_value": totp_code, "twofa_type": "totp"},
                allow_redirects=False, timeout=15)
    try:
        d2 = r2.json()
    except Exception:
        raise HeadlessTotpAuthError(f"Zerodha Step 2 (2FA): non-JSON response (HTTP {r2.status_code})")
    if d2.get("status") != "success":
        raise HeadlessTotpAuthError(f"Zerodha Step 2 (2FA): {d2.get('message', '2FA failed.')}")
    time.sleep(0.5)

    # Step 2b: re-GET the authenticated redirect for request_token
    r3 = s.get("https://kite.zerodha.com/connect/login", params={"v": "3", "api_key": api_key},
               allow_redirects=True, timeout=15)
    qs = parse_qs(urlparse(r3.url).query)
    request_token = (qs.get("request_token") or [""])[0]
    if not request_token:
        raise HeadlessTotpAuthError(
            f"Zerodha Step 2b (redirect): request_token not in final URL. final_url={r3.url!r}"
        )

    # Step 3: exchange request_token -> access_token
    checksum = hashlib.sha256(f"{api_key}{request_token}{api_secret}".encode()).hexdigest()
    r4 = s.post("https://api.kite.trade/session/token",
                data={"api_key": api_key, "request_token": request_token, "checksum": checksum},
                headers={"X-Kite-Version": "3"}, timeout=15)
    d4 = r4.json()
    if d4.get("status") != "success":
        raise HeadlessTotpAuthError(f"Zerodha Step 3 (token exchange): {d4.get('message', 'Token exchange failed.')}")
    access_token = (d4.get("data") or {}).get("access_token", "")
    if not access_token:
        raise HeadlessTotpAuthError(f"Zerodha Step 3: access_token missing in response: {d4}")
    return access_token
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/broker_auth/test_headless_totp_auth_zerodha.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add broker_auth/headless_totp_auth.py tests/broker_auth/test_headless_totp_auth_zerodha.py
git commit -m "feat(auth): revive Zerodha headless TOTP login for unattended morning start"
```

---

### Task 4: One-time credential seeding script

**Files:**
- Create: `scripts/seed_headless_creds.py`
- Test: `tests/scripts/test_seed_headless_creds.py`

**Interfaces:**
- Consumes: `ClientDB.upsert_feeder_creds(...)` (Task 1), `ClientDB.set_binding_password_totp` — **new** small setter needed for the Zerodha `broker_bindings` row (does not exist yet; add it in this task since it's a one-line analog of the existing `password_enc`/`totp_secret_enc` columns already on that table).
- Produces: `def seed_from_answers(db: ClientDB, answers: dict) -> None` (the testable core, no interactive I/O) and a `main()` that prompts via `getpass`/`input` and calls it.

- [ ] **Step 1: Add the missing `ClientDB` setter for broker_bindings password/TOTP**

Add to `data_layer/client_db.py`, near `set_trade_enabled`:

```python
    async def set_binding_password_totp(
        self, client_id: str, binding_id: str, password: str, totp_secret: str,
    ) -> None:
        """Store password/TOTP secret for a broker binding's headless login (e.g. Zerodha)."""
        await asyncio.to_thread(
            self._exec,
            "UPDATE broker_bindings SET password_enc=?, totp_secret_enc=? "
            "WHERE client_id=? AND binding_id=?",
            (_encode_cred(password), _encode_cred(totp_secret), client_id, binding_id),
        )
```

- [ ] **Step 2: Write the failing test for the seeding core**

```python
# tests/scripts/test_seed_headless_creds.py
import asyncio
from data_layer.client_db import ClientDB
from scripts.seed_headless_creds import seed_from_answers


def test_seed_from_answers_writes_upstox_zerodha_fyers(tmp_path):
    db = ClientDB(str(tmp_path / "clients.db"))
    answers = {
        "upstox": {"client_id": "UP1", "api_key": "upk", "secret": "ups",
                    "password": "111111", "totp_secret": "JBSWY3DPEHPK3PXP"},
        "fyers": {"client_id": "FY1", "api_key": "fyk", "secret": "fys",
                   "password": "2222", "totp_secret": "JBSWY3DPEHPK3PXP"},
        "zerodha_binding": {"client_id": "ssrajpal2001", "binding_id": "SA5770",
                              "password": "zpw", "totp_secret": "JBSWY3DPEHPK3PXP"},
    }
    seed_from_answers(db, answers)

    up = db.get_feeder_creds_sync("upstox")
    assert up["password"] == "111111" and up["totp_secret"] == "JBSWY3DPEHPK3PXP"

    fy = db.get_feeder_creds_sync("fyers")
    assert fy["password"] == "2222"

    bindings = db.get_bindings_safe_sync("ssrajpal2001")
    sa = next(b for b in bindings if b["binding_id"] == "SA5770")
    # get_bindings_safe_sync strips secrets by design (see client_db.py) --
    # verify via a direct decode instead of the safe accessor.
    import sqlite3
    from data_layer.client_db import _decode_cred
    con = sqlite3.connect(str(tmp_path / "clients.db"))
    row = con.execute(
        "SELECT password_enc, totp_secret_enc FROM broker_bindings WHERE binding_id='SA5770'"
    ).fetchone()
    con.close()
    assert _decode_cred(row[0]) == "zpw"
    assert _decode_cred(row[1]) == "JBSWY3DPEHPK3PXP"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/scripts/test_seed_headless_creds.py -v`
Expected: FAIL — module doesn't exist.

Note: if `ssrajpal2001`/`SA5770` doesn't already exist as a client/binding row in a fresh `tmp_path` DB, `set_binding_password_totp`'s `UPDATE ... WHERE` will silently match zero rows. Add a client + binding fixture row via `db.add_client_sync(...)`/existing binding-creation helper (check `data_layer/client_db.py` for the exact method name used elsewhere in this repo's own tests, e.g. `tests/data_layer/test_client_db*.py`, and match it) before calling `seed_from_answers` in the test.

- [ ] **Step 4: Implement `scripts/seed_headless_creds.py`**

```python
# scripts/seed_headless_creds.py
"""
One-time interactive script to seed the passwords/TOTP secrets needed by
scripts/auto_morning_start.py's headless login. Run once by hand (on EC2,
or locally against a copy of data/clients.db that's then deployed) --
never part of the daily automation. Prompts are masked (getpass); nothing
typed here is ever printed or logged.

Usage: python scripts/seed_headless_creds.py [--db-path data/clients.db]
"""
from __future__ import annotations

import argparse
import asyncio
import getpass

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

    seed_from_answers(db, answers)
    print("\nDone. Nothing typed above was logged or echoed back.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/scripts/test_seed_headless_creds.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add data_layer/client_db.py scripts/seed_headless_creds.py tests/scripts/test_seed_headless_creds.py
git commit -m "feat(auth): one-time interactive credential seeding script for headless auto-login"
```

---

### Task 5: Email alert helper (Gmail SMTP, built from scratch)

**Files:**
- Create: `utils/email_alert.py`
- Test: `tests/utils/test_email_alert.py`

**Interfaces:**
- Consumes: `ClientDB.get_setting_sync`/a new setting-style getter for the Gmail app password — reuse the existing `system_settings` key-value table (`get_setting_sync(key, default)` already exists per Task exploration) rather than adding new columns anywhere.
- Produces: `def send_summary_email(to_addr: str, subject: str, steps: list[tuple[str, bool, str]]) -> bool` — returns True if the send succeeded (never raises).

- [ ] **Step 1: Write the failing tests**

```python
# tests/utils/test_email_alert.py
from utils.email_alert import send_summary_email, format_summary_body


def test_format_summary_body_shows_all_steps_with_status():
    body = format_summary_body([
        ("pm2 start", True, ""),
        ("Upstox login", True, "token generated 09:14:52 IST"),
        ("Fyers login", False, "Cloudflare challenge page"),
    ])
    assert "pm2 start" in body and "OK" in body
    assert "Fyers login" in body and "FAILED" in body
    assert "Cloudflare challenge page" in body


def test_send_summary_email_returns_false_and_does_not_raise_on_smtp_error(monkeypatch):
    import utils.email_alert as mod

    class BoomSMTP:
        def __init__(self, *a, **k):
            raise ConnectionRefusedError("smtp down")

    monkeypatch.setattr(mod.smtplib, "SMTP_SSL", BoomSMTP)
    monkeypatch.setattr(mod, "_get_gmail_credentials", lambda: ("bot@gmail.com", "app-pw"))

    ok = send_summary_email("user@example.com", "subject", [("step", True, "")])
    assert ok is False


def test_send_summary_email_sends_via_smtp_ssl(monkeypatch):
    import utils.email_alert as mod
    sent = {}

    class FakeSMTP:
        def __init__(self, host, port):
            sent["host"] = host
            sent["port"] = port

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def login(self, user, pw):
            sent["user"] = user
            sent["pw"] = pw

        def send_message(self, msg):
            sent["msg"] = msg

    monkeypatch.setattr(mod.smtplib, "SMTP_SSL", FakeSMTP)
    monkeypatch.setattr(mod, "_get_gmail_credentials", lambda: ("bot@gmail.com", "app-pw"))

    ok = send_summary_email("user@example.com", "subject", [("step", True, "detail")])
    assert ok is True
    assert sent["user"] == "bot@gmail.com"
    assert sent["pw"] == "app-pw"
    assert sent["msg"]["To"] == "user@example.com"
    assert sent["msg"]["Subject"] == "subject"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/utils/test_email_alert.py -v`
Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement `utils/email_alert.py`**

```python
# utils/email_alert.py
"""
Gmail-SMTP email alerting for the unattended morning/evening automation
scripts. Built from scratch -- no prior alerting mechanism existed in this
codebase. The Gmail account + app-specific password are stored the same
way as every other credential in this system (system_settings key-value
table, XOR+PBKDF2 obfuscated) -- never in a script, config file, or env var.

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
    from data_layer.client_db import ClientDB
    db = ClientDB()
    user = db.get_setting_sync(_SETTING_GMAIL_USER, "")
    app_pw = db.get_setting_sync(_SETTING_GMAIL_APP_PASSWORD, "")
    return user, app_pw


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
```

- [ ] **Step 4: Add a `ClientDB` setter for the two new settings + a seeding step**

Confirm `ClientDB` already has `set_setting_sync`/an async setting-writer (grep `def set_setting` in `data_layer/client_db.py`); if present, extend `scripts/seed_headless_creds.py` (Task 4) with one more optional prompt block:

```python
    if input("Seed Gmail alert credentials? [y/N]: ").strip().lower() == "y":
        gmail_user = input("Gmail address to send FROM: ")
        gmail_app_pw = getpass.getpass("Gmail app-specific password: ")
        db.set_setting_sync("auto_alert_gmail_user", gmail_user)
        db.set_setting_sync("auto_alert_gmail_app_password", gmail_app_pw)
```

(If no such setter exists yet, add a minimal one mirroring `get_setting_sync`'s own table access — same file, same pattern, XOR-encode the app password before storing exactly like every other secret.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/utils/test_email_alert.py -v`
Expected: PASS (3 tests)

- [ ] **Step 6: Commit**

```bash
git add utils/email_alert.py scripts/seed_headless_creds.py tests/utils/test_email_alert.py
git commit -m "feat(alerts): add Gmail SMTP email summary alerting for automation scripts"
```

---

### Task 6: Morning orchestration script

**Files:**
- Create: `scripts/auto_morning_start.py`
- Test: `tests/scripts/test_auto_morning_start.py`

**Interfaces:**
- Consumes: `upstox_totp_login`/`zerodha_totp_login`/`HeadlessTotpAuthError` (Task 2/3), `ClientDB.get_feeder_creds_sync`/`update_feeder_token`/`get_bindings_sync`/`update_access_token`/`set_terminal_connected`/`set_trade_enabled` (existing + Task 1), `send_summary_email` (Task 5).
- Produces: `async def run_morning_sequence(dry_run: bool = False) -> list[tuple[str, bool, str]]` — the testable core (returns the same `steps` shape `send_summary_email` consumes); a `main()` CLI entrypoint with `--dry-run`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/scripts/test_auto_morning_start.py
import asyncio
import pytest
from scripts.auto_morning_start import run_morning_sequence


def _ok_step(name):
    return (name, True, "")


@pytest.mark.asyncio
async def test_dry_run_skips_pm2_and_db_writes_but_runs_logins(monkeypatch):
    import scripts.auto_morning_start as mod

    calls = {"pm2": 0, "db_writes": 0}

    async def fake_start_pm2():
        calls["pm2"] += 1
        return True, ""

    async def fake_wait_health():
        return True, ""

    def fake_upstox(**kw):
        return "UPTOKEN"

    def fake_zerodha(**kw):
        return "ZTOKEN"

    class FakeDB:
        def get_feeder_creds_sync(self, provider):
            return {"api_key": "k", "secret": "s", "password": "p", "totp_secret": "JBSWY3DPEHPK3PXP", "client_id": "c"}

        def get_bindings_sync(self, client_id):
            return [{"binding_id": "SA5770", "provider": "zerodha", "api_key": "zk", "api_secret": "zs", "user_id": "zu"}]

        async def update_feeder_token(self, *a, **k):
            calls["db_writes"] += 1

        async def update_access_token(self, *a, **k):
            calls["db_writes"] += 1

        async def set_terminal_connected(self, *a, **k):
            calls["db_writes"] += 1

        async def set_trade_enabled(self, *a, **k):
            calls["db_writes"] += 1

    monkeypatch.setattr(mod, "_start_pm2", fake_start_pm2)
    monkeypatch.setattr(mod, "_wait_for_dashboard_health", fake_wait_health)
    monkeypatch.setattr(mod, "upstox_totp_login", fake_upstox)
    monkeypatch.setattr(mod, "zerodha_totp_login", fake_zerodha)
    monkeypatch.setattr(mod, "ClientDB", lambda: FakeDB())

    steps = await run_morning_sequence(dry_run=True, zerodha_client_id="ssrajpal2001")

    assert calls["pm2"] == 0
    assert calls["db_writes"] == 0
    names_ok = {name: ok for name, ok, _ in steps}
    assert names_ok["Upstox login"] is True
    assert names_ok["Zerodha login (SA5770)"] is True


@pytest.mark.asyncio
async def test_upstox_failure_does_not_block_zerodha_step(monkeypatch):
    import scripts.auto_morning_start as mod
    from broker_auth.headless_totp_auth import HeadlessTotpAuthError

    async def fake_start_pm2():
        return True, ""

    async def fake_wait_health():
        return True, ""

    def fake_upstox(**kw):
        raise HeadlessTotpAuthError("Upstox: Step 4 failed — PIN rejected.")

    def fake_zerodha(**kw):
        return "ZTOKEN"

    class FakeDB:
        def get_feeder_creds_sync(self, provider):
            return {"api_key": "k", "secret": "s", "password": "p", "totp_secret": "JBSWY3DPEHPK3PXP", "client_id": "c"}

        def get_bindings_sync(self, client_id):
            return [{"binding_id": "SA5770", "provider": "zerodha", "api_key": "zk", "api_secret": "zs", "user_id": "zu"}]

    monkeypatch.setattr(mod, "_start_pm2", fake_start_pm2)
    monkeypatch.setattr(mod, "_wait_for_dashboard_health", fake_wait_health)
    monkeypatch.setattr(mod, "upstox_totp_login", fake_upstox)
    monkeypatch.setattr(mod, "zerodha_totp_login", fake_zerodha)
    monkeypatch.setattr(mod, "ClientDB", lambda: FakeDB())

    steps = await run_morning_sequence(dry_run=True, zerodha_client_id="ssrajpal2001")
    names_ok = {name: ok for name, ok, _ in steps}
    assert names_ok["Upstox login"] is False
    assert names_ok["Zerodha login (SA5770)"] is True


@pytest.mark.asyncio
async def test_pm2_failure_skips_all_downstream_steps(monkeypatch):
    import scripts.auto_morning_start as mod

    async def fake_start_pm2():
        return False, "pm2 binary not found"

    monkeypatch.setattr(mod, "_start_pm2", fake_start_pm2)

    steps = await run_morning_sequence(dry_run=True, zerodha_client_id="ssrajpal2001")
    names_ok = {name: ok for name, ok, _ in steps}
    assert names_ok["pm2 start"] is False
    assert "Upstox login" not in names_ok
```

(Add `pytest-asyncio` to test requirements if not already present — check `requirements.txt`/`pyproject.toml` for `pytest.ini`'s `asyncio_mode`; if the repo already runs async tests elsewhere via bare `asyncio.run(...)` inside a sync test function instead of `pytest.mark.asyncio`, match that existing convention rather than introducing a new one — grep `tests/` for `async def test_` to confirm which style this repo already uses before writing this task's tests.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/scripts/test_auto_morning_start.py -v`
Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement `scripts/auto_morning_start.py`**

```python
# scripts/auto_morning_start.py
"""
Unattended daily morning-start orchestrator, run once at EC2 boot via a
systemd oneshot unit (see ops/systemd/auto-morning-start.service). Starts
the app under pm2, headless-logs-in Upstox + Zerodha (best-effort each),
attempts Fyers via Playwright (best-effort, see Task 8), and emails one
summary regardless of outcome. Does NOT explicitly start any strategy --
each strategy's own book manager reconciles off terminal_connected/
trade_enabled/is_running on its existing 5s loop.

Usage:
    python scripts/auto_morning_start.py [--dry-run] [--alert-to you@example.com]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import subprocess
import time
from datetime import datetime
from typing import List, Tuple

import requests

from broker_auth.headless_totp_auth import (
    HeadlessTotpAuthError,
    upstox_totp_login,
    zerodha_totp_login,
)
from config.global_config import IST
from data_layer.client_db import ClientDB
from utils.email_alert import send_summary_email

logger = logging.getLogger(__name__)

_PM2_START_CMD = [
    "pm2", "start", "run_system.py", "--name", "terminus", "--interpreter", "python3",
    "--", "--mode", "live", "--ui", "--port", "5000", "--index", "NIFTY",
]
_DASHBOARD_HEALTH_URL = "http://localhost:5000/"
_HEALTH_TIMEOUT_SEC = 90
_HEALTH_POLL_INTERVAL_SEC = 3


async def _start_pm2() -> Tuple[bool, str]:
    try:
        result = subprocess.run(_PM2_START_CMD, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return False, result.stderr.strip()[:300]
        return True, ""
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/scripts/test_auto_morning_start.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Confirm the real `pm2 start` command against the current EC2 process**

Not a code step — before deploying, run `pm2 describe terminus` (or `pm2 show terminus`) on the actual EC2 instance and compare its `script args` to `_PM2_START_CMD` above; update the constant to match exactly (the placeholder above uses the documented default from this repo's own `CLAUDE.md` "Launch Commands" section — confirm `--strategies` isn't also pinned there today before finalizing).

- [ ] **Step 6: Commit**

```bash
git add scripts/auto_morning_start.py tests/scripts/test_auto_morning_start.py
git commit -m "feat(automation): add unattended morning-start orchestrator (pm2 + Upstox/Zerodha headless login)"
```

---

### Task 7: Manual dry-run + live validation checkpoint (runbook, no code)

**Files:** none (operational checkpoint)

- [ ] **Step 1:** On a real trading morning, after seeding real credentials (Task 4) and a real Gmail app password (Task 5), SSH into EC2 and run:
  ```bash
  python scripts/auto_morning_start.py --dry-run
  ```
  Confirm the emailed summary shows Upstox and Zerodha both `OK`, with no pm2/DB side effects (verify via `pm2 list` showing no new/changed process, and the dashboard's admin feeder panel still showing the token state from before the run).

- [ ] **Step 2:** Run it for real (no `--dry-run`) once, by hand, at approximately the normal trigger time, on a day you are available to watch:
  ```bash
  python scripts/auto_morning_start.py
  ```
  Confirm: pm2 process is up, dashboard reachable, Upstox feeder shows a fresh token in the admin panel, the Zerodha SA5770 binding shows `terminal_connected`/`is_trade_enabled` true in the client dashboard, and any deployment left `is_running=1` picks up ticks/starts evaluating signals within its normal reconcile window.

- [ ] **Step 3:** Only after both checks pass, proceed to Task 10 (systemd wiring) — do not wire the unattended trigger before this checkpoint passes.

---

### Task 8: Best-effort Fyers headless login via Playwright

**Files:**
- Create: `broker_auth/headless_totp_auth_fyers.py`
- Modify: `scripts/auto_morning_start.py` (add the Fyers step)
- Test: `tests/broker_auth/test_headless_totp_auth_fyers.py`

**Interfaces:**
- Produces: `def fyers_totp_login(client_id: str, app_id: str, password: str, totp_secret: str, pin: str) -> str` — same raise-on-failure contract as the other two providers, but explicitly documented as best-effort/may not work reliably (Cloudflare).

- [ ] **Step 1: Add `playwright` to dependencies**

Add to `requirements.txt` (or the repo's equivalent dependency file — confirm exact filename first): `playwright>=1.40.0`. Note in a comment above it: `# Fyers headless login only — best-effort, see broker_auth/headless_totp_auth_fyers.py`. After adding, the implementer must run `playwright install chromium` once on both the dev machine and EC2 (not a pytest-covered step — record this in the task's own commit message so it isn't forgotten at deploy time).

- [ ] **Step 2: Write the failing tests (structure + error-path only — the real Playwright browser flow is not unit-testable, matching the spec's own call on this)**

```python
# tests/broker_auth/test_headless_totp_auth_fyers.py
import pytest
from broker_auth.headless_totp_auth_fyers import FyersHeadlessLoginError, fyers_totp_login


def test_fyers_missing_client_id_raises():
    with pytest.raises(FyersHeadlessLoginError, match="client_id"):
        fyers_totp_login(client_id="", app_id="a", password="p", totp_secret="JBSWY3DPEHPK3PXP", pin="1234")


def test_fyers_missing_totp_secret_raises():
    with pytest.raises(FyersHeadlessLoginError, match="totp_secret"):
        fyers_totp_login(client_id="c", app_id="a", password="p", totp_secret="", pin="1234")


def test_fyers_playwright_timeout_raises_named_error(monkeypatch):
    import broker_auth.headless_totp_auth_fyers as mod

    class FakePage:
        def goto(self, *a, **k):
            pass

        def fill(self, *a, **k):
            raise TimeoutError("locator not found")

    class FakeBrowser:
        def new_page(self):
            return FakePage()

        def close(self):
            pass

    class FakeChromium:
        def launch(self, **k):
            return FakeBrowser()

    class FakePlaywrightCtx:
        def __enter__(self):
            class PW:
                chromium = FakeChromium()
            return PW()

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(mod, "sync_playwright", lambda: FakePlaywrightCtx())

    with pytest.raises(FyersHeadlessLoginError, match="locator not found"):
        fyers_totp_login(client_id="c", app_id="a", password="p", totp_secret="JBSWY3DPEHPK3PXP", pin="1234")
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/broker_auth/test_headless_totp_auth_fyers.py -v`
Expected: FAIL — module doesn't exist.

- [ ] **Step 4: Implement `broker_auth/headless_totp_auth_fyers.py`**

```python
# broker_auth/headless_totp_auth_fyers.py
"""
Best-effort Fyers headless login via Playwright (real browser automation).

Fyers' own vagator API was found Cloudflare-blocked even in this repo's
pre-refactor code (see docs/superpowers/specs/2026-09-03-fully-automated-
daily-lifecycle-design.md, "Open risks") -- this module attempts a real
headless-browser login instead, per direct user decision to try anyway.
Explicitly allowed to fail; scripts/auto_morning_start.py treats this as a
best-effort step that never blocks Upstox/Zerodha or strategy resume.
"""
from __future__ import annotations

from playwright.sync_api import sync_playwright


class FyersHeadlessLoginError(Exception):
    """Fyers headless login failed at a specific step (often Cloudflare)."""


def fyers_totp_login(client_id: str, app_id: str, password: str, totp_secret: str, pin: str) -> str:
    import pyotp

    if not client_id:
        raise FyersHeadlessLoginError("Fyers: client_id is required.")
    if not totp_secret:
        raise FyersHeadlessLoginError("Fyers: totp_secret is required.")

    try:
        totp_code = pyotp.TOTP(totp_secret.upper().replace(" ", "").replace("-", "")).now()
    except Exception as exc:
        raise FyersHeadlessLoginError(f"Fyers: invalid TOTP secret — {exc}")

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.goto("https://login.fyers.in/")
                page.fill("input[id='fy_client_id']", client_id)
                page.click("button[id='clientIdSubmit']")
                page.fill("input[id='fy_totp']", totp_code)
                page.click("button[id='totpSubmit']")
                page.fill("input[id='fy_pin']", pin)
                page.click("button[id='pinSubmit']")
                page.wait_for_url("**/api-login/redirect-uri/**", timeout=20000)
                final_url = page.url
                from urllib.parse import parse_qs, urlparse
                qs = parse_qs(urlparse(final_url).query)
                auth_code = (qs.get("auth_code") or [""])[0]
                if not auth_code:
                    raise FyersHeadlessLoginError(f"Fyers: auth_code not found in redirect URL {final_url!r}")
                from fyers_apiv3 import fyersModel
                session = fyersModel.SessionModel(
                    client_id=app_id, secret_key="", redirect_uri="",
                    response_type="code", grant_type="authorization_code",
                )
                session.set_token(auth_code)
                resp = session.generate_token()
                access_token = resp.get("access_token", "")
                if not access_token:
                    raise FyersHeadlessLoginError(f"Fyers: token exchange failed — {resp}")
                return access_token
            finally:
                browser.close()
    except FyersHeadlessLoginError:
        raise
    except Exception as exc:
        raise FyersHeadlessLoginError(str(exc))
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/broker_auth/test_headless_totp_auth_fyers.py -v`
Expected: PASS (3 tests)

- [ ] **Step 6: Wire the best-effort Fyers step into `run_morning_sequence`**

In `scripts/auto_morning_start.py`, add after the Zerodha block, before the "Strategies auto-resume" step:

```python
    # -- Fyers (best-effort — see broker_auth/headless_totp_auth_fyers.py) --
    try:
        from broker_auth.headless_totp_auth_fyers import FyersHeadlessLoginError, fyers_totp_login
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
```

- [ ] **Step 7: Add a regression test confirming a Fyers failure never blocks the "Strategies auto-resume" step**

Append to `tests/scripts/test_auto_morning_start.py`:

```python
@pytest.mark.asyncio
async def test_fyers_failure_does_not_block_strategy_resume_step(monkeypatch):
    import scripts.auto_morning_start as mod
    from broker_auth.headless_totp_auth_fyers import FyersHeadlessLoginError

    async def fake_start_pm2():
        return True, ""

    async def fake_wait_health():
        return True, ""

    def fake_upstox(**kw):
        return "UPTOKEN"

    def fake_zerodha(**kw):
        return "ZTOKEN"

    def fake_fyers(**kw):
        raise FyersHeadlessLoginError("Fyers: Cloudflare challenge page")

    class FakeDB:
        def get_feeder_creds_sync(self, provider):
            return {"api_key": "k", "secret": "s", "password": "p", "totp_secret": "JBSWY3DPEHPK3PXP", "client_id": "c"}

        def get_bindings_sync(self, client_id):
            return [{"binding_id": "SA5770", "provider": "zerodha", "api_key": "zk", "api_secret": "zs", "user_id": "zu"}]

    monkeypatch.setattr(mod, "_start_pm2", fake_start_pm2)
    monkeypatch.setattr(mod, "_wait_for_dashboard_health", fake_wait_health)
    monkeypatch.setattr(mod, "upstox_totp_login", fake_upstox)
    monkeypatch.setattr(mod, "zerodha_totp_login", fake_zerodha)
    monkeypatch.setattr(mod, "ClientDB", lambda: FakeDB())
    import broker_auth.headless_totp_auth_fyers as fyers_mod
    monkeypatch.setattr(fyers_mod, "fyers_totp_login", fake_fyers)

    steps = await mod.run_morning_sequence(dry_run=True, zerodha_client_id="ssrajpal2001")
    names_ok = {name: ok for name, ok, _ in steps}
    assert names_ok["Fyers login (best-effort)"] is False
    assert names_ok["Strategies auto-resume"] is True
```

Run: `pytest tests/scripts/test_auto_morning_start.py -v`
Expected: PASS (4 tests total)

- [ ] **Step 8: Commit**

```bash
git add broker_auth/headless_totp_auth_fyers.py scripts/auto_morning_start.py tests/broker_auth/test_headless_totp_auth_fyers.py tests/scripts/test_auto_morning_start.py requirements.txt
git commit -m "feat(auth): best-effort Fyers headless login via Playwright, wired as a non-blocking step"
```

---

### Task 9: Evening shutdown script

**Files:**
- Create: `scripts/auto_evening_stop.py`
- Test: `tests/scripts/test_auto_evening_stop.py`

**Interfaces:**
- Consumes: `send_summary_email` (Task 5).
- Produces: `async def run_evening_sequence(dry_run: bool = False) -> list[tuple[str, bool, str]]`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/scripts/test_auto_evening_stop.py
import pytest
from scripts.auto_evening_stop import run_evening_sequence


@pytest.mark.asyncio
async def test_evening_sequence_stops_pm2(monkeypatch):
    import scripts.auto_evening_stop as mod
    calls = {"n": 0}

    async def fake_stop_pm2():
        calls["n"] += 1
        return True, ""

    monkeypatch.setattr(mod, "_stop_pm2", fake_stop_pm2)
    steps = await run_evening_sequence()
    assert calls["n"] == 1
    names_ok = {name: ok for name, ok, _ in steps}
    assert names_ok["pm2 stop"] is True


@pytest.mark.asyncio
async def test_evening_sequence_dry_run_does_not_call_pm2(monkeypatch):
    import scripts.auto_evening_stop as mod
    calls = {"n": 0}

    async def fake_stop_pm2():
        calls["n"] += 1
        return True, ""

    monkeypatch.setattr(mod, "_stop_pm2", fake_stop_pm2)
    await run_evening_sequence(dry_run=True)
    assert calls["n"] == 0


@pytest.mark.asyncio
async def test_evening_sequence_reports_pm2_failure(monkeypatch):
    import scripts.auto_evening_stop as mod

    async def fake_stop_pm2():
        return False, "pm2 process not found"

    monkeypatch.setattr(mod, "_stop_pm2", fake_stop_pm2)
    steps = await run_evening_sequence()
    names_ok = {name: (ok, detail) for name, ok, detail in steps}
    assert names_ok["pm2 stop"] == (False, "pm2 process not found")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/scripts/test_auto_evening_stop.py -v`
Expected: FAIL — module doesn't exist.

- [ ] **Step 3: Implement `scripts/auto_evening_stop.py`**

```python
# scripts/auto_evening_stop.py
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
import subprocess
from datetime import datetime
from typing import List, Tuple

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/scripts/test_auto_evening_stop.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add scripts/auto_evening_stop.py tests/scripts/test_auto_evening_stop.py
git commit -m "feat(automation): add unattended evening-stop script (pm2 stop only, trusts existing force-exits)"
```

---

### Task 10: systemd units on EC2

**Files:**
- Create: `ops/systemd/auto-morning-start.service`, `ops/systemd/auto-evening-stop.service`, `ops/systemd/auto-evening-stop.timer`, `ops/systemd/README.md`

**Interfaces:** none (deployment config, not Python).

- [ ] **Step 1: Write the morning oneshot unit (boot-triggered, not time-triggered — the EventBridge Lambda controls WHEN the instance boots, this just runs once boot completes)**

```ini
# ops/systemd/auto-morning-start.service
[Unit]
Description=Automated daily morning start (feeders/broker headless login)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
WorkingDirectory=/home/ubuntu/OptionChainBasedStrategy
ExecStartPre=/bin/sleep 60
ExecStart=/usr/bin/python3 scripts/auto_morning_start.py
User=ubuntu
StandardOutput=append:/var/log/auto_morning_start.log
StandardError=append:/var/log/auto_morning_start.log

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: Write the evening stop unit + its own timer (time-triggered, since the box stays up until EC2's own stop, unlike the morning oneshot)**

```ini
# ops/systemd/auto-evening-stop.service
[Unit]
Description=Automated daily evening stop (pm2 stop only)

[Service]
Type=oneshot
WorkingDirectory=/home/ubuntu/OptionChainBasedStrategy
ExecStart=/usr/bin/python3 scripts/auto_evening_stop.py
User=ubuntu
StandardOutput=append:/var/log/auto_evening_stop.log
StandardError=append:/var/log/auto_evening_stop.log
```

```ini
# ops/systemd/auto-evening-stop.timer
[Unit]
Description=Trigger auto-evening-stop.service at 15:58 IST on weekdays

[Timer]
OnCalendar=Mon..Fri 15:58:00 Asia/Kolkata
Persistent=false

[Install]
WantedBy=timers.target
```

- [ ] **Step 3: Write the install instructions**

```markdown
# ops/systemd/README.md

## Install (run once on EC2)

    sudo cp ops/systemd/auto-morning-start.service /etc/systemd/system/
    sudo cp ops/systemd/auto-evening-stop.service /etc/systemd/system/
    sudo cp ops/systemd/auto-evening-stop.timer /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable auto-morning-start.service
    sudo systemctl enable --now auto-evening-stop.timer

## Verify

    systemctl status auto-morning-start.service
    systemctl list-timers auto-evening-stop.timer
    tail -f /var/log/auto_morning_start.log
    tail -f /var/log/auto_evening_stop.log

## Uninstall / pause automation

    sudo systemctl disable auto-morning-start.service
    sudo systemctl disable --now auto-evening-stop.timer
```

- [ ] **Step 4: Manual verification (no pytest — this is OS-level config)**

On EC2, run the install steps above, then either wait for a real reboot or run `sudo systemctl start auto-morning-start.service` manually and confirm `/var/log/auto_morning_start.log` shows the same output Task 7's manual run produced.

- [ ] **Step 5: Commit**

```bash
git add ops/systemd/
git commit -m "ops: add systemd units for unattended morning-start/evening-stop on EC2"
```

---

### Task 11: AWS EventBridge + Lambda for EC2 start/stop

**Files:**
- Create: `ops/aws/ec2_scheduler.yaml`, `ops/aws/README.md`

**Interfaces:** none (AWS infra config, not part of this repo's runtime).

- [ ] **Step 1: Write the CloudFormation template**

```yaml
# ops/aws/ec2_scheduler.yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: >
  Daily EC2 start/stop schedule for the trading instance (09:00/16:00 IST,
  weekdays only). See docs/superpowers/specs/2026-09-03-fully-automated-
  daily-lifecycle-design.md.

Parameters:
  InstanceId:
    Type: String
    Description: The EC2 instance ID to start/stop daily.

Resources:
  Ec2SchedulerRole:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal: {Service: lambda.amazonaws.com}
            Action: sts:AssumeRole
      Policies:
        - PolicyName: Ec2StartStopOnly
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
              - Effect: Allow
                Action: [ec2:StartInstances, ec2:StopInstances]
                Resource: !Sub "arn:aws:ec2:${AWS::Region}:${AWS::AccountId}:instance/${InstanceId}"
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

  Ec2StartStopFunction:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: trading-ec2-start-stop
      Runtime: python3.12
      Handler: index.handler
      Role: !GetAtt Ec2SchedulerRole.Arn
      Timeout: 30
      Environment:
        Variables:
          INSTANCE_ID: !Ref InstanceId
      Code:
        ZipFile: |
          import os
          import boto3

          def handler(event, context):
              ec2 = boto3.client("ec2")
              instance_id = os.environ["INSTANCE_ID"]
              action = event.get("action")
              if action == "start":
                  ec2.start_instances(InstanceIds=[instance_id])
              elif action == "stop":
                  ec2.stop_instances(InstanceIds=[instance_id])
              else:
                  raise ValueError(f"unknown action: {action}")
              return {"ok": True, "action": action, "instance_id": instance_id}

  StartRule:
    Type: AWS::Events::Rule
    Properties:
      Name: trading-ec2-start-0900-ist
      ScheduleExpression: "cron(30 3 ? * MON-FRI *)"  # 09:00 IST = 03:30 UTC
      State: ENABLED
      Targets:
        - Id: StartTarget
          Arn: !GetAtt Ec2StartStopFunction.Arn
          Input: '{"action": "start"}'

  StopRule:
    Type: AWS::Events::Rule
    Properties:
      Name: trading-ec2-stop-1600-ist
      ScheduleExpression: "cron(30 10 ? * MON-FRI *)"  # 16:00 IST = 10:30 UTC
      State: ENABLED
      Targets:
        - Id: StopTarget
          Arn: !GetAtt Ec2StartStopFunction.Arn
          Input: '{"action": "stop"}'

  StartRulePermission:
    Type: AWS::Lambda::Permission
    Properties:
      FunctionName: !Ref Ec2StartStopFunction
      Action: lambda:InvokeFunction
      Principal: events.amazonaws.com
      SourceArn: !GetAtt StartRule.Arn

  StopRulePermission:
    Type: AWS::Lambda::Permission
    Properties:
      FunctionName: !Ref Ec2StartStopFunction
      Action: lambda:InvokeFunction
      Principal: events.amazonaws.com
      SourceArn: !GetAtt StopRule.Arn
```

Note: EventBridge's `cron()` expressions run in UTC only — IST is UTC+5:30, so 09:00 IST is 03:30 UTC same day, and 16:00 IST is 10:30 UTC same day (comments in the template above document this; do not "fix" these to look like IST times, they are correct as UTC).

- [ ] **Step 2: Write the deployment README**

```markdown
# ops/aws/README.md

## Deploy

    aws cloudformation deploy \
      --template-file ops/aws/ec2_scheduler.yaml \
      --stack-name trading-ec2-scheduler \
      --parameter-overrides InstanceId=i-XXXXXXXXXXXXXXXXX \
      --capabilities CAPABILITY_IAM

## Verify

    aws events list-rules --name-prefix trading-ec2
    aws lambda invoke --function-name trading-ec2-start-stop \
      --payload '{"action":"start"}' /tmp/out.json && cat /tmp/out.json

## Pause (without deleting)

    aws events disable-rule --name trading-ec2-start-0900-ist
    aws events disable-rule --name trading-ec2-stop-1600-ist

## Re-enable

    aws events enable-rule --name trading-ec2-start-0900-ist
    aws events enable-rule --name trading-ec2-stop-1600-ist

## Remove entirely

    aws cloudformation delete-stack --stack-name trading-ec2-scheduler
```

- [ ] **Step 3: Manual verification**

Deploy the stack with the real instance ID, manually invoke the Lambda with `{"action":"start"}` while the instance is stopped, confirm via `aws ec2 describe-instances` that it transitions to `running`. Repeat with `{"action":"stop"}`. Only after both are confirmed working should the rules be left enabled to run on their real schedule.

- [ ] **Step 4: Commit**

```bash
git add ops/aws/
git commit -m "ops: add CloudFormation template for EC2 daily start/stop scheduling (EventBridge + Lambda)"
```

---

### Task 12: Full end-to-end go-live checklist (runbook, no code)

**Files:** none

- [ ] **Step 1:** Pick one full trading day where the user is available to watch closely (not the first day this runs unattended-and-unwatched).

- [ ] **Step 2:** Leave the EventBridge rules (Task 11) enabled. Leave the systemd units (Task 10) enabled. Do nothing manually that morning.

- [ ] **Step 3:** At 09:00 IST, confirm via AWS console the instance transitions to `running`. By ~09:16 IST, confirm the summary email arrives and shows the expected step outcomes (Upstox/Zerodha OK at minimum; Fyers may legitimately fail per its best-effort status).

- [ ] **Step 4:** Confirm in the dashboard: feeder status shows fresh tokens, the Zerodha SA5770 binding shows connected/trade-enabled, and any `is_running=1` deployment is actively evaluating (check its own strategy log for the day's first real tick/evaluation).

- [ ] **Step 5:** At 16:00 IST, confirm the evening summary email arrives, `pm2 list` (checked just before instance stop, e.g. via a quick SSH in) shows the process stopped, and the instance transitions to `stopped` in the AWS console shortly after.

- [ ] **Step 6:** Only once this full day is confirmed clean end-to-end should the user stop manually watching subsequent days — from here on, a failure surfaces via the email summary, not silence.
