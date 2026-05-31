"""
broker_auth/oauth_manager.py — Interactive OAuth URL generation + token exchange.

All 6 supported providers use browser-redirect OAuth:
  fyers     — OAuth2 code flow via api-t1.fyers.in
  upstox    — OAuth2 code flow via api.upstox.com
  zerodha   — Kite Connect OAuth via kite.zerodha.com
  dhan      — 3-step consent flow via auth.dhan.co
  angelone  — implicit redirect via smartapi.angelone.in/publisher-login/
  aliceblue — OAuth2 + SHA256 checksum via ant.aliceblueonline.com
  mock      — always succeeds instantly

NOT supported this phase: shoonya, groww

NO passwords, PINs, or TOTP secrets are ever accepted, stored, or processed here.
All identity verification happens on the broker's official secure portal.

Callback routing strategy:
  Fyers / Upstox / Zerodha / AliceBlue : state in ?state= query param
  AngelOne  : state embedded in redirect_url path → /callback/angelone/{state}
  Dhan      : no state; callback handler calls consume_dhan_consent() which returns
              dhanClientId → DB lookup to identify the binding

Execution time logging is emitted at every step for latency monitoring.
"""

from __future__ import annotations

import logging
import time
from typing import Optional, Tuple
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

# ── Provider capability registry ─────────────────────────────────────────────

_OAUTH_PROVIDERS = {"fyers", "upstox", "zerodha", "dhan", "angelone", "aliceblue"}
_MANUAL_TOKEN_PROVIDERS: set = set()  # empty — all supported brokers now use OAuth redirect


def supports_oauth(provider: str) -> bool:
    return provider.lower() in _OAUTH_PROVIDERS


def requires_manual_token(provider: str) -> bool:
    return provider.lower() in _MANUAL_TOKEN_PROVIDERS


# ── Auth URL Generation ───────────────────────────────────────────────────────

def generate_auth_url(
    provider:     str,
    api_key:      str,
    api_secret:   str,
    callback_url: str,
    state:        str,
    user_id:      str = "",
) -> Tuple[bool, str]:
    """
    Generate the broker's official OAuth authorization URL.

    For Dhan: makes a synchronous HTTP call to generate-consent before returning URL.
              Call this via asyncio.to_thread() from async contexts.

    Returns (ok, url_or_error_message).
    """
    t0 = time.monotonic()
    p = provider.lower()

    if p == "fyers":
        url = _fyers_auth_url(api_key, callback_url, state)
        logger.info("[OAuth] Fyers auth URL generated in %.1fms", (time.monotonic()-t0)*1000)
        return True, url

    elif p == "upstox":
        url = _upstox_auth_url(api_key, callback_url, state)
        logger.info("[OAuth] Upstox auth URL generated in %.1fms", (time.monotonic()-t0)*1000)
        return True, url

    elif p == "zerodha":
        url = _zerodha_auth_url(api_key)
        logger.info("[OAuth] Zerodha auth URL generated in %.1fms", (time.monotonic()-t0)*1000)
        return True, url

    elif p == "dhan":
        # Requires server-side HTTP call to generate consentAppId
        url = _dhan_auth_url(api_key, api_secret, user_id)
        elapsed = (time.monotonic()-t0)*1000
        if url:
            logger.info("[OAuth] Dhan consent generated + auth URL ready in %.1fms", elapsed)
            return True, url
        logger.error("[OAuth] Dhan consent generation FAILED in %.1fms", elapsed)
        return False, "Dhan consent generation failed. Check your App ID, App Secret, and Client ID."

    elif p == "angelone":
        url = _angelone_auth_url(api_key, callback_url, state)
        logger.info("[OAuth] AngelOne auth URL generated in %.1fms", (time.monotonic()-t0)*1000)
        return True, url

    elif p == "aliceblue":
        url = _aliceblue_auth_url(api_key, state)
        logger.info("[OAuth] AliceBlue auth URL generated in %.1fms", (time.monotonic()-t0)*1000)
        return True, url

    elif p == "mock":
        return True, "mock://auto"

    return False, f"Unknown provider '{provider}'. Cannot generate auth URL."


# ── Per-provider URL builders ─────────────────────────────────────────────────

def _fyers_auth_url(app_id: str, redirect_uri: str, state: str) -> str:
    if "-" not in app_id:
        app_id = f"{app_id}-100"
    params = {
        "client_id":     app_id,
        "redirect_uri":  redirect_uri,
        "response_type": "code",
        "state":         state,
    }
    return f"https://api-t1.fyers.in/api/v3/generate-authcode?{urlencode(params)}"


def _upstox_auth_url(api_key: str, redirect_uri: str, state: str) -> str:
    params = {
        "response_type": "code",
        "client_id":     api_key,
        "redirect_uri":  redirect_uri,
        "state":         state,
    }
    return f"https://api.upstox.com/v2/login/authorization/dialog?{urlencode(params)}"


def _zerodha_auth_url(api_key: str) -> str:
    return f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"


def _dhan_auth_url(app_id: str, app_secret: str, dhan_client_id: str) -> str:
    """
    Step 1 of Dhan 3-step flow: POST generate-consent → get consentAppId.
    Returns the browser login URL, or empty string on failure.
    Makes a synchronous HTTP call — wrap in asyncio.to_thread().
    """
    import requests
    try:
        r = requests.post(
            f"https://auth.dhan.co/app/generate-consent?client_id={dhan_client_id}",
            headers={"app_id": app_id, "app_secret": app_secret, "Content-Type": "application/json"},
            timeout=10,
        )
        data = r.json() if r.ok else {}
        consent_id = data.get("consentAppId", "")
        if consent_id:
            return f"https://auth.dhan.co/login/consentApp-login?consentAppId={consent_id}"
        logger.error("[OAuth] Dhan generate-consent response: %s", data)
        return ""
    except Exception as exc:
        logger.error("[OAuth] Dhan generate-consent error: %s", exc)
        return ""


def _angelone_auth_url(api_key: str, callback_url: str, state: str) -> str:
    """
    AngelOne implicit redirect flow.
    redirect_url must EXACTLY match the URL registered in the AngelOne developer portal.
    State is passed as ?state= so AngelOne returns it unchanged in the callback.
    callback_url should be https://<server>/callback/angelone (no trailing slash, no state suffix).
    """
    base_url = callback_url.rstrip("/")
    return (
        f"https://smartapi.angelone.in/publisher-login/"
        f"?api_key={api_key}&state={state}&redirect_url={base_url}"
    )


def _aliceblue_auth_url(app_code: str, state: str) -> str:
    """
    AliceBlue login URL. app_code = the AliceBlue appcode (api_key field).
    state is appended for correlation; AliceBlue may or may not return it.
    Callback routing falls back to userId-based DB lookup if state is absent.
    """
    return f"https://ant.aliceblueonline.com/?appcode={app_code}"


# ── Token Exchange ────────────────────────────────────────────────────────────

def exchange_code(
    provider:     str,
    api_key:      str,
    api_secret:   str,
    auth_code:    str,
    callback_url: str,
    extra:        Optional[dict] = None,
) -> Tuple[bool, str, str]:
    """
    Exchange OAuth auth_code for an access_token.

    Returns (ok, message, access_token).
    extra: dict of additional params needed by some providers:
      AliceBlue: {"user_id": "<aliceblue_userId_from_callback>"}

    Called by the /callback/{broker} route handler immediately after redirect.
    """
    import requests
    t0 = time.monotonic()
    p = provider.lower()
    _extra = extra or {}

    try:
        if p == "fyers":
            ok, msg, token = _fyers_exchange(api_key, api_secret, auth_code, callback_url)
        elif p == "upstox":
            ok, msg, token = _upstox_exchange(api_key, api_secret, auth_code, callback_url)
        elif p == "zerodha":
            ok, msg, token = _zerodha_exchange(api_key, api_secret, auth_code)
        elif p == "angelone":
            # Implicit flow — auth_code IS the access token
            ok, msg, token = _angelone_exchange(auth_code)
        elif p == "aliceblue":
            user_id = _extra.get("user_id", "")
            ok, msg, token = _aliceblue_exchange(api_key, api_secret, auth_code, user_id)
        elif p == "mock":
            ok, msg, token = True, "Mock token generated.", "mock_token_ok"
        else:
            return False, f"Provider '{provider}' does not support OAuth code exchange.", ""

        elapsed = (time.monotonic() - t0) * 1000
        status = "SUCCESS" if ok else "FAILED"
        logger.info(
            "[OAuth] %s exchange %s in %.1fms — %s",
            p.upper(), status, elapsed, msg,
        )
        return ok, msg, token

    except Exception as exc:
        logger.error("[OAuth] %s exchange error: %s", p, exc)
        return False, f"Token exchange error: {exc}", ""


def consume_dhan_consent(
    app_id:     str,
    app_secret: str,
    token_id:   str,
) -> Tuple[bool, str, str, str]:
    """
    Step 3 of Dhan 3-step flow: POST consumeApp-consent.

    Returns (ok, message, access_token, dhan_client_id).
    dhan_client_id is used by the callback handler to identify which binding this token belongs to.
    Makes a synchronous HTTP call — wrap in asyncio.to_thread().
    """
    import requests
    t0 = time.monotonic()
    try:
        r = requests.post(
            f"https://auth.dhan.co/app/consumeApp-consent?tokenId={token_id}",
            headers={"app_id": app_id, "app_secret": app_secret, "Content-Type": "application/json"},
            timeout=10,
        )
        data = r.json() if r.ok else {}
        access_token = data.get("accessToken", "")
        dhan_client_id = data.get("dhanClientId", "")
        elapsed = (time.monotonic() - t0) * 1000
        if access_token:
            logger.info(
                "[OAuth] Dhan consumeApp-consent SUCCESS — clientId=%s in %.1fms",
                dhan_client_id, elapsed,
            )
            return True, "Dhan access token obtained.", access_token, dhan_client_id
        msg = data.get("message") or str(data)
        logger.error("[OAuth] Dhan consumeApp-consent FAILED in %.1fms — %s", elapsed, msg)
        return False, f"Dhan consent exchange failed: {msg}", "", ""
    except Exception as exc:
        logger.error("[OAuth] Dhan consumeApp-consent error: %s", exc)
        return False, f"Dhan consent exchange error: {exc}", "", ""


# ── Per-provider exchange functions ───────────────────────────────────────────

def _fyers_exchange(
    app_id: str, secret: str, auth_code: str, redirect_uri: str
) -> Tuple[bool, str, str]:
    import requests, hashlib
    if "-" not in app_id:
        app_id = f"{app_id}-100"
    app_id_hash = hashlib.sha256(f"{app_id}:{secret}".encode()).hexdigest()
    r = requests.post(
        "https://api-t1.fyers.in/api/v3/validate-authcode",
        json={
            "grant_type": "authorization_code",
            "appIdHash":  app_id_hash,
            "code":       auth_code,
        },
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    d = r.json() if r.ok else {}
    token = d.get("access_token") or (d.get("data") or {}).get("access_token")
    if token:
        return True, "Fyers access token obtained.", token
    return False, d.get("message") or str(d), ""


def _upstox_exchange(
    api_key: str, api_secret: str, auth_code: str, redirect_uri: str
) -> Tuple[bool, str, str]:
    import requests
    r = requests.post(
        "https://api.upstox.com/v2/login/authorization/token",
        data={
            "code":          auth_code,
            "client_id":     api_key,
            "client_secret": api_secret,
            "redirect_uri":  redirect_uri,
            "grant_type":    "authorization_code",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        timeout=10,
    )
    d = r.json() if r.ok else {}
    token = d.get("access_token") or (d.get("data") or {}).get("access_token")
    if token:
        return True, "Upstox access token obtained.", token
    return False, d.get("message") or d.get("errors") or str(d), ""


def _zerodha_exchange(
    api_key: str, api_secret: str, request_token: str
) -> Tuple[bool, str, str]:
    try:
        from kiteconnect import KiteConnect  # type: ignore
        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=api_secret)
        token = data.get("access_token", "")
        if token:
            return True, "Zerodha access token obtained.", token
        return False, "Zerodha: access_token missing in response.", ""
    except ImportError:
        return False, "kiteconnect not installed. pip install kiteconnect", ""
    except Exception as exc:
        return False, str(exc), ""


def _angelone_exchange(auth_token: str) -> Tuple[bool, str, str]:
    """AngelOne implicit flow — the callback delivers the access token directly."""
    if not auth_token:
        return False, "AngelOne: auth_token missing in callback.", ""
    return True, "AngelOne access token obtained.", auth_token


def _aliceblue_exchange(
    app_code: str, api_secret: str, auth_code: str, user_id: str
) -> Tuple[bool, str, str]:
    """
    AliceBlue OAuth exchange via SHA-256 checksum.
    user_id = userId returned from AliceBlue callback.
    Returns userSession as access token.
    """
    import requests, hashlib
    if not user_id:
        return False, "AliceBlue: userId missing from callback.", ""
    checksum = hashlib.sha256(f"{user_id}{auth_code}{api_secret}".encode()).hexdigest()
    try:
        r = requests.post(
            "https://a3.aliceblueonline.com/open-api/od/v1/vendor/getUserDetails",
            json={"checkSum": checksum},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        d = r.json() if r.ok else {}
        token = d.get("userSession") or d.get("session")
        if token:
            return True, "AliceBlue user session obtained.", token
        msg = d.get("message") or d.get("emsg") or str(d)
        return False, f"AliceBlue exchange failed: {msg}", ""
    except Exception as exc:
        return False, f"AliceBlue exchange error: {exc}", ""


# ── Token Validation ──────────────────────────────────────────────────────────

def validate_token(
    provider:     str,
    api_key:      str,
    access_token: str,
) -> bool:
    """
    Lightweight API ping to confirm access_token is still valid.
    Returns True if valid, False otherwise.
    Designed to complete in < 300ms.
    """
    import requests
    t0 = time.monotonic()
    p = provider.lower()

    try:
        if p == "fyers":
            r = requests.get(
                "https://api-t1.fyers.in/api/v3/profile",
                headers={"Authorization": f"{api_key}:{access_token}"},
                timeout=5,
            )
            ok = r.status_code == 200 and (r.json() or {}).get("s") == "ok"

        elif p == "upstox":
            r = requests.get(
                "https://api.upstox.com/v2/user/profile",
                headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
                timeout=5,
            )
            ok = r.status_code == 200

        elif p == "zerodha":
            try:
                from kiteconnect import KiteConnect  # type: ignore
                kite = KiteConnect(api_key=api_key)
                kite.set_access_token(access_token)
                profile = kite.profile()
                ok = bool(profile.get("user_id"))
            except Exception:
                ok = False

        elif p == "dhan":
            r = requests.get(
                "https://api.dhan.co/v2/fundlimit",
                headers={"access-token": access_token, "Content-Type": "application/json"},
                timeout=5,
            )
            ok = r.status_code == 200

        elif p == "angelone":
            r = requests.get(
                "https://apiconnect.angelone.in/rest/secure/angelbroking/user/v1/getProfile",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "X-UserType": "USER", "X-SourceID": "WEB",
                    "X-ClientLocalIP": "127.0.0.1",
                    "X-ClientPublicIP": "127.0.0.1",
                    "X-MACAddress": "00:00:00:00:00:00",
                },
                timeout=5,
            )
            ok = r.status_code == 200

        elif p == "aliceblue":
            # AliceBlue userSession validation via profile endpoint
            r = requests.get(
                "https://a3.aliceblueonline.com/open-api/od/v1/client/web/profile",
                headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                timeout=5,
            )
            ok = r.status_code == 200

        elif p == "mock":
            ok = bool(access_token)

        else:
            ok = bool(access_token and len(access_token) > 10)

        elapsed = (time.monotonic() - t0) * 1000
        logger.info(
            "[OAuth] %s token validation: %s in %.1fms",
            p.upper(), "VALID" if ok else "INVALID", elapsed,
        )
        return ok

    except Exception as exc:
        logger.warning("[OAuth] %s validate_token error: %s", p, exc)
        return False


# ── State helpers ─────────────────────────────────────────────────────────────

def build_state(role: str, client_id: str, binding_id: str) -> str:
    """
    Build a URL-safe state parameter for OAuth CSRF protection.
    Format: {role}|{client_id}|{binding_id}|{timestamp}
    """
    import base64
    raw = f"{role}|{client_id}|{binding_id}|{int(time.time())}"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def parse_state(state: str) -> dict:
    """
    Decode state parameter. Returns dict with role, client_id, binding_id, ts.
    Returns empty dict on failure.
    """
    import base64
    try:
        pad = 4 - len(state) % 4
        decoded = base64.urlsafe_b64decode(state + "=" * pad).decode()
        parts = decoded.split("|")
        if len(parts) < 3:
            return {}
        return {
            "role":       parts[0],
            "client_id":  parts[1],
            "binding_id": parts[2],
            "ts":         int(parts[3]) if len(parts) > 3 else 0,
        }
    except Exception:
        return {}
