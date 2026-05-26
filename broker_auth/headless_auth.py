"""
broker_auth/headless_auth.py — Headless background TOTP authentication engine.

Generates daily session tokens for all supported brokers without any
browser interaction.  Called at system startup, on start_bot, and on
token-expiry detected during pre-flight validation.

Auth strategy per provider:
  mock      -- always succeeds
  shoonya   -- direct NorenAPI login with user_id + password + TOTP.now()
  angelone  -- SmartConnect.generateSession with client_code + password + TOTP.now()
  fyers     -- authCodeModel.generate_authcode(fy_id, TOTP, pin) + generate_token()
  upstox    -- aiohttp headless OAuth2 using Upstox internal auth endpoints
  dhan      -- validates existing access_token; no auto-generation

All network I/O uses asyncio.to_thread() or aiohttp async sessions.
All timestamps are Asia/Kolkata (IST).
Tokens are stored encrypted (via ClientDB.update_access_token) on success.

Usage:
    engine = HeadlessAuthEngine()
    ok, msg, token = await engine.authenticate_binding(binding_row, client_id, client_db)
    if not ok:
        # msg contains human-readable failure reason
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Tuple

from config.global_config import IST

if TYPE_CHECKING:
    from data_layer.client_db import ClientDB

logger = logging.getLogger(__name__)

# ── Token validity window ─────────────────────────────────────────────────────
# Tokens are considered fresh if generated on today's IST date AND
# the expiry_at stamp (if set) is still in the future.
_TOKEN_EXPIRY_BUFFER_HOURS = 1  # treat token as expired if within 1 hour of expiry


def _ist_today() -> str:
    return datetime.now(IST).date().isoformat()


def _ist_eod() -> str:
    now = datetime.now(IST)
    eod = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return eod.isoformat()


def _token_is_fresh(generated_at: str, expiry_at: str) -> bool:
    """Return True if the stored token was generated today and isn't about to expire."""
    try:
        if not generated_at:
            return False
        gen_date = generated_at[:10]  # "YYYY-MM-DD"
        if gen_date != _ist_today():
            return False
        if expiry_at:
            expiry = datetime.fromisoformat(expiry_at)
            now = datetime.now(IST)
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=IST)
            remaining = (expiry - now).total_seconds() / 3600
            if remaining < _TOKEN_EXPIRY_BUFFER_HOURS:
                return False
        return True
    except Exception:
        return False


# ── HeadlessAuthEngine ────────────────────────────────────────────────────────

class HeadlessAuthEngine:
    """
    Singleton-safe headless TOTP authentication engine.

    Call authenticate_binding() with a binding dict (as returned by
    ClientDB.get_bindings_sync) to get or refresh the access token.
    On success the new token is written back to the DB and returned.
    """

    # ── Public API ────────────────────────────────────────────────────────────

    async def authenticate_binding(
        self,
        binding: dict,
        client_id: str,
        db: "ClientDB",
    ) -> Tuple[bool, str, str]:
        """
        Ensure binding has a valid access token, generating one if needed.

        Returns (ok: bool, message: str, access_token: str).
        The returned token is also persisted to DB on success.
        """
        provider = (binding.get("provider") or "mock").lower()
        binding_id = binding.get("binding_id", "")

        # 1. Check if existing token is still fresh
        if _token_is_fresh(
            binding.get("token_generated_at", ""),
            binding.get("token_expiry_at", ""),
        ):
            existing = binding.get("access_token", "")
            if existing:
                # Lightweight validation — probe the broker with a fund call
                valid = await self._validate_token_live(binding, client_id)
                if valid:
                    logger.info(
                        "HeadlessAuth [%s/%s]: Existing token still valid.",
                        client_id, binding_id,
                    )
                    return True, "Token valid.", existing

        logger.info(
            "HeadlessAuth [%s/%s]: Token missing or expired — running headless auth for %s.",
            client_id, binding_id, provider,
        )

        # 2. Generate fresh token via provider-specific flow
        dispatch = {
            "mock":     self._mock_auth,
            "shoonya":  self._shoonya_auth,
            "angelone": self._angel_auth,
            "fyers":    self._fyers_auth,
            "upstox":   self._upstox_auth,
            "dhan":     self._dhan_auth,
        }
        handler = dispatch.get(provider)
        if handler is None:
            return False, f"No headless auth handler for provider '{provider}'.", ""

        ok, msg, token = await handler(binding)

        # 3. Persist token on success
        if ok and token:
            now = datetime.now(IST).isoformat()
            expiry = _ist_eod()
            try:
                await db.update_access_token(client_id, binding_id, token, now, expiry)
                logger.info(
                    "HeadlessAuth [%s/%s]: Token stored in DB (expires at EOD IST).",
                    client_id, binding_id,
                )
            except Exception as exc:
                logger.error(
                    "HeadlessAuth [%s/%s]: DB token write failed: %s",
                    client_id, binding_id, exc,
                )
                # Don't fail auth — in-memory token is still valid

        return ok, msg, token

    async def validate_feeder_creds(
        self,
        provider: str,
        creds: dict,
    ) -> Tuple[bool, str, str]:
        """
        Generate a feeder access token from long-lived credentials.
        Returns (ok, message, access_token).

        creds keys (provider-specific):
          upstox: client_id, api_key, secret, totp_secret, redirect_uri?
          fyers:  client_id, app_key, secret, fy_id, totp_secret, pin
        """
        if provider == "upstox":
            binding = {
                "provider":     "upstox",
                "user_id":      creds.get("client_id", ""),
                "api_key":      creds.get("api_key", ""),
                "api_secret":   creds.get("secret", ""),
                "password":     creds.get("password", "") or creds.get("pin", ""),
                "totp_secret":  creds.get("totp_secret", "") or creds.get("totp", ""),
                "access_token": creds.get("access_token", ""),
                "token_generated_at": "",
                "token_expiry_at": "",
            }
            return await self._upstox_auth(binding)

        elif provider == "fyers":
            # Fyers SDK v3.1.x has no FyersAuthCode — headless TOTP is unavailable.
            # If a fresh OAuth token was saved today (via the /exchange-code flow), use it.
            access_token = creds.get("access_token", "")
            generated_at = creds.get("token_generated_at", "")
            expiry_at    = creds.get("token_expiry_at", "")
            if access_token and _token_is_fresh(generated_at, expiry_at):
                logger.info(
                    "HeadlessAuth: Fyers cached OAuth token is fresh — skipping re-auth."
                )
                return True, "Fyers: cached OAuth token valid (generated today).", access_token
            # No fresh token — attempt headless (will fail on SDK 3.1.12 with clear msg)
            binding = {
                "provider":    "fyers",
                "user_id":     creds.get("fy_id", "") or creds.get("client_id", ""),
                "api_key":     creds.get("app_key", ""),
                "api_secret":  creds.get("secret", ""),
                "password":    creds.get("pin", ""),
                "totp_secret": creds.get("totp_secret", "") or creds.get("totp", ""),
                "access_token": access_token,
                "token_generated_at": "",
                "token_expiry_at": "",
            }
            return await self._fyers_auth(binding)

        return False, f"No headless feeder auth for provider '{provider}'.", ""

    # ── Token live validation ─────────────────────────────────────────────────

    async def _validate_token_live(self, binding: dict, client_id: str) -> bool:
        """Call a cheap, non-mutating API to check whether the stored token works."""
        provider = (binding.get("provider") or "mock").lower()
        try:
            if provider == "mock":
                return True

            elif provider == "shoonya":
                from NorenRestApiPy.NorenApi import NorenApi  # type: ignore
                # Shoonya session token is stored in-memory after login;
                # we can't validate a raw token without a live session object.
                # Treat as valid if generated today.
                return True

            elif provider == "angelone":
                # Angel One JWT token - validate via profile endpoint
                import aiohttp
                token = binding.get("access_token", "")
                api_key = binding.get("api_key", "")
                if not token:
                    return False
                async with aiohttp.ClientSession() as session:
                    resp = await session.get(
                        "https://apiconnect.angelone.in/rest/secure/angelbroking/user/v1/getProfile",
                        headers={
                            "Authorization": f"Bearer {token}",
                            "X-UserType": "USER",
                            "X-SourceID": "WEB",
                            "X-ClientLocalIP": "127.0.0.1",
                            "X-ClientPublicIP": "127.0.0.1",
                            "X-MACAddress": "00:00:00:00:00:00",
                            "X-PrivateKey": api_key,
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                        },
                        timeout=aiohttp.ClientTimeout(total=5),
                    )
                    data = await resp.json()
                    return data.get("status") is True

            elif provider == "fyers":
                from fyers_apiv3 import fyersModel  # type: ignore
                token = binding.get("access_token", "")
                api_key = binding.get("api_key", "")
                if not token:
                    return False
                fyers = fyersModel.FyersModel(
                    client_id=api_key, token=token, log_path="logs/",
                )
                profile = await asyncio.to_thread(fyers.get_profile)
                return bool(profile and profile.get("s") == "ok")

            elif provider == "upstox":
                import aiohttp
                token = binding.get("access_token", "")
                if not token:
                    return False
                async with aiohttp.ClientSession() as session:
                    resp = await session.get(
                        "https://api.upstox.com/v2/user/profile",
                        headers={
                            "Authorization": f"Bearer {token}",
                            "Accept": "application/json",
                        },
                        timeout=aiohttp.ClientTimeout(total=5),
                    )
                    return resp.status == 200

            elif provider == "dhan":
                from dhanhq import dhanhq  # type: ignore
                token = binding.get("access_token", "")
                client_code = binding.get("client_code", "") or binding.get("user_id", "")
                if not token or not client_code:
                    return False
                dhan = dhanhq(client_code, token)
                funds = await asyncio.to_thread(dhan.get_fund_limits)
                return bool(funds and funds.get("status") == "success")

        except Exception as exc:
            logger.debug("HeadlessAuth: validate_token_live [%s] failed: %s", provider, exc)
        return False

    # ── Provider handlers ─────────────────────────────────────────────────────

    async def _mock_auth(self, binding: dict) -> Tuple[bool, str, str]:
        return True, "Mock auth — always succeeds.", "mock_token_ok"

    async def _shoonya_auth(self, binding: dict) -> Tuple[bool, str, str]:
        """
        Shoonya direct TOTP login via NorenAPI.
        Required fields: user_id, password, api_secret, totp_secret
        Optional:        vendor_code, imei
        """
        try:
            import pyotp
            from NorenRestApiPy.NorenApi import NorenApi  # type: ignore

            user_id     = binding.get("user_id", "")
            password    = binding.get("password", "")
            api_secret  = binding.get("api_secret", "")
            totp_secret = binding.get("totp_secret", "")
            vendor_code = binding.get("vendor_code", "")
            imei        = binding.get("imei", "FA2020") or "FA2020"

            if not user_id or not password:
                return False, "Shoonya: user_id and password are required.", ""

            totp_code = pyotp.TOTP(totp_secret).now() if totp_secret else ""

            _BASE = "https://api.shoonya.com/NorenWClientTP"
            _WS   = "wss://api.shoonya.com/NorenWSTP/"

            class _API(NorenApi):
                def __init__(self_inner):
                    super().__init__(host=_BASE, websocket=_WS)

            api = _API()
            ret = await asyncio.to_thread(
                api.login,
                userid=user_id,
                password=password,
                twoFA=totp_code,
                vendor_code=vendor_code,
                api_secret=api_secret,
                imei=imei,
            )
            if ret and ret.get("stat") == "Ok":
                # Shoonya doesn't expose a bearer token we can store;
                # the session is maintained in the api object.
                # We store the session token from the response.
                token = ret.get("susertoken", "")
                return True, "Shoonya: authenticated.", token
            msg = (ret or {}).get("emsg", "Login failed.")
            return False, f"Shoonya: {msg}", ""

        except ImportError:
            return False, "Shoonya: NorenRestApiPy not installed. pip install NorenRestApiPy", ""
        except Exception as exc:
            return False, f"Shoonya: unexpected error — {exc}", ""

    async def _angel_auth(self, binding: dict) -> Tuple[bool, str, str]:
        """
        Angel One direct TOTP login via SmartConnect.
        Required fields: api_key, client_code (Angel client ID), password, totp_secret
        """
        try:
            import pyotp
            from SmartApi import SmartConnect  # type: ignore

            api_key     = binding.get("api_key", "")
            client_code = binding.get("client_code", "") or binding.get("user_id", "")
            password    = binding.get("password", "")
            totp_secret = binding.get("totp_secret", "")

            if not api_key or not client_code or not password:
                return False, "Angel One: api_key, client_code, and password are required.", ""

            totp_code = pyotp.TOTP(totp_secret).now() if totp_secret else ""
            smartapi = SmartConnect(api_key=api_key)
            data = await asyncio.to_thread(
                smartapi.generateSession, client_code, password, totp_code,
            )
            if data and data.get("status"):
                token = data.get("data", {}).get("jwtToken", "")
                return True, "Angel One: authenticated.", token
            msg = (data or {}).get("message", "Auth failed.")
            return False, f"Angel One: {msg}", ""

        except ImportError:
            return False, "Angel One: smartapi-python not installed. pip install smartapi-python pyotp", ""
        except Exception as exc:
            return False, f"Angel One: unexpected error — {exc}", ""

    async def _fyers_auth(self, binding: dict) -> Tuple[bool, str, str]:
        """
        Fyers v3 headless TOTP auth via authCodeModel.
        Required fields:
          api_key     = Fyers App ID (e.g. "XXXXX-100")
          api_secret  = App secret key
          user_id     = Fyers user ID (e.g. "XJ12345")
          password    = 4-digit Fyers PIN
          totp_secret = base32 TOTP seed
        """
        try:
            import pyotp
        except ImportError:
            return False, "pyotp not installed. pip install pyotp", ""

        # Two-stage import probe: distinguish missing package from broken sub-module.
        try:
            import fyers_apiv3 as _fyers_pkg  # noqa: F401
        except ImportError:
            return False, "Fyers: fyers-apiv3 not installed. pip install fyers-apiv3", ""

        try:
            from fyers_apiv3.FyersAuthCode import authCodeModel  # type: ignore
        except ImportError as exc:
            # Package present but FyersAuthCode sub-module missing — version too old.
            # authCodeModel (headless TOTP) was added in fyers-apiv3 3.1.x.
            try:
                import importlib.metadata as _imeta
                _fyers_ver = _imeta.version("fyers-apiv3")
            except Exception:
                _fyers_ver = "unknown"
            logger.error("HeadlessAuth: fyers_apiv3 import path error (installed=%s): %s", _fyers_ver, exc)
            return False, (
                f"Fyers: FyersAuthCode not found in fyers-apiv3=={_fyers_ver} — "
                "headless TOTP auth requires >= 3.1.0. "
                "Run: pip install --upgrade fyers-apiv3"
            ), ""

        try:
            app_id      = binding.get("api_key", "")
            secret_key  = binding.get("api_secret", "")
            fy_id       = binding.get("user_id", "")
            pin         = binding.get("password", "")
            totp_secret = binding.get("totp_secret", "")

            if not app_id or not fy_id:
                return False, "Fyers: api_key (App ID) and user_id (FY ID) are required.", ""
            if not totp_secret:
                return False, "Fyers: totp_secret is required for headless auth.", ""
            if not pin:
                return False, "Fyers: password (4-digit PIN) is required.", ""

            totp_secret_clean = totp_secret.upper().replace(" ", "").replace("-", "")
            try:
                totp_code = pyotp.TOTP(totp_secret_clean).now()
            except Exception as exc:
                return False, (
                    f"Fyers: invalid TOTP secret — {exc}. "
                    "Use the raw base32 key from your authenticator app (no spaces or dashes)."
                ), ""

            # Use Fyers-standard redirect URI
            redirect_uri = "https://trade.fyers.in/api-login/redirect-uri/index.html"
            session = authCodeModel(
                client_id=app_id,
                secret_key=secret_key,
                redirect_uri=redirect_uri,
                response_type="code",
                grant_type="authorization_code",
            )

            auth_resp = await asyncio.to_thread(
                session.generate_authcode,
                fy_id=fy_id,
                totp=totp_code,
                pin=pin,
            )

            if not auth_resp or auth_resp.get("s") != "ok":
                msg = (auth_resp or {}).get("message", "Auth code generation failed.")
                return False, f"Fyers: {msg}", ""

            # Extract auth code from redirect URL
            redirect_url = auth_resp.get("Url", "")
            if "auth_code=" not in redirect_url:
                return False, "Fyers: Could not extract auth_code from redirect URL.", ""
            auth_code = redirect_url.split("auth_code=")[1].split("&")[0]

            # Exchange auth code for access token
            token_resp = await asyncio.to_thread(session.generate_token, auth_code)
            access_token = (token_resp or {}).get("access_token", "")
            if not access_token:
                msg = (token_resp or {}).get("message", "Token exchange failed.")
                return False, f"Fyers: {msg}", ""

            logger.info("HeadlessAuth: Fyers auth succeeded for %s.", fy_id)
            return True, "Fyers: authenticated and token generated.", access_token

        except Exception as exc:
            return False, f"Fyers: unexpected error — {exc}", ""

    async def _upstox_auth(self, binding: dict) -> Tuple[bool, str, str]:
        """
        Upstox headless TOTP auth via service.upstox.com internal API.

        Flow (6 steps, adapted from proven upstox-totp community implementation):
          1. GET dialog → extract session user_id from redirect URL
          2. POST OTP generate (mobile number + session user_id)
          3. POST TOTP verify (pyotp code + validateOTPToken from step 2)
          4. POST 2FA PIN (base64-encoded PIN)
          5. POST OAuth approve → extract auth code from redirectUri field
          6. POST token exchange → access_token

        Uses curl_cffi with Chrome 131 TLS fingerprint to bypass Upstox bot-detection.
        The whole flow runs synchronously inside asyncio.to_thread().
        """
        api_key        = binding.get("api_key", "")
        api_secret     = binding.get("api_secret", "")
        user_id        = binding.get("user_id", "") or binding.get("client_code", "")
        password       = binding.get("password", "")
        totp_secret    = binding.get("totp_secret", "")
        existing_token = binding.get("access_token", "")

        if not api_key:
            return False, "Upstox: api_key is required.", ""

        if not totp_secret:
            if existing_token:
                valid = await self._validate_token_live(binding, "")
                if valid:
                    return True, "Upstox: existing token validated.", existing_token
            return False, "Upstox: totp_secret is required for headless auto-authentication.", ""

        # Sanitize TOTP secret — strip spaces/hyphens common in copy-paste
        totp_secret_clean = totp_secret.upper().replace(" ", "").replace("-", "")

        def _mask(s: str) -> str:
            return (s[:4] + "****") if len(s) > 4 else "****"

        logger.info(
            "HeadlessAuth Upstox: starting — api_key=%s mobile=%s",
            _mask(api_key), _mask(user_id),
        )

        def _run_sync() -> Tuple[bool, str, str]:
            import base64
            import random
            import string
            import time
            from urllib.parse import parse_qs, urlparse

            try:
                import pyotp
            except ImportError:
                return False, "pyotp not installed. pip install pyotp", ""

            try:
                from curl_cffi import requests as cffi_requests
            except ImportError:
                return False, (
                    "curl_cffi not installed — required for Upstox TLS fingerprinting. "
                    "Run: pip install curl_cffi"
                ), ""

            _API     = "https://api.upstox.com"
            _SVC     = "https://service.upstox.com"
            _LOGIN   = "https://login.upstox.com"
            _INT_RDR = "https://api-v2.upstox.com/login/authorization/redirect"
            redirect_uri = "https://www.google.com"

            request_id = "WPRO-" + "".join(
                random.choices(string.ascii_letters + string.digits, k=10)
            )
            headers = {
                "accept":             "*/*",
                "accept-language":    "en-GB,en;q=0.9",
                "content-type":       "application/json",
                "origin":             _LOGIN,
                "priority":           "u=1, i",
                "referer":            _LOGIN,
                "sec-ch-ua":          '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
                "sec-ch-ua-mobile":   "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest":     "empty",
                "sec-fetch-mode":     "cors",
                "sec-fetch-site":     "same-site",
                "user-agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
                ),
                "x-device-details": (
                    "platform=WEB|osName=Mac OS/10.15.7|osVersion=Chrome/140.0.0.0"
                    "|appVersion=4.0.0|modelName=Chrome|manufacturer=Apple"
                    "|uuid=3Z1IVTlV4rUUGbNp8KP0"
                    "|userAgent=Upstox 3.0 Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
                ),
                "x-request-id": request_id,
            }

            # impersonate="chrome131" sets TLS fingerprint; Chrome 140 UA in headers is
            # deliberate — matches the proven upstox-totp bypass combination.
            session = cffi_requests.Session(impersonate="chrome131", headers=headers)

            def _parse(resp):
                """Return data payload or raise ValueError with human-readable message."""
                try:
                    body = resp.json()
                except Exception:
                    raise ValueError(
                        f"Non-JSON response (HTTP {resp.status_code}): {resp.text[:300]}"
                    )
                if not isinstance(body, dict):
                    raise ValueError(f"Unexpected response shape: {body!r}")
                if "success" not in body:
                    return body  # flat response (e.g. token exchange)
                if not body.get("success", True):
                    err = body.get("error") or {}
                    if isinstance(err, dict):
                        code = err.get("errorCode") or err.get("code") or ""
                        msg  = err.get("message") or err.get("msg") or str(err)
                        raise ValueError(f"Upstox {code}: {msg}".strip(": "))
                    raise ValueError(f"Upstox login failed: {body}")
                return body.get("data")

            try:
                # ── Step 1: Dialog → session user_id ─────────────────────────
                r1 = session.get(
                    f"{_API}/v2/login/authorization/dialog",
                    params={"response_type": "code", "client_id": api_key,
                            "redirect_uri": redirect_uri},
                    allow_redirects=True,
                )
                qs1 = parse_qs(urlparse(r1.url).query)
                sess_user_id   = (qs1.get("user_id")   or [""])[0]
                sess_client_id = (qs1.get("client_id") or [api_key])[0]
                if not sess_user_id:
                    return False, (
                        f"Upstox: Step 1 failed — session user_id not in redirect URL. "
                        f"final_url={r1.url!r}"
                    ), ""
                logger.info(
                    "HeadlessAuth Upstox Step1: sess_user_id=%s", _mask(sess_user_id)
                )
                time.sleep(1)

                # ── Step 2: Generate OTP ──────────────────────────────────────
                r2 = session.post(
                    f"{_SVC}/login/open/v6/auth/1fa/otp/generate",
                    json={"data": {"mobileNumber": user_id, "userId": sess_user_id}},
                )
                d2 = _parse(r2)
                validate_otp_token = (d2 or {}).get("validateOTPToken") or (d2 or {}).get("validateOtpToken")
                if not validate_otp_token:
                    return False, f"Upstox: Step 2 failed — validateOTPToken missing. data={d2}", ""
                logger.info("HeadlessAuth Upstox Step2: OTP generated.")
                time.sleep(1)

                # ── Step 3: Verify TOTP ───────────────────────────────────────
                try:
                    live_totp = pyotp.TOTP(totp_secret_clean).now()
                except Exception as exc:
                    return False, (
                        f"Upstox: invalid TOTP secret — {exc}. "
                        "Use the raw base32 key from your authenticator app (no spaces or dashes)."
                    ), ""
                r3 = session.post(
                    f"{_SVC}/login/open/v4/auth/1fa/otp-totp/verify",
                    json={"data": {"otp": live_totp, "validateOtpToken": validate_otp_token}},
                )
                _parse(r3)
                logger.info("HeadlessAuth Upstox Step3: TOTP verified.")
                time.sleep(1)

                # ── Step 4: Submit PIN (2FA) ──────────────────────────────────
                if not password:
                    return False, (
                        "Upstox: Step 4 failed — 'password' (6-digit PIN) is not set. "
                        "Open the Upstox credentials modal and enter your 6-digit trading PIN "
                        "in the Password field, then Save and Connect again."
                    ), ""
                pin_b64 = base64.b64encode(password.encode()).decode()
                logger.info(
                    "HeadlessAuth Upstox Step4: submitting PIN (length=%d, b64_len=%d).",
                    len(password), len(pin_b64),
                )
                r4 = session.post(
                    f"{_SVC}/login/open/v3/auth/2fa",
                    params={"client_id": sess_client_id, "redirect_uri": _INT_RDR},
                    json={"data": {"twoFAMethod": "SECRET_PIN", "inputText": pin_b64}},
                    allow_redirects=True,
                )
                r4_body = r4.text
                logger.info(
                    "HeadlessAuth Upstox Step4: HTTP %s  body=%s",
                    r4.status_code, r4_body[:300],
                )
                try:
                    _parse(r4)
                except ValueError as exc:
                    msg = str(exc)
                    if "1017016" in msg or "Something went wrong" in msg:
                        return False, (
                            "Upstox: PIN rejected (error 1017016). "
                            "Please check: (1) The Password field contains your 6-digit Upstox trading PIN "
                            "(not your login password). "
                            "(2) The PIN has no leading zeros missing. "
                            "(3) Your account is not locked — try logging in via the Upstox app to confirm."
                        ), ""
                    raise
                logger.info("HeadlessAuth Upstox Step4: PIN accepted.")
                time.sleep(1)

                # ── Step 5: OAuth approve → auth code ────────────────────────
                r5 = session.post(
                    f"{_SVC}/login/v2/oauth/authorize",
                    params={"client_id": sess_client_id, "redirect_uri": _INT_RDR,
                            "requestId": request_id, "response_type": "code"},
                    json={"data": {"userOAuthApproval": True}},
                    allow_redirects=True,
                )
                d5 = _parse(r5)
                oauth_redirect = (d5 or {}).get("redirectUri", "")
                qs5 = parse_qs(urlparse(oauth_redirect).query)
                auth_code = (qs5.get("code") or [""])[0]
                if not auth_code:
                    return False, (
                        f"Upstox: Step 5 failed — auth code missing from redirectUri. "
                        f"redirectUri={oauth_redirect!r}"
                    ), ""
                logger.info("HeadlessAuth Upstox Step5: auth code obtained.")
                time.sleep(1)

                # ── Step 6: Token exchange ────────────────────────────────────
                tok_sess = cffi_requests.Session(impersonate="chrome131")
                r6 = tok_sess.post(
                    f"{_API}/v2/login/authorization/token",
                    data=(
                        f"code={auth_code}&client_id={api_key}"
                        f"&client_secret={api_secret}"
                        f"&redirect_uri={redirect_uri}&grant_type=authorization_code"
                    ),
                    headers={
                        "accept": "application/json",
                        "content-type": "application/x-www-form-urlencoded",
                    },
                )
                d6 = _parse(r6)
                access_token = (d6 or {}).get("access_token", "")
                if not access_token:
                    return False, f"Upstox: Step 6 failed — access_token missing. data={d6}", ""

                logger.info("HeadlessAuth: Upstox auth SUCCESS for user %s.", _mask(user_id))
                return True, "Upstox: authenticated and token generated.", access_token

            except ValueError as exc:
                return False, f"Upstox: {exc}", ""
            except Exception as exc:
                logger.error("HeadlessAuth: Upstox sync error: %s", exc)
                return False, f"Upstox: unexpected error — {exc}", ""

        try:
            return await asyncio.to_thread(_run_sync)
        except Exception as exc:
            logger.error("HeadlessAuth: Upstox thread error: %s", exc)
            return False, f"Upstox: unexpected error — {exc}", ""

    async def _dhan_auth(self, binding: dict) -> Tuple[bool, str, str]:
        """
        Dhan: validates the existing access_token via fund limits API.
        Dhan does not support TOTP-based headless token generation;
        tokens are generated from their web portal and are long-lived.
        """
        token = binding.get("access_token", "")
        client_code = binding.get("client_code", "") or binding.get("user_id", "")

        if not token or not client_code:
            return (
                False,
                "Dhan: access_token and client_code are required. "
                "Generate token from Dhan web portal.",
                "",
            )

        try:
            from dhanhq import dhanhq  # type: ignore
            dhan = dhanhq(client_code, token)
            funds = await asyncio.to_thread(dhan.get_fund_limits)
            if funds and funds.get("status") == "success":
                return True, "Dhan: existing token validated.", token
            return False, f"Dhan: token validation failed: {funds}", ""
        except ImportError:
            return False, "Dhan: dhanhq not installed. pip install dhanhq", ""
        except Exception as exc:
            return False, f"Dhan: unexpected error — {exc}", ""


# ── Singleton ─────────────────────────────────────────────────────────────────

headless_engine = HeadlessAuthEngine()
