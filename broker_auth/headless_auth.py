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
                "totp_secret":  creds.get("totp_secret", "") or creds.get("totp", ""),
                "access_token": creds.get("access_token", ""),
                "token_generated_at": "",
                "token_expiry_at": "",
            }
            return await self._upstox_auth(binding)

        elif provider == "fyers":
            binding = {
                "provider":    "fyers",
                "user_id":     creds.get("fy_id", "") or creds.get("client_id", ""),
                "api_key":     creds.get("app_key", ""),
                "api_secret":  creds.get("secret", ""),
                "password":    creds.get("pin", ""),
                "totp_secret": creds.get("totp_secret", "") or creds.get("totp", ""),
                "access_token": creds.get("access_token", ""),
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
            from fyers_apiv3.FyersAuthCode import authCodeModel  # type: ignore

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

            totp_code = pyotp.TOTP(totp_secret).now()

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

        except ImportError:
            return False, "Fyers: fyers-apiv3 not installed. pip install fyers-apiv3", ""
        except Exception as exc:
            return False, f"Fyers: unexpected error — {exc}", ""

    async def _upstox_auth(self, binding: dict) -> Tuple[bool, str, str]:
        """
        Upstox headless TOTP auth via internal Upstox authentication API.

        Required fields:
          user_id     = Upstox client/user ID (mobile number or email)
          api_key     = API key from Upstox developer portal
          api_secret  = API secret from Upstox developer portal
          totp_secret = base32 TOTP seed for authenticator app
          password    = Upstox login password (optional, if 2FA only via TOTP)

        Note: If TOTP secret is not available, falls back to validating the
        existing access_token. The headless flow uses Upstox's documented
        OAuth2 token endpoint after obtaining the authorization code.
        """
        try:
            import aiohttp
            import pyotp

            api_key     = binding.get("api_key", "")
            api_secret  = binding.get("api_secret", "")
            user_id     = binding.get("user_id", "") or binding.get("client_code", "")
            password    = binding.get("password", "")
            totp_secret = binding.get("totp_secret", "")
            existing_token = binding.get("access_token", "")

            if not api_key:
                return False, "Upstox: api_key is required.", ""

            # If no TOTP secret, try validating existing token
            if not totp_secret:
                if existing_token:
                    valid = await self._validate_token_live(binding, "")
                    if valid:
                        return True, "Upstox: existing token validated.", existing_token
                return (
                    False,
                    "Upstox: totp_secret is required for headless auto-authentication.",
                    "",
                )

            totp_code = pyotp.TOTP(totp_secret).now()
            redirect_uri = "https://127.0.0.1/"

            async with aiohttp.ClientSession() as sess:
                # Step 1: Initiate login — get session cookie
                headers = {
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                }

                # Identification (mobile/email)
                r1 = await sess.post(
                    "https://api.upstox.com/v2/login/identification",
                    json={"mobile_num": user_id},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                if r1.status != 200:
                    body = await r1.text()
                    return False, f"Upstox: identification failed (HTTP {r1.status}): {body[:120]}", ""

                # Step 2: Submit password
                r2 = await sess.post(
                    "https://api.upstox.com/v2/login/authorization/validate",
                    json={"password": password, "mobile_num": user_id},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                if r2.status not in (200, 400):
                    body = await r2.text()
                    return False, f"Upstox: login validate failed (HTTP {r2.status}): {body[:120]}", ""

                # Step 3: Submit TOTP
                r3 = await sess.post(
                    "https://api.upstox.com/v2/login/mpin",
                    json={"otp": totp_code, "mobile_num": user_id},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                r3_data = await r3.json()
                if r3.status != 200:
                    return False, f"Upstox: TOTP verification failed: {r3_data}", ""

                # Step 4: Fetch authorization code from OAuth2 dialog
                auth_url = (
                    f"https://api.upstox.com/v2/login/authorization/dialog"
                    f"?client_id={api_key}"
                    f"&redirect_uri={redirect_uri}"
                    f"&response_type=code"
                    f"&state=terminus"
                )
                r4 = await sess.get(
                    auth_url,
                    headers=headers,
                    allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                location = r4.headers.get("Location", "")
                if "code=" not in location:
                    # Try following the redirect chain
                    body = await r4.text()
                    return False, f"Upstox: Could not obtain auth code. Location: {location[:120]}", ""

                auth_code = location.split("code=")[1].split("&")[0]

                # Step 5: Exchange code for access token
                r5 = await sess.post(
                    "https://api.upstox.com/v2/login/authorization/token",
                    data={
                        "code":          auth_code,
                        "client_id":     api_key,
                        "client_secret": api_secret,
                        "redirect_uri":  redirect_uri,
                        "grant_type":    "authorization_code",
                    },
                    headers={"Accept": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                token_data = await r5.json()
                access_token = token_data.get("access_token", "")
                if not access_token:
                    return False, f"Upstox: token exchange failed: {token_data}", ""

                logger.info("HeadlessAuth: Upstox auth succeeded for user %s.", user_id)
                return True, "Upstox: authenticated and token generated.", access_token

        except ImportError:
            return False, "Upstox: aiohttp not installed. pip install aiohttp", ""
        except Exception as exc:
            logger.error("HeadlessAuth: Upstox auth error: %s", exc)
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
