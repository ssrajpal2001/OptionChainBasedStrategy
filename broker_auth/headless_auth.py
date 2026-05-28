"""
broker_auth/headless_auth.py — Headless background TOTP authentication engine.

Generates daily session tokens for all supported brokers without any
browser interaction.  Called at system startup, on start_bot, and on
token-expiry detected during pre-flight validation.

Auth strategy per provider:
  mock      -- always succeeds
  shoonya   -- direct NorenAPI login with user_id + password + TOTP.now()
  angelone  -- SmartConnect.generateSession with client_code + password + TOTP.now()
  fyers     -- Vagator v2 API: send_login_otp → verify_otp → verify_pin_v2 → auth_code → token
  upstox    -- aiohttp headless OAuth2 using Upstox internal auth endpoints
  dhan      -- auth.dhan.co/app/generateAccessToken with Client ID + PIN + TOTP

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
            "zerodha":  self._zerodha_auth,
            "shoonya":  self._shoonya_auth,
            "angelone": self._angel_auth,
            "fyers":    self._fyers_auth,
            "upstox":   self._upstox_auth,
            "dhan":     self._dhan_auth,
            "groww":    self._groww_auth,
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
        Fyers headless TOTP auth via Vagator v2 API.

        Flow:
          1. POST /vagator/v2/send_login_otp_v2  → request_key
          2. POST /vagator/v2/verify_otp (TOTP)  → request_key
          3. POST /vagator/v2/verify_pin_v2       → vagator access_token
          4. POST /api/v3/token (with Bearer)     → auth_code URL
          5. Exchange auth_code via SessionModel  → final access_token

        Required fields:
          api_key     = Fyers App ID  (e.g. "XXXXX-100")
          api_secret  = App secret key
          user_id     = Fyers client/user ID  (e.g. "XJ12345")
          password    = 4-digit Fyers PIN
          totp_secret = base32 TOTP seed
        """
        app_id      = binding.get("api_key", "")
        secret_key  = binding.get("api_secret", "")
        fy_id       = binding.get("user_id", "")
        pin         = binding.get("password", "")
        totp_secret = binding.get("totp_secret", "")

        if not app_id or not fy_id:
            return False, "Fyers: api_key (App ID) and user_id (FY ID) are required.", ""

        # Fyers vagator API is Cloudflare-protected — skip immediately and
        # instruct the user to use the manual OAuth flow in the UI.
        return False, (
            "Fyers: use the 'Get Token Manually' button on your broker card "
            "(or the OAuth Login section in the Fyers data feeder panel) to connect."
        ), ""

        def _run_sync() -> Tuple[bool, str, str]:  # noqa: unreachable (kept for reference)
            import hashlib
            import time
            from urllib.parse import urlparse, parse_qs

            try:
                import pyotp  # type: ignore
            except ImportError:
                return False, "pyotp not installed. pip install pyotp", ""
            try:
                import requests as _req
            except ImportError:
                return False, "requests not installed. pip install requests", ""
            try:
                from fyers_apiv3 import fyersModel  # type: ignore
            except ImportError:
                return False, "fyers-apiv3 not installed. pip install fyers-apiv3", ""

            _VAGATOR   = "https://api-t2.fyers.in/vagator/v2"
            _VAGATOR_T1 = "https://api-t1.fyers.in/vagator/v2"
            _API_V3    = "https://api-t2.fyers.in/api/v3"
            _REDIRECT  = "https://trade.fyers.in/api-login/redirect-uri/index.html"
            # strip -100 suffix for vagator app_id field
            _app_id_short = app_id.rsplit("-", 1)[0] if "-" in app_id else app_id
            # ensure full format for SessionModel
            _app_id_full  = app_id if "-" in app_id else f"{app_id}-100"

            totp_clean = totp_secret.upper().replace(" ", "").replace("-", "")
            try:
                totp_code = pyotp.TOTP(totp_clean).now()
            except Exception as exc:
                return False, (
                    f"Fyers: invalid TOTP secret — {exc}. "
                    "Use the raw base32 key from your authenticator app."
                ), ""

            def _mask(s: str) -> str:
                return (s[:4] + "****") if len(s) > 4 else "****"

            sess = _req.Session()
            sess.headers.update({
                "Content-Type": "application/json",
                "Accept":       "application/json",
                "Origin":       "https://trade.fyers.in",
                "Referer":      "https://trade.fyers.in/",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            })

            # ── Step 1: send_login_otp — try all known endpoint/payload variants ─
            # Fyers vagator has changed over time; we try every known combination.
            # app_id 2/"2" = login type constant used by Fyers web app.
            _FULL  = {"imei": "", "recaptcha_token": ""}
            _BARE  = {}
            _CANDIDATES = [
                (_VAGATOR,    "send_login_otp_v2", 2,    _FULL),
                (_VAGATOR,    "send_login_otp_v2", "2",  _FULL),
                (_VAGATOR_T1, "send_login_otp_v2", 2,    _FULL),
                (_VAGATOR_T1, "send_login_otp_v2", "2",  _FULL),
                (_VAGATOR,    "send_login_otp_v2", 2,    _BARE),
                (_VAGATOR_T1, "send_login_otp_v2", 2,    _BARE),
                (_VAGATOR,    "send_login_otp",    2,    _FULL),
                (_VAGATOR_T1, "send_login_otp",    2,    _FULL),
                (_VAGATOR,    "send_login_otp",    2,    _BARE),
                (_VAGATOR_T1, "send_login_otp",    2,    _BARE),
            ]
            request_key = None
            d1 = {}
            for _base, _ep, _aid, _extra in _CANDIDATES:
                payload = {"fy_id": fy_id, "app_id": _aid, **_extra}
                r = sess.post(f"{_base}/{_ep}", json=payload, timeout=15)
                try:
                    d1 = r.json()
                except Exception:
                    d1 = {}
                logger.info(
                    "HeadlessAuth Fyers %s host=%s app_id=%r extra=%s HTTP %s -> %s",
                    _ep, "t1" if "t1" in _base else "t2", _aid, bool(_extra),
                    r.status_code, r.text[:100],
                )
                if d1.get("s") == "ok":
                    request_key = d1.get("request_key")
                    logger.info("HeadlessAuth Fyers Step1: OK (%s/%s)", _base, _ep)
                    break

            if not request_key:
                msg = d1.get("message") or str(d1)
                return False, f"Fyers Step 1 (send_login_otp): {msg}", ""

            # ── Step 2: verify TOTP ──────────────────────────────────────────────
            r2 = sess.post(
                f"{_VAGATOR}/verify_otp",
                json={"request_key": request_key, "otp": totp_code},
                timeout=15,
            )
            d2 = r2.json() if r2.ok else {}
            if d2.get("s") != "ok":
                # TOTP window may have ticked — wait one period and retry
                time.sleep(31)
                totp_code = pyotp.TOTP(totp_clean).now()
                r2 = sess.post(
                    f"{_VAGATOR}/verify_otp",
                    json={"request_key": request_key, "otp": totp_code},
                    timeout=15,
                )
                d2 = r2.json() if r2.ok else {}
            if d2.get("s") != "ok":
                msg = d2.get("message") or str(d2)
                return False, f"Fyers Step 2 (verify_otp): {msg}", ""
            request_key = d2["request_key"]
            logger.info("HeadlessAuth Fyers Step2: TOTP verified for %s", _mask(fy_id))

            # ── Step 3: verify PIN ───────────────────────────────────────────────
            pin_hash = hashlib.sha256(pin.encode()).hexdigest()
            r3 = sess.post(
                f"{_VAGATOR}/verify_pin_v2",
                json={"request_key": request_key, "identity_type": "pin", "identifier": pin_hash},
                timeout=15,
            )
            d3 = r3.json() if r3.ok else {}
            if d3.get("s") != "ok":
                # Retry with plain PIN (some accounts use plain PIN)
                r3 = sess.post(
                    f"{_VAGATOR}/verify_pin_v2",
                    json={"request_key": request_key, "identity_type": "pin", "identifier": pin},
                    timeout=15,
                )
                d3 = r3.json() if r3.ok else {}
            if d3.get("s") != "ok":
                msg = d3.get("message") or str(d3)
                return False, f"Fyers Step 3 (verify_pin_v2): {msg}", ""
            vagator_token = (d3.get("data") or {}).get("access_token")
            if not vagator_token:
                return False, f"Fyers Step 3: access_token missing — {d3}", ""
            logger.info("HeadlessAuth Fyers Step3: PIN verified, vagator token obtained")

            # ── Step 4: get auth_code ────────────────────────────────────────────
            sess.headers.update({"authorization": f"Bearer {vagator_token}"})
            r4 = sess.post(
                f"{_API_V3}/token",
                json={
                    "fyers_id":       fy_id,
                    "app_id":         _app_id_short,
                    "redirect_uri":   _REDIRECT,
                    "appType":        "100",
                    "code_challenge": "",
                    "state":          "terminus",
                    "nonce":          "",
                    "response_type":  "code",
                    "create_cookie":  True,
                },
                timeout=15,
            )
            d4 = r4.json() if r4.ok else {}
            url = d4.get("Url") or d4.get("url") or ""
            auth_code = None
            if url:
                for part in url.split("?", 1)[-1].split("&"):
                    if part.startswith("auth_code="):
                        auth_code = part.split("=", 1)[1]
                        break
            if not auth_code:
                msg = d4.get("message") or d4.get("error") or str(d4)
                return False, f"Fyers Step 4 (get auth_code): {msg}", ""
            logger.info("HeadlessAuth Fyers Step4: auth_code obtained")

            # ── Step 5: exchange auth_code → access_token ────────────────────────
            import hashlib as _hs
            app_id_hash = _hs.sha256(f"{_app_id_full}:{secret_key}".encode()).hexdigest()
            try:
                r5 = _req.post(
                    "https://api-t1.fyers.in/api/v3/validate-authcode",
                    json={
                        "grant_type": "authorization_code",
                        "appIdHash":  app_id_hash,
                        "code":       auth_code,
                    },
                    headers={"Content-Type": "application/json", "Accept": "application/json"},
                    timeout=15,
                )
                d5 = r5.json() if r5.ok else {}
            except Exception as exc:
                return False, f"Fyers Step 5 (token exchange): {exc}", ""

            access_token = d5.get("access_token") or (d5.get("data") or {}).get("access_token")
            if not access_token:
                msg = d5.get("message") or str(d5)
                return False, f"Fyers Step 5 (token exchange): {msg}", ""

            logger.info("HeadlessAuth Fyers: auth SUCCESS for %s", _mask(fy_id))
            return True, "Fyers: authenticated and token generated.", access_token

        try:
            ok, msg, token = await asyncio.to_thread(_run_sync)
        except Exception as exc:
            msg = str(exc)
            if "timed out" in msg.lower() or "ConnectTimeout" in msg or "Max retries" in msg:
                return False, (
                    "Fyers: api-t2.fyers.in is unreachable (connection timeout). "
                    "Check: outbound port 443 to api-t2.fyers.in is allowed in your firewall."
                ), ""
            ok, msg, token = False, f"Fyers: unexpected error — {exc}", ""

        if ok:
            return ok, msg, token

        # Vagator API is Cloudflare-protected — direct API calls are blocked.
        # Use the manual flow: broker card → "Get Token Manually" → open login URL
        # → paste full redirect URL → Generate Token.
        return False, (
            "Fyers: headless API unavailable (Cloudflare protected). "
            "Use the 'Get Token Manually' button on your Fyers broker card: "
            "click 'Open Fyers Login Page', log in, copy the redirect URL, paste it, click Generate Token."
        ), ""

    async def _fyers_auth_browser(self, binding: dict) -> Tuple[bool, str, str]:
        """
        Fyers headless login via undetected-chromedriver (bypasses Cloudflare).
        Requires: pip install undetected-chromedriver selenium
        Chrome browser must be installed.
        """
        app_id      = binding.get("api_key", "")
        secret_key  = binding.get("api_secret", "")
        fy_id       = binding.get("user_id", "")
        pin         = binding.get("password", "")
        totp_secret = binding.get("totp_secret", "")

        def _run_browser() -> Tuple[bool, str, str]:
            import time as _time
            try:
                import pyotp  # type: ignore
            except ImportError:
                return False, "pyotp not installed. pip install pyotp", ""
            try:
                import fyers_apiv3  # type: ignore  # noqa: F401
                from fyers_apiv3 import fyersModel  # type: ignore
            except ImportError:
                return False, "fyers-apiv3 not installed.", ""

            try:
                import undetected_chromedriver as uc  # type: ignore
                _has_uc = True
            except ImportError:
                _has_uc = False

            try:
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
            except ImportError:
                return (
                    False,
                    "Selenium not installed. Run: pip install selenium undetected-chromedriver",
                    "",
                )

            if not _has_uc:
                return (
                    False,
                    "undetected-chromedriver not installed. "
                    "Run: pip install undetected-chromedriver\n"
                    "Then retry Connect — browser automation will handle the login automatically.",
                    "",
                )

            _app_id_full = app_id if "-" in app_id else f"{app_id}-100"
            _redirect    = "https://trade.fyers.in/api-login/redirect-uri/index.html"
            auth_url = (
                f"https://api-t1.fyers.in/api/v3/generate-authcode"
                f"?client_id={_app_id_full}"
                f"&redirect_uri={_redirect}"
                f"&response_type=code&state=terminus"
            )

            totp_clean = totp_secret.upper().replace(" ", "").replace("-", "")

            def _exchange_auth_code(code: str, prefix: str = "") -> Tuple[bool, str, str]:
                """Exchange Fyers auth_code for access_token via direct REST endpoint."""
                import hashlib as _hs, requests as _rq
                app_id_hash = _hs.sha256(f"{_app_id_full}:{secret_key}".encode()).hexdigest()
                try:
                    r = _rq.post(
                        "https://api-t1.fyers.in/api/v3/validate-authcode",
                        json={
                            "grant_type": "authorization_code",
                            "appIdHash":  app_id_hash,
                            "code":       code,
                        },
                        headers={"Content-Type": "application/json", "Accept": "application/json"},
                        timeout=15,
                    )
                    d = r.json() if r.ok else {}
                    token = d.get("access_token") or (d.get("data") or {}).get("access_token")
                    if token:
                        logger.info("HeadlessAuth Fyers: token exchange SUCCESS")
                        return True, "Fyers: authenticated.", token
                    msg = d.get("message") or str(d)
                    return False, f"{prefix}token exchange failed: {msg}", ""
                except Exception as exc:
                    return False, f"{prefix}token exchange error: {exc}", ""

            def _fill_digits(driver, value: str, n: int):
                ids = ["first", "second", "third", "fourth", "fifth", "sixth"][:n]
                for i, fid in enumerate(ids):
                    els = driver.find_elements(By.ID, fid)
                    el = next((e for e in els if e.is_displayed()), None)
                    if el is None:
                        _time.sleep(0.3)
                        els = driver.find_elements(By.ID, fid)
                        el = next((e for e in els if e.is_displayed()), None)
                    if el:
                        el.clear()
                        el.send_keys(value[i])
                        _time.sleep(0.06)

            def _get_auth_code(driver, timeout: int = 25) -> str:
                from urllib.parse import urlparse, parse_qs
                deadline = _time.monotonic() + timeout
                while _time.monotonic() < deadline:
                    try:
                        url = driver.current_url
                    except Exception:
                        break
                    qs = parse_qs(urlparse(url).query)
                    code = (qs.get("auth_code") or qs.get("code") or [""])[0]
                    if code:
                        return code
                    _time.sleep(0.4)
                return ""

            driver = None
            try:
                # Detect installed Chrome major version to avoid driver mismatch
                _chrome_ver = None
                import re as _re
                def _detect_chrome_ver():
                    # Method 1: winreg (most reliable on Windows)
                    try:
                        import winreg
                        for _hive, _path in [
                            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Google\Chrome\BLBeacon"),
                            (winreg.HKEY_CURRENT_USER,  r"SOFTWARE\Google\Chrome\BLBeacon"),
                            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Google\Chrome\BLBeacon"),
                        ]:
                            try:
                                k = winreg.OpenKey(_hive, _path)
                                v, _ = winreg.QueryValueEx(k, "version")
                                winreg.CloseKey(k)
                                m = _re.match(r"(\d+)", str(v))
                                if m:
                                    return int(m.group(1))
                            except Exception:
                                continue
                    except ImportError:
                        pass
                    # Method 2: reg query subprocess
                    try:
                        import subprocess as _sp
                        for _rpath in [
                            r'HKLM\SOFTWARE\Google\Chrome\BLBeacon',
                            r'HKCU\SOFTWARE\Google\Chrome\BLBeacon',
                            r'HKLM\SOFTWARE\WOW6432Node\Google\Chrome\BLBeacon',
                        ]:
                            try:
                                out = _sp.check_output(
                                    f'reg query "{_rpath}" /v version',
                                    shell=True, stderr=_sp.DEVNULL
                                ).decode(errors="ignore")
                                m = _re.search(r'version\s+REG_SZ\s+(\d+)', out, _re.IGNORECASE)
                                if m:
                                    return int(m.group(1))
                            except Exception:
                                continue
                    except Exception:
                        pass
                    # Method 3: find chrome.exe and get its file version
                    try:
                        import subprocess as _sp
                        for _chrome_path in [
                            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                        ]:
                            try:
                                out = _sp.check_output(
                                    f'powershell -command "(Get-Item \'{_chrome_path}\').VersionInfo.ProductVersion"',
                                    shell=True, stderr=_sp.DEVNULL
                                ).decode(errors="ignore").strip()
                                m = _re.match(r"(\d+)", out)
                                if m:
                                    return int(m.group(1))
                            except Exception:
                                continue
                    except Exception:
                        pass
                    return None
                _chrome_ver = _detect_chrome_ver()
                logger.info("HeadlessAuth Fyers Browser: detected Chrome version %s", _chrome_ver)

                opts = uc.ChromeOptions()
                opts.add_argument("--no-sandbox")
                opts.add_argument("--disable-dev-shm-usage")
                opts.add_argument("--window-size=1280,900")
                _uc_kwargs = {"options": opts, "headless": True}
                if _chrome_ver:
                    _uc_kwargs["version_main"] = _chrome_ver
                driver = uc.Chrome(**_uc_kwargs)
                wait   = WebDriverWait(driver, 15)

                logger.info("HeadlessAuth Fyers Browser: navigating to auth URL")
                driver.get(auth_url)
                _time.sleep(2)

                # Switch to Client ID mode
                try:
                    rb = wait.until(EC.presence_of_element_located((By.ID, "clientId_rb")))
                    driver.execute_script("arguments[0].click();", rb)
                    _time.sleep(0.5)
                except Exception:
                    pass  # already in client ID mode

                # Fill Client ID
                els = driver.find_elements(By.ID, "fy_client_id")
                id_el = next((e for e in els if e.is_displayed()), None)
                if not id_el:
                    return False, "Fyers browser: #fy_client_id not found", ""
                id_el.clear()
                id_el.send_keys(fy_id)

                # Submit
                submit_els = driver.find_elements(By.ID, "clientIdSubmit")
                submit_el  = next((e for e in submit_els if e.is_displayed()), None)
                if submit_el:
                    driver.execute_script("arguments[0].click();", submit_el)
                _time.sleep(3)

                # TOTP — 6 digit inputs
                totp_code = pyotp.TOTP(totp_clean).now()
                try:
                    wait.until(EC.visibility_of_element_located((By.ID, "first")))
                    _fill_digits(driver, totp_code, 6)
                    logger.info("HeadlessAuth Fyers Browser: TOTP filled")
                    _time.sleep(3)
                except Exception:
                    _time.sleep(31)
                    totp_code = pyotp.TOTP(totp_clean).now()
                    _fill_digits(driver, totp_code, 6)
                    _time.sleep(3)

                # Check for error page
                src = driver.page_source.lower()
                if "your account has been blocked" in src or "account is blocked" in src:
                    return False, "Fyers account is blocked. Unblock at trade.fyers.in.", ""
                if "invalid otp" in src or "incorrect otp" in src:
                    return False, "Fyers: TOTP rejected — check your TOTP secret.", ""

                # PIN — 4 digit inputs
                _pin_ready = False
                for _ in range(25):
                    if any(b.is_displayed() for b in driver.find_elements(By.ID, "verifyPinSubmit")):
                        _pin_ready = True
                        break
                    _time.sleep(0.4)
                if not _pin_ready:
                    return False, "Fyers browser: PIN form never appeared", ""

                _fill_digits(driver, pin, 4)
                driver.execute_script(
                    "arguments[0].click();",
                    driver.find_element(By.ID, "verifyPinSubmit"),
                )
                _time.sleep(2)

                # Capture auth_code
                auth_code = _get_auth_code(driver)
                if not auth_code:
                    return False, f"Fyers browser: auth_code not found in redirect URL", ""
                logger.info("HeadlessAuth Fyers Browser: auth_code captured")

            except Exception as exc:
                return False, f"Fyers browser automation error: {exc}", ""
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            # Exchange auth_code -> access_token via direct REST (no SDK dependency)
            return _exchange_auth_code(auth_code, "Fyers browser: ")

        try:
            return await asyncio.to_thread(_run_browser)
        except Exception as exc:
            return False, f"Fyers browser: unexpected error — {exc}", ""

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

    async def _zerodha_auth(self, binding: dict) -> Tuple[bool, str, str]:
        """
        Zerodha Kite Connect headless TOTP auth.
        Required: api_key, api_secret, user_id (Zerodha client ID), password, totp_secret

        Flow:
          1. POST kite.zerodha.com/api/login  → request_id
          2. POST kite.zerodha.com/api/twofa  → redirect with request_token
          3. POST api.kite.trade/session/token (checksum = sha256(api_key+request_token+api_secret))
        """
        api_key     = binding.get("api_key", "")
        api_secret  = binding.get("api_secret", "")
        user_id     = binding.get("user_id", "")
        password    = binding.get("password", "")
        totp_secret = binding.get("totp_secret", "")

        if not all([api_key, api_secret, user_id, password]):
            return (
                False,
                "Zerodha: api_key, api_secret, user_id, and password are required.",
                "",
            )
        if not totp_secret:
            return (
                False,
                "Zerodha: totp_secret is required for headless authentication. "
                "Enter the base32 TOTP seed from your authenticator app.",
                "",
            )

        def _run_sync() -> Tuple[bool, str, str]:
            import hashlib
            import time
            from urllib.parse import parse_qs, urlparse

            try:
                import pyotp
            except ImportError:
                return False, "pyotp not installed. pip install pyotp", ""
            try:
                import requests as _req
            except ImportError:
                return False, "requests not installed. pip install requests", ""

            s = _req.Session()
            s.headers.update({
                "X-Kite-Version": "3",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            })

            # Step 0: Initiate OAuth session — MUST visit connect/login first so
            # Zerodha knows which app (api_key) to redirect to after 2FA.
            # Without this the twofa response has no redirect context.
            r0 = s.get(
                "https://kite.zerodha.com/connect/login",
                params={"v": "3", "api_key": api_key},
                allow_redirects=True,
                timeout=15,
            )
            if r0.status_code not in (200, 302):
                return (
                    False,
                    f"Zerodha Step 0 (init): unexpected HTTP {r0.status_code}. "
                    "Check that your api_key is correct and the Kite Connect app is active.",
                    "",
                )
            logger.info("HeadlessAuth Zerodha Step0: OAuth session initialised.")
            time.sleep(0.5)

            # Step 1: Password login
            r1 = s.post("https://kite.zerodha.com/api/login", data={
                "user_id": user_id,
                "password": password,
            }, timeout=15)
            d1 = r1.json()
            if d1.get("status") != "success":
                msg = d1.get("message", "Login failed.")
                return False, f"Zerodha Step 1 (login): {msg}", ""
            request_id = d1["data"]["request_id"]
            logger.info("HeadlessAuth Zerodha Step1: login OK, request_id obtained.")
            time.sleep(0.5)

            # Step 2: TOTP 2FA
            totp_clean = totp_secret.upper().replace(" ", "").replace("-", "")
            try:
                totp_code = pyotp.TOTP(totp_clean).now()
            except Exception as exc:
                return False, f"Zerodha: invalid TOTP secret — {exc}", ""

            r2 = s.post("https://kite.zerodha.com/api/twofa", data={
                "user_id": user_id,
                "request_id": request_id,
                "twofa_value": totp_code,
                "twofa_type": "totp",
            }, allow_redirects=False, timeout=15)

            # /api/twofa is a pure JSON endpoint — the browser redirect to the
            # registered callback URL is driven by JavaScript, not an HTTP 3xx.
            # requests never follows it. Verify 2FA succeeded via JSON status.
            try:
                d2 = r2.json()
            except Exception:
                return False, f"Zerodha Step 2 (2FA): non-JSON response (HTTP {r2.status_code})", ""

            if d2.get("status") != "success":
                msg = d2.get("message", "2FA failed.")
                return False, f"Zerodha Step 2 (2FA): {msg}", ""

            logger.info("HeadlessAuth Zerodha Step2: TOTP verified.")
            time.sleep(0.5)

            # Step 2b: Now re-GET connect/login with the authenticated session.
            # Zerodha will redirect this to the registered redirect URL with
            # ?request_token=XXX&action=login&status=success in the query string.
            r3 = s.get(
                "https://kite.zerodha.com/connect/login",
                params={"v": "3", "api_key": api_key},
                allow_redirects=True,
                timeout=15,
            )
            final_url = r3.url
            qs = parse_qs(urlparse(final_url).query)
            request_token = (qs.get("request_token") or [""])[0]

            if not request_token:
                return (
                    False,
                    f"Zerodha Step 2b (redirect): request_token not in final URL. "
                    f"final_url={final_url!r}. "
                    f"If your registered redirect URL is an external site "
                    f"(e.g. https://kite.zerodha.com/connect/login), requests may not "
                    f"follow through — try setting it to https://127.0.0.1 in the "
                    f"Kite Connect developer console.",
                    "",
                )
            logger.info("HeadlessAuth Zerodha Step2b: request_token obtained from redirect.")

            # Step 3: Exchange request_token → access_token
            checksum = hashlib.sha256(
                f"{api_key}{request_token}{api_secret}".encode()
            ).hexdigest()
            r4 = s.post("https://api.kite.trade/session/token", data={
                "api_key": api_key,
                "request_token": request_token,
                "checksum": checksum,
            }, headers={"X-Kite-Version": "3"}, timeout=15)
            d4 = r4.json()
            if d4.get("status") != "success":
                msg = d4.get("message", "Token exchange failed.")
                return False, f"Zerodha Step 3 (token exchange): {msg}", ""

            access_token = (d4.get("data") or {}).get("access_token", "")
            if not access_token:
                return False, f"Zerodha Step 3: access_token missing in response: {d4}", ""

            logger.info(
                "HeadlessAuth Zerodha: auth SUCCESS for user_id=%s.", user_id[:4] + "****"
            )
            return True, "Zerodha: authenticated and token generated.", access_token

        try:
            return await asyncio.to_thread(_run_sync)
        except Exception as exc:
            logger.error("HeadlessAuth: Zerodha thread error: %s", exc)
            return False, f"Zerodha: unexpected error — {exc}", ""

    async def _groww_auth(self, binding: dict) -> Tuple[bool, str, str]:
        """Groww does not expose a public API for automated trading."""
        return (
            False,
            "Groww: automated headless auth is not supported — Groww does not provide a "
            "public trading API. Use paper mode or switch to a supported broker.",
            "",
        )

    async def _dhan_auth(self, binding: dict) -> Tuple[bool, str, str]:
        """
        Dhan headless auth via official token-generation endpoint.

        Flow:
          1. Validate saved JWT (fast path — no network if token is fresh)
          2. If api_key is a JWT (manual paste), validate it directly
          3. Headless: POST https://auth.dhan.co/app/generateAccessToken
             with dhanClientId + pin + TOTP → accessToken (24-hour validity)

        Field mapping:
          user_id      = Dhan Client ID  (e.g. "1000557682")
          password     = Dhan login PIN  (4–6 digits)
          totp_secret  = TOTP seed (base32) — required for headless generation
          api_key      = ACCESS TOKEN (eyJ…) — optional, paste here for manual-token mode
        """
        client_id   = binding.get("user_id", "") or binding.get("client_code", "")
        api_key     = binding.get("api_key", "")   # optional: manual JWT paste
        pin         = binding.get("password", "")
        totp_secret = binding.get("totp_secret", "")
        saved_token = binding.get("access_token", "")

        if not client_id:
            return False, "Dhan: Client ID is required.", ""

        def _is_jwt(t: str) -> bool:
            return bool(t and len(t) >= 50 and t.startswith("eyJ"))

        def _validate_token(token: str) -> Tuple[bool, str, str]:
            try:
                import requests as _req
                r = _req.get(
                    "https://api.dhan.co/v2/fundlimit",
                    headers={
                        "access-token": token,
                        "client-id": client_id,
                        "Content-Type": "application/json",
                    },
                    timeout=10,
                )
                if r.status_code == 200:
                    return True, "Dhan: access token validated.", token
                return False, f"Dhan: token rejected (HTTP {r.status_code})", ""
            except Exception as e:
                return False, f"Dhan: validation error — {e}", ""

        # ── Fast path 1: saved JWT still valid ───────────────────────────────
        if _is_jwt(saved_token):
            ok, msg, tok = await asyncio.to_thread(_validate_token, saved_token)
            if ok:
                logger.info("HeadlessAuth Dhan: saved token valid for %s****", client_id[:4])
                return True, "Dhan: access token validated.", tok

        # ── Fast path 2: api_key contains JWT (manual-paste mode) ────────────
        if _is_jwt(api_key):
            ok, msg, tok = await asyncio.to_thread(_validate_token, api_key)
            if ok:
                return True, "Dhan: access token (api_key field) validated.", api_key
            return False, f"Dhan: manual token expired — {msg}", ""

        # ── Headless path: generate fresh token via auth.dhan.co ─────────────
        if not pin:
            return (
                False,
                "Dhan: PIN (password) is required for headless token generation. "
                "Enter your Dhan login PIN in the Password field.",
                "",
            )
        if not totp_secret:
            return (
                False,
                "Dhan: TOTP secret is required for headless token generation. "
                "Enter your base32 TOTP seed in the TOTP SECRET field.",
                "",
            )

        def _run_sync() -> Tuple[bool, str, str]:
            import time

            try:
                import pyotp  # type: ignore
            except ImportError:
                return False, "pyotp not installed. pip install pyotp", ""
            try:
                import requests as _req
            except ImportError:
                return False, "requests not installed. pip install requests", ""

            totp_clean = totp_secret.upper().replace(" ", "").replace("-", "")
            try:
                totp_code = pyotp.TOTP(totp_clean).now()
            except Exception as exc:
                return False, f"Dhan: invalid TOTP secret — {exc}", ""

            def _mask(s: str) -> str:
                return (s[:4] + "****") if len(s) > 4 else "****"

            def _attempt(code: str) -> Tuple[bool, str, str]:
                try:
                    r = _req.post(
                        "https://auth.dhan.co/app/generateAccessToken",
                        data={
                            "dhanClientId": client_id,
                            "pin":          pin,
                            "totp":         code,
                        },
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        timeout=15,
                    )
                    try:
                        data = r.json()
                    except Exception:
                        return False, f"Dhan: non-JSON response (HTTP {r.status_code}): {r.text[:200]}", ""

                    if r.status_code == 200:
                        token = data.get("accessToken") or data.get("access_token")
                        if token:
                            logger.info("HeadlessAuth Dhan: token generated for %s", _mask(client_id))
                            return True, "Dhan: access token generated.", token
                        msg = data.get("message") or data.get("errorMessage") or str(data)
                        return False, f"Dhan: token missing in response — {msg}", ""
                    msg = data.get("message") or data.get("errorMessage") or r.text[:200]
                    return False, f"Dhan (HTTP {r.status_code}): {msg}", ""
                except Exception as e:
                    return False, f"Dhan: request error — {e}", ""

            ok, msg, token = _attempt(totp_code)
            if ok:
                return ok, msg, token

            # If TOTP was rejected, wait for next window and retry once
            if "totp" in msg.lower() or "otp" in msg.lower():
                logger.warning("HeadlessAuth Dhan: TOTP rejected — waiting 31s for next window")
                time.sleep(31)
                totp_code2 = pyotp.TOTP(totp_clean).now()
                ok2, msg2, token2 = _attempt(totp_code2)
                if ok2:
                    return ok2, msg2, token2

            # Dhan rate-limits: once every 2 minutes
            if "once every 2 minutes" in msg.lower():
                logger.warning("HeadlessAuth Dhan: rate limit hit — waiting 125s")
                time.sleep(125)
                totp_code3 = pyotp.TOTP(totp_clean).now()
                return _attempt(totp_code3)

            return ok, msg, token

        try:
            return await asyncio.to_thread(_run_sync)
        except Exception as exc:
            logger.error("HeadlessAuth Dhan: thread error: %s", exc)
            return False, f"Dhan: unexpected error — {exc}", ""


# ── Singleton ─────────────────────────────────────────────────────────────────

headless_engine = HeadlessAuthEngine()
