"""
broker_auth/headless_auth.py — Token validation layer (OAuth-only).

ARCHITECTURE (post-refactor):
  All headless browser automation, TOTP cracking, and credential injection
  have been removed. Authentication is now exclusively Interactive OAuth:

  1. Check DB for a valid cached access_token (generated today, passes API ping)
     -> If valid: return (True, "Token valid", token)
  2. If no valid token: return (False, "oauth_required", "")
     -> Caller uses oauth_manager.generate_auth_url() to redirect the user
     -> User logs in on broker's portal
     -> /callback/{broker} route exchanges auth_code for access_token
     -> Token stored in DB via update_access_token()

NO passwords, PINs, or TOTP secrets are used here.
Token validation is a lightweight API ping only.

For Dhan / Angel One / Shoonya (no standard OAuth redirect):
  authenticate_binding returns (False, "manual_token_required", "")
  -> User pastes their token directly in the UI
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Tuple

from config.global_config import IST
from broker_auth.oauth_manager import validate_token

if TYPE_CHECKING:
    from data_layer.client_db import ClientDB

logger = logging.getLogger(__name__)

_TOKEN_EXPIRY_BUFFER_HOURS = 1


def _ist_today() -> str:
    return datetime.now(IST).date().isoformat()


def _ist_eod() -> str:
    now = datetime.now(IST)
    return now.replace(hour=23, minute=59, second=59, microsecond=0).isoformat()


def _token_is_fresh(generated_at: str, expiry_at: str = "") -> bool:
    """True if token was generated today and not within expiry buffer."""
    try:
        if not generated_at:
            return False
        if generated_at[:10] != _ist_today():
            return False
        if expiry_at:
            from datetime import timedelta
            expiry = datetime.fromisoformat(expiry_at)
            now = datetime.now(IST)
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=IST)
            if (expiry - now).total_seconds() / 3600 < _TOKEN_EXPIRY_BUFFER_HOURS:
                return False
        return True
    except Exception:
        return False


# ── Token Validator ───────────────────────────────────────────────────────────

class HeadlessAuthEngine:
    """
    OAuth-aware token validator.

    authenticate_binding():
      1. If cached token is fresh and passes API ping -> (True, "Token valid", token)
      2. Otherwise -> (False, reason, "") where reason is:
           "oauth_required"       -- provider supports OAuth redirect (Fyers/Upstox/Zerodha)
           "manual_token_required"-- provider needs manual token paste (Dhan/AngelOne/Shoonya)
           "mock_ok"              -- mock provider, always succeeds

    The dashboard /connect endpoint reads the reason and either:
      a) Returns the broker OAuth URL for user to visit, OR
      b) Shows manual token paste instructions
    """

    async def authenticate_binding(
        self,
        binding:   dict,
        client_id: str,
        db:        "ClientDB",
    ) -> Tuple[bool, str, str]:
        t0 = time.monotonic()
        provider   = (binding.get("provider") or "mock").lower()
        binding_id = binding.get("binding_id", "")
        api_key    = binding.get("api_key", "")
        token      = binding.get("access_token", "")
        gen_at     = binding.get("token_generated_at", "")
        exp_at     = binding.get("token_expiry_at", "")

        # Mock always succeeds
        if provider == "mock":
            logger.info("[Auth] [%s/%s] mock -> instant connect", client_id, binding_id)
            return True, "Mock broker connected.", "mock_token"

        # Step 1: Check cached token freshness
        if token and _token_is_fresh(gen_at, exp_at):
            logger.info(
                "[Auth] [%s/%s] token fresh (generated %s) -> validating via API ping",
                client_id, binding_id, gen_at[:10],
            )
            valid = await asyncio.to_thread(validate_token, provider, api_key, token)
            elapsed = (time.monotonic() - t0) * 1000
            if valid:
                logger.info(
                    "[Auth] [%s/%s] CONNECTED (cached token valid) in %.1fms",
                    client_id, binding_id, elapsed,
                )
                return True, "Token valid — connected.", token
            logger.info(
                "[Auth] [%s/%s] cached token REJECTED by broker in %.1fms",
                client_id, binding_id, elapsed,
            )

        # Step 2: No valid token — signal which flow is needed
        from broker_auth.oauth_manager import supports_oauth, requires_manual_token
        elapsed = (time.monotonic() - t0) * 1000

        if supports_oauth(provider):
            logger.info(
                "[Auth] [%s/%s] no valid token -> OAuth redirect required (%.1fms)",
                client_id, binding_id, elapsed,
            )
            return False, "oauth_required", ""

        if requires_manual_token(provider):
            logger.info(
                "[Auth] [%s/%s] no valid token -> manual token paste required (%.1fms)",
                client_id, binding_id, elapsed,
            )
            return False, "manual_token_required", ""

        return False, f"Unknown provider '{provider}'", ""

    async def validate_feeder_creds(
        self,
        provider: str,
        creds:    dict,
    ) -> Tuple[bool, str, str]:
        """
        Validate feeder credentials.
        If cached token is valid -> return it.
        Otherwise -> (False, "oauth_required" or "manual_token_required", "")
        """
        token  = creds.get("access_token", "")
        gen_at = creds.get("token_generated_at", "")
        exp_at = creds.get("token_expiry_at", "")
        api_key = creds.get("api_key", "") or creds.get("app_key", "")

        if provider == "mock":
            return True, "Mock feeder.", "mock_token"

        if token and _token_is_fresh(gen_at, exp_at):
            t0 = time.monotonic()
            valid = await asyncio.to_thread(validate_token, provider, api_key, token)
            elapsed = (time.monotonic() - t0) * 1000
            if valid:
                logger.info(
                    "[Auth] Feeder %s cached token VALID in %.1fms", provider, elapsed,
                )
                return True, f"{provider.capitalize()} feeder token valid.", token
            logger.info("[Auth] Feeder %s cached token REJECTED in %.1fms", provider, elapsed)

        from broker_auth.oauth_manager import supports_oauth
        if supports_oauth(provider):
            return False, "oauth_required", ""
        return False, "manual_token_required", ""


# Singleton
headless_engine = HeadlessAuthEngine()
