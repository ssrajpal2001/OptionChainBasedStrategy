"""
ui_layer/auth.py — Lightweight HMAC-SHA256 JWT helpers.

Uses only Python stdlib (base64, hashlib, hmac, json, os, time).
No external dependencies required.

Secret is read once from env var TERMINUS_JWT_SECRET at import time.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Dict

# ── Secret & defaults ─────────────────────────────────────────────────────────

_SECRET: bytes = os.getenv(
    "TERMINUS_JWT_SECRET", "terminus-dev-secret-CHANGE-IN-PRODUCTION"
).encode()
_ALGO = "HS256"
_TTL  = 8 * 3600   # 8 hours — one full trading session


# ── Internal base64url helpers ────────────────────────────────────────────────

def _b64u_enc(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64u_dec(s: str) -> bytes:
    pad = (-len(s)) % 4
    return base64.urlsafe_b64decode(s + "=" * pad)


# ── Public API ────────────────────────────────────────────────────────────────

def create_token(sub: str, role: str, client_id: str = "") -> str:
    """
    Mint a signed HS256 JWT.

    Args:
        sub:       Username or client_id (used as JWT subject).
        role:      'admin' | 'client'
        client_id: Non-empty only for client role.

    Returns compact serialised JWT string (header.payload.signature).
    """
    header  = _b64u_enc(json.dumps({"alg": _ALGO, "typ": "JWT"}, separators=(",", ":")).encode())
    payload = _b64u_enc(json.dumps({
        "sub":       sub,
        "role":      role,
        "client_id": client_id,
        "exp":       int(time.time()) + _TTL,
    }, separators=(",", ":")).encode())
    sig_input = f"{header}.{payload}".encode()
    sig = hmac.new(_SECRET, sig_input, hashlib.sha256).digest()
    return f"{header}.{payload}.{_b64u_enc(sig)}"


def verify_token(token: str) -> Dict:
    """
    Verify and decode a JWT.

    Returns the payload dict on success.
    Raises ValueError with a descriptive message on any failure (bad format,
    invalid signature, expiry, corrupt payload).
    """
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Malformed token — expected 3 dot-separated parts.")

    header_b64, payload_b64, sig_b64 = parts
    expected_sig = hmac.new(
        _SECRET, f"{header_b64}.{payload_b64}".encode(), hashlib.sha256
    ).digest()

    try:
        provided_sig = _b64u_dec(sig_b64)
    except Exception:
        raise ValueError("Malformed token signature encoding.")

    if not hmac.compare_digest(provided_sig, expected_sig):
        raise ValueError("Invalid token signature.")

    try:
        payload: Dict = json.loads(_b64u_dec(payload_b64))
    except Exception as exc:
        raise ValueError(f"Corrupt token payload: {exc}")

    if payload.get("exp", 0) < time.time():
        raise ValueError("Token has expired.")

    return payload
