"""Fernet-encrypted-at-rest credential storage, replacing the old reversible
XOR obfuscation in client_db.py's own _encode_cred/_decode_cred. Mirrors
E:\\AlgoSoft\\Option_Buying\\algo\\secure_env.py's exact pattern: the master
key lives in <repo>/.keys/master.key (dir 0700, file 0600), is never
committed (.gitignore) and never synced by the direct-deploy script either
(scripts/deploy_direct.sh's own .rsync-exclude). Generate a key fresh on
each environment (local dev, EC2) -- it must never travel with the code.

    from data_layer.secure_store import encrypt, decrypt
    stored = encrypt(plaintext)      # "enc:<fernet token>"
    plain  = decrypt(stored)         # transparently decrypts "enc:..." OR
                                      # decodes a legacy XOR value unchanged
                                      # (see decode_legacy_xor's own docstring)
                                      # -- so a mixed-state DB (not yet fully
                                      # migrated) never breaks.

Never log, print or return a decrypted value to a browser/API response.
"""
from __future__ import annotations

import base64
import hashlib
import os
from pathlib import Path
from typing import Optional

PREFIX = "enc:"
BASE = Path(__file__).resolve().parent.parent
KEY_PATH = Path(os.environ.get("ALGOTRADER_KEY_FILE", BASE / ".keys" / "master.key"))

# Same fixed-key XOR scheme client_db.py's own _encode_cred/_decode_cred used
# before this module existed -- key derivation copied EXACTLY (same env var,
# same default seed string, same digest) so decrypt() can transparently read
# values stored under the old scheme during/after migration. Never used for
# new writes -- encrypt() always produces a real Fernet "enc:" value.
_LEGACY_CIPHER_KEY = hashlib.sha256(
    os.environ.get("TERMINUS_CIPHER_KEY", "terminus-key-change-in-production").encode()
).digest()


def _fernet():
    from cryptography.fernet import Fernet
    if not KEY_PATH.is_file():
        # 2026-10-15: lazy self-init on first real use (same pattern already
        # established elsewhere in this codebase, e.g. OptionMatrixEngine's
        # own lazy-init fix) -- a live admin-UI write of a NEW credential
        # must never crash just because no key has been generated yet on
        # this environment. Logged so a fresh key's creation is visible/
        # auditable, not silent.
        import logging
        logging.getLogger(__name__).warning(
            "secure_store: no master key found at %s -- generating one now "
            "(first real use on this environment).", KEY_PATH,
        )
        generate_key()
    return Fernet(KEY_PATH.read_bytes().strip())


def key_available() -> bool:
    return KEY_PATH.is_file()


def generate_key(force: bool = False) -> Path:
    """Create the master key (0600) in a 0700 directory. Refuses to overwrite
    an existing key unless force=True -- overwriting silently would make
    every already-encrypted value permanently undecryptable."""
    from cryptography.fernet import Fernet
    if KEY_PATH.exists() and not force:
        raise FileExistsError(f"{KEY_PATH} already exists")
    KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(KEY_PATH.parent, 0o700)
    except OSError:
        pass
    fd = os.open(KEY_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(Fernet.generate_key())
    return KEY_PATH


def is_encrypted(value: Optional[str]) -> bool:
    return bool(value) and value.startswith(PREFIX)


def encrypt(plaintext: str) -> str:
    """Plaintext -> "enc:<fernet token>". Empty string stays empty (matches
    the old _encode_cred's own no-op-on-empty behavior, avoids encrypting
    "nothing was ever set" into a confusing non-empty stored value)."""
    if not plaintext:
        return ""
    if is_encrypted(plaintext):
        return plaintext
    return PREFIX + _fernet().encrypt(plaintext.encode()).decode()


def _decode_legacy_xor(encoded: str) -> str:
    """Byte-for-byte reverse of client_db.py's original _decode_cred --
    reversible XOR obfuscation (not real encryption), kept only to
    transparently read values written before the Fernet migration ran. New
    code never writes this format again."""
    if not encoded:
        return ""
    try:
        b = base64.b64decode(encoded)
        key = (_LEGACY_CIPHER_KEY * ((len(b) // len(_LEGACY_CIPHER_KEY)) + 1))[: len(b)]
        return bytes(x ^ y for x, y in zip(b, key)).decode("utf-8")
    except Exception:
        return ""


def decrypt(value: Optional[str]) -> str:
    """"enc:..." -> plaintext (needs the key, raises if missing/wrong).
    Anything else is assumed to be a legacy XOR-encoded value and decoded
    with the old scheme -- lets a not-yet-migrated DB keep working exactly
    as before, no flag day required. Empty/None -> ""."""
    if not value:
        return ""
    if not is_encrypted(value):
        return _decode_legacy_xor(value)
    if not key_available():
        raise RuntimeError(f"encrypted value but master key missing: {KEY_PATH}")
    from cryptography.fernet import InvalidToken
    try:
        return _fernet().decrypt(value[len(PREFIX):].encode()).decode()
    except InvalidToken as e:
        raise RuntimeError("cannot decrypt value (wrong master key or corrupted)") from e
