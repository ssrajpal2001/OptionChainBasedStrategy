"""One-time migration: re-encrypt every stored credential in data/clients.db
from the old reversible XOR obfuscation to real Fernet encryption
(data_layer/secure_store.py). Idempotent -- safe to run more than once;
already-migrated ("enc:"-prefixed) values are left untouched.

Generates the master key (.keys/master.key) on first run if one doesn't
already exist. Run this ONCE per environment -- once locally against a dev
copy of the DB (optional, mostly to prove the script works), and once for
real on EC2 via the direct-deploy SSH path, e.g.:

    ssh terminus "cd ~/OptionChainBasedStrategy && python3 scripts/migrate_creds_to_fernet.py"

Usage: python scripts/migrate_creds_to_fernet.py [path/to/clients.db]
       (defaults to data/clients.db)
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_layer.secure_store import decrypt, encrypt, is_encrypted, key_available, generate_key  # noqa: E402

_BROKER_BINDINGS_COLS = ["user_id_enc", "api_key_enc", "api_secret_enc", "password_enc", "totp_secret_enc"]
_FEEDER_CREDS_COLS = ["client_id_enc", "api_key_enc", "secret_enc", "password_enc", "totp_secret_enc"]


def _migrate_table(conn: sqlite3.Connection, table: str, pk_col: str, cred_cols: list[str]) -> int:
    cur = conn.execute(f"SELECT {pk_col}, {', '.join(cred_cols)} FROM {table}")
    rows = cur.fetchall()
    migrated = 0
    for row in rows:
        pk = row[0]
        updates = {}
        for i, col in enumerate(cred_cols, start=1):
            raw = row[i] or ""
            if not raw or is_encrypted(raw):
                continue  # empty, or already migrated -- leave untouched
            plain = decrypt(raw)  # transparently decodes the OLD XOR scheme
            updates[col] = encrypt(plain)  # re-written as real Fernet "enc:"
        if updates:
            set_clause = ", ".join(f"{c}=?" for c in updates)
            conn.execute(f"UPDATE {table} SET {set_clause} WHERE {pk_col}=?",
                         (*updates.values(), pk))
            migrated += 1
    return migrated


def main() -> None:
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/clients.db"
    if not os.path.exists(db_path):
        print(f"No DB at {db_path} -- nothing to migrate.")
        return

    if not key_available():
        path = generate_key()
        print(f"Generated a fresh master key at {path} (none existed on this environment).")
    else:
        print("Master key already present -- reusing it.")

    conn = sqlite3.connect(db_path)
    try:
        n_bindings = _migrate_table(conn, "broker_bindings", "id", _BROKER_BINDINGS_COLS)
        n_feeders = _migrate_table(conn, "system_feeder_creds", "provider", _FEEDER_CREDS_COLS)
        conn.commit()
    finally:
        conn.close()

    print(f"Migrated {n_bindings} broker_bindings row(s), {n_feeders} system_feeder_creds row(s).")
    print("Re-run this script any time -- already-migrated values are skipped automatically.")


if __name__ == "__main__":
    main()
