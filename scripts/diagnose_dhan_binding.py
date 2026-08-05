"""
scripts/diagnose_dhan_binding.py — dump masked diagnostics for a Dhan broker
binding so the actual stored values can be sanity-checked without printing
full secrets to a shared terminal/log.

Usage (run from the repo root on EC2, where data/clients.db lives):
    python3 scripts/diagnose_dhan_binding.py [client_id] [binding_id]

Defaults to client_id=ssrajpal2001 if omitted. If binding_id is omitted,
prints every "dhan" provider binding for that client.

Field mapping for Dhan (see monitor.html's brokerFieldLabels):
    user_id_enc   -> CLIENT ID   (Dhan trading account client ID)
    api_key_enc   -> APP ID      (from developers.dhan.co)
    api_secret_enc-> APP SECRET  (from developers.dhan.co)
"""
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from data_layer.client_db import _decode_cred  # noqa: E402


def _mask(label: str, value: str) -> None:
    if not value:
        print(f"  {label:<12}: <EMPTY>")
        return
    stripped = value.strip()
    has_ws = stripped != value
    n = len(value)
    preview = f"{value[:3]}...{value[-3:]}" if n > 6 else "*" * n
    flags = []
    if has_ws:
        flags.append("HAS LEADING/TRAILING WHITESPACE")
    if "\n" in value or "\r" in value:
        flags.append("HAS NEWLINE CHAR")
    flag_str = f"  <-- {', '.join(flags)}" if flags else ""
    print(f"  {label:<12}: len={n:<4} value={preview}{flag_str}")


def main() -> None:
    client_id = sys.argv[1] if len(sys.argv) > 1 else "ssrajpal2001"
    binding_id = sys.argv[2] if len(sys.argv) > 2 else None

    db_path = ROOT / "data" / "clients.db"
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    if binding_id:
        rows = con.execute(
            "SELECT * FROM broker_bindings WHERE client_id=? AND binding_id=?",
            (client_id, binding_id),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT * FROM broker_bindings WHERE client_id=? AND provider='dhan'",
            (client_id,),
        ).fetchall()

    if not rows:
        print(f"No Dhan binding found for client_id={client_id!r} binding_id={binding_id!r}")
        return

    for b in rows:
        print(f"\n=== {b['client_id']} / {b['binding_id']} (provider={b['provider']}) ===")
        print(f"  enabled          : {b['enabled']}")
        print(f"  is_trade_enabled : {b['is_trade_enabled']}")
        print(f"  trading_mode     : {b['trading_mode']}")
        print(f"  terminal? (check broker_bindings columns present)")
        try:
            client_id_val = _decode_cred(b["user_id_enc"] or "")
        except Exception as exc:
            client_id_val = f"<decode error: {exc}>"
        try:
            app_id_val = _decode_cred(b["api_key_enc"] or "")
        except Exception as exc:
            app_id_val = f"<decode error: {exc}>"
        try:
            app_secret_val = _decode_cred(b["api_secret_enc"] or "")
        except Exception as exc:
            app_secret_val = f"<decode error: {exc}>"

        _mask("CLIENT ID", client_id_val)
        _mask("APP ID", app_id_val)
        _mask("APP SECRET", app_secret_val)

    con.close()


if __name__ == "__main__":
    main()
