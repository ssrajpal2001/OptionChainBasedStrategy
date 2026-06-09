"""Phase 2b — show_granular_ticks column migration + setter round-trip."""
import asyncio
from data_layer.client_db import ClientDB


def test_granular_ticks_roundtrip(tmp_path):
    db = ClientDB(db_path=str(tmp_path / "t.db"))

    async def _run():
        await db.initialise()
        await db.register_client(client_id="c1", name="T", password="x", capital=100000)
        await db.upsert_binding(client_id="c1", binding_id="b1", provider="mock")
        # Default is 0 (off)
        b = db.get_bindings_safe_sync("c1")[0]
        assert b["show_granular_ticks"] == 0
        # Enable, then disable
        await db.set_show_granular_ticks("c1", "b1", True)
        assert db.get_bindings_safe_sync("c1")[0]["show_granular_ticks"] == 1
        await db.set_show_granular_ticks("c1", "b1", False)
        assert db.get_bindings_safe_sync("c1")[0]["show_granular_ticks"] == 0

    asyncio.run(_run())
