"""
strategies/core/book_manager.py's liquidate_all(scope="system_shutdown") behavior.

2026-08-24: originally skipped the real close only for paper/paper_route
bindings on a routine pm2 restart -- forcing a real close there was
disrupting same-day paper testing for zero real safety benefit.

2026-08-25: extended to skip the real close for EVERY trading_mode, including
live -- direct user decision after a real incident where gurmeet's live NIFTY
straddle was force-closed by a routine restart, and the resulting forced
re-entry picked a materially worse strike pair than the position it had just
been pulled out of. The book's own start() already restores position/session/
pool-engine state (previously only ever exercised for paper/paper_route) with
exits held until fresh post-restart LTPs arrive -- see engine.py. A genuine
kill-switch (scope="FIRM_WIDE", a deliberate explicit action) is UNCHANGED --
still closes everything regardless of trading_mode.
"""
import pytest

from strategies.core.book_manager import StrategyBookManager


class _FakeBook:
    def __init__(self, key):
        self.key = key
        self._position = {"open": True}  # not flat -- has a position to protect
        self.liquidate_called_with = None
        self.stop_async_called = False

    async def liquidate(self, reason):
        self.liquidate_called_with = reason

    async def stop_async(self):
        self.stop_async_called = True


class _FakeDB:
    def __init__(self, bindings: dict) -> None:
        # {client_id: [{"binding_id": ..., "trading_mode": ...}, ...]}
        self._bindings = bindings
        self.raise_on_lookup = False

    def get_bindings_safe_sync(self, client_id):
        if self.raise_on_lookup:
            raise RuntimeError("simulated DB failure")
        return self._bindings.get(client_id, [])


class _OneBookManager(StrategyBookManager):
    """Spawns a single fixed book for one (client, binding, underlying) key."""

    def __init__(self, db, key=("c1", "b1", "NIFTY")):
        super().__init__(bus=None, cfg=None, client_db=db, monitored_indices=[])
        self._key = key
        self.book = _FakeBook(key)
        self._books[key] = self.book

    def _wanted(self):
        return {self._key: 1}

    def _spawn_book(self, key, value):
        return self.book


@pytest.mark.asyncio
async def test_system_shutdown_skips_real_close_for_paper_route_binding():
    db = _FakeDB({"c1": [{"binding_id": "b1", "trading_mode": "paper_route"}]})
    mgr = _OneBookManager(db)

    await mgr.liquidate_all(scope="system_shutdown")

    assert mgr.book.liquidate_called_with is None   # real close skipped
    assert mgr.book.stop_async_called is True        # book still stopped normally


@pytest.mark.asyncio
async def test_system_shutdown_skips_real_close_for_paper_binding():
    db = _FakeDB({"c1": [{"binding_id": "b1", "trading_mode": "paper"}]})
    mgr = _OneBookManager(db)

    await mgr.liquidate_all(scope="system_shutdown")

    assert mgr.book.liquidate_called_with is None
    assert mgr.book.stop_async_called is True


@pytest.mark.asyncio
async def test_system_shutdown_also_skips_real_close_for_live_binding():
    """2026-08-25: the real incident this regression guards against -- a live
    binding used to be force-closed here, producing an unwanted forced
    re-entry at a worse strike. Now it must be skipped exactly like paper."""
    db = _FakeDB({"c1": [{"binding_id": "b1", "trading_mode": "live"}]})
    mgr = _OneBookManager(db)

    await mgr.liquidate_all(scope="system_shutdown")

    assert mgr.book.liquidate_called_with is None
    assert mgr.book.stop_async_called is True


@pytest.mark.asyncio
async def test_system_shutdown_skips_even_when_binding_not_found_or_db_unavailable():
    """The skip decision no longer depends on a trading_mode lookup at all --
    confirm it's unconditional regardless of DB/binding state."""
    for db in (
        _FakeDB({"c1": []}),                      # binding_id not found
        None,                                       # no DB wired at all
    ):
        mgr = _OneBookManager(db)
        await mgr.liquidate_all(scope="system_shutdown")
        assert mgr.book.liquidate_called_with is None
        assert mgr.book.stop_async_called is True


@pytest.mark.asyncio
async def test_firm_wide_kill_switch_always_closes_regardless_of_mode():
    """A deliberate, explicit kill-switch action must NOT be softened by
    trading_mode -- only the incidental-restart (system_shutdown) path is."""
    db = _FakeDB({"c1": [{"binding_id": "b1", "trading_mode": "paper_route"}]})
    mgr = _OneBookManager(db)

    await mgr.liquidate_all(scope="FIRM_WIDE")

    assert mgr.book.liquidate_called_with == "kill_switch"
