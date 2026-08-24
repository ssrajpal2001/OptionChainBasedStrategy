"""
2026-08-24: regression for strategies/core/book_manager.py's
liquidate_all(scope="system_shutdown") change.

Real gap found live: the 2026-08-23 SIGTERM-handling fix made EVERY
routine `pm2 restart`/`pm2 stop` force-close every open position across
every strategy, including paper/paper_route bindings where there is no
real broker exposure to protect (a paper_route "position" is local
simulated bookkeeping; even the real order attempt is expected to be
broker-rejected). This disrupted same-day paper testing for zero real
safety benefit. Fix: scope="system_shutdown" (the ONLY caller is
stop_async(), reached exclusively via a genuine graceful shutdown) now
skips the real close for paper/paper_route bindings -- the book still
stops normally either way, only the close call is skipped. A genuine
kill-switch (scope="FIRM_WIDE", a deliberate explicit action) is
UNCHANGED -- still closes everything regardless of trading_mode. Any
binding actually in live mode is also unaffected -- real capital
protection is fully intact.
"""
import asyncio

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
async def test_system_shutdown_still_closes_live_binding():
    db = _FakeDB({"c1": [{"binding_id": "b1", "trading_mode": "live"}]})
    mgr = _OneBookManager(db)

    await mgr.liquidate_all(scope="system_shutdown")

    assert mgr.book.liquidate_called_with == "system_shutdown"
    assert mgr.book.stop_async_called is True


@pytest.mark.asyncio
async def test_system_shutdown_defaults_to_live_when_binding_not_found():
    db = _FakeDB({"c1": []})   # binding_id "b1" not in the list at all
    mgr = _OneBookManager(db)

    await mgr.liquidate_all(scope="system_shutdown")

    assert mgr.book.liquidate_called_with == "system_shutdown"   # fail toward protection


@pytest.mark.asyncio
async def test_system_shutdown_defaults_to_live_when_db_lookup_raises():
    db = _FakeDB({"c1": [{"binding_id": "b1", "trading_mode": "paper_route"}]})
    db.raise_on_lookup = True
    mgr = _OneBookManager(db)

    await mgr.liquidate_all(scope="system_shutdown")

    assert mgr.book.liquidate_called_with == "system_shutdown"   # fail toward protection


@pytest.mark.asyncio
async def test_system_shutdown_defaults_to_live_when_no_db_wired():
    mgr = _OneBookManager(db=None)

    await mgr.liquidate_all(scope="system_shutdown")

    assert mgr.book.liquidate_called_with == "system_shutdown"


@pytest.mark.asyncio
async def test_firm_wide_kill_switch_always_closes_regardless_of_mode():
    """A deliberate, explicit kill-switch action must NOT be softened by
    trading_mode -- only the incidental-restart (system_shutdown) path is."""
    db = _FakeDB({"c1": [{"binding_id": "b1", "trading_mode": "paper_route"}]})
    mgr = _OneBookManager(db)

    await mgr.liquidate_all(scope="FIRM_WIDE")

    assert mgr.book.liquidate_called_with == "kill_switch"
