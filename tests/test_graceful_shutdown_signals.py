"""
tests/test_graceful_shutdown_signals.py -- regression for the 2026-08-23
fix adding SIGTERM/SIGINT handling to run_system.py.

Real gap: run_system.py never registered a signal handler at all. `pm2
restart` (or any OS-level kill) sends SIGTERM; with no handler, Python's
default disposition terminates the process outright, skipping the
graceful stop_async()->liquidate_all() emergency-square-off path entirely
-- on literally every routine deploy restart, not just an unexpected
crash. _register_graceful_shutdown_signals() wires SIGTERM/SIGINT to set
the same shutdown_event the FIRST_COMPLETED barrier / admin console
shutdown callback already uses.

Drives the registered CALLBACK directly (via a mocked add_signal_handler)
rather than sending real OS signals -- POSIX signal delivery through
os.kill()/asyncio is inherently platform-fragile to test (Windows in
particular has no real signal semantics), and the actual behavior this
fix needs to prove is "the callback registered for SIGTERM/SIGINT sets
shutdown_event", not "the OS successfully delivers a signal".
"""
import asyncio
import signal
from unittest.mock import patch

import pytest

from run_system import _register_graceful_shutdown_signals


@pytest.mark.asyncio
async def test_registers_handlers_for_both_sigterm_and_sigint():
    shutdown_event = asyncio.Event()
    registered = {}

    def _fake_add_signal_handler(sig, callback, *args):
        registered[sig] = (callback, args)

    loop = asyncio.get_running_loop()
    with patch.object(loop, "add_signal_handler", side_effect=_fake_add_signal_handler):
        _register_graceful_shutdown_signals(shutdown_event)

    assert signal.SIGTERM in registered
    assert signal.SIGINT in registered


@pytest.mark.asyncio
async def test_sigterm_callback_sets_the_shutdown_event():
    shutdown_event = asyncio.Event()
    registered = {}

    def _fake_add_signal_handler(sig, callback, *args):
        registered[sig] = (callback, args)

    loop = asyncio.get_running_loop()
    with patch.object(loop, "add_signal_handler", side_effect=_fake_add_signal_handler):
        _register_graceful_shutdown_signals(shutdown_event)

    assert not shutdown_event.is_set()
    callback, args = registered[signal.SIGTERM]
    callback(*args)   # exactly what the event loop does when the real signal arrives
    assert shutdown_event.is_set()


@pytest.mark.asyncio
async def test_sigint_callback_sets_the_shutdown_event():
    shutdown_event = asyncio.Event()
    registered = {}

    def _fake_add_signal_handler(sig, callback, *args):
        registered[sig] = (callback, args)

    loop = asyncio.get_running_loop()
    with patch.object(loop, "add_signal_handler", side_effect=_fake_add_signal_handler):
        _register_graceful_shutdown_signals(shutdown_event)

    callback, args = registered[signal.SIGINT]
    callback(*args)
    assert shutdown_event.is_set()


@pytest.mark.asyncio
async def test_gracefully_degrades_when_add_signal_handler_not_implemented():
    """Windows: loop.add_signal_handler raises NotImplementedError. Must not
    propagate -- the app should still start, just without this protection."""
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    with patch.object(loop, "add_signal_handler", side_effect=NotImplementedError):
        _register_graceful_shutdown_signals(shutdown_event)   # must not raise
    assert not shutdown_event.is_set()


def test_gracefully_degrades_when_no_running_loop():
    """Called outside a running event loop (asyncio.get_running_loop()
    raises RuntimeError, caught by the broad except) -- must not raise,
    just log and fall back to default signal behavior."""
    shutdown_event = asyncio.Event()
    _register_graceful_shutdown_signals(shutdown_event)   # must not raise
