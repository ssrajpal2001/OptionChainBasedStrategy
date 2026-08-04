"""
Regression test for strategies/fno_positional/book_manager.py's _is_flat()
override.

FnOPositionalBook stores positions in a list (self._positions / the
_open_positions property), not a singular self._position like the
AbstractStrategyBook-based books (SellStraddle, D1Trap, FVG, V4Cascade). The
base StrategyBookManager's default _is_flat() (`getattr(book, "_position",
None) is None`) is therefore unconditionally True for any FnO book -- even
one with real open broker positions -- so on removal it always took the
plain _stop_book() path and never the liquidate-aware path.

Uses a minimal fake shaped like FnOPositionalBook (not the real class, which
needs a live Upstox token / DB / bus to construct) -- just enough to exercise
_is_flat()'s attribute reads.
"""
from strategies.fno_positional.book_manager import FnOPositionalBookManager


class _FakeFnOBook:
    """Minimal stand-in for FnOPositionalBook: a `_positions` list plus an
    `_open_positions` property filtering to non-closed statuses -- the exact
    shape FnOPositionalBookManager._is_flat() must read."""

    def __init__(self, statuses):
        self._positions = list(statuses)

    @property
    def _open_positions(self):
        return [s for s in self._positions if s in ("ENTRY_PLACED", "OPEN")]


def _mgr():
    # bus/cfg/client_db/monitored_indices are unused by _is_flat().
    return FnOPositionalBookManager(bus=None, cfg=None, client_db=None, monitored_indices=[])


def test_is_flat_true_when_no_open_positions():
    mgr = _mgr()
    book = _FakeFnOBook(statuses=[])
    assert mgr._is_flat(book) is True

    book_all_closed = _FakeFnOBook(statuses=["CLOSED", "CLOSED"])
    assert mgr._is_flat(book_all_closed) is True


def test_is_flat_false_when_open_position_present():
    mgr = _mgr()
    book = _FakeFnOBook(statuses=["OPEN"])
    assert mgr._is_flat(book) is False

    book_pending_entry = _FakeFnOBook(statuses=["ENTRY_PLACED", "CLOSED"])
    assert mgr._is_flat(book_pending_entry) is False


def test_is_flat_true_when_positions_attribute_missing():
    """A book that doesn't even have a `_positions` attribute (defensive
    fallback -- should never happen for a real FnOPositionalBook, but must
    not crash reconcile) is treated as flat rather than raising."""
    mgr = _mgr()

    class _NoPositionsBook:
        pass

    assert mgr._is_flat(_NoPositionsBook()) is True
