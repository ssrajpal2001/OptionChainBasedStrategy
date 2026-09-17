"""Regression test for the 2026-09-17 real gap: IronFlyBookManager._spawn_book
never injected self._rebalancer into a freshly-spawned book, unlike
StraddleBookManager's own _spawn_book (which already does this). The base
StrategyBookManager.set_rebalancer() only reaches books that already exist
at the moment it's called -- since every real Iron Fly book is spawned
LATER by this manager's own reconcile loop, the rebalancer never reached
any real book, silently making the 2026-09-17 strike-pinning fix in
engine.py inert (confirmed live: zero "strikes pinned" log lines ever
appeared for an actively-adjusting real Iron Fly position)."""
from strategies.iron_fly.book_manager import IronFlyBookManager


class _FakeRebalancer:
    def __init__(self):
        self.pinned = set()

    def pin_strike(self, underlying, strike):
        self.pinned.add(strike)

    def unpin_strike(self, underlying, strike):
        self.pinned.discard(strike)

    def pinned_strikes(self, underlying):
        return set(self.pinned)


def _manager():
    return IronFlyBookManager(bus=None, cfg=None, client_db=None, monitored_indices=[])


def test_spawn_book_injects_rebalancer_when_already_set_on_manager():
    mgr = _manager()
    reb = _FakeRebalancer()
    mgr._rebalancer = reb   # simulates set_rebalancer() having already run on the manager

    book = mgr._spawn_book(
        ("c1", "b1", "NIFTY"),
        {"lots": 1, "product_type": "NRML", "otm1": 50, "adjustment_distance": 100.0,
         "short_threshold": 20.0, "long_threshold": 20.0, "profit_target_pct": 0.65,
         "chain_depth_strikes": 20},
    )

    assert book._rebalancer is reb


def test_spawn_book_does_not_crash_when_no_rebalancer_set_yet():
    mgr = _manager()
    book = mgr._spawn_book(
        ("c1", "b1", "NIFTY"),
        {"lots": 1, "product_type": "NRML", "otm1": 50, "adjustment_distance": 100.0,
         "short_threshold": 20.0, "long_threshold": 20.0, "profit_target_pct": 0.65,
         "chain_depth_strikes": 20},
    )
    assert book._rebalancer is None
