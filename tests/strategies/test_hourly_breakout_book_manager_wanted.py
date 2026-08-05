"""hourly_breakout's _wanted() must propagate a DB read failure, not swallow
it into an empty dict -- strategies/core/book_manager.py's _reconcile()
centrally catches this and skips the tick, leaving existing books untouched.
Same contract as the other 5 strategy managers, fixed earlier."""
import pytest
from strategies.hourly_breakout.book_manager import HourlyBreakoutBookManager


class _RaisingDB:
    def get_running_deployments_by_strategy_sync(self, name):
        raise RuntimeError("simulated DB failure")


def test_wanted_propagates_db_exception_instead_of_swallowing():
    mgr = HourlyBreakoutBookManager(bus=None, cfg=None, client_db=_RaisingDB(), monitored_indices=[])
    with pytest.raises(RuntimeError):
        mgr._wanted()
