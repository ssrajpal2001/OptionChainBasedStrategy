"""V4CascadeBookManager must accept CRUDEOIL deployments (previously only
NIFTY/BTC/ETH), and its default squareoff_time fallback must be
underlying-aware -- a CRUDEOIL deployment with no configured squareoff_time
must not silently default to NIFTY's 15:15 (which would instantly force-
close it mid-session, the exact class of bug already documented for
sell_straddle in project memory)."""
import strategies.v4_cascade_book_manager as vm_mod
from strategies.v4_cascade_book_manager import V4CascadeBookManager


class _FakeBook:
    def __init__(self, bus, cfg, underlying="NIFTY", client_id="", binding_id="",
                 lot_multiplier=1, squareoff_time="15:15"):
        self._underlying = underlying; self._client_id = client_id; self._binding_id = binding_id
        self._lot_multiplier = lot_multiplier
        self.squareoff_time = squareoff_time
        self.started = False

    def set_client_db(self, db):
        pass

    def start(self):
        self.started = True


class _DB:
    def __init__(self, deps):
        self._deps = deps

    def get_running_deployments_by_strategy_sync(self, strategy_name):
        result = []
        for cid, deps in self._deps.items():
            for d in deps:
                if d.get("strategy_name") == strategy_name and int(d.get("is_running", 0) or 0) == 1:
                    result.append({**d, "client_id": cid})
        return result

    def get_deployments_sync(self, cid):
        return self._deps.get(cid, [])


def _dep(bid, und, is_running=1, lot_multiplier=1, squareoff_time=None):
    d = {"binding_id": bid, "strategy_name": "v4_cascade", "underlying": und,
         "is_running": is_running, "lot_multiplier": lot_multiplier}
    if squareoff_time is not None:
        d["squareoff_time"] = squareoff_time
    return d


def test_crudeoil_deployment_spawns(monkeypatch):
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "CRUDEOIL", squareoff_time="23:15")]})
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    assert len(m.books) == 1
    book = m.books[0]
    assert book._underlying == "CRUDEOIL"
    assert book.squareoff_time == "23:15"


def test_crudeoil_deployment_missing_squareoff_defaults_to_mcx_time(monkeypatch):
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "CRUDEOIL", squareoff_time=None)]})
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    book = m.books[0]
    assert book.squareoff_time == "23:15"  # NOT NIFTY's "15:15" default


def test_unsupported_underlying_still_skipped(monkeypatch):
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "GOLD")]})  # not in _SUPPORTED_UNDERLYINGS
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    assert m.books == []
