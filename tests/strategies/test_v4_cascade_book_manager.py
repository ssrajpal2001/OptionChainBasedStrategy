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
                 lot_multiplier=1, squareoff_time="15:15", use_pool_engine=False,
                 tracking_offsets_pts=None):
        self._underlying = underlying; self._client_id = client_id; self._binding_id = binding_id
        self._lot_multiplier = lot_multiplier
        self.squareoff_time = squareoff_time
        self.use_pool_engine = use_pool_engine
        self.tracking_offsets_pts = tracking_offsets_pts
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


def test_pool_engine_off_by_default_for_nifty(monkeypatch):
    monkeypatch.delenv("V4CASCADE_USE_POOL_ENGINE", raising=False)
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "NIFTY")]})
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    assert m.books[0].use_pool_engine is False


def test_pool_engine_on_for_nifty_when_env_var_set(monkeypatch):
    monkeypatch.setenv("V4CASCADE_USE_POOL_ENGINE", "1")
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "NIFTY")]})
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    assert m.books[0].use_pool_engine is True


def test_pool_engine_still_off_for_crudeoil_even_with_env_var_set(monkeypatch):
    """2026-07-24: the pool engine has only ever been validated against
    NIFTY -- the env toggle must not accidentally activate it for CRUDEOIL,
    even though book.py's own _is_mcx guard would also block it. Belt and
    suspenders: the manager should never even pass True for a non-NIFTY
    underlying in the first place."""
    monkeypatch.setenv("V4CASCADE_USE_POOL_ENGINE", "1")
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "CRUDEOIL", squareoff_time="23:15")]})
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    assert m.books[0].use_pool_engine is False


def test_tracking_offsets_default_when_env_unset(monkeypatch):
    monkeypatch.delenv("V4CASCADE_TRACKING_OFFSETS", raising=False)
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "NIFTY")]})
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    assert m.books[0].tracking_offsets_pts is None


def test_tracking_offsets_parsed_from_env(monkeypatch):
    monkeypatch.setenv("V4CASCADE_USE_POOL_ENGINE", "1")
    monkeypatch.setenv("V4CASCADE_TRACKING_OFFSETS", "100,200,300,400,500")
    monkeypatch.setattr(vm_mod, "V4CascadeBook", _FakeBook)
    db = _DB({"C1": [_dep("Z1", "NIFTY")]})
    m = V4CascadeBookManager(bus=None, cfg=None, client_db=db, monitored_indices=[])
    m._reconcile()
    assert m.books[0].tracking_offsets_pts == [100.0, 200.0, 300.0, 400.0, 500.0]
