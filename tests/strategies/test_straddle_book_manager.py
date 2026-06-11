"""StraddleBookManager spawns one independent book per (client,binding,index) sell_straddle
deployment, auto-spawns on deploy, and stops a book when its deployment is removed."""
import strategies.straddle_book_manager as bm_mod
from strategies.straddle_book_manager import StraddleBookManager


class _FakeBook:
    def __init__(self, bus, cfg, underlying="NIFTY", client_id="", binding_id=""):
        self._underlying = underlying; self._client_id = client_id; self._binding_id = binding_id
        self.started = False; self.stopped = False
    def set_client_db(self, db): self._db = db
    def start(self): self.started = True
    def stop(self): self.stopped = True


class _DB:
    def __init__(self, deps): self._deps = deps          # {client_id: [deployment dicts]}
    def get_all_clients_sync(self): return [{"client_id": c} for c in self._deps]
    def get_deployments_sync(self, cid): return self._deps.get(cid, [])


def _mgr(monkeypatch, deps):
    monkeypatch.setattr(bm_mod, "SellStraddleStrategy", _FakeBook)
    return StraddleBookManager(bus=None, cfg=None, client_db=_DB(deps),
                               monitored_indices=["NIFTY", "BANKNIFTY"])


def _dep(bid, und="NIFTY", strat="sell_straddle"):
    return {"binding_id": bid, "strategy_name": strat, "underlying": und}


def test_spawns_one_book_per_binding(monkeypatch):
    m = _mgr(monkeypatch, {"C1": [_dep("Z1")], "C2": [_dep("Z9")]})
    m._reconcile()
    keys = {(b._client_id, b._binding_id, b._underlying) for b in m.books}
    assert keys == {("C1", "Z1", "NIFTY"), ("C2", "Z9", "NIFTY")}
    assert all(b.started for b in m.books)
    assert m.find("C1", "Z1", "NIFTY") is not None


def test_ignores_other_strategies_and_unmonitored_index(monkeypatch):
    m = _mgr(monkeypatch, {"C1": [_dep("Z1", strat="iron_condor"), _dep("Z2", und="SENSEX")]})
    m._reconcile()
    assert m.books == []          # IC ignored; SENSEX not in monitored indices


def test_auto_spawn_on_deploy_and_stop_on_remove(monkeypatch):
    db = _DB({"C1": [_dep("Z1")]})
    monkeypatch.setattr(bm_mod, "SellStraddleStrategy", _FakeBook)
    m = StraddleBookManager(None, None, db, ["NIFTY"])
    m._reconcile()
    assert len(m.books) == 1
    # New deployment appears → next reconcile spawns it.
    db._deps["C1"].append(_dep("Z2"))
    m._reconcile()
    assert len(m.books) == 2
    # Deployment removed → its book is stopped + dropped.
    removed = m.find("C1", "Z1", "NIFTY")
    db._deps["C1"] = [_dep("Z2")]
    m._reconcile()
    assert removed.stopped is True
    assert m.find("C1", "Z1", "NIFTY") is None and m.find("C1", "Z2", "NIFTY") is not None
