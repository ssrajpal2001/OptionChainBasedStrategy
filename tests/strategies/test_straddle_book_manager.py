"""StraddleBookManager spawns one independent book per (client,binding,index) sell_straddle
deployment, auto-spawns on deploy, and stops a book when its deployment is removed."""
import strategies.straddle_book_manager as bm_mod
from strategies.straddle_book_manager import StraddleBookManager


class _FakeBook:
    def __init__(self, bus, cfg, underlying="NIFTY", lot_multiplier=1, client_id="", binding_id="",
                 shadow_on_reject=False, vwap_source_override=None):
        self._underlying = underlying; self._client_id = client_id; self._binding_id = binding_id
        self._lot_multiplier = lot_multiplier
        self._shadow_on_reject = shadow_on_reject
        self._vwap_source_override = vwap_source_override
        self._position = None
        self.started = False; self.stopped = False
    def set_client_db(self, db): self._db = db
    def start(self): self.started = True
    def stop(self): self.stopped = True


class _DB:
    def __init__(self, deps): self._deps = deps          # {client_id: [deployment dicts]}
    def get_all_clients_sync(self): return [{"client_id": c} for c in self._deps]
    def get_deployments_sync(self, cid): return self._deps.get(cid, [])
    def get_running_straddle_deployments_sync(self):
        """Batch query: all is_running=1 sell_straddle deployments. (Replaces N+1 loop.)"""
        result = []
        for cid, deps in self._deps.items():
            for d in deps:
                if str(d.get("strategy_name", "")).lower() == "sell_straddle" and int(d.get("is_running", 0) or 0) == 1:
                    result.append({
                        "client_id": cid,
                        "binding_id": d.get("binding_id", ""),
                        "underlying": d.get("underlying", ""),
                        "lot_multiplier": d.get("lot_multiplier", 1),
                        "strategy_name": d.get("strategy_name", ""),
                        "is_running": d.get("is_running", 0),
                        "assigned_instrument": d.get("assigned_instrument", ""),
                        "strategy_params": d.get("strategy_params", ""),
                    })
        return result


def _mgr(monkeypatch, deps):
    monkeypatch.setattr(bm_mod, "SellStraddleStrategy", _FakeBook)
    return StraddleBookManager(bus=None, cfg=None, client_db=_DB(deps),
                               monitored_indices=["NIFTY", "BANKNIFTY"])


def _dep(bid, und="NIFTY", strat="sell_straddle", is_running=1, lot_multiplier=1, strategy_params=""):
    return {"binding_id": bid, "strategy_name": strat, "underlying": und,
            "is_running": is_running, "lot_multiplier": lot_multiplier,
            "strategy_params": strategy_params}


def test_only_running_deployments_spawn(monkeypatch):
    # is_running=0 → deployed but Run toggle OFF → no book; toggling ON spawns it.
    db = _DB({"C1": [_dep("Z1", is_running=0)]})
    monkeypatch.setattr(bm_mod, "SellStraddleStrategy", _FakeBook)
    m = StraddleBookManager(None, None, db, ["NIFTY"])
    m._reconcile()
    assert m.books == []
    db._deps["C1"][0]["is_running"] = 1
    m._reconcile()
    assert len(m.books) == 1


def test_lot_multiplier_propagates(monkeypatch):
    m = _mgr(monkeypatch, {"C1": [_dep("Z1", lot_multiplier=3)]})
    m._reconcile()
    assert m.find("C1", "Z1", "NIFTY")._lot_multiplier == 3


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


def test_spawns_one_book_per_underlying_on_same_binding(monkeypatch):
    # Same client/broker can run sell_straddle on NIFTY and CRUDEOIL simultaneously.
    monkeypatch.setattr(bm_mod, "SellStraddleStrategy", _FakeBook)
    db = _DB({"C1": [_dep("Z1", und="NIFTY"), _dep("Z1", und="CRUDEOIL")]})
    m = StraddleBookManager(None, None, db, ["NIFTY", "CRUDEOIL"])
    m._reconcile()
    keys = {(b._client_id, b._binding_id, b._underlying) for b in m.books}
    assert keys == {("C1", "Z1", "NIFTY"), ("C1", "Z1", "CRUDEOIL")}
    assert all(b.started for b in m.books)
    assert m.find("C1", "Z1", "NIFTY") is not None
    assert m.find("C1", "Z1", "CRUDEOIL") is not None


def test_vwap_source_override_propagates_per_binding(monkeypatch):
    """2026-09-03, direct user spec: two bindings under the SAME client,
    SAME underlying, must be able to run genuinely independent vwap_source
    values -- the admin/client-level RuntimeConfig resolution alone can't
    differentiate them (scoped by underlying+client_id, not binding_id), so
    this must come from each deployment's own strategy_params."""
    m = _mgr(monkeypatch, {"C1": [
        _dep("Z1", strategy_params='{"vwap_source": "calculative"}'),
        _dep("Z2", strategy_params='{}'),
    ]})
    m._reconcile()
    assert m.find("C1", "Z1", "NIFTY")._vwap_source_override == "calculative"
    assert m.find("C1", "Z2", "NIFTY")._vwap_source_override is None


def test_vwap_source_override_invalid_value_ignored(monkeypatch):
    m = _mgr(monkeypatch, {"C1": [_dep("Z1", strategy_params='{"vwap_source": "nonsense"}')]})
    m._reconcile()
    assert m.find("C1", "Z1", "NIFTY")._vwap_source_override is None


def test_vwap_source_override_change_triggers_respawn(monkeypatch):
    db = _DB({"C1": [_dep("Z1", strategy_params='{}')]})
    monkeypatch.setattr(bm_mod, "SellStraddleStrategy", _FakeBook)
    m = StraddleBookManager(None, None, db, ["NIFTY"])
    m._reconcile()
    original = m.find("C1", "Z1", "NIFTY")
    assert original._vwap_source_override is None

    db._deps["C1"][0]["strategy_params"] = '{"vwap_source": "calculative"}'
    # Respawn is staged over two reconcile ticks -- the old book is stopped
    # and dropped on the first (guaranteeing old and new are never both
    # alive at once), the replacement spawns on the next.
    m._reconcile()
    assert original.stopped is True
    assert m.find("C1", "Z1", "NIFTY") is None
    m._reconcile()
    respawned = m.find("C1", "Z1", "NIFTY")
    assert respawned is not None
    assert respawned._vwap_source_override == "calculative"
    assert respawned is not original
