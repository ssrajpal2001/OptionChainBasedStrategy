"""tests/data_layer/test_strike_rebalancer_effective_depth.py -- regression for the
2026-08-26 real incident: a SellStraddle admin raised pool_otm_depth/pool_itm_depth
from 5 to 7 (the search radius select_balanced_pair_at uses when hunting for a
partner strike), restarted, and it had ZERO effect -- the settings banner and the
eval-log's own "offset=7" both showed the new value, but the partner-candidate
trace only ever showed strikes within ATM+/-4. Root cause: GlobalConfig.chain_depth
(default 4) is a SEPARATE, hardcoded value that governs how wide a window
StrikeRebalancer actually subscribes on the live WS feed -- strikes beyond
ATM+/-chain_depth never get a live quote, so select_balanced_pair_at's search loop
silently skips them (no quote = candidate invisible), regardless of what its own
search radius was told to be.

StrikeRebalancer._effective_chain_depth() now takes max(chain_depth, that
underlying's own configured pool_otm_depth/pool_itm_depth) so the WS subscription
window actually widens to match whatever the admin panel asks the search to cover.

RuntimeConfig.index_section is monkeypatched directly (rather than going through
set_index_section, which persists to the real data/strategy_config.json on disk)
so these tests can never mutate real repo/deployment config state.
"""
from data_layer.runtime_config import RuntimeConfig
from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from data_layer.strike_rebalancer import StrikeRebalancer


def _make_rebalancer(chain_depth: int = 4) -> StrikeRebalancer:
    cfg = GlobalConfig()
    cfg.chain_depth = chain_depth
    return StrikeRebalancer(EventBus(), cfg, feeder=None)


def _stub_index_section(monkeypatch, sections: dict):
    def _fake(index: str, strategy: str) -> dict:
        return dict(sections.get(index, {}))
    monkeypatch.setattr(RuntimeConfig, "index_section", staticmethod(_fake))


def test_effective_depth_defaults_to_chain_depth_when_no_admin_override(monkeypatch):
    _stub_index_section(monkeypatch, {})
    rb = _make_rebalancer(chain_depth=4)
    assert rb._effective_chain_depth("NIFTY") == 4


def test_effective_depth_widens_to_admin_configured_otm_depth(monkeypatch):
    _stub_index_section(monkeypatch, {
        "NIFTY": {"pool_otm_depth": 7, "pool_itm_depth": 7},
    })
    rb = _make_rebalancer(chain_depth=4)
    assert rb._effective_chain_depth("NIFTY") == 7


def test_effective_depth_takes_the_max_of_otm_and_itm(monkeypatch):
    _stub_index_section(monkeypatch, {
        "NIFTY": {"pool_otm_depth": 3, "pool_itm_depth": 6},
    })
    rb = _make_rebalancer(chain_depth=4)
    assert rb._effective_chain_depth("NIFTY") == 6


def test_effective_depth_never_narrower_than_global_chain_depth(monkeypatch):
    """An admin-configured depth smaller than the global default must not
    shrink the WS window below chain_depth -- other consumers may still
    rely on that baseline."""
    _stub_index_section(monkeypatch, {
        "NIFTY": {"pool_otm_depth": 2, "pool_itm_depth": 2},
    })
    rb = _make_rebalancer(chain_depth=4)
    assert rb._effective_chain_depth("NIFTY") == 4


def test_effective_depth_is_scoped_per_underlying(monkeypatch):
    """Widening NIFTY's search depth must NOT widen SENSEX's WS footprint --
    the whole point of scoping this per-underlying rather than raising the
    global chain_depth default."""
    _stub_index_section(monkeypatch, {
        "NIFTY": {"pool_otm_depth": 7, "pool_itm_depth": 7},
    })
    rb = _make_rebalancer(chain_depth=4)
    assert rb._effective_chain_depth("NIFTY") == 7
    assert rb._effective_chain_depth("SENSEX") == 4


def test_effective_depth_falls_back_safely_if_runtime_config_errors(monkeypatch):
    def _boom(index, strategy):
        raise RuntimeError("disk read failed")
    monkeypatch.setattr(RuntimeConfig, "index_section", staticmethod(_boom))
    rb = _make_rebalancer(chain_depth=4)
    assert rb._effective_chain_depth("NIFTY") == 4
