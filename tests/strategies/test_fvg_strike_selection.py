"""
tests/strategies/test_fvg_strike_selection.py — FVGStrategy's default ITM
offset must land on a strike that actually exists for the underlying.

2026-08-06 real-data bug: strategies/fvg/engine.py hardcoded
_DEFAULT_ITM_OFFSET_PTS=50 as the "1-strike ITM" offset for EVERY
underlying, regardless of that underlying's real strike grid. NIFTY's
strikes are on a 50-point grid, so atm-50/atm+50 lands on a real strike.
SENSEX's strikes are on a 100-point grid (self._strike_step, computed but
never applied to the strike math) -- atm-50/atm+50 lands on a strike that
was never listed at all. Confirmed live: scripts/fvg_today_check.py found
a real SENSEX FVG retest entry today whose computed strike (78750CE) had
no Upstox instrument key -- REGISTRY.get_upstox_key() correctly returned
"" because 78750 simply isn't a real SENSEX contract (only multiples of
100 are). book_manager.py's strategy_params default also force-fills
itm_offset_pts=50 for every deployment unless explicitly overridden, so
this wasn't just a constructor-default edge case -- it's the real
deployment path.

Fix: itm_offset_pts now defaults (when not explicitly configured) to the
underlying's own self._strike_step, so "1-strike ITM" actually means one
real strike on that underlying's real grid. An explicit override (a user
deliberately setting strategy_params.itm_offset_pts) is still respected
exactly as configured.
"""
from __future__ import annotations

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.fvg.engine import FVGStrategy


def _make(underlying: str, **kwargs) -> FVGStrategy:
    cfg = GlobalConfig()
    return FVGStrategy(
        EventBus(), cfg, underlying=underlying, client_id="C", binding_id="B",
        lot_multiplier=1, feeder_token="", **kwargs,
    )


def test_sensex_default_itm_offset_matches_its_own_strike_step():
    strat = _make("SENSEX")
    assert strat._strike_step == 100
    assert strat._itm_offset_pts == 100


def test_banknifty_default_itm_offset_matches_its_own_strike_step():
    strat = _make("BANKNIFTY")
    assert strat._strike_step == 100
    assert strat._itm_offset_pts == 100


def test_nifty_default_itm_offset_unchanged_at_50():
    strat = _make("NIFTY")
    assert strat._strike_step == 50
    assert strat._itm_offset_pts == 50


def test_explicit_itm_offset_override_is_still_respected():
    strat = _make("SENSEX", itm_offset_pts=75)
    assert strat._itm_offset_pts == 75


def test_sensex_computed_strikes_land_on_a_real_100pt_grid_strike():
    strat = _make("SENSEX")
    atm = round(78772.89 / 100) * 100  # matches _ATM_ROUND_STEP=100 in _open_position
    ce_strike = int(atm - strat._itm_offset_pts)
    pe_strike = int(atm + strat._itm_offset_pts)
    assert ce_strike % strat._strike_step == 0
    assert pe_strike % strat._strike_step == 0
