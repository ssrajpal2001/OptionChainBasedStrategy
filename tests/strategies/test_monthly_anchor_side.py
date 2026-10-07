"""Regression tests for the 2026-10-06 direct user spec: BEGINNING entry's
anchor SIDE is now decided from a one-time REST snapshot of the MONTHLY
contract's own theta (not the weekly spot+futures-mean ATM reading used
everywhere else). See entries.py's _resolve_monthly_anchor_side and
selection.py's select_balanced_pair_at(forced_anchor_side=...) for the real
implementation.
"""
import asyncio
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy

_RULES = [{
    "indicator": "advanced", "operand1": "slope", "operand2": "value",
    "operand2_val": 0.0, "operator_sym": "<", "tf": 1,
}]


def _strategy(spot=22600.0):
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    s._spot = spot
    s._entry_expiry_date = date(2026, 10, 6)
    s._ind_by_tf = lambda ce, pe, rules: {1: {"slope": -0.5}}
    return s


def _quote_side_effect(fut_px, ce_ltp, pe_ltp):
    def _fn(key, token):
        if key == "FUT":
            return {"last_price": fut_px}
        return {"last_price": ce_ltp} if key.startswith("CE") else {"last_price": pe_ltp}
    return _fn


def test_resolve_monthly_anchor_side_picks_lower_theta_and_passes_floor():
    async def run():
        s = _strategy()
        with patch("data_layer.instrument_registry.REGISTRY.get_futures_upstox",
                   return_value="FUT"), \
             patch("data_layer.instrument_registry.REGISTRY.get_monthly_expiry",
                   return_value=date(2026, 10, 27)), \
             patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
                   side_effect=lambda und, exp, strike, side, broker: f"{side}{strike}"), \
             patch("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                   return_value={"access_token": "tok"}), \
             patch("data_layer.historical_candles.fetch_upstox_v3_quote",
                   new=AsyncMock(side_effect=_quote_side_effect(22650.0, 80.0, 140.0))):
            result = await s._resolve_monthly_anchor_side(step=50.0, ltp_target=50.0, theta_target=20.0)
        # monthly ATM = round(22650/50)*50 = 22650, so both legs are pure
        # time value: CE=80 < PE=140 -> CE wins.
        assert result == ("CE", 22650)
    asyncio.run(run())


def test_resolve_monthly_anchor_side_returns_none_on_floor_failure():
    async def run():
        s = _strategy()
        with patch("data_layer.instrument_registry.REGISTRY.get_futures_upstox",
                   return_value="FUT"), \
             patch("data_layer.instrument_registry.REGISTRY.get_monthly_expiry",
                   return_value=date(2026, 10, 27)), \
             patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
                   side_effect=lambda und, exp, strike, side, broker: f"{side}{strike}"), \
             patch("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                   return_value={"access_token": "tok"}), \
             patch("data_layer.historical_candles.fetch_upstox_v3_quote",
                   new=AsyncMock(side_effect=_quote_side_effect(22650.0, 10.0, 15.0))):
            # Both legs well under ltp_target=50 -> CE (lower theta) fails the floor.
            result = await s._resolve_monthly_anchor_side(step=50.0, ltp_target=50.0, theta_target=20.0)
        assert result is None
    asyncio.run(run())


def test_resolve_monthly_anchor_side_returns_none_with_no_futures_key():
    async def run():
        s = _strategy()
        with patch("data_layer.instrument_registry.REGISTRY.get_futures_upstox", return_value=""), \
             patch("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                   return_value={"access_token": "tok"}):
            side = await s._resolve_monthly_anchor_side(step=50.0, ltp_target=50.0, theta_target=20.0)
        assert side is None
    asyncio.run(run())


def test_resolve_monthly_anchor_side_returns_none_on_rest_failure():
    async def run():
        s = _strategy()
        with patch("data_layer.instrument_registry.REGISTRY.get_futures_upstox",
                   return_value="FUT"), \
             patch("data_layer.client_db.ClientDB.get_feeder_creds_sync",
                   return_value={"access_token": "tok"}), \
             patch("data_layer.historical_candles.fetch_upstox_v3_quote",
                   new=AsyncMock(side_effect=Exception("network error"))):
            side = await s._resolve_monthly_anchor_side(step=50.0, ltp_target=50.0, theta_target=20.0)
        assert side is None
    asyncio.run(run())


def test_beginning_entry_passes_forced_side_and_monthly_atm():
    """When the monthly resolver returns (side, monthly_atm), _eval_beginning_
    near_far must (a) anchor the shift at the MONTHLY atm (2026-10-07
    correction -- NOT spot-rounded weekly atm, which was shifting 1-OTM
    from the wrong strike), (b) pass real spot as the stripping reference,
    and (c) pass forced_anchor_side through to select_balanced_pair_at."""
    async def run():
        s = _strategy(spot=22611.0)
        s._resolve_monthly_anchor_side = AsyncMock(return_value=("PE", 22700))
        captured = {}

        def _sel(strike_prem, atm, spot, step, offset, ltp_target, **kwargs):
            captured["atm"] = atm
            captured["spot"] = spot
            captured["forced_anchor_side"] = kwargs.get("forced_anchor_side")
            return None

        with patch("strategies.sell_straddle.selection.select_balanced_pair_at", side_effect=_sel):
            await s._eval_beginning_near_far(
                datetime.now(IST), "entry_rules_beginning", _RULES,
                step=50, offset=5, ltp_target=50.0, theta_target=20.0,
                variable_strikes=False, balance_ratio=1.0,
            )
        # atm = the MONTHLY atm itself (22700), so select_balanced_pair_at's
        # own anchor_otm_steps=1 shift lands on PE@22650 -- not PE@22600,
        # which is what a weekly-spot-rounded atm of 22600 would have shifted
        # to. spot stays real spot (22611) for intrinsic/time-value stripping.
        assert captured["atm"] == 22700
        assert captured["spot"] == 22611.0
        assert captured["forced_anchor_side"] == "PE"
    asyncio.run(run())


def test_beginning_entry_falls_back_to_mean_atm_when_monthly_unresolved():
    """When the monthly resolver returns None (any failure), BEGINNING must
    fall back to the original spot+futures-mean ATM with no forced side --
    byte-for-byte the pre-existing behavior."""
    async def run():
        s = _strategy(spot=22611.0)
        s._atm_ref = (22611.0 + 22650.0) / 2.0
        s._resolve_monthly_anchor_side = AsyncMock(return_value=None)
        captured = {}

        def _sel(strike_prem, atm, spot, step, offset, ltp_target, **kwargs):
            captured["atm"] = atm
            captured["spot"] = spot
            captured["forced_anchor_side"] = kwargs.get("forced_anchor_side")
            return None

        with patch("strategies.sell_straddle.selection.select_balanced_pair_at", side_effect=_sel):
            await s._eval_beginning_near_far(
                datetime.now(IST), "entry_rules_beginning", _RULES,
                step=50, offset=5, ltp_target=50.0, theta_target=20.0,
                variable_strikes=False, balance_ratio=1.0,
            )
        mean = (22611.0 + 22650.0) / 2.0
        assert captured["atm"] == round(mean / 50) * 50
        assert captured["forced_anchor_side"] is None
    asyncio.run(run())
