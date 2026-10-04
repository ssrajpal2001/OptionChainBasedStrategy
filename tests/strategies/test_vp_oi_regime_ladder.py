"""Regression tests for strategies/sell_straddle/exits.py's new
_check_vp_oi_regime ladder step (2026-10-04, opt-in via vp_oi_enabled).

Drives the REAL SellStraddleStrategy class via _check_exits(), same pattern
as test_itm_roll_protection_priority.py -- never reimplements the engine's
own state machine (this codebase's own feedback_backtest_drive_real_class
lesson).
"""
import asyncio
import datetime
from unittest.mock import patch

from data_layer.base_feeder import EventBus
from config.global_config import GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg
from strategies.vp_oi_regime.decision_matrix import DecisionResult, HedgeSpend
from strategies.vp_oi_regime.live_adapter import VpOiRegimeAdapter


def _base_strategy():
    bus = EventBus()
    s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
    s._lot_size = 75
    s._lot_multiplier = 1
    s._spot = 24000.0
    s._futures_spot = 24000.0
    s._force_exit = datetime.time(23, 59)
    s._itm_pair_gate_enabled = False
    s._itm_roll_protection = {}
    s._ltp_decay_enabled = False
    s._tsl_enabled = False
    s._vwap_rise_enabled = False
    s._exit_rules = []
    s._day_profit_target_pct = 0.0
    s._day_loss_sl_pct = 0.0
    s._ratio_threshold = 999.0  # disabled (0.0 would mean "always true", not "off")
    s._day_low_exit_enabled = False
    s._post1500_exit_enabled = False
    s._persist = lambda: None
    s._ind_by_tf = lambda *a, **k: {}
    return s


def test_vp_oi_disabled_is_a_byte_for_byte_noop():
    """vp_oi_enabled=False (the default): the new step must not even be
    reachable -- no adapter, no state, no behavior change."""
    async def run():
        s = _base_strategy()
        s._vp_oi_enabled = False
        s._vp_oi_adapter = None
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
            ce_leg=StraddleLeg("CE", 24000, 50.0, 50.0),
            pe_leg=StraddleLeg("PE", 24000, 50.0, 50.0),
            net_credit=100.0, status="open",
        )
        s._strike_prem = {(24000, "CE"): {"ltp": 50.0}, (24000, "PE"): {"ltp": 50.0}}
        with patch("strategies.sell_straddle.rolling.RuntimeConfig.index_section", return_value={}):
            await s._check_exits()
        assert s._position is not None and s._position.status == "open"
        assert s._position.ce_leg_closed is False and s._position.pe_leg_closed is False
        assert not hasattr(s, "_vp_oi_naked_leg") or s._vp_oi_naked_leg == {}
    asyncio.run(run())


def test_vp_oi_highly_bearish_exits_put_leg_and_buys_hedge():
    """Highly Bearish regime -> exits the PE leg, CE leg keeps running,
    hedge is bought per the matrix's premium-match target (70% of the
    exited leg's own entry premium, per the current unified Obs5/23 rule)."""
    async def run():
        s = _base_strategy()
        s._vp_oi_enabled = True
        adapter = VpOiRegimeAdapter()
        dr = DecisionResult(
            regime="Highly Bearish (Short Buildup)",
            call_action="Shift put to OTM", put_action="Exit the Put leg",
            call_hedge=HedgeSpend(enabled=False, pct_of_premium=None),
            put_hedge=HedgeSpend(enabled=True, pct_of_premium=(0.70, 0.70)),
            algo_sr_trigger="", reentry_rule="", hedge_exit_rule="",
        )
        adapter.evaluate = lambda spot, now_ts: dr
        s._vp_oi_adapter = adapter

        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
            ce_leg=StraddleLeg("CE", 24050, 50.0, 50.0),
            pe_leg=StraddleLeg("PE", 23950, 100.0, 100.0),  # entry_price=100 -> hedge target=70
            net_credit=150.0, status="open",
        )
        s._strike_prem = {
            (24050, "CE"): {"ltp": 50.0},
            (23950, "PE"): {"ltp": 100.0},
            (23850, "PE"): {"ltp": 70.0},  # lands exactly inside the [70,70] target band
        }

        close_leg_calls = []

        async def _fake_close_leg(side, reason, now):
            close_leg_calls.append((side, reason))
            s._position.pe_leg.close_time = now
            class _Ev:
                close_aborted = False
                realized_pnl = 0.0
            return _Ev()
        s._close_leg = _fake_close_leg

        hedge_calls = []

        async def _fake_dispatch_hedge_order(action, side, strike, price, entry_price, expiry, reason, entry_ts=None):
            hedge_calls.append((action, side, strike, reason))
            class _Fill:
                entry_aborted = False
                exit_aborted = False
                fill_price = price
            return _Fill()
        s._dispatch_hedge_order = _fake_dispatch_hedge_order

        with patch("strategies.sell_straddle.rolling.RuntimeConfig.index_section", return_value={}):
            await s._check_exits()

        assert ("PE", "vp_oi_regime_exit") in close_leg_calls
        assert s._position.pe_leg_closed is True
        assert s._position.status == "open"  # CE keeps running, position not fully closed
        assert ("BUY", "PE", 23850, "vp_oi_hedge") in hedge_calls
        assert s._vp_oi_naked_leg == {"PE": {"strike": 23850, "entry": 70.0}}
        assert adapter.naked_state == "NAKED_PE"
    asyncio.run(run())


def test_vp_oi_naked_leg_stop_hit_then_reentry_resells():
    """Once a naked long's 9-EMA stop fires, the Re-entry Rule arms; once
    price closes back on the fakeout side of POC, a fresh short is resold
    via _open_leg (not _single_side_roll, which assumes both legs open)."""
    async def run():
        s = _base_strategy()
        s._vp_oi_enabled = True
        adapter = VpOiRegimeAdapter()
        adapter.mark_naked("NAKED_PE", {"PE": 70.0})
        adapter.last_poc = 24000.0
        # Warm the EMA then feed a price that trips the PE stop (price RISES
        # back above the EMA -- a long put loses on a rise).
        for p in (70.0, 68.0, 66.0, 64.0, 62.0, 60.0, 58.0, 56.0, 54.0):
            adapter._ema["PE"].update(p)
        s._vp_oi_adapter = adapter
        s._vp_oi_naked_leg = {"PE": {"strike": 23850, "entry": 70.0}}
        s._vp_oi_last_exited_strike = {"PE": 23950}

        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
            ce_leg=StraddleLeg("CE", 24050, 50.0, 50.0),
            pe_leg=StraddleLeg("PE", 23950, 100.0, 100.0),
            net_credit=150.0, status="open",
        )
        s._position.pe_leg_closed = True
        s._spot = 24050.0  # above POC -> PE re-entry condition (close > POC, per Obs5's own text)
        s._strike_prem = {
            (24050, "CE"): {"ltp": 50.0},
            (23850, "PE"): {"ltp": 65.0},  # the naked long's own live LTP (above EMA -> stop)
        }

        hedge_calls = []

        async def _fake_dispatch_hedge_order(action, side, strike, price, entry_price, expiry, reason, entry_ts=None):
            hedge_calls.append((action, side, strike, reason))
            class _Fill:
                entry_aborted = False
                exit_aborted = False
                fill_price = price
            return _Fill()
        s._dispatch_hedge_order = _fake_dispatch_hedge_order

        picked = (23900, 95.0)
        with patch("strategies.sell_straddle.rolling.RuntimeConfig.index_section", return_value={}), \
             patch("strategies.sell_straddle.selection.select_rollover_partner_directional", return_value=picked):
            await s._check_exits()

        assert ("SELL", "PE", 23850, "vp_oi_hedge_exit") in hedge_calls
        assert s._vp_oi_naked_leg == {}
        assert adapter.awaiting_reentry.get("PE") is True

        # Second tick: re-entry condition is already satisfied (spot below
        # POC) -- fires the re-sell via _open_leg.
        with patch("strategies.sell_straddle.rolling.RuntimeConfig.index_section", return_value={}), \
             patch("strategies.sell_straddle.selection.select_rollover_partner_directional", return_value=picked):
            await s._check_exits()

        assert s._position.pe_leg.strike == 23900
        assert s._position.pe_leg.entry_price == 95.0
        assert s._position.pe_leg_closed is False
        assert adapter.awaiting_reentry.get("PE") is False
    asyncio.run(run())
