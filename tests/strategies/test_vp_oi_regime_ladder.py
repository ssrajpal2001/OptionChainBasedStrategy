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
from strategies.vp_oi_regime.volume_profile import VolumeProfileSnapshot


def _snap_with_lvn_below(val: float, lvn_price: float) -> VolumeProfileSnapshot:
    """A snapshot whose VAL is `val` and which has a real LVN row that
    `lvn_price` falls inside, below VAL -- i.e. in_lvn(lvn_price, "below")
    is True. Used to simulate "real breakout, volume-confirmed" below VAL."""
    row_size = 10.0
    row_low = lvn_price - (lvn_price % row_size)
    return VolumeProfileSnapshot(
        poc=val + 50.0, vah=val + 100.0, val=val,
        lvn_rows=[row_low], hvn_rows=[], total_volume=1000.0,
        row_size=row_size, rows={},
    )


def _snap_inside_value_area(poc: float, vah: float, val: float) -> VolumeProfileSnapshot:
    """A snapshot with no LVN rows at all below VAL/above VAH -- simulates
    "price hasn't shown LVN acceptance yet", regardless of where spot is."""
    return VolumeProfileSnapshot(
        poc=poc, vah=vah, val=val, lvn_rows=[], hvn_rows=[],
        total_volume=1000.0, row_size=10.0, rows={},
    )


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
    """Highly Bearish regime, AND price has shown real LVN volume
    acceptance below VAL (2026-10-09 confirmation gate) -> exits the PE
    leg, CE leg keeps running, hedge is bought per the matrix's
    premium-match target (70% of the exited leg's own entry premium, per
    the current unified Obs5/23 rule)."""
    async def run():
        s = _base_strategy()
        s._spot = 23795.0
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
        adapter.last_snapshot = _snap_with_lvn_below(val=23900.0, lvn_price=23795.0)
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


def test_vp_oi_highly_bearish_shifts_otm_when_not_yet_lvn_confirmed():
    """2026-10-09 direct user spec (the price-location confirmation gate):
    Highly Bearish regime, but price has NOT shown LVN volume acceptance
    below VAL -- must NOT exit the leg at all. Instead shifts the losing PE
    leg 100pts further OTM (the doc's own concrete number) and leaves the
    position open, watching for a later LVN confirmation."""
    async def run():
        s = _base_strategy()
        s._spot = 23950.0  # inside the value area, no breakout shown
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
        adapter.last_snapshot = _snap_inside_value_area(poc=24000.0, vah=24100.0, val=23900.0)
        s._vp_oi_adapter = adapter

        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
            ce_leg=StraddleLeg("CE", 24050, 50.0, 50.0),
            pe_leg=StraddleLeg("PE", 23950, 100.0, 100.0),
            net_credit=150.0, status="open",
        )
        s._strike_prem = {
            (24050, "CE"): {"ltp": 50.0},
            (23950, "PE"): {"ltp": 100.0},
            (23850, "PE"): {"ltp": 130.0},  # the 100pt-further-OTM strike (23950-100)
        }

        close_leg_calls = []

        async def _fake_close_leg(side, reason, now):
            close_leg_calls.append((side, reason))
            class _Ev:
                close_aborted = False
                realized_pnl = 0.0
            return _Ev()
        s._close_leg = _fake_close_leg

        open_leg_calls = []

        async def _fake_open_leg(side, strike, ltp, now, reason):
            open_leg_calls.append((side, strike, ltp, reason))
            leg = s._position.ce_leg if side == "CE" else s._position.pe_leg
            leg.strike = strike
            leg.entry_price = ltp
        s._open_leg = _fake_open_leg

        hedge_calls = []
        s._dispatch_hedge_order = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("no hedge should be dispatched -- the leg was shifted, not exited"))

        with patch("strategies.sell_straddle.rolling.RuntimeConfig.index_section", return_value={}):
            await s._check_exits()

        assert ("PE", "vp_oi_regime_otm_shift") in close_leg_calls
        assert ("PE", 23850, 130.0, "vp_oi_regime_otm_shift") in open_leg_calls
        assert s._position.pe_leg_closed is False  # not exited -- shifted in place
        assert s._position.status == "open"
        assert adapter.shifted_strike.get("PE") == 23850

        # A second cycle at the SAME (still-unconfirmed) state must NOT
        # shift again -- only once per episode.
        close_leg_calls.clear()
        open_leg_calls.clear()
        with patch("strategies.sell_straddle.rolling.RuntimeConfig.index_section", return_value={}):
            await s._check_exits()
        assert close_leg_calls == []
        assert open_leg_calls == []
    asyncio.run(run())


def test_vp_oi_highly_bearish_exits_flat_when_price_reverses_through_poc():
    """2026-10-09 direct user spec: Highly Bearish regime, price never
    showed LVN acceptance below VAL, but instead reversed back UP through
    POC -- the bearish thesis failed. Must exit the PE leg FLAT (no
    replacement hedge -- there's no confirmed breakout left to ride)."""
    async def run():
        s = _base_strategy()
        s._spot = 24010.0  # back above POC (24000) -- thesis reversed
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
        # No LVN rows at all -- never confirmed; POC=24000, spot=24010 is
        # above it -> reversal condition fires for PE.
        adapter.last_snapshot = _snap_inside_value_area(poc=24000.0, vah=24100.0, val=23900.0)
        s._vp_oi_adapter = adapter

        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
            ce_leg=StraddleLeg("CE", 24050, 50.0, 50.0),
            pe_leg=StraddleLeg("PE", 23950, 100.0, 100.0),
            net_credit=150.0, status="open",
        )
        s._strike_prem = {(24050, "CE"): {"ltp": 50.0}, (23950, "PE"): {"ltp": 80.0}}

        close_leg_calls = []

        async def _fake_close_leg(side, reason, now):
            close_leg_calls.append((side, reason))
            class _Ev:
                close_aborted = False
                realized_pnl = 0.0
            return _Ev()
        s._close_leg = _fake_close_leg
        s._open_leg = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("no leg should be opened -- this is a flat exit, not a shift"))
        s._dispatch_hedge_order = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("no hedge should be dispatched on a reversal exit"))

        with patch("strategies.sell_straddle.rolling.RuntimeConfig.index_section", return_value={}):
            await s._check_exits()

        assert ("PE", "vp_oi_regime_exit_reversal") in close_leg_calls
        assert s._position.pe_leg_closed is True
        assert s._position.status == "open"  # CE keeps running
        assert s._vp_oi_naked_leg == {}  # no naked long opened
    asyncio.run(run())


def test_vp_oi_volatile_closes_both_legs_and_still_manages_naked_longs_after():
    """2026-10-05 structural fix: a Volatile regime closes the WHOLE
    position (self._position -> None), but naked-leg management must keep
    running on later ticks anyway (via _check_vp_oi_naked_legs_standalone,
    called unconditionally at the top of _check_exits, before the
    'no position' early-return)."""
    async def run():
        s = _base_strategy()
        s._vp_oi_enabled = True
        adapter = VpOiRegimeAdapter()
        dr = DecisionResult(
            regime="Volatile", call_action="Buy put hedge", put_action="Buy call hedge",
            call_hedge=HedgeSpend(enabled=True, pct_of_premium=(0.50, 0.50)),
            put_hedge=HedgeSpend(enabled=True, pct_of_premium=(0.50, 0.50)),
            algo_sr_trigger="", reentry_rule="", hedge_exit_rule="",
        )
        adapter.evaluate = lambda spot, now_ts: dr
        s._vp_oi_adapter = adapter

        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
            ce_leg=StraddleLeg("CE", 24050, 50.0, 50.0),
            pe_leg=StraddleLeg("PE", 23950, 100.0, 100.0),
            net_credit=150.0, status="open",
        )
        s._entry_expiry_date = s._position.expiry_date
        s._strike_prem = {
            (24150, "CE"): {"ltp": 25.0},  # 50% of 50 = 25
            (23850, "PE"): {"ltp": 50.0},  # 50% of 100 = 50
        }

        closed = {"called": False}

        async def _fake_close_position(reason):
            closed["called"] = True
            closed["reason"] = reason
            s._position = None  # mirrors the real method's own effect once confirmed
        s._close_position = _fake_close_position

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

        assert closed["called"] and closed["reason"] == "vp_oi_regime_exit_volatile"
        assert s._position is None
        assert ("BUY", "CE", 24150, "vp_oi_hedge") in hedge_calls
        assert ("BUY", "PE", 23850, "vp_oi_hedge") in hedge_calls
        assert adapter.naked_state == "NAKED_BOTH"
        assert s._vp_oi_naked_leg == {"CE": {"strike": 24150, "entry": 25.0},
                                       "PE": {"strike": 23850, "entry": 50.0}}

        # Next tick: self._position is still None, but the 9-EMA stop on the
        # CE naked long must still be checked -- this is the actual proof of
        # the structural fix (before it, _check_exits returned immediately
        # with no position and never reached any VP/OI code at all).
        # Warm the EMA on an UPTREND (the naked long CALL riding price up),
        # then a sharp fall back through the EMA is what actually exits a
        # long-call trailing stop (direction='up': stop_hit when price < EMA).
        for p in (18.0, 19.0, 20.0, 22.0, 24.0, 26.0, 28.0, 30.0, 32.0):
            adapter.on_naked_leg_price("CE", p)
        s._strike_prem[(24150, "CE")] = {"ltp": 15.0}  # fell well below the EMA -> long CE stop

        with patch("strategies.sell_straddle.rolling.RuntimeConfig.index_section", return_value={}):
            await s._check_exits()

        assert ("SELL", "CE", 24150, "vp_oi_hedge_exit") in hedge_calls
        assert "CE" not in s._vp_oi_naked_leg
        assert adapter.awaiting_reentry.get("CE") is True
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
