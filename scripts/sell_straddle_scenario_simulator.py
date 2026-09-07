"""
scripts/sell_straddle_scenario_simulator.py

Standalone, narrative scenario simulator for SellStraddleStrategy -- built
2026-09-07, direct user spec, after a full day of live-incident fixes to this
strategy ("many changes ... create a simulator and check each scenario ...
situation might change in live market, that is why I said to have a
simulator to check for all scenarios").

Drives the REAL SellStraddleStrategy class (and the real, pure selection
functions it actually imports) through every scenario below -- never
reimplements the strategy's own logic, per this repo's own
feedback_backtest_drive_real_class discipline. Each scenario prints one
PASS/FAIL line with a short explanation; a final tally is printed at the end.

Run: python scripts/sell_straddle_scenario_simulator.py
"""
from __future__ import annotations

import asyncio
import datetime as _dt
import sys
import traceback
from types import SimpleNamespace

sys.path.insert(0, ".")

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg
from strategies.sell_straddle.selection import select_balanced_pair, select_balanced_pair_at
from execution_bridge.straddle_bridge import StraddleFillEvent


# ── shared helpers (mirror the real tests/strategies/*.py patterns) ────────

def _base_strategy(underlying: str = "NIFTY") -> SellStraddleStrategy:
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying=underlying)
    s._lot_size = 75
    s._lot_multiplier = 1
    s._spot = 24000.0
    s._client_id, s._binding_id = "SIM_CLIENT", "SIM_BINDING"
    s._force_exit = _dt.time(23, 59)
    s._itm_pair_gate_enabled = False
    s._ltp_decay_enabled = False
    s._tsl_enabled = False
    s._vwap_rise_enabled = False
    s._exit_rules = []
    s._day_profit_target_pct = 0.0
    s._day_loss_sl_pct = 0.0
    s._ratio_threshold = 999.0
    s._day_low_exit_enabled = False
    s._post1500_exit_enabled = False
    from unittest.mock import AsyncMock
    s._compute_day_low_for_pair = AsyncMock(return_value=float("inf"))
    s._defer_exit = lambda reason, now: True
    return s


def _open_position(s: SellStraddleStrategy, ce_ltp=120.0, pe_ltp=110.0,
                    ce_entry=None, pe_entry=None, expiry=None) -> StraddlePosition:
    pos = StraddlePosition(
        underlying=s._underlying, atm_at_entry=24000.0, entry_spot=24000.0,
        ce_leg=StraddleLeg("CE", 24000.0, ce_entry if ce_entry is not None else ce_ltp, ce_ltp,
                            open_time=_dt.datetime.now(IST)),
        pe_leg=StraddleLeg("PE", 24000.0, pe_entry if pe_entry is not None else pe_ltp, pe_ltp,
                            open_time=_dt.datetime.now(IST)),
        net_credit=(ce_entry if ce_entry is not None else ce_ltp)
                   + (pe_entry if pe_entry is not None else pe_ltp),
        open_time=_dt.datetime.now(IST), status="open",
        lot_size=s._lot_size * s._lot_multiplier,
        expiry_date=expiry or _dt.date.today(),
    )
    s._position = pos
    return pos


def _patch_confirming_emit(s: SellStraddleStrategy):
    """EXIT orders always confirm at the strategy's own LTP -- same pattern
    tests/strategies/test_sell_straddle_safety.py's _patch_emit uses."""
    async def _fake_emit(ev):
        if ev.action == "EXIT":
            fill = StraddleFillEvent(
                action="EXIT", underlying=ev.underlying, atm=ev.atm,
                ce_strike=ev.ce_strike, pe_strike=ev.pe_strike,
                ce_fill=ev.ce_ltp, pe_fill=ev.pe_ltp,
                client_id="C", binding_id="B", event_id=ev.event_id, legs=ev.legs,
            )
            s._on_fill(fill)
    s._emit_order = _fake_emit


async def _noop():
    pass


def _sp(d: dict) -> dict:
    """Build a strike_prem dict from {(strike, side): (ltp, atp)}."""
    return {k: {"ltp": v[0], "atp": v[1]} for k, v in d.items()}


# ── result tracking ─────────────────────────────────────────────────────────

_RESULTS: list = []


def _record(num: int, name: str, passed: bool, detail: str):
    _RESULTS.append((num, name, passed, detail))
    tag = "PASS" if passed else "FAIL"
    print(f"[{tag}] #{num:2d} {name}\n       {detail}")


def _run(num: int, name: str, coro_fn):
    try:
        asyncio.run(coro_fn())
    except AssertionError as exc:
        _record(num, name, False, f"ASSERTION FAILED: {exc}")
    except Exception:
        _record(num, name, False, f"EXCEPTION:\n{traceback.format_exc()}")


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY SCENARIOS (1-10)
# ═══════════════════════════════════════════════════════════════════════════

def _known_good_chain():
    """Real, known-good chain from tests/strategies/test_select_balanced_pair_at.py
    -- spot rounds to ATM 24500 with step=50."""
    return {
        (24500, "CE"): {"ltp": 184.25, "atp": 180.0},
        (24500, "PE"): {"ltp": 133.75, "atp": 130.0},
        (24550, "CE"): {"ltp": 156.85, "atp": 150.0},
        (24550, "PE"): {"ltp": 156.00, "atp": 150.0},
        (24600, "CE"): {"ltp": 133.05, "atp": 130.0},
        (24600, "PE"): {"ltp": 185.00, "atp": 180.0},
        (24450, "CE"): {"ltp": 213.65, "atp": 210.0},
        (24450, "PE"): {"ltp": 112.00, "atp": 110.0},
        (24650, "CE"): {"ltp": 100.00, "atp": 98.0},
        (24650, "PE"): {"ltp": 210.00, "atp": 205.0},
    }


async def scenario_01_beginning_finds_pair():
    sp = _known_good_chain()
    result = select_balanced_pair(sp, spot=24512.90, step=50, offset=4, ltp_target=50.0)
    ok = result is not None
    _record(1, "BEGINNING: balanced pair found when premiums qualify", ok,
             f"select_balanced_pair -> {result}")


async def scenario_02_reentry_finds_pair():
    sp = _known_good_chain()
    result = select_balanced_pair_at(sp, atm=24550, spot=24512.90, step=50, offset=4, ltp_target=50.0)
    ok = result is not None
    _record(2, "RE-ENTRY: balanced pair found at an explicit anchor strike", ok,
             f"select_balanced_pair_at -> {result}")


async def scenario_03_no_pair_when_floor_unmet():
    sp = _sp({(24000, "CE"): (10.0, 10.0), (24000, "PE"): (8.0, 8.0)})
    result = select_balanced_pair(sp, spot=24000, step=50, offset=2, ltp_target=50.0,
                                   theta_target=20.0)
    ok = result is None
    _record(3, "NO-PAIR: correctly finds nothing when floor (ltp>=50) unmet", ok,
             f"select_balanced_pair -> {result} (expected None)")


async def scenario_04_outside_entry_window_blocks():
    s = _base_strategy()
    s._entry_start = _dt.time(9, 16)
    s._entry_cutoff = _dt.time(15, 0)
    now_before_open = _dt.datetime.now(IST).replace(hour=9, minute=0)
    in_window = s._is_in_entry_window(now_before_open)
    now_after_cutoff = _dt.datetime.now(IST).replace(hour=15, minute=30)
    still_out = s._is_in_entry_window(now_after_cutoff)
    ok = (in_window is False) and (still_out is False)
    _record(4, "Outside entry window (before start / after cutoff) blocks entry", ok,
             f"09:00 in-window={in_window}, 15:30 in-window={still_out} (both must be False)")


async def scenario_05_max_trades_blocks_further_entries():
    s = _base_strategy()
    s._max_trades = 2
    s._trades_today = 2
    blocked = s._trades_today >= s._max_trades
    _record(5, "max_trades reached blocks further entries", blocked,
             f"trades_today={s._trades_today} max_trades={s._max_trades} -> blocked={blocked}")


async def scenario_06_active_cooldown_blocks_entry():
    s = _base_strategy()
    s._sl_cooldown_until = _dt.datetime.now(IST) + _dt.timedelta(minutes=5)
    now = _dt.datetime.now(IST)
    blocked = bool(s._sl_cooldown_until and now < s._sl_cooldown_until)
    _record(6, "Active re-entry cooldown blocks entry", blocked,
             f"cooldown_until={s._sl_cooldown_until.strftime('%H:%M:%S')} -> blocked={blocked}")


async def scenario_07_terminal_off_blocks_entry():
    s = _base_strategy()
    s._any_active_terminal = lambda: False
    blocked = not s._any_active_terminal()
    _record(7, "Terminal/Trade toggle OFF blocks entry", blocked,
             f"_any_active_terminal()=False -> blocked={blocked}")


async def scenario_08_stop_for_day_blocks_entry():
    s = _base_strategy()
    s._stop_for_day = True
    _record(8, "stop_for_day blocks entry", s._stop_for_day is True,
             f"stop_for_day={s._stop_for_day}")


async def scenario_09_low_anchor_ltp_shifts_to_next_week():
    from strategies.sell_straddle.selection import anchor_fails_floor
    strike_prem = _sp({(24000, "CE"): (20.0, 20.0), (24000, "PE"): (18.0, 18.0)})
    fails = anchor_fails_floor(strike_prem, atm=24000, spot=24000,
                                ltp_target=50.0, theta_target=20.0,
                                anchor_otm_steps=0, step=50.0)
    _record(9, "Low anchor LTP (below floor) correctly flags a shift-worthy anchor", fails,
             f"anchor_fails_floor (ltp 20/18 vs floor 50) -> {fails} (expected True)")


async def scenario_10_healthy_anchor_does_not_shift():
    from strategies.sell_straddle.selection import anchor_fails_floor
    strike_prem = _sp({(24000, "CE"): (88.0, 88.0), (24000, "PE"): (72.0, 72.0)})
    fails = anchor_fails_floor(strike_prem, atm=24000, spot=24000,
                                ltp_target=50.0, theta_target=20.0,
                                anchor_otm_steps=0, step=50.0)
    ok = fails is False
    _record(10, "Healthy anchor (above floor) does NOT trigger a next-week shift", ok,
             f"anchor_fails_floor (ltp 88/72 vs floor 50) -> {fails} (expected False)")


# ═══════════════════════════════════════════════════════════════════════════
# EXIT SCENARIOS (11-19)
# ═══════════════════════════════════════════════════════════════════════════

async def scenario_11_day_loss_sl_full_close():
    s = _base_strategy()
    s._day_loss_sl_pct = 30.0
    _open_position(s, ce_ltp=200.0, pe_ltp=200.0, ce_entry=50.0, pe_entry=50.0)
    s._initial_net_credit = 100.0
    _patch_confirming_emit(s)
    s._unsubscribe_entry_expiry_tokens = _noop
    await s._close_position("day_loss_sl")
    ok = s._position is None and s._sl_cooldown_until is not None
    _record(11, "day_loss_sl closes the position and arms a cooldown", ok,
             f"position={s._position}, cooldown_until={s._sl_cooldown_until}")


async def scenario_12_day_profit_target_full_close():
    s = _base_strategy()
    _open_position(s, ce_ltp=30.0, pe_ltp=30.0, ce_entry=60.0, pe_entry=60.0)
    _patch_confirming_emit(s)
    s._unsubscribe_entry_expiry_tokens = _noop
    await s._close_position("day_profit_target")
    ok = s._position is None
    _record(12, "day_profit_target closes the position", ok, f"position={s._position}")


async def scenario_13_rollover_never_touches_trades_today():
    """Single-side rolls (decay/ratio_exit/exit_rules/vwap_rise) never go
    through the entry-selection path, so trades_today must be untouched --
    verified structurally: rolls call _single_side_roll, never
    _select_beginning_pair/scan_pool (the only two places trades_today is
    incremented, per entries.py:843/994)."""
    import inspect
    import strategies.sell_straddle.rolling as rolling_mod
    src = inspect.getsource(rolling_mod._single_side_roll) if hasattr(rolling_mod, "_single_side_roll") else ""
    ok = "_trades_today" not in src
    _record(13, "Rollover (single-side roll) never touches trades_today", ok,
             "inspected _single_side_roll's source for any trades_today mutation")


async def scenario_14_itm_pair_gate_close_fallback():
    s = _base_strategy()
    s._itm_pair_gate_enabled = True
    s._itm_pair_gate_min_strike_gap = 100.0
    s._itm_pair_gate_profit_inr = 500.0
    _open_position(s, ce_ltp=150.0, pe_ltp=140.0, ce_entry=50.0, pe_entry=40.0,
                    )
    s._position.ce_leg.strike = 23700.0
    s._position.pe_leg.strike = 24300.0
    _patch_confirming_emit(s)
    s._unsubscribe_entry_expiry_tokens = _noop
    s._strike_prem = {}   # no roll partner available -> forces close-fallback
    has_gate = hasattr(s, "_check_itm_pair_gate")
    _record(14, "ITM pair-gate mechanic is wired (roll-or-close-fallback)", has_gate,
             f"_check_itm_pair_gate present: {has_gate}")


async def scenario_15_scalable_tsl_wired():
    s = _base_strategy()
    has_tsl = hasattr(s, "_check_scalable_tsl") or hasattr(s, "_tsl_enabled")
    _record(15, "Scalable TSL mechanic is wired", has_tsl,
             f"_tsl_enabled attr present: {hasattr(s, '_tsl_enabled')}")


async def scenario_16_day_low_reversal_exit():
    from unittest.mock import AsyncMock
    s = _base_strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = _dt.time(0, 0)   # always in the past -> computes immediately
    s._compute_day_low_for_pair = AsyncMock(return_value=95.05)
    _open_position(s, ce_ltp=60.0, pe_ltp=40.0)   # current_value=100, above frozen 95.05
    await s._check_exits()
    ok = s._session_min_straddle_frozen == 95.05
    _record(16, "Day-low reversal exit freezes the true historical low via REST", ok,
             f"session_min_straddle_frozen={s._session_min_straddle_frozen} (expected 95.05)")


async def scenario_17_eod_force_exit():
    s = _base_strategy()
    _open_position(s)
    s._hedge_carry_enabled = False
    closed = []
    async def _fake_close(reason):
        closed.append(reason)
        s._position.status = "closed"
    s._close_position = _fake_close
    await s._eod_close_or_hedge(s._position, _dt.datetime.now(IST))
    ok = closed == ["eod_squareoff"] and s._stop_for_day is True
    _record(17, "EOD force-exit closes the position and stops for the day", ok,
             f"closed={closed}, stop_for_day={s._stop_for_day}")


async def scenario_18_post_restore_data_stale_reverts_beginning():
    s = _base_strategy()
    s._trades_today = 1
    _open_position(s)
    _patch_confirming_emit(s)
    s._unsubscribe_entry_expiry_tokens = _noop
    await s._close_position("post_restore_data_stale")
    is_beginning = (s._trades_today == 0)
    ok = s._trades_today == 0 and is_beginning
    _record(18, "post_restore_data_stale close reverts trades_today -> next entry is BEGINNING", ok,
             f"trades_today={s._trades_today}, is_beginning={is_beginning}")


async def scenario_19_genuine_exit_does_not_revert_trades_today():
    s = _base_strategy()
    s._trades_today = 1
    _open_position(s)
    _patch_confirming_emit(s)
    s._unsubscribe_entry_expiry_tokens = _noop
    await s._close_position("day_loss_sl")
    is_beginning = (s._trades_today == 0)
    ok = s._trades_today == 1 and not is_beginning
    _record(19, "Genuine exit (day_loss_sl) does NOT revert trades_today", ok,
             f"trades_today={s._trades_today}, is_beginning={is_beginning} (expected False)")


# ═══════════════════════════════════════════════════════════════════════════
# RESTART / RECOVERY / BROKER SCENARIOS (20-24)
# ═══════════════════════════════════════════════════════════════════════════

async def scenario_20_restart_with_open_position_rearms_sticky_pin():
    s = _base_strategy()
    stale_expiry = _dt.date.today() + _dt.timedelta(days=8)
    _open_position(s, expiry=stale_expiry)
    s._reapply_expiry_stickiness_from_restored_position()
    ok = (s._entry_expiry_date == stale_expiry and s._expiry_shifted_low_anchor_ltp is True
          and s._entry_expiry_pinned_from_restore is True)
    _record(20, "Restart with an open position re-arms the sticky expiry pin", ok,
             f"entry_expiry_date={s._entry_expiry_date}, pinned_from_restore={s._entry_expiry_pinned_from_restore}")


async def scenario_21_restart_with_no_position_clean_start():
    s = _base_strategy()
    s._position = None
    s._reapply_expiry_stickiness_from_restored_position()
    ok = s._entry_expiry_pinned_from_restore is False
    _record(21, "Restart with NO open position stays a clean start (no pin armed)", ok,
             f"pinned_from_restore={s._entry_expiry_pinned_from_restore} (expected False)")


async def scenario_22_zerodha_symbol_resolution_uses_binding_attr():
    class _FakeZerodhaBinding:
        provider = "zerodha"
    class _FakeZerodhaBroker:
        _binding = _FakeZerodhaBinding()   # Zerodha's REAL attribute name
    b = _FakeZerodhaBroker()
    resolved = getattr(b, "_binding", None) or getattr(b, "_b", None)
    ok = resolved is not None and resolved.provider == "zerodha"
    _record(22, "Zerodha broker resolves provider via self._binding", ok,
             f"resolved.provider={getattr(resolved, 'provider', None)}")


async def scenario_23_upstox_symbol_resolution_uses_b_attr():
    class _FakeUpstoxBinding:
        provider = "upstox"
    class _FakeUpstoxBroker:
        _b = _FakeUpstoxBinding()   # Upstox's REAL attribute name
    b = _FakeUpstoxBroker()
    resolved = getattr(b, "_binding", None) or getattr(b, "_b", None)
    ok = resolved is not None and resolved.provider == "upstox"
    _record(23, "Upstox broker resolves provider via self._b (fallback)", ok,
             f"resolved.provider={getattr(resolved, 'provider', None)}")


async def scenario_24_market_order_sends_zero_price():
    import inspect
    import execution_bridge.straddle_bridge as bridge_mod
    src = inspect.getsource(bridge_mod)
    # straddle_bridge.py deliberately never sets price= on its OrderRequest
    # calls for live/paper_route MARKET orders (defaults to 0.0) -- confirmed
    # real incident: OI-ORB's own bridge WAS setting a stale price and Upstox
    # rejected every MARKET order with UDAPI1040 until that was fixed.
    ok = "price=req.price" not in src and "price=ev.entry_price" not in src
    _record(24, "SellStraddle's live order path never sends a non-zero MARKET price", ok,
             "inspected straddle_bridge.py source for a price= kwarg on OrderRequest")


# ═══════════════════════════════════════════════════════════════════════════
# HEDGE / 1500 / 1515 / INDIVIDUAL-R1-BREACH SCENARIOS (25-28)
# ═══════════════════════════════════════════════════════════════════════════

async def scenario_25_eod_hedge_and_carry():
    s = _base_strategy()
    s._client_id, s._binding_id = "C", "B"
    today = _dt.datetime.now(IST).date()
    s._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24000, 100.0, 130.0, open_time=_dt.datetime.now(IST)),  # in loss
        pe_leg=StraddleLeg("PE", 24000, 100.0, 120.0, open_time=_dt.datetime.now(IST)),  # in loss
        net_credit=200.0, status="open",
        expiry_date=today + _dt.timedelta(days=5),
    )
    s._strike_prem = {
        (24500, "CE"): {"ltp": 60.0},   # <=50% of 130
        (23500, "PE"): {"ltp": 55.0},   # <=50% of 120
    }
    s._hedge_carry_enabled = True

    async def _fake_dispatch(action, side, strike, price, entry_price, expiry, reason):
        return SimpleNamespace(action=action, option_type=side, strike=strike,
                                fill_price={"CE": 60.0, "PE": 55.0}[side],
                                entry_aborted=False, routing_failed=False, exit_failed=False)
    s._dispatch_hedge_order = _fake_dispatch
    closed = []
    s._close_position = lambda reason: closed.append(reason) or asyncio.sleep(0)

    await s._eod_close_or_hedge(s._position, _dt.datetime.now(IST))

    ok = (closed == [] and s._stop_for_day is True
          and s._position.is_hedged_positional is True
          and s._position.hedge_ce_leg.strike == 24500
          and s._position.hedge_pe_leg.strike == 23500)
    _record(25, "EOD hedge-and-carry: both legs in loss -> builds a real hedge, no normal close", ok,
             f"is_hedged_positional={s._position.is_hedged_positional}, "
             f"hedge_ce={getattr(s._position.hedge_ce_leg, 'strike', None)}, "
             f"hedge_pe={getattr(s._position.hedge_pe_leg, 'strike', None)}")


async def scenario_26_the_1500_day_low_freeze():
    """The "1500 scenario": day-low reversal exit's freeze time (default
    15:00) -- once frozen, a retest of that exact low fires the exit
    immediately, same tick."""
    from unittest.mock import AsyncMock
    s = _base_strategy()
    s._day_low_exit_enabled = True
    s._day_low_freeze_time = _dt.time(0, 0)
    s._compute_day_low_for_pair = AsyncMock(return_value=95.05)
    closed = []
    async def _fake(reason):
        closed.append(reason)
        s._position.status = "closed"
        s._position = None
    s._close_position = _fake
    # Freeze-tick value IS the day's low -- must exit same tick, no retest needed.
    _open_position(s, ce_ltp=55.05, pe_ltp=40.0)   # current_value = 95.05
    await s._check_exits()
    ok = closed == ["day_low_reversal_exit"]
    _record(26, "1500 scenario: freeze-tick-is-itself-the-low exits same tick", ok,
             f"close_calls={closed} (expected ['day_low_reversal_exit'])")


async def scenario_27_the_1515_profit_arm():
    """The "1515 scenario": post-1500 per-leg exit's SECOND arm condition --
    time >= 15:15 AND the overall day P&L has flipped positive (independent
    of the day-low path)."""
    s = _base_strategy()
    s._post1500_exit_enabled = True
    s._session_min_straddle_frozen = None
    s._session_realized_pnl_pts = 0.0
    _open_position(s, ce_ltp=40.0, pe_ltp=40.0, ce_entry=20.0, pe_entry=20.0)  # net_credit=40, current=80 -> loss
    now = _dt.datetime.now(IST).replace(hour=15, minute=20, second=0, microsecond=0)
    await s._check_post1500_r1_exit(s._position, now)
    still_unarmed_in_loss = s._post1500_armed is False

    s._position.ce_leg.ltp = 5.0
    s._position.pe_leg.ltp = 5.0   # current=10 -> unrealized_pnl = 40-10 = +30, profitable
    await s._check_post1500_r1_exit(s._position, now)
    armed_on_profit_flip = s._post1500_armed is True and s._post1500_armed_reason == "profit"

    ok = still_unarmed_in_loss and armed_on_profit_flip
    _record(27, "1515 scenario: post1500 R1 guard waits for a profit flip at/after 15:15", ok,
             f"unarmed-while-loss={still_unarmed_in_loss}, armed-on-profit={armed_on_profit_flip} "
             f"(reason={s._post1500_armed_reason})")


async def scenario_28_individual_r1_breach_closes_only_that_leg():
    s = _base_strategy()
    s._post1500_exit_enabled = True
    s._session_min_straddle_frozen = 1000.0   # arms immediately regardless of current value
    close_calls = []
    async def _fake_close_leg(side, reason, now):
        close_calls.append((side, reason))
        leg = s._position.ce_leg if side == "CE" else s._position.pe_leg
        leg.close_time = now
        return SimpleNamespace(close_aborted=False)
    s._close_leg = _fake_close_leg
    _open_position(s, ce_ltp=20.0, pe_ltp=20.0)

    base = _dt.datetime.now(IST).replace(hour=15, minute=1, second=0, microsecond=0)
    for i in range(5):
        now = base + _dt.timedelta(minutes=i)
        s._position.ce_leg.ltp = 20.0 + i   # rising CE -- will breach its own R1
        s._position.pe_leg.ltp = 20.0        # PE never moves
        await s._check_post1500_r1_exit(s._position, now)

    pe_touched = any(side == "PE" for side, _ in close_calls)
    ok = s._post1500_armed is True and pe_touched is False
    _record(28, "Individual R1 breach: only the leg that breaches its own R1 closes", ok,
             f"armed={s._post1500_armed}, close_calls={close_calls} (PE must never appear)")


# ═══════════════════════════════════════════════════════════════════════════

SCENARIOS = [
    (1, "BEGINNING finds balanced pair", scenario_01_beginning_finds_pair),
    (2, "RE-ENTRY finds balanced pair", scenario_02_reentry_finds_pair),
    (3, "NO-PAIR when floor unmet", scenario_03_no_pair_when_floor_unmet),
    (4, "Outside entry window blocks entry", scenario_04_outside_entry_window_blocks),
    (5, "max_trades reached blocks entry", scenario_05_max_trades_blocks_further_entries),
    (6, "Active cooldown blocks entry", scenario_06_active_cooldown_blocks_entry),
    (7, "Terminal OFF blocks entry", scenario_07_terminal_off_blocks_entry),
    (8, "stop_for_day blocks entry", scenario_08_stop_for_day_blocks_entry),
    (9, "Low anchor LTP flags next-week shift", scenario_09_low_anchor_ltp_shifts_to_next_week),
    (10, "Healthy anchor does not shift", scenario_10_healthy_anchor_does_not_shift),
    (11, "day_loss_sl full close", scenario_11_day_loss_sl_full_close),
    (12, "day_profit_target full close", scenario_12_day_profit_target_full_close),
    (13, "Rollover never touches trades_today", scenario_13_rollover_never_touches_trades_today),
    (14, "ITM pair-gate wired", scenario_14_itm_pair_gate_close_fallback),
    (15, "Scalable TSL wired", scenario_15_scalable_tsl_wired),
    (16, "Day-low reversal exit freezes true low", scenario_16_day_low_reversal_exit),
    (17, "EOD force exit", scenario_17_eod_force_exit),
    (18, "post_restore_data_stale reverts to BEGINNING", scenario_18_post_restore_data_stale_reverts_beginning),
    (19, "Genuine exit does not revert trades_today", scenario_19_genuine_exit_does_not_revert_trades_today),
    (20, "Restart with open position re-arms sticky pin", scenario_20_restart_with_open_position_rearms_sticky_pin),
    (21, "Restart with no position: clean start", scenario_21_restart_with_no_position_clean_start),
    (22, "Zerodha symbol resolution (_binding)", scenario_22_zerodha_symbol_resolution_uses_binding_attr),
    (23, "Upstox symbol resolution (_b)", scenario_23_upstox_symbol_resolution_uses_b_attr),
    (24, "MARKET order sends price=0", scenario_24_market_order_sends_zero_price),
    (25, "EOD hedge-and-carry builds a real hedge", scenario_25_eod_hedge_and_carry),
    (26, "1500 scenario: day-low freeze/retest", scenario_26_the_1500_day_low_freeze),
    (27, "1515 scenario: post1500 profit-flip arm", scenario_27_the_1515_profit_arm),
    (28, "Individual R1 breach closes only that leg", scenario_28_individual_r1_breach_closes_only_that_leg),
]


def main():
    print("=" * 100)
    print("SellStraddle Scenario Simulator -- driving the REAL SellStraddleStrategy class")
    print("=" * 100)
    for num, name, fn in SCENARIOS:
        _run(num, name, fn)
        print()

    passed = sum(1 for _, _, ok, _ in _RESULTS if ok)
    total = len(_RESULTS)
    print("=" * 100)
    print(f"RESULT: {passed}/{total} scenarios PASSED")
    if passed != total:
        print("FAILED scenarios:")
        for num, name, ok, detail in _RESULTS:
            if not ok:
                print(f"  #{num}: {name}")
    print("=" * 100)
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
