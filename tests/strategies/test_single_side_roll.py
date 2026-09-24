import asyncio
import datetime
from unittest.mock import patch, MagicMock, AsyncMock

from data_layer.base_feeder import EventBus
from config.global_config import IST, GlobalConfig
from execution_bridge.straddle_bridge import StraddleFillEvent
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def test_single_side_roll_anchors_new_strike_on_the_closed_strike_not_atm():
    """2026-08-26, direct user spec regression (real incident): the OLD ATM-centered
    global-best search could land the new leg anywhere in the whole ATM+/-offset
    window, with no predictable relationship to the strike actually being closed --
    confirmed live (24550 closed -> 24500 picked, only 50pts away; a separate roll
    the same day: 24500 closed -> 24400, 100pts away -- two different distances from
    the SAME kind of roll). Now the search is anchored on the CLOSING strike itself
    in expanding 100pt rings, so it must land exactly on closed_strike+/-100 whenever
    that ring has a valid candidate -- even when a strike FARTHER from the closed
    strike would have "balanced" better against the kept leg."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._itm_pair_gate_enabled = False
        s._ltp_target = 0.0
        s._theta_target = 0.0
        s._spot = 24460.0
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
            # CE is the "good"/less-loss leg -> gets rolled (roll_side=CE); PE is kept.
            ce_leg=StraddleLeg("CE", 24550, 60.0, 10.0, open_time=datetime.datetime.now(IST)),
            pe_leg=StraddleLeg("PE", 24450, 60.0, 90.0, open_time=datetime.datetime.now(IST)),
            net_credit=120.0, status="open",
        )
        # CE24750 is a near-PERFECT balance match against the kept PE's ltp (90) --
        # the OLD ATM-centered global-best search would have picked it over anything
        # closer to the closing strike. CE24450 sits in ring 1 (closing strike 24550,
        # one 100pt ring away) and is the only ring-1 candidate quoted at all (24650,
        # ring 1's other side, has no quote) -- it must win regardless of CE24750's
        # much better balance, because ring 2 must never even be reached once ring 1
        # already has a passing candidate.
        s._strike_prem = {
            (24450, "CE"): {"ltp": 65.0, "atp": 65.0},   # ring 1 (24550-100): must win
            (24750, "CE"): {"ltp": 90.0, "atp": 90.0},   # ring 2 -- perfect balance, must NOT win
        }

        emitted = []
        orig_emit = s._emit_order

        async def capture_emit(ev):
            emitted.append(ev)
            await orig_emit(ev)

        s._emit_order = capture_emit

        async def deliver_fills():
            await asyncio.sleep(0.02)
            close_ev = [o for o in emitted if o.action == "EXIT"][0]
            s._on_fill(StraddleFillEvent(
                action="EXIT", underlying="NIFTY", atm=24500.0,
                ce_strike=24550.0, pe_strike=24450.0,
                ce_fill=10.0, pe_fill=0.0,
                client_id="C", binding_id="B",
                event_id=close_ev.event_id, legs=["CE"],
            ))
            await asyncio.sleep(0.02)
            open_ev = [o for o in emitted if o.action == "ENTRY"][0]
            s._on_fill(StraddleFillEvent(
                action="ENTRY", underlying="NIFTY", atm=24500.0,
                ce_strike=open_ev.ce_strike, pe_strike=24450.0,
                ce_fill=open_ev.ce_ltp, pe_fill=0.0,
                client_id="C", binding_id="B",
                event_id=open_ev.event_id, legs=["CE"],
            ))

        task = asyncio.create_task(deliver_fills())
        # Bypass the real re-entry rules (default config needs warm pool-engine
        # indicators this synthetic test has none of) -- isolates the anchor-ring
        # selection logic itself, which is what this test targets.
        with patch("strategies.sell_straddle.rolling.RuntimeConfig") as _rc:
            _rc.index_section.return_value = {"entry_rules_reentry": []}
            rolled = await s._single_side_roll(datetime.datetime.now(IST), "ltp_decay")
        await task

        assert rolled is True
        assert s._position.ce_leg.strike == 24450   # ring 1, NOT the better-balanced 24650
    asyncio.run(run())


def test_single_side_roll_no_candidate_keeps_original_pair():
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23500, 80.0, 10.0),
            pe_leg=StraddleLeg("PE", 23500, 80.0, 70.0),
            net_credit=160.0, status="open",
        )
        s._spot = 23500
        s._strike_prem = {}   # empty → no rollover partner found
        await s._single_side_roll(datetime.datetime.now(IST), "ltp_decay")
        assert s._position is not None   # no partner → keep original pair
        assert s._position.status == "open"
    asyncio.run(run())


def test_single_side_roll_no_candidate_does_not_hedge_before_streak_threshold():
    """2026-09-24, direct user fix (real incident: a hedge fired at 09:18:35
    off just 2 failed vwap_rise_roll attempts within 3 minutes of a brand-new
    position, traced to the pool engine's own indicators still being noisy
    that early -- a clean candidate passed on its own 4 minutes later). The
    hedge-on-failed-rollover hook must NOT fire until
    _ROLL_FAIL_HEDGE_STREAK (10) CONSECUTIVE failures have accumulated --
    a single failure (streak starts at 0) must only increment the counter,
    never call the hedge dispatch."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23500, 80.0, 10.0),
            pe_leg=StraddleLeg("PE", 23500, 80.0, 70.0),
            net_credit=160.0, status="open",
        )
        s._spot = 23500
        s._strike_prem = {}  # empty -> no rollover partner found
        s._hedge_or_roll_if_eligible = AsyncMock(return_value=True)

        rolled = await s._single_side_roll(datetime.datetime.now(IST), "ltp_decay")

        assert rolled is False
        s._hedge_or_roll_if_eligible.assert_not_awaited()
        assert s._roll_fail_streak == 1
    asyncio.run(run())


def test_single_side_roll_no_candidate_activates_hedge_at_streak_threshold():
    """The 10th consecutive failure (streak already at 9 from prior calls)
    must attempt to activate the hedge via the shared
    _hedge_or_roll_if_eligible dispatch, with stop_for_day_on_hedge=False
    (same as the existing day_loss_sl call site), and reset the streak
    afterward."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23500, 80.0, 10.0),
            pe_leg=StraddleLeg("PE", 23500, 80.0, 70.0),
            net_credit=160.0, status="open",
        )
        s._spot = 23500
        s._strike_prem = {}
        s._roll_fail_streak = 9
        s._hedge_or_roll_if_eligible = AsyncMock(return_value=True)

        now = datetime.datetime.now(IST)
        rolled = await s._single_side_roll(now, "ltp_decay")

        assert rolled is False  # no roll happened -- hedge stands in for it
        s._hedge_or_roll_if_eligible.assert_awaited_once_with(
            s._position, now, stop_for_day_on_hedge=False,
        )
        assert s._roll_fail_streak == 0  # reset after a successful hedge build
    asyncio.run(run())


def test_single_side_roll_no_candidate_skips_hedge_when_already_hedged():
    """A position already carrying a hedge must not try to build a second
    one on a later failed roll, even once the streak threshold is reached."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23500, 80.0, 10.0),
            pe_leg=StraddleLeg("PE", 23500, 80.0, 70.0),
            net_credit=160.0, status="open",
        )
        s._position.is_hedged_positional = True
        s._spot = 23500
        s._strike_prem = {}
        s._roll_fail_streak = 9
        s._hedge_or_roll_if_eligible = AsyncMock(return_value=True)

        await s._single_side_roll(datetime.datetime.now(IST), "ltp_decay")

        s._hedge_or_roll_if_eligible.assert_not_awaited()
    asyncio.run(run())


def test_single_side_roll_no_candidate_skips_hedge_for_itm_pair_gate_reason():
    """itm_pair_gate_profit_rollover already has its own dedicated
    close-fallback on a failed roll -- the new hedge-on-failure hook must
    not also fire for that specific reason, even at the streak threshold."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23500, 80.0, 10.0),
            pe_leg=StraddleLeg("PE", 23500, 80.0, 70.0),
            net_credit=160.0, status="open",
        )
        s._spot = 23500
        s._strike_prem = {}
        s._roll_fail_streak = 9
        s._hedge_or_roll_if_eligible = AsyncMock(return_value=True)

        await s._single_side_roll(datetime.datetime.now(IST), "itm_pair_gate_profit_rollover")

        s._hedge_or_roll_if_eligible.assert_not_awaited()
    asyncio.run(run())


def test_single_side_roll_success_resets_the_fail_streak():
    """A successful roll (partner found) must reset the consecutive-failure
    streak -- a later, unrelated failure must not inherit a count from
    before this success."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._itm_pair_gate_enabled = False
        s._spot = 24400.0
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
            ce_leg=StraddleLeg("CE", 24450, 152.75, 107.0,
                               open_time=datetime.datetime.now(IST)),
            pe_leg=StraddleLeg("PE", 24450, 132.05, 162.95,
                               open_time=datetime.datetime.now(IST)),
            net_credit=284.8, status="open",
        )
        s._roll_fail_streak = 7

        emitted = []
        orig_emit = s._emit_order

        async def capture_emit(ev):
            emitted.append(ev)
            await orig_emit(ev)
        s._emit_order = capture_emit

        with patch("strategies.sell_straddle.selection.select_rollover_partner_directional",
                   return_value=(24350, 156.90)):
            async def deliver_fills():
                await asyncio.sleep(0.02)
                close_ev = [o for o in emitted if o.action == "EXIT"][0]
                s._on_fill(StraddleFillEvent(
                    action="EXIT", underlying="NIFTY", atm=24500.0,
                    ce_strike=24450.0, pe_strike=24450.0,
                    ce_fill=107.0, pe_fill=0.0,
                    client_id="C", binding_id="B",
                    event_id=close_ev.event_id, legs=["CE"],
                ))
                await asyncio.sleep(0.02)
                open_ev = [o for o in emitted if o.action == "ENTRY"][0]
                s._on_fill(StraddleFillEvent(
                    action="ENTRY", underlying="NIFTY", atm=24500.0,
                    ce_strike=24350.0, pe_strike=24450.0,
                    ce_fill=156.90, pe_fill=0.0,
                    client_id="C", binding_id="B",
                    event_id=open_ev.event_id, legs=["CE"],
                ))

            task = asyncio.create_task(deliver_fills())
            rolled = await s._single_side_roll(datetime.datetime.now(IST), "scalable_tsl")
            await task

        assert rolled is True
        assert s._roll_fail_streak == 0
    asyncio.run(run())


def test_single_side_roll_waits_for_close_fill_before_open():
    """A single-side roll must buy-to-close the good leg and only sell-to-open the
    new partner AFTER the close fill is confirmed. The untouched leg must keep its
    original entry price and strike."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._itm_pair_gate_enabled = False   # isolate roll mechanics from ITM gate
        s._spot = 24400.0
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
            ce_leg=StraddleLeg("CE", 24450, 152.75, 107.0,
                               open_time=datetime.datetime.now(IST)),
            pe_leg=StraddleLeg("PE", 24450, 132.05, 162.95,
                               open_time=datetime.datetime.now(IST)),
            net_credit=284.8, status="open",
        )

        emitted = []
        orig_emit = s._emit_order

        async def capture_emit(ev):
            emitted.append(ev)
            await orig_emit(ev)

        s._emit_order = capture_emit

        with patch("strategies.sell_straddle.selection.select_rollover_partner_directional",
                   return_value=(24350, 156.90)):
            async def deliver_fills():
                await asyncio.sleep(0.02)
                close_ev = [o for o in emitted if o.action == "EXIT"][0]
                # Confirm ONLY the CE leg was closed.
                s._on_fill(StraddleFillEvent(
                    action="EXIT", underlying="NIFTY", atm=24500.0,
                    ce_strike=24450.0, pe_strike=24450.0,
                    ce_fill=107.0, pe_fill=0.0,
                    client_id="C", binding_id="B",
                    event_id=close_ev.event_id, legs=["CE"],
                ))
                await asyncio.sleep(0.02)
                open_ev = [o for o in emitted if o.action == "ENTRY"][0]
                # Confirm ONLY the CE leg was opened.
                s._on_fill(StraddleFillEvent(
                    action="ENTRY", underlying="NIFTY", atm=24500.0,
                    ce_strike=24350.0, pe_strike=24450.0,
                    ce_fill=156.90, pe_fill=0.0,
                    client_id="C", binding_id="B",
                    event_id=open_ev.event_id, legs=["CE"],
                ))

            task = asyncio.create_task(deliver_fills())
            await s._single_side_roll(datetime.datetime.now(IST), "scalable_tsl")
            await task

        assert len(emitted) == 2
        assert emitted[0].action == "EXIT"
        assert emitted[0].legs == ["CE"]
        assert emitted[1].action == "ENTRY"
        assert emitted[1].legs == ["CE"]

        pos = s._position
        assert pos is not None
        assert pos.status == "open"
        # Rolled leg updated.
        assert pos.ce_leg.strike == 24350
        assert pos.ce_leg.entry_price == 156.90
        # Kept leg untouched.
        assert pos.pe_leg.strike == 24450
        assert pos.pe_leg.entry_price == 132.05
        assert s._roll_in_progress is False

    asyncio.run(run())


def test_single_side_roll_arms_70pct_protection_regardless_of_reason():
    """2026-08-27, direct user spec: the 70%-of-booked-profit roll-protection
    stop is NOT scoped to itm_pair_gate_profit_rollover anymore -- it arms
    after ANY single-side roll that books a profit on the closed leg,
    irrespective of ITM/OTM or which reason triggered it (ltp_decay, ratio,
    vwap_rise, exit_rules, scalable_tsl, ...). Same CE 152.75->107.0 close as
    test_single_side_roll_waits_for_close_fill_before_open (a real ₹ profit),
    but with reason='scalable_tsl' -- a plain non-ITM roll reason."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._itm_pair_gate_enabled = False
        # 2026-09-22, direct user instruction: the 70% roll-protection arming
        # block is now opt-in (default False) -- explicitly enable it here so
        # this test still exercises the arming logic itself (that it arms on
        # ANY roll reason, not just itm_pair_gate) rather than the new default.
        s._itm_roll_protection_enabled = True
        s._spot = 24400.0
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
            ce_leg=StraddleLeg("CE", 24450, 152.75, 107.0,
                               open_time=datetime.datetime.now(IST)),
            pe_leg=StraddleLeg("PE", 24450, 132.05, 162.95,
                               open_time=datetime.datetime.now(IST)),
            net_credit=284.8, status="open",
        )

        emitted = []
        orig_emit = s._emit_order

        async def capture_emit(ev):
            emitted.append(ev)
            await orig_emit(ev)
        s._emit_order = capture_emit

        with patch("strategies.sell_straddle.selection.select_rollover_partner_directional",
                   return_value=(24350, 156.90)):
            async def deliver_fills():
                await asyncio.sleep(0.02)
                close_ev = [o for o in emitted if o.action == "EXIT"][0]
                s._on_fill(StraddleFillEvent(
                    action="EXIT", underlying="NIFTY", atm=24500.0,
                    ce_strike=24450.0, pe_strike=24450.0,
                    ce_fill=107.0, pe_fill=0.0,
                    client_id="C", binding_id="B",
                    event_id=close_ev.event_id, legs=["CE"],
                ))
                await asyncio.sleep(0.02)
                open_ev = [o for o in emitted if o.action == "ENTRY"][0]
                s._on_fill(StraddleFillEvent(
                    action="ENTRY", underlying="NIFTY", atm=24500.0,
                    ce_strike=24350.0, pe_strike=24450.0,
                    ce_fill=156.90, pe_fill=0.0,
                    client_id="C", binding_id="B",
                    event_id=open_ev.event_id, legs=["CE"],
                ))

            task = asyncio.create_task(deliver_fills())
            rolled = await s._single_side_roll(datetime.datetime.now(IST), "scalable_tsl")
            await task

        assert rolled is True
        assert "CE" in s._itm_roll_protection, "a plain non-ITM roll reason must still arm protection"
        prot = s._itm_roll_protection["CE"]
        assert prot["new_strike"] == 24350
        assert prot["orig_strike"] == 24450
        assert prot["kept_side"] == "PE"
        assert prot["kept_strike"] == 24450
        assert prot["protect_rs"] > 0   # 70% of the real booked profit on the CE close

    asyncio.run(run())


def test_single_side_roll_reopen_rejected_closes_kept_leg_for_real():
    """2026-08-06 CRITICAL FIX regression test. Sequence: old CE leg closes for
    real (confirmed), then the broker REJECTS the new CE leg's reopen order
    (entry_aborted, legs=["CE"] -- the roll-reopen signature). The kept PE leg
    is still genuinely open at the broker. The old (pre-fix) behavior nulled
    self._position outright, discarding tracking of that real, untouched PE
    leg -- an orphaned real position the engine would then believe is flat.
    The fix must instead send a REAL close order for the kept PE leg and only
    then finalize the position as closed -- never a blind null, never a
    close order for the CE leg that was never actually opened."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._itm_pair_gate_enabled = False
        s._spot = 24400.0
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
            ce_leg=StraddleLeg("CE", 24450, 152.75, 107.0,
                               open_time=datetime.datetime.now(IST)),
            pe_leg=StraddleLeg("PE", 24450, 132.05, 162.95,
                               open_time=datetime.datetime.now(IST)),
            net_credit=284.8, status="open",
        )

        emitted = []
        orig_emit = s._emit_order

        async def capture_emit(ev):
            emitted.append(ev)
            await orig_emit(ev)

        s._emit_order = capture_emit

        with patch("strategies.sell_straddle.selection.select_rollover_partner_directional",
                   return_value=(24350, 156.90)):
            async def deliver_fills():
                await asyncio.sleep(0.02)
                close_ev = [o for o in emitted if o.action == "EXIT"][0]
                # Old CE leg closes for real.
                s._on_fill(StraddleFillEvent(
                    action="EXIT", underlying="NIFTY", atm=24500.0,
                    ce_strike=24450.0, pe_strike=24450.0,
                    ce_fill=107.0, pe_fill=0.0,
                    client_id="C", binding_id="B",
                    event_id=close_ev.event_id, legs=["CE"],
                ))
                await asyncio.sleep(0.02)
                open_ev = [o for o in emitted if o.action == "ENTRY"][0]
                # New CE leg's reopen is REJECTED by the broker.
                s._on_fill(StraddleFillEvent(
                    action="ENTRY", underlying="NIFTY", atm=24500.0,
                    ce_strike=24350.0, pe_strike=24450.0,
                    ce_fill=0.0, pe_fill=0.0,
                    client_id="C", binding_id="B",
                    event_id=open_ev.event_id, legs=["CE"],
                    entry_aborted=True,
                ))
                # Give the scheduled _abort_roll_reopen task a moment to run,
                # then confirm the real PE close it sends.
                await asyncio.sleep(0.02)
                pe_close_ev = [o for o in emitted if o.action == "EXIT" and o.legs == ["PE"]][0]
                s._on_fill(StraddleFillEvent(
                    action="EXIT", underlying="NIFTY", atm=24500.0,
                    ce_strike=24450.0, pe_strike=24450.0,
                    ce_fill=0.0, pe_fill=162.95,
                    client_id="C", binding_id="B",
                    event_id=pe_close_ev.event_id, legs=["PE"],
                ))

            task = asyncio.create_task(deliver_fills())
            await s._single_side_roll(datetime.datetime.now(IST), "scalable_tsl")
            await task
            await asyncio.sleep(0.05)  # let the asyncio.create_task cleanup finish

        # A real close order was sent for the KEPT leg (PE) -- never a blind null.
        pe_closes = [o for o in emitted if o.action == "EXIT" and o.legs == ["PE"]]
        assert len(pe_closes) == 1, emitted
        # No close/open order was ever sent for CE a second time (it was never
        # really open -- must not send a phantom close for it).
        ce_orders = [o for o in emitted if "CE" in o.legs]
        assert len(ce_orders) == 2  # the original close + the rejected reopen attempt only
        # Position correctly ends up fully flat, not silently orphaned.
        assert s._position is None
        assert s._roll_in_progress is False
        assert s._order_pending is False

    asyncio.run(run())


def test_single_side_roll_calls_the_directional_search_with_the_closing_strike():
    """2026-08-27: the main rollover path now calls select_rollover_partner_directional
    (not select_partner_for), which enforces "never roll into a richer leg than the
    one being kept" unconditionally inside _evaluate_roll_candidate (ltp_le_kept=True
    is no longer a caller-supplied flag at all -- see selection.py). This asserts the
    real call site passes the strike actually being closed as `closing_strike`, the
    keep_strike/keep_ltp of the OTHER (kept) leg, and DOESN'T ask for the guarantee
    via a kwarg since it's now baked in structurally."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._itm_pair_gate_enabled = False
        s._spot = 24400.0
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
            ce_leg=StraddleLeg("CE", 24450, 152.75, 107.0,
                               open_time=datetime.datetime.now(IST)),
            pe_leg=StraddleLeg("PE", 24450, 132.05, 162.95,
                               open_time=datetime.datetime.now(IST)),
            net_credit=284.8, status="open",
        )

        mock_select = MagicMock(return_value=None)  # None -> "no partner", roll no-ops cleanly
        with patch("strategies.sell_straddle.selection.select_rollover_partner_directional", mock_select):
            await s._single_side_roll(datetime.datetime.now(IST), "ltp_decay")

        assert mock_select.called
        _, kwargs = mock_select.call_args
        assert kwargs.get("closing_strike") == 24450   # the CE leg being rolled/exited
        assert kwargs.get("kept_strike") == 24450       # the PE leg (same strike, different side)
        assert kwargs.get("kept_ltp") == 162.95

    asyncio.run(run())


def test_itm_roll_protection_pool_search_enforces_ltp_le_kept():
    """Same fix, second call site: _check_itm_roll_protection_side's broader
    pool search must also enforce ltp_le_kept=True."""
    async def run():
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._spot = 24400.0
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=24500, entry_spot=24500,
            ce_leg=StraddleLeg("CE", 24450, 152.75, 107.0,
                               open_time=datetime.datetime.now(IST)),
            pe_leg=StraddleLeg("PE", 24450, 132.05, 162.95,
                               open_time=datetime.datetime.now(IST)),
            net_credit=284.8, status="open",
        )
        prot = {
            "protect_rs": 1000.0, "new_side": "CE", "new_strike": 24450,
            "orig_strike": 24400, "kept_side": "PE", "kept_strike": 24450,
        }

        mock_select = MagicMock(return_value=None)
        with patch("strategies.sell_straddle.selection.select_partner_for", mock_select):
            await s._check_itm_roll_protection_side("CE", prot, datetime.datetime.now(IST))

        if mock_select.called:  # only the broader pool-search branch calls it
            _, kwargs = mock_select.call_args
            assert kwargs.get("ltp_le_kept") is True

    asyncio.run(run())
