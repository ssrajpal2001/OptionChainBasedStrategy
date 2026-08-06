import asyncio
import datetime
from unittest.mock import patch, MagicMock

from data_layer.base_feeder import EventBus
from config.global_config import IST, GlobalConfig
from execution_bridge.straddle_bridge import StraddleFillEvent
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


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

        with patch("strategies.sell_straddle.selection.select_partner_for",
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

        with patch("strategies.sell_straddle.selection.select_partner_for",
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


def test_single_side_roll_enforces_ltp_le_kept_on_the_real_selection_call():
    """2026-08-06 CRITICAL FIX regression test. select_partner_for's ltp_le_kept
    parameter defaults to False (documented as intentional for OTHER callers,
    e.g. re-entry pair selection) -- but this codebase's OWN documented
    rollover rule is "the new partner must be ... STRICTLY <= the kept leg's
    LTP (never roll into a richer leg)". The two live rolling.py call sites
    were passing ltp_le_kept=False, silently disabling that rule for every
    real roll. This directly asserts the actual keyword argument
    _single_side_roll passes at its real call site -- not a re-derivation of
    select_partner_for's own already-tested behavior."""
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
        with patch("strategies.sell_straddle.selection.select_partner_for", mock_select):
            await s._single_side_roll(datetime.datetime.now(IST), "ltp_decay")

        assert mock_select.called
        _, kwargs = mock_select.call_args
        assert kwargs.get("ltp_le_kept") is True, (
            f"_single_side_roll called select_partner_for with ltp_le_kept="
            f"{kwargs.get('ltp_le_kept')!r} -- the 'never roll into a richer leg' "
            f"rule is not being enforced."
        )

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
