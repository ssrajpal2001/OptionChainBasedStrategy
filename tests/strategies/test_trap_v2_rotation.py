"""TT v2 Task 6 — immediate PE↔CE rotation."""

import asyncio

from config.global_config import GlobalConfig, Topic
from data_layer.base_feeder import EventBus
from strategies.trap_trading_engine import should_rotate, TrapTradingEngine


# ── Pure decision ────────────────────────────────────────────────────────────
def test_rotate_when_other_leg_and_position_open():
    assert should_rotate(running_side="PE", signal_side="CE", has_position=True) is True
    assert should_rotate(running_side="CE", signal_side="PE", has_position=True) is True


def test_no_rotate_same_side():
    assert should_rotate(running_side="PE", signal_side="PE", has_position=True) is False


def test_no_rotate_when_flat():
    assert should_rotate(running_side=None, signal_side="CE", has_position=False) is False


# ── Integration: CE running, PE signal → exit CE + enter PE ───────────────────
def _engine():
    eng = TrapTradingEngine(EventBus(), GlobalConfig())
    eng._spot_cache["CRUDEOIL"] = 8765.0
    return eng


def _drain(q):
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def test_rotation_ce_to_pe_exits_and_enters():
    eng = _engine()
    q = eng._bus.subscribe(Topic.SIGNAL)

    async def run():
        await eng._fire_entry_v2("CRUDEOIL", "CE", 380.0)   # open CE
        assert eng._v2_position["opt_type"] == "CE"
        await eng._fire_entry_v2("CRUDEOIL", "PE", 360.0)   # opposite → rotate
        await asyncio.sleep(0.02)

    asyncio.run(run())

    # position cleanly shifted to PE
    assert eng._v2_position is not None
    assert eng._v2_position["opt_type"] == "PE"
    assert eng._v2_position["strike"] == 8800   # fresh ATM from spot
    assert eng._v2_position["entry_premium"] == 360.0

    notes = [getattr(s, "notes", "") for s in _drain(q)]
    assert any("BUY CE 8800" in n for n in notes)                 # original CE entry
    assert any("EXIT rotation" in n and "CE 8800" in n for n in notes)  # CE rotated out
    assert any("BUY PE 8800" in n for n in notes)                 # new PE entry


def test_same_side_signal_ignored():
    eng = _engine()

    async def run():
        await eng._fire_entry_v2("CRUDEOIL", "CE", 380.0)
        await eng._fire_entry_v2("CRUDEOIL", "CE", 370.0)   # same side → ignored

    asyncio.run(run())
    assert eng._v2_position["opt_type"] == "CE"
    assert eng._v2_position["entry_premium"] == 380.0       # unchanged (not re-entered)
