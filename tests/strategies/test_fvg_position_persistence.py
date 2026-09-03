"""FVG must persist its open position the same way SellStraddle does
(strategies/core/position.py's PositionStoreMixin), so a mid-day restart
restores it instead of silently losing track while the real broker leg
stays open.

2026-08-05: _open_position/_square_off are now confirm-then-finalize (Task 7)
-- they dispatch an order via self._bus.publish(...) and AWAIT an
FVGOrderFillEvent on the same event_id before touching self._position /
persist()/clear() at all. A bare EventBus with nothing consuming
Topic.FVG_ORDER_FILL never answers that wait, so these tests now wire up a
small self-confirming fake bus (mirrors tests/strategies/
test_d1trap_bear_only_safety.py's _ConfirmingBus) that immediately feeds the
real _on_fill a confirmed (non-aborted) fill for whatever it just published --
exactly what execution_bridge/fvg_bridge.py does for a real paper/live fill in
production."""
from datetime import date, datetime, timedelta

from config.global_config import GlobalConfig
from data_layer import position_store
from data_layer.base_feeder import EventBus

IST_OFFSET = timedelta(hours=5, minutes=30)


def _make_strategy(cfg):
    from strategies.fvg.engine import FVGStrategy
    return FVGStrategy(
        EventBus(), cfg, underlying="NIFTY", client_id="c1", binding_id="b1",
        lot_multiplier=1, feeder_token="",
    )


def _fake_fvg():
    return {"zone_lo": 24500.0, "zone_hi": 24520.0}


class _ConfirmingBus:
    """Immediately confirms every BUY/SELL published to it -- same shape as
    test_d1trap_bear_only_safety.py's _ConfirmingBus, standing in for
    execution_bridge/fvg_bridge.py's real paper/live fill round trip."""

    def __init__(self, strat) -> None:
        self._strat = strat
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))
        from execution_bridge.fvg_bridge import FVGOrderFillEvent
        fill = FVGOrderFillEvent(
            action=event.action, underlying=event.underlying,
            option_type=event.option_type, strike=event.strike,
            fill_price=event.entry_price, qty=event.quantity,
            client_id=event.client_id, binding_id=event.binding_id,
            event_id=event.event_id,
        )
        self._strat._on_fill(fill)


def test_fvg_persists_position_on_open_and_clears_on_close(tmp_path, monkeypatch):
    # Point position_store at a scratch dir so this test doesn't touch real data/positions/
    monkeypatch.setattr(position_store, "_DIR", str(tmp_path))

    cfg = GlobalConfig()
    strat = _make_strategy(cfg)
    strat._bus = _ConfirmingBus(strat)
    expiry = date.today() + timedelta(days=10)

    # Directly populate the live premium book so _open_position's "no data ->
    # no trade" guard doesn't block us, then drive the real entry path.
    strat._option_ltp[(24450, "CE", expiry)] = 120.0

    import strategies.fvg.engine as fvg_engine
    monkeypatch.setattr(fvg_engine, "_next_week_expiry", lambda *a, **k: expiry)

    ts = datetime.now()
    import asyncio

    async def _run_open():
        await strat._open_position(_fake_fvg(), "LONG", 24500.0, 24450.0, ts)

    asyncio.run(_run_open())

    assert strat._position is not None
    stored = position_store.load(strat._persist_key)
    assert stored is not None
    assert stored["option_type"] == "CE"
    assert stored["strike"] == 24450

    async def _run_close():
        await strat._square_off("manual")

    asyncio.run(_run_close())

    assert strat._position is None
    assert position_store.load(strat._persist_key) is None


def test_fvg_restores_position_on_construction(tmp_path, monkeypatch):
    monkeypatch.setattr(position_store, "_DIR", str(tmp_path))

    cfg = GlobalConfig()
    expiry = date.today() + timedelta(days=10)
    position_store.save(
        "c1_b1_NIFTY_fvg",
        {
            "direction": "LONG", "entry": 24500.0, "sl": 24450.0,
            "premium_entry": 120.0, "premium_sl": 96.0, "high_lock_pct": 0.0,
            "option_type": "CE", "strike": 24450, "expiry": expiry.isoformat(),
            "qty": 75, "entry_ts": datetime.now().isoformat(),
            "fvg_zone": [24500.0, 24520.0],
        },
        product_type="MIS",
    )

    strat = _make_strategy(cfg)

    assert strat._position is not None
    assert strat._position["strike"] == 24450
    assert strat._position["option_type"] == "CE"
    assert strat._position["expiry"] == expiry


def test_fvg_fresh_construction_with_no_persisted_position_does_not_crash(tmp_path, monkeypatch):
    monkeypatch.setattr(position_store, "_DIR", str(tmp_path))
    cfg = GlobalConfig()
    strat = _make_strategy(cfg)
    assert strat._position is None
