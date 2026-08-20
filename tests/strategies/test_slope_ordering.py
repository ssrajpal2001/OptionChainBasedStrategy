"""Regression: per-pair VWAP slope must compare the current candle's ATP to the
PREVIOUS candle's ATP — not to itself. The prev-ATP snapshot must run AFTER entry
evaluation in _on_candle, else slope is always 0.00 and SLOPE<0 never passes
(live incident 2026-06-02: no straddle entries fired all session).

2026-08-20 update: slope must now come from two genuinely CLOSED candles, not one
close blended with the still-live current tick. `_pair_indicators()` used to fall
back to a live-vs-one-prior-close calc whenever the real pool-engine's RSI wasn't
warm yet -- which is always true this early -- letting BEGINNING's SLOPE(1m) fire a
full candle earlier than a genuine 2-candle comparison would allow (real incident:
entry_start=09:16, trade fired 09:16:05 off just the 09:15-09:16 candle + 5s of live
ticks). `_pair_indicators()` now trusts the real pool-engine result whenever it has
any data at all, so slope is correctly ABSENT after only one candle close and only
appears once a second candle has genuinely closed."""
import asyncio
import datetime

from data_layer.base_feeder import EventBus, IndexTick, OptionTick, CandleEvent
from config.global_config import IST, Topic, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy


def test_pair_slope_absent_after_only_one_closed_candle():
    async def run():
        import datetime as _dt
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s.start()
        # Make the test time-independent: _on_candle force-exits (and skips the
        # prev-ATP snapshot) once now >= squareoff. Pin squareoff to end-of-day and
        # stop _load_thresholds from resetting it each candle. Also pin entry_start
        # to midnight so the entry_start floor (2026-08-20 fix) never excludes a bar
        # regardless of the real wall-clock time this test happens to run at.
        s._load_thresholds = lambda: None
        s._force_exit = _dt.time(23, 59)
        s._entry_start = _dt.time(0, 0)
        await asyncio.sleep(0.2)
        now = datetime.datetime.now(IST)
        exp = datetime.date.today()

        async def push(atp):
            await bus.publish(Topic.INDEX_TICK,
                              IndexTick("NIFTY", 23300, 23300, 23300, 23300, 23300, 0, now))
            for strike in (23250, 23300, 23350):
                for side in ("CE", "PE"):
                    await bus.publish(Topic.OPTION_TICK, OptionTick(
                        f"N{strike}{side}", "NIFTY", strike, side, exp,
                        50.0, 49.5, 50.5, 0, 0, 0, 0, 0, now, atp=atp))
            await asyncio.sleep(0.2)

        # Candle 1: combined VWAP high (atp 60 each → combined 120)
        await push(60.0)
        await bus.publish(Topic.CANDLE_CLOSE,
                          CandleEvent("NIFTY", 1, 23300, 23310, 23290, 23300, 0, now))
        await asyncio.sleep(0.25)

        # Candle 2 ticks: VWAP falls (atp 55 each → combined 110), but candle 2 has
        # NOT closed yet -- only one real candle exists. Slope must not appear yet;
        # a live-vs-one-prior-close blend (the old fallback) is exactly what caused
        # the real early-entry incident.
        await push(55.0)
        ind = s._pair_indicators(23300, 23300)
        s.stop()
        assert ind is not None
        assert ind["close"] == 100.0  # ltp is a constant 50.0/leg in this fixture; only atp moves
        assert ind["vwap"] == 110.0
        assert "slope" not in ind, (
            "slope must NOT be available from only one closed candle + live ticks "
            f"— got {ind}"
        )

    asyncio.run(run())


def test_pair_slope_present_and_correct_after_two_closed_candles():
    async def run():
        import datetime as _dt
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s.start()
        s._load_thresholds = lambda: None
        s._force_exit = _dt.time(23, 59)
        s._entry_start = _dt.time(0, 0)
        await asyncio.sleep(0.2)
        now = datetime.datetime.now(IST)
        exp = datetime.date.today()

        async def push(atp):
            await bus.publish(Topic.INDEX_TICK,
                              IndexTick("NIFTY", 23300, 23300, 23300, 23300, 23300, 0, now))
            for strike in (23250, 23300, 23350):
                for side in ("CE", "PE"):
                    await bus.publish(Topic.OPTION_TICK, OptionTick(
                        f"N{strike}{side}", "NIFTY", strike, side, exp,
                        50.0, 49.5, 50.5, 0, 0, 0, 0, 0, now, atp=atp))
            await asyncio.sleep(0.2)

        # Candle 1: combined VWAP 120 (atp 60 each)
        await push(60.0)
        await bus.publish(Topic.CANDLE_CLOSE,
                          CandleEvent("NIFTY", 1, 23300, 23310, 23290, 23300, 0, now))
        await asyncio.sleep(0.25)

        # Candle 2: combined VWAP 110 (atp 55 each) — and this time it actually closes.
        await push(55.0)
        await bus.publish(Topic.CANDLE_CLOSE,
                          CandleEvent("NIFTY", 1, 23300, 23310, 23290, 23300, 0, now))
        await asyncio.sleep(0.25)

        ind = s._pair_indicators(23300, 23300)
        s.stop()
        assert ind is not None
        assert "slope" in ind, "slope must be present once TWO candles have genuinely closed"
        assert ind["slope"] == -10.0, f"expected -10.0 (110-120), got {ind['slope']}"
        assert ind["slope"] < 0, "falling VWAP must yield negative slope so SLOPE<0 can pass"

    asyncio.run(run())
