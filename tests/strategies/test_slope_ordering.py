"""Regression: per-pair VWAP slope must compare the current candle's ATP to the
PREVIOUS candle's ATP — not to itself. The prev-ATP snapshot must run AFTER entry
evaluation in _on_candle, else slope is always 0.00 and SLOPE<0 never passes
(live incident 2026-06-02: no straddle entries fired all session)."""
import asyncio
import datetime

from data_layer.base_feeder import EventBus, IndexTick, OptionTick, CandleEvent
from config.global_config import IST, Topic, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy


def test_pair_slope_is_inter_candle_not_zero():
    async def run():
        import datetime as _dt
        bus = EventBus()
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s.start()
        # Make the test time-independent: _on_candle force-exits (and skips the
        # prev-ATP snapshot) once now >= squareoff. Pin squareoff to end-of-day and
        # stop _load_thresholds from resetting it each candle.
        s._load_thresholds = lambda: None
        s._force_exit = _dt.time(23, 59)
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

        # Candle 2 ticks: VWAP falls (atp 55 each → combined 110). Inspect what the
        # entry evaluation sees — BEFORE the next candle close re-snapshots prev.
        await push(55.0)
        ind = s._pair_indicators(23300, 23300)
        s.stop()
        assert ind is not None
        assert "slope" in ind, "slope must be present once a prior candle exists"
        assert ind["slope"] == -10.0, f"expected -10.0 (110-120), got {ind['slope']}"
        assert ind["slope"] < 0, "falling VWAP must yield negative slope so SLOPE<0 can pass"

    asyncio.run(run())
