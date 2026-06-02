import asyncio
import datetime
from data_layer.base_feeder import EventBus
from config.global_config import IST, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def test_single_side_roll_no_candidate_closes_both():
    async def run():
        s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23500, 80.0, 10.0),
            pe_leg=StraddleLeg("PE", 23500, 80.0, 70.0),
            net_credit=160.0, status="open",
        )
        s._spot = 23500
        s._strike_prem = {}   # empty → scan_pool finds nothing
        await s._single_side_roll("CE", datetime.datetime.now(IST), "ltp_decay_CE")
        assert s._position is None   # 0-or-2 invariant: both legs closed
    asyncio.run(run())
