"""
strategies/fno_stock_monitor.py — FnO Stock Stage-2 Intraday Monitor.

Watches nightly-scan shortlisted stocks during market hours. Builds 15m (MTF)
and 5m (LTF) spot candle bars from live INDEX_TICK events. When MTF shows a
new TRAPPED zone matching the D1 direction, arms LTF. When LTF also shows a
TRAPPED zone, fires Topic.FNO_STOCK_ALERT with full trade details.

No auto-trade. Alert-only.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import math
import os
from datetime import date, datetime, time as dtime
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus, IndexTick

logger = logging.getLogger(__name__)

MARKET_OPEN  = dtime(9, 15, 0)
MARKET_CLOSE = dtime(15, 15, 0)
EOD_CLEAR    = dtime(15, 30, 0)


@dataclasses.dataclass
class FnoStockAlert:
    uid: str              # dedup key: f"{symbol}_{direction}_{zone_high:.0f}"
    symbol: str
    direction: str        # "CE" | "PE"
    spot_price: float
    d1_zone_low: float
    d1_zone_high: float
    d1_zone_date: str     # "Jun 30"
    strike: int
    lot_size: int
    sl: float
    t1: float
    risk_pts: float
    reward_pts: float
    rr_ratio: float
    mtf_trap_price: float
    ltf_trap_price: float
    fired_at: datetime
