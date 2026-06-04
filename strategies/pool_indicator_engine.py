"""Continuous per-strike indicator engine for the sell-straddle. Maintains a rolling 1-min
(ltp, atp) series per (strike, side) so any pair's combined VWAP/SLOPE/RSI/ROC can be computed
on demand — independent of the active position. Pure + unit-testable; the strategy feeds it
ticks/bars and (later) seeds prev-day history."""
from __future__ import annotations

from collections import deque
from typing import Dict, Optional, Tuple

import numpy as np

from matrix_engine.indicators import rsi as _rsi

Key = Tuple[int, str]


class PoolIndicatorEngine:
    def __init__(self, rsi_len: int = 14, roc_len: int = 10, maxlen: int = 240) -> None:
        self._rsi_len = rsi_len
        self._roc_len = roc_len
        self._maxlen = maxlen
        self._latest: Dict[Key, Tuple[float, float]] = {}
        self._closes: Dict[Key, deque] = {}
        self._atps:   Dict[Key, deque] = {}

    def _key(self, strike: int, side: str) -> Key:
        return (int(strike), side.upper())

    def update_tick(self, strike: int, side: str, ltp: float, atp: float) -> None:
        k = self._key(strike, side)
        self._latest[k] = (float(ltp), float(atp))

    def commit_bar(self) -> None:
        # Forward-fill EVERY tracked key once per minute so all per-strike series stay
        # minute-aligned (same deque index == same minute). A quiet leg holds its last
        # ltp/atp; without this, CE and PE deques would drift in length and combined
        # close/vwap/slope/rsi would sum bars from different minutes.
        for k, (ltp, atp) in self._latest.items():
            self._closes.setdefault(k, deque(maxlen=self._maxlen)).append(ltp)
            self._atps.setdefault(k, deque(maxlen=self._maxlen)).append(atp)

    def is_warm(self, strike: int, side: str) -> bool:
        k = self._key(strike, side)
        return len(self._closes.get(k, ())) >= max(self._rsi_len + 1, self._roc_len + 1)

    def pair_indicators(self, ce_strike: int, pe_strike: int) -> Optional[Dict[str, float]]:
        ce, pe = self._key(ce_strike, "CE"), self._key(pe_strike, "PE")
        if ce not in self._latest or pe not in self._latest:
            return None
        ce_ltp, ce_atp = self._latest[ce]
        pe_ltp, pe_atp = self._latest[pe]
        if min(ce_ltp, pe_ltp, ce_atp, pe_atp) <= 0:
            return None
        ind: Dict[str, float] = {"close": ce_ltp + pe_ltp, "vwap": ce_atp + pe_atp}
        ca, pa = self._atps.get(ce), self._atps.get(pe)
        if ca and pa and len(ca) >= 2 and len(pa) >= 2:
            ind["slope"] = (ca[-1] + pa[-1]) - (ca[-2] + pa[-2])
        cc, pc = self._closes.get(ce), self._closes.get(pe)
        if cc and pc:
            n = min(len(cc), len(pc))
            combined = np.array([cc[-n + i] + pc[-n + i] for i in range(n)], dtype=np.float64)
            if n >= self._rsi_len + 1:
                ind["rsi"] = float(_rsi(combined))
            if n >= self._roc_len + 1 and combined[-self._roc_len - 1] != 0:
                ref = combined[-self._roc_len - 1]
                ind["roc"] = float((combined[-1] - ref) / ref * 100.0)
        return ind
