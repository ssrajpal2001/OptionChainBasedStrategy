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
        self._mins:   Dict[Key, deque] = {}

    def _key(self, strike: int, side: str) -> Key:
        return (int(strike), side.upper())

    def update_tick(self, strike: int, side: str, ltp: float, atp: float) -> None:
        k = self._key(strike, side)
        # Keep the last GOOD value when a tick reports 0 (no-trade ltp or a momentary missing
        # atp from the feed). Without this, a single atp=0 makes pair_indicators() return None
        # for the active pair, the strategy falls back to the legacy active-series path, and on a
        # re-entry that path produces a garbage SLOPE (e.g. -258) from a stale prev-pair VWAP.
        _pl, _pa = self._latest.get(k, (0.0, 0.0))
        _l = float(ltp) if ltp and ltp > 0 else _pl
        _a = float(atp) if atp and atp > 0 else _pa
        self._latest[k] = (_l, _a)

    def commit_bar(self, minute: int = None) -> None:
        # Forward-fill EVERY tracked key once per minute so all per-strike series stay
        # minute-aligned (same deque index == same minute). A quiet leg holds its last
        # ltp/atp; without this, CE and PE deques would drift in length and combined
        # close/vwap/slope/rsi would sum bars from different minutes.
        # `minute` is a monotonically increasing minute-of-day index used for clock-aligned
        # tf resampling. If None, auto-increment from each key's last minute (legacy callers).
        for k, (ltp, atp) in self._latest.items():
            self._closes.setdefault(k, deque(maxlen=self._maxlen)).append(ltp)
            self._atps.setdefault(k, deque(maxlen=self._maxlen)).append(atp)
            md = self._mins.setdefault(k, deque(maxlen=self._maxlen))
            m = minute if minute is not None else ((md[-1] + 1) if md else 0)
            md.append(int(m))

    def seed_strike(self, strike: int, side: str, closes: list, atps: list) -> None:
        """Prefill the rolling series from historical bars (oldest-first) so RSI/ROC are valid
        immediately. VWAP/ATP are intraday-fresh so seeding atps only keeps lengths aligned."""
        k = self._key(strike, side)
        cd = self._closes.setdefault(k, deque(maxlen=self._maxlen))
        ad = self._atps.setdefault(k, deque(maxlen=self._maxlen))
        md = self._mins.setdefault(k, deque(maxlen=self._maxlen))
        # Negative, increasing minute indices so seeded bars precede live bars (which start at 0)
        n = len(closes)
        start = -n
        for i, (c, a) in enumerate(zip(closes, atps)):
            cd.append(float(c)); ad.append(float(a)); md.append(start + i)

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

    def _tf_groups(self, key: Key, tf: int):
        """Resample a leg's 1-min (minute, close, atp) to tf-minute candles.
        Returns dict group_id -> (close, atp) using the LAST 1-min bar per group, with the
        current in-progress (max) group dropped."""
        cc, aa, mm = self._closes.get(key), self._atps.get(key), self._mins.get(key)
        if not cc or not aa or not mm:
            return {}
        groups: Dict[int, Tuple[float, float]] = {}
        for c, a, m in zip(cc, aa, mm):
            groups[m // tf] = (c, a)  # last bar per group wins (iteration is ascending)
        if not groups:
            return {}
        m_max = max(mm)  # latest committed minute index
        # A tf-group is complete once its final minute slot ((g+1)*tf-1) has been committed.
        # (For consecutive minutes this equals "drop the in-progress max group"; but right after
        # a boundary the just-closed group's last minute IS committed, so it is correctly KEPT
        # instead of going one window stale.)
        return {g: v for g, v in groups.items() if (g + 1) * tf - 1 <= m_max}

    def pair_indicators_tf(self, ce_strike, pe_strike, tf: int) -> Optional[Dict[str, float]]:
        """Combined indicators for (ce,pe) resampled to `tf`-minute candles.
        Resample the 1-min (close, atp, minute) series to tf by grouping on (minute // tf);
        a group's value = its LAST 1-min bar (the tf candle's close / its atp). Use only COMPLETE
        tf groups (drop the current in-progress group). Combined per tf bar: close=ce_close+pe_close,
        vwap=ce_atp+pe_atp. Returns {close,vwap[,slope,rsi,roc]} or None if no data. tf<=1 delegates
        to the existing 1-min pair_indicators."""
        if tf is None or tf <= 1:
            return self.pair_indicators(ce_strike, pe_strike)
        ce, pe = self._key(ce_strike, "CE"), self._key(pe_strike, "PE")
        cg = self._tf_groups(ce, tf)
        pg = self._tf_groups(pe, tf)
        common = sorted(set(cg) & set(pg))
        if not common:
            return None
        closes = [cg[g][0] + pg[g][0] for g in common]
        vwaps  = [cg[g][1] + pg[g][1] for g in common]
        close, vwap = closes[-1], vwaps[-1]
        if close <= 0 or vwap <= 0:
            return None
        ind: Dict[str, float] = {"close": close, "vwap": vwap}
        if len(vwaps) >= 2:
            ind["slope"] = vwaps[-1] - vwaps[-2]
        n = len(closes)
        if n >= self._rsi_len + 1:
            ind["rsi"] = float(_rsi(np.array(closes, dtype=np.float64)))
        if n >= self._roc_len + 1 and closes[-self._roc_len - 1] != 0:
            ref = closes[-self._roc_len - 1]
            ind["roc"] = float((closes[-1] - ref) / ref * 100.0)
        return ind
