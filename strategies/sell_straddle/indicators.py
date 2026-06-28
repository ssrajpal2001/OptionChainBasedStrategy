"""
strategies/sell_straddle/indicators.py — indicator computation helpers.

All methods read from the book's buffers / pool engine and update ``self._ind``.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Optional, Tuple

import numpy as np

from config.global_config import IST
from matrix_engine.indicators import adx, ema, rsi

logger = logging.getLogger(__name__)


class IndicatorMixin:
    """Indicator-series maintenance for the sell-straddle book."""

    def _active_premium(self) -> Tuple[float, float, float, float]:
        """(ce_ltp, pe_ltp, ce_atp, pe_atp) for the indicator series."""
        if self._position and self._position.status == "open":
            pos = self._position
            _ce = self._strike_prem.get((int(pos.ce_leg.strike), "CE"), {})
            _pe = self._strike_prem.get((int(pos.pe_leg.strike), "PE"), {})
            return (float(_ce.get("ltp", 0.0) or 0.0), float(_pe.get("ltp", 0.0) or 0.0),
                    float(_ce.get("atp", 0.0) or 0.0), float(_pe.get("atp", 0.0) or 0.0))
        return (self._ce_ltp, self._pe_ltp, self._ce_atp, self._pe_atp)

    def _recompute_indicators(self) -> None:
        closes = np.array(self._prem_closes, dtype=np.float64)
        vols = np.array(self._prem_volumes, dtype=np.float64)
        idx_h = np.array(self._idx_highs, dtype=np.float64)
        idx_l = np.array(self._idx_lows, dtype=np.float64)
        idx_c = np.array(self._idx_closes, dtype=np.float64)
        _ce_ltp, _pe_ltp, _ce_atp, _pe_atp = self._active_premium()
        ltp = _ce_ltp + _pe_ltp
        self._ind["ltp"] = ltp
        self._ind["close"] = ltp
        if self._position and self._position.status == "open":
            _pe = self._pool_engine.pair_indicators(
                int(self._position.ce_leg.strike), int(self._position.pe_leg.strike))
            if _pe:
                for _k in ("rsi", "roc", "slope", "vwap", "vwap_prev", "close"):
                    if _k in _pe:
                        self._ind[_k] = _pe[_k]
                self._ind["ltp"] = ltp
                import time as _t
                if _t.monotonic() - getattr(self, "_ind_src_log", 0.0) > 60.0:
                    self._ind_src_log = _t.monotonic()
                    _ce_d = self._position.ce_leg.symbol or f"CE{int(self._position.ce_leg.strike)}"
                    _pe_d = self._position.pe_leg.symbol or f"PE{int(self._position.pe_leg.strike)}"
                    logger.info(
                        "SellStraddle[%s]: INDICATORS src=WARM-POOL-ENGINE %s/%s | "
                        "close=%.2f vwap=%.2f (prev=%.2f) slope=%.2f rsi=%.1f roc=%.2f",
                        self._underlying, _ce_d, _pe_d, _pe.get("close", 0.0),
                        _pe.get("vwap", 0.0), _pe.get("vwap_prev", 0.0), _pe.get("slope", 0.0),
                        _pe.get("rsi", 0.0), _pe.get("roc", 0.0))
                return
            else:
                import time as _t
                if _t.monotonic() - getattr(self, "_ind_src_log", 0.0) > 60.0:
                    self._ind_src_log = _t.monotonic()
                    logger.info("SellStraddle[%s]: INDICATORS src=FALLBACK-ACTIVE-SERIES "
                                "(pool engine not warm yet for CE%d/PE%d)", self._underlying,
                                int(self._position.ce_leg.strike), int(self._position.pe_leg.strike))
        if len(closes) >= 15:
            self._ind["rsi"] = rsi(closes)
        if len(closes) >= 9:
            self._ind["ema_fast"] = ema(closes, 9)
        if len(closes) >= 21:
            self._ind["ema_slow"] = ema(closes, 21)
        if len(idx_c) >= 42:
            adx_val, pdi_val, mdi_val = adx(idx_h, idx_l, idx_c)
            self._ind["adx"] = adx_val
            self._ind["pdi"] = pdi_val
            self._ind["mdi"] = mdi_val
        _cur_vwap = None
        if _ce_atp > 0 and _pe_atp > 0:
            _cur_vwap = float(_ce_atp + _pe_atp)
            self._ind["vwap"] = _cur_vwap
            _prev = self._prev_vwap_atp
            if _prev is not None and _prev > 0:
                _slope = float(_cur_vwap - _prev)
                self._ind["slope"] = _slope
                self._ind["vwap_slope"] = _slope
                self._ind["slope_curr"] = _cur_vwap
                self._ind["slope_prev"] = _prev
            self._prev_vwap_atp = _cur_vwap
        if len(closes) >= 10:
            _ref = closes[-10]
            if _ref != 0:
                self._ind["roc"] = float((closes[-1] - _ref) / _ref * 100.0)

    def _pair_indicators(self, ce_strike: int, pe_strike: int) -> Optional[Dict[str, float]]:
        """Per-pair {close, vwap, slope, rsi, roc}."""
        ind = self._pool_engine.pair_indicators(int(ce_strike), int(pe_strike))
        if ind is not None and "rsi" in ind:
            return ind
        from strategies.sell_straddle.selection import pair_indicators
        return pair_indicators(self._strike_prem, self._prev_atp_closed, ce_strike, pe_strike)

    def _ind_by_tf(self, ce_strike: int, pe_strike: int, *rule_lists) -> dict:
        """Map each tf used by the given rule list(s) -> that pair's indicators resampled to that tf."""
        tfs = {1}
        for rl in rule_lists:
            for r in (rl or []):
                try:
                    tfs.add(int(r.get("tf", 1)))
                except Exception:
                    tfs.add(1)
        out = {}
        for tf in tfs:
            if tf <= 1:
                out[tf] = self._pair_indicators(int(ce_strike), int(pe_strike)) or {}
            else:
                out[tf] = self._pool_engine.pair_indicators_tf(int(ce_strike), int(pe_strike), tf) or {}
        return out

    def _append_chart_point(self, ts: datetime) -> None:
        """Append one chart point for the given minute."""
        _m = ts.hour * 60 + ts.minute
        if getattr(self, "_chart_last_min", None) == _m:
            return
        self._chart_last_min = _m
        _ce_l, _pe_l, _, _ = self._active_premium()
        _ci = self._ind
        if self._position and self._position.status == "open":
            _pi = self._pool_engine.pair_indicators_tf(
                int(self._position.ce_leg.strike), int(self._position.pe_leg.strike), 1) or {}
            if _pi:
                _ci = {**self._ind, **_pi}
        self._chart_series.append({
            "ts": ts.timestamp(),
            "combined": round(float(_ce_l + _pe_l), 2),
            "ce_ltp": round(float(_ce_l), 2),
            "pe_ltp": round(float(_pe_l), 2),
            "vwap": round(float(_ci.get("vwap", 0.0) or 0.0), 2),
            "rsi": round(float(_ci.get("rsi", 0.0) or 0.0), 2),
            "slope": round(float(_ci.get("slope", 0.0) or 0.0), 2),
        })
