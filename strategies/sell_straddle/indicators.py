"""
strategies/sell_straddle/indicators.py — indicator computation helpers.

All methods read from the book's buffers / pool engine and update ``self._ind``.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, date
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

    def _pool_warmth_diag(self, side_filter: str | None = None, candidate_count: int = 20) -> dict:
        """Return a compact diagnostic of how warm the pool engine and strike_prem are.

        Used on roll failure to answer: did we reject all candidates because the market
        moved too far, or because the pool lacks recent ticks/history for them?
        """
        now = time.time()
        side_filter = (side_filter or "").upper()
        strike_prem_keys = list(self._strike_prem.keys())
        keys = [(s, sd) for (s, sd) in strike_prem_keys if not side_filter or sd == side_filter]
        # Keep only the strikes closest to spot for a compact summary.
        spot = getattr(self, "_spot", 0.0) or 0.0
        if spot > 0:
            keys.sort(key=lambda k: abs(float(k[0]) - spot))
            keys = keys[:candidate_count]
        else:
            keys = keys[:candidate_count]
        out = {
            "spot": spot,
            "pool_total_legs": len(strike_prem_keys),
            "sampled_legs": len(keys),
            "legs": [],
        }
        for (strike, side) in keys:
            v = self._strike_prem.get((strike, side), {})
            k = self._pool_engine._key(strike, side)
            bars = len(self._pool_engine._closes.get(k, []))
            last_ts = self._pool_engine._last_atp_ts.get(k)
            stale_sec = round(now - last_ts, 1) if last_ts else None
            out["legs"].append({
                "strike": int(strike),
                "side": side,
                "ltp": float(v.get("ltp", 0.0) or 0.0),
                "atp": float(v.get("atp", 0.0) or 0.0),
                "bars": bars,
                "stale_sec": stale_sec,
            })
        return out

    def _load_chart_history(self) -> None:
        """Load today's 1-min chart history from disk so restarts don't wipe it."""
        try:
            _path = os.path.join("data", "chart_history", f"{self._persist_key}.json")
            if not os.path.exists(_path):
                return
            with open(_path) as f:
                _data = json.load(f)
            if _data.get("date") != date.today().isoformat():
                return
            _series = _data.get("series", [])
            for p in _series:
                self._chart_series.append(p)
            logger.info("IndicatorMixin[%s]: loaded %d chart history points", self._underlying, len(_series))
        except Exception as exc:
            logger.debug("IndicatorMixin[%s]: chart history load failed: %s", self._underlying, exc)

    def _save_chart_history(self) -> None:
        """Persist today's 1-min chart history to disk."""
        try:
            _dir = os.path.join("data", "chart_history")
            os.makedirs(_dir, exist_ok=True)
            _path = os.path.join(_dir, f"{self._persist_key}.json")
            _tmp = _path + ".tmp"
            with open(_tmp, "w") as f:
                json.dump({
                    "date": date.today().isoformat(),
                    "series": list(self._chart_series),
                }, f, default=str)
            os.replace(_tmp, _path)
        except Exception as exc:
            logger.debug("IndicatorMixin[%s]: chart history save failed: %s", self._underlying, exc)

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
        _combined = round(float(_ce_l + _pe_l), 2)
        _vwap = round(float(_ci.get("vwap", 0.0) or 0.0), 2)
        _rsi = round(float(_ci.get("rsi", 0.0) or 0.0), 2)
        _slope = round(float(_ci.get("slope", 0.0) or 0.0), 2)
        # Skip incomplete points (e.g. during rollover/startup when vwap or a leg is still 0)
        if _combined <= 0 or _ce_l <= 0 or _pe_l <= 0 or _vwap <= 0:
            return
        self._chart_series.append({
            "ts": ts.timestamp(),
            "combined": _combined,
            "ce_ltp": round(float(_ce_l), 2),
            "pe_ltp": round(float(_pe_l), 2),
            "vwap": _vwap,
            "rsi": _rsi,
            "slope": _slope,
        })
        self._save_chart_history()
