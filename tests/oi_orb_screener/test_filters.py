"""
Unit tests for strategies/oi_orb_screener/filters.py -- the five additive,
independently-toggleable signal filters (OI-wall, distance-to-wall, PCR,
volume confirmation, OI rate-of-change). Hand-built inputs only, mirrors
the existing test_screener.py discipline for this strategy.
"""
from dataclasses import dataclass, field
from typing import Dict

import pytest

from strategies.oi_orb_screener import filters


@dataclass
class _FakeSnap:
    """Minimal stand-in for matrix_engine.option_matrix.ChainSnapshot,
    exposing only what filters.py actually reads."""
    call_oi: Dict[float, int] = field(default_factory=dict)
    put_oi: Dict[float, int] = field(default_factory=dict)

    @property
    def rows(self):
        return {**{s: None for s in self.call_oi}, **{s: None for s in self.put_oi}}

    def strikes(self):
        return sorted(set(self.call_oi) | set(self.put_oi))

    def call_oi_at(self, strike):
        return self.call_oi.get(strike, 0)

    def put_oi_at(self, strike):
        return self.put_oi.get(strike, 0)


# ── evaluate_oi_wall ──────────────────────────────────────────────────────

def test_oi_wall_unavailable_when_no_chain():
    v = filters.evaluate_oi_wall(None, "CALL", 1014)
    assert v.available is False
    assert v.blocks(enabled=True) is False   # never blocks on missing data


def test_oi_wall_detects_dominant_call_wall_above_entry():
    snap = _FakeSnap(call_oi={1000: 1000, 1020: 50000, 1040: 1200})
    v = filters.evaluate_oi_wall(snap, "CALL", entry_strike=1014, dominance_ratio=1.5)
    assert v.available is True
    assert v.passed is False
    assert v.numbers["wall_strike"] == 1020
    assert v.blocks(enabled=True) is True
    assert v.blocks(enabled=False) is False   # disabled filter never blocks regardless of verdict


def test_oi_wall_no_wall_when_oi_evenly_spread():
    snap = _FakeSnap(call_oi={1020: 1000, 1040: 1100, 1060: 950})
    v = filters.evaluate_oi_wall(snap, "CALL", entry_strike=1014, dominance_ratio=1.5)
    assert v.available is True
    assert v.passed is True


def test_oi_wall_put_side_looks_below_entry():
    snap = _FakeSnap(put_oi={980: 40000, 960: 500}, call_oi={1020: 500})
    v = filters.evaluate_oi_wall(snap, "PUT", entry_strike=1000, dominance_ratio=1.5)
    assert v.available is True
    assert v.passed is False
    assert v.numbers["wall_strike"] == 980


# ── evaluate_distance_to_wall ─────────────────────────────────────────────

def test_distance_to_wall_blocks_when_wall_too_close():
    snap = _FakeSnap(call_oi={1020: 50000})
    v = filters.evaluate_distance_to_wall(snap, "CALL", entry_strike=1014, min_distance_pct=1.5)
    # (1020-1014)/1014 * 100 = 0.59% < 1.5% required
    assert v.available is True
    assert v.passed is False
    assert v.blocks(enabled=True) is True


def test_distance_to_wall_passes_when_wall_far_enough():
    snap = _FakeSnap(call_oi={1100: 50000})
    v = filters.evaluate_distance_to_wall(snap, "CALL", entry_strike=1014, min_distance_pct=1.5)
    # (1100-1014)/1014 * 100 = 8.48% >= 1.5%
    assert v.passed is True


# ── evaluate_pcr ──────────────────────────────────────────────────────────

def test_pcr_unavailable_when_none():
    v = filters.evaluate_pcr(None, "CALL")
    assert v.available is False


def test_pcr_blocks_call_when_put_heavy():
    v = filters.evaluate_pcr(pcr=1.5, side="CALL", max_pcr_for_call=1.2)
    assert v.available is True
    assert v.passed is False


def test_pcr_passes_call_when_within_band():
    v = filters.evaluate_pcr(pcr=0.9, side="CALL", max_pcr_for_call=1.2)
    assert v.passed is True


def test_pcr_blocks_put_when_call_heavy():
    v = filters.evaluate_pcr(pcr=0.5, side="PUT", min_pcr_for_put=0.8)
    assert v.passed is False


# ── evaluate_volume_confirmation ──────────────────────────────────────────

def test_volume_confirmation_unavailable_without_history():
    v = filters.evaluate_volume_confirmation(None, None)
    assert v.available is False


def test_volume_confirmation_passes_on_strong_volume():
    v = filters.evaluate_volume_confirmation(recent_volume=300000, trailing_avg_volume=100000, min_ratio=1.5)
    assert v.available is True
    assert v.passed is True


def test_volume_confirmation_blocks_on_thin_volume():
    v = filters.evaluate_volume_confirmation(recent_volume=80000, trailing_avg_volume=100000, min_ratio=1.5)
    assert v.passed is False


# ── evaluate_oi_roc ────────────────────────────────────────────────────────

def test_oi_roc_unavailable_with_insufficient_history():
    v = filters.evaluate_oi_roc([(100.0, 5000.0)])
    assert v.available is False


def test_oi_roc_passes_on_fresh_buildup():
    hist = [(0.0, 10000.0), (150.0, 10300.0), (300.0, 10600.0)]
    v = filters.evaluate_oi_roc(hist, min_roc_pct=3.0, lookback_sec=300.0, now_ts=300.0)
    # (10600-10000)/10000 * 100 = 6.0% >= 3.0%
    assert v.available is True
    assert v.passed is True


def test_oi_roc_blocks_on_stale_slow_buildup():
    hist = [(0.0, 10000.0), (150.0, 10050.0), (300.0, 10100.0)]
    v = filters.evaluate_oi_roc(hist, min_roc_pct=3.0, lookback_sec=300.0, now_ts=300.0)
    # 1.0% < 3.0%
    assert v.passed is False


def test_oi_roc_only_considers_lookback_window():
    hist = [(-10000.0, 1.0), (0.0, 10000.0), (300.0, 10600.0)]
    v = filters.evaluate_oi_roc(hist, min_roc_pct=3.0, lookback_sec=300.0, now_ts=300.0)
    # the ancient (-10000, 1.0) sample must be excluded by the lookback window,
    # not treated as the "start" (which would give an absurd ROC%)
    assert v.numbers["start_oi"] == 10000.0
