"""
strategies/straddle_selection.py — backward-compatibility shim.

The implementation has moved to ``strategies.sell_straddle.selection``.
This module re-exports the public API so existing imports keep working.
"""
from __future__ import annotations

from strategies.sell_straddle.selection import (
    classify_roll,
    leg_entry_value,
    pair_indicators,
    reentry_block_reason,
    scan_pool,
    select_balanced_pair,
    select_partner_for,
    strip_intrinsic,
)

__all__ = [
    "classify_roll",
    "leg_entry_value",
    "pair_indicators",
    "reentry_block_reason",
    "scan_pool",
    "select_balanced_pair",
    "select_partner_for",
    "strip_intrinsic",
]
