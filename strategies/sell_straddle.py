"""
strategies/sell_straddle.py — backward-compatibility shim.

The implementation has moved into the ``strategies.sell_straddle`` sub-package.
This module re-exports the public API so existing imports keep working.
"""
from __future__ import annotations

from strategies.sell_straddle import (
    SellStraddleStrategy,
    StraddleLeg,
    StraddlePosition,
    format_exit_eval,
)
from strategies.sell_straddle.engine import pool_strike_set
from strategies.core.rule_evaluator import eval_rules as _eval_rules

# Backward-compatible alias used by tests and internal helpers.
_eval_rules = _eval_rules

__all__ = [
    "SellStraddleStrategy",
    "StraddleLeg",
    "StraddlePosition",
    "format_exit_eval",
    "pool_strike_set",
    "_eval_rules",
]
