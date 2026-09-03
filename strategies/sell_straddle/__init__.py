"""
strategies/sell_straddle — ATM Straddle selling strategy sub-package.

Re-exports the public API for backward compatibility with:
    from strategies.sell_straddle import (
        SellStraddleStrategy, StraddlePosition, StraddleLeg,
        format_exit_eval, pool_strike_set, _eval_rules,
    )
"""
from __future__ import annotations

from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition, format_exit_eval
from strategies.sell_straddle.engine import SellStraddleStrategy, pool_strike_set
from strategies.straddle_book_manager import StraddleBookManager
from strategies.core.rule_evaluator import eval_rules as _eval_rules

# Backward-compatible alias used by tests and internal helpers.
_eval_rules = _eval_rules

__all__ = [
    "SellStraddleStrategy",
    "StraddleBookManager",
    "StraddleLeg",
    "StraddlePosition",
    "format_exit_eval",
    "pool_strike_set",
    "_eval_rules",
]
