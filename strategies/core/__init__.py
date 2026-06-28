"""
strategies/core — reusable building blocks for per-binding strategy books.
"""
from __future__ import annotations

from strategies.core.base_book import AbstractStrategyBook
from strategies.core.book_manager import StrategyBookManager
from strategies.core.gate import can_trade
from strategies.core.order_emitter import OrderEmitter
from strategies.core.position import PositionStoreMixin
from strategies.core.rule_evaluator import RuleEvaluator, eval_rules

__all__ = [
    "AbstractStrategyBook",
    "StrategyBookManager",
    "can_trade",
    "OrderEmitter",
    "PositionStoreMixin",
    "RuleEvaluator",
    "eval_rules",
]
