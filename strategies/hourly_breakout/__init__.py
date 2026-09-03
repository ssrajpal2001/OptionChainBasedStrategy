"""
strategies/hourly_breakout — Hourly Breakout (1H trap + 5M retest) strategy.

Exports the public manager class used by ``strategies/registry.py``.
The pure strategy logic lives in ``strategy.py``; the live wrapper is in
``book_manager.py`` / ``book.py``.
"""
from __future__ import annotations

from strategies.hourly_breakout.book_manager import HourlyBreakoutBookManager

__all__ = ["HourlyBreakoutBookManager"]
