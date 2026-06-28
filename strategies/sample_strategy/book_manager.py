"""
strategies/sample_strategy/book_manager.py — minimal per-binding book manager.

Subclasses ``StrategyBookManager`` and reconciles wanted deployments. This is a
skeleton: ``_wanted()`` returns nothing, so no books are spawned until you wire a
real DB query.
"""
from __future__ import annotations

from typing import Any, Dict, Tuple

from strategies.core.book_manager import StrategyBookManager
from strategies.sample_strategy.engine import SampleStrategy

Key = Tuple[str, str, str]


class SampleBookManager(StrategyBookManager):
    """Example manager that would spawn one ``SampleStrategy`` per deployment."""

    def _wanted(self) -> Dict[Key, Any]:
        """Return wanted (client, binding, underlying) → config mapping.

        Hook this to a DB query (see ``StraddleBookManager._wanted``) to auto-spawn
        books when a deployment is running.
        """
        return {}

    def _spawn_book(self, key: Key, value: Any) -> SampleStrategy:
        client_id, binding_id, underlying = key
        return SampleStrategy(
            self._bus, self._cfg,
            underlying=underlying,
            client_id=client_id,
            binding_id=binding_id,
        )
