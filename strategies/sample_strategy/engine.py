"""
strategies/sample_strategy/engine.py — minimal per-binding strategy book skeleton.

Inherits the standard lifecycle from ``AbstractStrategyBook``. Override the feed
loops and ``reset_session()`` to implement real logic. This file imports cleanly
and is safe to leave unregistered.
"""
from __future__ import annotations

import logging

from strategies.core.base_book import AbstractStrategyBook

logger = logging.getLogger(__name__)


class SampleStrategy(AbstractStrategyBook):
    """No-op example strategy book — one instance per (client, binding, underlying)."""

    def reset_session(self) -> None:
        """Clear intraday/session state. Required by ``AbstractStrategyBook``."""
        logger.debug("SampleStrategy[%s/%s/%s]: reset_session",
                     self._client_id, self._binding_id, self._underlying)
