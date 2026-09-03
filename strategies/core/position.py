"""
strategies/core/position.py — reusable position-store helper.

Wraps data_layer.position_store with a small mixin so strategy books can
persist/load/clear positions. MIS-day-discard semantics are handled by the
underlying store.
"""
from __future__ import annotations

from data_layer import position_store as _position_store


class PositionStoreMixin:
    """Mixin providing persist/load/clear helpers backed by data_layer.position_store."""

    def persist(self, key: str, data: dict, product_type: str = "MIS") -> bool:
        """Persist ``data`` under ``key``. Default product type is MIS so
        intraday positions are auto-discarded on a new trading day. Returns
        True/False so callers can detect and react to a write failure
        instead of it being silently swallowed (2026-08-06)."""
        return _position_store.save(key, data, product_type=product_type)

    def load(self, key: str) -> dict | None:
        """Load previously persisted data for ``key``, or None."""
        return _position_store.load(key)

    def clear(self, key: str) -> bool:
        """Remove persisted data for ``key``. Returns True/False (2026-08-06)."""
        return _position_store.clear(key)
