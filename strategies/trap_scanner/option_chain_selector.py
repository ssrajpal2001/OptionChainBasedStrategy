"""
strategies/trap_scanner/option_chain_selector.py — dynamic strike selection from live option chain.

Used by the trap scanner when dynamic_premium_entry is enabled: on a CE/PE signal,
query the broker's option-chain snapshot and pick the strike whose premium is
closest to (but not above) a target premium — e.g. ₹100.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


@dataclass
class SelectedStrike:
    strike: int
    ltp: float
    instrument_key: str


class OptionChainSelector:
    """Reads a live option-chain snapshot and selects a strike by premium."""

    def __init__(self, instrument_registry, rebalancer) -> None:
        self._registry = instrument_registry
        self._rebalancer = rebalancer

    async def select_strike_by_premium(
        self,
        underlying: str,
        expiry_date: date,
        opt_type: str,
        target_premium: float = 100.0,
        mode: str = "nearest_below",
    ) -> Optional[SelectedStrike]:
        """
        Fetch the option chain for `underlying` + `expiry_date` and return the
        strike on the requested side (CE/PE) whose LTP is ≤ `target_premium`.

        mode:
          - "nearest_below": premium closest to target from below (default).
          - "cheapest": lowest premium ≤ target.
        """
        if self._rebalancer is None:
            return None

        underlying_key = self._registry.get_upstox_index_key(underlying)
        if not underlying_key:
            logger.warning("OptionChainSelector: no Upstox index key for %s", underlying)
            return None

        try:
            chain = await self._rebalancer.fetch_option_chain(underlying_key, expiry_date)
        except Exception as exc:
            logger.warning("OptionChainSelector: fetch failed for %s: %s", underlying, exc)
            return None

        if not chain or not isinstance(chain, dict):
            return None
        rows = chain.get("data") or []
        if not rows:
            logger.info("OptionChainSelector: empty chain for %s %s", underlying, expiry_date)
            return None

        side_key = "call_options" if opt_type == "CE" else "put_options"
        candidates: list[tuple[int, float, str]] = []
        for row in rows:
            strike = float(row.get("strike_price") or 0)
            if strike <= 0:
                continue
            side = row.get(side_key) or {}
            market_data = side.get("market_data") or {}
            ltp = float(market_data.get("ltp") or 0)
            instrument_key = side.get("instrument_key") or ""
            if ltp <= 0:
                continue
            if ltp <= target_premium:
                candidates.append((int(strike), ltp, instrument_key))

        if not candidates:
            logger.info(
                "OptionChainSelector: no %s strike ≤ %.2f for %s (rows=%d)",
                opt_type, target_premium, underlying, len(rows)
            )
            return None

        if mode == "cheapest":
            selected = min(candidates, key=lambda x: x[1])
        else:  # nearest_below
            selected = max(candidates, key=lambda x: x[1])

        logger.info(
            "OptionChainSelector: selected %s strike=%d ltp=%.2f (target=%.2f, mode=%s)",
            opt_type, selected[0], selected[1], target_premium, mode
        )
        return SelectedStrike(strike=selected[0], ltp=selected[1], instrument_key=selected[2])
