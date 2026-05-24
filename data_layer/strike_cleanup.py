"""
data_layer/strike_cleanup.py — Post-exit stream garbage collector.

Background task that eliminates network overhead from inactive option
data streams once a client position is fully closed.

Lifecycle:
  1. On ORDER_FILL (BUY side):   record strike as an open position.
  2. On ORDER_FILL (SELL side):  mark the position closed; trigger cleanup.
  3. On SYSTEM_EVENT POSITION_CLOSED: explicit close notification from the
     backtester or position manager (for systems that don't use SELL fills).
  4. notify_position_closed(underlying, strike, option_type):
     Public API for callers that manage position tracking externally.

Cleanup decision (per closed strike):
  open_count[underlying][strike] == 0           # No remaining positions
  AND strike NOT IN rebalancer.active_strikes() # Outside current ATM window

If both conditions hold, the strike is unsubscribed via:
  await feeder.unsubscribe_tokens([CE_token, PE_token])
  rebalancer.unpin_strike(underlying, strike)

If the strike is still inside the ATM window it remains subscribed —
the overhead is minimal and it may be needed for the next signal.

No time.sleep. All async.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, Optional, Set, Tuple

from config.global_config import IST, Topic, SysEvent, GlobalConfig
from data_layer.base_feeder import EventBus, SystemEvent, BaseFeeder
from execution_bridge.base_broker import OrderFill, OrderSide, OrderStatus

logger = logging.getLogger(__name__)


class StrikeCleanup:
    """
    Post-exit stream garbage collector.

    Owned by the main application and passed references to the StrikeRebalancer
    and BaseFeeder so it can coordinate unsubscription after position close.
    """

    def __init__(
        self,
        bus: EventBus,
        cfg: GlobalConfig,
        feeder: BaseFeeder,
        rebalancer,           # StrikeRebalancer — typed loosely to avoid circular import
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._feeder = feeder
        self._rebalancer = rebalancer
        self._fill_queue = bus.subscribe(Topic.ORDER_FILL)
        self._sys_queue  = bus.subscribe(Topic.SYSTEM_EVENT)
        self._running = False
        # open_counts[(underlying, strike)] = number of open positions on that strike
        # Incremented on BUY fill, decremented on SELL fill / explicit close
        self._open_counts: Dict[Tuple[str, float], int] = defaultdict(int)
        # Cleanup metrics
        self._cleanups_performed = 0
        self._cleanups_skipped_in_window = 0

    # ── Public API ────────────────────────────────────────────────────────────

    def notify_position_opened(self, underlying: str, strike: float) -> None:
        """
        Increment open-position counter for this strike.
        Called on any new position open.  Also called automatically from
        ORDER_FILL events (BUY side).
        """
        self._open_counts[(underlying, strike)] += 1
        logger.debug(
            "StrikeCleanup: [%s] position opened on strike %.0f (open_count=%d).",
            underlying, strike, self._open_counts[(underlying, strike)],
        )

    def notify_position_closed(
        self, underlying: str, strike: float, option_type: str = ""
    ) -> None:
        """
        Synchronously register a position close.  The actual unsubscription
        is deferred to the async _cleanup() coroutine which the run() loop
        will schedule asynchronously.

        Call this from any sync context (e.g. backtester, position manager).
        The cleanup request is queued and processed without blocking.
        """
        key = (underlying, strike)
        if self._open_counts[key] > 0:
            self._open_counts[key] -= 1
        # Schedule cleanup via the event loop (non-blocking)
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(
                    self._cleanup(underlying, strike),
                    name=f"cleanup_{underlying}_{strike:.0f}",
                )
        except RuntimeError:
            pass  # No event loop running (e.g. during testing)

    def open_position_count(self, underlying: str, strike: float) -> int:
        return self._open_counts.get((underlying, strike), 0)

    def cleanup_stats(self) -> dict:
        return {
            "cleanups_performed": self._cleanups_performed,
            "cleanups_skipped_in_window": self._cleanups_skipped_in_window,
            "open_positions": {
                f"{u}:{s:.0f}": c
                for (u, s), c in self._open_counts.items()
                if c > 0
            },
        }

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("StrikeCleanup: started.")
        await asyncio.gather(
            self._fill_loop(),
            self._sys_loop(),
        )

    def stop(self) -> None:
        self._running = False

    # ── Event loops ───────────────────────────────────────────────────────────

    async def _fill_loop(self) -> None:
        """
        Consume ORDER_FILL events.

        BUY fills → increment open count (position opened).
        SELL fills or CANCELLED/REJECTED status → decrement and trigger cleanup.
        """
        while self._running:
            try:
                fill: OrderFill = await asyncio.wait_for(
                    self._fill_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            try:
                underlying = getattr(fill, "underlying", None) or \
                             _extract_underlying(fill.broker_symbol)
                strike = getattr(fill, "strike", None) or \
                         _extract_strike(fill.broker_symbol)

                if not underlying or not strike:
                    continue

                strike = float(strike)

                if fill.side == OrderSide.BUY:
                    # New position opened — register it
                    self._open_counts[(underlying, strike)] += 1
                    logger.debug(
                        "StrikeCleanup: [%s] BUY fill — strike %.0f open_count=%d.",
                        underlying, strike,
                        self._open_counts[(underlying, strike)],
                    )

                elif fill.side == OrderSide.SELL or fill.status in (
                    OrderStatus.CANCELLED, OrderStatus.REJECTED
                ):
                    # Position closed — decrement and check for cleanup
                    key = (underlying, strike)
                    if self._open_counts[key] > 0:
                        self._open_counts[key] -= 1
                    await self._cleanup(underlying, strike)

            except Exception as exc:
                logger.debug("StrikeCleanup: _fill_loop error: %s", exc)

    async def _sys_loop(self) -> None:
        """
        Consume SYSTEM_EVENT events.
        Handles explicit POSITION_CLOSED events published by the position manager.
        """
        while self._running:
            try:
                evt = await asyncio.wait_for(
                    self._sys_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            try:
                code = getattr(evt, "code", None) or (evt.get("event") if isinstance(evt, dict) else None)
                if code == SysEvent.POSITION_CLOSED:
                    msg = getattr(evt, "message", "") or (evt.get("message", "") if isinstance(evt, dict) else "")
                    # Expected message format: "UNDERLYING:STRIKE:OPTTYPE"
                    parts = str(msg).split(":")
                    if len(parts) >= 2:
                        underlying = parts[0].upper()
                        strike = float(parts[1])
                        key = (underlying, strike)
                        if self._open_counts[key] > 0:
                            self._open_counts[key] -= 1
                        await self._cleanup(underlying, strike)
            except Exception as exc:
                logger.debug("StrikeCleanup: _sys_loop error: %s", exc)

    # ── Cleanup logic ─────────────────────────────────────────────────────────

    async def _cleanup(self, underlying: str, strike: float) -> None:
        """
        Check if the given strike is eligible for stream cleanup:
          1. open_count == 0  — no remaining live positions
          2. strike outside current ATM window  — not needed for signal generation

        If eligible: unsubscribe CE + PE tokens and unpin from rebalancer.
        If not eligible (still in window): leave subscribed, log and move on.
        """
        key = (underlying, strike)

        # Guard: still has open positions on this strike
        if self._open_counts[key] > 0:
            logger.debug(
                "StrikeCleanup: [%s] strike %.0f still has %d open position(s) — skipping.",
                underlying, strike, self._open_counts[key],
            )
            return

        # Guard: strike is still inside the active ATM window
        active = self._rebalancer.active_strikes(underlying)
        pinned = self._rebalancer.pinned_strikes(underlying)

        # Exclude from cleanup if it's still needed for live signal generation
        if strike in active and strike not in pinned:
            self._cleanups_skipped_in_window += 1
            logger.debug(
                "StrikeCleanup: [%s] strike %.0f is inside ATM window — keeping stream.",
                underlying, strike,
            )
            return

        # The strike is outside the window and has no open positions — clean up
        tokens = _make_tokens(underlying, strike, self._cfg)
        if tokens:
            try:
                await self._feeder.unsubscribe_tokens(tokens)
                self._cleanups_performed += 1
                logger.info(
                    "StrikeCleanup: [%s] unsubscribed strike %.0f (outside ATM window, "
                    "no open positions). Total cleanups: %d.",
                    underlying, strike, self._cleanups_performed,
                )
            except Exception as exc:
                logger.warning(
                    "StrikeCleanup: [%s] unsubscribe failed for strike %.0f: %s",
                    underlying, strike, exc,
                )

        # Unpin from rebalancer so it won't block future rebalance decisions
        self._rebalancer.unpin_strike(underlying, strike)

        # Remove from active tracking to keep memory clean
        try:
            self._rebalancer._state[underlying].active_strikes.discard(strike)
        except (AttributeError, KeyError):
            pass  # Rebalancer state not yet initialised


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

import re


def _extract_underlying(symbol: str) -> Optional[str]:
    for underlying in ("BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "SENSEX", "NIFTY"):
        if underlying in symbol.upper():
            return underlying
    return None


def _extract_strike(symbol: str) -> Optional[float]:
    matches = re.findall(r'\d{4,6}', symbol)
    return float(matches[-1]) if matches else None


def _make_tokens(underlying: str, strike: float, cfg: GlobalConfig) -> list:
    """Build broker-agnostic token strings for both CE and PE legs."""
    from data_layer.symbol_translator import InternalSymbol
    tokens = []
    today = datetime.now(IST).date()
    for otype in ("CE", "PE"):
        sym = InternalSymbol(
            underlying=underlying,
            strike=float(strike),
            option_type=otype,
            expiry=today,
        )
        tokens.append(str(sym))
    return tokens
