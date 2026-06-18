"""
data_layer/strike_rebalancer.py — Dynamic ATM strike monitor and rebalancer.

Subscribes to INDEX_TICK on the EventBus.  For each underlying:
  1. Records market-open ATM on the first tick at or after 09:15 IST.
  2. On every subsequent tick, checks if spot has drifted ≥ 3 × strike_step
     from the recorded current ATM.
  3. If drift is detected, computes the new ATM and:
       a. Calculates set difference: new_window - old_window = to_subscribe.
       b. Calculates set difference: old_window - new_window - pinned = to_unsubscribe.
       c. Calls feeder.unsubscribe_tokens(to_unsubscribe) — never touches pinned.
       d. Calls feeder.subscribe_tokens(to_subscribe).
       e. Updates active_strikes and current_atm baseline.

Pinned strikes (open / trailing positions):
  pin_strike(underlying, strike)   — called by execution layer on fill
  unpin_strike(underlying, strike) — called by execution layer on position close

  Pinned strikes are EXCLUDED from the unsubscribe set — the live data stream
  for an open contract is never killed regardless of how far the market moves.
  Pinned strikes also remain in active_strikes even after they leave the ATM
  window, so the set-difference logic never accidentally drops them.

Auto-pin from ORDER_FILL:
  StrikeRebalancer subscribes to ORDER_FILL and automatically pins the strike
  embedded in each fill's broker_symbol so the execution layer does not need to
  call pin_strike() manually.  Pins are removed when close_order_ticket() is
  called, or when unpin_strike() is called explicitly.

No time.sleep. All async.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Dict, Optional, Set

from config.global_config import IST, Topic, SysEvent, GlobalConfig
from data_layer.base_feeder import EventBus, IndexTick, SystemEvent, BaseFeeder
from data_layer.symbol_translator import InternalSymbol

logger = logging.getLogger(__name__)

_MARKET_OPEN  = time(9, 15, 0)
_NSE_CLOSE    = time(15, 30, 0)   # unsubscribe NSE/BSE strikes after this
_NSE_INDICES  = frozenset({"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"})
_REBALANCE_THRESHOLD = 3   # strike intervals drift before rebalancing


# ─────────────────────────────────────────────────────────────────────────────
# Per-Underlying Rebalance State
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _UnderlyingState:
    open_atm: Optional[float] = None         # ATM recorded at market open
    current_atm: Optional[float] = None      # Baseline updated after each rebalance
    # active_strikes = currently subscribed window UNION pinned strikes.
    # Invariant: pinned_strikes is always a subset of active_strikes.
    active_strikes: Set[float] = field(default_factory=set)
    pinned_strikes: Set[float] = field(default_factory=set)
    rebalance_count: int = 0
    eod_unsubscribed: bool = False           # True once NSE EOD cleanup has run


# ─────────────────────────────────────────────────────────────────────────────
# Strike Rebalancer
# ─────────────────────────────────────────────────────────────────────────────

class StrikeRebalancer:
    """
    Monitors spot drift and asynchronously resubscribes to the ATM ± chain_depth
    strike window whenever drift ≥ REBALANCE_THRESHOLD × strike_step.

    Open-position strikes are pinned in pinned_strikes and are never included in
    the unsubscribe set regardless of how far ATM has moved.
    """

    def __init__(
        self,
        bus: EventBus,
        cfg: GlobalConfig,
        feeder: BaseFeeder,
    ) -> None:
        self._bus = bus
        self._cfg = cfg
        self._feeder = feeder
        self._tick_queue = bus.subscribe(Topic.INDEX_TICK)
        self._fill_queue = bus.subscribe(Topic.ORDER_FILL)
        self._sysevt_queue = bus.subscribe(Topic.SYSTEM_EVENT)
        self._running = False
        self._state: Dict[str, _UnderlyingState] = {
            u: _UnderlyingState() for u in cfg.monitored_indices
        }

    # ── Public API ─────────────────────────────────────────────────────────────

    def pin_strike(self, underlying: str, strike: float) -> None:
        """
        Register strike as having an open position — prevents unsubscription.
        Called by the execution layer (or automatically via ORDER_FILL event).
        """
        if underlying in self._state:
            st = self._state[underlying]
            st.pinned_strikes.add(strike)
            st.active_strikes.add(strike)   # Ensure it stays in the active set
            logger.debug(
                "StrikeRebalancer: [%s] pinned strike %.0f (%d total pinned).",
                underlying, strike, len(st.pinned_strikes),
            )

    def unpin_strike(self, underlying: str, strike: float) -> None:
        """
        Remove pin when a position is closed.
        The strike is NOT automatically unsubscribed — call feeder.unsubscribe_tokens()
        separately if you want to drop the feed once no position remains.
        """
        if underlying in self._state:
            self._state[underlying].pinned_strikes.discard(strike)
            logger.debug(
                "StrikeRebalancer: [%s] unpinned strike %.0f.", underlying, strike,
            )

    def pinned_strikes(self, underlying: str) -> Set[float]:
        """Return a copy of the current pinned set for an underlying."""
        st = self._state.get(underlying)
        return set(st.pinned_strikes) if st else set()

    def active_strikes(self, underlying: str) -> Set[float]:
        """Return a copy of the full active (subscribed) set for an underlying."""
        st = self._state.get(underlying)
        return set(st.active_strikes) if st else set()

    def rebalance_stats(self) -> Dict[str, int]:
        return {u: s.rebalance_count for u, s in self._state.items()}

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("StrikeRebalancer: started.")
        # Run tick consumer, fill consumer, and feed-switch listener concurrently
        await asyncio.gather(
            self._tick_loop(),
            self._fill_loop(),
            self._sysevt_loop(),
        )

    def stop(self) -> None:
        self._running = False

    # ── Internal loops ────────────────────────────────────────────────────────

    async def _tick_loop(self) -> None:
        while self._running:
            try:
                tick: IndexTick = await asyncio.wait_for(
                    self._tick_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            await self._on_tick(tick)

    async def _sysevt_loop(self) -> None:
        """
        Listen for feed-provider switches. When the live feed (dual/single)
        activates, the market-open ATM may have been anchored on a MockFeeder
        synthetic tick (e.g. NIFTY 24500 vs real 23500). Reset the anchor so the
        rebalancer re-subscribes the strike window around the REAL ATM on the
        next incoming index tick — otherwise every strategy reads 0.00 premiums
        for strikes that were never subscribed.
        """
        while self._running:
            try:
                evt = await asyncio.wait_for(self._sysevt_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            msg = str(getattr(evt, "message", "")).lower()
            code = getattr(evt, "code", None)
            if code == SysEvent.FEEDER_RESTORED and ("dual" in msg or "single" in msg):
                for underlying, st in self._state.items():
                    st.open_atm = None
                    st.current_atm = None
                logger.info(
                    "StrikeRebalancer: live feed activated (%s) — ATM anchors reset; "
                    "will re-subscribe around real ATM on next tick.", msg,
                )

    async def _fill_loop(self) -> None:
        """
        Consume ORDER_FILL events and automatically pin the traded strike.
        This ensures the subscription window is never dropped for a contract
        that was just filled, without requiring the execution layer to call
        pin_strike() manually.
        """
        while self._running:
            try:
                fill = await asyncio.wait_for(
                    self._fill_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            try:
                underlying = getattr(fill, "underlying", None) or _extract_underlying(
                    getattr(fill, "broker_symbol", "")
                )
                strike = getattr(fill, "strike", None) or _extract_strike(
                    getattr(fill, "broker_symbol", "")
                )
                if underlying and strike and underlying in self._state:
                    self.pin_strike(underlying, float(strike))
            except Exception as exc:
                logger.debug("StrikeRebalancer: fill auto-pin error: %s", exc)

    # ── Tick handler ──────────────────────────────────────────────────────────

    async def _on_tick(self, tick: IndexTick) -> None:
        underlying = tick.symbol
        if underlying not in self._state:
            return

        state = self._state[underlying]

        # NSE/BSE: block initial subscription AND unsubscribe after 15:30 IST to free
        # Upstox/Fyers WS slots for MCX evening session (crude/gold run until ~23:30).
        if underlying in _NSE_INDICES:
            now_t = tick.timestamp.astimezone(IST).time()
            if now_t >= _NSE_CLOSE:
                # Mark so initial_subscribe is also blocked on restart after NSE close
                state.eod_unsubscribed = True
        if (underlying in _NSE_INDICES
                and not state.eod_unsubscribed
                and tick.timestamp.astimezone(IST).time() >= _NSE_CLOSE):
            to_unsub = state.active_strikes - state.pinned_strikes
            if to_unsub and self._feeder is not None:
                tokens = self._strikes_to_tokens(underlying, to_unsub)
                try:
                    await self._feeder.unsubscribe_tokens(tokens)
                    state.active_strikes -= to_unsub
                    logger.info(
                        "StrikeRebalancer: [%s] NSE EOD — unsubscribed %d strikes (%d pinned kept)",
                        underlying, len(to_unsub), len(state.pinned_strikes),
                    )
                except Exception as exc:
                    logger.warning("StrikeRebalancer: [%s] NSE EOD unsubscribe failed: %s",
                                   underlying, exc)
            state.eod_unsubscribed = True
            return  # skip normal rebalance processing after close

        step = self._cfg.exchange.strike_steps.get(underlying, 50.0)
        atm = _round_to_strike(tick.ltp, step)

        # Record market-open ATM on first qualifying tick
        # Skip for NSE/BSE indices after market close — don't waste WS slots
        if state.open_atm is None and _is_market_open(tick.timestamp):
            if state.eod_unsubscribed:
                return  # NSE already closed, don't subscribe
            state.open_atm = atm
            state.current_atm = atm
            await self._initial_subscribe(underlying, atm, step, state)
            logger.info(
                "StrikeRebalancer: [%s] Market-open ATM = %.0f",
                underlying, atm,
            )
            return

        if state.current_atm is None:
            return

        drift_intervals = abs(atm - state.current_atm) / step
        if drift_intervals >= _REBALANCE_THRESHOLD:
            logger.info(
                "StrikeRebalancer: [%s] Spot %.2f drifted %.1f intervals "
                "from ATM %.0f — rebalancing to new ATM %.0f.",
                underlying, tick.ltp, drift_intervals, state.current_atm, atm,
            )
            await self._rebalance(underlying, atm, step, state)

    # ── Subscription management ───────────────────────────────────────────────

    async def _initial_subscribe(
        self,
        underlying: str,
        atm: float,
        step: float,
        state: _UnderlyingState,
    ) -> None:
        depth = self._cfg.chain_depth
        window = set(_strike_window(atm, step, depth))
        # Union with any pre-existing pins (e.g. restored from SQLite on reboot)
        state.active_strikes = window | state.pinned_strikes
        tokens = self._strikes_to_tokens(underlying, list(state.active_strikes))
        if tokens:
            await self._feeder.subscribe_tokens(tokens)
        logger.debug(
            "StrikeRebalancer: [%s] Initial subscription %d strikes (window=%d + pinned=%d).",
            underlying, len(state.active_strikes), len(window), len(state.pinned_strikes),
        )

    async def _rebalance(
        self,
        underlying: str,
        new_atm: float,
        step: float,
        state: _UnderlyingState,
    ) -> None:
        depth = self._cfg.chain_depth
        old_active = state.active_strikes           # current subscribed set
        new_window = set(_strike_window(new_atm, step, depth))

        # Set difference — pinned strikes are NEVER included in to_unsub
        to_unsub = old_active - new_window - state.pinned_strikes
        to_sub   = new_window - old_active

        if to_unsub:
            tokens = self._strikes_to_tokens(underlying, list(to_unsub))
            if tokens:
                await self._feeder.unsubscribe_tokens(tokens)

        if to_sub:
            tokens = self._strikes_to_tokens(underlying, list(to_sub))
            if tokens:
                await self._feeder.subscribe_tokens(tokens)

        # Update active_strikes: remove dropped, add new, always keep pinned
        state.active_strikes = (old_active - to_unsub) | to_sub
        # Invariant check: pinned must always be a subset of active
        assert state.pinned_strikes.issubset(state.active_strikes), \
            "BUG: pinned_strikes escaped active_strikes — this should never happen"

        state.current_atm = new_atm
        state.rebalance_count += 1

        evt = SystemEvent(
            code=SysEvent.FEEDER_RESTORED,
            message=(
                f"REBALANCE {underlying}: new_atm={new_atm:.0f} "
                f"sub={len(to_sub)} unsub={len(to_unsub)} "
                f"pinned={len(state.pinned_strikes)}"
            ),
        )
        await self._bus.publish(Topic.SYSTEM_EVENT, evt)
        logger.info(
            "StrikeRebalancer: [%s] Rebalance #%d complete: "
            "active=%d sub=%d unsub=%d pinned=%d.",
            underlying, state.rebalance_count,
            len(state.active_strikes), len(to_sub),
            len(to_unsub), len(state.pinned_strikes),
        )

    def _strikes_to_tokens(self, underlying: str, strikes) -> list:
        """
        Convert strike prices to subscription tokens in the feeder's native format.
        Uses InstrumentRegistry to derive the correct broker-specific symbol.
        Falls back to internal canonical format if registry not loaded.
        """
        from data_layer.instrument_registry import REGISTRY, next_expiry as _next_expiry

        today  = datetime.now(IST).date()
        expiry = _next_expiry(underlying, today)
        if expiry is None:
            logger.warning("StrikeRebalancer[%s]: no expiry in registry — skipping token build", underlying)
            return []

        # Detect which provider format(s) to build. In dual mode we build BOTH
        # Upstox instrument_keys AND Fyers symbols so each feeder in the
        # active-active pair receives option contracts in its own native format.
        # Each feeder filters out tokens it doesn't understand (see
        # UpstoxFeeder/FyersFeeder.subscribe_tokens).
        providers = []
        if hasattr(self._feeder, "active_provider"):
            ap = self._feeder.active_provider
            if ap == "dual":
                providers = ["upstox", "fyers"]
            elif ap == "fyers":
                providers = ["fyers"]
            elif ap == "upstox":
                providers = ["upstox"]
        else:
            feeder_type = type(self._feeder).__name__
            if "Fyers" in feeder_type:
                providers = ["fyers"]
            elif "Upstox" in feeder_type:
                providers = ["upstox"]

        if not providers:
            providers = ["internal"]

        tokens = []
        for strike in strikes:
            strike_int = int(round(float(strike)))
            for otype in ("CE", "PE"):
                for provider in providers:
                    if provider == "internal":
                        sym = InternalSymbol(
                            underlying=underlying, strike=float(strike_int),
                            option_type=otype, expiry=expiry,
                        )
                        tokens.append(str(sym))
                    else:
                        sym = REGISTRY.get_broker_symbol(
                            underlying, expiry, strike_int, otype, provider
                        )
                        if sym:
                            tokens.append(sym)
        return tokens


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _round_to_strike(price: float, step: float) -> float:
    return round(round(price / step) * step, 2)


def _strike_window(atm: float, step: float, depth: int) -> list:
    return [atm + (i * step) for i in range(-depth, depth + 1)]


def _is_market_open(ts: datetime) -> bool:
    return ts.astimezone(IST).time() >= _MARKET_OPEN


def _extract_underlying(symbol: str) -> Optional[str]:
    """Try to identify the underlying from a broker symbol string."""
    for underlying in ("BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "SENSEX", "NIFTY"):
        if underlying in symbol.upper():
            return underlying
    return None


def _extract_strike(symbol: str) -> Optional[float]:
    """Extract a numeric strike from a broker symbol string (best-effort)."""
    # Match the longest numeric run that looks like a strike (4–6 digits)
    matches = re.findall(r'\d{4,6}', symbol)
    return float(matches[-1]) if matches else None
