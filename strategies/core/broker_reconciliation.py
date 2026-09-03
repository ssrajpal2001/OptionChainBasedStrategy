"""
strategies/core/broker_reconciliation.py — cross-checks a strategy book's
OWN believed position against the broker's real, live position book.

2026-08-23, built after an overnight crash-resilience audit found: if a
position's persisted state file is ever corrupted/lost, every strategy in
this codebase silently treats itself as flat, with ZERO cross-check
against what the broker actually shows (data_layer/position_store.py's own
load() was hardened the same night to at least log CRITICAL on a corrupt
file, but that only catches the "file exists but won't parse" case, not
"the file is fine but describes something the broker doesn't agree with"
or "the file is missing/never written for some other reason"). If a real
broker position is still open in either of those situations, nothing was
watching it: no SL, no target, no EOD squareoff, forever, until a human
happened to notice.

Deliberately DETECTION + LOUD ALERTING ONLY, never auto-remediation. Given
this cross-checks real money against real broker state, automatically
"fixing" a mismatch (force-closing a position the reconciliation doesn't
recognize, or fabricating a position dict from broker data alone) carries
its own real risk of doing the WRONG thing confidently -- e.g. a position
this bot doesn't recognize could be a different strategy's real position,
or a manual trade the user placed themselves. A human reviewing a loud,
precise alert is safer than software guessing. Matches the same
"flag, don't auto-act" philosophy already established for the
position_store.py corruption fix the same night.

Two distinct checks, with two very different confidence levels:

1. PRECISE (used whenever the book believes it holds a position): for
   each leg the book believes is open, the book itself computes the exact
   expected broker trading symbol (same REGISTRY.get_broker_symbol() /
   fill-recorded symbol every bridge already uses for real order
   placement -- this module does not re-derive symbols itself, on purpose,
   so it can never drift out of sync with what "the real symbol" means).
   A missing or zero-qty broker position at that exact symbol is a
   precise, high-confidence mismatch.

2. HEURISTIC (used only when the book believes it is FLAT): the broker's
   own raw position list is scanned for any nonzero position whose symbol
   starts with the underlying's own name -- a broker-agnostic string
   check that works across every provider's own symbol format without
   needing a full reverse-parser for each one (only Fyers/AngelOne have
   one; Zerodha/Dhan don't). This can NEVER be asserted as definitely
   "this bot's own missed position" -- it could just as easily be a
   different strategy's real position, or a manual trade the user placed
   themselves on the same underlying. Reported as "needs manual
   verification", never as a confirmed match.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExpectedLeg:
    symbol: str    # the exact broker trading symbol this leg is believed to occupy
    label: str     # human-readable, e.g. "CE 24500" -- used only in log/alert text


@dataclass(frozen=True)
class ReconciliationResult:
    ok: bool
    missing_legs: List[str]          # labels of legs believed open the broker doesn't confirm
    heuristic_flags: List[str]       # broker symbols that MIGHT be an untracked position
    detail: str
    skipped: bool = False            # broker unreachable / get_positions() failed -- not a fault


async def reconcile_book(
    broker, underlying: str, expected_legs: List[ExpectedLeg],
) -> ReconciliationResult:
    """expected_legs empty -> the book believes it is flat (heuristic check
    only). Non-empty -> the book believes those legs are open (precise
    check on each)."""
    if broker is None or not hasattr(broker, "get_positions"):
        return ReconciliationResult(
            ok=True, missing_legs=[], heuristic_flags=[],
            detail="no broker instance available -- reconciliation skipped, not treated as a fault",
            skipped=True,
        )
    try:
        positions = await broker.get_positions()
    except Exception as exc:
        return ReconciliationResult(
            ok=True, missing_legs=[], heuristic_flags=[],
            detail=f"broker.get_positions() failed ({exc}) -- reconciliation skipped, not treated as a fault",
            skipped=True,
        )

    live_by_symbol = {p.symbol: p for p in (positions or []) if getattr(p, "qty", 0)}

    if expected_legs:
        missing = [leg.label for leg in expected_legs if leg.symbol not in live_by_symbol]
        if missing:
            return ReconciliationResult(
                ok=False, missing_legs=missing, heuristic_flags=[],
                detail=(
                    f"book believes {len(expected_legs)} leg(s) open, broker confirms only "
                    f"{len(expected_legs) - len(missing)} -- MISSING AT BROKER: {', '.join(missing)}. "
                    f"A real position may be genuinely open and completely unmonitored, or the book's "
                    f"own tracked state may be stale/wrong -- verify directly against the broker's own "
                    f"terminal before trusting either side."
                ),
            )
        return ReconciliationResult(ok=True, missing_legs=[], heuristic_flags=[],
                                     detail="all believed-open legs confirmed at the broker.")

    prefix = (underlying or "").upper()
    flags = [sym for sym in live_by_symbol if prefix and sym.upper().startswith(prefix)]
    if flags:
        return ReconciliationResult(
            ok=False, missing_legs=[], heuristic_flags=flags,
            detail=(
                f"book believes it is FLAT, but the broker shows {len(flags)} nonzero "
                f"position(s) on {underlying}: {', '.join(flags)}. This is a HEURISTIC "
                f"(symbol-prefix) match, NOT a confirmed identification -- it may belong to "
                f"a different strategy or a manual trade on the same underlying. Verify "
                f"manually; do not assume this bot is responsible for it."
            ),
        )
    return ReconciliationResult(ok=True, missing_legs=[], heuristic_flags=[],
                                 detail="flat and no unaccounted broker position found for this underlying.")


async def reconcile_and_alert(
    bus, broker, underlying: str, expected_legs: List[ExpectedLeg],
    strategy_name: str, client_id: str, binding_id: str, clog: Optional[logging.Logger] = None,
) -> ReconciliationResult:
    """reconcile_book() plus the standard CRITICAL log + dashboard
    SYSTEM_EVENT alert on a real mismatch -- the wiring every strategy's
    own reconciliation call site shares, so the alert format/severity
    stays consistent across all of them."""
    result = await reconcile_book(broker, underlying, expected_legs)
    if result.ok or result.skipped:
        return result
    msg = f"{strategy_name}[{underlying}|{client_id}|{binding_id}]: BROKER RECONCILIATION MISMATCH -- {result.detail}"
    logger.critical(msg)
    if clog is not None:
        try:
            clog.critical(msg)
        except Exception:
            pass
    if bus is not None:
        try:
            from config.global_config import SysEvent, Topic
            from data_layer.base_feeder import SystemEvent
            await bus.publish(Topic.SYSTEM_EVENT, SystemEvent(SysEvent.POSITION_MISMATCH, msg))
        except Exception:
            pass
    return result
