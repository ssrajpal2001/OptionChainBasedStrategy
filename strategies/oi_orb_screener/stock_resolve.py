"""
strategies/oi_orb_screener/stock_resolve.py -- resolve broker symbol / lot
size / strike step / expiry for whatever F&O stock the screener shortlists
on a given day.

Unlike every other option-buyer strategy in this codebase, this one's
underlying is chosen DYNAMICALLY each trading day by screener.py, not fixed
at deployment time -- so it can't rely on the usual assumption that
InstrumentRegistry is already pre-loaded for its underlying (see
data_layer/instrument_registry.py's own load_sync() docstring: no existing
book in this codebase calls load_sync() itself). This module is the first
to do so.

Confirmed via direct code inspection (2026-08-24): InstrumentRegistry.
load_sync(underlying) already resolves real option contracts/expiries for
ARBITRARY NSE F&O stocks, not just indices -- it falls back to
_load_from_master_json() for anything not in the index-only
_UPSTOX_UNDERLYING_KEY map, and that fallback works WITHOUT an access_token
(confirmed 2026-08-09 fix comment in instrument_registry.py: "RELIANCE
resolves real 2026-08-25/09-29/10-27 monthly expiries"). What the registry
does NOT expose for arbitrary stocks is lot_size or strike_step -- only
FNO_STOCK_CONFIG (config/global_config.py, ~29 curated large-caps) has
that. This module's fallback for stocks not in that list is a small,
deliberately independent lot-size lookup against Upstox's own public
instrument master (same technique backtest/fno_scanner/scan_live.py
already uses for its own, separate universe scan) -- a small, documented
duplication, same accepted-tradeoff category as OI-Flow's independently
reimplemented swing detector, not scope creep.

Strike step, if not in FNO_STOCK_CONFIG either, falls back to the same
price-band heuristic the Colab screener already carries (and already
flags as unverified against a real broker chain) -- acceptable for a
paper_route connectivity pass, not real capital.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
import threading
from dataclasses import dataclass
from datetime import date
from typing import Optional

from config.global_config import FNO_STOCK_CONFIG, fno_stock_lot, fno_stock_step
from data_layer.instrument_registry import REGISTRY

logger = logging.getLogger(__name__)

_UPSTOX_NSE_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

# Process-wide cache of {symbol: lot_size} parsed from the Upstox NSE
# instrument master -- fetched at most ONCE per process lifetime (the
# master is large; re-downloading it per signal would be wasteful and slow
# down entry latency for no benefit, since lot sizes don't change intraday).
_lot_cache: dict = {}
_lot_cache_loaded = False
# 2026-08-27, direct user spec: OI-ORB's own dedicated upstox2 feeder needs
# each shortlisted stock's real NSE_EQ instrument_key to subscribe live spot
# ticks (UpstoxFeeder.register_extra_spot_keys takes instrument keys, not
# Fyers' "NSE:SYMBOL-EQ" string format). No such lookup existed anywhere in
# this codebase before this. Populated in the SAME pass as the lot cache
# (same master JSON, just also keeping NSE_EQ rows instead of only NSE_FO
# ones) -- avoids a second full download for a cache this closely related.
_eq_key_cache: dict = {}
# 2026-08-24 CRITICAL fix, confirmed live: resolve_lot() runs via
# asyncio.to_thread -- real OS threads, one per concurrently-firing signal.
# _lot_cache_loaded used to be set True BEFORE the fetch even started, so
# when multiple signals fired in the same batch (confirmed live: 5 at once
# on a real bearish-regime day), the first thread claimed "loaded" and
# started the slow fetch while the other threads saw loaded=True, skipped
# fetching themselves, and read the still-EMPTY cache -- 4 of 5 stocks
# failed lot resolution that were never actually unresolvable, just raced.
# A real threading.Lock (not asyncio.Lock -- these are genuine OS threads
# from the thread pool, not event-loop tasks) makes every concurrent caller
# actually wait for the one real fetch to finish before reading the cache.
_lot_cache_lock = threading.Lock()


def _round_to_strike_step(price: float) -> float:
    """Same heuristic as the Colab screener's own _round_to_strike_step --
    kept identical so behavior matches what was already validated live
    against real NSE data on 2026-08-24. CONFIRM against the real option
    chain before trusting this beyond a paper_route connectivity pass."""
    if price < 250:
        step = 2.5
    elif price < 500:
        step = 5
    elif price < 1000:
        step = 10
    elif price < 2500:
        step = 20
    elif price < 5000:
        step = 50
    else:
        step = 100
    return step


def _load_upstox_lot_cache_locked() -> None:
    """The actual fetch -- ONLY ever called while _lot_cache_lock is held
    (see resolve_lot() below). Never raises -- a failure here just means
    resolve_lot() returns 0 (caller must treat 0 as unresolvable and skip
    the entry, never guess a lot size for real order quantity).

    2026-08-27, direct user spec: also populates _eq_key_cache (symbol ->
    real NSE_EQ instrument_key) in this SAME pass over the SAME downloaded
    master JSON -- OI-ORB's dedicated upstox2 feeder needs this to subscribe
    live spot ticks (UpstoxFeeder.register_extra_spot_keys takes instrument
    keys, not a symbol string). A second full download just for this would
    be wasteful; NSE_EQ rows are already present in the same file."""
    global _lot_cache_loaded
    try:
        from curl_cffi import requests as cc
    except ImportError:
        logger.warning("stock_resolve: curl_cffi not installed -- cannot fetch Upstox "
                        "instrument master for lot-size fallback.")
        _lot_cache_loaded = True
        return
    try:
        r = cc.get(_UPSTOX_NSE_MASTER_URL, impersonate="chrome131", timeout=30)
        instruments = json.loads(gzip.decompress(r.content))
    except Exception as exc:
        logger.warning("stock_resolve: failed to fetch/parse Upstox NSE instrument master: %s", exc)
        _lot_cache_loaded = True
        return

    lots: dict = {}
    eq_keys: dict = {}
    for inst in instruments:
        segment = inst.get("segment")
        if segment == "NSE_EQ":
            sym = str(inst.get("trading_symbol", "")).upper()
            ikey = inst.get("instrument_key", "")
            if sym and ikey:
                eq_keys[sym] = ikey
            continue
        if segment != "NSE_FO":
            continue
        if inst.get("instrument_type") not in ("CE", "PE"):
            continue
        ts = inst.get("trading_symbol", "")
        parts = ts.split()
        if len(parts) < 2:
            continue
        underlying = parts[0].upper()
        ls = int(inst.get("lot_size") or 0)
        if ls <= 0:
            continue
        if underlying not in lots or ls < lots[underlying]:
            lots[underlying] = ls
    _lot_cache.update(lots)
    _eq_key_cache.update(eq_keys)
    _lot_cache_loaded = True
    logger.info("stock_resolve: Upstox instrument-master lot cache loaded (%d underlyings, "
                "%d NSE_EQ instrument keys).", len(lots), len(eq_keys))


def resolve_lot(stock_symbol: str) -> int:
    """Returns lot_size, or 0 if UNRESOLVABLE -- caller must skip the entry
    rather than guess a lot size for real order quantity. Blocking (network
    on first miss) -- call via asyncio.to_thread() from async code.

    2026-08-24 CRITICAL fix, confirmed live: multiple signals firing in the
    same batch (5 concurrent asyncio.to_thread calls, real OS threads) used
    to race on _lot_cache_loaded -- the first thread set it True instantly
    (before its own fetch even started) and every OTHER concurrent thread
    read the flag as already-loaded and returned an empty cache lookup (0,
    "unresolvable") for a stock that was never actually unresolvable, just
    raced. The lock below makes every concurrent caller actually block
    until the ONE real fetch (whichever thread wins the lock first) has
    fully finished and populated the cache, instead of racing past it."""
    sym = stock_symbol.upper()
    if sym in FNO_STOCK_CONFIG:
        return fno_stock_lot(sym)

    if not _lot_cache_loaded:
        with _lot_cache_lock:
            # Re-check inside the lock -- another thread may have already
            # completed the fetch while this one was waiting to acquire it.
            if not _lot_cache_loaded:
                _load_upstox_lot_cache_locked()
    return _lot_cache.get(sym, 0)


def resolve_eq_instrument_key(stock_symbol: str) -> str:
    """2026-08-27, direct user spec: returns the real Upstox NSE_EQ
    instrument_key for a stock (e.g. "NSE_EQ|INE202E01016" for RELIANCE), or
    "" if unresolvable -- caller must skip live spot-tick subscription for
    this symbol rather than guess. Same cache/lock/loaded-flag as
    resolve_lot() (same underlying master JSON, same real concurrent-signal
    race it already guards against). Blocking (network on first miss) --
    call via asyncio.to_thread() from async code."""
    sym = stock_symbol.upper()
    if not _lot_cache_loaded:
        with _lot_cache_lock:
            if not _lot_cache_loaded:
                _load_upstox_lot_cache_locked()
    return _eq_key_cache.get(sym, "")


def resolve_strike_step_for_price(stock_symbol: str, price: float) -> float:
    """Strike step actually depends on the stock's live price band, not a
    fixed value per stock -- FNO_STOCK_CONFIG's step IS fixed per stock
    (matches NSE's real per-symbol grid for those curated names), but for
    the Upstox-master fallback path there is no such grid available, so the
    Colab script's own price-band heuristic is used instead."""
    sym = stock_symbol.upper()
    if sym in FNO_STOCK_CONFIG:
        return float(fno_stock_step(sym))
    return _round_to_strike_step(price)


@dataclass
class ResolvedContract:
    underlying: str
    expiry: date
    strike: int
    option_type: str
    upstox_key: str
    broker_symbols: dict  # {provider_lower: symbol_str}


def resolve_contract(stock_symbol: str, raw_strike: float, option_type: str,
                      providers: "list[str]" = ("upstox", "zerodha", "fyers", "angelone", "dhan"),
                      ) -> Optional[ResolvedContract]:
    """Resolves the real, tradable option contract for a stock the screener
    just shortlisted. Loads the registry for this underlying if not already
    loaded (this book is the first in this codebase to call load_sync()
    itself -- see module docstring). Blocking (network on a cold underlying)
    -- call via asyncio.to_thread() from async code.

    Returns None if the registry can't resolve an active expiry or a
    matching upstox_key for the rounded strike (e.g. a stock whose F&O
    contracts have genuinely stopped trading, or an off-grid strike) --
    caller must skip the entry, never fabricate a symbol."""
    sym = stock_symbol.upper()
    if not REGISTRY.is_loaded(sym):
        REGISTRY.load_sync(sym)

    expiry = REGISTRY.get_active_expiry(sym)
    if expiry is None:
        logger.warning("stock_resolve: no active expiry resolved for %s -- skipping.", sym)
        return None

    # 2026-08-27 CRITICAL fix, confirmed live: GVT&D PE entry failed with
    # "no upstox_key resolved for GVT&D PE4350" -- the price-band heuristic
    # below assumed a flat 50pt grid for anything under Rs5000, but GVT&D's
    # REAL listed grid switches to 100pt around that price level (4300/4400
    # are real, 4350 was never listed at all -- confirmed directly against
    # the real Upstox master JSON via scripts/check_stock_expiries_raw.py).
    # Snap to the NEAREST REAL listed strike (now that the registry is
    # already loaded, it knows the true grid) instead of guessing a step --
    # only fall back to the old heuristic if this underlying/expiry
    # genuinely has no strikes loaded (never crash, never guess a symbol).
    available = REGISTRY.get_available_strikes(sym, expiry, option_type)
    if available:
        strike = min(available, key=lambda s: abs(s - raw_strike))
    else:
        step = resolve_strike_step_for_price(sym, raw_strike)
        strike = int(round(raw_strike / step) * step) if step > 0 else int(round(raw_strike))

    upstox_key = REGISTRY.get_upstox_key(sym, expiry, strike, option_type)
    if not upstox_key:
        logger.warning("stock_resolve: no upstox_key resolved for %s %s%d exp=%s -- skipping.",
                        sym, option_type, strike, expiry)
        return None

    broker_symbols = {}
    for provider in providers:
        try:
            broker_symbols[provider] = REGISTRY.get_broker_symbol(sym, expiry, strike, option_type, provider)
        except Exception:
            broker_symbols[provider] = ""

    return ResolvedContract(
        underlying=sym, expiry=expiry, strike=strike, option_type=option_type,
        upstox_key=upstox_key, broker_symbols=broker_symbols,
    )


async def resolve_contract_async(stock_symbol: str, raw_strike: float, option_type: str) -> Optional[ResolvedContract]:
    return await asyncio.to_thread(resolve_contract, stock_symbol, raw_strike, option_type)


async def resolve_lot_async(stock_symbol: str) -> int:
    return await asyncio.to_thread(resolve_lot, stock_symbol)


def resolve_contract_exact(stock_symbol: str, expiry, strike: int, option_type: str,
                            providers: "list[str]" = ("upstox", "zerodha", "fyers", "angelone", "dhan"),
                            ) -> Optional[ResolvedContract]:
    """Same as resolve_contract() but for an ALREADY-KNOWN exact strike/expiry
    -- used only to restore a position from strategies/oi_orb_screener/
    store.py after a restart. Deliberately skips the raw-price-to-strike
    rounding entirely rather than re-deriving the strike a second time from
    a raw trigger price: the exact strike that was genuinely traded is
    already known and stored, so re-deriving it risks resolving a DIFFERENT
    strike if the OTM%/rounding logic ever changes between the original
    entry and the restart -- strictly more risk than re-using the ground
    truth for no benefit."""
    sym = stock_symbol.upper()
    if not REGISTRY.is_loaded(sym):
        REGISTRY.load_sync(sym)

    if isinstance(expiry, str):
        expiry = date.fromisoformat(expiry)

    upstox_key = REGISTRY.get_upstox_key(sym, expiry, strike, option_type)
    if not upstox_key:
        logger.warning("stock_resolve: restore -- no upstox_key resolved for %s %s%d exp=%s.",
                        sym, option_type, strike, expiry)
        return None

    broker_symbols = {}
    for provider in providers:
        try:
            broker_symbols[provider] = REGISTRY.get_broker_symbol(sym, expiry, strike, option_type, provider)
        except Exception:
            broker_symbols[provider] = ""

    return ResolvedContract(
        underlying=sym, expiry=expiry, strike=strike, option_type=option_type,
        upstox_key=upstox_key, broker_symbols=broker_symbols,
    )


async def resolve_contract_exact_async(stock_symbol: str, expiry, strike: int,
                                        option_type: str) -> Optional[ResolvedContract]:
    return await asyncio.to_thread(resolve_contract_exact, stock_symbol, expiry, strike, option_type)


def resolve_delta_band_contract(stock_symbol: str, expiry, option_type: str,
                                 delta_min: float, delta_max: float, target_delta: float,
                                 chain_data: dict,
                                 providers: "list[str]" = ("upstox", "zerodha", "fyers", "angelone", "dhan"),
                                 ) -> Optional[ResolvedContract]:
    """2026-09-22, option-contract-native entry/exit mechanic (Layer 1
    contract selection) -- resolves the real, tradable option contract
    whose live delta is closest to `target_delta` (+0.55 for CE, -0.55 for
    PE) among every strike on `option_type` that falls within
    [delta_min, delta_max]. Unlike resolve_contract()/resolve_contract_exact
    above, the strike here is NOT derived from a raw spot-price offset --
    it's picked purely from delta, using the full Upstox option-chain
    response (`chain_data`, the dict GlobalFeeder.fetch_option_chain()
    already returns via resp.to_dict()) rather than the registry's own
    (delta-blind) strike list.

    Pure candidate-picking + tie-break logic lives in
    option_native.select_delta_band_candidate/parse_chain_side_candidates
    (fully unit-tested in isolation, no I/O) -- this function is only the
    thin, impure wrapper that (a) parses the real chain response shape into
    that pure function's candidate-dict input and (b) builds a
    ResolvedContract the same way resolve_contract() does (broker symbols
    per provider via REGISTRY, never fabricated).

    Returns None if no candidate falls in [delta_min, delta_max] at all
    (decision 8 -- caller must skip this side entirely for the day, no
    band-widening, no substitute rule) or if the picked strike can't
    resolve a real upstox_key/broker symbol set. Pure/no network of its own
    -- `chain_data` must already be fetched by the caller (blocking Upstox
    SDK call happens in GlobalFeeder.fetch_option_chain, already
    asyncio.to_thread-wrapped there); this function itself does no I/O and
    is safe to call directly from async code."""
    from strategies.oi_orb_screener import option_native

    sym = stock_symbol.upper()
    candidates = option_native.parse_chain_side_candidates(chain_data, option_type)
    picked = option_native.select_delta_band_candidate(candidates, delta_min, delta_max, target_delta)
    if picked is None:
        logger.info("stock_resolve: no %s candidate for %s in delta band [%.2f, %.2f] "
                     "(target=%.2f) -- skipping this side for today.",
                     option_type, sym, delta_min, delta_max, target_delta)
        return None

    if not REGISTRY.is_loaded(sym):
        REGISTRY.load_sync(sym)
    if isinstance(expiry, str):
        expiry = date.fromisoformat(expiry)

    strike = int(round(picked["strike"]))
    upstox_key = picked.get("upstox_key") or REGISTRY.get_upstox_key(sym, expiry, strike, option_type)
    if not upstox_key:
        logger.warning("stock_resolve: resolve_delta_band_contract -- no upstox_key resolved for "
                        "%s %s%d exp=%s (delta=%.4f).", sym, option_type, strike, expiry,
                        picked.get("delta") or 0.0)
        return None

    broker_symbols = {}
    for provider in providers:
        try:
            broker_symbols[provider] = REGISTRY.get_broker_symbol(sym, expiry, strike, option_type, provider)
        except Exception:
            broker_symbols[provider] = ""

    return ResolvedContract(
        underlying=sym, expiry=expiry, strike=strike, option_type=option_type,
        upstox_key=upstox_key, broker_symbols=broker_symbols,
    )


async def resolve_delta_band_contract_async(stock_symbol: str, expiry, option_type: str,
                                             delta_min: float, delta_max: float, target_delta: float,
                                             chain_data: dict) -> Optional[ResolvedContract]:
    return await asyncio.to_thread(resolve_delta_band_contract, stock_symbol, expiry, option_type,
                                    delta_min, delta_max, target_delta, chain_data)
