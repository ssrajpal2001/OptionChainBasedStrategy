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
_load_from_master_json() for anything not in the index/MCX-only
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


def _load_upstox_lot_cache() -> None:
    """Best-effort, one-shot fetch of Upstox's public NSE instrument master
    to extract lot_size per F&O-stock underlying. Never raises -- a failure
    here just means resolve_lot_and_step() falls through to the heuristic
    strike step and a lot_size of 0 (caller must treat 0 as unresolvable and
    skip the entry, never guess a lot size for real order quantity)."""
    global _lot_cache_loaded
    _lot_cache_loaded = True
    try:
        from curl_cffi import requests as cc
    except ImportError:
        logger.warning("stock_resolve: curl_cffi not installed -- cannot fetch Upstox "
                        "instrument master for lot-size fallback.")
        return
    try:
        r = cc.get(_UPSTOX_NSE_MASTER_URL, impersonate="chrome131", timeout=30)
        instruments = json.loads(gzip.decompress(r.content))
    except Exception as exc:
        logger.warning("stock_resolve: failed to fetch/parse Upstox NSE instrument master: %s", exc)
        return

    lots: dict = {}
    for inst in instruments:
        if inst.get("segment") != "NSE_FO":
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
    logger.info("stock_resolve: Upstox instrument-master lot cache loaded (%d underlyings).", len(lots))


def resolve_lot(stock_symbol: str) -> int:
    """Returns lot_size, or 0 if UNRESOLVABLE -- caller must skip the entry
    rather than guess a lot size for real order quantity. Blocking (network
    on first miss) -- call via asyncio.to_thread() from async code."""
    sym = stock_symbol.upper()
    if sym in FNO_STOCK_CONFIG:
        return fno_stock_lot(sym)

    if not _lot_cache_loaded:
        _load_upstox_lot_cache()
    return _lot_cache.get(sym, 0)


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
