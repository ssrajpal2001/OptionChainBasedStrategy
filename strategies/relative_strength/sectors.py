"""
strategies/relative_strength/sectors.py -- sector universe + live NSE
constituent-stock fetching for the Relative Strength scanner (2026-09-24,
direct user spec: fetch sector constituents live from NSE, not a hardcoded
list).

Reuses the NSESession HTTP-session pattern from
strategies/oi_orb_screener/screener.py (cookie warm-up, gzip-only
Accept-Encoding, retry-with-rewarm-on-401/403) -- this is generic NSE-fetch
plumbing already proven live in this codebase, not another strategy's own
decision logic, so it's imported directly rather than re-implemented (unlike
the "zero shared runtime" mandate that applies to each strategy's own
trading logic).

NOT yet verified against live NSE/Upstox data -- built directly per spec
(user explicitly said no backtest/validation pass is needed for this
feature). Run a real scan before trusting the sector name -> Upstox
instrument_key resolution or the NSE constituent response shape blindly.
"""
from __future__ import annotations

import gzip
import json
import logging
from typing import Dict, List, Optional

from strategies.oi_orb_screener.screener import NSESession

logger = logging.getLogger(__name__)

# Standard NSE broad sectoral indices, as their exact NSE display names
# (used both as the ?index= query param on NSE's own constituent endpoint,
# and as the fuzzy-match target against Upstox's NSE_INDEX instrument
# master below). Curated list, not fetched live -- NSE has no clean "list
# every sectoral index" endpoint to enumerate this from; the CONSTITUENT
# STOCKS within each of these are what's fetched live, per direct user
# choice.
SECTOR_INDEX_NAMES: List[str] = [
    "NIFTY AUTO",
    "NIFTY BANK",
    "NIFTY FIN SERVICE",
    "NIFTY FMCG",
    "NIFTY IT",
    "NIFTY MEDIA",
    "NIFTY METAL",
    "NIFTY PHARMA",
    "NIFTY PSU BANK",
    "NIFTY PVT BANK",
    "NIFTY REALTY",
    "NIFTY HEALTHCARE",       # CONFIRMED live 2026-09-24: Upstox's NSE_INDEX
                              # master calls this "NIFTY HEALTHCARE", not
                              # "NIFTY HEALTHCARE INDEX" -- the original
                              # guess resolved to nothing on a real run.
    "NIFTY CONSR DURBL",      # CONFIRMED live 2026-09-24: Upstox's real name
                              # is the abbreviated "NIFTY CONSR DURBL", not
                              # "NIFTY CONSUMER DURABLES" -- same class of
                              # miss as the Healthcare one above.
    "NIFTY OIL AND GAS",
    # 2026-09-24, direct user follow-up: market-cap TIER indices, not
    # sectoral ones (they cut across every industry) -- added alongside the
    # sector list per explicit user request, using the SAME RS-vs-NIFTY
    # scan. All 7 confirmed live against both the Upstox instrument master
    # AND the NSE constituent endpoint (which is case-sensitive and uses
    # NSE's own abbreviated "SMLCAP"/no-space "MICROCAP250" spelling --
    # "SMALLCAP"/"MICROCAP 250" both returned 0 real constituents).
    "NIFTY MIDCAP 50",
    "NIFTY MIDCAP 100",
    "NIFTY MIDCAP 150",
    "NIFTY SMLCAP 50",
    "NIFTY SMLCAP 100",
    "NIFTY SMLCAP 250",
    "NIFTY MICROCAP250",
    # 2026-09-24, direct user follow-up (from a TradingView symbol-search
    # screenshot of NSE "CNX*" indices): more thematic/broad-market
    # indices, same both-endpoints-confirmed discipline as every entry
    # above -- several use Upstox/NSE's own abbreviated spelling
    # ("Serv Sector" not "Services Sector", "Div Opps 50" not "Dividend
    # Opportunities 50", "Infra" not "Infrastructure", "Consumption" not
    # "India Consumption") which the unabbreviated guess returned 0
    # constituents for.
    "NIFTY ENERGY",
    "NIFTY SERV SECTOR",
    "NIFTY DIV OPPS 50",
    "NIFTY CONSUMPTION",
    "NIFTY COMMODITIES",
    "NIFTY MNC",
    "NIFTY INFRA",
    "NIFTY PSE",
    # Broad market-cap indices (not sector/theme-specific, same category as
    # the MIDCAP/SMLCAP/MICROCAP tiers above) -- included per the same
    # direct user request.
    "NIFTY 100",
    "NIFTY 200",
    "NIFTY 500",
]

_UPSTOX_NSE_MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

_index_key_cache: Dict[str, str] = {}
_index_key_cache_loaded = False


def _load_upstox_index_key_cache() -> None:
    """Downloads the same public Upstox NSE instrument master
    strategies/oi_orb_screener/stock_resolve.py already uses for its own
    NSE_EQ lot/key lookups, but keeps only NSE_INDEX segment rows here --
    this guarantees the REAL instrument_key string Upstox actually uses for
    each sector index (e.g. 'NSE_INDEX|Nifty Bank'), rather than guessing a
    display-name capitalization convention that could silently mismatch.
    Never raises -- resolve_index_key() returns None on any failure, same
    degrade-safely convention as stock_resolve.py's own lot-cache loader."""
    global _index_key_cache_loaded
    try:
        from curl_cffi import requests as cc
    except ImportError:
        logger.warning("sectors: curl_cffi not installed -- cannot fetch Upstox NSE "
                        "instrument master for sector-index key resolution.")
        _index_key_cache_loaded = True
        return
    try:
        r = cc.get(_UPSTOX_NSE_MASTER_URL, impersonate="chrome131", timeout=30)
        instruments = json.loads(gzip.decompress(r.content))
    except Exception as exc:
        logger.warning("sectors: failed to fetch/parse Upstox NSE instrument master: %s", exc)
        _index_key_cache_loaded = True
        return

    keys: Dict[str, str] = {}
    for inst in instruments:
        if inst.get("segment") != "NSE_INDEX":
            continue
        name = str(inst.get("name") or inst.get("trading_symbol") or "").strip().upper()
        ikey = inst.get("instrument_key", "")
        if name and ikey:
            keys[name] = ikey
    _index_key_cache.update(keys)
    _index_key_cache_loaded = True
    logger.info("sectors: Upstox NSE_INDEX key cache loaded (%d indices).", len(keys))


def resolve_index_key(index_name: str) -> Optional[str]:
    """Real Upstox instrument_key for a sector index name (case-insensitive
    match against the downloaded master), or None if unresolvable. Loads
    the master at most once per process lifetime, same caching discipline
    as stock_resolve.py's own lot cache."""
    if not _index_key_cache_loaded:
        _load_upstox_index_key_cache()
    return _index_key_cache.get(index_name.strip().upper())


_SECTOR_CONSTITUENTS_URL = "https://www.nseindia.com/api/NextApi/apiClient/marketWatchApi"


def fetch_sector_constituents(nse: NSESession, index_name: str) -> List[str]:
    """Live NSE constituent stock symbols for one sectoral index.

    2026-09-24 CORRECTION, confirmed live: the originally-guessed
    'api/equity-stockIndices' endpoint returns a genuine HTTP 404
    ('Resource not found') -- NOT a bot-block page, the path is simply
    wrong on NSE's current site. This is the exact same class of dead
    endpoint already documented in this codebase's OI-ORB screener history
    (its own 'equity-stock-indices guess... confirmed dead 2026-08-24').
    The REAL working endpoint, confirmed via direct probing, is the SAME
    NextApi/apiClient/marketWatchApi endpoint oi_orb_screener's own
    fetch_fno_price_universe already uses for the whole F&O universe --
    just with symbol=<sector index name> instead of
    symbol='SECURITIES IN F&O'. Same double-nested response shape as that
    function: {"data": {"aduCount": {...}, "data": [...rows...]}}.

    Returns [] on any failure -- caller degrades safely, same convention
    as every other NSE fetch in this codebase. The index's own summary row
    (present in the same response, distinguished by priority=1) is
    excluded -- only real constituent stock rows (priority=0) are
    returned."""
    payload = nse.get_json(_SECTOR_CONSTITUENTS_URL,
                            params={"functionName": "getIndicesData", "symbol": index_name})
    outer = (payload or {}).get("data")
    rows = (outer or {}).get("data") if isinstance(outer, dict) else None
    if not rows:
        return []
    symbols = [
        str(row.get("symbol", "")).strip()
        for row in rows
        if row.get("symbol") and int(row.get("priority", 0) or 0) == 0
    ]
    return [s for s in symbols if s]


def fetch_all_sector_constituents(
    nse: NSESession, index_names: Optional[List[str]] = None,
) -> Dict[str, List[str]]:
    """Constituent stocks for every sector in `index_names` (defaults to
    SECTOR_INDEX_NAMES), keyed by index name. A sector whose fetch fails
    (network error, NSE endpoint change) is simply omitted, not a fatal
    error for the whole scan -- other sectors still get ranked."""
    names = index_names if index_names is not None else SECTOR_INDEX_NAMES
    out: Dict[str, List[str]] = {}
    for name in names:
        stocks = fetch_sector_constituents(nse, name)
        if stocks:
            out[name] = stocks
        else:
            logger.warning("sectors: no constituents returned for %s -- skipping this sector.", name)
    return out
