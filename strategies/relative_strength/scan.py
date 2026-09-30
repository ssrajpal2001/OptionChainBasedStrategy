"""
strategies/relative_strength/scan.py -- orchestrates the full 5-step scan
(direct user spec, 2026-09-24):

STEP 3: fetch every sector index's WEEKLY candles + NIFTY's own WEEKLY
        candles, rank sectors by relative strength vs NIFTY.
STEP 4: take the best-performing sector, fetch its real constituent stocks
        live from NSE, fetch each stock's HOURLY candles + the sector
        index's own HOURLY candles, rank stocks by relative strength vs
        their own sector.
STEP 5: return the ranked, filtered result.

No backtest/validation pass (direct user instruction) -- this is a live
scanner only. NOT yet run against real live data; the RS math itself
(detector.py) is unit-tested, but the NSE constituent response shape and
the Upstox sector-index-key resolution (sectors.py) are unverified until a
real run happens.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import List, Optional

from data_layer.historical_candles import fetch_upstox_weekly, fetch_upstox_hourly
from strategies.oi_orb_screener.screener import NSESession
from strategies.oi_orb_screener.stock_resolve import resolve_eq_instrument_key
from strategies.relative_strength.detector import (
    align_closes, evaluate_relative_strength, rank_by_relative_strength, RSReading,
)
from strategies.relative_strength.sectors import (
    SECTOR_INDEX_NAMES, resolve_index_key, fetch_all_sector_constituents,
    fetch_large_cap_universe,
)

logger = logging.getLogger(__name__)

_RS_LENGTH = 123
_RS_TREND_BASE = 5
_RS_MA_LENGTH = 50

_NIFTY_KEY = "NSE_INDEX|Nifty 50"

# 2026-09-30, direct user decision ("weight toward small/mid-cap") for the
# "wealth creation" use case this scanner exists for: a stock whose symbol
# appears in the live-fetched NIFTY 100 (blue-chip) membership set has its
# RS score multiplied by this factor BEFORE ranking -- a genuinely strong
# blue-chip signal can still win if its raw RS clears the penalty, but a
# mid/small-cap stock with a similar raw RS will now consistently rank
# higher on the ranking. This is intentionally a WEIGHT, not a hard filter
# (avoids two traps at once: missing a genuinely strong large-cap move, and
# giving an illiquid microcap an unfair boost just for being small) -- see
# sectors.py's
# fetch_large_cap_universe() docstring for why NIFTY 100 is the real,
# live-fetched blue-chip proxy rather than a second hardcoded guess-list.
_LARGE_CAP_RS_WEIGHT = 0.7


@dataclass
class SectorScanResult:
    sector_name: str
    sector_key: str
    rs: RSReading


@dataclass
class StockScanResult:
    symbol: str
    stock_key: str
    rs: RSReading
    is_large_cap: bool = False
    cap_weighted_rs: float = 0.0


@dataclass
class FullScanResult:
    sectors_ranked: List[SectorScanResult]
    best_sector: Optional[SectorScanResult]
    stocks_ranked: List[StockScanResult]


async def scan_sectors(access_token: str) -> List[SectorScanResult]:
    """STEP 3: rank every sector in SECTOR_INDEX_NAMES against NIFTY on
    WEEKLY candles. A sector whose Upstox instrument_key can't be resolved,
    or whose candle history is too short for the RS lookback, is simply
    omitted (logged, not fatal) -- matches this codebase's established
    degrade-safely convention for a multi-symbol scan."""
    nifty_candles = await fetch_upstox_weekly(_NIFTY_KEY, access_token)
    if not nifty_candles:
        logger.warning("relative_strength: no weekly candles for NIFTY -- cannot scan sectors.")
        return []

    results: List[SectorScanResult] = []
    for name in SECTOR_INDEX_NAMES:
        # resolve_index_key does a blocking network fetch on its first-ever
        # call (downloads + parses the Upstox NSE instrument master) --
        # never call it directly from an async function without to_thread.
        key = await asyncio.to_thread(resolve_index_key, name)
        if not key:
            logger.warning("relative_strength: could not resolve Upstox instrument_key for "
                            "sector %s -- skipping.", name)
            continue
        candles = await fetch_upstox_weekly(key, access_token)
        if not candles:
            logger.warning("relative_strength: no weekly candles for sector %s (%s) -- skipping.",
                            name, key)
            continue
        base, comp = align_closes(candles, nifty_candles)
        reading = evaluate_relative_strength(
            name, base, comp, length=_RS_LENGTH, trend_base=_RS_TREND_BASE, ma_length=_RS_MA_LENGTH,
        )
        if reading is None:
            logger.warning("relative_strength: not enough aligned weekly history for sector %s "
                            "(%d common bars, need >%d) -- skipping.", name, len(base), _RS_LENGTH)
            continue
        results.append(SectorScanResult(sector_name=name, sector_key=key, rs=reading))

    ranked_readings = rank_by_relative_strength([r.rs for r in results])
    by_symbol = {r.rs.symbol: r for r in results}
    return [by_symbol[reading.symbol] for reading in ranked_readings]


async def scan_sector_stocks(
    sector_name: str, sector_key: str, access_token: str, nse: Optional[NSESession] = None,
) -> List[StockScanResult]:
    """STEP 4: rank every real constituent stock of `sector_name` (fetched
    live from NSE) against the sector's OWN index, on HOURLY candles. Same
    degrade-safely-per-symbol convention as scan_sectors -- one stock's
    failure never aborts the whole ranking."""
    sector_candles = await fetch_upstox_hourly(sector_key, access_token)
    if not sector_candles:
        logger.warning("relative_strength: no hourly candles for sector index %s -- "
                        "cannot rank its stocks.", sector_name)
        return []

    # NSESession.__init__ does a blocking warm-up GET; fetch_all_sector_
    # constituents does further blocking requests -- both to_thread'd.
    if nse is None:
        nse = await asyncio.to_thread(NSESession)
    constituents = await asyncio.to_thread(
        lambda: fetch_all_sector_constituents(nse, [sector_name]).get(sector_name, []),
    )
    if not constituents:
        logger.warning("relative_strength: no live NSE constituents found for sector %s.", sector_name)
        return []

    # 2026-09-30: real, live-fetched blue-chip membership (NIFTY 100), used
    # below to apply the mid/small-cap weighting -- see _LARGE_CAP_RS_WEIGHT's
    # own comment. A fetch failure degrades to an empty set (no penalty
    # applied this run), never a fatal error for the whole scan.
    large_caps = await asyncio.to_thread(fetch_large_cap_universe, nse)

    results: List[StockScanResult] = []
    for symbol in constituents:
        # resolve_eq_instrument_key is blocking on its own first-ever call
        # (same Upstox master download as resolve_index_key above) -- see
        # its own docstring's explicit "call via asyncio.to_thread()" note.
        stock_key = await asyncio.to_thread(resolve_eq_instrument_key, symbol)
        if not stock_key:
            logger.warning("relative_strength: could not resolve Upstox NSE_EQ key for %s -- skipping.",
                            symbol)
            continue
        candles = await fetch_upstox_hourly(stock_key, access_token)
        if not candles:
            logger.warning("relative_strength: no hourly candles for %s -- skipping.", symbol)
            continue
        base, comp = align_closes(candles, sector_candles)
        reading = evaluate_relative_strength(
            symbol, base, comp, length=_RS_LENGTH, trend_base=_RS_TREND_BASE, ma_length=_RS_MA_LENGTH,
        )
        if reading is None:
            logger.warning("relative_strength: not enough aligned hourly history for %s "
                            "(%d common bars, need >%d) -- skipping.", symbol, len(base), _RS_LENGTH)
            continue
        is_large_cap = symbol in large_caps
        weighted_rs = reading.rs * (_LARGE_CAP_RS_WEIGHT if is_large_cap else 1.0)
        results.append(StockScanResult(
            symbol=symbol, stock_key=stock_key, rs=reading,
            is_large_cap=is_large_cap, cap_weighted_rs=weighted_rs,
        ))

    # Ranked by the CAP-WEIGHTED score (mid/small-cap preference), not the
    # raw RS -- reading.rs itself is left untouched on each result so the
    # caller/report can still show the true, unweighted RS value alongside
    # the cap-aware rank. Same trend tie-break rule as
    # detector.rank_by_relative_strength (rising > flat > falling on a
    # near-tied score), reimplemented here since it now sorts on
    # cap_weighted_rs, not r.rs.
    _trend_rank = {"rising": 0, "flat": 1, "falling": 2, None: 1}
    results.sort(key=lambda r: (-round(r.cap_weighted_rs, 6), _trend_rank.get(r.rs.rs_trend, 1)))
    return results


async def run_full_scan(access_token: str, top_n_stocks: int = 10) -> FullScanResult:
    """STEP 3-5 end to end: rank sectors, take the single best one, rank its
    stocks, return the full picture (not just the winner) so a human can
    review the sector ranking that led to the stock shortlist, not only the
    final answer."""
    sectors_ranked = await scan_sectors(access_token)
    if not sectors_ranked:
        return FullScanResult(sectors_ranked=[], best_sector=None, stocks_ranked=[])

    best = sectors_ranked[0]
    stocks_ranked = await scan_sector_stocks(best.sector_name, best.sector_key, access_token)
    return FullScanResult(
        sectors_ranked=sectors_ranked, best_sector=best,
        stocks_ranked=stocks_ranked[:top_n_stocks] if top_n_stocks > 0 else stocks_ranked,
    )
