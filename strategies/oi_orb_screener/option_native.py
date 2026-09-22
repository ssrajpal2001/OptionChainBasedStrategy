"""
strategies/oi_orb_screener/option_native.py -- pure logic for the
option-contract-native entry/exit mechanic layered on top of the existing
OI-Spurt + top-gainer/loser stock shortlist (unchanged, see screener.py).

Implements Layers 1 (delta-band contract selection helper; the DB/registry-
touching wrapper lives in stock_resolve.py), 2 (5-min feature-bar merge), 3
(CE-vs-PE scoring + side selection) and 4 (entry/hold/exit) of the plan at
C:\\Users\\SERVER\\.claude\\plans\\immutable-popping-sphinx.md. Every rule here
implements one of that plan's 14 frozen decisions verbatim -- see that
file's own docstring-equivalent comments below for which decision each
function encodes. Mirrors the removed oi_swing.py's shape: dataclasses +
pure functions, no asyncio, no I/O, fully unit-testable in isolation from
the live engine.

Terminology reminder (frozen decision, do not get backwards): `option_type`
("CE"/"PE") identifies a contract's type. Delta is purely a selection/filter
criterion applied WITHIN an already-known option_type -- never inferred from
delta's sign.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import Dict, List, Optional, Tuple

from strategies.core.candle_indicators import to_n_min_bars_market_anchored
from strategies.core.trap_zone_utils import Bar

# ── Layer 1: delta-band contract selection (pure candidate-picking logic;
# the impure REGISTRY/ResolvedContract-building wrapper is
# stock_resolve.resolve_delta_band_contract, which calls this) ────────────

# Decision 8 / tie-break rule: near-ties within this epsilon of the minimum
# abs(delta - target_delta) distance are broken by liquidity (volume, then
# OI), never treated as a second scoring pass.
DEFAULT_TIE_EPSILON = 0.01


def select_delta_band_candidate(candidates: List[dict], delta_min: float, delta_max: float,
                                 target_delta: float,
                                 tie_epsilon: float = DEFAULT_TIE_EPSILON) -> Optional[dict]:
    """candidates: [{"strike", "delta", "volume", "oi", "bid", "ask", "iv",
    "upstox_key", ...}, ...] for ONE option_type (CE or PE) only -- the
    caller is responsible for splitting a raw chain into CE/PE lists before
    calling this (see parse_chain_side_candidates below).

    Decision 8: if no strike on this side falls in [delta_min, delta_max],
    returns None -- caller skips this side entirely for the day, no
    automatic band-widening, no substitute rule.

    Tie-break rule (frozen): among all in-band candidates, pick the one
    whose delta is CLOSEST to target_delta (min abs distance). If two or
    more candidates are within `tie_epsilon` of the best distance, break
    the tie by highest `volume`, then highest `oi` if still tied -- pure
    liquidity tie-break, never a second full score."""
    in_band = [c for c in candidates
               if c.get("delta") is not None and delta_min <= c["delta"] <= delta_max]
    if not in_band:
        return None
    scored = [(abs(c["delta"] - target_delta), c) for c in in_band]
    best_dist = min(d for d, _ in scored)
    near_ties = [c for d, c in scored if (d - best_dist) <= tie_epsilon]
    if len(near_ties) == 1:
        return near_ties[0]
    near_ties.sort(key=lambda c: (-(c.get("volume") or 0), -(c.get("oi") or 0)))
    return near_ties[0]


def parse_chain_side_candidates(raw_chain: dict, option_type: str) -> List[dict]:
    """Normalizes Upstox's real /v2/option/chain response shape (confirmed
    live 2026-09-xx against TCS, spot 2105 -- see the plan's own Context
    section) into the flat candidate-dict shape select_delta_band_candidate
    expects. `raw_chain` is the dict returned by
    GlobalFeeder.fetch_option_chain() (resp.to_dict()) -- a top-level
    {"data": [...]} envelope, one row per strike, each row carrying a
    "call_options"/"put_options" sub-dict with its own "instrument_key",
    "market_data" ({"ltp","bid_price","ask_price","volume","oi", ...}) and
    "option_greeks" ({"delta","iv", ...}).

    Defensive throughout (every key via .get()) -- a genuinely malformed or
    partially-degenerate row (e.g. a deep ITM/OTM strike with zeroed
    greeks, confirmed possible per the plan's own Context section) is
    simply excluded rather than raising, since contract selection only
    ever targets near-ATM strikes anyway."""
    side_key = "call_options" if option_type == "CE" else "put_options"
    out: List[dict] = []
    for row in (raw_chain or {}).get("data") or []:
        side = row.get(side_key) or {}
        if not side:
            continue
        market = side.get("market_data") or {}
        greeks = side.get("option_greeks") or {}
        strike = row.get("strike_price")
        delta = greeks.get("delta")
        if strike is None or delta is None:
            continue
        out.append({
            "strike": strike,
            "delta": delta,
            "iv": greeks.get("iv"),
            "theta": greeks.get("theta"),
            "gamma": greeks.get("gamma"),
            "vega": greeks.get("vega"),
            "bid": market.get("bid_price"),
            "ask": market.get("ask_price"),
            "ltp": market.get("ltp"),
            "volume": market.get("volume") or 0,
            "oi": market.get("oi") or 0,
            "prev_oi": market.get("prev_oi"),
            "upstox_key": side.get("instrument_key", ""),
        })
    return out


# ── Layer 2: 5-min feature bar ──────────────────────────────────────────

@dataclass
class OptionFeatureBar:
    """One fully-COMPLETED, market-open-anchored 5-min bar for one option
    contract (decision 9). ltp_* come from the live WS OptionTick stream
    (LTP/OI/volume, item 4's cumulative-volume-delta discipline already
    applied by the caller before construction); bid/ask/iv/delta come from
    the periodic REST option-chain poll (decision: never faked from the WS
    tick, since the live WS feed does not reliably carry these); vwap is
    this contract's OWN independently-computed running VWAP (decision 11)."""
    bucket_ts: datetime
    symbol: str
    option_type: str
    upstox_key: str
    ltp_open: float
    ltp_high: float
    ltp_low: float
    ltp_close: float
    volume_5min: float
    oi_close: Optional[float] = None     # 5-min OI level (diagnostic classify_oi_price_reversal)
    change_oi: Optional[float] = None    # broker change_oi field at bucket close (score item 1)
    bid: Optional[float] = None
    ask: Optional[float] = None
    iv: Optional[float] = None
    delta: Optional[float] = None
    vwap: Optional[float] = None


def merge_feature_bar(bucket_ts: datetime, symbol: str, option_type: str, upstox_key: str,
                       ltp_open: float, ltp_high: float, ltp_low: float, ltp_close: float,
                       volume_5min: float, oi_close: Optional[float] = None,
                       change_oi: Optional[float] = None,
                       rest_snapshot: Optional[dict] = None,
                       vwap: Optional[float] = None) -> OptionFeatureBar:
    """Merges the WS-tick-derived OHLCV/OI bar with the latest available
    REST-chain snapshot (bid/ask/iv/delta) at this bucket's close, per Layer
    2's own merge contract -- `rest_snapshot` is whatever the most recent
    poll returned for this exact contract (may be stale by up to
    option_native_poll_seconds; caller's problem, not this function's --
    this function only ever merges what it's given)."""
    rest_snapshot = rest_snapshot or {}
    return OptionFeatureBar(
        bucket_ts=bucket_ts, symbol=symbol, option_type=option_type, upstox_key=upstox_key,
        ltp_open=ltp_open, ltp_high=ltp_high, ltp_low=ltp_low, ltp_close=ltp_close,
        volume_5min=volume_5min, oi_close=oi_close, change_oi=change_oi,
        bid=rest_snapshot.get("bid"), ask=rest_snapshot.get("ask"),
        iv=rest_snapshot.get("iv"), delta=rest_snapshot.get("delta"), vwap=vwap,
    )


@dataclass
class TickBar:
    """A completed 1-min bar for one option contract side, carrying volume
    and OI level alongside plain OHLC -- trap_zone_utils.Bar has no
    volume/OI field, so this is a small local superset built from the live
    WS OptionTick stream, not a duplicate of a different concept."""
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    oi: Optional[float] = None
    change_oi: Optional[float] = None


def bucket_5min_bars(bars_1m: List[TickBar], symbol: str, option_type: str, upstox_key: str,
                      anchor_hour: int = 9, anchor_min: int = 15) -> List[OptionFeatureBar]:
    """Buckets a growing list of COMPLETED 1-min TickBars into completed,
    market-open-anchored 5-min OptionFeatureBars (decision 9). OHLC comes
    directly from candle_indicators.to_n_min_bars_market_anchored -- the
    SAME real function this package's VWAP-close SL mechanic already uses
    (see engine.py's _vwap_close_sl_check), never the midnight-aligned
    to_n_min_bars sibling. volume/OI are aggregated using the IDENTICAL
    (date, (minutes_since_open // n)) bucket key the market-anchored
    bucketer itself uses internally, so the two aggregations can never
    disagree about which 1-min bars belong to which 5-min bucket.

    Only FULLY completed buckets are returned -- the last (still-forming)
    bucket in the series is always excluded, same "never trust a forming
    bucket" discipline candle_indicators.py's own module docstring already
    establishes for this package. bid/ask/iv/delta/vwap are deliberately
    left unset on every returned bar -- the caller (engine.py) fills those
    in via merge_feature_bar with whatever REST snapshot / VWAP state
    applies at each bucket's own close."""
    if not bars_1m:
        return []
    plain_bars = [Bar(ts=b.ts, open=b.open, high=b.high, low=b.low, close=b.close) for b in bars_1m]
    ohlc_5m = to_n_min_bars_market_anchored(plain_bars, 5, anchor_hour, anchor_min)
    if not ohlc_5m:
        return []

    anchor_mins = anchor_hour * 60 + anchor_min
    grouped: Dict[tuple, List[TickBar]] = {}
    for b in bars_1m:
        mins = b.ts.hour * 60 + b.ts.minute
        key = (b.ts.date(), (mins - anchor_mins) // 5)
        grouped.setdefault(key, []).append(b)
    last_key = max(grouped.keys())

    out: List[OptionFeatureBar] = []
    for ohlc_bar in ohlc_5m:
        mins = ohlc_bar.ts.hour * 60 + ohlc_bar.ts.minute
        key = (ohlc_bar.ts.date(), (mins - anchor_mins) // 5)
        if key == last_key:
            continue   # still-forming bucket -- never trusted as "completed"
        group = sorted(grouped.get(key, []), key=lambda x: x.ts)
        if not group:
            continue
        out.append(OptionFeatureBar(
            bucket_ts=ohlc_bar.ts, symbol=symbol, option_type=option_type, upstox_key=upstox_key,
            ltp_open=ohlc_bar.open, ltp_high=ohlc_bar.high, ltp_low=ohlc_bar.low,
            ltp_close=ohlc_bar.close, volume_5min=sum(x.volume for x in group),
            oi_close=group[-1].oi, change_oi=group[-1].change_oi,
        ))
    return out


# ── Layer 3: CE-vs-PE scoring + side selection ──────────────────────────

# The exact condition keys written into score_breakdown / DB score_breakdown
# JSON -- kept as a module constant so store.py / engine.py / tests all
# agree on the field names.
SCORE_CONDITIONS = (
    "ltp_above_vwap", "ltp_rising", "volume_rising", "oi_rising",
    "bid_rising", "tight_spread", "delta_in_band", "iv_supportive",
)


def score_option_side(current: OptionFeatureBar, previous: Optional[OptionFeatureBar],
                       max_spread_pct: float) -> Tuple[int, Dict[str, object]]:
    """The exact 8-point score (frozen, verbatim from the spec table):

        LTP > ATP/VWAP                +1
        LTP rising (5-min)            +1
        Volume rising                 +1
        OI rising (ΔOI > 0)           +1   (item 1 -- change_oi directly)
        Bid rising (5-min)            +1
        Tight spread (<= max_spread_pct)  +1   (item 5)
        Delta in band                 +1   (always true once scored -- Layer 1
                                             already filtered on this; kept for
                                             score auditability)
        IV supportive (rising)        +1   (item 6)

    Every "X increasing" comparison is current-completed-bucket vs
    previous-completed-bucket (decision 9) -- `previous=None` (first bar of
    the day for this contract) means every rising/falling comparison that
    needs it scores False, never a guessed True.

    Returns (score 0-8, breakdown dict) -- breakdown carries every
    condition's boolean AND the raw spread_pct used, for full DB
    auditability (score_breakdown JSON column)."""
    breakdown: Dict[str, object] = {}

    c_ltp_above_vwap = current.vwap is not None and current.ltp_close > current.vwap
    breakdown["ltp_above_vwap"] = c_ltp_above_vwap

    c_ltp_rising = previous is not None and current.ltp_close > previous.ltp_close
    breakdown["ltp_rising"] = c_ltp_rising

    c_volume_rising = previous is not None and current.volume_5min > previous.volume_5min
    breakdown["volume_rising"] = c_volume_rising

    # Item 1: score's "OI rising" uses change_oi DIRECTLY -- never the 5-min
    # OI-level comparison (that's the separate diagnostic classification
    # below, classify_oi_price_reversal -- do not conflate the two).
    c_oi_rising = current.change_oi is not None and current.change_oi > 0
    breakdown["oi_rising"] = c_oi_rising

    c_bid_rising = (previous is not None and current.bid is not None and previous.bid is not None
                    and current.bid > previous.bid)
    breakdown["bid_rising"] = c_bid_rising

    spread_pct: Optional[float] = None
    if current.bid is not None and current.ask is not None:
        mid = (current.ask + current.bid) / 2.0
        if mid > 0:
            spread_pct = (current.ask - current.bid) / mid * 100.0
    c_tight_spread = spread_pct is not None and spread_pct <= max_spread_pct
    breakdown["tight_spread"] = c_tight_spread
    breakdown["spread_pct"] = spread_pct

    # Delta-in-band is guaranteed True once a bar is even being scored --
    # Layer 1 selection already filtered to only in-band strikes. Kept as
    # an explicit, always-True condition (not omitted) purely for score
    # auditability, per the plan's own note: this means min_score=6
    # effectively requires 5 of the 7 truly-variable conditions -- do not
    # "simplify" this away, it's an intentional, documented equivalence.
    c_delta_in_band = True
    breakdown["delta_in_band"] = c_delta_in_band

    c_iv_supportive = (previous is not None and current.iv is not None and previous.iv is not None
                        and current.iv > previous.iv)
    breakdown["iv_supportive"] = c_iv_supportive

    score = sum(1 for k in SCORE_CONDITIONS if breakdown[k])
    return score, breakdown


def select_winning_side(ce_score: Optional[int], pe_score: Optional[int],
                         min_score: int, min_score_gap: int) -> Optional[str]:
    """Decision 2 (min_score) + decision 3 (min_score_gap) + decision 8
    (one-sided availability): `ce_score`/`pe_score` are None when that side
    had no in-band contract selected at all today (Layer 1 skip) -- NOT the
    same as a scored-but-losing side.

    - Both None -> no trade (neither side ever qualified for a contract).
    - Exactly one side present (the other None) -> that side trades
      independently purely on decision 2 (must independently clear
      min_score); it never needs to "beat" a nonexistent other side, so
      decision 3's gap check does not apply.
    - Both present -> the higher-scoring side wins ONLY IF it independently
      clears min_score AND its score minus the loser's score is >=
      min_score_gap. Both conditions required together (decision 3, exact
      wording: "and" -- not either alone)."""
    if ce_score is None and pe_score is None:
        return None
    if pe_score is None:
        return "CE" if ce_score is not None and ce_score >= min_score else None
    if ce_score is None:
        return "PE" if pe_score >= min_score else None

    if ce_score >= pe_score:
        winner, winner_score, loser_score = "CE", ce_score, pe_score
    else:
        winner, winner_score, loser_score = "PE", pe_score, ce_score

    if winner_score < min_score:
        return None
    if (winner_score - loser_score) < min_score_gap:
        return None
    return winner


# ── diagnostic: 4(+1)-state OI/price reversal classification (item 7) ───

OI_PRICE_LONG_BUILDUP = "long_buildup"
OI_PRICE_SHORT_BUILDUP = "short_buildup"
OI_PRICE_LONG_UNWINDING = "long_unwinding"
OI_PRICE_SHORT_COVERING = "short_covering"
OI_PRICE_NEUTRAL = "neutral"
OI_PRICE_INSUFFICIENT_DATA = "insufficient_data"


def classify_oi_price_reversal(current_oi: Optional[float], previous_oi: Optional[float],
                                current_ltp: Optional[float],
                                previous_ltp: Optional[float]) -> str:
    """Diagnostic-only (item 7) -- NEVER a hard/automatic exit, purely
    logged every completed 5-min bar for any held position so it can be
    reviewed later. Uses the actual 5-min OI LEVEL comparison
    (current_5min_oi vs previous_5min_oi), NEVER `change_oi` -- do not
    conflate this with score_option_side's own "OI rising" condition,
    which is a completely separate comparison on a completely separate
    field (frozen, explicit in the plan).

    current_5min_oi == previous_5min_oi is its OWN explicit "neutral"
    output (item 7's 5th state), not forced into one of the 4 classic
    buildup/unwinding/covering labels regardless of what price did that
    bar. Missing data (either side unavailable) is its own distinct
    "insufficient_data" output, never silently defaulted to neutral or a
    guessed state.

    Classic OI/price matrix once OI has genuinely moved:
        price up   + OI up   -> long_buildup    (new longs entering)
        price up   + OI down -> short_covering  (shorts exiting into strength)
        price down + OI up   -> short_buildup   (new shorts entering)
        price down + OI down -> long_unwinding  (longs exiting into weakness)
    A flat/unchanged price with OI genuinely moved falls into the "not
    price up" branch (short_buildup/long_unwinding) -- price direction has
    only two branches (up vs not-up) since OI equality already has its own
    dedicated neutral state above."""
    if current_oi is None or previous_oi is None or current_ltp is None or previous_ltp is None:
        return OI_PRICE_INSUFFICIENT_DATA
    if current_oi == previous_oi:
        return OI_PRICE_NEUTRAL
    oi_up = current_oi > previous_oi
    price_up = current_ltp > previous_ltp
    if price_up and oi_up:
        return OI_PRICE_LONG_BUILDUP
    if price_up and not oi_up:
        return OI_PRICE_SHORT_COVERING
    if not price_up and oi_up:
        return OI_PRICE_SHORT_BUILDUP
    return OI_PRICE_LONG_UNWINDING


# ── Layer 4: entry -> hold -> exit ──────────────────────────────────────

EXIT_REASON_LTP_BELOW_VWAP = "ltp_below_own_vwap"
EXIT_REASON_BROKE_PREV_LOW = "broke_prev_5min_low"
EXIT_REASON_ADVERSE_LIQUIDITY = "adverse_liquidity"
EXIT_REASON_EOD = "eod_squareoff"


def check_native_exit(current: OptionFeatureBar, previous: Optional[OptionFeatureBar],
                       now_time: dtime, eod_time: dtime) -> Tuple[bool, str]:
    """V1 exit rule (frozen, consistent with item 7 -- OI/price reversal is
    diagnostic-only and never checked here):

        EXIT if:
          LTP <= own VWAP
          OR
          LTP breaks the previous completed 5-min bar's low
          OR
          adverse liquidity: bid_now < bid_prev AND ask_now > ask_prev
                              (both on completed 5-min buckets, decision 9)
          OR
          EOD (decision 12, 15:15)

    Hold = none of the above true. Checked in this exact order (first
    match wins) purely for a stable, single exit_reason per bar -- all
    branches are otherwise independent, non-overlapping decisions on the
    same bar."""
    if now_time >= eod_time:
        return True, EXIT_REASON_EOD
    if current.vwap is not None and current.ltp_close <= current.vwap:
        return True, EXIT_REASON_LTP_BELOW_VWAP
    if previous is not None and current.ltp_close < previous.ltp_low:
        return True, EXIT_REASON_BROKE_PREV_LOW
    if (previous is not None
            and current.bid is not None and previous.bid is not None
            and current.ask is not None and previous.ask is not None
            and current.bid < previous.bid and current.ask > previous.ask):
        return True, EXIT_REASON_ADVERSE_LIQUIDITY
    return False, ""
