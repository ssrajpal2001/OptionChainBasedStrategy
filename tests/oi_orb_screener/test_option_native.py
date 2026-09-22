"""
2026-09-22: unit tests for strategies/oi_orb_screener/option_native.py --
the pure Layer 2/3/4 logic for the option-contract-native entry/exit
mechanic (see that module's own docstring, and the approved plan at
C:\\Users\\SERVER\\.claude\\plans\\immutable-popping-sphinx.md for the 14
frozen decisions each test below is pinned to), plus
stock_resolve.resolve_delta_band_contract (Layer 1's impure wrapper).
"""
from datetime import date, datetime, time as dtime

import pytest

from data_layer.instrument_registry import REGISTRY
from strategies.oi_orb_screener import option_native as on
from strategies.oi_orb_screener import stock_resolve


def _bar(ltp_close, vwap=None, volume=100.0, change_oi=0.0, bid=None, ask=None, iv=None,
         oi_close=None, ltp_low=None, bucket_ts=None):
    return on.OptionFeatureBar(
        bucket_ts=bucket_ts or datetime(2026, 9, 22, 9, 25),
        symbol="TCS", option_type="CE", upstox_key="NSE_FO|1",
        ltp_open=ltp_close, ltp_high=ltp_close, ltp_low=ltp_low if ltp_low is not None else ltp_close,
        ltp_close=ltp_close, volume_5min=volume, oi_close=oi_close, change_oi=change_oi,
        bid=bid, ask=ask, iv=iv, vwap=vwap,
    )


# ── select_delta_band_candidate / resolve_delta_band_contract (Layer 1) ────

def test_select_delta_band_candidate_none_in_band_returns_none():
    candidates = [{"strike": 2000, "delta": 0.20, "volume": 100, "oi": 10}]
    assert on.select_delta_band_candidate(candidates, 0.45, 0.65, 0.55) is None


def test_select_delta_band_candidate_picks_closest_to_target():
    candidates = [
        {"strike": 2080, "delta": 0.48, "volume": 100, "oi": 10},
        {"strike": 2100, "delta": 0.57, "volume": 50, "oi": 5},    # closest to 0.55
        {"strike": 2120, "delta": 0.63, "volume": 200, "oi": 20},
    ]
    picked = on.select_delta_band_candidate(candidates, 0.45, 0.65, 0.55)
    assert picked["strike"] == 2100


def test_select_delta_band_candidate_tie_break_by_volume_then_oi():
    # Both 0.54 and 0.56 are 0.01 away from target 0.55 -- exactly at the
    # tie epsilon, so both count as a near-tie; higher volume wins.
    candidates = [
        {"strike": 2090, "delta": 0.54, "volume": 100, "oi": 500},
        {"strike": 2110, "delta": 0.56, "volume": 300, "oi": 50},
    ]
    picked = on.select_delta_band_candidate(candidates, 0.45, 0.65, 0.55)
    assert picked["strike"] == 2110


def test_select_delta_band_candidate_tie_break_falls_back_to_oi_when_volume_tied():
    candidates = [
        {"strike": 2090, "delta": 0.54, "volume": 100, "oi": 500},
        {"strike": 2110, "delta": 0.56, "volume": 100, "oi": 900},
    ]
    picked = on.select_delta_band_candidate(candidates, 0.45, 0.65, 0.55)
    assert picked["strike"] == 2110


def test_select_delta_band_candidate_pe_side_negative_target():
    candidates = [
        {"strike": 2080, "delta": -0.40, "volume": 100, "oi": 10},
        {"strike": 2100, "delta": -0.56, "volume": 50, "oi": 5},
        {"strike": 2060, "delta": -0.63, "volume": 200, "oi": 20},
    ]
    picked = on.select_delta_band_candidate(candidates, -0.65, -0.45, -0.55)
    assert picked["strike"] == 2100


def test_parse_chain_side_candidates_real_shape():
    raw_chain = {
        "data": [
            {
                "strike_price": 2100,
                "call_options": {
                    "instrument_key": "NSE_FO|CE2100",
                    "market_data": {"ltp": 35.85, "bid_price": 35.4, "ask_price": 36.0,
                                     "volume": 1200, "oi": 4500, "prev_oi": 4000},
                    "option_greeks": {"delta": 0.5728, "iv": 24.96, "theta": -1.2,
                                       "gamma": 0.002, "vega": 3.1},
                },
                "put_options": {
                    "instrument_key": "NSE_FO|PE2100",
                    "market_data": {"ltp": 20.0, "bid_price": 19.5, "ask_price": 20.5,
                                     "volume": 800, "oi": 3000, "prev_oi": 3100},
                    "option_greeks": {"delta": -0.42, "iv": 22.1, "theta": -1.0,
                                       "gamma": 0.0018, "vega": 2.7},
                },
            },
            {"strike_price": 2200, "call_options": {}, "put_options": None},
        ]
    }
    ce = on.parse_chain_side_candidates(raw_chain, "CE")
    assert len(ce) == 1
    assert ce[0]["strike"] == 2100
    assert ce[0]["delta"] == pytest.approx(0.5728)
    assert ce[0]["bid"] == 35.4 and ce[0]["ask"] == 36.0
    assert ce[0]["upstox_key"] == "NSE_FO|CE2100"
    assert ce[0]["prev_oi"] == 4000

    pe = on.parse_chain_side_candidates(raw_chain, "PE")
    assert len(pe) == 1
    assert pe[0]["delta"] == pytest.approx(-0.42)


def test_resolve_delta_band_contract_picks_and_builds_real_contract(monkeypatch):
    monkeypatch.setattr(REGISTRY, "is_loaded", lambda sym: True)
    monkeypatch.setattr(REGISTRY, "get_broker_symbol", lambda sym, exp, strike, opt, provider: f"{sym}{opt}{strike}")
    raw_chain = {
        "data": [
            {"strike_price": 2100, "call_options": {
                "instrument_key": "NSE_FO|CE2100",
                "market_data": {"bid_price": 35.4, "ask_price": 36.0, "volume": 1200, "oi": 4500},
                "option_greeks": {"delta": 0.5728, "iv": 24.96},
            }},
        ]
    }
    contract = stock_resolve.resolve_delta_band_contract(
        "TCS", date(2026, 9, 25), "CE", 0.45, 0.65, 0.55, raw_chain)
    assert contract is not None
    assert contract.strike == 2100
    assert contract.upstox_key == "NSE_FO|CE2100"
    assert contract.broker_symbols["upstox"] == "TCSCE2100"


def test_resolve_delta_band_contract_returns_none_when_no_in_band_candidate(monkeypatch):
    monkeypatch.setattr(REGISTRY, "is_loaded", lambda sym: True)
    raw_chain = {"data": [{"strike_price": 2100, "call_options": {
        "instrument_key": "NSE_FO|CE2100",
        "market_data": {"bid_price": 1, "ask_price": 2, "volume": 1, "oi": 1},
        "option_greeks": {"delta": 0.10, "iv": 5.0},
    }}]}
    contract = stock_resolve.resolve_delta_band_contract(
        "TCS", date(2026, 9, 25), "CE", 0.45, 0.65, 0.55, raw_chain)
    assert contract is None


def test_resolve_delta_band_contract_returns_none_when_no_upstox_key(monkeypatch):
    monkeypatch.setattr(REGISTRY, "is_loaded", lambda sym: True)
    monkeypatch.setattr(REGISTRY, "get_upstox_key", lambda sym, exp, strike, opt: "")
    raw_chain = {"data": [{"strike_price": 2100, "call_options": {
        "instrument_key": "",   # no key from the chain row itself either
        "market_data": {"bid_price": 35.4, "ask_price": 36.0, "volume": 1200, "oi": 4500},
        "option_greeks": {"delta": 0.5728, "iv": 24.96},
    }}]}
    contract = stock_resolve.resolve_delta_band_contract(
        "TCS", date(2026, 9, 25), "CE", 0.45, 0.65, 0.55, raw_chain)
    assert contract is None


# ── score_option_side (Layer 3) -- every condition independently toggled ──

def test_score_first_bar_of_day_no_previous_scores_only_vwap_oi_spread_delta():
    # previous=None -> ltp_rising/volume_rising/bid_rising/iv_supportive all False.
    current = _bar(ltp_close=100, vwap=95, change_oi=5, bid=99.5, ask=100.0, oi_close=1000)
    score, bd = on.score_option_side(current, None, max_spread_pct=1.0)
    assert bd["ltp_above_vwap"] is True
    assert bd["ltp_rising"] is False
    assert bd["volume_rising"] is False
    assert bd["oi_rising"] is True
    assert bd["bid_rising"] is False
    assert bd["tight_spread"] is True
    assert bd["delta_in_band"] is True
    assert bd["iv_supportive"] is False
    assert score == 4   # vwap, oi_rising, tight_spread, delta_in_band


def test_score_every_condition_true_scores_8():
    previous = _bar(ltp_close=90, volume=50, bid=90, iv=20, ltp_low=88)
    current = _bar(ltp_close=100, vwap=95, volume=80, change_oi=5, bid=95.5, ask=96, iv=25, oi_close=1000)
    score, bd = on.score_option_side(current, previous, max_spread_pct=1.0)
    assert score == 8
    assert all(bd[k] for k in on.SCORE_CONDITIONS)


def test_score_ltp_above_vwap_condition():
    prev = _bar(ltp_close=100)
    below = _bar(ltp_close=94, vwap=95)
    above = _bar(ltp_close=96, vwap=95)
    assert on.score_option_side(below, prev, 1.0)[1]["ltp_above_vwap"] is False
    assert on.score_option_side(above, prev, 1.0)[1]["ltp_above_vwap"] is True
    # No vwap available at all -> never scores True.
    no_vwap = _bar(ltp_close=200, vwap=None)
    assert on.score_option_side(no_vwap, prev, 1.0)[1]["ltp_above_vwap"] is False


def test_score_ltp_rising_condition():
    prev = _bar(ltp_close=100)
    assert on.score_option_side(_bar(ltp_close=101), prev, 1.0)[1]["ltp_rising"] is True
    assert on.score_option_side(_bar(ltp_close=100), prev, 1.0)[1]["ltp_rising"] is False   # equal, not rising
    assert on.score_option_side(_bar(ltp_close=99), prev, 1.0)[1]["ltp_rising"] is False


def test_score_volume_rising_condition():
    prev = _bar(ltp_close=100, volume=100)
    assert on.score_option_side(_bar(ltp_close=100, volume=150), prev, 1.0)[1]["volume_rising"] is True
    assert on.score_option_side(_bar(ltp_close=100, volume=100), prev, 1.0)[1]["volume_rising"] is False
    assert on.score_option_side(_bar(ltp_close=100, volume=50), prev, 1.0)[1]["volume_rising"] is False


def test_score_oi_rising_uses_change_oi_directly_not_oi_level():
    prev = None
    # change_oi > 0 -> True regardless of oi_close level.
    assert on.score_option_side(_bar(ltp_close=100, change_oi=1, oi_close=10), prev, 1.0)[1]["oi_rising"] is True
    # change_oi == 0 -> False.
    assert on.score_option_side(_bar(ltp_close=100, change_oi=0, oi_close=99999), prev, 1.0)[1]["oi_rising"] is False
    # change_oi < 0 -> False.
    assert on.score_option_side(_bar(ltp_close=100, change_oi=-1, oi_close=99999), prev, 1.0)[1]["oi_rising"] is False


def test_score_bid_rising_condition():
    prev = _bar(ltp_close=100, bid=10.0)
    assert on.score_option_side(_bar(ltp_close=100, bid=10.5), prev, 1.0)[1]["bid_rising"] is True
    assert on.score_option_side(_bar(ltp_close=100, bid=10.0), prev, 1.0)[1]["bid_rising"] is False
    assert on.score_option_side(_bar(ltp_close=100, bid=None), prev, 1.0)[1]["bid_rising"] is False


def test_score_tight_spread_boundary_exact_max_counts_as_tight():
    # bid=99.5, ask=100.5 -> mid=100, spread_pct = 1.0/100*100 = 1.0 -- exactly at max_spread_pct.
    current = _bar(ltp_close=100, bid=99.5, ask=100.5)
    score, bd = on.score_option_side(current, None, max_spread_pct=1.0)
    assert bd["spread_pct"] == pytest.approx(1.0)
    assert bd["tight_spread"] is True   # <=, not <
    wide = _bar(ltp_close=100, bid=99.0, ask=101.0)   # spread_pct = 2.0
    assert on.score_option_side(wide, None, max_spread_pct=1.0)[1]["tight_spread"] is False
    no_quote = _bar(ltp_close=100, bid=None, ask=None)
    assert on.score_option_side(no_quote, None, max_spread_pct=1.0)[1]["tight_spread"] is False


def test_score_delta_in_band_is_always_true():
    # Layer 1 already filtered on delta -- this condition never varies once
    # a bar is even being scored (frozen decision, see the plan's own note).
    assert on.score_option_side(_bar(ltp_close=100), None, 1.0)[1]["delta_in_band"] is True


def test_score_iv_supportive_condition():
    prev = _bar(ltp_close=100, iv=20.0)
    assert on.score_option_side(_bar(ltp_close=100, iv=21.0), prev, 1.0)[1]["iv_supportive"] is True
    assert on.score_option_side(_bar(ltp_close=100, iv=20.0), prev, 1.0)[1]["iv_supportive"] is False
    assert on.score_option_side(_bar(ltp_close=100, iv=None), prev, 1.0)[1]["iv_supportive"] is False


# ── select_winning_side (Layer 3) ──────────────────────────────────────

def test_select_winning_side_both_none_no_trade():
    assert on.select_winning_side(None, None, min_score=6, min_score_gap=2) is None


def test_select_winning_side_one_sided_ce_only_trades_independently():
    # No PE candidate existed at all (Layer 1 skip) -- CE only needs its OWN
    # score to clear min_score, no gap check against a nonexistent PE.
    assert on.select_winning_side(6, None, min_score=6, min_score_gap=2) == "CE"
    assert on.select_winning_side(5, None, min_score=6, min_score_gap=2) is None


def test_select_winning_side_one_sided_pe_only_trades_independently():
    assert on.select_winning_side(None, 7, min_score=6, min_score_gap=2) == "PE"
    assert on.select_winning_side(None, 4, min_score=6, min_score_gap=2) is None


def test_select_winning_side_gap_met_but_below_min_score():
    # CE=5, PE=3 -- gap of 2 is satisfied, but winner's own score (5) < min_score (6).
    assert on.select_winning_side(5, 3, min_score=6, min_score_gap=2) is None


def test_select_winning_side_min_score_met_but_gap_too_small():
    # CE=7, PE=6 -- CE clears min_score alone, but gap is only 1 < min_score_gap 2.
    assert on.select_winning_side(7, 6, min_score=6, min_score_gap=2) is None


def test_select_winning_side_both_conditions_met_picks_higher_score():
    assert on.select_winning_side(8, 5, min_score=6, min_score_gap=2) == "CE"
    assert on.select_winning_side(5, 8, min_score=6, min_score_gap=2) == "PE"


def test_select_winning_side_near_tie_score_no_trade():
    assert on.select_winning_side(6, 6, min_score=6, min_score_gap=2) is None


# ── check_native_exit (Layer 4) -- each OR-branch independently ────────

_EOD = dtime(15, 15)
_MID = dtime(11, 0)


def test_check_native_exit_hold_when_nothing_triggers():
    prev = _bar(ltp_close=100, ltp_low=98, bid=10.0, ask=10.5)
    current = _bar(ltp_close=101, vwap=95, ltp_low=100, bid=10.5, ask=10.9)
    exited, reason = on.check_native_exit(current, prev, _MID, _EOD)
    assert exited is False and reason == ""


def test_check_native_exit_ltp_below_vwap():
    current = _bar(ltp_close=94, vwap=95)
    exited, reason = on.check_native_exit(current, None, _MID, _EOD)
    assert exited is True and reason == on.EXIT_REASON_LTP_BELOW_VWAP
    # Equal counts too (<=).
    current_eq = _bar(ltp_close=95, vwap=95)
    assert on.check_native_exit(current_eq, None, _MID, _EOD) == (True, on.EXIT_REASON_LTP_BELOW_VWAP)


def test_check_native_exit_broke_prev_low():
    prev = _bar(ltp_close=100, ltp_low=98)
    current = _bar(ltp_close=97, vwap=50, ltp_low=97)   # vwap far below so that branch doesn't fire first
    exited, reason = on.check_native_exit(current, prev, _MID, _EOD)
    assert exited is True and reason == on.EXIT_REASON_BROKE_PREV_LOW


def test_check_native_exit_adverse_liquidity():
    prev = _bar(ltp_close=100, ltp_low=90, bid=10.0, ask=10.5)
    current = _bar(ltp_close=105, vwap=50, ltp_low=95, bid=9.5, ask=11.0)
    exited, reason = on.check_native_exit(current, prev, _MID, _EOD)
    assert exited is True and reason == on.EXIT_REASON_ADVERSE_LIQUIDITY


def test_check_native_exit_adverse_liquidity_requires_both_legs():
    # bid dropped but ask did NOT rise -- not adverse liquidity.
    prev = _bar(ltp_close=100, ltp_low=90, bid=10.0, ask=10.5)
    current = _bar(ltp_close=105, vwap=50, ltp_low=95, bid=9.5, ask=10.5)
    exited, reason = on.check_native_exit(current, prev, _MID, _EOD)
    assert exited is False


def test_check_native_exit_eod_overrides_everything():
    current = _bar(ltp_close=999, vwap=1)   # would otherwise clearly hold
    exited, reason = on.check_native_exit(current, None, dtime(15, 15), _EOD)
    assert exited is True and reason == on.EXIT_REASON_EOD
    # Past EOD also fires.
    exited2, reason2 = on.check_native_exit(current, None, dtime(15, 20), _EOD)
    assert exited2 is True and reason2 == on.EXIT_REASON_EOD


# ── classify_oi_price_reversal (diagnostic, item 7) ─────────────────────

def test_classify_oi_price_reversal_long_buildup():
    assert on.classify_oi_price_reversal(1100, 1000, 105, 100) == on.OI_PRICE_LONG_BUILDUP


def test_classify_oi_price_reversal_short_covering():
    assert on.classify_oi_price_reversal(900, 1000, 105, 100) == on.OI_PRICE_SHORT_COVERING


def test_classify_oi_price_reversal_short_buildup():
    assert on.classify_oi_price_reversal(1100, 1000, 95, 100) == on.OI_PRICE_SHORT_BUILDUP


def test_classify_oi_price_reversal_long_unwinding():
    assert on.classify_oi_price_reversal(900, 1000, 95, 100) == on.OI_PRICE_LONG_UNWINDING


def test_classify_oi_price_reversal_neutral_uses_oi_level_equality_not_change_oi():
    # OI level unchanged -> neutral, REGARDLESS of price direction.
    assert on.classify_oi_price_reversal(1000, 1000, 105, 100) == on.OI_PRICE_NEUTRAL
    assert on.classify_oi_price_reversal(1000, 1000, 95, 100) == on.OI_PRICE_NEUTRAL


def test_classify_oi_price_reversal_insufficient_data():
    assert on.classify_oi_price_reversal(None, 1000, 105, 100) == on.OI_PRICE_INSUFFICIENT_DATA
    assert on.classify_oi_price_reversal(1100, None, 105, 100) == on.OI_PRICE_INSUFFICIENT_DATA
    assert on.classify_oi_price_reversal(1100, 1000, None, 100) == on.OI_PRICE_INSUFFICIENT_DATA
    assert on.classify_oi_price_reversal(1100, 1000, 105, None) == on.OI_PRICE_INSUFFICIENT_DATA


# ── merge_feature_bar ──────────────────────────────────────────────────

def test_merge_feature_bar_merges_rest_snapshot_and_vwap():
    bar = on.merge_feature_bar(
        bucket_ts=datetime(2026, 9, 22, 9, 30), symbol="TCS", option_type="CE",
        upstox_key="NSE_FO|1", ltp_open=100, ltp_high=102, ltp_low=99, ltp_close=101,
        volume_5min=500, oi_close=1000, change_oi=10,
        rest_snapshot={"bid": 100.5, "ask": 101.5, "iv": 22.0, "delta": 0.55}, vwap=98.5,
    )
    assert bar.bid == 100.5 and bar.ask == 101.5 and bar.iv == 22.0 and bar.delta == 0.55
    assert bar.vwap == 98.5
    assert bar.ltp_close == 101


def test_merge_feature_bar_missing_rest_snapshot_leaves_fields_none():
    bar = on.merge_feature_bar(
        bucket_ts=datetime(2026, 9, 22, 9, 30), symbol="TCS", option_type="PE",
        upstox_key="NSE_FO|2", ltp_open=50, ltp_high=51, ltp_low=49, ltp_close=50,
        volume_5min=100,
    )
    assert bar.bid is None and bar.ask is None and bar.iv is None and bar.delta is None
    assert bar.vwap is None


# ── bucket_5min_bars -- reuses candle_indicators.to_n_min_bars_market_anchored ──

def test_bucket_5min_bars_uses_market_anchored_not_midnight_aligned():
    """The first real bucket must start at 09:15 (market open), not 09:00
    (midnight-aligned) -- proves to_n_min_bars_market_anchored is actually
    being used, not the plain to_n_min_bars sibling (same real incident
    class this package's own VWAP-close SL mechanic already hit once)."""
    bars = []
    t = datetime(2026, 9, 22, 9, 15)
    price = 100.0
    # 12 one-minute bars: 09:15..09:26 -- two full 5-min buckets
    # (09:15-09:20, 09:20-09:25) completed, the third still forming.
    for i in range(12):
        ts = datetime(2026, 9, 22, 9, 15 + i) if i < 45 else t
        bars.append(on.TickBar(ts=datetime(2026, 9, 22, 9, 15) .replace(minute=15 + i),
                                open=price, high=price + 1, low=price - 1, close=price,
                                volume=10.0, oi=1000 + i, change_oi=1))
        price += 1

    completed = on.bucket_5min_bars(bars, "TCS", "CE", "NSE_FO|1")
    # Exactly 2 completed buckets (09:15 and 09:20 starts) -- the 09:25
    # bucket (only 2 bars: 09:25, 09:26) is still forming and excluded.
    assert [b.bucket_ts.strftime("%H:%M") for b in completed] == ["09:15", "09:20"]
    first = completed[0]
    assert first.volume_5min == 50.0   # 5 bars * 10.0 each
    assert first.oi_close == 1000 + 4  # last bar (09:19) of that bucket
    assert first.change_oi == 1


def test_bucket_5min_bars_still_forming_bucket_never_returned():
    bars = [on.TickBar(ts=datetime(2026, 9, 22, 9, 15), open=100, high=100, low=100, close=100,
                        volume=5.0, oi=1000, change_oi=1),
            on.TickBar(ts=datetime(2026, 9, 22, 9, 16), open=100, high=101, low=100, close=100.5,
                       volume=5.0, oi=1001, change_oi=1)]
    completed = on.bucket_5min_bars(bars, "TCS", "CE", "NSE_FO|1")
    assert completed == []


def test_bucket_5min_bars_empty_input():
    assert on.bucket_5min_bars([], "TCS", "CE", "NSE_FO|1") == []
