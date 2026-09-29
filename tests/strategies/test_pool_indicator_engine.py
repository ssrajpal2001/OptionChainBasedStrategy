import numpy as np
from strategies.pool_indicator_engine import PoolIndicatorEngine

def test_pair_indicators_combined_close_and_vwap():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    # (ce_ltp, ce_atp, pe_ltp, pe_atp) per 1-min bar
    bars = [(50, 49, 40, 39), (51, 50, 41, 40), (52, 51, 42, 41)]
    # minute >= _SESSION_START_MIN (555 = 9:15) -- VWAP/SLOPE are LIVE-only by
    # design (2026-08-19 Seed VWAP Contamination fix); commit_bar()'s default
    # auto-increment starts at minute=0, which the same live/seed boundary
    # would treat as pre-session seed data and correctly omit slope/vwap for.
    for i, (cl, ca, pl, pa) in enumerate(bars):
        eng.update_tick(100, "CE", cl, ca)
        eng.update_tick(100, "PE", pl, pa)
        eng.commit_bar(minute=555 + i)
    ind = eng.pair_indicators(100, 100)
    assert ind["close"] == 52 + 42
    assert ind["vwap"] == 51 + 41
    assert round(ind["slope"], 6) == round((51 + 41) - (50 + 40), 6)


# 2026-08-25 CRITICAL FIX regression: real incident, a client rule referencing
# SLOPE>SLOPE_PREV(1m) could never fire on ANY session (confirmed live, N/A for
# over an hour of fully-warmed operation) because pair_indicators()/
# pair_indicators_tf() never returned a "slope_prev" key at all.

def test_pair_indicators_slope_prev_absent_with_only_two_bars():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    bars = [(50, 49, 40, 39), (51, 50, 41, 40)]
    for i, (cl, ca, pl, pa) in enumerate(bars):
        eng.update_tick(100, "CE", cl, ca)
        eng.update_tick(100, "PE", pl, pa)
        eng.commit_bar(minute=555 + i)
    ind = eng.pair_indicators(100, 100)
    assert "slope" in ind
    assert "slope_prev" not in ind   # needs a 3rd bar -- must not fabricate one


def test_pair_indicators_slope_prev_present_with_three_bars():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    # combined vwap per bar: 89, 91, 96, 92
    bars = [(50, 49, 40, 40), (51, 50, 41, 41), (52, 51, 46, 45), (53, 52, 41, 40)]
    for i, (cl, ca, pl, pa) in enumerate(bars):
        eng.update_tick(100, "CE", cl, ca)
        eng.update_tick(100, "PE", pl, pa)
        eng.commit_bar(minute=555 + i)
    ind = eng.pair_indicators(100, 100)
    # vwaps: 89, 91, 96, 92 -> slope = 92-96=-4, slope_prev = 96-91=5
    assert round(ind["slope"], 6) == -4.0
    assert round(ind["slope_prev"], 6) == 5.0


# 2026-09-29 CRITICAL FIX regression: real live incident. A freshly-subscribed
# strike's very first committed bar at market open can genuinely be (ltp=0,
# atp=0) -- the exchange just opened and that strike hasn't traded yet. The
# old code included that stored zero bar in the slope history purely because
# its minute was >= session start, so the first REAL bar afterward computed
# slope = real_vwap - 0, a false multi-hundred-point spike, instead of
# correctly waiting for a second genuinely-traded bar.

def test_pair_indicators_slope_ignores_leading_zero_atp_bar():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    # Minute 555 (09:15): CE hasn't traded yet this session -> atp=0 stored bar.
    eng.update_tick(100, "CE", 0.0, 0.0)
    eng.update_tick(100, "PE", 40.0, 39.0)
    eng.commit_bar(minute=555)
    # Minute 556 (09:16): CE gets its first real trade.
    eng.update_tick(100, "CE", 50.0, 49.0)
    eng.update_tick(100, "PE", 41.0, 40.0)
    eng.commit_bar(minute=556)
    ind = eng.pair_indicators(100, 100)
    # Only ONE genuinely-traded bar exists so far -- slope must NOT fabricate
    # a false spike off the stored zero bar (old bug: slope = 89 - 0 = 89).
    assert "slope" not in ind
    # Minute 557 (09:17): a second genuinely-traded bar -- NOW slope is valid,
    # computed only from the two real bars.
    eng.update_tick(100, "CE", 51.0, 50.0)
    eng.update_tick(100, "PE", 42.0, 41.0)
    eng.commit_bar(minute=557)
    ind2 = eng.pair_indicators(100, 100)
    assert round(ind2["slope"], 6) == round((50 + 41) - (49 + 40), 6)


def test_pair_indicators_tf_slope_ignores_leading_zero_atp_group():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    eng.update_tick(100, "CE", 0.0, 0.0)
    eng.update_tick(100, "PE", 40.0, 39.0)
    eng.commit_bar(minute=555)
    eng.update_tick(100, "CE", 50.0, 49.0)
    eng.update_tick(100, "PE", 41.0, 40.0)
    eng.commit_bar(minute=556)
    ind = eng.pair_indicators_tf(100, 100, tf=1)
    assert "slope" not in ind
    eng.update_tick(100, "CE", 51.0, 50.0)
    eng.update_tick(100, "PE", 42.0, 41.0)
    eng.commit_bar(minute=557)
    ind2 = eng.pair_indicators_tf(100, 100, tf=1)
    assert round(ind2["slope"], 6) == round((50 + 41) - (49 + 40), 6)


def test_pair_indicators_tf_slope_prev_present_with_three_tf_bars():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    bars = [(50, 49, 40, 40), (51, 50, 41, 41), (52, 51, 46, 45), (53, 52, 41, 40)]
    for i, (cl, ca, pl, pa) in enumerate(bars):
        eng.update_tick(100, "CE", cl, ca)
        eng.update_tick(100, "PE", pl, pa)
        eng.commit_bar(minute=555 + i)
    ind = eng.pair_indicators_tf(100, 100, tf=1)   # tf<=1 delegates to pair_indicators
    assert round(ind["slope"], 6) == -4.0
    assert round(ind["slope_prev"], 6) == 5.0

def test_pair_atp_fresh_true_when_both_recent():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    eng.update_tick(100, "CE", 50, 49)
    eng.update_tick(100, "PE", 40, 39)
    assert eng.pair_atp_fresh(100, 100, max_sec=90) is True
    assert eng.pair_atp_fresh(100, 100, max_sec=0) is True   # disabled → always fresh


def test_pair_atp_fresh_false_when_one_leg_stale():
    import time
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    eng.update_tick(100, "CE", 50, 49)
    eng.update_tick(100, "PE", 40, 39)
    # Age the PE leg's last-good ATP beyond the window (simulate a frozen illiquid leg).
    eng._last_atp_ts[(100, "PE")] = time.time() - 200
    assert eng.pair_atp_fresh(100, 100, max_sec=90) is False
    # A kept-last-good 0 tick must NOT refresh freshness.
    eng.update_tick(100, "PE", 40, 0)
    assert eng.pair_atp_fresh(100, 100, max_sec=90) is False
    # stale_atp flag is surfaced in pair_indicators when stale_sec is passed.
    ind = eng.pair_indicators(100, 100, stale_sec=90)
    assert ind["stale_atp"] == 1.0


def test_pair_rsi_roc_present_when_enough_bars():
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    ce_closes = list(range(50, 70))   # 20 ascending
    for c in ce_closes:
        eng.update_tick(100, "CE", c, c)
        eng.update_tick(100, "PE", 10, 10)   # flat PE every bar
        eng.commit_bar()
    ind = eng.pair_indicators(100, 100)
    assert "rsi" in ind and "roc" in ind
    assert ind["rsi"] > 50

def test_seed_prefills_series_for_rsi():
    from strategies.pool_indicator_engine import PoolIndicatorEngine
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    eng.seed_strike(100, "CE", closes=list(range(50, 70)), atps=list(range(49, 69)))
    eng.seed_strike(100, "PE", closes=[10] * 20, atps=[10] * 20)
    eng.update_tick(100, "CE", 70, 69); eng.update_tick(100, "PE", 10, 10)
    assert eng.is_warm(100, "CE")
    ind = eng.pair_indicators(100, 100)
    assert "rsi" in ind and "roc" in ind

def test_commit_bar_forward_fills_all_strikes():
    # a strike that ticked once keeps advancing on later commits (minute-aligned)
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    eng.update_tick(100, "CE", 50, 50)
    eng.update_tick(100, "PE", 10, 10)
    eng.commit_bar()
    eng.update_tick(100, "CE", 55, 55)   # only CE ticks this minute
    eng.commit_bar()                      # PE forward-fills 10
    ind = eng.pair_indicators(100, 100)
    assert ind["close"] == 55 + 10        # PE held at 10

def test_tf_resample_close_and_vwap_5min():
    from strategies.pool_indicator_engine import PoolIndicatorEngine
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    # 12 one-min bars (minutes 0..11). CE close = 100+min, PE close = 50 (flat). atp = close-1.
    for m in range(12):
        eng.update_tick(100, "CE", 100 + m, 100 + m - 1)
        eng.update_tick(100, "PE", 50, 49)
        eng.commit_bar(minute=m)
    ind = eng.pair_indicators_tf(100, 100, tf=5)
    # groups: 0->mins0-4 (last min4: CE104), 1->mins5-9 (last min9: CE109), 2->mins10-11 INCOMPLETE(dropped)
    # last complete group = 1 -> CE109 + PE50 = 159 close ; vwap = 108 + 49 = 157
    assert ind["close"] == 159
    assert ind["vwap"] == 157
    # slope = group1 vwap (108+49) - group0 vwap (103+49) = 157 - 152 = 5
    assert round(ind["slope"], 6) == 5.0

def test_tf_le_1_delegates_to_1min():
    from strategies.pool_indicator_engine import PoolIndicatorEngine
    eng = PoolIndicatorEngine()
    for m in range(3):
        eng.update_tick(100, "CE", 60 + m, 60 + m); eng.update_tick(100, "PE", 40, 40); eng.commit_bar(minute=m)
    assert eng.pair_indicators_tf(100, 100, tf=1) == eng.pair_indicators(100, 100)

def test_tf_keeps_just_closed_group_at_boundary():
    # minutes 540..544 = one complete 5-min group (g=108); evaluated right at the boundary,
    # BEFORE any minute of the next group exists. The just-closed group must be USED, not dropped.
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    for m in range(540, 545):  # 540,541,542,543,544
        eng.update_tick(100, "CE", 60 + (m - 540), 60 + (m - 540))
        eng.update_tick(100, "PE", 40, 40)
        eng.commit_bar(minute=m)
    ind = eng.pair_indicators_tf(100, 100, tf=5)
    assert ind is not None                      # group 108 is complete -> usable
    assert abs(ind["close"] - (64 + 40)) < 1e-9 # last 1-min bar of the group: CE=64, PE=40

def test_tf_none_when_no_complete_group():
    from strategies.pool_indicator_engine import PoolIndicatorEngine
    eng = PoolIndicatorEngine()
    for m in range(3):  # only 3 bars, tf=5 -> group 0 is incomplete -> dropped -> None
        eng.update_tick(100, "CE", 60, 60); eng.update_tick(100, "PE", 40, 40); eng.commit_bar(minute=m)
    assert eng.pair_indicators_tf(100, 100, tf=5) is None


def test_slope_and_vwap_ignore_seed_atp_contamination():
    # Seeds carry prev-day ATP (here a very different scale). SLOPE/VWAP are intraday and must
    # use LIVE bars only — otherwise the first live slope is a huge seed->live jump (false SLOPE,
    # and a contaminated session_min_vwap -> false vwap_rise_sl). RSI/ROC still use seed warmth.
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    eng.seed_strike(100, "CE", closes=[60] * 20, atps=[1000] * 20)
    eng.seed_strike(100, "PE", closes=[40] * 20, atps=[1000] * 20)
    # one LIVE bar (minute >= _SESSION_START_MIN=555, i.e. 9:15) -> slope
    # unavailable (only 1 live atp), NOT a seed->live jump
    eng.update_tick(100, "CE", 60, 50); eng.update_tick(100, "PE", 40, 50)
    eng.commit_bar(minute=555)
    ind1 = eng.pair_indicators(100, 100)
    assert "slope" not in ind1
    # second LIVE bar -> slope from LIVE atps only
    eng.update_tick(100, "CE", 61, 52); eng.update_tick(100, "PE", 41, 52)
    eng.commit_bar(minute=556)
    ind2 = eng.pair_indicators(100, 100)
    assert "slope" in ind2
    assert abs(ind2["slope"] - ((52 + 52) - (50 + 50))) < 1e-9   # = 4, not ~ -1900
    assert "rsi" in ind2   # seeds still warm RSI


def test_tf_vwap_slope_live_only_rsi_seeded():
    # tf resampling: VWAP/SLOPE from live tf groups only; RSI/ROC keep seed+live closes.
    eng = PoolIndicatorEngine(rsi_len=14, roc_len=10)
    eng.seed_strike(100, "CE", closes=[60] * 30, atps=[1000] * 30)
    eng.seed_strike(100, "PE", closes=[40] * 30, atps=[1000] * 30)
    # 6 live 1-min bars (minutes 540..545) -> two complete 2-min groups (270,271); 272 in-progress
    for i, m in enumerate(range(540, 546)):
        eng.update_tick(100, "CE", 60 + i, 50 + i)
        eng.update_tick(100, "PE", 40, 50)
        eng.commit_bar(minute=m)
    ind = eng.pair_indicators_tf(100, 100, tf=2)
    assert ind is not None
    # vwap from live atp only (~ 50s), NOT ~2000 from seeds
    assert ind["vwap"] < 200
    assert "slope" in ind and abs(ind["slope"]) < 50   # small live delta, not a seed jump
    assert "rsi" in ind                                # seed-warmed


# ── persistence (2026-08-21) -- restart-proofing VWAP/SLOPE, NOT REST-seeding ─

def test_to_dict_load_dict_round_trip_preserves_indicators():
    """The core requirement: an engine restored from a snapshot must produce
    IDENTICAL pair_indicators() output to the original -- proves this is a
    faithful restore of the same live data, not a lossy/altered one."""
    eng = PoolIndicatorEngine(rsi_len=3, roc_len=3)
    for i, m in enumerate(range(555, 562)):   # >= _SESSION_START_MIN (555) -- genuinely live
        eng.update_tick(100, "CE", 60 + i, 50 + i)
        eng.update_tick(100, "PE", 40 + i, 30 + i)
        eng.commit_bar(minute=m)
    original = eng.pair_indicators(100, 100)
    assert original is not None and "slope" in original and "rsi" in original

    snapshot = eng.to_dict()
    restored = PoolIndicatorEngine(rsi_len=3, roc_len=3)
    restored.load_dict(snapshot)

    assert restored.pair_indicators(100, 100) == original


def test_load_dict_preserves_live_vs_seed_minute_boundary():
    """A restored engine must still correctly separate seed (negative
    minute) bars from live (>= _SESSION_START_MIN) bars for VWAP/SLOPE --
    this is the exact invariant the 2026-08-19 'Seed VWAP Contamination' fix
    depends on; a persistence bug that lost minute indices would silently
    reintroduce that bug on every restart."""
    eng = PoolIndicatorEngine(rsi_len=3, roc_len=3)
    eng.seed_strike(100, "CE", closes=[60] * 10, atps=[1000] * 10)   # seed -- huge fake ATP
    eng.seed_strike(100, "PE", closes=[40] * 10, atps=[1000] * 10)
    for i, m in enumerate(range(555, 558)):
        eng.update_tick(100, "CE", 60 + i, 50 + i)   # real, small live ATP
        eng.update_tick(100, "PE", 40, 50)
        eng.commit_bar(minute=m)

    restored = PoolIndicatorEngine(rsi_len=3, roc_len=3)
    restored.load_dict(eng.to_dict())
    ind = restored.pair_indicators(100, 100)
    assert ind is not None
    assert ind["vwap"] < 200, "restored VWAP must still be live-only (~100), not ~2000 from seeds"


def test_load_dict_tolerates_malformed_snapshot():
    eng = PoolIndicatorEngine()
    eng.load_dict({"closes": {"not-a-valid-key": [1, 2, 3]}, "mins": {"100|CE": ["bad", "data"]}})
    assert eng.pair_indicators(100, 100) is None   # nothing usable survived, no crash


def test_load_dict_empty_snapshot_is_a_noop():
    eng = PoolIndicatorEngine()
    eng.load_dict({})
    assert eng.pair_indicators(100, 100) is None
