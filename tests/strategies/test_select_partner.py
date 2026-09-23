"""Rollover partner selection — balance the new leg against the running (kept) leg."""

from strategies.straddle_selection import select_partner_for, select_rollover_partner_directional


def _cache(d):
    return {k: {"ltp": v, "atp": v} for k, v in d.items()}


def test_partner_picks_closest_premium_ignoring_ltp_cap():
    # Keep CE (running) at ltp 60. Roll PE — ltp_le_kept is DISABLED by default for rollover,
    # so the new partner is simply the one CLOSEST to 60 (above or below).
    cache = _cache({
        (100, "CE"): 60.0,
        (95,  "PE"): 95.0,   # diff 35
        (105, "PE"): 62.0,   # diff 2   ← closest
        (110, "PE"): 58.0,   # diff 2   (encountered after 105)
        (115, "PE"): 40.0,   # diff 20
    })
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=60.0,
                             spot=100, step=5, offset=4, ltp_target=30.0,
                             rule_pass=lambda cs, ps: True)
    assert res == (105, 62.0)


def test_partner_can_enforce_ltp_le_kept():
    # With ltp_le_kept=True, the partner must NOT be richer than the kept leg.
    cache = _cache({
        (100, "CE"): 60.0,
        (95,  "PE"): 95.0,   # > 60 -> EXCLUDED
        (105, "PE"): 62.0,   # > 60 -> EXCLUDED
        (110, "PE"): 58.0,   # <= 60, diff 2   ← best eligible
        (115, "PE"): 40.0,   # <= 60, diff 20
    })
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=60.0,
                             spot=100, step=5, offset=4, ltp_target=30.0,
                             rule_pass=lambda cs, ps: True, ltp_le_kept=True)
    assert res == (110, 58.0)


def test_partner_respects_ltp_target_and_rules():
    # 2026-08-27: ltp_target no longer gates rollover candidates -- this now tests
    # that rule_pass alone can reject every candidate.
    cache = _cache({(100, "CE"): 60.0, (105, "PE"): 62.0, (110, "PE"): 20.0})
    res = select_partner_for(cache, "PE", 100, 60.0, 100, 5, 4, 30.0,
                             rule_pass=lambda cs, ps: False)
    assert res is None


def test_partner_uses_balanced_ratio_metric():
    # kept CE @100. Two PE candidates:
    #   PE95 @30 -> diff 70, ratio 70/130 = 0.538
    #   PE105 @180 -> diff 80, ratio 80/280 = 0.286
    # With balanced_ratio the lower ratio wins even though it is farther in absolute terms.
    cache = _cache({
        (100, "CE"): 100.0,
        (95, "PE"): 30.0,
        (105, "PE"): 180.0,
    })
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=100.0,
                             spot=100, step=5, offset=4, ltp_target=10.0,
                             rule_pass=lambda cs, ps: True, metric="balanced_ratio")
    assert res == (105, 180.0)


def test_partner_none_when_no_strikes():
    res = select_partner_for(_cache({(100, "CE"): 60.0}), "PE", 100, 60.0,
                             100, 5, 4, 30.0, rule_pass=lambda cs, ps: True)
    assert res is None


# ── anchor_strike expanding-ring search (2026-08-26, direct user spec) ──────
# "check all strikes, but take the strike 100 [i.e. one ring] diff from the
# strike we're closing" -- rather than the old ATM-centered global-best-match
# search. Ring 1 = anchor+/-step tried first; only widens outward if neither
# ring-1 candidate passes every existing filter.

def test_anchor_ring_picks_ring1_tiebreak_by_balanced_ratio():
    # anchor(closing strike)=100, step=5 -> ring 1 = {95, 105}. Both pass with
    # the SAME absolute diff from kept_ltp=60 (diff=5 each), so balanced_ratio
    # breaks the tie: ratio95=5/115=0.0435 vs ratio105=5/125=0.04 -> 105 wins.
    cache = _cache({(100, "CE"): 60.0, (95, "PE"): 55.0, (105, "PE"): 65.0})
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=60.0,
                             spot=100, step=5, offset=4, ltp_target=10.0,
                             rule_pass=lambda cs, ps: True, metric="balanced_ratio",
                             anchor_strike=100)
    assert res == (105, 65.0)


def test_anchor_ring_falls_back_to_other_side_of_ring1():
    # Only PE95 is quoted at all -- PE105 has no quote in the pool -- ring 1
    # still succeeds via the one side that IS quoted.
    cache = _cache({(100, "CE"): 60.0, (95, "PE"): 55.0})
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=60.0,
                             spot=100, step=5, offset=4, ltp_target=10.0,
                             rule_pass=lambda cs, ps: True, anchor_strike=100)
    assert res == (95, 55.0)


def test_anchor_ring_widens_to_ring2_when_ring1_fully_fails():
    # 2026-08-27: the dual-floor (ltp_target/theta_target) no longer applies during
    # rollover -- ring-widening is now exercised via ltp_le_kept (never roll into a
    # richer leg than the one being kept) instead.
    # Ring 1 (95, 105): both pricier than kept_ltp=60 -> fail ltp_above_kept.
    # Ring 2 (90, 110): 110 is <= kept_ltp -> ring 2 wins.
    cache = _cache({
        (100, "CE"): 60.0,
        (95, "PE"): 70.0,    # ring 1, fails ltp_above_kept
        (105, "PE"): 75.0,   # ring 1, fails ltp_above_kept
        (90, "PE"): 80.0,    # ring 2, fails ltp_above_kept
        (110, "PE"): 45.0,   # ring 2, passes
    })
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=60.0,
                             spot=100, step=5, offset=4, ltp_target=30.0,
                             rule_pass=lambda cs, ps: True, anchor_strike=100,
                             ltp_le_kept=True)
    assert res == (110, 45.0)


def test_anchor_ring_none_when_every_ring_exhausted():
    # 2026-08-27: see comment above -- exercised via ltp_le_kept, not the removed floor.
    cache = _cache({(100, "CE"): 60.0, (95, "PE"): 70.0, (105, "PE"): 75.0})
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=60.0,
                             spot=100, step=5, offset=2, ltp_target=30.0,
                             rule_pass=lambda cs, ps: True, anchor_strike=100,
                             ltp_le_kept=True)
    assert res is None


def test_anchor_ring_never_considers_a_farther_ring_once_ring1_passes():
    # Ring 1 candidate exists and passes -- a "better" (lower ratio) candidate
    # sitting in ring 2 must NOT be picked; proximity to the closed strike
    # wins over a marginally better balance score farther away.
    cache = _cache({
        (100, "CE"): 60.0,
        (105, "PE"): 58.0,   # ring 1, passes (diff=2)
        (110, "PE"): 60.0,   # ring 2, would be a PERFECT balance match (diff=0)
    })
    res = select_partner_for(cache, roll_side="PE", kept_strike=100, kept_ltp=60.0,
                             spot=100, step=5, offset=4, ltp_target=10.0,
                             rule_pass=lambda cs, ps: True, metric="balanced_ratio",
                             anchor_strike=100)
    assert res == (105, 58.0)


# ── select_rollover_partner_directional (2026-08-27, direct user spec: "one major
# change in rollover" -- the worked example below is the user's own Call-side
# rollover scenario, reproduced exactly) ────────────────────────────────────

def test_directional_rejects_candidate_closer_than_min_gap_even_with_good_premium():
    # Losing leg: PE22500 @150 (kept). Exiting leg: CE22000. 21950 CE is only
    # 50pts from the closing strike -- must be ignored even though its premium
    # (140 <= 150) is a good match.
    cache = _cache({(22500, "PE"): 150.0, (21950, "CE"): 140.0})
    res = select_rollover_partner_directional(
        cache, roll_side="CE", kept_strike=22500, kept_ltp=150.0, closing_strike=22000,
        spot=21980, real_step=50, min_gap_pts=100, rule_pass=lambda cs, ps: True,
    )
    assert res is None


def test_directional_accepts_first_candidate_that_clears_min_gap():
    # Same scenario -- 21900 CE is exactly 100pts from 22000 and clears the gap;
    # its premium (140) is <= the kept PE's 150 -- must be selected.
    cache = _cache({(22500, "PE"): 150.0, (21950, "CE"): 999.0, (21900, "CE"): 140.0})
    res = select_rollover_partner_directional(
        cache, roll_side="CE", kept_strike=22500, kept_ltp=150.0, closing_strike=22000,
        spot=21980, real_step=50, min_gap_pts=100, rule_pass=lambda cs, ps: True,
    )
    assert res == (21900, 140.0)


def test_directional_falls_back_to_away_from_spot_when_toward_spot_finds_nothing():
    # 2026-09-23, direct user spec, real live incident: the toward-spot-only
    # direction used to give up entirely here, even though a real, valid,
    # gap-clearing candidate sits on the OTHER side (farther OTM) -- confirmed
    # live, a kept leg repeatedly found "no valid partner" while the pool
    # diagnostic dump in the SAME log line showed several real, fresh, cheap
    # candidates just past the closing strike in the away-from-spot direction.
    # Now the fallback finds it: nothing toward spot (22100/CE has no quote in
    # this cache at all within reach), so the search tries the opposite
    # direction and finds CE22100 -- gap-clearing, premium (10.0) well below
    # the kept leg's 150.0.
    cache = _cache({(22500, "PE"): 150.0, (22100, "CE"): 10.0})  # above closing_strike, fallback direction
    res = select_rollover_partner_directional(
        cache, roll_side="CE", kept_strike=22500, kept_ltp=150.0, closing_strike=22000,
        spot=21980, real_step=50, min_gap_pts=100, rule_pass=lambda cs, ps: True,
    )
    assert res == (22100, 10.0)


def test_directional_toward_spot_still_wins_over_fallback_when_both_have_candidates():
    # The 2026-09-23 fallback must never override the existing 2026-08-27
    # "prefer toward-spot" design when the primary direction genuinely has a
    # valid candidate -- CE21950 (toward spot, gap 50<100 skipped), CE21900
    # (toward spot, gap 100, real candidate) must win over CE22100 (away from
    # spot, also a real candidate, but the fallback must never even run once
    # the primary direction already succeeded).
    cache = _cache({(22500, "PE"): 150.0, (21900, "CE"): 80.0, (22100, "CE"): 10.0})
    res = select_rollover_partner_directional(
        cache, roll_side="CE", kept_strike=22500, kept_ltp=150.0, closing_strike=22000,
        spot=21980, real_step=50, min_gap_pts=100, rule_pass=lambda cs, ps: True,
    )
    assert res == (21900, 80.0)


def test_directional_fallback_picks_closest_to_kept_not_first_found():
    # Unlike the primary (first-that-passes) search, the fallback must pick
    # the BEST match (closest to, but not above, kept_ltp) across the whole
    # away-from-spot range, not just the first one encountered -- CE22100
    # (ltp=10.0, far from kept 150.0) is found first walking outward, but
    # CE22300 (ltp=140.0, much closer to 150.0) must win instead.
    cache = _cache({(22500, "PE"): 150.0, (22100, "CE"): 10.0, (22300, "CE"): 140.0})
    res = select_rollover_partner_directional(
        cache, roll_side="CE", kept_strike=22500, kept_ltp=150.0, closing_strike=22000,
        spot=21980, real_step=50, min_gap_pts=100, rule_pass=lambda cs, ps: True,
    )
    assert res == (22300, 140.0)


def test_directional_put_side_searches_upward_toward_spot():
    # PE rolls UP toward spot (mirror of the CE example). Closing strike 22000 PE,
    # kept CE @150. 22050 (50pts) must be skipped; 22100 (100pts, premium<=150) wins.
    cache = _cache({(22500, "CE"): 150.0, (22050, "PE"): 140.0, (22100, "PE"): 145.0})
    res = select_rollover_partner_directional(
        cache, roll_side="PE", kept_strike=22500, kept_ltp=150.0, closing_strike=22000,
        spot=22020, real_step=50, min_gap_pts=100, rule_pass=lambda cs, ps: True,
    )
    assert res == (22100, 145.0)


def test_directional_enforces_premium_le_kept_leg():
    # 21900 clears the gap but is pricier than the kept leg (150) -- rejected;
    # no candidate within reach passes, so the roll finds no partner.
    cache = _cache({(22500, "PE"): 150.0, (21900, "CE"): 160.0})
    res = select_rollover_partner_directional(
        cache, roll_side="CE", kept_strike=22500, kept_ltp=150.0, closing_strike=22000,
        spot=21980, real_step=50, min_gap_pts=100, rule_pass=lambda cs, ps: True,
    )
    assert res is None


def test_directional_none_when_nothing_within_max_search_steps():
    cache = _cache({(22500, "PE"): 150.0, (21400, "CE"): 50.0})  # far beyond the search reach
    res = select_rollover_partner_directional(
        cache, roll_side="CE", kept_strike=22500, kept_ltp=150.0, closing_strike=22000,
        spot=21980, real_step=50, min_gap_pts=100, rule_pass=lambda cs, ps: True,
        max_search_steps=4,   # only reaches 22000-200=21800
    )
    assert res is None


def test_directional_itm_cap_uses_its_own_step_unit_not_the_real_grid_step():
    # 2026-08-27 regression: the ITM-depth safety cap must be measured in
    # itm_cap_step_pts (default/explicit 100), NOT real_step (50) -- otherwise
    # max_itm_steps silently becomes twice as strict as before. spot=21980,
    # candidate 21500 is 480pts ITM for a CE -- passes under a 100pt-unit cap of
    # max_itm_steps=5 (500pt reach) but would fail under a 50pt-unit cap (250pt reach).
    cache = _cache({(22500, "PE"): 150.0, (21500, "CE"): 140.0})
    res = select_rollover_partner_directional(
        cache, roll_side="CE", kept_strike=22500, kept_ltp=150.0, closing_strike=22000,
        spot=21980, real_step=50, min_gap_pts=100, rule_pass=lambda cs, ps: True,
        max_itm_steps=5, max_search_steps=20, itm_cap_step_pts=100.0,
    )
    assert res == (21500, 140.0)
