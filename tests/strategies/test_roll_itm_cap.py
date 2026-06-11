"""Rollover partner selection must keep the re-sold leg near ATM (max_itm_steps cap) so a roll
re-pairs a real straddle instead of selling a deep-ITM strike (user issue 2026-06-11: scalable-TSL
roll re-entered CE 23100 / PE 23350 deep ITM with spot ~23205)."""
from strategies.straddle_selection import select_partner_for

_RP = lambda cs, ps: True


def _pool():
    # spot 23205, step 50, keep CE 23150 @192 → roll PE side
    return {
        (23200, "PE"): {"ltp": 185.0},   # ATM
        (23250, "PE"): {"ltp": 190.0},   # ~1 step ITM, balanced
        (23350, "PE"): {"ltp": 191.0},   # ~3 steps ITM (145 pts) — deep
    }


def test_no_cap_picks_deepest_balanced():
    r = select_partner_for(_pool(), "PE", 23150, 192.0, 23205, 50, 6, 50, _RP)
    assert r[0] == 23350   # closest to 192 from below, no ITM cap


def test_cap_excludes_deep_itm():
    r = select_partner_for(_pool(), "PE", 23150, 192.0, 23205, 50, 6, 50, _RP, max_itm_steps=2)
    assert r[0] == 23250   # 23350 (145 ITM) skipped → nearest-ATM eligible


def test_cap_allows_otm_and_atm():
    # A CE roll: OTM strikes (strike > spot) are never ITM, always allowed under the cap.
    pool = {(23250, "CE"): {"ltp": 120.0}, (23300, "CE"): {"ltp": 100.0}}
    r = select_partner_for(pool, "CE", 23100, 130.0, 23205, 50, 6, 50, _RP, max_itm_steps=1)
    assert r is not None and r[0] in (23250, 23300)
