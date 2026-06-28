"""
strategies/sell_straddle/selection.py — pure candidate-selection math for the
sell-straddle. No async, no EventBus, no I/O. Exact port of the reference
Option_Selling_May_2026 sell_v3 entry_logic.py selection logic, restricted to
feed-available indicators (LTP + broker ATP = VWAP). Unit-testable in isolation.

Cache shape (built by the strategy from option ticks):
    strike_prem: Dict[Tuple[int, str], dict]   # (int strike, "CE"/"PE") -> {"ltp", "atp"}
    prev_atp_closed: Dict[Tuple[int, str], float]  # previous closed-candle ATP per leg
"""
from __future__ import annotations

from typing import Dict, Optional, Tuple

Key = Tuple[int, str]


def select_partner_for(strike_prem, roll_side, kept_strike, kept_ltp,
                       spot, step, offset, ltp_target, rule_pass, max_itm_steps=None):
    """Rollover partner selection — keep the RUNNING leg fixed and pick the best strike on
    `roll_side` to re-sell, BALANCED against the running leg, within ATM±offset, >= ltp_target,
    with premium STRICTLY <= the kept leg's premium (never roll into a leg richer than the leg we
    keep), and passing rule_pass(ce_strike, pe_strike). Among the eligible (<= kept_ltp) strikes it
    picks the one CLOSEST to kept_ltp (most balanced from below).
    `max_itm_steps` (optional): cap how deep ITM the re-sold leg may be (in strike steps) so the
    roll stays near ATM (a real straddle) instead of selling a deep-ITM strike.
    Returns (strike, ltp) or None (→ caller closes all and starts fresh)."""
    atm = round(spot / step) * step if spot > 0 else 0
    best = None  # (premium_diff, strike, ltp)
    for (strike, side), v in strike_prem.items():
        if side != roll_side:
            continue
        if atm and abs(strike - atm) > offset * step:
            continue
        # Keep the re-sold leg near ATM: skip strikes deeper ITM than max_itm_steps.
        if max_itm_steps is not None and spot > 0 and step > 0:
            itm_pts = (spot - strike) if roll_side == "CE" else (strike - spot)  # >0 = ITM
            if itm_pts > max_itm_steps * step:
                continue
        ltp = float(v.get("ltp", 0.0) or 0.0)
        if ltp < ltp_target:
            continue
        if kept_ltp and ltp > float(kept_ltp):
            continue   # strict: partner must NOT be richer than the kept (losing) leg
        ce_s, pe_s = (int(kept_strike), int(strike)) if roll_side == "PE" else (int(strike), int(kept_strike))
        if not rule_pass(ce_s, pe_s):
            continue
        diff = abs(ltp - float(kept_ltp))
        if best is None or diff < best[0]:
            best = (diff, int(strike), ltp)
    return (best[1], best[2]) if best else None


def strip_intrinsic(ltp: float, side: str, strike: float, spot: float) -> float:
    """Time-value-only LTP. CE intrinsic = max(0, spot-strike); PE = max(0, strike-spot)."""
    if side == "CE":
        intrinsic = max(0.0, spot - strike)
    else:
        intrinsic = max(0.0, strike - spot)
    return ltp - intrinsic


def leg_entry_value(side: str, strike: float, ltp: float, spot: float, basis: str) -> float:
    """The per-leg metric the ENTRY threshold filters on. basis='theta' → time value
    (intrinsic-stripped, never negative); anything else → raw LTP. Balancing always stays on
    LTP; only the MIN floor switches metric, so basis='ltp' is byte-identical to the old path."""
    if str(basis).lower() == "theta":
        return max(0.0, strip_intrinsic(float(ltp), side, float(strike), float(spot)))
    return float(ltp)


def pair_indicators(
    strike_prem: Dict[Key, dict],
    prev_atp_closed: Dict[Key, float],
    ce_strike: int,
    pe_strike: int,
) -> Optional[Dict[str, float]]:
    """
    Per-pair indicators from feed data only:
      close = ce_ltp + pe_ltp
      vwap  = ce_atp + pe_atp          (broker ATP, never computed)
      slope = current combined VWAP - previous closed combined VWAP   (if both prev present)
    Returns None if either leg's LTP/ATP is missing or non-positive.
    'slope' key is omitted when either leg lacks a previous closed ATP.
    """
    ce = strike_prem.get((int(ce_strike), "CE"))
    pe = strike_prem.get((int(pe_strike), "PE"))
    if not ce or not pe:
        return None
    ce_ltp, ce_atp = ce.get("ltp", 0.0), ce.get("atp", 0.0)
    pe_ltp, pe_atp = pe.get("ltp", 0.0), pe.get("atp", 0.0)
    if ce_ltp <= 0 or pe_ltp <= 0 or ce_atp <= 0 or pe_atp <= 0:
        return None
    ind: Dict[str, float] = {
        "close": ce_ltp + pe_ltp,
        "vwap": ce_atp + pe_atp,
    }
    ce_prev = prev_atp_closed.get((int(ce_strike), "CE"))
    pe_prev = prev_atp_closed.get((int(pe_strike), "PE"))
    if ce_prev and pe_prev and ce_prev > 0 and pe_prev > 0:
        cur = ce_atp + pe_atp
        prev = ce_prev + pe_prev
        ind["slope"] = cur - prev
    return ind


def select_balanced_pair(
    strike_prem: Dict[Key, dict],
    spot: float,
    step: float,
    offset: int,
    ltp_target: float,
    trace: Optional[list] = None,
    entry_basis: str = "ltp",
    theta_target: float = 0.0,
) -> Optional[Tuple[int, int, float, float]]:
    """
    Beginning concept (reference _get_strictly_lower_balanced_pair):
      1. ATM both sides; require both LTP > 0.
      2. Anchor = side with LOWER time-value (intrinsic-stripped) LTP.
      3. Anchor raw LTP must be >= ltp_target.
      4. Partner = scan other side over ATM +/- offset for ltp_target <= ltp < anchor_ltp;
         pick the HIGHEST such LTP (closest below anchor).
    Returns (ce_strike, pe_strike, ce_ltp, pe_ltp) or None.
    """
    atm = int(round(spot / step) * step)
    ce_atm = strike_prem.get((atm, "CE"))
    pe_atm = strike_prem.get((atm, "PE"))
    if not ce_atm or not pe_atm:
        return None
    ce_ltp = ce_atm.get("ltp", 0.0)
    pe_ltp = pe_atm.get("ltp", 0.0)
    if ce_ltp <= 0 or pe_ltp <= 0:
        return None

    ce_corr = strip_intrinsic(ce_ltp, "CE", atm, spot)
    pe_corr = strip_intrinsic(pe_ltp, "PE", atm, spot)

    _basis = str(entry_basis).lower()
    _floor = float(theta_target) if _basis == "theta" else float(ltp_target)

    if ce_corr < pe_corr:
        anchor_side, anchor_strike, anchor_ltp, partner_side = "CE", atm, ce_ltp, "PE"
    else:
        anchor_side, anchor_strike, anchor_ltp, partner_side = "PE", atm, pe_ltp, "CE"

    if trace is not None:
        trace.append(
            f"ANCHOR atm={atm} ce_tv={ce_corr:.2f} pe_tv={pe_corr:.2f} -> "
            f"anchor={anchor_side}@{anchor_strike} ltp={anchor_ltp:.2f} "
            f"(basis={_basis} need {_basis}>={_floor:.0f}); partner={partner_side} "
            f"wants {_basis}>={_floor:.0f} and ltp<{anchor_ltp:.2f}"
        )

    anchor_val = leg_entry_value(anchor_side, anchor_strike, anchor_ltp, spot, _basis)
    if anchor_val < _floor:
        if trace is not None:
            trace.append(f"REJECT anchor {_basis} {anchor_val:.2f} < target {_floor:.0f}")
        return None

    best = None  # (ltp, strike)
    for i in range(-offset, offset + 1):
        s = int(atm + i * step)
        leg = strike_prem.get((s, partner_side))
        if not leg:
            continue
        ltp = leg.get("ltp", 0.0)
        # Floor on the chosen metric (ltp or time value); balance on LTP (< anchor_ltp).
        val = leg_entry_value(partner_side, s, ltp, spot, _basis)
        _ok = (val >= _floor) and (ltp < anchor_ltp)
        if trace is not None:
            trace.append(
                f"  cand {partner_side}{s} ltp={ltp:.2f} {_basis}={val:.2f} "
                f"{'OK' if _ok else 'skip(out-of-band)'}"
            )
        if _ok:
            if best is None or ltp > best[0]:
                best = (ltp, s)
    if best is None:
        if trace is not None:
            trace.append("NO-PARTNER")
        return None

    partner_ltp, partner_strike = best
    if anchor_side == "CE":
        ce, pe = anchor_strike, partner_strike
        result = (anchor_strike, partner_strike, anchor_ltp, partner_ltp)
    else:
        ce, pe = partner_strike, anchor_strike
        result = (partner_strike, anchor_strike, partner_ltp, anchor_ltp)
    if trace is not None:
        trace.append(f"SELECTED CE{ce}/PE{pe} (beginning)")
    return result


def reentry_block_reason(strike_prem, spot, step, offset, ltp_target, rule_eval):
    """Diagnose why the re-entry pool produced no trade, so the log can distinguish
    'no balanced pair exists' from 'a pair exists but the gate blocked it'.

    rule_eval: callable(ce_strike, pe_strike) -> (passed: bool, reason: str)
    Returns: {"kind": "no_pair"} | {"kind": "blocked"|"passed", ce, pe, ce_ltp, pe_ltp, reason}
    """
    pair = select_balanced_pair(strike_prem, spot, step, offset, ltp_target)
    if not pair:
        return {"kind": "no_pair"}
    ce, pe, ce_ltp, pe_ltp = pair
    passed, reason = rule_eval(ce, pe)
    return {"kind": "passed" if passed else "blocked",
            "ce": ce, "pe": pe, "ce_ltp": ce_ltp, "pe_ltp": pe_ltp, "reason": reason}


def scan_pool(
    strike_prem: Dict[Key, dict],
    spot: float,
    step: float,
    offset: int,
    ltp_target: float,
    rule_pass,                      # callable(ce_strike:int, pe_strike:int) -> bool
    metric: str = "balanced_premium",
    trace: Optional[list] = None,
    entry_basis: str = "ltp",
    theta_target: float = 0.0,
) -> Optional[Tuple[int, int, float, float]]:
    """
    Re-entry concept (reference _scan_v_slope_pool, balanced_premium metric):
      1. Strikes = ATM +/- offset.
      2. ATM bias from corrected ATM LTP: CE stronger if ce_corr > pe_corr.
      3. N x N over (s_ce, s_pe): both LTP >= ltp_target; bias filter
         (CE stronger -> ce_ltp < pe_ltp; else pe_ltp < ce_ltp).
      4. rule_pass(ce_strike, pe_strike) must be True (dynamic technical gate).
      5. balanced_score = abs(ce-pe)/(ce+pe); pick MIN score.
    Returns (ce_strike, pe_strike, ce_ltp, pe_ltp) or None.
    """
    atm = int(round(spot / step) * step)
    ce_atm = strike_prem.get((atm, "CE"))
    pe_atm = strike_prem.get((atm, "PE"))
    if not ce_atm or not pe_atm:
        return None
    ce_corr = strip_intrinsic(ce_atm.get("ltp", 0.0), "CE", atm, spot)
    pe_corr = strip_intrinsic(pe_atm.get("ltp", 0.0), "PE", atm, spot)
    ce_bias_stronger = ce_corr > pe_corr
    _basis = str(entry_basis).lower()
    _floor = float(theta_target) if _basis == "theta" else float(ltp_target)

    if trace is not None:
        trace.append(
            f"ANCHOR atm={atm} ce_tv={ce_corr:.2f} pe_tv={pe_corr:.2f} -> "
            f"bias={'CE' if ce_bias_stronger else 'PE'}-stronger "
            f"(weaker side must have lower ltp)"
        )

    skipped = 0
    strikes = [int(atm + i * step) for i in range(-offset, offset + 1)]
    best = None  # (score, ce_strike, pe_strike, ce_ltp, pe_ltp)
    for s_ce in strikes:
        ce = strike_prem.get((s_ce, "CE"))
        if not ce:
            continue
        ce_ltp = ce.get("ltp", 0.0)
        if ce_ltp <= 0:
            continue
        for s_pe in strikes:
            pe = strike_prem.get((s_pe, "PE"))
            if not pe:
                continue
            pe_ltp = pe.get("ltp", 0.0)
            if pe_ltp <= 0:
                continue
            # Floor each leg on the chosen metric (raw LTP, or time value when basis=theta).
            ce_val = leg_entry_value("CE", s_ce, ce_ltp, spot, _basis)
            pe_val = leg_entry_value("PE", s_pe, pe_ltp, spot, _basis)
            if ce_val < _floor or pe_val < _floor:
                skipped += 1
                continue
            if ce_bias_stronger:
                if ce_ltp >= pe_ltp:
                    skipped += 1
                    continue
            else:
                if pe_ltp >= ce_ltp:
                    skipped += 1
                    continue
            denom = ce_ltp + pe_ltp
            score = abs(ce_ltp - pe_ltp) / denom if denom > 0 else 999.0
            _rp = rule_pass(s_ce, s_pe)
            if trace is not None:
                trace.append(
                    f"  cand CE{s_ce}({ce_ltp:.2f})/PE{s_pe}({pe_ltp:.2f}) "
                    f"score={score:.4f} rule={'PASS' if _rp else 'BLOCK'}"
                )
            if not _rp:
                continue
            if best is None or score < best[0]:
                best = (score, s_ce, s_pe, ce_ltp, pe_ltp)
    if trace is not None:
        trace.append(f"  ({skipped} candidates skipped: below target or wrong bias)")
    if best is None:
        if trace is not None:
            trace.append("NO-PAIR")
        return None
    _, s_ce, s_pe, ce_ltp, pe_ltp = best
    if trace is not None:
        trace.append(
            f"SELECTED CE{s_ce}/PE{s_pe} score={best[0]:.4f} (reentry, most-balanced)"
        )
    return s_ce, s_pe, ce_ltp, pe_ltp


def find_rollover_partner(
    strike_prem,
    roll_side: str,
    kept_strike: int,
    kept_ltp: float,
    spot: float,
    step: float,
    offset: int,
    ltp_target: float,
    max_entry_ratio: float,
    rule_eval,                      # callable(ce_strike:int, pe_strike:int) -> (passed:bool, reason:str)
    max_itm_steps: Optional[int] = None,
) -> Optional[Tuple[int, float]]:
    """
    Rollover partner selection (check-first):
      - Keep the RUNNING / bleeding leg fixed.
      - Scan `roll_side` strikes in ATM ± offset, >= ltp_target ONLY (no theta), and
        not deeper ITM than `max_itm_steps`.
      - For each candidate, build the combined pair and apply the re-entry rules.
      - Enforce CE/PE ratio <= max_entry_ratio.
      - Return the candidate with the LOWEST ratio (most balanced) or None.
    Returns (new_strike, new_ltp) or None.
    """
    atm = round(spot / step) * step if spot > 0 else 0
    best = None  # (ratio, strike, ltp)
    for (strike, side), v in strike_prem.items():
        if side != roll_side:
            continue
        if atm and abs(strike - atm) > offset * step:
            continue
        if max_itm_steps is not None and spot > 0 and step > 0:
            itm_pts = (spot - strike) if roll_side == "CE" else (strike - spot)
            if itm_pts > max_itm_steps * step:
                continue
        ltp = float(v.get("ltp", 0.0) or 0.0)
        if ltp < ltp_target:
            continue
        ce_s, pe_s = (int(strike), int(kept_strike)) if roll_side == "CE" else (int(kept_strike), int(strike))
        ce_ltp, pe_ltp = (ltp, kept_ltp) if roll_side == "CE" else (kept_ltp, ltp)
        if ce_ltp <= 0 or pe_ltp <= 0:
            continue
        passed, _ = rule_eval(ce_s, pe_s)
        if not passed:
            continue
        ratio = max(ce_ltp, pe_ltp) / min(ce_ltp, pe_ltp)
        if max_entry_ratio > 0 and ratio > max_entry_ratio:
            continue
        if best is None or ratio < best[0]:
            best = (ratio, int(strike), ltp)
    return (best[1], best[2]) if best else None


def classify_roll(ce_same: bool, pe_same: bool, has_candidates: bool) -> str:
    """Smart-roll outcome (reference exit_logic.perform_smart_roll):
      no candidates       -> "full_exit"
      both strikes same    -> "virtual"
      only PE changed      -> "partial_pe"   (CE stays)
      only CE changed      -> "partial_ce"   (PE stays)
      both changed         -> "physical"
    """
    if not has_candidates:
        return "full_exit"
    if ce_same and pe_same:
        return "virtual"
    if ce_same and not pe_same:
        return "partial_pe"
    if pe_same and not ce_same:
        return "partial_ce"
    return "physical"
