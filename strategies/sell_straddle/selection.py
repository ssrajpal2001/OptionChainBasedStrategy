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

from typing import Dict, List, Optional, Tuple

Key = Tuple[int, str]


def _available_strikes(strike_prem: Dict[Key, dict], side: str) -> List[int]:
    """Return all available strike prices for `side` with positive LTP."""
    return [
        int(strike) for (strike, s), v in strike_prem.items()
        if s == side and float(v.get("ltp", 0.0) or 0.0) > 0
    ]


def _common_atm(strike_prem: Dict[Key, dict], spot: float) -> int:
    """Return the strike closest to `spot` that has both CE and PE quotes.

    For crypto/variable-strike chains this is safer than computing ATM from a
    fixed step because the actual strikes may be 100, 200 or 500 apart.
    """
    ce_strikes = set(_available_strikes(strike_prem, "CE"))
    pe_strikes = set(_available_strikes(strike_prem, "PE"))
    common = sorted(ce_strikes & pe_strikes)
    if not common:
        return 0
    if spot <= 0:
        return common[0]
    return min(common, key=lambda s: abs(float(s) - spot))


def _strikes_around_atm(
    strike_prem: Dict[Key, dict],
    side: str,
    spot: float,
    offset: int,
) -> List[int]:
    """Return `offset` strikes below and `offset` strikes above the ATM for `side`.

    Uses the actual available strikes from the chain (handles variable gaps such as
    100/200/400/500 on Delta crypto). This mirrors DeltaChainManager._window_symbols.
    If `spot` is unavailable, fall back to the first `2*offset+1` strikes.
    """
    strikes = sorted(_available_strikes(strike_prem, side))
    if not strikes:
        return []
    if spot <= 0:
        return strikes[:min(len(strikes), 2 * offset + 1)]
    atm = min(strikes, key=lambda s: abs(float(s) - spot))
    i = strikes.index(atm)
    lo = max(0, i - offset)
    hi = min(len(strikes), i + offset + 1)
    return strikes[lo:hi]


def _strikes_near_spot(
    strike_prem: Dict[Key, dict],
    side: str,
    spot: float,
    n: int,
) -> List[int]:
    """Backward-compat helper: return up to `n` available strikes closest to `spot`."""
    strikes = _available_strikes(strike_prem, side)
    if not strikes:
        return []
    if spot <= 0:
        return sorted(strikes)[:n]
    strikes.sort(key=lambda s: abs(float(s) - spot))
    nearest = strikes[:n]
    return sorted(nearest)


def select_partner_for(strike_prem, roll_side, kept_strike, kept_ltp,
                       spot, step, offset, ltp_target, rule_pass, max_itm_steps=None,
                       theta_target: float = 0.0, variable_strikes: bool = False,
                       trace: Optional[list] = None, ltp_le_kept: bool = False,
                       metric: str = "closest_to_kept"):
    """Rollover partner selection — keep the RUNNING leg fixed and pick the best strike on
    `roll_side` to re-sell, BALANCED against the running leg, within ATM±offset, >= ltp_target
    and >= theta_target, optionally with premium <= the kept leg's premium, and passing
    rule_pass(ce_strike, pe_strike).

    Selection metric:
      - "closest_to_kept": minimize abs(ltp - kept_ltp)
      - "balanced_ratio": minimize abs(ltp - kept_ltp) / (ltp + kept_ltp)

    `variable_strikes=True`: for crypto chains where strike gaps are non-uniform.
    In that mode `offset` is interpreted as "number of strikes below and above ATM"
    (i.e. the candidate window is ATM±offset from the actual quoted strikes).

    `max_itm_steps` (optional): cap how deep ITM the re-sold leg may be (in strike steps) so the
    roll stays near ATM (a real straddle) instead of selling a deep-ITM strike.

    `trace` (optional): a list to which structured diagnostic dicts are appended for every
    candidate strike considered. This makes it easy to see WHY each candidate was rejected.
    Returns (strike, ltp) or None (→ caller closes all and starts fresh)."""
    if variable_strikes:
        candidate_strikes = _strikes_around_atm(strike_prem, roll_side, spot, offset=max(1, int(offset)))
    else:
        atm = round(spot / step) * step if spot > 0 else 0
        candidate_strikes = [int(atm + i * step) for i in range(-offset, offset + 1)]

    if trace is not None:
        trace.append({
            "event": "select_partner_for_start",
            "roll_side": roll_side,
            "kept_strike": int(kept_strike),
            "kept_ltp": float(kept_ltp or 0.0),
            "spot": float(spot or 0.0),
            "step": float(step or 0.0),
            "offset": int(offset or 0),
            "ltp_target": float(ltp_target or 0.0),
            "theta_target": float(theta_target or 0.0),
            "max_itm_steps": max_itm_steps,
            "variable_strikes": variable_strikes,
            "candidate_strikes": [int(s) for s in candidate_strikes],
        })

    best = None  # (premium_diff, strike, ltp)
    reject_counts = {
        "no_quote_in_pool": 0,
        "too_itm": 0,
        "dual_floor_fail": 0,
        "ltp_above_kept": 0,
        "rule_fail": 0,
        "not_closest": 0,
    }
    for strike in candidate_strikes:
        v = strike_prem.get((strike, roll_side))
        diag = {
            "event": "candidate",
            "roll_side": roll_side,
            "strike": int(strike),
            "ltp": None,
            "has_quote": bool(v),
            "itm_pass": None,
            "dual_floor_pass": None,
            "ltp_le_kept_pass": None,
            "rule_pass": None,
            "rule_reason": None,
            "selected": False,
            "reject_reason": None,
        }
        if not v:
            diag["reject_reason"] = "no_quote_in_pool"
            reject_counts["no_quote_in_pool"] += 1
            if trace is not None:
                trace.append(diag)
            continue
        ltp = float(v.get("ltp", 0.0) or 0.0)
        diag["ltp"] = ltp
        # Keep the re-sold leg near ATM: skip strikes deeper ITM than max_itm_steps.
        if max_itm_steps is not None and spot > 0 and step > 0:
            itm_pts = (spot - strike) if roll_side == "CE" else (strike - spot)  # >0 = ITM
            diag["itm_pts"] = float(itm_pts)
            diag["itm_limit"] = float(max_itm_steps * step)
            if itm_pts > max_itm_steps * step:
                diag["itm_pass"] = False
                diag["reject_reason"] = f"too_itm ({itm_pts:.2f} > {max_itm_steps * step:.2f})"
                reject_counts["too_itm"] += 1
                if trace is not None:
                    trace.append(diag)
                continue
            diag["itm_pass"] = True
        else:
            diag["itm_pass"] = True
        if not leg_passes_dual_floor(roll_side, strike, ltp, spot, ltp_target, theta_target):
            diag["dual_floor_pass"] = False
            _tv = strip_intrinsic(float(ltp), roll_side, float(strike), float(spot)) if ltp > 0 and spot > 0 else 0.0
            diag["reject_reason"] = (
                f"dual_floor_fail (ltp={ltp:.2f} < ltp_target={ltp_target:.2f} "
                f"or tv={_tv:.2f} < theta_target={theta_target:.2f})"
            )
            reject_counts["dual_floor_fail"] += 1
            if trace is not None:
                trace.append(diag)
            continue
        diag["dual_floor_pass"] = True
        # Optional: require partner premium <= kept leg premium. Disabled by default for rollover
        # so the bot can choose the closest premium regardless of direction.
        if ltp_le_kept and kept_ltp and ltp > float(kept_ltp):
            diag["ltp_le_kept_pass"] = False
            diag["reject_reason"] = f"ltp_above_kept ({ltp:.2f} > {float(kept_ltp):.2f})"
            reject_counts["ltp_above_kept"] += 1
            if trace is not None:
                trace.append(diag)
            continue
        diag["ltp_le_kept_pass"] = True
        ce_s, pe_s = (int(kept_strike), int(strike)) if roll_side == "PE" else (int(strike), int(kept_strike))
        try:
            _rp = rule_pass(ce_s, pe_s)
            # Backward compat: rule_pass may return bool, (bool, reason), or (bool, reason, ind_by_tf).
            if isinstance(_rp, tuple):
                rp = bool(_rp[0])
                rr = str(_rp[1]) if len(_rp) > 1 else ""
                if len(_rp) > 2 and _rp[2] is not None:
                    diag["rule_ind_by_tf"] = _rp[2]
            else:
                rp, rr = bool(_rp), ""
        except Exception as exc:
            rp, rr = False, f"rule_eval_exception: {exc}"
        diag["rule_pass"] = bool(rp)
        diag["rule_reason"] = str(rr)
        if not rp:
            diag["reject_reason"] = f"rule_fail ({rr})"
            reject_counts["rule_fail"] += 1
            if trace is not None:
                trace.append(diag)
            continue
        if metric == "balanced_ratio":
            denom = ltp + float(kept_ltp)
            score = abs(ltp - float(kept_ltp)) / denom if denom > 0 else 999.0
        else:
            score = abs(ltp - float(kept_ltp))
        if best is None or score < best[0]:
            diag["selected"] = True
            diag["score"] = float(score)
            best = (score, int(strike), ltp)
        else:
            diag["reject_reason"] = f"not_best (metric={metric} score={score:.4f} > best={best[0]:.4f})"
            reject_counts["not_closest"] += 1
        if trace is not None:
            trace.append(diag)

    if trace is not None:
        trace.append({
            "event": "select_partner_for_end",
            "best_strike": int(best[1]) if best else None,
            "best_ltp": float(best[2]) if best else None,
            "reject_counts": reject_counts,
            "candidates_total": len(candidate_strikes),
        })
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


def leg_passes_dual_floor(
    side: str,
    strike: float,
    ltp: float,
    spot: float,
    ltp_target: float,
    theta_target: float,
) -> bool:
    """
    Enforce BOTH the raw-LTP floor and the time-value (theta) floor.
    A target <= 0 means that particular floor is disabled.
    """
    if ltp_target > 0 and float(ltp) < float(ltp_target):
        return False
    if theta_target > 0:
        tv = strip_intrinsic(float(ltp), side, float(strike), float(spot))
        if tv < float(theta_target):
            return False
    return True


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
    rule_pass=None,  # optional callable(ce_strike, pe_strike) -> bool
    variable_strikes: bool = False,
    balance_ratio: float = 1.0,
) -> Optional[Tuple[int, int, float, float]]:
    """
    Balanced-pair selection for beginning AND re-entry:
      1. ATM both sides; require both LTP > 0.
      2. Anchor = side with LOWER TIME VALUE at ATM.
      3. Anchor must pass the dual floor (raw LTP >= ltp_target, time value >= theta_target).
      4. Partner = scan the other side over ATM +/- offset for a strike whose raw LTP is
         <= anchor_time_value * balance_ratio and passes the dual floor.  If rule_pass is supplied, the
         combined (ce_strike, pe_strike) pair must also pass it.  Pick the HIGHEST such LTP
         (closest to anchor time value from below).  The partner may be ITM or OTM.

    `variable_strikes=True`: discover ATM and candidate strikes from the actual quoted
    chain instead of assuming a fixed strike step. Used for Delta BTC/ETH daily options.
    Returns (ce_strike, pe_strike, ce_ltp, pe_ltp) or None.
    """
    if variable_strikes:
        atm = _common_atm(strike_prem, spot)
    else:
        atm = int(round(spot / step) * step)
    ce_atm = strike_prem.get((atm, "CE"))
    pe_atm = strike_prem.get((atm, "PE"))
    if not ce_atm or not pe_atm:
        return None
    ce_ltp = ce_atm.get("ltp", 0.0)
    pe_ltp = pe_atm.get("ltp", 0.0)
    if ce_ltp <= 0 or pe_ltp <= 0:
        return None

    ce_tv = strip_intrinsic(ce_ltp, "CE", atm, spot)
    pe_tv = strip_intrinsic(pe_ltp, "PE", atm, spot)

    # Anchor = side with lower TIME VALUE at ATM.  The partner's raw LTP must not
    # exceed the anchor's TIME VALUE * balance_ratio so the pair is balanced in
    # theta/extrinsic value while allowing a small configurable skew.
    if ce_tv < pe_tv:
        anchor_side, anchor_strike, anchor_ltp, anchor_tv, partner_side = "CE", atm, ce_ltp, ce_tv, "PE"
    else:
        anchor_side, anchor_strike, anchor_ltp, anchor_tv, partner_side = "PE", atm, pe_ltp, pe_tv, "CE"

    if trace is not None:
        trace.append(
            f"ANCHOR atm={atm} ce_tv={ce_tv:.2f} pe_tv={pe_tv:.2f} -> "
            f"anchor={anchor_side}@{anchor_strike} ltp={anchor_ltp:.2f} tv={anchor_tv:.2f} "
            f"(need ltp>={ltp_target:.0f} theta>={theta_target:.0f}); partner={partner_side} "
            f"wants same floor and ltp<={anchor_tv * balance_ratio:.2f} (ratio={balance_ratio:.2f})"
        )

    if not leg_passes_dual_floor(anchor_side, anchor_strike, anchor_ltp, spot, ltp_target, theta_target):
        if trace is not None:
            _tv = strip_intrinsic(anchor_ltp, anchor_side, anchor_strike, spot)
            trace.append(
                f"REJECT anchor {anchor_side}{anchor_strike} ltp={anchor_ltp:.2f} "
                f"tv={_tv:.2f} fails dual floor (ltp>={ltp_target:.0f}, theta>={theta_target:.0f})"
            )
        return None

    if variable_strikes:
        partner_strikes = _strikes_around_atm(
            strike_prem, partner_side, spot, offset=max(1, int(offset))
        )
    else:
        partner_strikes = [int(atm + i * step) for i in range(-offset, offset + 1)]

    best = None  # (ltp, strike)
    for s in partner_strikes:
        leg = strike_prem.get((s, partner_side))
        if not leg:
            continue
        ltp = leg.get("ltp", 0.0)
        _ok_floor = leg_passes_dual_floor(partner_side, s, ltp, spot, ltp_target, theta_target)
        _ok_balance = ltp <= anchor_tv * balance_ratio
        # Build the combined pair for the optional rule gate.
        if anchor_side == "CE":
            cs, ps = anchor_strike, s
        else:
            cs, ps = s, anchor_strike
        _ok_rule = rule_pass(cs, ps) if rule_pass is not None else True
        _ok = _ok_floor and _ok_balance and _ok_rule
        if trace is not None:
            _tv = strip_intrinsic(ltp, partner_side, s, spot) if ltp > 0 else 0.0
            if not _ok_floor:
                _why = "floor"
            elif not _ok_balance:
                _why = "balance"
            elif not _ok_rule:
                _why = "rule"
            else:
                _why = "OK"
            trace.append(
                f"  cand {partner_side}{s} ltp={ltp:.2f} tv={_tv:.2f} {_why}"
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
        _ctx = "rule-filtered " if rule_pass is not None else ""
        trace.append(f"SELECTED CE{ce}/PE{pe} ({_ctx}balanced-pair)")
    return result


def reentry_block_reason(strike_prem, spot, step, offset, ltp_target, rule_eval,
                         theta_target: float = 0.0, variable_strikes: bool = False,
                         balance_ratio: float = 1.0):
    """Diagnose why the re-entry pool produced no trade, so the log can distinguish
    'no balanced pair exists' from 'a pair exists but the gate blocked it'.

    rule_eval: callable(ce_strike, pe_strike) -> (passed: bool, reason: str)
    Returns: {"kind": "no_pair"} | {"kind": "blocked"|"passed", ce, pe, ce_ltp, pe_ltp, reason}
    """
    pair = select_balanced_pair(strike_prem, spot, step, offset, ltp_target,
                                theta_target=theta_target,
                                variable_strikes=variable_strikes,
                                balance_ratio=balance_ratio)
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
    variable_strikes: bool = False,
) -> Optional[Tuple[int, int, float, float]]:
    """
    Re-entry concept (reference _scan_v_slope_pool, balanced_premium metric):
      1. Strikes = ATM +/- offset.
      2. ATM bias from corrected ATM LTP: CE stronger if ce_corr > pe_corr.
      3. N x N over (s_ce, s_pe): both LTP >= ltp_target; bias filter
         (CE stronger -> ce_ltp < pe_ltp; else pe_ltp < ce_ltp).
      4. rule_pass(ce_strike, pe_strike) must be True (dynamic technical gate).
      5. balanced_score = abs(ce-pe)/(ce+pe); pick MIN score.

    `variable_strikes=True`: discover ATM and candidate strikes from the actual quoted
    chain instead of assuming a fixed strike step.
    Returns (ce_strike, pe_strike, ce_ltp, pe_ltp) or None.
    """
    if variable_strikes:
        atm = _common_atm(strike_prem, spot)
    else:
        atm = int(round(spot / step) * step)
    ce_atm = strike_prem.get((atm, "CE"))
    pe_atm = strike_prem.get((atm, "PE"))
    if not ce_atm or not pe_atm:
        return None
    ce_corr = strip_intrinsic(ce_atm.get("ltp", 0.0), "CE", atm, spot)
    pe_corr = strip_intrinsic(pe_atm.get("ltp", 0.0), "PE", atm, spot)
    ce_bias_stronger = ce_corr > pe_corr

    if trace is not None:
        trace.append(
            f"ANCHOR atm={atm} ce_tv={ce_corr:.2f} pe_tv={pe_corr:.2f} -> "
            f"bias={'CE' if ce_bias_stronger else 'PE'}-stronger "
            f"(weaker side must have lower ltp); dual floor ltp>={ltp_target:.0f} theta>={theta_target:.0f}"
        )

    skipped = 0
    if variable_strikes:
        ce_strikes = _strikes_around_atm(strike_prem, "CE", spot, offset=max(1, int(offset)))
        pe_strikes = _strikes_around_atm(strike_prem, "PE", spot, offset=max(1, int(offset)))
    else:
        ce_strikes = [int(atm + i * step) for i in range(-offset, offset + 1)]
        pe_strikes = ce_strikes
    best = None  # (score, ce_strike, pe_strike, ce_ltp, pe_ltp)
    for s_ce in ce_strikes:
        ce = strike_prem.get((s_ce, "CE"))
        if not ce:
            continue
        ce_ltp = ce.get("ltp", 0.0)
        if ce_ltp <= 0:
            continue
        for s_pe in pe_strikes:
            pe = strike_prem.get((s_pe, "PE"))
            if not pe:
                continue
            pe_ltp = pe.get("ltp", 0.0)
            if pe_ltp <= 0:
                continue
            # Dual floor: both raw LTP and time value must meet their targets.
            if not leg_passes_dual_floor("CE", s_ce, ce_ltp, spot, ltp_target, theta_target):
                skipped += 1
                if trace is not None:
                    _tv = strip_intrinsic(ce_ltp, "CE", s_ce, spot) if ce_ltp > 0 else 0.0
                    trace.append(
                        f"  skip CE{s_ce} ltp={ce_ltp:.2f} tv={_tv:.2f} fails dual floor"
                    )
                continue
            if not leg_passes_dual_floor("PE", s_pe, pe_ltp, spot, ltp_target, theta_target):
                skipped += 1
                if trace is not None:
                    _tv = strip_intrinsic(pe_ltp, "PE", s_pe, spot) if pe_ltp > 0 else 0.0
                    trace.append(
                        f"  skip PE{s_pe} ltp={pe_ltp:.2f} tv={_tv:.2f} fails dual floor"
                    )
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
    theta_target: float = 0.0,
    variable_strikes: bool = False,
) -> Optional[Tuple[int, float]]:
    """
    Rollover partner selection (check-first):
      - Keep the RUNNING / bleeding leg fixed.
      - Scan `roll_side` strikes in ATM ± offset, >= ltp_target AND >= theta_target,
        and not deeper ITM than `max_itm_steps`.
      - For each candidate, build the combined pair and apply the re-entry rules.
      - Enforce CE/PE ratio <= max_entry_ratio.
      - Return the candidate with the LOWEST ratio (most balanced) or None.

    `variable_strikes=True`: discover ATM and candidate strikes from the actual quoted
    chain instead of assuming a fixed strike step.
    Returns (new_strike, new_ltp) or None.
    """
    if variable_strikes:
        candidate_strikes = _strikes_around_atm(strike_prem, roll_side, spot, offset=max(1, int(offset)))
    else:
        atm = round(spot / step) * step if spot > 0 else 0
        candidate_strikes = [int(atm + i * step) for i in range(-offset, offset + 1)]

    best = None  # (ratio, strike, ltp)
    for strike in candidate_strikes:
        v = strike_prem.get((strike, roll_side))
        if not v:
            continue
        if max_itm_steps is not None and spot > 0 and step > 0:
            itm_pts = (spot - strike) if roll_side == "CE" else (strike - spot)
            if itm_pts > max_itm_steps * step:
                continue
        ltp = float(v.get("ltp", 0.0) or 0.0)
        if not leg_passes_dual_floor(roll_side, strike, ltp, spot, ltp_target, theta_target):
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
