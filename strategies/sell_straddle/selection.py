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


def find_hedge_strike(
    strike_prem: Dict[Key, dict],
    side: str,
    leg_strike: float,
    leg_ltp: float,
    step: float,
    max_offset_steps: int = 20,
) -> Optional[Tuple[int, float]]:
    """EOD hedge-and-carry (2026-08-20, user spec): find the protective strike
    for a sold leg running in loss. Moves further OTM from `leg_strike` (CE:
    increasing strike; PE: decreasing strike) in `step` increments, and
    returns the FIRST quoted strike whose LTP is <= 50% of `leg_ltp` -- i.e.
    the closest-to-half-price strike found by walking outward, not a search
    for the closest match to exactly 50%.

    Returns (strike, ltp) or None if no quoted strike within max_offset_steps
    satisfies the 50%-or-below condition (caller should fall back to a normal
    EOD close rather than leave a sold leg unprotected)."""
    if leg_ltp <= 0 or step <= 0:
        return None
    target = leg_ltp * 0.5
    direction = 1 if side == "CE" else -1
    for i in range(1, max_offset_steps + 1):
        candidate = int(leg_strike + direction * i * step)
        leg = strike_prem.get((candidate, side))
        if not leg:
            continue
        ltp = float(leg.get("ltp", 0.0) or 0.0)
        if ltp > 0 and ltp <= target:
            return (candidate, ltp)
    return None


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


def _evaluate_roll_candidate(strike_prem, roll_side, strike, kept_strike, kept_ltp,
                              spot, step, ltp_target, theta_target, max_itm_steps,
                              ltp_le_kept, rule_pass, metric):
    """One candidate's full filter chain (quote → ITM cap → dual floor → ltp_le_kept →
    rule_pass → score), factored out of select_partner_for so both the original flat
    scan and the 2026-08-26 anchor-ring scan share EXACTLY the same checks. Returns a
    populated diag dict; diag["reject_reason"] is None iff the candidate passed
    everything (diag["score"] then holds its metric score)."""
    v = strike_prem.get((strike, roll_side))
    diag = {
        "event": "candidate", "roll_side": roll_side, "strike": int(strike),
        "ltp": None, "has_quote": bool(v), "itm_pass": None, "dual_floor_pass": None,
        "ltp_le_kept_pass": None, "rule_pass": None, "rule_reason": None,
        "selected": False, "reject_reason": None,
    }
    if not v:
        diag["reject_reason"] = "no_quote_in_pool"
        return diag
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
            return diag
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
        return diag
    diag["dual_floor_pass"] = True
    # Optional: require partner premium <= kept leg premium. Disabled by default for rollover
    # so the bot can choose the closest premium regardless of direction.
    if ltp_le_kept and kept_ltp and ltp > float(kept_ltp):
        diag["ltp_le_kept_pass"] = False
        diag["reject_reason"] = f"ltp_above_kept ({ltp:.2f} > {float(kept_ltp):.2f})"
        return diag
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
        return diag
    if metric == "balanced_ratio":
        denom = ltp + float(kept_ltp)
        diag["score"] = abs(ltp - float(kept_ltp)) / denom if denom > 0 else 999.0
    else:
        diag["score"] = abs(ltp - float(kept_ltp))
    return diag


def select_partner_for(strike_prem, roll_side, kept_strike, kept_ltp,
                       spot, step, offset, ltp_target, rule_pass, max_itm_steps=None,
                       theta_target: float = 0.0, variable_strikes: bool = False,
                       trace: Optional[list] = None, ltp_le_kept: bool = False,
                       metric: str = "closest_to_kept",
                       anchor_strike: Optional[int] = None):
    """Rollover partner selection — keep the RUNNING leg fixed and pick a strike on
    `roll_side` to re-sell, >= ltp_target and >= theta_target, optionally with premium
    <= the kept leg's premium, and passing rule_pass(ce_strike, pe_strike).

    Two search modes:

    - `anchor_strike=None` (default, unchanged since before 2026-08-26): candidates are
      the flat window ATM±offset*step; a SINGLE GLOBAL BEST wins by `metric` across
      every passing candidate in that whole window, regardless of how far from ATM it
      sits.

    - `anchor_strike=<strike>` (2026-08-26, direct user spec): candidates are searched
      in EXPANDING RINGS of `step` around `anchor_strike` (the strike actually being
      closed, not ATM) — ring 1 = {anchor-step, anchor+step}, ring 2 = {anchor-2*step,
      anchor+2*step}, etc., up to `offset` rings. The FIRST ring containing at least
      one candidate that passes every filter wins; `metric` only breaks a tie WITHIN
      that same ring (both anchor±N passing). Rings are exhausted outward until one
      succeeds or `offset` rings are checked with nothing passing → None. Matches the
      user's own framing: "check all strikes, but take the strike 100 [i.e. one ring]
      diff from the strike we're closing" — falls back to the next-closest ring rather
      than the old free global search only when the closest ring has no valid partner.

    Selection metric:
      - "closest_to_kept": minimize abs(ltp - kept_ltp)
      - "balanced_ratio": minimize abs(ltp - kept_ltp) / (ltp + kept_ltp)

    `variable_strikes=True`: for crypto chains where strike gaps are non-uniform.
    In that mode `offset` is interpreted as "number of strikes below and above ATM"
    (i.e. the candidate window is ATM±offset from the actual quoted strikes) --
    `anchor_strike` is not supported together with `variable_strikes` (falls back to
    the ATM-centered flat scan).

    `max_itm_steps` (optional): cap how deep ITM the re-sold leg may be (in strike steps) so the
    roll stays near ATM (a real straddle) instead of selling a deep-ITM strike.

    `trace` (optional): a list to which structured diagnostic dicts are appended for every
    candidate strike considered. This makes it easy to see WHY each candidate was rejected.
    Returns (strike, ltp) or None (→ caller closes all and starts fresh)."""
    if anchor_strike is not None and not variable_strikes:
        return _select_partner_by_ring(
            strike_prem, roll_side, kept_strike, kept_ltp, spot, step, offset,
            ltp_target, rule_pass, max_itm_steps, theta_target, trace, ltp_le_kept,
            metric, int(anchor_strike),
        )

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

    best = None  # (score, strike, ltp)
    reject_counts = {
        "no_quote_in_pool": 0, "too_itm": 0, "dual_floor_fail": 0,
        "ltp_above_kept": 0, "rule_fail": 0, "not_closest": 0,
    }
    for strike in candidate_strikes:
        diag = _evaluate_roll_candidate(
            strike_prem, roll_side, strike, kept_strike, kept_ltp, spot, step,
            ltp_target, theta_target, max_itm_steps, ltp_le_kept, rule_pass, metric,
        )
        if diag["reject_reason"] is not None:
            _reason_key = diag["reject_reason"].split(" ", 1)[0].split("(", 1)[0].strip()
            _key_map = {
                "no_quote_in_pool": "no_quote_in_pool", "too_itm": "too_itm",
                "dual_floor_fail": "dual_floor_fail", "ltp_above_kept": "ltp_above_kept",
                "rule_fail": "rule_fail",
            }
            reject_counts[_key_map.get(_reason_key, "rule_fail")] += 1
            if trace is not None:
                trace.append(diag)
            continue
        score = diag["score"]
        if best is None or score < best[0]:
            diag["selected"] = True
            best = (score, int(strike), diag["ltp"])
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


def _select_partner_by_ring(strike_prem, roll_side, kept_strike, kept_ltp,
                             spot, step, offset, ltp_target, rule_pass, max_itm_steps,
                             theta_target, trace, ltp_le_kept, metric, anchor_strike):
    """Expanding-ring search around anchor_strike (the strike being closed) -- see
    select_partner_for's own docstring for the full rationale. Ring 1 = anchor±step
    (e.g. ±100), ring 2 = anchor±2*step, etc. First ring with >=1 passing candidate
    wins; ties within a ring broken by `metric`."""
    if trace is not None:
        trace.append({
            "event": "select_partner_for_start", "roll_side": roll_side,
            "kept_strike": int(kept_strike), "kept_ltp": float(kept_ltp or 0.0),
            "spot": float(spot or 0.0), "step": float(step or 0.0),
            "offset": int(offset or 0), "ltp_target": float(ltp_target or 0.0),
            "theta_target": float(theta_target or 0.0), "max_itm_steps": max_itm_steps,
            "variable_strikes": False, "anchor_strike": int(anchor_strike),
            "candidate_strikes": [
                int(anchor_strike + sign * ring * step)
                for ring in range(1, int(offset) + 1) for sign in (-1, 1)
            ],
        })

    reject_counts = {
        "no_quote_in_pool": 0, "too_itm": 0, "dual_floor_fail": 0,
        "ltp_above_kept": 0, "rule_fail": 0, "not_closest": 0,
    }
    total_checked = 0
    max_ring = max(1, int(offset))
    for ring in range(1, max_ring + 1):
        ring_strikes = [int(anchor_strike - ring * step), int(anchor_strike + ring * step)]
        ring_diags = []
        for strike in ring_strikes:
            total_checked += 1
            diag = _evaluate_roll_candidate(
                strike_prem, roll_side, strike, kept_strike, kept_ltp, spot, step,
                ltp_target, theta_target, max_itm_steps, ltp_le_kept, rule_pass, metric,
            )
            ring_diags.append(diag)
        passers = [d for d in ring_diags if d["reject_reason"] is None]
        if not passers:
            for d in ring_diags:
                _reason_key = d["reject_reason"].split(" ", 1)[0].split("(", 1)[0].strip()
                reject_counts[_reason_key if _reason_key in reject_counts else "rule_fail"] += 1
            if trace is not None:
                trace.extend(ring_diags)
            continue
        best_diag = min(passers, key=lambda d: d["score"])
        best_diag["selected"] = True
        for d in ring_diags:
            if d is not best_diag and d["reject_reason"] is None:
                d["reject_reason"] = f"not_best (metric={metric} score={d['score']:.4f} > best={best_diag['score']:.4f})"
                reject_counts["not_closest"] += 1
        if trace is not None:
            trace.extend(ring_diags)
            trace.append({
                "event": "select_partner_for_end",
                "best_strike": best_diag["strike"], "best_ltp": best_diag["ltp"],
                "reject_counts": reject_counts, "candidates_total": total_checked,
            })
        return (best_diag["strike"], best_diag["ltp"])

    if trace is not None:
        trace.append({
            "event": "select_partner_for_end", "best_strike": None, "best_ltp": None,
            "reject_counts": reject_counts, "candidates_total": total_checked,
        })
    return None


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
    Balanced-pair selection for RE-ENTRY (and any other single-ATM caller):
      ATM = spot rounded to the nearest strike (or the closest doubly-quoted strike
      for variable-strike chains), then select_balanced_pair_at() at that one strike.

    `variable_strikes=True`: discover ATM and candidate strikes from the actual quoted
    chain instead of assuming a fixed strike step. Used for Delta BTC/ETH daily options.
    Returns (ce_strike, pe_strike, ce_ltp, pe_ltp) or None.
    """
    if variable_strikes:
        atm = _common_atm(strike_prem, spot)
    else:
        atm = int(round(spot / step) * step)
    return select_balanced_pair_at(
        strike_prem, atm, spot, step, offset, ltp_target, trace=trace,
        entry_basis=entry_basis, theta_target=theta_target, rule_pass=rule_pass,
        variable_strikes=variable_strikes, balance_ratio=balance_ratio,
    )


def select_balanced_pair_at(
    strike_prem: Dict[Key, dict],
    atm: int,
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
    anchor_otm_steps: int = 0,
) -> Optional[Tuple[int, int, float, float]]:
    """
    Same anchor+partner balanced-pair search as select_balanced_pair(), but takes the
    anchor strike explicitly instead of computing it by rounding spot to one nearest
    strike. Lets a caller anchor the search at any strike -- e.g. BEGINNING entry's
    near/far dual-anchor selection (2026-08-05), which evaluates the two strikes
    actually bracketing spot (floor(spot/step)*step and that +step) as two independent
    candidates instead of only ever considering the single nearest-rounded strike.

      1. Both sides quoted at `atm`; require both LTP > 0.
      2. Anchor SIDE = whichever side has LOWER TIME VALUE at `atm` -- this decision is
         always made from the raw ATM reading, regardless of `anchor_otm_steps` below.
      3. If `anchor_otm_steps > 0` (2026-08-20, user spec): the anchor SIDE from step 2
         is kept, but the anchor's own STRIKE (and the LTP/time-value used for the floor
         check and the partner's balance target) shifts `anchor_otm_steps` strikes further
         OTM from `atm` on that side (CE: atm + steps*step; PE: atm - steps*step) --
         i.e. "anchor selection is correct, but for pairing use 1-OTM of the anchored
         side" rather than pairing off the literal ATM reading. If that shifted strike
         isn't quoted or has no live LTP, no pair is returned (same as any other missing
         leg). Default 0 preserves the original at-ATM anchor behaviour unchanged --
         RE-ENTRY keeps anchor_otm_steps=0.
      4. Anchor must pass the dual floor (raw LTP >= ltp_target, time value >= theta_target)
         at its (possibly shifted) strike.
      5. Partner = scan the other side over anchor_strike +/- offset (the anchor's OWN,
         possibly-OTM-shifted strike -- 2026-08-24 user spec: "we need to pair with the
         OTM just selected", not the pre-shift ATM) for a strike whose raw LTP is
         <= anchor_time_value * balance_ratio and passes the dual floor. If rule_pass is
         supplied, the combined (ce_strike, pe_strike) pair must also pass it. Pick the
         HIGHEST such LTP (closest to anchor time value from below). The partner may be
         ITM or OTM. When anchor_otm_steps=0, anchor_strike == atm, so this is byte-
         identical to the original ATM-centred window (RE-ENTRY's own behaviour, which
         always calls with anchor_otm_steps=0, is completely unchanged).

    Returns (ce_strike, pe_strike, ce_ltp, pe_ltp) or None.
    """
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

    # Anchor SIDE = side with lower TIME VALUE at ATM -- this decision always reads the
    # raw ATM quotes, independent of anchor_otm_steps below.
    if ce_tv < pe_tv:
        anchor_side, anchor_strike, anchor_ltp, anchor_tv, partner_side = "CE", atm, ce_ltp, ce_tv, "PE"
    else:
        anchor_side, anchor_strike, anchor_ltp, anchor_tv, partner_side = "PE", atm, pe_ltp, pe_tv, "CE"

    if trace is not None:
        trace.append(
            f"ANCHOR atm={atm} ce_tv={ce_tv:.2f} pe_tv={pe_tv:.2f} -> "
            f"anchor_side={anchor_side} (lower time value at ATM)"
        )

    if anchor_otm_steps > 0:
        _shift = anchor_otm_steps * step
        shifted_strike = int(atm + _shift) if anchor_side == "CE" else int(atm - _shift)
        shifted_leg = strike_prem.get((shifted_strike, anchor_side))
        shifted_ltp = shifted_leg.get("ltp", 0.0) if shifted_leg else 0.0
        if not shifted_leg or shifted_ltp <= 0:
            if trace is not None:
                trace.append(
                    f"REJECT anchor {anchor_side}{shifted_strike} (1-OTM of {anchor_side}@{atm}) "
                    f"-- no live quote"
                )
            return None
        anchor_strike = shifted_strike
        anchor_ltp = shifted_ltp
        anchor_tv = strip_intrinsic(shifted_ltp, anchor_side, shifted_strike, spot)
        if trace is not None:
            trace.append(
                f"ANCHOR SHIFTED {anchor_otm_steps}-OTM -> {anchor_side}@{anchor_strike} "
                f"ltp={anchor_ltp:.2f} tv={anchor_tv:.2f}"
            )

    if trace is not None:
        trace.append(
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
        # Centred on anchor_strike (the anchor's own, possibly-OTM-shifted strike), not
        # the pre-shift `atm` -- see the docstring's step 5. When anchor_otm_steps=0,
        # anchor_strike == atm, so this line is a no-op for RE-ENTRY.
        partner_strikes = [int(anchor_strike + i * step) for i in range(-offset, offset + 1)]

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


def anchor_fails_floor(
    strike_prem: Dict[Key, dict],
    atm: int,
    spot: float,
    ltp_target: float,
    theta_target: float = 0.0,
    anchor_otm_steps: int = 0,
    step: float = 0.0,
) -> bool:
    """2026-08-23, user spec: True if the ANCHOR leg (side with lower time
    value at `atm` -- same anchor-selection rule select_balanced_pair_at
    already uses) would fail the existing dual floor (ltp_target/
    theta_target) -- the SAME threshold that function already rejects a
    pair on, not a new one. Deliberately a small, standalone, additive
    function rather than refactoring select_balanced_pair_at to expose its
    internal rejection reason -- zero risk of changing that already-proven,
    live-money selection function's own behavior. Also True (fails) if
    either leg has no live quote yet -- never guess "passes" from missing
    data. Used by the caller to decide whether to shift the entry expiry
    to next week (see SellStraddleStrategy._maybe_shift_expiry_for_low_
    anchor_ltp in entries.py) -- this function only diagnoses, it never
    mutates anything itself."""
    ce_atm = strike_prem.get((atm, "CE"))
    pe_atm = strike_prem.get((atm, "PE"))
    if not ce_atm or not pe_atm:
        return True
    ce_ltp = ce_atm.get("ltp", 0.0)
    pe_ltp = pe_atm.get("ltp", 0.0)
    if ce_ltp <= 0 or pe_ltp <= 0:
        return True

    ce_tv = strip_intrinsic(ce_ltp, "CE", atm, spot)
    pe_tv = strip_intrinsic(pe_ltp, "PE", atm, spot)
    if ce_tv < pe_tv:
        anchor_side, anchor_strike, anchor_ltp = "CE", atm, ce_ltp
    else:
        anchor_side, anchor_strike, anchor_ltp = "PE", atm, pe_ltp

    if anchor_otm_steps > 0 and step > 0:
        _shift = anchor_otm_steps * step
        shifted_strike = int(atm + _shift) if anchor_side == "CE" else int(atm - _shift)
        shifted_leg = strike_prem.get((shifted_strike, anchor_side))
        shifted_ltp = shifted_leg.get("ltp", 0.0) if shifted_leg else 0.0
        if not shifted_leg or shifted_ltp <= 0:
            return True
        anchor_strike, anchor_ltp = shifted_strike, shifted_ltp

    return not leg_passes_dual_floor(anchor_side, anchor_strike, anchor_ltp, spot, ltp_target, theta_target)


def anchor_floor_detail(
    strike_prem: Dict[Key, dict],
    atm: int,
    spot: float,
    theta_target: float = 0.0,
    anchor_otm_steps: int = 0,
    step: float = 0.0,
) -> dict:
    """2026-08-26, direct user request: diagnostic twin of anchor_fails_floor
    that returns the ACTUAL measured anchor side/strike/ltp/theta, not just a
    bool -- so the EXPIRY-SHIFT log line can show what was actually observed
    (e.g. "PE24450 ltp=42.10 tv=38.50") instead of only the floor thresholds
    it failed to clear. Mirrors anchor_fails_floor's own logic exactly (same
    anchor-side/anchor-otm-shift steps) so the two can never disagree about
    which leg is the anchor -- this one is purely for logging, never used for
    any real decision itself."""
    ce_atm = strike_prem.get((atm, "CE"))
    pe_atm = strike_prem.get((atm, "PE"))
    if not ce_atm or not pe_atm:
        return {"reason": "no_quote_at_atm"}
    ce_ltp = ce_atm.get("ltp", 0.0)
    pe_ltp = pe_atm.get("ltp", 0.0)
    if ce_ltp <= 0 or pe_ltp <= 0:
        return {"reason": "zero_ltp_at_atm", "ce_ltp": ce_ltp, "pe_ltp": pe_ltp}

    ce_tv = strip_intrinsic(ce_ltp, "CE", atm, spot)
    pe_tv = strip_intrinsic(pe_ltp, "PE", atm, spot)
    if ce_tv < pe_tv:
        anchor_side, anchor_strike, anchor_ltp = "CE", atm, ce_ltp
    else:
        anchor_side, anchor_strike, anchor_ltp = "PE", atm, pe_ltp

    if anchor_otm_steps > 0 and step > 0:
        _shift = anchor_otm_steps * step
        shifted_strike = int(atm + _shift) if anchor_side == "CE" else int(atm - _shift)
        shifted_leg = strike_prem.get((shifted_strike, anchor_side))
        shifted_ltp = shifted_leg.get("ltp", 0.0) if shifted_leg else 0.0
        if not shifted_leg or shifted_ltp <= 0:
            return {"reason": "no_quote_at_shifted_anchor", "anchor_side": anchor_side,
                    "anchor_strike": shifted_strike}
        anchor_strike, anchor_ltp = shifted_strike, shifted_ltp

    anchor_tv = strip_intrinsic(anchor_ltp, anchor_side, anchor_strike, spot)
    return {
        "reason": "measured", "anchor_side": anchor_side, "anchor_strike": anchor_strike,
        "anchor_ltp": anchor_ltp, "anchor_tv": anchor_tv,
    }


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
