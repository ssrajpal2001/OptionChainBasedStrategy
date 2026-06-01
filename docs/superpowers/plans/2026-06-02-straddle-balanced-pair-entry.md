# Straddle Balanced-Pair Entry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the sell-straddle's spot-ATM same-strike entry selection with an exact replica of the reference (`Option_Selling_May_2026` sell_v3) anchor/partner + pool-scan balanced-pair selection, using broker-ATP feed data only — entry selection only, exits/rollover decision logic untouched.

**Architecture:** Core selection algorithms are extracted into a new pure-function module `strategies/straddle_selection.py` (fully unit-testable, no async/EventBus). `SellStraddleStrategy` gains a per-strike feed cache and a per-strike previous-closed-ATP map, builds per-pair indicators `{close, vwap, slope}` from broker ATP, and feeds them to the **existing, unchanged** dynamic `_eval_rules`. Entry rules (`entry_rules_beginning` / `entry_rules_reentry`) remain fully config-driven and dynamic — only the strikes the rules evaluate against change. A minimal per-leg-strike LTP plumbing change lets asymmetric CE/PE legs price correctly for exits.

**Tech Stack:** Python 3.12, asyncio, numpy, pytest 9.

---

## Spec reference
`docs/superpowers/specs/2026-06-02-straddle-balanced-pair-entry-design.md`

## File Structure
- **Create** `strategies/straddle_selection.py` — pure helpers: `strip_intrinsic`, `pair_indicators`, `select_balanced_pair`, `scan_pool`. One responsibility: candidate selection math. No I/O, no async.
- **Create** `tests/conftest.py` — adds repo root to `sys.path` so `from strategies... import` works under pytest.
- **Create** `tests/strategies/test_straddle_selection.py` — unit tests for the pure module.
- **Modify** `strategies/sell_straddle.py` — wire cache, per-pair indicators, selection, hybrid workflow, per-leg LTP plumbing.
- **Create** `tests/strategies/test_sell_straddle_legs.py` — unit test for per-leg LTP routing.

> NOTE: The bridge (`execution_bridge/straddle_bridge.py`) already accepts and places orders on separate `ce_strike`/`pe_strike` (line ~364). No bridge change needed.

---

## Task 1: Pure module skeleton + intrinsic/pair-indicator helpers

**Files:**
- Create: `strategies/straddle_selection.py`
- Create: `tests/conftest.py`
- Create: `tests/strategies/test_straddle_selection.py`

- [ ] **Step 1: Create the conftest for imports**

Create `tests/conftest.py`:
```python
import os
import sys

# Repo root = two levels up from this file (tests/conftest.py)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
```

- [ ] **Step 2: Write the failing test for the helpers**

Create `tests/strategies/test_straddle_selection.py`:
```python
from strategies.straddle_selection import strip_intrinsic, pair_indicators


def test_strip_intrinsic_ce_itm():
    # spot 100, strike 90 → CE intrinsic = 10, time value = ltp - 10
    assert strip_intrinsic(ltp=25.0, side="CE", strike=90, spot=100) == 15.0


def test_strip_intrinsic_pe_itm():
    # spot 100, strike 110 → PE intrinsic = 10, time value = ltp - 10
    assert strip_intrinsic(ltp=25.0, side="PE", strike=110, spot=100) == 15.0


def test_strip_intrinsic_otm_unchanged():
    # OTM → intrinsic 0 → time value = ltp
    assert strip_intrinsic(ltp=20.0, side="CE", strike=110, spot=100) == 20.0


def test_pair_indicators_full():
    cache = {
        (100, "CE"): {"ltp": 30.0, "atp": 28.0},
        (100, "PE"): {"ltp": 26.0, "atp": 25.0},
    }
    prev = {(100, "CE"): 29.0, (100, "PE"): 27.0}  # prev closed atp
    ind = pair_indicators(cache, prev, 100, 100)
    assert ind == {"close": 56.0, "vwap": 53.0, "slope": 53.0 - 56.0}


def test_pair_indicators_missing_leg_returns_none():
    cache = {(100, "CE"): {"ltp": 30.0, "atp": 28.0}}
    assert pair_indicators(cache, {}, 100, 100) is None


def test_pair_indicators_no_prev_omits_slope():
    cache = {
        (100, "CE"): {"ltp": 30.0, "atp": 28.0},
        (100, "PE"): {"ltp": 26.0, "atp": 25.0},
    }
    ind = pair_indicators(cache, {}, 100, 100)
    assert ind["close"] == 56.0 and ind["vwap"] == 53.0
    assert "slope" not in ind
```

- [ ] **Step 3: Run to verify failure**

Run: `python -m pytest tests/strategies/test_straddle_selection.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'strategies.straddle_selection'`

- [ ] **Step 4: Implement the helpers**

Create `strategies/straddle_selection.py`:
```python
"""
strategies/straddle_selection.py — pure candidate-selection math for the
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


def strip_intrinsic(ltp: float, side: str, strike: float, spot: float) -> float:
    """Time-value-only LTP. CE intrinsic = max(0, spot-strike); PE = max(0, strike-spot)."""
    if side == "CE":
        intrinsic = max(0.0, spot - strike)
    else:
        intrinsic = max(0.0, strike - spot)
    return ltp - intrinsic


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
        "ltp": ce_ltp + pe_ltp,
        "vwap": ce_atp + pe_atp,
    }
    ce_prev = prev_atp_closed.get((int(ce_strike), "CE"))
    pe_prev = prev_atp_closed.get((int(pe_strike), "PE"))
    if ce_prev and pe_prev and ce_prev > 0 and pe_prev > 0:
        cur = ce_atp + pe_atp
        prev = ce_prev + pe_prev
        ind["slope"] = cur - prev
        ind["vwap_slope"] = cur - prev
    return ind
```

- [ ] **Step 5: Run to verify pass**

Run: `python -m pytest tests/strategies/test_straddle_selection.py -v`
Expected: PASS (5 tests)

- [ ] **Step 6: Commit**

```bash
git add strategies/straddle_selection.py tests/conftest.py tests/strategies/test_straddle_selection.py
git commit -m "feat(straddle): pure intrinsic/pair-indicator helpers for balanced-pair selection"
```

---

## Task 2: Beginning concept — `select_balanced_pair`

**Files:**
- Modify: `strategies/straddle_selection.py`
- Test: `tests/strategies/test_straddle_selection.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/strategies/test_straddle_selection.py`:
```python
from strategies.straddle_selection import select_balanced_pair


def _cache(d):
    # d: {(strike, side): ltp} -> full cache with atp == ltp
    return {k: {"ltp": v, "atp": v} for k, v in d.items()}


def test_select_balanced_anchor_is_lower_time_value_side():
    # spot=atm=100, CE=60, PE=80 → CE is anchor (lower LTP, both OTM-ish).
    # Partner = PE side strike with ltp_target<=ltp<60, highest such.
    cache = _cache({
        (100, "CE"): 60.0, (100, "PE"): 80.0,
        (105, "PE"): 55.0, (110, "PE"): 40.0,
    })
    res = select_balanced_pair(cache, spot=100, step=5, offset=4, ltp_target=30.0)
    assert res is not None
    ce_strike, pe_strike, ce_ltp, pe_ltp = res
    # Anchor CE@100=60; partner PE = highest strictly below 60 and >=30 → 105@55
    assert (ce_strike, ce_ltp) == (100, 60.0)
    assert (pe_strike, pe_ltp) == (105, 55.0)


def test_select_balanced_anchor_below_target_returns_none():
    cache = _cache({(100, "CE"): 20.0, (100, "PE"): 80.0, (105, "PE"): 15.0})
    # CE anchor LTP 20 < target 30 → None
    assert select_balanced_pair(cache, spot=100, step=5, offset=4, ltp_target=30.0) is None


def test_select_balanced_no_partner_below_anchor_returns_none():
    # All partner candidates >= anchor LTP → no strictly-lower partner.
    cache = _cache({(100, "CE"): 50.0, (100, "PE"): 80.0, (105, "PE"): 90.0})
    assert select_balanced_pair(cache, spot=100, step=5, offset=4, ltp_target=30.0) is None


def test_select_balanced_missing_atm_returns_none():
    cache = _cache({(100, "CE"): 60.0})  # no PE ATM
    assert select_balanced_pair(cache, spot=100, step=5, offset=4, ltp_target=30.0) is None
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/strategies/test_straddle_selection.py -k balanced -v`
Expected: FAIL — `ImportError: cannot import name 'select_balanced_pair'`

- [ ] **Step 3: Implement `select_balanced_pair`**

Append to `strategies/straddle_selection.py`:
```python
def select_balanced_pair(
    strike_prem: Dict[Key, dict],
    spot: float,
    step: float,
    offset: int,
    ltp_target: float,
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

    if ce_corr < pe_corr:
        anchor_side, anchor_strike, anchor_ltp, partner_side = "CE", atm, ce_ltp, "PE"
    else:
        anchor_side, anchor_strike, anchor_ltp, partner_side = "PE", atm, pe_ltp, "CE"

    if anchor_ltp < ltp_target:
        return None

    best = None  # (ltp, strike)
    for i in range(-offset, offset + 1):
        s = int(atm + i * step)
        leg = strike_prem.get((s, partner_side))
        if not leg:
            continue
        ltp = leg.get("ltp", 0.0)
        if ltp_target <= ltp < anchor_ltp:
            if best is None or ltp > best[0]:
                best = (ltp, s)
    if best is None:
        return None

    partner_ltp, partner_strike = best
    if anchor_side == "CE":
        return anchor_strike, partner_strike, anchor_ltp, partner_ltp
    return partner_strike, anchor_strike, partner_ltp, anchor_ltp
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/strategies/test_straddle_selection.py -k balanced -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add strategies/straddle_selection.py tests/strategies/test_straddle_selection.py
git commit -m "feat(straddle): beginning-concept anchor/partner balanced-pair selection"
```

---

## Task 3: Re-entry concept — `scan_pool`

**Files:**
- Modify: `strategies/straddle_selection.py`
- Test: `tests/strategies/test_straddle_selection.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/strategies/test_straddle_selection.py`:
```python
from strategies.straddle_selection import scan_pool


def test_scan_pool_picks_min_balanced_score():
    # CE bias stronger (CE corrected > PE corrected) → require ce_ltp < pe_ltp.
    # Two passing pairs; the more balanced (smaller abs(ce-pe)/(ce+pe)) wins.
    cache = _cache({
        (100, "CE"): 50.0, (100, "PE"): 50.0,   # ATM: ce_corr==pe_corr → CE not stronger
        (95, "CE"): 40.0, (105, "PE"): 60.0,
        (90, "CE"): 30.0, (110, "PE"): 70.0,
    })
    # Force a deterministic bias by making ATM CE corrected > PE corrected:
    cache[(100, "CE")] = {"ltp": 55.0, "atp": 55.0}
    cache[(100, "PE")] = {"ltp": 50.0, "atp": 50.0}

    # rules: always pass (empty) so selection is pure balanced-score.
    def always_ok(ce_s, pe_s):
        return True

    res = scan_pool(
        cache, spot=100, step=5, offset=4, ltp_target=30.0,
        rule_pass=always_ok, metric="balanced_premium",
    )
    assert res is not None
    ce_strike, pe_strike, ce_ltp, pe_ltp = res
    # CE stronger → ce_ltp < pe_ltp enforced. Candidate (95CE=40, 105PE=60):
    # score=abs(40-60)/100=0.20; (90CE=30,110PE=70): score=0.40 → 95/105 wins.
    assert (ce_strike, pe_strike) == (95, 105)


def test_scan_pool_respects_ltp_target_floor():
    cache = _cache({
        (100, "CE"): 55.0, (100, "PE"): 50.0,
        (95, "CE"): 40.0, (105, "PE"): 25.0,   # PE 25 below target → excluded
    })

    def always_ok(ce_s, pe_s):
        return True

    res = scan_pool(cache, spot=100, step=5, offset=4, ltp_target=30.0,
                    rule_pass=always_ok, metric="balanced_premium")
    assert res is None


def test_scan_pool_rule_rejection_excludes_pair():
    cache = _cache({
        (100, "CE"): 55.0, (100, "PE"): 50.0,
        (95, "CE"): 40.0, (105, "PE"): 60.0,
    })

    def reject_all(ce_s, pe_s):
        return False

    res = scan_pool(cache, spot=100, step=5, offset=4, ltp_target=30.0,
                    rule_pass=reject_all, metric="balanced_premium")
    assert res is None
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest tests/strategies/test_straddle_selection.py -k scan_pool -v`
Expected: FAIL — `ImportError: cannot import name 'scan_pool'`

- [ ] **Step 3: Implement `scan_pool`**

Append to `strategies/straddle_selection.py`:
```python
def scan_pool(
    strike_prem: Dict[Key, dict],
    spot: float,
    step: float,
    offset: int,
    ltp_target: float,
    rule_pass,                      # callable(ce_strike:int, pe_strike:int) -> bool
    metric: str = "balanced_premium",
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
            if ce_ltp < ltp_target or pe_ltp < ltp_target:
                continue
            if ce_bias_stronger:
                if ce_ltp >= pe_ltp:
                    continue
            else:
                if pe_ltp >= ce_ltp:
                    continue
            if not rule_pass(s_ce, s_pe):
                continue
            denom = ce_ltp + pe_ltp
            score = abs(ce_ltp - pe_ltp) / denom if denom > 0 else 999.0
            if best is None or score < best[0]:
                best = (score, s_ce, s_pe, ce_ltp, pe_ltp)
    if best is None:
        return None
    _, s_ce, s_pe, ce_ltp, pe_ltp = best
    return s_ce, s_pe, ce_ltp, pe_ltp
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/strategies/test_straddle_selection.py -k scan_pool -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Run the whole pure-module suite**

Run: `python -m pytest tests/strategies/test_straddle_selection.py -v`
Expected: PASS (12 tests)

- [ ] **Step 6: Commit**

```bash
git add strategies/straddle_selection.py tests/strategies/test_straddle_selection.py
git commit -m "feat(straddle): re-entry pool-scan balanced-pair selection"
```

---

## Task 4: Wire per-strike cache + previous-closed-ATP into the strategy

**Files:**
- Modify: `strategies/sell_straddle.py`

- [ ] **Step 1: Add cache fields in `__init__`**

In `strategies/sell_straddle.py`, find in `__init__` the block after
`self._prev_vwap_atp: Optional[float] = None   # previous closed-candle combined VWAP`
(currently ~line 182) and add immediately after it:
```python
        # Per-strike feed cache for balanced-pair selection (all subscribed strikes).
        # Key = (int strike, "CE"/"PE") -> {"ltp": float, "atp": float}.
        self._strike_prem: Dict[Tuple[int, str], dict] = {}
        # Previous closed-candle ATP per leg, for per-pair VWAP slope.
        self._prev_atp_closed: Dict[Tuple[int, str], float] = {}
        # Hybrid workflow: set when the beginning concept's pair fails its gate,
        # routes the next pulse to the pool scan even while trades_today == 0.
        self._beginning_failed: bool = False
```

Confirm the file already imports `Tuple` (top of file: `from typing import Dict, List, Optional, Tuple`). It does.

- [ ] **Step 2: Clear cache in `reset_session`**

In `reset_session` (currently ~line 377), add before the final `logger.info(... session reset ...)`:
```python
        self._strike_prem.clear()
        self._prev_atp_closed.clear()
        self._beginning_failed = False
```

- [ ] **Step 3: Populate the cache in `_option_loop`**

In `_option_loop`, locate the ATM-capture block that begins with
`if atm > 0 and tick.ltp > 0 and abs(tick.strike - atm) < step / 2:` (currently ~line 500).
Immediately BEFORE that `if`, add the per-strike cache write:
```python
            # Per-strike cache (every subscribed strike) for balanced-pair selection.
            if tick.ltp > 0:
                _k = (int(tick.strike), tick.option_type)
                _a = float(getattr(tick, "atp", 0.0) or 0.0)
                entry = self._strike_prem.get(_k)
                if entry is None:
                    self._strike_prem[_k] = {"ltp": float(tick.ltp), "atp": _a}
                else:
                    entry["ltp"] = float(tick.ltp)
                    if _a > 0:
                        entry["atp"] = _a
```

- [ ] **Step 4: Update previous-closed-ATP each 1m candle in `_on_candle`**

In `_on_candle`, find the call `self._recompute_indicators()` (currently ~line 558).
Immediately AFTER that line, add:
```python
        # Snapshot every cached leg's current ATP as its "previous closed" value
        # for the next candle's per-pair slope. Only overwrite on a valid ATP so a
        # missing tick never corrupts the slope (same discipline as the ATM path).
        for _k, _v in self._strike_prem.items():
            _a = _v.get("atp", 0.0)
            if _a and _a > 0:
                self._prev_atp_closed[_k] = _a
```

- [ ] **Step 5: Add `_pair_indicators` method**

In `strategies/sell_straddle.py`, add this method to `SellStraddleStrategy` (place it right after `_recompute_indicators`, ~line 615):
```python
    def _pair_indicators(self, ce_strike: int, pe_strike: int) -> Optional[Dict[str, float]]:
        """Per-pair {close, vwap, slope} from the feed cache (broker ATP). None if not ready."""
        from strategies.straddle_selection import pair_indicators
        return pair_indicators(self._strike_prem, self._prev_atp_closed, ce_strike, pe_strike)
```

- [ ] **Step 6: Syntax/smoke check (import compiles)**

Run: `python -c "import strategies.sell_straddle as m; print('ok')"`
Expected: `ok`

- [ ] **Step 7: Commit**

```bash
git add strategies/sell_straddle.py
git commit -m "feat(straddle): per-strike feed cache + per-pair indicators wiring"
```

---

## Task 5: Selection-driven entry + hybrid workflow in `_try_entry` / `_open_position`

**Files:**
- Modify: `strategies/sell_straddle.py`

- [ ] **Step 1: Replace ATM block in `_try_entry` with selection**

In `_try_entry`, the current code (after the `_order_pending` / spot-ltp checks)
computes `is_beginning`, `rule_key`, `rules`, a single `_atm`, then calls
`_eval_rules(rules, self._ind)` and `_open_position(now, ss, rule_key, reason)`.

Replace from the line:
```python
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        is_beginning = (self._trades_today == 0)
```
down to (and including) the call:
```python
        await self._open_position(now, ss, rule_key, reason)
```
with:
```python
        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        is_beginning = (self._trades_today == 0)

        # Workflow mode: hybrid (default) → beginning concept first trade, else pool.
        # _beginning_failed flips a hybrid first-trade pulse to the pool scan.
        workflow_mode = ss.get("entry_workflow_mode", "hybrid")
        if workflow_mode == "beginning_only":
            use_beginning = True
        elif workflow_mode == "reentry_only":
            use_beginning = False
        else:  # hybrid
            use_beginning = is_beginning and not self._beginning_failed

        rule_key = "entry_rules_beginning" if use_beginning else "entry_rules_reentry"
        rules    = ss.get(rule_key, [])

        if not self._is_primed(now, rules):
            self._clog.info(
                "EVAL %s [%s] PRIMING — waiting for indicator priming", self._underlying, rule_key,
            )
            return

        step   = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0

        from strategies.straddle_selection import select_balanced_pair, scan_pool

        if use_beginning:
            sel = select_balanced_pair(self._strike_prem, self._spot, step, offset, ltp_target)
            concept = "beginning"
        else:
            sel = scan_pool(
                self._strike_prem, self._spot, step, offset, ltp_target,
                rule_pass=lambda cs, ps: _eval_rules(rules, self._pair_indicators(cs, ps) or {})[0],
                metric=ss.get("reentry_best_metric", "balanced_premium"),
            )
            concept = "reentry"

        if not sel:
            self._clog.info(
                "EVAL %s [%s] NO-PAIR — spot=%.2f no balanced pair (target=%.2f offset=%d)",
                self._underlying, rule_key, self._spot, ltp_target, offset,
            )
            return

        ce_strike, pe_strike, ce_ltp, pe_ltp = sel
        ind = self._pair_indicators(ce_strike, pe_strike) or dict(self._ind)
        passed, reason = _eval_rules(rules, ind)
        self._clog.info(
            "EVAL %s [%s/%s] sell CE%d=%.2f + PE%d=%.2f credit=%.2f | rules: %s | result=%s | ind=%s",
            self._underlying, rule_key, concept,
            ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp,
            reason, "PASS" if passed else "BLOCK",
            {k: round(v, 2) for k, v in ind.items()},
        )
        if not passed:
            # Hybrid: beginning concept failed its gate → next pulse uses the pool scan.
            if use_beginning and workflow_mode == "hybrid":
                self._beginning_failed = True
            return

        self._clog.info(
            "ENTRY attempting — CE%d=%.2f PE%d=%.2f credit=%.2f rules_passed",
            ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp,
        )
        await self._open_position(now, ce_strike, pe_strike, ce_ltp, pe_ltp, rule_key, reason)
```

> The old `ltp_target` BOTH-legs block earlier in `_try_entry` (the
> `if self._ltp_target > 0.0:` guard that checks `self._ce_ltp/_pe_ltp`) is now
> redundant with the per-pair floor inside selection, but it is HARMLESS (it
> only guards the ATM legs). Leave it — it short-circuits cheaply when no ATM
> premium exists yet. The earlier `is_primed` call inside it is removed because
> priming is now checked above against the chosen rule set.

- [ ] **Step 2: Remove the now-duplicated priming block**

Earlier in `_try_entry` there was an `is_primed`/`EVAL ... PRIMING` block tied to
the old `rules`/`_atm`. After Step 1 replaced the lower half, ensure there is only
ONE priming check. If a duplicate `if not self._is_primed(now, rules):` remains
above the replaced region (referring to the old `rules` variable), delete that
earlier duplicate so priming is evaluated once, against the workflow-selected rules.

Run: `python -c "import strategies.sell_straddle"` — must import without `NameError`.

- [ ] **Step 3: Update `_open_position` signature + legs**

Replace the `_open_position` definition header and body up to the position
construction. Change:
```python
    async def _open_position(
        self, now: datetime, ss: dict, rule_key: str, reason: str,
    ) -> None:
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        atm  = round(self._spot / step) * step
```
to:
```python
    async def _open_position(
        self, now: datetime, ce_strike: int, pe_strike: int,
        ce_ltp: float, pe_ltp: float, rule_key: str, reason: str,
    ) -> None:
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        atm  = round(self._spot / step) * step
```

Then in the same method, change the `StraddlePosition(...)` construction legs and credit:
```python
            ce_leg            = StraddleLeg("CE", atm, self._ce_ltp, self._ce_ltp),
            pe_leg            = StraddleLeg("PE", atm, self._pe_ltp, self._pe_ltp),
            net_credit        = self._ce_ltp + self._pe_ltp,
```
to:
```python
            ce_leg            = StraddleLeg("CE", ce_strike, ce_ltp, ce_ltp),
            pe_leg            = StraddleLeg("PE", pe_strike, pe_ltp, pe_ltp),
            net_credit        = ce_ltp + pe_ltp,
```

And change the `entry_indicators` line in the same constructor:
```python
            entry_indicators  = dict(self._ind),
```
to:
```python
            entry_indicators  = self._pair_indicators(ce_strike, pe_strike) or dict(self._ind),
```

Then change the `_initial_net_credit` capture and the order event. Replace:
```python
        if self._initial_net_credit <= 0:
            self._initial_net_credit = self._ce_ltp + self._pe_ltp
```
with:
```python
        if self._initial_net_credit <= 0:
            self._initial_net_credit = ce_ltp + pe_ltp
```

Replace the `StraddleOrderEvent(...)` fields:
```python
            atm            = atm,
            ce_strike      = atm,
            pe_strike      = atm,
            ce_ltp         = self._ce_ltp,
            pe_ltp         = self._pe_ltp,
```
with:
```python
            atm            = atm,
            ce_strike      = ce_strike,
            pe_strike      = pe_strike,
            ce_ltp         = ce_ltp,
            pe_ltp         = pe_ltp,
```

Finally update the two log lines in `_open_position` that reference
`self._ce_ltp`/`self._pe_ltp`/`atm` for the entered premium to use `ce_ltp`,
`pe_ltp`, `ce_strike`, `pe_strike` (the `logger.info("... ENTERED ...")` call):
```python
        logger.info(
            "SellStraddle[%s]: ENTERED — CE%d=%.2f PE%d=%.2f credit=%.2f | %s=PASS [%s]",
            self._underlying, ce_strike, ce_ltp, pe_strike, pe_ltp, ce_ltp + pe_ltp,
            rule_key, reason,
        )
```

- [ ] **Step 4: Reset `_beginning_failed` after a successful entry**

In `_open_position`, after `self._trades_today += 1`, add:
```python
        self._beginning_failed = False
```

- [ ] **Step 5: Import compiles**

Run: `python -c "import strategies.sell_straddle as m; print('ok')"`
Expected: `ok`

- [ ] **Step 6: Pure suite still green**

Run: `python -m pytest tests/strategies/test_straddle_selection.py -v`
Expected: PASS (12 tests)

- [ ] **Step 7: Commit**

```bash
git add strategies/sell_straddle.py
git commit -m "feat(straddle): selection-driven entry with dynamic hybrid workflow"
```

---

## Task 6: Per-leg-strike LTP plumbing (asymmetric legs)

**Files:**
- Modify: `strategies/sell_straddle.py`
- Create: `tests/strategies/test_sell_straddle_legs.py`

- [ ] **Step 1: Write the failing test for per-leg LTP routing**

Create `tests/strategies/test_sell_straddle_legs.py`:
```python
from strategies.sell_straddle import StraddlePosition, StraddleLeg


def _route_tick(pos, strike, side, ltp):
    """Mirror the strategy's open-position LTP update rule (per-leg strike)."""
    if side == "CE" and int(strike) == int(pos.ce_leg.strike):
        pos.ce_leg.ltp = ltp
    elif side == "PE" and int(strike) == int(pos.pe_leg.strike):
        pos.pe_leg.ltp = ltp


def test_asymmetric_legs_route_to_correct_leg():
    pos = StraddlePosition(
        underlying="NIFTY", atm_at_entry=100, entry_spot=100,
        ce_leg=StraddleLeg("CE", 100, 60.0, 60.0),
        pe_leg=StraddleLeg("PE", 105, 55.0, 55.0),
        net_credit=115.0,
    )
    _route_tick(pos, 100, "CE", 58.0)   # CE leg strike
    _route_tick(pos, 105, "PE", 50.0)   # PE leg strike
    _route_tick(pos, 105, "CE", 999.0)  # wrong strike for CE → ignored
    assert pos.ce_leg.ltp == 58.0
    assert pos.pe_leg.ltp == 50.0
    assert pos.current_value == 108.0
    assert pos.unrealized_pnl == 115.0 - 108.0
```

- [ ] **Step 2: Run to verify pass-or-fail honestly**

Run: `python -m pytest tests/strategies/test_sell_straddle_legs.py -v`
Expected: PASS (this test encodes the TARGET behaviour using a local router; it
verifies the dataclass math. The strategy change in Step 3 makes the real loop
match this router.)

- [ ] **Step 3: Update the open-position LTP branch in `_option_loop`**

In `_option_loop`, find the open-position update block (currently ~line 510):
```python
            if self._position and self._position.status == "open":
                if abs(tick.strike - self._position.atm_at_entry) < 0.01:
                    if tick.option_type == "CE":
                        self._position.ce_leg.ltp = tick.ltp
                    elif tick.option_type == "PE":
                        self._position.pe_leg.ltp = tick.ltp
```
replace with per-leg-strike routing:
```python
            if self._position and self._position.status == "open":
                pos = self._position
                if tick.option_type == "CE" and abs(tick.strike - pos.ce_leg.strike) < 0.01:
                    pos.ce_leg.ltp = tick.ltp
                elif tick.option_type == "PE" and abs(tick.strike - pos.pe_leg.strike) < 0.01:
                    pos.pe_leg.ltp = tick.ltp
```

- [ ] **Step 4: Physical roll — keep per-leg strikes (already symmetric new ATM)**

In `_try_smart_roll`, the physical-roll branch builds a new `StraddlePosition`
with `StraddleLeg("CE", new_atm, ...)` / `StraddleLeg("PE", new_atm, ...)`.
This is correct (rolls re-anchor to a symmetric ATM). NO change to roll decision
logic. Confirm by reading the block — leave it as-is. (This step is a verify-only
checkpoint; make no edit unless the legs reference `pos.atm_at_entry` incorrectly.)

- [ ] **Step 5: Import + full suite**

Run: `python -c "import strategies.sell_straddle"` then
`python -m pytest tests/strategies/ -v`
Expected: all PASS (13 tests).

- [ ] **Step 6: Commit**

```bash
git add strategies/sell_straddle.py tests/strategies/test_sell_straddle_legs.py
git commit -m "feat(straddle): per-leg-strike LTP routing for asymmetric balanced pair"
```

---

## Task 7: Integration smoke + dry-run verification

**Files:**
- Create: `tests/strategies/test_straddle_entry_integration.py`

- [ ] **Step 1: Write an async integration smoke test**

Create `tests/strategies/test_straddle_entry_integration.py`:
```python
import asyncio
import datetime
import pytest

from data_layer.base_feeder import EventBus, OptionTick
from strategies.straddle_selection import select_balanced_pair


def test_select_uses_live_cache_shape():
    # Build a cache exactly as _option_loop would, then select.
    cache = {}
    spot = 100.0
    samples = [
        (100, "CE", 60.0, 59.0), (100, "PE", 80.0, 79.0),
        (105, "PE", 55.0, 54.0), (110, "PE", 40.0, 39.0),
    ]
    for strike, side, ltp, atp in samples:
        cache[(strike, side)] = {"ltp": ltp, "atp": atp}
    res = select_balanced_pair(cache, spot=spot, step=5, offset=4, ltp_target=30.0)
    assert res == (100, 105, 60.0, 55.0)
```

- [ ] **Step 2: Run it**

Run: `python -m pytest tests/strategies/test_straddle_entry_integration.py -v`
Expected: PASS

- [ ] **Step 3: Full repo test sweep**

Run: `python -m pytest tests/ -v`
Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add tests/strategies/test_straddle_entry_integration.py
git commit -m "test(straddle): integration smoke for balanced-pair selection"
```

- [ ] **Step 5: Manual dry-run checklist (no funds, live feed)**

Run the system and confirm in `logs/clients/ss_NIFTY_*.log`:
- `EVAL NIFTY [entry_rules_beginning/beginning] sell CE<n>=.. + PE<m>=..` shows the
  **selected** CE/PE strikes (which may differ) and the live rule values.
- On a beginning-gate failure, the next pulse logs `[entry_rules_reentry/reentry]`
  (hybrid transition working).
- On entry, `straddle_bridge` logs an `ENTRY` line with the same CE/PE strikes,
  and on close an `EXIT` line — confirming orders reach the bridge start→stop.

---

## Self-Review notes
- **Spec coverage:** §2.1 cache → Task 4; §2.2 prev-ATP/slope → Task 4 + Task 1
  `pair_indicators`; §2.3 per-pair eval → Task 5 (`rule_pass` lambda + final
  `_eval_rules`); §2.4 beginning → Task 2; §2.5 pool → Task 3; §2.6 hybrid →
  Task 5 (`_beginning_failed`); §4 `_open_position` → Task 5; §6 per-leg plumbing
  → Task 6. All covered.
- **Dynamic rules preserved:** every gate routes through the existing
  `_eval_rules(rules, ind)` with `rules` read live from config — no thresholds
  hardcoded. Beginning vs re-entry rule sets stay config-driven.
- **Type consistency:** `select_balanced_pair`/`scan_pool` both return
  `(ce_strike, pe_strike, ce_ltp, pe_ltp)`; `_open_position` consumes exactly
  that order; cache key is `(int strike, side)` everywhere.
