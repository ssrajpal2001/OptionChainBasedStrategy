# Straddle Faithful Rollover/Exit Mirror — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mirror the reference sell_v3 rollover/exit behaviour in the straddle: real single-leg orders, the 0-or-2 leg invariant, single-side LTP-decay rolls, and a 4-outcome smart roll (Virtual/Partial/Physical/Full-exit).

**Architecture:** A pure `classify_roll` decision function (testable) drives the smart roll. The bridge gains an optional `legs` selector so EXIT/ENTRY can act on one leg. The strategy gains `_close_leg`, `_single_side_roll`, `_single_side_roll_to`, and a rewritten `_try_smart_roll` that uses `scan_pool` candidates and per-leg strike comparison. guardrail_pnl moves before day-%.

**Tech Stack:** Python 3.12, asyncio, pytest 9.

**Spec:** `docs/superpowers/specs/2026-06-02-straddle-faithful-rollover-design.md`

## File Structure
- **Modify** `strategies/straddle_selection.py` — add pure `classify_roll`.
- **Modify** `execution_bridge/straddle_bridge.py` — `legs` field on events + per-leg filter.
- **Modify** `strategies/sell_straddle.py` — `_close_leg`, `_single_side_roll`, `_single_side_roll_to`, rewritten `_try_smart_roll`, LTP-decay loop, guardrail_pnl reorder, leg-aware `_on_fill`.
- **Create** `tests/strategies/test_roll_classify.py`, `tests/strategies/test_bridge_legs.py`, `tests/strategies/test_single_side_roll.py`.

---

## Task 1: Pure `classify_roll` decision

**Files:**
- Modify: `strategies/straddle_selection.py`
- Create: `tests/strategies/test_roll_classify.py`

- [ ] **Step 1: Write the failing test**

Create `tests/strategies/test_roll_classify.py`:
```python
from strategies.straddle_selection import classify_roll


def test_no_candidates_full_exit():
    assert classify_roll(ce_same=True, pe_same=True, has_candidates=False) == "full_exit"


def test_both_same_virtual():
    assert classify_roll(ce_same=True, pe_same=True, has_candidates=True) == "virtual"


def test_ce_same_pe_changed_partial_pe():
    assert classify_roll(ce_same=True, pe_same=False, has_candidates=True) == "partial_pe"


def test_pe_same_ce_changed_partial_ce():
    assert classify_roll(ce_same=False, pe_same=True, has_candidates=True) == "partial_ce"


def test_both_changed_physical():
    assert classify_roll(ce_same=False, pe_same=False, has_candidates=True) == "physical"
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/strategies/test_roll_classify.py -v`
Expected: FAIL — `ImportError: cannot import name 'classify_roll'`

- [ ] **Step 3: Implement**

Append to `strategies/straddle_selection.py`:
```python
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
```

- [ ] **Step 4: Run to verify pass**

Run: `python -m pytest tests/strategies/test_roll_classify.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add strategies/straddle_selection.py tests/strategies/test_roll_classify.py
git commit -m "feat(straddle): pure classify_roll smart-roll outcome decision"
```

---

## Task 2: Bridge single-leg order capability (`legs` selector)

**Files:**
- Modify: `execution_bridge/straddle_bridge.py`
- Create: `tests/strategies/test_bridge_legs.py`

Read `execution_bridge/straddle_bridge.py` first. `StraddleOrderEvent` and
`StraddleFillEvent` are `@dataclass` near the top; `_paper_fill` and `_live_fill`
build fills / place orders. The live order loop is
`for opt_type, strike in [("CE", ev.ce_strike), ("PE", ev.pe_strike)]:`.

- [ ] **Step 1: Write the failing test**

Create `tests/strategies/test_bridge_legs.py`:
```python
from execution_bridge.straddle_bridge import StraddleOrderEvent


def test_order_event_defaults_both_legs():
    ev = StraddleOrderEvent(
        action="ENTRY", underlying="NIFTY", atm=100, ce_strike=100, pe_strike=100,
        ce_ltp=10.0, pe_ltp=10.0,
    )
    assert ev.legs == ["CE", "PE"]


def test_order_event_single_leg():
    ev = StraddleOrderEvent(
        action="EXIT", underlying="NIFTY", atm=100, ce_strike=100, pe_strike=100,
        ce_ltp=10.0, pe_ltp=10.0, legs=["CE"],
    )
    assert ev.legs == ["CE"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/strategies/test_bridge_legs.py -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'legs'`

- [ ] **Step 3: Add `legs` to both events**

In `execution_bridge/straddle_bridge.py`, in `StraddleOrderEvent` add after `event_id`:
```python
    legs:           list = field(default_factory=lambda: ["CE", "PE"])  # legs to act on
```
In `StraddleFillEvent` add after `paper_mode`:
```python
    legs:       list = field(default_factory=lambda: ["CE", "PE"])
```
Ensure `from dataclasses import dataclass, field` is imported (it is — `field` already used).

- [ ] **Step 4: Filter legs in `_live_fill`**

In `_live_fill`, change:
```python
        for opt_type, strike in [("CE", ev.ce_strike), ("PE", ev.pe_strike)]:
```
to:
```python
        for opt_type, strike in [("CE", ev.ce_strike), ("PE", ev.pe_strike)]:
            if opt_type not in ev.legs:
                continue
```

- [ ] **Step 5: Echo legs in `_paper_fill` and `_live_fill` fills**

In `_paper_fill`, the `StraddleFillEvent(...)` constructor: add `legs = ev.legs,`.
For legs NOT acted on, set their fill price to 0.0:
```python
        fill = StraddleFillEvent(
            action     = ev.action,
            underlying = ev.underlying,
            atm        = ev.atm,
            ce_strike  = ev.ce_strike,
            pe_strike  = ev.pe_strike,
            ce_fill    = ev.ce_ltp if "CE" in ev.legs else 0.0,
            pe_fill    = ev.pe_ltp if "PE" in ev.legs else 0.0,
            client_id  = client_id,
            binding_id = binding_id,
            event_id   = ev.event_id,
            paper_mode = True,
            legs       = ev.legs,
        )
```
In `_live_fill`, find where it constructs the `StraddleFillEvent` after placing orders and add `legs = ev.legs,` to it (locate the `StraddleFillEvent(` near the end of `_live_fill`).

- [ ] **Step 6: Run tests**

Run: `python -m pytest tests/strategies/test_bridge_legs.py -v`
Expected: PASS (2 tests)
Run: `python -c "import execution_bridge.straddle_bridge; print('ok')"` → `ok`

- [ ] **Step 7: Commit**

```bash
git add execution_bridge/straddle_bridge.py tests/strategies/test_bridge_legs.py
git commit -m "feat(straddle-bridge): per-leg order selector (legs=) for single-leg rolls"
```

---

## Task 3: Strategy `_close_leg` + leg-aware `_on_fill`

**Files:**
- Modify: `strategies/sell_straddle.py`

Read `_on_fill` and `_close_position` in `strategies/sell_straddle.py` first.

- [ ] **Step 1: Add `_close_leg` method**

Add this method to `SellStraddleStrategy` (place it right before `_close_position`):
```python
    async def _close_leg(self, side: str, reason: str, now: datetime) -> float:
        """Close ONE leg (publish EXIT legs=[side]); book that leg's P&L into the
        session total; remove the leg from the position. Returns leg P&L (pts).
        When both legs are gone, the position is cleared. 0-or-2 invariant is the
        caller's responsibility (single-side roll re-opens or closes the survivor)."""
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        pos = self._position
        if not pos:
            return 0.0
        leg = pos.ce_leg if side == "CE" else pos.pe_leg
        leg_pnl = leg.entry_price - leg.ltp  # short option: credit - buyback
        self._event_counter += 1
        order_ev = StraddleOrderEvent(
            action="EXIT", underlying=self._underlying, atm=pos.atm_at_entry,
            ce_strike=pos.ce_leg.strike, pe_strike=pos.pe_leg.strike,
            ce_ltp=pos.ce_leg.ltp, pe_ltp=pos.pe_leg.ltp,
            lot_multiplier=self._lot_multiplier, lot_size=self._lot_size,
            spot=self._spot, close_reason=reason, realized_pnl=leg_pnl,
            event_id=f"{self._underlying}_EXITLEG_{side}_{self._event_counter}",
            legs=[side],
        )
        await self._bus.publish(Topic.ORDER_REQUEST, order_ev)
        self._session_realized_pnl_pts += leg_pnl
        logger.info("SellStraddle[%s]: CLOSE LEG %s strike=%.0f pnl=%.2fpts [%s]",
                    self._underlying, side, leg.strike, leg_pnl, reason)
        return leg_pnl
```

- [ ] **Step 2: Make `_on_fill` leg-aware**

In `_on_fill`, the ENTRY branch updates both legs. Guard each leg by `fill.legs`:
find the ENTRY branch and change the leg-price assignments to:
```python
        if fill.action == "ENTRY":
            if self._position and self._position.status == "open":
                if "CE" in getattr(fill, "legs", ["CE", "PE"]):
                    self._position.ce_leg.ltp         = fill.ce_fill
                    self._position.ce_leg.entry_price = fill.ce_fill
                if "PE" in getattr(fill, "legs", ["CE", "PE"]):
                    self._position.pe_leg.ltp         = fill.pe_fill
                    self._position.pe_leg.entry_price = fill.pe_fill
                self._position.net_credit = self._position.ce_leg.entry_price + self._position.pe_leg.entry_price
                logger.info(
                    "SellStraddle[%s]: ENTRY confirmed — CE=%.2f PE=%.2f credit=%.2f [%s/%s] legs=%s",
                    self._underlying, self._position.ce_leg.entry_price, self._position.pe_leg.entry_price,
                    self._position.net_credit, fill.client_id, fill.binding_id,
                    getattr(fill, "legs", ["CE", "PE"]),
                )
            self._order_pending = False
```

- [ ] **Step 3: Import + tests stay green**

Run: `python -c "import strategies.sell_straddle; print('ok')"` → `ok`
Run: `python -m pytest tests/strategies/ -q` → all pass.

- [ ] **Step 4: Commit**

```bash
git add strategies/sell_straddle.py
git commit -m "feat(straddle): _close_leg single-leg close + leg-aware _on_fill"
```

---

## Task 4: `_single_side_roll` + LTP-decay loop both sides

**Files:**
- Modify: `strategies/sell_straddle.py`
- Create: `tests/strategies/test_single_side_roll.py`

- [ ] **Step 1: Add `_open_leg` helper + `_single_side_roll`**

Add these methods to `SellStraddleStrategy` (after `_close_leg`):
```python
    async def _open_leg(self, side: str, strike: int, ltp: float, now: datetime, reason: str) -> None:
        """Open ONE leg at a new strike (publish ENTRY legs=[side]); update the leg."""
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        pos = self._position
        if not pos:
            return
        leg = pos.ce_leg if side == "CE" else pos.pe_leg
        leg.strike = strike
        leg.entry_price = ltp
        leg.ltp = ltp
        pos.net_credit = pos.ce_leg.entry_price + pos.pe_leg.entry_price
        pos.tsl_high_lock_rs = 0.0
        pos.open_time = now
        self._event_counter += 1
        order_ev = StraddleOrderEvent(
            action="ENTRY", underlying=self._underlying, atm=pos.atm_at_entry,
            ce_strike=pos.ce_leg.strike, pe_strike=pos.pe_leg.strike,
            ce_ltp=pos.ce_leg.ltp, pe_ltp=pos.pe_leg.ltp,
            lot_multiplier=self._lot_multiplier, lot_size=self._lot_size,
            spot=self._spot, indicators=dict(self._ind),
            event_id=f"{self._underlying}_OPENLEG_{side}_{self._event_counter}",
            legs=[side],
        )
        await self._bus.publish(Topic.ORDER_REQUEST, order_ev)
        logger.info("SellStraddle[%s]: OPEN LEG %s strike=%.0f @%.2f [%s]",
                    self._underlying, side, strike, ltp, reason)

    async def _single_side_roll(self, side: str, now: datetime, reason: str) -> None:
        """Close decayed leg, re-enter that side via pool scan; enforce 0-or-2 invariant."""
        from strategies.straddle_selection import scan_pool
        other = "PE" if side == "CE" else "CE"
        pos = self._position
        if not pos:
            return
        await self._close_leg(side, reason, now)

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0

        sel = scan_pool(
            self._strike_prem, self._spot, step, offset, ltp_target,
            rule_pass=lambda cs, ps: _eval_rules(rules, self._pair_indicators(cs, ps) or {})[0],
            metric=ss.get("reentry_best_metric", "balanced_premium"),
        )
        new_strike = new_ltp = None
        if sel:
            ce_s, pe_s, ce_l, pe_l = sel
            new_strike, new_ltp = (ce_s, ce_l) if side == "CE" else (pe_s, pe_l)

        if new_strike and new_ltp and new_ltp >= ltp_target:
            await self._open_leg(side, new_strike, new_ltp, now, f"single_side_roll_{reason}")
            self._persist()
            return
        # 0-or-2 invariant — no valid re-entry → close the surviving leg too.
        logger.warning("SellStraddle[%s]: single-side roll %s found no candidate — closing %s (0-or-2).",
                       self._underlying, side, other)
        await self._close_leg(other, f"single_side_cleanup_{reason}", now)
        self._position = None
        self._persist()
```

- [ ] **Step 2: Rewrite the LTP-decay branch in `_check_exits`**

Find the LTP-decay block (the `if self._ltp_decay_enabled:` block that computes
`decayed` and calls `_try_smart_roll`). Replace its body with a both-sides loop:
```python
        # LTP Decay → single-side roll per decayed leg (reference exit_logic step 2)
        if self._ltp_decay_enabled:
            rolled_any = False
            for _side, _ltp in (("CE", pos.ce_leg.ltp), ("PE", pos.pe_leg.ltp)):
                if 0 < _ltp < self._ltp_exit_min and self._position and self._position.status == "open":
                    logger.info("SellStraddle[%s]: LTP DECAY %s ltp=%.2f < %.2f — single-side roll",
                                self._underlying, _side, _ltp, self._ltp_exit_min)
                    await self._single_side_roll(_side, now, f"ltp_decay_{_side}")
                    rolled_any = True
            if rolled_any:
                return
```

- [ ] **Step 3: Write the invariant test**

Create `tests/strategies/test_single_side_roll.py`:
```python
import asyncio
import datetime
from data_layer.base_feeder import EventBus
from config.global_config import IST, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


def _mk(strategy):
    strategy._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
        ce_leg=StraddleLeg("CE", 23500, 80.0, 10.0),   # CE decayed to 10
        pe_leg=StraddleLeg("PE", 23500, 80.0, 70.0),
        net_credit=160.0, status="open",
    )


def test_single_side_roll_no_candidate_closes_both():
    async def run():
        s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
        _mk(s)
        s._spot = 23500
        s._strike_prem = {}   # empty → scan_pool finds nothing
        await s._single_side_roll("CE", datetime.datetime.now(IST), "ltp_decay_CE")
        assert s._position is None   # 0-or-2 invariant: both legs closed
    asyncio.run(run())
```

- [ ] **Step 4: Run**

Run: `python -m pytest tests/strategies/test_single_side_roll.py -v`
Expected: PASS
Run: `python -c "import strategies.sell_straddle; print('ok')"` → `ok`

- [ ] **Step 5: Commit**

```bash
git add strategies/sell_straddle.py tests/strategies/test_single_side_roll.py
git commit -m "feat(straddle): single-side LTP-decay roll with 0-or-2 invariant"
```

---

## Task 5: 4-outcome `_try_smart_roll` rewrite + `_single_side_roll_to`

**Files:**
- Modify: `strategies/sell_straddle.py`

- [ ] **Step 1: Add `_single_side_roll_to`**

Add to `SellStraddleStrategy` (after `_single_side_roll`):
```python
    async def _single_side_roll_to(self, side: str, strike: int, ltp: float, now: datetime, reason: str) -> None:
        """Partial roll: close one side and open a pre-selected candidate strike on that side.
        0-or-2 invariant: if ltp invalid, close the surviving leg too."""
        other = "PE" if side == "CE" else "CE"
        await self._close_leg(side, f"partial_roll_{reason}", now)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0
        if strike and ltp and ltp >= ltp_target:
            await self._open_leg(side, strike, ltp, now, f"partial_roll_{reason}")
            self._persist()
            return
        logger.warning("SellStraddle[%s]: partial roll %s invalid candidate — closing %s (0-or-2).",
                       self._underlying, side, other)
        await self._close_leg(other, f"partial_cleanup_{reason}", now)
        self._position = None
        self._persist()
```

- [ ] **Step 2: Rewrite `_try_smart_roll`**

Replace the entire body of `_try_smart_roll` with the 4-outcome version:
```python
    async def _try_smart_roll(self, now: datetime, trigger: str) -> bool:
        """Reference exit_logic.perform_smart_roll — scan pool, then by per-leg strike:
        Virtual / Partial / Physical / Full-exit. Returns True (caller does not also close)."""
        from strategies.straddle_selection import scan_pool, classify_roll
        pos = self._position
        if not pos:
            return True
        if not self._is_in_entry_window(now) or self._trades_today >= self._max_trades:
            await self._close_position(f"full_exit_{trigger}")
            return True

        ss = RuntimeConfig.index_section(self._underlying, "sell_straddle")
        rules = ss.get("entry_rules_reentry", [])
        step = self._cfg.exchange.strike_steps.get(self._underlying, 50.0) if self._cfg else 50.0
        offset = int(ss.get("v_slope_pool_offset") or ss.get("reentry_offset") or 4)
        ltp_target = self._ltp_target if self._ltp_target > 0 else 50.0

        sel = scan_pool(
            self._strike_prem, self._spot, step, offset, ltp_target,
            rule_pass=lambda cs, ps: _eval_rules(rules, self._pair_indicators(cs, ps) or {})[0],
            metric=ss.get("reentry_best_metric", "balanced_premium"),
        )
        ce_same = pe_same = False
        ce_s = pe_s = ce_l = pe_l = None
        if sel:
            ce_s, pe_s, ce_l, pe_l = sel
            ce_same = int(ce_s) == int(pos.ce_leg.strike)
            pe_same = int(pe_s) == int(pos.pe_leg.strike)

        outcome = classify_roll(ce_same, pe_same, has_candidates=bool(sel))
        logger.info("SellStraddle[%s]: SMART ROLL (%s) → %s | cand=%s",
                    self._underlying, trigger, outcome,
                    f"{ce_s}/{pe_s}" if sel else "none")

        if outcome == "full_exit":
            await self._close_position(f"full_exit_{trigger}")
            return True
        if outcome == "virtual":
            pos.ce_leg.entry_price = ce_l
            pos.pe_leg.entry_price = pe_l
            pos.ce_leg.ltp = ce_l
            pos.pe_leg.ltp = pe_l
            pos.net_credit = ce_l + pe_l
            pos.tsl_high_lock_rs = 0.0
            pos.peak_profit = 0.0
            pos.trailing_active = False
            pos.open_time = now
            pos.session_min_vwap = self._ind.get("vwap", float("inf"))
            self._persist()
            return True
        if outcome == "partial_pe":
            await self._single_side_roll_to("PE", pe_s, pe_l, now, trigger)
            return True
        if outcome == "partial_ce":
            await self._single_side_roll_to("CE", ce_s, ce_l, now, trigger)
            return True
        # physical — close both, open new pair
        await self._close_leg("CE", f"physical_roll_{trigger}", now)
        await self._close_leg("PE", f"physical_roll_{trigger}", now)
        self._position = StraddlePosition(
            underlying=self._underlying, atm_at_entry=round(self._spot / step) * step,
            entry_spot=self._spot,
            ce_leg=StraddleLeg("CE", ce_s, ce_l, ce_l),
            pe_leg=StraddleLeg("PE", pe_s, pe_l, pe_l),
            net_credit=ce_l + pe_l, open_time=now, status="open",
            session_min_vwap=self._ind.get("vwap", float("inf")),
            entry_indicators=dict(self._ind),
        )
        from execution_bridge.straddle_bridge import StraddleOrderEvent
        self._event_counter += 1
        await self._bus.publish(Topic.ORDER_REQUEST, StraddleOrderEvent(
            action="ENTRY", underlying=self._underlying, atm=self._position.atm_at_entry,
            ce_strike=ce_s, pe_strike=pe_s, ce_ltp=ce_l, pe_ltp=pe_l,
            lot_multiplier=self._lot_multiplier, lot_size=self._lot_size, spot=self._spot,
            indicators=dict(self._ind),
            event_id=f"{self._underlying}_PHYSROLL_{self._event_counter}",
        ))
        self._persist()
        logger.info("SellStraddle[%s]: PHYSICAL ROLL → CE%s PE%s", self._underlying, ce_s, pe_s)
        return True
```

- [ ] **Step 3: Import + full sweep**

Run: `python -c "import strategies.sell_straddle; print('ok')"` → `ok`
Run: `python -m pytest tests/strategies/ -q` → all pass.

- [ ] **Step 4: Commit**

```bash
git add strategies/sell_straddle.py
git commit -m "feat(straddle): 4-outcome smart roll (virtual/partial/physical/full-exit) via scan_pool"
```

---

## Task 6: guardrail_pnl before day-%

**Files:**
- Modify: `strategies/sell_straddle.py`

- [ ] **Step 1: Move the guardrail_pnl block above the day-% block**

In `_check_exits`, the order today is: EOD → day-% (`if self._initial_net_credit > 0:`)
→ guardrail_pnl (`if self._guardrail_pnl_enabled:`). Cut the entire
`if self._guardrail_pnl_enabled:` block and paste it ABOVE the
`if self._initial_net_credit > 0:` day-% block (immediately after the EOD block's
closing `return`). This matches the reference (PnL guardrail is the first mandatory
guardrail). Make no logic changes inside the block — only its position.

- [ ] **Step 2: Verify order**

Run: `python -c "import strategies.sell_straddle; print('ok')"` → `ok`
Run (confirm guardrail_pnl now precedes day-profit):
`python -m pytest tests/strategies/ -q` → all pass.
Manually confirm in the file: `_guardrail_pnl_enabled` block appears BEFORE
`_day_profit_target_pct` usage in `_check_exits`.

- [ ] **Step 3: Commit**

```bash
git add strategies/sell_straddle.py
git commit -m "fix(straddle): guardrail_pnl precedes day-% (reference exit order)"
```

---

## Task 7: Async smoke + full sweep

**Files:**
- Create: `tests/strategies/test_roll_smoke.py`

- [ ] **Step 1: Write the smoke test**

Create `tests/strategies/test_roll_smoke.py`:
```python
import asyncio
import datetime
from data_layer.base_feeder import EventBus, OptionTick
from config.global_config import IST, Topic, GlobalConfig
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg
from execution_bridge.straddle_bridge import StraddleOrderEvent


def test_single_side_roll_emits_close_and_open_when_candidate_exists():
    async def run():
        bus = EventBus()
        seen = []
        q = bus.subscribe(Topic.ORDER_REQUEST)
        s = SellStraddleStrategy(bus, cfg=GlobalConfig(), underlying="NIFTY")
        s._spot = 23500
        s._position = StraddlePosition(
            underlying="NIFTY", atm_at_entry=23500, entry_spot=23500,
            ce_leg=StraddleLeg("CE", 23500, 80.0, 10.0),
            pe_leg=StraddleLeg("PE", 23500, 80.0, 70.0),
            net_credit=160.0, status="open",
        )
        # populate cache so scan_pool can find a CE candidate >= target on both sides
        for k in (23450, 23500, 23550):
            s._strike_prem[(k, "CE")] = {"ltp": 70.0, "atp": 69.0}
            s._strike_prem[(k, "PE")] = {"ltp": 72.0, "atp": 71.0}
            s._prev_atp_closed[(k, "CE")] = 69.5
            s._prev_atp_closed[(k, "PE")] = 72.5
        await s._single_side_roll("CE", datetime.datetime.now(IST), "ltp_decay_CE")
        while not q.empty():
            seen.append(q.get_nowait())
        actions = [(e.action, tuple(e.legs)) for e in seen if isinstance(e, StraddleOrderEvent)]
        # Expect at least a single-leg EXIT(CE) then ENTRY(CE)
        assert ("EXIT", ("CE",)) in actions
        assert ("ENTRY", ("CE",)) in actions
        assert s._position is not None   # survived (re-entered), not full-closed
    asyncio.run(run())
```

- [ ] **Step 2: Run it**

Run: `python -m pytest tests/strategies/test_roll_smoke.py -v`
Expected: PASS

- [ ] **Step 3: Full repo sweep**

Run: `python -m pytest tests/ -q`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add tests/strategies/test_roll_smoke.py
git commit -m "test(straddle): single-side roll emits single-leg EXIT+ENTRY (smoke)"
```

---

## Self-Review notes
- **Spec coverage:** C1 bridge legs → Task 2; C2 0-or-2 invariant → Task 4 (`_single_side_roll`) + Task 5 (`_single_side_roll_to`); C3 single-side decay roll → Task 4; C4 4-outcome smart roll → Task 1 (`classify_roll`) + Task 5; C5 guardrail_pnl order → Task 6, partial P&L → Task 3 (`_close_leg` books leg P&L). All covered.
- **Type consistency:** `classify_roll(ce_same, pe_same, has_candidates) -> str` used identically in Task 5; `legs` is `list[str]` everywhere; `_close_leg`/`_open_leg`/`_single_side_roll`/`_single_side_roll_to` signatures consistent across tasks.
- **Regression safety:** bridge default `legs=["CE","PE"]` preserves whole-straddle behaviour; existing 15 selection tests must remain green (checked in Tasks 4/5/7 sweeps).
