# Modular Indicators + Per-TF Engine + Plug-and-Play Strategies — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure indicators into a modular package computed once by a shared per-timeframe engine (VWAP sourced from broker ATP), evaluate rules on their own timeframe, and make each strategy a config-gated plug-and-play plugin.

**Architecture:** Keep the EventBus, feeders, and execution path. Add `matrix_engine/indicators/` (one file per indicator), a shared `IndicatorEngine` (per-`(symbol,tf)` closed-minute snapshots), a tf-aware `RuleEvaluator`, and a `StrategyPlugin` framework with a registry + config-gated loader. Migrate IC/Straddle/Trap onto it.

**Tech Stack:** Python 3.9+, asyncio, numpy, pytest. Reference semantics from `E:\Option_Selling\Option_Selling_May_2026/bot/hub/indicators`.

---

## File Structure

| File | Responsibility |
|---|---|
| `matrix_engine/indicators/__init__.py` | Re-export all indicator fns + constants (backward compat) |
| `matrix_engine/indicators/constants.py` | RSI_PERIOD, VWAP_WINDOW, ADX_PERIOD |
| `matrix_engine/indicators/rsi.py` | Wilder's RSI(14) |
| `matrix_engine/indicators/roc.py` | ROC = 100*(src-src[len])/src[len] |
| `matrix_engine/indicators/vwap.py` | Feed-ATP helpers: `leg_atp(tick)`, `combined_vwap(atps)` |
| `matrix_engine/indicators/slope.py` | `vwap_slope(vwaps, occurrences)` rising/falling + counts |
| `matrix_engine/indicators/adx.py` `ema.py` `atr.py` `volume.py` | Moved from current indicators.py |
| `matrix_engine/indicators/snapshot.py` | `TechSnapshot` dataclass |
| `matrix_engine/indicator_engine.py` | Shared per-(symbol,tf) engine + closed-minute cache + ATP store |
| `matrix_engine/rule_evaluator.py` | tf-aware rule evaluation with occurrences |
| `strategies/plugin.py` | `StrategyPlugin` ABC, `@register_strategy`, `StrategyRegistry` |
| `strategies/loader.py` | Instantiate enabled plugins from config |
| `data_layer/base_feeder.py` | Add `atp` field to `OptionTick` |

---

## Task 1: Split indicators into a package (no behavior change)

**Files:**
- Create: `matrix_engine/indicators/__init__.py`, `constants.py`, `rsi.py`, `vwap.py`, `roc.py`, `adx.py`, `ema.py`, `atr.py`, `volume.py`, `snapshot.py`
- Delete: `matrix_engine/indicators.py`
- Test: `tests/indicators/test_backward_compat.py`

- [ ] **Step 1: Write the failing test** (`tests/indicators/test_backward_compat.py`)

```python
import numpy as np

def test_existing_imports_still_work():
    from matrix_engine.indicators import rsi, vwap, adx, ema, atr, volume_spike
    from matrix_engine.indicators import RSI_PERIOD, VWAP_WINDOW, ADX_PERIOD
    assert RSI_PERIOD == 14 and VWAP_WINDOW == 500 and ADX_PERIOD == 20

def test_rsi_neutral_when_insufficient():
    from matrix_engine.indicators import rsi
    assert rsi(np.array([100.0, 101.0])) == 50.0

def test_new_indicators_exported():
    from matrix_engine.indicators import roc, vwap_slope, combined_vwap, leg_atp
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/indicators/test_backward_compat.py -v`
Expected: FAIL (import errors — `roc`/`vwap_slope` not present yet)

- [ ] **Step 3: Create constants.py** (already created in session; ensure exact content)

```python
from __future__ import annotations
RSI_PERIOD:  int = 14
VWAP_WINDOW: int = 500
ADX_PERIOD:  int = 20
```

- [ ] **Step 4: Create rsi.py, adx.py, ema.py, atr.py, volume.py, snapshot.py** by moving the existing function bodies verbatim from the old `matrix_engine/indicators.py` (rsi, adx, ema, atr, volume_spike, TechSnapshot). Each imports constants where needed (`from matrix_engine.indicators.constants import RSI_PERIOD`, etc.). Show rsi.py as the pattern:

```python
from __future__ import annotations
import numpy as np
from numpy.typing import NDArray
from matrix_engine.indicators.constants import RSI_PERIOD

def rsi(closes: NDArray[np.float64]) -> float:
    period = RSI_PERIOD
    n = period + 1
    if len(closes) < n:
        return 50.0
    deltas = np.diff(closes[-n:])
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_g = float(gains[:period].mean()); avg_l = float(losses[:period].mean())
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + float(gains[i]))  / period
        avg_l = (avg_l * (period - 1) + float(losses[i])) / period
    if avg_l == 0:
        return 100.0
    return float(100.0 - 100.0 / (1.0 + avg_g / avg_l))
```

- [ ] **Step 5: Create roc.py** (ported from reference `bot/hub/indicators/roc.py`)

```python
from __future__ import annotations
import numpy as np
from numpy.typing import NDArray

def roc(closes: NDArray[np.float64], length: int = 9) -> float:
    """100 * (src - src[length]) / src[length]; matches reference ROCIndicator."""
    if len(closes) <= length:
        return 0.0
    ref = float(closes[-1 - length])
    if ref == 0:
        return 0.0
    return float(100.0 * (float(closes[-1]) - ref) / ref)
```

- [ ] **Step 6: Create vwap.py** (feed-ATP helpers — VWAP is NOT computed)

```python
from __future__ import annotations
from typing import List, Optional

def leg_atp(tick) -> float:
    """Return the exchange ATP (avg traded price = instrument VWAP) from a tick."""
    return float(getattr(tick, "atp", 0.0) or 0.0)

def combined_vwap(atps: List[float]) -> Optional[float]:
    """Sum the per-leg ATPs for the strategy's legs. None if any leg missing."""
    if not atps or any(a is None or a <= 0 for a in atps):
        return None
    return float(sum(atps))
```

- [ ] **Step 7: Create slope.py** (ported from reference `get_vwap_slope_status`)

```python
from __future__ import annotations
from typing import List, Tuple

def vwap_slope(vwaps: List[float], occurrences: int = 1) -> Tuple[bool, bool, float, float, int, int]:
    """
    vwaps: closed-minute combined VWAPs, newest first [v_curr, v_prev, ...],
           each one timeframe-boundary apart.
    Returns (rising_now_ok, falling_now_ok, v_curr, v_prev, cons_rising, cons_falling).
    """
    if len(vwaps) < 2:
        v = vwaps[0] if vwaps else 0.0
        return False, False, v, v, 0, 0
    v_curr, v_prev = vwaps[0], vwaps[1]
    cons_rising = 0
    for i in range(len(vwaps) - 1):
        if vwaps[i] > vwaps[i + 1]: cons_rising += 1
        else: break
    cons_falling = 0
    for i in range(len(vwaps) - 1):
        if vwaps[i] < vwaps[i + 1]: cons_falling += 1
        else: break
    return (
        (v_curr > v_prev) and cons_rising >= occurrences,
        (v_curr < v_prev) and cons_falling >= occurrences,
        v_curr, v_prev, cons_rising, cons_falling,
    )
```

- [ ] **Step 8: Create __init__.py** re-exporting everything

```python
from matrix_engine.indicators.constants import RSI_PERIOD, VWAP_WINDOW, ADX_PERIOD
from matrix_engine.indicators.rsi import rsi
from matrix_engine.indicators.roc import roc
from matrix_engine.indicators.vwap import leg_atp, combined_vwap
from matrix_engine.indicators.slope import vwap_slope
from matrix_engine.indicators.adx import adx
from matrix_engine.indicators.ema import ema
from matrix_engine.indicators.atr import atr
from matrix_engine.indicators.volume import volume_spike
from matrix_engine.indicators.snapshot import TechSnapshot
__all__ = ["RSI_PERIOD","VWAP_WINDOW","ADX_PERIOD","rsi","roc","leg_atp",
           "combined_vwap","vwap_slope","adx","ema","atr","volume_spike","TechSnapshot"]
```

Note: the old top-level `vwap(highs,lows,closes,volumes)` function is intentionally NOT re-exported (VWAP is now feed-ATP). Update its two importers in Step 9.

- [ ] **Step 9: Update importers that used the old `vwap()`**. Grep `from matrix_engine.indicators import` across the repo; `sell_straddle.py` imported `vwap` — remove it there (straddle will use engine ATP in Task 6). `matrix_engine/candle_cache.py` and `strategies/base_strategy.py`/`strategy_*` may import `vwap`; replace any self-computed VWAP usage with a TODO marker comment `# VWAP now from IndicatorEngine ATP (Task 5)` and keep them compiling by removing the import. Verify with: `python -c "import matrix_engine.indicators"`.

- [ ] **Step 10: Delete old file**

```bash
git rm matrix_engine/indicators.py
```

- [ ] **Step 11: Run tests + import smoke**

Run: `python -m pytest tests/indicators/ -v && python -c "import matrix_engine.indicators; print('ok')"`
Expected: PASS, prints `ok`

- [ ] **Step 12: Commit**

```bash
git add matrix_engine/indicators tests/indicators
git commit -m "refactor: split indicators into modular package + add roc/slope/atp helpers"
```

---

## Task 2: Add ATP to OptionTick + capture in feeders

**Files:**
- Modify: `data_layer/base_feeder.py` (OptionTick dataclass)
- Modify: `data_layer/global_feeder.py` (FyersFeeder + UpstoxFeeder `_parse_frame`)
- Test: `tests/test_option_tick_atp.py`

- [ ] **Step 1: Write the failing test**

```python
from data_layer.base_feeder import OptionTick
from datetime import date, datetime

def test_optiontick_has_atp_default_zero():
    t = OptionTick(symbol="X", underlying="NIFTY", strike=23450.0,
                   option_type="CE", expiry=date(2026,6,2), ltp=100.0,
                   bid=99.0, ask=101.0, oi=0, change_oi=0, volume=0,
                   iv=0.0, delta=0.0, timestamp=datetime.now())
    assert t.atp == 0.0
    t2 = OptionTick(symbol="X", underlying="NIFTY", strike=23450.0,
                    option_type="CE", expiry=date(2026,6,2), ltp=100.0,
                    bid=99.0, ask=101.0, oi=0, change_oi=0, volume=0,
                    iv=0.0, delta=0.0, timestamp=datetime.now(), atp=51.1)
    assert t2.atp == 51.1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_option_tick_atp.py -v`
Expected: FAIL (`atp` is not a field)

- [ ] **Step 3: Add `atp` field** to `OptionTick` in `data_layer/base_feeder.py` — add `atp: float = 0.0` as the last field (after `delta`/`timestamp`, keep defaulted so all existing constructions stay valid).

- [ ] **Step 4: Capture ATP in FyersFeeder._parse_frame** (`data_layer/global_feeder.py`) — in the OptionTick construction add `atp = float(raw.get("avg_trade_price") or 0.0),`.

- [ ] **Step 5: Capture ATP in UpstoxFeeder._parse_frame** — in `_extract_extras` add an `atp` key reading the full-mode field, and set `atp=extras["atp"]` on the OptionTick. Mark with `# VERIFY exact Upstox atp path from a live decoded frame (spec §8)`.

- [ ] **Step 6: Run tests**

Run: `python -m pytest tests/test_option_tick_atp.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add data_layer/base_feeder.py data_layer/global_feeder.py tests/test_option_tick_atp.py
git commit -m "feat: capture broker ATP (avg_trade_price) on OptionTick for feed-sourced VWAP"
```

---

## Task 3: IndicatorEngine — closed-minute snapshots + ATP store

**Files:**
- Create: `matrix_engine/indicator_engine.py`
- Test: `tests/test_indicator_engine.py`

- [ ] **Step 1: Write the failing test**

```python
import asyncio, numpy as np
from datetime import datetime
from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus, OptionTick
from matrix_engine.indicator_engine import IndicatorEngine

def _opt(strike, ot, ltp, atp):
    from datetime import date
    return OptionTick(symbol=f"NIFTY{int(strike)}{ot}", underlying="NIFTY",
        strike=float(strike), option_type=ot, expiry=date(2026,6,2), ltp=ltp,
        bid=ltp, ask=ltp, oi=0, change_oi=0, volume=0, iv=0.0, delta=0.0,
        timestamp=datetime.now(IST), atp=atp)

def test_engine_stores_latest_atp_per_instrument():
    async def run():
        bus = EventBus(); eng = IndicatorEngine(bus)
        await eng._on_option_tick(_opt(23450, "CE", 120.0, 51.1))
        assert eng.atp("NIFTY23450CE") == 51.1
    asyncio.run(run())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_indicator_engine.py -v`
Expected: FAIL (no module)

- [ ] **Step 3: Implement IndicatorEngine**

```python
from __future__ import annotations
import asyncio, logging
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, Deque, Optional
import numpy as np
from config.global_config import IST, Topic
from data_layer.base_feeder import EventBus, OptionTick, CandleEvent
from matrix_engine.indicators import rsi, roc, adx, ema

logger = logging.getLogger(__name__)

class IndicatorEngine:
    """Single source of truth for indicators, per (symbol, timeframe)."""
    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._running = False
        self._atp: Dict[str, float] = {}                     # instrument_symbol -> latest ATP
        self._closes: Dict[tuple, Deque[float]] = defaultdict(lambda: deque(maxlen=600))
        self._snap: Dict[tuple, dict] = {}                   # (symbol,tf) -> indicator dict

    async def _on_option_tick(self, tick: OptionTick) -> None:
        if tick.atp and tick.atp > 0:
            self._atp[tick.symbol] = float(tick.atp)

    async def _on_candle(self, ev: CandleEvent) -> None:
        key = (ev.symbol, ev.timeframe)
        self._closes[key].append(float(ev.close))
        arr = np.array(self._closes[key], dtype=np.float64)
        snap = {"close": float(ev.close)}
        if len(arr) >= 15: snap["rsi"] = rsi(arr)
        if len(arr) >= 10: snap["roc"] = roc(arr, 9)
        self._snap[key] = snap

    def atp(self, instrument_symbol: str) -> float:
        return self._atp.get(instrument_symbol, 0.0)

    def get(self, symbol: str, tf: int, name: str) -> Optional[float]:
        return self._snap.get((symbol, tf), {}).get(name)

    def snapshot(self, symbol: str, tf: int) -> dict:
        return dict(self._snap.get((symbol, tf), {}))

    async def run(self) -> None:
        self._running = True
        opt_q = self._bus.subscribe(Topic.OPTION_TICK)
        cdl_q = self._bus.subscribe(Topic.CANDLE_CLOSE)
        async def drain(q, handler):
            while self._running:
                try: ev = await asyncio.wait_for(q.get(), timeout=1.0)
                except asyncio.TimeoutError: continue
                except asyncio.CancelledError: break
                try: await handler(ev)
                except Exception as exc: logger.warning("IndicatorEngine handler error: %s", exc)
        await asyncio.gather(drain(opt_q, self._on_option_tick), drain(cdl_q, self._on_candle))

    def stop(self) -> None:
        self._running = False
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_indicator_engine.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add matrix_engine/indicator_engine.py tests/test_indicator_engine.py
git commit -m "feat: shared IndicatorEngine — per-(symbol,tf) snapshots + per-instrument ATP store"
```

---

## Task 4: RuleEvaluator — tf-aware with occurrences

**Files:**
- Create: `matrix_engine/rule_evaluator.py`
- Test: `tests/test_rule_evaluator.py`

- [ ] **Step 1: Write the failing test**

```python
from matrix_engine.rule_evaluator import RuleEvaluator

def test_close_lt_vwap_uses_ctx_values():
    ev = RuleEvaluator(engine=None)
    rules = [{"indicator":"advanced","tf":"1","operator_sym":"<",
              "operand1":"CLOSE","operand2":"VWAP","operator":"AND"}]
    ctx = {("1","close"): 193.0, ("1","vwap"): 195.0}
    passed, reason = ev.evaluate(rules, symbol="NIFTY", ctx=ctx)
    assert passed is True
    assert "CLOSE(193" in reason and "VWAP(195" in reason

def test_slope_lt_zero_from_ctx():
    ev = RuleEvaluator(engine=None)
    rules = [{"indicator":"advanced","tf":"2","operator_sym":"<",
              "operand1":"SLOPE","operand2":"VALUE","operand2_val":0,"operator":"AND"}]
    ctx = {("2","slope"): -1.2}
    passed, _ = ev.evaluate(rules, symbol="NIFTY", ctx=ctx)
    assert passed is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_rule_evaluator.py -v`
Expected: FAIL (no module)

- [ ] **Step 3: Implement RuleEvaluator**

```python
from __future__ import annotations
import logging
from typing import Dict, List, Optional, Tuple
logger = logging.getLogger(__name__)

def _cmp(a: float, b: float, op: str) -> bool:
    return {"<":a<b,">":a>b,"<=":a<=b,">=":a>=b,"==":a==b}.get(op, False)

class RuleEvaluator:
    """
    Evaluate admin rules against the indicator value of each rule's OWN timeframe.
    Operand values come from ctx keyed by (tf, operand_lower); if absent, the
    engine snapshot for that tf is consulted. None => rule fails (N/A).
    """
    def __init__(self, engine=None) -> None:
        self._engine = engine

    def _val(self, symbol, tf, name, ctx):
        v = ctx.get((tf, name.lower())) if ctx else None
        if v is None and self._engine is not None:
            v = self._engine.get(symbol, int(tf), name.lower())
        return v

    def evaluate(self, rules: List[dict], symbol: str, ctx: Optional[Dict]=None) -> Tuple[bool, str]:
        if not rules:
            return True, "No rules"
        tokens: List[str] = []; reasons: List[str] = []
        for i, r in enumerate(rules):
            tf = str(r.get("tf", "1"))
            op = r.get("operator_sym", "<")
            o1 = (r.get("operand1") or "").lower()
            o2 = (r.get("operand2") or "").lower()
            v1 = self._val(symbol, tf, o1, ctx)
            v2 = float(r.get("operand2_val", 0)) if o2 == "value" else self._val(symbol, tf, o2, ctx)
            passed = (v1 is not None and v2 is not None and _cmp(float(v1), float(v2), op))
            v1s = f"{v1:.2f}" if isinstance(v1,(int,float)) else "N/A"
            v2s = f"{v2:.2f}" if isinstance(v2,(int,float)) else "N/A"
            reasons.append(f"{o1.upper()}({v1s})[tf{tf}]{op}{o2.upper()}({v2s})={'✓' if passed else '✗'}")
            for b in str(r.get("openBrackets","")): tokens.append(b)
            tokens.append("True" if passed else "False")
            for b in str(r.get("closeBrackets","")): tokens.append(b)
            if i < len(rules)-1:
                tokens.append("and" if (r.get("operator","AND").upper()=="AND") else "or")
        try:
            result = bool(eval(" ".join(tokens)))  # noqa: S307
        except Exception as exc:
            logger.error("RuleEvaluator eval error: %s tokens=%s", exc, tokens); result = False
        return result, " | ".join(reasons)
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_rule_evaluator.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add matrix_engine/rule_evaluator.py tests/test_rule_evaluator.py
git commit -m "feat: tf-aware RuleEvaluator (reads each rule's own timeframe)"
```

---

## Task 5: StrategyPlugin framework + loader

**Files:**
- Create: `strategies/plugin.py`, `strategies/loader.py`
- Test: `tests/test_plugin_loader.py`

- [ ] **Step 1: Write the failing test**

```python
from strategies.plugin import StrategyPlugin, register_strategy, StrategyRegistry
from strategies.loader import load_enabled

def test_registry_and_enabled_gating():
    reg = StrategyRegistry()
    @register_strategy("demo", registry=reg)
    class Demo(StrategyPlugin):
        name = "demo"
        @classmethod
        def enabled(cls, cfg, underlying): return cfg.get("on", False)
        async def start(self): self.started = True
        async def stop(self): pass
    on  = load_enabled(reg, cfg_for={"NIFTY":{"demo":{"on":True}}}, indices=["NIFTY"],
                       deps={})
    off = load_enabled(reg, cfg_for={"NIFTY":{"demo":{"on":False}}}, indices=["NIFTY"],
                       deps={})
    assert len(on) == 1 and on[0].name == "demo"
    assert off == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_plugin_loader.py -v`
Expected: FAIL (no modules)

- [ ] **Step 3: Implement plugin.py**

```python
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Type

class StrategyPlugin(ABC):
    name: str = "base"
    def __init__(self, bus=None, cfg=None, underlying="NIFTY", engine=None,
                 evaluator=None, registry=None, **kw):
        self._bus=bus; self._cfg=cfg; self._underlying=underlying
        self._engine=engine; self._evaluator=evaluator; self._registry=registry
    @classmethod
    def enabled(cls, cfg: dict, underlying: str) -> bool:
        return bool(cfg.get("enabled", False))
    @abstractmethod
    async def start(self) -> None: ...
    @abstractmethod
    async def stop(self) -> None: ...

class StrategyRegistry:
    def __init__(self): self._plugins: Dict[str, Type[StrategyPlugin]] = {}
    def register(self, name, cls): self._plugins[name] = cls
    def all(self): return dict(self._plugins)

_DEFAULT = StrategyRegistry()
def register_strategy(name, registry: StrategyRegistry = None):
    reg = registry or _DEFAULT
    def deco(cls):
        cls.name = name; reg.register(name, cls); return cls
    return deco
def default_registry() -> StrategyRegistry: return _DEFAULT
```

- [ ] **Step 4: Implement loader.py**

```python
from __future__ import annotations
import logging
from typing import Dict, List
from strategies.plugin import StrategyRegistry, StrategyPlugin
logger = logging.getLogger(__name__)

def load_enabled(registry: StrategyRegistry, cfg_for: Dict[str, dict],
                 indices: List[str], deps: dict) -> List[StrategyPlugin]:
    """Instantiate every registered plugin that is enabled for each index."""
    started: List[StrategyPlugin] = []
    for idx in indices:
        idx_cfg = cfg_for.get(idx, {})
        for name, cls in registry.all().items():
            scfg = idx_cfg.get(name, {})
            try:
                if not cls.enabled(scfg, idx): continue
                inst = cls(underlying=idx, cfg=scfg, **deps)
                started.append(inst)
            except Exception as exc:
                logger.error("loader: %s/%s failed: %s", idx, name, exc)
    return started
```

- [ ] **Step 5: Run tests**

Run: `python -m pytest tests/test_plugin_loader.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add strategies/plugin.py strategies/loader.py tests/test_plugin_loader.py
git commit -m "feat: StrategyPlugin ABC + registry + config-gated loader"
```

---

## Task 6: Migrate SellStraddle to plugin + engine (feed-ATP VWAP/SLOPE)

**Files:**
- Modify: `strategies/sell_straddle.py`
- Test: `tests/test_straddle_plugin.py`

- [ ] **Step 1: Write the failing test**

```python
from strategies.sell_straddle import SellStraddleStrategy
from strategies.plugin import StrategyPlugin

def test_straddle_is_plugin_and_enabled_reads_config():
    assert issubclass(SellStraddleStrategy, StrategyPlugin)
    assert SellStraddleStrategy.enabled({"enabled": True}, "NIFTY") is True
    assert SellStraddleStrategy.enabled({"enabled": False}, "NIFTY") is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_straddle_plugin.py -v`
Expected: FAIL (not a StrategyPlugin subclass)

- [ ] **Step 3: Make SellStraddleStrategy subclass StrategyPlugin** — add `@register_strategy("sell_straddle")`, inherit `StrategyPlugin`, accept `engine`/`evaluator` in `__init__`. Keep `start()/stop()`.

- [ ] **Step 4: Replace self-computed indicators** — delete `_recompute_indicators`' VWAP/SLOPE computation; instead in `_option_loop`, record each ATM leg's ATP into the engine (already captured) and build:
  - `vwap = combined_vwap([engine.atp(ce_sym), engine.atp(pe_sym)])`
  - maintain a deque of closed-minute `vwap` values; `slope = vwap_slope(deque, occurrences)`
  - keep RSI/ROC from `engine.get(underlying_premium_key, tf, ...)` OR compute locally from the combined-premium close series (interim acceptable; note in code).

- [ ] **Step 5: Route entry/exit rule checks through RuleEvaluator** — build `ctx` with `(tf,"close")`, `(tf,"vwap")`, `(tf,"slope")`, `(tf,"rsi")`, `(tf,"roc")` for the tfs the rules use, then `self._evaluator.evaluate(rules, self._underlying, ctx)`. Keep the EVAL/EXIT-EVAL logging.

- [ ] **Step 6: Run tests**

Run: `python -m pytest tests/test_straddle_plugin.py tests/test_rule_evaluator.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add strategies/sell_straddle.py tests/test_straddle_plugin.py
git commit -m "refactor(straddle): plugin + feed-ATP VWAP/SLOPE via shared engine + tf-aware rules"
```

---

## Task 7: Migrate IronCondor + Trap to plugins; wire loader in run_system

**Files:**
- Modify: `strategies/iron_condor.py`, `strategies/trap_trading_engine.py`, `run_system.py`
- Test: `tests/test_ic_plugin.py`

- [ ] **Step 1: Write the failing test**

```python
from strategies.iron_condor import IronCondorStrategy
from strategies.plugin import StrategyPlugin

def test_ic_is_plugin():
    assert issubclass(IronCondorStrategy, StrategyPlugin)
    assert IronCondorStrategy.enabled({"enabled": True}, "NIFTY") is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_ic_plugin.py -v`
Expected: FAIL

- [ ] **Step 3: Make IronCondorStrategy + TrapTradingEngine subclass StrategyPlugin** with `@register_strategy("iron_condor")` / `@register_strategy("trap_trading")`, accepting `engine`/`evaluator`. IC keeps its immediate tick-driven entry; its premium reads can stay on `_prem_cache` for now (IC doesn't use VWAP rules) — no behavior change to the working IC path.

- [ ] **Step 4: Wire run_system.py** — construct `engine = IndicatorEngine(bus)`, `evaluator = RuleEvaluator(engine)`, add `engine.run()` to the task list, and replace the hardcoded `_iron_condors`/`_sell_straddles`/`trap_engine` construction with:

```python
from strategies.loader import load_enabled
from strategies.plugin import default_registry
import strategies.iron_condor, strategies.sell_straddle, strategies.trap_trading_engine  # register
deps = {"bus": bus, "engine": engine, "evaluator": evaluator}
cfg_for = {idx: RuntimeConfig.all_sections(idx) for idx in cfg.monitored_indices}
plugins = load_enabled(default_registry(), cfg_for, cfg.monitored_indices, deps)
for p in plugins: p.start() if not asyncio.iscoroutinefunction(p.start) else await p.start()
```
(Provide `RuntimeConfig.all_sections(idx)` returning `{"iron_condor":{...},"sell_straddle":{...},"trap_trading":{...}}` for that index; add it to `data_layer/runtime_config.py` if missing.)

- [ ] **Step 5: Run tests + boot smoke**

Run: `python -m pytest tests/ -v && python -c "import run_system"`
Expected: PASS, import ok

- [ ] **Step 6: Commit**

```bash
git add strategies/iron_condor.py strategies/trap_trading_engine.py run_system.py data_layer/runtime_config.py tests/test_ic_plugin.py
git commit -m "refactor: IC + Trap as plugins; run_system uses IndicatorEngine + loader"
```

---

## Task 8: Remove dead indicator code + live regression

**Files:**
- Modify: strategies (remove leftover private indicator helpers now unused)
- Test: manual live regression on EC2

- [ ] **Step 1: Grep for dead code** — `grep -rn "_recompute_indicators\|_prem_closes\|from matrix_engine.indicators import vwap" strategies/`; remove any now-unused private indicator computation that the engine replaced. Keep ADX/index-based ones the engine doesn't yet provide if still referenced.

- [ ] **Step 2: Run full test suite**

Run: `python -m pytest tests/ -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "chore: remove per-strategy indicator code superseded by IndicatorEngine"
```

- [ ] **Step 4: Live regression (EC2)** — pull, restart, confirm:
  - `IronCondor[NIFTY]: ENTRY ... net_credit>0` still fires (IC unaffected).
  - `EVAL NIFTY [entry_rules_beginning] ... SLOPE(-x.x)[tf2]<VALUE(0)=✓` shows real per-tf values (no N/A).
  - Disabling a strategy via `enabled:false` + restart → that strategy never starts (grep absence of its `started.` log).

---

## Self-Review Notes
- Spec §4.1–4.6 each map to Tasks 1–7; §4.2 ATP → Task 2; §4.3 engine → Task 3; §4.4 evaluator → Task 4; §4.5 framework → Task 5; §4.6 migration → Tasks 6–7; §7 rollout order preserved.
- VWAP-from-ATP is consistent: `leg_atp`/`combined_vwap` (Task 1) → ATP capture (Task 2) → engine store (Task 3) → straddle combines legs (Task 6).
- Names consistent: `register_strategy`, `StrategyRegistry`, `load_enabled`, `IndicatorEngine.get/atp/snapshot`, `RuleEvaluator.evaluate`.
- Open verification (spec §8): exact Upstox ATP field path — flagged in Task 2 Step 5.
