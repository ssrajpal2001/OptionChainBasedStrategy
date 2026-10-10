r"""scripts/sell_straddle_oct9_full_simulator.py

Full-fidelity replay of 2026-10-09 driving the REAL SellStraddleStrategy
engine with SA5770's ACTUAL live NIFTY config (pulled via
scripts/dump_live_config.py + dump_live_config2.py, hardcoded below) --
real entry selection (not a manual seed), real exit-ladder evaluation every
cycle, real rollovers, real VP/OI hedge events, against real Zerodha data
(.backtest_scratch/oct9_real_data.json, oi=1).

Purpose (direct user ask): "I want the simulator to run the backtest such
that when I see the test report I can understand what exactly happened
[on 2026-10-09]" -- which strike was sold and why (the real SLOPE value at
entry), every exit condition checked each cycle (not just the one that
fires), every rollover (old strike -> new strike, why), every VP/OI hedge
event (what regime reading triggered it).

Approach: rather than reimplement the exit ladder's own pass/fail logic
(risking silent drift from the real code), this captures the REAL log
lines the engine already emits every cycle (EXIT-EVAL, ENTRY confirmed,
ROLL complete, VP/OI REGIME, etc.) via a logging.Handler attached to the
strategies.sell_straddle.* loggers, tagged with the simulated timestamp --
same "drive the real class" discipline as every other backtest in this
codebase, extended to capture its own diagnostic output instead of
re-deriving it.

Determinism fixes applied from the start (found + fixed 2026-10-10 in
scripts/sell_straddle_oct9_vp_oi_backtest.py -- see that script's own
docstring for the full incident writeup): backstop-loop cancelled, queue-
drain barrier after every publish batch, asyncio.to_thread patched
synchronous.

Usage: python scripts/sell_straddle_oct9_full_simulator.py
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import date as _date_cls, datetime as _dt_cls, time as _time_cls, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import GlobalConfig, IST, Topic  # noqa: E402
from data_layer.base_feeder import CandleEvent, EventBus, IndexTick, OptionTick  # noqa: E402

CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", ".backtest_scratch", "oct9_real_data.json")
PREVDAY_CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", ".backtest_scratch", "oct8_prevday_data.json")
# CRITICAL SAFETY FIX, found 2026-10-10 real incident: using the REAL
# client_id/binding_id here, needed (it seemed) to load the real config,
# actually caused this backtest to overwrite a REAL tracked file
# (data/chart_history/ssrajpal2001_SA5770_NIFTY_sell_straddle.json) with
# synthetic replay data on its very first run -- indicators.py's own
# _save_chart_history() writes to data/chart_history/{persist_key}.json,
# and persist_key is built directly from client_id+binding_id+underlying,
# completely independent of the config-loading path below. Reverted via
# git checkout; not a real production-data incident (this repo's local
# data/ is a separate, gitignored, machine-local copy per CLAUDE.md's own
# documented architecture -- EC2's real data/ was never touched), but a
# real close call worth fixing at the root rather than re-litigating every
# future backtest script's own file-write side effects individually.
# _patch_real_config() below intercepts at RuntimeConfig.index_section(),
# keyed only on (underlying, section) -- NOT client_id -- so a fake
# client_id/binding_id gets the exact same real effective config with zero
# collision risk against any persist_key-based file path.
CLIENT_ID = "BT_SIM"
BINDING_ID = "BT_SIM_B1"
DAY = _date_cls(2026, 10, 9)
EXPIRY = _date_cls(2026, 10, 13)

# ── SA5770's REAL effective config, 2026-10-10 (dump_live_config.py +
#    dump_live_config2.py, pasted by the user) -- merged into one dict the
#    monkeypatches below serve back to the real config loaders. ──────────────
REAL_RAW_SECTION = {
    "entry_start": "09:15", "entry_end": "15:00", "squareoff_time": "15:37",
    "entry_workflow_mode": "hybrid", "ltp_target": 50, "trail_lock_pct": 10,
    "trail_floor_pct": 5, "pool_itm_depth": 4, "pool_otm_depth": 4,
    "roll_max_itm_steps": 5, "itm_pair_gate_enabled": True,
    "itm_pair_gate_profit_inr": 500, "itm_pair_gate_min_strike_gap": 100.0,
    "day_low_exit_enabled": True, "day_low_freeze_time": "15:20",
    "vp_oi_noise_floor_pct": 15.0, "vp_oi_override_pct": 30.0,
    "post1500_exit_enabled": True, "shadow_vwap_enabled": True,
    "entry_rules_beginning": [
        {"indicator": "advanced", "operator": "AND", "tf": "1",
         "operator_sym": "<", "operand1": "SLOPE", "operand2": "VALUE", "operand2_val": 0},
    ],
    "entry_rules_reentry": [],
    "exit_rules": [
        {"indicator": "advanced", "operator": "AND", "tf": "3",
         "operator_sym": ">", "operand1": "RSI", "operand2": "VALUE", "operand2_val": 55},
        {"indicator": "advanced", "operator": "AND", "tf": "3",
         "operator_sym": ">", "operand1": "ROC", "operand2": "VALUE", "operand2_val": 5},
        {"indicator": "advanced", "operator": "AND", "tf": "3",
         "operator_sym": ">", "operand1": "CLOSE", "operand2": "VWAP", "operand2_val": 0},
        {"indicator": "advanced", "operator": "AND", "tf": "1",
         "operator_sym": ">", "operand1": "SLOPE", "operand2": "SLOPE_PREV", "operand2_val": 0},
    ],
    "tsl_enabled": True,
    "tsl_scalable": {"enabled": False, "base_profit": 1500, "base_lock": 750,
                      "step_profit": 250, "step_lock": 250, "basis": "ltp"},
    "ratio_exit": {"enabled": True, "threshold": 4, "max_entry_ratio": 0},
    "ltp_decay": {"enabled": True, "ltp_exit_min": 20},
    "vwap_rise_sl": {"enabled": True, "tf": 3, "threshold": 3},
    "sl_cooldown_tf_multiplier": 1, "sl_cooldown_minutes": 1,
    "max_trades": 5,
    "per_day": {
        "monday":    {"enabled": True, "profit_target_pct": 11, "loss_sl_pct": 30, "exit_basis": "ltp"},
        "tuesday":   {"enabled": True, "profit_target_pct": 12, "loss_sl_pct": 30, "exit_basis": "ltp"},
        "wednesday": {"enabled": True, "profit_target_pct": 13, "loss_sl_pct": 30, "exit_basis": "ltp"},
        "thursday":  {"enabled": True, "profit_target_pct": 14, "loss_sl_pct": 30, "exit_basis": "ltp"},
        "friday":    {"enabled": True, "profit_target_pct": 15, "loss_sl_pct": 30, "exit_basis": "ltp"},
        "saturday":  {"enabled": False, "profit_target_pct": 0, "loss_sl_pct": 0, "exit_basis": "ltp"},
        "sunday":    {"enabled": False, "profit_target_pct": 0, "loss_sl_pct": 0, "exit_basis": "ltp"},
    },
    "same_day_expiry_enabled": False,
    "lot_multiplier": 20, "product_type": "NRML",
    "lot_size": 65, "entry_basis": "theta", "theta_target": 20,
    "trail_basis": "ltp", "hedge_carry_enabled": True,
    "itm_roll_protection_enabled": False,
    "vp_oi_enabled": True,  # this deployment is sell_straddle_calc_vwap w/ vp_oi_enabled=true
    "balance_ratio": 1.0, "is_crypto": False,
}

logging.basicConfig(level=logging.WARNING,
                     format="%(asctime)s %(levelname)s %(name)s: %(message)s")


class _FakeClock:
    sim_now: _dt_cls = None


class _FakeDateTime(_dt_cls):
    @classmethod
    def now(cls, tz=None):
        base = _FakeClock.sim_now
        return base.replace(tzinfo=None) if tz is None else base.astimezone(tz)


def _patch_clock():
    import strategies.sell_straddle.engine as _eng
    import strategies.sell_straddle.exits as _ex
    import strategies.sell_straddle.rolling as _rl
    import strategies.sell_straddle.config as _cfgmod
    import strategies.sell_straddle.entries as _en
    for mod in (_eng, _ex, _rl, _cfgmod, _en):
        mod.datetime = _FakeDateTime


def _patch_real_config():
    """Force load_sell_straddle_config() (admin-defaults + client-overrides,
    read by _load_thresholds every candle-close) and RuntimeConfig.index_section()
    (read directly by entries.py for entry_rules_beginning/reentry) to both
    always return SA5770's REAL dumped config, instead of this backtest's own
    empty RuntimeConfig/ClientProfile DB rows."""
    import strategies.sell_straddle.config as _ss_cfg
    import data_layer.runtime_config as _rc

    _orig_load_cfg = _ss_cfg.load_sell_straddle_config

    def _patched_load_cfg(underlying, cfg, client_id=""):
        return _orig_load_cfg.__wrapped__(underlying, cfg, client_id="") if False else _real_load_cfg(underlying, cfg)

    # Simplest robust approach: monkeypatch RuntimeConfig.index_section itself
    # (the ONE real data source both load_sell_straddle_config and entries.py
    # ultimately read from) rather than wrap the loader function twice.
    def _patched_index_section(underlying, section):
        if underlying == "NIFTY" and section == "sell_straddle":
            return dict(REAL_RAW_SECTION)
        return {}
    _rc.RuntimeConfig.index_section = staticmethod(_patched_index_section)


def _intrabar_points(bar: dict) -> list:
    o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
    return [o, l, h, c] if c >= o else [o, h, l, c]


def _parse_bars(raw_candles: list) -> list:
    out = []
    for c in raw_candles:
        ts = _dt_cls.fromisoformat(c[0])
        out.append({"ts": ts, "open": c[1], "high": c[2], "low": c[3], "close": c[4],
                    "volume": c[5], "oi": c[6] if len(c) > 6 else 0})
    return out


async def _noop():
    return None


class _CaptureHandler(logging.Handler):
    """Captures every real log record from the engine during replay, tagged
    with the SIMULATED timestamp at the moment it was emitted (not real
    wall-clock time) -- this IS the "what exactly happened" trail: ENTRY
    confirmed, EXIT-EVAL (full ladder status every cycle), ROLL complete,
    VP/OI REGIME, naked-leg/hedge events, all exactly as the real engine
    already logs them, zero reimplementation."""

    def __init__(self):
        super().__init__(level=logging.INFO)
        self.records: list[dict] = []

    def emit(self, record):
        msg = record.getMessage()
        self.records.append({
            "sim_ts": _FakeClock.sim_now.isoformat() if _FakeClock.sim_now else None,
            "logger": record.name, "level": record.levelname, "msg": msg,
        })


async def main() -> None:
    with open(CACHE_PATH) as f:
        cache = json.load(f)

    spot_bars = _parse_bars(cache["spot"])
    fut_bars = _parse_bars(cache["futures"])
    opt_bars = {k: _parse_bars(v) for k, v in cache["options"].items()}
    fut_by_ts = {b["ts"]: b for b in fut_bars}
    opt_by_ts = {k: {b["ts"]: b for b in v} for k, v in opt_bars.items()}

    _FakeClock.sim_now = _dt_cls.combine(DAY, _time_cls(9, 15, 0), tzinfo=IST)
    _patch_clock()
    _patch_real_config()

    capture = _CaptureHandler()
    logging.getLogger("strategies.sell_straddle").addHandler(capture)
    logging.getLogger("strategies.sell_straddle").setLevel(logging.INFO)
    # CRITICAL: the EXIT-EVAL line (full ladder status every cycle -- exactly
    # the "all exit conditions checked" output this report exists to show)
    # is written via self._clog, a SEPARATE per-book logger
    # (_make_strategy_logger, name f"ss_{tag}_{date}", propagate=False) --
    # confirmed earlier tonight while chasing the VP/OI heartbeat logging
    # bug. propagate=False means it NEVER reaches the standard
    # "strategies.sell_straddle" hierarchy above, handler attached there
    # or not. Must attach directly to the real Logger instance.

    from data_layer.instrument_registry import REGISTRY
    try:
        await asyncio.to_thread(REGISTRY.load_sync, "NIFTY", os.environ.get("UPSTOX_TOKEN", ""))
    except Exception as exc:
        print(f"REGISTRY.load_sync non-fatal failure: {exc}")

    from strategies.sell_straddle import SellStraddleStrategy
    from data_layer import position_store as ps

    bus = EventBus()
    ss = SellStraddleStrategy(
        bus, GlobalConfig(), underlying="NIFTY", lot_multiplier=1,
        client_id=CLIENT_ID, binding_id=BINDING_ID,
    )
    ss._seed_pool = _noop
    ss._entry_expiry_date = EXPIRY
    ss._clog.addHandler(capture)
    ss._clog.setLevel(logging.INFO)
    ps.clear(ss._persist_key)
    ps.clear(ss._persist_key + "_session")

    # Real live bindings warm RSI/ROC from the previous trading day's REST
    # history on startup (_seed_exec_legs / fetch_upstox_warm_1m) -- disabling
    # ss._seed_pool above (to avoid a real broker/REST call during replay)
    # otherwise leaves this backtest's pool engine starting completely cold,
    # which is almost certainly why the first attempt's entry selection
    # (CE22550/PE22300) differed from the real live entry (CE22400/PE22400):
    # direct user ask, 2026-10-10: "u can get that from calling historical
    # from prev day as well to warmup the indicators such as rsi,roc,vwamp,
    # vslope." Seed all 34 strikes' CE/PE from REAL Oct 8 Zerodha 1-min
    # closes (same contract, same strike band) -- same seed_strike(strike,
    # side, closes, closes) call _seed_exec_legs itself uses. VWAP/SLOPE are
    # NOT seeded here -- they reset fresh every session by design (broker
    # ATP VWAP has no prior-day carry-over in the live engine either).
    with open(PREVDAY_CACHE_PATH) as f:
        prevday = json.load(f)
    for key, candles in prevday["options"].items():
        strike_str, side = key[:-2], key[-2:]
        closes = [c[4] for c in candles]  # Kite candle: [ts,o,h,l,c,vol,(oi)]
        if closes:
            ss._pool_engine.seed_strike(int(strike_str), side, closes, closes)
    print(f"Seeded RSI/ROC for {len(prevday['options'])} strikes from real 2026-10-08 data.")

    regime_timeline: list[dict] = []
    events: list[dict] = []

    async def _fake_emit(ev):
        events.append({
            "sim_ts": _FakeClock.sim_now.isoformat(), "action": ev.action,
            "legs": list(getattr(ev, "legs", []) or []),
            "close_reason": getattr(ev, "close_reason", None),
            "ce_strike": ev.ce_strike, "pe_strike": ev.pe_strike,
            "ce_ltp": ev.ce_ltp, "pe_ltp": ev.pe_ltp, "event_id": ev.event_id,
        })
        print(f"  [{_FakeClock.sim_now.strftime('%H:%M:%S')}] {ev.action:5s} "
              f"CE{ev.ce_strike:.0f}/PE{ev.pe_strike:.0f} legs={''.join(ev.legs or []) or '-'} "
              f"reason={getattr(ev, 'close_reason', None)}")

        # CRITICAL: the original (non-deterministic) version of this backtest
        # pattern used a REAL asyncio.sleep(0.15/0.2) delay here to simulate
        # "realistic fill latency" -- confirmed as a direct, independent
        # source of nondeterminism: this task's completion races against the
        # fast simulated-time replay loop on REAL wall-clock time, and the
        # _drain() barrier below only waits on the EventBus tick/option/
        # candle queues, not this separately-scheduled task. For a
        # deterministic backtest the fill simply doesn't need an artificial
        # delay -- call it immediately, synchronously, no sleep at all.
        from execution_bridge.straddle_bridge import StraddleFillEvent
        fill = StraddleFillEvent(
            action=ev.action, underlying=ev.underlying, atm=ev.atm,
            ce_strike=ev.ce_strike, pe_strike=ev.pe_strike,
            ce_fill=ev.ce_ltp, pe_fill=ev.pe_ltp,
            client_id=ss._client_id, binding_id=ss._binding_id,
            event_id=ev.event_id, legs=ev.legs,
        )
        ss._on_fill(fill)

    ss._emit_order = _fake_emit
    ss.start()
    await asyncio.sleep(0.05)

    for task in asyncio.all_tasks():
        if task.get_name().endswith("_eod_backstop"):
            task.cancel()

    # exits.py's own record_snapshot call dispatches to a real OS thread pool
    # every cycle -- confirmed source of nondeterminism, see the vp_oi
    # backtest script's own writeup. Stub + globally sync asyncio.to_thread.
    import strategies.vp_oi_regime.recorder as _vp_oi_recorder
    _vp_oi_recorder.record_snapshot = lambda **kwargs: None

    async def _sync_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)
    asyncio.to_thread = _sync_to_thread

    async def _drain():
        qs = [q for k, q in ss._loop_queues.items() if k in ("tick", "option", "candle")]
        for _ in range(200):
            if all(q.qsize() == 0 for q in qs):
                break
            await asyncio.sleep(0)
        for _ in range(20):
            await asyncio.sleep(0)

    last_known_opt: dict = {}
    print(f"\n{'='*78}\nFull simulator replay — real entry selection + real exit ladder\n{'='*78}")

    for bi, sbar in enumerate(spot_bars):
        bar_ts = sbar["ts"]
        spot_pts = _intrabar_points(sbar)
        fbar = fut_by_ts.get(bar_ts)
        fut_pts = _intrabar_points(fbar) if fbar else [None] * 4

        for sub_i in range(4):
            sim_t = bar_ts + timedelta(seconds=15 * sub_i)
            _FakeClock.sim_now = sim_t

            await bus.publish(Topic.INDEX_TICK, IndexTick(
                symbol="NIFTY", ltp=spot_pts[sub_i], open=sbar["open"], high=sbar["high"],
                low=sbar["low"], close=spot_pts[sub_i], volume=0, timestamp=sim_t,
            ))
            if fbar:
                await bus.publish(Topic.INDEX_TICK, IndexTick(
                    symbol="NIFTY", ltp=fut_pts[sub_i], open=fbar["open"], high=fbar["high"],
                    low=fbar["low"], close=fut_pts[sub_i], volume=fbar["volume"],
                    timestamp=sim_t, source="futures", oi=int(fbar["oi"] or 0),
                ))
            for key, series in opt_by_ts.items():
                strike_str, side = key[:-2], key[-2:]
                bar = series.get(bar_ts) or last_known_opt.get(key)
                if bar is None:
                    continue
                last_known_opt[key] = bar
                opt_pts = _intrabar_points(bar)
                px = opt_pts[sub_i]
                await bus.publish(Topic.OPTION_TICK, OptionTick(
                    symbol=f"NIFTY{strike_str}{side}", underlying="NIFTY",
                    strike=float(strike_str), option_type=side, expiry=EXPIRY,
                    ltp=px, bid=px - 0.5, ask=px + 0.5, oi=int(bar["oi"] or 0),
                    change_oi=0, volume=int(bar["volume"] or 0), iv=0.0, delta=0.0,
                    timestamp=sim_t, atp=px,
                ))
            await _drain()

        await bus.publish(Topic.CANDLE_CLOSE, CandleEvent(
            symbol="NIFTY", timeframe=1, open=sbar["open"], high=sbar["high"],
            low=sbar["low"], close=sbar["close"], volume=0,
            timestamp=bar_ts.replace(second=0, microsecond=0),
        ))
        await _drain()

        adapter = getattr(ss, "_vp_oi_adapter", None)
        if adapter is not None:
            regime_timeline.append({
                "ts": bar_ts.isoformat(), "spot": spot_pts[-1],
                "regime": adapter.last_decision.regime if adapter.last_decision else None,
                "future_oi": adapter.last_future_oi_now, "call_oi": adapter.last_call_oi_now,
                "put_oi": adapter.last_put_oi_now,
                "future_trend": adapter.last_future_trend, "call_trend": adapter.last_call_trend,
                "put_trend": adapter.last_put_trend,
                "naked_state": adapter.naked_state,
            })

    await asyncio.sleep(0.5)

    print(f"\n{'='*78}\nRESULT\n{'='*78}")
    print(f"Total trade events: {len(events)}")
    for ev in events:
        print(f"  [{ev['sim_ts'][11:19]}] {ev['action']:5s} CE{ev['ce_strike']:.0f}/"
              f"PE{ev['pe_strike']:.0f} legs={''.join(ev['legs']) or '-'} reason={ev['close_reason']}")
    print(f"\nCaptured {len(capture.records)} real log lines from the engine during replay.")

    out_dir = os.path.join(os.path.dirname(__file__), "..", ".backtest_scratch")
    with open(os.path.join(out_dir, "oct9_full_sim_events.json"), "w") as f:
        json.dump(events, f, indent=1)
    with open(os.path.join(out_dir, "oct9_full_sim_regime.json"), "w") as f:
        json.dump(regime_timeline, f, indent=1)
    with open(os.path.join(out_dir, "oct9_full_sim_logs.json"), "w") as f:
        json.dump(capture.records, f, indent=1)
    print(f"Saved events/regime/logs to {out_dir}")


if __name__ == "__main__":
    asyncio.run(main())
