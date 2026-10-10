"""scripts/sell_straddle_oct9_vp_oi_backtest.py

Drives the REAL SellStraddleStrategy engine + REAL VpOiRegimeAdapter against
REAL NIFTY data for 2026-10-09 (today's actual live session), fetched from
Zerodha's Kite Connect historical API (with real oi=1 -- see
.backtest_scratch/fetch_oct9_data.py). Purpose: re-run today with GENUINE,
never-frozen OI (unlike the real live run, which hit the DedupBuffer bug and
froze futures_oi for ~4 hours from ~10:20 onward) and directly check whether
the regime would have reached Highly Bearish/Bullish/Volatile with correct
data -- settling the open question from tonight's live-day review.

Harness: same fake-clock + real-EventBus seam as
scripts/sell_straddle_calculative_vwap_week_backtest.py, simplified to one
real day. vp_oi_enabled is set directly on the strategy instance (bypassing
RuntimeConfig/ClientProfile, which a backtest has no real DB row for) --
same construction VpOiRegimeAdapter(strike_step=step) config.py itself uses.

Usage: python scripts/sell_straddle_oct9_vp_oi_backtest.py
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

logging.basicConfig(level=logging.WARNING,
                     format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("strategies.sell_straddle").setLevel(logging.INFO)
logging.getLogger("strategies.sell_straddle.exits").setLevel(logging.WARNING)  # quiet per-tick noise

CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", ".backtest_scratch", "oct9_real_data.json")
CLIENT_ID = "BT_OCT9"
BINDING_ID = "BT_OCT9_B1"
DAY = _date_cls(2026, 10, 9)
EXPIRY = _date_cls(2026, 10, 13)  # NIFTY26O13... weekly, confirmed from fetch script's tradingsymbols


class _FakeClock:
    sim_now: _dt_cls = None


class _FakeDateTime(_dt_cls):
    @classmethod
    def now(cls, tz=None):
        base = _FakeClock.sim_now
        if tz is None:
            return base.replace(tzinfo=None)
        return base.astimezone(tz)


def _patch_clock():
    import strategies.sell_straddle.engine as _eng
    import strategies.sell_straddle.exits as _ex
    import strategies.sell_straddle.rolling as _rl
    import strategies.sell_straddle.config as _cfgmod
    import strategies.sell_straddle.entries as _en
    for mod in (_eng, _ex, _rl, _cfgmod, _en):
        mod.datetime = _FakeDateTime


def _intrabar_points(bar: dict) -> list:
    o, h, l, c = bar["open"], bar["high"], bar["low"], bar["close"]
    if c >= o:
        return [o, l, h, c]
    return [o, h, l, c]


def _parse_bars(raw_candles: list) -> list:
    """Kite candle shape: [ts, o, h, l, c, vol, (oi)] -> dict."""
    out = []
    for c in raw_candles:
        ts = _dt_cls.fromisoformat(c[0])
        out.append({
            "ts": ts, "open": c[1], "high": c[2], "low": c[3], "close": c[4],
            "volume": c[5], "oi": c[6] if len(c) > 6 else 0,
        })
    return out


async def _noop():
    return None


async def main():
    with open(CACHE_PATH) as f:
        cache = json.load(f)

    spot_bars = _parse_bars(cache["spot"])
    fut_bars = _parse_bars(cache["futures"])
    opt_bars = {k: _parse_bars(v) for k, v in cache["options"].items()}
    fut_by_ts = {b["ts"]: b for b in fut_bars}
    opt_by_ts = {k: {b["ts"]: b for b in v} for k, v in opt_bars.items()}

    _FakeClock.sim_now = _dt_cls.combine(DAY, _time_cls(9, 15, 0), tzinfo=IST)
    _patch_clock()

    from data_layer.instrument_registry import REGISTRY
    REGISTRY_TOKEN = os.environ.get("UPSTOX_TOKEN", "")
    try:
        await asyncio.to_thread(REGISTRY.load_sync, "NIFTY", REGISTRY_TOKEN)
    except Exception as exc:
        print(f"REGISTRY.load_sync failed (non-fatal for this backtest, strike resolution "
              f"for exec legs may be limited): {exc}")

    from strategies.sell_straddle import SellStraddleStrategy
    from strategies.vp_oi_regime.live_adapter import VpOiRegimeAdapter
    from data_layer import position_store as ps

    # ConfigMixin._load_thresholds() re-reads vp_oi_enabled from the real
    # RuntimeConfig/ClientProfile loader on EVERY cycle (not just once at
    # startup) -- a one-time post-start attribute override gets stomped back
    # to False within the first cycle or two. Force it at the real source
    # instead, matching this script's own REST-seed monkeypatch style.
    import strategies.sell_straddle.config as _ss_cfg
    _orig_load_cfg = _ss_cfg.load_sell_straddle_config
    def _load_cfg_force_vp_oi(underlying, cfg, client_id=""):
        result = _orig_load_cfg(underlying, cfg, client_id=client_id)
        result.vp_oi_enabled = True
        return result
    _ss_cfg.load_sell_straddle_config = _load_cfg_force_vp_oi

    bus = EventBus()
    ss = SellStraddleStrategy(
        bus, GlobalConfig(), underlying="NIFTY", lot_multiplier=1,
        client_id=CLIENT_ID, binding_id=BINDING_ID,
    )
    ss._seed_pool = _noop
    ss._entry_expiry_date = EXPIRY
    ss._is_crypto = False
    ss._captures_futures = True  # so futures-source ticks actually update self._futures_oi

    ps.clear(ss._persist_key)
    ps.clear(ss._persist_key + "_session")

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

        async def _deliver(delay: float):
            await asyncio.sleep(delay)
            from execution_bridge.straddle_bridge import StraddleFillEvent
            fill = StraddleFillEvent(
                action=ev.action, underlying=ev.underlying, atm=ev.atm,
                ce_strike=ev.ce_strike, pe_strike=ev.pe_strike,
                ce_fill=ev.ce_ltp, pe_fill=ev.pe_ltp,
                client_id=ss._client_id, binding_id=ss._binding_id,
                event_id=ev.event_id, legs=ev.legs,
            )
            ss._on_fill(fill)
        asyncio.create_task(_deliver(0.15 if ev.action == "ENTRY" else 0.2))

    ss._emit_order = _fake_emit
    ss.start()
    await asyncio.sleep(0.05)
    assert ss._vp_oi_enabled is True and ss._vp_oi_adapter is not None, (
        "vp_oi_enabled monkeypatch didn't take -- _load_thresholds() hasn't "
        "run yet or patched the wrong reference")

    # _check_vp_oi_regime only runs while a position is open (matches live
    # behavior exactly -- confirmed in the real log, the VP/OI heartbeat
    # started the instant of the real 09:17 entry). This backtest's own
    # entry-selection rules use default admin config, not SA5770's real
    # tuned thresholds (not available to this script) -- rather than try
    # to replicate those, directly seed the SAME real entry that actually
    # happened live (CE22400/PE22400 @ 09:17:00, credit 255.70, confirmed
    # from last night's real log), so the VP/OI question this backtest
    # exists to answer gets tested from minute 1, independent of whether
    # this script's own entry-rule replication is faithful.
    from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition
    entry_ts = _dt_cls.combine(DAY, _time_cls(9, 17, 0), tzinfo=IST)
    ss._position = StraddlePosition(
        underlying="NIFTY", atm_at_entry=22400, entry_spot=22400,
        ce_leg=StraddleLeg("CE", 22400, 124.65, ltp=124.65, open_time=entry_ts,
                            symbol="NIFTY26O1322400CE"),
        pe_leg=StraddleLeg("PE", 22400, 131.05, ltp=131.05, open_time=entry_ts,
                            symbol="NIFTY26O1322400PE"),
        expiry_date=EXPIRY, net_credit=255.70, open_time=entry_ts, status="open",
        lot_size=65,
    )
    ss._trades_today = 1

    last_known_opt: dict = {}
    print(f"\n{'='*78}\nReplaying REAL 2026-10-09 session with GENUINE (never-frozen) OI\n{'='*78}")

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
            await asyncio.sleep(0.001)

        await bus.publish(Topic.CANDLE_CLOSE, CandleEvent(
            symbol="NIFTY", timeframe=1, open=sbar["open"], high=sbar["high"],
            low=sbar["low"], close=sbar["close"], volume=0,
            timestamp=bar_ts.replace(second=0, microsecond=0),
        ))

        # Snapshot VP/OI state every bar for the full-resolution timeline.
        adapter = ss._vp_oi_adapter
        regime_timeline.append({
            "ts": bar_ts.isoformat(), "spot": spot_pts[-1],
            "regime": adapter.last_decision.regime if adapter.last_decision else None,
            "future_oi": adapter.last_future_oi_now, "call_oi": adapter.last_call_oi_now,
            "put_oi": adapter.last_put_oi_now,
            "future_trend": adapter.last_future_trend, "call_trend": adapter.last_call_trend,
            "put_trend": adapter.last_put_trend,
        })

    await asyncio.sleep(0.5)

    # ── Report ──────────────────────────────────────────────────────────────
    print(f"\n{'='*78}\nRESULT\n{'='*78}")
    highly_or_volatile = [r for r in regime_timeline if r["regime"] and
                          ("Highly" in r["regime"] or "Volatile" in r["regime"])]
    print(f"Total 1-min snapshots: {len(regime_timeline)}")
    print(f"Snapshots with a regime reading at all: {sum(1 for r in regime_timeline if r['regime'])}")
    print(f"Highly Bearish/Bullish/Volatile occurrences: {len(highly_or_volatile)}")
    if highly_or_volatile:
        print("\nThe moments it WOULD have fired, with real unfrozen OI:")
        for r in highly_or_volatile:
            print(f"  {r['ts']}  regime={r['regime']}  "
                  f"future={r['future_trend']}({r['future_oi']})  "
                  f"call={r['call_trend']}({r['call_oi']})  put={r['put_trend']}({r['put_oi']})")
    else:
        print("\nCONFIRMED: even with genuine, never-frozen OI the whole day, the regime "
              "never once reached Highly Bearish/Bullish/Volatile. The absence of a signal "
              "today was a real market read, not a consequence of the live freeze bug.")

    print(f"\nTotal trade events: {len(events)}")
    for ev in events:
        print(f"  [{ev['sim_ts'][11:19]}] {ev['action']:5s} CE{ev['ce_strike']:.0f}/"
              f"PE{ev['pe_strike']:.0f} legs={''.join(ev['legs']) or '-'} reason={ev['close_reason']}")

    out_path = os.path.join(os.path.dirname(__file__), "..", ".backtest_scratch",
                             "oct9_backtest_regime_timeline.json")
    with open(out_path, "w") as f:
        json.dump(regime_timeline, f, indent=1)
    print(f"\nFull timeline saved to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
