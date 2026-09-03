"""
scripts/fno_positional_mock_dayrun.py — mock day-run for FnOPositionalBook.

Drives the REAL FnOPositionalBook (real entry gating, real breakeven-trail
logic, real SL/T1 monitoring, real expiry-week exit) against a synthetic
watchlist + synthetic spot/option price paths, with a fake FnOExecutionBridge
simulating realistic order confirmation.

Architecture note: unlike SellStraddle (tick-driven, reacts instantly to
published events), FnOPositionalBook's own _main_loop() paces itself with a
REAL `await asyncio.sleep(POLL_INTERVAL)` (POLL_INTERVAL=30s) — there is no
event to inject to compress this. Simulating a full 09:15-15:30 day at real
30s/poll would take ~6 real hours. So this script does NOT call book.start()
/ book._main_loop() at all -- it drives the book's own constituent methods
(_run_scan, _try_enter_triggered, _try_enter_approaching, _poll_and_monitor,
_check_expiry_exit) directly, in the same order and same conditions
_main_loop uses, advancing a fake clock between calls -- a full simulated
day runs in seconds of real time.

Scope: intercepts at _fetch_ltp / _run_scan / the FNO_ORDER_REQUEST-FNO_ORDER_FILL
bus round-trip (real EventBus, real FnOOrderEvent/FnOFillEvent dataclasses) --
same principle as the SellStraddle harness: full fidelity to the strategy's
own decision logic and the real bridge's confirm-timing contract, without the
production ClientRegistry/ExecutionRouter/broker-auth stack.

Usage:
    python scripts/fno_positional_mock_dayrun.py
"""
from __future__ import annotations

import asyncio
import datetime as _dt_module
import logging
import random
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.global_config import IST, Topic  # noqa: E402
from data_layer.base_feeder import EventBus  # noqa: E402

logging.basicConfig(level=logging.WARNING)
logging.getLogger("strategies.fno_positional").setLevel(logging.INFO)

CLIENT_ID = "MOCKFNO"
BINDING_ID = "MOCKBIND"


class _FakeClock:
    sim_now: _dt_module.datetime = None


class _FakeDateTime(_dt_module.datetime):
    @classmethod
    def now(cls, tz=None):
        base = _FakeClock.sim_now
        if tz is None:
            return base.replace(tzinfo=None)
        return base.astimezone(tz)


def _patch_clock():
    import strategies.fno_positional.book as _bookmod
    _bookmod.datetime = _FakeDateTime


# ── Synthetic stock spot path + option premium model ───────────────────────

class StockPath:
    """One stock's spot price -- simple bounded random walk with a deliberate
    directional push toward either the T1 target or the hard SL, so the mock
    day actually exercises both a winning and a losing exit, not just noise."""

    def __init__(self, start: float, hard_sl: float, day_t1: float, direction: str,
                 push: str):
        self.spot = start
        self.hard_sl = hard_sl
        self.day_t1 = day_t1
        self.direction = direction
        self.push = push  # "toward_t1" | "toward_sl" | "none"

    def step(self) -> float:
        target = self.day_t1 if self.push == "toward_t1" else (
            self.hard_sl if self.push == "toward_sl" else self.spot)
        if self.push != "none":
            self.spot += (target - self.spot) * 0.08 + random.gauss(0, self.spot * 0.0015)
        else:
            self.spot += random.gauss(0, self.spot * 0.002)
        return self.spot


def _option_ltp(spot: float, strike: int, direction: str, entry_spot: float, entry_opt: float) -> float:
    """Simple linear delta-ish proxy: option premium moves roughly proportionally
    to how far spot has moved from its entry, floored at a small residual value."""
    move = (spot - entry_spot) if direction == "CE" else (entry_spot - spot)
    delta_proxy = 0.35  # rough avg delta for a moderately OTM/ATM stock option
    return max(1.0, entry_opt + move * delta_proxy)


# ── Fake bridge: real FNO_ORDER_REQUEST -> FNO_ORDER_FILL round trip ───────

class FakeFnOBridge:
    def __init__(self, bus: EventBus):
        self._bus = bus
        self.events: list = []
        self._task: asyncio.Task = None
        self._timeout_once_for: str = ""  # symbol to deliberately never confirm (once)
        self._timed_out_done = False

    def install(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self):
        from execution_bridge.fno_bridge import FnOFillEvent
        q = self._bus.subscribe(Topic.FNO_ORDER_REQUEST)
        while True:
            ev = await q.get()
            self.events.append({
                "sim_ts": _FakeClock.sim_now.strftime("%H:%M:%S"),
                "action": ev.action, "symbol": ev.symbol, "event_id": ev.event_id,
            })
            if (ev.action == "EXIT" and ev.symbol == self._timeout_once_for
                    and not self._timed_out_done):
                self._timed_out_done = True
                print(f"  [stress] deliberately NOT replying to EXIT {ev.symbol} "
                      f"(event_id={ev.event_id}) -- probing the 15s wait-timeout path")
                continue  # never publish a fill for this one -- book._place_order will time out
            fill_price = ev.price_hint if ev.price_hint > 0 else 10.0
            await asyncio.sleep(0.05)  # realistic-fast confirm, matches the real bridge's
                                       # create_task-per-event concurrency (no serialization)
            await self._bus.publish(Topic.FNO_ORDER_FILL, FnOFillEvent(
                event_id=ev.event_id, action=ev.action, symbol=ev.symbol,
                fill_price=fill_price, qty=ev.qty,
                client_id=ev.client_id, binding_id=ev.binding_id,
                order_id=f"MOCK-{ev.event_id[:8]}",
            ))


async def run() -> int:
    random.seed(20260806)
    import strategies.fno_positional.book as _bookmod_early
    from strategies.fno_positional.book import FnOPositionalBook
    from backtest.fno_scanner.scan_live import Signal

    # CRITICAL: _save_positions()/_load_positions() write to the REAL production
    # data/fno_positions.json by default (module-level POSITIONS_PATH), merging by
    # client_id/binding_id -- a mock run would otherwise pollute real position data
    # (confirmed and cleaned up once already while building this script). Redirect
    # to an isolated scratch path for the whole run, restored at the end.
    import tempfile
    from pathlib import Path
    _real_positions_path = _bookmod_early.POSITIONS_PATH
    _scratch_path = Path(tempfile.gettempdir()) / "fno_positional_mock_dayrun_positions.json"
    _bookmod_early.POSITIONS_PATH = _scratch_path

    today = _dt_module.date.today()
    sim_start = _dt_module.datetime.combine(today, _dt_module.time(9, 15, 0), tzinfo=IST)
    sim_end = _dt_module.datetime.combine(today, _dt_module.time(15, 30, 0), tzinfo=IST)
    _FakeClock.sim_now = sim_start
    _patch_clock()

    bus = EventBus()
    book = FnOPositionalBook(bus, upstox_token="", client_id=CLIENT_ID,
                             binding_id=BINDING_ID, mode="paper", max_slots=2)

    bridge = FakeFnOBridge(bus)
    bridge.install()
    bridge._timeout_once_for = "STOCKB"  # STOCKB's exit will be deliberately unconfirmed

    # ── Synthetic watchlist: one clean winner (hits T1), one clean loser (hits
    # hard SL) so both exit paths get exercised, not just the entry mechanics. ──
    signals = [
        Signal(symbol="STOCKA", direction="CE", status="TRIGGERED",
              entry_line=1000.0, current=1000.0, dist_pct=0.0,
              hard_sl=970.0, day_t1=1060.0, zone_age=3, lock_date="03 Aug",
              rr=2.0, btst_rr=2.0, suggested_strike=1000, expiry="28 AUG 26",
              upstox_key="NSE_EQ|STOCKA", zone_lo=980.0, zone_hi=1020.0),
        Signal(symbol="STOCKB", direction="PE", status="TRIGGERED",
              entry_line=500.0, current=500.0, dist_pct=0.0,
              hard_sl=515.0, day_t1=470.0, zone_age=2, lock_date="04 Aug",
              rr=2.0, btst_rr=2.0, suggested_strike=500, expiry="28 AUG 26",
              upstox_key="NSE_EQ|STOCKB", zone_lo=490.0, zone_hi=510.0),
    ]
    stock_state = {
        "STOCKA": {"entry_spot": 1000.0, "entry_opt": 25.0, "opt_ltp": 25.0},
        "STOCKB": {"entry_spot": 500.0, "entry_opt": 15.0, "opt_ltp": 15.0},
    }
    paths = {
        "STOCKA": StockPath(1000.0, 970.0, 1060.0, "CE", push="toward_t1"),
        "STOCKB": StockPath(500.0, 515.0, 470.0, "PE", push="toward_sl"),
    }

    async def _fake_run_scan():
        book._pending = list(signals)
        book._log.info("FnOBook[mock]: loaded synthetic watchlist -- %d signals", len(signals))

    async def _fake_check_oi(_sig):
        return "OI: unavailable (mock run)"

    def _fake_subscribe_watchlist_spot(_sigs):
        pass

    async def _fake_fetch_ltp(instrument_key: str) -> float:
        for sym, st in stock_state.items():
            if instrument_key == f"NSE_EQ|{sym}":
                return paths[sym].spot
            if instrument_key.startswith("OPT|") and instrument_key.endswith(sym):
                return st["opt_ltp"]
        return 0.0

    async def _fake_to_thread_master(_token):
        return []

    book._run_scan = _fake_run_scan
    book._check_oi_buildup = _fake_check_oi
    book._subscribe_watchlist_spot = _fake_subscribe_watchlist_spot
    book._fetch_ltp = _fake_fetch_ltp
    book._instruments = []

    # resolve_option_key / resolve_lot_size hit the real (empty, mocked-out)
    # instrument master via asyncio.to_thread -- patch the module-level
    # helpers _open_position calls directly instead of faking a master list.
    import strategies.fno_positional.book as _bookmod

    def _fake_resolve_option_key(_instruments, symbol, strike, direction, expiry):
        return f"OPT|{strike}|{direction}|{symbol}", strike

    def _fake_resolve_lot_size(_instruments, _key):
        return 1  # keep qty math simple/legible in the report

    _bookmod.resolve_option_key = _fake_resolve_option_key
    _bookmod.resolve_lot_size = _fake_resolve_lot_size

    scan_done = False
    step_minutes = 2
    steps = int((sim_end - sim_start).total_seconds() // 60 // step_minutes)

    for i in range(steps + 1):
        now = sim_start + _dt_module.timedelta(minutes=step_minutes * i)
        _FakeClock.sim_now = now
        t = now.time()

        for sym, path in paths.items():
            spot = path.step()
            stock_state[sym]["opt_ltp"] = round(_option_ltp(
                spot, 0, path.direction, stock_state[sym]["entry_spot"], stock_state[sym]["entry_opt"]), 2)
            book._equity_ltp[sym] = round(spot, 2)

        if not scan_done and t >= _dt_module.time(9, 0):
            await book._run_scan()
            scan_done = True

        if _dt_module.time(9, 15) <= t <= _dt_module.time(14, 30):
            await book._try_enter_triggered()
            await book._try_enter_approaching()

        if book._open_positions and _dt_module.time(9, 15) <= t <= _dt_module.time(15, 30):
            await book._poll_and_monitor()

        await asyncio.sleep(0.01)

    # Let the deliberately-unconfirmed EXIT's real 15s wait play out.
    if bridge._timed_out_done:
        print("  [stress] waiting out the real 15s confirm timeout for the "
              "deliberately-unconfirmed EXIT...")
        await asyncio.sleep(16.0)

    await bridge.stop()

    # ── Report ───────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("FnOPositionalBook mock day-run report")
    print("=" * 70)
    print(f"Signals: {len(signals)} (STOCKA push=toward_t1, STOCKB push=toward_sl, "
          f"STOCKB's exit deliberately left unconfirmed by the fake bridge)")

    entry_events = [e for e in bridge.events if e["action"] == "ENTRY"]
    exit_events = [e for e in bridge.events if e["action"] == "EXIT"]
    print(f"\nOrder events: {len(bridge.events)} total ({len(entry_events)} ENTRY, {len(exit_events)} EXIT)")
    for e in bridge.events:
        print(f"  [{e['sim_ts']}] {e['action']:5s} {e['symbol']}")

    print("\nFinal position states:")
    ok = True
    for pos in book._positions:
        print(f"  {pos.symbol} {pos.direction}: status={pos.status} "
              f"close_reason={pos.close_reason or '-'} "
              f"entry_order_id={'yes' if pos.entry_order_id else 'NO'} "
              f"exit_order_id={'yes' if pos.exit_order_id else 'NO'} pnl={pos.pnl:.2f}")

    # STOCKB's exit was deliberately never confirmed by the bridge. Check what
    # the book actually did with that.
    stockb = next((p for p in book._positions if p.symbol == "STOCKB"), None)
    print("\n" + "-" * 70)
    if stockb is None:
        print("WARNING: STOCKB never entered -- couldn't exercise the unconfirmed-exit "
              "stress case at all this run (random-walk timing didn't trigger entry).")
    elif stockb.status == "CLOSED" and not stockb.exit_order_id:
        print("FINDING (pre-existing, not introduced by this test): _close_position() marks "
              "the position CLOSED and books P&L from the last-known LTP UNCONDITIONALLY, "
              "even when the exit order was never confirmed by the bridge (exit_order_id is "
              "empty here -- the broker never confirmed this close). If the real SELL order "
              "genuinely never reached the broker, this position could still be open for real "
              "on the exchange while the app believes it is flat and stops managing it. This "
              "mirrors the exact incident class already fixed in SellStraddle "
              "(2026-08-04, 'bridge fabricated fills when broker REJECTED an order') -- "
              "FnOPositionalBook's _close_position was not covered by that fix.")
    elif stockb.status == "OPEN":
        print("STOCKB is still OPEN at end of run (didn't hit SL/T1/expiry in this random "
              "walk) -- unconfirmed-exit case not exercised this run.")
    else:
        print(f"STOCKB closed with exit_order_id={stockb.exit_order_id!r} -- "
              f"unexpected, the fake bridge was set to never confirm this one.")

    # Restore the real path and remove the scratch file -- never leave mock state
    # pointed at (or lingering near) the real data/fno_positions.json.
    _bookmod_early.POSITIONS_PATH = _real_positions_path
    try:
        _scratch_path.unlink(missing_ok=True)
    except Exception:
        pass

    return 0 if ok else 1


def main():
    rc = asyncio.run(run())
    sys.exit(rc)


if __name__ == "__main__":
    main()
