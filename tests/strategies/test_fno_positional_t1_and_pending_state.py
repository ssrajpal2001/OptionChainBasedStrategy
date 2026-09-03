"""
Regression tests for two 2026-08-05 fixes to FnOPositionalBook:

1. T1-hit was (mis)recorded in `close_reason`, a field meant to describe why a
   position actually CLOSED -- but T1 is alert-only (never auto-closes), so an
   OPEN position showing close_reason="t1_hit" looked exactly like a booked
   close with a negative `pnl` (which is really just the live unrealized
   value, not anything booked). Now tracked via a dedicated `t1_alerted`
   bool field; close_reason stays empty until the position is genuinely
   closed.
2. get_state() only ever returned `pending_count` (a number), never the
   actual pending signal details -- the dashboard had nothing to render for
   the other N-1 watchlist stocks beyond whatever actually got entered.
   get_state() now also returns a `pending` list with each signal's
   symbol/direction/status/entry_line/hard_sl/day_t1/dist_pct/btst_rr/strike/
   expiry/blocked.
"""
import asyncio

from backtest.fno_scanner.scan_live import Signal
from strategies.fno_positional.book import FnOPositionalBook, FnOPosition


def _book() -> FnOPositionalBook:
    return FnOPositionalBook(bus=None, upstox_token="", client_id="c1", binding_id="b1", mode="paper")


def test_t1_hit_sets_t1_alerted_not_close_reason():
    book = _book()
    pos = FnOPosition(
        slot_id="s1", symbol="POLYCAB", direction="PE",
        spot_instrument_key="k1", option_instrument_key="k2", broker_symbol="X",
        strike=9300, expiry_str="25 AUG 26", lot_size=125, qty=125,
        spot_entry=9162.0, spot_sl=9242.86, day_t1=9001.0,
        entry_ltp=200.0, current_ltp=180.0, status="OPEN",
    )
    book._positions = [pos]

    async def _fake_fetch_ltp(key):
        return 9001.0 if key == "k1" else 180.0
    book._fetch_ltp = _fake_fetch_ltp

    class _FakeBus:
        async def publish(self, topic, ev):
            pass
    book._bus = _FakeBus()

    asyncio.run(book._poll_and_monitor())

    assert pos.t1_alerted is True
    assert pos.close_reason == "", "close_reason must stay empty -- T1 is alert-only, never a close"
    assert pos.status == "OPEN", "T1 must never auto-close the position"


def test_t1_alert_only_fires_once():
    book = _book()
    pos = FnOPosition(
        slot_id="s1", symbol="POLYCAB", direction="PE",
        spot_instrument_key="k1", option_instrument_key="k2", broker_symbol="X",
        strike=9300, expiry_str="25 AUG 26", lot_size=125, qty=125,
        spot_entry=9162.0, spot_sl=9242.86, day_t1=9001.0,
        entry_ltp=200.0, current_ltp=180.0, status="OPEN", t1_alerted=True,
    )
    book._positions = [pos]
    published = []

    async def _fake_fetch_ltp(key):
        return 9001.0 if key == "k1" else 180.0
    book._fetch_ltp = _fake_fetch_ltp

    class _FakeBus:
        async def publish(self, topic, ev):
            published.append(ev)
    book._bus = _FakeBus()

    asyncio.run(book._poll_and_monitor())

    assert published == [], "must not re-publish the T1 alert once already alerted"


def test_get_state_includes_pending_signal_details():
    book = _book()
    book._pending = [
        Signal(
            symbol="DELHIVERY", direction="CE", status="APPROACHING",
            entry_line=465.2, current=470.0, dist_pct=1.03, hard_sl=459.3,
            day_t1=487.9, zone_age=3, lock_date="31 Jul", rr=3.89, btst_rr=1.68,
            suggested_strike=460, expiry="25 AUG 26", upstox_key="NSE_EQ|X",
        ),
    ]
    state = book.get_state()

    assert state["pending_count"] == 1
    assert len(state["pending"]) == 1
    p = state["pending"][0]
    assert p["symbol"] == "DELHIVERY"
    assert p["direction"] == "CE"
    assert p["status"] == "APPROACHING"
    assert p["entry_line"] == 465.2
    assert p["hard_sl"] == 459.3
    assert p["day_t1"] == 487.9
    assert p["btst_rr"] == 1.68
    assert p["blocked"] is False


def test_get_state_marks_blocked_pending_signals():
    book = _book()
    book._pending = [
        Signal(
            symbol="POLYCAB", direction="PE", status="APPROACHING",
            entry_line=9162.0, current=9266.0, dist_pct=0.61, hard_sl=9242.86,
            day_t1=8924.0, zone_age=3, lock_date="30 Jul", rr=2.94, btst_rr=1.34,
            suggested_strike=9300, expiry="25 AUG 26", upstox_key="NSE_EQ|X",
        ),
    ]
    book._blocked_today.add(("POLYCAB", "PE"))
    state = book.get_state()
    assert state["pending"][0]["blocked"] is True
