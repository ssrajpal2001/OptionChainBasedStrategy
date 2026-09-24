"""
Regression tests for the 2026-09-23 critical live-incident fix to
strategies/sell_straddle/r1_breach_reentry.py.

Real incident: a rolled-in leg's R1-breach close never set
pos.{ce,pe}_leg_closed, so the very next call to
_check_r1_breach_and_reentry() saw the leg as still "open", re-armed a fresh
watch against the same (never-updated) leg data, found the same breach true
again, and closed it again -- forever, each time booking another real leg_pnl
into session_realized_pnl_pts and (live) sending another real broker exit
order. Separately, _tick_loop and _eod_backstop_loop can both reach
_check_exits() -> _check_r1_breach_and_reentry() concurrently (same
documented race already fixed once for _post1500_closing), which compounded
the runaway into bursts of duplicate closes within the same second.

These tests drive the real R1BreachReentryMixin methods (via SellStraddleStrategy)
with a fake SupportResistanceCalculator so R1-breach state is controlled
directly, rather than replaying real candles through the actual calculator
(that state machine is already covered by test_post1500_r1_exit.py's
_establish_r1 helper / support_resistance.py's own tests). Follows this
file's own established async-test convention (asyncio.run(...) inside a
plain `def test_...`), matching test_post1500_r1_exit.py.
"""
import asyncio
from datetime import datetime, time as dtime
from unittest.mock import AsyncMock, patch

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from strategies.sell_straddle import SellStraddleStrategy, StraddlePosition, StraddleLeg


class _FakeOrderEvent:
    def __init__(self, close_aborted=False):
        self.close_aborted = close_aborted


class _FakeCalc:
    """Stands in for SupportResistanceCalculator -- controls breach state directly."""

    def __init__(self, r1_established: bool = False, phase: str = "R1_TRACKING"):
        self.r1_established = r1_established
        self.phase = phase

    def process_straddle_candle(self, *a, **k):
        pass

    def get_calculated_sr_state(self, key):
        return {
            "sr_levels": {
                "R1": {"is_established": self.r1_established, "high": 100.0},
                "S1": {"is_established": False, "low": 50.0},
            },
            "current_phase": self.phase,
            "r1_established": self.r1_established,
            "s1_established": False,
        }


def _strategy():
    s = SellStraddleStrategy(EventBus(), cfg=GlobalConfig(), underlying="NIFTY")
    s._lot_size = 75
    s._lot_multiplier = 1
    s._spot = 24000.0
    s._force_exit = dtime(23, 59)
    return s


def _position(ce_entry=100.0, ce_ltp=100.0, pe_entry=100.0, pe_ltp=120.0) -> StraddlePosition:
    return StraddlePosition(
        underlying="NIFTY", atm_at_entry=24000, entry_spot=24000,
        ce_leg=StraddleLeg("CE", 24000, ce_entry, ce_ltp, open_reason="single_side_roll_vwap_rise_roll"),
        pe_leg=StraddleLeg("PE", 24000, pe_entry, pe_ltp, open_reason="single_side_roll_vwap_rise_roll"),
        net_credit=ce_entry + pe_entry,
        status="open",
    )


def _spy_close_leg(s, close_aborted=False, yield_before_finalize=False):
    """Spy mirroring the real _close_leg contract: booked P&L / close_time
    only finalize on a non-aborted close. `yield_before_finalize` inserts a
    real await point so two concurrent callers can genuinely interleave
    (proving the _r1_closing guard, not just accidental ordering)."""
    calls = []

    async def _fake(side, reason, now):
        calls.append((side, reason))
        if yield_before_finalize:
            await asyncio.sleep(0.01)
        leg = s._position.ce_leg if side == "CE" else s._position.pe_leg
        if not close_aborted:
            leg.close_time = now
        return _FakeOrderEvent(close_aborted=close_aborted)

    s._close_leg = _fake
    return calls


def test_confirmed_r1_breach_close_sets_leg_closed_flag():
    """Core fix: after a confirmed close, pos.pe_leg_closed must be True."""
    s = _strategy()
    s._position = _position()
    s._seed_r1s1_calc = AsyncMock(return_value=_FakeCalc(r1_established=False))
    calls = _spy_close_leg(s)
    s._persist = lambda: None

    # PE leg in loss (entry=100, ltp=120) -> arms watch -> breach immediately
    # true (r1_established=False) -> closes in the SAME call.
    now = datetime(2026, 9, 23, 12, 27, 35)
    asyncio.run(s._check_r1_breach_and_reentry(now))

    assert len(calls) == 1
    assert s._position.pe_leg_closed is True


def test_second_call_does_not_re_close_already_closed_leg():
    """THE bug: before the fix, a second call (simulating the very next real
    tick) would see pe_leg_closed still False, re-arm, and close again."""
    s = _strategy()
    s._position = _position()
    s._seed_r1s1_calc = AsyncMock(return_value=_FakeCalc(r1_established=False))
    calls = _spy_close_leg(s)
    s._persist = lambda: None

    now = datetime(2026, 9, 23, 12, 27, 35)
    asyncio.run(s._check_r1_breach_and_reentry(now))
    assert len(calls) == 1

    # A later tick with the SAME stale leg data (nothing replaced it yet --
    # Part 2 hasn't found an S1-breach re-entry candidate).
    asyncio.run(s._check_r1_breach_and_reentry(now))
    assert len(calls) == 1, "leg must not be re-armed/re-closed once already closed"


def test_concurrent_calls_only_close_once():
    """_tick_loop and _eod_backstop_loop can both reach this method for the
    same real moment -- the _r1_closing guard must serialize them."""
    s = _strategy()
    s._position = _position()
    s._seed_r1s1_calc = AsyncMock(return_value=_FakeCalc(r1_established=False))
    calls = _spy_close_leg(s, yield_before_finalize=True)
    s._persist = lambda: None

    now = datetime(2026, 9, 23, 12, 27, 35)

    async def _race():
        await asyncio.gather(
            s._check_r1_breach_and_reentry(now),
            s._check_r1_breach_and_reentry(now),
        )
    asyncio.run(_race())

    assert len(calls) == 1, "concurrent breach evaluations must not both close the same leg"
    assert s._position.pe_leg_closed is True


def test_aborted_close_clears_guard_and_allows_retry():
    s = _strategy()
    s._position = _position()
    s._seed_r1s1_calc = AsyncMock(return_value=_FakeCalc(r1_established=False))
    calls = _spy_close_leg(s, close_aborted=True)
    s._persist = lambda: None

    now = datetime(2026, 9, 23, 12, 27, 35)
    asyncio.run(s._check_r1_breach_and_reentry(now))
    assert len(calls) == 1
    assert s._position.pe_leg_closed is False  # never confirmed -- leg stays open
    assert s._r1_closing["PE"] is False  # guard cleared so a later tick can retry

    asyncio.run(s._check_r1_breach_and_reentry(now))
    assert len(calls) == 2


def test_open_leg_clears_leg_closed_and_r1_closing_guard():
    """A fresh _open_leg() (e.g. the S1-breach re-entry itself) must clear
    both flags so this side's P&L resumes counting and a FUTURE roll-in on
    this side isn't permanently blocked by a stale guard."""
    s = _strategy()
    s._position = _position()
    s._position.pe_leg_closed = True
    s._r1_watch = {}
    s._r1_pending = None
    s._r1_closing = {"CE": False, "PE": True}
    s._seed_exec_legs = AsyncMock()
    s._emit_order = AsyncMock()

    asyncio.run(s._open_leg(
        "PE", 23450, 106.0, datetime(2026, 9, 23, 12, 30, 0), "s1_breach_reentry_post_roll",
    ))

    assert s._position.pe_leg_closed is False
    assert s._r1_closing["PE"] is False


def test_open_leg_does_not_reset_position_open_time():
    """2026-09-23 CRITICAL FIX, real live incident: _open_leg() used to also
    reset pos.open_time to `now` on every single-leg roll. Dashboard's
    Booked-P&L filter (dashboard_server.py) treats pos.open_time as the
    "cycle start" and only counts trade_history rows with ts >= that value --
    resetting it to the roll's own timestamp moved the boundary PAST the
    close record that same roll had just written moments earlier, silently
    excluding the just-booked P&L from the Booked P&L display every roll.
    Confirmed live: a real rollover completed (leg closed+reopened, R1-watch
    armed on the new leg) yet Booked P&L showed +Rs0. Only leg.open_time
    (per-leg display/tracking) should update -- the position's own
    open_time must be set once at genuine entry and never touched by a roll."""
    s = _strategy()
    s._position = _position()
    _original_open_time = datetime(2026, 9, 23, 13, 0, 18)
    s._position.open_time = _original_open_time
    s._seed_exec_legs = AsyncMock()
    s._emit_order = AsyncMock()

    asyncio.run(s._open_leg(
        "PE", 23300, 55.80, datetime(2026, 9, 23, 12, 59, 30), "single_side_roll_vwap_rise_roll",
    ))

    assert s._position.open_time == _original_open_time, (
        "a single-leg roll must not reset the whole position's open_time -- "
        "that breaks the dashboard's Booked-P&L cycle-start filter"
    )
    # The LEG's own open_time still updates -- that's correct and used for
    # per-leg display/tracking, unrelated to the position-level bug above.
    assert s._position.pe_leg.open_time == datetime(2026, 9, 23, 12, 59, 30)


def test_r1_bucket_start_aligns_to_5min_market_anchored_boundaries():
    """2026-09-23, direct user spec: R1/S1 must run on 5-min bars, anchored
    to market open (09:15), not clock/midnight-aligned -- e.g. 09:19:59
    still belongs to the [09:15,09:20) bucket, not [09:15,09:20) rounded to
    a clock-aligned [09:15,09:20) that would coincidentally match here, but
    09:12:00 (before the first real bucket even opens) must NOT round up to
    09:15 -- it belongs to the bucket starting at 09:10 (anchor - 5), same
    market-anchored arithmetic to_n_min_bars_market_anchored itself uses."""
    s = _strategy()
    cases = [
        (datetime(2026, 9, 23, 9, 15, 0), datetime(2026, 9, 23, 9, 15, 0)),
        (datetime(2026, 9, 23, 9, 17, 30), datetime(2026, 9, 23, 9, 15, 0)),
        (datetime(2026, 9, 23, 9, 19, 59), datetime(2026, 9, 23, 9, 15, 0)),
        (datetime(2026, 9, 23, 9, 20, 0), datetime(2026, 9, 23, 9, 20, 0)),
        (datetime(2026, 9, 23, 10, 3, 22), datetime(2026, 9, 23, 10, 0, 0)),
        (datetime(2026, 9, 23, 13, 12, 0), datetime(2026, 9, 23, 13, 10, 0)),
    ]
    for now, expected in cases:
        assert s._r1_bucket_start(now) == expected, f"now={now}"


def _mk_1m_row(ts: datetime, high: float, low: float) -> dict:
    mid = (high + low) / 2
    return {"ts": ts.isoformat(), "open": mid, "high": high, "low": low, "close": mid}


def test_seed_excludes_the_still_forming_bucket():
    """2026-09-24 CRITICAL FIX, real live incident (Gurmeet's NIFTY book): if
    the REST fetch lands mid-bucket (arming at 09:43:45, inside the
    [09:40,09:45) window), the seed must NOT feed that partial bucket as a
    genuine candle -- only fully-closed ones. Real bars: 09:15-09:35 are five
    complete 5-min buckets; 09:40-09:43 is only 4 of 5 real minutes (still
    forming as of "now"=09:43:45). The seeded calculator's last_candle must
    stop at 09:35, never reach 09:40."""
    s = _strategy()
    s._position = _position()
    s._is_crypto = False

    rows = []
    for i, (h, l) in enumerate([(223.85, 175.50), (186.55, 173.40), (184.95, 175.50),
                                 (189.00, 179.10), (186.90, 174.30)]):
        bstart = datetime(2026, 9, 24, 9, 15, tzinfo=IST) + __import__("datetime").timedelta(minutes=5 * i)
        for m in range(5):
            rows.append(_mk_1m_row(bstart + __import__("datetime").timedelta(minutes=m), h, l))
    # Still-forming 09:40 bucket: only 4 of 5 real minutes by "now".
    for m in range(4):
        rows.append(_mk_1m_row(
            datetime(2026, 9, 24, 9, 40, tzinfo=IST) + __import__("datetime").timedelta(minutes=m),
            176.35, 166.10,
        ))

    async def _fake_fetch(ikey, token):
        return rows

    with patch("data_layer.historical_candles.fetch_upstox_intraday_1m", _fake_fetch), \
         patch("data_layer.client_db.ClientDB.get_feeder_creds_sync",
               lambda self, provider: {"access_token": "dummy"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry",
               lambda *a, **k: datetime(2026, 9, 29).date()), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
               lambda *a, **k: "NSE_FO|TEST"), \
         patch("strategies.sell_straddle.r1_breach_reentry.datetime") as _dt_mock:
        _dt_mock.now.return_value = datetime(2026, 9, 24, 9, 43, 45, tzinfo=IST)
        _dt_mock.fromisoformat = datetime.fromisoformat
        calc = asyncio.run(s._seed_r1s1_calc(23150, "CE", "TEST_KEY"))

    st = calc.get_calculated_sr_state("TEST_KEY")
    assert st["last_candle"]["timestamp"] == datetime(2026, 9, 24, 9, 35, tzinfo=IST), (
        "seed must stop at the last FULLY CLOSED bucket (09:35) -- the still-forming "
        "09:40 bucket must never be fed as a genuine candle"
    )


def test_seed_then_live_feed_does_not_silently_drop_the_next_bucket():
    """The actual bug: seeding a still-forming bucket sets the calculator's
    last_candle.timestamp to that SAME bucket the live feed later finalizes,
    and SupportResistanceCalculator.process_straddle_candle's own duplicate-
    candle guard (ts == last_candle.timestamp and duration <= last duration)
    silently drops the live feed's candle for it -- real price data lost,
    every subsequent R1/S1 transition pushed one bucket late. After the fix,
    seed stops at 09:35, so the live feed's first finalized bucket (09:40) is
    never a duplicate and genuinely advances last_candle."""
    s = _strategy()
    s._position = _position()
    s._is_crypto = False

    rows = []
    for i, (h, l) in enumerate([(223.85, 175.50), (186.55, 173.40), (184.95, 175.50),
                                 (189.00, 179.10), (186.90, 174.30)]):
        bstart = datetime(2026, 9, 24, 9, 15, tzinfo=IST) + __import__("datetime").timedelta(minutes=5 * i)
        for m in range(5):
            rows.append(_mk_1m_row(bstart + __import__("datetime").timedelta(minutes=m), h, l))
    for m in range(4):
        rows.append(_mk_1m_row(
            datetime(2026, 9, 24, 9, 40, tzinfo=IST) + __import__("datetime").timedelta(minutes=m),
            176.35, 166.10,
        ))

    async def _fake_fetch(ikey, token):
        return rows

    with patch("data_layer.historical_candles.fetch_upstox_intraday_1m", _fake_fetch), \
         patch("data_layer.client_db.ClientDB.get_feeder_creds_sync",
               lambda self, provider: {"access_token": "dummy"}), \
         patch("data_layer.instrument_registry.REGISTRY.get_active_expiry",
               lambda *a, **k: datetime(2026, 9, 29).date()), \
         patch("data_layer.instrument_registry.REGISTRY.get_broker_symbol",
               lambda *a, **k: "NSE_FO|TEST"), \
         patch("strategies.sell_straddle.r1_breach_reentry.datetime") as _dt_mock:
        _dt_mock.now.return_value = datetime(2026, 9, 24, 9, 43, 45, tzinfo=IST)
        _dt_mock.fromisoformat = datetime.fromisoformat
        calc = asyncio.run(s._seed_r1s1_calc(23150, "CE", "TEST_KEY"))

    entry = {"calc": calc, "inst_key": "TEST_KEY", "bar_acc": None, "strike": 23150}
    # Live ticks spanning the rest of the (real) 09:40-09:45 bucket, then the
    # tick that rolls into 09:45 finalizes it -- this must genuinely reach
    # process_straddle_candle, not get silently dropped.
    s._r1_feed_bar(entry, 167.00, datetime(2026, 9, 24, 9, 43, 45, tzinfo=IST))
    s._r1_feed_bar(entry, 176.35, datetime(2026, 9, 24, 9, 44, 30, tzinfo=IST))
    s._r1_feed_bar(entry, 166.10, datetime(2026, 9, 24, 9, 44, 55, tzinfo=IST))
    pre_advance_ts = calc.get_calculated_sr_state("TEST_KEY")["last_candle"]["timestamp"]
    assert pre_advance_ts == datetime(2026, 9, 24, 9, 35, tzinfo=IST)

    s._r1_feed_bar(entry, 170.00, datetime(2026, 9, 24, 9, 45, 5, tzinfo=IST))
    post_advance_ts = calc.get_calculated_sr_state("TEST_KEY")["last_candle"]["timestamp"]
    assert post_advance_ts == datetime(2026, 9, 24, 9, 40, tzinfo=IST), (
        "the live feed's 09:40 candle must genuinely advance last_candle -- "
        "if this is still 09:35, the candle was silently dropped (the bug)"
    )


def test_r1_feed_bar_only_processes_candle_on_5min_boundary_change():
    """Ticks within the same 5-min bucket must accumulate (high/low widen,
    no process_straddle_candle call); a tick in the NEXT bucket must flush
    the completed bucket as one 5-min candle (duration=5) before starting a
    new accumulator."""
    calls = []

    class _SpyCalc:
        def process_straddle_candle(self, inst_key, candle):
            calls.append((inst_key, dict(candle)))

    s = _strategy()
    entry = {"calc": _SpyCalc(), "inst_key": "NIFTY_PE_23300_TEST", "bar_acc": None}

    s._r1_feed_bar(entry, 100.0, datetime(2026, 9, 23, 10, 1, 0))
    s._r1_feed_bar(entry, 105.0, datetime(2026, 9, 23, 10, 2, 30))   # same bucket [10:00,10:05)
    s._r1_feed_bar(entry, 95.0, datetime(2026, 9, 23, 10, 4, 59))    # same bucket, new low
    assert calls == [], "must not flush mid-bucket"
    assert entry["bar_acc"] == {"minute": datetime(2026, 9, 23, 10, 0, 0), "h": 105.0, "l": 95.0}

    s._r1_feed_bar(entry, 110.0, datetime(2026, 9, 23, 10, 5, 1))    # next bucket [10:05,10:10)
    assert len(calls) == 1
    inst_key, candle = calls[0]
    assert inst_key == "NIFTY_PE_23300_TEST"
    assert candle == {
        "timestamp": datetime(2026, 9, 23, 10, 0, 0), "high": 105.0, "low": 95.0, "duration": 5,
    }
    # New bucket's accumulator started fresh with the flushing tick.
    assert entry["bar_acc"] == {"minute": datetime(2026, 9, 23, 10, 5, 0), "h": 110.0, "l": 110.0}
