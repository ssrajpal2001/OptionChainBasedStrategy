"""Regression test for the 2026-08-20 live incident: SellStraddleStrategy's `_on_fill`
used to set `pos.ce_leg.ltp`/`pos.pe_leg.ltp` to the broker's real fill price on ENTRY
confirmation, alongside `entry_price`. `.ltp` is supposed to be a purely LIVE,
continuously-updated field -- initialized from the live feed at optimistic-open and
refreshed on every subsequent OPTION_TICK, completely independent of the fill.

Real incident: a live Zerodha CE fill (45.90) landed noticeably below the strategy's
own live LTP estimate (55.40) at entry (ordinary execution slippage). Because `.ltp`
was briefly overwritten with the fill price, `pos.current_value` (ce_leg.ltp +
pe_leg.ltp) read ~114 for one tick instead of the genuinely-traded ~122 -- and the
Day-Low Reversal Exit tracker, which samples `pos.current_value` on every tick to find
the pair's own running minimum, latched onto that one-tick artifact as "the day's low"
before the very next live OPTION_TICK corrected `.ltp` back. The 15:00 frozen low ended
up ~8pts below anything the live market ever actually traded at, for that one client
only (a paper/simulated fill always matches the strategy's own LTP exactly by
construction, so this never surfaced there).

Fix: `_on_fill`'s ENTRY branch (engine.py) now only sets `entry_price` from the fill,
never `.ltp`.
"""
from __future__ import annotations

from datetime import date, datetime

from config.global_config import GlobalConfig, IST
from data_layer.base_feeder import EventBus
from execution_bridge.straddle_bridge import StraddleFillEvent
from strategies.sell_straddle import SellStraddleStrategy
from strategies.sell_straddle.dataclasses import StraddleLeg, StraddlePosition


def _make_optimistic_position(ss: SellStraddleStrategy) -> StraddlePosition:
    """Mirrors entries.py's optimistic open: entry_price and ltp both start at the
    strategy's own live LTP estimate (55.40 / 67.10), matching the real incident."""
    pos = StraddlePosition(
        underlying=ss._underlying, atm_at_entry=24250.0, entry_spot=24237.0,
        ce_leg=StraddleLeg("CE", 24400.0, 55.40, 55.40, open_time=datetime.now(IST)),
        pe_leg=StraddleLeg("PE", 24250.0, 67.10, 67.10, open_time=datetime.now(IST)),
        net_credit=122.50, open_time=datetime.now(IST), status="open",
        lot_size=ss._lot_size * ss._lot_multiplier, expiry_date=date.today(),
    )
    ss._position = pos
    return pos


def test_entry_fill_confirmation_does_not_overwrite_live_ltp():
    ss = SellStraddleStrategy(EventBus(), GlobalConfig(), underlying="NIFTY")
    pos = _make_optimistic_position(ss)
    # Sanity: current_value reflects the genuinely-live, strategy-estimated price before
    # the fill confirmation lands.
    assert pos.current_value == 55.40 + 67.10

    # Real broker fill lands below the strategy's own LTP estimate (ordinary slippage,
    # exactly like the real Zerodha incident).
    fill = StraddleFillEvent(
        action="ENTRY", underlying="NIFTY", atm=24250.0,
        ce_strike=24400.0, pe_strike=24250.0,
        ce_fill=45.90, pe_fill=68.08,
        client_id="gurmeet", binding_id="zerodha", event_id="NIFTY_ENTRY_1",
        paper_mode=False, legs=["CE", "PE"],
    )
    ss._on_fill(fill)

    # entry_price (and net_credit, used for real P&L) must reflect the real fill.
    assert ss._position.ce_leg.entry_price == 45.90
    assert ss._position.pe_leg.entry_price == 68.08
    assert ss._position.net_credit == 45.90 + 68.08

    # But `.ltp` — the field the Day-Low tracker and every other live running-min/max
    # reads via pos.current_value — must be UNCHANGED by the fill, still reflecting the
    # genuinely-live market price, not the one-off fill/slippage number.
    assert ss._position.ce_leg.ltp == 55.40, (
        "ce_leg.ltp must not be overwritten by the fill price — it must stay whatever "
        "the live feed last reported, so pos.current_value never briefly reads a value "
        "the live market didn't actually trade at"
    )
    assert ss._position.pe_leg.ltp == 67.10
    assert pos.current_value == 55.40 + 67.10, (
        "current_value (used by the Day-Low tracker's running-minimum) must stay on the "
        "live series and must NOT dip to the fill-price sum (113.98) for even one tick"
    )
