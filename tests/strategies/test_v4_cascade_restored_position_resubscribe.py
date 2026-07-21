"""strategies/v4_cascade/book.py's _resubscribe_execution_contract_for_open_position
-- 2026-07-21 critical bugfix: execution-contract live-price tracking
(_exec_symbol/_exec_live_price) is pure in-memory state, only ever
established inside _open_entry_async when a trade freshly fires. A RESTORED
position (after any restart while a trade was open) never goes through
that path, so it permanently loses its execution-contract subscription --
LTP/P&L silently falls back to the tracking contract's price for the rest
of that trade's life. Confirmed live: a real 24200 CE @ ~21 showed an LTP
of ~181, actually the CE tracking contract's price, after a routine
restart. This must re-establish the same subscription at boot."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from strategies.v4_cascade.book import V4CascadeBook
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


class _FakeFeeder:
    def __init__(self) -> None:
        self.subscribed = []

    async def subscribe_tokens(self, tokens) -> None:
        self.subscribed.extend(tokens)


class _FakeRebalancer:
    def __init__(self, feeder) -> None:
        self._feeder = feeder


def _book():
    cfg = GlobalConfig()
    book = V4CascadeBook(
        EventBus(), cfg, underlying="NIFTY", client_id="C1", binding_id="B1",
        lot_multiplier=1, squareoff_time="15:15",
    )
    book._running = True
    book._expiry = date(2026, 7, 21)
    feeder = _FakeFeeder()
    book._rebalancer = _FakeRebalancer(feeder)
    return book, feeder


def _open_position(side="CE", strike=24200.0):
    t1 = TrancheLeg(tranche="T1", option_type=side, strike=strike, qty=65,
                     entry_price=21.20, status="open")
    t2 = TrancheLeg(tranche="T2", option_type=side, strike=strike, qty=65,
                     entry_price=21.20, status="open")
    return CascadePosition(
        underlying="NIFTY", side=side, tracking_strike=24000.0, execution_strike=strike,
        atm_at_trigger=24216.05, entry_spot=24216.05, t1=t1, t2=t2, status="open",
        open_time=datetime(2026, 7, 21, 12, 15, tzinfo=IST),
    )


@pytest.mark.asyncio
async def test_resubscribes_execution_contract_for_a_restored_open_position(monkeypatch):
    book, feeder = _book()
    book._engine.position = _open_position()

    monkeypatch.setattr(
        "strategies.v4_cascade.book.REGISTRY.get_upstox_key",
        lambda und, exp, strike, opt_type: f"NSE_FO|{strike}{opt_type}",
    )

    await book._resubscribe_execution_contract_for_open_position()

    assert book._exec_symbol["CE"] == "NSE_FO|24200CE"
    assert feeder.subscribed == ["NSE_FO|24200CE"]


@pytest.mark.asyncio
async def test_noop_when_no_position_open(monkeypatch):
    book, feeder = _book()
    book._engine.position = None

    monkeypatch.setattr(
        "strategies.v4_cascade.book.REGISTRY.get_upstox_key",
        lambda und, exp, strike, opt_type: f"NSE_FO|{strike}{opt_type}",
    )

    await book._resubscribe_execution_contract_for_open_position()

    assert feeder.subscribed == []
    assert book._exec_symbol["CE"] == ""


@pytest.mark.asyncio
async def test_crypto_skips_entirely(monkeypatch):
    book, feeder = _book()
    book._is_crypto = True
    book._engine.position = _open_position()

    called = []
    monkeypatch.setattr(
        "strategies.v4_cascade.book.REGISTRY.get_upstox_key",
        lambda *a, **kw: called.append(a) or "SHOULD_NOT_BE_USED",
    )

    await book._resubscribe_execution_contract_for_open_position()

    assert feeder.subscribed == []
    assert called == []


@pytest.mark.asyncio
async def test_symbol_resolution_failure_is_handled_gracefully(monkeypatch):
    book, feeder = _book()
    book._engine.position = _open_position()

    monkeypatch.setattr(
        "strategies.v4_cascade.book.REGISTRY.get_upstox_key",
        lambda und, exp, strike, opt_type: "",
    )

    await book._resubscribe_execution_contract_for_open_position()  # must not raise

    assert feeder.subscribed == []
