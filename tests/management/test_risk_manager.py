"""
tests/management/test_risk_manager.py -- 2026-09-25, direct user spec: no
automated account-level risk gate. "risk depends on strategy risk management
itself."

Real incident: ssrajpal2001's NIFTY sell_straddle_calc_vwap binding
deliberately runs with day_loss_sl_pct=0 (no strategy-level loss limit
configured), yet RiskManager's own account-level DAILY_LOSS_LIMIT check
(client.risk.max_daily_loss_pct=3.0%, completely independent of any
strategy's own settings) halted the client anyway, repeating every
_RISK_CHECK_INTERVAL (1s) since _liquidate_client() never actually resolves
the underlying loss the check keeps re-firing on. _check_all_clients() no
longer auto-liquidates on breach -- the admin's own manual kill_all() is
unchanged.
"""
import asyncio

from data_layer.base_feeder import EventBus
from config.client_profiles import ClientProfile, ClientRegistry, RiskProfile
from management.risk_manager import RiskManager


def _registry_with_breaching_client(tmp_path) -> ClientRegistry:
    reg = ClientRegistry(profiles_path=str(tmp_path / "client_profiles.json"))
    profile = ClientProfile(
        client_id="ssrajpal2001",
        risk=RiskProfile(capital=500_000.0, max_daily_loss_pct=3.0),
    )
    # 9% daily loss on 500,000 capital -- well past the 3% limit.
    profile._daily_pnl = -45_000.0
    reg.register(profile)
    return reg


def test_check_all_clients_does_not_liquidate_on_daily_loss_breach(tmp_path):
    async def run():
        bus = EventBus()
        registry = _registry_with_breaching_client(tmp_path)
        rm = RiskManager(bus, registry)

        liquidate_calls = []

        async def _spy_liquidate(client_id, reason=""):
            liquidate_calls.append((client_id, reason))
        rm._liquidate_client = _spy_liquidate

        await rm._check_all_clients()

        assert liquidate_calls == [], (
            "the automated breach sweep must never call _liquidate_client -- "
            "risk is the strategy's own responsibility now"
        )
        client = registry.get("ssrajpal2001")
        assert client._halted is False
    asyncio.run(run())


def test_check_all_clients_still_syncs_state_for_the_dashboard(tmp_path):
    """The automatic liquidation trigger is removed, but risk_summary()'s
    own admin-dashboard client listing must keep working (state still
    synced from the registry, even though nothing auto-liquidates)."""
    async def run():
        bus = EventBus()
        registry = _registry_with_breaching_client(tmp_path)
        rm = RiskManager(bus, registry)

        await rm._check_all_clients()

        summary = rm.risk_summary()
        assert any(c["client_id"] == "ssrajpal2001" for c in summary["clients"])
    asyncio.run(run())


def test_kill_all_still_manually_liquidates(tmp_path):
    """The admin's own explicit, manual kill_all() action is unchanged --
    only the automated per-tick sweep was disabled."""
    async def run():
        bus = EventBus()
        registry = _registry_with_breaching_client(tmp_path)
        rm = RiskManager(bus, registry)

        liquidate_calls = []

        async def _spy_liquidate(client_id, reason=""):
            liquidate_calls.append((client_id, reason))
        rm._liquidate_client = _spy_liquidate

        await rm.kill_all()

        assert ("ssrajpal2001", "FIRM_KILL_ALL") in liquidate_calls, (
            "manual kill_all() must still liquidate every active client"
        )
    asyncio.run(run())
