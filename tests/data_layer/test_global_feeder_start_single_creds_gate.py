"""
tests/data_layer/test_global_feeder_start_single_creds_gate.py -- regression
for the 2026-09-06 GlobalFeeder.start_single() creds-gating fix.

Real bug found while wiring AngelOne into the live DualFeeder pairing:
`await dual.start_providers({provider: creds} if creds and
creds.get("access_token") else {})` gated purely on an "access_token" key
existing in the creds dict. Upstox/Fyers legitimately have one (obtained
via a separate OAuth/headless step before start_single is ever called),
but AngelOne authenticates internally inside its own connect()
(client_code+password+TOTP -> jwtToken+feedToken) and never has an
"access_token" key at all -- so calling start_single("angelone", {...})
would ALWAYS silently pass an empty creds_map to start_providers, never
actually starting the AngelOne feed, with no error raised anywhere.

Fixed via `_has_usable_creds` accepting EITHER credential shape. These
tests drive the real GlobalFeeder.start_single() method (not a
reimplementation) with DualFeeder.start_providers/stop mocked out, so a
future regression on this exact gate is caught the same way it would
actually manifest live: `start_providers` receiving an empty dict.
"""
import pytest

from config.global_config import GlobalConfig
from data_layer.base_feeder import EventBus
from data_layer.global_feeder import GlobalFeeder, DualFeeder


class _CapturingBus(EventBus):
    def __init__(self) -> None:
        super().__init__()
        self.published: list = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


def _make_feeder() -> GlobalFeeder:
    cfg = GlobalConfig()
    return GlobalFeeder(_CapturingBus(), cfg)


@pytest.fixture
def capture_start_providers(monkeypatch):
    """Patches DualFeeder.start_providers to record what creds_map it was
    called with, instead of actually connecting anything."""
    calls = []

    async def _fake_start_providers(self, creds_map):
        calls.append(creds_map)

    monkeypatch.setattr(DualFeeder, "start_providers", _fake_start_providers)
    return calls


@pytest.mark.asyncio
async def test_angelone_client_password_creds_are_treated_as_usable(capture_start_providers):
    """The exact bug: AngelOne's own credential shape (client_id + password,
    no access_token) must NOT be silently dropped to an empty creds_map."""
    feeder = _make_feeder()
    creds = {"client_id": "AB1234", "api_key": "key", "password": "pw", "totp_secret": "secret"}

    await feeder.start_single("angelone", creds)

    assert capture_start_providers == [{"angelone": creds}]


@pytest.mark.asyncio
async def test_upstox_access_token_creds_still_work(capture_start_providers):
    """The pre-existing shape (Upstox/Fyers, access_token from a prior OAuth/
    headless step) must keep working unchanged."""
    feeder = _make_feeder()
    creds = {"access_token": "tok123", "api_key": "key"}

    await feeder.start_single("upstox", creds)

    assert capture_start_providers == [{"upstox": creds}]


@pytest.mark.asyncio
async def test_genuinely_empty_or_incomplete_creds_still_gate_to_empty_map(capture_start_providers):
    """Neither shape present (e.g. AngelOne creds only half-saved, missing
    password) -- must still degrade safely to an empty creds_map, not crash
    or falsely treat it as usable."""
    feeder = _make_feeder()

    await feeder.start_single("angelone", {"client_id": "AB1234"})   # no password
    await feeder.start_single("upstox", {})                          # nothing at all
    await feeder.start_single("upstox", None)                        # None creds

    assert capture_start_providers == [{}, {}, {}]
