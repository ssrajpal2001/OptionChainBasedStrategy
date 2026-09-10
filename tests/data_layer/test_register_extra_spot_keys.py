"""
tests/data_layer/test_register_extra_spot_keys.py -- 2026-09-10, real
incident: UpstoxFeeder.register_extra_spot_keys() used to permanently mark
a key as "subscribed" the instant it was registered, regardless of whether
self._streamer existed yet or the subscribe call actually succeeded. A key
registered in the brief window right after a restart/reconnect (before the
streamer was fully up) never got a real WS subscribe sent, and -- since
the only retry path (_reapply_extra_spot_keys, fired on every subsequent
feeder reconnect) calls this SAME method with the SAME dedup check -- it
was never retried for the rest of the session. Real trade: TECHM's live
spot ticks never arrived after a restart; VWAP had a correct value (from
the historical REST seed) but self._live_spot_ltp stayed None all day.
"""
import sys

sys.path.insert(0, ".")

from data_layer.global_feeder import UpstoxFeeder


class _FakeBus:
    pass


class _FakeStreamer:
    def __init__(self, raise_on_subscribe: bool = False) -> None:
        self.raise_on_subscribe = raise_on_subscribe
        self.subscribed_calls = []

    def subscribe(self, keys, mode="full"):
        if self.raise_on_subscribe:
            raise RuntimeError("simulated WS error")
        self.subscribed_calls.append(list(keys))


def test_register_extra_spot_keys_defers_when_streamer_not_ready():
    """Streamer not up yet -- key must NOT be marked subscribed, so a later
    call (once the streamer exists) can genuinely retry it."""
    feeder = UpstoxFeeder(_FakeBus())
    feeder._streamer = None

    feeder.register_extra_spot_keys({"NSE_EQ|INE669C01036": "TECHM"})

    assert "NSE_EQ|INE669C01036" not in feeder._subscribed_keys
    assert feeder._extra_spot_keys == {"NSE_EQ|INE669C01036": "TECHM"}


def test_register_extra_spot_keys_retries_once_streamer_ready():
    """The exact recovery path this fix enables: register while the streamer
    is down (deferred, not marked), then register the SAME key again once
    the streamer is up (e.g. via _reapply_extra_spot_keys on reconnect) --
    it must now actually reach the real subscribe() call."""
    feeder = UpstoxFeeder(_FakeBus())
    feeder._streamer = None
    feeder.register_extra_spot_keys({"NSE_EQ|INE669C01036": "TECHM"})
    assert "NSE_EQ|INE669C01036" not in feeder._subscribed_keys

    streamer = _FakeStreamer()
    feeder._streamer = streamer
    feeder.register_extra_spot_keys(feeder._extra_spot_keys)

    assert "NSE_EQ|INE669C01036" in feeder._subscribed_keys
    assert streamer.subscribed_calls == [["NSE_EQ|INE669C01036"]]


def test_register_extra_spot_keys_does_not_mark_subscribed_on_subscribe_exception():
    """A real subscribe() call that raises must also leave the key eligible
    for retry, not silently marked done."""
    feeder = UpstoxFeeder(_FakeBus())
    feeder._streamer = _FakeStreamer(raise_on_subscribe=True)

    feeder.register_extra_spot_keys({"NSE_EQ|INE669C01036": "TECHM"})

    assert "NSE_EQ|INE669C01036" not in feeder._subscribed_keys


def test_register_extra_spot_keys_marks_subscribed_on_genuine_success():
    """Baseline: the streamer is ready and subscribe succeeds -- the key
    IS marked subscribed (so a duplicate call doesn't re-send it)."""
    feeder = UpstoxFeeder(_FakeBus())
    streamer = _FakeStreamer()
    feeder._streamer = streamer

    feeder.register_extra_spot_keys({"NSE_EQ|INE669C01036": "TECHM"})

    assert "NSE_EQ|INE669C01036" in feeder._subscribed_keys
    assert streamer.subscribed_calls == [["NSE_EQ|INE669C01036"]]

    # A second call with the same key must be a no-op (already subscribed).
    feeder.register_extra_spot_keys({"NSE_EQ|INE669C01036": "TECHM"})
    assert streamer.subscribed_calls == [["NSE_EQ|INE669C01036"]]
