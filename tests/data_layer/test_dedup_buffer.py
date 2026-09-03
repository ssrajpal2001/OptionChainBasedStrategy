"""
tests/data_layer/test_dedup_buffer.py -- regression for the 2026-08-23 fix
to DedupBuffer's active-passive failover gate (data_layer/global_feeder.py).

Real bug: self._last_primary_ts was a SINGLE global timestamp, refreshed by
ANY symbol ticking from the primary provider. If the primary silently
dropped just ONE strike/index while every other symbol on that connection
kept ticking fine, the primary looked "healthy" globally -- so the
secondary's real ticks for that one dead symbol were still rejected,
defeating the entire purpose of having a backup feed for exactly that
scenario. Fixed to track primary freshness per-symbol.
"""
from data_layer.global_feeder import DedupBuffer


def test_secondary_takes_over_for_a_symbol_the_primary_stopped_ticking():
    """The core bug: primary keeps ticking OTHER symbols fine, but has gone
    silent for one specific symbol past stale_sec. The secondary's tick for
    THAT symbol must be accepted, even though the primary looks globally
    healthy (real recent ticks for a different symbol)."""
    buf = DedupBuffer()
    buf.set_primary("upstox", stale_sec=3.0)

    # Primary establishes freshness for BOTH symbols first.
    assert buf.accept("NIFTY", 24500.0, provider="upstox") is True
    assert buf.accept("BANKNIFTY", 51000.0, provider="upstox") is True

    # Time passes: fake the passage of stale_sec+ for NIFTY specifically by
    # directly manipulating the per-symbol timestamp (avoids a real sleep).
    import time
    buf._last_primary_ts["NIFTY"] = time.monotonic() - 5.0
    # BANKNIFTY stays fresh (simulates primary still ticking everything else).
    buf._last_primary_ts["BANKNIFTY"] = time.monotonic()

    # Secondary's NIFTY tick must now be accepted -- NIFTY's own primary feed is stale.
    assert buf.accept("NIFTY", 24505.0, provider="fyers") is True
    # Secondary's BANKNIFTY tick must still be rejected -- primary is genuinely fresh for it.
    assert buf.accept("BANKNIFTY", 51010.0, provider="fyers") is False


def test_secondary_blocked_while_primary_is_fresh_for_that_symbol():
    buf = DedupBuffer()
    buf.set_primary("upstox", stale_sec=3.0)
    assert buf.accept("NIFTY", 24500.0, provider="upstox") is True
    # Immediately, primary is still fresh -- secondary must be rejected.
    assert buf.accept("NIFTY", 24501.0, provider="fyers") is False


def test_secondary_allowed_for_a_symbol_the_primary_has_never_ticked_within_boot_window():
    """Preserves the original boot-window guard: before ANY primary tick for
    a given symbol, the secondary is still gated by the boot-time reference,
    not immediately allowed through."""
    buf = DedupBuffer()
    buf.set_primary("upstox", stale_sec=3.0)
    # Primary has never ticked this symbol at all yet -- boot window still active.
    assert buf.accept("SENSEX", 79000.0, provider="fyers") is False


def test_secondary_allowed_once_boot_window_expires_for_a_never_ticked_symbol():
    buf = DedupBuffer()
    buf.set_primary("upstox", stale_sec=3.0)
    import time
    buf._primary_set_ts = time.monotonic() - 5.0   # boot window long expired
    assert buf.accept("SENSEX", 79000.0, provider="fyers") is True


def test_set_primary_resets_per_symbol_state():
    buf = DedupBuffer()
    buf.set_primary("upstox", stale_sec=3.0)
    buf.accept("NIFTY", 24500.0, provider="upstox")
    assert "NIFTY" in buf._last_primary_ts
    buf.set_primary("upstox", stale_sec=3.0)   # e.g. a reconnect re-establishing the primary
    assert buf._last_primary_ts == {}


def test_legacy_active_active_mode_unaffected_when_no_primary_set():
    """set_primary() never called (or called with None) -- legacy
    active-active dedup-only behavior must be completely unaffected by
    this fix (provider argument is simply not gated at all)."""
    buf = DedupBuffer()
    assert buf.accept("NIFTY", 24500.0, provider="upstox") is True
    assert buf.accept("NIFTY", 24500.5, provider="fyers") is True   # price moved enough, price-diff dedup only
