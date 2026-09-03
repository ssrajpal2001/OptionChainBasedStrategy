"""2026-07-23 whole-branch review fix (final review of the pool-engine
feature across commits 7530295..0c7a9cb): several V4CascadeBook methods NOT
touched by any of the 9 pool-engine tasks still read self._engine.position
unconditionally -- always None for a pool-engine book, since a pool book's
real position lives on self._pool_engine.position instead. Fixed via a new
V4CascadeBook._active_position property/setter (routes to whichever engine
is active), used at every affected call site:

  C1 (Critical) -- _maybe_recenter_tracking_strikes's flatness gate (both
      the pre-fetch check and the atomic post-fetch re-check) never blocked
      recentering under an open pool position; the re-warm block also
      rebuilt the OLD (inert, for a pool book) engine's scanners instead of
      the pool engine's own zone pool.
  C2 (Critical) -- _on_fill's ENTRY-abort path never cleared
      self._pool_engine.position, leaving a phantom "open" position that
      survived restarts and blocked all future entries on that side.
  I1 (Important) -- square_off() was a complete no-op for pool positions
      (0 legs closed, dashboard flatten button did nothing).
  I2 (Important) -- _on_fill's EXIT-failure path reverted the leg back to
      "open" but left pos.status stuck at "closed", so
      PoolCascadeEngine.is_open() (gated on status=="open") would never
      re-check/retry that leg again.

These tests exercise each fix against a REAL V4CascadeBook(use_pool_engine=
True), not mocks -- following the pattern already established in
tests/strategies/test_v4_cascade_pool_engine_*.py."""
import asyncio
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest

from config.global_config import GlobalConfig
from data_layer import position_store
from data_layer.base_feeder import EventBus
from execution_bridge.cascade_bridge import CascadeFillEvent
from strategies.v4_cascade.book import V4CascadeBook, _Bar
from strategies.v4_cascade.dataclasses import CascadePosition, TrancheLeg

IST = ZoneInfo("Asia/Kolkata")


def _open_pos(side="CE"):
    t1 = TrancheLeg(tranche="T1", option_type=side, strike=23700, qty=65,
                     entry_price=100.0, sl_price=90.0, target_price=110.0)
    t2 = TrancheLeg(tranche="T2", option_type=side, strike=23700, qty=65,
                     entry_price=100.0, sl_price=90.0, tracking_current_stop=95.0)
    return CascadePosition(underlying="NIFTY", side=side, tracking_strike=23700,
                            execution_strike=23700, atm_at_trigger=23700, entry_spot=23700,
                            tracking_entry_price=100.0, t1=t1, t2=t2,
                            open_time=datetime.now(IST))


def _book(client_id="ARPOS", binding_id="BRPOS"):
    cfg = GlobalConfig()
    book = V4CascadeBook(EventBus(), cfg, underlying="NIFTY", client_id=client_id,
                          binding_id=binding_id, lot_multiplier=1, squareoff_time="15:15",
                          use_pool_engine=True)
    book._running = True
    return book


def _cleanup(book):
    try:
        position_store.clear(book._persist_key)
    except Exception:
        pass


# ── C1: recenter must respect an OPEN POOL position ─────────────────────────

@pytest.mark.asyncio
async def test_recenter_refused_when_pool_position_open():
    """The flatness gate previously read self._engine.position (always None
    for a pool book) and so NEVER blocked recentering under a genuinely open
    pool position -- NIFTY's 100pt tracking_recenter_pts threshold is a
    routine intraday drift, so this fired with real frequency and corrupted
    the bars/strikes an open pool position's exit-checking depends on."""
    book = _book()
    try:
        book._expiry = date(2026, 7, 21)
        book._tracking_reference_atm = 24216.05
        book._ce_strike, book._pe_strike = 24000, 24400
        book._pool_engine.position = _open_pos("CE")
        assert book._engine.position is None  # old engine stays inert/flat

        with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=[])), \
             patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
             patch.object(book, "_access_token", return_value="tok"), \
             patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", return_value="NSE_FO|new"):
            await book._maybe_recenter_tracking_strikes(current_atm=25000.0)   # huge drift

        assert book._ce_strike == 24000
        assert book._pe_strike == 24400
        assert book._tracking_reference_atm == 24216.05
        assert book._pool_engine.position.status == "open"
        assert book._pool_engine.position.t1.status == "open"
    finally:
        _cleanup(book)


@pytest.mark.asyncio
async def test_recenter_rewarms_pool_engine_zone_pool_via_multi_strike_diff():
    """C1's second half (the re-warm decision), re-targeted 2026-07-24 for
    Task 6's diff-based multi-strike recenter: the single-strike
    pool-engine recenter branch this test originally exercised (a bare
    ``reset_side`` + 2-candidate ``_replay_pool_engine_history`` call
    living directly inside ``_maybe_recenter_tracking_strikes``) was
    deleted outright by Task 6 -- a pool-engine book now always goes
    through ``_recenter_multi_strike``, which diffs the OLD candidate
    window against the NEW one and only resets the strikes that actually
    changed (``PoolCascadeEngine.reset_candidate``, not a full
    ``reset_side`` wipe). The regression this still guards against is
    unchanged: a strike swap must not silently keep mixing the OLD
    strike's 75m bar history into the SAME zone-pool/dedup state as the
    NEW strike's bars forever (worse than not re-warming at all) -- and
    the old (inert, for a pool book) engine must never be touched."""
    book = _book()
    try:
        book._expiry = date(2026, 7, 21)
        book._tracking_reference_atm = 24216.05
        old_ce, old_pe = 24000, 24400
        book._ce_strike, book._pe_strike = old_ce, old_pe
        book._ce_strikes, book._pe_strikes = [old_ce], [old_pe]
        book._ce_symbols, book._pe_symbols = ["NSE_FO|old_ce"], ["NSE_FO|old_pe"]
        book._pool_engine.position = None

        # Poison pre-existing per-side state as if built from the OLD
        # strike's history -- must be CLEARED (not silently appended to)
        # by the recenter, not carried forward mixed with the new bars.
        stale_bar = _Bar(datetime(2026, 6, 1, 10, 30, tzinfo=IST), 9000, 9050, 8950, 9010, tf=75)
        old_ce_key = ("CE", old_ce)
        book._pool_engine._all_75m.setdefault(old_ce_key, []).append(stale_bar)
        book._pool_engine._known_ref_ts.setdefault(old_ce_key, set()).add(datetime(2026, 6, 1, 9, 15, tzinfo=IST))

        # 100 one-minute rows starting at the 09:15 session open crosses a
        # real 75-minute bucket boundary, so on_75m_bar actually fires
        # during the re-warm (mirrors test_v4_cascade_pool_engine_history_
        # replay.py's proven pattern for a meaningful assertion).
        base = datetime(2026, 7, 21, 9, 15, tzinfo=IST)
        rows = [{"ts": (base + timedelta(minutes=i)).isoformat(), "open": 100, "high": 101,
                 "low": 99, "close": 100, "volume": 10} for i in range(100)]

        def _fake_upstox_key(underlying, expiry, strike, opt_type):
            return f"NSE_FO|{opt_type}{int(strike)}"

        with patch("strategies.v4_cascade.book.fetch_upstox_range_1m", new=AsyncMock(return_value=rows)), \
             patch("strategies.v4_cascade.book.fetch_upstox_intraday_1m", new=AsyncMock(return_value=[])), \
             patch.object(book, "_access_token", return_value="tok"), \
             patch("strategies.v4_cascade.book.REGISTRY.get_upstox_key", side_effect=_fake_upstox_key):
            await book._maybe_recenter_tracking_strikes(current_atm=24320.0)   # drift = 103.95 >= 100

        # Old engine must never be touched -- completely inert for a pool book.
        assert book._engine._scanners["CE"].setups == []
        assert book._engine.position is None

        # The window actually moved (100pt-offset default -> ATM 24300 rounds
        # to CE=24100/PE=24500), so this genuinely exercises the diff, not a
        # no-op.
        new_ce = book._ce_strikes[0]
        assert new_ce != old_ce
        new_ce_key = ("CE", new_ce)

        # New candidate's zone pool must have real state rebuilt from the NEW
        # strike's history ...
        assert len(book._pool_engine._all_75m[new_ce_key]) > 0
        # ... and the OLD strike's zone pool must have been reset_candidate-
        # cleared (it fell out of the window), not left dangling with the
        # stale bar mixed into whatever else accumulates there.
        assert book._pool_engine._all_75m.get(old_ce_key, []) == []
        assert stale_bar not in book._pool_engine._all_75m.get(old_ce_key, [])
    finally:
        _cleanup(book)


# ── C2: aborted/failed pool-engine ENTRY must clear the position ───────────

def test_on_fill_entry_abort_clears_pool_engine_position_not_old_engine():
    """Pre-fix: `if target is None or self._engine.position is target` was
    always False for a pool book (target is the real pool position object,
    self._engine.position is always None) -- the clear never fired, so a
    phantom "open" position got persisted to disk and survived restarts,
    blocking all future entries on that side."""
    book = _book()
    try:
        pos = _open_pos("CE")
        book._pool_engine.position = pos
        book._pending_fills["evt-abort-1"] = pos

        fill = CascadeFillEvent(action="ENTRY", underlying="NIFTY", side="CE", tranche="BOTH",
                                 fill_price=0.0, qty=130, client_id="ARPOS", binding_id="BRPOS",
                                 event_id="evt-abort-1", entry_aborted=True)
        book._on_fill(fill)

        assert book._pool_engine.position is None
        assert book._engine.position is None   # was already None -- must stay untouched

        # And the clear must actually be persisted (not just in-memory) --
        # otherwise a restart would restore the phantom position right back.
        assert position_store.load(book._persist_key) is None
    finally:
        _cleanup(book)


# ── I1: square_off() must close pool-engine legs ────────────────────────────

@pytest.mark.asyncio
async def test_square_off_closes_pool_engine_position_legs(monkeypatch):
    """Pre-fix: pos = self._engine.position was always None for a pool
    book, so square_off() returned 0 before emitting anything -- the
    dashboard's manual/Run-toggle-OFF flatten button did nothing."""
    book = _book()
    try:
        book._pool_engine.position = _open_pos("CE")

        published = []

        async def fake_publish(topic, event):
            published.append(event)

        monkeypatch.setattr(book._bus, "publish", fake_publish)

        closed = await book.square_off(reason="manual")

        assert closed == 2   # T1 + T2
        assert book._pool_engine.position.status == "closed"
        assert book._pool_engine.position.t1.status == "closed"
        assert book._pool_engine.position.t2.status == "closed"
        assert book._engine.position is None   # old (unused) engine untouched

        await asyncio.sleep(0)   # let the fire-and-forget CASCADE_ORDER_REQUEST publishes run
        assert len(published) == 1
        assert published[0].tranche == "BOTH"
        assert published[0].qty == 130
        assert published[0].close_reason == "manual"
    finally:
        _cleanup(book)


# ── I2: EXIT-failure must leave the pool position "open", not stuck ───────

def test_on_fill_exit_failed_reverts_pool_position_status_to_open():
    """Pre-fix: the leg-level revert (leg.status = "open") worked, but the
    position-level revert read pos = self._engine.position (always None for
    a pool book) and so was skipped -- leaving a leg marked "open" inside a
    position whose .status was already "closed". PoolCascadeEngine.is_open()
    gates on status=="open", so that reopened leg would never be
    re-checked/retried again."""
    book = _book()
    try:
        pos = _open_pos("CE")
        # Simulate the engine having optimistically closed T1 and, believing
        # it was the last open leg, the whole position -- BEFORE the order
        # round-trip confirmed the exit actually failed.
        pos.t1.status = "closed"
        pos.status = "closed"
        book._pool_engine.position = pos
        book._pending_fills["evt-exit-1"] = pos.t1

        fill = CascadeFillEvent(action="EXIT", underlying="NIFTY", side="CE", tranche="T1",
                                 fill_price=0.0, qty=65, client_id="ARPOS", binding_id="BRPOS",
                                 event_id="evt-exit-1", exit_failed=True)
        book._on_fill(fill)

        assert pos.t1.status == "open"
        assert pos.status == "open"   # must NOT stay stuck "closed" with an "open" leg inside
    finally:
        _cleanup(book)
