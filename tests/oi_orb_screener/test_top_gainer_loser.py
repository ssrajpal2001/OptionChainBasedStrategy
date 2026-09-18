"""
Unit + integration tests for the standalone "top gainer/loser" data
pipeline (strategies/oi_orb_screener/screener.py's fetch_top_gainers_
losers/filter_top_gainer_loser_candidates/poll_top_gainers_losers, and
engine.py's _top_gainer_loser_loop/_do_top_gainer_loser_poll).

Verify-only pipeline -- these tests confirm the pure filtering logic and
that the poll loop correctly recomputes membership across cycles; they
deliberately do NOT touch (and there is nothing here that could touch)
any entry/exit/shortlist/position state.
"""
import asyncio
from datetime import datetime

import pandas as pd
import pytest

from config.global_config import IST
from strategies.oi_orb_screener import screener, store


def _cfg(**overrides):
    cfg = dict(screener.CONFIG)
    cfg.update(overrides)
    return cfg


# ── fetch_top_gainers_losers (Step 1) ───────────────────────────────────

class TestFetchTopGainersLosers:
    def test_ranks_gainers_and_losers_separately(self):
        universe = pd.DataFrame([
            {"symbol": "A", "pChange": 5.0}, {"symbol": "B", "pChange": 3.0},
            {"symbol": "C", "pChange": -4.0}, {"symbol": "D", "pChange": -6.0},
            {"symbol": "E", "pChange": 0.5},
        ])
        out = screener.fetch_top_gainers_losers(universe, top_n=2)
        gainers = out[out["rank_type"] == "gainer"]
        losers = out[out["rank_type"] == "loser"]
        assert list(gainers.sort_values("rank")["symbol"]) == ["A", "B"]
        assert list(gainers.sort_values("rank")["rank"]) == [1, 2]
        assert list(losers.sort_values("rank")["symbol"]) == ["D", "C"]
        assert list(losers.sort_values("rank")["rank"]) == [1, 2]

    def test_truncates_to_top_n(self):
        universe = pd.DataFrame([
            {"symbol": f"S{i}", "pChange": float(i)} for i in range(20)
        ])
        out = screener.fetch_top_gainers_losers(universe, top_n=10)
        assert len(out[out["rank_type"] == "gainer"]) == 10
        assert len(out[out["rank_type"] == "loser"]) == 10
        assert len(out) == 20

    def test_empty_universe_returns_empty(self):
        out = screener.fetch_top_gainers_losers(pd.DataFrame(), top_n=10)
        assert out.empty

    def test_missing_pchange_column_returns_empty(self):
        out = screener.fetch_top_gainers_losers(pd.DataFrame({"symbol": ["A"]}), top_n=10)
        assert out.empty

    def test_nan_pchange_rows_excluded(self):
        universe = pd.DataFrame([
            {"symbol": "A", "pChange": 5.0}, {"symbol": "B", "pChange": None},
        ])
        out = screener.fetch_top_gainers_losers(universe, top_n=10)
        assert "B" not in set(out["symbol"])
        assert "A" in set(out["symbol"])


# ── filter_top_gainer_loser_candidates (Steps 3-4) ──────────────────────

class TestFilterTopGainerLoserCandidates:
    def test_passes_above_oi_spurt_and_below_pchange_max(self):
        candidates = pd.DataFrame([
            {"symbol": "A", "pChange": 3.0, "oi_spurt_pct": 8.0},
        ])
        out = screener.filter_top_gainer_loser_candidates(candidates, oi_spurt_min_pct=7.0,
                                                            pchange_max_pct=4.0)
        assert list(out["symbol"]) == ["A"]

    def test_fails_when_oi_spurt_at_or_below_threshold(self):
        candidates = pd.DataFrame([
            {"symbol": "A", "pChange": 3.0, "oi_spurt_pct": 7.0},   # exactly at threshold -- strict >
            {"symbol": "B", "pChange": 3.0, "oi_spurt_pct": 5.0},
        ])
        out = screener.filter_top_gainer_loser_candidates(candidates, oi_spurt_min_pct=7.0,
                                                            pchange_max_pct=4.0)
        assert out.empty

    def test_fails_when_pchange_at_or_above_max(self):
        """Step 4 is a genuine UPPER bound -- a move already too large is
        excluded, opposite direction from every other price-move filter."""
        candidates = pd.DataFrame([
            {"symbol": "A", "pChange": 4.0, "oi_spurt_pct": 10.0},   # exactly at max -- strict <
            {"symbol": "B", "pChange": 6.0, "oi_spurt_pct": 10.0},
        ])
        out = screener.filter_top_gainer_loser_candidates(candidates, oi_spurt_min_pct=7.0,
                                                            pchange_max_pct=4.0)
        assert out.empty

    def test_pchange_upper_bound_applies_to_negative_moves_too(self):
        """|pChange| < max -- a -6% loser is excluded same as a +6% gainer."""
        candidates = pd.DataFrame([
            {"symbol": "A", "pChange": -6.0, "oi_spurt_pct": 10.0},
            {"symbol": "B", "pChange": -2.0, "oi_spurt_pct": 10.0},
        ])
        out = screener.filter_top_gainer_loser_candidates(candidates, oi_spurt_min_pct=7.0,
                                                            pchange_max_pct=4.0)
        assert list(out["symbol"]) == ["B"]

    def test_no_oi_spurt_match_never_qualifies(self):
        candidates = pd.DataFrame([
            {"symbol": "A", "pChange": 3.0, "oi_spurt_pct": float("nan")},
        ])
        out = screener.filter_top_gainer_loser_candidates(candidates, oi_spurt_min_pct=7.0,
                                                            pchange_max_pct=4.0)
        assert out.empty

    def test_empty_input_returns_empty(self):
        out = screener.filter_top_gainer_loser_candidates(pd.DataFrame(), 7.0, 4.0)
        assert out.empty

    def test_realistic_20_candidate_mix(self):
        """Fixed set of 20 gainer/loser candidates with known pChange/OI-
        spurt values, per the task's own requested test shape."""
        rows = []
        for i in range(10):
            rows.append({"symbol": f"G{i}", "rank_type": "gainer", "rank": i + 1,
                         "pChange": 2.0 + i * 0.5, "oi_spurt_pct": 10.0 - i})
        for i in range(10):
            rows.append({"symbol": f"L{i}", "rank_type": "loser", "rank": i + 1,
                         "pChange": -(2.0 + i * 0.5), "oi_spurt_pct": 10.0 - i})
        candidates = pd.DataFrame(rows)
        out = screener.filter_top_gainer_loser_candidates(candidates, oi_spurt_min_pct=7.0,
                                                            pchange_max_pct=4.0)
        # oi_spurt_pct>7 -> i in {0,1,2} (10,9,8); pChange abs<4 -> 2.0+i*0.5<4 -> i<4
        # intersection -> i in {0,1,2} for both gainers and losers
        assert set(out["symbol"]) == {"G0", "G1", "G2", "L0", "L1", "L2"}


# ── poll_top_gainers_losers (full pipeline, one poll cycle) ─────────────

class TestPollTopGainersLosers:
    def test_full_pipeline_real_functions_mocked_at_fetch_boundary(self, monkeypatch):
        universe = pd.DataFrame([
            {"symbol": "A", "pChange": 3.0}, {"symbol": "B", "pChange": 2.5},
            {"symbol": "C", "pChange": -3.5}, {"symbol": "D", "pChange": -8.0},
        ])
        oi_spurts = pd.DataFrame({"symbol": ["A", "B", "C", "D"],
                                   "oi_spurt_pct": [9.0, 3.0, 8.0, 20.0]})
        monkeypatch.setattr(screener, "fetch_fno_price_universe", lambda nse: universe)
        monkeypatch.setattr(screener, "fetch_oi_spurts_nse", lambda nse: oi_spurts)

        cfg = _cfg(TOP_GAINER_LOSER_N=2, TOP_GAINER_LOSER_OI_SPURT_MIN_PCT=7.0,
                   TOP_GAINER_LOSER_PCHANGE_MAX_PCT=4.0)
        candidates, qualifying = screener.poll_top_gainers_losers(nse=None, cfg=cfg)

        assert len(candidates) == 4   # top-2 gainers + top-2 losers
        assert set(candidates["symbol"]) == {"A", "B", "C", "D"}
        # A: oi=9>7, |pChange|=3<4 -> qualifies. B: oi=3, fails. C: oi=8>7,
        # |pChange|=3.5<4 -> qualifies. D: oi=20>7 but |pChange|=8 NOT <4 -> fails.
        assert set(qualifying["symbol"]) == {"A", "C"}

    def test_empty_universe_returns_empty_both(self, monkeypatch):
        monkeypatch.setattr(screener, "fetch_fno_price_universe", lambda nse: pd.DataFrame())
        monkeypatch.setattr(screener, "fetch_oi_spurts_nse", lambda nse: pd.DataFrame())
        candidates, qualifying = screener.poll_top_gainers_losers(nse=None, cfg=_cfg())
        assert candidates.empty and qualifying.empty


# ── store.py persistence ─────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _isolated_store_db(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "_DB_PATH", str(tmp_path / "oi_orb_test.db"))
    monkeypatch.setattr(store, "_initialized", False)
    yield


class TestStorePersistence:
    def test_record_and_readback(self):
        rows = [
            {"symbol": "A", "rank_type": "gainer", "rank": 1,
             "price_change_pct": 3.0, "oi_spurt_pct": 9.0, "qualified": True},
            {"symbol": "D", "rank_type": "loser", "rank": 1,
             "price_change_pct": -8.0, "oi_spurt_pct": 20.0, "qualified": False},
        ]
        store.record_top_gainer_loser_poll("C1", "B1", "2026-09-18T09:20:00+05:30", rows,
                                            trade_date="2026-09-18")
        import sqlite3
        con = sqlite3.connect(store._DB_PATH)
        con.row_factory = sqlite3.Row
        out = con.execute("SELECT * FROM top_gainer_loser_history ORDER BY id").fetchall()
        con.close()
        assert len(out) == 2
        assert out[0]["symbol"] == "A"
        assert out[0]["qualified"] == 1
        assert out[1]["symbol"] == "D"
        assert out[1]["qualified"] == 0
        assert out[1]["oi_spurt_pct"] == 20.0


# ── engine.py poll loop integration ──────────────────────────────────────

class _FakeGlobalFeeder:
    async def subscribe_tokens(self, tokens):
        pass

    def subscribe_fno_equity(self, a, b):
        pass


class _FakeBus:
    def __init__(self):
        self._queues = {}
        self._global_feeder = _FakeGlobalFeeder()

    def subscribe(self, topic):
        return self._queues.setdefault(topic, asyncio.Queue())

    def unsubscribe(self, topic, q):
        pass

    async def publish(self, topic, event):
        pass


class TestDoTopGainerLoserPoll:
    @pytest.mark.asyncio
    async def test_poll_records_and_updates_in_memory_state(self, monkeypatch):
        from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy
        bus = _FakeBus()
        book = OiOrbScreenerStrategy(bus, cfg=None, client_id="C1", binding_id="B1",
                                       lot_multiplier=1, product_type="MIS", squareoff_time="15:15")

        universe = pd.DataFrame([
            {"symbol": "A", "pChange": 3.0}, {"symbol": "B", "pChange": 2.0},
            {"symbol": "C", "pChange": -3.0}, {"symbol": "D", "pChange": -8.0},
        ])
        oi_spurts = pd.DataFrame({"symbol": ["A", "B", "C", "D"],
                                   "oi_spurt_pct": [9.0, 1.0, 1.0, 20.0]})
        monkeypatch.setattr(screener, "fetch_fno_price_universe", lambda nse: universe)
        monkeypatch.setattr(screener, "fetch_oi_spurts_nse", lambda nse: oi_spurts)

        now = datetime(2026, 9, 18, 9, 20, tzinfo=IST)
        await book._do_top_gainer_loser_poll(now, dict(screener.CONFIG, TOP_GAINER_LOSER_N=2))

        assert len(book._top_gainer_loser_all) == 4   # top-2 gainers + top-2 losers
        assert {r["symbol"] for r in book._top_gainer_loser_qualifying} == {"A"}
        # never touches any trading-decision state
        assert book._shortlist_symbols == []
        assert book._positions == {}
        assert book._rejected == set()

    @pytest.mark.asyncio
    async def test_membership_recomputes_across_two_poll_cycles(self, monkeypatch):
        """A symbol added, a symbol dropped, across two consecutive polls
        with different underlying real data -- confirms the qualifying
        list is genuinely recomputed each cycle, not accumulated/frozen."""
        from strategies.oi_orb_screener.engine import OiOrbScreenerStrategy
        bus = _FakeBus()
        book = OiOrbScreenerStrategy(bus, cfg=None, client_id="C1", binding_id="B1",
                                       lot_multiplier=1, product_type="MIS", squareoff_time="15:15")
        cfg = dict(screener.CONFIG, TOP_GAINER_LOSER_N=2)

        # Cycle 1: A qualifies, B does not.
        universe1 = pd.DataFrame([{"symbol": "A", "pChange": 3.0}, {"symbol": "B", "pChange": -3.0}])
        oi1 = pd.DataFrame({"symbol": ["A", "B"], "oi_spurt_pct": [9.0, 2.0]})
        monkeypatch.setattr(screener, "fetch_fno_price_universe", lambda nse: universe1)
        monkeypatch.setattr(screener, "fetch_oi_spurts_nse", lambda nse: oi1)
        await book._do_top_gainer_loser_poll(datetime(2026, 9, 18, 9, 20, tzinfo=IST), cfg)
        assert {r["symbol"] for r in book._top_gainer_loser_qualifying} == {"A"}

        # Cycle 2 (real data moved on): A's OI-spurt fades below threshold
        # (drops off), C is now a fresh qualifying candidate (newly joins).
        universe2 = pd.DataFrame([{"symbol": "A", "pChange": 3.0}, {"symbol": "C", "pChange": 3.5}])
        oi2 = pd.DataFrame({"symbol": ["A", "C"], "oi_spurt_pct": [4.0, 12.0]})
        monkeypatch.setattr(screener, "fetch_fno_price_universe", lambda nse: universe2)
        monkeypatch.setattr(screener, "fetch_oi_spurts_nse", lambda nse: oi2)
        await book._do_top_gainer_loser_poll(datetime(2026, 9, 18, 9, 21, tzinfo=IST), cfg)
        assert {r["symbol"] for r in book._top_gainer_loser_qualifying} == {"C"}
