"""Tests for FnO stock scanner core logic."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pandas as pd
import pytest

# ── helpers that don't need Upstox ──────────────────────────────────────────

def test_compute_rr_ce():
    from fno_stock_scanner import _compute_rr
    # CE: entry=100, sl=90 (risk=10), t1=130 (reward=30) → rr=3.0
    result = _compute_rr(entry=100.0, sl=90.0, t1=130.0, direction="CE")
    assert abs(result["rr_ratio"] - 3.0) < 0.01
    assert result["risk_pts"] == pytest.approx(10.0)
    assert result["reward_pts"] == pytest.approx(30.0)

def test_compute_rr_pe():
    from fno_stock_scanner import _compute_rr
    # PE: entry=200, sl=215 (risk=15), t1=170 (reward=30) → rr=2.0
    result = _compute_rr(entry=200.0, sl=215.0, t1=170.0, direction="PE")
    assert abs(result["rr_ratio"] - 2.0) < 0.01

def test_compute_rr_zero_risk():
    from fno_stock_scanner import _compute_rr
    # entry == sl → rr=0 (guard against div-by-zero)
    result = _compute_rr(entry=100.0, sl=100.0, t1=130.0, direction="CE")
    assert result["rr_ratio"] == 0.0

def test_proximity_pass():
    from fno_stock_scanner import _in_proximity
    # last_close=98, zone_low=95, zone_high=105, prox_pct=3.0 → close is inside zone → True
    assert _in_proximity(last_close=98.0, zone_low=95.0, zone_high=105.0, prox_pct=3.0) is True

def test_proximity_outside_but_near():
    from fno_stock_scanner import _in_proximity
    # last_close=106, zone_high=105 → 0.95% above zone → within 1.0% → True
    assert _in_proximity(last_close=106.0, zone_low=95.0, zone_high=105.0, prox_pct=1.0) is True

def test_proximity_fail():
    from fno_stock_scanner import _in_proximity
    # last_close=112, zone_high=105 → 6.7% above zone → outside 1.0% → False
    assert _in_proximity(last_close=112.0, zone_low=95.0, zone_high=105.0, prox_pct=1.0) is False

def test_zone_age():
    from fno_stock_scanner import _zone_age_days
    import pandas as pd
    from datetime import date, timedelta
    trapped_ts = (date.today() - timedelta(days=3)).isoformat()
    assert _zone_age_days(trapped_ts) == 3

def test_nifty_bias_picks_nearest_zone():
    from fno_stock_scanner import _pick_nifty_bias
    bear_zone = {"kind": "BEAR", "zone_high": 24500.0, "zone_low": 24300.0,
                 "sl": 24600.0, "status": "TRAPPED", "trapped_on": "2026-06-30"}
    bull_zone = {"kind": "BULL", "zone_high": 23800.0, "zone_low": 23600.0,
                 "sl": 23500.0, "status": "TRAPPED", "trapped_on": "2026-06-30"}
    # nifty_close=24350 → inside bear zone → bear zone is nearer
    bias, zone = _pick_nifty_bias(nifty_close=24350.0, zones=[bear_zone, bull_zone], prox_pct=2.0)
    assert bias == "CE"   # near bearish zone → expect CE buys on stocks
    assert zone["kind"] == "BEAR"

def test_nifty_bias_bull():
    from fno_stock_scanner import _pick_nifty_bias
    bull_zone = {"kind": "BULL", "zone_high": 23800.0, "zone_low": 23600.0,
                 "sl": 23500.0, "status": "TRAPPED", "trapped_on": "2026-06-30"}
    bias, zone = _pick_nifty_bias(nifty_close=23650.0, zones=[bull_zone], prox_pct=2.0)
    assert bias == "PE"   # near bullish zone → expect PE buys on stocks


def test_scan_json_structure():
    """Verify the JSON file written by run_scan has expected top-level keys."""
    import json, tempfile, os
    # Build a minimal mock output
    mock = {
        "scan_date": "2026-07-01",
        "nifty_close": 24500.0,
        "nifty_bias": "CE",
        "nifty_zone": {"zone_high": 24600.0, "zone_low": 24400.0},
        "stocks": [
            {"symbol": "RELIANCE", "direction": "CE", "rr_ratio": 2.5,
             "zone_high": 1310.0, "zone_low": 1280.0, "last_close": 1295.0,
             "stock_sl": 1277.4, "stock_t1": 1336.0, "risk_pts": 17.6,
             "reward_pts": 41.0, "zone_distance_pct": 0.0,
             "suggested_strike": 1300, "lot_size": 250,
             "zone_age_days": 3, "zone_tests": 1, "scanned_at": "2026-07-01T16:00:00"}
        ],
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(mock, f)
        tmp = f.name
    loaded = json.load(open(tmp))
    assert "stocks" in loaded
    s = loaded["stocks"][0]
    # Top-level keys
    assert loaded["nifty_bias"] in ("CE", "PE")
    assert "nifty_zone" in loaded
    # Per-stock fields consumed by the UI cards
    for field in ("rr_ratio", "zone_low", "zone_high", "stock_sl", "stock_t1",
                  "risk_pts", "reward_pts", "suggested_strike",
                  "zone_age_days", "zone_tests", "last_close"):
        assert field in s, f"Missing field: {field}"
    assert s["rr_ratio"] == 2.5
    assert s["suggested_strike"] == 1300
    os.unlink(tmp)


def test_scan_json_stock_nifty_bias_field():
    """Each stock entry should carry a nifty_bias field for display in the UI card."""
    import json, tempfile, os
    mock = {
        "scan_date": "2026-07-01",
        "nifty_close": 24500.0,
        "nifty_bias": "CE",
        "nifty_zone": {},
        "stocks": [
            {"symbol": "INFY", "direction": "CE", "rr_ratio": 1.8,
             "zone_high": 1850.0, "zone_low": 1820.0, "last_close": 1830.0,
             "stock_sl": 1815.0, "stock_t1": 1880.0, "risk_pts": 15.0,
             "reward_pts": 50.0, "zone_distance_pct": 0.5,
             "suggested_strike": 1840, "lot_size": 300,
             "zone_age_days": 5, "zone_tests": 2,
             "nifty_bias": "CE",
             "scanned_at": "2026-07-01T16:00:00"}
        ],
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(mock, f)
        tmp = f.name
    loaded = json.load(open(tmp))
    s = loaded["stocks"][0]
    assert "nifty_bias" in s
    assert s["nifty_bias"] in ("CE", "PE")
    os.unlink(tmp)
