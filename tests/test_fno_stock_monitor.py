import dataclasses
from strategies.fno_stock_monitor import FnoStockAlert
from config.global_config import Topic
from datetime import date, datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

def test_fno_stock_alert_fields():
    a = FnoStockAlert(
        uid="EICHERMOT_PE_7178",
        symbol="EICHERMOT",
        direction="PE",
        spot_price=7144.0,
        d1_zone_low=7165.0,
        d1_zone_high=7178.0,
        d1_zone_date="Jun 30",
        strike=7200,
        lot_size=175,
        sl=7192.0,
        t1=6950.0,
        risk_pts=48.0,
        reward_pts=194.0,
        rr_ratio=4.04,
        mtf_trap_price=7168.0,
        ltf_trap_price=7155.0,
        fired_at=datetime.now(IST),
    )
    d = dataclasses.asdict(a)
    assert d["uid"] == "EICHERMOT_PE_7178"
    assert d["direction"] == "PE"
    assert d["rr_ratio"] == 4.04

def test_topic_constant():
    assert Topic.FNO_STOCK_ALERT == "fno_stock_alert"

def test_register_extra_spot_keys_stored():
    """GlobalFeeder stores extra spot key mapping without error."""
    from unittest.mock import MagicMock
    from data_layer.global_feeder import GlobalFeeder
    bus = MagicMock()
    cfg = MagicMock()
    cfg.mode = "demo"
    gf = GlobalFeeder(bus, cfg)
    # Should not raise even if no underlying feeder active
    gf.register_extra_spot_keys({"NSE_EQ|INE066A01021": "EICHERMOT"})
    assert gf._extra_spot_keys == {"NSE_EQ|INE066A01021": "EICHERMOT"}


# ── Task 3 tests ──────────────────────────────────────────────────────────────
from strategies.fno_stock_monitor import FnoStockMonitor
from data_layer.base_feeder import IndexTick


def _make_monitor(tmp_path, scan_data: dict):
    """Helper: write a fake scan file and return a monitor pointed at it."""
    import json, os
    from unittest.mock import MagicMock
    from data_layer.base_feeder import EventBus

    scan_dir = str(tmp_path)
    today = date.today().isoformat()
    path = os.path.join(scan_dir, f"fno_scan_{today}.json")
    with open(path, "w") as f:
        json.dump(scan_data, f)

    bus = EventBus()
    cfg = MagicMock()
    db  = MagicMock()
    mon = FnoStockMonitor(bus, cfg, db, scan_dir=scan_dir)
    return mon


def test_warm_start_loads_stocks(tmp_path):
    scan = {
        "ce_stocks": [],
        "pe_stocks": [{
            "symbol": "EICHERMOT",
            "direction": "PE",
            "instrument_key": "NSE_EQ|INE066A01021",
            "zone_low": 7165.0, "zone_high": 7178.0,
            "zone_date": "Jun 30",
            "strike": 7200, "lot_size": 175,
            "sl": 7192.0, "t1": 6950.0,
            "risk_pts": 48.0, "reward_pts": 194.0, "rr_ratio": 4.04,
        }],
    }
    mon = _make_monitor(tmp_path, scan)
    mon.warm_start()
    assert "EICHERMOT" in mon._watched
    assert mon._watched["EICHERMOT"]["direction"] == "PE"


def test_warm_start_missing_file_is_noop(tmp_path):
    from data_layer.base_feeder import EventBus
    from unittest.mock import MagicMock
    bus = EventBus(); cfg = MagicMock(); db = MagicMock()
    mon = FnoStockMonitor(bus, cfg, db, scan_dir=str(tmp_path))
    mon.warm_start()   # no file today — must not raise
    assert mon._watched == {}


def test_bucket_closes_on_minute_boundary():
    from data_layer.base_feeder import EventBus
    from unittest.mock import MagicMock
    bus = EventBus(); cfg = MagicMock(); db = MagicMock()
    mon = FnoStockMonitor(bus, cfg, db)
    ts1 = datetime(2026, 7, 3, 9, 15, 30, tzinfo=IST)
    ts2 = datetime(2026, 7, 3, 9, 20, 10, tzinfo=IST)  # new 5m bucket
    closed = mon._update_bucket("5m", "EICHERMOT", 7160.0, ts1)
    assert closed is None   # first tick, no closed bar yet
    closed = mon._update_bucket("5m", "EICHERMOT", 7155.0, ts2)
    assert closed is not None
    assert closed["open"] == 7160.0
    assert closed["close"] == 7160.0  # only one tick in bucket


def test_d1_sl_breach_removes_stock(tmp_path):
    scan = {
        "ce_stocks": [],
        "pe_stocks": [{
            "symbol": "EICHERMOT", "direction": "PE",
            "instrument_key": "NSE_EQ|INE066A01021",
            "zone_low": 7165.0, "zone_high": 7178.0, "zone_date": "Jun 30",
            "strike": 7200, "lot_size": 175,
            "sl": 7192.0, "t1": 6950.0,
            "risk_pts": 48.0, "reward_pts": 194.0, "rr_ratio": 4.04,
        }],
    }
    mon = _make_monitor(tmp_path, scan)
    mon.warm_start()
    ts = datetime(2026, 7, 3, 10, 0, 0, tzinfo=IST)
    # PE SL breaches when price rises ABOVE sl (7192)
    tick = IndexTick(symbol="EICHERMOT", ltp=7200.0,
                     open=7200.0, high=7200.0, low=7200.0, close=7200.0,
                     volume=0, timestamp=ts)
    mon._check_sl_breach(tick)
    assert "EICHERMOT" not in mon._watched


def test_mark_notified_removes_alert(tmp_path):
    scan = {"ce_stocks": [], "pe_stocks": []}
    mon = _make_monitor(tmp_path, scan)
    alert = FnoStockAlert(
        uid="EICHERMOT_PE_7178", symbol="EICHERMOT", direction="PE",
        spot_price=7144.0, d1_zone_low=7165.0, d1_zone_high=7178.0,
        d1_zone_date="Jun 30", strike=7200, lot_size=175,
        sl=7192.0, t1=6950.0, risk_pts=48.0, reward_pts=194.0,
        rr_ratio=4.04, mtf_trap_price=7168.0, ltf_trap_price=7155.0,
        fired_at=datetime.now(IST),
    )
    mon._active_alerts.append(alert)
    mon.mark_notified("EICHERMOT_PE_7178")
    assert mon.get_active_alerts() == []
    assert "EICHERMOT_PE_7178" in mon._notified_uids
