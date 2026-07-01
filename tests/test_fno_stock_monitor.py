import dataclasses
from strategies.fno_stock_monitor import FnoStockAlert
from config.global_config import Topic
from datetime import datetime
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
