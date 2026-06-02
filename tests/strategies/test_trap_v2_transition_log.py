"""TT v2 — per-transition log messages (Below→Above→Return story per leg)."""

from strategies.trap_trading_engine import trap_transition_msg


def test_sellers_in_message():
    m = trap_transition_msg("CE", 8800, "HTF", "SELLERS_IN", 900.0, 1000.0, 880.0)
    assert "CE 8800 HTF SELLERS_IN" in m
    assert "broke below L=900.00" in m
    assert "sl=1000.00" in m
    assert "sellers entered" in m


def test_trapped_message():
    m = trap_transition_msg("CE", 8800, "HTF", "TRAPPED", 900.0, 1000.0, 1010.0)
    assert "TRAPPED" in m
    assert "broke above H=1000.00" in m
    assert "trapped" in m


def test_entry_ready_message():
    m = trap_transition_msg("PE", 9300, "MTF", "ENTRY_READY", 855.0, 940.0, 855.0)
    assert "PE 9300 MTF ENTRY_READY" in m
    assert "returned to L=855.00" in m


def test_watch_message_is_generic():
    m = trap_transition_msg("CE", 8800, "HTF", "WATCH", 0.0, 0.0, 500.0)
    assert "CE 8800 HTF WATCH" in m
