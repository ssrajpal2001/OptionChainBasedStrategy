"""EXIT-EVAL formatter — full exit-criteria line on the max-TF close."""

from strategies.sell_straddle import format_exit_eval


def test_hold_when_nothing_triggers():
    line = format_exit_eval("NIFTY", pnl_pts=12.0, credit=300.0, criteria=[
        ("ProfitTgt", "4.0% vs 30%", False),
        ("HardSL", "4.0% vs -200%", False),
        ("Dynamic", "CLOSE>VWAP=x", False),
    ])
    assert "EXIT-EVAL NIFTY" in line
    assert "pnl=12.00" in line
    assert "4.0% of credit" in line
    assert "ProfitTgt(4.0% vs 30%)=" in line
    assert line.rstrip().endswith("HOLD")


def test_exit_lists_fired_criteria():
    line = format_exit_eval("NIFTY", pnl_pts=90.0, credit=300.0, criteria=[
        ("ProfitTgt", "30.0% vs 30%", True),
        ("HardSL", "30.0% vs -200%", False),
    ])
    assert "HIT" in line
    assert "EXIT:ProfitTgt" in line


def test_pct_zero_credit_safe():
    line = format_exit_eval("X", 5.0, 0.0, [("HardSL", "n/a", False)])
    assert "EXIT-EVAL X" in line  # no divide-by-zero
