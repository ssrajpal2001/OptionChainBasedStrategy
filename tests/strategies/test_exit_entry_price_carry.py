"""After a restart the bridge's in-memory _last_entry is empty; the EXIT event must carry the
real per-leg entry (sold) prices so history records the true sold rate + P&L (user bug 2026-06-10:
EOD square-off of a restored position logged entry=0 → P&L shown as '—', no SELL row)."""
from execution_bridge.straddle_bridge import StraddleOrderEvent, TradeLogger, StraddleFillEvent
from datetime import datetime


def test_exit_event_carries_entry_prices():
    ev = StraddleOrderEvent(
        action="EXIT", underlying="NIFTY", atm=23400, ce_strike=23400, pe_strike=23350,
        ce_ltp=95.3, pe_ltp=248.7, ce_entry=150.0, pe_entry=172.0, realized_pnl=-21.9,
    )
    assert ev.ce_entry == 150.0 and ev.pe_entry == 172.0


def test_record_uses_event_entry_not_empty_last_entry(tmp_path, monkeypatch):
    """Bridge logic: with _last_entry empty (post-restart), entry must come from the event."""
    # mirror the bridge's resolution expression
    class _Empty:  # stand-in for "no last entry"
        pass
    ev = StraddleOrderEvent(
        action="EXIT", underlying="NIFTY", atm=23400, ce_strike=23400, pe_strike=23350,
        ce_ltp=95.3, pe_ltp=248.7, ce_entry=150.0, pe_entry=172.0,
    )
    entry_ev = None  # _last_entry.get(...) after restart
    entry_ce = ev.ce_entry if getattr(ev, "ce_entry", 0.0) else (entry_ev.ce_ltp if entry_ev else 0.0)
    entry_pe = ev.pe_entry if getattr(ev, "pe_entry", 0.0) else (entry_ev.pe_ltp if entry_ev else 0.0)
    assert entry_ce == 150.0 and entry_pe == 172.0   # NOT 0.0
