# strategies/trap_scanner_engine.py — backward-compat shim (Phase 3).
# The implementation has moved to strategies/trap_scanner/.
from strategies.trap_scanner.engine import TrapScannerEngine
from strategies.trap_scanner.zones import _bars_to_df, _resample_htf

__all__ = ["TrapScannerEngine", "_bars_to_df", "_resample_htf"]
