# strategies/trap_scanner/__init__.py — public API
from strategies.trap_scanner.engine import TrapScannerEngine

__all__ = ["TrapBookManager", "TrapScannerEngine"]


def __getattr__(name: str):
    """Lazy import so the package can export TrapBookManager without creating a
    circular dependency with strategies.trap_scanner_engine."""
    if name == "TrapBookManager":
        from strategies.trap_book_manager import TrapBookManager
        return TrapBookManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
