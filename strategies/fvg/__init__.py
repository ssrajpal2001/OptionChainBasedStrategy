"""strategies/fvg — Fair Value Gap (SMC) strategy: detector, engine, book manager."""
from strategies.fvg.book_manager import FVGBookManager
from strategies.fvg.engine import FVGOrderEvent, FVGStrategy

__all__ = ["FVGStrategy", "FVGOrderEvent", "FVGBookManager"]
