# strategies/sample_strategy/__init__.py — plug-and-play example (not registered by default)
from strategies.sample_strategy.book_manager import SampleBookManager
from strategies.sample_strategy.engine import SampleStrategy

__all__ = ["SampleBookManager", "SampleStrategy"]
