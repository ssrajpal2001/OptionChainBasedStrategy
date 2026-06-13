# tests/test_logging_utils.py
import logging
import os
import tempfile
import pytest
from utils.logging_utils import make_strategy_logger


def cleanup_handler(logger_name):
    """Helper to close and remove all handlers for a logger."""
    lg = logging.getLogger(logger_name)
    for handler in lg.handlers[:]:
        handler.close()
        lg.removeHandler(handler)


def test_returns_logger():
    with tempfile.TemporaryDirectory() as d:
        lg = make_strategy_logger("ss_TEST_20260613", log_dir=d)
        try:
            assert isinstance(lg, logging.Logger)
        finally:
            cleanup_handler(lg.name)


def test_idempotent():
    with tempfile.TemporaryDirectory() as d:
        lg1 = make_strategy_logger("ss_IDEM_20260613", log_dir=d)
        try:
            lg2 = make_strategy_logger("ss_IDEM_20260613", log_dir=d)
            assert lg1 is lg2
            assert len(lg1.handlers) == 1  # not doubled
        finally:
            cleanup_handler(lg1.name)


def test_log_file_created():
    with tempfile.TemporaryDirectory() as d:
        lg = make_strategy_logger("ss_FILE_20260613", log_dir=d)
        try:
            files = os.listdir(d)
            assert any("ss_FILE" in f for f in files)
        finally:
            cleanup_handler(lg.name)
