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


def test_debug_calls_are_not_written_to_the_file_by_default():
    """2026-09-23 CRITICAL FIX, real live incident: the file handler had no
    level of its own, so every .debug(...) call was ALWAYS written to the
    file at full volume regardless of the logger's own DEBUG level -- a
    "downgrade to DEBUG" fix produced zero visible change in the real log.
    The file handler must now genuinely drop DEBUG-level records."""
    with tempfile.TemporaryDirectory() as d:
        lg = make_strategy_logger("ss_DEBUGFILTER_20260613", log_dir=d)
        try:
            lg.debug("this debug line must NOT reach the file")
            lg.info("this info line MUST reach the file")
            for h in lg.handlers:
                h.flush()
            path = os.path.join(d, "ss_DEBUGFILTER_20260613.log")
            with open(path) as f:
                content = f.read()
            assert "this info line MUST reach the file" in content
            assert "this debug line must NOT reach the file" not in content
        finally:
            cleanup_handler(lg.name)


def test_file_level_can_be_lowered_explicitly():
    """A caller that genuinely needs full DEBUG detail in the file can still
    opt in via file_level=logging.DEBUG."""
    with tempfile.TemporaryDirectory() as d:
        lg = make_strategy_logger("ss_DEBUGOPTIN_20260613", log_dir=d, file_level=logging.DEBUG)
        try:
            lg.debug("this debug line SHOULD reach the file")
            for h in lg.handlers:
                h.flush()
            path = os.path.join(d, "ss_DEBUGOPTIN_20260613.log")
            with open(path) as f:
                content = f.read()
            assert "this debug line SHOULD reach the file" in content
        finally:
            cleanup_handler(lg.name)
