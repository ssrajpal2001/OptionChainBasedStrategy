"""utils/logging_utils.py — unified strategy/client logger factory."""
from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler


def make_strategy_logger(
    filename_stem: str,
    *,
    log_dir: str = os.path.join("logs", "clients"),
    propagate: bool = False,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 3,
    file_level: int = logging.INFO,
) -> logging.Logger:
    """Return a RotatingFileHandler logger. Idempotent — safe to call multiple times.

    Args:
        filename_stem: The log filename without extension, e.g. ``ss_NIFTY_client1_b1_20260613``.
        log_dir:       Directory for log files (created if missing).
        propagate:     Whether to also emit to the root logger / parent handlers.
        max_bytes:     Rotate after this many bytes (default 10 MB).
        backup_count:  Keep this many rotated backups.
        file_level:    Minimum level actually WRITTEN to the file (default INFO).

    2026-09-23 CRITICAL FIX, real live incident: the logger itself was set to
    DEBUG but the file handler had no level of its own -- an unset handler
    level defaults to processing everything the logger lets through, so every
    `.debug(...)` call site across this codebase's per-binding loggers (e.g.
    sell_straddle's SHADOW_VWAP REST-seed, several in oi_orb_screener) has
    ALWAYS been written to these log files at full volume, never actually
    suppressed. Confirmed live: a "downgrade this line to DEBUG so it stops
    cluttering the log" fix produced zero visible change -- the line kept
    appearing, just relabeled "DEBUG" instead of "INFO", because nothing
    was ever filtering by level. The file handler now has its own level
    (default INFO); DEBUG-level calls are genuinely dropped before ever
    reaching the file, while the logger itself stays at DEBUG so a caller
    can still request file_level=logging.DEBUG explicitly if a specific
    log genuinely needs full detail."""
    name = f"strat.{filename_stem}"
    lg = logging.getLogger(name)
    if lg.handlers:
        return lg  # already configured — idempotent
    lg.setLevel(logging.DEBUG)
    os.makedirs(log_dir, exist_ok=True)
    fh = RotatingFileHandler(
        os.path.join(log_dir, f"{filename_stem}.log"),
        encoding="utf-8",
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    fh.setLevel(file_level)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
    lg.addHandler(fh)
    lg.propagate = propagate
    return lg
