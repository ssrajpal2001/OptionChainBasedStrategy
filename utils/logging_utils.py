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
) -> logging.Logger:
    """Return a RotatingFileHandler logger. Idempotent — safe to call multiple times.

    Args:
        filename_stem: The log filename without extension, e.g. ``ss_NIFTY_client1_b1_20260613``.
        log_dir:       Directory for log files (created if missing).
        propagate:     Whether to also emit to the root logger / parent handlers.
        max_bytes:     Rotate after this many bytes (default 10 MB).
        backup_count:  Keep this many rotated backups.
    """
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
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
    lg.addHandler(fh)
    lg.propagate = propagate
    return lg
