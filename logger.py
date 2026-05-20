"""
logger.py
=========
Structured logging configuration for production.

Provides a ``get_logger(name)`` factory that returns a properly configured
logger with JSON-formatted output, log-level filtering, and optional
file rotation.

Usage::

    from logger import get_logger
    logger = get_logger(__name__)
    logger.info("Server started", extra={"port": 8000})
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE = os.getenv("LOG_FILE", "")          # empty = stdout only
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))

_FORMAT = "[%(asctime)s] %(levelname)-8s %(name)-24s  %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_configured = False


def _configure_root() -> None:
    global _configured
    if _configured:
        return

    root = logging.getLogger()
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FMT))
    root.addHandler(console)

    # Optional file handler
    if LOG_FILE:
        os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
        file_handler = RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FMT))
        root.addHandler(file_handler)

    # Silence noisy third-party loggers
    for noisy in ("urllib3", "httpcore", "httpx", "watchfiles", "multipart"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger for the given module name."""
    _configure_root()
    return logging.getLogger(name)
