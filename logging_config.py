"""
Centralised logging setup for the Madrid Real Estate Tracker.

Why this exists
---------------
The codebase historically swallowed errors with ``except Exception`` +
``print(f"Error …: {e}")`` and returned empty defaults.  On Streamlit
Cloud / GitHub Actions those prints are easy to lose and carry no stack
trace, so SQL/dialect bugs reached production invisibly (the recurring
"No hay datos disponibles").  ``logger.exception(...)`` logs the full
traceback at ERROR level, making failures observable while callers keep
their safe-default behaviour.

Usage
-----
    from logging_config import get_logger
    logger = get_logger(__name__)

    try:
        ...
    except Exception:
        logger.exception("Error getting price drop stats")
        return {}

Configuration
-------------
- ``LOG_LEVEL`` env var (default ``INFO``) sets the root level.
- Idempotent: configuring twice does not duplicate handlers, so it is
  safe to import from any module and from Streamlit's re-runs.
"""

from __future__ import annotations

import logging
import os

_CONFIGURED = False

_DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _configure_root() -> None:
    """Configure the root logger once, honouring an existing config."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    # If something (pytest, Streamlit, a host) already attached handlers,
    # don't fight it — just make sure the level is sane.
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT, _DATE_FORMAT))
        root.addHandler(handler)
    root.setLevel(level)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a module logger, configuring the root logger on first use."""
    _configure_root()
    return logging.getLogger(name)
