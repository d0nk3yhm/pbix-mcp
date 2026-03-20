"""
Diagnostic logging for pbix-mcp.

Supports three verbosity levels:
  - normal: warnings and errors only
  - debug:  info-level messages for each operation phase
  - trace:  detailed data dumps for binary format debugging

Usage:
    from pbix_mcp.logging_config import logger, set_level
    set_level("debug")
    logger.info("ZIP opened: %s", path)
"""

import logging
import os

# Create package-level logger
logger = logging.getLogger("pbix_mcp")

# Default: only warnings/errors unless PBIX_MCP_LOG_LEVEL is set
_default_level = os.environ.get("PBIX_MCP_LOG_LEVEL", "WARNING").upper()

_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
))
logger.addHandler(_handler)
logger.setLevel(getattr(logging, _default_level, logging.WARNING))

# Trace level (below DEBUG)
TRACE = 5
logging.addLevelName(TRACE, "TRACE")


def trace(self, message, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, message, args, **kwargs)


logging.Logger.trace = trace  # type: ignore[attr-defined]


def set_level(level: str) -> None:
    """Set logging verbosity: 'normal', 'debug', or 'trace'."""
    level_map = {
        "normal": logging.WARNING,
        "debug": logging.DEBUG,
        "trace": TRACE,
    }
    logger.setLevel(level_map.get(level.lower(), logging.WARNING))
