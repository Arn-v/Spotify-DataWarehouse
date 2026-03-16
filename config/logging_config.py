"""Structured JSON logging configuration."""

import logging
import sys

from pythonjsonlogger import jsonlogger

from config.settings import get_settings


def setup_logging() -> None:
    """Configure structured JSON logging for all modules."""
    settings = get_settings()

    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(settings.log_level.upper())
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    # Quieten noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("spotipy").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
