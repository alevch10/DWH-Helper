import logging
from typing import Optional


def configure_logging(level: Optional[str] = None) -> None:
    """Configure logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
               Defaults to INFO if not provided.
    """
    log_level = level or "INFO"
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Console handler with formatting
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)

    # Log initial configuration
    logger = logging.getLogger(__name__)
    logger.info("Logging configured with level: %s", log_level)


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger instance for a module.

    Args:
        name: Module name (typically __name__).

    Returns:
        Configured logger instance.
    """
    return logging.getLogger(name)
