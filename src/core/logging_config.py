"""Production logging configuration."""
import logging
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from datetime import datetime


def setup_logging(
    log_dir: Path = None,
    level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
    console: bool = True,
) -> logging.Logger:
    """
    Set up production logging with file rotation.

    Args:
        log_dir: Directory for log files
        level: Logging level
        max_bytes: Max size per log file before rotation
        backup_count: Number of backup files to keep
        console: Whether to also log to console
    """
    # Determine log directory
    if log_dir is None:
        if os.name == 'nt':  # Windows
            log_dir = Path(os.environ.get('APPDATA', '')) / 'FractalConnector' / 'logs'
        else:
            log_dir = Path.home() / '.fractal-connector' / 'logs'

    log_dir.mkdir(parents=True, exist_ok=True)

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Clear existing handlers
    root_logger.handlers.clear()

    # Main log file with rotation by size
    main_log_file = log_dir / 'fractal-connector.log'
    file_handler = RotatingFileHandler(
        main_log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8',
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(file_handler)

    # Error log file (errors only)
    error_log_file = log_dir / 'errors.log'
    error_handler = RotatingFileHandler(
        error_log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8',
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(error_handler)

    # Console handler
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(simple_formatter)
        root_logger.addHandler(console_handler)

    # Log startup
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log directory: {log_dir}")

    return root_logger


def get_log_dir() -> Path:
    """Get the log directory path."""
    if os.name == 'nt':
        return Path(os.environ.get('APPDATA', '')) / 'FractalConnector' / 'logs'
    else:
        return Path.home() / '.fractal-connector' / 'logs'
