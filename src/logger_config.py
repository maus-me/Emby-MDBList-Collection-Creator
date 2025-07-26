# src/logger_config.py
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from colorama import init, Fore, Style

# Initialize colorama for cross-platform color support
init()


class ColoredFormatter(logging.Formatter):
    """Custom formatter adding colors to logs based on severity level."""

    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Style.BRIGHT
    }

    def format(self, record):
        # Save original levelname
        levelname = record.levelname
        # Add color to the levelname in the format string
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{Style.RESET_ALL}"
        result = super().format(record)
        # Restore original levelname
        record.levelname = levelname
        return result


def setup_logging(level=logging.INFO, log_file_path='logs/logs.log', max_bytes=5 * 1024 * 1024, backup_count=5):
    """
    Configure application logging with console and file handlers.

    Args:
        level: Minimum log level to display (default: INFO)
        log_file_path: Path to the log file (default: 'logs/logs.log')
        max_bytes: Maximum size of each log file before rotation (default: 5MB)
        backup_count: Number of backup files to keep (default: 5)
    """
    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove any existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler with colored output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # Create formatters - colored for console, plain for file
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    color_formatter = ColoredFormatter(log_format)
    file_formatter = logging.Formatter(log_format)

    # Apply color formatter to console
    console_handler.setFormatter(color_formatter)
    root_logger.addHandler(console_handler)

    # Set up file logging
    log_dir = os.path.dirname(log_file_path) or '.'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Add rotating file handler with plain text formatter
    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)  # Use plain formatter for files
    root_logger.addHandler(file_handler)