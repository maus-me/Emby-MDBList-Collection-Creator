# src/logger_config.py
import logging
import os

def setup_logging(level=logging.INFO):
    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove any existing handlers to avoid duplicates
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # Add handler to root logger
    root_logger.addHandler(console_handler)

    # Create a directory for logs if it doesn't exist
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create logs.log file if it doesn't exist
    log_file_path = os.path.join(log_dir, 'logs.log')
    if not os.path.exists(log_file_path):
        with open(log_file_path, 'w') as f:
            pass  # Create an empty file

    # Add file handler if needed
    file_handler = logging.FileHandler('logs/logs.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)