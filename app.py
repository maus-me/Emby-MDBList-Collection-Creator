# At the beginning of app.py
import logging
import time

from src.logger_config import setup_logging
from src.modules.config_parse import *
from src.modules.mdblist import run

# Configure logging before importing other modules
setup_logging(log_file_path=LOG_FILE_PATH)
logger = logging.getLogger(__name__)

def main():
    if ON_STARTUP:
        logger.info("Running MDbList on startup...")
        run()

    while True:
        wait_seconds = WAIT_TIME * 3600
        logger.info(f"Waiting {WAIT_TIME} hours for the next cycle...")
        time.sleep(wait_seconds)
        run()

if __name__ == "__main__":
    main()
