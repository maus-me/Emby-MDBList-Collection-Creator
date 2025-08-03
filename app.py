# At the beginning of app.py
import logging
from src.logger_config import setup_logging
from src.modules.config_parse import *
from src.modules.mdblist import run

# Configure logging before importing other modules
setup_logging(log_file_path=LOG_FILE_PATH)
logger = logging.getLogger(__name__)

def main():
        run()


if __name__ == "__main__":
    main()
