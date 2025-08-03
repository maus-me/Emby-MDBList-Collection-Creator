"""
Configuration Parser Module

This module handles loading and parsing of configuration settings from config files.
It provides access to configuration values through exported variables and functions.
"""
import configparser
import logging
import os
from configparser import ConfigParser
from typing import Any, List

logger = logging.getLogger(__name__)

def create_config_parser() -> configparser.ConfigParser:
    """
    Create and initialize a ConfigParser object with appropriate settings.

    Returns:
        configparser.ConfigParser: Initialized ConfigParser object
    """
    parser = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    parser.optionxform = str.lower
    return parser

def load_config(config_paths: List[str] = None) -> ConfigParser | bool:
    """
    Load configuration from specified paths or default paths.

    Args:
        config_paths: List of configuration file paths to try (in order of preference)

    Returns:
        configparser.ConfigParser: ConfigParser object with loaded configuration
    """
    if config_paths is None:
        config_paths = [os.path.join("config", "config_hidden.cfg"), os.path.join("config", "config.cfg")]

    config_parser = create_config_parser()

    # Try to read from each path in the list
    for path in config_paths:
        if os.path.exists(path):
            try:
                config_parser.read(path, encoding="utf-8")
                logger.info(f"Loaded configuration from {path}")
                return config_parser
            except Exception as e:
                logger.error(f"Error loading configuration from {path}: {e}")

    return config_parser


def get_config_value(config_parser: configparser.ConfigParser, section: str, option: str,
                     default: Any = None, value_type: str = "str") -> Any:
    """
    Get a configuration value with error handling and type conversion.

    Args:
        config_parser: ConfigParser object to get value from
        section: Configuration section name
        option: Configuration option name
        default: Default value to return if option is not found
        value_type: Type of value to return ('str', 'int', 'float', 'bool', 'list')

    Returns:
        Configuration value with appropriate type
    """
    try:
        if value_type == "str":
            return config_parser.get(section, option)
        elif value_type == "int":
            return config_parser.getint(section, option)
        elif value_type == "float":
            return config_parser.getfloat(section, option)
        elif value_type == "bool":
            return config_parser.getboolean(section, option)
        elif value_type == "list":
            value = config_parser.get(section, option)
            return [item.strip() for item in value.split(",") if item.strip()]
        else:
            logger.warning(f"Unknown value type '{value_type}' for {section}.{option}. Using string.")
            return config_parser.get(section, option)
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        logger.warning(f"Configuration {section}.{option} not found: {e}. Using default value: {default}")
        return default
    except Exception as e:
        logger.error(f"Error getting configuration {section}.{option}: {e}. Using default value: {default}")
        return default

# Load configuration
config_parser = load_config()

# Emby Configurations
EMBY_SERVER_URL = get_config_value(config_parser, "emby", "emby_server_url")
EMBY_USER_ID = get_config_value(config_parser, "emby", "emby_user_id")
EMBY_API_KEY = get_config_value(config_parser, "emby", "emby_api_key")

# MDbList Configurations
MDBLIST_API_KEY = get_config_value(config_parser, "mdblist", "mdblist_api_key")

# Admin Configurations
DOWNLOAD_MANUALLY_ADDED_LISTS = get_config_value(config_parser,"admin", "download_manually_added_lists", False, "bool")
DOWNLOAD_MY_MDBLIST_LISTS_AUTOMATICALLY = get_config_value(config_parser,"admin", "download_my_mdblist_lists_automatically", True, "bool")
UPDATE_ITEMS_SORT_NAMES_DEFAULT_VALUE = get_config_value(config_parser, "admin", "update_items_sort_names_default_value", False, "bool")
REFRESH_ITEMS = get_config_value(config_parser, "admin", "refresh_items_in_collections", False, "bool")
REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_ADDED = get_config_value(config_parser, "admin", "refresh_items_in_collections_max_days_since_added", 10, "int")
REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_PREMIERED = get_config_value(config_parser, "admin", "refresh_items_in_collections_max_days_since_premiered", 30, "int")
USE_MDB_COLLECTION_DESCRIPTION = get_config_value(config_parser, "admin", "use_mdblist_collection_description", False, "bool")

WAIT_TIME = get_config_value(config_parser, "admin", "hours_between_refresh", 24, "int")
LOG_FILE_PATH = get_config_value(config_parser, "admin", "log_file_path", "logs/logs.log", "str")
TESTING = get_config_value(config_parser, "admin", "testing", False, "bool")
ON_STARTUP = get_config_value(config_parser, "admin", "on_startup", True, "bool")

__all__ = [
    "EMBY_SERVER_URL",
    "EMBY_USER_ID",
    "EMBY_API_KEY",
    "MDBLIST_API_KEY",
    "DOWNLOAD_MANUALLY_ADDED_LISTS",
    "DOWNLOAD_MY_MDBLIST_LISTS_AUTOMATICALLY",
    "UPDATE_ITEMS_SORT_NAMES_DEFAULT_VALUE",
    "REFRESH_ITEMS",
    "REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_ADDED",
    "REFRESH_ITEMS_IN_COLLECTIONS_MAX_DAYS_SINCE_PREMIERED",
    "USE_MDB_COLLECTION_DESCRIPTION",
    "WAIT_TIME",
    "LOG_FILE_PATH",
    "TESTING",
    "ON_STARTUP"
]