"""
Configuration Parser Module

This module handles loading and parsing of configuration settings from the collections config files.
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
        config_paths = [os.path.join("config", "collections_hidden.cfg"), os.path.join("config", "collections.cfg")]

    collections_parser = create_config_parser()

    # Try to read from each path in the list
    for path in config_paths:
        if os.path.exists(path):
            try:
                collections_parser.read(path, encoding="utf-8")
                logger.info(f"Loaded configuration from {path}")
                return collections_parser
            except Exception as e:
                logger.error(f"Error loading configuration from {path}: {e}")

    return collections_parser

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


