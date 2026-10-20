# -*- coding: utf-8 -*-
"""
Logging configuration module for PortBackRank.

This module provides functions to configure application-wide logging.
Individual modules should use `logging.getLogger(__name__)` to get their logger.
"""

import logging
from pathlib import Path
from names import APP_NAME


def get_log_file_path() -> str:
    """Get the default log file path in user's home directory."""
    return str(Path.home() / f'{APP_NAME}.log')


def configure_logging(debug: bool = False, log_file: str = None) -> None:
    """Configure application-wide logging.
    
    This should be called once at application startup (in main.py).
    
    Args:
        debug: If True, set log level to DEBUG. Otherwise WARNING (to avoid info spam).
        log_file: Optional custom log file path. Defaults to $HOME/PortBackRank.log
    """
    # Set root logger level
    level = logging.DEBUG if debug else logging.WARNING
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Log message format: timestamp, module name, level, and message
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler for persistent logging
    if log_file is None:
        log_file = get_log_file_path()
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(log_format)
    file_handler.setLevel(level)
    root_logger.addHandler(file_handler)
    
    # Console handler - only show WARNING and above unless debug
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    console_handler.setLevel(level)
    root_logger.addHandler(console_handler)
    
    # Suppress noisy third-party loggers unless in debug mode
    if not debug:
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('yfinance').setLevel(logging.WARNING)
        logging.getLogger('matplotlib').setLevel(logging.WARNING)
        logging.getLogger('PIL').setLevel(logging.WARNING)


def get_logger(name: str = None) -> logging.Logger:
    """Get a logger instance for a module.
    
    Args:
        name: Logger name. If None, uses the caller's module name.
              Typically called as `get_logger(__name__)` in each module.
    
    Returns:
        Configured logger instance.
    """
    return logging.getLogger(name)