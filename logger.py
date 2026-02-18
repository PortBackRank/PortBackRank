# -*- coding: utf-8 -*-
"""
Universal logger module for application-wide logging configuration.

This module provides a centralized logging setup with both file and console
output handlers, ensuring consistent log formatting across the application.
"""

import logging
from pathlib import Path
from os import sep
from names import *

# Log file path in user's home directory
LOG_FILE = str(Path.home()) + sep + APP_NAME + '.log'
# Logging level threshold
LOG_LEVEL = logging.INFO

logger = logging.getLogger(APP_NAME)
if not logger.handlers:
    # Log message format: timestamp, module name, level, and message
    str_format = '%(asctime)s - %(module)s - %(levelname)s - %(message)s'
    log_format = logging.Formatter(str_format)
    
    # File handler for persistent logging
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(log_format)
    
    # Console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
