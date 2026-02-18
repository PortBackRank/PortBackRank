# -*- coding: utf-8 -*-

'''
Global constants and configuration names for PortBackRank application.

This module defines shared constants used across the application for B3 data
fetching, file management, and market index configuration.
'''

from typing import Dict

# B3 data fetching constants
# Subdirectory name for storing historical B3 market data
HISTORICAL_SUBDIR = 'historical'
REQUEST_TIMEOUT = 1
B3_QUOTE_URL = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M'
B3_SUBDIR = 'b3'
RECENT_ASSETS_FILENAME = 'recent_assets.json'
APP_NAME = 'PortBackRank'

# File management constants
CACHE_DIR = '.cache/port_back'

# Market configuration
# Dictionary mapping market index names to their respective CSV source files
MARKETS: Dict[str, Dict[str, str]] = {
    'IBOV': {
        'source_file': 'assets/IBOV.csv',
    },
    'IFIX': {
        'source_file': 'assets/IFIX.csv',
    },
    'IBRA': {
        'source_file': 'assets/IBRA.csv',
    },
    'SMLL': {
        'source_file': 'assets/SMLL.csv',
    },
    'IBXX': {
        'source_file': 'assets/IBXX.csv',
    },
    'SP500': {
        'source_file': 'assets/SP500.csv',
    }
}

# Backwards-compatible aliases (old names used across the codebase)
# Keep these so existing imports continue to work while names were modernized above.
DIR_CACHE = CACHE_DIR
SUB_DIR_HIST = HISTORICAL_SUBDIR
URL_QUOTE = B3_QUOTE_URL
TIMEOUT = REQUEST_TIMEOUT
RECENT_ASSETS_FILE = RECENT_ASSETS_FILENAME