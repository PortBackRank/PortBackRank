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
    },
    'CUSTOM_TESTE': {
        'source_file': 'assets/custom_teste.csv',
    }
}

# Backwards-compatible aliases (old names used across the codebase)
# Keep these so existing imports continue to work while names were modernized above.
DIR_CACHE = CACHE_DIR
SUB_DIR_HIST = HISTORICAL_SUBDIR
URL_QUOTE = B3_QUOTE_URL
TIMEOUT = REQUEST_TIMEOUT
RECENT_ASSETS_FILE = RECENT_ASSETS_FILENAME

# Common directories and filenames
ASSETS_DIR = 'assets'
RESULTS_DIR = 'results'
TIMELINE_DIR = f'{RESULTS_DIR}'

# CSV and DataFrame column name constants
CSV_ENCODING = 'ISO-8859-1'
COL_DATE = 'Date'
COL_OPEN = 'Open'
COL_HIGH = 'High'
COL_LOW = 'Low'
COL_CLOSE = 'Close'
COL_VOLUME = 'Volume'

# Symbol / code column names (various formats in different CSVs)
COL_SYMBOL = 'symbol'
COL_SYMBOL_ALT = 'Symbol'
COL_CODE = 'Code'
COL_CODE_PT = 'Codigo'

# Sector / industry column names (English / Portuguese variations)
COL_SECTOR = 'sector'
COL_SECTOR_PT = 'Setor'
COL_INDUSTRY = 'industry'
COL_INDUSTRY_PT = 'Subsetor'
COL_SEGMENT_PT = 'Segmento'

# Default market identifier
DEFAULT_MARKET = 'SP500'