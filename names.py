# -*- coding: utf-8 -*-

'''
Global constants and configuration names for PortBackRank application.

This module defines shared constants used across the application for market data
fetching, file management, and backtesting configuration.
'''

from typing import Dict

# Application info
APP_NAME = 'PortBackRank'

# File management and directories
DIR_ASSETS = 'assets'
DIR_RESULTS = 'results'
DIR_TRACKING = 'tracking'
DIR_CACHE = '.cache/port_back'
DIR_HISTORICAL = 'historical'

# Market identifiers
MARKET_SP500 = 'SP500'
MARKET_IBOV = 'IBOV'
MARKET_IFIX = 'IFIX'
MARKET_IBRA = 'IBRA'
MARKET_SMLL = 'SMLL'
MARKET_IBXX = 'IBXX'
MARKET_CUSTOM_TESTE = 'CUSTOM_TESTE'

# Market configuration
MARKET_KEY_SOURCE_FILE = 'source_file'
SYMBOL_SUFFIX_SA = '.SA'

# Dictionary mapping market index names to their respective CSV source files
MARKETS: Dict[str, Dict[str, str]] = {
    MARKET_IBOV: {MARKET_KEY_SOURCE_FILE: f'{DIR_ASSETS}/IBOV.csv'},
    MARKET_IFIX: {MARKET_KEY_SOURCE_FILE: f'{DIR_ASSETS}/IFIX.csv'},
    MARKET_IBRA: {MARKET_KEY_SOURCE_FILE: f'{DIR_ASSETS}/IBRA.csv'},
    MARKET_SMLL: {MARKET_KEY_SOURCE_FILE: f'{DIR_ASSETS}/SMLL.csv'},
    MARKET_IBXX: {MARKET_KEY_SOURCE_FILE: f'{DIR_ASSETS}/IBXX.csv'},
    MARKET_SP500: {MARKET_KEY_SOURCE_FILE: f'{DIR_ASSETS}/SP500.csv'},
    MARKET_CUSTOM_TESTE: {MARKET_KEY_SOURCE_FILE: f'{DIR_ASSETS}/custom_teste.csv'}
}

# Default settings
DEFAULT_MARKET = MARKET_SP500
DEFAULT_CAPITAL = 10000
DEFAULT_INTERVAL = ['2024-01-01', '2024-12-31']

# CSV and data settings
SEP_PIPE = '|'
SEP_COMMA = ','
ENCODING_UTF8 = 'utf-8'
ENCODING_ISO = 'ISO-8859-1'

# CSV and DataFrame column names
COL_DATE = 'Date'
COL_OPEN = 'Open'
COL_HIGH = 'High'
COL_LOW = 'Low'
COL_CLOSE = 'Close'
COL_VOLUME = 'Volume'

# Symbol / code column names
COL_SYMBOL = 'symbol'
COL_SYMBOL_ALT = 'Symbol'
COL_CODE = 'Code'
COL_CODIGO = 'Codigo'

# Sector / industry column names
COL_SECTOR = 'sector'
COL_SECTOR_PT = 'Setor'
COL_INDUSTRY = 'industry'
COL_INDUSTRY_PT = 'Subsetor'
COL_SEGMENT_PT = 'Segmento'

# Legacy column names for compatibility
COL_GICS_SECTOR = 'GICS Sector'
COL_GICS_SUBINDUSTRY = 'GICS Sub-Industry'
COL_SETOR = 'Setor'
COL_SUBSETOR = 'Subsetor'
COL_SEGMENTO = 'Segmento'

# Config and Result keys (JSON/Dict keys)
KEY_ID = 'id'
KEY_INTERVAL = 'interval'
KEY_CAPITAL = 'capital'
KEY_RANKER = 'ranker'
KEY_RANKER_PARAMS = 'ranker-params'
KEY_PROFIT = 'profit'
KEY_LOSS = 'loss'
KEY_DIVERSIFICATION = 'diversification'
KEY_VOLUME = 'volume'
KEY_WINDOW = 'window'
KEY_CAIXA_FINAL = 'caixa_final'
KEY_PORTFOLIO_VALUE = 'portfolio_value'
KEY_RETORNO_TOTAL = 'retorno_total'
KEY_BALANCE = 'balance'
KEY_FINAL_TOTAL_VALUE = 'final_total_value'
KEY_ASSET = 'asset'
KEY_QUANTITY = 'quantity'
KEY_UNIT_VALUE = 'unit_value'
KEY_TOTAL_ASSET_VALUE = 'total_asset_value'
KEY_TYPE = 'type'
KEY_PRICE = 'price'
KEY_COST = 'cost'
KEY_PROFIT_LOSS = 'profit_loss'
KEY_ORIGIN_DATE = 'origin_date'

# Download modes
MODE_ALL = 'all'
MODE_MISSING = 'missing'
MODE_NONE = 'none'

# Ranker names
RANKER_MA = 'MARanker'
RANKER_RSI = 'RSIRanker'

# yfinance parameters
YF_PERIOD_MAX = 'max'
YF_AUTO_ADJUST = False

# Placeholders and status
STR_UNKNOWN = 'Unknown'
STR_UNKNOWN_FULL = 'Unknown - Unknown'
TYPE_BUY = 'BUY'
TYPE_SELL = 'SELL'

# (End of module)