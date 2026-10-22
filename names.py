# -*- coding: utf-8 -*-

"""
Global constants and configuration names for PortBackRank application.

This module defines shared constants used across the application for market data
fetching, file management, and backtesting configuration.
"""

from typing import Dict, Any

# Application info
APP_NAME = 'PortBackRank'

# File management and directories
DIR_ASSETS = 'assets'
DIR_RESULTS = 'results'

# Centralized base directory for user data (cache, tracking, etc.)
# Can be overridden via config or environment variable
_BASE_DIR_OVERRIDE: str = None


def set_base_dir_override(path: str) -> None:
    """Set an override for the base directory.
    
    Args:
        path: Custom base directory path.
    """
    global _BASE_DIR_OVERRIDE
    _BASE_DIR_OVERRIDE = path


def get_base_dir() -> str:
    """Get the base directory for PortBackRank user data.
    
    Returns:
        Path to the base directory (default: $HOME/PortBackRank)
    """
    import os
    from pathlib import Path
    
    # Check for programmatic override
    if _BASE_DIR_OVERRIDE:
        return _BASE_DIR_OVERRIDE
    
    # Check for environment variable override
    env_dir = os.environ.get('PORTBACKRANK_DIR')
    if env_dir:
        return env_dir
    
    # Default to $HOME/PortBackRank
    return str(Path.home() / 'PortBackRank')

# Subdirectories under base dir
DIR_CACHE = 'cache'
DIR_TRACKING = 'tracking'
DIR_HISTORICAL = 'historical'

# Legacy constants for backward compatibility (deprecated)
LEGACY_DIR_CACHE = '.cache/port_back'
LEGACY_DIR_TRACKING = 'tracking'

# Market identifiers
MARKET_SP500 = 'SP500'
MARKET_IBOV = 'IBOV'
MARKET_IFIX = 'IFIX'
MARKET_IBRA = 'IBRA'
MARKET_SMLL = 'SMLL'
MARKET_IBXX = 'IBXX'

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
STR_UNKNOWN_ID = 'unknown'
TYPE_BUY = 'BUY'
TYPE_SELL = 'SELL'

# Runner literals
DATE_FMT = '%Y-%m-%d'
KEY_BUY_PRICE = 'buy_price'
KEY_OPERATION = 'operation'
KEY_CURRENT_PRICE = 'current_price'
KEY_DAILY_VOLUME = 'daily_volume'
KEY_PORTFOLIO_DETAILS = 'portfolio_details'
KEY_SEED = 'SEED'
FILE_TRADES = 'trades.csv'
FILE_PORTFOLIO = 'portfolio.csv'
MIN_BALANCE = 2
ROUND_DIGITS = 2
DEFAULT_DECIMALS = 2

# Config keys
KEY_DECIMALS = 'decimals'

# Result key used in the backtest summary table (column label)
KEY_INTERVALO = 'intervalo'

# Backtesting configuration keys (dictionary passed to Backtesting.__init__)
CFG_RANKER_CLS = 'ranker_cls'
"""Ranker class used to build one ranker per simulation."""
CFG_RUNNER_CLS = 'runner_cls'
"""Runner class used to execute each simulation (optional, defaults to Runner)."""
CFG_CAPITAL = 'capital'
"""Initial capital for every simulation."""
CFG_INTERVAL = 'interval'
"""Simulation interval as [start_date, end_date]."""
CFG_DATA = 'data'
"""MemData instance with the market data."""
CFG_TRACE = 'trace'
"""Whether tracking files (trades.csv / portfolio.csv) are written."""
CFG_DECIMALS = 'decimals'
"""Number of decimal places applied to output values."""
CFG_N_JOBS = 'n_jobs'
"""Number of parallel jobs (-1 uses all cores)."""

# Price types accepted by the simulation (see Data.get_mega_dataframe)
PRICE_OPEN = 'O'
PRICE_HIGH = 'H'
PRICE_LOW = 'L'
PRICE_CLOSE = 'C'
PRICE_CLOSE_NEXT = 'CN'
PRICE_HL2 = 'HL2'
PRICE_HLC3 = 'HLC3'
PRICE_OHLC4 = 'OHLC4'

# MegaDataFrame index level holding the asset symbol
IDX_SYMBOL = 'Symbol'

# File extension and dtype literals used by the data layer
EXT_CSV = '.csv'
FLOAT_DTYPES = ['float64', 'float32']

# Extra calendar days loaded before the start date so indicators
# (e.g. moving averages) are already warmed up on the first simulated day
HISTORY_LOOKBACK_DAYS = 365

# Ranker name for the random baseline ranker
RANKER_RANDOM = 'RandomRanker'

# Validation functions
def validate_positive_float(value: float, name: str) -> float:
    """Validate that a float value is positive."""
    if value <= 0:
        raise ValueError(f'{name} must be > 0')
    return value


def validate_diversification(value: float) -> float:
    """Validate diversification is in (0, 1]."""
    if not 0 < value <= 1:
        raise ValueError('diversification must be in (0, 1]')
    return value


def validate_decimals(value: Any, default: int = 2) -> int:
    """Validate decimals is a non-negative integer."""
    if value is None:
        return default
    if not isinstance(value, int) or value < 0:
        raise ValueError(f'decimals must be a non-negative integer, got {value}')
    return value


# (End of module)