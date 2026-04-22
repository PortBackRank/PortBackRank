'''
Constants and Configuration

Central location for all application constants including market definitions,
directory paths, data URLs, and file naming conventions.
Imported by all modules for configuration consistency.
'''

from typing import Dict

# Data download constants
SUB_DIR_HIST = 'historical'
TIMEOUT = 1
URL_QUOTE = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M'
SUB_DIR_B3 = 'b3'
RECENT_ASSETS_FILE = 'recent_assets.json'
APP_NAME = 'PortBackRank'

# File management constants
DIR_CACHE = '.cache/port_back'

# Market configuration
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
}

# Market constants
MARKET_SP500 = 'SP500'
MARKET_IBOV = 'IBOV'
MARKET_IFIX = 'IFIX'
MARKET_IBRA = 'IBRA'
MARKET_SMLL = 'SMLL'
MARKET_IBXX = 'IBXX'
MARKET_KEY_SOURCE_FILE = 'source_file'
SYMBOL_SUFFIX_SA = '.SA'

# Data columns (OHLCV format)
COL_DATE = 'Date'
COL_OPEN = 'Open'
COL_HIGH = 'High'
COL_LOW = 'Low'
COL_CLOSE = 'Close'
COL_VOLUME = 'Volume'

OHLCV_COLUMNS = [COL_OPEN, COL_HIGH, COL_LOW, COL_CLOSE, COL_VOLUME]
PRICE_COLUMNS = [COL_OPEN, COL_HIGH, COL_LOW, COL_CLOSE]

# Market data columns
COL_SYMBOL = 'Symbol'
COL_CODIGO = 'Codigo'
COL_SECTOR = 'sector'
COL_INDUSTRY = 'industry'
COL_GICS_SECTOR = 'GICS Sector'
COL_GICS_SUBINDUSTRY = 'GICS Sub-Industry'
COL_SETOR = 'Setor'  # Brazilian B3 field
COL_SUBSETOR = 'Subsetor'  # Brazilian B3 field
COL_SEGMENTO = 'Segmento'  # Brazilian B3 alternative field

# Portfolio tracking columns
COL_BALANCE = 'balance'
COL_PORTFOLIO = 'portfolio'
COL_QUANTITY = 'quantity'
COL_PURCHASE_PRICE = 'purchase_price'
COL_SYMBOL_FIELD = 'symbol'
COL_ENTRY_DATE = 'entry_date'
COL_SECTOR_INFO = 'sector_info'

# Timeline and logging columns
COL_ALLOCATION = 'allocation'
COL_PROFIT_TARGET = 'profit_target'
COL_LOSS_TARGET = 'loss_target'
COL_QUANTITY_BOUGHT = 'quantity_bought'
COL_PRICE_FILLED = 'price_filled'

# Result file naming prefixes
PREFIX_TIMELINE = 'timeline'
PREFIX_BUY_LOG = 'buy_log'
PREFIX_SELL_LOG = 'sell_log'
SUBDIR_SELL_BUY_LOGS = 'sell_buy_logs'

# File extensions
EXT_CSV = '.csv'
EXT_JSON = '.json'
EXT_PNG = '.png'

# Directory paths
DIR_ASSETS = 'assets'
DIR_RESULTS = 'results'
DIR_AUXILLARY = 'auxi'

# Numeric precision
DECIMAL_PLACES = 2
PERCENT_MULTIPLIER = 100

# Default trading parameters
DEFAULT_PROFIT_TARGET = 0.1  # 10% gain
DEFAULT_LOSS_STOP = -0.05  # 5% loss
DEFAULT_DIVERSIFICATION = 0.2  # 20% max per sector
DEFAULT_SHORT_WINDOW = 9  # Short MA window
DEFAULT_LONG_WINDOW = 21  # Long MA window
DEFAULT_RSI_PERIOD = 14  # RSI technical period
DEFAULT_RSI_OVERSOLD = 30  # Oversold threshold
DEFAULT_RSI_OVERBOUGHT = 70  # Overbought threshold

# Performance metrics
METRIC_FINAL_BALANCE = 'final_balance'
METRIC_TOTAL_RETURN = 'total_return'
METRIC_PORTFOLIO_VALUE = 'portfolio_value'
METRIC_PROFIT_TARGET = 'profit'
METRIC_LOSS_STOP = 'loss'
METRIC_DIVERSIFICATION = 'diversification'
