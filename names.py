'''
Docstring for names
'''

from typing import Dict

# b3.py const
SUB_DIR_HIST = 'historical'
TIMEOUT = 1
URL_QUOTE = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M'
SUB_DIR_B3 = 'b3'
RECENT_ASSETS_FILE = 'recent_assets.json'
APP_NAME = 'PortBackRank'

# files.py const
DIR_CACHE = '.cache/port_back'

# markets.py dict
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
