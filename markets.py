import os
from typing import List, Dict

import pandas as pd

from names import MARKETS, MARKET_SP500, MARKET_KEY_SOURCE_FILE, SYMBOL_SUFFIX_SA, COL_SECTOR, COL_INDUSTRY, COL_GICS_SECTOR, COL_GICS_SUBINDUSTRY, COL_CODIGO, COL_SETOR, COL_SUBSETOR, COL_SEGMENTO, COL_SYMBOL

SUB_DIR_HIST = 'historical'


def read_symbols(file_path: str) -> List[str]:
    '''
    Reads asset symbols from market CSV file.
    
    Supports multiple formats:
    - SP500: symbol column with UTF-8 encoding and pipe separator
    - Brazilian B3: Codigo column with clean ticker symbols
    
    :param file_path: Path to market CSV file
    :return: List of valid ticker symbols
    '''
    try:
        if 'assets' not in file_path:
            file_path = os.path.join('assets', file_path)

        if 'SP500' in file_path.upper() or 'IBOV' in file_path.upper():
            df = pd.read_csv(
                file_path,
                encoding='utf-8',
                sep='|',
            )
        else:
            df = pd.read_csv(file_path, encoding='ISO-8859-1', sep=',')

        df.columns = df.columns.str.strip()

        # Try to find symbol column - standardized format uses 'symbol' (lowercase)
        if 'symbol' in df.columns:
            return df['symbol'].dropna().tolist()
        # Fallback for older formats with uppercase
        if 'Symbol' in df.columns:
            return df['Symbol'].dropna().tolist()
        if 'Codigo' in df.columns:
            return df['Codigo'].dropna().tolist()
            
        return df.iloc[1:, 0].dropna().tolist()
    except Exception as e:
        print(f'Error reading {file_path}: {e}')
        return []


class MarketData:
    '''
    Market configuration and asset symbol management.
    
    Manages market identification, asset listing, and sector/industry
    classification for backtesting simulations. Supports S&P 500 and Brazilian B3 markets.
    '''

    def __init__(self, file_path: str = None):
        '''
        Initializes MarketData instance from a market ticker or file path.
        
        The 'file_path' parameter can be:
        1. A market ticker (e.g., 'IBOV', 'IFIX', etc.)
        2. A relative file path (e.g., 'assets/IBOVQuad.csv')
        
        Examples:
        market_ibov = MarketData('IBOV')
        market_sp500 = MarketData('SP500')
        market_custom = MarketData('assets/custom.csv')
        '''
        self.file_path = file_path
        print(f'Initializing MarketData with file_path: {file_path}')
        self.market = self.from_file_path(file_path)
        if self.market is None:
            raise ValueError(
                'The parameter \'file_path\' or \'market\' must be provided!'
            )

    @classmethod
    def from_file_path(cls, file_path: str) -> str:
        '''
        Identifies the market key from either a file path or a ticker symbol.
        
        Searches in the global MARKETS configuration. If no match is found,
        dynamically creates a new market entry using the filename.
        
        :param file_path: Input identifier (file path or ticker)
        :return: Market key (e.g., 'SP500')
        '''
        if file_path is None:
            raise ValueError(
                'It is necessary to provide a \'file_path\' or a valid market ticker.'
            )

        market = None
        if file_path.upper() in MARKETS:
            market = file_path.upper()
        else:
            file_name = os.path.basename(file_path)
            for key, config in MARKETS.items():
                if os.path.basename(config[MARKET_KEY_SOURCE_FILE]).lower() == file_name.lower():
                    market = key
                    break

        if market is None:
            # Create market dynamically for custom CSVs
            file_name = os.path.basename(file_path)
            market = file_name.replace('.csv', '').upper()
            MARKETS[market] = {
                MARKET_KEY_SOURCE_FILE: file_path,
            }

        return market

    @classmethod
    def list_recent_symbols(cls, market: str = None, force_update: bool = False) -> List[str]:
        '''
        Returns current list of available assets in specified market.
        
        Reads directly from market CSV file and formats symbols appropriately:
        For S&P 500: symbols returned as-is (e.g., AAPL)
        For Brazilian markets: symbols appended with .SA suffix (e.g., VALE3.SA)
        
        :param market: Market ticker ('SP500', 'IBOV', 'IFIX', etc.)
        :param force_update: Ignored; kept for API compatibility
        :return: List of ticker symbols ready for data downloads
        '''
        if market is None:
            raise ValueError('It is necessary to provide the \'market\'.')

        if market not in MARKETS:
            raise ValueError(
                f'Invalid market. Available options: {list(MARKETS.keys())}'
            )

        config = MARKETS[market]
        
        # Reads directly from CSV (without JSON cache)
        if market == MARKET_SP500:
            symbols = read_symbols(config[MARKET_KEY_SOURCE_FILE])
        else:
            symbols = [s + SYMBOL_SUFFIX_SA for s in read_symbols(config[MARKET_KEY_SOURCE_FILE])]
        
        return symbols

    @classmethod
    def get_sector_mapping(cls, market: str) -> Dict[str, Dict[str, str]]:
        '''
        Retrieves asset-to-sector mapping for diversification control.
        
        Reads market CSV and extracts sector and industry classification
        for each symbol. Supports both S&P 500 and Brazilian market formats.
        
        :param market: Market identifier ('SP500', 'IBOV', etc.)
        :return: Dictionary mapping symbols to {sector, industry} classification
        :raises ValueError: If market not configured in MARKETS
        '''
        if market not in MARKETS:
            raise ValueError(f'Invalid market. Options: {list(MARKETS.keys())}')

        config = MARKETS[market]
        file_path = config[MARKET_KEY_SOURCE_FILE]

        try:
            if MARKET_SP500 in market or 'IBOV' in market:
                df = pd.read_csv(file_path, encoding='utf-8', sep='|')
            else:
                df = pd.read_csv(file_path, encoding='ISO-8859-1', sep=',')
            
            df.columns = df.columns.str.strip()
            
            sector_map = {}
            
            # Standardized format uses 'symbol', 'sector', 'industry' (lowercase)
            if 'symbol' in df.columns:
                for _, row in df.iterrows():
                    symbol = str(row['symbol']).strip()
                    
                    # Tries different variations of column names
                    sector = str(row.get(COL_SECTOR, row.get(COL_GICS_SECTOR, 'Unknown'))).strip()
                    industry = str(row.get(COL_INDUSTRY, row.get(COL_GICS_SUBINDUSTRY, 'Unknown'))).strip()
                    
                    sector_map[symbol] = {
                        COL_SECTOR: sector,
                        COL_INDUSTRY: industry
                    }
                
                print(f'Sectors loaded for {market}: {len(sector_map)} assets')
            
            # Fallback for older format with 'Codigo' column (B3)
            elif COL_CODIGO in df.columns:
                for _, row in df.iterrows():
                    codigo = str(row[COL_CODIGO]).strip()
                    symbol = f'{codigo}{SYMBOL_SUFFIX_SA}'
                    sector_map[symbol] = {
                        COL_SECTOR: str(row.get(COL_SETOR, 'Unknown')).strip(),
                        COL_INDUSTRY: str(row.get(COL_SUBSETOR, row.get(COL_SEGMENTO, 'Unknown'))).strip()
                    }
                
                print(f'Sectors loaded for {market}: {len(sector_map)} assets')
            
            else:
                print(f'WARNING: Columns not recognized in CSV. Columns found: {df.columns.tolist()}')
            
            return sector_map

        except Exception as e:
            print(f'Error reading sectors from {file_path}: {e}')
            import traceback
            traceback.print_exc()
            return {}
      
    @classmethod
    def get_symbol_list(cls, market: str = 'SP500'):
        '''Return the list of symbols'''
        return MarketData.list_recent_symbols(market=market)
    
    @classmethod
    def update_symbols(cls, market: str, update=False):
        '''Update the list of symbols'''
        return MarketData.list_recent_symbols(market=market, force_update=update)

def list_recent_symbols(market: str, force_update: bool = False) -> List[str]:
    '''Helper function to maintain compatibility with existing calls.'''
    return MarketData.list_recent_symbols(market, force_update)


def test():
    '''Test market reading'''
    for market_name in ['SP500', 'IBOV']:
        print(f"\n--- Testing {market_name} ---")
        data = MarketData(market_name)
        symbols = data.list_recent_symbols(market_name)
        print(f'Total {market_name} assets: {len(symbols)}')
        print(f'First 5: {symbols[:5]}')
        
        print('\nTesting sectors:')
        sectors = data.get_sector_mapping(market_name)
        print(f'Total mapped sectors: {len(sectors)}')
        for i, (symbol, info) in enumerate(sectors.items()):
            if i >= 3:
                break
            print(f'{symbol}: {info[COL_SECTOR]} - {info[COL_INDUSTRY]}')


if __name__ == '__main__':
    test()