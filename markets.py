import os
from typing import List, Dict

import pandas as pd


from names import MARKETS


def read_symbols(file_path: str) -> List[str]:
    '''Reads stock codes and returns a list.'''
    try:
        if 'assets' not in file_path:
            file_path = os.path.join('assets', file_path)

        if 'SP500' in file_path.upper():
            df = pd.read_csv(
                file_path,
                encoding='ISO-8859-1',
                sep=None,
                engine='python',
            )
        else:
            df = pd.read_csv(file_path, encoding='ISO-8859-1', sep=',')

        df.columns = df.columns.str.strip()

        if 'Code' in df.columns:
            return df['Code'].dropna().tolist()
        if 'Symbol' in df.columns:
            return df['Symbol'].dropna().tolist()
        return df.iloc[1:, 0].dropna().tolist()
    except Exception as e:
        print(f'Error reading {file_path}: {e}')
        return []


class MarketData:
    '''Management of data for markets configured in MARKETS.'''

    def __init__(self, file_path: str = None):
        '''
        Initializes a MarketData instance from a market abbreviation or file path.

        The 'file_path' parameter can be:
        1. A **market abbreviation** (e.g., 'IBOV', 'IFIX', etc.)
        2. A **file path** (e.g., 'assets/IBOV.csv')

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
                'The \'file_path\' or \'market\' parameter must be provided!'
            )

    @classmethod
    def from_file_path(cls, file_path: str) -> str:
        '''Identifies the market by file or abbreviation.'''
        if file_path is None:
            raise ValueError(
                'A valid \'file_path\' or market abbreviation must be provided.'
            )

        market = None
        if file_path.upper() in MARKETS:
            market = file_path.upper()
        else:
            file_name = os.path.basename(file_path)
            for key, config in MARKETS.items():
                if os.path.basename(config['source_file']).lower() == file_name.lower():
                    market = key
                    break

        if market is None:
            # Creates market dynamically
            file_name = os.path.basename(file_path)
            market = file_name.replace('.csv', '').upper()
            MARKETS[market] = {
                'source_file': file_path,
            }

        return market

    @classmethod
    def list_recent_symbols(cls, market: str = None, force_update: bool = False) -> List[str]:
        '''
        Lists assets by reading directly from CSV.
        
        :param market: Market abbreviation
        :param force_update: Ignored (maintained for compatibility)
        :return: List of symbols
        '''
        if market is None:
            raise ValueError('The \'market\' parameter must be provided.')

        if market not in MARKETS:
            raise ValueError(
                f'Invalid market. Available options: {list(MARKETS.keys())}'
            )

        config = MARKETS[market]
        
        # Reads directly from CSV (no JSON cache)
        if market == 'SP500':
            symbols = read_symbols(config['source_file'])
        else:
            symbols = [s + '.SA' for s in read_symbols(config['source_file'])]
        
        return symbols

    @classmethod
    def get_sector_mapping(cls, market: str) -> Dict[str, Dict[str, str]]:
        '''
        Reads input CSV and returns symbol -> {sector, industry} mapping
        
        :param market: Market (e.g., 'IBOV', 'SP500')
        :return: Dictionary {symbol: {sector: ..., industry: ...}}
        '''
        if market not in MARKETS:
            raise ValueError(f'Invalid market. Options: {list(MARKETS.keys())}')

        config = MARKETS[market]
        file_path = config['source_file']

        try:
            if 'SP500' in market:
                df = pd.read_csv(file_path, encoding='ISO-8859-1', sep=None, engine='python')
            else:
                df = pd.read_csv(file_path, encoding='ISO-8859-1', sep=',')
            
            df.columns = df.columns.str.strip()
            
            sector_map = {}
            
            # For S&P500 - UPDATED VERSION
            if 'symbol' in df.columns:
                for _, row in df.iterrows():
                    symbol = str(row['symbol']).strip()
                    
                    # Tries different column name variations
                    sector = str(row.get('sector', row.get('GICS Sector', 'Unknown'))).strip()
                    industry = str(row.get('industry', row.get('GICS Sub-Industry', 'Unknown'))).strip()
                    
                    sector_map[symbol] = {
                        'sector': sector,
                        'industry': industry
                    }
                
                print(f'Sectors loaded for {market}: {len(sector_map)} assets')
            
            # For Brazilian files (B3)
            elif 'Code' in df.columns:
                for _, row in df.iterrows():
                    code = str(row['Code']).strip()
                    symbol = f'{code}.SA'
                    sector_map[symbol] = {
                        'sector': str(row.get('Sector', 'Unknown')).strip(),
                        'industry': str(row.get('Subsector', row.get('Segment', 'Unknown'))).strip()
                    }
                
                print(f'Sectors loaded for {market}: {len(sector_map)} assets')
            
            else:
                print(f'WARNING: Unrecognized columns in CSV. Columns found: {df.columns.tolist()}')
            
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
    '''Tests market reading'''
    data = MarketData('SP500')
    symbols_sp500 = data.list_recent_symbols('SP500')
    print(f'Total SP500 assets: {len(symbols_sp500)}')
    print(f'First 5: {symbols_sp500[:5]}')
    
    print(f'Asset update: {data.update_symbols(market="SP500", update=True)}')

    print('\nTesting sectors:')
    sectors = data.get_sector_mapping('SP500')
    print(f'Total sectors mapped: {len(sectors)}')
    for i, (symbol, info) in enumerate(sectors.items()):
        if i >= 3:
            break
        print(f'{symbol}: {info["sector"]} - {info["industry"]}')


if __name__ == '__main__':
    test()