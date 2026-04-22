'''
Data Management Module

Provides Data and MemData classes for efficient management of historical
financial data and sector information. Includes support for downloading,
caching, and querying asset historical prices and classifications.
'''

import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf
from tqdm import tqdm
from files import open_dataframe, save_dataframe
from markets import MarketData
from names import SUB_DIR_HIST, COL_DATE, COL_CLOSE, COL_VOLUME, DECIMAL_PLACES
from logger import * 


class Data():
    '''
    Historical data management with downloading and caching capabilities.
    
    Handles downloading asset price history from Yahoo Finance,
    caching locally, and providing access to OHLCV data.
    '''

    subdir = SUB_DIR_HIST

    def __init__(self, market: str = 'SP500', end_date: Optional[str] = None, precision: int = 2):
        '''
        Initializes the Data manager.
        
        :param market: Market identifier for sector information (e.g., 'SP500', 'IBOV')
        :param end_date: Optional end date for data retrieval
        :param precision: Decimal places for price rounding (default: 2)
        '''
        self.end_date = end_date
        self.precision = precision

        self.market_data = MarketData(market)
        self.sector_info = {}
        mapping = self.market_data.get_sector_mapping(market)
        for s, info in mapping.items():
            self.sector_info[s] = f"{info.get('industry')} - {info.get('sector')}"

    def update_symbols(self, market: str, update: bool = False) -> List[str]:
        '''
        Refreshes the list of symbols for a market.
        
        :param market: Market identifier (e.g., 'SP500', 'IBOV')
        :param update: Force refresh from source if True
        :return: List of symbol strings
        '''
        return MarketData.list_recent_symbols(market=market, force_update=update)

    def list_symbols(self, market: str = 'SP500') -> List[str]:
        '''Returns a list of symbols via MarketData.
        
        :param market: Market identifier
        :return: List of symbols
        '''
        return MarketData.get_symbol_list(market=market)


    def download_history(self, asset: str) -> None:
        '''
        Downloads complete historical price data for an asset.
        
        Fetches maximum available history from Yahoo Finance and saves to cache.
        
        :param asset: Asset symbol (e.g., 'AAPL', 'PETR4.SA')
        '''
        asset_data = yf.Ticker(asset).history(period='max', auto_adjust=False)
        self._save_asset_data(asset, asset_data)


    def download_histories(self, assets: List[str]) -> None:
        '''
        Downloads historical data for multiple assets concurrently.
        
        Uses ThreadPoolExecutor for faster parallel downloads with progress tracking.
        
        :param assets: List of asset symbols to download
        '''
        tickers = yf.Tickers(assets)
        assets_list = list(tickers.tickers.keys())

        with tqdm(total=len(assets_list), desc='Downloading data', unit='asset') as pbar:
            def download_and_save(asset):
                self._save_asset_data(
                    asset, tickers.tickers[asset].history(period='max', auto_adjust=False))
                pbar.update(1)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(download_and_save, assets_list)

    def fetch_history(self, assets: List[str]) -> List[Dict[str, pd.DataFrame]]:
        '''
        Retrieves cached historical data for assets.
        
        :param assets: List of asset symbols to fetch
        :return: List of dicts with 'symbol' and 'data' (DataFrame) keys
        '''
        result = []
        for asset in assets:
            df = self.get_asset_data_by_name(asset)
            if isinstance(df, pd.DataFrame) and not df.empty:
                result.append({'symbol': asset, 'data': df})
        return result


    def get_history_interval(
        self,
        assets: List[str],
        start_date: str,
        end_date: str,
        column_filter: Optional[str] = 'Close',
    ) -> List[Dict[str, pd.DataFrame]]:
        '''
        Retrieves historical data for a specific date range.
        
        Filters cached data by date interval and includes volume data.

        :param assets: Asset symbols to retrieve
        :param start_date: Start date in 'YYYY-MM-DD' format
        :param end_date: End date in 'YYYY-MM-DD' format
        :param column_filter: Price column to include ('Close', 'Open', 'High', 'Low')
        :return: List of dicts with historical data in specified interval
        '''
        historical_data = self.fetch_history(assets=assets)

        if not historical_data:
            logger.info('No historical data available')
            return []

        result = []

        start_date_dt = pd.to_datetime(start_date, utc=True).tz_localize(None)
        end_date_dt = pd.to_datetime(end_date, utc=True).tz_localize(None)

        columns_to_return = [COL_VOLUME]
        if column_filter in {'Close', None}:
            columns_to_return.append(COL_CLOSE)
        elif column_filter != 'None':
            columns_to_return.append(column_filter)

        for asset in historical_data:
            symbol = asset['symbol']
            data = asset['data']

            data[COL_DATE] = pd.to_datetime(
                data[COL_DATE], utc=True).dt.tz_localize(None)

            filtered_data = data[
                (data[COL_DATE] >= start_date_dt) & (data[COL_DATE] <= end_date_dt)
            ]

            if filtered_data.empty:
                continue

            filtered_data.set_index(COL_DATE, inplace=True)
            filtered_data = filtered_data[columns_to_return]

            result.append({
                'symbol': symbol,
                'data': filtered_data
            })

        return result
    
    def _save_asset_data(self, asset: str, asset_data: pd.DataFrame) -> None:
        '''Save asset data to CSV files if data is available.
        
        :param asset: Asset symbol
        :param asset_data: DataFrame with asset data
        '''
        if asset_data.empty:
            return

        asset_data_reset = asset_data.reset_index()
        asset_data_reset['Date'] = asset_data_reset['Date'].astype(str)

        numeric_columns = asset_data_reset.select_dtypes(
            include=['float64', 'float32']).columns
        asset_data_reset[numeric_columns] = asset_data_reset[numeric_columns].round(DECIMAL_PLACES)

        save_dataframe(f'{asset}.csv', asset_data_reset, self.subdir)


    def get_asset_data(self, assets: List[str]) -> List[pd.DataFrame]:
        '''Load historical data for one or more assets.
        
        :param assets: List of asset symbols
        :return: List of DataFrames with historical data
        '''
        assets_data = []
        for asset in assets:
            asset_data = self.get_asset_data_by_name(asset)
            if asset_data is not None and not asset_data.empty:
                assets_data.append(asset_data)
        return assets_data

    def load_dataframe(self, file_name: str) -> Optional[pd.DataFrame]:
        '''Load data from a CSV file.
        
        :param file_name: Name of the file
        :return: DataFrame or None if not found
        '''
        
        return open_dataframe(file_name, self.subdir)

    def get_asset_data_by_name(self, asset: str) -> Optional[pd.DataFrame]:
        '''Get historical data for a specific asset.
        If the CSV is missing or empty, attempt to download and load again.
        
        :param asset: Asset symbol
        :return: DataFrame with historical data or None
        '''
        file_name = f'{asset}.csv'
        df = None
        
        try:
            df = self.load_dataframe(file_name)
        except FileNotFoundError:
            df = None

        # open_dataframe may return None instead of raising
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print(f'File {file_name} not found or empty. Downloading data for {asset}.')
            self.download_history(asset)
            try:
                df = self.load_dataframe(file_name)
            except FileNotFoundError:
                df = None
        
        return df


class MemData:
    '''
    In-memory data management with integrated sector information.
    
    Combines historical price data (via Data class) with sector/industry
    classifications (via MarketData class). Provides unified interface for
    asset ranking strategies that need both historical prices and fundamentals.
    
    Responsibilities:
    - Load and cache historical data in memory
    - Manage sector/industry classification mappings
    - Provide O(1) access to both data types
    '''

    def __init__(self, interval: List[str], market_identifier: str = 'SP500'):
        '''Initialize in-memory data structures.
        
        Loads asset list, sector information, and historical price data
        for the specified market and date interval.
        
        :param interval: [start_date, end_date] in 'YYYY-MM-DD' format
        :param market_identifier: Market ticker or custom file path
        '''
        self.history_data: Dict[str, pd.DataFrame] = {}
        self.sector_info: Dict[str, str] = {}  # symbol -> 'industry - sector'
        self.market_identifier = market_identifier

        # Initialize Data instance (only for historical data)
        self.data = Data()

        # Initialize MarketData (for symbols and sectors)
        self.market_data = MarketData(market_identifier)
        
        # Load asset list from MarketData
        self.assets = self.market_data.list_recent_symbols(
            self.market_data.market, force_update=False
        )
        
        # Load sector information from MarketData
        self.load_sector_info()

        logger.info(f'Assets available: {len(self.assets)}')
        logger.info(f'Sectors in cache: {len(self.sector_info)}')
        if self.assets:
            logger.info(f'First 5 assets: {self.assets[:5]}')
        
        if not self.assets:
            logger.info(f'No assets found for {market_identifier}.')
            return

        # Load historical data using Data class
        start_date, end_date = interval
        self.load(start_date, end_date)

    def load_sector_info(self) -> None:
        '''
        Loads sector and industry information for all assets.
        
        Reads from CSV files via MarketData and stores as 'industry - sector'
        concatenated string for quick lookup during backtesting.
        '''
        try:
            # MarketData.get_sector_mapping returns: {symbol: {sector: ..., industry: ...}}
            sector_mapping = self.market_data.get_sector_mapping(self.market_data.market)
            
            for symbol, info in sector_mapping.items():
                industry = info.get('industry', 'Unknown')
                sector = info.get('sector', 'Unknown')
                # Concatenate industry and sector
                self.sector_info[symbol] = f'{industry} - {sector}'
            
            logger.info(f'Loaded sector info for {len(self.sector_info)} symbols')
        except Exception as e:
            logger.info(f'Error loading sector info: {e}')
            import traceback
            traceback.print_exc()
            self.sector_info = {}
    
    def load(self, start_date: str, end_date: str) -> None:
        '''
        Loads historical price data into memory.
        
        Retrieves historical data via Data class and indexes by symbol.
        Uses get_history_interval for date-range filtering.

        :param start_date: Start date in 'YYYY-MM-DD' format
        :param end_date: End date in 'YYYY-MM-DD' format
        '''
        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')

        logger.info(f'Loading historical data from {start_date} to {end_date}...')
        
        # Use Data class to get historical data
        historical_data = self.data.get_history_interval(
            assets=self.assets, start_date=start_date, end_date=end_date
        )

        for asset_data in historical_data:
            asset = asset_data['symbol']
            self.history_data[asset] = asset_data['data']

        logger.info(f'Historical data loaded: {len(self.history_data)} assets')

    def get_assets(self) -> List[str]:
        '''
        Returns all loaded asset symbols.
        
        :return: List of asset ticker symbols
        '''
        return list(self.assets)

    def get_all_history(self) -> Dict[str, pd.DataFrame]:
        '''
        Returns all loaded historical price data.
        
        :return: Dict mapping symbols to DataFrames with OHLCV data
        '''
        return self.history_data
    
    def get_sector(self, symbol: str) -> str:
        '''
        Retrieves sector information for a specific asset.
        
        :param symbol: Asset symbol
        :return: String in 'industry - sector' format, or 'Unknown - Unknown'
        '''
        return self.sector_info.get(symbol, 'Unknown - Unknown')
    
    def get_all_sectors(self) -> Dict[str, str]:
        '''
        Returns all sector mappings.
        
        :return: Dict mapping symbols to 'industry - sector' strings
        '''
        return self.sector_info.copy()

def test():
    '''Test function'''
    try:
        # Choose an asset to test (can be any available)
        test_asset = 'AAPL'  # or 'ACGL' if available
        
        print(f'Testing rounding for asset: {test_asset}')
        
        # Load the DataFrame for the asset
        df = Data.get_asset_data_by_name(test_asset)
        
        if df is not None and not df.empty:
            print(f'\nData loaded successfully!')
            print(f'Total de linhas: {len(df)}')
            
            # Show first rows of price columns
            price_cols = ['Open', 'High', 'Low', 'Close']
            existing_cols = [col for col in price_cols if col in df.columns]
            
            print(f'\nFirst 5 rows of price columns:')
            print(df[existing_cols].head())
            
            # Check if values have at most 2 decimal places
            print(f'\nVerifying rounding...')
            for col in existing_cols:
                max_decimals = df[col].apply(lambda x: len(str(x).split('.')[-1]) if '.' in str(x) else 0).max()
                print(f'  {col}: maximum {max_decimals} decimal places')
                if max_decimals <= 2:
                    print(f'Correctly rounded')
                else:
                    print(f'ERROR: not rounded to 2 decimal places')
        else:
            print(f'Error: unable to load data for {test_asset}')
            
    except Exception as e:
        print(f'Error in test: {e}')
        import traceback
        traceback.print_exc()
    
    try:
        df_assets = pd.read_csv('assets/SP500.csv')
        
        if 'symbol' in df_assets.columns:
            assets = df_assets['symbol'].tolist()
        else:
            assets = df_assets.iloc[:,0].tolist()

        assets = [asset for asset in assets if pd.notna(asset) and asset != '']
            
        print(f'Total assets loaded from CSV: {len(assets)}')
        print(f'First 5 assets: {assets[:5]}')
        print(f'Sector-Industry of 5 assets{assets[:5]}')
    except FileNotFoundError:
        print('File \'assets.csv\' not found!')
        print('Using default SP500 market list as fallback')
        assets = Data.list_symbols(market='SP500')
    except Exception as e:
        print(f'Error reading CSV: {e}')
        print('Using default SP500 market list as fallback')
        assets = Data.list_symbols(market='SP500')


def test_sp500():
    '''Test function'''
    print('--------------Listing assets----------------')
    market_data = MarketData('SP500')
    assets = market_data.list_recent_symbols(market_data.market, force_update=True)
    print(assets)

    print('--------------Testing sectors via MemData----------------')
    # Data no longer has get_sector - use MemData
    interval = ['2024-01-10', '2024-11-10']
    mem_data = MemData(interval, market_identifier='SP500')
    print(mem_data.get_sector('AAPL')) 
    print(len(mem_data.get_all_sectors()))


def test_mem_data():
    '''Test function'''
    interval = ['2024-01-10', '2024-11-10']
    mem_data = MemData(interval, market_identifier='SP500')

    print('All historical data:')
    all_info = mem_data.get_all_history()

    print('History of asset not in sp500:')
    print(all_info.get('EQPA3.SA'))

    print('History of asset in ibov:')
    print(all_info.get('PETR4.SA'))

    print('History of asset in sp500:')
    print(all_info.get('AAPL'))

    print('History of asset in ibra:')
    print(all_info.get('B3SA3.SA'))

    print('Sectors of existing asset')
    print(mem_data.get_sector('MSFT'))

    print('Sectors of all assets')
    sectors = mem_data.get_all_sectors()
    print(sectors)

    print('Asset information ---- uncomment:')
    # print(mem_data.get_assets())

    print('All information ----- uncomment:')
    # print(mem_data.get_all_info())


if __name__ == '__main__':
    test()
