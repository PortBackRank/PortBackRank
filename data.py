# -*- coding: utf-8 -*-

'''
Data management module for downloading and caching historical financial data.
'''

import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf
from tqdm import tqdm
from utils import open_dataframe, save_dataframe
from markets import MarketData
from names import (
    DIR_HISTORICAL, MARKET_SP500, MARKET_IBOV, MARKET_IBRA,
    COL_DATE, COL_CLOSE, COL_VOLUME, COL_OPEN, COL_HIGH, COL_LOW,
    YF_PERIOD_MAX, YF_AUTO_ADJUST, STR_UNKNOWN, STR_UNKNOWN_FULL
)
from logger import * 


class Data():
    '''Data management class for downloading and caching historical market data.
    
    Handles downloading of historical data from external sources and managing
    local cache storage via CSV files.
    '''

    subdir = DIR_HISTORICAL

    def __init__(self, market: str = MARKET_SP500, end_date: Optional[str] = None, precision: int = 2):
        '''Initialize Data manager with market configuration.
        
        Args:
            market: Market identifier (e.g., MARKET_SP500, MARKET_IBOV). Defaults to MARKET_SP500.
            end_date: Optional end date for data retrieval (YYYY-MM-DD format).
            precision: Number of decimal places for numeric data. Defaults to 2.
        '''
        self.end_date = end_date
        self.precision = precision

        self.market_data = MarketData(market)
        self.sector_info = {}
        mapping = self.market_data.get_sector_mapping(self.market_data.market)
        for symbol, info in mapping.items():
            self.sector_info[symbol] = f"{info.get('industry')} - {info.get('sector')}"

    def update_symbols(self, market: str, update: bool = False) -> List[str]:
        '''Update the list of symbols from the market data source.
        
        Args:
            market: Market identifier (e.g., 'SP500', 'IBOV').
            update: Force update from remote source if True.
            
        Returns:
            List of current market symbols.
        '''
        return MarketData.list_recent_symbols(market=market, force_update=update)

    def list_symbols(self, market: str = MARKET_SP500) -> List[str]:
        '''Retrieve the list of symbols for a specific market.
        
        Args:
            market: Market identifier (e.g., MARKET_SP500, MARKET_IBOV). Defaults to MARKET_SP500.
            
        Returns:
            List of market symbols.
        '''
        return MarketData.get_symbol_list(market=market)

    def download_history(self, asset: str) -> None:
        '''Download historical price data for a single asset.
        
        Args:
            asset: Asset symbol (ticker) to download.
        '''
        asset_data = yf.Ticker(asset).history(period=YF_PERIOD_MAX, auto_adjust=YF_AUTO_ADJUST)
        self._save_asset_data(asset, asset_data)

    def download_histories(self, assets: List[str]) -> None:
        '''Download historical data for multiple assets concurrently.
        
        Downloads data in parallel using ThreadPoolExecutor for improved performance.
        Progress is tracked with a progress bar.
        
        Args:
            assets: List of asset symbols to download.
        '''
        tickers = yf.Tickers(assets)
        assets_list = list(tickers.tickers.keys())

        with tqdm(total=len(assets_list), desc='Downloading data', unit='asset') as pbar:
            def download_and_save(asset):
                self._save_asset_data(
                    asset, tickers.tickers[asset].history(period=YF_PERIOD_MAX, auto_adjust=YF_AUTO_ADJUST))
                pbar.update(1)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(download_and_save, assets_list)

    def fetch_history(self, assets: List[str]) -> List[Dict[str, pd.DataFrame]]:
        '''Fetch historical data for given assets from cache.
        
        Retrieves cached data for valid, non-empty DataFrames only.
        
        Args:
            assets: List of asset symbols.
            
        Returns:
            List of dictionaries containing 'symbol' and 'data' (DataFrame) keys.
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
        column_filter: Optional[str] = COL_CLOSE,
        price_type: str = 'C',
    ) -> List[Dict[str, pd.DataFrame]]:
        '''Retrieve historical data filtered by date range and columns.

        Fetches historical data and filters by specified time interval.
        Automatically includes 'Volume' and filtered price column(s).

        Args:
            assets: List of asset symbols.
            start_date: Start of date range (YYYY-MM-DD format).
            end_date: End of date range (YYYY-MM-DD format).
            column_filter: Price column to include (COL_CLOSE, custom column, or 'None').
                Defaults to COL_CLOSE.
            price_type: The type of price to simulate logic with. Defaults to 'C'.
            
        Returns:
            List of dictionaries with 'symbol' and 'data' (filtered DataFrame) keys.
            Empty list if no data matches criteria.
        '''
        historical_data = self.fetch_history(assets=assets)

        if not historical_data:
            logger.info('Empty historical data')
            return []

        result = []

        start_date_dt = pd.to_datetime(start_date, utc=True).tz_localize(None)
        end_date_dt = pd.to_datetime(end_date, utc=True).tz_localize(None)

        columns_to_return = [COL_VOLUME]
        if column_filter in {COL_CLOSE, None}:
            columns_to_return.append(COL_CLOSE)
        elif column_filter != 'None':
            columns_to_return.append(column_filter)

        for asset in historical_data:
            symbol = asset['symbol']
            data = asset['data'].copy()

            data[COL_DATE] = pd.to_datetime(
                data[COL_DATE], utc=True).dt.tz_localize(None)

            filtered_data = data[
                (data['Date'] >= start_date_dt) & (data['Date'] <= end_date_dt)
            ]

            if filtered_data.empty:
                continue

            filtered_data.set_index(COL_DATE, inplace=True)
            
            if price_type == 'O':
                filtered_data[COL_CLOSE] = filtered_data[COL_OPEN]
            elif price_type == 'H':
                filtered_data[COL_CLOSE] = filtered_data[COL_HIGH]
            elif price_type == 'L':
                filtered_data[COL_CLOSE] = filtered_data[COL_LOW]
            elif price_type == 'CN':
                filtered_data[COL_CLOSE] = filtered_data[COL_CLOSE].shift(-1)
            elif price_type == 'HL2':
                filtered_data[COL_CLOSE] = (filtered_data[COL_HIGH] + filtered_data[COL_LOW]) / 2.0
            elif price_type == 'HLC3':
                filtered_data[COL_CLOSE] = (filtered_data[COL_HIGH] + filtered_data[COL_LOW] + filtered_data[COL_CLOSE]) / 3.0
            elif price_type == 'OHLC4':
                filtered_data[COL_CLOSE] = (filtered_data[COL_OPEN] + filtered_data[COL_HIGH] + filtered_data[COL_LOW] + filtered_data[COL_CLOSE]) / 4.0

            filtered_data = filtered_data[columns_to_return]

            result.append({
                'symbol': symbol,
                'data': filtered_data
            })

        return result
    
    def _save_asset_data(self, asset: str, asset_data: pd.DataFrame) -> None:
        '''Save asset data to CSV file with proper formatting.
        
        Rounds numeric columns to specified precision and converts dates to strings
        before persisting to CSV. Skips empty DataFrames.
        
        Args:
            asset: Asset symbol (used as filename).
            asset_data: DataFrame containing asset historical data.
        '''
        if asset_data.empty:
            return

        asset_data_reset = asset_data.reset_index()
        asset_data_reset[COL_DATE] = asset_data_reset[COL_DATE].astype(str)

        numeric_columns = asset_data_reset.select_dtypes(
            include=['float64', 'float32']).columns
        asset_data_reset[numeric_columns] = asset_data_reset[numeric_columns].round(self.precision)

        save_dataframe(f'{asset}.csv', asset_data_reset, self.subdir)

    def get_asset_data(self, assets: List[str]) -> List[pd.DataFrame]:
        '''Load historical data for one or more assets.
        
        Args:
            assets: List of asset symbols.
            
        Returns:
            List of DataFrames with historical data (empty frames excluded).
        '''
        assets_data = []
        for asset in assets:
            asset_data = self.get_asset_data_by_name(asset)
            if asset_data is not None and not asset_data.empty:
                assets_data.append(asset_data)
        return assets_data

    def load_dataframe(self, file_name: str) -> Optional[pd.DataFrame]:
        '''Load DataFrame from CSV file.
        
        Args:
            file_name: Name of the CSV file to load.
            
        Returns:
            DataFrame if file exists and is valid, None otherwise.
        '''
        return open_dataframe(file_name, self.subdir)

    def get_asset_data_by_name(self, asset: str) -> Optional[pd.DataFrame]:
        '''Retrieve historical data for a specific asset with automatic fallback.
        
        Attempts to load cached data. If not found or empty, automatically downloads
        the data and retries loading. Logs warnings for missing files.
        
        Args:
            asset: Asset symbol.
            
        Returns:
            DataFrame with historical data, or None if unable to retrieve.
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
    '''In-memory data management with historical and sector information.
    
    Responsibilities:
    - Load and cache historical data in memory (via Data class)
    - Manage sector and industry classification data (via MarketData class)
    - Provide unified access to consolidated market data
    - Pre-index data by date for optimized lookups
    '''

    def __init__(self, interval: List[str], market_identifier: str = MARKET_SP500, price_type: str = 'C'):
        '''Initialize MemData with historical data and sector information.
        
        Args:
            interval: List containing [start_date, end_date] in YYYY-MM-DD format.
            market_identifier: Market identifier (e.g., 'SP500', 'IBOV'). 
                Defaults to MARKET_SP500.
            price_type: The type of price to simulate logic with. Defaults to 'C'.
        '''
        self.history_data: Dict[str, pd.DataFrame] = {}
        self.sector_info: Dict[str, str] = {}  # symbol -> 'industry - sector'
        self.market_identifier = market_identifier
        self.price_type = price_type

        # Initialize Data instance for historical data retrieval
        self.data = Data()

        # Initialize MarketData for symbol lists and sector classification
        self.market_data = MarketData(market_identifier)
        
        # Load current list of assets for the market
        self.assets = self.market_data.list_recent_symbols(
            self.market_data.market, force_update=False
        )
        
        # Load sector classification information
        self.load_sector_info()

        logger.info(f'Assets available: {len(self.assets)}')
        logger.info(f'Sectors in cache: {len(self.sector_info)}')
        if self.assets:
            logger.info(f'First 5 assets: {self.assets[:5]}')
        
        if not self.assets:
            logger.info(f'No assets found for {market_identifier}.')
            return

        # Load historical data for the specified interval
        start_date, end_date = interval
        self.load(start_date, end_date)
        
        # Pre-index historical data by date for faster access
        self._history_by_date = {}
        self._generate_date_index() 

    def load_sector_info(self) -> None:
        '''Load sector and industry classifications from MarketData source.
        
        Combines industry and sector information into a single string format
        ('industry - sector') for each symbol. Logs errors gracefully.
        '''
        try:
            # MarketData.get_sector_mapping returns: {symbol: {sector: ..., industry: ...}}
            sector_mapping = self.market_data.get_sector_mapping(self.market_data.market)
            
            for symbol, info in sector_mapping.items():
                industry = info.get('industry', STR_UNKNOWN)
                sector = info.get('sector', STR_UNKNOWN)
                # Store combined industry and sector designation
                self.sector_info[symbol] = f'{industry} - {sector}'
            
            logger.info(f'Loaded sector information for {len(self.sector_info)} symbols')
        except Exception as e:
            logger.info(f'Error loading sector information: {e}')
            import traceback
            traceback.print_exc()
            self.sector_info = {}
    
    def load(self, start_date: str, end_date: str) -> None:
        '''Load historical market data into memory for specified date range.

        Retrieves historical price data for all assets and stores in memory.
        If end_date is None, uses current date. Logs loaded asset count.

        Args:
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format (None defaults to today).
        '''
        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')

        logger.info(f'Loading historical data from {start_date} to {end_date}...')
        
        # Use Data class to retrieve historical data for date interval
        historical_data = self.data.get_history_interval(
            assets=self.assets, start_date=start_date, end_date=end_date, price_type=self.price_type
        )

        for asset_data in historical_data:
            asset = asset_data['symbol']
            self.history_data[asset] = asset_data['data']

        logger.info(f'Historical data loaded: {len(self.history_data)} assets')

    def get_assets(self) -> List[str]:
        '''Get the list of all loaded assets.

        Returns:
            List of asset symbols currently in memory.
        '''
        return list(self.assets)

    def get_all_history(self) -> Dict[str, pd.DataFrame]:
        '''Retrieve all loaded historical data.

        Returns:
            Dictionary mapping asset symbols to their historical price DataFrames.
        '''
        return self.history_data
    
    def _generate_date_index(self) -> None:
        '''Pre-index historical data by date for optimized lookups.
        
        Creates nested dictionary structure mapping symbols to date-based row access.
        Improves performance for repeated date-range queries.
        '''
        for symbol, df in self.get_all_history().items():
            df_copy = df.copy()
            # Ensure index is datetime, then generate string key for lookup
            df_copy['date_str'] = df_copy.index.strftime('%Y-%m-%d')
            self._history_by_date[symbol] = {
                date_key: row for date_key, row in df_copy.set_index('date_str').iterrows()
            }

    def get_history_by_date(self) -> Dict[str, Dict[str, pd.Series]]:
        '''Retrieve date-indexed historical data structure.
        
        Returns:
            Nested dictionary: symbol -> (date_string -> Series data)
        '''
        return self._history_by_date
    
    def get_sector(self, symbol: str) -> str:
        '''Get sector classification for a specific asset.
        
        Args:
            symbol: Asset symbol.
            
        Returns:
            Sector information in 'industry - sector' format,
            or 'Unknown - Unknown' if not found.
        '''
        return self.sector_info.get(symbol, STR_UNKNOWN_FULL)
    
    def get_all_sectors(self) -> Dict[str, str]:
        '''Get sector classifications for all loaded assets.
        
        Returns:
            Dictionary mapping symbols to 'industry - sector' strings.
        '''
        return self.sector_info.copy()


def test():
    '''Basic functionality test for data loading and rounding verification.'''
    try:
        # Choose an asset to test (works with any valid symbol)
        test_asset = 'AAPL'
        
        print(f'Testing rounding for asset: {test_asset}')
        
        # Load DataFrame using Data helper method
        data_instance = Data()
        df = data_instance.get_asset_data_by_name(test_asset)
        
        if df is not None and not df.empty:
            print('\nData loaded successfully!')
            print(f'Number of rows: {len(df)}')
            
            # Display price-related columns
            existing_cols = [c for c in [COL_OPEN, COL_HIGH, COL_LOW, COL_CLOSE, COL_VOLUME] if c in df.columns]
            print(df[existing_cols].head())
            
            # Verify rounding to 2 decimal places
            print('\nVerifying rounding precision...')
            for col in existing_cols:
                if df[col].dtype in ['float64', 'float32']:
                    max_decimals = df[col].apply(
                        lambda x: len(str(x).split('.')[-1]) if '.' in str(x) else 0
                    ).max()
                    status = 'OK' if max_decimals <= 2 else 'ERROR'
                    print(f'  {col}: {max_decimals} decimal places [{status}]')
        else:
            print(f'Error: could not load data for {test_asset}')
            
    except Exception as e:
        print(f'Error in test: {e}')
        import traceback
        traceback.print_exc()
    
    # Load and display asset list
    try:
        df_assets = pd.read_csv('assets/SP500.csv')
        
        if 'symbol' in df_assets.columns:
            assets = df_assets['symbol'].tolist()
        else:
            assets = df_assets.iloc[:, 0].tolist()

        assets = [asset for asset in assets if pd.notna(asset) and asset != '']
            
        print(f'\nTotal assets loaded from CSV: {len(assets)}')
        print(f'First 5 assets: {assets[:5]}')
    except FileNotFoundError:
        print("\nFile 'assets/SP500.csv' not found!")
        print('Using default SP500 market list as fallback')
        data_instance = Data()
        assets = data_instance.list_symbols(market=MARKET_SP500)
    except Exception as e:
        print(f'Error reading CSV: {e}')
        print('Using default SP500 market list as fallback')


def test_sp500():
    '''Test SP500 market data functionality.'''
    print('-' * 40 + 'Listing assets' + '-' * 40)
    market_data = MarketData(MARKET_SP500)
    assets = market_data.list_recent_symbols(market_data.market, force_update=True)
    print(f'Found {len(assets)} assets')
    print(assets)

    print('\n' + '-' * 40 + 'Testing sectors via MemData' + '-' * 40)
    interval = ['2024-01-10', '2024-11-10']
    mem_data = MemData(interval, market_identifier=MARKET_SP500)
    print(f'AAPL sector: {mem_data.get_sector("AAPL")}')
    print(f'Total sectors loaded: {len(mem_data.get_all_sectors())}')


def test_mem_data():
    '''Diagnostic test for MemData functionality.''' 
    interval = ['2024-01-10', '2024-11-10']
    mem_data = MemData(interval, market_identifier=MARKET_SP500)

    print('Testing historical data retrieval:')
    all_history = mem_data.get_all_history()
    print(f'Total assets with data: {len(all_history)}')

    test_symbols = [('EQPA3.SA', 'Non-SP500'), ('PETR4.SA', MARKET_IBOV), 
                    ('AAPL', MARKET_SP500), ('B3SA3.SA', MARKET_IBRA)]
    
    for symbol, market_type in test_symbols:
        data = all_history.get(symbol)
        status = 'Found' if data is not None else 'Not found'
        print(f'  {symbol} ({market_type}): {status}')

    print('\nTesting sector information:')
    print(f'  MSFT sector: {mem_data.get_sector("MSFT")}')
    
    sectors = mem_data.get_all_sectors()
    print(f'  Total sectors available: {len(sectors)}')


if __name__ == '__main__':
    test()