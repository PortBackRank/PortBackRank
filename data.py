# -*- coding: utf-8 -*-

"""
Data management module for downloading and caching historical financial data.
Includes integration for market configurations and the MegaDataFrame structure
for optimized backtesting.
"""

import logging
from concurrent import futures
import os
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf
from tqdm import tqdm
from utils import open_dataframe, save_dataframe
from names import (
    DIR_HISTORICAL, MARKET_SP500, MARKET_IBOV, 
    COL_DATE, COL_CLOSE, COL_VOLUME, COL_OPEN, COL_HIGH, COL_LOW,
    YF_PERIOD_MAX, YF_AUTO_ADJUST, STR_UNKNOWN, STR_UNKNOWN_FULL,
    MARKET_KEY_SOURCE_FILE, SYMBOL_SUFFIX_SA, COL_SECTOR, COL_INDUSTRY, 
    COL_GICS_SECTOR, COL_GICS_SUBINDUSTRY, COL_CODIGO, COL_SETOR, 
    COL_SUBSETOR, COL_SEGMENTO, COL_SYMBOL, COL_SYMBOL_ALT,
    SEP_PIPE, SEP_COMMA, ENCODING_UTF8, ENCODING_ISO, DIR_ASSETS, MARKETS
)

logger = logging.getLogger(__name__) 


# ==========================================
# Market Identification and Reading Logic
# ==========================================

def identify_market(market_identifier: str) -> str:
    """Identifies the market key from either a file path or a ticker symbol."""
    if market_identifier is None:
        raise ValueError("It is necessary to provide a 'market_identifier'.")
    if market_identifier.upper() in MARKETS:
        return market_identifier.upper()
    
    file_name = os.path.basename(market_identifier)
    for key, config in MARKETS.items():
        if os.path.basename(config[MARKET_KEY_SOURCE_FILE]).lower() == file_name.lower():
            return key
            
    # Create dynamically
    market = file_name.replace('.csv', '').upper()
    MARKETS[market] = {MARKET_KEY_SOURCE_FILE: market_identifier}
    return market


def get_market_config(market: str) -> dict:
    """Retrieves the configuration dictionary for a given market."""
    market = identify_market(market)
    if market not in MARKETS:
        raise ValueError(f"Invalid market: {market}")
    return MARKETS[market]


def read_symbols(market: str) -> List[str]:
    """Reads asset symbols from market CSV file and appends suffixes if necessary."""
    config = get_market_config(market)
    file_path = config[MARKET_KEY_SOURCE_FILE]
    if DIR_ASSETS not in file_path:
        file_path = os.path.join(DIR_ASSETS, file_path)
        
    try:
        if MARKET_SP500 in market.upper() or MARKET_IBOV in market.upper():
            df = pd.read_csv(file_path, encoding=ENCODING_UTF8, sep=SEP_PIPE)
        else:
            df = pd.read_csv(file_path, encoding=ENCODING_ISO, sep=SEP_COMMA)
            
        df.columns = df.columns.str.strip()
        
        symbols = []
        if COL_SYMBOL in df.columns:
            symbols = df[COL_SYMBOL].dropna().tolist()
        elif COL_SYMBOL_ALT in df.columns:
            symbols = df[COL_SYMBOL_ALT].dropna().tolist()
        elif COL_CODIGO in df.columns:
            symbols = df[COL_CODIGO].dropna().tolist()
        else:
            symbols = df.iloc[1:, 0].dropna().tolist()
            
        # Append suffixes depending on the market
        if market != MARKET_SP500:
            symbols = [str(s).strip() + SYMBOL_SUFFIX_SA for s in symbols]
        else:
            symbols = [str(s).strip() for s in symbols]
            
        return symbols
    except Exception as e:
        logger.error('Error reading symbols from %s: %s', file_path, e)
        return []


def get_sector_mapping(market: str) -> Dict[str, str]:
    """Retrieves asset-to-sector mapping for diversification control."""
    config = get_market_config(market)
    file_path = config[MARKET_KEY_SOURCE_FILE]
    if DIR_ASSETS not in file_path:
        file_path = os.path.join(DIR_ASSETS, file_path)
        
    sector_map = {}
    try:
        if MARKET_SP500 in market.upper() or MARKET_IBOV in market.upper():
            df = pd.read_csv(file_path, encoding=ENCODING_UTF8, sep=SEP_PIPE)
        else:
            df = pd.read_csv(file_path, encoding=ENCODING_ISO, sep=SEP_COMMA)
            
        df.columns = df.columns.str.strip()
        
        if 'symbol' in df.columns or COL_SYMBOL_ALT in df.columns:
            sym_col = 'symbol' if 'symbol' in df.columns else COL_SYMBOL_ALT
            for _, row in df.iterrows():
                symbol = str(row[sym_col]).strip()
                if market != MARKET_SP500:
                    symbol += SYMBOL_SUFFIX_SA
                sector = str(row.get(COL_SECTOR, row.get(COL_GICS_SECTOR, STR_UNKNOWN))).strip()
                industry = str(row.get(COL_INDUSTRY, row.get(COL_GICS_SUBINDUSTRY, STR_UNKNOWN))).strip()
                sector_map[symbol] = f'{industry} - {sector}'
                
        elif COL_CODIGO in df.columns:
            for _, row in df.iterrows():
                codigo = str(row[COL_CODIGO]).strip()
                symbol = f'{codigo}{SYMBOL_SUFFIX_SA}'
                sector = str(row.get(COL_SETOR, STR_UNKNOWN)).strip()
                industry = str(row.get(COL_SUBSETOR, row.get(COL_SEGMENTO, STR_UNKNOWN))).strip()
                sector_map[symbol] = f'{industry} - {sector}'
                
        return sector_map
    except Exception as e:
        logger.error('Error reading sectors from %s: %s', file_path, e)
        return {}


# ==========================================
# Data Management and Cache
# ==========================================

class Data():
    """Data management class for downloading and caching historical market data."""
    _SUBDIR = DIR_HISTORICAL

    def __init__(self, precision: int = 2):
        self._precision = precision

    @property
    def precision(self) -> int:
        """Number of decimal places for price rounding (read-only)."""
        return self._precision

    @property
    def subdir(self) -> str:
        """Cache subdirectory for historical data (read-only)."""
        return self._SUBDIR

    def download_histories(self, assets: List[str], repair: bool = False) -> None:
        """Download historical data for multiple assets concurrently (or single if len=1)."""
            
        tickers = yf.Tickers(assets)
        assets_list = list(tickers.tickers.keys())

        with tqdm(total=len(assets_list), desc='Downloading data', unit='asset') as pbar:
            def download_and_save(asset):
                try:
                    df = tickers.tickers[asset].history(period=YF_PERIOD_MAX, auto_adjust=YF_AUTO_ADJUST, repair=repair)
                    self._save_asset_data(asset, df)
                except Exception as e:
                    logger.error('Error downloading %s: %s', asset, e)
                pbar.update(1)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(download_and_save, assets_list)

    def _save_asset_data(self, asset: str, asset_data: pd.DataFrame) -> None:
        if asset_data.empty:
            return
        asset_data_reset = asset_data.reset_index()
        asset_data_reset[COL_DATE] = asset_data_reset[COL_DATE].astype(str)
        numeric_columns = asset_data_reset.select_dtypes(include=['float64', 'float32']).columns
        asset_data_reset[numeric_columns] = asset_data_reset[numeric_columns].round(self._precision)
        save_dataframe(f'{asset}.csv', asset_data_reset, self.subdir)

    def get_asset_data_by_name(self, asset: str) -> Optional[pd.DataFrame]:
        """Retrieve cached historical data. Downloads if missing or empty."""
        file_name = f'{asset}.csv'
        df = open_dataframe(file_name, self.subdir)
        if df is None or df.empty:
            logger.debug('Cache miss for %s, downloading...', asset)
            print(f'Downloading data for {asset}...')
            self.download_histories([asset], repair=False)
            df = open_dataframe(file_name, self.subdir)
        return df

    def get_mega_dataframe(self, assets: List[str], start_date: str, end_date: str, price_type: str = 'C') -> pd.DataFrame:
        """
        Loads requested assets, processes price_type, and concatenates them into 
        a single MegaDataFrame with MultiIndex (Date, Symbol) for fast vectorized access.
        """
        start_date_dt = pd.to_datetime(start_date, utc=True).tz_localize(None)
        fetch_start_date = start_date_dt - pd.Timedelta(days=365)
        end_date_dt = pd.to_datetime(end_date, utc=True).tz_localize(None)

        all_data = []
        for asset in assets:
            df = self.get_asset_data_by_name(asset)
            if df is None or df.empty:
                continue
            
            df[COL_DATE] = pd.to_datetime(df[COL_DATE], utc=True).dt.tz_localize(None)
            df = df[(df[COL_DATE] >= fetch_start_date) & (df[COL_DATE] <= end_date_dt)].copy()
            df[COL_DATE] = df[COL_DATE].dt.strftime('%Y-%m-%d')
            if df.empty:
                continue
                
            # Filter and create the simulated 'Close' based on price_type
            if price_type == 'O':
                df[COL_CLOSE] = df[COL_OPEN]
            elif price_type == 'H':
                df[COL_CLOSE] = df[COL_HIGH]
            elif price_type == 'L':
                df[COL_CLOSE] = df[COL_LOW]
            elif price_type == 'CN':
                df[COL_CLOSE] = df[COL_CLOSE].shift(-1)
            elif price_type == 'HL2':
                df[COL_CLOSE] = (df[COL_HIGH] + df[COL_LOW]) / 2.0
            elif price_type == 'HLC3':
                df[COL_CLOSE] = (df[COL_HIGH] + df[COL_LOW] + df[COL_CLOSE]) / 3.0
            elif price_type == 'OHLC4':
                df[COL_CLOSE] = (df[COL_OPEN] + df[COL_HIGH] + df[COL_LOW] + df[COL_CLOSE]) / 4.0

            df['Symbol'] = asset
            all_data.append(df[[COL_DATE, 'Symbol', COL_VOLUME, COL_CLOSE]])
            
        if not all_data:
            return pd.DataFrame()
            
        mega_df = pd.concat(all_data, ignore_index=True)
        # Create MultiIndex (Date, Symbol) to allow fast cross-section slice operations
        mega_df.set_index([COL_DATE, 'Symbol'], inplace=True)
        mega_df.sort_index(inplace=True)
        return mega_df


class MemData:
    """
    In-memory data management acting as a facade for the MegaDataFrame
    and holding sector information for the current simulation context.
    """

    def __init__(self, interval: List[str], market_identifier: str = MARKET_SP500, price_type: str = 'C'):
        self._market_identifier = identify_market(market_identifier)
        self._price_type = price_type
        
        start_date, end_date = interval
        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
            
        self._assets = read_symbols(self._market_identifier)
        self._sector_info = get_sector_mapping(self._market_identifier)
        
        logger.debug('Assets mapped for %s: %d', self._market_identifier, len(self._assets))
        logger.debug('Sectors in cache: %d', len(self._sector_info))
        print(f'Loading data for {self._market_identifier} ({len(self._assets)} assets)...')
        
        self._data = Data()
        logger.debug('Loading MegaDataFrame from %s to %s', start_date, end_date)
        self._mega_df = self._data.get_mega_dataframe(self._assets, start_date, end_date, price_type)
        
        # Determine actual available assets and dates from the MegaDataFrame
        if not self._mega_df.empty:
            self._assets = list(self._mega_df.index.get_level_values('Symbol').unique())
            # Convert Timestamps to str for easier date logic, or just leave as DatetimeIndex
            # We'll expose a string list for compatibility
            self._dates_ts = self._mega_df.index.get_level_values(COL_DATE).unique()
            self._dates = list(self._dates_ts)
        else:
            self._assets = []
            self._dates = []
            
        logger.debug('MegaDataFrame loaded: %d assets over %d dates', len(self._assets), len(self._dates))
        print(f'Ready: {len(self._assets)} assets, {len(self._dates)} dates')

    @property
    def market_identifier(self) -> str:
        """Market identifier (read-only)."""
        return self._market_identifier

    @property
    def price_type(self) -> str:
        """Price type used for simulation (read-only)."""
        return self._price_type

    @property
    def assets(self) -> List[str]:
        """List of available asset symbols (read-only)."""
        return self._assets

    @property
    def sector_info(self) -> Dict[str, str]:
        """Sector mapping for all assets (read-only copy)."""
        return self._sector_info.copy()

    @property
    def data(self) -> Data:
        """Data manager instance (read-only)."""
        return self._data

    @property
    def mega_df(self) -> pd.DataFrame:
        """MegaDataFrame with all simulation data (read-only)."""
        return self._mega_df

    @property
    def dates(self) -> List[str]:
        """List of available dates as strings (read-only)."""
        return self._dates

    @property
    def dates_ts(self):
        """DatetimeIndex of available dates (read-only)."""
        return self._dates_ts

    def get_assets(self) -> List[str]:
        return self.assets

    def get_sector(self, symbol: str) -> str:
        return self._sector_info.get(symbol, STR_UNKNOWN_FULL)
    
    def get_all_sectors(self) -> Dict[str, str]:
        return self._sector_info.copy()

    def get_mega_df(self) -> pd.DataFrame:
        """Returns the multi-indexed DataFrame containing all simulation data."""
        return self._mega_df


def test():
    """Diagnostic test for MegaDataFrame loading and data reading."""
    print('Testing data loading with MemData...')
    interval = ['2024-01-10', '2024-11-10']
    mem_data = MemData(interval, market_identifier=MARKET_SP500)
    
    print(f"Total Assets: {len(mem_data.get_assets())}")
    
    df = mem_data.get_mega_df()
    if not df.empty:
        print(f"MegaDataFrame shape: {df.shape}")
        print('Head:')
        print(df.head())
    else:
        print('MegaDataFrame is empty!')


if __name__ == '__main__':
    test()