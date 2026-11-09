# -*- coding: utf-8 -*-

'''
Data management module for downloading and caching historical financial data.
Includes the MegaDataFrame structure for optimized backtesting.
'''

import logging
import os
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
import yfinance as yf
from tqdm import tqdm
from utils import open_dataframe, save_dataframe
from names import *

logger = logging.getLogger(__name__)


# ==========================================
# Asset CSV Reading and Resolving Logic
# ==========================================

def resolve_csv_path(path_or_identifier: str) -> str:
    '''Resolve a market identifier or relative/absolute path to an existing CSV path.'''
    if not path_or_identifier:
        return f'{DIR_ASSETS}/{DEFAULT_MARKET}.csv'

    # Direct match
    if os.path.exists(path_or_identifier):
        return path_or_identifier

    # Check under DIR_ASSETS
    candidate = os.path.join(DIR_ASSETS, path_or_identifier)
    if os.path.exists(candidate):
        return candidate

    # Check with .csv extension
    if not path_or_identifier.lower().endswith(EXT_CSV):
        candidate_ext = os.path.join(DIR_ASSETS, f'{path_or_identifier}{EXT_CSV}')
        if os.path.exists(candidate_ext):
            return candidate_ext

    return path_or_identifier


def identify_market(market_identifier: str) -> str:
    '''Resolve market/assets identifier to CSV path (backward compatibility).'''
    return resolve_csv_path(market_identifier)


def read_symbols(assets_source: str) -> List[str]:
    '''Reads asset symbols from asset CSV file.'''
    path = resolve_csv_path(assets_source)
    try:
        try:
            df = pd.read_csv(path, sep=SEP_PIPE, encoding=ENCODING_UTF8)
            if len(df.columns) <= 1:
                df = pd.read_csv(path, sep=SEP_COMMA, encoding=ENCODING_UTF8)
        except Exception:
            df = pd.read_csv(path, sep=SEP_COMMA, encoding=ENCODING_ISO)

        df.columns = df.columns.str.strip()

        sym_col = None
        for col in [COL_SYMBOL, COL_SYMBOL_ALT, COL_CODE, COL_CODIGO]:
            if col in df.columns:
                sym_col = col
                break

        if sym_col:
            symbols = df[sym_col].dropna().astype(str).str.strip().tolist()
        else:
            symbols = df.iloc[:, 0].dropna().astype(str).str.strip().tolist()

        # If Brazilian assets, append .SA if missing
        is_b3 = 'IBOV' in path.upper() or any(s[-1].isdigit() for s in symbols if len(s) > 0 and not s.endswith(SYMBOL_SUFFIX_SA))
        if is_b3:
            symbols = [s + SYMBOL_SUFFIX_SA if not s.endswith(SYMBOL_SUFFIX_SA) else s for s in symbols]

        return symbols
    except Exception as e:
        logger.error('Error reading symbols from %s: %s', path, e)
        return []


def get_sector_mapping(assets_source: str) -> Dict[str, str]:
    '''Retrieves asset-to-sector mapping (symbol -> 'industry - sector') from CSV.'''
    path = resolve_csv_path(assets_source)
    try:
        try:
            df = pd.read_csv(path, sep=SEP_PIPE, encoding=ENCODING_UTF8)
            if len(df.columns) <= 1:
                df = pd.read_csv(path, sep=SEP_COMMA, encoding=ENCODING_UTF8)
        except Exception:
            df = pd.read_csv(path, sep=SEP_COMMA, encoding=ENCODING_ISO)

        df.columns = df.columns.str.strip()
        sector_map = {}

        sym_col = None
        for col in [COL_SYMBOL, COL_SYMBOL_ALT, COL_CODE, COL_CODIGO]:
            if col in df.columns:
                sym_col = col
                break

        if not sym_col:
            return {}

        is_b3 = 'IBOV' in path.upper()

        for _, row in df.iterrows():
            symbol = str(row[sym_col]).strip()
            if (is_b3 or (len(symbol) > 0 and symbol[-1].isdigit())) and not symbol.endswith(SYMBOL_SUFFIX_SA):
                symbol += SYMBOL_SUFFIX_SA

            sector = str(row.get(COL_SECTOR, row.get(COL_GICS_SECTOR, row.get(COL_SETOR, STR_UNKNOWN)))).strip()
            industry = str(row.get(COL_INDUSTRY, row.get(COL_GICS_SUBINDUSTRY, row.get(COL_SUBSETOR, row.get(COL_SEGMENTO, STR_UNKNOWN))))).strip()
            sector_map[symbol] = f'{industry} - {sector}'

        return sector_map
    except Exception as e:
        logger.error('Error reading sectors from %s: %s', path, e)
        return {}


# ==========================================
# Data Management and Cache
# ==========================================

class Data:
    '''Data management class for downloading and caching historical market data.'''

    def __init__(self, precision: int = 2):
        self._precision = precision

    @property
    def precision(self) -> int:
        '''Number of decimal places for price rounding (read-only).'''
        return self._precision

    @property
    def subdir(self) -> str:
        '''Cache subdirectory for historical data (read-only).'''
        return DIR_HISTORICAL

    def download_histories(self, assets: List[str], repair: bool = True) -> None:
        '''Download historical data for multiple assets concurrently.

        Args:
            assets: list of ticker symbols.
            repair: ask yfinance to repair known price errors (100x unit
                mix-ups, missing dividend/split adjustments, etc.).
        '''
        tickers = yf.Tickers(assets)
        assets_list = list(tickers.tickers.keys())

        with tqdm(total=len(assets_list), desc='Downloading data', unit='asset') as pbar:
            def download_and_save(asset):
                try:
                    df = tickers.tickers[asset].history(period=YF_PERIOD_MAX, auto_adjust=YF_AUTO_ADJUST, repair=repair)
                    self._save_asset_data(asset, df)
                except Exception as e:
                    try:
                        logger.warning('Downloading %s with period=%s failed (%s), retrying with period="10y"...', asset, YF_PERIOD_MAX, e)
                        df = tickers.tickers[asset].history(period='10y', auto_adjust=YF_AUTO_ADJUST, repair=repair)
                        self._save_asset_data(asset, df)
                    except Exception as e2:
                        try:
                            df = tickers.tickers[asset].history(period='10y', auto_adjust=YF_AUTO_ADJUST, repair=False)
                            self._save_asset_data(asset, df)
                        except Exception as e3:
                            logger.error('Error downloading %s: %s', asset, e3)
                finally:
                    pbar.update(1)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(download_and_save, assets_list)

    def _save_asset_data(self, asset: str, asset_data: pd.DataFrame) -> None:
        '''Round float columns and save one asset history to the cache.'''
        if asset_data.empty:
            return
        asset_data_reset = asset_data.reset_index()
        asset_data_reset[COL_DATE] = asset_data_reset[COL_DATE].astype(str)
        numeric_columns = asset_data_reset.select_dtypes(include=FLOAT_DTYPES).columns
        asset_data_reset[numeric_columns] = asset_data_reset[numeric_columns].round(self._precision)
        save_dataframe(f'{asset}{EXT_CSV}', asset_data_reset, DIR_HISTORICAL)

    def get_asset_data_by_name(self, asset: str) -> Optional[pd.DataFrame]:
        '''Retrieve cached historical data. Downloads if missing or empty.'''
        file_name = f'{asset}{EXT_CSV}'
        df = open_dataframe(file_name, DIR_HISTORICAL)
        if df is None or df.empty:
            logger.debug('Cache miss for %s, downloading...', asset)
            print(f'Downloading data for {asset}...')
            self.download_histories([asset], repair=True)
            df = open_dataframe(file_name, DIR_HISTORICAL)
        return df

    def ensure_assets(self, assets_or_source: Union[str, List[str]], max_retries: int = 3) -> None:
        '''Ensure all assets in the list or CSV have downloaded historical data.'''
        if isinstance(assets_or_source, str):
            assets = read_symbols(assets_or_source)
        else:
            assets = list(assets_or_source)

        missing = []
        for attempt in range(1, max_retries + 1):
            missing = []
            for asset in assets:
                df = open_dataframe(f'{asset}{EXT_CSV}', DIR_HISTORICAL)
                if df is None or df.empty:
                    missing.append(asset)

            if not missing:
                print('All assets have local data.')
                break

            print(
                f'Attempt {attempt}/{max_retries}: '
                f'downloading {len(missing)} assets without local data.'
            )
            print('Missing assets:', ', '.join(missing))
            self.download_histories(missing, repair=True)

        if missing:
            print('After download attempts, historical data is still missing for:')
            for asset in missing:
                print(f'  - {asset}')

    def get_mega_dataframe(self, assets: List[str], start_date: str, end_date: str, price_type: str = PRICE_CLOSE) -> pd.DataFrame:
        '''
        Loads requested assets, processes price_type, and concatenates them into 
        a single MegaDataFrame with MultiIndex (Date, Symbol) for fast vectorized access.

        Each asset keeps its own dates; alignment between assets is given by
        the (Date, Symbol) index, not by row position. Dates are stored as
        'YYYY-MM-DD' strings so lexicographic order equals chronological order.

        Raises:
            ValueError: if the resulting index has duplicated (Date, Symbol) pairs.
        '''
        start_date_dt = pd.to_datetime(start_date, utc=True).tz_localize(None)
        fetch_start_date = start_date_dt - pd.Timedelta(days=HISTORY_LOOKBACK_DAYS)
        end_date_dt = pd.to_datetime(end_date, utc=True).tz_localize(None)

        all_data = []
        for asset in assets:
            df = self.get_asset_data_by_name(asset)
            if df is None or df.empty:
                continue
            
            df[COL_DATE] = pd.to_datetime(df[COL_DATE], utc=True).dt.tz_localize(None)
            df = df[(df[COL_DATE] >= fetch_start_date) & (df[COL_DATE] <= end_date_dt)].copy()
            df[COL_DATE] = df[COL_DATE].dt.strftime(DATE_FMT)
            if df.empty:
                continue
                
            # Create the simulated 'Buy' price based on price_type (Close is preserved)
            if price_type == PRICE_OPEN:
                df[COL_BUY] = df[COL_OPEN]
            elif price_type == PRICE_HIGH:
                df[COL_BUY] = df[COL_HIGH]
            elif price_type == PRICE_LOW:
                df[COL_BUY] = df[COL_LOW]
            elif price_type == PRICE_CLOSE_NEXT:
                df[COL_BUY] = df[COL_CLOSE].shift(-1)
            elif price_type == PRICE_HL2:
                df[COL_BUY] = (df[COL_HIGH] + df[COL_LOW]) / 2.0
            elif price_type == PRICE_HLC3:
                df[COL_BUY] = (df[COL_HIGH] + df[COL_LOW] + df[COL_CLOSE]) / 3.0
            elif price_type == PRICE_OHLC4:
                df[COL_BUY] = (df[COL_OPEN] + df[COL_HIGH] + df[COL_LOW] + df[COL_CLOSE]) / 4.0
            else:
                df[COL_BUY] = df[COL_CLOSE]

            df[IDX_SYMBOL] = asset
            # Missing volume would break int(volume * pct) in the Runner;
            # treat it as 'no liquidity' (0) so the asset is simply not traded that day.
            missing_volume = df[COL_VOLUME].isna().sum()
            if missing_volume:
                logger.debug('%s: %d rows with missing Volume set to 0', asset, missing_volume)
                df[COL_VOLUME] = df[COL_VOLUME].fillna(0)
            all_data.append(df[[COL_DATE, IDX_SYMBOL, COL_VOLUME, COL_CLOSE, COL_BUY]])
            
        if not all_data:
            return pd.DataFrame()
            
        # ignore_index only discards each asset's positional RangeIndex;
        # rows are aligned afterwards by the (Date, Symbol) MultiIndex.
        mega_df = pd.concat(all_data, ignore_index=True)
        # Create MultiIndex (Date, Symbol) to allow fast cross-section slice operations
        mega_df.set_index([COL_DATE, IDX_SYMBOL], inplace=True)
        mega_df.sort_index(inplace=True)

        duplicated = mega_df.index.duplicated(keep=False)
        if duplicated.any():
            sample = list(mega_df.index[duplicated].unique()[:5])
            raise ValueError(
                f'MegaDataFrame has {duplicated.sum()} rows with duplicated (Date, Symbol) pairs, '
                f'dates are not aligned per asset. Examples: {sample}'
            )
        return mega_df


def ensure_market_assets(assets_source: str = f'{DIR_ASSETS}/{MARKET_SP500}.csv') -> None:
    '''Ensure all assets in the CSV have downloaded historical data.'''
    data_handler = Data()
    data_handler.ensure_assets(assets_source)


_ensure_market_assets = ensure_market_assets


class MemData:
    '''
    In-memory data management acting as a facade for the MegaDataFrame
    and holding sector information for the current simulation context.
    '''

    def __init__(self, interval: List[str], assets: str = None, price_type: str = PRICE_CLOSE, market_identifier: str = None):
        assets_source = assets or market_identifier or f'{DIR_ASSETS}/{MARKET_SP500}.csv'
        self._assets_path = resolve_csv_path(assets_source)
        self._market_identifier = os.path.basename(self._assets_path).replace(EXT_CSV, '')
        self._price_type = price_type
        
        start_date, end_date = interval
        if end_date is None:
            end_date = datetime.today().strftime(DATE_FMT)
            
        self._assets = read_symbols(self._assets_path)
        self._sector_info = get_sector_mapping(self._assets_path)
        
        logger.debug('Assets mapped for %s: %d', self._assets_path, len(self._assets))
        logger.debug('Sectors in cache: %d', len(self._sector_info))
        print(f'Loading data for {self._market_identifier} ({len(self._assets)} assets)...')
        
        self._data = Data()
        logger.debug('Loading MegaDataFrame from %s to %s', start_date, end_date)
        self._mega_df = self._data.get_mega_dataframe(self._assets, start_date, end_date, price_type)
        
        # Determine actual available assets and dates from the MegaDataFrame
        if not self._mega_df.empty:
            self._assets = list(self._mega_df.index.get_level_values(IDX_SYMBOL).unique())
            self._dates_ts = self._mega_df.index.get_level_values(COL_DATE).unique()
            self._dates = list(self._dates_ts)
        else:
            self._assets = []
            self._dates = []
            
        logger.debug('MegaDataFrame loaded: %d assets over %d dates', len(self._assets), len(self._dates))
        print(f'Ready: {len(self._assets)} assets, {len(self._dates)} dates')

    @property
    def market_identifier(self) -> str:
        '''Market identifier (read-only).'''
        return self._market_identifier

    @property
    def assets_path(self) -> str:
        '''Assets CSV file path (read-only).'''
        return self._assets_path

    @property
    def price_type(self) -> str:
        '''Price type used for simulation (read-only).'''
        return self._price_type

    @property
    def assets(self) -> List[str]:
        '''List of available asset symbols (read-only).'''
        return self._assets

    @property
    def sector_info(self) -> Dict[str, str]:
        '''Sector mapping for all assets (read-only copy).'''
        return self._sector_info.copy()

    @property
    def data(self) -> Data:
        '''Data manager instance (read-only).'''
        return self._data

    @property
    def mega_df(self) -> pd.DataFrame:
        '''MegaDataFrame with all simulation data (read-only).'''
        return self._mega_df

    @property
    def dates(self) -> List[str]:
        '''List of available dates as strings (read-only).'''
        return self._dates

    @property
    def dates_ts(self):
        '''DatetimeIndex of available dates (read-only).'''
        return self._dates_ts

    def get_assets(self) -> List[str]:
        '''Return the list of available asset symbols.'''
        return self.assets

    def get_sector(self, symbol: str) -> str:
        '''Return 'industry - sector' for a symbol, or STR_UNKNOWN_FULL if unmapped.'''
        return self._sector_info.get(symbol, STR_UNKNOWN_FULL)
    
    def get_all_sectors(self) -> Dict[str, str]:
        '''Return a copy of the full sector mapping.'''
        return self._sector_info.copy()

    def get_mega_df(self) -> pd.DataFrame:
        '''Returns the multi-indexed DataFrame containing all simulation data.'''
        return self._mega_df
