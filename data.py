# -*- coding: utf-8 -*-

'''
Data class
'''

import concurrent.futures
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf
from tqdm import tqdm
from files import open_dataframe, save_dataframe
from markets import MarketData
from names import SUB_DIR_HIST 


class Data():
    '''Data management - downloading and caching historical data'''

    subdir = SUB_DIR_HIST

    def __init__(self, end_date: Optional[str] = None, precision: int = 2):
        """Initialize Data class for historical data management.
        
        :param end_date: Optional end date for data
        :param precision: Decimal precision for numeric values
        """
        self.end_date = end_date
        self.precision = precision

    @classmethod
    def update_symbols(cls, market: str, update: bool = False) -> List[str]:
        """Updates the list of symbols via MarketData.
        
        :param market: Market identifier (e.g., 'SP500', 'IBOV')
        :param update: Force update from source
        :return: List of symbols
        """
        return MarketData.list_recent_symbols(market=market, force_update=update)

    @classmethod
    def list_symbols(cls, market: str = "SP500") -> List[str]:
        """Returns a list of symbols via MarketData.
        
        :param market: Market identifier
        :return: List of symbols
        """
        return MarketData.get_symbol_list(market=market)

    @classmethod
    def download_history(cls, asset: str) -> None:
        """Downloads historical data for a single asset.
        
        :param asset: Asset symbol
        """
        asset_data = yf.Ticker(asset).history(period="max")
        cls._save_asset_data(asset, asset_data)

    @classmethod
    def download_histories(cls, assets: List[str]) -> None:
        """Downloads historical data for all assets in the list concurrently.
        
        :param assets: List of asset symbols
        """
        tickers = yf.Tickers(assets)
        assets_list = list(tickers.tickers.keys())

        with tqdm(total=len(assets_list), desc="Downloading data", unit="asset") as pbar:
            def download_and_save(asset):
                cls._save_asset_data(
                    asset, tickers.tickers[asset].history(period="max"))
                pbar.update(1)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(download_and_save, assets_list)

    @classmethod
    def fetch_history(cls, assets: List[str]) -> List[Dict[str, pd.DataFrame]]:
        """Fetches and concatenates historical data for the given list of assets.
        
        :param assets: List of asset symbols
        :return: List of dictionaries with symbol and corresponding dataframe
        """
        result = []
        for asset in assets:
            df = cls.get_asset_data_by_name(asset)
            if isinstance(df, pd.DataFrame) and not df.empty:
                result.append({"symbol": asset, "data": df})
        return result

    @classmethod
    def get_history_interval(
        cls,
        assets: List[str],
        start_date: str,
        end_date: str,
        column_filter: Optional[str] = "Close",
    ) -> List[Dict[str, pd.DataFrame]]:
        """Returns historical data filtered by a time interval.

        :param assets: List of assets
        :param start_date: Start date of the interval (YYYY-MM-DD)
        :param end_date: End date of the interval (YYYY-MM-DD)
        :param column_filter: Column to filter (default is "Close")
        :return: List of dictionaries with the symbol and the data in a DataFrame
        """
        historical_data = cls.fetch_history(assets=assets)

        if not historical_data:
            print("Empty historical data")
            return []

        result = []

        start_date_dt = pd.to_datetime(start_date, utc=True).tz_localize(None)
        end_date_dt = pd.to_datetime(end_date, utc=True).tz_localize(None)

        columns_to_return = ["Volume"]
        if column_filter in {"Close", None}:
            columns_to_return.append("Close")
        elif column_filter != "None":
            columns_to_return.append(column_filter)

        for asset in historical_data:
            symbol = asset["symbol"]
            data = asset["data"]

            data["Date"] = pd.to_datetime(
                data["Date"], utc=True).dt.tz_localize(None)

            filtered_data = data[
                (data["Date"] >= start_date_dt) & (data["Date"] <= end_date_dt)
            ]

            if filtered_data.empty:
                continue

            filtered_data.set_index("Date", inplace=True)
            filtered_data = filtered_data[columns_to_return]

            result.append({
                "symbol": symbol,
                "data": filtered_data
            })

        return result
    
    @classmethod
    def _save_asset_data(cls, asset: str, asset_data: pd.DataFrame) -> None:
        """Save asset data to CSV files if data is available.
        
        :param asset: Asset symbol
        :param asset_data: DataFrame with asset data
        """
        if asset_data.empty:
            return

        asset_data_reset = asset_data.reset_index()
        asset_data_reset['Date'] = asset_data_reset['Date'].astype(str)

        numeric_columns = asset_data_reset.select_dtypes(
            include=['float64', 'float32']).columns
        asset_data_reset[numeric_columns] = asset_data_reset[numeric_columns].round(4)

        save_dataframe(f"{asset}.csv", asset_data_reset, cls.subdir)

    @classmethod
    def get_asset_data(cls, assets: List[str]) -> List[pd.DataFrame]:
        """Load historical data for one or more assets.
        
        :param assets: List of asset symbols
        :return: List of DataFrames with historical data
        """
        assets_data = []
        for asset in assets:
            asset_data = cls.get_asset_data_by_name(asset)
            if asset_data is not None and not asset_data.empty:
                assets_data.append(asset_data)
        return assets_data

    @classmethod
    def load_dataframe(cls, file_name: str) -> Optional[pd.DataFrame]:
        """Load data from a CSV file.
        
        :param file_name: Name of the file
        :return: DataFrame or None if not found
        """
        return open_dataframe(file_name, cls.subdir)

    @classmethod
    def get_asset_data_by_name(cls, asset: str) -> Optional[pd.DataFrame]:
        """Get historical data for a specific asset.
        If the CSV is missing or empty, attempt to download and load again.
        
        :param asset: Asset symbol
        :return: DataFrame with historical data or None
        """
        file_name = f"{asset}.csv"
        df = None
        
        try:
            df = cls.load_dataframe(file_name)
        except FileNotFoundError:
            df = None

        # open_dataframe may return None instead of raising
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print(f"File {file_name} not found or empty. Downloading data for {asset}.")
            cls.download_history(asset)
            try:
                df = cls.load_dataframe(file_name)
            except FileNotFoundError:
                df = None
        
        return df


class MemData:
    """In-memory data management for assets with sector information.
    
    Responsibilities:
    - Load historical data into memory (via Data class)
    - Manage sector/industry information (via MarketData class)
    - Provide unified data access methods
    """

    def __init__(self, interval: List[str], market_identifier: str = "SP500"):
        """Initialize MemData with historical data and sector information.
        
        :param interval: List with [start_date, end_date]
        :param market_identifier: Market identifier (e.g., "SP500", "IBOV")
        """
        self.history_data: Dict[str, pd.DataFrame] = {}
        self.sector_info: Dict[str, str] = {}  # symbol -> "industry - sector"
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

        print(f"Assets available: {len(self.assets)}")
        print(f"Sectors in cache: {len(self.sector_info)}")
        if self.assets:
            print(f"First 5 assets: {self.assets[:5]}")
        
        if not self.assets:
            print(f"No assets found for {market_identifier}.")
            return

        # Load historical data using Data class
        start_date, end_date = interval
        self.load(start_date, end_date)

    def load_sector_info(self) -> None:
        """Load and concatenate sector information as 'industry - sector'.
        Uses MarketData.get_sector_mapping() to read from CSV.
        """
        try:
            # MarketData.get_sector_mapping retorna: {symbol: {sector: ..., industry: ...}}
            sector_mapping = self.market_data.get_sector_mapping(self.market_data.market)
            
            for symbol, info in sector_mapping.items():
                industry = info.get('industry', 'Unknown')
                sector = info.get('sector', 'Unknown')
                # Concatenate industry and sector
                self.sector_info[symbol] = f'{industry} - {sector}'
            
            print(f'Loaded sector info for {len(self.sector_info)} symbols')
        except Exception as e:
            print(f'Error loading sector info: {e}')
            import traceback
            traceback.print_exc()
            self.sector_info = {}
    
    def load(self, start_date: str, end_date: str) -> None:
        """Loads historical data into memory using Data class.

        :param start_date: Start date (YYYY-MM-DD)
        :param end_date: End date (YYYY-MM-DD)
        """
        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')

        print(f"Loading historical data from {start_date} to {end_date}...")
        
        # Use Data class to get historical data
        historical_data = self.data.get_history_interval(
            assets=self.assets, start_date=start_date, end_date=end_date
        )

        for asset_data in historical_data:
            asset = asset_data["symbol"]
            self.history_data[asset] = asset_data["data"]

        print(f"Historical data loaded: {len(self.history_data)} assets")

    def get_assets(self) -> List[str]:
        """Returns the list of assets in memory.

        :return: List of asset symbols
        """
        return list(self.assets)

    def get_all_history(self) -> Dict[str, pd.DataFrame]:
        """Returns all stored historical data.

        :return: Dictionary with asset symbols as keys and dataframes as values
        """
        return self.history_data
    
    def get_sector(self, symbol: str) -> str:
        """Get sector information for a specific symbol.
        
        :param symbol: Asset symbol
        :return: Concatenated 'industry - sector' string
        """
        return self.sector_info.get(symbol, 'Unknown - Unknown')
    
    def get_all_sectors(self) -> Dict[str, str]:
        """Get all sector information.
        
        :return: Dictionary mapping symbols to 'industry - sector'
        """
        return self.sector_info.copy()

def teste():
    '''Test function'''
    try:
        df_assets = pd.read_csv('assets/SP500.csv')
        
        if 'symbol' in df_assets.columns:
            assets = df_assets['symbol'].tolist()
        else:
            assets = df_assets.iloc[:,0].tolist()

        assets = [asset for asset in assets if pd.notna(asset) and asset != '']
            
        print(f"Total de ativos carregados do CSV: {len(assets)}")
        print(f"Primeiros 5 ativos: {assets[:5]}")
        print(f"Sector-Industry of all assets{assets}")
    except FileNotFoundError:
        print("Arquivo 'assets.csv' não encontrado!")
        print("Usando lista padrão do mercado SP500 como fallback")
        assets = Data.list_symbols(market="SP500")
    except Exception as e:
        print(f"Erro ao ler CSV: {e}")
        print("Usando lista padrão do mercado SP500 como fallback")
        assets = Data.list_symbols(market="SP500")


def teste_sp500():
    '''Test function'''
    print('--------------Listando ativos----------------')
    market_data = MarketData("SP500")
    assets = market_data.list_recent_symbols(market_data.market, force_update=True)
    print(assets)

    print('--------------Testando setores via MemData----------------')
    # Data não tem mais get_sector - usar MemData
    interval = ["2024-01-10", "2024-11-10"]
    mem_data = MemData(interval, market_identifier="SP500")
    print(mem_data.get_sector('AAPL')) 
    print(len(mem_data.get_all_sectors()))


def teste_mem_data():
    '''Test function'''
    interval = ["2024-01-10", "2024-11-10"]
    mem_data = MemData(interval, market_identifier="SP500")

    print("Todos os dados históricos:")
    todas_info = mem_data.get_all_history()

    print("Histórico de um ativo que nao existe no sp500:")
    print(todas_info.get('EQPA3.SA'))

    print("Histórico de um ativo que existe no ibov:")
    print(todas_info.get('PETR4.SA'))

    print("Histórico de um ativo que existe em sp500:")
    print(todas_info.get('AAPL'))

    print("Histórico de um ativo que existe em ibra:")
    print(todas_info.get('B3SA3.SA'))

    print("Setores de um ativo existente")
    print(mem_data.get_sector('MSFT'))

    print("Setores de todos os ativos")
    sectors = mem_data.get_all_sectors()
    print(sectors)

    print("informações de ativos ---- descomentar :")
    # print(mem_data.get_assets())

    print("Todas as informações----- descomentar:")
    # print(mem_data.get_all_info())


if __name__ == "__main__":
    teste_mem_data()
