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
from files import open_dataframe, save_dataframe, save_json
from b3 import update_symbols, get_symbol_list
from markets import MarketData

SUB_DIR_HIST = "historical"


class Yahoo:
    '''Yahoo Finance data management'''
    subdir = SUB_DIR_HIST

    @classmethod
    def _save_asset_data(cls, asset: str, asset_data) -> None:
        """Save asset data to JSON and CSV files if data is available."""
        if asset_data.empty:
            return

        asset_data_reset = asset_data.reset_index()
        asset_data_reset['Date'] = asset_data_reset['Date'].astype(str)

        numeric_columns = asset_data_reset.select_dtypes(include=['float64', 'float32']).columns
        asset_data_reset[numeric_columns] = asset_data_reset[numeric_columns].round(4)

        # save_json(f"{asset}.json", data_dict, cls.subdir)
        save_dataframe(f"{asset}.csv", asset_data_reset, cls.subdir)

    @classmethod
    def download_history(cls, asset: str) -> None:
        """Download historical data for a single asset."""
        asset_data = yf.Ticker(asset).history(period="max")
        cls._save_asset_data(asset, asset_data)

    @classmethod
    def download_histories(cls, assets: List[str]) -> None:
        """Download historical data for all assets in the list concurrently."""
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
    def get_asset_data(cls, assets: List[str]) -> List[pd.DataFrame]:
        '''Load historical data for one or more assets'''
        assets_data = []
        for asset in assets:
            asset_data = cls.get_asset_data_by_name(asset)
            if asset_data is not None and not asset_data.empty:
                assets_data.append(asset_data)
        return assets_data

    @classmethod
    def get_info(cls, assets: List[str]) -> Dict[str, pd.DataFrame]:
        '''Load information about one or more assets.
        Returns a mapping {symbol: DataFrame} and skips missing/empty files
        without breaking alignment with the original symbol list.'''
        info_map: Dict[str, pd.DataFrame] = {}
        history = cls()
        for asset in assets:
            file_name = f"{asset}_info.csv"
            asset_data = history.load_dataframe(file_name)
            # Some loaders return None when the file does not exist.
            if isinstance(asset_data, pd.DataFrame) and not asset_data.empty:
                info_map[asset] = asset_data
        return info_map

    @classmethod
    def load_dataframe(cls, file_name: str) -> pd.DataFrame:
        '''Load data from a CSV file'''
        return open_dataframe(file_name, cls.subdir)

    @classmethod
    def get_asset_data_by_name(cls, asset: str) -> pd.DataFrame:
        '''Get historical data for a specific asset.
        If the CSV is missing or empty, attempt to download and load again.'''
        file_name = f"{asset}.csv"
        df = None
        try:
            df = cls.load_dataframe(file_name)
        except FileNotFoundError:
            df = None

        # open_dataframe may return None instead of raising. Treat as missing.
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            print(f"File {file_name} not found or empty. Downloading data for {asset}.")
            cls.download_history(asset)
            try:
                df = cls.load_dataframe(file_name)
            except FileNotFoundError:
                df = None
        return df


class Data(Yahoo):
    '''Data management'''

    def __init__(self, end_date: Optional[str] = None):
        self.end_date = end_date

    @classmethod
    def update_symbols(cls, update: bool = False) -> None:
        """Updates the list of symbols and removes those without desired information."""
        update_symbols(update=update)

    @classmethod
    def list_symbols(cls) -> List[str]:
        """Returns a list of symbols."""
        return get_symbol_list()

    @classmethod
    def get_asset_info(cls, symbols: List[str]) -> Dict[str, pd.DataFrame]:
        """Returns a mapping of symbol -> info DataFrame for the given symbols."""
        return cls.get_info(assets=symbols)

    @classmethod
    def download_history(cls, assets: List[str]) -> None:
        """Downloads historical data for the given list of assets."""
        cls.download_histories(assets=assets)

    @classmethod
    def fetch_history(cls, assets: List[str]) -> List[dict]:
        """
        Fetches and concatenates historical data for the given list of assets.
        Now returns a list of dictionaries with symbol
        as the key and its corresponding dataframe as the value.
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
        """
        Returns historical data filtered by a time interval.

        :param assets: List of assets.
        :param start_date: Start date of the interval (YYYY-MM-DD).
        :param end_date: End date of the interval (YYYY-MM-DD).
        :param column_filter: Column to filter (default is "Close").
        :return: A list of dictionaries with the symbol and the data in a DataFrame.
        """
        historical_data = cls.fetch_history(assets=assets)

        if not historical_data:
            print("Empty historical data")
            cls.update_symbols(update=True)
            historical_data = cls.fetch_history(assets=assets)
            if not historical_data:
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


class MemData:
    '''In-memory data management for assets.'''

    def __init__(self, interval: List[str], market_identifier: str = "SP500"):
        """
        Inicializa MemData com dados históricos e informações de setor.
        
        :param interval: Lista com [data_inicio, data_fim]
        :param market_identifier: Identificador do mercado (ex: "SP500", "IBOV")
        """
        self.history_data: Dict[str, pd.DataFrame] = {}
        self.sector_data: Dict[str, Dict[str, str]] = {}  # NOVO!
        self.data = Data()

        # Carrega lista de ativos do mercado
        market_data = MarketData(market_identifier)
        
        self.assets = market_data.list_recent_symbols(
            market_data.market, force_update=False
        )
        
        # Carrega informações de setor do CSV de entrada
        self.sector_data = market_data.get_sector_mapping(market_data.market)
        
        print(f"Ativos disponíveis: {len(self.assets)}")
        print(f"Setores em cache: {len(self.sector_data)}")
        print(self.assets[:5])
        
        if not self.assets:
            print(f"Nenhum ativo encontrado para {market_identifier}.")
            return

        start_date, end_date = interval
        self.load(start_date, end_date)

    def load(self, start_date: str, end_date: str):
        """
        Loads historical data into memory.

        :param start_date: Data de início (YYYY-MM-DD)
        :param end_date: Data de fim (YYYY-MM-DD)
        """
        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')

        historical_data = self.data.get_history_interval(
            assets=self.assets, start_date=start_date, end_date=end_date
        )

        for asset_data in historical_data:
            asset = asset_data["symbol"]
            self.history_data[asset] = asset_data["data"]

        print("Data loaded successfully.")

    def get_assets(self) -> List[str]:
        """
        Returns the list of assets in memory.

        :return: List of asset symbols.
        """
        return list(self.assets)

    def get_all_history(self) -> Dict[str, pd.DataFrame]:
        """
        Returns all stored historical data.

        :return: Dictionary with asset symbols as keys and dataframes as values.
        """
        return self.history_data
    
    def get_sector_info(self, symbol: str) -> Dict[str, str]:
        """
        Retorna informações de setor para um símbolo específico.
        
        :param symbol: Símbolo do ativo
        :return: Dict com 'sector' e 'industry'
        """
        return self.sector_data.get(symbol, {
            'sector': 'Unknown',
            'industry': 'Unknown'
        })
    
    def get_all_sectors(self) -> Dict[str, Dict[str, str]]:
        """
        Retorna todas as informações de setor carregadas do CSV.
        
        :return: Dict {symbol: {sector, industry}}
        """
        return self.sector_data

def teste():
    '''Test function'''
    print('--------------Atualizando ativos (deve descomentar a linha abaixo)----------------')
    # Data.update_symbols(update=True)

    print('--------------Listando ativos----------------')
    assets = Data.list_symbols()
    print(assets)

    print('--------Baixando histórico de ativos (deve descomentar a linha abaixo)---------')
    # Data.download_history(assets)

    print('--------------Buscando histórico de 10 ativos----------------')
    print(Data.fetch_history(assets=assets[:10]))

    print('--------------Buscando historico de um ativo----------------')
    print(Data.fetch_history(assets=['EQPA3.SA']))


def teste_sp500():
    '''Test function'''
    print('--------------Listando ativos----------------')
    market_data = MarketData("IFIX")
    assets = market_data.list_recent_symbols(
        market_data.market, force_update=True)
    print(assets)
    # Data.download_history(assets)
    # print('--------------Buscando histórico de 10 ativos----------------')
    # print(Data.fetch_history(assets=assets[:5]))

    # print('--------------Buscando informação de ativos----------------')
    # print(Data.get_asset_info(assets[:10]))
    # print(Data.get_asset_info("AZUL4.SA"))


def teste_mem_data():
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

    print("informações de ativos ---- descomentar :")
    # print(mem_data.get_assets())

    print("Todas as informações----- descomentar:")
    # print(mem_data.get_all_info())


if __name__ == "__main__":
    teste_mem_data()
