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

SUB_DIR_HIST = "historical"


class Yahoo:
    '''Yahoo Finance data management'''
    subdir = SUB_DIR_HIST

    @classmethod
    def download_histories(cls, assets: List[str], end_date: Optional[str] = None) -> None:
        '''Download historical data for all assets in the list'''
        tickers = yf.Tickers(assets)

        with tqdm(total=len(tickers.tickers.keys()), desc="Downloading data", unit="asset") as pbar:
            def download_asset_data(asset):
                asset_data = tickers.tickers[asset].history(period="max")

                if not asset_data.empty:
                    asset_data_reset = asset_data.reset_index()
                    asset_data_reset['Date'] = asset_data_reset['Date'].astype(
                        str)
                    data_dict = asset_data_reset.to_dict(orient='records')

                    json_file_name = f"{asset}.json"
                    save_json(json_file_name, data_dict, cls.subdir)

                    csv_file_name = f"{asset}.csv"
                    save_dataframe(csv_file_name, asset_data_reset, cls.subdir)

                pbar.update(1)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(download_asset_data, tickers.tickers.keys())

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
    def get_info(cls, assets: List[str]) -> List[pd.DataFrame]:
        '''Load information about one or more assets'''
        info_data = []
        history = cls()
        for asset in assets:
            file_name = f"{asset}_info.csv"
            asset_data = history.load_dataframe(file_name)
            if not asset_data.empty:
                info_data.append(asset_data)
        return info_data

    @classmethod
    def load_dataframe(cls, file_name: str) -> pd.DataFrame:
        '''Load data from a CSV file'''
        return open_dataframe(file_name, cls.subdir)

    @classmethod
    def get_asset_data_by_name(cls, asset: str) -> pd.DataFrame:
        '''Get historical data for a specific asset'''
        file_name = f"{asset}.csv"
        return cls.load_dataframe(file_name)


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
    def get_asset_info(cls, symbols: List[str]) -> List[pd.DataFrame]:
        """Returns information for the assets in the given list of symbols."""
        return cls.get_info(assets=symbols)

    @classmethod
    def download_history(cls, assets: List[str], end_date: Optional[str] = None) -> None:
        """Downloads historical data for the given list of assets."""
        cls.download_histories(assets=assets, end_date=end_date)

    @classmethod
    def fetch_history(cls, assets: List[str]) -> List[dict]:
        """
        Fetches and concatenates historical data for the given list of assets.
        Now returns a list of dictionaries with symbol
        as the key and its corresponding dataframe as the value.
        """
        asset_data_list = cls.get_asset_data(assets=assets)

        if asset_data_list:
            result = []
            for asset, data in zip(assets, asset_data_list):
                result.append({"symbol": asset, "data": data})
            return result
        else:
            return []

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

    def __init__(self, interval: List[str]):
        self.history_data: Dict[str, pd.DataFrame] = {}
        self.info_data: Dict[str, pd.DataFrame] = {}
        self.data = Data()
        self.assets = self.data.list_symbols()

        start_date, end_date = interval
        self.load(start_date, end_date)

    def load(self, start_date: str, end_date: str):
        """
        Loads historical data and asset information into memory.

        :param assets: List of assets to load. If empty, loads all assets.
        :param data: Instance of the Data class to fetch the data.
        :param interval: List of two strings representing the start and end dates
        """

        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')

        historical_data = self.data.get_history_interval(
            assets=self.assets, start_date=start_date, end_date=end_date)

        for asset_data in historical_data:
            asset = asset_data["symbol"]
            self.history_data[asset] = asset_data["data"]

        info_data = self.data.get_asset_info(self.assets)
        for asset, data in zip(self.assets, info_data):
            self.info_data[asset] = data
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

    def get_all_info(self) -> Dict[str, pd.DataFrame]:
        """
        Returns all stored asset information.

        :return: Dictionary with asset symbols as keys and dataframes as values.
        """
        return self.info_data


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


def teste_mem_data():
    interval = ["2024-01-10", "2024-11-10"]
    mem_data = MemData(interval)

    print("Todos os dados históricos:")
    todas_info = mem_data.get_all_history()

    print(todas_info.get('EQPA3.SA'))

    print("Todas as informações:")
    print(mem_data.get_all_info())


if __name__ == "__main__":
    teste()
