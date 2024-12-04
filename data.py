# -*- coding: utf-8 -*-

'''
Data class
'''

from datetime import datetime
from typing import Dict, List, Optional
import tempfile
import time
import zipfile
import pandas as pd
import requests
import yfinance as yf
from tqdm import tqdm
from files import open_dataframe, open_json, save_dataframe, save_json

SUB_DIR_HIST = "historical"

PAUSE = 1
TIMEOUT = 1
MAX_ATTEMPTS = 5

URL_QUOTE = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M'

SUB_DIR_B3 = 'b3'

RECENT_ASSETS_FILE = 'recent_assets.json'

VALID_ASSETS = 'valid_assets.json'


def previous_month(date):
    '''Returns the previous month of a given date'''
    if date.month > 1:
        date = date.replace(month=date.month - 1)
    else:
        date = date.replace(year=date.year - 1, month=12)
    return date


class AssetHistory:
    '''Asset history management'''
    _recent_assets_file = RECENT_ASSETS_FILE
    _url = URL_QUOTE
    _valid_assets_file = VALID_ASSETS

    subdir = SUB_DIR_HIST

    @classmethod
    def _download_quote(cls, year, month):
        '''Download ZIP file containing historical quotes'''
        url = cls._url + str(month).zfill(2) + str(year) + '.ZIP'
        downloaded_file = tempfile.mktemp()
        wait_time = 1
        attempts = 0

        while attempts < 10:
            try:
                response = requests.get(
                    url, stream=True, timeout=TIMEOUT, verify=False)
                with open(downloaded_file, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
                break
            except Exception as error:  # pylint: disable=broad-except
                print('Error:', error.__class__.__name__)
                print(error)
                wait_time *= 2
                time.sleep(wait_time)

        return downloaded_file

    @classmethod
    def download_symbols(cls):
        '''Download symbols for recent trades (previous month)'''
        prev_month = previous_month(datetime.now())
        temp_dir = tempfile.mkdtemp()
        file = cls._download_quote(prev_month.year, prev_month.month)

        with zipfile.ZipFile(file) as zip_ref:
            zip_ref.extractall(temp_dir)
        file = temp_dir + '/COTAHIST_M' + \
            str(prev_month.month).zfill(2) + str(prev_month.year) + '.TXT'
        symbols_set = set()

        with open(file, 'r', encoding='utf-8') as f:
            next(f)
            for line in f:
                if line[24:27] == '010':
                    symbol = line[12:24].strip() + '.SA'
                    symbols_set.add(symbol)

        save_json(cls._recent_assets_file, list(symbols_set), SUB_DIR_B3)

        return list(symbols_set)

    @classmethod
    def list_recent_symbols(cls, force_update=False):
        '''Returns a list of assets'''
        item_list = open_json(cls._recent_assets_file, SUB_DIR_B3)

        if item_list is None or len(item_list) == 0 or force_update:
            symbols = cls.download_symbols()
            cls.download_info(symbols)

        item_list = open_json(cls._recent_assets_file, SUB_DIR_B3)

        return item_list

    @classmethod
    def remove_symbols(cls, symbol_list):
        '''Remove symbols not included in the provided list'''
        current_list = cls.list_recent_symbols()
        current_list = [
            symbol for symbol in current_list if symbol in symbol_list]

        save_json(cls._recent_assets_file, current_list, SUB_DIR_B3)
        return current_list

    @classmethod
    def download_info(cls, symbols: List[str]) -> List[str]:
        '''Download information for the given list of assets'''
        desired_fields = [
            'sector', 'industry', 'financialCurrency']

        print(f"Downloading information for {len(symbols)}")

        asset_info = yf.Tickers(symbols)
        assets_with_info = []

        with tqdm(total=len(asset_info.tickers.keys()),
                  desc="Downloading information", unit="asset") as pbar:

            for asset in asset_info.tickers.keys():
                time.sleep(0.2)
                try:
                    asset_info_dict = asset_info.tickers[asset].info

                    filtered_info = {field: asset_info_dict.get(
                        field) for field in desired_fields}

                    if all(filtered_info.get(field) not in [None, '']
                           and pd.notna(filtered_info.get(field)) for field in desired_fields):

                        assets_with_info.append(asset)
                        json_file_name = f"{asset}_info.json"

                        save_json(json_file_name, filtered_info, cls.subdir)

                        csv_file_name = f"{asset}_info.csv"
                        save_dataframe(csv_file_name, pd.DataFrame(
                            [filtered_info]), cls.subdir)
                    else:
                        print(f"Skipping {asset}, incomplete information")

                    pbar.update(1)
                except (requests.exceptions.RequestException, KeyError, ValueError) as e:
                    print(f"Error processing {asset}: {e}")
                    continue

        cls.remove_symbols(assets_with_info)
        return assets_with_info


def update_symbols(update=False):
    '''Update the list of symbols'''
    AssetHistory.list_recent_symbols(force_update=update)


def get_symbol_list():
    '''Return the list of symbols'''
    return AssetHistory.list_recent_symbols()


class Yahoo:
    '''Yahoo Finance data management'''
    subdir = SUB_DIR_HIST

    @classmethod
    def download_histories(cls, assets: List[str]):
        '''Download historical data for all assets in the list'''
        tickers = yf.Tickers(assets)

        with tqdm(total=len(tickers.tickers.keys()), desc="Downloading data", unit="asset") as pbar:

            for asset in tickers.tickers.keys():
                asset_data = tickers.tickers[asset].history(period="max")

                if not asset_data.empty:
                    asset_data_reset = asset_data.reset_index()
                    asset_data_reset['Date'] = asset_data_reset['Date'].astype(
                        str)
                    data_dict = asset_data_reset.to_dict(orient='records')

                    json_file_name = f"{asset}.json"
                    save_json(json_file_name, data_dict, cls.subdir)

                    csv_file_name = f"{asset}.csv"
                    save_dataframe(
                        csv_file_name, asset_data_reset, cls.subdir)
                pbar.update(1)

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
    def download_historical_data(cls, assets: List[str], start_date: str, end_date: str):
        '''Download historical data for a specified date range'''
        tickers = yf.Tickers(assets)

        for asset in tickers.tickers.keys():
            asset_data = tickers.tickers[asset].history(
                start=start_date, end=end_date)

            if not asset_data.empty:
                asset_data_reset = asset_data.reset_index()
                asset_data_reset['Date'] = asset_data_reset['Date'].astype(str)

                csv_file_name = f"{asset}.csv"
                save_dataframe(csv_file_name, asset_data_reset, cls.subdir)

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

        for asset in historical_data:
            symbol = asset["symbol"]
            data = asset["data"]

            data["Date"] = pd.to_datetime(
                data["Date"], utc=True).dt.tz_localize(None)

            start_date_dt = pd.to_datetime(
                start_date, utc=True).tz_localize(None)
            end_date_dt = pd.to_datetime(end_date, utc=True).tz_localize(None)

            filtered_data = data[
                (data["Date"] >= start_date_dt) & (data["Date"] <= end_date_dt)
            ]

            if not filtered_data.empty:
                if column_filter and column_filter in filtered_data.columns:
                    filtered_data = filtered_data[["Date", column_filter]]

                result.append({
                    "symbol": symbol,
                    "data": filtered_data.reset_index(drop=True)
                })

        return result


def teste():
    '''Test function'''
    print('--------------Atualizando ativos (deve descomentar a linha abaixo)----------------')
    # Data.update_symbols(update=True)

    print('--------------Listando ativos----------------')
    ativos = Data.list_symbols()
    print(ativos)

    print('--------Baixando histórico de ativos (deve descomentar a linha abaixo)---------')
    # Data.download_history(assets=Data.list_symbols())

    print('--------------Buscando histórico de 10 ativos----------------')
    print(Data.fetch_history(assets=ativos[:10]))

    print('--------------Baixando historico de um ativo----------------')
    print(Data.download_history(assets=['EQPA3.SA']))

    print('--------------Buscando historico de um ativo----------------')
    print(Data.fetch_history(assets=['EQPA3.SA']))


if __name__ == "__main__":
    teste()
