import time
from typing import List
import requests
from tqdm import tqdm
import yfinance as yf
import pandas as pd
from files import open_dataframe, open_json, save_json, save_dataframe

RECENT_ASSETS_FILE = 'recent_assets_sp500.json'
SUB_DIR_SP500 = 'sp500'

csv_file = 's&p500.csv'
SUB_DIR_HIST = "historical"


class SP500Data:
    '''S&P 500 data management'''
    csv_file = csv_file

    def __init__(self, csv_file=csv_file):
        self.csv_file = csv_file
        self.df = None

    @classmethod
    def download_data(cls):
        cls.df = pd.read_csv(cls.csv_file)

        cls.df = cls.df.dropna(subset=['GICS Sector'])
        cls.df = cls.df[['Symbol', 'GICS Sector']]

        symbols = cls.df['Symbol'].tolist()

        save_json(RECENT_ASSETS_FILE, symbols, SUB_DIR_SP500)

        return symbols

    @classmethod
    def list_recent_symbols(cls, force_update=False):
        item_list = open_json(RECENT_ASSETS_FILE, SUB_DIR_SP500)

        if item_list is None or len(item_list) == 0 or force_update:
            symbols = cls.download_data()
            cls.download_info(symbols)

        item_list = open_json(RECENT_ASSETS_FILE, SUB_DIR_SP500)
        return item_list

    @classmethod
    def get_info(cls, symbol: str):
        '''Get information for the given symbol'''
        csv_file_name = f"{symbol}_info.csv"
        print(csv_file_name)
        return open_dataframe(csv_file_name, SUB_DIR_HIST)

    # TODO: COLOCAR NUMA UTILS
    @classmethod
    def remove_symbols(cls, symbol_list):
        '''Remove symbols not included in the provided list'''
        current_list = cls.list_recent_symbols()
        current_list = [
            symbol for symbol in current_list if symbol in symbol_list]

        save_json(RECENT_ASSETS_FILE, current_list, SUB_DIR_SP500)
        return current_list

    @classmethod
    def download_info(cls, symbols: List[str]) -> List[str]:
        """Download information for the given list of assets."""
        desired_fields = {'sector', 'industry'}
        asset_info = yf.Tickers(symbols)
        assets_with_info = []

        with tqdm(total=len(asset_info.tickers), desc="Downloading information", unit="asset") as pbar:
            for asset, ticker_data in asset_info.tickers.items():
                time.sleep(0.1)
                try:
                    asset_info_dict = ticker_data.info
                    filtered_info = {field: asset_info_dict.get(
                        field) for field in desired_fields}

                    if all(filtered_info[field] for field in desired_fields):
                        assets_with_info.append(asset)

                        csv_file_name = f"{asset}_info.csv"
                        save_dataframe(csv_file_name, pd.DataFrame(
                            [filtered_info]), SUB_DIR_HIST)

                    pbar.update(1)
                except (requests.exceptions.RequestException, KeyError, ValueError) as e:
                    print(f"Error processing {asset}: {e}")
                    continue

        cls.remove_symbols(assets_with_info)
        return assets_with_info


def list_recent_symbols(force_update=False):
    return SP500Data.list_recent_symbols(force_update)


def teste():
    data = SP500Data()
    symbols = data.list_recent_symbols()
    # print(symbols)
    # for symbol in symbols:
    #     print(data.get_info(symbol))


if __name__ == '__main__':
    teste()
