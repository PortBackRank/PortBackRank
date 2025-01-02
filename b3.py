'''
B3 module
'''

from datetime import datetime
from typing import List
import tempfile
import time
import zipfile
import pandas as pd
import requests
import yfinance as yf
from tqdm import tqdm
from files import open_json, save_dataframe, save_json


SUB_DIR_HIST = "historical"
TIMEOUT = 1
MAX_ATTEMPTS = 5

URL_QUOTE = 'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_M'

SUB_DIR_B3 = 'b3'

RECENT_ASSETS_FILE = 'recent_assets.json'


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
            'sector', 'industry']

        print(f"Downloading information for {len(symbols)}")

        asset_info = yf.Tickers(symbols)
        assets_with_info = []

        with tqdm(total=len(asset_info.tickers.keys()),
                  desc="Downloading information", unit="asset") as pbar:

            for asset in asset_info.tickers.keys():
                time.sleep(0.18)
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
