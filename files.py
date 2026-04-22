# -*- coding: utf-8 -*-

'''
File Management Module

Handles directory structure creation and JSON/CSV file operations
for backtesting data persistence and configuration loading.
'''

import json
from os.path import isdir, isfile
from os import mkdir, sep
from pathlib import Path
import pandas as pd
from names import DIR_CACHE, PRICE_COLUMNS, DECIMAL_PLACES


def dir_cache():
    '''
    Returns path to market data cache directory.
    
    Creates directory in user home folder if it does not exist yet.
    Uses DIR_CACHE constant from names module for directory name.
    
    :return: Absolute path to cache directory
    '''
    _data_dir = str(Path.home()) + sep + DIR_CACHE
    if not isdir(_data_dir):
        mkdir(_data_dir)
    return _data_dir


def file_path(file_name, subdir=None):
    '''
    Generates full path for cached file.
    
    Creates subdirectory structure if needed.
    
    :param file_name: Name of the file
    :param subdir: Optional subdirectory within cache folder
    :return: Full path to file
    '''
    directory = dir_cache()
    if subdir:
        directory += sep + subdir
    if not isdir(directory):
        mkdir(directory)
    file_name = directory + sep + file_name
    return file_name


def open_json(file, subdir=None):
    '''
    Loads JSON file from cache directory.
    
    :param file: JSON filename
    :param subdir: Optional subdirectory within cache folder
    :return: Parsed JSON data if file exists, None otherwise
    '''
    file_name = file_path(file, subdir)
    if isfile(file_name):
        with open(file_name, 'r', encoding='utf-8') as file:
            return json.load(file)
    return None

def save_json(file, content, subdir=None):
    '''
    Persists data structure to JSON file in cache directory.
    
    :param file: JSON filename
    :param content: Data to serialize (dict or list)
    :param subdir: Optional subdirectory within cache folder
    '''
    file_name = file_path(file, subdir)
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(content, f, indent=2, ensure_ascii=False)


def open_dataframe(file, subdir=None):
    '''
    Loads CSV file as DataFrame with price normalization.
    
    Automatically rounds OHLCV price columns to 2 decimal places
    for consistency with backtesting calculations.
    
    :param file: CSV filename
    :param subdir: Optional subdirectory within cache folder
    :return: Pandas DataFrame if file exists, None otherwise
    '''
    file_name = file_path(file, subdir)
    if isfile(file_name):
        df = pd.read_csv(file_name, index_col=False)
        
        # Round only price columns to 2 decimal places
        existing_price_cols = [col for col in PRICE_COLUMNS if col in df.columns]
        
        if existing_price_cols:
            df[existing_price_cols] = df[existing_price_cols].round(DECIMAL_PLACES)
        
        return df
    return None


def save_dataframe(file, dataframe, subdir=None):
    '''Saves DataFrame to a CSV file'''
    file_name = file_path(file, subdir)
    dataframe.to_csv(file_name, index=False)


def main():
    '''Main function'''
    print(file_path('test.txt'))


if __name__ == '__main__':
    main()
