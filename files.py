# -*- coding: utf-8 -*-

'''
Files
'''

import json
from os.path import isdir, isfile
from os import mkdir, sep
from pathlib import Path
import pandas as pd
from names import DIR_CACHE


def dir_cache():
    '''Cache directory'''
    _data_dir = str(Path.home()) + sep + DIR_CACHE
    if not isdir(_data_dir):
        mkdir(_data_dir)
    return _data_dir


def file_path(file_name, subdir=None):
    '''File path'''
    directory = dir_cache()
    if subdir:
        directory += sep + subdir
    if not isdir(directory):
        mkdir(directory)
    file_name = directory + sep + file_name
    return file_name


def open_json(file, subdir=None):
    '''Opens JSON file'''
    file_name = file_path(file, subdir)
    if isfile(file_name):
        with open(file_name, 'r', encoding='utf-8') as file:
            return json.load(file)
    return None


def save_json(file, content, subdir=None):
    '''Saves JSON file'''
    file_name = file_path(file, subdir)
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(content, f, indent=2, ensure_ascii=False)


def open_dataframe(file, subdir=None):
    '''Opens CSV file as a DataFrame and rounds price columns to 2 decimal places'''
    file_name = file_path(file, subdir)
    if isfile(file_name):
        df = pd.read_csv(file_name, index_col=False)
        
        # Round only price columns to 2 decimal places
        price_columns = ['Open', 'High', 'Low', 'Close']
        existing_price_cols = [col for col in price_columns if col in df.columns]
        
        if existing_price_cols:
            df[existing_price_cols] = df[existing_price_cols].round(2)
        
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
