# -*- coding: utf-8 -*-

'''
Files
'''

import json
from os.path import isfile
from pathlib import Path
import pandas as pd

DIR_CACHE = Path('.cache') / 'port_back'


def dir_cache():
    '''Data directory'''
    data_dir = Path.home() / DIR_CACHE
    data_dir.mkdir(parents=True, exist_ok=True)
    return str(data_dir)


def file_path(file_name, subdir=None):
    '''Symbol file'''
    directory = Path(dir_cache())
    if subdir:
        directory = directory / subdir
    directory.mkdir(parents=True, exist_ok=True)
    file_name = directory / file_name
    return str(file_name)


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
    '''Opens CSV file as a DataFrame'''
    file_name = file_path(file, subdir)
    if isfile(file_name):
        return pd.read_csv(file_name, index_col=False)
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
