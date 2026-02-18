# -*- coding: utf-8 -*-

"""
File management utilities for handling JSON and CSV operations.

This module provides functions to manage cache directories and perform
read/write operations on JSON and CSV files.
"""

import json
from os.path import isdir, isfile
from os import mkdir, sep
from pathlib import Path
import pandas as pd
from names import DIR_CACHE


def dir_cache():
    """
    Get or create the cache directory.
    
    Returns:
        str: Path to the cache directory.
    """
    data_dir = str(Path.home()) + sep + DIR_CACHE
    if not isdir(data_dir):
        mkdir(data_dir)
    return data_dir


def file_path(file_name, subdir=None):
    """
    Get the full file path, creating subdirectories if necessary.
    
    Args:
        file_name (str): Name of the file.
        subdir (str, optional): Subdirectory name. Defaults to None.
    
    Returns:
        str: Full path to the file.
    """
    directory = dir_cache()
    if subdir:
        directory += sep + subdir
    if not isdir(directory):
        mkdir(directory)
    return directory + sep + file_name


def open_json(file, subdir=None):
    """
    Load and parse a JSON file.
    
    Args:
        file (str): Filename to read.
        subdir (str, optional): Subdirectory name. Defaults to None.
    
    Returns:
        dict or None: Parsed JSON content, or None if file doesn't exist.
    """
    file_name = file_path(file, subdir)
    if isfile(file_name):
        with open(file_name, 'r', encoding='utf-8') as file:
            return json.load(file)
    return None


def save_json(file, content, subdir=None):
    """
    Write data to a JSON file with formatted indentation.
    
    Args:
        file (str): Filename to write.
        content (dict): Data to serialize.
        subdir (str, optional): Subdirectory name. Defaults to None.
    """
    file_name = file_path(file, subdir)
    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(content, f, indent=2, ensure_ascii=False)


def open_dataframe(file, subdir=None):
    """
    Load a CSV file as a DataFrame with price columns rounded to 2 decimals.
    
    Args:
        file (str): Filename to read.
        subdir (str, optional): Subdirectory name. Defaults to None.
    
    Returns:
        DataFrame or None: Pandas DataFrame with rounded prices, or None if file doesn't exist.
    """
    file_name = file_path(file, subdir)
    if isfile(file_name):
        df = pd.read_csv(file_name, index_col=False)
        
        # Round price columns to 2 decimal places
        price_columns = ['Open', 'High', 'Low', 'Close']
        existing_price_cols = [col for col in price_columns if col in df.columns]
        
        if existing_price_cols:
            df[existing_price_cols] = df[existing_price_cols].round(2)
        
        return df
    return None


def save_dataframe(file, dataframe, subdir=None):
    """
    Write a DataFrame to a CSV file.
    
    Args:
        file (str): Filename to write.
        dataframe (DataFrame): Pandas DataFrame to save.
        subdir (str, optional): Subdirectory name. Defaults to None.
    """
    file_name = file_path(file, subdir)
    dataframe.to_csv(file_name, index=False)


def main():
    """Entry point for testing file path generation."""
    print(file_path('test.txt'))


if __name__ == '__main__':
    main()
