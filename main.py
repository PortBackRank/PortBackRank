'''
Main application entry point for PortBackRank.
'''

import argparse
import json
import time
import pandas as pd
from backtesting import run_backtests_for_price_types
from data import Data, read_symbols
from logger import configure_logging
from names import *


def _print_df_full(df: pd.DataFrame):
    '''Display entire DataFrame without truncation.'''
    try:
        old_max_cols = pd.get_option('display.max_columns')
        old_width = pd.get_option('display.width')
        old_max_colwidth = None
        try:
            old_max_colwidth = pd.get_option('display.max_colwidth')
        except Exception:
            pass

        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        try:
            pd.set_option('display.max_colwidth', None)
        except Exception:
            pd.set_option('display.max_colwidth', 0)
        
        if df is None or df.empty:
            print('Aviso: O DataFrame de resultados está vazio ou é nulo.')
            return

        print(df.to_string(index=False))
    finally:
        try:
            pd.set_option('display.max_columns', old_max_cols)
            pd.set_option('display.width', old_width)
            if old_max_colwidth is not None:
                pd.set_option('display.max_colwidth', old_max_colwidth)
        except Exception:
            pass


def _load_config(path: str) -> dict:
    '''Load configuration from JSON file.'''
    with open(path, 'r', encoding=ENCODING_UTF8) as f:
        return json.load(f)


def run_from_config(config_path: str, download_mode: str = MODE_MISSING, trace: bool = False, data_dir: str = None):
    '''
    Run backtest from configuration file. Supports single or multiple price types.
    '''
    config = _load_config(config_path)

    # Apply data directory override (CLI > JSON > default)
    data_dir_override = data_dir or config.get('data-dir')
    if data_dir_override:
        set_base_dir_override(data_dir_override)
        print(f'Using custom data directory: {data_dir_override}')

    assets_source = config.get(KEY_ASSETS, config.get(KEY_ID, MARKET_SP500))
    data_handler = Data()
    
    mode = (download_mode or MODE_MISSING).lower()
    if mode == MODE_ALL:
        assets = read_symbols(assets_source)
        data_handler.download_histories(assets)
    elif mode == MODE_MISSING:
        data_handler.ensure_assets(assets_source)

    final_results = run_backtests_for_price_types(
        config=config,
        trace=trace,
        assets_path=assets_source
    )

    _print_df_full(final_results)


def _parse_args(argv=None):
    '''Parse command-line arguments.'''
    parser = argparse.ArgumentParser(
        description='PortBackRank - Portfolio backtesting with technical indicators'
    )
    parser.add_argument(
        '-c', '--config',
        required=True,
        help='Path to configuration JSON file'
    )
    parser.add_argument(
        '-d', '--download-data',
        choices=[MODE_ALL, MODE_MISSING, MODE_NONE],
        default=MODE_MISSING,
        help='Download strategy: all (rebuild universe), missing (download only missing), or none (skip downloads)'
    )
    parser.add_argument(
        '--data-dir',
        default=None,
        help='Base directory for cache and tracking files (default: $HOME/.cache/PortBackRank)'
    )
    parser.add_argument(
        '-t', '--trace',
        action='store_true',
        help='Enable trace mode for detailed output'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    return parser.parse_args(argv)


def main(argv=None):
    '''Entry point for the application.'''
    args = _parse_args(argv)
    
    # Configure logging based on debug flag
    configure_logging(debug=args.debug)
    
    start = time.time()
    run_from_config(
        args.config, 
        download_mode=args.download_data,
        trace=args.trace, 
        data_dir=args.data_dir
    )
    end = time.time()
    print('Time:', end - start)


if __name__ == '__main__':
    main()
