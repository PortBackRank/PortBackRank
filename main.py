from backtesting import Backtesting
from ranker import MARanker, RSIRanker
from data import Data
from markets import MarketData, list_recent_symbols
from utils import generate_filename
import argparse
import pandas as pd
import json
import os
from names import (
    MARKET_SP500, RANKER_MA, RANKER_RSI, MODE_ALL, MODE_MISSING, MODE_NONE,
    KEY_ID, KEY_INTERVAL, KEY_CAPITAL, KEY_RANKER, KEY_RANKER_PARAMS,
    KEY_PROFIT, KEY_LOSS, KEY_DIVERSIFICATION, KEY_VOLUME,
    DEFAULT_INTERVAL, DEFAULT_CAPITAL, MARKET_CUSTOM_TESTE
)


def _calc_allocation(entry):
    """Calculate total portfolio allocation (cash + invested value)."""
    return entry.get('final_total_value', 0.0)


def _print_df_full(df: pd.DataFrame):
    """Display entire DataFrame without truncation."""
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
            print("Aviso: O DataFrame de resultados está vazio ou é nulo.")
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

def _ensure_market_assets(market_code: str = MARKET_SP500):
    """
    Ensure all market assets have downloaded historical data.
    """
    market_data = MarketData(market_code)
    assets = market_data.list_recent_symbols(market_data.market, force_update=False)

    data_handler = Data(market=market_code)
    max_retries = 3
    missing = []

    for attempt in range(1, max_retries + 1):
        missing = []
        for asset in assets:
            try:
                df = data_handler.load_dataframe(f'{asset}.csv')
            except (FileNotFoundError, pd.errors.EmptyDataError):
                df = None

            if not isinstance(df, pd.DataFrame) or df.empty:
                missing.append(asset)

        if not missing:
            print('All assets have local data.')
            break

        print(
            f'Attempt {attempt}/{max_retries}: '
            f'downloading {len(missing)} assets without local data.'
        )
        print('Missing assets:', ', '.join(missing))
        data_handler.download_histories(missing)
    
    if missing:
        print('After download attempts, historical data is still missing for:')
        for asset in missing:
            print(f'  - {asset}')


def run_backtest_ma():
    """Run backtest with Moving Average ranker."""
    interval = DEFAULT_INTERVAL
    _ensure_market_assets(MARKET_SP500)

    backtester = Backtesting(
        _get_ranker_class(RANKER_MA),
        capital=DEFAULT_CAPITAL,
        interval=interval,
        market_identifier=MARKET_SP500,
    )

    ranker_grid = {KEY_WINDOW: [9, 21]}
    runner_grid = {
        KEY_PROFIT: [0.1, 0.15],
        KEY_LOSS: [0.05],
        KEY_DIVERSIFICATION: [0.1, 0.2],
        KEY_VOLUME: [0.1]
    }

    results = backtester.run(runner_grid, ranker_grid=ranker_grid, n_jobs=1)
    _print_df_full(results)


def run_backtest_rsi():
    """Run backtest with RSI (Relative Strength Index) ranker."""
    interval = DEFAULT_INTERVAL
    _ensure_market_assets(MARKET_SP500)

    backtester = Backtesting(
        _get_ranker_class(RANKER_RSI),
        capital=DEFAULT_CAPITAL,
        interval=interval,
        market_identifier=MARKET_SP500,
    )

    ranker_grid = {
        KEY_WINDOW: [[9, 9], [14, 14], [21, 21]],
        'oversold': [30],
        'overbought': [70],
        'mode': ['mean_reversion'],
    }
    runner_grid = {
        KEY_PROFIT: [0.1, 0.15],
        KEY_LOSS: [0.05],
        KEY_DIVERSIFICATION: [0.1, 0.2],
    }

    results = backtester.run(runner_grid, ranker_grid=ranker_grid, n_jobs=-1)
    _print_df_full(results)


def _load_config(path: str) -> dict:
    """Load configuration from JSON file."""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _build_grids(config: dict):
    """
    Build parameter grids for backtesting from configuration.
    """
    params = config.get(KEY_RANKER_PARAMS) or {}

    runner_grid = {
        KEY_PROFIT: params.get(KEY_PROFIT, []),
        KEY_LOSS: params.get(KEY_LOSS, []),
        KEY_DIVERSIFICATION: params.get(KEY_DIVERSIFICATION, []),
        KEY_VOLUME: params.get(KEY_VOLUME, [1.0]),
    }

    ranker_grid = {}
    for key, value in params.items():
        if key not in runner_grid:
            ranker_grid[key] = value

    return runner_grid, ranker_grid


def _get_ranker_class(name: str):
    """Get ranker class by name."""
    if name == RANKER_MA:
        return MARanker
    if name == RANKER_RSI:
        return RSIRanker
    raise ValueError(f'Ranker "{name}" is not supported.')


def run_from_config(config_path: str, download_mode: str = MODE_MISSING, price_type: str = None, trace: bool = False):
    """
    Run backtest from configuration file.
    """
    config = _load_config(config_path)

    market_identifier = config.get(KEY_ID, MARKET_CUSTOM_TESTE)
    interval = config.get(KEY_INTERVAL, ['2024-01-01', '2024-06-30'])
    capital = config.get(KEY_CAPITAL, DEFAULT_CAPITAL)

    ranker_name = config.get(KEY_RANKER, RANKER_MA)
    ranker_cls = _get_ranker_class(ranker_name)

    if price_type is None:
        price_type = config.get('price-type', 'C')

    runner_grid, ranker_grid = _build_grids(config)
    data_handler = Data(market=market_identifier)
    
    mode = (download_mode or MODE_MISSING).lower()
    if mode == MODE_ALL:
        assets = list_recent_symbols(data_handler.market_data.market, force_update=True)
        data_handler.download_histories(assets)
    elif mode == MODE_MISSING:
        _ensure_market_assets(market_identifier)

    n_jobs = config.get('n_jobs', 1)

    backtester = Backtesting(
        ranker_cls,
        capital=capital,
        interval=interval,
        market_identifier=market_identifier,
        price_type=price_type,
        trace=trace
    )

    results = backtester.run(
        runner_grid,
        ranker_grid=ranker_grid,
        n_jobs=n_jobs,
    )
    _print_df_full(results)


def _parse_args(argv=None):
    """Parse command-line arguments."""
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
        help='Download strategy: all (rebuild universe), missing (download only missing), '
             'or none (skip downloads)'
    )
    parser.add_argument(
        '-p', '--price-type',
        choices=['O', 'H', 'L', 'C'],
        default=None,
        help='Price type to use for backtesting (default: from config)'
    )
    parser.add_argument(
        '-t', '--trace',
        action='store_true',
        help='Enable trace mode for detailed output'
    )
    return parser.parse_args(argv)


def main(argv=None):
    """Entry point for the application."""
    args = _parse_args(argv)
    run_from_config(args.config, download_mode=args.download_data, price_type=args.price_type, trace=args.trace)


if __name__ == '__main__':
    main()
