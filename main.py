from backtesting import Backtesting
from ranker import MARanker, RSIRanker
from data import Data
from markets import MarketData, list_recent_symbols
from utils import generate_filename
import argparse
import pandas as pd
import json
import os


def _calc_allocation(entry):
    """Calculate total portfolio allocation (cash + invested value)."""
    portfolio_total = entry['portfolio'].get('valor_total', 0.0)

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

def _ensure_market_assets(market_code: str = 'SP500'):
    """
    Ensure all market assets have downloaded historical data.
    
    Attempts to download missing asset histories up to max_retries times.
    Prints status messages and lists any assets with persistent missing data.
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
                df = Data.load_dataframe(f'{asset}.csv')
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
    interval = ['2024-01-01', '2024-12-31']
    _ensure_market_assets('SP500')

    backtester = Backtesting(
        MARanker,
        capital=10000,
        interval=interval,
        market_identifier='SP500',
    )

    ranker_grid = {'window': [[9, 21], [20, 50], [50, 200]]}
    runner_grid = {
        'profit': [0.1, 0.15],
        'loss': [0.05],
        'diversification': [0.1, 0.2],
        'volume': [0.1]
    }

    results = backtester.run(runner_grid, ranker_grid=ranker_grid, n_jobs=1)
    _print_df_full(results)


def run_backtest_rsi():
    """Run backtest with RSI (Relative Strength Index) ranker."""
    interval = ['2024-01-01', '2024-12-31']
    _ensure_market_assets('SP500')

    backtester = Backtesting(
        RSIRanker,
        capital=10000,
        interval=interval,
        market_identifier='SP500',
    )

    ranker_grid = {
        'window': [[9, 9], [14, 14], [21, 21]],
        'oversold': [30],
        'overbought': [70],
        'mode': ['mean_reversion'],
    }
    runner_grid = {
        'profit': [0.1, 0.15],
        'loss': [0.05],
        'diversification': [0.1, 0.2],
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
    
    Returns:
        tuple: (runner_grid, ranker_grid) for backtesting iterations.
    """
    params = config.get('ranker-params') or {}

    runner_grid = {
        'profit': params.get('profit', []),
        'loss': params.get('loss', []),
        'diversification': params.get('diversification', []),
    }

    ranker_grid = {}
    for key, value in params.items():
        if key not in runner_grid:
            ranker_grid[key] = value

    return runner_grid, ranker_grid


def _get_ranker_class(name: str):
    """Get ranker class by name."""
    if name == 'MARanker':
        return MARanker
    if name == 'RSIRanker':
        return RSIRanker
    raise ValueError(f'Ranker "{name}" is not supported.')


def run_from_config(config_path: str, download_mode: str = 'missing'):
    """
    Run backtest from configuration file.
    
    Args:
        config_path: Path to configuration JSON file.
        download_mode: 'all' (rebuild universe), 'missing' (download missing only),
                      or 'none' (skip downloads).
    """
    config = _load_config(config_path)

    market_identifier = config.get('id', 'SP500')
    interval = config.get('interval', ['2024-01-01', '2024-12-31'])
    capital = config.get('capital', 10000)

    ranker_name = config.get('ranker', 'MARanker')
    ranker_cls = _get_ranker_class(ranker_name)

    runner_grid, ranker_grid = _build_grids(config)
    data_handler = Data(market=market_identifier)
    
    mode = (download_mode or 'missing').lower()
    if mode == 'all':
        assets = list_recent_symbols(market_identifier, force_update=True)
        data_handler.download_histories(assets)
    elif mode == 'missing':
        _ensure_market_assets(market_identifier)

    backtester = Backtesting(
        ranker_cls,
        capital=capital,
        interval=interval,
        market_identifier=market_identifier,
    )

    results = backtester.run(
        runner_grid,
        ranker_grid=ranker_grid,
        n_jobs=-1,
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
        choices=['all', 'missing', 'none'],
        default='missing',
        help='Download strategy: all (rebuild universe), missing (download only missing), '
             'or none (skip downloads)'
    )
    return parser.parse_args(argv)


def main(argv=None):
    """Entry point for the application."""
    args = _parse_args(argv)
    run_from_config(args.config, download_mode=args.download_data)


if __name__ == '__main__':
    main()
