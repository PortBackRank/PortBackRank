'''
Backtesting module

Contains the Backtesting class for running investment strategy simulations
by varying Runner and Ranker parameters, as well as utilities for saving
results to disk.
'''

import logging
from itertools import product
from typing import List, Dict, Union
from joblib import Parallel, delayed
import pandas as pd
from data import MemData
from runner import Runner
from names import *

logger = logging.getLogger(__name__)


def get_ranker_class(name: str):
    '''Get ranker class by name.'''
    from ranker import MARanker, RandomRanker
    if name == RANKER_MA:
        return MARanker
    if name == RANKER_RANDOM:
        return RandomRanker
    raise ValueError(f'Ranker "{name}" is not supported.')


def build_grids(config: dict):
    '''
    Build parameter grids for backtesting from configuration.
    '''
    runner_p = config.get(KEY_RUNNER_PARAMS) or config.get(KEY_RANKER_PARAMS) or {}
    ranker_p = config.get(KEY_RANKER_PARAMS) or {}

    runner_grid = {
        KEY_PROFIT: runner_p.get(KEY_PROFIT, []),
        KEY_LOSS: runner_p.get(KEY_LOSS, []),
        KEY_DIVERSIFICATION: runner_p.get(KEY_DIVERSIFICATION, []),
        KEY_VOLUME: runner_p.get(KEY_VOLUME, [1.0]),
    }

    ranker_grid = {}
    for key, value in ranker_p.items():
        if key not in runner_grid:
            ranker_grid[key] = value

    return runner_grid, ranker_grid


def _run_single_simulation(runner_config: dict, ranker_config: dict, context: dict):
    '''Run one parameter combination and return its evaluation row.

    Module-level function (not a method) so it can be pickled by joblib.
    All arguments are plain dicts (classes and MemData are picklable).

    Args:
        runner_config: Runner parameters for this combination
            (profit, loss, diversification, volume).
        ranker_config: Ranker parameters for this combination (e.g. window).
        context: simulation settings shared by every combination, using the
            CONF_* keys (ranker_cls, data, interval, capital,
            trace, decimals).

    Returns:
        Dictionary with the parameters and results, or None if the
        simulation raised an exception.
    '''
    capital = context[CONF_CAPITAL]
    logger.debug('Running simulation: runner=%s, ranker=%s', runner_config, ranker_config)

    runner_params = {
        KEY_PROFIT: runner_config[KEY_PROFIT],
        KEY_LOSS: runner_config[KEY_LOSS],
        KEY_DIVERSIFICATION: runner_config[KEY_DIVERSIFICATION],
        KEY_VOLUME: runner_config[KEY_VOLUME],
        CONF_RANKER_CLS: context[CONF_RANKER_CLS],
        CONF_TRACE: context[CONF_TRACE],
        CONF_DECIMALS: context[CONF_DECIMALS]
    }
    runner = Runner(config=runner_params, data=context[CONF_DATA])

    try:
        result = runner.single_run(context[CONF_INTERVAL], ranker_config, capital)
        # Evaluate results internally
        portfolio_value = result[KEY_FINAL_TOTAL_VALUE]
        total_return = ((portfolio_value - capital) / capital) * 100
        
        eval_dict = {
            KEY_INTERVALO: result[KEY_INTERVAL],
            KEY_PROFIT: runner_config[KEY_PROFIT],
            KEY_LOSS: runner_config[KEY_LOSS],
            KEY_DIVERSIFICATION: runner_config[KEY_DIVERSIFICATION],
            KEY_VOLUME: runner_config[KEY_VOLUME],
            **ranker_config,
            KEY_CAIXA_FINAL: result[KEY_BALANCE],
            KEY_PORTFOLIO_VALUE: portfolio_value,
            KEY_RETORNO_TOTAL: f'{total_return:.2f}%'
        }
        logger.debug('Simulation result: return=%.2f%%, final_value=%s', total_return, portfolio_value)
        return eval_dict
    except Exception as e:
        logger.exception('Error executing configuration %s: %s', runner_config, e)
        return None


class Backtesting:
    '''Class for running investment strategy backtests.

    The class organizes the execution of simulations varying Runner and Ranker
    parameters, allows parallel execution, and aggregates performance metrics.
    '''

    _REQUIRED_KEYS = [
        CONF_RANKER_CLS, CONF_CAPITAL, CONF_INTERVAL, CONF_DATA,
        CONF_TRACE, CONF_DECIMALS, CONF_N_JOBS
    ]
    _REQUIRED_RUNNER_KEYS = [
        KEY_PROFIT, KEY_LOSS, KEY_DIVERSIFICATION, KEY_VOLUME
    ]
    _REQUIRED_RANKER_KEYS = [
        KEY_WINDOW
    ]

    def __init__(self, config: dict):
        '''
        Initializes the backtester with a configuration dictionary.

        Required keys in config dict:
        - ranker_cls: Ranker class used in simulations.
        - capital: initial capital for all simulations.
        - interval: list [start_date, end_date] for the simulation.
        - data: MemData object with the simulation data.
        - trace: whether to generate tracking files.
        - decimals: number of decimal places for output.
        - n_jobs: number of parallel jobs.

        Raises:
            ValueError: if a required key is missing.
        '''
        missing = [key for key in self._REQUIRED_KEYS if key not in config]
        if missing:
            raise ValueError(f'Missing required config key(s): {", ".join(missing)}')

        self._config = {
            CONF_RANKER_CLS: config[CONF_RANKER_CLS],
            CONF_CAPITAL: config[CONF_CAPITAL],
            CONF_INTERVAL: config[CONF_INTERVAL],
            CONF_DATA: config[CONF_DATA],
            CONF_TRACE: config[CONF_TRACE],
            CONF_DECIMALS: validate_decimals(config[CONF_DECIMALS]),
            CONF_N_JOBS: config[CONF_N_JOBS]
        }
        
        logger.debug('Backtesting initialized: ranker=%s, capital=%d, interval=%s, decimals=%d', 
                     self.ranker_cls.__name__, self.capital, self.interval, self.decimals)

    @property
    def ranker_cls(self):
        '''Ranker class used in simulations (read-only).'''
        return self._config[CONF_RANKER_CLS]

    @property
    def capital(self) -> int:
        '''Initial capital (read-only).'''
        return self._config[CONF_CAPITAL]

    @property
    def interval(self) -> List[str]:
        '''Simulation interval [start_date, end_date] (read-only).'''
        return self._config[CONF_INTERVAL]

    @property
    def data(self) -> MemData:
        '''MemData with the simulation data (read-only).'''
        return self._config[CONF_DATA]

    @property
    def trace(self) -> bool:
        '''Whether tracking files are written (read-only).'''
        return self._config[CONF_TRACE]

    @property
    def runner_cls(self):
        '''Runner class used in simulations (always Runner).'''
        return Runner

    @property
    def decimals(self) -> int:
        '''Decimal places applied to output values (read-only).'''
        return self._config[CONF_DECIMALS]

    @property
    def n_jobs(self) -> int:
        '''Default number of parallel jobs (read-only).'''
        return self._config[CONF_N_JOBS]

    def run(
        self,
        parameter_grid: Dict[str, List[float]],
        ranker_grid: Dict[str, List[float]],
        n_jobs: int = None
    ) -> pd.DataFrame:
        '''
        Runs backtests varying Runner and Ranker parameters.

        Parameters:
        - parameter_grid: dictionary with Runner parameters and their value lists.
        - ranker_grid: dictionary with Ranker parameters (only window).
        - n_jobs: number of parallel jobs (-1 uses all available cores). Defaults to config value.

        Returns:
        - DataFrame with aggregated results by parameter combination.
        '''
        missing_runner = [k for k in self._REQUIRED_RUNNER_KEYS if k not in parameter_grid]
        if missing_runner:
            raise ValueError(f'Missing required runner parameter(s): {", ".join(missing_runner)}')

        missing_ranker = [k for k in self._REQUIRED_RANKER_KEYS if k not in ranker_grid]
        if missing_ranker:
            raise ValueError(f'Missing required ranker parameter(s): {", ".join(missing_ranker)}')

        if n_jobs is None:
            n_jobs = self.n_jobs
            
        runner_params = list(product(*parameter_grid.values()))
        ranker_params = list(product(*ranker_grid.values()))
        parameter_names = list(parameter_grid.keys())
        ranker_names = list(ranker_grid.keys())

        combinations = list(product(runner_params, ranker_params))
        
        logger.debug('Starting backtest: %d runner configs x %d ranker configs = %d total combinations (n_jobs=%d)',
                     len(runner_params), len(ranker_params), len(combinations), n_jobs)

        results = Parallel(n_jobs=n_jobs)(
            delayed(_run_single_simulation)(
                dict(zip(parameter_names, runner_values)),
                dict(zip(ranker_names, ranker_values)),
                self._config
            ) for runner_values, ranker_values in combinations
        )
        
        valid_results = [r for r in results if r is not None]
        logger.debug('Backtest complete: %d/%d configurations succeeded', len(valid_results), len(combinations))
        return pd.DataFrame(results)


def run_backtests_for_price_types(
    config: dict,
    price_types: Union[str, List[str]] = None,
    trace: bool = False,
    decimals: int = None,
    assets_path: str = None
) -> pd.DataFrame:
    '''
    Execute backtest simulation suite across one or multiple price types.

    Args:
        config: Dictionary loaded from configuration JSON.
        price_types: One or more price types to evaluate (overrides config).
        trace: Whether tracking files are generated.
        decimals: Decimal precision for display/rounding.
        assets_path: Custom assets CSV file path.

    Returns:
        Consolidated pd.DataFrame with results from all price types.
    '''
    resolved_assets = assets_path or config.get(KEY_ASSETS, config.get(KEY_ID, MARKET_SP500))
    interval = config.get(KEY_INTERVAL, DEFAULT_INTERVAL)
    capital = config.get(KEY_CAPITAL, DEFAULT_CAPITAL)

    ranker_name = config.get(KEY_RANKER, RANKER_MA)
    ranker_cls = get_ranker_class(ranker_name)

    if price_types is None:
        raw_price_type = config.get('price-type', PRICE_CLOSE)
        if isinstance(raw_price_type, list):
            selected_price_types = raw_price_type
        else:
            selected_price_types = [raw_price_type]
    elif isinstance(price_types, list):
        selected_price_types = price_types
    else:
        selected_price_types = [price_types]

    runner_grid, ranker_grid = build_grids(config)
    n_jobs = config.get('n_jobs', 1)

    if decimals is not None:
        resolved_decimals = decimals
    else:
        resolved_decimals = config.get(KEY_DECIMALS, DEFAULT_DECIMALS)

    all_results = []

    for pt in selected_price_types:
        print(f'Running simulation for price type: {pt}')
        mem_data = MemData(interval, assets=resolved_assets, price_type=pt)

        if trace:
            print(f'Salvando MegaDataFrame em megadataframe_{pt}.csv para demonstracao...')
            mem_data.mega_df.to_csv(f'megadataframe_{pt}.csv')

        backtest_config = {
            CONF_RANKER_CLS: ranker_cls,
            CONF_CAPITAL: capital,
            CONF_INTERVAL: interval,
            CONF_DATA: mem_data,
            CONF_DECIMALS: resolved_decimals,
            CONF_TRACE: trace,
            CONF_N_JOBS: n_jobs
        }

        backtester = Backtesting(backtest_config)
        results = backtester.run(runner_grid, ranker_grid=ranker_grid)
        if results is not None and not results.empty:
            results['price_type'] = pt
            all_results.append(results)

    if all_results:
        return pd.concat(all_results, ignore_index=True)
    return pd.DataFrame()
