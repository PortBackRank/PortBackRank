"""
Backtesting module

Contains the Backtesting class for running investment strategy simulations
by varying Runner and Ranker parameters, as well as utilities for saving
results to disk.
"""

import logging
from itertools import product
from typing import List, Dict, Callable
from joblib import Parallel, delayed
import pandas as pd
from data import MemData
from ranker import MARanker, RandomRanker, Ranker
from runner import Runner
from names import (
    MARKET_SP500, KEY_PROFIT, KEY_LOSS, KEY_DIVERSIFICATION, KEY_VOLUME,
    KEY_WINDOW, KEY_CAIXA_FINAL, KEY_PORTFOLIO_VALUE, KEY_RETORNO_TOTAL,
    KEY_FINAL_TOTAL_VALUE, KEY_INTERVAL, KEY_BALANCE,
    validate_positive_float, validate_diversification, validate_decimals
)

logger = logging.getLogger(__name__)


def _run_single_simulation(params, config_keys, runner_cls, ranker_cls, data, interval, capital, trace, decimals):
    runner_values, ranker_values = params
    parameter_names, ranker_names = config_keys
    
    runner_config = dict(zip(parameter_names, runner_values))
    ranker_config = dict(zip(ranker_names, ranker_values))
    
    logger.debug('Running simulation: runner=%s, ranker=%s', runner_config, ranker_config)

    runner_cfg = RunnerConfig(
        profit=runner_config[KEY_PROFIT],
        loss=runner_config[KEY_LOSS],
        diversification=runner_config[KEY_DIVERSIFICATION],
        volume=runner_config.get(KEY_VOLUME, 1.0),
        trace=trace,
        decimals=decimals,
        ranker_cls=ranker_cls
    )
    
    runner = Runner(config=runner_cfg, data=data)

    try:
        result = runner.single_run(interval, ranker_config, capital)
        # Evaluate results internally
        portfolio_value = result.get(KEY_FINAL_TOTAL_VALUE, 0)
        total_return = ((portfolio_value - capital) / capital) * 100
        
        eval_dict = {
            'intervalo': result.get(KEY_INTERVAL),
            KEY_PROFIT: runner_config[KEY_PROFIT],
            KEY_LOSS: runner_config[KEY_LOSS],
            KEY_DIVERSIFICATION: runner_config[KEY_DIVERSIFICATION],
            KEY_VOLUME: runner_config.get(KEY_VOLUME, 1.0),
            KEY_WINDOW: ranker_config.get(KEY_WINDOW),
            KEY_CAIXA_FINAL: result.get(KEY_BALANCE),
            KEY_PORTFOLIO_VALUE: portfolio_value,
            KEY_RETORNO_TOTAL: f"{total_return:.2f}%"
        }
        eval_dict.update(ranker_config)
        logger.debug('Simulation result: return=%.2f%%, final_value=%s', total_return, portfolio_value)
        return eval_dict
    except Exception as e:
        logger.exception('Error executing configuration %s: %s', runner_config, e)
        return None


class Backtesting:
    """Class for running investment strategy backtests.

    The class organizes the execution of simulations varying Runner and Ranker
    parameters, allows parallel execution, and aggregates performance metrics.
    """

    def __init__(self, config: dict):
        """
        Initializes the backtester with a configuration dictionary.

        Expected keys in config dict:
        - ranker_cls: Ranker class used in simulations.
        - capital: initial capital for all simulations.
        - interval: list [start_date, end_date] for the simulation.
        - data: MemData object with the simulation data.
        - trace: whether to generate tracking files.
        - runner_cls: (Optional) custom Runner class.
        - decimals: (Optional) number of decimal places for output.
        - n_jobs: (Optional) number of parallel jobs.
        """
        # Validate required keys
        required_keys = ['ranker_cls', 'capital', 'interval', 'data']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required config key: {key}")

        # Store config with defaults
        self._config = {
            'ranker_cls': config['ranker_cls'],
            'capital': config['capital'],
            'interval': config['interval'],
            'data': config['data'],
            'trace': config.get('trace', False),
            'runner_cls': config.get('runner_cls', Runner),
            'decimals': validate_decimals(config.get('decimals')),
            'n_jobs': config.get('n_jobs', 1)
        }
        
        logger.debug('Backtesting initialized: ranker=%s, capital=%d, interval=%s, decimals=%d', 
                     self._config['ranker_cls'].__name__, self._config['capital'], 
                     self._config['interval'], self._config['decimals'])

    @property
    def ranker_cls(self):
        return self._config['ranker_cls']

    @property
    def capital(self) -> int:
        return self._config['capital']

    @property
    def interval(self) -> List[str]:
        return self._config['interval']

    @property
    def data(self) -> MemData:
        return self._config['data']

    @property
    def trace(self) -> bool:
        return self._config['trace']

    @property
    def runner_cls(self):
        return self._config['runner_cls']

    @property
    def decimals(self) -> int:
        return self._config['decimals']

    @property
    def n_jobs(self) -> int:
        return self._config['n_jobs']

    def run(
        self,
        parameter_grid: Dict[str, List[float]],
        ranker_grid: Dict[str, List[float]],
        n_jobs: int = None
    ) -> pd.DataFrame:
        """
        Runs backtests varying Runner and Ranker parameters.

        Parameters:
        - parameter_grid: dictionary with Runner parameters and their value lists.
        - ranker_grid: dictionary with Ranker parameters.
        - n_jobs: number of parallel jobs (-1 uses all available cores). Defaults to config value.

        Returns:
        - DataFrame with aggregated results by parameter combination.
        """
        if n_jobs is None:
            n_jobs = self._config['n_jobs']
            
        runner_params = list(product(*parameter_grid.values()))
        ranker_params = list(product(*ranker_grid.values()))
        parameter_names = list(parameter_grid.keys())
        ranker_names = list(ranker_grid.keys())

        combinations = list(product(runner_params, ranker_params))
        config_keys = (parameter_names, ranker_names)
        
        logger.info('Starting backtest: %d runner configs x %d ranker configs = %d total combinations (n_jobs=%d)',
                    len(runner_params), len(ranker_params), len(combinations), n_jobs)

        results = Parallel(n_jobs=n_jobs)(
            delayed(_run_single_simulation)(
                c, config_keys, self._config['runner_cls'], self._config['ranker_cls'], 
                self._config['data'], self._config['interval'], self._config['capital'], self._config['trace'], self._config['decimals']
            ) for c in combinations
        )
        
        valid_results = [r for r in results if r is not None]
        logger.info('Backtest complete: %d/%d configurations succeeded', len(valid_results), len(combinations))
        return pd.DataFrame(results)


def test_bt_with_random():
    """Quick test using RandomRanker."""
    interval = ['2024-01-01', '2024-12-31']
    data = MemData(interval)
    config = {
        'ranker_cls': RandomRanker,
        'capital': 10000,
        'interval': interval,
        'data': data
    }
    backtester = Backtesting(config)

    parameter_grid = {
        'profit': [0.06, 0.1],
        'loss': [0.04],
        'diversification': [0.2]
    }
    ranker_ranges = {'SEED': [0, 1, 42]}

    results = backtester.run(parameter_grid, ranker_grid=ranker_ranges, n_jobs=1)
    print(results)


def test_bt_with_ma():
    """Test using MARanker (moving averages)."""
    interval = ['2024-01-01', '2024-06-30']
    data = MemData(interval, market_identifier=MARKET_SP500)
    config = {
        'ranker_cls': MARanker,
        'capital': 10000,
        'interval': interval,
        'data': data
    }
    backtester = Backtesting(config)

    parameters = {'window': [[9, 21], [20, 50], [50, 200]]}
    parameter_grid = {
        'profit': [0.1, 0.15],
        'loss': [0.05],
        'diversification': [0.1, 0.2]
    }

    results = backtester.run(parameter_grid, ranker_grid=parameters, n_jobs=1)
    print(results)


if __name__ == '__main__':
    test_bt_with_ma()
