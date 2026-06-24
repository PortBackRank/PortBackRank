'''
Backtesting module

Contains the Backtesting class for running investment strategy simulations
by varying Runner and Ranker parameters, as well as utilities for saving
results to disk.
'''

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
    KEY_FINAL_TOTAL_VALUE, KEY_INTERVAL, KEY_BALANCE
)


def _run_single_simulation(params, config_keys, runner_cls, ranker_cls, data, interval, capital, trace):
    runner_values, ranker_values = params
    parameter_names, ranker_names = config_keys
    
    runner_config = dict(zip(parameter_names, runner_values))
    ranker_config = dict(zip(ranker_names, ranker_values))

    runner = runner_cls(
        profit=runner_config[KEY_PROFIT],
        loss=runner_config[KEY_LOSS],
        diversification=runner_config[KEY_DIVERSIFICATION],
        volume=runner_config.get(KEY_VOLUME, 1.0),
        ranker=ranker_cls,
        data=data,
        trace=trace
    )

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
        return eval_dict
    except Exception as e:
        print(f'Error executing configuration {runner_config}: {e}')
        return None


class Backtesting:
    '''Class for running investment strategy backtests.

    The class organizes the execution of simulations varying Runner and Ranker
    parameters, allows parallel execution, and aggregates performance metrics.
    '''

    def __init__(self, config: dict):
        '''
        Initializes the backtester with a configuration dictionary.

        Expected keys in config:
        - ranker_cls: Ranker class used in simulations.
        - capital: initial capital for all simulations.
        - interval: list [start_date, end_date] for the simulation.
        - data: MemData object with the simulation data.
        - trace: whether to generate tracking files.
        - runner_cls: (Optional) custom Runner class.
        '''
        self.ranker_cls = config.get('ranker_cls', RandomRanker)
        self.capital = config.get('capital', 10000)
        self.interval = config.get('interval', ['2024-01-01', '2024-12-31'])
        self.data = config.get('data')
        self.trace = config.get('trace', False)
        self.runner_cls = config.get('runner_cls', Runner)
        
        if self.data is None:
            raise ValueError("A 'data' object (MemData instance) must be provided in config.")

    def run(
        self,
        parameter_grid: Dict[str, List[float]],
        ranker_grid: Dict[str, List[float]],
        n_jobs: int = 1
    ) -> pd.DataFrame:
        '''
        Runs backtests varying Runner and Ranker parameters.

        Parameters:
        - parameter_grid: dictionary with Runner parameters and their value lists.
        - ranker_grid: dictionary with Ranker parameters.
        - n_jobs: number of parallel jobs (-1 uses all available cores).

        Returns:
        - DataFrame with aggregated results by parameter combination.
        '''
        runner_params = list(product(*parameter_grid.values()))
        ranker_params = list(product(*ranker_grid.values()))
        parameter_names = list(parameter_grid.keys())
        ranker_names = list(ranker_grid.keys())

        combinations = list(product(runner_params, ranker_params))
        config_keys = (parameter_names, ranker_names)

        results = Parallel(n_jobs=n_jobs)(
            delayed(_run_single_simulation)(
                c, config_keys, self.runner_cls, self.ranker_cls, 
                self.data, self.interval, self.capital, self.trace
            ) for c in combinations
        )
        return pd.DataFrame(results)


def test_bt_with_random():
    '''Quick test using RandomRanker.'''
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
    '''Test using MARanker (moving averages).'''
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
