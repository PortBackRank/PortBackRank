'''
Backtesting Module - Investment Strategy Evaluation Framework

This module provides the Backtesting class, which enables running
multiple backtesting simulations with different parameter combinations
using parallel processing for improved performance.
'''

from itertools import product
from typing import List, Dict
from joblib import Parallel, delayed
import pandas as pd
from data import MemData
from ranker import MARanker, RandomRanker
from runner import Runner
from utils import generate_filename, save_json, generate_performance_plot
from names import PREFIX_TIMELINE, PREFIX_BUY_LOG, PREFIX_SELL_LOG, SUBDIR_SELL_BUY_LOGS


def save_results(results):
    '''
    Saves backtesting results to JSON files.
    
    Processes results from parallel executions and persists:
    - Timeline data (balance and portfolio evolution)
    - Buy logs (purchase transactions)
    - Sell logs (sale transactions)
    '''
    for result in results:
        start_date = result['start_date']
        end_date = result['end_date']

        save_json(generate_filename(PREFIX_TIMELINE, result, start_date,
                  end_date), result['shared_data']['timeline'])
        save_json(generate_filename(f'{SUBDIR_SELL_BUY_LOGS}/{PREFIX_SELL_LOG}',
                  result, start_date, end_date), result['sell_log'])
        save_json(generate_filename(f'{SUBDIR_SELL_BUY_LOGS}/{PREFIX_BUY_LOG}',
                  result, start_date, end_date), result['buy_log'])


class Backtesting:
    '''
    Backtesting engine for evaluating investment strategies.
    
    Supports parameterized testing of multiple strategy configurations
    using different rankers and runner parameters. Executes simulations
    in parallel to improve performance.
    '''

    def __init__(self, ranker_cls, capital: float, interval: List[str], market_identifier = 'SP500'):
        '''
        Initializes the backtesting engine.

        :param ranker_cls: Ranker class (e.g., MARanker, RandomRanker) for asset ranking
        :param capital: Initial capital in currency units for all simulations
        :param interval: Simulation period as [start_date, end_date] in 'YYYY-MM-DD' format
        :param market_identifier: Market identifier (ticker like 'SP500' or file path like 'assets/IBOV.csv')
                                 Market identifiers can be predefined (IBOV, IFIX) or custom file paths.
                                 If not found in MARKETS config, will be created dynamically.

        Example:
            backtester = Backtesting(
                MARanker,
                capital=10000,
                interval=['2024-01-01', '2024-12-31'],
                market_identifier='SP500'
            )
        '''
        self.ranker_cls = ranker_cls
        self.capital = capital
        self.interval = interval
        self.runner_cls = Runner
        self.data = MemData(interval, market_identifier)

    def run(
        self,
        parameter_grid: Dict[str, List[float]],
        ranker_grid: Dict[str, List[float]],
        n_jobs: int = -1
    ) -> pd.DataFrame:
        '''
        Executes backtesting simulations with parameter combinations.
        
        Creates all combinations of runner and ranker parameters, then runs
        parallel simulations for each combination. Results are aggregated
        and returned as a DataFrame.

        :param parameter_grid: Runner parameters to test.
                              Keys: 'profit', 'loss', 'diversification'
                              Example: {'profit': [0.05, 0.1], 'loss': [0.05, 0.1]}
        :param ranker_grid: Ranker-specific parameters to test.
                           Example: {'window': [[9, 21], [20, 50]], 'SEED': [42]}
        :param n_jobs: Parallel execution: -1 (all cores), 1 (sequential), N (N processes)
        :return: DataFrame with results for each parameter combination
        '''
        runner_params = list(product(*parameter_grid.values()))
        ranker_params = list(product(*ranker_grid.values()))
        parameter_names = list(parameter_grid.keys())
        ranker_names = list(ranker_grid.keys())

        combinations = list(product(runner_params, ranker_params))

        def run_simulation(params):
            runner_values, ranker_values = params
            runner_config = dict(zip(parameter_names, runner_values))
            ranker_config = dict(zip(ranker_names, ranker_values))

            runner = self.runner_cls(
                profit=runner_config['profit'],
                loss=runner_config['loss'],
                diversification=runner_config['diversification'],
                ranker=self.ranker_cls,
                data=self.data
            )

            try:

                results_runner = []

                result = runner.single_run(
                    self.interval, ranker_config, self.capital)

                results_runner.append(result)

                return self._evaluate_results(results_runner, runner_config, ranker_config)
            except Exception as e:
                print(f'Error running configuration {runner_config} with ranker {ranker_config}: {e}')
                return None

        results = [
            res for res in Parallel(n_jobs=n_jobs)(
                delayed(run_simulation)(comb) for comb in combinations
            ) if res is not None
        ]

        # Save simulation timelines and transaction logs to JSON files
        # Useful for detailed analysis and performance visualization
        save_results(results)

        for result in results:
            del result['shared_data']
            del result['sell_log']
            del result['buy_log']

        return pd.DataFrame(results)

    def _evaluate_results(
        self, result: List[Dict], runner_params: Dict, ranker_params: Dict
    ) -> Dict:
        '''
        Calculates and aggregates performance metrics from simulation.
        
        Computes final balance, portfolio value, and total return percentage.
        Combines these metrics with the parameters used for easy result analysis.

        :param result: Simulation timeline as list of state dictionaries
        :param runner_params: Runner configuration used (profit, loss, diversification)
        :param ranker_params: Ranker configuration used (window, SEED, etc.)
        :return: Dictionary with metrics and configuration for result storage
        '''
        final_cash = result[-1]['balance'] if result else 0

        portfolio_value = sum(
            item['quantity'] * item['purchase_price'] for item in result[-1]['portfolio']
        ) if result else 0

        total_return = (final_cash + portfolio_value) / self.capital - 1
        total_return = round(total_return * 100, 2)

        shared_data = result[-1].get('shared_data', {}) if result else {}

        return {
            'start_date': self.interval[0],
            'end_date': self.interval[1],
            **runner_params,
            **ranker_params,
            'final_cash': final_cash,
            'portfolio_value': portfolio_value,
            'total_return': f'{total_return:.2f}%',
            'shared_data': shared_data,
            'sell_log': result[-1].get('sell_log', []),
            'buy_log': result[-1].get('buy_log', [])
        }


def test_backtest_random():
    '''Test backtesting with RandomRanker strategy.'''
    interval = ['2024-01-01', '2024-12-31']

    backtester = Backtesting(RandomRanker, capital=10000, interval=interval)

    parameter_grid = {
        'profit': [0.06, 0.1],
        'loss': [0.04],
        'diversification': [0.2]
    }

    ranker_ranges = {'SEED': [0, 1, 42]}

    results = backtester.run(
        parameter_grid, ranker_grid=ranker_ranges, n_jobs=-1)

    print(results)


def test_backtest_ma():
    '''Test backtesting with Moving Average (MA) Ranker strategy.'''
    interval = ['2024-01-01', '2024-06-30']

    parameters = {'window': [[9, 21], [20, 50], [50, 200]]}

    backtester = Backtesting(MARanker, capital=10000,
                             interval=interval, market_identifier='SP500')

    parameter_grid = {
        'profit': [0.1, 0.15],
        'loss': [0.05],
        'diversification': [0.1, 0.2]
    }

    results = backtester.run(
        parameter_grid, ranker_grid=parameters, n_jobs=-1)

    # generate_performance_plot(market_symbol='SP500')

    print(results)


if __name__ == '__main__':
    test_backtest_ma()
