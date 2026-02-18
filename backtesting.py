'''
    Class Backtesting
'''

from itertools import product
from typing import List, Dict
from joblib import Parallel, delayed
import pandas as pd
from data import MemData
from ranker import MARanker, RandomRanker
from runner import Runner
from logger import logger
from files import save_dataframe
from utils import generate_filename, save_json, generate_performance_plot
import os 
import json


def save_results(results):
    '''
    Receives results from parallel executions and writes files to disk.
    '''
    for result in results:
        start_date, end_date = result['interval'].split(' - ')

        timeline_filename = generate_filename('timeline', result, start_date, end_date)
        
        # create directory if necessary
        directory = os.path.dirname(timeline_filename)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        
        # save the JSON directly
        with open(timeline_filename, 'w', encoding='utf-8') as f:
            json.dump(result['shared_data']['timeline'], f, indent=2, ensure_ascii=False)
        
        # process trade_log
        trades = result.get('trade_log', [])
        
        if trades:
            df_trades = pd.DataFrame(trades)
            filename_base = generate_filename('trade_logs/trades', result, start_date, end_date)
            csv_filename = filename_base.replace('.json', '.csv')
            
            # create directory if necessary
            directory = os.path.dirname(csv_filename)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)

            # save CSV directly
            df_trades.to_csv(csv_filename, index=False)
            
            logger.info(f"Trade log saved: {csv_filename}")
        else:
            logger.warning(f"No trades to save for interval {result['interval']}")


class Backtesting:
    '''Class used to perform backtesting of an investment strategy.'''

    def __init__(self, ranker_cls, capital: float, interval: List[str], market_identifier = 'SP500'):
        '''
        Initialize the backtesting with basic information.

        :param ranker_cls: Ranker class used to create instances.
        :param capital: Initial capital for all simulations.
        :param interval: List with the start and end dates of the simulation.
        :param market_identifier: Symbol or path of the assets to be used
                                  (e.g. IBOV.csv or 'IBOV').

        The ``market_identifier`` parameter can be:
        1. A **market symbol** (e.g. 'IBOV', 'IFIX', etc.) corresponding to an
           existing market in MARKETS.
        2. A **file path** (e.g. 'assets/IBOV.csv') that will be used to
           identify the corresponding market and, if it does not exist, the
           market will be created dynamically.

        EXAMPLE::
            backtester = Backtesting(MARanker, capital=10000,
                                     interval=['2024-01-01', '2024-12-31'],
                                     market_identifier='SP500')
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
        Execute backtesting while varying Runner and ranker parameters.

        :param parameter_grid: Dictionary with parameters to vary and their
                               values. Example: ``{'profit': [0.05, 0.1],
                               'loss': [0.05, 0.1]}``.
        :param ranker_grid: Dictionary with ranker parameters to vary.
                            Example: ``{'SEED': [0, 1, 42]}``.
        :param n_jobs: Number of parallel jobs (-1 uses all available cores).
        :return: DataFrame with the simulation results.
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
                volume=runner_config.get('volume', 1.0),
                ranker=self.ranker_cls,
                data=self.data
            )

            try:
                results_runner = []

                result = runner.single_run(self.interval, ranker_config, self.capital)

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

        # uncomment to save the timeline files when using MARanker
        save_results(results)

        for result in results:
            del result['shared_data']
            result.pop('trade_log', None)  
            result.pop('sell_log', None)   
            result.pop('buy_log', None)
        return pd.DataFrame(results)

    def _evaluate_results(
        self, result: List[Dict], runner_params: Dict, ranker_params: Dict
    ) -> Dict:
        '''
        Calculate performance metrics of the simulation.

        :param result: Simulation result (list of dictionaries).
        :param runner_params: Parameters used in the Runner.
        :param ranker_params: Parameters used in the Ranker.
        :return: Dictionary with computed metrics.
        '''
        final_cash = result[-1]['balance'] if result else 0

        portfolio_value = sum(
            item['quantity'] * item['purchase_price'] for item in result[-1]['portfolio']
        ) if result else 0

        total_return = (final_cash + portfolio_value) / self.capital - 1
        total_return = round(total_return * 100, 2)

        shared_data = result[-1].get('shared_data', {}) if result else {}

        trade_log = result[-1].get('trade_log', []) if result else []
        return {
            'interval': f'{self.interval[0]} - {self.interval[1]}',
            **runner_params,
            **ranker_params,
            'final_cash': final_cash,
            'portfolio_value': portfolio_value,
            'total_return': f'{total_return:.2f}%',
            'shared_data': shared_data,
            'trade_log': trade_log
        }


def test_bt_with_random():
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


def test_bt_with_ma():
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
    test_bt_with_ma()
