'''
Backtesting module

Contains the Backtesting class for running investment strategy simulations
by varying Runner and Ranker parameters, as well as utilities for saving
results to disk.
'''

from itertools import product
from typing import List, Dict
from joblib import Parallel, delayed
import pandas as pd
from data import MemData
from ranker import MARanker, RandomRanker
from runner import Runner



class Backtesting:
    '''Class for running investment strategy backtests.

    The class organizes the execution of simulations varying Runner and Ranker
    parameters, allows parallel execution, and aggregates performance metrics.
    '''

    def __init__(self, ranker_cls, capital: float, interval: List[str], market_identifier='SP500'):
        '''
        Initializes the backtester.

        Parameters:
        - ranker_cls: Ranker class used in simulations.
        - capital: initial capital for all simulations.
        - interval: list [start_date, end_date] for the simulation.
        - market_identifier: market identifier (symbol or file path).

        The market_identifier parameter can be a symbol (e.g., 'SP500') or
        a file path (e.g., 'assets/IBOV.csv').
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
        n_jobs: int = 1
    ) -> pd.DataFrame:
        '''
        Runs backtests varying Runner and Ranker parameters.

        Parameters:
        - parameter_grid: dictionary with Runner parameters and their value lists.
                          E.g.: {'profit': [0.05, 0.1], 'loss': [0.05, 0.1]}.
        - ranker_grid: dictionary with Ranker parameters.
                       E.g.: {'SEED': [0, 1, 42]}.
        - n_jobs: number of parallel jobs (-1 uses all available cores).

        Returns:
        - DataFrame with aggregated results by parameter combination.
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
                # O Runner executa e salva seus próprios arquivos internamente
                result = runner.single_run(self.interval, ranker_config, self.capital)

                # Agora retornamos o dicionário completo para o DataFrame do console
                return self._evaluate_results(result, runner_config, ranker_config)
            except Exception as e:
                print(f'Error executing configuration {runner_config}: {e}')
                return None

        results = Parallel(n_jobs=n_jobs)(delayed(run_simulation)(c) for c in combinations)
        return pd.DataFrame(results)



    def _evaluate_results(self, result: Dict, runner_params: Dict, ranker_params: Dict) -> Dict:
        '''
        Calculates performance metrics from a simulation result.

        Parameters:
        - result: list of states/results over time (last contains final state).
        - runner_params: Runner parameters used in the simulation.
        - ranker_params: Ranker parameters used in the simulation.

        Returns:
        - dictionary with aggregated metrics (final cash, portfolio value, total return, etc.).
        '''
        portfolio_value = result.get('final_total_value', 0)
        total_return = ((portfolio_value - self.capital) / self.capital) * 100
        
        # Retornamos apenas o que deve aparecer na tabela do console
        eval_dict = {
            'intervalo': result.get('interval'),
            'profit': runner_params['profit'],
            'loss': runner_params['loss'],
            'diversification': runner_params['diversification'],
            'window': ranker_params.get('window'),
            'caixa_final': result.get('balance'),
            'portfolio_value': portfolio_value,
            'retorno_total': f"{total_return:.2f}%"
        }
        
        # Adiciona dinamicamente os parâmetros do ranker (ex: window)
        eval_dict.update(ranker_params)
        return eval_dict


def test_bt_with_random():
    '''Quick test using RandomRanker.'''
    interval = ['2024-01-01', '2024-12-31']

    backtester = Backtesting(RandomRanker, capital=10000, interval=interval)

    parameter_grid = {
        'profit': [0.06, 0.1],
        'loss': [0.04],
        'diversification': [0.2]
    }

    ranker_ranges = {'SEED': [0, 1, 42]}

    results = backtester.run(
        parameter_grid, ranker_grid=ranker_ranges, n_jobs=1)

    print(results)


def test_bt_with_ma():
    '''Test using MARanker (moving averages).'''
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
        parameter_grid, ranker_grid=parameters, n_jobs=1)

    # To generate performance plots, reactivate the call below
    # generate_performance_plot(market_symbol='SP500')

    print(results)


if __name__ == '__main__':
    test_bt_with_ma()
