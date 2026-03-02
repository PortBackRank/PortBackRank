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
from logger import logger
from utils import generate_filename
import os
import json


def save_results(results):
    '''
    Recebe os resultados das execuções paralelas e salva os arquivos no disco.
    Mantém a gravação da timeline (JSON) e dos logs de trade (CSV).
    '''
    for result in results:
        interval = result.get('interval', '')
        if ' - ' not in interval:
            logger.error(f"Intervalo inválido: {interval}")
            continue
            
        start_date, end_date = interval.split(' - ')

        timeline_filename = generate_filename('timeline', result, start_date, end_date)
        
        abs_timeline_path = os.path.abspath(timeline_filename)
        directory_timeline = os.path.dirname(abs_timeline_path)
        
        if not os.path.exists(directory_timeline):
            os.makedirs(directory_timeline, exist_ok=True)

        data_to_save = result.get('shared_data', {}).get('timeline', [])
        
        if data_to_save:
            with open(abs_timeline_path, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, indent=2, ensure_ascii=False)
            logger.info(f"Timeline salva com sucesso: {abs_timeline_path}")
        else:
            logger.warning(f"Timeline vazia para o intervalo {interval}. Arquivo não gerado.")

        trades = result.get('trade_log', [])
        if trades:
            df_trades = pd.DataFrame(trades)
            
            filename_base = generate_filename('trade_logs/trades', result, start_date, end_date)
            csv_filename = os.path.abspath(filename_base.replace('.json', '.csv'))

            directory_trades = os.path.dirname(csv_filename)
            if not os.path.exists(directory_trades):
                os.makedirs(directory_trades, exist_ok=True)

            df_trades.to_csv(csv_filename, index=False)
            logger.info(f"Trade log salvo: {csv_filename}")
        else:
            logger.warning(f"Nenhum trade realizado no intervalo {interval}")

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
        n_jobs: int = -1
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
                results_runner = []

                result = runner.single_run(self.interval, ranker_config, self.capital)

                results_runner.append(result)

                return self._evaluate_results(results_runner, runner_config, ranker_config)
            except Exception as e:
                print(f'Error executing configuration {runner_config} with ranker {ranker_config}: {e}')
                return None

        results = [
            res for res in Parallel(n_jobs=n_jobs)(
                delayed(run_simulation)(comb) for comb in combinations
            ) if res is not None
        ]

        if not results:
            logger.error("Nenhuma simulação foi concluída com sucesso. Verifique os erros acima.")
            return pd.DataFrame() # Retorna DF vazio em vez de None
        
        # Saves auxiliary files (timeline, trade logs)
        try:
            save_results(results)
            logger.info("Todos os logs de timeline e trades foram salvos com sucesso.")
        except Exception as e:
            logger.error(f"Erro crítico ao salvar resultados: {e}")

        # 2. Só remova os dados pesados APÓS a confirmação do salvamento
        for result in results:
            result.pop('shared_data', None)
            result.pop('trade_log', None)
            result.pop('sell_log', None)
            result.pop('buy_log', None)
        
        return pd.DataFrame(results)


    def _evaluate_results(
        self, result: List[Dict], runner_params: Dict, ranker_params: Dict
    ) -> Dict:
        '''
        Calculates performance metrics from a simulation result.

        Parameters:
        - result: list of states/results over time (last contains final state).
        - runner_params: Runner parameters used in the simulation.
        - ranker_params: Ranker parameters used in the simulation.

        Returns:
        - dictionary with aggregated metrics (final cash, portfolio value, total return, etc.).
        '''

        last_state = result[-1] if result else 0
        final_cash = last_state.get('balance', 0)

        portfolio_data = last_state.get('portfolio', {})
        portfolio_value = portfolio_data.get('valor_total', 0.0)

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
        parameter_grid, ranker_grid=ranker_ranges, n_jobs=-1)

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
        parameter_grid, ranker_grid=parameters, n_jobs=-1)

    # To generate performance plots, reactivate the call below
    # generate_performance_plot(market_symbol='SP500')

    print(results)


if __name__ == '__main__':
    test_bt_with_ma()
