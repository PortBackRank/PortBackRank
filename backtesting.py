"""
Backtesting module

Contains the Backtesting class for running investment strategy simulations
by varying Runner and Ranker parameters, as well as utilities for saving
results to disk.
"""

import logging
from itertools import product
from typing import List, Dict
from joblib import Parallel, delayed
import pandas as pd
from data import MemData
from runner import Runner, RunnerConfig
from names import *

logger = logging.getLogger(__name__)


def _run_single_simulation(runner_config: dict, ranker_config: dict, context: dict):
    """Run one parameter combination and return its evaluation row.

    Module-level function (not a method) so it can be pickled by joblib.
    All arguments are plain dicts (classes and MemData are picklable).

    Args:
        runner_config: Runner parameters for this combination
            (profit, loss, diversification, volume).
        ranker_config: Ranker parameters for this combination (e.g. window).
        context: simulation settings shared by every combination, using the
            CFG_* keys (runner_cls, ranker_cls, data, interval, capital,
            trace, decimals).

    Returns:
        Dictionary with the parameters and results, or None if the
        simulation raised an exception.
    """
    capital = context[CFG_CAPITAL]
    logger.debug('Running simulation: runner=%s, ranker=%s', runner_config, ranker_config)

    runner_cfg = RunnerConfig(
        profit=runner_config[KEY_PROFIT],
        loss=runner_config[KEY_LOSS],
        diversification=runner_config[KEY_DIVERSIFICATION],
        volume=runner_config[KEY_VOLUME],
        ranker_cls=context[CFG_RANKER_CLS],
        trace=context[CFG_TRACE],
        decimals=context[CFG_DECIMALS]
    )
    runner = context[CFG_RUNNER_CLS](config=runner_cfg, data=context[CFG_DATA])

    try:
        result = runner.single_run(context[CFG_INTERVAL], ranker_config, capital)
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
    """Class for running investment strategy backtests.

    The class organizes the execution of simulations varying Runner and Ranker
    parameters, allows parallel execution, and aggregates performance metrics.
    """

    _REQUIRED_KEYS = [
        CFG_RANKER_CLS, CFG_CAPITAL, CFG_INTERVAL, CFG_DATA,
        CFG_TRACE, CFG_DECIMALS, CFG_N_JOBS
    ]

    def __init__(self, config: dict):
        """
        Initializes the backtester with a configuration dictionary.

        Required keys in config dict:
        - ranker_cls: Ranker class used in simulations.
        - capital: initial capital for all simulations.
        - interval: list [start_date, end_date] for the simulation.
        - data: MemData object with the simulation data.
        - trace: whether to generate tracking files.
        - decimals: number of decimal places for output.
        - n_jobs: number of parallel jobs.

        Optional keys:
        - runner_cls: custom Runner class (defaults to Runner).

        Raises:
            ValueError: if a required key is missing.
        """
        missing = [key for key in self._REQUIRED_KEYS if key not in config]
        if missing:
            raise ValueError(f'Missing required config key(s): {", ".join(missing)}')

        self._config = {
            CFG_RANKER_CLS: config[CFG_RANKER_CLS],
            CFG_CAPITAL: config[CFG_CAPITAL],
            CFG_INTERVAL: config[CFG_INTERVAL],
            CFG_DATA: config[CFG_DATA],
            CFG_TRACE: config[CFG_TRACE],
            CFG_RUNNER_CLS: config.get(CFG_RUNNER_CLS, Runner),
            CFG_DECIMALS: validate_decimals(config[CFG_DECIMALS]),
            CFG_N_JOBS: config[CFG_N_JOBS]
        }
        
        logger.debug('Backtesting initialized: ranker=%s, capital=%d, interval=%s, decimals=%d', 
                     self.ranker_cls.__name__, self.capital, self.interval, self.decimals)

    @property
    def ranker_cls(self):
        """Ranker class used in simulations (read-only)."""
        return self._config[CFG_RANKER_CLS]

    @property
    def capital(self) -> int:
        """Initial capital (read-only)."""
        return self._config[CFG_CAPITAL]

    @property
    def interval(self) -> List[str]:
        """Simulation interval [start_date, end_date] (read-only)."""
        return self._config[CFG_INTERVAL]

    @property
    def data(self) -> MemData:
        """MemData with the simulation data (read-only)."""
        return self._config[CFG_DATA]

    @property
    def trace(self) -> bool:
        """Whether tracking files are written (read-only)."""
        return self._config[CFG_TRACE]

    @property
    def runner_cls(self):
        """Runner class used in simulations (read-only)."""
        return self._config[CFG_RUNNER_CLS]

    @property
    def decimals(self) -> int:
        """Decimal places applied to output values (read-only)."""
        return self._config[CFG_DECIMALS]

    @property
    def n_jobs(self) -> int:
        """Default number of parallel jobs (read-only)."""
        return self._config[CFG_N_JOBS]

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
