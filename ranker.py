'''
Ranker module for the investment strategy.
'''

import logging
from abc import ABC, abstractmethod
from typing import List
import random
import numpy as np
import pandas as pd
from data import MemData
from names import *

logger = logging.getLogger(__name__)


class Ranker(ABC):
    '''Abstract base class for ranking strategies.'''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        self._interval = interval
        self._data = data
        self._parameters = parameters or {}

    @property
    def interval(self):
        return self._interval

    @property
    def data(self) -> MemData:
        return self._data

    @property
    def parameters(self) -> dict:
        return self._parameters

    @abstractmethod
    def rank(self, date: str = None) -> List[str]:
        '''
        Generate a ranked list of stock symbols based on the strategy.
        '''

    @abstractmethod
    def prepare(self) -> None:
        '''
        Prepare required data or indicators before simulation execution.
        '''


class RandomRanker(Ranker):
    '''Ranker that generates random symbol rankings.'''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        self._seed = self.parameters.get(KEY_SEED, 42)
        logger.debug('RandomRanker initialized: seed=%s', self._seed)

    @property
    def seed(self):
        return self._seed

    def rank(self, date: str = None) -> List[str]:
        symbols = self.data.get_assets()
        if self._seed is not None:
            random.seed(self._seed)
        random.shuffle(symbols)
        logger.debug('RandomRanker.rank(%s): %d symbols', date, len(symbols))
        return symbols

    def prepare(self) -> None:
        pass


class MARanker(Ranker):
    '''Ranker based on moving average crossover strength (impulse and decay).'''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data = None):
        super().__init__(parameters, interval, data)
        windows = self.parameters.get(KEY_WINDOW)
        self._short = windows[0]
        self._long = windows[1]
        self._force_col = 'force'
        logger.debug('MARanker initialized: short=%d, long=%d', self._short, self._long)

    @property
    def short(self) -> int:
        return self._short

    @property
    def long(self) -> int:
        return self._long

    @property
    def short_col(self) -> str:
        return f'ma{self._short}'

    @property
    def long_col(self) -> str:
        return f'ma{self._long}'

    @property
    def force_col(self) -> str:
        return self._force_col

    @property
    def strength_col(self) -> str:
        return self._force_col

    def prepare(self) -> None:
        '''
        Calculates moving average crossover force vectorized across the MegaDataFrame.
        Keeps only the force attribute in the MegaDataFrame.
        '''
        df = self.data.get_mega_df()
        if df.empty:
            logger.debug('MARanker.prepare: empty dataframe')
            return

        logger.debug('MARanker.prepare: calculating force for %d rows', len(df))
        # Ensure chronological sorting per symbol
        df.sort_index(level=[IDX_SYMBOL, COL_DATE], inplace=True)

        # 1. Moving averages calculation
        ma_short = df.groupby(level=IDX_SYMBOL)[COL_CLOSE].transform(lambda x: x.rolling(self._short).mean())
        ma_long = df.groupby(level=IDX_SYMBOL)[COL_CLOSE].transform(lambda x: x.rolling(self._long).mean())

        # 2. Direction and crossover identification
        direction = np.sign(ma_short - ma_long)
        crossover = direction.groupby(level=IDX_SYMBOL).transform(lambda x: (x != x.shift(1)) & (x != 0))

        # 3. Crossover history and bar count
        has_crossover = crossover.groupby(level=IDX_SYMBOL).transform(lambda x: x.cumsum() > 0)
        cross_id = crossover.groupby(level=IDX_SYMBOL).transform(lambda x: x.cumsum())

        symbol_level = df.index.get_level_values(IDX_SYMBOL)
        bars_since_cross = df.groupby([symbol_level, cross_id]).cumcount()

        # 4. Abruptness (S0) via angle differential (ArcTan)
        k = 3
        gamma = 50.0

        ma_short_shift = ma_short.groupby(level=IDX_SYMBOL).shift(k)
        ma_long_shift = ma_long.groupby(level=IDX_SYMBOL).shift(k)

        slope_short = (ma_short - ma_short_shift) / (ma_short_shift * k)
        slope_long = (ma_long - ma_long_shift) / (ma_long_shift * k)

        angle_diff = np.abs(np.arctan(gamma * slope_short) - np.arctan(gamma * slope_long))
        s0_raw = (angle_diff / (np.pi / 2.0)).clip(0.0, 1.0)
        signed_s0_raw = direction * s0_raw

        signed_s0 = signed_s0_raw.where(crossover)
        signed_s0 = signed_s0.groupby(level=IDX_SYMBOL).ffill().fillna(0)

        # 5. Linear decay in N bars to 0
        n_bars = 10
        decay_factor = (1.0 - bars_since_cross / n_bars).clip(lower=0.0)

        # 6. Force calculation
        force = signed_s0 * decay_factor
        force = force.where(has_crossover, 0.0)

        # Retain only force attribute on the mega-dataframe
        keep_cols = [COL_CLOSE, COL_BUY, COL_VOLUME, self._force_col]
        cols_to_drop = [c for c in df.columns if c not in keep_cols]
        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True)

        df[self._force_col] = force

        # Restore original index sorting
        df.sort_index(level=[COL_DATE, IDX_SYMBOL], inplace=True)
        logger.debug('MARanker.prepare: force calculated (%s)', self._force_col)

    def rank(self, date: str = None) -> List[str]:
        '''
        Rank symbols based on positive force (> 0) using pre-calculated indicators.
        '''
        df = self.data.get_mega_df()
        if df.empty or date not in df.index.get_level_values(COL_DATE):
            return []

        all_dates = df.index.get_level_values(COL_DATE).unique()
        date_loc = all_dates.get_loc(date)
        if date_loc < 1:
            return []

        prev_date = all_dates[date_loc - 1]
        prev_date_data = df.xs(prev_date, level=COL_DATE)
        current_date_data = df.xs(date, level=COL_DATE)

        ranked_symbols = []
        for symbol, row in current_date_data.iterrows():
            if symbol not in prev_date_data.index:
                continue
            val = row[self._force_col]
            if pd.isna(val) or val <= 0:
                continue
            ranked_symbols.append((val, symbol))

        ranked_symbols.sort(reverse=True)
        result = [x[1] for x in ranked_symbols]
        logger.debug('MARanker.rank(%s): %d signals (force > 0)', date, len(result))
        return result
