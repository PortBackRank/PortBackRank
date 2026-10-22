"""
Ranker module for the investment strategy.
"""

import logging
from abc import ABC, abstractmethod
from typing import List
import random
import pandas as pd
from data import MemData
from names import *

logger = logging.getLogger(__name__)


class Ranker(ABC):
    """Abstract base class for ranking strategies."""

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
        """
        Generate a ranked list of stock symbols based on the strategy.
        """

    @abstractmethod
    def prepare(self) -> None:
        """
        Prepare required data or indicators before simulation execution.
        """


class RandomRanker(Ranker):
    """Ranker that generates random symbol rankings."""

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
    """Ranker based on Mean Reversion strategy using moving averages."""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data = None):
        super().__init__(parameters, interval, data)
        windows = self.parameters.get(KEY_WINDOW)
        self._short = windows[0]
        self._long = windows[1]
        self._short_col = f'ma{self._short}'
        self._long_col = f'ma{self._long}'
        self._strength_col = f'strength_{self._short}_{self._long}'
        logger.debug('MARanker initialized: short=%d, long=%d', self._short, self._long)

    @property
    def short(self) -> int:
        return self._short

    @property
    def long(self) -> int:
        return self._long

    @property
    def short_col(self) -> str:
        return self._short_col

    @property
    def long_col(self) -> str:
        return self._long_col

    @property
    def strength_col(self) -> str:
        return self._strength_col

    def prepare(self) -> None:
        """
        Calculates moving averages and cross-strength vectorized across the MegaDataFrame.
        """
        df = self.data.get_mega_df()
        if df.empty:
            logger.debug('MARanker.prepare: empty dataframe')
            return

        logger.debug('MARanker.prepare: calculating MAs and strength for %d rows', len(df))
        # Ensure sorting by Symbol and Date for correct chronological rolling
        df.sort_index(level=[IDX_SYMBOL, COL_DATE], inplace=True)
        
        df[self.short_col] = df.groupby(level=IDX_SYMBOL)[COL_CLOSE].transform(lambda x: x.rolling(self._short).mean())
        df[self.long_col] = df.groupby(level=IDX_SYMBOL)[COL_CLOSE].transform(lambda x: x.rolling(self._long).mean())
        
        # Precompute crossover and signal strength vectorized across all symbols
        prev_short = df.groupby(level=IDX_SYMBOL)[self.short_col].shift(1)
        prev_long = df.groupby(level=IDX_SYMBOL)[self.long_col].shift(1)

        # Valid rows where both current and previous MAs are not NaN
        valid = (
            df[self.short_col].notna() & df[self.long_col].notna() &
            prev_short.notna() & prev_long.notna()
        )
        cross_mask = valid & (prev_short <= prev_long) & (df[self.short_col] > df[self.long_col])
        strength_vals = (df[self.short_col] / df[self.long_col] - 1.0) * 100.0

        # Initialize strength with -inf for valid rows and update cross signals
        df[self.strength_col] = float('-inf')
        df[self.strength_col] = df[self.strength_col].where(~cross_mask, strength_vals)
        # Keep NaN where MAs are not yet valid
        df[self.strength_col] = df[self.strength_col].where(valid, float('nan'))

        # Restore original index sorting
        df.sort_index(level=[COL_DATE, IDX_SYMBOL], inplace=True)
        logger.debug('MARanker.prepare: MAs and strength calculated (%s, %s)', self.short_col, self.long_col)

    def rank(self, date: str = None) -> List[str]:
        """
        Rank symbols based on mean reversion signals using pre-calculated indicators.
        """
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

        # Retain exact legacy behavior: evaluate valid symbols present on both days
        ranked_symbols = []
        for symbol, row in current_date_data.iterrows():
            if symbol not in prev_date_data.index:
                continue
            val = row[self.strength_col]
            if pd.isna(val):
                continue
            ranked_symbols.append((val, symbol))

        ranked_symbols.sort(reverse=True)
        result = [x[1] for x in ranked_symbols]
        logger.debug('MARanker.rank(%s): %d signals', date, len(result))
        return result


class RSIRanker(Ranker):
    """Ranker based on Relative Strength Index (RSI) technical indicator."""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        self._period = int(self.parameters.get('period', 14))
        
        win = self.parameters.get(KEY_WINDOW)
        try:
            if isinstance(win, (list, tuple)) and len(win) >= 1:
                self._period = int(win[0])
        except Exception:
            pass
        
        self._oversold = float(self.parameters.get('oversold', 30))
        self._overbought = float(self.parameters.get('overbought', 70))
        self._mode = self.parameters.get('mode', 'mean_reversion')
        self._rsi_col = f'rsi{self._period}'
        logger.debug('RSIRanker initialized: period=%d, oversold=%s, overbought=%s, mode=%s', 
                     self._period, self._oversold, self._overbought, self._mode)

    @property
    def period(self) -> int:
        return self._period

    @property
    def oversold(self) -> float:
        return self._oversold

    @property
    def overbought(self) -> float:
        return self._overbought

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def rsi_col(self) -> str:
        return self._rsi_col

    def prepare(self) -> None:
        """
        Calculates RSI vectorized across the MegaDataFrame.
        """
        df = self.data.get_mega_df()
        if df.empty:
            logger.debug('RSIRanker.prepare: empty dataframe')
            return

        logger.debug('RSIRanker.prepare: calculating RSI for %d rows', len(df))
        df.sort_index(level=[IDX_SYMBOL, COL_DATE], inplace=True)
        
        def compute_rsi(group):
            delta = group.diff()
            up = delta.clip(lower=0)
            down = -delta.clip(upper=0)
            roll_up = up.ewm(alpha=1 / self.period, adjust=False).mean()
            roll_down = down.ewm(alpha=1 / self.period, adjust=False).mean()
            rs = roll_up / roll_down
            rsi = 100 - (100 / (1 + rs))
            return rsi.replace([float('inf'), float('-inf')], float('nan')).ffill()

        df[self.rsi_col] = df.groupby(level=IDX_SYMBOL)[COL_CLOSE].transform(compute_rsi)
        df.sort_index(level=[COL_DATE, IDX_SYMBOL], inplace=True)
        logger.debug('RSIRanker.prepare: RSI calculated (%s)', self.rsi_col)

    def rank(self, date: str = None) -> List[str]:
        """
        Rank symbols based on RSI signal crossovers using pre-calculated indicators.
        """
        df = self.data.get_mega_df()
        if df.empty or date not in df.index.get_level_values(COL_DATE):
            logger.debug('RSIRanker.rank(%s): no data', date)
            return []

        current_date_data = df.xs(date, level=COL_DATE)
        
        all_dates = df.index.get_level_values(COL_DATE).unique()
        date_loc = all_dates.get_loc(date)
        
        if date_loc < 1:
            return []
            
        prev_date = all_dates[date_loc - 1]
        prev_date_data = df.xs(prev_date, level=COL_DATE)

        ranked_symbols = []
        
        for symbol in current_date_data.index:
            if symbol not in prev_date_data.index:
                continue
                
            latest = current_date_data.loc[symbol]
            prev = prev_date_data.loc[symbol]
            
            if pd.isna(latest[self.rsi_col]) or pd.isna(prev[self.rsi_col]):
                continue

            strength = float('-inf')
            
            if self.mode == 'mean_reversion':
                if prev[self.rsi_col] <= self.oversold and latest[self.rsi_col] > self.oversold:
                    strength = latest[self.rsi_col] - self.oversold
            else:
                if prev[self.rsi_col] <= 50 and latest[self.rsi_col] > 50:
                    strength = latest[self.rsi_col] - 50
            
            ranked_symbols.append((symbol, strength))

        ranked_symbols.sort(key=lambda x: x[1], reverse=True)
        result = [x[0] for x in ranked_symbols]
        logger.debug('RSIRanker.rank(%s): %d signals', date, len(result))
        return result
