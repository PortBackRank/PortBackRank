"""
Ranker module for the investment strategy.
"""

import logging
from abc import ABC, abstractmethod
from typing import List
import random
import pandas as pd
from data import MemData

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
        self._seed = self.parameters.get('SEED', 42)
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

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        windows = self.parameters.get('window')
        self._short = windows[0]
        self._long = windows[1]
        self._short_col = f'ma{self._short}'
        self._long_col = f'ma{self._long}'
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

    def prepare(self) -> None:
        """
        Calculates moving averages vectorized across the MegaDataFrame.
        """
        df = self.data.get_mega_df()
        if df.empty:
            logger.debug('MARanker.prepare: empty dataframe')
            return

        logger.debug('MARanker.prepare: calculating MAs for %d rows', len(df))
        # Calculate moving averages for all symbols at once using groupby
        # Since MegaDataFrame has MultiIndex (Date, Symbol), we groupby Symbol
        # and calculate rolling mean on 'Close'
        
        # Sort index to ensure chronological order before rolling
        df.sort_index(level=['Symbol', 'Date'], inplace=True)
        
        df[self.short_col] = df.groupby(level='Symbol')['Close'].transform(lambda x: x.rolling(self._short).mean())
        df[self.long_col] = df.groupby(level='Symbol')['Close'].transform(lambda x: x.rolling(self._long).mean())
        
        # Restore original sorting if needed
        df.sort_index(level=['Date', 'Symbol'], inplace=True)
        logger.debug('MARanker.prepare: MAs calculated (%s, %s)', self.short_col, self.long_col)

    def rank(self, date: str = None) -> List[str]:
        """
        Rank symbols based on mean reversion signals using pre-calculated indicators.
        """
        df = self.data.get_mega_df()
        if df.empty or date not in df.index.get_level_values('Date'):
            return []

        # Get data for the current date
        current_date_data = df.xs(date, level='Date')
        
        # Find previous date in the index
        all_dates = df.index.get_level_values('Date').unique()
        date_loc = all_dates.get_loc(date)
        
        if date_loc < 1:
            return []
            
        prev_date = all_dates[date_loc - 1]
        prev_date_data = df.xs(prev_date, level='Date')

        ranked_symbols = []
        
        for symbol in current_date_data.index:
            if symbol not in prev_date_data.index:
                continue
                
            latest = current_date_data.loc[symbol]
            prev = prev_date_data.loc[symbol]
            
            # Check if MAs are calculated
            if pd.isna(latest[self.short_col]) or pd.isna(latest[self.long_col]) or \
               pd.isna(prev[self.short_col]) or pd.isna(prev[self.long_col]):
                continue

            strength = float('-inf')
            
            # Detect mean reversion: short MA crosses above long MA
            if prev[self.short_col] <= prev[self.long_col] and latest[self.short_col] > latest[self.long_col]:
                strength = (latest[self.short_col] / latest[self.long_col] - 1) * 100
                
            ranked_symbols.append((strength, symbol))

        ranked_symbols.sort(reverse=True)
        result = [x[1] for x in ranked_symbols]
        logger.debug('MARanker.rank(%s): %d signals', date, len(result))
        return result


class RSIRanker(Ranker):
    """Ranker based on Relative Strength Index (RSI) technical indicator."""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        self._period = int(self.parameters.get('period', 14))
        
        win = self.parameters.get('window')
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
        df.sort_index(level=['Symbol', 'Date'], inplace=True)
        
        def compute_rsi(group):
            delta = group.diff()
            up = delta.clip(lower=0)
            down = -delta.clip(upper=0)
            roll_up = up.ewm(alpha=1 / self.period, adjust=False).mean()
            roll_down = down.ewm(alpha=1 / self.period, adjust=False).mean()
            rs = roll_up / roll_down
            rsi = 100 - (100 / (1 + rs))
            return rsi.replace([float('inf'), float('-inf')], float('nan')).ffill()

        df[self.rsi_col] = df.groupby(level='Symbol')['Close'].transform(compute_rsi)
        df[self.rsi_col] = df.groupby(level='Symbol')['Close'].transform(compute_rsi)
        df.sort_index(level=['Date', 'Symbol'], inplace=True)
        logger.debug('RSIRanker.prepare: RSI calculated (%s)', self.rsi_col)

    def rank(self, date: str = None) -> List[str]:
        """
        Rank symbols based on RSI signal crossovers using pre-calculated indicators.
        """
        df = self.data.get_mega_df()
        if df.empty or date not in df.index.get_level_values('Date'):
            logger.debug('RSIRanker.rank(%s): no data', date)
            return []

        current_date_data = df.xs(date, level='Date')
        
        all_dates = df.index.get_level_values('Date').unique()
        date_loc = all_dates.get_loc(date)
        
        if date_loc < 1:
            return []
            
        prev_date = all_dates[date_loc - 1]
        prev_date_data = df.xs(prev_date, level='Date')

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


if __name__ == '__main__':
    print('Testing rankers with MegaDataFrame...')
    from data import MemData
    mem_data = MemData(['2024-01-01', '2024-06-30'])
    
    ma_ranker = MARanker(parameters={'window': [9, 21]}, data=mem_data)
    ma_ranker.prepare()
    print('MAs prepared.')
    
    rsi_ranker = RSIRanker(parameters={'period': 14}, data=mem_data)
    rsi_ranker.prepare()
    print('RSI prepared.')