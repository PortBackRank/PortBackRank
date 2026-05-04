'''
Ranker module for the investment strategy.
'''

from abc import ABC, abstractmethod
from typing import List
import random
import pandas as pd
from data import MemData


class Ranker(ABC):
    '''Abstract base class for ranking strategies.'''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        self.interval = interval
        self.data = data
        self.parameters = parameters or {}

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
        self.seed = self.parameters.get('SEED', 42)

    def rank(self, date: str = None) -> List[str]:
        symbols = self.data.get_assets()
        if self.seed is not None:
            random.seed(self.seed)
        random.shuffle(symbols)
        return symbols

    def prepare(self) -> None:
        pass


class MARanker(Ranker):
    '''Ranker based on Mean Reversion strategy using moving averages.'''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        windows = self.parameters.get('window')
        self._short = windows[0]
        self._long = windows[1]
        self.short_col = f'ma{self._short}'
        self.long_col = f'ma{self._long}'

    def prepare(self) -> None:
        '''
        Calculates moving averages vectorized across the MegaDataFrame.
        '''
        df = self.data.get_mega_df()
        if df.empty:
            return

        # Calculate moving averages for all symbols at once using groupby
        # Since MegaDataFrame has MultiIndex (Date, Symbol), we groupby Symbol
        # and calculate rolling mean on 'Close'
        
        # Sort index to ensure chronological order before rolling
        df.sort_index(level=['Symbol', 'Date'], inplace=True)
        
        df[self.short_col] = df.groupby(level='Symbol')['Close'].transform(lambda x: x.rolling(self._short).mean())
        df[self.long_col] = df.groupby(level='Symbol')['Close'].transform(lambda x: x.rolling(self._long).mean())
        
        # Restore original sorting if needed
        df.sort_index(level=['Date', 'Symbol'], inplace=True)

    def rank(self, date: str = None) -> List[str]:
        '''
        Rank symbols based on mean reversion signals using pre-calculated indicators.
        '''
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
        return [x[1] for x in ranked_symbols]


class RSIRanker(Ranker):
    '''Ranker based on Relative Strength Index (RSI) technical indicator.'''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        self.period = int(self.parameters.get('period', 14))
        
        win = self.parameters.get('window')
        try:
            if isinstance(win, (list, tuple)) and len(win) >= 1:
                self.period = int(win[0])
        except Exception:
            pass
        
        self.oversold = float(self.parameters.get('oversold', 30))
        self.overbought = float(self.parameters.get('overbought', 70))
        self.mode = self.parameters.get('mode', 'mean_reversion')
        self.rsi_col = f'rsi{self.period}'

    def prepare(self) -> None:
        '''
        Calculates RSI vectorized across the MegaDataFrame.
        '''
        df = self.data.get_mega_df()
        if df.empty:
            return

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
        df.sort_index(level=['Date', 'Symbol'], inplace=True)

    def rank(self, date: str = None) -> List[str]:
        '''
        Rank symbols based on RSI signal crossovers using pre-calculated indicators.
        '''
        df = self.data.get_mega_df()
        if df.empty or date not in df.index.get_level_values('Date'):
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
        return [x[0] for x in ranked_symbols]


if __name__ == '__main__':
    print("Testing rankers with MegaDataFrame...")
    from data import MemData
    mem_data = MemData(['2024-01-01', '2024-06-30'])
    
    ma_ranker = MARanker(parameters={'window': [9, 21]}, data=mem_data)
    ma_ranker.prepare()
    print("MAs prepared.")
    
    rsi_ranker = RSIRanker(parameters={'period': 14}, data=mem_data)
    rsi_ranker.prepare()
    print("RSI prepared.")