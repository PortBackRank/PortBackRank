
'''
Ranker Module - Asset Ranking Strategies

Provides abstract Ranker base class and concrete implementations
(MARanker, RandomRanker, RSIRanker) for ranking assets based on
trading signals and technical indicators.
'''

from abc import ABC, abstractmethod
from typing import List
import random
import pandas as pd
from data import MemData


class Ranker(ABC):
    '''
    Abstract base class for asset ranking strategies.
    
    Defines the interface for implementations that rank assets
    based on market data and technical indicators.
    '''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        '''
        Initializes ranker with strategy parameters.
        
        :param parameters: Strategy-specific parameter dict
        :param interval: [start_date, end_date] for data filtering
        :param data: MemData instance with historical data and sectors
        '''
        self.interval = interval

        self.data = data
        self.parameters = parameters or {}

    @abstractmethod
    def rank(self, date: str = None) -> List[str]:
        '''
        Abstract ranking method to be implemented by subclasses.
        
        :param date: Reference date for ranking calculation
        :return: Sorted list of asset symbols ranked by signal strength
        '''


class RandomRanker(Ranker):
    '''
    Random asset ranker for baseline strategy testing.
    
    Shuffles the asset list randomly for comparison against
    smarter ranking strategies. Supports seeding for reproducibility.
    '''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        '''
        Initializes RandomRanker with optional seed.
        
        :param parameters: Dict with optional 'SEED' key for reproducibility
        :param interval: Date interval for data
        :param data: MemData instance
        '''
        super().__init__(parameters, interval, data)
        self.seed = self.parameters.get('SEED', 42)

    def rank(self, date: str = None) -> List[str]:
        '''
        Returns randomly shuffled asset list.
        
        :param date: Ignored for random ranking
        :return: Randomly permuted list of all available assets
        '''
        symbols = self.data.get_assets()

        if self.seed is not None:
            random.seed(self.seed)

        random.shuffle(symbols)

        return symbols


def test_random_ranker():
    '''
    Simple function to test RandomRanker functionality.
    '''
    interval = ['2024-01-10', '2024-11-10']
    data = MemData(interval=interval)

    parameters = {'SEED': 42}

    ranker = RandomRanker(data=data, parameters=parameters)
    ranked_symbols = ranker.rank()

    print('Randomly ranked symbols:', ranked_symbols)


class MARanker(Ranker):
    '''
    Mean Reversion ranker using Moving Average crossover signals.
    
    Identifies buy signals when short MA crosses above long MA,
    indicating potential mean reversion opportunities.
    '''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        windows = self.parameters.get('window')
        self._short = windows[0]
        self._long = windows[1]

    def rank(self, date: str = None) -> List[str]:
        dict_data = self.data.get_all_history()
        ranked_symbols = []
        for symbol, df_data in dict_data.items():
            # Calculate means
            short = 'w' + str(self._short)
            long = 'w' + str(self._long)
            df_data[short] = df_data['Close'].rolling(self._short).mean()
            df_data[long] = df_data['Close'].rolling(self._long).mean()
            # Default strength is negative infinity
            strength = float('-inf')

            if date in df_data.index:
                idx = df_data.index.get_loc(date)

                if isinstance(idx, slice):
                    idx = idx.start  # or idx.stop

                # Check if index is valid (>= 1)
                if idx >= 1:
                    latest = df_data.iloc[idx]
                    prev = df_data.iloc[idx-1]
                    # Check for mean reversion
                    if prev[short] <= prev[long] and latest[short] > latest[long]:
                        # Calculate strength
                        strength = (latest[short] / latest[long] - 1) * 100
            else:
                continue

            ranked_symbols.append((strength, symbol ))

        # ranked_symbols.sort(key=lambda x: x[1], reverse=True)
        ranked_symbols.sort(reverse=True)       
        return [x[1] for x in ranked_symbols]


def test_ma_ranker():
    '''
    Simple function to test MARanker functionality.
    '''
    interval = ['2024-01-10', '2024-11-10']
    data = MemData(interval=interval)

    parameters = {'window': [9, 21]}

    ranker = MARanker(data=data, parameters=parameters)
    ranked_symbols = ranker.rank(date='2024-05-29')

    print('Symbols ranked by Mean Reversion:', ranked_symbols)


if __name__ == '__main__':
    test_ma_ranker()

class RSIRanker(Ranker):
    '''
    Relative Strength Index (RSI) based ranker.
    
    Identifies overbought/oversold conditions and mean reversion
    opportunities based on RSI values.
    '''

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

    def _ensure_rsi(self, df: pd.DataFrame) -> pd.DataFrame:
        key = f'rsi{self.period}'
        if key in df.columns:
            return df
        delta = df['Close'].diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        roll_up = up.ewm(alpha=1 / self.period, adjust=False).mean()
        roll_down = down.ewm(alpha=1 / self.period, adjust=False).mean()
        rs = roll_up / roll_down
        rsi = 100 - (100 / (1 + rs))
        df[key] = rsi.replace([float('inf'), float('-inf')], float('nan')).ffill()
        return df

    def rank(self, date: str = None) -> List[str]:
        dict_data = self.data.get_all_history()
        ranked_symbols = []
        key = f'rsi{self.period}'
        for symbol, df in dict_data.items():
            if 'Close' not in df.columns or df.empty:
                continue
            df = self._ensure_rsi(df.copy())
            if date not in df.index:
                continue
            idx = df.index.get_loc(date)
            if isinstance(idx, slice):
                idx = idx.start
            if idx < 1:
                continue
            latest = df.iloc[idx]
            prev = df.iloc[idx - 1]
            strength = float('-inf')
            if self.mode == 'mean_reversion':
                if prev[key] <= self.oversold and latest[key] > self.oversold:
                    strength = latest[key] - self.oversold
            else:
                if prev[key] <= 50 and latest[key] > 50:
                    strength = latest[key] - 50
            ranked_symbols.append((symbol, strength))

        ranked_symbols.sort(key=lambda x: x[1], reverse=True)
        return [x[0] for x in ranked_symbols]
