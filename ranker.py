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
        '''
        Initialize the Ranker with optional parameters and data.

        Args:
            parameters: Optional dictionary containing strategy parameters.
            interval: List of two strings representing start and end dates [start_date, end_date].
            data: MemData instance containing historical data and market information.
        '''
        self.interval = interval
        self.data = data
        self.parameters = parameters or {}

    @abstractmethod
    def rank(self, date: str = None) -> List[str]:
        '''
        Generate a ranked list of stock symbols based on the strategy.

        Args:
            date: Optional date string for ranking calculation.

        Returns:
            List[str]: Ranked stock symbols sorted by strategy criteria.
        '''

    @abstractmethod
    def prepare(self, data: MemData) -> None:
        '''
        Prepare required data or indicators before simulation execution.

        Args:
            data: MemData object containing historical price data and sector information.
        '''


class RandomRanker(Ranker):
    '''Ranker that generates random symbol rankings.'''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        '''
        Initialize RandomRanker with optional seed for reproducibility.

        Args:
            parameters: Optional dictionary containing strategy parameters including 'SEED'.
            interval: List of two strings representing start and end dates.
            data: MemData instance for data retrieval.
        '''
        super().__init__(parameters, interval, data)
        self.seed = self.parameters.get('SEED', 42)

    def rank(self, date: str = None) -> List[str]:
        '''
        Generate a random ranking of available symbols.

        Args:
            date: Unused parameter (for interface compatibility).

        Returns:
            List[str]: Symbols in random order.
        '''
        symbols = self.data.get_assets()

        if self.seed is not None:
            random.seed(self.seed)

        random.shuffle(symbols)

        return symbols

    def prepare(self, data: MemData) -> None:
        '''
        No preparation required for random ranking.

        Args:
            data: MemData object (unused).
        '''
        pass


def test_random_ranker():
    '''Test RandomRanker functionality with sample data.'''
    interval = ['2024-01-10', '2024-11-10']
    data = MemData(interval=interval)

    parameters = {'SEED': 42}

    ranker = RandomRanker(data=data, parameters=parameters)
    ranked_symbols = ranker.rank()

    print('Randomly ranked symbols:', ranked_symbols)


class MARanker(Ranker):
    '''Ranker based on Mean Reversion strategy using moving averages.'''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        '''
        Initialize MARanker with short and long moving average windows.

        Args:
            parameters: Dictionary containing 'window' key with [short_period, long_period].
            interval: List of two strings representing start and end dates.
            data: MemData instance for data retrieval.
        '''
        super().__init__(parameters, interval, data)
        windows = self.parameters.get('window')
        self._short = windows[0]
        self._long = windows[1]

    def rank(self, date: str = None) -> List[str]:
        '''
        Rank symbols based on mean reversion signals.

        Identifies symbols where the short-term MA crosses above the long-term MA,
        indicating potential mean reversion opportunities.

        Args:
            date: Date string for ranking calculation.

        Returns:
            List[str]: Symbols ranked by mean reversion strength (highest first).
        '''
        dict_data = self.data.get_all_history()
        ranked_symbols = []
        
        for symbol, df_data in dict_data.items():
            # Calculate moving averages
            short_col = f'ma{self._short}'
            long_col = f'ma{self._long}'
            df_data[short_col] = df_data['Close'].rolling(self._short).mean()
            df_data[long_col] = df_data['Close'].rolling(self._long).mean()
            
            # Default to negative infinity (worst ranking)
            strength = float('-inf')

            if date in df_data.index:
                idx = df_data.index.get_loc(date)

                # Handle slice type indices
                if isinstance(idx, slice):
                    idx = idx.start

                # Ensure sufficient history for comparison
                if idx >= 1:
                    latest = df_data.iloc[idx]
                    prev = df_data.iloc[idx - 1]
                    
                    # Detect mean reversion: short MA crosses above long MA
                    if prev[short_col] <= prev[long_col] and latest[short_col] > latest[long_col]:
                        strength = (latest[short_col] / latest[long_col] - 1) * 100
            else:
                continue

            ranked_symbols.append((strength, symbol))

        # Sort by strength in descending order
        ranked_symbols.sort(reverse=True)
        return [x[1] for x in ranked_symbols]

    def prepare(self, data: MemData) -> None:
        '''
        No preparation required for mean reversion ranking.

        Args:
            data: MemData object (unused).
        '''
        pass


def test_ma_ranker():
    '''Test MARanker functionality with sample data and date.'''
    interval = ['2024-01-10', '2024-11-10']
    data = MemData(interval=interval)

    parameters = {'window': [9, 21]}

    ranker = MARanker(data=data, parameters=parameters)
    ranked_symbols = ranker.rank(date='2024-05-29')

    print('Mean Reversion ranked symbols:', ranked_symbols)


if __name__ == '__main__':
    test_ma_ranker()


class RSIRanker(Ranker):
    '''Ranker based on Relative Strength Index (RSI) technical indicator.'''

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        '''
        Initialize RSIRanker with RSI period and thresholds.

        Args:
            parameters: Dictionary with 'period', 'oversold', 'overbought', and 'mode' keys.
            interval: List of two strings representing start and end dates.
            data: MemData instance for data retrieval.
        '''
        super().__init__(parameters, interval, data)
        self.period = int(self.parameters.get('period', 14))
        
        # Override period from window parameter if provided
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
        '''
        Calculate and add RSI column to dataframe if not present.

        Args:
            df: DataFrame with 'Close' price column.

        Returns:
            DataFrame with RSI column added.
        '''
        key = f'rsi{self.period}'
        if key in df.columns:
            return df
        
        # Calculate price changes
        delta = df['Close'].diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        
        # Calculate exponential moving averages of gains and losses
        roll_up = up.ewm(alpha=1 / self.period, adjust=False).mean()
        roll_down = down.ewm(alpha=1 / self.period, adjust=False).mean()
        
        # Calculate RSI
        rs = roll_up / roll_down
        rsi = 100 - (100 / (1 + rs))
        
        # Handle infinite values and fill gaps
        df[key] = rsi.replace([float('inf'), float('-inf')], float('nan')).ffill()
        return df

    def rank(self, date: str = None) -> List[str]:
        '''
        Rank symbols based on RSI signal crossovers.

        In mean_reversion mode: identifies symbols crossing above oversold level.
        In momentum mode: identifies symbols crossing above 50 level.

        Args:
            date: Date string for ranking calculation.

        Returns:
            List[str]: Symbols ranked by signal strength (highest first).
        '''
        dict_data = self.data.get_all_history()
        ranked_symbols = []
        key = f'rsi{self.period}'
        
        for symbol, df in dict_data.items():
            # Skip invalid or empty data
            if 'Close' not in df.columns or df.empty:
                continue
            
            df = self._ensure_rsi(df.copy())
            
            # Skip if date not in index
            if date not in df.index:
                continue
            
            idx = df.index.get_loc(date)
            
            # Handle slice type indices
            if isinstance(idx, slice):
                idx = idx.start
            
            # Require sufficient history
            if idx < 1:
                continue
            
            latest = df.iloc[idx]
            prev = df.iloc[idx - 1]
            strength = float('-inf')
            
            # Detect signal crossover based on mode
            if self.mode == 'mean_reversion':
                # Mean reversion: detect oversold bounce
                if prev[key] <= self.oversold and latest[key] > self.oversold:
                    strength = latest[key] - self.oversold
            else:
                # Momentum: detect midline cross
                if prev[key] <= 50 and latest[key] > 50:
                    strength = latest[key] - 50
            
            ranked_symbols.append((symbol, strength))

        # Sort by strength in descending order
        ranked_symbols.sort(key=lambda x: x[1], reverse=True)
        return [x[0] for x in ranked_symbols]

    def prepare(self, data: MemData) -> None:
        '''
        No preparation required for RSI ranking.

        Args:
            data: MemData object (unused).
        '''
        pass