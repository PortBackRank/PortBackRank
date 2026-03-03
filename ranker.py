
"""
Ranker module for the investment strategy.
"""

from abc import ABC, abstractmethod
from typing import List
import random
import pandas as pd
from data import MemData


class Ranker(ABC):
    """class abstract Ranker"""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        """
        Constructor for the Ranker class, 
        which defines default parameters for the investment strategy.

        :param parameters: Optional dictionary of parameters for the strategy.
        :param interval: List of two strings representing the start and end dates of the data to be used.
        :param data: Data instance to be used for the strategy.
            If not provided, the current date will be used.
        """
        self.interval = interval

        self.data = data
        self.parameters = parameters or {}

    @abstractmethod
    def rank(self, date: str = None) -> List[str]:
        """
        Abstract method that must be implemented by subclasses.

        :return: List of ranked stock symbols.
        """


class RandomRanker(Ranker):
    """RandomRanker class"""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        """
        Constructor for the RandomRanker class, allowing for an optional seed for reproducibility.

        :param parameters: Optional dictionary of parameters for the strategy.
        :param date: List of two strings representing the start and end dates of the data to be used.
        :param data: Data instance to be used for the strategy.
        :param seed: Optional seed for randomization.
        """
        super().__init__(parameters, interval, data)
        self.seed = self.parameters.get("SEED", 42)

    def rank(self, date: str = None) -> List[str]:
        """
        Generates a random ranking of symbols based on the data retrieved from the `Data` instance.

        :return: List of symbols in random order.
        """
        symbols = self.data.get_assets()

        if self.seed is not None:
            random.seed(self.seed)

        random.shuffle(symbols)

        return symbols


def test_random_ranker():
    """
    Função simples para testar o funcionamento do RandomRanker.
    """
    interval = ["2024-01-10", "2024-11-10"]
    data = MemData(interval=interval)

    parameters = {"SEED": 42}

    ranker = RandomRanker(data=data, parameters=parameters)
    ranked_symbols = ranker.rank()

    print("Símbolos ranqueados aleatoriamente:", ranked_symbols)


class MARanker(Ranker):
    """Mean Reversion Ranker class"""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        windows = self.parameters.get("window")
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
                    idx = idx.start  # ou idx.stop

                # Verifique se o índice é válido (>= 1)
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
    """
    Função simples para testar o funcionamento do MARanker.
    """
    interval = ["2024-01-10", "2024-11-10"]
    data = MemData(interval=interval)

    parameters = {"window": [9, 21]}

    ranker = MARanker(data=data, parameters=parameters)
    ranked_symbols = ranker.rank(date="2024-05-29")

    print("Símbolos ranqueados por Mean Reversion:", ranked_symbols)


if __name__ == "__main__":
    test_ma_ranker()

class RSIRanker(Ranker):
    """RSI Ranker Class"""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        self.period = int(self.parameters.get("period", 14))
        win = self.parameters.get("window")
        try:
            if isinstance(win, (list, tuple)) and len(win) >= 1:
                self.period = int(win[0])
        except Exception:
            pass
        self.oversold = float(self.parameters.get("oversold", 30))
        self.overbought = float(self.parameters.get("overbought", 70))
        self.mode = self.parameters.get("mode", "mean_reversion")

    @staticmethod
    def _compute_rsi(close: pd.Series, period: int) -> pd.Series:
        """
        Calcula o RSI pelo método clássico de Wilder
        A série resultante só começa a ter valores após o período inicial
        """
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        rsi = pd.Series(index=close.index, dtype="float64")

        # Necessário pelo menos (period + 1) pontos para ter delta suficiente
        if len(close) < period + 1:
            return rsi

        avg_gain = gain.iloc[1:period + 1].mean()
        avg_loss = loss.iloc[1:period + 1].mean()

        def _calc_rsi(avg_g, avg_l):
            if avg_l == 0 and avg_g == 0:
                return 50.0
            if avg_l == 0:
                return 100.0
            if avg_g == 0:
                return 0.0
            rs = avg_g / avg_l
            return 100 - (100 / (1 + rs))

        rsi.iloc[period] = _calc_rsi(avg_gain, avg_loss)

        for i in range(period + 1, len(close)):
            avg_gain = (avg_gain * (period - 1) + gain.iloc[i]) / period
            avg_loss = (avg_loss * (period - 1) + loss.iloc[i]) / period
            rsi.iloc[i] = _calc_rsi(avg_gain, avg_loss)

        return rsi

    def _ensure_rsi(self, df: pd.DataFrame) -> pd.DataFrame:
        key = f"rsi{self.period}"
        if key in df.columns:
            return df
        df[key] = self._compute_rsi(df['Close'], self.period)
        return df

    def rank(self, date: str = None) -> List[str]:
        dict_data = self.data.get_all_history()
        ranked_symbols = []
        key = f"rsi{self.period}"
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
                if pd.notna(prev[key]) and pd.notna(latest[key]):
                    # Compra no cruzamento para fora da sobrevenda
                    if prev[key] <= self.oversold < latest[key]:
                        strength = latest[key] - self.oversold
                    # Penaliza ativos que acabaram de sair da sobrecompra
                    elif prev[key] >= self.overbought > latest[key]:
                        strength = - (prev[key] - self.overbought)
            else:
                if pd.notna(prev[key]) and pd.notna(latest[key]):
                    # Tendência: cruzamento acima de 50, limita em sobrecompra
                    threshold = 50
                    if prev[key] <= threshold < latest[key] <= self.overbought:
                        strength = latest[key] - threshold
            ranked_symbols.append((symbol, strength))

        ranked_symbols.sort(key=lambda x: x[1], reverse=True)
        return [x[0] for x in ranked_symbols]


def test_rsi_ranker():
    """
    Função simples para testar o funcionamento do RSIRanker
    """
    interval = ["2024-01-10", "2024-11-10"]
    data = MemData(interval=interval)

    parameters = {
        "window": [14],
        "oversold": 30,
        "overbought": 70,
        "mode": "mean_reversion",
    }

    ranker = RSIRanker(data=data, parameters=parameters)
    ranked_symbols = ranker.rank(date="2024-05-29")

    print("Símbolos ranqueados por RSI:", ranked_symbols)


class EMARanker(Ranker):
    """EMA Ranker Class"""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        windows = self.parameters.get("window")
        self._short = windows[0]
        self._long = windows[1]

    def rank(self, date: str = None) -> List[str]:
        dict_data = self.data.get_all_history()
        ranked_symbols = []
        for symbol, df_data in dict_data.items():
            # Calculate exponential moving averages
            short = 'ema' + str(self._short)
            long = 'ema' + str(self._long)
            df_data[short] = df_data['Close'].ewm(span=self._short, adjust=False).mean()
            df_data[long] = df_data['Close'].ewm(span=self._long, adjust=False).mean()
            # Default strength is negative infinity
            strength = float('-inf')

            if date in df_data.index:
                idx = df_data.index.get_loc(date)

                if isinstance(idx, slice):
                    idx = idx.start

                # Verifique se o índice é válido (>= 1)
                if idx >= 1:
                    latest = df_data.iloc[idx]
                    prev = df_data.iloc[idx-1]
                    # Check EMA crossover
                    if prev[short] <= prev[long] and latest[short] > latest[long]:
                        # Calculate strength
                        strength = (latest[short] / latest[long] - 1) * 100
            else:
                continue

            ranked_symbols.append((strength, symbol))

        ranked_symbols.sort(reverse=True)
        return [x[1] for x in ranked_symbols]


def test_ema_ranker():
    """
    Função simples para testar o funcionamento do EMARanker
    """
    interval = ["2024-01-10", "2024-11-10"]
    data = MemData(interval=interval)

    parameters = {"window": [9, 21]}

    ranker = EMARanker(data=data, parameters=parameters)
    ranked_symbols = ranker.rank(date="2024-05-29")

    print("Símbolos ranqueados por EMA:", ranked_symbols)


class BollingerRanker(Ranker):
    """Bollinger Bands Ranker class"""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        self.period = int(self.parameters.get("period", 20))
        self.std_dev = float(self.parameters.get("std_dev", 2))
        self.mode = self.parameters.get("mode", "mean_reversion")

    def _ensure_bbands(self, df: pd.DataFrame) -> pd.DataFrame:
        mid_key = f"bb_mid_{self.period}"
        std_key = f"bb_std_{self.period}"
        upper_key = f"bb_upper_{self.period}_{self.std_dev}"
        lower_key = f"bb_lower_{self.period}_{self.std_dev}"

        if upper_key in df.columns and lower_key in df.columns:
            return df

        df[mid_key] = df["Close"].rolling(self.period).mean()
        df[std_key] = df["Close"].rolling(self.period).std()
        df[upper_key] = df[mid_key] + (self.std_dev * df[std_key])
        df[lower_key] = df[mid_key] - (self.std_dev * df[std_key])
        return df

    def rank(self, date: str = None) -> List[str]:
        dict_data = self.data.get_all_history()
        ranked_symbols = []
        upper_key = f"bb_upper_{self.period}_{self.std_dev}"
        lower_key = f"bb_lower_{self.period}_{self.std_dev}"

        for symbol, df in dict_data.items():
            if "Close" not in df.columns or df.empty:
                continue

            df = self._ensure_bbands(df.copy())

            if date not in df.index:
                continue

            idx = df.index.get_loc(date)
            if isinstance(idx, slice):
                idx = idx.start
            if idx < 1:
                continue

            latest = df.iloc[idx]
            prev = df.iloc[idx - 1]
            strength = float("-inf")

            if pd.notna(prev[lower_key]) and pd.notna(latest[lower_key]) and pd.notna(prev[upper_key]) and pd.notna(latest[upper_key]):
                band_width = latest[upper_key] - latest[lower_key]
                norm = band_width if pd.notna(band_width) and band_width > 0 else 1.0

                if self.mode == "mean_reversion":
                    # Compra ao reentrar na banda após tocar/romper a banda inferior.
                    if prev["Close"] <= prev[lower_key] and latest["Close"] > latest[lower_key]:
                        strength = ((latest["Close"] - latest[lower_key]) / norm) * 100
                    # Penaliza saída da sobrecompra.
                    elif prev["Close"] >= prev[upper_key] and latest["Close"] < latest[upper_key]:
                        strength = -((prev["Close"] - prev[upper_key]) / norm) * 100
                else:
                    # Modo tendência: breakout acima da banda superior.
                    if prev["Close"] <= prev[upper_key] and latest["Close"] > latest[upper_key]:
                        strength = ((latest["Close"] - latest[upper_key]) / norm) * 100

            ranked_symbols.append((symbol, strength))

        ranked_symbols.sort(key=lambda x: x[1], reverse=True)
        return [x[0] for x in ranked_symbols]


def test_bollinger_ranker():
    """
    Função simples para testar o funcionamento do BollingerRanker.
    """
    interval = ["2024-01-10", "2024-11-10"]
    data = MemData(interval=interval)

    parameters = {
        "period": 20,
        "std_dev": 2,
        "mode": "mean_reversion",
    }

    ranker = BollingerRanker(data=data, parameters=parameters)
    ranked_symbols = ranker.rank(date="2024-05-29")

    print("Símbolos ranqueados por Bollinger Bands:", ranked_symbols)


class MACDRanker(Ranker):
    """MACD Ranker Class"""

    def __init__(self, parameters: dict = None, interval: List[str] = None, data: MemData = None):
        super().__init__(parameters, interval, data)
        self.fast = int(self.parameters.get("fast", 12))
        self.slow = int(self.parameters.get("slow", 26))
        self.signal = int(self.parameters.get("signal", 9))
        self.mode = self.parameters.get("mode", "trend")

        if self.fast >= self.slow:
            raise ValueError("No MACD, 'fast' deve ser menor que 'slow'.")

    def _ensure_macd(self, df: pd.DataFrame) -> pd.DataFrame:
        macd_key = f"macd_{self.fast}_{self.slow}"
        signal_key = f"macd_signal_{self.signal}"
        hist_key = f"macd_hist_{self.fast}_{self.slow}_{self.signal}"

        if macd_key in df.columns and signal_key in df.columns and hist_key in df.columns:
            return df

        ema_fast = df["Close"].ewm(span=self.fast, adjust=False).mean()
        ema_slow = df["Close"].ewm(span=self.slow, adjust=False).mean()
        df[macd_key] = ema_fast - ema_slow
        df[signal_key] = df[macd_key].ewm(span=self.signal, adjust=False).mean()
        df[hist_key] = df[macd_key] - df[signal_key]
        return df

    def rank(self, date: str = None) -> List[str]:
        dict_data = self.data.get_all_history()
        ranked_symbols = []
        macd_key = f"macd_{self.fast}_{self.slow}"
        signal_key = f"macd_signal_{self.signal}"
        hist_key = f"macd_hist_{self.fast}_{self.slow}_{self.signal}"

        for symbol, df in dict_data.items():
            if "Close" not in df.columns or df.empty:
                continue

            df = self._ensure_macd(df.copy())

            if date not in df.index:
                continue

            idx = df.index.get_loc(date)
            if isinstance(idx, slice):
                idx = idx.start
            if idx < 1:
                continue

            latest = df.iloc[idx]
            prev = df.iloc[idx - 1]
            strength = float("-inf")

            if pd.notna(prev[macd_key]) and pd.notna(prev[signal_key]) and pd.notna(latest[macd_key]) and pd.notna(latest[signal_key]):
                crossed_up = prev[macd_key] <= prev[signal_key] and latest[macd_key] > latest[signal_key]

                if self.mode == "trend":
                    if crossed_up and latest[macd_key] > 0:
                        strength = latest[hist_key]
                else:
                    if crossed_up and latest[macd_key] < 0:
                        strength = abs(latest[hist_key])

            ranked_symbols.append((symbol, strength))

        ranked_symbols.sort(key=lambda x: x[1], reverse=True)
        return [x[0] for x in ranked_symbols]


def test_macd_ranker():
    """
    Função simples para testar o funcionamento do MACDRanker.
    """
    interval = ["2024-01-10", "2024-11-10"]
    data = MemData(interval=interval)

    parameters = {
        "fast": 12,
        "slow": 26,
        "signal": 9,
        "mode": "trend",
    }

    ranker = MACDRanker(data=data, parameters=parameters)
    ranked_symbols = ranker.rank(date="2024-05-29")

    print("Símbolos ranqueados por MACD:", ranked_symbols)
