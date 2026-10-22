"""
    class Runner
"""

import logging
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Type, Optional
from ranker import Ranker
from data import MemData
from utils import dir_tracking
from names import *

logger = logging.getLogger(__name__)


@dataclass
class RunnerConfig:
    """Configuration parameters for execution of a strategy runner."""
    profit: float
    loss: float
    diversification: float
    volume: float
    ranker_cls: Type[Ranker]
    trace: bool = False
    decimals: int = 2

    def __post_init__(self):
        self.profit = validate_positive_float(self.profit, 'profit')
        self.loss = validate_positive_float(self.loss, 'loss')
        self.diversification = validate_diversification(self.diversification)
        self.volume = validate_positive_float(self.volume, 'volume')
        self.decimals = validate_decimals(self.decimals)


class Runner:
    def __init__(
        self, 
        profit: float = None, 
        loss: float = None, 
        diversification: float = None, 
        volume: float = None, 
        ranker: Type[Ranker] = None, 
        data: MemData = None, 
        trace: bool = False, 
        decimals: int = 2,
        config: Optional[RunnerConfig] = None
    ):
        """
        Initialize Runner with provided parameters.
        
        Args:
            profit: Take profit threshold (e.g., 0.1 for 10%)
            loss: Stop loss threshold (e.g., 0.05 for 5%)
            diversification: Max sector allocation as fraction of portfolio (0-1]
            volume: Max daily volume participation (e.g., 0.1 for 10%)
            ranker: Ranker class to use
            data: MemData instance with market data
            trace: Enable trace logging to files
            decimals: Decimal places for output rounding
        """
        if config is not None:
            self._config = config
        else:
            self._config = RunnerConfig(
                profit=profit,
                loss=loss,
                diversification=diversification,
                volume=volume,
                ranker_cls=ranker,
                trace=trace,
                decimals=decimals
            )

        # Configuration (read-only after init)
        self._profit = self._config.profit
        self._loss = self._config.loss
        self._diversification = self._config.diversification
        self._volume = self._config.volume
        self._ranker_cls = self._config.ranker_cls
        self._data = data
        self._trace = self._config.trace
        self._decimals = self._config.decimals

        # Internal state
        self.__portfolio_details = pd.DataFrame(columns=[
            COL_SECTOR, KEY_ASSET, COL_DATE, KEY_QUANTITY, KEY_UNIT_VALUE
        ])

        self._portfolio_summary = {} # Dict: sector -> total_value
        
        self._balance = 0
        self._total_portfolio_value = 0
        
        self._ranker_instance = None
        self._all_sectors = None
        self._mega_df = None
        self._interval = None
        self._trade_log = []
        self._portfolio_log = []
        
        logger.debug('Runner initialized: profit=%s, loss=%s, div=%s, vol=%s, decimals=%d', 
                     self._profit, self._loss, self._diversification, self._volume, self._decimals)

    # Configuration properties (read-only)
    @property
    def config(self) -> RunnerConfig:
        return self._config

    @property
    def profit(self) -> float:
        return self._profit

    @property
    def loss(self) -> float:
        return self._loss

    @property
    def diversification(self) -> float:
        return self._diversification

    @property
    def volume(self) -> float:
        return self._volume

    @property
    def ranker(self) -> Type[Ranker]:
        return self._ranker_cls

    @property
    def data(self) -> MemData:
        return self._data

    @property
    def trace(self) -> bool:
        return self._trace

    @property
    def decimals(self) -> int:
        return self._decimals

    # State properties (read-only)
    @property
    def balance(self) -> float:
        return self._balance

    @property
    def total_portfolio_value(self) -> float:
        return self._total_portfolio_value

    @property
    def portfolio_summary(self) -> Dict[str, float]:
        return self._portfolio_summary.copy()

    @property
    def portfolio_details(self) -> pd.DataFrame:
        return self.__portfolio_details.copy() 

    def _update_portfolio_metrics(self):
        """Updates sector summary and total portfolio value in memory."""
        if self.__portfolio_details.empty:
            self._portfolio_summary = {}
            self._total_portfolio_value = self._balance
            return

        # Calculate total value per row
        temp_df = self.__portfolio_details.copy()
        temp_df[KEY_TOTAL_ASSET_VALUE] = temp_df[KEY_QUANTITY] * temp_df[KEY_UNIT_VALUE]

        # Group by sector for the summary
        self._portfolio_summary = temp_df.groupby(COL_SECTOR)[KEY_TOTAL_ASSET_VALUE].sum().to_dict()
        
        # Total Value = Cash + Sum of all assets
        self._total_portfolio_value = self._balance + temp_df[KEY_TOTAL_ASSET_VALUE].sum()

    def prepare_data(self, interval: List[str], ranker_conf: Dict[str, float], capital: float):
        """Resets environment for a new simulation run."""
        logger.debug('Preparing runner for interval=%s, capital=%s, ranker_conf=%s', interval, capital, ranker_conf)
        self._ranker_instance = self.ranker(parameters=ranker_conf, data=self.data)

        self._balance = capital
        self._total_portfolio_value = capital
        self.__portfolio_details = pd.DataFrame({
            COL_SECTOR: pd.Series(dtype='str'),
            KEY_ASSET: pd.Series(dtype='str'),
            COL_DATE: pd.Series(dtype='str'),
            KEY_QUANTITY: pd.Series(dtype='int'),
            KEY_UNIT_VALUE: pd.Series(dtype='float')
        })
        self._portfolio_summary = {}
        self._trade_log = []
        self._portfolio_log = []

        self._all_sectors = self.data.get_all_sectors()
        self._mega_df = self.data.get_mega_df()
        
        self._ranker_instance.prepare()
        self._interval = interval
        logger.debug('Runner prepared: %d assets, %d dates', len(self._all_sectors), len(self._mega_df.index.get_level_values(COL_DATE).unique()) if not self._mega_df.empty else 0)

    def _log_portfolio_state(self, date: str):
        # We pre-slice the dataframe for the given date to speed up lookups
        if not self._mega_df.empty and date in self._mega_df.index.get_level_values(COL_DATE):
            current_date_data = self._mega_df.xs(date, level=COL_DATE)
        else:
            current_date_data = None

        for index, row in self.__portfolio_details.iterrows():
            symbol = row[KEY_ASSET]
            current_price = row[KEY_UNIT_VALUE]
            
            if current_date_data is not None and symbol in current_date_data.index:
                price_val = current_date_data.loc[symbol, COL_CLOSE]
                if not pd.isna(price_val):
                    current_price = price_val
            
            self._portfolio_log.append({
                COL_DATE: date,
                COL_SYMBOL: symbol,
                COL_SECTOR: row[COL_SECTOR],
                KEY_QUANTITY: row[KEY_QUANTITY],
                KEY_BUY_PRICE: row[KEY_UNIT_VALUE],
                KEY_PRICE: current_price
            })

    def _save_trace(self, result_data: Dict, ranker_conf: Dict):
        if not self._trace:
            return

        # Obter identifier de maneira segura
        market_id = STR_UNKNOWN_ID
        if hasattr(self.data, 'market_identifier'):
            market_id = self.data.market_identifier

        ranker_name = self.ranker.__name__
        windows = '-'.join(map(str, ranker_conf.get(KEY_WINDOW, [])))
        
        # Ex: sp500-MARanker-9-21-P01-L005-D01
        folder_name = f"{market_id}-{ranker_name}-{windows}-P{str(self._profit).replace('.', '')}-L{str(self._loss).replace('.', '')}-D{str(self._diversification).replace('.', '')}"
        
        tracking_base = Path(dir_tracking())
        trades_path = tracking_base / folder_name / FILE_TRADES
        port_path = tracking_base / folder_name / FILE_PORTFOLIO

        trades_path.parent.mkdir(parents=True, exist_ok=True)

        # Salva trades.csv (date|symbol|operation|quantity|price|balance)
        if self._trade_log:
            df_trades = pd.DataFrame(self._trade_log)
            rename_map = {KEY_TYPE: KEY_OPERATION, KEY_BALANCE: KEY_BALANCE}
            df_trades = df_trades.rename(columns=rename_map)
            cols = [COL_DATE, COL_SYMBOL, KEY_OPERATION, KEY_QUANTITY, KEY_PRICE, KEY_BALANCE]
            cols_to_save = [c for c in cols if c in df_trades.columns]
            df_trades[cols_to_save].to_csv(trades_path, index=False, sep=SEP_PIPE)

        # Salva portfolio.csv (date|symbol|sector|quantity|buy_price|price)
        if self._portfolio_log:
            df_port = pd.DataFrame(self._portfolio_log)
            cols = [COL_DATE, COL_SYMBOL, COL_SECTOR, KEY_QUANTITY, KEY_BUY_PRICE, KEY_PRICE]
            cols_to_save = [c for c in cols if c in df_port.columns]
            df_port[cols_to_save].to_csv(port_path, index=False, sep=SEP_PIPE)

    def single_run(self, interval: List[str], ranker_conf: Dict[str, float], capital: float) -> Dict:
        logger.debug('Starting single_run: interval=%s, capital=%s', interval, capital)
        self.prepare_data(interval, ranker_conf, capital)
        start_date, end_date = interval
        trade_count = 0

        # Iterating over the dates present in MegaDataFrame or just a date_range
        # Let's use date_range for consistency, but checking if data exists
        for date in pd.date_range(start_date, end_date).strftime(DATE_FMT):
            if self._mega_df.empty or date not in self._mega_df.index.get_level_values(COL_DATE):
                continue
                
            current_date_data = self._mega_df.xs(date, level=COL_DATE)
            trades_before = len(self._trade_log)
            self._sell(date, current_date_data)
            self._buy(date, self._ranker_instance, current_date_data)
            
            if len(self._trade_log) > trades_before:
                self._log_portfolio_state(date)
                trade_count += len(self._trade_log) - trades_before

        logger.debug('Simulation complete: %d trades executed', trade_count)
        # Dados consolidados para retorno
        result = {
            KEY_INTERVAL: f'{interval[0]} - {interval[1]}',
            KEY_PROFIT: self._profit,
            KEY_LOSS: self._loss,
            KEY_DIVERSIFICATION: self._diversification,
            KEY_BALANCE: round(self._balance, self._decimals),
            KEY_FINAL_TOTAL_VALUE: round(self._total_portfolio_value, self._decimals),
            KEY_PORTFOLIO_DETAILS: self.__portfolio_details.copy()
        }

        self._save_trace(result, ranker_conf)

        return result


    def _sell(self, date: str, current_date_data: pd.DataFrame):
        if self.__portfolio_details.empty:
            return

        symbols_in_portfolio = self.__portfolio_details[KEY_ASSET].unique()
        asset_histories = {}
        for symbol in symbols_in_portfolio:
            if symbol in current_date_data.index:
                row = current_date_data.loc[symbol]
                asset_histories[symbol] = {
                    KEY_CURRENT_PRICE: row[COL_CLOSE],
                    KEY_DAILY_VOLUME: row[COL_VOLUME]
                }

        indices_to_remove = []
        sell_count = 0
        for index, row in self.__portfolio_details.iterrows():
            symbol = row[KEY_ASSET]
            if symbol not in asset_histories:
                continue

            purchase_price = row[KEY_UNIT_VALUE] 
            quantity = row[KEY_QUANTITY]
            current_price = asset_histories[symbol][KEY_CURRENT_PRICE]
            daily_volume = asset_histories[symbol][KEY_DAILY_VOLUME]

            percent_change = (current_price - purchase_price) / purchase_price

            if percent_change >= self._profit or percent_change <= -self._loss:
                sell_count += 1
                reason = 'take_profit' if percent_change >= self._profit else 'stop_loss'
                logger.debug('SELL %s: %d shares at %.2f (%s: %.2f%%)', symbol, min(quantity, int(daily_volume * self._volume)), current_price, reason, percent_change * 100)
                to_sell = min(quantity, int(daily_volume * self._volume))
                self._balance += (current_price * to_sell)
                
                self._update_portfolio_metrics()

                self._trade_log.append({
                    COL_DATE: date,
                    COL_SYMBOL: symbol,
                    KEY_TYPE: TYPE_SELL,
                    KEY_QUANTITY: to_sell,
                    KEY_PRICE: current_price,
                    KEY_COST: purchase_price,
                    KEY_PROFIT_LOSS: (current_price - purchase_price) * to_sell,
                    KEY_ORIGIN_DATE: row[COL_DATE],
                    COL_SECTOR: row[COL_SECTOR],
                    KEY_BALANCE: round(self._balance, self._decimals),
                    KEY_PORTFOLIO_VALUE: round(self._total_portfolio_value, self._decimals)
                })

                if quantity > to_sell:
                    self.__portfolio_details.at[index, KEY_QUANTITY] = quantity - to_sell
                else:
                    indices_to_remove.append(index)

        if indices_to_remove:
            self.__portfolio_details = self.__portfolio_details.drop(indices_to_remove).reset_index(drop=True)
        self._update_portfolio_metrics()

    def _buy(self, date: str, ranker: Ranker, current_date_data: pd.DataFrame):
        ranked_symbols = ranker.rank(date)
        if not ranked_symbols:
            logger.debug('No ranked symbols for %s', date)
            return

        total_val = self._total_portfolio_value 
        available_balance = self._balance
        buy_count = 0

        for symbol in ranked_symbols:
            if available_balance <= MIN_BALANCE:
                logger.debug('Insufficient balance for further buys: %s', available_balance)
                break

            sector = self._all_sectors.get(symbol, STR_UNKNOWN_FULL)
            if sector == STR_UNKNOWN_FULL:
                logger.debug('Skipping %s: unknown sector', symbol)
                continue

            current_sector_val = self._portfolio_summary.get(sector, 0)
            
            if total_val <= 0:
                max_sector_investment = available_balance * self._diversification
            else:
                max_sector_investment = (total_val * self._diversification) - current_sector_val

            if symbol not in current_date_data.index:
                logger.debug('Skipping %s: not in current date data', symbol)
                continue
                
            day_row = current_date_data.loc[symbol]
            if pd.isna(day_row[COL_CLOSE]):
                logger.debug('Skipping %s: NaN price', symbol)
                continue

            current_price = day_row[COL_CLOSE]
            daily_volume = day_row[COL_VOLUME]

            max_qty = int(available_balance // current_price)
            sector_qty = int(max(0, max_sector_investment) // current_price)
            qty_to_buy = min(max_qty, sector_qty, int(daily_volume * self._volume))

            if qty_to_buy <= 0:
                logger.debug('Skipping %s: qty_to_buy=%d (max_qty=%d, sector_qty=%d, vol_limit=%d)', 
                            symbol, qty_to_buy, max_qty, sector_qty, int(daily_volume * self._volume))
                continue

            buy_count += 1
            logger.debug('BUY %s: %d shares at %.2f (sector=%s, avail_bal=%.2f)', symbol, qty_to_buy, current_price, sector, available_balance)
            self._balance -= (qty_to_buy * current_price)
            available_balance = self._balance 

            new_buy = {
                COL_SECTOR: sector,
                KEY_ASSET: symbol,
                COL_DATE: date,
                KEY_QUANTITY: qty_to_buy,
                KEY_UNIT_VALUE: current_price
            }
            self.__portfolio_details = pd.concat(
                [self.__portfolio_details, pd.DataFrame([new_buy])], 
                ignore_index=True
            )

            self._update_portfolio_metrics()
            total_val = self._total_portfolio_value

            self._trade_log.append({
                COL_DATE: date,
                COL_SYMBOL: symbol,
                KEY_TYPE: TYPE_BUY,
                KEY_QUANTITY: qty_to_buy,
                KEY_PRICE: current_price,
                KEY_BALANCE: round(self._balance, self._decimals),
                KEY_PORTFOLIO_VALUE: round(self._total_portfolio_value, self._decimals)
            })
