'''
    class Runner
'''

import os
import pandas as pd
from pathlib import Path
from typing import List, Dict, Type
from ranker import MARanker, Ranker, RandomRanker
from data import MemData
from utils import generate_filename 
from names import (
    COL_SECTOR, COL_DATE, KEY_ASSET, KEY_QUANTITY, KEY_UNIT_VALUE,
    KEY_TOTAL_ASSET_VALUE, KEY_TYPE, KEY_PRICE, KEY_COST, KEY_PROFIT_LOSS,
    KEY_ORIGIN_DATE, KEY_BALANCE, KEY_PORTFOLIO_VALUE, KEY_INTERVAL,
    KEY_PROFIT, KEY_LOSS, KEY_DIVERSIFICATION, KEY_FINAL_TOTAL_VALUE,
    STR_UNKNOWN_FULL, TYPE_BUY, TYPE_SELL, DIR_RESULTS, MARKET_SP500,
    COL_SYMBOL, COL_CLOSE, COL_VOLUME, KEY_WINDOW, SEP_PIPE, DIR_TRACKING
)

class Runner:
    def __init__(self, profit, loss, diversification, volume, ranker: Type[Ranker], data: MemData, trace: bool = False):
        '''
        Initialize Runner with provided parameters.
        '''
        self.profit = profit
        self.loss = loss
        self.diversification = diversification
        self.volume = volume
        self.ranker = ranker
        self.data = data
        self.trace = trace

        self.__portfolio_details = pd.DataFrame(columns=[
            COL_SECTOR, KEY_ASSET, COL_DATE, KEY_QUANTITY, KEY_UNIT_VALUE
        ])

        self.portfolio_summary = {} # Dict: sector -> total_value
        
        self.balance = 0
        self.total_portfolio_value = 0 

    def _update_portfolio_metrics(self):
        """Updates sector summary and total portfolio value in memory."""
        if self.__portfolio_details.empty:
            self.portfolio_summary = {}
            self.total_portfolio_value = self.balance
            return

        # Calculate total value per row
        temp_df = self.__portfolio_details.copy()
        temp_df[KEY_TOTAL_ASSET_VALUE] = temp_df[KEY_QUANTITY] * temp_df[KEY_UNIT_VALUE]

        # Group by sector for the summary
        self.portfolio_summary = temp_df.groupby(COL_SECTOR)[KEY_TOTAL_ASSET_VALUE].sum().to_dict()
        
        # Total Value = Cash + Sum of all assets
        self.total_portfolio_value = self.balance + temp_df[KEY_TOTAL_ASSET_VALUE].sum()

    def prepare_data(self, interval: List[str], ranker_conf: Dict[str, float], capital: float):
        '''Resets environment for a new simulation run.'''
        self._ranker_instance = self.ranker(parameters=ranker_conf, data=self.data)

        self.balance = capital
        self.total_portfolio_value = capital
        self.__portfolio_details = pd.DataFrame({
            COL_SECTOR: pd.Series(dtype='str'),
            KEY_ASSET: pd.Series(dtype='str'),
            COL_DATE: pd.Series(dtype='str'),
            KEY_QUANTITY: pd.Series(dtype='int'),
            KEY_UNIT_VALUE: pd.Series(dtype='float')
        })
        self.portfolio_summary = {}
        self.trade_log = []
        self.portfolio_log = []

        self._all_history = self.data.get_all_history()
        self._all_sectors = self.data.get_all_sectors()
        self._history_by_date = self.data.get_history_by_date()
        self._ranker_instance.prepare(self.data)
        self._interval = interval

    def _log_portfolio_state(self, date: str):
        for index, row in self.__portfolio_details.iterrows():
            symbol = row[KEY_ASSET]
            day_row = self._history_by_date.get(symbol, {}).get(date)
            current_price = day_row[COL_CLOSE] if day_row is not None and not pd.isna(day_row[COL_CLOSE]) else row[KEY_UNIT_VALUE]
            
            self.portfolio_log.append({
                COL_DATE: date,
                COL_SYMBOL: symbol,
                COL_SECTOR: row[COL_SECTOR],
                KEY_QUANTITY: row[KEY_QUANTITY],
                'buy_price': row[KEY_UNIT_VALUE],
                KEY_PRICE: current_price
            })

    def _save_trace(self, result_data: Dict, ranker_conf: Dict):
        if not self.trace:
            return

        project_root = Path(__file__).resolve().parent

        # Obter identifier de maneira segura
        market_id = 'unknown'
        if hasattr(self.data, 'market_identifier'):
            market_id = self.data.market_identifier
        elif hasattr(self.data, 'market'):
            market_id = self.data.market

        ranker_name = self.ranker.__name__
        windows = '-'.join(map(str, ranker_conf.get(KEY_WINDOW, [])))
        
        # Ex: sp500-MARanker-9-21-P01-L005-D01
        folder_name = f"{market_id}-{ranker_name}-{windows}-P{str(self.profit).replace('.', '')}-L{str(self.loss).replace('.', '')}-D{str(self.diversification).replace('.', '')}"
        
        trades_path = project_root / DIR_TRACKING / folder_name / "trades.csv"
        port_path = project_root / DIR_TRACKING / folder_name / "portfolio.csv"

        trades_path.parent.mkdir(parents=True, exist_ok=True)

        # Salva trades.csv (date|symbol|operation|quantity|price|balance)
        if self.trade_log:
            df_trades = pd.DataFrame(self.trade_log)
            rename_map = {KEY_TYPE: 'operation', KEY_BALANCE: 'balance'}
            df_trades = df_trades.rename(columns=rename_map)
            cols = [COL_DATE, COL_SYMBOL, 'operation', KEY_QUANTITY, KEY_PRICE, 'balance']
            cols_to_save = [c for c in cols if c in df_trades.columns]
            df_trades[cols_to_save].to_csv(trades_path, index=False, sep=SEP_PIPE)

        # Salva portfolio.csv (date|symbol|sector|quantity|buy_price|price)
        if self.portfolio_log:
            df_port = pd.DataFrame(self.portfolio_log)
            cols = [COL_DATE, COL_SYMBOL, COL_SECTOR, KEY_QUANTITY, 'buy_price', KEY_PRICE]
            cols_to_save = [c for c in cols if c in df_port.columns]
            df_port[cols_to_save].to_csv(port_path, index=False, sep=SEP_PIPE)

    def single_run(self, interval: List[str], ranker_conf: Dict[str, float], capital: float) -> Dict:
        self.prepare_data(interval, ranker_conf, capital)
        start_date, end_date = interval

        for date in pd.date_range(start_date, end_date).strftime('%Y-%m-%d'):
            trades_before = len(self.trade_log)
            self._sell(date)
            self._buy(date, self._ranker_instance)
            
            if len(self.trade_log) > trades_before:
                self._log_portfolio_state(date)

        # Dados consolidados para retorno
        result = {
            KEY_INTERVAL: f"{interval[0]} - {interval[1]}",
            KEY_PROFIT: self.profit,
            KEY_LOSS: self.loss,
            KEY_DIVERSIFICATION: self.diversification,
            KEY_BALANCE: round(self.balance, 2),
            KEY_FINAL_TOTAL_VALUE: round(self.total_portfolio_value, 2),
            'portfolio_details': self.__portfolio_details.copy()
        }

        self._save_trace(result, ranker_conf)

        return result


    def _sell(self, date: str):
        if self.__portfolio_details.empty:
            return

        symbols_in_portfolio = self.__portfolio_details[KEY_ASSET].unique()
        asset_histories = {}
        for symbol in symbols_in_portfolio:
            day_row = self._history_by_date.get(symbol, {}).get(date)
            if day_row is not None:
                asset_histories[symbol] = {
                    'current_price': day_row[COL_CLOSE],
                    'daily_volume': day_row[COL_VOLUME]
                }

        indices_to_remove = []
        for index, row in self.__portfolio_details.iterrows():
            symbol = row[KEY_ASSET]
            if symbol not in asset_histories:
                continue

            purchase_price = row[KEY_UNIT_VALUE] 
            quantity = row[KEY_QUANTITY]
            current_price = asset_histories[symbol]['current_price']
            daily_volume = asset_histories[symbol]['daily_volume']

            percent_change = (current_price - purchase_price) / purchase_price

            if percent_change >= self.profit or percent_change <= -self.loss:
                to_sell = min(quantity, int(daily_volume * self.volume))
                self.balance += (current_price * to_sell)
                
                self._update_portfolio_metrics()

                self.trade_log.append({
                    COL_DATE: date,
                    COL_SYMBOL: symbol,
                    KEY_TYPE: TYPE_SELL,
                    KEY_QUANTITY: to_sell,
                    KEY_PRICE: current_price,
                    KEY_COST: purchase_price,
                    KEY_PROFIT_LOSS: (current_price - purchase_price) * to_sell,
                    KEY_ORIGIN_DATE: row[COL_DATE],
                    COL_SECTOR: row[COL_SECTOR],
                    KEY_BALANCE: round(self.balance, 2),
                    KEY_PORTFOLIO_VALUE: round(self.total_portfolio_value, 2)
                })

                if quantity > to_sell:
                    self.__portfolio_details.at[index, KEY_QUANTITY] = quantity - to_sell
                else:
                    indices_to_remove.append(index)

        if indices_to_remove:
            self.__portfolio_details = self.__portfolio_details.drop(indices_to_remove).reset_index(drop=True)
        self._update_portfolio_metrics()

    def _buy(self, date: str, ranker: Ranker):
        ranked_symbols = ranker.rank(date)
        if not ranked_symbols:
            return

        total_val = self.total_portfolio_value 
        available_balance = self.balance

        for symbol in ranked_symbols:
            if available_balance <= 2:
                break

            sector = self._all_sectors.get(symbol, STR_UNKNOWN_FULL)
            if sector == STR_UNKNOWN_FULL:
                continue

            current_sector_val = self.portfolio_summary.get(sector, 0)
            
            if total_val <= 0:
                max_sector_investment = available_balance * self.diversification
            else:
                max_sector_investment = (total_val * self.diversification) - current_sector_val

            day_row = self._history_by_date.get(symbol, {}).get(date)
            if day_row is None or pd.isna(day_row[COL_CLOSE]):
                continue

            current_price = day_row[COL_CLOSE]
            daily_volume = day_row[COL_VOLUME]

            max_qty = int(available_balance // current_price)
            sector_qty = int(max(0, max_sector_investment) // current_price)
            qty_to_buy = min(max_qty, sector_qty, int(daily_volume * self.volume))

            if qty_to_buy <= 0:
                continue

            self.balance -= (qty_to_buy * current_price)
            available_balance = self.balance 

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
            total_val = self.total_portfolio_value

            self.trade_log.append({
                COL_DATE: date,
                COL_SYMBOL: symbol,
                KEY_TYPE: TYPE_BUY,
                KEY_QUANTITY: qty_to_buy,
                KEY_PRICE: current_price,
                KEY_BALANCE: round(self.balance, 2),
                KEY_PORTFOLIO_VALUE: round(self.total_portfolio_value, 2)
            })

def test_runner():
    '''Test basic runner functionality with RandomRanker.'''
    interval = ['2024-06-10', '2024-11-10']
    capital = 10000

    ranker_config = {'SEED': 42}

    runner = Runner(
        profit=0.1,
        loss=0.05,
        diversification=0.2,
        ranker=RandomRanker,
        data=MemData(interval)
    )

    try:
        result = runner.single_run(interval, ranker_config, capital)
        print('Runner execution successful')
        print(f'Final balance: {result[KEY_BALANCE]}')
    except Exception as e:
        print(f'Error during test: {e}')
        import traceback
        traceback.print_exc()


def test_runner_ma():
    '''Test runner functionality with Moving Average ranker.'''
    interval = ['2024-04-10', '2024-08-10']
    ranker_config = {'window': [9, 21]}
    capital = 10000

    runner = Runner(
        profit=0.1,
        loss=0.05,
        diversification=0.2,
        volume=0.1, # Adicionado o parâmetro volume que faltava no seu teste
        ranker=MARanker,
        data=MemData(interval, market_identifier=MARKET_SP500)
    )

    try:
        result = runner.single_run(interval, ranker_config, capital)
        print('Runner execution successful')
        print(f"Final balance: {result[KEY_BALANCE]}")
        
        # Acessando o DataFrame de detalhes
        df_portfolio = result['portfolio_details']
        if not df_portfolio.empty:
            total_portfolio = (df_portfolio[KEY_QUANTITY] * df_portfolio[KEY_UNIT_VALUE]).sum()
            print(f'Portfolio value: {round(total_portfolio, 2)}')
        else:
            print('Portfolio is empty.')
            
    except Exception as e:
        print(f'Error during test: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_runner_ma()