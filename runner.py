'''
Runner Module - Simulation Execution Engine

Provides the Runner class which executes trading simulations
based on portfolio management rules and asset ranking strategies.
'''

from typing import List, Dict, Type
from datetime import datetime
import pandas as pd
from ranker import MARanker, Ranker, RandomRanker
from data import MemData
from names import COL_BALANCE, COL_PORTFOLIO, COL_QUANTITY, COL_PURCHASE_PRICE, COL_SYMBOL_FIELD, COL_SECTOR_INFO, COL_ENTRY_DATE


class Runner:
    '''
    Trading simulation engine that executes buy/sell decisions.
    
    Maintains portfolio with position tracking and executes trades
    according to profit targets, loss limits, and diversification rules.
    '''
    def __init__(self, profit, loss, diversification, ranker: Type[Ranker], data: MemData):
        '''
        Initializes the trading engine.

        :param profit: Profit target percentage (e.g., 0.1 = 10% gain triggers sell)
        :param loss: Maximum loss percentage (e.g., 0.05 = 5% loss triggers sell)
        :param diversification: Max portfolio % per sector (e.g., 0.2 = 20% max per sector)
        :param ranker: Asset ranking strategy class (MARanker, RandomRanker, etc.)
        :param data: MemData instance with historical prices and sectors
        '''
        self.profit = profit
        self.loss = loss
        self.diversification = diversification
        self.ranker = ranker
        self.data = data

    def prepare_data(self, interval: List[str], ranker_conf: Dict[str, float], capital: float):
        '''
        Prepares the simulation environment.
        
        Initializes ranker, resets state, pre-loads data tables,
        and creates date indexes for O(1) history lookups.
        
        :param interval: [start_date, end_date] for simulation period
        :param ranker_conf: Configuration dict for the ranker
        :param capital: Initial capital amount
        '''
        # Initialize ranker with parameters and data
        self._ranker_instance = self.ranker(parameters=ranker_conf, data=self.data)

        # Reset internal states
        self.balance = capital
        self.__portfolio = []
        self.timeline = []
        self.sell_log = []
        self.buy_log = []

        # Pre-load historical data
        self._all_history = self.data.get_all_history()
        
        # Pre-load sectors from CSV
        self._all_sectors = self.data.get_all_sectors()

        # Pre-index historical data by date for faster access
        self._history_by_date = {}
        for symbol, df in self._all_history.items():
            df = df.copy()
            df['date_str'] = df.index.strftime('%Y-%m-%d')
            self._history_by_date[symbol] = {
                d: row for d, row in df.set_index('date_str').iterrows()
            }

        # Store interval
        self._interval = interval

    def single_run(self, interval: List[str], ranker_conf: Dict[str, float], capital: float) -> Dict:
        '''
        Executes a single simulation from start_date to end_date.
        
        Processes daily price data, executing sell orders (profit/loss targets),
        then buy orders (based on ranking), and recording state.
        
        :param interval: Simulation period [start_date, end_date]
        :param ranker_conf: Ranker configuration
        :param capital: Starting capital
        :return: Dict with final balance, portfolio, and execution logs
        '''
        self.prepare_data(interval, ranker_conf, capital)

        start_date, end_date = interval
        ranker = self._ranker_instance

        for date in pd.date_range(start_date, end_date).strftime('%Y-%m-%d'):
            self._sell(date)
            self._buy(date, ranker)
            self._record_state(date)

        shared_data = {
            'timeline': self.timeline,
            'profit': self.profit,
            'loss': self.loss,
            'diversification': self.diversification
        }

        return {
            'balance': self.balance,
            'portfolio': self.__portfolio,
            'shared_data': shared_data,
            'sell_log': self.sell_log,
            'buy_log': self.buy_log
        }

    def _sell(self, date: str):
        '''
        Executes sell orders for positions meeting profit/loss criteria.
        
        Checks each position against profit target or loss limit.
        Uses FIFO ordering for partial fills respecting daily volume.
        Updates portfolio and records every transaction.
        
        :param date: Current simulation date
        '''
        historical_assets = {}

        for symbol in [item['symbol'] for item in self.__portfolio]:
            day_row = self._history_by_date.get(symbol, {}).get(date)
            if day_row is not None:
                historical_assets[symbol] = {
                    'current_price': day_row['Close'],
                    'daily_volume': day_row['Volume']
                }

        new_portfolio = []
        for item in self.__portfolio:
            symbol = item['symbol']
            purchase_price = item['purchase_price']
            quantity = item['quantity']
            purchase_date = item['purchase_date']

            if symbol not in historical_assets:
                new_portfolio.append(item)
                continue

            current_price = historical_assets[symbol]['current_price']
            daily_volume = historical_assets[symbol]['daily_volume']

            percentage_change = (current_price - purchase_price) / purchase_price

            if percentage_change >= self.profit or percentage_change <= -self.loss:
                quantity_to_sell = min(quantity, daily_volume)
                sale_value = current_price * quantity_to_sell
                self.balance += sale_value

                self.sell_log.append({
                    'sale_date': date,
                    'symbol': symbol,
                    'quantity_sold': quantity_to_sell,
                    'purchase_price': purchase_price,
                    'sale_price': current_price,
                    'profit_loss': (current_price - purchase_price) * quantity_to_sell,
                    'purchase_date': purchase_date
                })

                if quantity > quantity_to_sell:
                    new_portfolio.append({
                        'symbol': symbol,
                        'quantity': quantity - quantity_to_sell,
                        'purchase_price': purchase_price,
                        'purchase_date': purchase_date,
                        'sector': item['sector']
                    })
            else:
                new_portfolio.append(item)

        self.__portfolio = new_portfolio

    def _buy(self, date: str, ranker: Ranker):
        '''
        Executes buy orders based on ranking and constraints.
        
        Ranks assets, respects sector diversification limits, checks
        daily volume availability, and logs all purchases.
        
        :param date: Current simulation date
        :param ranker: Initialized ranker instance for ranking assets
        '''
        ranked_symbols = ranker.rank(date)
        if not ranked_symbols:
            return

        historical_data = self._history_by_date

        total_portfolio_value = sum(
            item['purchase_price'] * item['quantity'] for item in self.__portfolio
        )

        sector_percentage = {}
        if total_portfolio_value > 0:
            for item in self.__portfolio:
                sector = item.get('sector')
                purchase_price = item.get('purchase_price', 0)
                quantity = item.get('quantity', 0)
                if not sector or sector == 'Unknown - Unknown':
                    continue
                item_value = purchase_price * quantity
                sector_percentage[sector] = sector_percentage.get(
                    sector, 0) + (item_value / total_portfolio_value)

        available_balance = self.balance

        for symbol in ranked_symbols:
            if available_balance <= 2:
                break

            # Gets the sector from the CSV cache as a string ('industry - sector')
            sector = self._all_sectors.get(symbol, 'Unknown - Unknown')
            
            # Ignores assets without defined sector
            if sector == 'Unknown - Unknown':
                continue

            # Calculates maximum investment in sector
            if sector not in sector_percentage:
                max_investment_sector = available_balance * self.diversification
            else:
                max_investment_sector = (
                    total_portfolio_value * self.diversification -
                    sector_percentage.get(sector, 0) * total_portfolio_value
                )

            # Retrieves historical data for the day
            day_row = historical_data.get(symbol, {}).get(date)
            if day_row is None:
                continue

            current_price = day_row['Close']
            daily_volume = day_row['Volume']

            if pd.isna(current_price) or pd.isna(daily_volume):
                continue

            # Calculates quantity to buy
            quantity_max = int(available_balance // current_price)
            quantity_sector = int(max_investment_sector // current_price)
            quantity_to_buy = min(
                quantity_max, quantity_sector, daily_volume)

            if quantity_to_buy <= 0:
                continue

            # Records purchase
            self.buy_log.append({
                'purchase_date': date,
                'symbol': symbol,
                'quantity': quantity_to_buy,
                'purchase_price': current_price,
                'sector': sector
            })

            self.__portfolio.append({
                'symbol': symbol,
                'quantity': quantity_to_buy,
                'purchase_price': current_price,
                'purchase_date': date,
                'sector': sector
            })

            # Updates balances
            purchase_value = quantity_to_buy * current_price
            available_balance -= purchase_value
            total_portfolio_value += purchase_value
            
            # Updates sector percentage
            sector_percentage[sector] = sector_percentage.get(sector, 0) + (
                purchase_value / total_portfolio_value
            )

        self.balance = available_balance

    def _record_state(self, date):
        '''
        Records portfolio state for analysis and visualization.
        
        Creates timeline entry with current balance, portfolio composition,
        and position details for later backtest analysis.

        :param date: Current simulation date
        '''
        self.timeline.append({
            'date': date,
            'balance': float(self.balance),
            'portfolio': [
                {
                    'symbol': item['symbol'],
                    'quantity': int(item['quantity']),
                    'purchase_price': float(item['purchase_price']),
                    'purchase_date': item['purchase_date'],
                    'sector': item['sector']
                }
                for item in self.__portfolio
            ]
        })


def test_runner():
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
        print(f'Final balance: {result["balance"]}')
    except Exception as e:
        print(f'Error during test: {e}')
        import traceback
        traceback.print_exc()


def test_runner_ma():
    interval = ['2024-04-10', '2024-08-10']
    ranker_config = {'window': [9, 21]}

    runner = Runner(
        profit=0.1,
        loss=0.05,
        diversification=0.2,
        ranker=MARanker,
        data=MemData(interval, market_identifier='SP500')
    )

    try:
        result = runner.single_run(interval, ranker_config, capital=10000)
        print('Runner execution successful')
        print(f'Final balance: {result["balance"]}')
        print(f'Portfolio value: {sum(item["quantity"] * item["purchase_price"] for item in result["portfolio"])}')
    except Exception as e:
        print(f'Error during test: {e}')
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    test_runner_ma()