'''
    class Runner
'''

from typing import List, Dict, Type
from datetime import datetime
import pandas as pd
from ranker import MARanker, Ranker, RandomRanker
from data import MemData


class Runner:
    def __init__(self, profit, loss, diversification, volume, ranker: Type[Ranker], data: MemData):
        '''
        Initialize Runner with provided parameters.

        :param profit: Target profit threshold for selling (percentage).
        :param loss: Loss limit for selling (percentage).
        :param diversification: Maximum sector diversification (percentage).
        :param volume: Percentage of daily volume to consider.
        :param ranker: Ranker class to use.
        :param data: MemData instance containing historical and sector data.
        '''
        self.profit = profit
        self.loss = loss
        self.diversification = diversification
        self.volume = volume
        self.ranker = ranker
        self.data = data

        # represent purchased stocks (symbol, quantity, purchase price, etc.)
        self.__portfolio: List[Dict[str, float]] = []
        self.balance = 0
        self.timeline = []

    def prepare_data(self, interval: List[str], ranker_conf: Dict[str, float], capital: float):
        '''
        Prepare environment for a new simulation:
        - initialize ranker
        - reset balance, portfolio and logs
        - preload historical data and sectors into memory
        - create date-index for quick lookup
        '''
        # initialize ranker with parameters and data
        self._ranker_instance = self.ranker(parameters=ranker_conf, data=self.data)

        # reset internal state
        self.balance = capital
        self.__portfolio = []
        self.timeline = []
        self.trade_log = []

        # preload history and sector information
        self._all_history = self.data.get_all_history()
        self._all_sectors = self.data.get_all_sectors()

        # pre-index history by date for faster lookup
        self._history_by_date = self.data.get_history_by_date()

        self._ranker_instance.prepare(self.data)

        # keep interval for reference
        self._interval = interval

    def single_run(self, interval: List[str], ranker_conf: Dict[str, float], capital: float) -> Dict:
        '''
        Execute a simulation for a single ranker configuration,
        maintaining portfolio with quantity and purchase price of assets.
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
            'diversification': self.diversification,
            'volume': self.volume
        }

        return {
            'balance': self.balance,
            'portfolio': self.__portfolio,
            'shared_data': shared_data,
            'trade_log': self.trade_log
        }

    def _sell(self, date: str):
        '''
        Sell assets that have reached the profit or loss threshold,
        respecting FIFO order and checking daily volume.
        '''
        asset_histories = {}

        for symbol in [item['symbol'] for item in self.__portfolio]:
            day_row = self._history_by_date.get(symbol, {}).get(date)
            if day_row is not None:
                asset_histories[symbol] = {
                    'current_price': day_row['Close'],
                    'daily_volume': day_row['Volume']
                }

        new_portfolio = []
        for item in self.__portfolio:
            symbol = item['symbol']
            purchase_price = item['purchase_price']
            quantity = item['quantity']
            purchase_date = item['purchase_date']

            if symbol not in asset_histories:
                new_portfolio.append(item)
                continue

            current_price = asset_histories[symbol]['current_price']
            daily_volume = asset_histories[symbol]['daily_volume']

            percent_change = (current_price - purchase_price) / purchase_price

            if percent_change >= self.profit or percent_change <= -self.loss:
                to_sell = min(quantity, daily_volume)
                sale_value = current_price * to_sell
                self.balance += sale_value

                self.trade_log.append({
                    'date': date,
                    'symbol': symbol,
                    'type': 'SELL',
                    'quantity': to_sell,
                    'price': current_price,
                    'cost': purchase_price,
                    'profit_loss': (current_price - purchase_price) * to_sell,
                    'origin_date': purchase_date,  # when it was bought
                    'sector': item.get('sector', 'Unknown')
                })

                if quantity > to_sell:
                    new_portfolio.append({
                        'symbol': symbol,
                        'quantity': quantity - to_sell,
                        'purchase_price': purchase_price,
                        'purchase_date': purchase_date,
                        'sector': item['sector']
                    })
            else:
                new_portfolio.append(item)

        self.__portfolio = new_portfolio

    def _buy(self, date: str, ranker: Ranker):
        '''
        Buy assets based on ranker output, enforcing sector diversification
        and checking available daily volume.
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

            # get sector from cache as 'industry - sector'
            sector = self._all_sectors.get(symbol, 'Unknown - Unknown')
            
            # skip assets without defined sector
            if sector == 'Unknown - Unknown':
                continue

            # compute max investment for this sector
            if sector not in sector_percentage:
                max_sector_investment = available_balance * self.diversification
            else:
                max_sector_investment = (
                    total_portfolio_value * self.diversification -
                    sector_percentage.get(sector, 0) * total_portfolio_value
                )

            # fetch today's historical row
            day_row = historical_data.get(symbol, {}).get(date)
            if day_row is None:
                continue

            current_price = day_row['Close']
            daily_volume = day_row['Volume']

            if pd.isna(current_price) or pd.isna(daily_volume):
                continue

            # calculate how many shares we can buy
            max_qty = int(available_balance // current_price)
            sector_qty = int(max_sector_investment // current_price)
            qty_to_buy = min(
                max_qty, sector_qty, daily_volume)

            if qty_to_buy <= 0:
                continue

            # record purchase
            self.trade_log.append({
                'date': date,
                'symbol': symbol,
                'type': 'BUY',
                'quantity': qty_to_buy,
                'price': current_price,
                'cost': current_price,
                'profit_loss': 0,
                'origin_date': date,
                'sector': sector
            })

            self.__portfolio.append({
                'symbol': symbol,
                'quantity': qty_to_buy,
                'purchase_price': current_price,
                'purchase_date': date,
                'sector': sector
            })

            # update balances
            purchase_value = qty_to_buy * current_price
            available_balance -= purchase_value
            total_portfolio_value += purchase_value
            
            # update sector percentage
            sector_percentage[sector] = sector_percentage.get(sector, 0) + (
                purchase_value / total_portfolio_value
            )

        self.balance = available_balance

    def _record_state(self, date):
        '''
        Record full portfolio and balance state for the given date.

        :param date: Current simulation date.
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
        total_portfolio = sum(item["quantity"] * item["purchase_price"] for item in result["portfolio"])
        print(f'Portfolio value: {total_portfolio}')
    except Exception as e:
        print(f'Error during test: {e}')
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    test_runner_ma()