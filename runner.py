'''
    class Runner
'''

from typing import List, Dict, Type
import pandas as pd
import os
from ranker import MARanker, Ranker, RandomRanker
from data import MemData
from utils import generate_filename 

class Runner:
    def __init__(self, profit, loss, diversification, volume, ranker: Type[Ranker], data: MemData):
        '''
        Initialize Runner with provided parameters.
        '''
        self.profit = profit
        self.loss = loss
        self.diversification = diversification
        self.volume = volume
        self.ranker = ranker
        self.data = data

        self.__portfolio_details = pd.DataFrame(columns=[
            'sector', 'asset', 'date', 'quantity', 'unit_value'
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
        temp_df['total_asset_value'] = temp_df['quantity'] * temp_df['unit_value']

        # Group by sector for the summary
        self.portfolio_summary = temp_df.groupby('sector')['total_asset_value'].sum().to_dict()
        
        # Total Value = Cash + Sum of all assets
        self.total_portfolio_value = self.balance + temp_df['total_asset_value'].sum()

    def prepare_data(self, interval: List[str], ranker_conf: Dict[str, float], capital: float):
        '''Resets environment for a new simulation run.'''
        self._ranker_instance = self.ranker(parameters=ranker_conf, data=self.data)

        self.balance = capital
        self.total_portfolio_value = capital
        self.__portfolio_details = pd.DataFrame({
            'sector': pd.Series(dtype='str'),
            'asset': pd.Series(dtype='str'),
            'date': pd.Series(dtype='str'),
            'quantity': pd.Series(dtype='int'),
            'unit_value': pd.Series(dtype='float')
        })
        self.portfolio_summary = {}
        self.trade_log = []

        self._all_history = self.data.get_all_history()
        self._all_sectors = self.data.get_all_sectors()
        self._history_by_date = self.data.get_history_by_date()
        self._ranker_instance.prepare(self.data)
        self._interval = interval

    def _save_logs(self, result_data: Dict, ranker_conf: Dict):
        """Cria pastas e salva arquivos CSV."""
        import os
        from utils import generate_filename

        # Contexto para o nome do arquivo (precisa de todos os parâmetros)
        ctx = {**result_data, **ranker_conf}
        start_date, end_date = result_data['interval'].split(' - ')

        # Caminhos
        trades_path = generate_filename('results/trade_logs/trades', ctx, start_date, end_date).replace('.json', '.csv')
        port_path = generate_filename('results/portfolios/final_portfolio', ctx, start_date, end_date).replace('.json', '.csv')

        # Cria as pastas se não existirem
        os.makedirs(os.path.dirname(trades_path), exist_ok=True)
        os.makedirs(os.path.dirname(port_path), exist_ok=True)

        # Salva Trade Log
        if self.trade_log:
            pd.DataFrame(self.trade_log).to_csv(trades_path, index=False)
        
        # Salva Portfolio Final
        if not self.__portfolio_details.empty:
            self.__portfolio_details.to_csv(port_path, index=False)

    def single_run(self, interval: List[str], ranker_conf: Dict[str, float], capital: float) -> Dict:
        self.prepare_data(interval, ranker_conf, capital)
        start_date, end_date = interval

        for date in pd.date_range(start_date, end_date).strftime('%Y-%m-%d'):
            self._sell(date)
            self._buy(date, self._ranker_instance)

        # Dados consolidados para retorno
        result = {
            'interval': f"{interval[0]} - {interval[1]}",
            'profit': self.profit,
            'loss': self.loss,
            'diversification': self.diversification,
            'balance': round(self.balance, 2),
            'final_total_value': round(self.total_portfolio_value, 2)
        }

        self._save_logs(result, ranker_conf)

        return result


    def _sell(self, date: str):
        if self.__portfolio_details.empty:
            return

        symbols_in_portfolio = self.__portfolio_details['asset'].unique()
        asset_histories = {}
        for symbol in symbols_in_portfolio:
            day_row = self._history_by_date.get(symbol, {}).get(date)
            if day_row is not None:
                asset_histories[symbol] = {
                    'current_price': day_row['Close'],
                    'daily_volume': day_row['Volume']
                }

        indices_to_remove = []
        for index, row in self.__portfolio_details.iterrows():
            symbol = row['asset']
            if symbol not in asset_histories:
                continue

            purchase_price = row['unit_value'] 
            quantity = row['quantity']
            current_price = asset_histories[symbol]['current_price']
            daily_volume = asset_histories[symbol]['daily_volume']

            percent_change = (current_price - purchase_price) / purchase_price

            if percent_change >= self.profit or percent_change <= -self.loss:
                to_sell = min(quantity, daily_volume)
                self.balance += (current_price * to_sell)
                
                self._update_portfolio_metrics()

                self.trade_log.append({
                    'date': date,
                    'symbol': symbol,
                    'type': 'SELL',
                    'quantity': to_sell,
                    'price': current_price,
                    'cost': purchase_price,
                    'profit_loss': (current_price - purchase_price) * to_sell,
                    'origin_date': row['date'],
                    'sector': row['sector'],
                    'total_balance': round(self.balance, 2),
                    'total_portfolio_value': round(self.total_portfolio_value, 2)
                })

                if quantity > to_sell:
                    self.__portfolio_details.at[index, 'quantity'] = quantity - to_sell
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

            sector = self._all_sectors.get(symbol, 'Unknown - Unknown')
            if sector == 'Unknown - Unknown':
                continue

            current_sector_val = self.portfolio_summary.get(sector, 0)
            
            if total_val <= 0:
                max_sector_investment = available_balance * self.diversification
            else:
                max_sector_investment = (total_val * self.diversification) - current_sector_val

            day_row = self._history_by_date.get(symbol, {}).get(date)
            if day_row is None or pd.isna(day_row['Close']):
                continue

            current_price = day_row['Close']
            daily_volume = day_row['Volume']

            max_qty = int(available_balance // current_price)
            sector_qty = int(max(0, max_sector_investment) // current_price)
            qty_to_buy = min(max_qty, sector_qty, daily_volume)

            if qty_to_buy <= 0:
                continue

            self.balance -= (qty_to_buy * current_price)
            available_balance = self.balance 

            new_buy = {
                'sector': sector,
                'asset': symbol,
                'date': date,
                'quantity': qty_to_buy,
                'unit_value': current_price
            }
            self.__portfolio_details = pd.concat(
                [self.__portfolio_details, pd.DataFrame([new_buy])], 
                ignore_index=True
            )

            self._update_portfolio_metrics()
            total_val = self.total_portfolio_value

            self.trade_log.append({
                'date': date,
                'symbol': symbol,
                'type': 'BUY',
                'quantity': qty_to_buy,
                'price': current_price,
                'total_balance': round(self.balance, 2),
                'total_portfolio_value': round(self.total_portfolio_value, 2)
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
        print(f'Final balance: {result["balance"]}')
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
        data=MemData(interval, market_identifier='SP500')
    )

    try:
        result = runner.single_run(interval, ranker_config, capital)
        print('Runner execution successful')
        print(f"Final balance: {result['balance']}")
        
        # Acessando o DataFrame de detalhes
        df_portfolio = result['portfolio_details']
        if not df_portfolio.empty:
            total_portfolio = (df_portfolio['quantity'] * df_portfolio['unit_value']).sum()
            print(f'Portfolio value: {round(total_portfolio, 2)}')
        else:
            print('Portfolio is empty.')
            
    except Exception as e:
        print(f'Error during test: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_runner_ma()