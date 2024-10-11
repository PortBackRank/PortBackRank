import backtrader as bt
import json
import itertools
import yfinance as yf
import matplotlib.pyplot as plt

class MovingAverageStrategy(bt.Strategy):
    params = (
        ('short_period', 5),  
        ('long_period', 20), 
    )

    def __init__(self):
        self.short_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.short_period
        )
        self.long_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.long_period
        )

    def next(self):
        if self.short_ma > self.long_ma and not self.position:
            self.buy()  
        elif self.short_ma < self.long_ma and self.position:
            self.sell() 

class Backtester:
    def __init__(self, config_json):
        self.config = json.loads(config_json)
        self.results = []

    def run_backtest(self):
        cerebro = bt.Cerebro()
        cerebro.addstrategy(MovingAverageStrategy,
                             short_period=self.config['short_period'],
                             long_period=self.config['long_period'])
        
        data = self.get_data(self.config['ticker'], self.config['start_date'], self.config['end_date'])
        cerebro.adddata(data)
        
        cerebro.run()
        
        self.analyze_results(cerebro)

        cerebro.plot(style='candlestick')

    def get_data(self, ticker, start_date, end_date):
        df = yf.download(ticker, start=start_date, end=end_date)
        df.reset_index(inplace=True)  

        data = bt.feeds.PandasData(dataname=df, datetime='Date', open='Open', high='High', low='Low', close='Close', volume='Volume', openinterest=None)
        return data

    def analyze_results(self, cerebro):
        self.results.append({
            'final_value': cerebro.broker.getvalue(),
            'start_date': cerebro.datas[0].datetime.date(0),
            'end_date': cerebro.datas[0].datetime.date(-1),
            'num_trades': len(cerebro.broker.get_trades()),
            'pnl': cerebro.broker.getvalue() - cerebro.broker.startingcash,
        })
        print(self.results[-1])

    def grid_search(self, short_range, long_range):
        short_periods = range(short_range[0], short_range[1] + 1)
        long_periods = range(long_range[0], long_range[1] + 1)

        combinations = list(itertools.product(short_periods, long_periods))
        
        best_result = None

        for short_period, long_period in combinations:
            self.config['short_period'] = short_period
            self.config['long_period'] = long_period
            
            self.run_backtest()

            last_result = self.results[-1]
            if best_result is None or last_result['pnl'] > best_result['pnl']:
                best_result = last_result
        
        print("Best Result:", best_result)

def main():
    config_json = json.dumps({
        "short_period": 5,
        "long_period": 20,
        "start_date": '2020-01-01',
        "end_date": '2024-01-01',
    })

    backtester = Backtester(config_json)
    
    backtester.grid_search(short_range=(2, 10), long_range=(10, 30))

if __name__ == "__main__":
    main()
