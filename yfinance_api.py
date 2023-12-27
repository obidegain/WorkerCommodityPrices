import yfinance as yf
from datetime import datetime
from decimal import Decimal
import re

ticker_commodities = [
    "ZSX", # soja noviembre
    "ZSF", # soja enero
    "ZSK", # soja mayo
    "ZSN", # soja julio
    "ZCZ", # maiz diciembre
    "ZCK", # maiz mayo
    "ZCN", # maiz julio
    "ZCU", # maix septiembre
]


class YFinanceApiUpdater:
    def __init__(self):
        self.ticker_commodities = ticker_commodities
        self.all_data = self.get_data_of_all_commodities()

    def get_data_of_all_commodities(self):
        dictionary = {}
        for commodity in self.ticker_commodities:
            current_year = datetime.now().year - 2000
            for i in range(5):
                year = str(current_year + i)
                ticker = commodity + year + ".CBT"
                data = yf.download(ticker)
                if len(data) > 0:
                    dictionary[ticker] = data
        return dictionary

    def get_last_trade(self, ticker_commodity):
        data = self.all_data.get(ticker_commodity)
        last_price = data.iloc[-1]['Close']
        last_date = data.index[-1].date()
        date = f"{last_date.year}-{last_date.month}-{last_date.day}"
        return last_price, date

    def get_data(self, ticker_commodity):
        return self.all_data.get(ticker_commodity)

    def get_data_of_specific_date(self, ticker_commodity, date):
        data = self.all_data.get(ticker_commodity)
        re_expresion = r'^\d{4}-\d{2}-\d{2}$'
        if re.match(re_expresion, date):
            return data.iloc[date]['Close']
        return
