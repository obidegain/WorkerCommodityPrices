from connect_with_bbdd import get_all_data_from_table, connect_to_bbdd
from datetime import datetime
import re
import yfinance as yf

mapping_ticket_file_name = {
        "ZCZ": "CRN.CME/DIC",
        "ZCN": "CRN.CME/JUL",
        "ZCK": "CRN.CME/MAY",
        "ZCU": "CRN.CME/SEP",
        "ZSF": "SOY.CME/ENE",
        "ZSN": "SOY.CME/JUL",
        "ZSK": "SOY.CME/MAY",
        "ZSX": "SOY.CME/NOV"
    }

ticker_commodities = [
    "ZSX", # soja noviembre
    "ZSF", # soja enero
    "ZSK", # soja mayo
    "ZSN", # soja julio

    "ZCZ", # maiz diciembre
    "ZCK", # maiz mayo
    "ZCN", # maiz julio
    "ZCU", # maiz septiembre
]

soy_tickers = ["ZSX", "ZSF", "ZSK", "ZSN"]
crn_tickers = ["ZCZ", "ZCK", "ZCN", "ZCU"]


class YFinanceApiUpdater:
    def __init__(self):
        self.ticker_commodities = ticker_commodities
        self.all_data = self.get_data_of_all_commodities()
        self.cbot_taxes = self.get_cbot_taxes()
        self.futures_dict = self.get_all_last_close_price()

    def get_data_of_all_commodities(self):
        dictionary = {}
        for commodity in self.ticker_commodities:
            current_year = datetime.now().year - 2000
            for i in range(5):
                year = str(current_year + i)
                ticker = commodity + year + ".CBT"
                try:
                    data = yf.download(ticker)
                except:
                    data = list()
                if len(data) > 0:
                    ticker_to_bbdd = f'{mapping_ticket_file_name.get(commodity)}{year}'
                    dictionary[ticker_to_bbdd] = data
        return dictionary

    def get_cbot_taxes(self):
        conn = connect_to_bbdd()
        taxes = get_all_data_from_table(conn, 'current_taxes')
        taxes_dict = dict()
        for row in taxes:
            commodity, market, current_tax = row
            if market == 'CBOT':
                taxes_dict[commodity] = current_tax
        return taxes_dict

    def get_all_last_close_price(self):
        futures_dict = dict()
        for ticker, data in self.all_data.items():
            commodity = ticker.split(".")[0]
            last_price = data.iloc[-1]['Close']
            last_date = data.index[-1].date()
            date = f"{last_date.year}-{last_date.month}-{last_date.day}"
            tax = self.cbot_taxes.get(commodity)
            futures_dict[ticker] = {'date': date, 'last_price': last_price, 'tax': tax}
        return futures_dict

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
