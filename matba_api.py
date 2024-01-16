from collections import defaultdict
from connect_with_bbdd import get_all_data_from_table, connect_to_bbdd
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

mapping_ticket_file_name = {
        "CRN.CME/NOV": "CRN.CME_DIC",
        "SOJ.ROS/ENE": "SOJ.ROS_ENE",
        "CRN.CME/JUN": "CRN.CME_JUL",
        "SOJ.ROS/JUL": "SOJ.ROS_JUL",
        "CRN.CME/ABR": "CRN.CME_MAY",
        "SOJ.ROS/MAY": "SOJ.ROS_MAY",
        "MAI.ROS/ABR": "MAI.ROS_ABR",
        "SOJ.ROS/NOV": "SOJ.ROS_NOV",
        "MAI.ROS/DIC": "MAI.ROS_DIC",
        "SOY.CME/DIC": "SOY.CME_ENE",
        "MAI.ROS/JUL": "MAI.ROS_JUL",
        "SOY.CME/JUN": "SOY.CME_JUL",
        "MAI.ROS/SEP": "MAI.ROS_SEP",
        "SOY.CME/ABR": "SOY.CME_MAY",
        "SOY.CME/OCT": "SOY.CME_NOV"
    }

mapping_months = {
            'ENE': 'DIC',
            'FEB': 'ENE',
            'MAR': 'FEB',
            'ABR': 'MAR',
            'MAY': 'ABR',
            'JUN': 'MAY',
            'JUL': 'JUN',
            'AGO': 'JUL',
            'SEP': 'AGO',
            'OCT': 'SEP',
            'NOV': 'OCT',
            'DIC': 'NOV'
        }


class MatbaApiUpdater:
    def __init__(self):
        self.token = self.get_token()
        self.all_data = self.get_all_agro_tickers_availables()
        self.matba_taxes = self.get_matba_taxes()
        self.futures_dict = self.get_all_last_close_price()

    def get_token(self):
        url = 'https://api.remarkets.primary.com.ar/auth/getToken'
        matba_api_username = os.getenv('MATBA_API_USERNAME')
        matba_api_password = os.getenv('MATBA_API_PASSWORD')
        matba_api_username = 'octabidegain8345'
        matba_api_password = 'viwioA1!'

        headers = {
            'X-Username': matba_api_username,
            'X-Password': matba_api_password
        }

        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            token = response.headers.get("X-Auth-Token")
            print(f"Token: {token}")
            return token
        else:
            return None

    def get_all_agro_tickers_availables(self):
        if self.token:
            token = self.token
        else:
            token = self.get_token()
            self.token = token

        url = 'https://api.remarkets.primary.com.ar/rest/instruments/details'
        headers = {
            'X-Auth-Token': token
        }
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            instruments_json = response.json()
            agro_instruments = [dictionary for dictionary in instruments_json['instruments'] if dictionary['segment']['marketSegmentId'] == 'DDA']
            agro_symbols = [instruments['instrumentId']['symbol'] for instruments in agro_instruments]
            futures_dict = self.extract_only_futures_data(agro_symbols)
            return futures_dict
        return

    # def get_ticker_api_from_file_name(self, file_name):
    #     commodity = file_name.split('.')[0]
    #     market = file_name.split('.')[1].split('_')[0]
    #     expiration_month = file_name.split('.')[1].split('_')[1]
    #
    #     futures_dict = self.futures_dict
    #
    #     result = [d for d in futures_dict if (
    #             d['commodity'] == commodity and
    #             d['market'] == market and
    #             d['expiration month'] == expiration_month
    #     )]
    #
    #     if len(result) >= 1:
    #         return result
    #     return ""

    def extract_only_futures_data(self, agro_symbols):
        futures = list()
        others = list()
        for symbol in agro_symbols:
            if len(symbol) == 13:
                commodity, rest = symbol.split('.')
                market, expiration = rest.split('/')
                close_prices_data = self.get_all_close_prices(symbol)
                data = {
                    'symbol': symbol,
                    'commodity': commodity,
                    'market': market,
                    'expiration month': expiration[:3],
                    'expiration year': expiration[3:],
                    'close_prices_data': close_prices_data
                }
                futures.append(data)
            else:
                others.append(symbol)

        return futures

    def get_all_last_close_price(self):
        futures_dict = dict()
        for data in self.all_data:
            ticker = data.get('symbol')
            commodity = ticker.split(".")[0]
            if data.get('close_prices_data') and len(data.get('close_prices_data')) > 0 and "ROS" in ticker:
                last_trade = data.get('close_prices_data')[-1]
                last_price = last_trade.get('price')
                date = last_trade.get('date')
                tax = self.matba_taxes.get(commodity)
                futures_dict[ticker] = {'date': date, 'last_price': last_price, 'tax': tax}

        return futures_dict

    def get_matba_taxes(self):
        conn = connect_to_bbdd()
        taxes = get_all_data_from_table(conn, 'current_taxes')
        taxes_dict = dict()
        for row in taxes:
            commodity, market, current_tax = row
            if market == 'ROS':
                taxes_dict[commodity] = current_tax
        return taxes_dict

    def get_all_close_prices(self, symbol):
        close_prices_data = self.get_new_rows(symbol)
        return close_prices_data

    def get_new_rows(self, symbol):
        token = self.token
        today_ref = datetime.now().date()

        url = 'https://api.remarkets.primary.com.ar/rest/data/getTrades'
        headers = {'X-Auth-Token': token}
        month = today_ref.month if today_ref.month >= 10 else f'0{today_ref.month}'
        day = today_ref.day if today_ref.day >= 10 else f'0{today_ref.day}'
        date_to = f'{today_ref.year}-{month}-{day}'

        date_from = today_ref - timedelta(weeks=1)

        print("")
        print(f"symbol_api = {symbol}")
        print(f"dateFrom = {date_from}")
        print(f"dateTo = {date_to}")

        # Las fechas deben tener el formato YYYY-MM-DD
        params = {
            "marketId": "ROFX",
            "symbol": symbol,
            "dateFrom": date_from,
            "dateTo": date_to
        }

        response = requests.get(url, headers=headers, params=params)
        new_rows = list()
        print("")
        print(f'Response: {response.status_code}')
        print(f'Status: {response.json()["status"]}')

        if response.status_code == 200 and response.json()['status'] == 'OK':
            # Dentro de response tengo TODOS los trades por día, debo extraer el último trade de cada día
            try:
                trades = response.json()['trades']
            except Exception as e:
                print("error")
                print(response.json())
                print(e)
                return None

            dicts_by_dates = defaultdict(list)
            for trade_dictionary in trades:
                date = trade_dictionary['datetime'].split()[0]  # Obtiene la parte de la fecha (YYYY-MM-DD)
                # Change the date format from YYYY-MM-DD (%Y-%m-%d) to MM/DD/YYYY (%m/%d/%Y)
                year, month, day = date.split("-")
                new_date = f"{month}/{day}/{year}"
                dicts_by_dates[new_date].append(trade_dictionary)

            for date, dictionaries in dicts_by_dates.items():
                last_dict = max(dictionaries, key=lambda x: x['datetime'])
                correct_ticker_name = get_correct_ticket_name(symbol)
                new_rows.append(
                    {
                        "date": date,
                        "price": last_dict['price'],
                        "ticket": correct_ticker_name,
                        "from_api": "yes"
                    }
                )
            return new_rows
        print(response.status_code)
        return None


def get_correct_ticket_name(ticker):
    commodity = ticker.split('.')[0]
    market = ticker.split('.')[1].split('/')[0]
    month_and_year = ticker.split('.')[1].split('/')[1]
    month = month_and_year[:3]
    year = month_and_year[3:]
    if market == "ROS":
        return ticker
    else:
        for new_month, value in mapping_months.items():
            if value == month:
                new_ticker = f'{commodity}.{market}/{new_month}{year}'
                return new_ticker
