import pandas as pd
from collections import defaultdict
from connect_with_bbdd import get_all_data_from_table, connect_to_bbdd
from datetime import datetime, timedelta
from decimal import Decimal

SHEET_URL = "https://docs.google.com/spreadsheets/d/1j-ZrWBO-fCkGUPqWtWRsGgGswMRCm2mnMhsPmX6osLI/gviz/tq?tqx=out:csv&sheet=Resumen%20-%20Agropecuarios"

VALID_TICKERS = [
    'CRN.CME/NOV',
    'SOJ.ROS/ENE',
    'CRN.CME/JUN',
    'SOJ.ROS/JUL',
    'CRN.CME/ABR',
    'SOJ.ROS/MAY',
    'MAI.ROS/ABR',
    'SOJ.ROS/NOV',
    'MAI.ROS/DIC',
    'SOY.CME/DIC',
    'MAI.ROS/JUL',
    'SOY.CME/JUN',
    'MAI.ROS/SEP',
    'SOY.CME/ABR',
    'SOY.CME/OCT'
]

class MtrInfoSpreadsheetScrapUpdater:
    def __init__(self):
        self.matba_taxes = self.get_matba_taxes()
        self.futures_dict = self.get_futures_dict()

    def drop_unnamed_columns(self, df):
        columns_to_drop = df.columns
        columns = list()
        for column in columns_to_drop:
            if "Unnamed" not in column:
                columns.append(column)
        return df.loc[:, columns]

    def rename_and_set_new_index(self, df):
        columns = list(df.columns)
        if "CONTRATO" in columns[0].upper():
            new_columns = columns.copy()
            new_columns[0] = "CONTRATO"
            df.columns = new_columns
            new_index = df.iloc[:, 0]
            df.set_index(new_index, inplace=True)
        return df

    def get_info_of_valid_tickers(self, df):
        def drop_year_from_ticker(ticker):
            return ticker[:-2]
        def filter_data(ticker):
            return ticker in VALID_TICKERS

        df['ticker'] = df['CONTRATO'].apply(drop_year_from_ticker)
        df['is_valid'] = df['ticker'].apply(filter_data)
        return df[df['is_valid'] == True]

    def get_data_from_sheet(self, sheet_url):
        df = pd.read_csv(sheet_url)
        df = self.drop_unnamed_columns(df)
        df = self.rename_and_set_new_index(df)
        df_filtered = self.get_info_of_valid_tickers(df)
        return df_filtered

    def apply_formmating(self, df):
        futures_dict = dict()
        taxes_dict = self.get_matba_taxes()
        for data in df.iterrows():
            ticker = data[0]
            commodity = ticker.split('.')[0]
            market, date = ticker.split('.')[1].split('/')
            if market == 'ROS':
                tax = taxes_dict.get(commodity)
                info = data[1]
                price = info['Ajuste/Valor teórico']
                date = info['Fecha de Datos']
                row = {'date': date, 'last_price': Decimal(price.replace(',','.')), 'tax':tax}
                futures_dict[ticker] = row
        return futures_dict

    def get_futures_dict(self, sheet_url=SHEET_URL):
        df = self.get_data_from_sheet(sheet_url)
        futures_dict = self.apply_formmating(df)
        return futures_dict
    
    def get_matba_taxes(self):
        conn = connect_to_bbdd()
        taxes = get_all_data_from_table(conn, 'current_taxes')
        taxes_dict = dict()
        for row in taxes:
            commodity, market, current_tax, index = row
            if market == 'ROS':
                taxes_dict[commodity] = round(current_tax, 2)
        return taxes_dict