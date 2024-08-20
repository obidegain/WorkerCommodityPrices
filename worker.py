from connect_with_bbdd import update_new_record, get_next_index_value
from datetime import datetime
from matba_api import MatbaApiUpdater
from yfinance_api import YFinanceApiUpdater
from scrap_mtr_info_spreadsheet import MtrInfoSpreadsheetScrapUpdater
import schedule
import time


def worker():
    last_trades = None
    last_index_from_ddbb = None

    try:
        print(f"Starting worker at {datetime.now()}")
        print("Estableciendo conexión con API")
        print("Comenzando matba_api download")

        last_index_from_ddbb = get_next_index_value()
        print(f'last_index_from_ddbb: {last_index_from_ddbb}')

        last_trades = dict()

        try:
            print("Estableciendo conexión con Matba API ...")
            matba_api = MatbaApiUpdater()
            last_trades.update(matba_api.futures_dict)
            print("Matba API - OK")
        except Exception as e:
            print("Matba API - ERROR")
            print("Comenzando scrap Mtr Info spreadsheet ...")
            mtr_info = MtrInfoSpreadsheetScrapUpdater()
            last_trades.update(mtr_info.futures_dict)
            print("Mtr Info Scrapper - OK")
        try:
            print("Estableciendo conexión con YFinance ...")
            yfinance_api = YFinanceApiUpdater()
            last_trades.update(yfinance_api.futures_dict)
            print("YFinance - OK")
        except Exception as e:
            print(e)

    except Exception as e:
        print(f'Error general: {e}')

    if last_trades:
        print("Dentro de last_trades")
        new_records = list()
        for i, value in enumerate(last_trades.items()):
            ticker = value[0]
            data = value[1]
            index = last_index_from_ddbb + i
            date = data.get('date')
            last_price = data.get('last_price')
            tax = data.get('tax')
            now = f'Ubuntu Cron - {str(datetime.now())}'

            new_record = (date, last_price, ticker, tax, now, index)
            new_records.append(new_record)
        new_records_added, new_records_not_added = update_new_record(new_records)

        print(f'New records added:')
        print(new_records_added)
        print(f'New records not added:')
        print(new_records_not_added)


if __name__ == "__main__":
    worker()
