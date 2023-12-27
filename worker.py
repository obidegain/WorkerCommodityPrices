from yfinance_api import YFinanceApiUpdater
from connect_with_bbdd import connect_to_bbdd, get_all_data_from_table, verify_conn_open, add_new_record_to_historical_prices


def main():
    try:
        print("Estableciendo conexión con API")
        yfinance_api_updater = YFinanceApiUpdater()
        print("Obteniendo último tarde")
        ticker = "ZSX24.CBT"
        last_price, last_date = yfinance_api_updater.get_last_trade(ticker)
        print(f"last_date = {last_date} - last_price = {last_price}")

        new_record = (last_date, last_price, ticker, 0.393685, "Docker")

    except Exception as e:
        print(f'Error en conexión con API: {e}')

    try:
        print("Estableciendo conexión con BBDD")
        conn = connect_to_bbdd()
        if verify_conn_open(conn):
            add_new_record_to_historical_prices(conn, new_record)
            table = "HistoricalPrice"
            rows = get_all_data_from_table(conn, table)
            print(rows)
    except Exception as e:
        print(f'Error en conexión con API: {e}')


if __name__ == '__main__':
    main()
