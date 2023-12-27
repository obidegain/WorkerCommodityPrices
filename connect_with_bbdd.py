import psycopg2
import os

# Datos de conexión
# dbname = "price-commodity"
# user = "fl0user"
# password = "oZ3RWcjl6zBU"
# host = "ep-old-tooth-91354645.us-east-2.aws.neon.fl0.io"
# port = "5432"
# sslmode = "require"

dbname = os.getenv('DBNAME')
user = os.getenv('USER')
password = os.getenv('PASSWORD')
host = os.getenv('HOST')
port = os.getenv('PORT')
sslmode = os.getenv('SSLMODE')


def connect_to_bbdd():
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            sslmode=sslmode
        )
        print("¡Conexión exitosa!")
        return conn
    except psycopg2.Error as e:
        print("Error al conectar a la base de datos:", e)


def execute_psql_command(conn, command):
    """
    :param conn: it's a conection with psycopg2
    :param command: it's a sql_command, like a drop_table, add_table, modify_table, etc
    :return: nothing
    """
    try:
        cursor = conn.cursor()
        cursor.execute(command)
        conn.commit()
    except psycopg2.Error as e:
        print("Error al ejecutar el comando en la base de datos:", e)


def add_new_record_to_historical_prices(conn, data_to_insert):
    insert_query = '''
    INSERT INTO HistoricalPrice (date, price, ticker, tax, from_api)
    VALUES (%s, %s, %s, %s, %s);
    '''
    cursor = conn.cursor()
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    print("Datos insertados correctamente en 'HistoricalPrice'.")


def verify_conn_open(conn):
    if conn and not conn.closed:
        print("La conexión está abierta.")
        return True
    print("La conexión está cerrada o no se pudo establecer.")
    return False


def get_all_data_from_table(conn, table):
    select_query = f'SELECT * FROM {table};'

    cursor = conn.cursor()
    cursor.execute(select_query)
    rows = cursor.fetchall()

    return rows
