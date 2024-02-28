import psycopg2
import os

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
    INSERT INTO HistoricalPrice (date, price, ticker, tax, from_api, index)
    VALUES (%s, %s, %s, %s, %s, %s);
    '''
    cursor = conn.cursor()
    cursor.execute(insert_query, data_to_insert)
    conn.commit()


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


def delete_all_records_of_specific_table(conn, table):
    select_query = f'DELETE FROM {table};'

    cursor = conn.cursor()
    cursor.execute(select_query)
    conn.commit()

    select_query = f'SELECT * FROM {table};'
    cursor = conn.cursor()
    cursor.execute(select_query)
    rows = cursor.fetchall()

    return rows


def get_files(path):
    try:
        files = os.listdir(path)
        return files
    except OSError as e:
        print(f"No se pudo acceder a la carpeta: {e}")
        return []


def upload_data_from_csv_to_historicalprice(conn, path='/home/obidegain/Workspaces/FinancialArbitrageVisualization/data'):
    files = get_files(path)
    cursor = conn.cursor()
    for file_name in files:
        if "_" in file_name:
            full_path = f'{path}/{file_name}'
            f = open(full_path, 'r')
            f.readline()
            print(file_name)
            if verify_conn_open(conn):
                print(f"Comenzando a cargar datos de {file_name}")
                cursor.copy_from(f, 'historicalprice', sep=',')
                conn.commit()
                print("Datos cargados exitosamente")
            print("")
            print(f'Cantidad de líneas: {len(get_all_data_from_table(conn, "historicalprice"))}')
            print("")


def update_new_record(new_records):
    conn = connect_to_bbdd()
    table = "HistoricalPrice"
    rows = get_all_data_from_table(conn, table)
    new_records_added = list()
    new_records_not_added = list()
    for new_record in new_records:
        try:
            exists = any(row[:2] == new_record[:2] and row[2] == new_record[2] for row in rows)
            if not exists:
                add_new_record_to_historical_prices(conn, new_record)
                new_records_added.append(new_record)
                print(f'Agregado: {new_record}')
            else:
                print(f'YA EXISTE: {new_record}')
                new_records_not_added.append(new_record)
        except Exception as e:
            print(f'Error en conexión con BBDD: {e}')

    return new_records_added, new_records_not_added


def get_next_index_value():
    conn = connect_to_bbdd()
    table = "HistoricalPrice"
    cursor = conn.cursor()
    # Obtener el próximo valor para la columna 'index'
    cursor.execute(f"SELECT MAX(index) + 1 FROM {table}")
    next_index = cursor.fetchone()[0] or 1
    cursor.close()
    return next_index
