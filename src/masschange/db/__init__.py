import psycopg2


def get_db_connection():
    return psycopg2.connect(database='masschange', user='postgres', password='password', host='localhost', port=5433)
