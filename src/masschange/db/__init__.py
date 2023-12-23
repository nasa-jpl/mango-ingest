import psycopg2

from masschange.utils.config import get_config

def get_db_connection():
    config = get_config()

    db_config = config['database']
    host = db_config['host']
    port = int(db_config['port'])
    user = db_config['user']
    password = db_config['password']
    database = db_config['database']

    return psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
