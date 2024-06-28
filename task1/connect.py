import psycopg2 as psycopg
from contextlib import contextmanager

database_url = "postgresql://postgres:1234567@localhost:5432"

@contextmanager
def create_connection(db_url):
    """ create a database connection to a PostgreSQL database """
    conn = psycopg.connect(db_url)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()