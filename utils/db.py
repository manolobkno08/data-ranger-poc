import psycopg2
import logging


class DatabaseManager:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.conn.set_session(autocommit=True)

    def execute_query(self, query, args=None, fetch=False):
        """Execute a query and return results."""
        with self.conn.cursor() as cursor:
            cursor.execute(query, args)
            if fetch:
                result = cursor.fetchall()
                return result

    def close(self):
        """Close the database connection."""
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
