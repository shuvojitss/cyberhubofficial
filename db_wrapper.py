import os
import sqlite3
from typing import Any

DATABASE_URL = os.environ.get("DATABASE_URL")

class DBWrapper:
    def __init__(self, conn, is_postgres: bool):
        self.conn = conn
        self.is_postgres = is_postgres

    def execute(self, query: str, params: tuple = None):
        cursor = self.conn.cursor()
        if self.is_postgres:
            # Replace sqlite syntax with postgres syntax
            query = query.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            query = query.replace("INTEGER PRIMARY KEY", "SERIAL PRIMARY KEY")
            
            if "INSERT OR REPLACE INTO" in query:
                import re
                match = re.search(r"INSERT OR REPLACE INTO\s+(\w+)\s*\((.*?)\)", query, re.IGNORECASE)
                if match:
                    table = match.group(1)
                    cols = [c.strip() for c in match.group(2).split(",")]
                    pk = cols[0]
                    updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != pk])
                    query = re.sub(r"INSERT OR REPLACE INTO", "INSERT INTO", query, flags=re.IGNORECASE)
                    query = query.rstrip()
                    query += f" ON CONFLICT ({pk}) DO UPDATE SET {updates}"

            # Replace placeholders
            query = query.replace("?", "%s")
            
        if params is not None:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor

    def executemany(self, query: str, params_list: list):
        cursor = self.conn.cursor()
        if self.is_postgres:
            # Replace sqlite syntax with postgres syntax
            query = query.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            query = query.replace("INTEGER PRIMARY KEY", "SERIAL PRIMARY KEY")
            
            if "INSERT OR REPLACE INTO" in query:
                import re
                match = re.search(r"INSERT OR REPLACE INTO\s+(\w+)\s*\((.*?)\)", query, re.IGNORECASE)
                if match:
                    table = match.group(1)
                    cols = [c.strip() for c in match.group(2).split(",")]
                    pk = cols[0]
                    updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != pk])
                    query = re.sub(r"INSERT OR REPLACE INTO", "INSERT INTO", query, flags=re.IGNORECASE)
                    query = query.rstrip()
                    query += f" ON CONFLICT ({pk}) DO UPDATE SET {updates}"

            # Replace placeholders
            query = query.replace("?", "%s")
            
        cursor.executemany(query, params_list)
        return cursor

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


def get_db_connection():
    if DATABASE_URL:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.DictCursor)
        return DBWrapper(conn, True)
    else:
        conn = sqlite3.connect("users.db")
        conn.row_factory = sqlite3.Row
        return DBWrapper(conn, False)

def get_app_data_db_connection():
    if DATABASE_URL:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.DictCursor)
        # Postgres auto-commit behavior is different, we can just turn it on or stick to manual commits
        # conn.autocommit = True
        return DBWrapper(conn, True)
    else:
        APP_DATA_DB_PATH = os.environ.get("APP_DATA_DB_PATH", "app_data.db")
        conn = sqlite3.connect(APP_DATA_DB_PATH)
        conn.row_factory = sqlite3.Row
        return DBWrapper(conn, False)

try:
    import psycopg2
    IntegrityError = (sqlite3.IntegrityError, psycopg2.IntegrityError)
except ImportError:
    IntegrityError = sqlite3.IntegrityError
