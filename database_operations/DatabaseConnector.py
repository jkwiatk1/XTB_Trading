import sqlite3
from typing import Optional
from xapi.exceptions import DatabaseConnectionError

ROW_FACTORY = True

class DatabaseConnector:
    def __init__(self, sql_database_file_path: str):
        self.DB_FILE = sql_database_file_path
        self.__conn = None
        self.__cursor = None
        self.__connected = False

    async def connect_to_db(self, ROW_FACTORY = True):
        try:
            self.__conn = sqlite3.connect(self.DB_FILE)
            self.__connected = True
            print("Connected to db.")
            if ROW_FACTORY:
                self.__conn.row_factory = sqlite3.Row
                print("Set row factory.")
            self.__cursor = self.__conn.cursor()
        except sqlite3.Error as e:
            error = f"Error connecting to the database: {e}"
            raise DatabaseConnectionError(error)
    async def close_db_connection(self):
        try:
            if self.__conn:
                self.__conn.close()
                self.__connected = False
                print("Disconnected from db.")
        except sqlite3.Error as e:
            error = f"Error closing the database connection: {e}"
            raise DatabaseConnectionError(error)

    def __del__(self):
        if self.__conn:
            self.__conn.close()
            self.__connected = False
            print("Disconnected from db due to deletion of the DatabaseConnector object.")

    async def execute_query(self, query: str, params: Optional[tuple] = None):
        try:
            if params != None:
                self.__cursor.execute(query, params)
            else:
                self.__cursor.execute(query)
            self.__conn.commit()
        except sqlite3.Error as e:
            self.__conn.rollback()
            error = f"Database error while executing query: {e}"
            raise DatabaseConnectionError(error)

    async def fetch_data(self, query, params=None):
        try:
            await self.execute_query(query, params)
            return self.__cursor.fetchall()
        except sqlite3.Error as e:
            error = f"Database error while executing query (fetching data): {e}"
            raise DatabaseConnectionError(error)

    async def fetch_one(self, query, params=None):
        try:
            await self.execute_query(query, params)
            return self.__cursor.fetchone()
        except sqlite3.Error as e:
            error = f"Database error while executing query (fetch data): {e}"
            raise DatabaseConnectionError(error)

    async def drop_table(self, table_name):
        query = f"DROP TABLE IF EXISTS {table_name}"
        await self.execute_query(query)

    async def delete_from_table(self, table_name, symbol):
        query = f"DELETE FROM {table_name} WHERE symbol LIKE ?"
        params = (f"%{symbol}%",)
        await self.execute_query(query,params)

    async def create_table(self, table_name, columns_definition):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition})"
        await self.execute_query(query)

    async def get_cursor(self):
        return self.__cursor

    async def get_conn_status(self):
        return self.__connected