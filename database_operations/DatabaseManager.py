import logging
from xapi import PeriodCode
import logging
import sqlite3
import asyncio
import xapi

from my_secrets import config
from DatabaseConnector import DatabaseConnector
from xapi.exceptions import DatabaseConnectionError
from scripts.DataCollector import DataCollector
from scripts.HistoricalDataCollector import HistoricalDataCollector



class DatabaseManager:
    def __init__(self, sql_database_file_path):
        self.stock_id = None
        self.symbol = None
        self.connector = DatabaseConnector(sql_database_file_path)
        self.credentials = config.CREDENTIALS_PATH

    async def connect_to_db(self):
        try:
            await self.connector.connect_to_db()
        except DatabaseConnectionError as e:
            print(f"Database connection error: {e}")

    async def disconnect_from_db(self):
        try:
            await self.connector.close_db_connection()
        except DatabaseConnectionError as e:
            print(f"Database disconnection error: {e}")

    async def get_symbols(self, table_name):
        try:
            return  await self.connector.fetch_data(f"SELECT id, symbol FROM {table_name}")
        except DatabaseConnectionError as e:
            print(f"Database symbols error: {e}")

    async def populate_db(self, table_name):
        logging.basicConfig(level=logging.INFO)
        query = f"SELECT symbol, name FROM {table_name}"
        query_insert = f"INSERT INTO {table_name} (symbol, name) VALUES (?, ?)"

        try:
            data_collector = DataCollector(self.credentials)
            cursor = await self.connector.get_cursor()
            if not cursor:
                await self.connect_to_db()

            rows = await self.connector.fetch_data(query)
            symbols = [row['symbol'] for row in rows]

            await data_collector.connect_to_xapi()
            await data_collector.download_real_data()
            df = data_collector.get_data_df()

            for index, row in df.iterrows():
                symbol = row['Ticker']
                name = row['Description']

                try:
                    if symbol not in symbols:
                        print(f"Added a new stock {symbol}")
                        await self.connector.execute_query(query_insert,(symbol, name))
                except DatabaseConnectionError as e:
                    print(symbol)
                    print(f"Error:  {e}")

        except xapi.LoginFailed as e:
            print(f"Log in failed: {e}")

        except xapi.ConnectionClosed as e:
            print(f"Connection closed: {e}")

        except ValueError as e:
            print(f"Error occurred: {e}")

        finally:
            await self.disconnect_from_db()

    # async def populate_prices(self, stock_id, symbol):
    #     try:
    #         if not self.conn or not self.cursor:
    #             await self.connect_to_db()
    #
    #         self.cursor.execute("SELECT COUNT(*) FROM stock_price_1d WHERE stock_id = ?", (self.stock_id,))
    #         count = self.cursor.fetchone()[0]
    #
    #         if count == 0:
    #             hist_data_collector = HistoricalDataCollector(
    #                 symbol=self.symbol,
    #                 start='2000-01-01',
    #                 end='2023-08-01',
    #                 period=PeriodCode.PERIOD_D1,
    #                 credentials_file=config.CREDENTIALS_PATH
    #             )
    #
    #             await hist_data_collector.connect_to_xapi()
    #             hist_data_df = await hist_data_collector.download_history_data()
    #
    #             for date_index, row in hist_data_df.iterrows():
    #                 self.cursor.execute("""
    #                     INSERT INTO stock_price_1d (stock_id, date, open, close, high, low, volume)
    #                     VALUES (?, ?, ?, ?, ?, ?, ?)
    #                 """, [self.stock_id, date_index.strftime('%Y-%m-%d %H:%M:%S'), row['Open'], row['Close'], row['High'], row['Low'], row['Volume']])
    #
    #             await hist_data_collector.disconnect_from_xapi()
    #
    #     except xapi.LoginFailed as e:
    #         print(f"Log in failed: {e}")
    #
    #     except xapi.ConnectionClosed as e:
    #         print(f"Connection closed: {e}")
    #
    #     except ValueError as e:
    #         print(f"Error occurred: {e}")
    #
    #     finally:
    #         await self.disconnect_from_db()



async def main():
    try:
        database_conn = DatabaseManager(config.DB_FILE)
        await database_conn.populate_db("stock")


        # existing_symbols = await database_conn.get_symbols()
        # iterations_num = 0

        # for stock_id, symbol in existing_symbols:
        #     iterations_num += 1
        #     print(f"{iterations_num}. Processing symbol: {symbol}")
        #
        #     stock_data_collector = DatabaseManager(config.CREDENTIALS_PATH)
        #     await stock_data_collector.populate_db()

        await database_conn.disconnect_from_db()

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())