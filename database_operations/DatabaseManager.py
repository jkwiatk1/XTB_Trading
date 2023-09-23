import logging
from xapi import PeriodCode
import logging
import gc
import asyncio
import xapi

from my_secrets import config
from DatabaseConnector import DatabaseConnector
from xapi.exceptions import DatabaseConnectionError
from scripts.DataCollector import DataCollector
from scripts.HistoricalDataCollector import HistoricalDataCollector



class DatabaseManager:
    def __init__(self, sql_database_file_path):
        self.connector = DatabaseConnector(sql_database_file_path)
        self.connection_status = False
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

    async def populate_db(self, table_name: str, data_collector: DataCollector):
        query = f"SELECT symbol, name FROM {table_name}"
        query_insert = f"INSERT INTO {table_name} (symbol, name) VALUES (?, ?)"

        try:
            cursor = await self.connector.get_cursor()
            self.connection_status = await self.connector.get_conn_status()
            if not cursor or not self.connection_status:
                await self.connect_to_db()

            rows = await self.connector.fetch_data(query)
            symbols = [row['symbol'] for row in rows]

            data_collector = data_collector
            if not data_collector:
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

        except ValueError as e:
            print(f"Error occurred: {e}")

        finally:
            await self.disconnect_from_db()

    # TODO create methods to populate_prices according to populate_db
    # async def populate_prices(self, table_name, hist_data_collector, stock_id, symbol, period: PeriodCode = PeriodCode.PERIOD_D1):
    #     query = f"SELECT COUNT(*) FROM {table_name} WHERE stock_id = ?"
    #     params = (stock_id,)
    #     query_insert = f"INSERT INTO {table_name} (stock_id, date, open, close, high, low, volume) VALUES (?, ?, ?, ?, ?, ?, ?)"
    #
    #     try:
    #         cursor = await self.connector.get_cursor()
    #         self.connection_status = await self.connector.get_conn_status()
    #         if not cursor or not self.connection_status:
    #             await self.connect_to_db()
    #
    #         await self.connector.execute_query(query, params)
    #         count = await self.connector.fetch_one("SELECT column1, column2 FROM my_table WHERE condition=?", (param_value,))[0]
    #
    #         if count == 0:
    #             hist_data_df = await hist_data_collector.download_history_data()
    #
    #             for date_index, row in hist_data_df.iterrows():
    #                 params_insert = (stock_id, date_index.strftime('%Y-%m-%d %H:%M:%S'), row['Open'], row['Close'], row['High'], row['Low'], row['Volume'])
    #                 await self.connector.execute_query(query_insert, params_insert)
    #
    #             del hist_data_collector
    #             gc.collect()
    #
    #         else:
    #             print(f"Data for: {symbol} already exist in the database.")
    #
    #
    #     except ValueError as e:
    #         print(f"Error occurred: {e}")
    #
    #     finally:
    #         await self.disconnect_from_db()

    async def get_data(self, table: str, params_to_get = None):
        cursor = await self.connector.get_cursor()
        self.connection_status = await self.connector.get_conn_status()
        if not cursor or not self.connection_status:
            await self.connect_to_db()

        if not params_to_get:
            query = f"SELECT * FROM {table}"
        else:
            placeholders = ",".join(param for param in params_to_get)
            query = f"SELECT {placeholders} FROM {table}"

        return await self.connector.fetch_data(query)



async def main():
    logging.basicConfig(level=logging.INFO)



    try:
        data_collector = DataCollector(config.CREDENTIALS_PATH)
        await data_collector.connect_to_xapi()

        database_conn = DatabaseManager(config.DB_FILE)
        await database_conn.populate_db("stock", data_collector)

        existing_symbols  = await database_conn.get_data("stock", ("id","symbol"))
        iterations_num = 0

        for stock_id, symbol in existing_symbols:
            iterations_num += 1
            print(f"{iterations_num}. Processing symbol: {symbol}")


        # symbol = "EURUSD"
        # hist_data_collector = HistoricalDataCollector(
        #     symbol= symbol,
        #     start='2000-01-01',
        #     end='2023-08-01',
        #     period=PeriodCode.PERIOD_D1,
        #     credentials_file=config.CREDENTIALS_PATH
        # )
        # await hist_data_collector.connect_to_xapi()
        #
        # await database_conn.populate_db(hist_data_collector,)


        # existing_symbols = await database_conn.get_symbols()
        # iterations_num = 0

        # for stock_id, symbol in existing_symbols:
        #     iterations_num += 1
        #     print(f"{iterations_num}. Processing symbol: {symbol}")
        #
        #     stock_data_collector = DatabaseManager(config.CREDENTIALS_PATH)
        #     await stock_data_collector.populate_db()




    except xapi.LoginFailed as e:
        print(f"Log in failed: {e}")

    except xapi.ConnectionClosed as e:
        print(f"Connection closed: {e}")

    except ValueError as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())