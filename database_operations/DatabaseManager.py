import logging
import gc
import asyncio
import xapi

from typing import Optional
from xapi import PeriodCode
from my_secrets import config
from database_operations.DatabaseConnector import DatabaseConnector
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

    async def ensure_connection(self):
        cursor = await self.connector.get_cursor()
        self.connection_status = await self.connector.get_conn_status()
        if not cursor or not self.connection_status:
            await self.connect_to_db()

    async def get_symbols(self, table_name):
        try:
            return  await self.connector.fetch_data(f"SELECT id, symbol FROM {table_name}")
        except DatabaseConnectionError as e:
            print(f"Database symbols error: {e}")

    async def populate_db(self, table_name: str, data_collector: DataCollector):
        query = f"SELECT symbol, name FROM {table_name}"
        query_insert = f"INSERT INTO {table_name} (symbol, name) VALUES (?, ?)"

        try:
            await self.ensure_connection()

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

    async def populate_prices(self, table_name: str, stock_id, symbol, hist_data_collector: HistoricalDataCollector):
        query = f"SELECT COUNT(*) FROM {table_name} WHERE stock_id = ?"
        params = (stock_id,)
        query_insert = f"INSERT INTO {table_name} (stock_id, date, open, close, high, low, volume) VALUES (?, ?, ?, ?, ?, ?, ?)"

        try:
            await self.ensure_connection()

            await self.connector.execute_query(query,params)
            count = await self.connector.fetch_one("SELECT COUNT(*) FROM stock_price_1d WHERE stock_id=?",params)

            if count[0] == 0:
                hist_data_df = await hist_data_collector.download_history_data()

                for date_index, row in hist_data_df.iterrows():
                    params_insert = (stock_id, date_index.strftime('%Y-%m-%d %H:%M:%S'), row['Open'], row['Close'], row['High'], row['Low'], row['Volume'])
                    await self.connector.execute_query(query_insert, params_insert)
                print(f"Data for symbol: {symbol} just added to the database.")

            else:
                print(f"Data for symbol: {symbol} already exist in the database.")


        except ValueError as e:
            print(f"Error occurred: {e}")

        finally:
            await self.disconnect_from_db()

    async def execute_custom_query(self, query: str, params: Optional[tuple] = None):
        try:
            await self.ensure_connection()
            return await self.connector.fetch_data(query, params)
        except Exception as e:
            error = f"Custom query execution error: {e}"
            print(error)

    async def get_ordered_data(self, table_name: str, columns: list[str], order_by_column: str, ascending: bool = True, conditions: str = "", condition_params: Optional[tuple] = None):
        try:
            await self.ensure_connection()

            order_direction = "ASC" if ascending else "DESC"

            if conditions == "":
                query = f"SELECT {', '.join(columns)} FROM {table_name} ORDER BY {order_by_column} {order_direction}"
            else:
                query = f"SELECT {', '.join(columns)} FROM {table_name} WHERE {conditions} ORDER BY {order_by_column} {order_direction}"
            return await self.connector.fetch_data(query, condition_params)

        except Exception as e:
            error = f"Get ordered data method error: {e}"
            print(error)

    async def get_specify_data(self, table_name: str, columns: list[str], conditions: str = "", condition_params: Optional[tuple] = None):
        try:
            await self.ensure_connection()

            if conditions == "":
                query = f"SELECT {', '.join(columns)} FROM {table_name}"
            else:
                query = f"SELECT {', '.join(columns)} FROM {table_name} WHERE {conditions}"

            return await self.connector.fetch_data(query, condition_params)

        except Exception as e:
            error = f"Specify data method error: {e}"
            print(error)

    async def get_whole_data(self, table_name: str, params_to_get = None):
        try:
            await self.ensure_connection()

            if not params_to_get:
                query = f"SELECT * FROM {table_name}"
            else:
                placeholders = ",".join(param for param in params_to_get)
                query = f"SELECT {placeholders} FROM {table_name}"

            return await self.connector.fetch_data(query)

        except Exception as e:
            error = f"Get whole data method error: {e}"
            print(error)



async def main():
    logging.basicConfig(level=logging.INFO)

    try:
        database_conn = DatabaseManager(config.DB_FILE)

        data_collector = DataCollector(config.CREDENTIALS_PATH)
        await data_collector.connect_to_xapi()

        await database_conn.populate_db("stock", data_collector)


        '''
        Specify query
        '''
        query = """
            select * from(
                    select symbol, name, stock_id, max(close), date
                    from stock_price_1d join stock on stock.id = stock_price_1d.stock_id
                    group by stock_id
                    order by symbol
            ) where date = ?
        """
        params = ('2020-10-29',)
        result = await database_conn.execute_custom_query(query, params)
        for stock in result:
            print("SYMBOL: " + stock['symbol'] + "  NAME: " + stock['name'] + "  ID: " + str(stock['id']))
        print("DONE\n\n")


        query = "SELECT * FROM stock WHERE name LIKE '%Technologies%'"
        params = None
        result = await database_conn.execute_custom_query(query, params)
        for stock in result:
            print("SYMBOL: " + stock['symbol'] + "  NAME: " + stock['name'] + "  ID: " + str(stock['id']))
        print("DONE\n\n")


        query = "SELECT * FROM stock WHERE symbol LIKE 'B%' OR symbol LIKE 'C%'"
        params = None
        result = await database_conn.execute_custom_query(query, params)
        for stock in result:
            print("SYMBOL: " + stock['symbol'] + "  NAME: " + stock['name'] + "  ID: " + str(stock['id']))
        print("DONE\n\n")


        '''
        Specify symbol
        '''
        # symbol = "EURUSD"
        # symbol_id  = await database_conn.get_specify_data("stock",["id"], f"symbol = ?",(symbol,))
        # symbol_id = symbol_id[0][0]
        #
        # hist_data_collector = HistoricalDataCollector(
        #     symbol= symbol,
        #     start='2000-01-01',
        #     end='2023-08-01',
        #     period=PeriodCode.PERIOD_D1,
        #     credentials_file=config.CREDENTIALS_PATH
        # )
        # await hist_data_collector.connect_to_xapi()
        # await database_conn.populate_prices("stock_price_1d", symbol_id, symbol, hist_data_collector)


        '''
        All symbols
        '''
        # existing_symbols  = await database_conn.get_whole_data("stock", ("id","symbol"))
        # iterations_num = 0
        #
        # for stock_id, symbol in existing_symbols:
        #     iterations_num += 1
        #     print(f"{iterations_num}. Processing symbol: {symbol}")
        #
        #     hist_data_collector = HistoricalDataCollector(
        #         symbol= symbol,
        #         start='2000-01-01',
        #         end='2023-08-01',
        #         period=PeriodCode.PERIOD_D1,
        #         credentials_file=config.CREDENTIALS_PATH
        #     )
        #     await hist_data_collector.connect_to_xapi()
        #
        #     await database_conn.populate_prices("stock_price_1d", stock_id, symbol, hist_data_collector)
        #
        #     await hist_data_collector.close()
        #     del hist_data_collector
        #     gc.collect()


    except xapi.LoginFailed as e:
        print(f"Log in failed: {e}")

    except xapi.ConnectionClosed as e:
        print(f"Connection closed: {e}")

    except ValueError as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())