import logging
import asyncio
import gc
import sqlite3
import xapi

from HistoricalDataCollector import HistoricalDataCollector
from my_secrets import config
from xapi.enums import PeriodCode

async def main():
    logging.basicConfig(level=logging.INFO)

    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT symbol FROM stock")
        rows = cursor.fetchall()

        for row in rows:
            symbol = row[0]
            print(symbol)
            hist_data_collector = HistoricalDataCollector(
                symbol = symbol,
                start ='2000-01-01',
                end ='2023-08-01',
                period = PeriodCode.PERIOD_D1,
                credentials_file = "my_secrets/credentials.json"
            )

            hist_data_df = await hist_data_collector.run()

            for index,row in hist_data_df.iterrows():
                pass #TODO read data in chunks size
                # cursor.execute("""
                #     INSERT INTO stock_price_1d
                # """)

            del hist_data_collector
            gc.collect()

    except xapi.LoginFailed as e:
        print(f"Log in failed: {e}")

    except xapi.ConnectionClosed as e:
        print(f"Connection closed: {e}")

    except ValueError as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())
