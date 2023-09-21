import logging
import asyncio
import gc
import sqlite3
import xapi

from scripts.HistoricalDataCollector import HistoricalDataCollector
from my_secrets import config
from xapi.enums import PeriodCode

async def main():
    logging.basicConfig(level=logging.INFO)

    try:
        conn = sqlite3.connect(config.DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT id, symbol FROM stock")
        existing_symbols = cursor.fetchall()
        iterations_num = 0

        for stock_id, symbol in existing_symbols:
            iterations_num += 1
            print(f"{iterations_num}. Processing symbol: {symbol}")

            cursor.execute("SELECT COUNT(*) FROM stock_price_1d WHERE stock_id = ?", (stock_id,))
            count = cursor.fetchone()[0]

            if count == 0:
                hist_data_collector = HistoricalDataCollector(
                    symbol = symbol,
                    start ='2000-01-01',
                    end ='2023-08-01',
                    period = PeriodCode.PERIOD_D1,
                    credentials_file ="../my_secrets/credentials.json"
                )

                await hist_data_collector.connect_to_xapi()
                hist_data_df = await hist_data_collector.download_history_data()

                for date_index, row in hist_data_df.iterrows():
                    cursor.execute("""
                        INSERT INTO stock_price_1d (stock_id, date, open, close, high, low, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [stock_id, date_index.strftime('%Y-%m-%d %H:%M:%S'), row['Open'], row['Close'], row['High'], row['Low'], row['Volume']])

                del hist_data_collector
                gc.collect()
                conn.commit()

        conn.close()

    except xapi.LoginFailed as e:
        print(f"Log in failed: {e}")

    except xapi.ConnectionClosed as e:
        print(f"Connection closed: {e}")

    except ValueError as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())
