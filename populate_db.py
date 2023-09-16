import logging
import sqlite3
import asyncio
import config

from DataCollector import DataCollector
import xapi


async def main():

    logging.basicConfig(level=logging.INFO)

    data_collector = DataCollector("credentials.json")

    try:
        conn = sqlite3.connect(config.DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT symbol, company FROM stock")
        rows = cursor.fetchall()
        symbols = [row['symbol'] for row in rows]

        await data_collector.connect()
        await data_collector.get_real_data()
        df = data_collector.get_dataframe()

        for index, row in df.iterrows():
            symbol = row['Ticker']
            company = row['Description']
            try:
                if symbol not in symbols:
                    print(f"Added a new stock {symbol}")
                    cursor.execute("INSERT INTO stock (symbol, company) VALUES (?, ?)", (symbol, company))
            except Exception as e:
                print(symbol)
                print(e)
        conn.commit()

    except xapi.LoginFailed as e:
        print(f"Log in failed: {e}")

    except xapi.ConnectionClosed as e:
        print(f"Connection closed: {e}")

    except ValueError as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())