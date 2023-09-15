import logging
import sqlite3
import asyncio

from DataCollector import DataCollector
import xapi


async def main():
    conn = sqlite3.connect('trade_app_db.sqlite')

    cursor = conn.cursor()

    logging.basicConfig(level=logging.INFO)

    data_collector = DataCollector("credentials.json")

    try:
        await data_collector.connect()
        await data_collector.get_real_data()
        df = data_collector.get_dataframe()

        for index, row in df.iterrows():
            symbol = row['Ticker']
            company = row['Description']
            try:
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