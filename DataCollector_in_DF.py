import logging
import asyncio
import json
import xapi
import pandas as pd


class DataCollector:
    def __init__(self, credentials_file):
        self.credentials_file = credentials_file
        self.my_columns = ['Ticker', 'Ask', 'Bid', 'Spread', 'Description', 'Ask/Bid Time']
        self.final_dataframe = pd.DataFrame(columns=self.my_columns)

    async def connect(self):
        with open(self.credentials_file, "r") as f:
            self.CREDENTIALS = json.load(f)
        self.x = await xapi.connect(**self.CREDENTIALS)

    async def collect_data(self):
        response = await self.x.socket.getAllSymbols()
        for item in response['returnData']:
            self.final_dataframe = self.final_dataframe.append(
                pd.Series(
                    [
                        item['symbol'],
                        item['ask'],
                        item['bid'],
                        item['spreadRaw'],
                        item['description'],
                        item['time']
                    ],
                    index=self.my_columns
                ),
                ignore_index=True
            )

    def divide_chunks(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def create_symbol_groups(self, chunk_size = 100):
        self.symbol_groups = list(self.divide_chunks(self.final_dataframe['Ticker'], chunk_size))

    def get_symbol_strings(self):
        symbol_strings = []
        for group in self.symbol_groups:
            symbol_strings.append(','.join(group))
        return symbol_strings

    def get_dataframe(self):
        return self.final_dataframe




async def main():
    logging.basicConfig(level=logging.INFO)

    data_collector = DataCollector("credentials.json")

    try:
        await data_collector.connect()
        await data_collector.collect_data()
        df = data_collector.get_dataframe()
        print(df)

    except xapi.LoginFailed as e:
        print(f"Log in failed: {e}")

    except xapi.ConnectionClosed as e:
        print(f"Connection closed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
