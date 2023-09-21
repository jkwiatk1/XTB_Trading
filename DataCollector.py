import logging
import asyncio
import json
import xapi
import pandas as pd
from ApiConnection import ApiConnection


class DataCollector:
    ''' Class for collection real data from XTB

    Attrs
    ==================
    credentials_file - json
                file with xtb credentials

    cols_to_save - list
                list of columns to save, names taken from DataCollector class

    '''
    def __init__(self, credentials_file):
        self.credentials_file = credentials_file
        self.cols_to_save = ['Ticker', 'Ask', 'Bid', 'Spread', 'Description', 'Ask/Bid Time']
        self.data = pd.DataFrame(columns=self.cols_to_save)

        self.api_connection = ApiConnection(credentials_file)
        self.api_client = None

    async def connect_to_xapi(self):
        self.api_client = await self.api_connection.connect()

    def disconnect_from_xapi(self):
        asyncio.run(self.api_connection.disconnect())

    async def get_real_data(self):
        response = await self.api_client.socket.getAllSymbols()
        self.data.drop(index=self.data.index, inplace=True)
        for item in response['returnData']:
            self.data = self.data.append(
                pd.Series(
                    [
                        item['symbol'],
                        item['ask'],
                        item['bid'],
                        item['spreadRaw'],
                        item['description'],
                        item['time']
                    ],
                    index=self.cols_to_save
                ),
                ignore_index=True
            )

    def save_to_csv(self, filename='docs/data.csv'):
        if self.data.empty:
            raise ValueError("The data frame is empty. Cannot save to CSV file.")
        self.data.to_csv(filename, index=False)

    def get_symbol_strings(self):
        if self.data.empty:
            try:
                self.data = pd.read_csv('docs/data.csv')
                print("Data was loaded from a CSV file to get symbols.")
            except FileNotFoundError:
                print("CSV file not found to get symbols.")
                return []

        symbol_strings = self.data['Ticker'].unique().tolist()
        return symbol_strings

    def divide_chunks(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def create_symbol_groups(self, chunk_size = 100):
        self.symbol_groups = list(self.divide_chunks(self.data['Ticker'], chunk_size))

    def get_symbol_groups_strings(self):
        symbol_groups_strings = []
        for group in self.symbol_groups:
            symbol_groups_strings.append(','.join(group))
        return symbol_groups_strings

    def get_column_names(self):
        return self.cols_to_save

    def get_data_df(self):
        return self.data



async def main():
    logging.basicConfig(level=logging.INFO)

    data_collector = DataCollector("my_secrets/credentials.json")

    try:
        await data_collector.connect_to_xapi()
        await data_collector.get_real_data()
        df = data_collector.get_data_df()
        data_collector.save_to_csv()
        print(df)
        print()
        print(data_collector.get_symbol_strings())
        print()
        print(len(data_collector.get_symbol_strings()))
        print()
        print(data_collector.get_column_names())

    except xapi.LoginFailed as e:
        print(f"Log in failed: {e}")

    except xapi.ConnectionClosed as e:
        print(f"Connection closed: {e}")

    except ValueError as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())
