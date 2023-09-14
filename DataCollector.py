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
        self.x = None

    async def connect(self):
        with open(self.credentials_file, "r") as f:
            self.CREDENTIALS = json.load(f)
        self.x = await xapi.connect(**self.CREDENTIALS)

    async def collect_real_data(self):
        response = await self.x.socket.getAllSymbols()
        self.final_dataframe.drop(index=self.final_dataframe.index, inplace=True)
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

    def save_to_csv(self, filename='docs/data.csv'):
        if self.final_dataframe.empty:
            raise ValueError("The data frame is empty. Cannot save to CSV file.")
        self.final_dataframe.to_csv(filename, index=False)

    def get_socket_object(self):
        return self.x.socket

    def get_symbol_strings(self):
        if self.final_dataframe.empty:
            try:
                self.final_dataframe = pd.read_csv('docs/data.csv')
                print("Data was loaded from a CSV file to get symbols.")
            except FileNotFoundError:
                print("CSV file not found to get symbols.")
                return []
            # except FileNotFoundError:
            #     raise FileNotFoundError("CSV file not founddas.")

        symbol_strings = self.final_dataframe['Ticker'].unique().tolist()
        return symbol_strings

    def divide_chunks(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def create_symbol_groups(self, chunk_size = 100):
        self.symbol_groups = list(self.divide_chunks(self.final_dataframe['Ticker'], chunk_size))

    def get_symbol_groups_strings(self):
        symbol_groups_strings = []
        for group in self.symbol_groups:
            symbol_groups_strings.append(','.join(group))
        return symbol_groups_strings

    def get_column_names(self):
        return self.my_columns

    def get_dataframe(self):
        return self.final_dataframe



async def main():
    logging.basicConfig(level=logging.INFO)

    data_collector = DataCollector("credentials.json")

    try:
        await data_collector.connect()
        await data_collector.collect_real_data()
        df = data_collector.get_dataframe()
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
