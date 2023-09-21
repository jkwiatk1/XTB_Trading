import pandas as pd
import logging
import xapi
import asyncio

from datetime import datetime
from dateutil.relativedelta import *
from xapi.enums import PeriodCode
from ApiConnection import  ApiConnection


class HistoricalDataCollector:

    ''' Class for collection historical data from XTB

    Attrs
    ==================

    symbol - string
            ticker symbol e.g 'EURUSD'

    start - string
            start date

    end - string
          end date

    period - string
           period in enum format from PeriodCode Class

    cols_to_save - list
                list of columns to save, names taken from DataCollector class

    '''
    def __init__(self, symbol, start, credentials_file, end=None, period = PeriodCode.PERIOD_MN1):
        self.api_connection = ApiConnection(credentials_file)
        self.api_client = None

        self.cols_to_save =['Date','Open', 'Close', 'High', 'Low', 'Volume']
        self.symbol = symbol
        self.start = start
        self.end = end if end is not None else datetime.now().strftime('%Y-%m-%d')
        self.max_range = {'PERIOD_M1': 1, 'PERIOD_M5': 1, 'PERIOD_M15': 1, 'PERIOD_M30': 6, 'PERIOD_H1': 6, 'PERIOD_H4': 12}
        self.period = period
        self.check_max_range()

    async def connect_to_xapi(self):
        self.api_client = await self.api_connection.connect()

    def disconnect_from_xapi(self):
        asyncio.run(self.api_connection.disconnect())

    def __repr__(self):
        rep = "DataCollector(symbol = {}, start = {}, end = {}, period= {})"
        return rep.format(self.symbol, self.start, self.end, self.period)

    async def download_history_data(self):
        ''' Collect and prepares the data'''

        self.check_max_range()

        end_date = datetime.strptime(self.end, '%Y-%m-%d')
        end = int(datetime.timestamp(end_date) * 1000)
        start_date = datetime.strptime(self.start, '%Y-%m-%d')
        start = int(datetime.timestamp(start_date) * 1000)

        history_data = await self.api_client.socket.getChartRangeRequest(symbol= self.symbol, start = start, end = end, period= self.period, ticks= 0)
        df = self.history_converter(history_data)
        return  df


    def history_converter(self, history):
        '''Convert data from dict to pandas df'''

        df_dict = history['returnData']['rateInfos']
        digits = history['returnData']['digits']

        df = pd.DataFrame.from_dict(df_dict)

        df['Date'] = df['ctm'].apply(lambda x: datetime.fromtimestamp(x / 1000))
        df['Open'] = df['open'] / (10 ** digits)
        df['Close'] = df['Open'] + df['close'] / (10 ** digits)
        df['High'] = df['Open'] + df['high'] / (10 ** digits)
        df['Low'] = df['Open'] + df['low'] / (10 ** digits)
        df['Volume'] = df['vol']

        df = df[self.cols_to_save]
        df.set_index("Date", inplace=True, drop=True)

        return df

    def check_max_range(self):
        '''Check max range for given period and correct it if exceeded'''

        if self.period in self.max_range.keys():

            end = datetime.now()
            start = datetime.strptime(self.start, '%Y-%m-%d')
            delta = relativedelta(end, start)

            delta_months = delta.months + (delta.years * 12)

            if self.max_range[self.period] < delta_months:
                print(f"Max range for given period {self.period} is {self.max_range[self.period]} months from now")
                date_start = datetime.now() + relativedelta(months=-self.max_range[self.period])
                if date_start > datetime.strptime(self.end, '%Y-%m-%d'):
                    self.end = datetime.now().strftime('%Y-%m-%d')
                    print(f"End date is set to {self.end}")

                self.start = date_start.strftime('%Y-%m-%d')
                print(f"Start date is set to {self.start}")

    def get_column_names(self):
        return self.cols_to_save

async def main():
    logging.basicConfig(level=logging.INFO)
    hist_obj = HistoricalDataCollector(
        symbol='EURUSD',
        start='2000-01-01',
        end='2023-08-01',
        period=PeriodCode.PERIOD_D1,
        credentials_file='my_secrets/credentials.json'
    )
    try:
        await hist_obj.connect_to_xapi()
        response = await hist_obj.download_history_data()
        print(response)
        print()

    except xapi.LoginFailed as e:
        print(f"Log in failed: {e}")

    except xapi.ConnectionClosed as e:
        print(f"Connection closed: {e}")

    except ValueError as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())