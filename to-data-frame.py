import logging
import asyncio
import json
import xapi
import pandas as pd

logging.basicConfig(level=logging.INFO)

with open("credentials.json", "r") as f:
    CREDENTIALS = json.load(f)

my_columns = ['Ticker', 'Ask','Bid','Description']
final_dataframe = pd.DataFrame(columns=my_columns)

async def main():
    try:
        async with await xapi.connect(**CREDENTIALS) as x:
            response = await x.socket.getAllSymbols()
            if response['status'] == True:
                # print(response['returnData'])
                for item in response['returnData']:
                    final_dataframe.append(
                        pd.Series(
                            [
                                item['symbol'],
                                item['ask'],
                                item['bid'],
                                item['description']
                            ],
                            index = my_columns
                        ),
                        ignore_index=True
                    )
                print(final_dataframe)
            else:
                print("Failed to get all symbols", response)

    except xapi.LoginFailed as e:
        print(f"Log in failed: {e}")

    except xapi.ConnectionClosed as e:
        print(f"Connection closed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
