import logging
import asyncio
import json
import xapi


class ApiConnection:
    ''' Class for connection with XTB

    Attrs
    ==================
    credentials_file - json
                file with xtb credentials

    ws_client - websocket object
                used to maintain a connection to a WebSocket

    '''
    def __init__(self, credentials_file):
        self.credentials_file = credentials_file
        self.ws_client = None

    async def connect(self):
        with open(self.credentials_file, "r") as f:
            self.CREDENTIALS = json.load(f)
        self.ws_client = await xapi.connect(**self.CREDENTIALS)
        return self.ws_client

    async def disconnect(self):
        await self.ws_client.disconnect()

    def get_websocket_object(self):
        return self.ws_client




async def main():
    logging.basicConfig(level=logging.INFO)
    my_connection = ApiConnection("my_secrets/credentials.json")

    try:
        await my_connection.connect()
        print("Polaczono")
        response = my_connection.get_websocket_object()
        response_symbols = await response.socket.getAllSymbols()
        print(response_symbols['returnData'])
        await my_connection.disconnect()
        print("Rozlaczono")

    except xapi.LoginFailed as e:
        print(f"Log in failed: {e}")

    except xapi.ConnectionClosed as e:
        print(f"Connection closed: {e}")

    except ValueError as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    asyncio.run(main())
