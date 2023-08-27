# Trading course:
<https://www.youtube.com/watch?v=xfzGZB4HhEE&t=1094s>
# xStation5 API Python Library
forked from: <https://github.com/pawelkn/xapi-python>

[![Test xapi-python](https://github.com/pawelkn/xapi-python/actions/workflows/test-xapi-python.yml/badge.svg)](https://github.com/pawelkn/xapi-python/actions/workflows/test-xapi-python.yml) [![PyPi](https://img.shields.io/pypi/v/xapi-python.svg)](https://pypi.python.org/pypi/xapi-python/) [![Downloads](https://img.shields.io/pypi/dm/xapi-python)](https://pypi.python.org/pypi/xapi-python/) [![Codecov](https://codecov.io/gh/pawelkn/xapi-python/branch/master/graph/badge.svg)](https://codecov.io/gh/pawelkn/xapi-python/)

The xStation5 API Python library provides a simple and easy-to-use API for interacting with the xStation5 trading platform. With this library, you can connect to the xStation5 platform, retrieve market data, and execute trades.

This library may be used for [BFB Capital](https://bfb.capital) and [XTB](https://www.xtb.com) xStation5 accounts.

API documentation: <http://developers.xstore.pro/documentation>

## Disclaimer

This xStation5 API Python library is not affiliated with, endorsed by, or in any way officially connected to the xStation5 trading platform or its parent company. The library is provided as-is and is not guaranteed to be suitable for any particular purpose. The use of this library is at your own risk, and the author(s) of this library will not be liable for any damages arising from the use or misuse of this library. Please refer to the license file for more information.

## Installation

You can install xAPI using pip. Simply run the following command:

```shell
pip install xapi-python
```

## Usage

To use xAPI, you will need to have an active account with the xStation5 trading platform. Once you have an account, you can use the xAPI library to connect to the platform and begin trading.



### credentials.json

```json
{
    "accountId": "<your_client_id>",
    "password": "<your_password>",
    "host": "ws.xtb.com",
    "type": "real",
    "safe": false
}
```

Once you have created the _credentials.json_ file, you can run.

## Unit Tests

This will run all of the unit tests in the tests directory:

```shell
cd xapi-python
python3 -m unittest discover tests
```

