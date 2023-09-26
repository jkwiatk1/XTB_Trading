# XTB xStation5 Trading Project
### Based on code from: <https://github.com/pawelkn/xapi-python>

### API documentation: <http://developers.xstore.pro/documentation>

### Web App framework: <https://semantic-ui.com/introduction/getting-started.html>

## Usage:
You need to have xStation5 account and create `config.py` and `credentials.json` (with your own XTB credentials).

#### Config.py:
* DB_FILE = "absolute .sqlite database file"
* CREDENTIALS_PATH = "credential path"
* PROJECT_PATH = "absolute project path"

#### Credentials.json:
```
{
    "accountId": "11111111",
    "password": "your_password",
    "host": "ws.xtb.com",
    "type": "demo/real",
    "safe": false
}
```

To run uvicorn server in cmd paste: `uvicorn web_app.app:app --reload`. To open Web App in webbrowser put: **http://localhost:8000/** 


## Disclaimer

This xStation5 API Python library is not affiliated with, endorsed by, or in any way officially connected to the xStation5 trading platform or its parent company. The library is provided as-is and is not guaranteed to be suitable for any particular purpose. The use of this library is at your own risk, and the author(s) of this library will not be liable for any damages arising from the use or misuse of this library. Please refer to the license file for more information.

