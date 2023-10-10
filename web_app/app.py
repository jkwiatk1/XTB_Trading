from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from database_operations.DatabaseManager import DatabaseManager
from my_secrets import config
from scripts.HistoricalDataCollector import HistoricalDataCollector
from xapi import PeriodCode
from datetime import datetime

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/")
async def index(request: Request):
    stock_filter = request.query_params.get('filter', False)

    database_conn = DatabaseManager(config.DB_FILE)
    await database_conn.connect_to_db()

    if stock_filter == 'new_intraday_highs':
        query = "SELECT * FROM stock WHERE name LIKE '%Technologies%'"
        params = None
        data = await database_conn.execute_custom_query(query, params)
    else:
        data = await database_conn.get_ordered_data(table_name= "stock", columns= ["id","symbol","name"] ,order_by_column ="symbol")

    return templates.TemplateResponse("index.html",{"request": request, "stocks": data})


@app.get("/stock/{symbol}")
async def stock_detail(request: Request, symbol):
    database_conn = DatabaseManager(config.DB_FILE)
    await database_conn.connect_to_db()
    company = await database_conn.get_specify_data("stock",["id","symbol", "name"], f"symbol = ?",(symbol,))
    for stock in company:
        print("SYMBOL: " + stock['symbol'] + "  NAME: " + stock['name']+ "  ID: " + str(stock['id']))
        print()

    prices = await database_conn.get_ordered_data("stock_price_1d",columns = ["*"], order_by_column = "date",conditions= f"stock_id = ?", condition_params =  (company[0]['id'],), ascending=False)
    if not prices:
        print(f"No data in db for this symbol: {symbol}. Will be downloaded.")
        symbol = symbol
        symbol_id  = await database_conn.get_specify_data("stock",["id"], f"symbol = ?",(symbol,))
        symbol_id = symbol_id[0][0]

        hist_data_collector = HistoricalDataCollector(
            symbol= symbol,
            start='2000-01-01',
            end=datetime.today().strftime('%Y-%m-%d'),
            period=PeriodCode.PERIOD_D1,
            credentials_file=config.CREDENTIALS_PATH
        )
        await hist_data_collector.connect_to_xapi()
        await database_conn.populate_prices("stock_price_1d", symbol_id, symbol, hist_data_collector)
        print("Successfully downloaded.")

    prices = await database_conn.get_ordered_data("stock_price_1d",columns = ["*"], order_by_column = "date",conditions= f"stock_id = ?", condition_params =  (company[0]['id'],), ascending=False)


    return templates.TemplateResponse("stock_detail.html",{"request": request, "stock": company[0], "bars": prices})


@app.post("/stock/{symbol}/download")
async def download_newest_data(request: Request, symbol: str, symbol_id: int = Form(...)):
    database_conn = DatabaseManager(config.DB_FILE)
    await database_conn.connect_to_db()
    company = await database_conn.get_specify_data("stock",["id","symbol", "name"], f"symbol = ?",(symbol,))

    print(f"Newest data for symbol: {symbol} will be downloaded.")
    symbol = symbol
    symbol_id = symbol_id

    hist_data_collector = HistoricalDataCollector(
        symbol= symbol,
        start='2000-01-01',
        end=datetime.today().strftime('%Y-%m-%d'),
        period=PeriodCode.PERIOD_D1,
        credentials_file=config.CREDENTIALS_PATH
    )
    await hist_data_collector.connect_to_xapi()
    await database_conn.populate_prices("stock_price_1d", symbol_id, symbol, hist_data_collector)
    print("Successfully downloaded.")

    prices = await database_conn.get_ordered_data("stock_price_1d",columns = ["*"], order_by_column = "date",conditions= f"stock_id = ?", condition_params =  (company[0]['id'],), ascending=False)

    # return RedirectResponse(url=f"/stock/{symbol}", status_code=303)  from fastapi.responses import RedirectResponse
    return templates.TemplateResponse("stock_detail.html", {"request": request, "stock": company[0], "bars": prices})