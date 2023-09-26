from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from database_operations.DatabaseManager import DatabaseManager
from my_secrets import config

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/")
async def index(request: Request):
    database_conn = DatabaseManager(config.DB_FILE)
    await database_conn.connect_to_db()
    data = await database_conn.get_ordered_data(table_name= "stock", columns= ["id","symbol","name"] ,order_by_column ="symbol")

    return templates.TemplateResponse("index.html",{"request": request, "stocks": data})


@app.get("/stock/{symbol}")
async def stock_detail(request: Request, symbol):
    database_conn = DatabaseManager(config.DB_FILE)
    await database_conn.connect_to_db()
    row = await database_conn.get_specify_data("stock",["symbol", "name"], f"symbol = ?",(symbol,))

    return templates.TemplateResponse("stock_detail.html",{"request": request, "stock": row})