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
    existing_symbols = await database_conn.get_whole_data("stock", ("id", "symbol", "name"))



    return templates.TemplateResponse("index.html",{"request": request, "stocks": existing_symbols})