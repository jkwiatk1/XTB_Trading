import sqlite3

connection = sqlite3.connect('trade_app_db.sqlite')

cursor = connection.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock (
    id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    company TEXT NOT NULL
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_price (
        id INTEGER PRIMARY KEY ,
        stock_id INTEGER,
        date DATE  NOT NULL,
        open REAL NOT NULL,
        close REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        volume INTEGER NOT NULL,
        FOREIGN KEY (stock_id) REFERENCES stock (id)
    )
""")

connection.commit()
