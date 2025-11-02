import sqlite3
import json
import glob

conn = sqlite3.connect('data/retail_dwh.db')
cur = conn.cursor()

# create table
cur.execute('''
CREATE TABLE IF NOT EXISTS fact_retail_sales (
    sale_id INTEGER PRIMARY KEY,
    date TEXT,
    store_id TEXT,
    product_id TEXT,
    quantity INTEGER,
    unit_price REAL,
    total_amount REAL
)
''')

# load all daily sales JSON files
for file in glob.glob('data/raw/sales_*.json'):
    with open(file, 'r') as f:
        sales = json.load(f)
        for s in sales:
            cur.execute('''
            INSERT INTO fact_retail_sales
            (sale_id, date, store_id, product_id, quantity, unit_price, total_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (s['sale_id'], s['date'], s['store_id'], s['product_id'],
                  s['quantity'], s['unit_price'], s['total_amount']))

conn.commit()
conn.close()
