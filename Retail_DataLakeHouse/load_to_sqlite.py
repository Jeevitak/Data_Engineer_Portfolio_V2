import sqlite3
import pandas as pd

PROCESSED_PATH = "data/processed/retail_cleaned.json"
DB_PATH = "data/retail_dwh.db"

df = pd.read_json(PROCESSED_PATH)
conn = sqlite3.connect(DB_PATH)

df.to_sql("fact_reatil_sales", conn, if_exists="replace",index=False)
conn.close()
print("Data successfully loaded into SQLite (fact_retail_sales tables)")