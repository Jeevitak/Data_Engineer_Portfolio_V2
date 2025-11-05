import os
import json
import pandas as pd
import sqlite3

RAW_PATH = "Retail_DataLakeHouse/data/raw"
DB_PATH = "Retail_DataLakeHouse/data/retail_dwh.db"

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)

# Process all JSON sales files
for file in os.listdir(RAW_PATH):
    if not file.startswith("sales_") or not file.endswith(".json"):
        continue

    filepath = os.path.join(RAW_PATH, file)
    table_name = f"sales_{file.split('_')[1].replace('.json', '').replace('-', '_')}"

    print(f"Processing: {file}")
    try:
        df = pd.read_json(filepath)
        df.to_sql(f"main.{table_name}", conn, if_exists="replace", index=False)
        print(f"✅ Loaded {file} into table: {table_name}")
    except Exception as e:
        print(f"⚠ Error loading {file}: {e}")

conn.close()
print("✅ All JSON files loaded into SQLite successfully.")
