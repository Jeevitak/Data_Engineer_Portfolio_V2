import os
import json
import sqlite3
import shutil

RAW_PATH = "Retail_DataLakeHouse/data/raw"
STAGING_PATH = "Retail_DataLakeHouse/data/staging"
DB_PATH = "Retail_DataLakeHouse/data/retail_dwh.db"

REQUIRED_KEYS = ["sale_id", "date", "store_id", "product_id", "quantity", "unit_price", "total_amount"]

os.makedirs(STAGING_PATH, exist_ok=True)

files = os.listdir(RAW_PATH)
print(f"\nFound {len(files)} files in raw folder.\n")

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

for filename in files:
    if not filename.endswith(".json"):
        continue

    filepath = os.path.join(RAW_PATH, filename)
    print(f"Processing: {filename}")

    try:
        with open(filepath, "r") as f:
            data = json.load(f)

        if filename.startswith("sales_") and isinstance(data, list):
            valid = all(all(key in record for key in REQUIRED_KEYS) for record in data)
            if not valid:
                print(f"⚠ Skipping invalid file: {filename}")
                continue

            # Extract table name from filename
            table_name = filename.replace(".json", "")
            print(f"Creating table: main.{table_name}")

            # Convert JSON to SQLite table
            columns = ", ".join(REQUIRED_KEYS)
            cur.execute(f"DROP TABLE IF EXISTS main.{table_name}")
            cur.execute(
                f"CREATE TABLE main.{table_name} ({columns.replace(',', ' TEXT,')} TEXT)"
            )

            for record in data:
                values = tuple(str(record[key]) for key in REQUIRED_KEYS)
                cur.execute(
                    f"INSERT INTO main.{table_name} VALUES ({','.join(['?'] * len(REQUIRED_KEYS))})",
                    values
                )
            conn.commit()
            print(f" Table '{table_name}' created with {len(data)} rows.\n")

        shutil.copy(filepath, os.path.join(STAGING_PATH, filename))
        print(f"Copied to staging.\n")

    except Exception as e:
        print(f"⚠ Error in {filename}: {e}")

conn.close()
print("\nIngestion complete!")
