import os
import json
import shutil
import sqlite3
import pandas as pd

RAW_PATH = "data/raw"
STAGING_PATH = "data/staging"
DB_PATH = "data/retail_dwh.db"

REQUIRED_KEYS = ["sale_id", "date", "store_id", "product_id", "quantity", "unit_price", "total_amount"]

os.makedirs(STAGING_PATH, exist_ok=True)

files = os.listdir(RAW_PATH)
print(f"\nFound {len(files)} files in raw folder.\n")

# Connect to SQLite DB
conn = sqlite3.connect(DB_PATH)

for filename in files:
    if not filename.endswith(".json"):
        continue

    filepath = os.path.join(RAW_PATH, filename)
    print(f"Processing: {filename}")

    try:
        with open(filepath, "r") as f:
            data = json.load(f)

        # ✅ Always copy to staging, including products, stores, etc.
        shutil.copy(filepath, os.path.join(STAGING_PATH, filename))
        print(f"Copied to staging.\n")

        # ✅ Load sales files into SQLite
        if filename.startswith("sales_") and isinstance(data, list):
            valid = all(all(key in record for key in REQUIRED_KEYS) for record in data)
            if not valid:
                print(f"⚠ Skipping invalid file: {filename}")
                continue

            # ✅ Fix table name (replace '-' with '_')
            table_name = filename.replace(".json", "").replace("-", "_")
            full_table = f"main.{table_name}"
            print(f"Creating table: {full_table}")

            df = pd.DataFrame(data)
            df.to_sql(full_table, conn, index=False, if_exists="replace")
            print(f"✅ Loaded {len(df)} records into {full_table}\n")

    except Exception as e:
        print(f"⚠ Error in {filename}: {e}")

conn.close()
print("\nIngestion complete!")
