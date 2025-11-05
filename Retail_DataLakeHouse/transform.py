import os
import json
import pandas as pd
import sqlite3

STAGING_PATH = "Retail_DataLakeHouse/data/staging"
PROCESSED_PATH = "Retail_DataLakeHouse/data/processed"
DB_PATH = "Retail_DataLakeHouse/data/retail_dwh.db"

os.makedirs(PROCESSED_PATH, exist_ok=True)

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)

# Load dimension tables
products = pd.read_json(f"{STAGING_PATH}/products.json")
stores = pd.read_json(f"{STAGING_PATH}/stores.json")

# Combine all sales JSON files
sales_files = [f for f in os.listdir(STAGING_PATH) if f.startswith("sales_") and f.endswith(".json")]
sales_data = pd.concat([pd.read_json(f"{STAGING_PATH}/{f}") for f in sales_files])

# Join and transform
merged = (
    sales_data.merge(products, on="product_id", how="left")
              .merge(stores, on="store_id", how="left")
)
merged["profit"] = merged["total_amount"] - (merged["quantity"] * merged["cost_price"])

# Save to processed folder (JSON output)
merged.to_json(f"{PROCESSED_PATH}/retail_cleaned.json", orient="records", indent=2)
print("✅ Transformation complete! Cleaned data stored in processed folder.")

# ✅ Also save cleaned data into SQLite for dbt
merged.to_sql("main.fact_retail_sales", conn, index=False, if_exists="replace")
print("✅ fact_retail_sales table loaded into SQLite successfully.")

conn.close()
