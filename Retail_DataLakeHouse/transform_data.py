import json
import os
import pandas as pd

STAGING_PATH = "data/staging"
PROCESSED_PATH = "data/processed"
os.makedirs(PROCESSED_PATH, exist_ok=True)
##Load all data
products = pd.read_json(f"{STAGING_PATH}/products.json")
stores = pd.read_json(f"{STAGING_PATH}/stores.json")

##Combine all sales Json files
sales_files = [f for f in os.listdir(STAGING_PATH) if f.startswith("sales_")]
sales_data = pd.concat([pd.read_json(f"{STAGING_PATH}/{f}") for f in sales_files])

#join data
merged = (
    sales_data.merge(products, on="product_id", how="left")
              .merge(stores, on="store_id", how="left")

)
merged["profit"] = merged["total_amount"] - (merged["quantity"]*merged["cost_price"])
merged.to_json(f"{PROCESSED_PATH}/retail_cleaned.json", orient="records", indent=2)
print("Transformation complete! Cleaned data stored in data/processed.")


