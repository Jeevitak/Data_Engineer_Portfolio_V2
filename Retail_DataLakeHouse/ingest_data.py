import os
import json
import shutil

RAW_PATH = "data/raw"
STAGING_PATH = "data/staging"

REQUIRED_KEYS = ["sale_id", "date", "store_id", "product_id", "quantity", "unit_price", "total_amount"]

os.makedirs(STAGING_PATH, exist_ok=True)

files = os.listdir(RAW_PATH)
print(f"\nFound {len(files)} files in raw folder.\n")

for filename in files:
    if not filename.endswith(".json"):
        continue

    filepath = os.path.join(RAW_PATH, filename)
    print(f"Processing: {filename}")

    try:
        with open(filepath, "r") as f:
            data = json.load(f)

        # Validate only sales files
        if filename.startswith("sales_") and isinstance(data, list):
            valid = all(all(key in record for key in REQUIRED_KEYS) for record in data)
            if not valid:
                print(f"⚠ Skipping invalid file: {filename}")
                continue

        shutil.copy(filepath, os.path.join(STAGING_PATH, filename))
        print(f"Copied to staging.\n")

    except Exception as e:
        print(f"⚠ Error in {filename}: {e}")

print("\nIngestion complete!")
