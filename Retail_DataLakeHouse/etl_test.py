import os
import json
import pandas as pd
RAW_PATH = "data/raw"
STAGING_PATH = "data/staging"
PROCESSED_PATH = "data/processed"

def test_file_existance():
    assert os.path.exists(f"{RAW_PATH}/products.json")
    assert os.path.exists(f"{STAGING_PATH}/products.json")
    assert os.path.exists(f"{PROCESSED_PATH}/retail_cleaned.json")
    print("All files exist in each zone")
def test_row_counts():
    with open(f"{RAW_PATH}/products.json") as f:
        raw_products = len(json.load(f))
    with open(f"{STAGING_PATH}/products.json") as f:
        stage_products = len(json.load(f))
    assert raw_products == stage_products    
print("Row counts consistent between raw and staging")
def test_null_values():
    df = pd.read_json(f"{PROCESSED_PATH}/retail_cleaned.json")
    assert not df.isnull().any().any() 
    print("No null values in processed data")
def test_profit_calculation():
    df = pd.read_json(f"{PROCESSED_PATH}/retail_cleaned.json")
    sample = df.iloc[0]
    expected = sample["total_amount"] - (sample["quantity"]*sample["cost_price"])
    assert abs(sample["profit"] - expected) < 0.01
    print("profit calculation verified")
if __name__ == "__main__":
    test_file_existance()
    test_row_counts()
    test_null_values()
    test_profit_calculation()
    print("ETL Testing Completed Successfully!")