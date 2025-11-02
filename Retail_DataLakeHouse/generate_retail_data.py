import json
import os
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
RAW_PATH = 'data/raw'
os.makedirs(RAW_PATH, exist_ok=True)

# ---------- Products ----------
products = [
    {
        "product_id": f"P{i:03}",
        "product_name": fake.word().title(),
        "category": random.choice(["Apparel", "Electronics", "Home", "Accessories"]),
        "brand": fake.company(),
        "cost_price": random.randint(100, 800),
        "unit_price": random.randint(200, 1500)
    }
    for i in range(1, 21)
]

with open(f"{RAW_PATH}/products.json", "w") as f:
    json.dump(products, f, indent=2)

# ---------- Stores ----------
cities = [
    ("Delhi", "North"), ("Mumbai", "West"), ("Bangalore", "South"),
    ("Chennai", "South"), ("Kolkata", "East"), ("Hyderabad", "South"),
    ("Pune", "West"), ("Jaipur", "North"), ("Ahmedabad", "West"), ("Lucknow", "North")
]

stores = [
    {
        "store_id": f"S{i:03}",
        "store_name": f"Store_{city}_{i}",
        "city": city,
        "region": region
    }
    for i, (city, region) in enumerate(cities, start=1)
]

with open(f"{RAW_PATH}/stores.json", "w") as f:
    json.dump(stores, f, indent=2)

# ---------- Calendar ----------
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 1, 31)
calendar = []

current = start_date
while current <= end_date:
    calendar.append({
        "date": current.strftime("%Y-%m-%d"),
        "day_of_week": current.strftime("%A"),
        "month": current.strftime("%B"),
        "quarter": f"Q{(current.month - 1) // 3 + 1}",
        "year": current.year
    })
    current += timedelta(days=1)

with open(f"{RAW_PATH}/calendar.json", "w") as f:
    json.dump(calendar, f, indent=2)

# ---------- Sales ----------
sale_id = 1000
for date_info in calendar:
    date = date_info["date"]
    daily_sales = []
    num_sales = random.randint(80, 200)

    for _ in range(num_sales):
        product = random.choice(products)
        store = random.choice(stores)
        qty = random.randint(1, 5)

        sale = {
            "sale_id": sale_id,
            "date": date,
            "store_id": store["store_id"],
            "product_id": product["product_id"],
            "quantity": qty,
            "unit_price": product["unit_price"],
            "total_amount": qty * product["unit_price"]
        }
        daily_sales.append(sale)
        sale_id += 1

    filename = f"{RAW_PATH}/sales_{date}.json"
    with open(filename, "w") as f:
        json.dump(daily_sales, f, indent=2)

print(f"Data generation complete! JSON files saved under '{RAW_PATH}'")
