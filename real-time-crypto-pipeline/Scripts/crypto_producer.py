# crypto_producer.py
import json
import time
import requests
from kafka import KafkaProducer

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to fetch crypto prices (example: Bitcoin)
def fetch_crypto_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    response = requests.get(url)
    data = response.json()
    return data

# Send data to Kafka every 5 seconds
while True:
    crypto_data = fetch_crypto_price()
    print("Sending:", crypto_data)
    producer.send('crypto_prices', value=crypto_data)
    time.sleep(5)