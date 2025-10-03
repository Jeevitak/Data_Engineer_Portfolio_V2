# crypto_consumer.py
import json
from kafka import KafkaConsumer

# Connect to Kafka
consumer = KafkaConsumer(
    'crypto_prices',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='crypto-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening to crypto_prices topic...")

for message in consumer:
    print("Received:", message.value)