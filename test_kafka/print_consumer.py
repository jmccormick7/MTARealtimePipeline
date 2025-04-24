from kafka import KafkaConsumer
import json

# Connect to the Kafka broker
consumer = KafkaConsumer(
    'gtfs-test-topic',  # Topic to consume
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  # Start from earliest messages
    group_id='test-consumer-group'
)

print("Starting Kafka consumer...")
for message in consumer:
    print(f"Received message: {message.value}")