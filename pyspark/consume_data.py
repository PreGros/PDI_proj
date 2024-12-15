from kafka import KafkaConsumer
import json

# Kafka broker address
KAFKA_BROKER = 'kafka:9093'  # or 'kafka:9093' if running inside Docker
TOPIC_NAME = 'api_data'

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    group_id='data-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Deserialize JSON messages
    auto_offset_reset='earliest'  # Start consuming from the earliest message if there is no offset
)

# Consume and print messages from the topic
print(f"Consuming messages from {TOPIC_NAME}...")

for message in consumer:
    print(f"Received message: {message.value}")
