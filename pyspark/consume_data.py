from kafka import KafkaConsumer
import json

# Kafka broker address
KAFKA_BROKER = "kafka:9093"  # Replace with your broker address
TOPIC_NAME = "api_data"  # Replace with your topic name

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',  # Start reading from the earliest message if no offset is committed
    group_id='my-consumer-group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON messages
)

# Consume messages
for message in consumer:
    print(f"Received message: {message.value}")
