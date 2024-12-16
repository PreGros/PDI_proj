import requests
import json
from kafka import KafkaProducer
import time
import os
from datetime import datetime, timedelta, timezone

api_url = "https://api.golemio.cz/v2/vehiclepositions"
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MzA5NSwiaWF0IjoxNzM0MDg4MTE3LCJleHAiOjExNzM0MDg4MTE3LCJpc3MiOiJnb2xlbWlvIiwianRpIjoiYWVlMmM2M2ItOTE4OS00ODkxLTkwZTktZjZjNjk0ODg0N2JhIn0.gI9Ez6DxLPKv3uR0U1GkBREFvcdkIDBs6J7RwBXY5xw"

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9093")
TOPIC_NAME = 'api_data'

# Create a Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_send_data():
    while True:
        try:
            last_fetch_time = (datetime.now(timezone.utc) - timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
            url = f"{api_url}?updatedSince={last_fetch_time}"
            headers = {
                'accept': 'application/json',
                'X-Access-Token': api_key
            }

            response = requests.get(url, headers=headers)
            data = response.json()  # Assuming the API returns JSON data

            # Send data to Kafka topic
            producer.send(TOPIC_NAME, value=data)
            print(f"Sent data")

        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

        # Sleep for 5 seconds before the next API call
        time.sleep(5)

if __name__ == '__main__':
    fetch_and_send_data()
