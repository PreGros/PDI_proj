import requests
import json
from kafka import KafkaProducer
import time
import os
from datetime import datetime, timezone, timedelta

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--m', required=True, help="Mode fetch data from API or use local for testing purpose.")
args = parser.parse_args()

api_url = "https://api.golemio.cz/v2/vehiclepositions"
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MzA5NSwiaWF0IjoxNzM0MDg4MTE3LCJleHAiOjExNzM0MDg4MTE3LCJpc3MiOiJnb2xlbWlvIiwianRpIjoiYWVlMmM2M2ItOTE4OS00ODkxLTkwZTktZjZjNjk0ODg0N2JhIn0.gI9Ez6DxLPKv3uR0U1GkBREFvcdkIDBs6J7RwBXY5xw"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9093")
TOPIC_NAME = 'api_data'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

def fetch_and_send_data():
    lastFetchTime = datetime.now(timezone.utc) - timedelta(minutes=1)
    index = 0
    file_path = f'getData/data{index}.json'

    while True:
        try:
            updatedSince = lastFetchTime.strftime('%Y-%m-%dT%H:%M:%SZ')
            params = {
                'updatedSince': updatedSince,
                'includeNotTracking': 'false',
                'includeNotPublic': 'false'
            }
            headers = {
                'accept': 'application/json',
                'X-Access-Token': api_key
            }

            response = requests.get(api_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

            lastFetchTime = datetime.now(timezone.utc)
            print(f"Sending batch of features!")

            if "features" in data:
                features = data["features"]
                for feature in features:
                    try:
                        producer.send(TOPIC_NAME, value=feature).get(timeout=10)
                    except Exception as e:
                        print(f"Error sending feature to Kafka: {e}")
            else:
                print("No 'features' field in the response data.")

        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

        time.sleep(5)


def load_and_send_data():   

    jsonFile = open('testData/testData.json')

    data = json.load(jsonFile)

    if "features" in data:
        features = data["features"]
        for feature in features:
            try:
                producer.send(TOPIC_NAME, value=feature).get(timeout=10)
                print("Sending feature!")

            except Exception as e:
                print(f"Error sending feature to Kafka: {e}")
    else:
        print("No 'features' field in the response data.")


if __name__ == '__main__':
    if (args.m == "api"):
        fetch_and_send_data()
    if (args.m == "local"):
        load_and_send_data()
