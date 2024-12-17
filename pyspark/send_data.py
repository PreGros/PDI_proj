import requests
import json
from kafka import KafkaProducer
import time
import os
from datetime import datetime, timezone, timedelta

# API configuration
api_url = "https://api.golemio.cz/v2/vehiclepositions"
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MzA5NSwiaWF0IjoxNzM0MDg4MTE3LCJleHAiOjExNzM0MDg4MTE3LCJpc3MiOiJnb2xlbWlvIiwianRpIjoiYWVlMmM2M2ItOTE4OS00ODkxLTkwZTktZjZjNjk0ODg0N2JhIn0.gI9Ez6DxLPKv3uR0U1GkBREFvcdkIDBs6J7RwBXY5xw"

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9093")
TOPIC_NAME = 'api_data'

# Create a Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    # max_request_size=20971520  # 20MB
)

def fetch_and_send_data():
    # Start tracking time from the current moment
    last_fetch_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    index = 0
    file_path = f'getData/data{index}.json'

    while True:
        try:
            # Format the timestamp for the API query
            updated_since = last_fetch_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            # url = f"{api_url}?updatedSince={updated_since}"
            params = {
                'updatedSince': updated_since,
                'includeNotTracking': 'false',
                'includeNotPublic': 'false'
            }
            headers = {
                'accept': 'application/json',
                'X-Access-Token': api_key
            }

            # Fetch data from the API
            response = requests.get(api_url, params=params, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            data = response.json()  # Parse the response as JSON


            # # Write JSON data to a file
            # file_path = f'getData/data{index}.json'
            # with open(file_path, 'w') as json_file:
            #     json.dump(data, json_file, indent=4)  # indent=4 formats the JSON for readability
            # index = index + 1




            last_fetch_time = datetime.now(timezone.utc)
            print(f"Sending batch of features!")

            # Send each feature to Kafka
            if "features" in data:
                features = data["features"]
                for feature in features:
                    try:
                        producer.send(TOPIC_NAME, value=feature).get(timeout=10)

                        # # Write JSON data to a file
                        # if (index % 500 == 0):
                        #     file_path = f'getData/data{index}.json'
                        #     with open(file_path, 'w') as json_file:
                        #         json.dump(feature, json_file, indent=4)  # indent=4 formats the JSON for readability
                        # index = index + 1

                        # print(f"Successfully sent feature!")
                    except Exception as e:
                        print(f"Error sending feature to Kafka: {e}")
            else:
                print("No 'features' field in the response data.")



        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

        # Sleep for 45 seconds before the next API call
        time.sleep(5)

if __name__ == '__main__':
    fetch_and_send_data()
