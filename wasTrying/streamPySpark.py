import requests
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import col, explode
import time

# API details
api_url = "https://api.golemio.cz/v2/vehiclepositions"
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MzA5NSwiaWF0IjoxNzM0MDg4MTE3LCJleHAiOjExNzM0MDg4MTE3LCJpc3MiOiJnb2xlbWlvIiwianRpIjoiYWVlMmM2M2ItOTE4OS00ODkxLTkwZTktZjZjNjk0ODg0N2JhIn0.gI9Ez6DxLPKv3uR0U1GkBREFvcdkIDBs6J7RwBXY5xw"

timeReq = (datetime.now(timezone.utc) - timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

# Update the timestamp for the API call
url = f"{api_url}?updatedSince={timeReq}"
headers = {
    'accept': 'application/json',
    'X-Access-Token': api_key
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    # Parse the response JSON
    data = response.json()

    # Save the response to a file
    with open("response.json", "w") as file:
        json.dump(data, file, indent=4)

    print("Response saved to 'response.json'")
else:
    print(f"Failed to fetch data: {response.status_code}, {response.text}")