import requests
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import col, explode
import time

# API details
api_url = "https://api.golemio.cz/v2/vehiclepositions"
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MzA5NSwiaWF0IjoxNzM0MDg4MTE3LCJleHAiOjExNzM0MDg4MTE3LCJpc3MiOiJnb2xlbWlvIiwianRpIjoiYWVlMmM2M2ItOTE4OS00ODkxLTkwZTktZjZjNjk0ODg0N2JhIn0.gI9Ez6DxLPKv3uR0U1GkBREFvcdkIDBs6J7RwBXY5xw"

# Global variables
last_fetch_time = None
cumulative_df = None

def fetchProcessData(spark):
    global last_fetch_time, cumulative_df

    # If this is the first call, set the initial timestamp
    if last_fetch_time is None:
        last_fetch_time = (datetime.now(timezone.utc) - timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Update the timestamp for the API call
    url = f"{api_url}?updatedSince={last_fetch_time}"
    headers = {
        'accept': 'application/json',
        'X-Access-Token': api_key
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        # data = response.json()

        # Update the last fetch time to current UTC time
        last_fetch_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        json_data = json.loads(response.text)

        source_df = spark.createDataFrame(json_data)




    else:
        print(f"Failed to fetch data: {response.status_code} {response.text}")

def main():
    spark = SparkSession.builder \
        .appName("MySparkApp") \
        .getOrCreate()

    while True:
        fetchProcessData(spark)

        time.sleep(15)

if __name__ == "__main__":
    main()
