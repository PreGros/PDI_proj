import requests
import json
from pyspark.sql import SparkSession, Row
from datetime import datetime, timedelta, timezone

# api_url = "https://api.golemio.cz/v2/vehiclepositions"
# api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MzA5NSwiaWF0IjoxNzM0MDg4MTE3LCJleHAiOjExNzM0MDg4MTE3LCJpc3MiOiJnb2xlbWlvIiwianRpIjoiYWVlMmM2M2ItOTE4OS00ODkxLTkwZTktZjZjNjk0ODg0N2JhIn0.gI9Ez6DxLPKv3uR0U1GkBREFvcdkIDBs6J7RwBXY5xw"

def fetchProcessData(spark, api_url, api_key):
    updated_since = (datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = {
        "accept": "application/json", 
        "X-Access-Token": api_key
    }
    params = {"updatedSince": updated_since}

    response = requests.get(api_url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} {response.text}")
    
    if "data" in data:
        # Convert the JSON data into PySpark Rows
        rows = [Row(**item) for item in data["data"]]
        rdd = spark.sparkContext.parallelize(rows)  # Create an RDD from the rows
        return spark.createDataFrame(rdd)          # Convert RDD to DataFrame
    else:
        # Return an empty DataFrame with no schema if no data is found
        return spark.createDataFrame([], schema=None)
    
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Custom Data Source Example") \
        .getOrCreate()
    
    api_url = "https://api.golemio.cz/v2/vehiclepositions"
    api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MzA5NSwiaWF0IjoxNzM0MDg4MTE3LCJleHAiOjExNzM0MDg4MTE3LCJpc3MiOiJnb2xlbWlvIiwianRpIjoiYWVlMmM2M2ItOTE4OS00ODkxLTkwZTktZjZjNjk0ODg0N2JhIn0.gI9Ez6DxLPKv3uR0U1GkBREFvcdkIDBs6J7RwBXY5xw"

    df = fetchProcessData(spark, api_url, api_key)

    print("Processing data...")
    df.show(10, truncate=False)