import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GTFS Realtime Streaming") \
    .getOrCreate()

# Function to fetch data from API
def fetch_api_data():
    API_KEY = "MY-API-KEY"
    url = "https://api.golemio.cz/v2/vehiclepositions?updatedSince=$(date --date='1 minutes ago' --utc '+%Y-%m-%dT%H:%M:%SZ')"
    headers = {
        'accept': 'application/json',
        'X-Access-Token': API_KEY
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()["features"]
    else:
        return []
    
# Read data into Spark streaming DataFrame
data_schema = """
    geometry STRUCT<coordinates: ARRAY<DOUBLE>>,
    properties STRUCT<last_position: STRUCT<speed: DOUBLE>>
"""

# Replace `fetch_api_data` with a custom source that streams data
raw_data = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 1) \
    .load() \
    .selectExpr("CAST(NULL AS STRING) as dummy") \
    .withColumn("data", fetch_api_data())  # Replace with custom source logic

# Process data
vehicles_df = raw_data.selectExpr("data.*").select(
    col("geometry.coordinates"),
    col("properties.last_position.speed").alias("speed")
).filter(col("speed") > 50)

# Output vehicles exceeding 50 km/h
query = vehicles_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()