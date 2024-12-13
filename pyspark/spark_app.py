import requests
import json
from pyspark.sql import SparkSession, Row
from datetime import datetime, timedelta, timezone

class GTFSRealtimeSource:
    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.api_key = api_key

    def fetch_data(self):
        """Fetch data from the API."""
        # Generate the timestamp for updatedSince
        updated_since = (datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Fetch data from the API
        headers = {
            "accept": "application/json", 
            "X-Access-Token": self.api_key
        }
        params = {"updatedSince": updated_since}
        
        response = requests.get(self.api_url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data: {response.status_code} {response.text}")

    def to_spark_dataframe(self, spark):
        """Fetch the API data and return a PySpark DataFrame."""
        data = self.fetch_data()
        if "data" in data:
            # Convert the JSON data into PySpark Rows
            rows = [Row(**item) for item in data["data"]]
            rdd = spark.sparkContext.parallelize(rows)  # Create an RDD from the rows
            return spark.createDataFrame(rdd)          # Convert RDD to DataFrame
        else:
            # Return an empty DataFrame with no schema if no data is found
            return spark.createDataFrame([], schema=None)


if __name__ == "__main__":
    # Initialize PySpark session
    spark = SparkSession.builder \
        .appName("Custom Data Source Example") \
        .getOrCreate()

    # Define the API endpoint and your API key
    api_url = "https://api.golemio.cz/v2/vehiclepositions"
    api_key = "MY-API-KEY"

    # Initialize the custom data source
    custom_source = GTFSRealtimeSource(api_url, api_key)

    # Periodically fetch data and process it in PySpark
    import time
    while True:
        try:
            # Fetch data and convert to DataFrame
            print("Fetching data...")
            df = custom_source.to_spark_dataframe(spark)

            # Process the DataFrame (example: show top 10 records)
            print("Processing data...")
            df.show(10, truncate=False)

        except Exception as e:
            print(f"Error occurred: {e}")

        # Wait for the next interval (e.g., 60 seconds)
        time.sleep(60)
