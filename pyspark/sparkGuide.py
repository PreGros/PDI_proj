import requests
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import col, explode

# API details
api_url = "https://api.golemio.cz/v2/vehiclepositions"
api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MzA5NSwiaWF0IjoxNzM0MDg4MTE3LCJleHAiOjExNzM0MDg4MTE3LCJpc3MiOiJnb2xlbWlvIiwianRpIjoiYWVlMmM2M2ItOTE4OS00ODkxLTkwZTktZjZjNjk0ODg0N2JhIn0.gI9Ez6DxLPKv3uR0U1GkBREFvcdkIDBs6J7RwBXY5xw"

# Generate timestamp for 1 minute ago in UTC
timestamp = (datetime.utcnow() - timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

# API endpoint and headers
url = f"{api_url}?updatedSince={timestamp}"
headers = {
    'accept': 'application/json',
    'X-Access-Token': api_key
}

# Fetch data from API
response = requests.get(url, headers=headers)

if response.status_code == 200:
    # Parse JSON response
    data = response.json()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MySparkApp") \
        .getOrCreate()

    # Convert the JSON data into a JSON string
    json_data = json.dumps(data)

    # Parallelize the JSON string into an RDD of JSON records
    rdd = spark.sparkContext.parallelize([json_data])

    # Read the RDD as a DataFrame
    df = spark.read.json(rdd)

    # Exploding the 'features' array to get each item as a separate row
    exploded_df = df.select(explode(col("features")).alias("feature"))

    # Extract 'coordinates' and 'type' from the exploded 'feature' column
    final_df = exploded_df.select(
        col("feature.properties.last_position.speed").alias("speed"),
        col("feature.properties.trip.vehicle_registration_number").alias("vehicle_registration_number")
    )

    # Filter rows where 'speed' is greater than 50
    filtered_speed_df = final_df.filter(col("speed") > 50)

    # Show the resulting DataFrame
    filtered_speed_df.show(60, truncate=False)

    # Write the filtered DataFrame to a CSV file
    filtered_speed_df.write.csv("output/filtered_speed.csv", header=True, mode="overwrite")

















#     with open("api_response.json", "w") as f:
#         json.dump(data, f)

#     # Print the raw data to inspect its structure
#     print(json.dumps(data, indent=4))  # Inspect the raw JSON response

#     # Extract 'features' array to ensure we are processing the correct part of the response
#     json_data = data.get("features", [])

#     # Initialize SparkSession
#     spark = SparkSession.builder.appName("Read API Data").getOrCreate()

#     # Convert the JSON list to an RDD
#     rdd = spark.sparkContext.parallelize(json_data)

#     # Read the RDD as a JSON DataFrame
#     df = spark.read.option("multiLine", "true").json(rdd)

#     # Exploding the 'features' array to separate each feature into a row
#     exploded_df = df.select(explode(col("features")).alias("feature"))

#     # Flatten the nested structure to extract specific fields
#     final_df = exploded_df.select(
#         col("feature.geometry.coordinates").alias("coordinates"),
#         col("feature.geometry.type").alias("geometry_type"),
#         col("feature.properties.last_position.speed").alias("speed"),
#         col("feature.properties.last_position.state_position").alias("state_position")
#     )

#     # Filter rows where 'speed' is not null
#     non_null_speed_df = final_df.filter(col("speed").isNotNull())

#     # Show the resulting DataFrame
#     non_null_speed_df.show(truncate=False)

# else:
#     print(f"Failed to fetch data: {response.status_code}")





    # # Initialize Spark session
    # spark = SparkSession.builder \
    #     .appName("MySparkApp") \
    #     .getOrCreate()

    # # Load the JSON file
    # df = spark.read.option("multiLine", "true").json("api_response.json")

    # # Exploding the 'features' array to get each item as a separate row
    # exploded_df = df.select(explode(col("features")).alias("feature"))

    # # Extract 'coordinates' and 'type' from the exploded 'feature' column
    # final_df = exploded_df.select(
    #     col("feature.geometry.coordinates").alias("coordinates"),
    #     col("feature.geometry.type").alias("type"),
    #     col("feature.properties.last_position.speed").alias("speed")
    # )

    # # Filter rows where 'speed' is not null
    # non_null_speed_df = final_df.filter(col("speed").isNotNull())

    # # Show the resulting DataFrame
    # non_null_speed_df.show(truncate=False)