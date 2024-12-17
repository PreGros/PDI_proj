from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, TimestampType, ArrayType, IntegerType




import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--s', required=True, help="Switch argument described in detail in readme.")
args = parser.parse_args()

# print(f"Received argument: {args.s}")




# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define the Kafka source
kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "api_data"  # Replace with the actual topic name

# Read data from Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, ArrayType, LongType
)

# Define the schema
schema = StructType([
    StructField("geometry", StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(DoubleType(), True), True)
    ]), True),
    StructField("properties", StructType([
        StructField("trip", StructType([
            StructField("agency_name", StructType([
                StructField("real", StringType(), True),
                StructField("scheduled", StringType(), True)
            ]), True),
            StructField("cis", StructType([
                StructField("line_id", StringType(), True),
                StructField("trip_number", IntegerType(), True)
            ]), True),
            StructField("sequence_id", IntegerType(), True),
            StructField("origin_route_name", StringType(), True),
            StructField("gtfs", StructType([
                StructField("route_id", StringType(), True),
                StructField("route_short_name", StringType(), True),
                StructField("route_type", IntegerType(), True),
                StructField("trip_id", StringType(), True),
                StructField("trip_headsign", StringType(), True)
            ]), True),
            StructField("start_timestamp", StringType(), True),
            StructField("vehicle_type", StructType([
                StructField("id", IntegerType(), True),
                StructField("description_cs", StringType(), True),
                StructField("description_en", StringType(), True)
            ]), True),
            StructField("vehicle_registration_number", IntegerType(), True),
            StructField("wheelchair_accessible", BooleanType(), True),
            StructField("air_conditioned", BooleanType(), True),
            StructField("usb_chargers", BooleanType(), True)
        ]), True),
        StructField("last_position", StructType([
            StructField("bearing", IntegerType(), True),
            StructField("delay", StructType([
                StructField("actual", IntegerType(), True),
                StructField("last_stop_arrival", IntegerType(), True),
                StructField("last_stop_departure", IntegerType(), True)
            ]), True),
            StructField("last_stop", StructType([
                StructField("id", StringType(), True),
                StructField("sequence", IntegerType(), True),
                StructField("arrival_time", StringType(), True),
                StructField("departure_time", StringType(), True)
            ]), True),
            StructField("next_stop", StructType([
                StructField("id", StringType(), True),
                StructField("sequence", IntegerType(), True),
                StructField("arrival_time", StringType(), True),
                StructField("departure_time", StringType(), True)
            ]), True),
            StructField("is_canceled", BooleanType(), True),
            StructField("origin_timestamp", StringType(), True),
            StructField("speed", IntegerType(), True),
            StructField("shape_dist_traveled", StringType(), True),
            StructField("tracking", BooleanType(), True)
        ]), True),
        StructField("all_positions", StructType([
            StructField("type", StringType(), True),
            StructField("features", ArrayType(StructType([
                StructField("geometry", StructType([
                    StructField("type", StringType(), True),
                    StructField("coordinates", ArrayType(DoubleType(), True), True)
                ]), True),
                StructField("properties", StructType([
                    StructField("bearing", IntegerType(), True),
                    StructField("delay", StructType([
                        StructField("actual", IntegerType(), True),
                        StructField("last_stop_arrival", IntegerType(), True),
                        StructField("last_stop_departure", IntegerType(), True)
                    ]), True),
                    StructField("last_stop", StructType([
                        StructField("id", StringType(), True),
                        StructField("sequence", IntegerType(), True),
                        StructField("arrival_time", StringType(), True),
                        StructField("departure_time", StringType(), True)
                    ]), True),
                    StructField("next_stop", StructType([
                        StructField("id", StringType(), True),
                        StructField("sequence", IntegerType(), True),
                        StructField("arrival_time", StringType(), True),
                        StructField("departure_time", StringType(), True)
                    ]), True),
                    StructField("is_canceled", BooleanType(), True),
                    StructField("origin_timestamp", StringType(), True),
                    StructField("speed", IntegerType(), True),
                    StructField("shape_dist_traveled", StringType(), True),
                    StructField("tracking", BooleanType(), True)
                ]), True),
                StructField("type", StringType(), True)
            ]), True), True)
        ]), True)
    ]), True),
    StructField("type", StringType(), True)
])

if (args.s == "1"):
    # Parse the incoming Kafka JSON message
    parsed_df = kafka_stream_df.select(from_json(col("value").cast("string"), schema).alias("data"))

    # Flatten the schema and select only the vehicle ID and speed columns
    flattened_df = parsed_df.selectExpr(
        "data.properties.trip.vehicle_registration_number as vehicle_id",  # Vehicle ID
        "data.properties.last_position.speed as speed"  # Speed
    )

    # Filter out rows where 'speed' is null
    filtered_df = flattened_df.filter((col("speed").isNotNull()) & (col("speed") > 50))

    # Write the filtered data to the console
    query = filtered_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .start()

    query.awaitTermination()