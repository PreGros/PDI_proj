from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, last, max, expr, current_timestamp, to_utc_timestamp, unix_timestamp, date_trunc, window, date_format, min as spark_min, row_number, max as spark_max, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, TimestampType, ArrayType, IntegerType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout   
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--s', required=True, help="Switch argument described in detail in readme.")
parser.add_argument('--m', required=True, help="Mode fetch data from API or use local for testing purpose.")
args = parser.parse_args()

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Feature schema
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

# Define the Kafka source
kafkaBootstrapServers = "kafka:9092"
kafkaTopic = "api_data"

# For testing purpose, script takes earliest messages from kafka
if (args.m == "api"):
    startingOffset = "latest"
else:

    startingOffset = "earliest"

# Read data from Kafka
kafkaStreamDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaBootstrapServers) \
    .option("subscribe", kafkaTopic) \
    .option("startingOffsets", startingOffset) \
    .load()


if (args.s == "1"):
    # Extracting important columns from stream to df
    parsedStreamDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select(
        col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
        col("data.properties.last_position.speed").alias("speed")
    )    

    # Filter out rows where 'speed' is null
    filteredStreamDF = parsedStreamDF.filter((col("speed").isNotNull()) & (col("speed") > 50) & (col("vehicle_id").isNotNull()))

    # Write the filtered data to the console
    filteredStreamDF.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .start()

    spark.streams.awaitAnyTermination()

# if (args.s == "1"):
#     # Parse the incoming Kafka JSON message
#     parsedStreamDF = kafkaStreamDF.select(from_json(col("value").cast("string"), schema).alias("data"))

#     featureStreamDF = parsedStreamDF.select(
#         col("data.properties").alias("properties")  # Alias properties to remove "data."
# )

#     # Flatten the schema and select only the vehicle ID and speed columns
#     flattenedDF = featureStreamDF.selectExpr(
#         "properties.trip.vehicle_registration_number as vehicle_id",  # Vehicle ID
#         "properties.last_position.speed as speed"  # Speed
#     )

#     filteredStreamDF = flattenedDF.filter((col("speed").isNotNull()) & (col("speed") > 50) & (col("vehicle_id").isNotNull()))

#     # Write the filtered data to the console
#     query = filteredStreamDF.writeStream \
#         .outputMode("append") \
#         .format("console") \
#         .option("truncate", "false") \
#         .trigger(processingTime='5 seconds') \
#         .start()

#     query.awaitTermination()








if (args.s == "2"):

    # Extract relevant fields from the Kafka message and parse the JSON
    parsedStreamDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select(
        col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
        col("data.properties.trip.gtfs.route_type").alias("vehicle_type"),
        col("data.properties.last_position.last_stop.id").alias("last_stop_id"),
        to_timestamp(col("data.properties.last_position.origin_timestamp")).alias("update_time")
    )

    # Vehicles with vehicle type 0 are trams
    filteredStreamDF = parsedStreamDF.filter((col("vehicle_id").isNotNull()) & (col("vehicle_type") == 0))

    latestTram = filteredStreamDF.groupBy("vehicle_id").agg(
        last("last_stop_id").alias("last_stop_id"),
        spark_max("update_time").alias("update_time")
    ).orderBy(col("vehicle_id").desc())

    latestTram.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

    spark.streams.awaitAnyTermination()











if (args.s == "3"):
    parsedStreamDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select(
        col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
        col("data.properties.last_position.speed").alias("speed"),
        to_timestamp(col("data.properties.last_position.origin_timestamp")).alias("update_time")
    )

    filteredStreamDF = parsedStreamDF.filter((col("vehicle_id").isNotNull()) & (col("speed").isNotNull()))

    aggregatedStreamDF = filteredStreamDF.groupBy("vehicle_id").agg(
            max(col("speed")).alias("max_speed"),
            max(col("update_time")).alias("update_time")) \
        .orderBy(col("max_speed").desc()) 

    aggregatedStreamDF.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 5) \
        .trigger(processingTime='5 seconds') \
        .start()

    spark.streams.awaitAnyTermination()







if (args.s == "4"):
    parsedStreamDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select(
        col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
        col("data.properties.last_position.speed").alias("speed"),
        to_timestamp(col("data.properties.last_position.origin_timestamp")).alias("update_time")
    )

    filteredStreamDF = parsedStreamDF.filter((col("vehicle_id").isNotNull()) & (col("speed").isNotNull()))

    aggregatedStreamDF = filteredStreamDF.groupBy("vehicle_id").agg(
        max(col("speed")).alias("max_speed"),
        max(col("update_time")).alias("update_time")
    ).orderBy(col("max_speed").desc())

    # For testing purposes there is fixed time
    if (args.m == "local"):
        timeThen = to_timestamp(lit("2024-12-20T14:04:20+01:00"))
        latestDataDF = aggregatedStreamDF.filter(col("update_time") > (timeThen - expr(f"INTERVAL 3 MINUTES")))
    else:
        latestDataDF = aggregatedStreamDF.filter(col("update_time") > (current_timestamp() - expr(f"INTERVAL 3 MINUTES")))

    latestDataDF.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 5) \
        .start()

    spark.streams.awaitAnyTermination()






if (args.s == "5"):
    parsedStreamDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select(
        col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
        col("data.properties.last_position.delay.last_stop_departure").alias("delay"),
        to_timestamp(col("data.properties.last_position.origin_timestamp")).alias("update_time")
    )

    filteredStreamDF = parsedStreamDF.filter((col("vehicle_id").isNotNull()) & (col("delay").isNotNull()))

    aggregatedStreamDF = filteredStreamDF.groupBy('vehicle_id').agg(
        expr("max_by(delay, update_time)").alias("delay"),
        max(col("update_time")).alias("update_time"),
    )

    # For testing purposes there is fixed time
    if (args.m == "local"):
        timeThen = to_timestamp(lit("2024-12-20T14:04:20+01:00")) # Fixed time allign with testing data
        latestDataDF = aggregatedStreamDF.filter(col("update_time") > (timeThen - expr(f"INTERVAL 3 MINUTES")))
    else:
        latestDataDF = aggregatedStreamDF.filter(col("update_time") > (current_timestamp() - expr(f"INTERVAL 3 MINUTES")))

    highestDelayDF = latestDataDF.orderBy(col("delay").desc()).limit(1)
    lowestDelayDF = latestDataDF.orderBy(col("delay").asc()).limit(1)

    # Add a column to identify the rows
    highestDelayDF = highestDelayDF.withColumn("type", lit("max_delay_last_3min"))
    lowestDelayDF = lowestDelayDF.withColumn("type", lit("min_delay_last_3min"))

    # Combine both rows into a single DataFrame
    highestLowestDelayDF = highestDelayDF.union(lowestDelayDF)

    # Write the combined DataFrame to the console
    highestLowestDelayDF.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    spark.streams.awaitAnyTermination()








if (args.s == "6"):
    parsedStreamDF = kafkaStreamDF.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select(
        col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
        col("data.properties.last_position.shape_dist_traveled").alias("dist_traveled"),
        to_timestamp(col("data.properties.last_position.origin_timestamp")).alias("update_time")
    )       

    filteredStreamDF = parsedStreamDF.filter(col("vehicle_id").isNotNull())

    aggregatedStreamDF = filteredStreamDF.groupBy("vehicle_id").agg(
        max(col("update_time")).alias("update_time"),
        expr("max_by(dist_traveled, update_time)").alias("dist_traveled")
    )

    latestTenDF = aggregatedStreamDF.orderBy(col("update_time").desc()).limit(10)

    highestDistTravelledDF = latestTenDF.orderBy(col("dist_traveled").desc()).limit(1)

    query = highestDistTravelledDF.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()