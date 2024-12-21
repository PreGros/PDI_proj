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

# spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

spark.sparkContext.setLogLevel("ERROR")

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

# Define the Kafka source
kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "api_data"  # Replace with the actual topic name

if (args.m == "api"):
    startingOffset = "latest"
else:
    startingOffset = "earliest"

# Read data from Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", startingOffset) \
    .load()

# Parse the incoming Kafka JSON message
parsed_df = kafka_stream_df.select(from_json(col("value").cast("string"), schema).alias("data"))

feature_stream_df = parsed_df.select(
    col("data.properties").alias("properties")  # Alias properties to remove "data."
)



if (args.s == "1"):
    # Flatten the schema and select only the vehicle ID and speed columns
    flattened_df = feature_stream_df.selectExpr(
        "properties.trip.vehicle_registration_number as vehicle_id",  # Vehicle ID
        "properties.last_position.speed as speed"  # Speed
    )

    filtered_stream = flattened_df.filter(col("vehicle_id").isNotNull())

    # Filter out rows where 'speed' is null
    filtered_df = filtered_stream.filter((col("speed").isNotNull()) & (col("speed") > 50))

    # Write the filtered data to the console
    query = filtered_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .start()

    query.awaitTermination()










if (args.s == "2"):

    # Extract relevant fields from the Kafka message and parse the JSON
    parsed_stream = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select(
        col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
        col("data.properties.trip.gtfs.route_type").alias("vehicle_type"),
        col("data.properties.last_position.last_stop.id").alias("last_stop_id"),
        to_timestamp(col("data.properties.last_position.origin_timestamp")).alias("update_time")
    )

    filtered_stream = parsed_stream.filter(col("vehicle_id").isNotNull())

    df_trams = filtered_stream.filter(col("vehicle_type") == 0)

    latest_tram_data = df_trams.groupBy("vehicle_id").agg(
        last("last_stop_id").alias("last_stop_id"),
        spark_max("update_time").alias("update_time")
    ).orderBy(col("vehicle_id").desc())

    latest_tram_data.writeStream.outputMode("complete").format("console").option("truncate", False).start()

    spark.streams.awaitAnyTermination()











if (args.s == "3"):
    parsed_stream = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select(
        col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
        col("data.properties.last_position.speed").alias("speed"),
        to_timestamp(col("data.properties.last_position.origin_timestamp")).alias("update_time")
    )

    filtered_stream = parsed_stream.filter(col("vehicle_id").isNotNull())

    filtered_stream = filtered_stream.filter(col("speed").isNotNull())

    aggregated_stream = filtered_stream.groupBy("vehicle_id").agg(
            max(col("speed")).alias("max_speed"),
            max(col("update_time")).alias("update_time")) \
        .orderBy(col("max_speed").desc()) 

    aggregated_stream.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 5) \
        .trigger(processingTime='5 seconds') \
        .start()

    spark.streams.awaitAnyTermination()

# if (args.s == "4"):
#     parsed_stream = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_data") \
#     .select(from_json("json_data", schema).alias("data")) \
#     .select(
#         col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
#         col("data.properties.last_position.speed").alias("speed"),
#         to_timestamp(col("data.properties.last_position.origin_timestamp")).alias("update_time")
#     )

#     # Filter out null speeds and apply a time-based filter
#     filtered_stream = parsed_stream.filter(col("speed").isNotNull())
#     filtered_stream = filtered_stream.filter(unix_timestamp(col("update_time")) > (unix_timestamp(current_timestamp()) - 30))

#     # Aggregation: Group by vehicle_id and calculate the max speed within a window of time
#     aggregated_stream = filtered_stream \
#         .withWatermark("update_time", "3 minutes")  # Ensure we consider late data within a 30-second window

#     # Aggregation: Group by vehicle_id and get the max speed
#     aggregated_stream = aggregated_stream \
#         .groupBy("vehicle_id") \
#         .agg(F.max("speed").alias("max_speed"),
#             F.max("update_time").alias("latest_update_time")) \
#         .orderBy(F.col("max_speed").desc(), F.col("latest_update_time"))

#     # Write the stream to console
#     aggregated_stream.writeStream \
#         .outputMode("complete") \
#         .format("console") \
#         .option("truncate", False) \
#         .option("numRows", 5) \
#         .start()

#     spark.streams.awaitAnyTermination()







if (args.s == "4"):
    parsed_stream = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select(
        col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
        col("data.properties.last_position.speed").alias("speed"),
        to_timestamp(col("data.properties.last_position.origin_timestamp")).alias("update_time")
    )

    parsed_stream = parsed_stream.filter(col("vehicle_id").isNotNull())

    parsed_stream = parsed_stream.filter(col("speed").isNotNull())

    aggregated_stream = parsed_stream.groupBy("vehicle_id").agg(
        max(col("speed")).alias("max_speed"),
        max(col("update_time")).alias("update_time")
    ).orderBy(col("max_speed").desc())

    # For testing purposes
    if (args.m == "local"):
        timeThen = to_timestamp(lit("2024-12-20T14:04:20+01:00"))
        streamP = aggregated_stream.filter(col("update_time") > (timeThen - expr(f"INTERVAL 3 MINUTES")))
    else:
        streamP = aggregated_stream.filter(col("update_time") > (current_timestamp() - expr(f"INTERVAL 3 MINUTES")))

    # result_stream = parsed_stream.withColumn(
    #     "time_then",
    #     timeThen
    # )

    streamP.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 5) \
        .start()

    spark.streams.awaitAnyTermination()




    # filtered_stream = parsed_stream.filter(col("speed").isNotNull())

    # # debug_stream = parsed_stream.withColumn(
    # # "time_difference_in_minutes",
    # # (unix_timestamp(current_timestamp()) - unix_timestamp(col("update_time"))) / 60)

    # filtered_stream_time = filtered_stream.filter(
    #     unix_timestamp(col("update_time")) > (unix_timestamp(current_timestamp()) - 180)) # 180 = 3min

    # # Perform aggregation: Find the maximum speed and latest update_time for each vehicle
    # aggregated_stream = filtered_stream_time.groupBy("vehicle_id") \
    #     .agg(
    #         max(col("speed")).alias("max_speed"),
    #         max(col("update_time")).alias("latest_update_time")  # Get the most recent update_time
    #     ) \
    #     .orderBy(col("max_speed").desc())  # Sort by max_speed descending

    # # Write aggregated results to the console
    # aggregated_stream.writeStream \
    #     .outputMode("complete") \
    #     .format("console") \
    #     .option("truncate", False) \
    #     .option("numRows", 5) \
    #     .start()

    # spark.streams.awaitAnyTermination()







if (args.s == "5"):
    parsed_stream = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select(
        col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
        col("data.properties.last_position.delay.last_stop_departure").alias("delay"),
        to_timestamp(col("data.properties.last_position.origin_timestamp")).alias("update_time")
    )

    filtered_stream = parsed_stream.filter(col("vehicle_id").isNotNull())

    filtered_stream = filtered_stream.filter(col("delay").isNotNull())

    aggregated_stream = filtered_stream.groupBy('vehicle_id').agg(
        expr("max_by(delay, update_time)").alias("delay"),
        max(col("update_time")).alias("update_time"),
    )

    # streamP = aggregated_stream.filter(col("update_time") > (current_timestamp() - expr(f"INTERVAL 3 MINUTES")))

    if (args.m == "local"):
        timeThen = to_timestamp(lit("2024-12-20T14:04:20+01:00"))
        streamP = aggregated_stream.filter(col("update_time") > (timeThen - expr(f"INTERVAL 3 MINUTES")))
    else:
        streamP = aggregated_stream.filter(col("update_time") > (current_timestamp() - expr(f"INTERVAL 3 MINUTES")))

    highest_delay_row = streamP.orderBy(col("delay").desc()).limit(1)
    lowest_delay_row = streamP.orderBy(col("delay").asc()).limit(1)

    # Add a column to identify the rows
    highest_delay_row = highest_delay_row.withColumn("type", lit("max_delay_last_3min"))
    lowest_delay_row = lowest_delay_row.withColumn("type", lit("min_delay_last_3min"))

    # Combine both rows into a single DataFrame
    combined_stream = highest_delay_row.union(lowest_delay_row)

    # Write the combined DataFrame to the console
    combined_stream.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    spark.streams.awaitAnyTermination()








if (args.s == "6"):
    parsed_stream = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", schema).alias("data")) \
    .select(
        col("data.properties.trip.vehicle_registration_number").alias("vehicle_id"),
        col("data.properties.last_position.shape_dist_traveled").alias("dist_traveled"),
        to_timestamp(col("data.properties.last_position.origin_timestamp")).alias("update_time")
    )       

    filtered_stream = parsed_stream.filter(col("vehicle_id").isNotNull())

    aggregated_stream = filtered_stream.groupBy("vehicle_id").agg(
        max(col("update_time")).alias("update_time"),
        expr("max_by(dist_traveled, update_time)").alias("dist_traveled")
    )

    ordered_stream = aggregated_stream.orderBy(col("update_time").desc()).limit(10)

    highest_dist_row = ordered_stream.orderBy(col("dist_traveled").desc()).limit(1)

    query = highest_dist_row.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()