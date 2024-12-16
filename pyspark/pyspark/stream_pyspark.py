from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Define the Kafka source
kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "api_data"  # Replace with the actual topic name

# Read data from Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("fetch.message.max.bytes", "20971520") \
    .option("max.partition.fetch.bytes", "20971520") \
    .load()

# The Kafka data is in binary format (key, value), we need to decode it
decoded_df = kafka_stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Show the data
query = decoded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Await termination to keep the stream alive
query.awaitTermination()
