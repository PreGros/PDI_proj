from pyspark.sql.functions import explode, col
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .getOrCreate()

# Load the JSON file
df = spark.read.option("multiLine", "true").json("test3.json")

# Exploding the 'features' array to get each item as a separate row
exploded_df = df.select(explode(col("features")).alias("feature"))

# Extract 'coordinates' and 'type' from the exploded 'feature' column
final_df = exploded_df.select(
    col("feature.geometry.coordinates").alias("coordinates"),
    col("feature.geometry.type").alias("type"),
    col("feature.properties.last_position.speed").alias("speed")
)

# Filter rows where 'speed' is not null
non_null_speed_df = final_df.filter(col("speed").isNotNull())

# Show the resulting DataFrame
non_null_speed_df.show(truncate=False)

