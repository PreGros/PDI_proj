from pyspark.sql import SparkSession

# Step 1: Create a SparkSession
spark = SparkSession.builder \
    .appName("Basic PySpark Example") \
    .getOrCreate()

# Step 2: Create a DataFrame
data = [
    ("Alice", 29),
    ("Bob", 35),
    ("Catherine", 23)
]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Step 3: Show the DataFrame
print("Original DataFrame:")
df.show()

# Step 4: Perform a transformation
# Add a new column "AgeGroup" based on the "Age"
from pyspark.sql.functions import when

df_transformed = df.withColumn(
    "AgeGroup",
    when(df["Age"] < 30, "Young").otherwise("Adult")
)

print("Transformed DataFrame:")
df_transformed.show()

# Step 5: Save the result to a CSV file
output_path = "output"
df_transformed.write.csv(output_path, header=True)

# Stop the SparkSession
spark.stop()
