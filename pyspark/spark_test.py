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


# {
#   "features": [
#         "geometry": {
#             "coordinates": [
#             14.57476,
#             50.1061
#             ],
#             "type": "Point"
#         },
#         "properties": {
#             "last_position": {
#             "bearing": 316,
#             "delay": {
#                 "actual": 25,
#                 "last_stop_arrival": -12,
#                 "last_stop_departure": 25
#             },
#             "is_canceled": null,
#             "last_stop": {
#                 "arrival_time": "2024-12-12T12:47:00+01:00",
#                 "departure_time": "2024-12-12T12:47:00+01:00",
#                 "id": "U1129Z1P",
#                 "sequence": 10
#             },
#             "next_stop": {
#                 "arrival_time": "2024-12-12T12:49:00+01:00",
#                 "departure_time": "2024-12-12T12:49:00+01:00",
#                 "id": "U897Z1P",
#                 "sequence": 11
#             },
#             "origin_timestamp": "2024-12-12T12:47:25+01:00",
#             "shape_dist_traveled": "4.585142",
#             "speed": null,
#             "state_position": "on_track",
#             "tracking": true
#             },
#             "trip": {
#             "agency_name": {
#                 "real": "DP PRAHA",
#                 "scheduled": "DP PRAHA"
#             },
#             "cis": {
#                 "line_id": null,
#                 "trip_number": null
#             },
#             "gtfs": {
#                 "route_id": "L141",
#                 "route_short_name": "141",
#                 "route_type": 3,
#                 "trip_headsign": "Ve Žlíbku",
#                 "trip_id": "141_11_240902",
#                 "trip_short_name": null
#             },
#             "origin_route_name": "141",
#             "sequence_id": 1,
#             "start_timestamp": "2024-12-12T12:34:00+01:00",
#             "vehicle_registration_number": 4113,
#             "vehicle_type": {
#                 "description_cs": "autobus",
#                 "description_en": "bus",
#                 "id": 3
#             },
#             "wheelchair_accessible": true,
#             "air_conditioned": true,
#             "usb_chargers": false
#             }
#         },
#         "type": "Feature"
#     },
#     {
#         "geometry": {
#             "coordinates": [
#             14.28862,
#             50.09922
#             ],
#             "type": "Point"
#         },
#         "properties": {
#             "last_position": {
#             "bearing": 319,
#             "delay": {
#                 "actual": 42,
#                 "last_stop_arrival": 11,
#                 "last_stop_departure": 42
#             },
#             "is_canceled": null,
#             "last_stop": {
#                 "arrival_time": "2024-12-12T12:46:00+01:00",
#                 "departure_time": "2024-12-12T12:46:00+01:00",
#                 "id": "U698Z1P",
#                 "sequence": 6
#             },
#             "next_stop": {
#                 "arrival_time": "2024-12-12T12:49:00+01:00",
#                 "departure_time": "2024-12-12T12:49:00+01:00",
#                 "id": "U776Z1P",
#                 "sequence": 7
#             },
#             "origin_timestamp": "2024-12-12T12:46:42+01:00",
#             "shape_dist_traveled": "4.589042",
#             "speed": null,
#             "state_position": "on_track",
#             "tracking": true
#             },
#             "trip": {
#             "agency_name": {
#                 "real": "DP PRAHA",
#                 "scheduled": "DP PRAHA"
#             },
#             "cis": {
#                 "line_id": null,
#                 "trip_number": null
#             },
#             "gtfs": {
#                 "route_id": "L59",
#                 "route_short_name": "59",
#                 "route_type": 11,
#                 "trip_headsign": "Letiště / Airport ✈",
#                 "trip_id": "59_1091_241202",
#                 "trip_short_name": null
#             },
#             "origin_route_name": "59",
#             "sequence_id": 3,
#             "start_timestamp": "2024-12-12T12:36:00+01:00",
#             "vehicle_registration_number": 418,
#             "vehicle_type": {
#                 "description_cs": "trolejbus",
#                 "description_en": "trolleybus",
#                 "id": 18
#             },
#             "wheelchair_accessible": true,
#             "air_conditioned": true,
#             "usb_chargers": false
#             }
#         },
#         "type": "Feature"
#     },
#   ],
#   "type": "FeatureCollection"
# }
