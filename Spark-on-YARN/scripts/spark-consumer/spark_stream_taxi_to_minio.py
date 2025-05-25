from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from delta.tables import *

# Tạo Spark session
spark = SparkSession.builder \
    .appName("TaxiKafkaIngestionDelta") \
    .getOrCreate()

# Schema cho dữ liệu taxi
trip_schema = StructType([
    StructField("hvfhs_license_num", StringType(), True),
    StructField("dispatching_base_num", StringType(), True),
    StructField("originating_base_num", StringType(), True),
    StructField("request_datetime", StringType(), True),
    StructField("on_scene_datetime", StringType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("trip_miles", DoubleType(), True),
    StructField("trip_time", DoubleType(), True),
    StructField("base_passenger_fare", DoubleType(), True),
    StructField("tolls", DoubleType(), True),
    StructField("bcf", DoubleType(), True),
    StructField("sales_tax", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("tips", DoubleType(), True),
    StructField("driver_pay", DoubleType(), True),
    StructField("shared_request_flag", StringType(), True),
    StructField("shared_match_flag", StringType(), True),
    StructField("access_a_ride_flag", StringType(), True),
    StructField("wav_request_flag", StringType(), True),
    StructField("wav_match_flag", StringType(), True)
])

# Đọc từ Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "nyc_taxi_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON từ Kafka value
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), trip_schema).alias("trip")) \
    .select("trip.*")

# Đường dẫn trên MinIO
output_path = "s3a://deltalake/bronze/nyc_taxi_trips/"
checkpoint_path = "s3a://deltalake/checkpoints/nyc_taxi_delta/"

# Ghi xuống MinIO (Delta format)
query = df_parsed.writeStream \
    .format("delta") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start()

query.awaitTermination()
