from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# Kafka schema
schema = StructType() \
    .add("id", IntegerType()) \
    .add("message", StringType())

spark = SparkSession.builder.appName("KafkaToDeltaMinIO").getOrCreate()

# Read stream from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "miniotopic") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

output_bucket = "s3a://mybucket/test"
checkpoint_location = "s3a://mybucket/checkpoints/test"

query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location) \
    .partitionBy("id") \
    .start(output_bucket)

query.awaitTermination()
