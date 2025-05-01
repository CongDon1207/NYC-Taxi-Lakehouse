from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, BooleanType

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("BinanceKafkaIngestion") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()

# Định nghĩa schema dữ liệu từ Binance
schema = StructType() \
    .add("e", StringType()) \
    .add("E", LongType()) \
    .add("s", StringType()) \
    .add("t", LongType()) \
    .add("p", StringType()) \
    .add("q", StringType()) \
    .add("T", LongType()) \
    .add("m", BooleanType()) \
    .add("M", BooleanType())

# Đọc dữ liệu từ Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "binance_trades") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON payload từ Kafka
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("parsed_json")) \
    .select(
        col("parsed_json.e").alias("event_type"),
        col("parsed_json.E").alias("event_time"),
        col("parsed_json.s").alias("symbol"),
        col("parsed_json.t").alias("trade_id"),
        col("parsed_json.p").cast(DoubleType()).alias("price"),
        col("parsed_json.q").cast(DoubleType()).alias("quantity"),
        col("parsed_json.T").alias("trade_time"),
        col("parsed_json.m").alias("is_buyer_maker")
    )



# Ghi ra HDFS theo định dạng Parquet
query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "hdfs://tanhung-master:9000/lakehouse/binance_trades/") \
    .option("checkpointLocation", "hdfs://tanhung-master:9000/lakehouse/checkpoints/binance_trades/") \
    .outputMode("append") \
    .start()

# Đợi query chạy liên tục
query.awaitTermination()
