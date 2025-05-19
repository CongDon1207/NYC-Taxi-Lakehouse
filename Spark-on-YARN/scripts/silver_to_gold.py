from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, count, sum as _sum, avg

spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. Đọc Silver data
silver = spark.read.format("delta") \
    .load("hdfs://quochuy-master:9000/deltalake/silver/nyc_taxi_enriched")

# 2. Tính metrics hàng ngày
gold_daily = silver.groupBy(date_format(col("pickup_datetime"), "yyyy-MM-dd").alias("date")) \
    .agg(
        count("*").alias("total_trips"),
        _sum(col("fare_amount")).alias("total_revenue"),
        avg(col("trip_distance")).alias("avg_distance")
    )

# 3. Ghi ra Gold layer
gold_daily.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .option("overwriteSchema","true") \
    .save("hdfs://quochuy-master:9000/deltalake/gold/daily_taxi_metrics")
