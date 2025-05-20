from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, count, sum as _sum, avg
)
from pyspark.sql.utils import AnalysisException

# 1. Khởi tạo SparkSession với Delta support
spark = SparkSession.builder \
    .appName("SilverToGold_MLMetrics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. Đọc Silver layer
silver_path = "hdfs://quochuy-master:9000/deltalake/silver/nyc_taxi_enriched"
silver = spark.read.format("delta").load(silver_path)

# 3. Lọc null just in case
silver = silver.filter(col("pickup_ts").isNotNull() & col("trip_duration_s").isNotNull())

# 4. Tính toán các metric theo ngày
gold_daily = (
    silver
    .withColumn("date", date_format(col("pickup_ts"), "yyyy-MM-dd"))
    .groupBy("date")
    .agg(
        count("*").alias("total_trips"),
        _sum(col("trip_miles")).alias("sum_miles"),
        _sum(col("total_amount")).alias("sum_amount"),
        _sum(col("tips")).alias("sum_tips"),
        _sum(col("driver_pay")).alias("sum_driver_pay"),
        avg(col("trip_duration_s")).alias("avg_duration_s"),
        avg(col("speed_mph")).alias("avg_speed_mph"),
        avg(col("tip_ratio")).alias("avg_tip_ratio"),
        avg(col("trip_miles")).alias("avg_trip_miles")
    )
)

# 5. Ghi ra Gold layer
gold_path = "hdfs://quochuy-master:9000/deltalake/gold/daily_taxi_metrics"
gold_daily.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .option("overwriteSchema", "true") \
    .save(gold_path)

spark.stop()
