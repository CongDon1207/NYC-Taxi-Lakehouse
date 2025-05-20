from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_format,
    unix_timestamp, when, lit,
    hour, dayofweek, month, year
)
from delta.tables import DeltaTable

# 1. Khởi tạo SparkSession với Delta support
spark = SparkSession.builder \
    .appName("BronzeToSilver_MLReady") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. Đọc data Bronze và lookup zone
bronze_trips = spark.read.format("delta") \
    .load("hdfs://quochuy-master:9000/deltalake/bronze/nyc_taxi_trips")
zone_map = spark.read.format("delta") \
    .load("hdfs://quochuy-master:9000/deltalake/bronze/zone_map")

# 3. Parse các timestamp và loại bỏ record không hợp lệ
df = bronze_trips \
    .withColumn("pickup_ts",  to_timestamp("pickup_datetime")) \
    .withColumn("dropoff_ts", to_timestamp("dropoff_datetime")) \
    .filter(col("pickup_ts").isNotNull() & col("dropoff_ts").isNotNull()) \
    .filter(col("trip_miles") > 0) \
    .filter(col("trip_time") > 0)

# 4. Tính thêm các feature phụ trợ
df = df \
    .withColumn("trip_duration_s", unix_timestamp("dropoff_ts") - unix_timestamp("pickup_ts")) \
    .withColumn("trip_duration_h", col("trip_duration_s")/3600) \
    .withColumn("speed_mph", col("trip_miles")/col("trip_duration_h")) \
    .filter(col("speed_mph") < 100)  # loại trip speed bất thường

# 5. Tính tổng thu nhập và tỉ lệ tip
df = df.withColumn(
        "total_amount",
        col("base_passenger_fare")
        + col("tolls")
        + col("bcf")
        + col("sales_tax")
        + col("congestion_surcharge")
        + col("airport_fee")
        + col("tips")
    ) \
    .withColumn("tip_ratio", when(col("total_amount")>0, col("tips")/col("total_amount")).otherwise(lit(0)))

# 6. Sinh các cột thời gian tiện phân tích
df = df \
    .withColumn("pickup_date",   date_format("pickup_ts","yyyy-MM-dd")) \
    .withColumn("pickup_year",   year("pickup_ts")) \
    .withColumn("pickup_month",  month("pickup_ts")) \
    .withColumn("pickup_dow",    dayofweek("pickup_ts")) \
    .withColumn("pickup_hour",   hour("pickup_ts"))

# 7. Enrich với zone_map
df = df.alias("t").join(
        zone_map.alias("z"),
        col("t.PULocationID") == col("z.LocationID"),
        "left"
    ).select(
        col("t.*"),
        col("z.Zone").alias("pickup_zone"),
        col("z.Borough").alias("pickup_borough")
    )

# 8. Loại bản ghi trùng
df = df.dropDuplicates(["hvfhs_license_num","pickup_ts"])

# 9. Ghi ra Silver layer, partition theo năm/tháng/ngày
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("pickup_year","pickup_month","pickup_date") \
    .option("overwriteSchema","true") \
    .save("hdfs://quochuy-master:9000/deltalake/silver/nyc_taxi_enriched") 

spark.stop()
