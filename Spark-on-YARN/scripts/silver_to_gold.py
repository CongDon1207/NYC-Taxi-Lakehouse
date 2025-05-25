from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (
    col, trim, initcap, monotonically_increasing_id
)

#  fhvhv_trips ///////////////////////////////////////////////

spark = (SparkSession.builder
    .appName("Gold–Step1: fhvhv_trips")
    .getOrCreate())

# 1. Đọc từ Silver main
df_trips = (spark.read
    .format("delta")
    .load("s3a://deltalake/silver/fhvhv_main")
)

# 2. Chọn các cột fact
cols = [
    "hvfhs_license_num", "hvfhs_company",
    "pickup_datetime", "dropoff_datetime",
    "pickup_hour", "pickup_day", "pickup_weekday",
    "PULocationID", "PUZone", "PUBorough",
    "DOLocationID", "DOZone", "DOBorough",
    "trip_miles", "trip_time", "trip_duration_min",
    "total_fare", "fare_per_mile", "speed_mph",
    "base_passenger_fare", "tolls", "bcf",
    "sales_tax", "congestion_surcharge",
    "tips", "driver_pay",
    "shared_request_flag", "shared_match_flag",
    "wav_request_flag", "access_a_ride_flag"
]
df_trips = df_trips.select(*cols)

# 3. Ghi ra Gold
df_trips.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://deltalake/gold/fhvhv_trips")




# fhvhv_zones /////////////////////////////////////////
# Bước 2: Dimension fhvhv_zones, dùng lại spark đã khởi tạo
df_zone_map = (spark.read
    .format("delta")
    .load("s3a://deltalake/bronze/zone_map")
)

df_zones = (df_zone_map
    .select(
        col("LocationID"),
        initcap(trim(col("Borough"))).alias("Borough"),
        initcap(trim(col("Zone"))).alias("Zone"),
        initcap(trim(col("service_zone"))).alias("service_zone")
    )
    .dropDuplicates(["LocationID"])
)

df_zones.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://deltalake/gold/fhvhv_zones")





# Bảng fhvhv_zone_stats ////////////////////////////////////////////
# B1. pickup_count, total_fare_pu, avg_fare_pu
pickup_stats = (
  df_trips.groupBy("PULocationID")
    .agg(
      F.count("*").alias("pickup_count"),
      F.sum("total_fare").alias("total_fare_pu"),
      F.avg("fare_per_mile").alias("avg_fare_per_mile_pu")
    )
    .withColumnRenamed("PULocationID", "LocationID")
)

# B2. dropoff_count, total_fare_do, avg_fare_do
dropoff_stats = (
  df_trips.groupBy("DOLocationID")
    .agg(
      F.count("*").alias("dropoff_count"),
      F.sum("total_fare").alias("total_fare_do"),
      F.avg("fare_per_mile").alias("avg_fare_per_mile_do")
    )
    .withColumnRenamed("DOLocationID", "LocationID")
)

# Thay bằng cách giống trên, tách 2 groupBy rồi cộng lại:
wav_pu = (
  df_trips.filter((F.col("wav_request_flag")=="Y")|(F.col("access_a_ride_flag")=="Y"))
    .groupBy("PULocationID")
    .count()
    .withColumnRenamed("PULocationID","LocationID")
    .withColumnRenamed("count", "wav_pu_count")
)
wav_do = (
  df_trips.filter((F.col("wav_request_flag")=="Y")|(F.col("access_a_ride_flag")=="Y"))
    .groupBy("DOLocationID")
    .count()
    .withColumnRenamed("DOLocationID","LocationID")
    .withColumnRenamed("count", "wav_do_count")
)

wav_stats = (
  wav_pu.join(wav_do, on="LocationID", how="full")
        .na.fill(0, subset=["wav_pu_count","wav_do_count"])
        .withColumn("wav_trip_count", F.col("wav_pu_count") + F.col("wav_do_count"))
        .select("LocationID","wav_trip_count")
)

# B4. Shared trips count
shared_pu = (
  df_trips.filter((F.col("shared_match_flag")=="Y")|(F.col("shared_request_flag")=="Y"))
    .groupBy("PULocationID")
    .count()
    .withColumnRenamed("PULocationID","LocationID")
    .withColumnRenamed("count","shared_pu_count")
)
shared_do = (
  df_trips.filter((F.col("shared_match_flag")=="Y")|(F.col("shared_request_flag")=="Y"))
    .groupBy("DOLocationID")
    .count()
    .withColumnRenamed("DOLocationID","LocationID")
    .withColumnRenamed("count","shared_do_count")
)
shared_stats = (
  shared_pu.join(shared_do, on="LocationID", how="full")
           .na.fill(0, ["shared_pu_count","shared_do_count"])
           .withColumn("shared_trip_count", F.col("shared_pu_count") + F.col("shared_do_count"))
           .select("LocationID","shared_trip_count")
)

# C. Kết hợp tất cả về lookup zone
zone_aggregation = (
  df_zones.alias("z")
    .join(pickup_stats, "LocationID", "left")
    .join(dropoff_stats, "LocationID", "left")
    .join(wav_stats, "LocationID", "left")
    .join(shared_stats, "LocationID", "left")
    .na.fill(0, 
             subset=[
               "pickup_count","dropoff_count",
               "total_fare_pu","total_fare_do",
               "avg_fare_per_mile_pu","avg_fare_per_mile_do",
               "wav_trip_count","shared_trip_count"
             ])
    .withColumn("total_fare_sum", F.col("total_fare_pu")+F.col("total_fare_do"))
    .withColumn(
       "avg_fare_per_mile", 
       (F.col("avg_fare_per_mile_pu")+F.col("avg_fare_per_mile_do"))/2
    )
    .select(
      "LocationID","Zone","Borough",
      "pickup_count","dropoff_count",
      "total_fare_sum","avg_fare_per_mile",
      "wav_trip_count","shared_trip_count"
    )
)

# D. Ghi ra Delta
zone_aggregation.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema","true") \
  .save("s3a://deltalake/gold/fhvhv_zone_stats")


# fhvhv_time_stats //////////////////////////////////

# 1. Tính các chỉ số nhóm theo tổ hợp các cột
time_stats = (df_trips
    .groupBy(
        "pickup_hour",
        "pickup_day",
        "pickup_weekday",
        "hvfhs_company",
        "PUBorough"
    )
    .agg(
        F.count("*").alias("trip_count"),
        F.avg("trip_duration_min").alias("avg_trip_duration_min"),
        F.avg("total_fare").alias("avg_total_fare"),
        F.avg("speed_mph").alias("avg_speed_mph")
    )
)

# 2. Thêm surrogate key aggregation_id
time_stats = time_stats.withColumn("aggregation_id", monotonically_increasing_id())

# 3. Chọn lại thứ tự cột cho đúng schema
time_stats = time_stats.select(
    "aggregation_id",
    "pickup_hour",
    "pickup_day",
    "pickup_weekday",
    "hvfhs_company",
    "PUBorough",
    "trip_count",
    "avg_trip_duration_min",
    "avg_total_fare",
    "avg_speed_mph"
)

# 4. Ghi ra Delta Gold Layer
time_stats.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://deltalake/gold/fhvhv_time_stats")

spark.stop()
