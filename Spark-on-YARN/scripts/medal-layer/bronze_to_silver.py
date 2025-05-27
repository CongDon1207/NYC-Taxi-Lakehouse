from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, unix_timestamp, expr,
    trim, initcap, hour, dayofmonth, dayofweek, month,
    create_map, lit
)
from itertools import chain
from functools import reduce
import pyspark.sql.functions as F


# Khởi tạo SparkSession với hỗ trợ Delta
spark = (SparkSession.builder
    .appName("Silver: Clean FHVHV")

    .getOrCreate())

bronze_trips = spark.read.format("delta") \
    .load("s3a://deltalake/bronze/nyc_taxi_trips")

zone_map = spark.read.format("delta") \
    .load("s3a://deltalake/bronze/zone_map")

# Ép kiểu cột pickup_datetime thành timestamp nếu chưa có
bronze_trips = bronze_trips.withColumn("pickup_datetime", to_timestamp("pickup_datetime"))


# Điền originating_base_num bằng dispatching_base_num nếu null
bronze_trips = bronze_trips.withColumn(
    "originating_base_num",
    when(col("originating_base_num").isNull(), col("dispatching_base_num"))
    .otherwise(col("originating_base_num"))
)

# Điền dispatching_base_num bằng originating_base_num nếu null
bronze_trips = bronze_trips.withColumn(
    "dispatching_base_num",
    when(col("dispatching_base_num").isNull(), col("originating_base_num"))
    .otherwise(col("dispatching_base_num"))
)

# Loại bỏ bản ghi nếu cả hai cột vẫn null (rất hiếm sau bước trên)
bronze_trips = bronze_trips.dropna(subset=["dispatching_base_num", "originating_base_num"])

bronze_trips = (bronze_trips
    .withColumn("request_datetime", to_timestamp("request_datetime"))
    .withColumn("dropoff_datetime", to_timestamp("dropoff_datetime"))
    .withColumn("on_scene_datetime", to_timestamp("on_scene_datetime"))
)



# Điền missing on_scene_datetime bằng pickup_datetime - 2.05 phút
bronze_trips = bronze_trips.withColumn(
    "on_scene_datetime",
    when(col("on_scene_datetime").isNull(),
         expr("pickup_datetime - interval 2 minutes - interval 3 seconds"))  # 2.05 phút = 2m3s
    .otherwise(col("on_scene_datetime"))
)

bronze_trips = (bronze_trips
    .withColumn("request_to_pickup",
        (unix_timestamp("pickup_datetime") - unix_timestamp("request_datetime")) / 60)
    .withColumn("pickup_to_dropoff",
        (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60)
    .withColumn("on_scene_to_pickup",
        (unix_timestamp("pickup_datetime") - unix_timestamp("on_scene_datetime")) / 60)
)


bronze_trips = bronze_trips.filter(
    (col("request_to_pickup") >= 0) & (col("on_scene_to_pickup") >= 0)
)


#Đặt ngưỡng
threshold_request = 60
threshold_dropoff = 120


bronze_trips = (bronze_trips
    .withColumn("outlier_request", col("request_to_pickup") > threshold_request)
    .withColumn("outlier_dropoff", col("pickup_to_dropoff") > threshold_dropoff)
)

# Xóa các bản ghi vượt ngưỡng
df_clean = bronze_trips.filter(~(col("outlier_request") | col("outlier_dropoff")))
df_clean = df_clean.drop("airport_fee", "wav_match_flag")



df_clean = df_clean.withColumn(
    "originating_base_num",
    when(trim(col("originating_base_num")) == "", col("dispatching_base_num"))
    .otherwise(col("originating_base_num"))
)	

# Loại các dòng trip_miles ≤ 0 hoặc trip_time ≤ 0
df_clean = df_clean.filter((col("trip_miles") > 0) & (col("trip_time") > 0))


def filter_iqr_outliers(df, columns, alpha=1.5, relative_error=0.01):
    def _filter_one(acc_df, col_name):
        q1, q3 = acc_df.approxQuantile(col_name, [0.25, 0.75], relative_error)
        iqr = q3 - q1
        lower = q1 - alpha * iqr
        upper = q3 + alpha * iqr
        return acc_df.filter(
            (F.col(col_name) >= lower) & (F.col(col_name) <= upper)
        )
    
    return reduce(_filter_one, columns, df)

iqr_cols = ["trip_miles", "trip_time"]
df_clean = filter_iqr_outliers(df_clean, iqr_cols)



# Tạo các đặc trưng mới từ DataFrame

# Tính thời gian chuyến đi bằng phút
df_clean = df_clean.withColumn("trip_duration_min", (col("trip_time") / 60).cast("double"))

# Trích xuất các đặc trưng thời gian từ pickup_datetime
df_clean = df_clean.withColumn("pickup_hour", hour(col("pickup_datetime"))) \
                   .withColumn("pickup_day", dayofmonth(col("pickup_datetime"))) \
                   .withColumn("pickup_weekday", dayofweek(col("pickup_datetime"))) \
                   .withColumn("pickup_month", month(col("pickup_datetime")))

# Tính tổng chi phí
df_clean = df_clean.withColumn("total_fare",
    (col("base_passenger_fare") + col("tolls") + col("bcf") +
     col("sales_tax") + col("congestion_surcharge") + col("tips")).cast("double")
)

# Tính tốc độ (dặm/giờ)
df_clean = df_clean.withColumn("speed_mph", (col("trip_miles") / (col("trip_time") / 3600)).cast("double"))

# Tính chi phí mỗi dặm (tránh chia 0)
df_clean = df_clean.withColumn("fare_per_mile",
    when(col("trip_miles") > 0, col("total_fare") / col("trip_miles")).otherwise(None).cast("double")
)

# Mapping mã hãng sang tên hãng
license_mapping = {
    'HV0002': 'Juno',
    'HV0003': 'Uber',
    'HV0004': 'Via',
    'HV0005': 'Lyft'
}

mapping_expr = create_map([lit(x) for x in chain(*license_mapping.items())])

df_clean = df_clean.withColumn("hvfhs_company", mapping_expr[col("hvfhs_license_num")])


# Phần zonemap
# Chuẩn hóa định dạng chữ

zone_map = zone_map \
    .withColumn("Borough", initcap(trim(col("Borough")))) \
    .withColumn("Zone", initcap(trim(col("Zone")))) \
    .withColumn("service_zone", initcap(trim(col("service_zone"))))

# Loại bỏ dòng thiếu mã LocationID (nếu có)
zone_map = zone_map.filter(col("LocationID").isNotNull())

# Merge bảng :
df_clean = df_clean.join(zone_map.selectExpr("LocationID as PULocationID", "Zone as PUZone", "Borough as PUBorough"),
                         on="PULocationID", how="left")

df_clean = df_clean.join(zone_map.selectExpr("LocationID as DOLocationID", "Zone as DOZone", "Borough as DOBorough"),
                         on="DOLocationID", how="left")

# Ghi vào Silver zone dưới dạng Delta

df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://deltalake/silver/fhvhv_main")

spark.stop()