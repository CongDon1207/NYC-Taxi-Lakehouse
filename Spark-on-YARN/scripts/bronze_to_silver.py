from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, unix_timestamp, expr,
    trim, initcap, hour, dayofmonth, dayofweek, month,
    create_map, lit
)
from itertools import chain


# ── 0. Khởi tạo SparkSession với hỗ trợ Delta ─────────────────────────
spark = (SparkSession.builder
    .appName("Silver: Clean FHVHV")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

bronze_trips = spark.read.format("delta") \
    .load("hdfs://quochuy-master:9000/deltalake/bronze/nyc_taxi_trips")

zone_map = spark.read.format("delta") \
    .load("hdfs://quochuy-master:9000/deltalake/bronze/zone_map")

# Ép kiểu cột pickup_datetime thành timestamp nếu chưa có
bronze_trips = bronze_trips.withColumn("pickup_datetime", to_timestamp("pickup_datetime"))

# Lọc từ 1/4 đến hết 15/4
filtered_trips = bronze_trips.filter(
    (col("pickup_datetime") >= "2019-04-01") &
    (col("pickup_datetime") <= "2019-04-15 23:59:59")
)

# In số bản ghi sau khi lọc
print("Số bản ghi sau khi lọc nửa tháng đầu:", filtered_trips.count())

# 1. Điền originating_base_num bằng dispatching_base_num nếu null
filtered_trips = filtered_trips.withColumn(
    "originating_base_num",
    when(col("originating_base_num").isNull(), col("dispatching_base_num"))
    .otherwise(col("originating_base_num"))
)

# 2. Điền dispatching_base_num bằng originating_base_num nếu null
filtered_trips = filtered_trips.withColumn(
    "dispatching_base_num",
    when(col("dispatching_base_num").isNull(), col("originating_base_num"))
    .otherwise(col("dispatching_base_num"))
)

# 3. Loại bỏ bản ghi nếu cả hai cột vẫn null (rất hiếm sau bước trên)
filtered_trips = filtered_trips.dropna(subset=["dispatching_base_num", "originating_base_num"])

filtered_trips = (filtered_trips
    .withColumn("request_datetime", to_timestamp("request_datetime"))
    .withColumn("dropoff_datetime", to_timestamp("dropoff_datetime"))
    .withColumn("on_scene_datetime", to_timestamp("on_scene_datetime"))
)

filtered_trips = (filtered_trips
    .withColumn("request_to_pickup",
        (unix_timestamp("pickup_datetime") - unix_timestamp("request_datetime")) / 60)
    .withColumn("pickup_to_dropoff",
        (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60)
    .withColumn("on_scene_to_pickup",
        (unix_timestamp("pickup_datetime") - unix_timestamp("on_scene_datetime")) / 60)
)

# Điền missing on_scene_datetime bằng pickup_datetime - 2.05 phút
filtered_trips = filtered_trips.withColumn(
    "on_scene_datetime",
    when(col("on_scene_datetime").isNull(),
         expr("pickup_datetime - interval 2 minutes - interval 3 seconds"))  # 2.05 phút = 2m3s
    .otherwise(col("on_scene_datetime"))
)


filtered_trips = filtered_trips.withColumn(
    "on_scene_to_pickup",
    (unix_timestamp("pickup_datetime") - unix_timestamp("on_scene_datetime")) / 60
)

filtered_trips = filtered_trips.filter(
    (col("request_to_pickup") >= 0) & (col("on_scene_to_pickup") >= 0)
)



# Tính Q1 và Q3 của request_to_pickup
quantiles_req = filtered_trips.approxQuantile("request_to_pickup", [0.25, 0.75], 0.01)
Q1_req, Q3_req = quantiles_req
IQR_req = Q3_req - Q1_req
lower_req = Q1_req - 1.5 * IQR_req
upper_req = Q3_req + 1.5 * IQR_req

# Tính Q1 và Q3 của pickup_to_dropoff
quantiles_drp = filtered_trips.approxQuantile("pickup_to_dropoff", [0.25, 0.75], 0.01)
Q1_drp, Q3_drp = quantiles_drp
IQR_drp = Q3_drp - Q1_drp
lower_drp = Q1_drp - 1.5 * IQR_drp
upper_drp = Q3_drp + 1.5 * IQR_drp

# Lọc outlier của request_to_pickup (chỉ phía trên)
outliers_request = filtered_trips.filter(
    col("request_to_pickup") > upper_req
)

# Lọc outlier của pickup_to_dropoff (chỉ phía trên)
outliers_dropoff = filtered_trips.filter(
    col("pickup_to_dropoff") > upper_drp
)


#Đặt ngưỡng
threshold_request = 60
threshold_dropoff = 120

# Đánh dấu bản ghi bất thường
from pyspark.sql.functions import when

filtered_trips = (filtered_trips
    .withColumn("outlier_request", col("request_to_pickup") > threshold_request)
    .withColumn("outlier_dropoff", col("pickup_to_dropoff") > threshold_dropoff)
)

# Xóa các bản ghi vượt ngưỡng
df_clean = filtered_trips.filter(~(col("outlier_request") | col("outlier_dropoff")))
df_clean = df_clean.drop("airport_fee", "wav_match_flag")


# Thay giá trị trống trong originating_base_num bằng dispatching_base_num
from pyspark.sql.functions import when, trim, col

df_clean = df_clean.withColumn(
    "originating_base_num",
    when(trim(col("originating_base_num")) == "", col("dispatching_base_num"))
    .otherwise(col("originating_base_num"))
)	

df_clean = df_clean.withColumn("request_datetime", to_timestamp(col("request_datetime"))) \
                   .withColumn("on_scene_datetime", to_timestamp(col("on_scene_datetime"))) \
                   .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"))) \
                   .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime")))


# Giả sử bạn đã xác định các cột cờ là:
# flag_cols = ['shared_request_flag', 'shared_match_flag', 'access_a_ride_flag', 'wav_request_flag']

df_clean = df_clean \
    .withColumn("hvfhs_license_num", col("hvfhs_license_num").cast("string")) \
    .withColumn("dispatching_base_num", col("dispatching_base_num").cast("string")) \
    .withColumn("originating_base_num", col("originating_base_num").cast("string")) \
    .withColumn("PULocationID", col("PULocationID").cast("int")) \
    .withColumn("DOLocationID", col("DOLocationID").cast("int")) \
    .withColumn("shared_request_flag", col("shared_request_flag").cast("string")) \
    .withColumn("shared_match_flag", col("shared_match_flag").cast("string")) \
    .withColumn("access_a_ride_flag", col("access_a_ride_flag").cast("string")) \
    .withColumn("wav_request_flag", col("wav_request_flag").cast("string"))

df_clean = df_clean \
    .withColumn("trip_miles", col("trip_miles").cast("double")) \
    .withColumn("trip_time", col("trip_time").cast("double")) \
    .withColumn("base_passenger_fare", col("base_passenger_fare").cast("double")) \
    .withColumn("tolls", col("tolls").cast("double")) \
    .withColumn("bcf", col("bcf").cast("double")) \
    .withColumn("sales_tax", col("sales_tax").cast("double")) \
    .withColumn("congestion_surcharge", col("congestion_surcharge").cast("double")) \
    .withColumn("tips", col("tips").cast("double")) \
    .withColumn("driver_pay", col("driver_pay").cast("double"))

# Loại các dòng trip_miles ≤ 0 hoặc trip_time ≤ 0
df_clean = df_clean.filter((col("trip_miles") > 0) & (col("trip_time") > 0))

# Tính IQR & loại outliers cho từng cột
quantiles_miles = df_clean.approxQuantile("trip_miles", [0.25, 0.75], 0.01)
Q1_miles, Q3_miles = quantiles_miles
IQR_miles = Q3_miles - Q1_miles
lower_miles = Q1_miles - 1.5 * IQR_miles
upper_miles = Q3_miles + 1.5 * IQR_miles

df_clean = df_clean.filter((col("trip_miles") >= lower_miles) & (col("trip_miles") <= upper_miles))

quantiles_time = df_clean.approxQuantile("trip_time", [0.25, 0.75], 0.01)
Q1_time, Q3_time = quantiles_time
IQR_time = Q3_time - Q1_time
lower_time = Q1_time - 1.5 * IQR_time
upper_time = Q3_time + 1.5 * IQR_time

df_clean = df_clean.filter((col("trip_time") >= lower_time) & (col("trip_time") <= upper_time))


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
from pyspark.sql.functions import trim, initcap

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
    .save("hdfs://quochuy-master:9000/deltalake/silver/fhvhv_main")

# ── Cuối cùng ─────────────────────────────────────────────────────────────
spark.stop()
