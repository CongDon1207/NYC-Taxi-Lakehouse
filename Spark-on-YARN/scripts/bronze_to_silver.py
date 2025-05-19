from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# Khởi tạo SparkSession với Delta Lake support
spark = SparkSession.builder \
    .appName("BronzeToSilver_Simple") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

# Đường dẫn output
output_path = "hdfs://quochuy-master:9000/deltalake/silver/nyc_taxi_cleaned"

# 1. Đọc dữ liệu Bronze
bronze_trips = spark.read.format("delta") \
    .load("hdfs://quochuy-master:9000/deltalake/bronze/nyc_taxi_trips")

# 2. Làm sạch & chuẩn hoá
trips_clean = bronze_trips \
    .withColumn("pickup_datetime",  to_timestamp(col("tpep_pickup_datetime"))) \
    .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))) \
    .withColumn("passenger_count", col("passenger_count").cast("int")) \
    .withColumn("trip_distance",    col("trip_distance").cast("double")) \
    .filter(col("trip_distance") > 0) \
    .dropDuplicates(["ride_id"]) \
    .withColumn("pickup_date", date_format(col("pickup_datetime"), "yyyy-MM-dd"))

# 3. Ghi ra Silver layer, có bắt exception để phát hiện lỗi
try:
    trips_clean.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("pickup_date") \
        .option("overwriteSchema", "true") \
        .save(output_path)
    print("✅ Viết Silver thành công")
except Exception as e:
    print("❌ Viết Silver thất bại:", e)
    spark.stop()
    raise

# 4. (Tuỳ chọn) Đọc lại để verify số bản ghi
df_verify = spark.read.format("delta").load(output_path)
count_written  = df_verify.count()
count_expected = trips_clean.count()
if count_written == count_expected:
    print(f"✅ Verification: wrote {count_written} rows (khớp với expected)")
else:
    print(f"⚠️ Verification mismatch: wrote {count_written} rows, expected {count_expected}")

# 5. (Tuỳ chọn) Kiểm tra history của Delta table
if DeltaTable.isDeltaTable(spark, output_path):
    dt = DeltaTable.forPath(spark, output_path)
    print("=== Last Commit History ===")
    dt.history(1).show(truncate=False)
else:
    print("❌ Không phải Delta table tại:", output_path)

# Kết thúc
spark.stop()
