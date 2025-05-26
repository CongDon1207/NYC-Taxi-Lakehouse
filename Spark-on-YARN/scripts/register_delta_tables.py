from pyspark.sql import SparkSession

# Khởi tạo SparkSession với Hive support và Delta 
spark = (
    SparkSession.builder
    .appName("Register Delta Tables")
    .enableHiveSupport()
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

# Đảm bảo tồn tại các database 
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

#  Danh sách bảng cần đăng ký 
tables = [
    # Silver layer
    ("silver", "fhvhv_main", "s3a://deltalake/silver/fhvhv_main"),
    # Gold layer
    ("gold", "fhvhv_trips",        "s3a://deltalake/gold/fhvhv_trips"),
    ("gold", "fhvhv_zones",        "s3a://deltalake/gold/fhvhv_zones"),
    ("gold", "fhvhv_zone_stats",   "s3a://deltalake/gold/fhvhv_zone_stats"),
    ("gold", "fhvhv_time_stats",   "s3a://deltalake/gold/fhvhv_time_stats")
]

# Tạo bảng nếu chưa có
for db, tbl, path in tables:
    full_name = f"{db}.{tbl}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name}
        USING DELTA
        LOCATION '{path}'
    """.strip())
    print(f"Registered table {full_name} -> {path}")

#  Kiểm tra danh sách bảng đã đăng ký
print("\nDanh sách tables in metastore:")
spark.sql("SHOW TABLES IN silver").show(truncate=False)
spark.sql("SHOW TABLES IN gold").show(truncate=False)

spark.stop()
