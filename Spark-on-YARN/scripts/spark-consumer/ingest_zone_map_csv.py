from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ZoneMapIngestionDelta") \
    .getOrCreate()

# Đường dẫn CSV local và nơi lưu Delta trên MinIO
local_csv_path = "s3a://data/taxi+_zone_lookup.csv"
s3a_delta_path = "s3a://deltalake/bronze/zone_map/"

# Đọc file CSV local
df_lookup = spark.read.csv(local_csv_path, header=True, inferSchema=True)

# Ghi xuống MinIO dưới định dạng Delta Lake
df_lookup.write.format("delta") \
    .mode("overwrite") \
    .save(s3a_delta_path)
