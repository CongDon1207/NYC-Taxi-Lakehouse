from pyspark.sql import SparkSession

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("ZoneMapIngestionDelta") \
    .master("yarn") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()

local_csv_path = "/data/taxi+_zone_lookup.csv"

hdfs_zone_path = "hdfs://quochuy-master:9000/deltalake/bronze/zone_map/"

# Đọc và ghi file vào HDFS
df_lookup = spark.read.csv(local_csv_path, header=True, inferSchema=True)

df_lookup.write.mode("overwrite") \
    .option("header", "true") \
    .csv(hdfs_zone_path)

print("Đã ingest file zone map CSV vào HDFS.")
