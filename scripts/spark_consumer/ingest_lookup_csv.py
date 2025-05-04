from pyspark.sql import SparkSession

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("IngestLookupCSV") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Đường dẫn file CSV
local_csv_path = "file:///opt/bitnami/spark/data/taxi+_zone_lookup.csv"

# Đường dẫn HDFS
hdfs_lookup_path = "hdfs://tanhung-master:9000/lakehouse/bronze/lookup/"

# Đọc và ghi file vào HDFS
df_lookup = spark.read.csv(local_csv_path, header=True, inferSchema=True)

df_lookup.write.mode("overwrite") \
    .option("header", "true") \
    .csv(hdfs_lookup_path)

print("Đã ingest file lookup CSV vào HDFS.")
