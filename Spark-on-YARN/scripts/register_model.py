from pyspark.sql import SparkSession

# 1. Khởi tạo SparkSession với Hive support để tạo bảng trong metastore
spark = (SparkSession.builder
         .appName("Register-Delta-Tables")
         .enableHiveSupport()
         .getOrCreate())

# 2. Tạo database gold nếu chưa có
spark.sql("""
  CREATE DATABASE IF NOT EXISTS gold
""")

# 3. Đăng ký bảng tip_predictions_lr
spark.sql("""
  CREATE TABLE IF NOT EXISTS gold.tip_predictions_lr
  USING DELTA
  LOCATION 's3a://deltalake/gold/tip_predictions_lr'
""")

# 4. Đăng ký bảng kmeans_clusters
spark.sql("""
  CREATE TABLE IF NOT EXISTS gold.kmeans_clusters
  USING DELTA
  LOCATION 's3a://deltalake/gold/kmeans_clusters'
""")

print("✅ Đã đăng ký thành công 2 bảng Delta vào schema `gold`")

spark.stop()
