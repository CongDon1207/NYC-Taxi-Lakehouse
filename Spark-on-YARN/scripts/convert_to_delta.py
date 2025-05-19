#!/usr/bin/env python3
"""
Chuyển parquet FHVHV ➜ Delta, partition theo pickup_date
Path Delta: /lakehouse/bronze/trips_fhvhv_2024
"""

from pyspark.sql import SparkSession, functions as F

# 1. Khởi tạo Spark – ĐÃ cài Delta jar sẵn trong image
spark = (SparkSession.builder
         .appName("ConvertFHVHVtoDelta")
         .config("spark.sql.extensions",
                 "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog",
                 "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

SOURCE = "hdfs:///raw/fhvhv/parquet"              # đường dẫn parquet (đã mount)
TARGET = "hdfs:///lakehouse/bronze/trips_fhvhv_2024"   # đích Delta (HDFS volume)

# 2. Đọc parquet & chuẩn hoá schema
df = (spark.read.parquet(SOURCE)
      .withColumn("pickup_date", F.to_date("pickup_datetime"))
      .select(  # Liệt kê đúng thứ tự & tên cột mong muốn
          "hvfhs_license_num",
          "pickup_datetime",
          "dropoff_datetime",
          "PUlocationID",
          "DOlocationID",
          "pickup_date",
          "trip_miles",
          "base_passenger_fare",     # <— thay vì fare_amount
            "sales_tax",
            "congestion_surcharge",
            "airport_fee"
      ))

# 3. Ghi Delta – overwrite lần đầu, có partition
(df.write
   .format("delta")
   .mode("overwrite")
   .partitionBy("pickup_date")
   .save(TARGET))

# 4. In ra vài thông tin xác nhận
print("=== WRITE DONE ===")
spark.read.format("delta").load(TARGET).printSchema()

spark.stop()
