#!/usr/bin/env bash

# Script tự động đăng ký Delta tables vào Hive Metastore
# Usage: chmod +x register_delta_tables.sh && ./register_delta_tables.sh

spark-sql \
  --packages io.delta:delta-core_2.13:2.4.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" << 'EOF'

-- Tạo database nếu chưa có
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

-- Đăng ký bảng Silver layer
CREATE TABLE IF NOT EXISTS silver.nyc_taxi_enriched
USING DELTA
LOCATION 'hdfs://quochuy-master:9000/deltalake/silver/nyc_taxi_enriched';

-- Đăng ký bảng Gold layer
CREATE TABLE IF NOT EXISTS gold.daily_taxi_metrics
USING DELTA
LOCATION 'hdfs://quochuy-master:9000/deltalake/gold/daily_taxi_metrics';

EOF
