-- Tạo DB chứa bronze layer nếu chưa có
CREATE DATABASE IF NOT EXISTS lakehouse_bronze;
USE lakehouse_bronze;

-- Đăng ký bảng Delta đã được tạo ở bước convert_to_delta.py
CREATE TABLE IF NOT EXISTS trips_fhvhv_2024
USING DELTA
LOCATION 'hdfs:///lakehouse/bronze/trips_fhvhv_2024';



