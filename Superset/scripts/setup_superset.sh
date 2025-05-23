#!/bin/bash

# Thiết lập database và admin 
echo "=== Khởi tạo database ==="
docker exec -it superset superset db upgrade

echo "=== Tạo admin user ==="
docker exec -it superset superset fab create-admin \
  --username admin \
  --firstname Superset \
  --lastname Admin \
  --email admin@example.com \
  --password AdminPassword123! 

echo "=== Khởi tạo roles ==="
docker exec -it superset superset init

# Cài driver Trino 
echo "=== Cài đặt driver Trino ==="
docker exec -it superset pip install trino sqlalchemy-trino

echo "=== Khởi động lại Superset ==="
docker restart superset

echo "HOÀN TẤT! Truy cập http://localhost:8088"