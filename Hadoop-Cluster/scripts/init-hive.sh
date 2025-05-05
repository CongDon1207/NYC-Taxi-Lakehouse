#!/bin/bash
set -e

# Đường dẫn thư mục cài đặt Hive
HIVE_HOME="/home/hadoopquochuy/hive"

echo "📁 Hive home: $HIVE_HOME"

# ---[ Khởi tạo schema Derby (chỉ chạy 1 lần) ]---
if [ ! -d "$HIVE_HOME/metastore_db/seg0" ]; then
  echo "⚙️  Đang khởi tạo Hive Metastore schema (Derby)..."
  "$HIVE_HOME/bin/schematool" -dbType derby -initSchema
else
  echo "✅ Hive Metastore schema đã tồn tại, bỏ qua."
fi


