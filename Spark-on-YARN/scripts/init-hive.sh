#!/bin/bash

# Script để khởi tạo Hive Metastore và khởi động các dịch vụ Hive.
# Chạy script này trên container master của Hive.
# PHIÊN BẢN NÀY HOÀN TOÀN BỎ QUA CÁC THAO TÁC HDFS.
# YÊU CẦU:
# 1. Thư mục HDFS warehouse (/user/hive/warehouse) PHẢI được tạo và cấp quyền THỦ CÔNG trước đó.
# 2. Cơ sở dữ liệu MySQL cho Metastore (ví dụ: 'metastore_db') đã được DỌN SẠCH (DROP & CREATE)
#    trước khi chạy script này để `schematool -initSchema` thành công lần đầu.
# 3. Container master đã cài đặt 'net-tools' (cho lệnh netstat).

export HIVE_HOME=${HIVE_HOME:-/home/hadoopquochuy/hive}
export HADOOP_HOME=${HADOOP_HOME:-/home/hadoopquochuy/hadoop} # Vẫn cần cho schematool và hive cmds
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$PATH

echo "INFO: Ensuring Hive log directory exists: $HIVE_HOME/logs"
mkdir -p "$HIVE_HOME/logs"
echo # Dòng trống

# ---[ BỎ QUA HOÀN TOÀN PHẦN KIỂM TRA VÀ TẠO THƯ MỤC HDFS WAREHOUSE ]---
echo "INFO: Skipping HDFS warehouse directory operations. Ensure it's created manually if needed."
echo # Dòng trống

# ---[ 1. Khởi tạo/Kiểm tra Schema cho Hive Metastore (MySQL) ]---
echo "INFO: Checking Hive Metastore schema status..."
# Chạy -info để kiểm tra phiên bản schema hiện tại
schematool -dbType mysql -info --verbose > /tmp/schematool_info.log 2>&1
SCHEMA_INFO_EXIT_CODE=$?
CURRENT_SCHEMA_VERSION=$(grep "Metastore schema version:" /tmp/schematool_info.log | awk '{print $4}')
# Đặt phiên bản schema mong đợi cho Hive của bạn (ví dụ: 2.3.0 cho Hive 2.3.x)
EXPECTED_SCHEMA_VERSION="2.3.0" # <<== CẬP NHẬT CHO ĐÚNG PHIÊN BẢN HIVE CỦA BẠN

if [ $SCHEMA_INFO_EXIT_CODE -ne 0 ] || [ -z "$CURRENT_SCHEMA_VERSION" ]; then
    echo "INFO: Could not retrieve current schema version, or schema does not exist. Attempting to initialize schema..."
    schematool -dbType mysql -initSchema --verbose
    if [ $? -ne 0 ]; then
        echo "ERROR: Hive Metastore schema initialization FAILED. Please check logs and MySQL database status."
        echo "ERROR: You might need to DROP and RECREATE the 'metastore_db' database if it's corrupted or partially initialized."
        # exit 1 # Cân nhắc thoát nếu lỗi
    else
        echo "INFO: Hive Metastore schema initialization completed successfully."
    fi
elif [ "$CURRENT_SCHEMA_VERSION" == "$EXPECTED_SCHEMA_VERSION" ]; then
    echo "INFO: Hive Metastore schema version $CURRENT_SCHEMA_VERSION is up to date."
else
    echo "WARNING: Hive Metastore schema version is $CURRENT_SCHEMA_VERSION, expected $EXPECTED_SCHEMA_VERSION."
    echo "WARNING: You might need to run 'schematool -dbType mysql -upgradeSchema' or investigate further."
    # Ví dụ:
    # echo "INFO: Attempting to upgrade schema from $CURRENT_SCHEMA_VERSION to $EXPECTED_SCHEMA_VERSION..."
    # schematool -dbType mysql -upgradeSchema --verbose # Hive 3.x dùng: schematool -dbType mysql -upgradeSchema
    # if [ $? -ne 0 ]; then
    #     echo "ERROR: Schema upgrade failed."
    # fi
fi
rm -f /tmp/schematool_info.log
echo # Dòng trống

# ---[ 2. Khởi động Hive Metastore Service (HMS) ]---
echo "INFO: Checking Hive Metastore Service (HMS) status..."
if ! sudo netstat -tulnp | grep -q ':9083.*LISTEN'; then
    echo "INFO: Starting Hive Metastore Service..."
    nohup $HIVE_HOME/bin/hive --service metastore -p 9083 > "$HIVE_HOME/logs/metastore.log" 2>&1 &
    HMS_PID=$!
    echo "INFO: Hive Metastore Service start command issued (PID $HMS_PID). Check $HIVE_HOME/logs/metastore.log"
    sleep 15
    if ps -p $HMS_PID > /dev/null && sudo netstat -tulnp | grep -q ':9083.*LISTEN'; then
        echo "INFO: Hive Metastore Service appears to be running on port 9083."
    else
        echo "ERROR: Hive Metastore Service failed to start or is not listening on port 9083. Check logs."
    fi
else
    echo "INFO: Hive Metastore Service appears to be already running on port 9083."
fi
echo # Dòng trống

# ---[ 3. Khởi động HiveServer2 Service (HS2) ]---
echo "INFO: Checking HiveServer2 (HS2) status..."
if ! sudo netstat -tulnp | grep -q ':10000.*LISTEN'; then
    echo "INFO: Starting HiveServer2 Service..."
    nohup $HIVE_HOME/bin/hive --service hiveserver2 > "$HIVE_HOME/logs/hiveserver2.log" 2>&1 &
    HS2_PID=$!
    echo "INFO: HiveServer2 Service start command issued (PID $HS2_PID). Check $HIVE_HOME/logs/hiveserver2.log"
    sleep 30
    if ps -p $HS2_PID > /dev/null && sudo netstat -tulnp | grep -q ':10000.*LISTEN'; then
        echo "INFO: HiveServer2 Service appears to be running on port 10000."
    else
        echo "ERROR: HiveServer2 Service failed to start or is not listening on port 10000. Check logs."
    fi
else
    echo "INFO: HiveServer2 Service appears to be already running on port 10000."
fi
echo # Dòng trống

echo "INFO: Hive initialization and services startup script finished."
echo "INFO: Please verify services and check logs in $HIVE_HOME/logs for details."