# Base Image
FROM base

USER hadoopquochuy
WORKDIR /home/hadoopquochuy

# Biến môi trường (quan trọng để các lệnh sau dùng đúng đường dẫn)
ENV HIVE_HOME=/home/hadoopquochuy/hive
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Copy Hadoop configuration files
COPY config/core-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/core-site.xml
COPY config/hdfs-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/hdfs-site.xml
COPY config/mapred-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/mapred-site.xml
COPY config/yarn-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/yarn-site.xml
# COPY config/spark-defaults.conf /home/hadoopquochuy/spark/conf/spark-defaults.conf
# Copy hive-site.xml
COPY config/hive-site.xml /home/hadoopquochuy/hive/conf/hive-site.xml

USER root
# Convert files to Unix format
RUN dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/core-site.xml && \
    dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/hdfs-site.xml && \
    dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/yarn-site.xml && \
    dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/mapred-site.xml && \
    dos2unix /home/hadoopquochuy/hive/conf/hive-site.xml
    # dos2unix /home/hadoopquochuy/spark/conf/spark-defaults.conf

# Tạo thư mục logs cho Hive và thư mục lib nếu chưa có
RUN mkdir -p ${HIVE_HOME}/logs && \
    mkdir -p ${HIVE_HOME}/lib && \
    chown -R hadoopquochuy:hadoopquochuy ${HIVE_HOME}

# Tải và cài đặt MySQL Connector/J
ARG MYSQL_CONNECTOR_J_VERSION=8.0.33
ARG MYSQL_CONNECTOR_J_DOWNLOAD_URL=https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-${MYSQL_CONNECTOR_J_VERSION}.tar.gz

RUN apt-get update && apt-get install -y wget tar && \
    cd /tmp && \
    wget -q -O mysql-connector-j.tar.gz "${MYSQL_CONNECTOR_J_DOWNLOAD_URL}" && \
    tar -xzf mysql-connector-j.tar.gz && \
    cp mysql-connector-j-${MYSQL_CONNECTOR_J_VERSION}/mysql-connector-j-${MYSQL_CONNECTOR_J_VERSION}.jar ${HIVE_HOME}/lib/mysql-connector-java-${MYSQL_CONNECTOR_J_VERSION}.jar && \
    chown hadoopquochuy:hadoopquochuy ${HIVE_HOME}/lib/mysql-connector-java-${MYSQL_CONNECTOR_J_VERSION}.jar && \
    rm -rf mysql-connector-j.tar.gz mysql-connector-j-${MYSQL_CONNECTOR_J_VERSION}* && \
    # Bỏ lệnh apt-get purge ở đây để tránh lỗi phụ thuộc
    # apt-get purge -y --auto-remove wget tar && \
    rm -rf /var/lib/apt/lists/*



# Start SSH and Hadoop services
CMD ["/bin/bash", "-c", "service ssh start && su - hadoopquochuy && bash"]
