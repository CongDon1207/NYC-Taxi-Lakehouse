FROM base

USER hadoopquochuy
WORKDIR /home/hadoopquochuy

# Cài đặt wget
RUN apt-get update && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*


RUN wget -q https://dlcdn.apache.org/hive/hive-4.0.1/apache-hive-4.0.1-bin.tar.gz && \
    tar -xzf apache-hive-4.0.1-bin.tar.gz && \
    mv apache-hive-4.0.1-bin hive && \
    rm apache-hive-4.0.1-bin.tar.gz

# Thiết lập biến môi trường Hive
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HIVE_HOME/bin

COPY hive-site.xml $HIVE_HOME/conf/hive-site.xml

# Cài đặt MySQL Connector
RUN wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.20/mysql-connector-java-8.0.20.jar && \
    mv mysql-connector-java-8.0.20.jar $HIVE_HOME/lib/

# Expose port Hive Metastore (9083)
EXPOSE 9083

# Khởi động Hive Metastore service
CMD ["hive", "--service", "metastore", "-p", "9083"]
