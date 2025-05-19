# Base Image
FROM base

USER hadoopquochuy
WORKDIR /home/hadoopquochuy

# Copy Hadoop & Spark config
COPY config/core-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/core-site.xml
COPY config/hdfs-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/hdfs-site.xml
COPY config/yarn-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/yarn-site.xml
COPY config/spark-defaults.conf /home/hadoopquochuy/spark/conf/spark-defaults.conf



# Copy hive-site.xml
COPY config/hive-site.xml /home/hadoopquochuy/hive/conf/hive-site.xml

# Chuyển file sang Unix
USER root
RUN dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/core-site.xml && \
    dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/hdfs-site.xml && \
    dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/yarn-site.xml && \
    dos2unix /home/hadoopquochuy/spark/conf/spark-defaults.conf && \
    dos2unix /home/hadoopquochuy/hive/conf/hive-site.xml

# ✅ FORMAT HDFS TRONG QUÁ TRÌNH BUILD (theo yêu cầu)
USER hadoopquochuy
RUN hdfs namenode -format

USER root
CMD ["/bin/bash", "-c", "service ssh start && su - hadoopquochuy && bash"]
