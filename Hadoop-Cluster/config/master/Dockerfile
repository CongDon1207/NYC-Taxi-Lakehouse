# Base Image
FROM base

USER hadoopquochuy
WORKDIR /home/hadoopquochuy

# Copy Hadoop configuration files
COPY config/core-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/core-site.xml
COPY config/hdfs-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/hdfs-site.xml
COPY config/mapred-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/mapred-site.xml
COPY config/yarn-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/yarn-site.xml
# COPY config/spark-defaults.conf /home/hadoopquochuy/spark/conf/spark-defaults.conf

USER root
# Convert files to Unix format
RUN dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/core-site.xml && \
    dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/hdfs-site.xml && \
    dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/yarn-site.xml && \
    dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/mapred-site.xml
    # dos2unix /home/hadoopquochuy/spark/conf/spark-defaults.conf

# Start SSH and Hadoop services
CMD ["/bin/bash", "-c", "service ssh start && su - hadoopquochuy && bash"]
