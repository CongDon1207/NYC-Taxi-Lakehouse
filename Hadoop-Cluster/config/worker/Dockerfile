# Base Image
FROM base

USER hadoopquochuy
WORKDIR /home/hadoopquochuy


# Copy Hadoop configuration files
COPY config/core-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/core-site.xml
COPY config/hdfs-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/hdfs-site.xml
COPY config/yarn-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/yarn-site.xml

RUN mkdir -p /home/hadoopquochuy/hadoop/hadoop_data/hdfs/datanode && \
    chown -R hadoopquochuy:hadoopquochuy /home/hadoopquochuy/hadoop/hadoop_data/hdfs/datanode

USER root
# Convert files to Unix format
RUN dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/core-site.xml && \
    dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/hdfs-site.xml && \
    dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/yarn-site.xml


# Format HDFS
USER hadoopquochuy
RUN hdfs namenode -format


USER root
# Start SSH and Hadoop services at runtime
CMD ["/bin/bash", "-c", "service ssh start && su - hadoopquochuy && bash"]
