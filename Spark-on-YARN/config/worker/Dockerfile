# Base Image
FROM base

USER hadoopquochuy
WORKDIR /home/hadoopquochuy

# Spark config
RUN wget https://archive.apache.org/dist/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3-scala2.13.tgz && \
    tar -xvzf spark-3.4.4-bin-hadoop3-scala2.13.tgz && \
    mv spark-3.4.4-bin-hadoop3-scala2.13 spark && \
    rm spark-3.4.4-bin-hadoop3-scala2.13.tgz

RUN mkdir -p /home/hadoopquochuy/hadoop/hadoop_data/hdfs/datanode && \
    chown -R hadoopquochuy:hadoopquochuy /home/hadoopquochuy/hadoop/hadoop_data/hdfs/datanode

RUN echo 'export PATH=$PATH:/home/hadoopquochuy/spark/bin' >> /home/hadoopquochuy/.bashrc && \
    echo 'export SPARK_HOME=/home/hadoopquochuy/spark' >> /home/hadoopquochuy/.bashrc && \
    echo 'export PATH=$PATH:$SPARK_HOME/bin' >> /home/hadoopquochuy/.bashrc
USER root
# Start SSH and Hadoop services at runtime
CMD ["/bin/bash", "-c", "service ssh start && su - hadoopquochuy && bash"]
