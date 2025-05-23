FROM base

USER hadoopquochuy
WORKDIR /home/hadoopquochuy


RUN wget -q https://archive.apache.org/dist/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz && \
    tar -xzf apache-hive-2.3.9-bin.tar.gz && \
    mv apache-hive-2.3.9-bin hive && \
    rm apache-hive-2.3.9-bin.tar.gz

# Thiết lập biến môi trường Hive
ENV HIVE_HOME=/home/hadoopquochuy/hive

ENV PATH=$PATH:$HIVE_HOME/bin

COPY hive-site.xml $HIVE_HOME/conf/hive-site.xml

# Cài đặt MySQL Connector
RUN wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.20/mysql-connector-java-8.0.20.jar && \
    mv mysql-connector-java-8.0.20.jar $HIVE_HOME/lib/

RUN echo "export HIVE_HOME=/home/hadoopquochuy/hive" >> /home/hadoopquochuy/.bashrc && \
    echo "export PATH=\$PATH:\$HIVE_HOME/bin" >> /home/hadoopquochuy/.bashrc
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /home/hadoopquochuy/hadoop/etc/hadoop/hadoop-env.sh

# Expose port Hive Metastore (9083)
EXPOSE 9083

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

