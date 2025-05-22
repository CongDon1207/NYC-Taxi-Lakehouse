# Base Image
FROM base

USER hadoopquochuy
WORKDIR /home/hadoopquochuy

# Spark config
RUN wget https://archive.apache.org/dist/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3-scala2.13.tgz && \
    tar -xvzf spark-3.4.4-bin-hadoop3-scala2.13.tgz && \
    mv spark-3.4.4-bin-hadoop3-scala2.13 spark && \
    rm spark-3.4.4-bin-hadoop3-scala2.13.tgz


COPY config/spark-defaults.conf /home/hadoopquochuy/spark/conf/spark-defaults.conf


# Chuyển file sang Unix
USER root
RUN dos2unix /home/hadoopquochuy/spark/conf/spark-defaults.conf 


USER hadoopquochuy
WORKDIR /home/hadoopquochuy

RUN curl -s "https://get.sdkman.io" | bash && \
    echo 'source "/home/hadoopquochuy/.sdkman/bin/sdkman-init.sh"' >> /home/hadoopquochuy/.bashrc && \
    bash -c "source /home/hadoopquochuy/.sdkman/bin/sdkman-init.sh && sdk install scala 2.13.0"


RUN echo 'export PATH=$PATH:/home/hadoopquochuy/spark/bin' >> /home/hadoopquochuy/.bashrc && \
    echo 'export SPARK_HOME=/home/hadoopquochuy/spark' >> /home/hadoopquochuy/.bashrc && \
    echo 'export PATH=$PATH:$SPARK_HOME/bin' >> /home/hadoopquochuy/.bashrc
USER root
CMD ["/bin/bash", "-c", "service ssh start && su - hadoopquochuy && bash"]
