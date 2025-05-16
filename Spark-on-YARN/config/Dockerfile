FROM master

USER hadoopquochuy
WORKDIR /home/hadoopquochuy

COPY master/config/workers /home/hadoopquochuy/hadoop/etc/hadoop/workers

USER root
# Convert files to Unix format
RUN dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/workers

# Format HDFS
USER hadoopquochuy

RUN hdfs namenode -format

USER root
# Start SSH and Hadoop services
CMD ["/bin/bash", "-c", "service ssh start && su - hadoopquochuy && bash"]
