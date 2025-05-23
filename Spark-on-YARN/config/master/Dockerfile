# Base Image
FROM base

USER hadoopquochuy
WORKDIR /home/hadoopquochuy
# Set environment variables
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /home/hadoopquochuy/hadoop/etc/hadoop/hadoop-env.sh


COPY config/mapred-site.xml /home/hadoopquochuy/hadoop/etc/hadoop/mapred-site.xml

USER root
# Convert files to Unix format
RUN dos2unix /home/hadoopquochuy/hadoop/etc/hadoop/mapred-site.xml

# Start SSH and Hadoop services
CMD ["/bin/bash", "-c", "service ssh start && su - hadoopquochuy && bash"]
