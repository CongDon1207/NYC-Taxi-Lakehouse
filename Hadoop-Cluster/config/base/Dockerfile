# Base Image
FROM ubuntu:22.04

# Set root password
RUN echo "root:root" | chpasswd

# Update and install required packages
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y openjdk-11-jdk wget ssh openssh-server dos2unix vim sudo telnet iputils-ping && \
    apt-get clean

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/home/hadoopquochuy/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin

# Create user hadoopquochuy
RUN adduser --disabled-password --gecos "" hadoopquochuy && \
    echo "hadoopquochuy:hadoopquochuy" | chpasswd && \
    usermod -aG sudo hadoopquochuy && \
    echo "hadoopquochuy ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Install Hadoop
USER hadoopquochuy

WORKDIR /home/hadoopquochuy
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz && \
    tar -xzf hadoop-3.4.1.tar.gz && \
    mv hadoop-3.4.1 hadoop && \
    rm hadoop-3.4.1.tar.gz


RUN wget https://dlcdn.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz && \
    tar -xvzf spark-3.4.4-bin-hadoop3.tgz && \
    mv spark-3.4.4-bin-hadoop3 spark && \
    rm spark-3.4.4-bin-hadoop3.tgz


RUN sudo apt-get update && sudo apt-get install -y curl zip unzip

COPY .bashrc /home/hadoopquochuy/.bashrc

USER root
RUN dos2unix /home/hadoopquochuy/.bashrc 

RUN chown hadoopquochuy:hadoopquochuy /home/hadoopquochuy/.bashrc

USER hadoopquochuy
WORKDIR /home/hadoopquochuy

RUN curl -s "https://get.sdkman.io" | bash && \
    echo 'source "/home/hadoopquochuy/.sdkman/bin/sdkman-init.sh"' >> /home/hadoopquochuy/.bashrc && \
    bash -c "source /home/hadoopquochuy/.sdkman/bin/sdkman-init.sh && sdk install scala 2.12.17"


# Set JAVA_HOME in hadoop-env.sh
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /home/hadoopquochuy/hadoop/etc/hadoop/hadoop-env.sh
    

# Copy Hadoop configuration files

RUN mkdir -p /home/hadoopquochuy/.ssh && \
    ssh-keygen -t rsa -P '' -f /home/hadoopquochuy/.ssh/id_rsa && \
    cat /home/hadoopquochuy/.ssh/id_rsa.pub >> /home/hadoopquochuy/.ssh/authorized_keys && \
    chmod 600 /home/hadoopquochuy/.ssh/authorized_keys && \
    chown -R hadoopquochuy:hadoopquochuy /home/hadoopquochuy/.ssh

# Expose ports
EXPOSE 9000 50070 9001 9002 9003 9004 9005 9006 9870 9864 8088 8042 8080 7070 4040
