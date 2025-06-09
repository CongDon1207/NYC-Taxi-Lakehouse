#!/bin/bash
# Script để tạo Kafka topic mới

# dat replication-factor la 1 vi chi co 1 broker trong cluster
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic nyc_taxi_stream
